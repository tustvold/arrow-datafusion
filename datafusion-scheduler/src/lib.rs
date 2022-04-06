// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::query::{Query, WorkItem};
use crossbeam_deque::Steal;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::stream::{BoxStream, StreamExt};
use parking_lot::{Condvar, Mutex};
use std::cell::RefCell;
use std::sync::Arc;
use std::thread::JoinHandle;

mod node;
mod query;
mod repartition;

pub struct Scheduler {
    pool: WorkerPool,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            pool: WorkerPool::new(2),
        }
    }

    pub async fn schedule_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, ArrowResult<RecordBatch>>> {
        let (query, receiver) = Query::new(plan, context).await?;
        WorkItem::spawn_query(self.pool.spawner(), Arc::new(query));
        Ok(receiver.boxed())
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        self.pool.shutdown()
    }
}

#[derive(Debug)]
struct WorkerThread {
    handle: JoinHandle<()>,
}

struct ThreadState {
    idx: usize,
    shared: Arc<SharedState>,
    local: crossbeam_deque::Worker<WorkItem>,
    steal: Vec<(usize, crossbeam_deque::Stealer<WorkItem>)>,
}

thread_local! {
    static CURRENT_THREAD: RefCell<Option<ThreadState>> = RefCell::new(None)
}

/// Spawns a [`WorkItem`] to the worker local queue
fn spawn_local(item: WorkItem) {
    ThreadState::local(|s| {
        println!("Spawning {:?} to local worker {}", item, s.idx);
        s.local.push(item)
    })
}

impl ThreadState {
    fn find_task(&self) -> Option<WorkItem> {
        if let Some(item) = self.local.pop() {
            println!("Worker {} fetched {:?} from local queue", self.idx, item);
            return Some(item);
        }

        loop {
            match self.shared.injector.steal_batch_and_pop(&self.local) {
                Steal::Success(item) => {
                    println!("Worker {} fetched {:?} from injector", self.idx, item);
                    return Some(item);
                }
                Steal::Retry => continue,
                Steal::Empty => {}
            }

            // TODO: Should the search order be randomized?
            for (idx, steal) in &self.steal {
                match steal.steal() {
                    Steal::Success(item) => {
                        println!(
                            "Worker {} stole {:?} from worker {}",
                            self.idx, item, idx
                        );
                        return Some(item);
                    }
                    Steal::Retry => continue,
                    Steal::Empty => {}
                }
            }

            println!("Worker {} failed to find any work", self.idx);
            return None;
        }
    }

    fn local<F, T>(f: F) -> T
    where
        F: FnOnce(&Self) -> T,
    {
        CURRENT_THREAD.with(|s| f(s.borrow().as_ref().expect("worker thread")))
    }

    fn run(self) {
        let shared = self.shared.clone();
        let worker_idx = self.idx;

        CURRENT_THREAD.with(|s| *s.borrow_mut() = Some(self));

        loop {
            match Self::local(Self::find_task) {
                Some(task) => task.do_work(),
                None => {
                    if !Self::wait_for_input(worker_idx, &shared) {
                        println!("Worker {} shutting down", worker_idx);
                        break;
                    }
                }
            }
        }
    }

    /// Park this thread, returning false if this worker should terminate
    fn wait_for_input(worker_idx: usize, shared: &SharedState) -> bool {
        let mut lock = shared.inner.lock();
        if lock.is_shutdown {
            return false;
        }

        println!("Worker {} entered sleep", worker_idx);
        shared.signal.wait(&mut lock);
        println!("Worker {} exited sleep", worker_idx);

        !lock.is_shutdown
    }
}

#[derive(Debug)]
struct SharedState {
    injector: crossbeam_deque::Injector<WorkItem>,
    signal: Condvar,
    inner: Arc<Mutex<SharedStateInner>>,
}

#[derive(Debug)]
struct SharedStateInner {
    is_shutdown: bool,
}

#[derive(Debug)]
struct WorkerPool {
    shared: Arc<SharedState>,
    workers: Vec<WorkerThread>,
}

impl WorkerPool {
    fn new(workers: usize) -> Self {
        let shared = Arc::new(SharedState {
            injector: crossbeam_deque::Injector::new(),
            signal: Default::default(),
            inner: Arc::new(Mutex::new(SharedStateInner { is_shutdown: false })),
        });

        let worker_queues: Vec<_> = (0..workers)
            .map(|_| crossbeam_deque::Worker::new_fifo())
            .collect();
        let stealers: Vec<_> = worker_queues.iter().map(|x| x.stealer()).collect();

        let workers = worker_queues
            .into_iter()
            .enumerate()
            .map(|(idx, local)| {
                let steal = stealers
                    .iter()
                    .enumerate()
                    .filter(|(steal_idx, _)| *steal_idx != idx)
                    .map(|(steal_idx, steal)| (steal_idx, steal.clone()))
                    .collect();

                let state = ThreadState {
                    idx,
                    local,
                    steal,
                    shared: shared.clone(),
                };

                WorkerThread {
                    handle: std::thread::spawn(move || state.run()),
                }
            })
            .collect();

        WorkerPool { shared, workers }
    }

    fn spawner(&self) -> Spawner {
        Spawner {
            shared: self.shared.clone(),
        }
    }

    fn shutdown(&mut self) {
        println!("Shutting down worker pool");
        self.shared.inner.lock().is_shutdown = true;
        self.shared.signal.notify_all();

        for (idx, worker) in self.workers.drain(..).enumerate() {
            if let Err(_) = worker.handle.join() {
                panic!("worker {} panicked", idx)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Spawner {
    shared: Arc<SharedState>,
}

impl Spawner {
    fn spawn(&self, task: WorkItem) {
        println!("Spawning {:?} to any worker", task);
        self.shared.injector.push(task);

        // Ensure that at least one worker thread is awake
        self.shared.signal.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, PrimitiveArray};
    use datafusion::arrow::datatypes::{ArrowPrimitiveType, Float64Type, Int32Type};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::TryStreamExt;
    use rand::distributions::uniform::SampleUniform;
    use rand::{thread_rng, Rng};
    use std::ops::Range;

    fn generate_primitive<T, R>(
        rng: &mut R,
        len: usize,
        valid_percent: f64,
        range: Range<T::Native>,
    ) -> ArrayRef
    where
        T: ArrowPrimitiveType,
        T::Native: SampleUniform,
        R: Rng,
    {
        Arc::new(PrimitiveArray::<T>::from_iter((0..len).map(|_| {
            rng.gen_bool(valid_percent)
                .then(|| rng.gen_range(range.clone()))
        })))
    }

    fn generate_batch<R: Rng>(rng: &mut R, row_count: usize) -> RecordBatch {
        let a = generate_primitive::<Int32Type, _>(rng, row_count, 0.5, 0..1000);
        let b = generate_primitive::<Float64Type, _>(rng, row_count, 0.5, 0. ..1000.);
        RecordBatch::try_from_iter([("a", a), ("b", b)]).unwrap()
    }

    // This is a temporary hack until #1939 is fixed
    fn format_batches(batches: &[RecordBatch]) -> Vec<String> {
        let formatted = arrow::util::pretty::pretty_format_batches(batches)
            .unwrap()
            .to_string();
        let mut split: Vec<_> = formatted.split('\n').map(ToString::to_string).collect();

        let num_lines = split.len();
        if num_lines > 3 {
            split.as_mut_slice()[2..num_lines - 1].sort_unstable()
        }
        split
    }

    #[tokio::test]
    async fn test_simple() {
        let scheduler = Scheduler::new();
        let mut rng = thread_rng();

        let batches_per_partition = 3;
        let rows_per_batch = 1000;
        let num_partitions = 2;

        let batches: Vec<Vec<_>> = std::iter::from_fn(|| {
            Some(
                std::iter::from_fn(|| Some(generate_batch(&mut rng, rows_per_batch)))
                    .take(batches_per_partition)
                    .collect(),
            )
        })
        .take(num_partitions)
        .collect();

        let schema = batches.first().unwrap().first().unwrap().schema();
        let provider = MemTable::try_new(schema, batches).unwrap();

        let config = SessionConfig::new().with_target_partitions(2);
        let mut context = SessionContext::with_config(config);

        context.register_table("t", Arc::new(provider)).unwrap();

        let task = context.task_ctx();

        let query = context.sql("SELECT * FROM t where a > 100").await.unwrap();

        let plan = query.create_physical_plan().await.unwrap();

        println!("Plan: {}", displayable(plan.as_ref()).indent());

        let stream = scheduler.schedule_plan(plan, task).await.unwrap();
        let scheduled: Vec<_> = stream.try_collect().await.unwrap();
        let expected = query.collect().await.unwrap();

        let scheduled_formatted = format_batches(&scheduled);
        let expected_formatted = format_batches(&expected);

        assert_eq!(
            scheduled_formatted, expected_formatted,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_formatted, scheduled_formatted
        );
    }
}
