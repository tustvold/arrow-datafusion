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
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::stream::{BoxStream, StreamExt};
use parking_lot::{Condvar, Mutex};
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
    shared: Arc<SharedState>,
    local: crossbeam_deque::Worker<WorkItem>,
    steal: Vec<crossbeam_deque::Stealer<WorkItem>>,
}

impl ThreadState {
    fn find_task(&self) -> Option<WorkItem> {
        // Pop a task from the local queue, if not empty.
        self.local.pop().or_else(|| {
            // Otherwise, we need to look for a task elsewhere.
            std::iter::repeat_with(|| {
                // Try stealing a batch of tasks from the global queue.
                self.shared
                    .injector
                    .steal_batch_and_pop(&self.local)
                    // Or try stealing a task from one of the other threads.
                    .or_else(|| self.steal.iter().map(|s| s.steal()).collect())
            })
            // Loop while no task was stolen and any steal operation needs to be retried.
            .find(|s| !s.is_retry())
            // Extract the stolen task, if there is one.
            .and_then(|s| s.success())
        })
    }

    fn run(&self) {
        loop {
            match self.find_task() {
                Some(task) => task.do_work(),
                None => {
                    if !self.wait_for_input() {
                        println!("Worker shutting down");
                        break;
                    }
                }
            }
        }
    }

    /// Park this thread, returning false if this worker should terminate
    fn wait_for_input(&self) -> bool {
        let mut lock = self.shared.inner.lock();
        if lock.is_shutdown {
            return false;
        }
        self.shared.signal.wait(&mut lock);
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

        // TODO: Would LIFO be better?
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
                    .map(|(_, stealer)| stealer.clone())
                    .collect();

                let state = ThreadState {
                    shared: shared.clone(),
                    local,
                    steal,
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

        for worker in self.workers.drain(..) {
            worker.handle.join().expect("worker panicked");
        }
    }
}

#[derive(Debug, Clone)]
pub struct Spawner {
    shared: Arc<SharedState>,
}

impl Spawner {
    fn spawn(&self, task: WorkItem) {
        println!("Spawning {:?}", task);
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

        assert_eq!(scheduled, expected);
    }
}
