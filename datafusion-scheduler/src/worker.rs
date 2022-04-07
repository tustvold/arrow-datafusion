use std::cell::RefCell;
use std::sync::Arc;
use std::thread::JoinHandle;

use crossbeam_deque::Steal;
use log::{debug, error, info, trace};
use parking_lot::{Condvar, Mutex};

use crate::WorkItem;

#[derive(Debug)]
struct WorkerThread {
    signal: Condvar,
    state: Mutex<WorkerThreadState>,
    stealer: crossbeam_deque::Stealer<WorkItem>,
}

impl WorkerThread {
    pub fn wake(&self) {
        self.signal.notify_one();
    }

    pub fn shutdown(&self) {
        self.state.lock().is_shutdown = true;
        self.signal.notify_one();
    }
}

#[derive(Debug)]
struct WorkerThreadState {
    is_shutdown: bool,
}

struct WorkerThreadLocalState {
    idx: usize,
    pool: Arc<PoolState>,
    local: crossbeam_deque::Worker<WorkItem>,
}

thread_local! {
    static CURRENT_THREAD: RefCell<Option<WorkerThreadLocalState>> = RefCell::new(None)
}

/// Spawns a [`WorkItem`] to the worker local queue
pub fn spawn_local(item: WorkItem) {
    WorkerThreadLocalState::local(|s| {
        trace!("Spawning {:?} to local worker {}", item, s.idx);
        s.local.push(item)
    })
}

impl WorkerThreadLocalState {
    fn find_task(&self) -> Option<WorkItem> {
        if let Some(item) = self.local.pop() {
            trace!("Worker {} fetched {:?} from local queue", self.idx, item);
            return Some(item);
        }

        loop {
            match self.pool.injector.steal_batch_and_pop(&self.local) {
                Steal::Success(item) => {
                    trace!("Worker {} fetched {:?} from injector", self.idx, item);
                    return Some(item);
                }
                Steal::Retry => continue,
                Steal::Empty => {}
            }

            // TODO: Should the search order be randomized?
            for (idx, worker) in self.pool.workers.iter().enumerate() {
                if idx == self.idx {
                    continue
                }

                match worker.stealer.steal() {
                    Steal::Success(item) => {
                        trace!(
                            "Worker {} stole {:?} from worker {}",
                            self.idx,
                            item,
                            idx
                        );
                        return Some(item);
                    }
                    Steal::Retry => continue,
                    Steal::Empty => {}
                }
            }

            trace!("Worker {} failed to find any work", self.idx);
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
        let pool = self.pool.clone();
        let worker_idx = self.idx;

        CURRENT_THREAD.with(|s| *s.borrow_mut() = Some(self));

        loop {
            match Self::local(Self::find_task) {
                Some(task) => task.do_work(),
                None => {
                    if !pool.wait_for_input(worker_idx) {
                        info!("Worker {} shutting down", worker_idx);
                        break;
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
struct PoolState {
    injector: crossbeam_deque::Injector<WorkItem>,
    workers: Vec<WorkerThread>,
}

impl PoolState {
    /// Park this thread, returning false if this worker should terminate
    fn wait_for_input(&self, worker_idx: usize) -> bool {
        let worker = &self.workers[worker_idx];

        let mut state = worker.state.lock();
        if state.is_shutdown {
            return false;
        }

        debug!("Worker {} entered sleep", worker_idx);
        worker.signal.wait(&mut state);
        debug!("Worker {} exited sleep", worker_idx);

        !state.is_shutdown
    }
}

#[derive(Debug)]
pub struct WorkerPool {
    pool: Arc<PoolState>,
    handles: Vec<JoinHandle<()>>,
}

impl WorkerPool {
    pub fn new(workers: usize) -> Self {
        let worker_queues: Vec<_> = (0..workers)
            .map(|_| crossbeam_deque::Worker::new_fifo())
            .collect();

        let worker_threads: Vec<_> = worker_queues
            .iter()
            .map(|queue| WorkerThread {
                signal: Default::default(),
                state: Mutex::new(WorkerThreadState { is_shutdown: false }),
                stealer: queue.stealer(),
            })
            .collect();

        let pool = Arc::new(PoolState {
            injector: crossbeam_deque::Injector::new(),
            workers: worker_threads,
        });

        let handles = worker_queues
            .into_iter()
            .enumerate()
            .map(|(idx, local)| {
                let state = WorkerThreadLocalState {
                    idx,
                    local,
                    pool: pool.clone(),
                };

                std::thread::Builder::new()
                    .name(format!("df-worker-{}", idx))
                    .spawn(move || state.run())
                    .unwrap()
            })
            .collect();

        WorkerPool {
            pool,
            handles,
        }
    }

    pub fn spawner(&self) -> Spawner {
        Spawner {
            pool: self.pool.clone(),
        }
    }

    pub fn shutdown(&mut self) {
        info!("Shutting down worker pool");
        for worker in &self.pool.workers {
            worker.shutdown();
        }

        for (idx, handle) in self.handles.drain(..).enumerate() {
            if let Err(_) = handle.join() {
                error!("worker {} panicked", idx)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Spawner {
    pool: Arc<PoolState>,
}

impl Spawner {
    pub fn spawn(&self, task: WorkItem) {
        debug!("Spawning {:?} to any worker", task);
        self.pool.injector.push(task);

        // TODO: More sophisticated wakeup policy
        // NB: THIS WILL CURRENTLY RESULT IN ONLY A SINGLE THREAD RUNNING
        self.pool.workers.first().unwrap().wake();
    }
}
