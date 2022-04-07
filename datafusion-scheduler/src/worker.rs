use std::cell::RefCell;
use std::sync::Arc;
use std::thread::JoinHandle;

use crossbeam_deque::Steal;
use log::{debug, error, info, trace};
use parking_lot::{Condvar, Mutex};

use crate::WorkItem;

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
pub fn spawn_local(item: WorkItem) {
    ThreadState::local(|s| {
        trace!("Spawning {:?} to local worker {}", item, s.idx);
        s.local.push(item)
    })
}

impl ThreadState {
    fn find_task(&self) -> Option<WorkItem> {
        if let Some(item) = self.local.pop() {
            trace!("Worker {} fetched {:?} from local queue", self.idx, item);
            return Some(item);
        }

        loop {
            match self.shared.injector.steal_batch_and_pop(&self.local) {
                Steal::Success(item) => {
                    trace!("Worker {} fetched {:?} from injector", self.idx, item);
                    return Some(item);
                }
                Steal::Retry => continue,
                Steal::Empty => {}
            }

            // TODO: Should the search order be randomized?
            for (idx, steal) in &self.steal {
                match steal.steal() {
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
        let shared = self.shared.clone();
        let worker_idx = self.idx;

        CURRENT_THREAD.with(|s| *s.borrow_mut() = Some(self));

        loop {
            match Self::local(Self::find_task) {
                Some(task) => task.do_work(),
                None => {
                    if !Self::wait_for_input(worker_idx, &shared) {
                        info!("Worker {} shutting down", worker_idx);
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

        debug!("Worker {} entered sleep", worker_idx);
        shared.signal.wait(&mut lock);
        debug!("Worker {} exited sleep", worker_idx);

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
pub struct WorkerPool {
    shared: Arc<SharedState>,
    workers: Vec<WorkerThread>,
}

impl WorkerPool {
    pub fn new(workers: usize) -> Self {
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

    pub fn spawner(&self) -> Spawner {
        Spawner {
            shared: self.shared.clone(),
        }
    }

    pub fn shutdown(&mut self) {
        info!("Shutting down worker pool");
        self.shared.inner.lock().is_shutdown = true;
        self.shared.signal.notify_all();

        for (idx, worker) in self.workers.drain(..).enumerate() {
            if let Err(_) = worker.handle.join() {
                error!("worker {} panicked", idx)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Spawner {
    shared: Arc<SharedState>,
}

impl Spawner {
    pub fn spawn(&self, task: WorkItem) {
        debug!("Spawning {:?} to any worker", task);
        self.shared.injector.push(task);

        // Ensure that at least one worker thread is awake
        self.shared.signal.notify_one();
    }
}
