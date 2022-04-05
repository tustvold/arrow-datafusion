use crate::node::ExecutionNode;
use crate::{ArrowResult, Spawner};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::channel::mpsc;
use futures::task::ArcWake;
use std::collections::VecDeque;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

pub struct WorkItem {
    query: Arc<Query>,
    waker: Arc<WorkItemWaker>,
}

impl std::fmt::Debug for WorkItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkItem")
            .field("node", &self.waker.node)
            .field("partition", &self.waker.partition)
            .finish()
    }
}

impl WorkItem {
    pub fn spawn_query(spawner: &Spawner, query: Arc<Query>) {
        println!("Spawning query: {:?}", query);

        for (node_idx, node) in query.nodes.iter().enumerate() {
            for partition in 0..node.node.output_partitions() {
                spawner.spawn(WorkItem {
                    query: query.clone(),
                    waker: Arc::new(WorkItemWaker {
                        query: Arc::downgrade(&query),
                        spawner: spawner.clone(),
                        node: node_idx,
                        partition,
                    }),
                })
            }
        }
    }

    pub fn do_work(self) {
        if self.query.output.is_closed() {
            return;
        }

        let node = self.waker.node;
        let partition = self.waker.partition;

        let waker = futures::task::waker_ref(&self.waker);
        let mut cx = Context::from_waker(&*waker);

        let query_node = &self.query.nodes[node];
        match query_node.node.poll_partition(&mut cx, partition) {
            Poll::Pending => println!("Poll {:?}: Pending", self),
            Poll::Ready(None) => println!("Poll {:?}: None", self),
            Poll::Ready(Some(Ok(batch))) => {
                println!("Poll {:?}: Ok: {}", self, batch.num_rows());
                match query_node.parent_idx {
                    Some(idx) => {
                        println!(
                            "Published batch to node {} partition {}",
                            idx, partition
                        );
                        self.query.nodes[idx].node.push(batch, partition)
                    }
                    None => {
                        println!("Published batch to output");
                        let _ = self.query.output.unbounded_send(Ok(batch));
                    }
                }

                // Reschedule this task for the next batch
                self.waker.spawner.spawn(Self {
                    query: self.query,
                    waker: self.waker.clone(),
                });
            }
            Poll::Ready(Some(Err(e))) => {
                println!("Poll {:?}: Error: {:?}", self, e);
                let _ = self.query.output.unbounded_send(Err(e));
            }
        }
    }
}

struct WorkItemWaker {
    query: Weak<Query>,
    spawner: Spawner,
    node: usize,
    partition: usize,
}

impl ArcWake for WorkItemWaker {
    fn wake(self: Arc<Self>) {
        if let Some(query) = self.query.upgrade() {
            let item = WorkItem {
                query,
                waker: self.clone(),
            };
            println!("Wakeup {:?}", item);
            self.spawner.spawn(item)
        } else {
            println!("Dropped wakeup");
        }
    }

    fn wake_by_ref(s: &Arc<Self>) {
        ArcWake::wake(s.clone())
    }
}

#[derive(Debug)]
pub struct QueryNode {
    node: ExecutionNode,
    parent_idx: Option<usize>,
}

#[derive(Debug)]
pub struct Query {
    nodes: Vec<QueryNode>,
    output: mpsc::UnboundedSender<ArrowResult<RecordBatch>>,
}

impl Drop for Query {
    fn drop(&mut self) {
        println!("Query dropped");
    }
}

impl Query {
    pub async fn new(
        plan: Arc<dyn ExecutionPlan>,
        task_context: Arc<TaskContext>,
    ) -> Result<(Query, mpsc::UnboundedReceiver<ArrowResult<RecordBatch>>)> {
        let mut nodes = Vec::new();
        let mut dequeue = VecDeque::new();
        dequeue.push_back((plan, None));

        while let Some((plan, parent_idx)) = dequeue.pop_front() {
            let children = plan.children();
            dequeue.extend(children.into_iter().map(|plan| (plan, Some(nodes.len()))));

            let node = ExecutionNode::new(plan, task_context.clone()).await?;
            nodes.push(QueryNode { node, parent_idx });
        }

        let (sender, receiver) = mpsc::unbounded();
        Ok((
            Query {
                nodes,
                output: sender,
            },
            receiver,
        ))
    }
}
