use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use futures::channel::mpsc;
use futures::task::ArcWake;
use log::{debug, trace};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};

use crate::node::ExecutionNode;
use crate::repartition::RepartitionNode;
use crate::{
    worker::{spawn_local, Spawner},
    ArrowResult,
};

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
    pub fn spawn_query(spawner: Spawner, query: Arc<Query>) {
        debug!("Spawning query: {:#?}", query);

        for (node_idx, node) in query.nodes.iter().enumerate() {
            for partition in 0..node.node.output_partitions() {
                spawner.spawn(WorkItem {
                    query: query.clone(),
                    waker: Arc::new(WorkItemWaker {
                        query: Arc::downgrade(&query),
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
            Poll::Ready(Some(Ok(batch))) => {
                trace!("Poll {:?}: Ok: {}", self, batch.num_rows());
                match query_node.parent {
                    Some(link) => {
                        trace!(
                            "Published batch to node {:?} partition {}",
                            link,
                            partition
                        );
                        self.query.nodes[link.node]
                            .node
                            .push(batch, link.child, partition)
                    }
                    None => {
                        trace!("Published batch to output");
                        let _ = self.query.output.unbounded_send(Ok(batch));
                    }
                }

                // Reschedule this task for the next batch
                spawn_local(Self {
                    query: self.query,
                    waker: self.waker.clone(),
                });
            }
            Poll::Ready(Some(Err(e))) => {
                trace!("Poll {:?}: Error: {:?}", self, e);
                let _ = self.query.output.unbounded_send(Err(e));
                if let Some(link) = query_node.parent {
                    self.query.nodes[link.node]
                        .node
                        .close(link.child, partition)
                }
            }
            Poll::Ready(None) => {
                trace!("Poll {:?}: None", self);
                if let Some(link) = query_node.parent {
                    self.query.nodes[link.node]
                        .node
                        .close(link.child, partition)
                }
            }
            Poll::Pending => trace!("Poll {:?}: Pending", self),
        }
    }
}

struct WorkItemWaker {
    query: Weak<Query>,
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
            trace!("Wakeup {:?}", item);
            spawn_local(item)
        } else {
            trace!("Dropped wakeup");
        }
    }

    fn wake_by_ref(s: &Arc<Self>) {
        ArcWake::wake(s.clone())
    }
}

pub trait Node: Send + Sync + std::fmt::Debug {
    /// Push a [`RecordBatch`] to the given input partition
    fn push(&self, input: RecordBatch, child: usize, partition: usize);

    /// Mark a partition as exhausted
    fn close(&self, child: usize, partition: usize);

    fn output_partitions(&self) -> usize;

    /// Poll an output partition, attempting to get its output
    ///
    /// TODO: The futures plumbing is unfortunate
    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<ArrowResult<RecordBatch>>>;
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct ParentLink {
    node: usize,
    child: usize,
}

#[derive(Debug)]
pub struct QueryNode {
    node: Box<dyn Node>,
    parent: Option<ParentLink>,
}

#[derive(Debug)]
pub struct Query {
    nodes: Vec<QueryNode>,
    output: mpsc::UnboundedSender<ArrowResult<RecordBatch>>,
}

impl Drop for Query {
    fn drop(&mut self) {
        debug!("Query finished");
    }
}

impl Query {
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        task_context: Arc<TaskContext>,
    ) -> Result<(Query, mpsc::UnboundedReceiver<ArrowResult<RecordBatch>>)> {
        QueryBuilder::new(plan, task_context).build()
    }
}

struct ExecGroup {
    parent: Option<ParentLink>,
    root: Arc<dyn ExecutionPlan>,
    depth: usize,
}

struct QueryBuilder {
    nodes: Vec<QueryNode>,
    task_context: Arc<TaskContext>,
    to_visit: Vec<(Arc<dyn ExecutionPlan>, Option<ParentLink>)>,
    exec_buffer: Option<ExecGroup>,
}

impl QueryBuilder {
    fn new(plan: Arc<dyn ExecutionPlan>, task_context: Arc<TaskContext>) -> Self {
        Self {
            nodes: vec![],
            to_visit: vec![(plan, None)],
            task_context,
            exec_buffer: None,
        }
    }

    fn flush_exec(&mut self) -> Result<usize> {
        let group = self.exec_buffer.take().unwrap();
        let node_idx = self.nodes.len();
        self.nodes.push(QueryNode {
            node: Box::new(ExecutionNode::new(
                group.root,
                self.task_context.clone(),
                group.depth,
            )?),
            parent: group.parent,
        });
        Ok(node_idx)
    }

    fn visit_exec(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
        parent: Option<ParentLink>,
    ) -> Result<()> {
        let children = plan.children();

        match self.exec_buffer.as_mut() {
            Some(buffer) => {
                assert_eq!(parent, buffer.parent, "QueryBuilder out of sync");
                buffer.depth += 1;
            }
            None => {
                self.exec_buffer = Some(ExecGroup {
                    parent,
                    root: plan,
                    depth: 0,
                })
            }
        }

        match children.len() {
            1 => self
                .to_visit
                .push((children.into_iter().next().unwrap(), parent)),
            _ => {
                let node = self.flush_exec()?;
                self.enqueue_children(children, node);
            }
        }

        Ok(())
    }

    fn enqueue_children(
        &mut self,
        children: Vec<Arc<dyn ExecutionPlan>>,
        parent_node_idx: usize,
    ) {
        for (child_idx, child) in children.into_iter().enumerate() {
            self.to_visit.push((
                child,
                Some(ParentLink {
                    node: parent_node_idx,
                    child: child_idx,
                }),
            ))
        }
    }

    fn push_node(&mut self, node: QueryNode, children: Vec<Arc<dyn ExecutionPlan>>) {
        let node_idx = self.nodes.len();
        self.nodes.push(node);
        self.enqueue_children(children, node_idx)
    }

    fn push_repartition(
        &mut self,
        input: Partitioning,
        output: Partitioning,
        parent: Option<ParentLink>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<()> {
        let parent = match &self.exec_buffer {
            Some(buffer) => {
                assert_eq!(buffer.parent, parent, "QueryBuilder out of sync");
                Some(ParentLink {
                    node: self.flush_exec()?,
                    child: 0, // Must be the only child
                })
            }
            None => parent,
        };

        let node = Box::new(RepartitionNode::new(input, output));
        self.push_node(QueryNode { node, parent }, children);
        Ok(())
    }

    fn visit_node(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
        parent: Option<ParentLink>,
    ) -> Result<()> {
        if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>() {
            self.push_repartition(
                repartition.input().output_partitioning(),
                repartition.output_partitioning(),
                parent,
                repartition.children(),
            )
        } else if let Some(coalesce) =
            plan.as_any().downcast_ref::<CoalescePartitionsExec>()
        {
            self.push_repartition(
                coalesce.input().output_partitioning(),
                Partitioning::RoundRobinBatch(1),
                parent,
                coalesce.children(),
            )
        } else {
            self.visit_exec(plan, parent)
        }
    }

    fn build(
        mut self,
    ) -> Result<(Query, mpsc::UnboundedReceiver<ArrowResult<RecordBatch>>)> {
        // We do a depth-first scan of the operator tree, extracting a list of [`QueryNode`]
        while let Some((plan, parent)) = self.to_visit.pop() {
            self.visit_node(plan, parent)?;
        }

        if self.exec_buffer.is_some() {
            self.flush_exec()?;
        }

        let (sender, receiver) = mpsc::unbounded();
        Ok((
            Query {
                nodes: self.nodes,
                output: sender,
            },
            receiver,
        ))
    }
}
