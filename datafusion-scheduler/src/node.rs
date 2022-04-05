use std::any::Any;
use std::collections::VecDeque;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use async_trait::async_trait;
use futures::{Stream, StreamExt, TryFutureExt};
use parking_lot::Mutex;

use crate::query::Node;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::{error::Result as ArrowResult, record_batch::RecordBatch};
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayFormatType, Distribution, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};

/// An [`ExecutionNode`] wraps a single node within an [`ExecutionPlan`] and
/// converts it to a push-based API that can be orchestrated by a [`super::Scheduler`]
///
/// This is hopefully a temporary hack, pending reworking the [`ExecutionPlan`] trait
pub struct ExecutionNode {
    inputs: Vec<Arc<Mutex<InputPartition>>>,
    outputs: Vec<Mutex<SendableRecordBatchStream>>,
}

impl std::fmt::Debug for ExecutionNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionNode").finish()
    }
}

impl ExecutionNode {
    pub async fn new(
        plan: Arc<dyn ExecutionPlan>,
        task_context: Arc<TaskContext>,
    ) -> Result<Self> {
        let children = plan.children();
        let mut inputs = Vec::new();

        let proxied = if !children.is_empty() {
            let mut proxies: Vec<Arc<dyn ExecutionPlan>> =
                Vec::with_capacity(children.len());
            for child in children {
                let count = child.output_partitioning().partition_count();

                let mut child_inputs = Vec::with_capacity(count);
                for _ in 0..count {
                    child_inputs.push(Default::default())
                }

                inputs.extend_from_slice(&child_inputs);
                proxies.push(Arc::new(ProxyExecutionPlan {
                    inner: child,
                    inputs: child_inputs,
                }));
            }

            plan.with_new_children(proxies)?
        } else {
            plan.clone()
        };

        let output_count = proxied.output_partitioning().partition_count();

        let outputs = futures::future::try_join_all(
            (0..output_count)
                .map(|x| proxied.execute(x, task_context.clone()).map_ok(Mutex::new)),
        )
        .await?;

        Ok(Self { inputs, outputs })
    }
}

impl Node for ExecutionNode {
    /// Push a [`RecordBatch`] to the given input partition
    fn push(&self, input: RecordBatch, partition: usize) {
        let mut partition = self.inputs[partition].lock();
        assert!(!partition.is_closed);

        partition.buffer.push_back(input);
        for waker in partition.wait_list.drain(..) {
            waker.wake()
        }
    }

    fn close(&self, partition: usize) {
        println!("Closing partition: {}", partition);
        let mut partition = self.inputs[partition].lock();
        assert!(!partition.is_closed);

        partition.is_closed = true;
        for waker in partition.wait_list.drain(..) {
            waker.wake()
        }
    }

    fn output_partitions(&self) -> usize {
        self.outputs.len()
    }

    /// Poll an output partition, attempting to get its output
    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<ArrowResult<RecordBatch>>> {
        self.outputs[partition].lock().poll_next_unpin(cx)
    }
}

#[derive(Debug, Default)]
struct InputPartition {
    buffer: VecDeque<RecordBatch>,
    wait_list: Vec<Waker>,
    is_closed: bool,
}

struct InputPartitionStream {
    schema: SchemaRef,
    partition: Arc<Mutex<InputPartition>>,
}

impl Stream for InputPartitionStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut partition = self.partition.lock();
        match partition.buffer.pop_front() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None if partition.is_closed => Poll::Ready(None),
            _ => {
                partition.wait_list.push(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl RecordBatchStream for InputPartitionStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// This is a hack that allows injecting [`InputPartitionStream`] in place of the
/// streams yielded by the child of the wrapped [`ExecutionPlan`]
///
/// This is hopefully temporary pending reworking [`ExecutionPlan`]
#[derive(Debug)]
struct ProxyExecutionPlan {
    inner: Arc<dyn ExecutionPlan>,

    inputs: Vec<Arc<Mutex<InputPartition>>>,
}

#[async_trait]
impl ExecutionPlan for ProxyExecutionPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.inner.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.inner.output_ordering()
    }

    fn required_child_distribution(&self) -> Distribution {
        self.inner.required_child_distribution()
    }

    fn relies_on_input_order(&self) -> bool {
        self.inner.relies_on_input_order()
    }

    fn maintains_input_order(&self) -> bool {
        self.inner.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        self.inner.benefits_from_input_partitioning()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> crate::Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    async fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> crate::Result<SendableRecordBatchStream> {
        Ok(Box::pin(InputPartitionStream {
            schema: self.schema(),
            partition: self.inputs[partition].clone(),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }

    fn statistics(&self) -> Statistics {
        self.inner.statistics()
    }
}
