use std::any::Any;
use std::collections::VecDeque;
use std::fmt::{Formatter, Pointer};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use async_trait::async_trait;
use futures::Stream;
use parking_lot::Mutex;

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

/// An [`ExecutionNode`] wraps an [`ExecutionPlan`] and converts it to a push-based API
///
/// This is hopefully a temporary hack, pending reworking the [`ExecutionPlan`] trait
pub struct ExecutionNode {
    inputs: Vec<Arc<Mutex<InputPartition>>>,
    outputs: Vec<SendableRecordBatchStream>,
}

impl ExecutionNode {
    pub async fn new(
        plan: Arc<dyn ExecutionPlan>,
        task_context: Arc<TaskContext>,
    ) -> Result<Self> {
        let children = plan.children();
        let mut inputs = Vec::new();
        let mut proxies: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(children.len());

        for child in children {
            let count = child.output_partitioning().partition_count();

            let child_inputs = vec![Default::default(); count];
            inputs.extend_from_slice(&child_inputs);
            proxies.push(Arc::new(ProxyExecutionPlan {
                inner: child,
                inputs: child_inputs,
            }));
        }

        let proxied = plan.with_new_children(proxies)?;
        let output_count = proxied.output_partitioning().partition_count();

        let outputs = futures::future::try_join_all(
            (0..output_count).map(|x| proxied.execute(x, task_context.clone())),
        )
        .await?;

        Ok(Self { inputs, outputs })
    }

    /// Push a [`RecordBatch`] to the given partition
    pub fn push(&mut self, input: RecordBatch, partition: usize) {
        self.inputs[partition].lock().buffer.push_back(input)
    }

    /// Poll a partition, attempting to get its output
    pub fn poll_partition(
        &mut self,
        partition: usize,
    ) -> Option<ArrowResult<RecordBatch>> {
        todo!()
    }
}

#[derive(Debug, Default)]
struct InputPartition {
    buffer: VecDeque<RecordBatch>,
    wait_list: Vec<Waker>,
}

struct InputPartitionStream {
    schema: SchemaRef,
    partition: Arc<Mutex<InputPartition>>,
}

impl Stream for InputPartitionStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut partition = self.partition.lock();
        if let Some(batch) = partition.buffer.pop_front() {
            return Poll::Ready(Some(Ok(batch)));
        }
        partition.wait_list.push(cx.waker().clone());
        Poll::Pending
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
        children: Vec<Arc<dyn ExecutionPlan>>,
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
