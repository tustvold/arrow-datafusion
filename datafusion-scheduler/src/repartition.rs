use crate::query::Node;
use crate::ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::Partitioning;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::task::{Context, Poll, Waker};

#[derive(Debug)]
pub struct RepartitionNode {
    output: Partitioning,
    state: Mutex<RepartitionState>,
}

impl RepartitionNode {
    pub fn new(input: Partitioning, output: Partitioning) -> Self {
        let input_count = input.partition_count();
        assert_ne!(input_count, 0);

        let output_buffers = match output {
            Partitioning::RoundRobinBatch(num_partitions) => {
                assert_ne!(num_partitions, 0);

                (0..num_partitions).map(|_| Default::default()).collect()
            }
            _ => panic!("unsupported output partitioning: {:?}", output),
        };

        let state = Mutex::new(RepartitionState {
            next_idx: 0,
            partition_closed: vec![false; input_count],
            input_closed: false,
            output_buffers,
        });

        Self { output, state }
    }
}

#[derive(Debug)]
struct RepartitionState {
    next_idx: usize,
    partition_closed: Vec<bool>,
    input_closed: bool,
    output_buffers: Vec<OutputBuffer>,
}

impl Node for RepartitionNode {
    fn push(&self, input: RecordBatch, partition: usize) {
        // Currently only support round robin batch

        let mut state = self.state.lock();
        assert!(!state.partition_closed[partition]);

        let idx = state.next_idx;
        state.next_idx = (state.next_idx + 1) % state.output_buffers.len();

        let buffer = &mut state.output_buffers[idx];

        buffer.batches.push_back(input);

        for waker in buffer.wait_list.drain(..) {
            waker.wake()
        }
    }

    fn close(&self, partition: usize) {
        let mut state = self.state.lock();
        assert!(!state.partition_closed[partition]);
        state.partition_closed[partition] = true;

        // If all input streams exhausted, wake outputs
        if state.partition_closed.iter().all(|x| *x) {
            state.input_closed = true;
            for buffer in &mut state.output_buffers {
                for waker in buffer.wait_list.drain(..) {
                    waker.wake()
                }
            }
        }
    }

    fn output_partitions(&self) -> usize {
        self.output.partition_count()
    }

    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<ArrowResult<RecordBatch>>> {
        let mut state = self.state.lock();
        let input_closed = state.input_closed;
        let buffer = &mut state.output_buffers[partition];

        match buffer.batches.pop_front() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None if input_closed => Poll::Ready(None),
            _ => {
                buffer.wait_list.push(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

#[derive(Debug, Default)]
struct OutputBuffer {
    batches: VecDeque<RecordBatch>,
    wait_list: Vec<Waker>,
}
