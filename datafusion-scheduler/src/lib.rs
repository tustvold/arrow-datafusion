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

use datafusion::arrow::error::{ArrowError, Result as ArrowResult};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::stream::{BoxStream, StreamExt};
use futures::TryStreamExt;
use std::sync::Arc;
use std::task::Waker;

mod node;

pub struct Scheduler {}

impl Scheduler {
    pub fn new() -> Self {
        Self {}
    }

    pub fn schedule(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, ArrowResult<RecordBatch>>> {
        Ok(futures::stream::once(async move {
            let streams = futures::future::try_join_all(
                (0..plan.output_partitioning().partition_count())
                    .map(|x| plan.execute(x, context.clone())),
            )
            .await?;

            Ok::<_, ArrowError>(futures::stream::select_all(streams).boxed())
        })
        .try_flatten()
        .boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, PrimitiveArray};
    use datafusion::arrow::datatypes::{ArrowPrimitiveType, Float64Type, Int32Type};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
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

    #[test]
    fn test_simple() {
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

        let mut context = SessionContext::new();
        context.register_table("t", Arc::new(provider)).unwrap();

        let task = context.task_ctx();

        let stream = futures::executor::block_on(async move {
            let query = context.sql("SELECT distinct b FROM t where a > 100;").await.unwrap();
            let plan = query.create_physical_plan().await.unwrap();

            scheduler.schedule(plan, task).unwrap()
        });

        let batches = futures::executor::block_on_stream(stream)
            .collect::<ArrowResult<Vec<_>>>()
            .unwrap();

        let total = batches.iter().map(|x| x.num_rows()).sum::<usize>();
        let expected_total = batches_per_partition * rows_per_batch * num_partitions;

        assert_eq!(total, expected_total);
    }
}
