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

use std::sync::Arc;

use futures::stream::{BoxStream, StreamExt};

use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::ExecutionPlan;

use crate::query::{Query, WorkItem};
use crate::worker::WorkerPool;

mod node;
mod query;
mod repartition;
mod worker;

pub struct Scheduler {
    pool: WorkerPool,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            pool: WorkerPool::new(2),
        }
    }

    pub fn schedule_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, ArrowResult<RecordBatch>>> {
        let (query, receiver) = Query::new(plan, context)?;
        WorkItem::spawn_query(self.pool.spawner(), Arc::new(query));
        Ok(receiver.boxed())
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        self.pool.shutdown()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use futures::TryStreamExt;
    use rand::distributions::uniform::SampleUniform;
    use rand::{thread_rng, Rng};

    use datafusion::arrow::array::{ArrayRef, PrimitiveArray};
    use datafusion::arrow::datatypes::{ArrowPrimitiveType, Float64Type, Int32Type};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::{MemTable, TableProvider};
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::*;

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

    fn make_batches() -> Vec<Vec<RecordBatch>> {
        let mut rng = thread_rng();

        let batches_per_partition = 3;
        let rows_per_batch = 1000;
        let num_partitions = 2;

        std::iter::from_fn(|| {
            Some(
                std::iter::from_fn(|| Some(generate_batch(&mut rng, rows_per_batch)))
                    .take(batches_per_partition)
                    .collect(),
            )
        })
        .take(num_partitions)
        .collect()
    }

    fn make_provider() -> Arc<dyn TableProvider> {
        let batches = make_batches();
        let schema = batches.first().unwrap().first().unwrap().schema();
        Arc::new(MemTable::try_new(schema, make_batches()).unwrap())
    }

    #[tokio::test]
    async fn test_simple() {
        let scheduler = Scheduler::new();

        let config = SessionConfig::new().with_target_partitions(2);
        let mut context = SessionContext::with_config(config);

        context.register_table("table1", make_provider()).unwrap();
        context.register_table("table2", make_provider()).unwrap();

        let task = context.task_ctx();

        let query = context
            .sql("SELECT * FROM table1 where a > 100 order by a, b")
            .await
            .unwrap();

        let plan = query.create_physical_plan().await.unwrap();

        println!("Plan: {}", displayable(plan.as_ref()).indent());

        let stream = scheduler.schedule_plan(plan, task).unwrap();
        let scheduled: Vec<_> = stream.try_collect().await.unwrap();
        let expected = query.collect().await.unwrap();

        assert_eq!(scheduled, expected);
    }
}
