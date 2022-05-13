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

//! A generic stream over file format readers that can be used by
//! any file format that read its files from start to end.
//!
//! Note: Most traits here need to be marked `Sync + Send` to be
//! compliant with the `SendableRecordBatchStream` trait.

use std::collections::VecDeque;
use std::fs::File;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::{
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use futures::future::BoxFuture;
use futures::{ready, FutureExt, Stream};
use object_store::{ObjectMeta, ObjectStore};

use datafusion_common::ScalarValue;

use crate::datasource::listing::{FileRange, PartitionedFile};
use crate::datasource::local::fetch_to_local_file;
use crate::physical_plan::file_format::{FileScanConfig, PartitionColumnProjector};
use crate::physical_plan::RecordBatchStream;

pub trait FormatReaderOpener: Unpin {
    type Reader: Iterator<Item = ArrowResult<RecordBatch>> + Send + Unpin;

    type File: Unpin;

    fn fetch(
        &mut self,
        store: Arc<dyn ObjectStore>,
        file: ObjectMeta,
        range: Option<FileRange>,
    ) -> BoxFuture<'static, ArrowResult<Self::File>>;

    fn reader(&mut self, file: Self::File) -> ArrowResult<Self::Reader>;
}

impl<T, R> FormatReaderOpener for T
where
    T: FnMut(File) -> ArrowResult<R> + Unpin,
    R: Iterator<Item = ArrowResult<RecordBatch>> + Send + Unpin,
{
    type Reader = R;

    type File = File;

    fn fetch(
        &mut self,
        store: Arc<dyn ObjectStore>,
        file: ObjectMeta,
        _range: Option<FileRange>,
    ) -> BoxFuture<'static, ArrowResult<Self::File>> {
        Box::pin(async move {
            fetch_to_local_file(store.as_ref(), &file.location)
                .await
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
        })
    }

    fn reader(&mut self, file: File) -> ArrowResult<Self::Reader> {
        self(file)
    }
}

/// A stream that iterates record batch by record batch, file over file.
pub struct FileStream<F: FormatReaderOpener> {
    /// An iterator over input files.
    file_iter: VecDeque<PartitionedFile>,
    /// The stream schema (file schema including partition columns and after
    /// projection).
    projected_schema: SchemaRef,
    /// The remaining number of records to parse, None if no limit
    remain: Option<usize>,
    /// A closure that takes a reader and an optional remaining number of lines
    /// (before reaching the limit) and returns a batch iterator. If the file reader
    /// is not capable of limiting the number of records in the last batch, the file
    /// stream will take care of truncating it.
    file_reader: F,
    /// The partition column projector
    pc_projector: PartitionColumnProjector,
    /// the store from which to source the files.
    object_store: Arc<dyn ObjectStore>,
    /// The stream state
    state: FileStreamState<F>,
}

enum FileStreamState<F: FormatReaderOpener> {
    Idle,
    Fetch {
        future: BoxFuture<'static, ArrowResult<F::File>>,
        partition_values: Vec<ScalarValue>,
    },
    Scan {
        /// Partitioning column values for the current batch_iter
        partition_values: Vec<ScalarValue>,
        /// The reader instance
        reader: F::Reader,
    },
    Error,
    Limit,
}

impl<F: FormatReaderOpener> FileStream<F> {
    pub fn new(config: &FileScanConfig, partition: usize, file_reader: F) -> Self {
        let (projected_schema, _) = config.project();
        let pc_projector =
            PartitionColumnProjector::new(projected_schema, &config.table_partition_cols);

        let files = config.file_groups[partition].clone();

        Self {
            file_iter: files.into(),
            projected_schema: config.file_schema.clone(),
            remain: config.limit,
            file_reader,
            pc_projector,
            object_store: config.object_store.clone(),
            state: FileStreamState::Idle,
        }
    }

    fn poll_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<ArrowResult<RecordBatch>>> {
        loop {
            match &mut self.state {
                FileStreamState::Idle => {
                    let file = match self.file_iter.pop_front() {
                        Some(file) => file,
                        None => return Poll::Ready(None),
                    };

                    let future = self.file_reader.fetch(
                        self.object_store.clone(),
                        file.object_meta,
                        file.range,
                    );

                    self.state = FileStreamState::Fetch {
                        future,
                        partition_values: file.partition_values,
                    }
                }
                FileStreamState::Fetch {
                    future,
                    partition_values,
                } => match ready!(future.poll_unpin(cx))
                    .and_then(|file| self.file_reader.reader(file))
                {
                    Ok(reader) => {
                        self.state = FileStreamState::Scan {
                            partition_values: std::mem::take(partition_values),
                            reader,
                        };
                    }
                    Err(e) => {
                        self.state = FileStreamState::Error;
                        return Poll::Ready(Some(Err(e)));
                    }
                },
                FileStreamState::Scan {
                    reader,
                    partition_values,
                } => match reader.next() {
                    Some(result) => {
                        let result = result
                            .and_then(|b| self.pc_projector.project(b, partition_values))
                            .map(|batch| match &mut self.remain {
                                Some(remain) => {
                                    if *remain > batch.num_rows() {
                                        *remain -= batch.num_rows();
                                        batch
                                    } else {
                                        let batch = batch.slice(0, *remain);
                                        self.state = FileStreamState::Limit;
                                        *remain = 0;
                                        batch
                                    }
                                }
                                None => batch,
                            });

                        if result.is_err() {
                            self.state = FileStreamState::Error
                        }

                        return Poll::Ready(Some(result));
                    }
                    None => self.state = FileStreamState::Idle,
                },
                FileStreamState::Error | FileStreamState::Limit => {
                    return Poll::Ready(None)
                }
            }
        }
    }
}

impl<F: FormatReaderOpener> Stream for FileStream<F> {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }
}

impl<F: FormatReaderOpener> RecordBatchStream for FileStream<F> {
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use crate::datasource::listing::PartitionedFile;
    use crate::{
        error::Result, test::make_partition, test::object_store::make_in_memory_store,
    };

    use super::*;

    /// helper that creates a stream of 2 files with the same pair of batches in each ([0,1,2] and [0,1])
    async fn create_and_collect(limit: Option<usize>) -> Vec<RecordBatch> {
        let records = vec![make_partition(3), make_partition(2)];
        let file_schema = records[0].schema();

        let reader = move |_file| {
            // this reader returns the same batch regardless of the file
            Ok(records.clone().into_iter().map(Ok))
        };

        let object_store = make_in_memory_store([("mock_file1", 10), ("mock_file2", 20)]);

        let config = FileScanConfig {
            object_store,
            file_schema,
            file_groups: vec![vec![
                PartitionedFile::new("mock_file1".to_owned(), 10),
                PartitionedFile::new("mock_file2".to_owned(), 20),
            ]],
            statistics: Default::default(),
            projection: None,
            limit,
            table_partition_cols: vec![],
        };

        FileStream::new(&config, 0, reader)
            .map(|b| b.expect("No error expected in stream"))
            .collect::<Vec<_>>()
            .await
    }

    #[tokio::test]
    async fn without_limit() -> Result<()> {
        let batches = create_and_collect(None).await;

        #[rustfmt::skip]
        crate::assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "+---+",
        ], &batches);

        Ok(())
    }

    #[tokio::test]
    async fn with_limit_between_files() -> Result<()> {
        let batches = create_and_collect(Some(5)).await;
        #[rustfmt::skip]
        crate::assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "+---+",
        ], &batches);

        Ok(())
    }

    #[tokio::test]
    async fn with_limit_at_middle_of_batch() -> Result<()> {
        let batches = create_and_collect(Some(6)).await;
        #[rustfmt::skip]
        crate::assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "| 0 |",
            "+---+",
        ], &batches);

        Ok(())
    }
}
