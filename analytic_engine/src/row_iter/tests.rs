// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use common_types::{
    projected_schema::{ProjectedSchema, RowProjector},
    record_batch::{FetchedRecordBatch, FetchedRecordBatchBuilder},
    row::{
        contiguous::{ContiguousRowReader, ContiguousRowWriter, ProjectedContiguousRow},
        Row,
    },
    schema::{IndexInWriterSchema, RecordSchemaWithKey, Schema},
};
use macros::define_result;
use snafu::Snafu;

use crate::row_iter::FetchedRecordBatchIterator;

#[derive(Debug, Snafu)]
pub enum Error {}

define_result!(Error);

pub struct VectorIterator {
    schema: RecordSchemaWithKey,
    items: Vec<Option<FetchedRecordBatch>>,
    idx: usize,
}

impl VectorIterator {
    pub fn new(schema: RecordSchemaWithKey, items: Vec<FetchedRecordBatch>) -> Self {
        Self {
            schema,
            items: items.into_iter().map(Some).collect(),
            idx: 0,
        }
    }
}

#[async_trait]
impl FetchedRecordBatchIterator for VectorIterator {
    type Error = Error;

    fn schema(&self) -> &RecordSchemaWithKey {
        &self.schema
    }

    async fn next_batch(&mut self) -> Result<Option<FetchedRecordBatch>> {
        if self.idx == self.items.len() {
            return Ok(None);
        }

        let ret = Ok(self.items[self.idx].take());
        self.idx += 1;

        ret
    }
}

pub fn build_fetched_record_batch_with_key(schema: Schema, rows: Vec<Row>) -> FetchedRecordBatch {
    assert!(schema.num_columns() > 1);
    let projection: Vec<usize> = (0..schema.num_columns()).collect();
    let projected_schema = ProjectedSchema::new(schema.clone(), Some(projection)).unwrap();
    let fetched_schema = projected_schema.to_record_schema_with_key();
    let primary_key_indexes = fetched_schema.primary_key_idx().to_vec();
    let fetched_schema = fetched_schema.to_record_schema();
    let table_schema = projected_schema.table_schema();
    let row_projector = RowProjector::new(
        &fetched_schema,
        Some(primary_key_indexes),
        table_schema,
        table_schema,
    )
    .unwrap();
    let primary_key_indexes = row_projector
        .primary_key_indexes()
        .map(|idxs| idxs.to_vec());
    let mut builder =
        FetchedRecordBatchBuilder::with_capacity(fetched_schema, primary_key_indexes, 2);
    let index_in_writer = IndexInWriterSchema::for_same_schema(schema.num_columns());

    let mut buf = Vec::new();
    for row in rows {
        let mut writer = ContiguousRowWriter::new(&mut buf, &schema, &index_in_writer);

        writer.write_row(&row).unwrap();

        let source_row = ContiguousRowReader::try_new(&buf, &schema).unwrap();
        let projected_row = ProjectedContiguousRow::new(source_row, &row_projector);
        builder
            .append_projected_contiguous_row(&projected_row)
            .unwrap();
    }
    builder.build().unwrap()
}

pub async fn check_iterator<T: FetchedRecordBatchIterator>(iter: &mut T, expected_rows: Vec<Row>) {
    let mut visited_rows = 0;
    while let Some(batch) = iter.next_batch().await.unwrap() {
        for row_idx in 0..batch.num_rows() {
            assert_eq!(batch.clone_row_at(row_idx), expected_rows[visited_rows]);
            visited_rows += 1;
        }
    }

    assert_eq!(visited_rows, expected_rows.len());
}
