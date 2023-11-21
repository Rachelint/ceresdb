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

//! Skiplist memtable iterator

use arrow::ipc::RecordBatchBuilder;
use common_types::{
    record_batch::{FetchingRecordBatch, FetchingRecordBatchBuilder},
    schema::Schema,
    time::TimeRange,
};
use generic_error::BoxError;
use snafu::ResultExt;

use crate::memtable::{
    layered::{ImmutableSegment, MutableSegment},
    ColumnarIterPtr, Internal, ProjectSchema, Result, ScanContext, ScanRequest,
};

/// Columnar iterator for [LayeredMemTable]
pub(crate) struct ColumnarIterImpl {
    selected_batch_iter: ColumnarIterPtr,
}

impl ColumnarIterImpl {
    pub fn new(
        memtable_schema: &Schema,
        ctx: ScanContext,
        request: ScanRequest,
        mutable: &MutableSegment,
        immutables: &[ImmutableSegment],
    ) -> Result<Self> {
        // Create projection for the memtable schema
        let record_fetching_ctx = request
            .record_fetching_ctx_builder
            .build(memtable_schema)
            .context(ProjectSchema)?;

        let (maybe_mutable, selected_immutables) =
            Self::filter_by_time_range(mutable, immutables, request.time_range);

        // TODO: reduce clone here.
        let immutable_batches = selected_immutables
            .flat_map(|imm| {
                imm.record_batches().iter().map(|batch| {
                    let fetching_schema = record_fetching_ctx.fetching_schema().clone();
                    let primary_key_indexes = record_fetching_ctx
                        .primary_key_indexes()
                        .map(|idxs| idxs.to_vec());
                    let fetching_column_indexes =
                        record_fetching_ctx.fetching_source_column_indexes();
                    FetchingRecordBatch::try_new(
                        fetching_schema,
                        primary_key_indexes,
                        fetching_column_indexes,
                        batch.clone(),
                    )
                    .box_err()
                    .with_context(|| Internal {
                        msg: format!("record_fetching_ctx:{record_fetching_ctx:?}",),
                    })
                })
            })
            .collect::<Vec<_>>();
        let immutable_iter = immutable_batches.into_iter();

        let maybe_mutable_iter = match maybe_mutable {
            Some(mutable) => Some(mutable.scan(ctx, request)?),
            None => None,
        };

        let maybe_chained_iter = match maybe_mutable_iter {
            Some(mutable_iter) => Box::new(mutable_iter.chain(immutable_iter)) as _,
            None => Box::new(immutable_iter) as _,
        };

        Ok(Self {
            selected_batch_iter: maybe_chained_iter,
        })
    }

    fn filter_by_time_range<'a>(
        mutable: &'a MutableSegment,
        immutables: &'a [ImmutableSegment],
        time_range: TimeRange,
    ) -> (
        Option<&'a MutableSegment>,
        impl Iterator<Item = &'a ImmutableSegment>,
    ) {
        let maybe_mutable = {
            let mutable_time_range = mutable.time_range();
            mutable_time_range.and_then(|range| {
                if range.intersect_with(time_range) {
                    Some(mutable)
                } else {
                    None
                }
            })
        };

        let selected_immutables = immutables
            .iter()
            .filter(move |imm| imm.time_range().intersect_with(time_range));

        (maybe_mutable, selected_immutables)
    }
}

impl Iterator for ColumnarIterImpl {
    type Item = Result<FetchingRecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.selected_batch_iter.next()
    }
}
