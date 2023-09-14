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

//! Aggregate functions.

use std::{fmt, ops::Deref};

use arrow::array::ArrayRef as DfArrayRef;
use common_types::column_block::ColumnBlock;
use datafusion::{
    error::{DataFusionError, Result as DfResult},
    physical_plan::Accumulator as DfAccumulator,
    scalar::ScalarValue as DfScalarValue,
};
use generic_error::GenericError;
use macros::define_result;
use snafu::Snafu;

use crate::functions::ScalarValue;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to get state, err:{}", source))]
    GetState { source: GenericError },

    #[snafu(display("Failed to merge state, err:{}", source))]
    MergeState { source: GenericError },
}

define_result!(Error);

// TODO: Use `Datum` rather than `ScalarValue`.
pub struct State(Vec<DfScalarValue>);

impl State {
    /// Convert to a set of ScalarValues
    fn into_state(self) -> Vec<DfScalarValue> {
        self.0
    }
}

impl From<ScalarValue> for State {
    fn from(value: ScalarValue) -> Self {
        Self(vec![value.into_df_scalar_value()])
    }
}

pub struct Input<'a>(&'a [ColumnBlock]);

impl<'a> Input<'a> {
    pub fn num_columns(&self) -> usize {
        self.0.len()
    }

    pub fn column(&self, col_idx: usize) -> Option<&ColumnBlock> {
        self.0.get(col_idx)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

pub struct StateRef<'a>(Input<'a>);

impl<'a> Deref for StateRef<'a> {
    type Target = Input<'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An accumulator represents a stateful object that lives throughout the
/// evaluation of multiple rows and generically accumulates values.
///
/// An accumulator knows how to:
/// * update its state from inputs via `update`
/// * convert its internal state to column blocks
/// * update its state from multiple accumulators' states via `merge`
/// * compute the final value from its internal state via `evaluate`
pub trait Accumulator: Send + Sync + fmt::Debug {
    /// Returns the state of the accumulator at the end of the accumulation.
    // in the case of an average on which we track `sum` and `n`, this function
    // should return a vector of two values, sum and n.
    // TODO: Use `Datum` rather than `ScalarValue`.
    fn state(&self) -> Result<State>;

    /// updates the accumulator's state from column blocks.
    fn update(&mut self, values: Input) -> Result<()>;

    /// updates the accumulator's state from column blocks.
    fn merge(&mut self, states: StateRef) -> Result<()>;

    /// returns its value based on its current state.
    // TODO: Use `Datum` rather than `ScalarValue`.
    fn evaluate(&self) -> Result<ScalarValue>;
}

#[derive(Debug)]
pub struct ToDfAccumulator<T> {
    accumulator: T,
}

impl<T> ToDfAccumulator<T> {
    pub fn new(accumulator: T) -> Self {
        Self { accumulator }
    }
}

impl<T: Accumulator> DfAccumulator for ToDfAccumulator<T> {
    fn state(&self) -> DfResult<Vec<DfScalarValue>> {
        let state = self.accumulator.state().map_err(|e| {
            DataFusionError::Execution(format!("Accumulator failed to get state, err:{e}"))
        })?;
        Ok(state.into_state())
    }

    fn update_batch(&mut self, values: &[DfArrayRef]) -> DfResult<()> {
        if values.is_empty() {
            return Ok(());
        };

        let column_blocks = values
            .iter()
            .map(|array| {
                ColumnBlock::try_cast_arrow_array_ref(array).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Accumulator failed to cast arrow array to column block, column, err:{e}"
                    ))
                })
            })
            .collect::<DfResult<Vec<_>>>()?;

        let input = Input(&column_blocks);
        self.accumulator.update(input).map_err(|e| {
            DataFusionError::Execution(format!("Accumulator failed to update, err:{e}"))
        })
    }

    fn merge_batch(&mut self, states: &[DfArrayRef]) -> DfResult<()> {
        if states.is_empty() {
            return Ok(());
        };

        let column_blocks = states
            .iter()
            .map(|array| {
                ColumnBlock::try_cast_arrow_array_ref(array).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Accumulator failed to cast arrow array to column block, column, err:{e}"
                    ))
                })
            })
            .collect::<DfResult<Vec<_>>>()?;

        let state_ref = StateRef(Input(&column_blocks));
        self.accumulator.merge(state_ref).map_err(|e| {
            DataFusionError::Execution(format!("Accumulator failed to merge, err:{e}"))
        })
    }

    fn evaluate(&self) -> DfResult<DfScalarValue> {
        let value = self.accumulator.evaluate().map_err(|e| {
            DataFusionError::Execution(format!("Accumulator failed to evaluate, err:{e}"))
        })?;

        Ok(value.into_df_scalar_value())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}
