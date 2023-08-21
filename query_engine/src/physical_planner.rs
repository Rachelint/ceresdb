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

use std::{any::Any, fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::execution::TaskContext as DfTaskContext;
use query_frontend::plan::QueryPlan;
use table_engine::stream::SendableRecordBatchStream;

use crate::{context::Context, error::*};

/// Physical query planner that converts a logical plan to a
/// physical plan suitable for execution.
/// During the convert process, it may do following things:
///   + Optimize the logical plan.
///   + Create the initial physical plan from the optimized logical.
///   + Optimize and get the final physical plan.
#[async_trait]
pub trait PhysicalPlanner: fmt::Debug + Send + Sync + 'static {
    /// Create a physical plan from a logical plan
    async fn plan(&self, ctx: &Context, logical_plan: QueryPlan) -> Result<PhysicalPlanPtr>;
}

pub type PhysicalPlannerRef = Arc<dyn PhysicalPlanner>;

pub trait PhysicalPlan: std::fmt::Debug + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;

    /// execute this plan and returns the result
    fn execute(&self, task_ctx: &TaskContext) -> Result<SendableRecordBatchStream>;

    /// Convert internal metrics to string.
    fn metrics_to_string(&self) -> String;
}

pub type PhysicalPlanPtr = Box<dyn PhysicalPlan>;

/// Task context, just a wrapper of datafusion task context now
#[derive(Default)]
pub struct TaskContext {
    df_task_context: Option<Arc<DfTaskContext>>,
}

impl TaskContext {
    pub fn with_datafusion_task_ctx(mut self, df_task_ctx: Arc<DfTaskContext>) -> Self {
        self.df_task_context = Some(df_task_ctx);
        self
    }

    pub fn try_to_datafusion_task_ctx(&self) -> Option<Arc<DfTaskContext>> {
        self.df_task_context.clone()
    }
}
