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

use std::sync::Arc;

use ceresdbproto::remote_engine::dist_sql_query_extension_node;
use datafusion::{
    error::{DataFusionError, Result as DfResult},
    execution::FunctionRegistry,
    physical_plan::ExecutionPlan,
};
use datafusion_proto::{physical_plan::PhysicalExtensionCodec, bytes::{physical_plan_to_bytes_with_extension_codec, physical_plan_from_bytes_with_extension_codec}};
use prost::{Message, bytes::Bytes};

use crate::dist_sql_query::physical_plan::UnresolvedSubTableScan;

pub struct ProtobufEncoder {
    extension_codec: DistSqlQueryCodec,
}

impl ProtobufEncoder {
    fn new() -> Self {
        Self {
            extension_codec: DistSqlQueryCodec,
        }
    }

    fn encode(&self, physical_plan: Arc<dyn ExecutionPlan>) -> DfResult<Bytes> {        
        physical_plan_to_bytes_with_extension_codec(physical_plan, &self.extension_codec)
    }

    fn decode(&self, bytes: &[u8]) -> DfResult<Arc<dyn ExecutionPlan>> {
        physical_plan_from_bytes_with_extension_codec(&bytes, ctx, &self.extension_codec)
    }
}

#[derive(Debug)]
struct DistSqlQueryCodec;

impl PhysicalExtensionCodec for DistSqlQueryCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let dist_sql_query_plan: ceresdbproto::remote_engine::DistSqlQueryExtensionNode =
            Message::decode(buf).map_err(|e| {
                DataFusionError::Internal(format!(
                    "failed to decode dist sql query extension plan, err:{e}"
                ))
            })?;

        match dist_sql_query_plan.node {
            Some(dist_sql_query_extension_node::Node::UnreolvedSubScan(plan_pb)) => {
                let plan: UnresolvedSubTableScan = plan_pb.try_into()?;
                Ok(Arc::new(plan))
            }

            None => Err(DataFusionError::Internal(format!(
                "actual node not found in dist query extension plan"
            ))),
        }
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DfResult<()> {
        let plan_node_pb = if let Some(plan) =
            node.as_any().downcast_ref::<UnresolvedSubTableScan>()
        {
            let plan_pb: ceresdbproto::remote_engine::UnreolvedSubScan = plan.clone().try_into()?;

            plan_pb
        } else {
            return Err(DataFusionError::Internal(
                "unexpected dist sql query extension plan type, expected:[ UnresolvedSubScan ]"
                    .to_string(),
            ));
        };

        let extension_plan_pb = ceresdbproto::remote_engine::DistSqlQueryExtensionNode {
            node: Some(dist_sql_query_extension_node::Node::UnreolvedSubScan(
                plan_node_pb,
            )),
        };

        extension_plan_pb.encode(buf).map_err(|e| 
            DataFusionError::Internal(format!("failed to encode dist sql query extension plan, plan:{extension_plan_pb:?}, err:{e}")))
    }
}
