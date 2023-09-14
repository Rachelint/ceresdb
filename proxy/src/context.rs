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

//! Server context

use std::time::Duration;

use common_types::request_id::RequestId;
use macros::define_result;
use snafu::{ensure, Backtrace, Snafu};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing catalog.\nBacktrace:\n{}", backtrace))]
    MissingCatalog { backtrace: Backtrace },

    #[snafu(display("Missing schema.\nBacktrace:\n{}", backtrace))]
    MissingSchema { backtrace: Backtrace },

    #[snafu(display("Missing runtime.\nBacktrace:\n{}", backtrace))]
    MissingRuntime { backtrace: Backtrace },

    #[snafu(display("Missing router.\nBacktrace:\n{}", backtrace))]
    MissingRouter { backtrace: Backtrace },
}

define_result!(Error);

/// Server request context
///
/// Context for request, may contains
/// 1. Request context and options
/// 2. Info from http headers
#[derive(Debug)]
pub struct RequestContext {
    /// Catalog of the request
    pub catalog: String,
    /// Schema of request
    pub schema: String,
    /// Request timeout
    pub timeout: Option<Duration>,
    /// Request id
    pub request_id: RequestId,
}

impl RequestContext {
    pub fn builder() -> Builder {
        Builder::default()
    }
}

#[derive(Default)]
pub struct Builder {
    catalog: String,
    schema: String,
    timeout: Option<Duration>,
}

impl Builder {
    pub fn catalog(mut self, catalog: String) -> Self {
        self.catalog = catalog;
        self
    }

    pub fn schema(mut self, schema: String) -> Self {
        self.schema = schema;
        self
    }

    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn build(self) -> Result<RequestContext> {
        ensure!(!self.catalog.is_empty(), MissingCatalog);
        ensure!(!self.schema.is_empty(), MissingSchema);

        Ok(RequestContext {
            catalog: self.catalog,
            schema: self.schema,
            timeout: self.timeout,
            request_id: RequestId::next_id(),
        })
    }
}
