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

use generic_error::GenericError;
use macros::define_result;
use snafu::Snafu;
define_result!(Error);

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Internal error, message:{}, err:{}", msg, source))]
    Internal { msg: String, source: GenericError },

    #[snafu(display("Datafusion error, message:{}, err:{}", msg, source))]
    Datafusion { msg: String, source: GenericError },
}
