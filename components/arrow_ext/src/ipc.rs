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

//! Utilities for `RecordBatch` serialization using Arrow IPC

use std::{borrow::Cow, io::Cursor, sync::Arc};

use arrow::{
    datatypes::{DataType, Field, Schema, SchemaRef},
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};
use snafu::{Backtrace, ResultExt, Snafu};

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Arrow error, err:{}.\nBacktrace:\n{}", source, backtrace))]
    ArrowError {
        source: arrow::error::ArrowError,
        backtrace: Backtrace,
    },

    #[snafu(display("Zstd decode error, err:{}.\nBacktrace:\n{}", source, backtrace))]
    ZstdError {
        source: std::io::Error,
        backtrace: Backtrace,
    },
}

type Result<T> = std::result::Result<T, Error>;

const DEFAULT_COMPRESS_MIN_LENGTH: usize = 80 * 1024;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub enum CompressionMethod {
    #[default]
    None,
    Zstd,
}

// https://facebook.github.io/zstd/zstd_manual.html
// The lower the level, the faster the speed (at the cost of compression).
const ZSTD_LEVEL: i32 = 3;

#[derive(Default)]
/// Encoder that can encode a batch of record batches with specific compression
/// options.
pub struct RecordBatchesEncoder {
    stream_writer: Option<StreamWriter<Vec<u8>>>,
    num_rows: usize,
    /// Whether the writer has more than one dict fields, we need to do schema
    /// convert.
    cached_converted_schema: Option<SchemaRef>,
    compress_opts: CompressOptions,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct CompressOptions {
    /// The minimum length of the payload to be compressed.
    pub compress_min_length: usize,
    pub method: CompressionMethod,
}

impl Default for CompressOptions {
    fn default() -> Self {
        Self {
            compress_min_length: DEFAULT_COMPRESS_MIN_LENGTH,
            method: CompressionMethod::Zstd,
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct CompressOutput {
    pub method: CompressionMethod,
    pub payload: Vec<u8>,
}

impl CompressOutput {
    #[inline]
    pub fn no_compression(payload: Vec<u8>) -> Self {
        Self {
            method: CompressionMethod::None,
            payload,
        }
    }
}

impl CompressOptions {
    pub fn maybe_compress(&self, input: Vec<u8>) -> Result<CompressOutput> {
        if input.len() < self.compress_min_length {
            return Ok(CompressOutput::no_compression(input));
        }

        match self.method {
            CompressionMethod::None => Ok(CompressOutput::no_compression(input)),
            CompressionMethod::Zstd => {
                let payload = zstd::bulk::compress(&input, ZSTD_LEVEL).context(ZstdError)?;
                Ok(CompressOutput {
                    method: CompressionMethod::Zstd,
                    payload,
                })
            }
        }
    }
}

impl RecordBatchesEncoder {
    pub fn new(compress_opts: CompressOptions) -> Self {
        Self {
            stream_writer: None,
            num_rows: 0,
            cached_converted_schema: None,
            compress_opts,
        }
    }

    /// Get the number of rows that have been encoded.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// When schema more than one dict fields, it will return a new owned
    /// schema, otherwise it just return the origin schema.
    ///
    /// Workaround for https://github.com/apache/arrow-datafusion/issues/6784
    fn convert_schema(schema: &SchemaRef) -> Cow<SchemaRef> {
        let dict_field_num: usize = schema
            .fields()
            .iter()
            .map(|f| {
                if let DataType::Dictionary(_, _) = f.data_type() {
                    1
                } else {
                    0
                }
            })
            .sum();
        if dict_field_num <= 1 {
            return Cow::Borrowed(schema);
        }

        let new_fields = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if let DataType::Dictionary(_, _) = f.data_type() {
                    let dict_id = i as i64;
                    Arc::new(Field::new_dict(
                        f.name(),
                        f.data_type().clone(),
                        f.is_nullable(),
                        dict_id,
                        f.dict_is_ordered().unwrap_or(false),
                    ))
                } else {
                    f.clone()
                }
            })
            .collect::<Vec<_>>();

        let schema_ref = Arc::new(Schema::new_with_metadata(
            new_fields,
            schema.metadata.clone(),
        ));

        Cow::Owned(schema_ref)
    }

    /// Append one batch into the encoder for encoding.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let stream_writer = if let Some(v) = &mut self.stream_writer {
            v
        } else {
            let mem_size = batch
                .columns()
                .iter()
                .map(|col| col.get_buffer_memory_size())
                .sum();
            let buffer: Vec<u8> = Vec::with_capacity(mem_size);
            let schema = batch.schema();
            let schema = Self::convert_schema(&schema);
            let stream_writer = StreamWriter::try_new(buffer, &schema).context(ArrowError)?;
            if schema.is_owned() {
                self.cached_converted_schema = Some(schema.into_owned());
            }
            self.stream_writer = Some(stream_writer);
            self.stream_writer.as_mut().unwrap()
        };

        if let Some(schema) = &self.cached_converted_schema {
            let batch = RecordBatch::try_new(schema.clone(), batch.columns().to_vec())
                .context(ArrowError)?;
            stream_writer.write(&batch).context(ArrowError)?;
        } else {
            stream_writer.write(batch).context(ArrowError)?;
        }
        self.num_rows += batch.num_rows();
        Ok(())
    }

    /// Finish encoding and generate the final encoded bytes, which may be
    /// compressed.
    pub fn finish(mut self) -> Result<CompressOutput> {
        let stream_writer = match self.stream_writer.take() {
            None => return Ok(CompressOutput::no_compression(Vec::new())),
            Some(v) => v,
        };

        let encoded_bytes = stream_writer.into_inner().context(ArrowError)?;
        self.compress_opts.maybe_compress(encoded_bytes)
    }
}

/// Encode one record batch with given compression.
pub fn encode_record_batch(
    batch: &RecordBatch,
    compress_opts: CompressOptions,
) -> Result<CompressOutput> {
    let mut encoder = RecordBatchesEncoder::new(compress_opts);
    encoder.write(batch)?;
    encoder.finish()
}

/// Decode multiple record batches from the encoded bytes.
pub fn decode_record_batches(
    bytes: Vec<u8>,
    compression: CompressionMethod,
) -> Result<Vec<RecordBatch>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let bytes = match compression {
        CompressionMethod::None => bytes,
        CompressionMethod::Zstd => {
            zstd::stream::decode_all(Cursor::new(bytes)).context(ZstdError)?
        }
    };

    let stream_reader = StreamReader::try_new(Cursor::new(bytes), None).context(ArrowError)?;
    stream_reader
        .collect::<std::result::Result<Vec<RecordBatch>, _>>()
        .context(ArrowError)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int32Array, StringArray, StringDictionaryBuilder},
        datatypes::{DataType, Field, Int32Type, Schema},
    };

    use super::*;

    fn create_dictionary_record_batch() -> RecordBatch {
        let col1 = Field::new_dict(
            "dic1",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
            1,
            false,
        );
        let col2 = Field::new_dict(
            "dic2",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
            0,
            false,
        );
        let schema = Schema::new(vec![col1, col2]);
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder.append_value("d1");
        builder.append_value("d2");
        let dic1 = builder.finish();
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder.append_null();
        builder.append_value("d3");
        let dic2 = builder.finish();

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dic1), Arc::new(dic2)]).unwrap()
    }

    fn create_batch(seed: usize, rows: usize) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
            Field::new(
                "c",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new(
                "d",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
        ]);

        let a = Int32Array::from_iter_values(0..rows as i32);
        let b = StringArray::from_iter_values((0..rows).map(|i| (i + seed).to_string()));
        let c = {
            let mut b = StringDictionaryBuilder::<Int32Type>::new();
            for i in 0..rows {
                b.append_value(((i + seed) % 10).to_string());
            }
            b.finish()
        };
        let d = {
            let mut b = StringDictionaryBuilder::<Int32Type>::new();
            for i in 0..rows {
                b.append_value(((i + seed) % 20).to_string());
            }
            b.finish()
        };
        RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a), Arc::new(b), Arc::new(c), Arc::new(d)],
        )
        .unwrap()
    }

    fn ensure_encoding_and_decoding(
        input: &RecordBatch,
        compress_opts: CompressOptions,
        expect_compress_method: CompressionMethod,
    ) {
        let output = encode_record_batch(input, compress_opts).unwrap();
        assert_eq!(output.method, expect_compress_method);
        let decoded_batches = decode_record_batches(output.payload, output.method).unwrap();
        assert_eq!(decoded_batches.len(), 1);
        assert_eq!(input, &decoded_batches[0]);
    }

    #[test]
    fn test_ipc_encode_decode() {
        let batch = create_batch(0, 1024);
        for compression in [CompressionMethod::None, CompressionMethod::Zstd] {
            let compress_opts = CompressOptions {
                compress_min_length: 0,
                method: compression,
            };
            ensure_encoding_and_decoding(&batch, compress_opts, compression);
        }
    }

    #[test]
    fn test_ipc_encode_decode_with_dicitonary_encode() {
        let batch = create_dictionary_record_batch();
        for compression in [CompressionMethod::None, CompressionMethod::Zstd] {
            let compress_opts = CompressOptions {
                compress_min_length: 0,
                method: compression,
            };
            ensure_encoding_and_decoding(&batch, compress_opts, compression);
        }
    }

    #[test]
    fn test_encode_multiple_record_batches() {
        let num_batches = 1000;
        let mut batches = Vec::with_capacity(num_batches);
        for i in 0..num_batches {
            batches.push(create_batch(i, 1024));
        }

        let compress_opts = CompressOptions {
            compress_min_length: 0,
            method: CompressionMethod::Zstd,
        };
        let mut encoder = RecordBatchesEncoder::new(compress_opts);
        for batch in &batches {
            encoder.write(batch).unwrap();
        }
        let output = encoder.finish().unwrap();
        assert_eq!(output.method, CompressionMethod::Zstd);
        let decoded_batches =
            decode_record_batches(output.payload, CompressionMethod::Zstd).unwrap();
        assert_eq!(decoded_batches, batches);
    }

    #[test]
    fn test_compression_decision() {
        let batch = create_batch(0, 1024);

        {
            // Encode the record batch with a large `compress_min_length`, so the output
            // should not be compressed.
            let compress_opts = CompressOptions {
                compress_min_length: 1024 * 1024 * 1024,
                method: CompressionMethod::Zstd,
            };
            ensure_encoding_and_decoding(&batch, compress_opts, CompressionMethod::None);
        }

        {
            // Encode the record batch with a small `compress_min_length`, so the output
            // should be compressed.
            let compress_opts = CompressOptions {
                compress_min_length: 10,
                method: CompressionMethod::Zstd,
            };
            ensure_encoding_and_decoding(&batch, compress_opts, CompressionMethod::Zstd);
        }
    }

    // Test that encoding without any record batch should not panic.
    #[test]
    fn test_encode_no_record_batch() {
        let compress_opts = CompressOptions {
            compress_min_length: 0,
            method: CompressionMethod::Zstd,
        };
        let encoder = RecordBatchesEncoder::new(compress_opts);
        let output = encoder.finish().unwrap();
        assert_eq!(output.method, CompressionMethod::None);
        assert!(output.payload.is_empty());
    }
}
