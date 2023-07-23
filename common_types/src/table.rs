// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

pub type TableId = u64;
pub type TableName = String;
pub type ShardId = u32;
pub type ShardVersion = u64;
pub const DEFAULT_SHARD_ID: u32 = 0;
pub const DEFAULT_SHARD_VERSION: u64 = 0;
pub const DEFAULT_SYSTEM_SHARD_ID: u32 = u32::MAX;
