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

// Tests.

use std::{collections::HashSet, time::Duration};

use log::{error, info};
use rand::prelude::*;

use super::*;
use crate::{config::ObkvConfig, memory::MemoryImpl, obkv::ObkvImpl};

const MAX_TABLE_ID: u32 = 30;

struct TableKvTester<T: TableKv> {
    table_kv: T,
    tables: HashSet<String>,
}

impl<T: TableKv> TableKvTester<T> {
    fn new(table_kv: T) -> Self {
        TableKvTester {
            table_kv,
            tables: HashSet::new(),
        }
    }

    fn create_table(&mut self, table_name: &str) {
        self.table_kv.create_table(table_name).unwrap();

        self.tables.insert(table_name.to_string());
    }

    fn insert_batch(&self, table_name: &str, pairs: &[(&[u8], &[u8])]) {
        self.try_insert_batch(table_name, pairs).unwrap();
    }

    fn try_insert_batch(
        &self,
        table_name: &str,
        pairs: &[(&[u8], &[u8])],
    ) -> std::result::Result<(), T::Error> {
        let mut batch = T::WriteBatch::with_capacity(pairs.len());
        for pair in pairs {
            batch.insert(pair.0, pair.1);
        }

        self.table_kv
            .write(WriteContext::default(), table_name, batch)
    }

    fn insert_or_update_batch(&self, table_name: &str, pairs: &[(&[u8], &[u8])]) {
        let mut batch = T::WriteBatch::with_capacity(pairs.len());
        for pair in pairs {
            batch.insert_or_update(pair.0, pair.1);
        }

        self.table_kv
            .write(WriteContext::default(), table_name, batch)
            .unwrap();
    }

    fn delete_batch(&self, table_name: &str, keys: &[&[u8]]) {
        let mut batch = T::WriteBatch::with_capacity(keys.len());
        for key in keys {
            batch.delete(key);
        }

        self.table_kv
            .write(WriteContext::default(), table_name, batch)
            .unwrap();
    }

    fn scan(
        &self,
        ctx: ScanContext,
        table_name: &str,
        scan_req: ScanRequest,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut iter = self.table_kv.scan(ctx, table_name, scan_req).unwrap();

        let mut pairs = Vec::new();
        while iter.valid() {
            let pair = (iter.key().to_vec(), iter.value().to_vec());
            pairs.push(pair);

            iter.next().unwrap();
        }

        pairs
    }

    fn get(&self, table_name: &str, key: &[u8]) -> Option<Vec<u8>> {
        self.table_kv.get(table_name, key).unwrap()
    }

    fn truncate_table(&self, table_name: &str) {
        info!("truncate table, table_name:{}", table_name);

        self.table_kv.truncate_table(table_name).unwrap();

        // It seems that truncate of obkv isn't taking effect immediately.
        std::thread::sleep(Duration::from_secs(1));
    }
}

impl<T: TableKv> Drop for TableKvTester<T> {
    fn drop(&mut self) {
        for table in &self.tables {
            info!("tester truncate table, table_name:{}", table);

            if let Err(e) = self.table_kv.truncate_table(table) {
                error!(
                    "tester failed to truncate table, table_name:{}, err:{}",
                    table, e
                );
            }

            if let Err(e) = self.table_kv.drop_table(table) {
                error!(
                    "tester failed to drop table, table_name:{}, err:{}",
                    table, e
                );
            }
        }
    }
}

fn new_obkv_config() -> ObkvConfig {
    ObkvConfig::for_test()
}

fn new_obkv_tester() -> TableKvTester<ObkvImpl> {
    let config = new_obkv_config();

    let table_kv = ObkvImpl::new(config).unwrap();

    TableKvTester::new(table_kv)
}

fn new_memory_tester() -> TableKvTester<MemoryImpl> {
    let table_kv = MemoryImpl::default();

    TableKvTester::new(table_kv)
}

fn random_table_name(prefix: &str) -> String {
    let mut rng = thread_rng();
    let v: u32 = rng.gen_range(0, MAX_TABLE_ID);

    format!("{prefix}_{v}")
}

fn new_scan_ctx(batch_size: i32) -> ScanContext {
    ScanContext {
        batch_size,
        ..Default::default()
    }
}

fn all_scan_ctxs() -> Vec<ScanContext> {
    vec![
        new_scan_ctx(1),
        new_scan_ctx(10),
        new_scan_ctx(50),
        ScanContext::default(),
    ]
}

fn check_scan_result(expect: &[(&[u8], &[u8])], result: &[(Vec<u8>, Vec<u8>)]) {
    assert_eq!(expect.len(), result.len());

    for (pair1, pair2) in expect.iter().zip(result) {
        assert_eq!(pair1.0, pair2.0);
        assert_eq!(pair1.1, pair2.1);
    }
}

#[test]
#[ignore]
fn test_obkv() {
    let tester = new_obkv_tester();

    run_table_kv_test(tester);
}

#[test]
fn test_memory() {
    let tester = new_memory_tester();

    run_table_kv_test(tester);
}

fn run_table_kv_test<T: TableKv>(mut tester: TableKvTester<T>) {
    let table_name = random_table_name("ceresdb");
    tester.create_table(&table_name);
    tester.truncate_table(&table_name);

    info!("test table kv, table_name:{}", table_name);

    test_simple_write_read(&tester, &table_name);

    test_update(&tester, &table_name);

    test_insert_duplicate(&tester, &table_name);

    test_partial_scan(&tester, &table_name);

    test_prefix_scan(&tester, &table_name);

    test_delete(&tester, &table_name);

    test_min_max_scan(&tester, &table_name);

    test_reverse_scan(&tester, &table_name);
}

// This test does a full scan, need to truncate table.
fn test_simple_write_read<T: TableKv>(tester: &TableKvTester<T>, table_name: &str) {
    tester.truncate_table(table_name);

    let mut data: [(&[u8], &[u8]); 3] = [
        (b"simple:a1", b"value a1"),
        (b"simple:b2", b"value b2"),
        (b"simple:a2", b"value a2"),
    ];

    // Test get empty.
    for pair in data {
        assert!(tester.get(table_name, pair.0).is_none());
    }

    // Test scan empty.
    let scan_req = ScanRequest {
        start: KeyBoundary::min_included(),
        end: KeyBoundary::max_included(),
        reverse: false,
    };
    for ctx in all_scan_ctxs() {
        let result = tester.scan(ctx, table_name, scan_req.clone());
        assert!(result.is_empty());
    }

    tester.insert_batch(table_name, &data);

    for pair in data {
        let v = tester.get(table_name, pair.0).unwrap();
        assert_eq!(pair.1, v);
    }

    let scan_req = ScanRequest {
        start: KeyBoundary::min_included(),
        end: KeyBoundary::max_included(),
        reverse: false,
    };
    data.sort_unstable_by_key(|v| v.0);
    for ctx in all_scan_ctxs() {
        let result = tester.scan(ctx, table_name, scan_req.clone());

        check_scan_result(&data, &result);
    }
}

fn test_update<T: TableKv>(tester: &TableKvTester<T>, table_name: &str) {
    let data: [(&[u8], &[u8]); 2] = [(b"update:a1", b"value a1"), (b"update:b1", b"value b1")];

    tester.insert_or_update_batch(table_name, &data);

    for pair in data {
        let v = tester.get(table_name, pair.0).unwrap();
        assert_eq!(pair.1, v);
    }

    let data: [(&[u8], &[u8]); 2] = [
        (b"update:b1", b"update value b1"),
        (b"update:c1", b"update value c1"),
    ];

    tester.insert_or_update_batch(table_name, &data);

    for pair in data {
        let v = tester.get(table_name, pair.0).unwrap();
        assert_eq!(pair.1, v);
    }
}

fn test_insert_duplicate<T: TableKv>(tester: &TableKvTester<T>, table_name: &str) {
    let data: [(&[u8], &[u8]); 1] = [(b"duplicate:a1", b"value a1")];

    tester.insert_batch(table_name, &data);

    let ret = tester.try_insert_batch(table_name, &data);
    if let Err(err) = ret {
        assert!(err.is_primary_key_duplicate());
    } else {
        panic!("Unexpected insert result, ret:{ret:?}");
    }
}

fn test_delete<T: TableKv>(tester: &TableKvTester<T>, table_name: &str) {
    let data: [(&[u8], &[u8]); 4] = [
        (b"delete:a1", b"value a1"),
        (b"delete:b1", b"value b1"),
        (b"delete:b2", b"value b2"),
        (b"delete:c1", b"value c1"),
    ];

    tester.insert_batch(table_name, &data);

    for pair in data {
        let v = tester.get(table_name, pair.0).unwrap();
        assert_eq!(pair.1, v);
    }

    tester.delete_batch(table_name, &[b"b1", b"b2"]);

    assert_eq!(
        b"value a1",
        tester.get(table_name, b"delete:a1").unwrap().as_slice()
    );
    assert!(tester.get(table_name, b"b1").is_none());
    assert!(tester.get(table_name, b"b2").is_none());
    assert_eq!(
        b"value c1",
        tester.get(table_name, b"delete:c1").unwrap().as_slice()
    );
}

// This test scan to min/max, need to truncate table.
fn test_min_max_scan<T: TableKv>(tester: &TableKvTester<T>, table_name: &str) {
    tester.truncate_table(table_name);

    let data: [(&[u8], &[u8]); 5] = [
        (b"minmax:a1", b"value a1"),
        (b"minmax:b1", b"value b1"),
        (b"minmax:c1", b"value c1"),
        (b"minmax:d1", b"value d1"),
        (b"minmax:e1", b"value e1"),
    ];

    tester.insert_batch(table_name, &data);

    let scan_req = ScanRequest {
        start: KeyBoundary::min_included(),
        end: KeyBoundary::max_included(),
        reverse: false,
    };
    for ctx in all_scan_ctxs() {
        let result = tester.scan(ctx, table_name, scan_req.clone());

        check_scan_result(&data, &result);
    }

    let scan_req = ScanRequest {
        start: KeyBoundary::min_included(),
        end: KeyBoundary::min_included(),
        reverse: false,
    };
    for ctx in all_scan_ctxs() {
        let result = tester.scan(ctx, table_name, scan_req.clone());

        check_scan_result(&[], &result);
    }

    let scan_req = ScanRequest {
        start: KeyBoundary::included(b"minmax"),
        end: KeyBoundary::min_included(),
        reverse: false,
    };
    for ctx in all_scan_ctxs() {
        let result = tester.scan(ctx, table_name, scan_req.clone());

        check_scan_result(&[], &result);
    }

    let scan_req = ScanRequest {
        start: KeyBoundary::max_included(),
        end: KeyBoundary::max_included(),
        reverse: false,
    };
    for ctx in all_scan_ctxs() {
        let result = tester.scan(ctx, table_name, scan_req.clone());

        check_scan_result(&[], &result);
    }

    let scan_req = ScanRequest {
        start: KeyBoundary::max_included(),
        end: KeyBoundary::included(b"minmax:d"),
        reverse: false,
    };
    for ctx in all_scan_ctxs() {
        let result = tester.scan(ctx, table_name, scan_req.clone());

        check_scan_result(&[], &result);
    }

    let scan_req = ScanRequest {
        start: KeyBoundary::max_included(),
        end: KeyBoundary::min_included(),
        reverse: false,
    };
    for ctx in all_scan_ctxs() {
        let result = tester.scan(ctx, table_name, scan_req.clone());

        check_scan_result(&[], &result);
    }
}

// This test does a full scan, need to truncate table.
fn test_reverse_scan<T: TableKv>(tester: &TableKvTester<T>, table_name: &str) {
    tester.truncate_table(table_name);

    let data: [(&[u8], &[u8]); 5] = [
        (b"reverse:e1", b"value e1"),
        (b"reverse:d1", b"value d1"),
        (b"reverse:c1", b"value c1"),
        (b"reverse:b1", b"value b1"),
        (b"reverse:a1", b"value a1"),
    ];

    tester.insert_batch(table_name, &data);

    let scan_req = ScanRequest {
        start: KeyBoundary::min_included(),
        end: KeyBoundary::max_included(),
        reverse: true,
    };
    for ctx in all_scan_ctxs() {
        let result = tester.scan(ctx, table_name, scan_req.clone());

        check_scan_result(&data, &result);
    }

    let scan_req = ScanRequest {
        start: KeyBoundary::min_included(),
        end: KeyBoundary::excluded(b"reverse:d1"),
        reverse: true,
    };
    for ctx in all_scan_ctxs() {
        let result = tester.scan(ctx, table_name, scan_req.clone());

        check_scan_result(&data[2..], &result);
    }

    let scan_req = ScanRequest {
        start: KeyBoundary::included(b"reverse:b1"),
        end: KeyBoundary::max_included(),
        reverse: true,
    };
    for ctx in all_scan_ctxs() {
        let result = tester.scan(ctx, table_name, scan_req.clone());

        check_scan_result(&data[..4], &result);
    }
}

fn test_partial_scan<T: TableKv>(tester: &TableKvTester<T>, table_name: &str) {
    let data: [(&[u8], &[u8]); 7] = [
        (b"partial:a1", b"value a1"),
        (b"partial:b1", b"value b1"),
        (b"partial:c1", b"value c1"),
        (b"partial:d1", b"value d1"),
        (b"partial:e1", b"value e1"),
        (b"partial:f1", b"value f1"),
        (b"partial:g1", b"value g1"),
    ];

    tester.insert_batch(table_name, &data);

    let scan_req = ScanRequest {
        start: KeyBoundary::included(data[1].0),
        end: KeyBoundary::included(data[5].0),
        reverse: false,
    };
    for ctx in all_scan_ctxs() {
        let result = tester.scan(ctx, table_name, scan_req.clone());

        check_scan_result(&data[1..=5], &result);
    }

    let scan_req = ScanRequest {
        start: KeyBoundary::excluded(data[1].0),
        end: KeyBoundary::included(data[5].0),
        reverse: false,
    };
    for ctx in all_scan_ctxs() {
        let result = tester.scan(ctx, table_name, scan_req.clone());

        check_scan_result(&data[2..=5], &result);
    }

    let scan_req = ScanRequest {
        start: KeyBoundary::included(data[1].0),
        end: KeyBoundary::excluded(data[5].0),
        reverse: false,
    };
    for ctx in all_scan_ctxs() {
        let result = tester.scan(ctx, table_name, scan_req.clone());

        check_scan_result(&data[1..5], &result);
    }

    let scan_req = ScanRequest {
        start: KeyBoundary::excluded(data[1].0),
        end: KeyBoundary::excluded(data[5].0),
        reverse: false,
    };
    for ctx in all_scan_ctxs() {
        let result = tester.scan(ctx, table_name, scan_req.clone());

        check_scan_result(&data[2..5], &result);
    }
}

fn test_prefix_scan<T: TableKv>(tester: &TableKvTester<T>, table_name: &str) {
    let data: [(&[u8], &[u8]); 6] = [
        (b"prefix:a1", b"value a1"),
        (b"prefix:b1", b"value b1"),
        (b"prefix:b2", b"value b2"),
        (b"prefix:b3", b"value b3"),
        (b"prefix:b4", b"value b4"),
        (b"prefix:c1", b"value c1"),
    ];

    tester.insert_batch(table_name, &data);

    let scan_req = ScanRequest {
        start: KeyBoundary::included(b"prefix:b"),
        end: KeyBoundary::included(b"prefix:z"),
        reverse: false,
    };
    for ctx in all_scan_ctxs() {
        let result = tester.scan(ctx, table_name, scan_req.clone());

        check_scan_result(&data[1..], &result);
    }

    let scan_req = ScanRequest {
        start: KeyBoundary::included(b"prefix:b"),
        end: KeyBoundary::excluded(b"prefix:b4"),
        reverse: false,
    };
    for ctx in all_scan_ctxs() {
        let result = tester.scan(ctx, table_name, scan_req.clone());

        check_scan_result(&data[1..4], &result);
    }
}
