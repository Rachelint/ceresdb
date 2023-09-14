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

//! Benchmarks

use std::sync::Once;

use benchmarks::{
    config::{self, BenchConfig},
    merge_memtable_bench::MergeMemTableBench,
    merge_sst_bench::MergeSstBench,
    parquet_bench::ParquetBench,
    scan_memtable_bench::ScanMemTableBench,
    sst_bench::SstBench,
    wal_write_bench::WalWriteBench,
};
use criterion::*;
use pprof::criterion::{Output, PProfProfiler};

static INIT_LOG: Once = Once::new();

pub fn init_bench() -> BenchConfig {
    INIT_LOG.call_once(|| {
        env_logger::init();
    });

    config::bench_config_from_env()
}

fn bench_read_sst_iter(b: &mut Bencher<'_>, bench: &SstBench) {
    b.iter(|| {
        bench.run_bench();
    })
}

fn bench_read_sst(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("read_sst");
    group.measurement_time(config.sst_bench.bench_measurement_time.0);
    group.sample_size(config.sst_bench.bench_sample_size);

    let mut bench = SstBench::new(config.sst_bench);

    for i in 0..bench.num_benches() {
        bench.init_for_bench(i);

        group.bench_with_input(
            BenchmarkId::new("read_sst", format!("{}/{}", bench.sst_file_name, i)),
            &bench,
            bench_read_sst_iter,
        );
    }

    group.finish();
}

fn bench_merge_sst_iter(b: &mut Bencher<'_>, bench: &MergeSstBench) {
    b.iter(|| bench.run_bench())
}

fn bench_merge_sst(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("merge_sst");

    group.measurement_time(config.merge_sst_bench.bench_measurement_time.0);
    group.sample_size(config.sst_bench.bench_sample_size);

    let sst_file_ids = format!("{:?}", config.merge_sst_bench.sst_file_ids);
    let mut bench = MergeSstBench::new(config.merge_sst_bench);

    for i in 0..bench.num_benches() {
        bench.init_for_bench(i, true);
        group.bench_with_input(
            BenchmarkId::new("merge_sst", format!("{sst_file_ids}/{i}/dedup")),
            &bench,
            bench_merge_sst_iter,
        );

        bench.init_for_bench(i, false);
        group.bench_with_input(
            BenchmarkId::new("merge_sst", format!("{sst_file_ids}/{i}/no-dedup")),
            &bench,
            bench_merge_sst_iter,
        );
    }

    group.finish();
}

fn bench_parquet_iter(b: &mut Bencher<'_>, bench: &ParquetBench) {
    b.iter(|| bench.run_bench())
}

fn bench_parquet(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("read_parquet");

    group.measurement_time(config.sst_bench.bench_measurement_time.0);
    group.sample_size(config.sst_bench.bench_sample_size);

    let mut bench = ParquetBench::new(config.sst_bench);

    for i in 0..bench.num_benches() {
        bench.init_for_bench(i);

        group.bench_with_input(
            BenchmarkId::new("read_parquet", format!("{}/{}", bench.sst_file_name, i)),
            &bench,
            bench_parquet_iter,
        );
    }

    group.finish();
}

fn bench_scan_memtable_iter(b: &mut Bencher<'_>, bench: &ScanMemTableBench) {
    b.iter(|| bench.run_bench())
}

fn bench_scan_memtable(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("scan_memtable");

    let mut bench = ScanMemTableBench::new(config.scan_memtable_bench);

    for i in 0..bench.num_benches() {
        bench.init_for_bench(i);

        group.bench_with_input(
            BenchmarkId::new("scan_memtable", i),
            &bench,
            bench_scan_memtable_iter,
        );
    }

    group.finish();
}

fn bench_merge_memtable_iter(b: &mut Bencher<'_>, bench: &MergeMemTableBench) {
    b.iter(|| bench.run_bench())
}

fn bench_merge_memtable(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("merge_memtable");

    let sst_file_ids = format!("{:?}", config.merge_memtable_bench.sst_file_ids);
    let mut bench = MergeMemTableBench::new(config.merge_memtable_bench);

    for i in 0..bench.num_benches() {
        bench.init_for_bench(i, true);
        group.bench_with_input(
            BenchmarkId::new("merge_memtable", format!("{sst_file_ids}/{i}/dedup")),
            &bench,
            bench_merge_memtable_iter,
        );

        bench.init_for_bench(i, false);
        group.bench_with_input(
            BenchmarkId::new("merge_memtable", format!("{sst_file_ids}/{i}/no-dedup")),
            &bench,
            bench_merge_memtable_iter,
        );
    }

    group.finish();
}

fn bench_wal_write_iter(b: &mut Bencher<'_>, bench: &WalWriteBench) {
    b.iter(|| bench.run_bench())
}

fn bench_wal_write(c: &mut Criterion) {
    let config = init_bench();

    let mut group = c.benchmark_group("wal_write");

    group.measurement_time(config.wal_write_bench.bench_measurement_time.0);
    group.sample_size(config.wal_write_bench.bench_sample_size);

    let bench = WalWriteBench::new(config.wal_write_bench);

    group.bench_with_input(
        BenchmarkId::new("wal_write", 0),
        &bench,
        bench_wal_write_iter,
    );

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_parquet,
    bench_read_sst,
    bench_merge_sst,
    bench_scan_memtable,
    bench_merge_memtable,
    bench_wal_write,
);

criterion_main!(benches);
