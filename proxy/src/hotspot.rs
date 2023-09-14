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

//! hotspot recorder
use std::{fmt::Write, sync::Arc};

use ceresdbproto::storage::{
    PrometheusQueryRequest, RequestContext, SqlQueryRequest, WriteRequest,
};
use log::{info, warn};
use runtime::Runtime;
use serde::{Deserialize, Serialize};
use spin::Mutex as SpinMutex;
use time_ext::ReadableDuration;
use timed_task::TimedTask;
use tokio::sync::mpsc::{self, Sender};

use crate::{hotspot_lru::HotspotLru, util};

type QueryKey = String;
type WriteKey = String;
const TAG: &str = "hotspot autodump";
const RECORDER_CHANNEL_CAP: usize = 64 * 1024;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    /// Max items size for query hotspot
    query_cap: Option<usize>,
    /// Max items size for write hotspot
    write_cap: Option<usize>,
    /// The hotspot records will be auto dumped if set.
    enable_auto_dump: bool,
    /// The interval between two auto dumps
    auto_dump_interval: ReadableDuration,
    /// The number of items for auto dump
    auto_dump_num_items: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            query_cap: Some(10_000),
            write_cap: Some(10_000),
            auto_dump_interval: ReadableDuration::minutes(1),
            enable_auto_dump: true,
            auto_dump_num_items: 10,
        }
    }
}

pub enum Message {
    Query(QueryKey),
    Write {
        key: WriteKey,
        row_count: usize,
        field_count: usize,
    },
}

#[derive(Clone)]
pub struct HotspotRecorder {
    tx: Arc<Sender<Message>>,
    stat: HotspotStat,
}

#[derive(Clone)]
pub struct HotspotStat {
    hotspot_query: Option<Arc<SpinMutex<HotspotLru<QueryKey>>>>,
    hotspot_write: Option<Arc<SpinMutex<HotspotLru<WriteKey>>>>,
    hotspot_field_write: Option<Arc<SpinMutex<HotspotLru<WriteKey>>>>,
}

impl HotspotStat {
    /// return read count / write row count / write field count
    pub fn dump(&self) -> Dump {
        Dump {
            read_hots: self
                .pop_read_hots()
                .map_or_else(Vec::new, HotspotStat::format_hots),
            write_hots: self
                .pop_write_hots()
                .map_or_else(Vec::new, HotspotStat::format_hots),
            write_field_hots: self
                .pop_write_field_hots()
                .map_or_else(Vec::new, HotspotStat::format_hots),
        }
    }

    fn format_hots(hots: Vec<(String, u64)>) -> Vec<String> {
        hots.into_iter()
            .map(|(k, v)| format!("metric={k}, heats={v}"))
            .collect()
    }

    fn pop_read_hots(&self) -> Option<Vec<(QueryKey, u64)>> {
        HotspotStat::pop_hots(&self.hotspot_query)
    }

    fn pop_write_hots(&self) -> Option<Vec<(WriteKey, u64)>> {
        HotspotStat::pop_hots(&self.hotspot_write)
    }

    fn pop_write_field_hots(&self) -> Option<Vec<(WriteKey, u64)>> {
        HotspotStat::pop_hots(&self.hotspot_field_write)
    }

    fn pop_hots(target: &Option<Arc<SpinMutex<HotspotLru<String>>>>) -> Option<Vec<(String, u64)>> {
        target.as_ref().map(|hotspot| {
            let mut hots = hotspot.lock().pop_all();
            hots.sort_by(|a, b| b.1.cmp(&a.1));
            hots
        })
    }
}

#[derive(Clone)]
pub struct Dump {
    pub read_hots: Vec<String>,
    pub write_hots: Vec<String>,
    pub write_field_hots: Vec<String>,
}

// TODO: move HotspotRecorder to components dir for reuse.
impl HotspotRecorder {
    pub fn new(config: Config, runtime: Arc<Runtime>) -> Self {
        let hotspot_query = Self::init_lru(config.query_cap);
        let hotspot_write = Self::init_lru(config.write_cap);
        let hotspot_field_write = Self::init_lru(config.write_cap);

        let stat = HotspotStat {
            hotspot_query: hotspot_query.clone(),
            hotspot_write: hotspot_write.clone(),
            hotspot_field_write: hotspot_field_write.clone(),
        };

        let task_handle = if config.enable_auto_dump {
            let interval = config.auto_dump_interval;
            let dump_len = config.auto_dump_num_items;
            let stat_clone = stat.clone();
            let builder = move || {
                let stat_in_builder = stat_clone.clone();
                async move {
                    let Dump {
                        read_hots,
                        write_hots,
                        write_field_hots,
                    } = stat_in_builder.dump();

                    read_hots
                        .into_iter()
                        .take(dump_len)
                        .for_each(|hot| info!("{} query {}", TAG, hot));
                    write_hots
                        .into_iter()
                        .take(dump_len)
                        .for_each(|hot| info!("{} write rows {}", TAG, hot));
                    write_field_hots
                        .into_iter()
                        .take(dump_len)
                        .for_each(|hot| info!("{} write fields {}", TAG, hot));
                }
            };

            Some(TimedTask::start_timed_task(
                String::from("hotspot_dump"),
                &runtime,
                interval.0,
                builder,
            ))
        } else {
            None
        };

        let (tx, mut rx) = mpsc::channel(RECORDER_CHANNEL_CAP);
        runtime.spawn(async move {
            loop {
                match rx.recv().await {
                    None => {
                        warn!("Hotspot recoder sender stopped");
                        if let Some(handle) = task_handle {
                            handle.stop_task().await.unwrap();
                        }
                        break;
                    }
                    Some(msg) => match msg {
                        Message::Query(read_key) => {
                            if let Some(hotspot) = &hotspot_query {
                                hotspot.lock().inc(&read_key, 1);
                            }
                        }
                        Message::Write {
                            key,
                            row_count,
                            field_count,
                        } => {
                            if let Some(hotspot) = &hotspot_write {
                                hotspot.lock().inc(&key, row_count as u64);
                            }

                            if let Some(hotspot) = &hotspot_field_write {
                                hotspot.lock().inc(&key, field_count as u64);
                            }
                        }
                    },
                }
            }
        });

        Self {
            tx: Arc::new(tx),
            stat,
        }
    }

    #[inline]
    fn init_lru(cap: Option<usize>) -> Option<Arc<SpinMutex<HotspotLru<QueryKey>>>> {
        HotspotLru::new(cap?).map(|lru| Arc::new(SpinMutex::new(lru)))
    }

    fn key_prefix(context: &Option<RequestContext>) -> String {
        let mut prefix = String::new();
        match context {
            Some(ctx) => {
                // use database as prefix
                if !ctx.database.is_empty() {
                    write!(prefix, "{}/", ctx.database).unwrap();
                }
            }
            None => {}
        }

        prefix
    }

    #[inline]
    fn table_hot_key(context: &Option<RequestContext>, table: &String) -> String {
        let prefix = Self::key_prefix(context);
        prefix + table
    }

    pub async fn inc_sql_query_reqs(&self, req: &SqlQueryRequest) {
        if self.stat.hotspot_query.is_none() {
            return;
        }

        for table in &req.tables {
            self.send_msg_or_log(
                "inc_query_reqs",
                Message::Query(Self::table_hot_key(&req.context, table)),
            )
            .await;
        }
    }

    pub async fn inc_write_reqs(&self, req: &WriteRequest) {
        if self.stat.hotspot_write.is_some() && self.stat.hotspot_field_write.is_some() {
            for table_request in &req.table_requests {
                let hot_key = Self::table_hot_key(&req.context, &table_request.table);
                let mut row_count = 0;
                let mut field_count = 0;
                for entry in &table_request.entries {
                    row_count += 1;
                    for field_group in &entry.field_groups {
                        field_count += field_group.fields.len();
                    }
                }
                self.send_msg_or_log(
                    "inc_write_reqs",
                    Message::Write {
                        key: hot_key,
                        row_count,
                        field_count,
                    },
                )
                .await;
            }
        }
    }

    pub async fn inc_promql_reqs(&self, req: &PrometheusQueryRequest) {
        if self.stat.hotspot_query.is_none() {
            return;
        }

        if let Some(expr) = &req.expr {
            if let Some(table) = util::table_from_expr(expr) {
                let hot_key = Self::table_hot_key(&req.context, &table);
                self.send_msg_or_log("inc_query_reqs", Message::Query(hot_key))
                    .await
            }
        }
    }

    pub async fn send_msg_or_log(&self, method: &str, msg: Message) {
        if let Err(e) = self.tx.send(msg).await {
            warn!(
                "HotspotRecoder::{} fail to send \
                measurement to recoder, err:{}",
                method, e
            );
        }
    }
}

#[cfg(test)]
mod test {
    use std::{thread, time::Duration};

    use ceresdbproto::{
        storage,
        storage::{
            value::Value::StringValue, Field, FieldGroup, Value, WriteSeriesEntry,
            WriteTableRequest,
        },
    };
    use runtime::Builder;

    fn new_runtime() -> Arc<Runtime> {
        let runtime = Builder::default()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();

        Arc::new(runtime)
    }

    use super::*;

    #[test]
    #[allow(clippy::redundant_clone)]
    fn test_hotspot() {
        let hotspot_runtime = new_runtime();
        let basic_runtime = new_runtime();
        let runtime = hotspot_runtime.clone();
        basic_runtime.block_on(async move {
            let read_cap: Option<usize> = Some(3);
            let write_cap: Option<usize> = Some(3);
            let options = Config {
                query_cap: read_cap,
                write_cap,
                enable_auto_dump: false,
                auto_dump_interval: ReadableDuration::millis(5000),
                auto_dump_num_items: 10,
            };
            let recorder = HotspotRecorder::new(options, runtime.clone());
            assert!(recorder.stat.pop_read_hots().unwrap().is_empty());
            assert!(recorder.stat.pop_write_hots().unwrap().is_empty());
            let table = String::from("table1");
            let context = mock_context();
            let req = SqlQueryRequest {
                context,
                tables: vec![table],
                sql: String::from("select * from table1 limit 10"),
            };

            recorder.inc_sql_query_reqs(&req).await;
            thread::sleep(Duration::from_millis(100));

            let vec = recorder.stat.pop_read_hots().unwrap();
            assert_eq!(1, vec.len());
            assert_eq!("public/table1", vec.get(0).unwrap().0);
        })
    }

    #[test]
    #[allow(clippy::redundant_clone)]
    fn test_hotspot_dump() {
        let hotspot_runtime = new_runtime();
        let basic_runtime = new_runtime();
        let runtime = hotspot_runtime.clone();
        basic_runtime.block_on(async move {
            let read_cap: Option<usize> = Some(10);
            let write_cap: Option<usize> = Some(10);
            let options = Config {
                query_cap: read_cap,
                write_cap,
                enable_auto_dump: false,
                auto_dump_interval: ReadableDuration::millis(5000),
                auto_dump_num_items: 10,
            };

            let recorder = HotspotRecorder::new(options, runtime.clone());

            assert!(recorder.stat.pop_read_hots().unwrap().is_empty());
            assert!(recorder.stat.pop_write_hots().unwrap().is_empty());

            let table = String::from("table1");
            let context = mock_context();
            let query_req = SqlQueryRequest {
                context,
                tables: vec![table.clone()],
                sql: String::from("select * from table1 limit 10"),
            };
            recorder.inc_sql_query_reqs(&query_req).await;

            let write_req = WriteRequest {
                context: mock_context(),
                table_requests: vec![WriteTableRequest {
                    table,
                    tag_names: vec![String::from("name")],
                    field_names: vec![String::from("value1"), String::from("value2")],
                    entries: vec![WriteSeriesEntry {
                        tags: vec![storage::Tag {
                            name_index: 0,
                            value: Some(Value {
                                value: Some(StringValue(String::from("name1"))),
                            }),
                        }],
                        field_groups: vec![FieldGroup {
                            timestamp: 1679647020000,
                            fields: vec![
                                Field {
                                    name_index: 0,
                                    value: Some(Value { value: None }),
                                },
                                Field {
                                    name_index: 1,
                                    value: Some(Value { value: None }),
                                },
                            ],
                        }],
                    }],
                }],
            };
            recorder.inc_write_reqs(&write_req).await;

            thread::sleep(Duration::from_millis(100));
            let Dump {
                read_hots,
                write_hots,
                write_field_hots,
            } = recorder.stat.dump();
            assert_eq!(vec!["metric=public/table1, heats=1",], write_hots);
            assert_eq!(vec!["metric=public/table1, heats=1"], read_hots);
            assert_eq!(vec!["metric=public/table1, heats=2",], write_field_hots);
            thread::sleep(Duration::from_millis(100));
        });
        drop(hotspot_runtime);
    }

    fn mock_context() -> Option<RequestContext> {
        Some(RequestContext {
            database: String::from("public"),
        })
    }
}
