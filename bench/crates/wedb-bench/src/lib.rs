//! wedb 评测 harness：与 [redb-bench](https://github.com/cberner/redb) 同构的
//! 引擎契约、18 段 workload 与 markdown 结果表。
//!
//! 设计对齐 redb 的两处切分：
//! - 本 crate 只带自家引擎（`hash` = wkv、`bftree` = wbftree）与 harness；
//! - 第三方对照引擎放在 `wedb-bench-compare`，把它们的原生构建依赖挡在主 crate 之外。
//!
//! 结果表与 redb 逐字同构：同样的行名、同样的单位标注（key/s、txn/s、scan/s）、
//! 同样的速率三档格式化与最优加粗（含并列），因此可与 redb 公布的表并列阅读。

pub mod config;
pub mod engines;
pub mod harness;
pub mod json;
pub mod machine;
pub mod report;
pub mod result;
pub mod runner;
pub mod table;
pub mod traits;
pub use config::{REDB_CACHE_SIZE, Workload};
pub use harness::{benchmark, database_size, na_rows, row_names};
pub use json::{JSON_SCHEMA, JsonEngine, JsonHistory, JsonReport, JsonRow, JsonRun};
pub use machine::MachineInfo;
pub use result::{ResultType, ThroughputUnit, format_duration, format_rate, metric_key};
pub use runner::{EngineRunFn, EngineSpec, main_logic};
pub use table::{EngineResults, print_results_table, results_table_markdown};
pub use traits::*;
