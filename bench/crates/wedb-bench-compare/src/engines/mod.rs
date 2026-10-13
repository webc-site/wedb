//! 对比评测的列注册表：自家 `hash` / `bftree` 列复用 `wedb-bench` 的注册表，
//! 加第三方三列组成一张表。
//!
//! 每个第三方适配器只需暴露 `pub fn open(path: &Path, workload: &Workload) -> Result<Self, String>`
//! 并实现 `wedb_bench::traits::BenchDatabase`，与自家引擎同一套契约。

// 驱动入口只在至少编入一个第三方引擎时使用，纯自家列构建时这组导入闲置
#[cfg(any(
  feature = "bftree_native",
  feature = "fjall",
  feature = "rocksdb",
  feature = "sqlite"
))]
use std::path::Path;

#[cfg(any(
  feature = "bftree_native",
  feature = "fjall",
  feature = "rocksdb",
  feature = "sqlite"
))]
use wedb_bench::config::Workload;
#[cfg(any(
  feature = "bftree_native",
  feature = "fjall",
  feature = "rocksdb",
  feature = "sqlite"
))]
use wedb_bench::harness::benchmark;
#[cfg(any(
  feature = "bftree_native",
  feature = "fjall",
  feature = "rocksdb",
  feature = "sqlite"
))]
use wedb_bench::result::ResultType;
use wedb_bench::{engines::wedb_specs, runner::EngineSpec};

#[cfg(feature = "bftree_native")]
pub mod bftree_native_engine;
#[cfg(feature = "fjall")]
pub mod fjall_engine;
#[cfg(feature = "rocksdb")]
pub mod rocksdb_engine;
#[cfg(feature = "sqlite")]
pub mod sqlite_engine;

pub const BFTREE_NATIVE_SOURCE: &str =
  "https://github.com/microsoft/garnet/tree/main/libs/native/bftree-garnet";

pub const FJALL_SOURCE: &str = "https://github.com/fjall-rs/fjall";
pub const ROCKSDB_SOURCE: &str = "https://github.com/rust-rocksdb/rust-rocksdb";
pub const SQLITE_SOURCE: &str = "https://github.com/rusqlite/rusqlite";

#[cfg(feature = "bftree_native")]
fn run_bftree_native(
  path: &Path,
  workload: &Workload,
) -> Result<Vec<(String, ResultType)>, String> {
  let db = bftree_native_engine::BfTreeNativeEngine::open(path, workload)?;
  Ok(benchmark(db, path, workload))
}

#[cfg(feature = "fjall")]
fn run_fjall(path: &Path, workload: &Workload) -> Result<Vec<(String, ResultType)>, String> {
  let db = fjall_engine::FjallEngine::open(path, workload)?;
  Ok(benchmark(db, path, workload))
}

#[cfg(feature = "rocksdb")]
fn run_rocksdb(path: &Path, workload: &Workload) -> Result<Vec<(String, ResultType)>, String> {
  let db = rocksdb_engine::RocksdbEngine::open(path, workload)?;
  Ok(benchmark(db, path, workload))
}

#[cfg(feature = "sqlite")]
fn run_sqlite(path: &Path, workload: &Workload) -> Result<Vec<(String, ResultType)>, String> {
  let db = sqlite_engine::SqliteEngine::open(path, workload)?;
  Ok(benchmark(db, path, workload))
}

/// 本 crate 编译进来的全部列：自家列在前，对照列按 fjall / rocksdb / sqlite 排列
pub fn compare_specs() -> Vec<EngineSpec> {
  #[allow(unused_mut)] // 无第三方 feature 时只剩自家列，specs 不再追加
  let mut specs = wedb_specs();
  #[cfg(feature = "bftree_native")]
  specs.push(EngineSpec {
    name: "bftree_native",
    source: BFTREE_NATIVE_SOURCE,
    run: run_bftree_native,
  });
  #[cfg(feature = "fjall")]
  specs.push(EngineSpec {
    name: "fjall",
    source: FJALL_SOURCE,
    run: run_fjall,
  });
  #[cfg(feature = "rocksdb")]
  specs.push(EngineSpec {
    name: "rocksdb",
    source: ROCKSDB_SOURCE,
    run: run_rocksdb,
  });
  #[cfg(feature = "sqlite")]
  specs.push(EngineSpec {
    name: "sqlite",
    source: SQLITE_SOURCE,
    run: run_sqlite,
  });
  specs
}
