//! 自家引擎注册表：列名、来源与 18 段驱动入口。
//!
//! `hash` 列 = `wkv`（混合日志 KV），`bftree` 列 = `wbftree`（有序索引）。
//! 第三方对照引擎在 `wedb-bench-compare` 里按同样形态注册。

// 驱动入口只在至少编入一个自家引擎存在时使用，无引擎构建时整组导入闲置
#[cfg(any(feature = "hash", feature = "bftree"))]
use std::path::Path;

#[cfg(any(feature = "hash", feature = "bftree"))]
use crate::config::Workload;
#[cfg(any(feature = "hash", feature = "bftree"))]
use crate::harness::benchmark;
#[cfg(any(feature = "hash", feature = "bftree"))]
use crate::result::ResultType;
use crate::runner::EngineSpec;

#[cfg(feature = "bftree")]
pub mod bftree_engine;
#[cfg(feature = "hash")]
pub mod hash_engine;

pub const HASH_SOURCE: &str = "https://github.com/webc-site/wedb/tree/main/wedb/wkv";
pub const BFTREE_SOURCE: &str = "https://github.com/webc-site/wedb/tree/main/wedb/wbftree";

#[cfg(feature = "hash")]
fn run_hash(path: &Path, workload: &Workload) -> Result<Vec<(String, ResultType)>, String> {
  let db = hash_engine::HashEngine::open(path, workload)?;
  Ok(benchmark(db, path, workload))
}

#[cfg(feature = "bftree")]
fn run_bftree(path: &Path, workload: &Workload) -> Result<Vec<(String, ResultType)>, String> {
  let db = bftree_engine::BftreeEngine::open(path, workload)?;
  Ok(benchmark(db, path, workload))
}

/// 本 crate 编译进来的自家引擎列
pub fn wedb_specs() -> Vec<EngineSpec> {
  #[allow(unused_mut)] // 无任何自家 feature 时 specs 保持空
  let mut specs: Vec<EngineSpec> = Vec::new();
  #[cfg(feature = "hash")]
  specs.push(EngineSpec {
    name: "hash",
    source: HASH_SOURCE,
    run: run_hash,
  });
  #[cfg(feature = "bftree")]
  specs.push(EngineSpec {
    name: "bftree",
    source: BFTREE_SOURCE,
    run: run_bftree,
  });
  specs
}
