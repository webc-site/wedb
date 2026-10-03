//! StoreConfig / GcConfig 默认值约定与 builder setter 断言
//!
//! 紧缩不设周期旋钮：判定节奏单点为内置 GC 物理回收轮次，C# 的
//! `CompactionFrequencySecs` 只用于注册周期任务、回退段数在调用点恒为字面量 1，
//! 二者在 wedb 均无对应配置字段；本测试锚定收敛后的默认值约定（缺席登记见
//! doc/zh/deviations.md §158 b）。

use aok::{OK, Void};
use wbase::cfg::LogCompactionType;
use wkv::{
  DEFAULT_DB_GC_RECLAIM_DELAY_SECS, DEFAULT_GC_MAX_BATCH_DELETES, DEFAULT_GC_MAX_SEGMENTS,
  GcConfig, StoreConfig,
};

/// 验证三类构造器的 GC/紧缩默认值约定与 builder 风格 setter
#[test]
fn store_config_compaction_defaults() -> Void {
  // 默认 GcConfig 全关（对标 Garnet ExpiredKeyDeletionScanFrequencySecs = -1：
  // 后台周期任务默认禁用，由读请求惰性过期淘汰）
  let d = GcConfig::default();
  assert!(!d.enabled);
  assert_eq!(d.scan_interval_ms, 0);
  assert_eq!(d.compaction_max_segments, DEFAULT_GC_MAX_SEGMENTS);
  // 紧缩档位默认 None（对标 C# GarnetServerOptions.CompactionType 默认，
  // 与 wconf 槽位默认一致：CONFIG GET 回显即引擎实效）
  assert_eq!(d.compaction_type, LogCompactionType::None);
  assert_eq!(d.max_batch_deletes, DEFAULT_GC_MAX_BATCH_DELETES);
  assert_eq!(d.db_gc_reclaim_delay_secs, DEFAULT_DB_GC_RECLAIM_DELAY_SECS);

  // auto()/auto_with_budget()（wedb_server 全新开库路径）与 minimal()/new()
  // （嵌入式与测试路径）约定一致：GC 默认关闭，开启经 GcConfig 显式字段注入
  assert_eq!(StoreConfig::auto().gc, GcConfig::default());
  assert_eq!(
    StoreConfig::auto_with_budget(1 << 30).gc,
    GcConfig::default()
  );
  let minimal = StoreConfig::minimal();
  assert_eq!(minimal, StoreConfig::default());
  assert_eq!(minimal.gc, GcConfig::default());
  let custom = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
  assert_eq!(custom.gc, GcConfig::default());

  // 显式开启：主动扫描仅 enabled + 间隔两键（C# ExpiredKeyDeletionScanFrequencySecs
  // > 0 的对译），紧缩仅经 compaction_type 档位——两者皆无周期字段
  let tuned = GcConfig {
    enabled: true,
    scan_interval_ms: 5_000,
    compaction_type: LogCompactionType::Lookup,
    ..GcConfig::default()
  };
  assert!(tuned.enabled);
  assert_eq!(tuned.scan_interval_ms, 5_000);
  assert_eq!(tuned.compaction_type, LogCompactionType::Lookup);

  // 字段直改覆写（与现有 with_* 构造口径等价）
  let mut with_gc = StoreConfig::minimal();
  with_gc.gc = tuned.clone();
  assert_eq!(with_gc.gc, tuned);

  OK
}

/// 验证自适应内存预算推导算法在极值、不同键规模下的严格不超标与合法性
#[test]
fn store_config_memory_budget_planning() -> Void {
  use wkv::{INDEX_BUCKET_BYTES, MAX_INDEX_SIZE, MIN_ADAPTIVE_BUDGET_BYTES, MIN_INDEX_SIZE};

  let budgets = [
    0,
    1,
    1024 * 1024,
    MIN_ADAPTIVE_BUDGET_BYTES,
    16 * 1024 * 1024,
    32 * 1024 * 1024,
    64 * 1024 * 1024,
    128 * 1024 * 1024,
    256 * 1024 * 1024,
    1024 * 1024 * 1024,
    16 * 1024 * 1024 * 1024,
    64 * 1024 * 1024 * 1024,
  ];

  let key_scenarios: [Option<u64>; 6] = [
    None,
    Some(0),
    Some(1_000),
    Some(50_000),
    Some(1_000_000),
    Some(100_000_000),
  ];

  for &b in &budgets {
    for &keys in &key_scenarios {
      let cfg = StoreConfig::from_memory_budget_with_keys(b, keys);

      // 1. 结构与合法性
      cfg.validate()?;
      assert!(cfg.index_size.is_power_of_two());
      assert!(cfg.page_size.is_power_of_two());
      assert!(cfg.num_pages.is_power_of_two());
      assert!(cfg.max_sessions.is_power_of_two());

      // 2. 边界约束
      assert!(cfg.index_size >= MIN_INDEX_SIZE);
      assert!(cfg.index_size <= MAX_INDEX_SIZE);
      assert!(cfg.num_pages >= 16);

      // 3. 严格不超标保证：在有效预算区间内，索引与日志内存之和绝不超预算
      let effective_budget = b.max(MIN_ADAPTIVE_BUDGET_BYTES);
      let index_mem = (cfg.index_size * INDEX_BUCKET_BYTES) as u64;
      let log_mem = (cfg.num_pages * cfg.page_size) as u64;
      assert!(
        index_mem + log_mem <= effective_budget,
        "内存规划超标: index({index_mem}) + log({log_mem}) = {} > budget({effective_budget})",
        index_mem + log_mem
      );
    }
  }

  OK
}

/// 页容量随预算自适应（大值通道，对标 C# GarnetServerOptions PageSize = "16m"
/// 与 KVSettings.DefaultMaxInlineValueSize = 1MB）
///
/// 生产大机（预算 ≥ 1GB）推导 16MB 页，单页可内联承载 1MB 大值记录；
/// 小预算宿主按 `预算 / 64` 收缩页容量，内存占用与固定 64KB 页同量级
#[test]
fn store_config_page_size_scales_with_budget() -> Void {
  use whlog::{DEFAULT_PAGE_SIZE, DEFAULT_SERVER_PAGE_SIZE};

  // 记录头 + 键的容量余量（1MB 值内联所需的页下限）
  let one_mb = 1024 * 1024usize;

  let cases: &[(u64, usize)] = &[
    (16 * 1024 * 1024, 256 * 1024), // 测试预算：256KB 页
    (64 * 1024 * 1024, 1024 * 1024),
    (256 * 1024 * 1024, 4 * one_mb),
    (512 * 1024 * 1024, 8 * one_mb),
    (1024 * 1024 * 1024, DEFAULT_SERVER_PAGE_SIZE), // 生产基线：16MB
    (16 * 1024 * 1024 * 1024, DEFAULT_SERVER_PAGE_SIZE),
  ];
  for &(budget, expected_page) in cases {
    let cfg = StoreConfig::auto_with_budget(budget);
    assert_eq!(
      cfg.page_size, expected_page,
      "预算 {budget} 字节的页容量推导不符"
    );
    // 页容量决定内联能力：128MB 及以上预算（生产服务器区间）必须能内联
    // 1MB 值 + 记录头 + 键（C# DefaultMaxInlineValueSize 基线）
    if budget >= 128 * 1024 * 1024 {
      assert!(
        cfg.page_size >= one_mb + 64,
        "预算 {budget} 的页容量 {} 无法内联 1MB 大值",
        cfg.page_size
      );
    }
  }

  // 极小预算回落 64KB 下限（微型嵌入式基线不变）
  let tiny = StoreConfig::auto_with_budget(1024 * 1024);
  assert_eq!(tiny.page_size, DEFAULT_PAGE_SIZE);

  OK
}
