//! StoreConfig / GcConfig 默认值约定与 builder setter 断言
//!
//! 周期紧缩配置已二合一：原 `StoreConfig::compaction_freq_secs` 与
//! `compaction_max_seek_bytes` 无任何驱动者，Garnet CompactionTask 频率在 wedb
//! 由 `GcConfig::compaction_interval_ms` 统一承担，本测试锚定收敛后的默认值约定。

use aok::{OK, Void};
use wkv::{
  DEFAULT_GC_COMPACTION_INTERVAL_MS, DEFAULT_GC_MAX_BATCH_DELETES, DEFAULT_GC_MAX_SEGMENTS,
  DEFAULT_GC_NUM_SEGMENTS, DEFAULT_GC_SCAN_INTERVAL_MS, GcConfig, StoreConfig,
};

/// 验证三类构造器的 GC/紧缩默认值约定与 builder 风格 setter
#[test]
fn store_config_compaction_defaults() -> Void {
  // 默认 GcConfig 全关（对标 Garnet ExpiredKeyDeletionScanFrequencySecs = -1：
  // 后台周期任务默认禁用，由读请求惰性过期淘汰）
  let d = GcConfig::default();
  assert!(!d.enabled);
  assert_eq!(d.scan_interval_ms, 0);
  assert_eq!(d.compaction_interval_ms, 0);
  assert_eq!(d.compaction_max_segments, DEFAULT_GC_MAX_SEGMENTS);
  assert_eq!(d.compaction_num_segments, DEFAULT_GC_NUM_SEGMENTS);
  assert_eq!(d.max_batch_deletes, DEFAULT_GC_MAX_BATCH_DELETES);

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

  // 显式开启生产推荐值：5s 主动扫描 + 60s 紧缩判定
  let tuned = GcConfig {
    enabled: true,
    scan_interval_ms: DEFAULT_GC_SCAN_INTERVAL_MS,
    compaction_interval_ms: DEFAULT_GC_COMPACTION_INTERVAL_MS,
    ..GcConfig::default()
  };
  assert!(tuned.enabled);
  assert_eq!(tuned.scan_interval_ms, DEFAULT_GC_SCAN_INTERVAL_MS);
  assert_eq!(
    tuned.compaction_interval_ms,
    DEFAULT_GC_COMPACTION_INTERVAL_MS
  );

  // 字段直改覆写（与现有 with_* 构造口径等价）
  let mut with_gc = StoreConfig::minimal();
  with_gc.gc = tuned.clone();
  assert_eq!(with_gc.gc, tuned);

  OK
}
