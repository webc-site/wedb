//! 测试存储配置装配（对标 C# 测试基类显式 MemorySizeBits 的小预算收窄）

use wkv::{DEFAULT_GC_COMPACTION_INTERVAL_MS, DEFAULT_GC_SCAN_INTERVAL_MS, StoreConfig};

/// 测试进程的显式收窄内存预算（对标 C# 测试基类 MemorySizeBits = 14 → 16MB）
const TEST_MEMORY_BUDGET_BYTES: u64 = 16 * 1024 * 1024;

/// 测试/嵌入式装配的小预算存储配置（对标 C# 测试基类显式
/// MemorySizeBits = 14 → 16MB）
///
/// 生产装配按整机内存自适应（大机上可规划出 GB 级索引与日志缓冲），
/// 测试进程并发启动时索引分配、检查点 CRC 与 fsync 全链被放大拖垮；
/// 测试必须显式收窄内存预算
#[must_use]
pub fn test_store_config() -> StoreConfig {
  let mut config = StoreConfig::auto_with_budget(TEST_MEMORY_BUDGET_BYTES);
  config.gc.enabled = true;
  config.gc.scan_interval_ms = DEFAULT_GC_SCAN_INTERVAL_MS;
  config.gc.compaction_interval_ms = DEFAULT_GC_COMPACTION_INTERVAL_MS;
  config
}
