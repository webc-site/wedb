//! 测试存储配置装配（对标 C# 测试基类显式 MemorySizeBits 的小预算收窄）

use wkv::StoreConfig;

/// 测试进程的显式收窄内存预算（对标 C# 测试基类 MemorySizeBits = 14 → 16MB）
const TEST_MEMORY_BUDGET_BYTES: u64 = 16 * 1024 * 1024;

/// 测试/嵌入式装配的小预算存储配置（对标 C# 测试基类显式
/// MemorySizeBits = 14 → 16MB）
///
/// 生产装配按整机内存自适应（大机上可规划出 GB 级索引与日志缓冲），
/// 测试进程并发启动时索引分配、检查点 CRC 与 fsync 全链被放大拖垮；
/// 测试必须显式收窄内存预算。内置 GC 保持默认禁用（对标 C#
/// ExpiredKeyDeletionScanFrequencySecs = -1），需要后台扫描的测试显式
/// `start_gc` 或经 CONFIG SET 调停拉起，槽位即唯一启停真值源
#[must_use]
pub fn test_store_config_with_budget(budget: u64) -> StoreConfig {
  StoreConfig::auto_with_budget(budget)
}

/// 测试/嵌入式装配的小预算存储配置（对标 C# 测试基类显式
/// MemorySizeBits = 14 → 16MB）
///
/// 生产装配按整机内存自适应（大机上可规划出 GB 级索引与日志缓冲），
/// 测试进程并发启动时索引分配、检查点 CRC 与 fsync 全链被放大拖垮；
/// 测试必须显式收窄内存预算
#[must_use]
pub fn test_store_config() -> StoreConfig {
  test_store_config_with_budget(TEST_MEMORY_BUDGET_BYTES)
}
