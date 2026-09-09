//! 测试辅助函数与夹具

use windex::HashBucketEntry;

/// 构造语义化测试键
pub fn make_key(prefix: &str, id: usize) -> Vec<u8> {
  format!("{}_{}", prefix, id).into_bytes()
}

/// 批量预生成语义化测试键，消除并发读写热路径中的重复堆分配
pub fn make_keys(prefix: &str, count: usize) -> Vec<Vec<u8>> {
  (0..count).map(|id| make_key(prefix, id)).collect()
}

/// 构造受 48 位地址掩码限制的测试地址
pub const fn make_address(high: u64, low: u64) -> u64 {
  ((high << 32) | (low & 0xFFFF_FFFF)) & HashBucketEntry::ADDRESS_MASK
}
