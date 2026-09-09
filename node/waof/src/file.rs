use std::sync::atomic::{AtomicI64, Ordering};

/// 日志首有效地址（对标 C# GarnetAppendOnlyFile.cs 的 kFirstValidAofAddress：
/// 保留 64 字节头区，复制恢复以 64 作"副本非空"哨兵）
const FIRST_VALID_AOF_ADDRESS: i64 = 64;

/// libs/server/AOF/GarnetAppendOnlyFile.cs:GarnetAppendOnlyFile
pub struct GarnetAppendOnlyFile {
  pub tail_address: AtomicI64,
}

impl GarnetAppendOnlyFile {
  pub fn new() -> Self {
    Self {
      tail_address: AtomicI64::new(FIRST_VALID_AOF_ADDRESS),
    }
  }

  pub fn enqueue(&self, data: &[u8]) {
    self
      .tail_address
      .fetch_add(data.len() as i64, Ordering::SeqCst);
  }
}

impl Default for GarnetAppendOnlyFile {
  fn default() -> Self {
    Self::new()
  }
}
