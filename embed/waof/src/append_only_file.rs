use std::sync::atomic::{AtomicI64, Ordering};

/// garnet相对路径:garnet/libs/server/AOF/GarnetAppendOnlyFile.cs:GarnetAppendOnlyFile
pub struct GarnetAppendOnlyFile {
  pub tail_address: AtomicI64,
}

impl GarnetAppendOnlyFile {
  pub fn new() -> Self {
    Self {
      tail_address: AtomicI64::new(64),
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
