use parking_lot::RwLock;

use crate::server::migration::sketch_status::SketchStatus;

/// 默认 Sketch bitmap 槽位数（2^20 = 1,048,576 位，占 128KB）
const DEFAULT_KEY_COUNT: usize = 1 << 20;

/// 在 garnet 中的相对路径: libs/cluster/Server/Migration/Sketch.cs:Sketch
///
/// 迁移门控 bloom bitmap：收录键置位、probe 判定键级可访问性；
/// 传输与待删清单由调用方结构承担（SLOTS 扫描 work 列表、transferred 确认集、
/// KEYS 命令键参数），不承载 C# argSliceVector / Keys 的收集去重职责
pub struct Sketch {
  size: usize,
  bitmap: RwLock<Vec<u8>>,
  pub status: RwLock<SketchStatus>,
}

impl Sketch {
  /// 在 garnet 中的相对路径: libs/cluster/Server/Migration/Sketch.cs:Sketch
  pub fn new() -> Self {
    Self::with_key_count(DEFAULT_KEY_COUNT)
  }

  /// 指定 key 槽位上限构造 Sketch（必须是 2 的幂）
  pub(crate) fn with_key_count(key_count: usize) -> Self {
    assert!(
      key_count >= 8 && key_count.is_power_of_two(),
      "Sketch size should be power of 2 and >= 8"
    );
    let size = key_count;
    let bitmap = vec![0u8; size >> 3];
    Self {
      size,
      bitmap: RwLock::new(bitmap),
      status: RwLock::new(SketchStatus::Initializing),
    }
  }

  /// 在 garnet 中的相对路径: libs/cluster/Server/Migration/Sketch.cs:SetStatus
  pub fn set_status(&self, status: SketchStatus) {
    *self.status.write() = status;
  }

  /// 在 garnet 中的相对路径: libs/cluster/Server/Migration/Sketch.cs:Clear
  pub fn clear(&self) {
    self.bitmap.write().fill(0);
    *self.status.write() = SketchStatus::Initializing;
  }

  /// 计算带种子 key 对应 bitmap 的字节偏移与位偏移
  #[inline]
  fn slot_offset_with_seed(&self, key: &[u8], seed: u64) -> (usize, u8) {
    let slot = (whasher::fast_hash_with_seed(key, seed) as usize) & (self.size - 1);
    (slot >> 3, (slot & 7) as u8)
  }

  /// 在 garnet 中的相对路径: libs/cluster/Server/Migration/Sketch.cs:HashAndStore
  pub fn hash_and_store(&self, key: &[u8]) {
    self.hash_and_store_with_seed(key, 0);
  }

  /// HashAndStore 带种子内部实现（键置位；种子仅作 hash 实参，对位 C# ns 种子）
  pub(crate) fn hash_and_store_with_seed(&self, key: &[u8], seed: u64) {
    let (byte_offset, bit_offset) = self.slot_offset_with_seed(key, seed);
    let mut bm = self.bitmap.write();
    // SAFETY: self.size >= 8 且为 2 的幂，slot < self.size，byte_offset < (self.size >> 3) == bm.len()
    unsafe {
      *bm.get_unchecked_mut(byte_offset) |= 1 << bit_offset;
    }
  }

  /// 在 garnet 中的相对路径: libs/cluster/Server/Migration/Sketch.cs:Probe
  pub fn probe(&self, key: &[u8]) -> (bool, SketchStatus) {
    self.probe_with_seed(key, 0)
  }

  /// Probe 带种子内部实现（can_access_key 键门消费）
  pub(crate) fn probe_with_seed(&self, key: &[u8], seed: u64) -> (bool, SketchStatus) {
    let (byte_offset, bit_offset) = self.slot_offset_with_seed(key, seed);
    let exists = {
      let bm = self.bitmap.read();
      // SAFETY: self.size >= 8 且为 2 的幂，slot < self.size，byte_offset < (self.size >> 3) == bm.len()
      unsafe { (*bm.get_unchecked(byte_offset) & (1 << bit_offset)) != 0 }
    };
    let status = if exists {
      *self.status.read()
    } else {
      SketchStatus::Initializing
    };
    (exists, status)
  }
}

impl Default for Sketch {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn sketch_probe_follows_hash_and_store_and_status() {
    let sketch = Sketch::with_key_count(1024);

    // 未收录：probe 不命中，状态回落 Initializing
    let (exists, status) = sketch.probe(b"user:1001");
    assert!(!exists);
    assert_eq!(status, SketchStatus::Initializing);

    // 收录后置位命中
    sketch.hash_and_store(b"user:1001");
    let (exists, status) = sketch.probe(b"user:1001");
    assert!(exists);
    assert_eq!(status, SketchStatus::Initializing);

    // 状态推进经 probe 透出
    sketch.set_status(SketchStatus::Transmitting);
    let (exists, status) = sketch.probe(b"user:1001");
    assert!(exists);
    assert_eq!(status, SketchStatus::Transmitting);

    sketch.hash_and_store(b"user:1002");
    let (exists, status) = sketch.probe(b"user:1002");
    assert!(exists);
    assert_eq!(status, SketchStatus::Transmitting);

    // clear 复位 bitmap 与状态
    sketch.clear();
    let (exists, status) = sketch.probe(b"user:1001");
    assert!(!exists);
    assert_eq!(status, SketchStatus::Initializing);
    assert!(!sketch.probe(b"user:1002").0);
  }

  #[test]
  fn sketch_seed_kernel_buckets_are_seed_scoped() {
    let sketch = Sketch::with_key_count(1024);

    // 种子只在 crate 内作 hash 实参（C# 对外以 ns 语义暴露，无裸 seed 面）
    sketch.hash_and_store_with_seed(b"custom:seed:key", 42);
    assert!(sketch.probe_with_seed(b"custom:seed:key", 42).0);
    assert!(!sketch.probe_with_seed(b"custom:seed:key", 99).0);

    // seed=0 门面与带种子内核同轨
    sketch.hash_and_store(b"plain:key");
    assert!(sketch.probe_with_seed(b"plain:key", 0).0);
  }
}
