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
  pub fn with_key_count(key_count: usize) -> Self {
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

  /// 计算 key 对应 bitmap 的字节偏移与位偏移（编译期/纯位运算优化）
  #[inline]
  pub const fn slot_offset_from_hash(hash: u64, size_mask: usize) -> (usize, u8) {
    let slot = (hash as usize) & size_mask;
    (slot >> 3, (slot & 7) as u8)
  }

  /// 计算 key 对应 bitmap 的字节偏移与位偏移
  #[inline]
  fn slot_offset(&self, key: &[u8]) -> (usize, u8) {
    Self::slot_offset_from_hash(whasher::fast_hash(key), self.size - 1)
  }

  /// 在 garnet 中的相对路径: libs/cluster/Server/Migration/Sketch.cs:HashAndStore
  pub fn hash_and_store(&self, key: &[u8]) {
    let (byte_offset, bit_offset) = self.slot_offset(key);
    let mut bm = self.bitmap.write();
    // SAFETY: self.size >= 8 且为 2 的幂，slot < self.size，byte_offset < (self.size >> 3) == bm.len()
    unsafe {
      *bm.get_unchecked_mut(byte_offset) |= 1 << bit_offset;
    }
  }

  /// 在 garnet 中的相对路径: libs/cluster/Server/Migration/Sketch.cs:Probe
  /// （can_access_key 键门消费）
  pub fn probe(&self, key: &[u8]) -> (bool, SketchStatus) {
    let (byte_offset, bit_offset) = self.slot_offset(key);
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
