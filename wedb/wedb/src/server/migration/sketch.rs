use parking_lot::RwLock;

use crate::server::migration::sketch_status::SketchStatus;

/// 默认 Sketch bitmap 槽位数（2^20 = 1,048,576 位，占 128KB）
const DEFAULT_KEY_COUNT: usize = 1 << 20;

/// libs/cluster/Server/Migration/Sketch.cs:Sketch
pub struct Sketch {
  size: usize,
  bitmap: RwLock<Vec<u8>>,
  pub status: RwLock<SketchStatus>,
  keys: RwLock<Vec<(Vec<u8>, bool)>>,
}

impl Sketch {
  /// libs/cluster/Server/Migration/Sketch.cs:Sketch
  pub fn new() -> Self {
    Self::with_key_count(DEFAULT_KEY_COUNT)
  }

  /// 指定 key 槽位上限构造 Sketch（必须是 2 的幂）
  pub fn with_key_count(key_count: usize) -> Self {
    debug_assert!(
      key_count > 0 && (key_count & (key_count - 1)) == 0,
      "Sketch size should be power of 2"
    );
    let size = key_count;
    let bitmap = vec![0u8; size >> 3];
    Self {
      size,
      bitmap: RwLock::new(bitmap),
      status: RwLock::new(SketchStatus::Initializing),
      keys: RwLock::new(Vec::new()),
    }
  }

  /// libs/cluster/Server/Migration/Sketch.cs:Keys
  pub fn keys(&self) -> Vec<(Vec<u8>, bool)> {
    self.keys.read().clone()
  }

  /// libs/cluster/Server/Migration/Sketch.cs:SetStatus
  pub fn set_status(&self, status: SketchStatus) {
    *self.status.write() = status;
  }

  /// libs/cluster/Server/Migration/Sketch.cs:Clear
  pub fn clear(&self) {
    let mut bm = self.bitmap.write();
    bm.fill(0);
    self.keys.write().clear();
    *self.status.write() = SketchStatus::Initializing;
  }

  /// libs/cluster/Server/Migration/Sketch.cs:TryHashAndStore
  pub fn try_hash_and_store(&self, key: &[u8]) -> bool {
    let slot = (gxhash::gxhash64(key, 0) as usize) & (self.size - 1);
    let byte_offset = slot >> 3;
    let bit_offset = slot & 7;
    let mut bm = self.bitmap.write();
    bm[byte_offset] |= 1 << bit_offset;
    true
  }

  /// libs/cluster/Server/Migration/Sketch.cs:HashAndStore
  pub fn hash_and_store(&self, key: &[u8]) {
    let slot = (gxhash::gxhash64(key, 0) as usize) & (self.size - 1);
    let byte_offset = slot >> 3;
    let bit_offset = slot & 7;
    let mut bm = self.bitmap.write();
    bm[byte_offset] |= 1 << bit_offset;
    self.keys.write().push((key.to_vec(), false));
  }

  /// libs/cluster/Server/Migration/Sketch.cs:Probe
  pub fn probe(&self, key: &[u8]) -> (bool, SketchStatus) {
    let slot = (gxhash::gxhash64(key, 0) as usize) & (self.size - 1);
    let byte_offset = slot >> 3;
    let bit_offset = slot & 7;
    let bm = self.bitmap.read();
    let exists = (bm[byte_offset] & (1 << bit_offset)) != 0;
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
