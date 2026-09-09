#![allow(clippy::new_without_default)]
use roaring::RoaringBitmap;

/// 表示 RoaringBitmap 对象
#[derive(Clone)]
pub struct RoaringBitmapObj {
  pub bitmap: RoaringBitmap,
}

impl RoaringBitmapObj {
  pub fn new() -> Self {
    Self {
      bitmap: RoaringBitmap::new(),
    }
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmap.cs:Remove
  pub fn remove(&mut self, value: u32) -> bool {
    self.bitmap.remove(value)
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmap.cs:SetBit
  pub fn set_bit(&mut self, value: u32) -> bool {
    self.bitmap.insert(value)
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmap.cs:GetBit
  pub fn get_bit(&self, value: u32) -> bool {
    self.bitmap.contains(value)
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmap.cs:BitPos
  pub fn bit_pos(&self) -> i64 {
    unimplemented!()
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmap.cs:Enumerate
  pub fn enumerate(&self) -> impl Iterator<Item = u32> + '_ {
    self.bitmap.iter()
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmap.cs:GetEnumerator
  pub fn get_enumerator(&self) -> impl Iterator<Item = u32> + '_ {
    self.bitmap.iter()
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmap.cs:Deserialize
  pub fn deserialize(_reader: &[u8]) -> Self {
    unimplemented!()
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmap.cs:InsertChunk
  pub fn insert_chunk(&mut self) {
    unimplemented!()
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmap.cs:RemoveChunk
  pub fn remove_chunk(&mut self) {
    unimplemented!()
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmap.cs:EnsureChunkCapacity
  pub fn ensure_chunk_capacity(&mut self) {
    unimplemented!()
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmap.cs:GetChunkKind
  pub fn get_chunk_kind(&self) -> u8 {
    unimplemented!()
  }
}
