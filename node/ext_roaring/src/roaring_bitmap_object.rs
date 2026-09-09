use std::io::{Read, Write};

use crate::roaring_bitmap::RoaringBitmapObj;

/// 表示 RoaringBitmap 对象相关的集合
pub struct RoaringBitmapObject {
  pub bitmap: RoaringBitmapObj,
}

impl RoaringBitmapObject {
  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:CloneObject
  pub fn clone_object(&self) -> Self {
    Self {
      bitmap: self.bitmap.clone(),
    }
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:SerializeObject
  pub fn serialize_object<W: Write>(&self, writer: &mut W) {
    self.bitmap.serialize(writer);
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:SetBit
  pub fn set_bit(&mut self, value: u32, set: bool) -> bool {
    let previous = self.bitmap.get_bit(value);
    if set {
      self.bitmap.set_bit(value);
    } else {
      self.bitmap.remove(value);
    }
    previous
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:GetBit
  pub fn get_bit(&self, value: u32) -> bool {
    self.bitmap.get_bit(value)
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:BitCount
  pub fn bit_count(&self) -> i64 {
    self.bitmap.bitmap.len() as i64
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:BitPos
  pub fn bit_pos(&self, bit: bool, from: u32) -> i64 {
    self.bitmap.bit_pos(bit, from)
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:Create
  pub fn create() -> Self {
    Self {
      bitmap: RoaringBitmapObj::new(),
    }
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:Deserialize
  pub fn deserialize<R: Read>(reader: &mut R) -> Self {
    Self {
      bitmap: RoaringBitmapObj::deserialize(reader),
    }
  }
}
