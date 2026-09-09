use std::io::{Read, Write};

use crate::{error::Result, roaring_bitmap::RoaringBitmapObj};

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
  pub fn serialize_object<W: Write>(&self, writer: &mut W) -> Result<()> {
    self.bitmap.serialize(writer)
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapObject.cs:SetBit
  ///
  /// insert/remove 的返回值即旧值语义（insert：true=原先不存在；remove：true=原先存在），
  /// 单次查找完成读取旧值 + 置位/清除，免除 get+set 两次树下降
  pub fn set_bit(&mut self, value: u32, set: bool) -> bool {
    if set {
      !self.bitmap.set_bit(value)
    } else {
      self.bitmap.remove(value)
    }
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
  pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
    Ok(Self {
      bitmap: RoaringBitmapObj::deserialize(reader)?,
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn serialize_round_trip() {
    let mut obj = RoaringBitmapObject::create();
    obj.set_bit(1, true);
    obj.set_bit(1000, true);

    let mut buf = Vec::new();
    obj.serialize_object(&mut buf).unwrap();
    let back = RoaringBitmapObject::deserialize(&mut &buf[..]).unwrap();
    assert_eq!(back.bit_count(), 2);
    assert!(back.get_bit(1) && back.get_bit(1000));
  }

  #[test]
  fn corrupt_stream_is_error_not_panic() {
    assert!(RoaringBitmapObject::deserialize(&mut &b"garbage"[..]).is_err());
  }

  #[test]
  fn bit_pos_boundaries() {
    let mut obj = RoaringBitmapObject::create();
    obj.set_bit(5, true);
    // 已置位：命中自身
    assert_eq!(obj.bit_pos(true, 5), 5);
    // 第一个 >= 6 的置位位不存在
    assert_eq!(obj.bit_pos(true, 6), -1);
    // 0..5 均未置位，首个未置位即 0
    assert_eq!(obj.bit_pos(false, 0), 0);
    // 5 已置位，首个未置位是 6
    assert_eq!(obj.bit_pos(false, 5), 6);
  }
}
