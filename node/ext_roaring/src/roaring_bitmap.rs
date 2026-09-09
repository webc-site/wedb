#![allow(clippy::new_without_default)]
use std::io::{Read, Write};

use roaring::RoaringBitmap;

use crate::error::Result;

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
  pub fn bit_pos(&self, bit: bool, from: u32) -> i64 {
    if bit {
      // Find first set bit >= from
      if let Some(pos) = self.bitmap.iter().find(|&x| x >= from) {
        return pos as i64;
      }
      -1
    } else {
      // Find first unset bit >= from
      let mut current = from;
      for set_bit in self.bitmap.iter().skip_while(|&x| x < from) {
        if set_bit > current {
          return current as i64;
        }
        if current == u32::MAX {
          return -1;
        }
        current += 1;
      }
      current as i64
    }
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmap.cs:Enumerate
  pub fn enumerate(&self) -> impl Iterator<Item = u32> + '_ {
    self.bitmap.iter()
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmap.cs:GetEnumerator
  pub fn get_enumerator(&self) -> impl Iterator<Item = u32> + '_ {
    self.bitmap.iter()
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmap.cs:Serialize
  ///
  /// I/O 与格式错误上抛，不以 unwrap panic 形式失败
  pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
    self.bitmap.serialize_into(writer)?;
    Ok(())
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmap.cs:Deserialize
  pub fn deserialize<R: Read>(reader: &mut R) -> Result<Self> {
    Ok(Self {
      bitmap: RoaringBitmap::deserialize_from(reader)?,
    })
  }
}
