use std::str;

use crate::{
  error::{Error, Result},
  roaring_bitmap_object::RoaringBitmapObject,
};

pub struct RoaringBitmapCommands;

impl RoaringBitmapCommands {
  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:TryParseUInt32
  pub fn try_parse_uint32(raw: &[u8]) -> Option<u32> {
    str::from_utf8(raw).ok()?.parse().ok()
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:TryParseBit
  pub fn try_parse_bit(raw: &[u8]) -> Option<bool> {
    match raw {
      b"0" => Some(false),
      b"1" => Some(true),
      _ => None,
    }
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:Updater
  pub fn updater(rb: &mut RoaringBitmapObject, offset: u32, bit: bool) -> bool {
    rb.set_bit(offset, bit)
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:TryParseArgs
  pub fn try_parse_args(bit_raw: &[u8], from_raw: Option<&[u8]>) -> Result<(bool, u32)> {
    let bit = Self::try_parse_bit(bit_raw).ok_or(Error::InvalidBit)?;
    let from = from_raw.map_or(Ok(0), |r| {
      Self::try_parse_uint32(r).ok_or(Error::InvalidOffset)
    })?;
    Ok((bit, from))
  }
}
