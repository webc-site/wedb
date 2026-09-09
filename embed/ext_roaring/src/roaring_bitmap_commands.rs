use crate::roaring_bitmap_object::RoaringBitmapObject;
use crate::error::Result;
use crate::error::Error;

pub struct RoaringBitmapCommands;

impl RoaringBitmapCommands {
  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:TryParseUInt32
  pub fn try_parse_uint32(raw: &[u8]) -> Option<u32> {
    std::str::from_utf8(raw).ok().and_then(|s| s.parse().ok())
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:TryParseBit
  pub fn try_parse_bit(raw: &[u8]) -> Option<bool> {
    if raw.len() == 1 && raw[0] == b"0"[0] {
      return Some(false);
    }
    if raw.len() == 1 && raw[0] == b"1"[0] {
      return Some(true);
    }
    None
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:Updater
  pub fn updater(rb: &mut RoaringBitmapObject, offset: u32, bit: bool) -> bool {
    rb.set_bit(offset, bit)
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:TryParseArgs
  pub fn try_parse_args(bit_raw: &[u8], from_raw: Option<&[u8]>) -> Result<(bool, u32)> {
    let bit = Self::try_parse_bit(bit_raw).ok_or(Error::InvalidBit)?;
    let from = from_raw.map_or(Ok(0), |r| Self::try_parse_uint32(r).ok_or(Error::InvalidOffset))?;
    Ok((bit, from))
  }
}
