pub struct RoaringBitmapCommands;

impl RoaringBitmapCommands {
  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:TryParseUInt32
  pub fn try_parse_uint32(raw: &[u8]) -> Option<u32> {
    std::str::from_utf8(raw).ok().and_then(|s| s.parse().ok())
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:TryParseBit
  pub fn try_parse_bit(raw: &[u8]) -> Option<bool> {
    if raw.len() == 1 && raw[0] == b'0' {
      return Some(false);
    }
    if raw.len() == 1 && raw[0] == b'1' {
      return Some(true);
    }
    None
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:Updater
  pub fn updater() {
    panic!("NotImplementedException")
  }

  /// garnet相对路径:modules/RoaringBitmap/RoaringBitmapCommands.cs:TryParseArgs
  pub fn try_parse_args() {
    panic!("NotImplementedException")
  }
}
