use std::str;

use super::session_parse_state::strict_i64;

pub trait RespSliceExt {
  fn as_str_safe(&self) -> &str;
  /// 严格解析：参数整体须为合法整数（对应 C# parseState.TryGetInt / TryGetLong，
  /// allowLeadingZeros: false —— 前导零、空白、尾随垃圾一律失败），失败返回 None
  fn try_parse_i64(&self) -> Option<i64>;
}

impl RespSliceExt for [u8] {
  #[inline]
  fn as_str_safe(&self) -> &str {
    str::from_utf8(self).unwrap_or("")
  }
  #[inline]
  fn try_parse_i64(&self) -> Option<i64> {
    strict_i64(self)
  }
}

pub trait RespVecExt {
  fn write_resp_int(&mut self, val: i64);
  fn write_resp_bulk_string(&mut self, val: &[u8]);
  fn write_resp_array_len(&mut self, len: usize);
  fn write_resp_error(&mut self, msg: &str);
  fn write_resp_simple_string(&mut self, msg: &str);
  fn write_resp_null(&mut self);
}

impl RespVecExt for Vec<u8> {
  #[inline]
  fn write_resp_int(&mut self, val: i64) {
    self.push(b':');
    let mut buffer = itoa::Buffer::new();
    self.extend_from_slice(buffer.format(val).as_bytes());
    self.extend_from_slice(b"\r\n");
  }
  #[inline]
  fn write_resp_bulk_string(&mut self, val: &[u8]) {
    self.push(b'$');
    let mut buffer = itoa::Buffer::new();
    self.extend_from_slice(buffer.format(val.len()).as_bytes());
    self.extend_from_slice(b"\r\n");
    self.extend_from_slice(val);
    self.extend_from_slice(b"\r\n");
  }
  #[inline]
  fn write_resp_array_len(&mut self, len: usize) {
    self.push(b'*');
    let mut buffer = itoa::Buffer::new();
    self.extend_from_slice(buffer.format(len).as_bytes());
    self.extend_from_slice(b"\r\n");
  }
  #[inline]
  fn write_resp_error(&mut self, msg: &str) {
    self.extend_from_slice(b"-ERR ");
    self.extend_from_slice(msg.as_bytes());
    self.extend_from_slice(b"\r\n");
  }
  #[inline]
  fn write_resp_simple_string(&mut self, msg: &str) {
    self.push(b'+');
    self.extend_from_slice(msg.as_bytes());
    self.extend_from_slice(b"\r\n");
  }
  #[inline]
  fn write_resp_null(&mut self) {
    self.extend_from_slice(b"$-1\r\n");
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn try_parse_i64_strict() {
    // 合法：带符号十进制整数
    assert_eq!(b"42".try_parse_i64(), Some(42));
    assert_eq!(b"-7".try_parse_i64(), Some(-7));
    assert_eq!(b"+3".try_parse_i64(), Some(3));
    assert_eq!(b"0".try_parse_i64(), Some(0));
    assert_eq!(b"-0".try_parse_i64(), Some(0));
    assert_eq!(b"9223372036854775807".try_parse_i64(), Some(i64::MAX));
    assert_eq!(b"-9223372036854775808".try_parse_i64(), Some(i64::MIN));
    // 非法：前导零 / 非数字 / 空串 / 带空白 / 溢出一律 None
    //（对齐 C# TryGetInt/TryGetLong 的 allowLeadingZeros: false 严格语义）
    assert_eq!(b"007".try_parse_i64(), None);
    assert_eq!(b"-007".try_parse_i64(), None);
    assert_eq!(b"abc".try_parse_i64(), None);
    assert_eq!(b"".try_parse_i64(), None);
    assert_eq!(b"1 2".try_parse_i64(), None);
    assert_eq!(b" 1".try_parse_i64(), None);
    assert_eq!(b"5 ".try_parse_i64(), None);
    assert_eq!(b"1x".try_parse_i64(), None);
    assert_eq!(b"9223372036854775808".try_parse_i64(), None);
    assert_eq!(b"-9223372036854775809".try_parse_i64(), None);
  }
}
