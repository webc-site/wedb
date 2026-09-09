use std::str;

pub trait RespSliceExt {
  fn as_str_safe(&self) -> &str;
  /// 严格解析：参数整体须为合法整数（对应 C# parseState.TryGetInt），失败返回 None
  fn try_parse_i64(&self) -> Option<i64>;
  /// 严格解析：参数整体须为合法浮点（对应 C# NumUtils.TryParse/double.Parse），失败返回 None
  fn try_parse_f64(&self) -> Option<f64>;
  fn parse_i64(&self, default: i64) -> i64;
  fn parse_usize(&self, default: usize) -> usize;
  fn parse_f64(&self, default: f64) -> f64;
  fn parse_isize(&self, default: isize) -> isize;
}

impl RespSliceExt for [u8] {
  #[inline]
  fn as_str_safe(&self) -> &str {
    str::from_utf8(self).unwrap_or("")
  }
  #[inline]
  fn try_parse_i64(&self) -> Option<i64> {
    str::from_utf8(self).ok()?.parse().ok()
  }
  #[inline]
  fn try_parse_f64(&self) -> Option<f64> {
    str::from_utf8(self).ok()?.parse().ok()
  }
  #[inline]
  fn parse_i64(&self, default: i64) -> i64 {
    str::from_utf8(self)
      .unwrap_or("")
      .parse()
      .unwrap_or(default)
  }
  #[inline]
  fn parse_usize(&self, default: usize) -> usize {
    str::from_utf8(self)
      .unwrap_or("")
      .parse()
      .unwrap_or(default)
  }
  #[inline]
  fn parse_f64(&self, default: f64) -> f64 {
    str::from_utf8(self)
      .unwrap_or("")
      .parse()
      .unwrap_or(default)
  }
  #[inline]
  fn parse_isize(&self, default: isize) -> isize {
    str::from_utf8(self)
      .unwrap_or("")
      .parse()
      .unwrap_or(default)
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
    assert_eq!(b"9223372036854775807".try_parse_i64(), Some(i64::MAX));
    // 非法：非数字/空串/带空白/溢出一律 None（对齐 C# TryGetInt 的严格语义）
    assert_eq!(b"abc".try_parse_i64(), None);
    assert_eq!(b"".try_parse_i64(), None);
    assert_eq!(b"1 2".try_parse_i64(), None);
    assert_eq!(b" 1".try_parse_i64(), None);
    assert_eq!(b"9223372036854775808".try_parse_i64(), None);
  }

  #[test]
  fn try_parse_f64_strict() {
    assert_eq!(b"1.5".try_parse_f64(), Some(1.5));
    assert_eq!(b"-0.5".try_parse_f64(), Some(-0.5));
    assert_eq!(b"3".try_parse_f64(), Some(3.0));
    assert_eq!(b"abc".try_parse_f64(), None);
    assert_eq!(b"".try_parse_f64(), None);
    assert_eq!(b"1 2".try_parse_f64(), None);
  }

  #[test]
  fn parse_i64_fallback_default() {
    assert_eq!(b"42".parse_i64(0), 42);
    assert_eq!(b"x".parse_i64(-1), -1);
  }
}
