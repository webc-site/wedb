use core::str;

/// 最大单行错误文案长度（防恶意巨幅文案攻击）
pub const MAX_ERROR_MSG_LEN: usize = 512;

/// 净化错误文案：以 `\r` 或 `\n` 截断防止 RESP 协议帧注入，并截断至最大长度（确保 UTF-8 字符边界）
#[inline]
pub fn sanitize_error_str(s: &str, max_len: usize) -> &str {
  let truncated = match s.find(['\r', '\n']) {
    Some(idx) => &s[..idx],
    None => s,
  };
  if truncated.len() <= max_len {
    truncated
  } else {
    let mut boundary = max_len;
    while boundary > 0 && !truncated.is_char_boundary(boundary) {
      boundary -= 1;
    }
    &truncated[..boundary]
  }
}

/// C# 严格整数解析（对照 RespReadUtils.TryReadInt64Safe allowLeadingZeros: false 语义）：
/// 可选 +/- 号；首数字 '0' 且后续仍有数字即拒绝（"0"/"-0"
/// 合法，"007" 非法）；须为纯数字且整体消费；负值域至 i64::MIN；
/// 溢出返回 None
#[inline]
pub fn strict_i64(raw: &[u8]) -> Option<i64> {
  let (digits, negative) = match raw {
    [b'+', rest @ ..] => (rest, false),
    [b'-', rest @ ..] => (rest, true),
    rest => (rest, false),
  };
  if digits.is_empty() || (digits.len() > 1 && digits[0] == b'0') {
    return None;
  }
  let mut number: u64 = 0;
  for &d in digits {
    if !d.is_ascii_digit() {
      return None;
    }
    number = number.checked_mul(10)?.checked_add(u64::from(d - b'0'))?;
  }
  if negative {
    if number > i64::MAX as u64 + 1 {
      return None;
    }
    if number == i64::MAX as u64 + 1 {
      return Some(i64::MIN);
    }
    Some(-(number as i64))
  } else if number <= i64::MAX as u64 {
    Some(number as i64)
  } else {
    None
  }
}

/// 同 [`strict_i64`] 的 i32 值域版（C# int.MaxValue 上限语义）
#[inline]
pub fn strict_i32(raw: &[u8]) -> Option<i32> {
  i32::try_from(strict_i64(raw)?).ok()
}

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
    let clean = sanitize_error_str(msg, MAX_ERROR_MSG_LEN);
    self.extend_from_slice(b"-ERR ");
    self.extend_from_slice(clean.as_bytes());
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
    assert_eq!(b"42".try_parse_i64(), Some(42));
    assert_eq!(b"-7".try_parse_i64(), Some(-7));
    assert_eq!(b"+3".try_parse_i64(), Some(3));
    assert_eq!(b"0".try_parse_i64(), Some(0));
    assert_eq!(b"-0".try_parse_i64(), Some(0));
    assert_eq!(b"9223372036854775807".try_parse_i64(), Some(i64::MAX));
    assert_eq!(b"-9223372036854775808".try_parse_i64(), Some(i64::MIN));
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

  #[test]
  fn vec_ext_formatting() {
    let mut buf = Vec::new();
    buf.write_resp_int(100);
    buf.write_resp_simple_string("OK");
    buf.write_resp_bulk_string(b"foo");
    buf.write_resp_array_len(2);
    buf.write_resp_null();
    buf.write_resp_error("something failed");
    assert_eq!(
      buf,
      b":100\r\n+OK\r\n$3\r\nfoo\r\n*2\r\n$-1\r\n-ERR something failed\r\n"
    );
  }
}
