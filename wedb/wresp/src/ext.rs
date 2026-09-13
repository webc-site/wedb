use core::str;

/// 最大单行错误文案长度（防恶意巨幅文案攻击）
pub const MAX_ERROR_MSG_LEN: usize = 512;

// 过渡期兼容转发（实现单一落 wbase::num；存量调用方迁移完成后删除）
pub use wbase::num::{strict_i32, strict_i64};

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
    let boundary = truncated.floor_char_boundary(max_len);
    &truncated[..boundary]
  }
}

pub trait RespSliceExt {
  fn as_str_safe(&self) -> &str;
  /// 严格解析：参数整体须为合法整数（对应 C# parseState.TryGetInt / TryGetLong，
  /// allowLeadingZeros: false —— 前导零、空白、尾随垃圾一律失败），失败返回 None
  fn try_parse_i64(&self) -> Option<i64>;
  fn try_parse_i32(&self) -> Option<i32>;
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
  #[inline]
  fn try_parse_i32(&self) -> Option<i32> {
    strict_i32(self)
  }
}

pub trait RespVecExt {
  fn write_resp_int(&mut self, val: i64);
  fn write_resp_bulk_string(&mut self, val: &[u8]);
  fn write_resp_array_len(&mut self, len: usize);
  fn write_resp_error(&mut self, msg: &str);
  fn write_resp_simple_string(&mut self, msg: &str);
  fn write_resp_null(&mut self);
  fn resp_writer<P: crate::RespProtocol>(&mut self) -> crate::RespWriter<&mut Vec<u8>, P>;
  fn resp_writer2(&mut self) -> crate::RespWriter<&mut Vec<u8>, crate::Resp2>;
  fn resp_writer3(&mut self) -> crate::RespWriter<&mut Vec<u8>, crate::Resp3>;
}

impl RespVecExt for Vec<u8> {
  #[inline]
  fn write_resp_int(&mut self, val: i64) {
    crate::RespWriter::new_ref(self).write_int64(val);
  }
  #[inline]
  fn write_resp_bulk_string(&mut self, val: &[u8]) {
    crate::RespWriter::new_ref(self).write_bulk_string(val);
  }
  #[inline]
  fn write_resp_array_len(&mut self, len: usize) {
    crate::RespWriter::new_ref(self).write_array_length(len);
  }
  #[inline]
  fn write_resp_error(&mut self, msg: &str) {
    if let Some(rest) = msg.strip_prefix("ERR ") {
      crate::RespWriter::new_ref(self).write_error_with_prefix("ERR", rest);
    } else if msg == "ERR" {
      crate::RespWriter::new_ref(self).write_error("ERR");
    } else {
      crate::RespWriter::new_ref(self).write_error_with_prefix("ERR", msg);
    }
  }
  #[inline]
  fn write_resp_simple_string(&mut self, msg: &str) {
    crate::RespWriter::new_ref(self).write_simple_string(msg);
  }
  #[inline]
  fn write_resp_null(&mut self) {
    crate::RespWriter::new_ref(self).write_null();
  }
  #[inline]
  fn resp_writer<P: crate::RespProtocol>(&mut self) -> crate::RespWriter<&mut Vec<u8>, P> {
    crate::RespWriter::new_ref_p(self)
  }
  #[inline]
  fn resp_writer2(&mut self) -> crate::RespWriter<&mut Vec<u8>, crate::Resp2> {
    crate::RespWriter::new_ref(self)
  }
  #[inline]
  fn resp_writer3(&mut self) -> crate::RespWriter<&mut Vec<u8>, crate::Resp3> {
    crate::RespWriter::new_ref_p(self)
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

    let mut buf2 = Vec::new();
    buf2.write_resp_error("ERR already has prefix");
    assert_eq!(buf2, b"-ERR already has prefix\r\n");
  }

  #[test]
  fn try_parse_i32_strict() {
    assert_eq!(b"42".try_parse_i32(), Some(42));
    assert_eq!(b"-7".try_parse_i32(), Some(-7));
    assert_eq!(b"2147483647".try_parse_i32(), Some(i32::MAX));
    assert_eq!(b"-2147483648".try_parse_i32(), Some(i32::MIN));
    assert_eq!(b"2147483648".try_parse_i32(), None);
    assert_eq!(b"-2147483649".try_parse_i32(), None);
    assert_eq!(b"007".try_parse_i32(), None);
  }
}
