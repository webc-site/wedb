use core::str;

use wbase::num::strict_i64;

use crate::{Resp2, Resp3, RespProtocol, RespWriter};

/// Redis 规范错误前缀最小长度（如 "ERR" 长度为 3）
pub const MIN_ERROR_PREFIX_LEN: usize = 3;

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
    let boundary = truncated.floor_char_boundary(max_len);
    &truncated[..boundary]
  }
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
  fn resp_writer<P: RespProtocol>(&mut self) -> RespWriter<&mut Vec<u8>, P>;
  fn resp_writer2(&mut self) -> RespWriter<&mut Vec<u8>, Resp2>;
  fn resp_writer3(&mut self) -> RespWriter<&mut Vec<u8>, Resp3>;
}

impl RespVecExt for Vec<u8> {
  #[inline]
  fn write_resp_int(&mut self, val: i64) {
    RespWriter::new_ref(self).write_int64(val);
  }
  #[inline]
  fn write_resp_bulk_string(&mut self, val: &[u8]) {
    RespWriter::new_ref(self).write_bulk_string(val);
  }
  #[inline]
  fn write_resp_array_len(&mut self, len: usize) {
    RespWriter::new_ref(self).write_array_length(len);
  }
  #[inline]
  fn write_resp_error(&mut self, msg: &str) {
    let mut writer = RespWriter::new_ref(self);
    if let Some((prefix, _)) = msg.split_once(' ')
      && prefix.len() >= MIN_ERROR_PREFIX_LEN
      && prefix.bytes().all(|b| b.is_ascii_uppercase())
    {
      writer.write_error(msg);
    } else if msg == "ERR" {
      writer.write_error("ERR");
    } else {
      writer.write_error_with_prefix("ERR", msg);
    }
  }
  #[inline]
  fn write_resp_simple_string(&mut self, msg: &str) {
    RespWriter::new_ref(self).write_simple_string(msg);
  }
  #[inline]
  fn write_resp_null(&mut self) {
    RespWriter::new_ref(self).write_null();
  }
  #[inline]
  fn resp_writer<P: RespProtocol>(&mut self) -> RespWriter<&mut Vec<u8>, P> {
    RespWriter::new_ref_p(self)
  }
  #[inline]
  fn resp_writer2(&mut self) -> RespWriter<&mut Vec<u8>, Resp2> {
    RespWriter::new_ref(self)
  }
  #[inline]
  fn resp_writer3(&mut self) -> RespWriter<&mut Vec<u8>, Resp3> {
    RespWriter::new_ref_p(self)
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

    let mut buf3 = Vec::new();
    buf3.write_resp_error("WRONGTYPE Operation against a key holding the wrong kind of value.");
    assert_eq!(
      buf3,
      b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n"
    );
  }
}
