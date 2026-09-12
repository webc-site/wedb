//! RESP 服务器会话输出格式化（对标 libs/server/Resp/RespServerSessionOutput.cs）
//!
//! C# 会话经 RespWriteUtils 循环尝试直写 `dcurr/dend` 指针管道并在缓冲满时
//! `SendAndReset()`；Rust 侧托管会话以 `self.output` 动态缓冲承接，具备同等
//! RESP2/RESP3 协议语义与零拷贝写出路径。

use itoa::Buffer as IntBuf;
use wresp::RespVecExt;

use crate::{
  objects::types::object_output::ObjectOutput, resp::resp_server_session::RespServerSession,
};

impl RespServerSession {
  /// libs/server/Resp/RespServerSessionOutput.cs:ProcessOutput
  pub fn process_output(&mut self, output: &[u8]) {
    self.output.extend_from_slice(output);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteAsciiBulkString
  pub fn write_ascii_bulk_string(&mut self, message: &str) {
    self.write_bulk_string(message.as_bytes());
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteAsciiDirect
  pub fn write_ascii_direct(&mut self, message: &str) {
    self.output.extend_from_slice(message.as_bytes());
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteArrayLength
  pub fn write_array_length(&mut self, len: usize) {
    self.output.push(b'*');
    let mut buf = IntBuf::new();
    self.output.extend_from_slice(buf.format(len).as_bytes());
    self.output.extend_from_slice(b"\r\n");
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteBulkString
  pub fn write_bulk_string(&mut self, message: &[u8]) {
    self.output.write_resp_bulk_string(message);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteDirectLargeRespString
  pub fn write_direct_large_resp_string(&mut self, message: &[u8]) {
    self.output.write_resp_bulk_string(message);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteDirect
  pub fn write_direct(&mut self, span: &[u8]) {
    self.output.extend_from_slice(span);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteDoubleNumeric
  ///
  /// RESP3 写出 `,1.23\r\n`，RESP2 降级为 bulk string `$len\r\n1.23\r\n`；
  /// NaN/±∞ 走 [`ObjectOutput::format_double`] 的 `nan`/`inf` 口径
  /// （对标 C# TryWriteNaN_Numeric / TryWriteInfinity_Numeric，zmij 直写
  /// 会产出 `NaN`/`Infinity` 偏差）
  pub fn write_double_numeric(&mut self, value: f64) {
    let formatted = ObjectOutput::format_double(value);
    if self.resp_protocol_version >= 3 {
      self.output.push(b',');
      self.output.extend_from_slice(formatted.as_bytes());
      self.output.extend_from_slice(b"\r\n");
    } else {
      self.output.write_resp_bulk_string(formatted.as_bytes());
    }
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteEmptyArray
  pub fn write_empty_array(&mut self) {
    self.output.extend_from_slice(b"*0\r\n");
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteEmptySet
  ///
  /// RESP3 写出 `~0\r\n`，RESP2 降级为 `*0\r\n`
  pub fn write_empty_set(&mut self) {
    if self.resp_protocol_version >= 3 {
      self.output.extend_from_slice(b"~0\r\n");
    } else {
      self.output.extend_from_slice(b"*0\r\n");
    }
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteError
  pub fn write_error(&mut self, error_string: &str) {
    self.abort_error_message(error_string);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteInt32
  pub fn write_int32(&mut self, value: i32) {
    self.output.push(b':');
    let mut buf = IntBuf::new();
    self.output.extend_from_slice(buf.format(value).as_bytes());
    self.output.extend_from_slice(b"\r\n");
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteInt32AsBulkString
  pub fn write_int32_as_bulk_string(&mut self, value: i32) {
    let mut buf = IntBuf::new();
    let s = buf.format(value);
    self.output.write_resp_bulk_string(s.as_bytes());
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteInt64
  pub fn write_int64(&mut self, value: i64) {
    self.output.push(b':');
    let mut buf = IntBuf::new();
    self.output.extend_from_slice(buf.format(value).as_bytes());
    self.output.extend_from_slice(b"\r\n");
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteInt64AsBulkString
  pub fn write_int64_as_bulk_string(&mut self, value: i64) {
    let mut buf = IntBuf::new();
    let s = buf.format(value);
    self.output.write_resp_bulk_string(s.as_bytes());
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteIntegerFromBytes
  pub fn write_integer_from_bytes(&mut self, integer_bytes: &[u8]) {
    self.output.push(b':');
    self.output.extend_from_slice(integer_bytes);
    self.output.extend_from_slice(b"\r\n");
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteMapLength
  ///
  /// RESP3 写出 `%count\r\n`，RESP2 降级为倍长数组 `*(count*2)\r\n`
  pub fn write_map_length(&mut self, count: usize) {
    if self.resp_protocol_version >= 3 {
      self.output.push(b'%');
      let mut buf = IntBuf::new();
      self.output.extend_from_slice(buf.format(count).as_bytes());
      self.output.extend_from_slice(b"\r\n");
    } else {
      self.write_array_length(count * 2);
    }
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteZero
  pub fn write_zero(&mut self) {
    self.output.extend_from_slice(b":0\r\n");
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteOne
  pub fn write_one(&mut self) {
    self.output.extend_from_slice(b":1\r\n");
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteNull
  ///
  /// RESP3 写出 `_\r\n`，RESP2 写出 `$-1\r\n`
  pub fn write_null(&mut self) {
    if self.resp_protocol_version >= 3 {
      self.output.extend_from_slice(b"_\r\n");
    } else {
      self.output.write_resp_null();
    }
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteNullArray
  ///
  /// RESP3 写出 `_\r\n`，RESP2 写出 `*-1\r\n`
  pub fn write_null_array(&mut self) {
    if self.resp_protocol_version >= 3 {
      self.output.extend_from_slice(b"_\r\n");
    } else {
      self.output.extend_from_slice(b"*-1\r\n");
    }
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WritePushLength
  ///
  /// RESP3 写出 `>count\r\n`，RESP2 写出 `*count\r\n`
  pub fn write_push_length(&mut self, count: usize) {
    if self.resp_protocol_version >= 3 {
      self.output.push(b'>');
      let mut buf = IntBuf::new();
      self.output.extend_from_slice(buf.format(count).as_bytes());
      self.output.extend_from_slice(b"\r\n");
    } else {
      self.write_array_length(count);
    }
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteSetLength
  ///
  /// RESP3 写出 `~count\r\n`，RESP2 写出 `*count\r\n`
  pub fn write_set_length(&mut self, count: usize) {
    if self.resp_protocol_version >= 3 {
      self.output.push(b'~');
      let mut buf = IntBuf::new();
      self.output.extend_from_slice(buf.format(count).as_bytes());
      self.output.extend_from_slice(b"\r\n");
    } else {
      self.write_array_length(count);
    }
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteSimpleString
  pub fn write_simple_string(&mut self, simple_string: &str) {
    self.output.push(b'+');
    self.output.extend_from_slice(simple_string.as_bytes());
    self.output.extend_from_slice(b"\r\n");
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteUtf8BulkString
  pub fn write_utf8_bulk_string(&mut self, chars: &str) {
    self.write_bulk_string(chars.as_bytes());
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteLargeVerbatimString
  ///
  /// RESP3 写出 `={total_len}\r\n{fmt}:{msg}\r\n`，RESP2 降级为 bulk string
  pub fn write_large_verbatim_string(&mut self, message: &[u8], format: &[u8; 3]) {
    if self.resp_protocol_version >= 3 {
      self.output.push(b'=');
      let total_len = message.len() + 4; // format (3) + ':' (1)
      let mut buf = IntBuf::new();
      self
        .output
        .extend_from_slice(buf.format(total_len).as_bytes());
      self.output.extend_from_slice(b"\r\n");
      self.output.extend_from_slice(format);
      self.output.push(b':');
      self.output.extend_from_slice(message);
      self.output.extend_from_slice(b"\r\n");
    } else {
      self.write_direct_large_resp_string(message);
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_resp_server_session_output_resp2_vs_resp3() {
    let mut session = RespServerSession::default();
    session.update_resp_protocol_version(2);

    session.output.clear();
    session.write_null();
    assert_eq!(&session.output, b"$-1\r\n");

    session.output.clear();
    session.write_null_array();
    assert_eq!(&session.output, b"*-1\r\n");

    session.output.clear();
    session.write_map_length(2);
    assert_eq!(&session.output, b"*4\r\n");

    session.output.clear();
    session.write_set_length(3);
    assert_eq!(&session.output, b"*3\r\n");

    session.output.clear();
    session.write_push_length(3);
    assert_eq!(&session.output, b"*3\r\n");

    session.output.clear();
    session.write_double_numeric(1.25);
    assert_eq!(&session.output, b"$4\r\n1.25\r\n");

    session.output.clear();
    session.write_empty_set();
    assert_eq!(&session.output, b"*0\r\n");

    session.output.clear();
    session.write_large_verbatim_string(b"hello", b"txt");
    assert_eq!(&session.output, b"$5\r\nhello\r\n");

    // 切到 RESP3
    session.update_resp_protocol_version(3);

    session.output.clear();
    session.write_null();
    assert_eq!(&session.output, b"_\r\n");

    session.output.clear();
    session.write_null_array();
    assert_eq!(&session.output, b"_\r\n");

    session.output.clear();
    session.write_map_length(2);
    assert_eq!(&session.output, b"%2\r\n");

    session.output.clear();
    session.write_set_length(3);
    assert_eq!(&session.output, b"~3\r\n");

    session.output.clear();
    session.write_push_length(3);
    assert_eq!(&session.output, b">3\r\n");

    session.output.clear();
    session.write_double_numeric(1.25);
    assert_eq!(&session.output, b",1.25\r\n");

    // NaN/±∞ 采用 C# 的 nan/inf 口径（而非 zmij 直写的 NaN）
    session.output.clear();
    session.write_double_numeric(f64::NAN);
    assert_eq!(&session.output, b",nan\r\n");

    session.output.clear();
    session.write_double_numeric(f64::NEG_INFINITY);
    assert_eq!(&session.output, b",-inf\r\n");

    session.output.clear();
    session.write_empty_set();
    assert_eq!(&session.output, b"~0\r\n");

    session.output.clear();
    session.write_large_verbatim_string(b"hello", b"txt");
    assert_eq!(&session.output, b"=9\r\ntxt:hello\r\n");
  }
}
