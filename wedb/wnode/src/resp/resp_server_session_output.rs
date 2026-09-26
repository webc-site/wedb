//! RESP 服务器会话输出格式化（对标 libs/server/Resp/RespServerSessionOutput.cs）
//!
//! 统一代理至 [`wresp::resp_memory_writer::RespWriter`] 泛型单态化写出机制，消除重复拼装与热路径协议分支。

use wresp::{
  ext::{RespVecExt, is_resp3},
  resp_memory_writer::{Resp2, Resp3, RespWriter},
};

use crate::resp::resp_server_session::RespServerSession;

/// 按会话协议版本分派写出：同方法名同实参，RESP3 走 writer3，否则 writer2
macro_rules! ver_out {
  ($s:expr, $m:ident $(, $a:expr)*) => {
    if is_resp3($s.resp_protocol_version) {
      $s.writer3().$m($($a),*);
    } else {
      $s.writer2().$m($($a),*);
    }
  };
}

impl RespServerSession {
  /// 获取默认 RESP2 协议写出器（零运行时分支）
  #[inline(always)]
  pub fn writer2(&mut self) -> RespWriter<&mut Vec<u8>, Resp2> {
    RespWriter::new_ref(&mut self.output)
  }

  /// 获取 RESP3 协议写出器（零运行时分支）
  #[inline(always)]
  pub fn writer3(&mut self) -> RespWriter<&mut Vec<u8>, Resp3> {
    RespWriter::new_ref_p(&mut self.output)
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteAsciiBulkString
  #[inline]
  pub fn write_ascii_bulk_string(&mut self, message: &str) {
    self.writer2().write_ascii_bulk_string(message);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteAsciiDirect
  #[inline]
  pub fn write_ascii_direct(&mut self, message: &str) {
    self.writer2().write_ascii_direct(message);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteArrayLength
  #[inline]
  pub fn write_array_length(&mut self, len: usize) {
    self.writer2().write_array_length(len);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteBulkString
  #[inline]
  pub fn write_bulk_string(&mut self, message: &[u8]) {
    self.writer2().write_bulk_string(message);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteDirect
  #[inline]
  pub fn write_direct(&mut self, span: &[u8]) {
    self.writer2().write_direct(span);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteDoubleNumeric
  #[inline]
  pub fn write_double_numeric(&mut self, value: f64) {
    ver_out!(self, write_double_numeric, value);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteEmptyArray
  #[inline]
  pub fn write_empty_array(&mut self) {
    self.writer2().write_empty_array();
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteEmptySet
  #[inline]
  pub fn write_empty_set(&mut self) {
    ver_out!(self, write_empty_set);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteError
  #[inline]
  pub fn write_error(&mut self, error_string: &str) {
    self.abort_error_message(error_string);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteInt32
  #[inline]
  pub fn write_int32(&mut self, value: i32) {
    self.writer2().write_int32(value);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteInt32AsBulkString
  #[inline]
  pub fn write_int32_as_bulk_string(&mut self, value: i32) {
    self.writer2().write_int32_as_bulk_string(value);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteInt64
  #[inline]
  pub fn write_int64(&mut self, value: i64) {
    self.writer2().write_int64(value);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteInt64AsBulkString
  #[inline]
  pub fn write_int64_as_bulk_string(&mut self, value: i64) {
    self.writer2().write_int64_as_bulk_string(value);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteIntegerFromBytes
  #[inline]
  pub fn write_integer_from_bytes(&mut self, integer_bytes: &[u8]) {
    self.writer2().write_integer_from_bytes(integer_bytes);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteMapLength
  #[inline]
  pub fn write_map_length(&mut self, count: usize) {
    ver_out!(self, write_map_length, count);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteZero
  #[inline]
  pub fn write_zero(&mut self) {
    self.writer2().write_zero();
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteOne
  #[inline]
  pub fn write_one(&mut self) {
    self.writer2().write_one();
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteNull
  ///（版本分派单源在 wresp::ext::RespVecExt::write_resp_null_ver）
  #[inline]
  pub fn write_null(&mut self) {
    self.output.write_resp_null_ver(self.resp_protocol_version);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteNullArray
  ///（版本分派单源在 wresp::ext::RespVecExt::write_resp_null_array_ver）
  #[inline]
  pub fn write_null_array(&mut self) {
    let resp_version = self.resp_protocol_version;
    self.output.write_resp_null_array_ver(resp_version);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WritePushLength
  #[inline]
  pub fn write_push_length(&mut self, count: usize) {
    ver_out!(self, write_push_length, count);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteSetLength
  #[inline]
  pub fn write_set_length(&mut self, count: usize) {
    ver_out!(self, write_set_length, count);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteSimpleString
  #[inline]
  pub fn write_simple_string(&mut self, simple_string: &str) {
    self.writer2().write_simple_string(simple_string);
  }

  /// libs/server/Resp/RespServerSessionOutput.cs:WriteLargeVerbatimString
  #[inline]
  pub fn write_large_verbatim_string(&mut self, message: &[u8], format: &[u8; 3]) {
    ver_out!(self, write_large_verbatim_string, message, format);
  }

  /// 强制写 RESP3 布尔值（`#t`/`#f`）
  #[inline]
  pub fn write_resp3_bool(&mut self, value: bool) {
    self.writer3().write_resp3_bool(value);
  }
}
