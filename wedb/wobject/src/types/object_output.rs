//! 对象存储输出（对标 libs/server/Objects/Types/ObjectOutput.cs）
//!
//! 刻意差异（对照 C#）：C# 的 `SpanByteAndMemory` 直接在钉住缓冲上就地写 RESP，
//! Rust 侧统一为堆上 [`ObjectOutput::payload`]（`Vec<u8>`），由调用方取出转发；
//! `IGarnetObject GarnetObject` 引用字段不适用（Rust 以值返回对象，见 storage 层）。

use std::mem::take;

use bitflags::bitflags;

bitflags! {
  /// 存储输出标志（对标 libs/server/Objects/Types/ObjectOutput.cs:ObjectOutputFlags）
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
  pub struct ObjectOutputFlags: u8 {
    /// 无标志
    const NONE = 0;
    /// 移除键（对象为空时回收）
    const REMOVE_KEY = 1;
    /// 值类型不匹配
    const WRONG_TYPE = 1 << 1;
  }
}

/// 对象操作的结构化输出：计数字段 + RESP 负载
///
/// libs/server/Objects/Types/ObjectOutput.cs:ObjectOutput
#[derive(Debug, Clone, Default)]
pub struct ObjectOutput {
  /// 操作产出的 RESP 字节（对应 C# SpanByteAndMemory）
  pub payload: Vec<u8>,
  /// 操作结果计数（如成功添加的元素个数）
  pub result1: i64,
  /// 次级结果计数
  pub result2: i64,
  /// 输出标志
  pub output_flags: ObjectOutputFlags,
}

impl ObjectOutput {
  /// 空输出
  ///
  /// libs/server/Objects/Types/ObjectOutput.cs:ObjectOutput()
  #[inline]
  pub fn new() -> Self {
    Self::default()
  }

  /// 以既有缓冲构造（对标 C# FromPinnedPointer：复用调用方缓冲，避免额外分配；
  /// Rust 语义为接管缓冲所有权）
  #[inline]
  pub fn from_pinned_pointer(payload: Vec<u8>) -> Self {
    Self {
      payload,
      ..Self::default()
    }
  }

  /// 值类型不匹配
  ///
  /// libs/server/Objects/Types/ObjectOutput.cs:HasWrongType
  #[inline]
  pub fn has_wrong_type(&self) -> bool {
    self.output_flags.contains(ObjectOutputFlags::WRONG_TYPE)
  }

  /// 对象已空、须移除键
  ///
  /// libs/server/Objects/Types/ObjectOutput.cs:HasRemoveKey
  #[inline]
  pub fn has_remove_key(&self) -> bool {
    self.output_flags.contains(ObjectOutputFlags::REMOVE_KEY)
  }

  /// 取走 RESP 负载（对应 C# ProcessOutput(output.SpanByteAndMemory)）
  #[inline]
  pub fn take_payload(&mut self) -> Vec<u8> {
    take(&mut self.payload)
  }

  // ---- 以下统一代理至 wresp::RespWriter ----

  #[inline(always)]
  pub fn writer2(&mut self) -> wresp::RespWriter<&mut Vec<u8>, wresp::Resp2> {
    wresp::RespWriter::new_ref(&mut self.payload)
  }

  #[inline(always)]
  pub fn writer3(&mut self) -> wresp::RespWriter<&mut Vec<u8>, wresp::Resp3> {
    wresp::RespWriter::new_ref_p(&mut self.payload)
  }

  #[inline(always)]
  pub fn writer<P: wresp::RespProtocol>(&mut self) -> wresp::RespWriter<&mut Vec<u8>, P> {
    wresp::RespWriter::new_ref_p(&mut self.payload)
  }

  /// 错误行 `-<msg>\r\n`
  ///
  /// libs/common/RespMemoryWriter.cs:WriteError
  #[inline]
  pub fn write_error(&mut self, msg: &[u8]) {
    self.writer2().write_error_bytes(msg);
  }

  /// 整数回复 `:<v>\r\n`
  // C# libs/common/RespMemoryWriter.cs:WriteInt32/WriteInt64
  #[inline]
  pub fn write_int64(&mut self, value: i64) {
    self.writer2().write_int64(value);
  }

  /// 整数的 bulk string 形式
  // C# libs/common/RespMemoryWriter.cs:WriteInt64AsBulkString
  #[inline]
  pub fn write_int64_as_bulk_string(&mut self, value: i64) {
    self.writer2().write_int64_as_bulk_string(value);
  }

  /// bulk string `$<len>\r\n<bytes>\r\n`
  // C# libs/common/RespMemoryWriter.cs:WriteBulkString
  #[inline]
  pub fn write_bulk_string(&mut self, item: &[u8]) {
    self.writer2().write_bulk_string(item);
  }

  /// ASCII bulk string
  // C# libs/common/RespMemoryWriter.cs:WriteAsciiBulkString
  #[inline]
  pub fn write_ascii_bulk_string(&mut self, chars: &[u8]) {
    self.writer2().write_bulk_string(chars);
  }

  /// 数组头 `*<n>\r\n`
  // C# libs/common/RespMemoryWriter.cs:WriteArrayLength
  #[inline]
  pub fn write_array_length(&mut self, len: usize) {
    self.writer2().write_array_length(len);
  }

  /// 空数组 `*0\r\n`
  // C# libs/common/RespMemoryWriter.cs:WriteEmptyArray
  #[inline]
  pub fn write_empty_array(&mut self) {
    self.writer2().write_empty_array();
  }

  /// 泛型写 map 头
  #[inline(always)]
  pub fn write_map_len<P: wresp::RespProtocol>(&mut self, len: usize) {
    self.writer::<P>().write_map_len(len);
  }

  /// 依据协议版本写 map 头
  #[inline]
  pub fn write_map_length(&mut self, len: usize, resp_protocol_version: u8) {
    if resp_protocol_version >= 3 {
      self.write_map_len::<wresp::Resp3>(len);
    } else {
      self.write_map_len::<wresp::Resp2>(len);
    }
  }

  /// 泛型写 set 头
  #[inline(always)]
  pub fn write_set_len<P: wresp::RespProtocol>(&mut self, len: usize) {
    self.writer::<P>().write_set_len(len);
  }

  /// 依据协议版本写 set 头
  #[inline]
  pub fn write_set_length(&mut self, len: usize, resp_protocol_version: u8) {
    if resp_protocol_version >= 3 {
      self.write_set_len::<wresp::Resp3>(len);
    } else {
      self.write_set_len::<wresp::Resp2>(len);
    }
  }

  /// 泛型写 null
  #[inline(always)]
  pub fn write_null_p<P: wresp::RespProtocol>(&mut self) {
    self.writer::<P>().write_null();
  }

  /// null：RESP3 `_`，RESP2 `$-1`
  // C# libs/common/RespMemoryWriter.cs:WriteNull
  #[inline]
  pub fn write_null(&mut self, resp_protocol_version: u8) {
    if resp_protocol_version >= 3 {
      self.write_null_p::<wresp::Resp3>();
    } else {
      self.write_null_p::<wresp::Resp2>();
    }
  }

  /// 泛型写 null 数组
  #[inline(always)]
  pub fn write_null_array_p<P: wresp::RespProtocol>(&mut self) {
    self.writer::<P>().write_null_array();
  }

  /// null 数组：RESP3 `_`，RESP2 `*-1`
  // libs/common/RespMemoryWriter.cs:WriteNullArray
  #[inline]
  pub fn write_null_array(&mut self, resp_protocol_version: u8) {
    if resp_protocol_version >= 3 {
      self.write_null_array_p::<wresp::Resp3>();
    } else {
      self.write_null_array_p::<wresp::Resp2>();
    }
  }

  /// 整数作为数组项 `$<len>\r\n<int>\r\n`
  // libs/common/RespMemoryWriter.cs:WriteArrayItem
  #[inline]
  pub fn write_array_item(&mut self, item: i64) {
    self.writer2().write_array_item(item);
  }

  /// 双精度浮点 → Redis 文本（零堆分配，借用传入缓冲）
  #[inline]
  pub fn format_double_to(value: f64, buf: &mut zmij::Buffer) -> &str {
    wresp::format_double(value, buf)
  }

  /// 双精度浮点 → Redis 文本：NaN → "nan"，±∞ → "inf"/"-inf"，其余经 zmij 零堆分配格式化
  // （对标 RespWriteUtils.TryFormat 路径；严禁使用 format!("{value}")）
  #[inline]
  pub fn format_double(value: f64) -> String {
    let mut buf = zmij::Buffer::new();
    wresp::format_double(value, &mut buf).to_string()
  }

  /// bulk string 形式的双精度
  // libs/common/RespWriteUtils.cs:TryWriteDoubleBulkString
  #[inline]
  pub fn write_double_bulk_string(&mut self, value: f64) {
    self.writer2().write_double_bulk_string(value);
  }

  /// 泛型写数值形式的双精度
  #[inline(always)]
  pub fn write_double_numeric_p<P: wresp::RespProtocol>(&mut self, value: f64) {
    self.writer::<P>().write_double_numeric(value);
  }

  /// 数值形式的双精度：RESP3 写 `,<v>\r\n`，RESP2 退化为 bulk string
  // libs/common/RespWriteUtils.cs:TryWriteDoubleNumeric
  #[inline]
  pub fn write_double_numeric(&mut self, value: f64, resp_protocol_version: u8) {
    if resp_protocol_version >= 3 {
      self.write_double_numeric_p::<wresp::Resp3>(value);
    } else {
      self.write_double_numeric_p::<wresp::Resp2>(value);
    }
  }

  /// 写简单字符串 `+<msg>\r\n`
  #[inline]
  pub fn write_simple_string(&mut self, msg: &str) {
    self.writer2().write_simple_string(msg);
  }

  /// 泛型写布尔值
  #[inline(always)]
  pub fn write_bool_p<P: wresp::RespProtocol>(&mut self, value: bool) {
    self.writer::<P>().write_bool(value);
  }

  /// 写布尔值（RESP3 为 `#t`/`#f`，RESP2 为 `:1`/`:0`）
  #[inline]
  pub fn write_bool(&mut self, value: bool, resp_protocol_version: u8) {
    if resp_protocol_version >= 3 {
      self.write_bool_p::<wresp::Resp3>(value);
    } else {
      self.write_bool_p::<wresp::Resp2>(value);
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn writes_match_resp_wire_format() {
    let mut o = ObjectOutput::new();
    o.write_int64(-42);
    o.write_bulk_string(b"abc");
    o.write_array_length(2);
    o.write_empty_array();
    assert_eq!(o.payload, b":-42\r\n$3\r\nabc\r\n*2\r\n*0\r\n");
  }

  #[test]
  fn double_output_protocol_aware() {
    let mut o = ObjectOutput::new();
    o.write_double_numeric(1.5, 2);
    o.write_double_numeric(1.5, 3);
    o.write_double_bulk_string(f64::INFINITY);
    o.write_double_numeric(f64::NAN, 3);
    assert_eq!(o.payload, b"$3\r\n1.5\r\n,1.5\r\n$3\r\ninf\r\n,nan\r\n");
  }

  #[test]
  fn null_variants_by_protocol() {
    let mut o = ObjectOutput::new();
    o.write_null(2);
    o.write_null(3);
    o.write_null_array(2);
    o.write_null_array(3);
    assert_eq!(o.payload, b"$-1\r\n_\r\n*-1\r\n_\r\n");
  }

  #[test]
  fn flags_and_take() {
    let mut o = ObjectOutput::new();
    assert!(!o.has_wrong_type() && !o.has_remove_key());
    o.output_flags |= ObjectOutputFlags::REMOVE_KEY | ObjectOutputFlags::WRONG_TYPE;
    assert!(o.has_wrong_type() && o.has_remove_key());
    o.payload.extend_from_slice(b"x");
    assert_eq!(o.take_payload(), b"x");
    assert!(o.payload.is_empty());
  }

  #[test]
  fn int_helpers() {
    let mut o = ObjectOutput::new();
    o.write_int64_as_bulk_string(7);
    o.write_array_item(-9);
    assert_eq!(o.payload, b"$1\r\n7\r\n$2\r\n-9\r\n");
    let o2 = ObjectOutput::from_pinned_pointer(vec![1, 2]);
    assert_eq!(o2.payload.len(), 2);
  }
}
