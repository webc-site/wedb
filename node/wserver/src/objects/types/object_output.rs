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
  #[inline]
  pub fn has_wrong_type(&self) -> bool {
    self.output_flags.contains(ObjectOutputFlags::WRONG_TYPE)
  }

  /// 对象已空、须移除键
  #[inline]
  pub fn has_remove_key(&self) -> bool {
    self.output_flags.contains(ObjectOutputFlags::REMOVE_KEY)
  }

  /// 取走 RESP 负载（对应 C# ProcessOutput(output.SpanByteAndMemory)）
  #[inline]
  pub fn take_payload(&mut self) -> Vec<u8> {
    take(&mut self.payload)
  }

  // ---- 以下对标 Garnet.common RespMemoryWriter 的写入方法 ----

  /// 错误行 `-<msg>\r\n`（RespMemoryWriter.WriteError）
  #[inline]
  pub fn write_error(&mut self, msg: &[u8]) {
    self.payload.push(b'-');
    self.payload.extend_from_slice(msg);
    self.payload.extend_from_slice(b"\r\n");
  }

  /// 整数回复 `:<v>\r\n`（RespMemoryWriter.WriteInt32/WriteInt64）
  #[inline]
  pub fn write_int64(&mut self, value: i64) {
    self.payload.push(b':');
    let mut buf = itoa::Buffer::new();
    self.payload.extend_from_slice(buf.format(value).as_bytes());
    self.payload.extend_from_slice(b"\r\n");
  }

  /// 整数的 bulk string 形式（RespMemoryWriter.WriteInt64AsBulkString）
  #[inline]
  pub fn write_int64_as_bulk_string(&mut self, value: i64) {
    let mut buf = itoa::Buffer::new();
    self.write_bulk_string(buf.format(value).as_bytes());
  }

  /// bulk string `$<len>\r\n<bytes>\r\n`（RespMemoryWriter.WriteBulkString）
  #[inline]
  pub fn write_bulk_string(&mut self, item: &[u8]) {
    self.payload.push(b'$');
    let mut buf = itoa::Buffer::new();
    self
      .payload
      .extend_from_slice(buf.format(item.len()).as_bytes());
    self.payload.extend_from_slice(b"\r\n");
    self.payload.extend_from_slice(item);
    self.payload.extend_from_slice(b"\r\n");
  }

  /// ASCII bulk string（RespMemoryWriter.WriteAsciiBulkString）
  #[inline]
  pub fn write_ascii_bulk_string(&mut self, chars: &[u8]) {
    self.write_bulk_string(chars);
  }

  /// 数组头 `*<n>\r\n`（RespMemoryWriter.WriteArrayLength）
  #[inline]
  pub fn write_array_length(&mut self, len: usize) {
    self.payload.push(b'*');
    let mut buf = itoa::Buffer::new();
    self.payload.extend_from_slice(buf.format(len).as_bytes());
    self.payload.extend_from_slice(b"\r\n");
  }

  /// 空数组 `*0\r\n`（RespMemoryWriter.WriteEmptyArray）
  #[inline]
  pub fn write_empty_array(&mut self) {
    self.payload.extend_from_slice(b"*0\r\n");
  }

  /// null：RESP3 `_`，RESP2 `$-1`（RespMemoryWriter.WriteNull）
  #[inline]
  pub fn write_null(&mut self, resp_protocol_version: u8) {
    if resp_protocol_version >= 3 {
      self.payload.extend_from_slice(b"_\r\n");
    } else {
      self.payload.extend_from_slice(b"$-1\r\n");
    }
  }

  /// null 数组：RESP3 `_`，RESP2 `*-1`（RespMemoryWriter.WriteNullArray）
  #[inline]
  pub fn write_null_array(&mut self, resp_protocol_version: u8) {
    if resp_protocol_version >= 3 {
      self.payload.extend_from_slice(b"_\r\n");
    } else {
      self.payload.extend_from_slice(b"*-1\r\n");
    }
  }

  /// 整数作为数组项 `$<len>\r\n<int>\r\n`（RespMemoryWriter.WriteArrayItem）
  #[inline]
  pub fn write_array_item(&mut self, item: i64) {
    let mut buf = itoa::Buffer::new();
    self.write_bulk_string(buf.format(item).as_bytes());
  }

  /// 双精度浮点：NaN → "nan"，±∞ → "inf"/"-inf"，其余走最短往返表示
  /// （对标 RespWriteUtils.TryFormat 路径；指数记法大小写差异见文件头说明）
  fn format_double(value: f64) -> String {
    if value.is_nan() {
      "nan".to_string()
    } else if value.is_infinite() {
      if value > 0.0 { "inf" } else { "-inf" }.to_string()
    } else {
      format!("{value}")
    }
  }

  /// bulk string 形式的双精度（RespMemoryWriter.WriteDoubleBulkString）
  #[inline]
  pub fn write_double_bulk_string(&mut self, value: f64) {
    let s = Self::format_double(value);
    self.write_bulk_string(s.as_bytes());
  }

  /// 数值形式的双精度：RESP3 写 `,<v>\r\n`，RESP2 退化为 bulk string
  /// （RespMemoryWriter.WriteDoubleNumeric）
  #[inline]
  pub fn write_double_numeric(&mut self, value: f64, resp_protocol_version: u8) {
    if resp_protocol_version >= 3 {
      let s = Self::format_double(value);
      self.payload.push(b',');
      self.payload.extend_from_slice(s.as_bytes());
      self.payload.extend_from_slice(b"\r\n");
    } else {
      self.write_double_bulk_string(value);
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
