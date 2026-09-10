//! RESP 元数据应答写出器（对标 libs/common/RespMemoryWriter.cs 的
//! 元数据序列化面：map/set 长度按协议版本降级为 RESP2 数组）
//!
//! COMMAND 系应答的 C# 写出经 `RespMemoryWriter`（resp3 时 `%`/`~`，
//! RESP2 时数组化）；Rust 侧元数据域以本结构承接。

/// 元数据应答写出器（持有输出缓冲与协议版本标记）
pub struct RespMemoryWriter {
  /// 输出缓冲（C# dcurr 游标的托管等价）
  pub out: Vec<u8>,
  /// RESP3 及以上（C# resp3）
  pub(crate) resp3: bool,
}

impl RespMemoryWriter {
  /// C# libs/common/RespMemoryWriter.cs 构造（按协议版本）
  pub fn new(resp3: bool) -> Self {
    Self {
      out: Vec::with_capacity(256),
      resp3,
    }
  }

  /// C# libs/common/RespMemoryWriter.cs 的 WriteArrayLength
  #[inline]
  pub fn write_array_length(&mut self, len: usize) {
    self.out.extend_from_slice(format!("*{len}\r\n").as_bytes());
  }

  /// C# libs/common/RespMemoryWriter.cs 的 WriteMapLength（RESP2 降级为倍长数组）
  #[inline]
  pub fn write_map_length(&mut self, len: usize) {
    if self.resp3 {
      self.out.extend_from_slice(format!("%{len}\r\n").as_bytes());
    } else {
      self.write_array_length(len * 2);
    }
  }

  /// C# libs/common/RespMemoryWriter.cs 的 WriteSetLength（RESP2 降级为数组）
  #[inline]
  pub fn write_set_length(&mut self, len: usize) {
    if self.resp3 {
      self.out.extend_from_slice(format!("~{len}\r\n").as_bytes());
    } else {
      self.write_array_length(len);
    }
  }

  /// C# libs/common/RespMemoryWriter.cs 的 WriteBulkString
  #[inline]
  pub fn write_bulk_string(&mut self, item: &[u8]) {
    self
      .out
      .extend_from_slice(format!("${}\r\n", item.len()).as_bytes());
    self.out.extend_from_slice(item);
    self.out.extend_from_slice(b"\r\n");
  }

  /// C# libs/common/RespMemoryWriter.cs 的 WriteAsciiBulkString
  #[inline]
  pub fn write_ascii_bulk_string(&mut self, chars: &str) {
    self.write_bulk_string(chars.as_bytes());
  }

  /// C# libs/common/RespMemoryWriter.cs 的 WriteSimpleString
  #[inline]
  pub fn write_simple_string(&mut self, simple_string: &str) {
    self.out.push(b'+');
    self.out.extend_from_slice(simple_string.as_bytes());
    self.out.extend_from_slice(b"\r\n");
  }

  /// C# libs/common/RespMemoryWriter.cs 的 WriteInt32
  #[inline]
  pub fn write_int32(&mut self, value: i32) {
    self
      .out
      .extend_from_slice(format!(":{value}\r\n").as_bytes());
  }

  /// C# libs/common/RespMemoryWriter.cs 的 WriteNull（RESP2 `$-1` / RESP3 `_`）
  #[inline]
  pub fn write_null(&mut self) {
    if self.resp3 {
      self.out.extend_from_slice(b"_\r\n");
    } else {
      self.out.extend_from_slice(b"$-1\r\n");
    }
  }
}

#[cfg(test)]
mod tests {
  use super::RespMemoryWriter;

  #[test]
  fn protocol_aware_lengths() {
    let mut w = RespMemoryWriter::new(false);
    w.write_map_length(2);
    assert_eq!(w.out, b"*4\r\n".to_vec());
    w.out.clear();
    w.write_set_length(3);
    assert_eq!(w.out, b"*3\r\n".to_vec());

    let mut w = RespMemoryWriter::new(true);
    w.write_map_length(2);
    assert_eq!(w.out, b"%2\r\n".to_vec());
    w.out.clear();
    w.write_set_length(3);
    assert_eq!(w.out, b"~3\r\n".to_vec());
    w.out.clear();
    w.write_null();
    assert_eq!(w.out, b"_\r\n".to_vec());
  }

  #[test]
  fn strings_and_ints() {
    let mut w = RespMemoryWriter::new(false);
    w.write_bulk_string(b"abc");
    assert_eq!(w.out, b"$3\r\nabc\r\n".to_vec());
    w.out.clear();
    w.write_ascii_bulk_string("set");
    assert_eq!(w.out, b"$3\r\nset\r\n".to_vec());
    w.out.clear();
    w.write_simple_string("fast");
    assert_eq!(w.out, b"+fast\r\n".to_vec());
    w.out.clear();
    w.write_int32(-2);
    assert_eq!(w.out, b":-2\r\n".to_vec());
    w.out.clear();
    w.write_null();
    assert_eq!(w.out, b"$-1\r\n".to_vec());
  }
}
