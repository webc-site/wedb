use crate::write::*;

/// libs/common/RespMemoryWriter.cs:RespMemoryWriter
pub struct RespMemoryWriter<'a> {
  pub output: &'a mut Vec<u8>,
  pub resp3: bool,
}

impl<'a> RespMemoryWriter<'a> {
  pub fn new(output: &'a mut Vec<u8>, resp3: bool) -> Self {
    Self { output, resp3 }
  }

  #[inline(always)]
  fn write_with<F>(&mut self, max_len: usize, mut f: F)
  where
    F: FnMut(&mut &mut [u8]) -> bool,
  {
    loop {
      let old_len = self.output.len();
      if self.output.capacity() - old_len < max_len {
        let reserve_amount = std::cmp::max(self.output.capacity(), max_len);
        self.output.reserve(reserve_amount);
      }

      let cap = self.output.capacity();
      unsafe {
        self.output.set_len(cap);
      }

      let slice = &mut self.output[old_len..];
      let mut curr = slice;
      let success = f(&mut curr);
      let remaining = curr.len();

      unsafe {
        self.output.set_len(cap - remaining);
      }

      if success {
        break;
      }
      // Should not happen if max_len is accurate, but loop if it does
      let new_cap = self.output.capacity() * 2;
      self.output.reserve(new_cap - self.output.capacity());
    }
  }

  /// libs/common/RespMemoryWriter.cs:WriteAsciiBulkString
  #[inline]
  pub fn write_ascii_bulk_string(&mut self, chars: &str) {
    let max_len = get_bulk_string_length(chars.len() as i32) as usize;
    self.write_with(max_len, |curr| try_write_ascii_bulk_string(chars, curr));
  }

  /// libs/common/RespMemoryWriter.cs:WriteAsciiDirect
  #[inline]
  pub fn write_ascii_direct(&mut self, span: &str) {
    self.write_with(span.len(), |curr| try_write_ascii_direct(span, curr));
  }

  /// libs/common/RespMemoryWriter.cs:WriteArrayItem
  #[inline]
  pub fn write_array_item(&mut self, item: i64) {
    // approx max length for integer bulk string
    self.write_with(32, |curr| try_write_array_item(item, curr));
  }

  /// libs/common/RespMemoryWriter.cs:WriteArrayLength
  #[inline]
  pub fn write_array_length(&mut self, len: i32) {
    self.write_with(16, |curr| try_write_array_length(len, curr));
  }

  /// libs/common/RespMemoryWriter.cs:WriteBulkString
  #[inline]
  pub fn write_bulk_string(&mut self, item: &[u8]) {
    let max_len = get_bulk_string_length(item.len() as i32) as usize;
    self.write_with(max_len, |curr| try_write_bulk_string(item, curr));
  }

  /// libs/common/RespMemoryWriter.cs:WriteDirect
  #[inline]
  pub fn write_direct(&mut self, span: &[u8]) {
    self.write_with(span.len(), |curr| try_write_direct(span, curr));
  }

  /// libs/common/RespMemoryWriter.cs:WriteDoubleBulkString
  #[inline]
  pub fn write_double_bulk_string(&mut self, value: f64) {
    self.write_with(48, |curr| try_write_double_bulk_string(value, curr));
  }

  /// libs/common/RespMemoryWriter.cs:WriteDoubleNumeric
  #[inline]
  pub fn write_double_numeric(&mut self, value: f64) {
    if self.resp3 {
      self.write_with(48, |curr| try_write_double_numeric(value, curr));
    } else {
      self.write_with(48, |curr| try_write_double_bulk_string(value, curr));
    }
  }

  /// libs/common/RespMemoryWriter.cs:WriteEmptyArray
  #[inline]
  pub fn write_empty_array(&mut self) {
    self.write_with(4, try_write_empty_array);
  }

  /// libs/common/RespMemoryWriter.cs:WriteEmptyMap
  #[inline]
  pub fn write_empty_map(&mut self) {
    if self.resp3 {
      self.write_with(4, try_write_empty_map);
    } else {
      self.write_with(4, try_write_empty_array);
    }
  }

  /// libs/common/RespMemoryWriter.cs:WriteError
  #[inline]
  pub fn write_error(&mut self, error_string: &[u8]) {
    self.write_with(error_string.len() + 3, |curr| {
      try_write_error(error_string, curr)
    });
  }

  /// libs/common/RespMemoryWriter.cs:TryWriteFalse
  #[inline]
  pub fn try_write_false(&mut self) {
    if self.resp3 {
      self.write_with(4, try_write_false);
    } else {
      self.write_with(4, try_write_zero);
    }
  }

  /// libs/common/RespMemoryWriter.cs:WriteInt32
  #[inline]
  pub fn write_i32(&mut self, value: i32) {
    self.write_with(16, |curr| try_write_i32(value, curr));
  }

  /// libs/common/RespMemoryWriter.cs:WriteInt64AsBulkString
  #[inline]
  pub fn write_i64_as_bulk_string(&mut self, integer: i64) {
    self.write_with(32, |curr| try_write_i64_as_bulk_string(integer, curr));
  }

  /// libs/common/RespMemoryWriter.cs:WriteInt64
  #[inline]
  pub fn write_i64(&mut self, value: i64) {
    self.write_with(24, |curr| try_write_i64(value, curr));
  }

  /// libs/common/RespMemoryWriter.cs:WriteIntegerFromBytes
  #[inline]
  pub fn write_integer_from_bytes(&mut self, integer_bytes: &[u8]) {
    self.write_with(integer_bytes.len() + 3, |curr| {
      try_write_integer_from_bytes(integer_bytes, curr)
    });
  }

  /// libs/common/RespMemoryWriter.cs:WriteMapLength
  #[inline]
  pub fn write_map_length(&mut self, len: i32) {
    self.write_with(16, |curr| try_write_map_length(len, curr));
  }

  /// libs/common/RespMemoryWriter.cs:WriteNewLine
  #[inline]
  pub fn write_newline(&mut self) {
    self.write_with(2, try_write_new_line);
  }

  /// libs/common/RespMemoryWriter.cs:WriteNull
  #[inline]
  pub fn write_null(&mut self) {
    if self.resp3 {
      self.write_with(3, try_write_resp3_null);
    } else {
      self.write_with(5, try_write_null);
    }
  }

  /// libs/common/RespMemoryWriter.cs:WriteNullArray
  #[inline]
  pub fn write_null_array(&mut self) {
    self.write_with(5, try_write_null_array);
  }

  /// libs/common/RespMemoryWriter.cs:WritePushLength
  #[inline]
  pub fn write_push_length(&mut self, len: i32) {
    self.write_with(16, |curr| try_write_push_length(len, curr));
  }

  /// libs/common/RespMemoryWriter.cs:WriteSetLength
  #[inline]
  pub fn write_set_length(&mut self, len: i32) {
    self.write_with(16, |curr| try_write_set_length(len, curr));
  }

  /// libs/common/RespMemoryWriter.cs:WriteSimpleString
  #[inline]
  pub fn write_simple_string(&mut self, simple_string: &[u8]) {
    self.write_with(simple_string.len() + 3, |curr| {
      try_write_simple_string(simple_string, curr)
    });
  }

  /// libs/common/RespMemoryWriter.cs:WriteTrue
  #[inline]
  pub fn write_true(&mut self) {
    if self.resp3 {
      self.write_with(4, try_write_true);
    } else {
      self.write_with(4, try_write_one);
    }
  }

  /// libs/common/RespMemoryWriter.cs:WriteUtf8BulkString
  #[inline]
  pub fn write_utf8_bulk_string(&mut self, chars: &str) {
    self.write_bulk_string(chars.as_bytes());
  }

  /// libs/common/RespMemoryWriter.cs:WriteVerbatimString
  #[inline]
  pub fn write_verbatim_string(&mut self, str_bytes: &[u8], ext: &[u8]) {
    let actual_length = 3 + 1 + str_bytes.len();
    let max_len = get_bulk_string_length(actual_length as i32) as usize + 4; // roughly
    self.write_with(max_len, |curr| {
      try_write_verbatim_string(str_bytes, ext, curr)
    });
  }

  /// libs/common/RespMemoryWriter.cs:WriteZero
  #[inline]
  pub fn write_zero(&mut self) {
    self.write_with(4, try_write_zero);
  }

  /// libs/common/RespMemoryWriter.cs:WriteOne
  #[inline]
  pub fn write_one(&mut self) {
    self.write_with(4, try_write_one);
  }
  pub fn decrease_array_length(&mut self, new_count: i32, old_total_array_header_len: usize) {
    let mut header_buf = itoa::Buffer::new();
    let header_str = header_buf.format(new_count).as_bytes();
    let new_total_array_header_len = 1 + header_str.len() + 2;

    debug_assert!(old_total_array_header_len >= new_total_array_header_len);

    self.output[0] = b'*';
    self.output[1..1 + header_str.len()].copy_from_slice(header_str);
    self.output[1 + header_str.len()..1 + header_str.len() + 2].copy_from_slice(b"\r\n");

    if old_total_array_header_len != new_total_array_header_len {
      let diff = old_total_array_header_len - new_total_array_header_len;
      let len = self.output.len();
      self
        .output
        .copy_within(old_total_array_header_len..len, new_total_array_header_len);
      self.output.truncate(len - diff);
    }
  }

  /// libs/common/RespMemoryWriter.cs:AsReadOnlySpan
  pub fn as_read_only_span(&self) -> &[u8] {
    self.output.as_slice()
  }

  /// libs/common/RespMemoryWriter.cs:GetPosition
  pub fn get_position(&self) -> usize {
    self.output.len()
  }

  /// libs/common/RespMemoryWriter.cs:ResetPosition
  pub fn reset_position(&mut self) {
    self.output.clear();
  }
}
