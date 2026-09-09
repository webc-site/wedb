use std::str;

pub trait RespSliceExt {
  fn as_str_safe(&self) -> &str;
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
}
