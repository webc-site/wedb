pub trait RespSliceExt {
  fn parse_usize(&self, default: usize) -> usize;
  fn parse_isize(&self, default: isize) -> isize;
  fn parse_f64(&self, default: f64) -> f64;
  fn as_str_safe(&self) -> &str;
}

impl RespSliceExt for [u8] {
  fn parse_usize(&self, default: usize) -> usize {
    if let Ok(s) = std::str::from_utf8(self) {
      s.parse().unwrap_or(default)
    } else {
      default
    }
  }

  fn parse_isize(&self, default: isize) -> isize {
    if let Ok(s) = std::str::from_utf8(self) {
      s.parse().unwrap_or(default)
    } else {
      default
    }
  }

  fn parse_f64(&self, default: f64) -> f64 {
    if let Ok(s) = std::str::from_utf8(self) {
      s.parse().unwrap_or(default)
    } else {
      default
    }
  }

  fn as_str_safe(&self) -> &str {
    std::str::from_utf8(self).unwrap_or("")
  }
}

pub trait RespVecExt {
  fn write_resp_error(&mut self, msg: &str);
  fn write_resp_bulk_string(&mut self, msg: &[u8]);
  fn write_resp_simple_string(&mut self, msg: &str);
  fn write_resp_int(&mut self, val: i64);
  fn write_resp_null(&mut self);
}

impl RespVecExt for Vec<u8> {
  fn write_resp_error(&mut self, msg: &str) {
    self.extend_from_slice(b"-ERR ");
    self.extend_from_slice(msg.as_bytes());
    self.extend_from_slice(b"\r\n");
  }
  fn write_resp_bulk_string(&mut self, msg: &[u8]) {
    self.extend_from_slice(b"$");
    let mut buffer = itoa::Buffer::new();
    self.extend_from_slice(buffer.format(msg.len()).as_bytes());
    self.extend_from_slice(b"\r\n");
    self.extend_from_slice(msg);
    self.extend_from_slice(b"\r\n");
  }
  fn write_resp_simple_string(&mut self, msg: &str) {
    self.extend_from_slice(b"+");
    self.extend_from_slice(msg.as_bytes());
    self.extend_from_slice(b"\r\n");
  }
  fn write_resp_int(&mut self, val: i64) {
    self.extend_from_slice(b":");
    let mut buffer = itoa::Buffer::new();
    self.extend_from_slice(buffer.format(val).as_bytes());
    self.extend_from_slice(b"\r\n");
  }
  fn write_resp_null(&mut self) {
    self.extend_from_slice(b"$-1\r\n");
  }
}
