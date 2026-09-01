use std::io::{self, ErrorKind, Write};
use std::str::from_utf8;

use bytes::{Buf, BytesMut};

/// RESP3 协议数据类型枚举（纯 RESP3 规范实现）
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
  /// 简单字符串 (+OK\r\n)
  Simple(String),
  /// 错误信息 (-ERR ...\r\n)
  Error(String),
  /// 整数 (:100\r\n)
  Int(i64),
  /// 浮点数 (,3.14\r\n)
  Float(f64),
  /// 布尔值 (#t\r\n / #f\r\n)
  Bool(bool),
  /// 二进制安全字符串 ($6\r\nfoobar\r\n)
  Blob(Vec<u8>),
  /// 空值 (_\r\n)
  Null,
  /// 数组 (*2\r\n...)
  Arr(Vec<RespValue>),
  /// 字典映射 (%2\r\n...)
  Map(Vec<(RespValue, RespValue)>),
  /// 集合 (~2\r\n...)
  Set(Vec<RespValue>),
  /// 推送消息 (>2\r\n...)
  Push(Vec<RespValue>),
}

/// 借用型零拷贝 RESP 视图
#[derive(Debug, Clone, PartialEq)]
pub enum RespBorrow<'a> {
  Simple(&'a str),
  Error(&'a str),
  Int(i64),
  Float(f64),
  Bool(bool),
  Blob(&'a [u8]),
  Null,
  Arr(Vec<RespBorrow<'a>>),
  Map(Vec<(RespBorrow<'a>, RespBorrow<'a>)>),
  Set(Vec<RespBorrow<'a>>),
  Push(Vec<RespBorrow<'a>>),
}

impl RespBorrow<'_> {
  /// 转换为拥有所有权的 RespValue
  pub fn into_owned(self) -> RespValue {
    match self {
      Self::Simple(s) => RespValue::Simple(s.to_string()),
      Self::Error(s) => RespValue::Error(s.to_string()),
      Self::Int(i) => RespValue::Int(i),
      Self::Float(f) => RespValue::Float(f),
      Self::Bool(b) => RespValue::Bool(b),
      Self::Blob(b) => RespValue::Blob(b.to_vec()),
      Self::Null => RespValue::Null,
      Self::Arr(v) => RespValue::Arr(v.into_iter().map(RespBorrow::into_owned).collect()),
      Self::Map(v) => RespValue::Map(
        v.into_iter()
          .map(|(k, v)| (k.into_owned(), v.into_owned()))
          .collect(),
      ),
      Self::Set(v) => RespValue::Set(v.into_iter().map(RespBorrow::into_owned).collect()),
      Self::Push(v) => RespValue::Push(v.into_iter().map(RespBorrow::into_owned).collect()),
    }
  }

  /// 转换为拥有所有权的 RespValue（克隆借用内容）
  pub fn to_owned_resp(&self) -> RespValue {
    self.clone().into_owned()
  }

  #[inline]
  pub fn as_str(&self) -> Option<&str> {
    match self {
      Self::Simple(s) | Self::Error(s) => Some(s),
      Self::Blob(b) => from_utf8(b).ok(),
      _ => None,
    }
  }

  #[inline]
  pub fn as_bytes(&self) -> Option<&[u8]> {
    match self {
      Self::Blob(b) => Some(b),
      Self::Simple(s) | Self::Error(s) => Some(s.as_bytes()),
      _ => None,
    }
  }
}

impl<'a> From<RespBorrow<'a>> for RespValue {
  #[inline]
  fn from(borrow: RespBorrow<'a>) -> Self {
    borrow.into_owned()
  }
}

#[inline(always)]
const fn digits_count(n: usize) -> usize {
  if n < 10 {
    1
  } else if n < 100 {
    2
  } else if n < 1000 {
    3
  } else if n < 10000 {
    4
  } else if n < 100000 {
    5
  } else if n < 1000000 {
    6
  } else if n < 10000000 {
    7
  } else if n < 100000000 {
    8
  } else if n < 1000000000 {
    9
  } else {
    (n.ilog10() + 1) as usize
  }
}

#[inline(always)]
const fn digits_len_i64(n: i64) -> usize {
  let abs = n.unsigned_abs() as usize;
  let digits = digits_count(abs);
  if n < 0 { digits + 1 } else { digits }
}

impl RespValue {
  #[inline]
  pub fn ok() -> Self {
    Self::Simple("OK".to_string())
  }

  #[inline]
  pub fn queued() -> Self {
    Self::Simple("QUEUED".to_string())
  }

  #[inline]
  pub fn pong() -> Self {
    Self::Simple("PONG".to_string())
  }

  #[inline]
  pub fn null() -> Self {
    Self::Null
  }

  #[inline]
  pub fn simple(s: impl Into<String>) -> Self {
    Self::Simple(s.into())
  }

  #[inline]
  pub fn error(msg: impl Into<String>) -> Self {
    Self::Error(msg.into())
  }

  #[inline]
  pub fn bool(b: bool) -> Self {
    Self::Bool(b)
  }

  #[inline]
  pub fn int(i: i64) -> Self {
    Self::Int(i)
  }

  #[inline]
  pub fn float(f: f64) -> Self {
    Self::Float(f)
  }

  #[inline]
  pub fn blob(b: impl Into<Vec<u8>>) -> Self {
    Self::Blob(b.into())
  }

  #[inline]
  pub fn arr(elements: Vec<RespValue>) -> Self {
    Self::Arr(elements)
  }

  #[inline]
  pub fn map(pairs: Vec<(RespValue, RespValue)>) -> Self {
    Self::Map(pairs)
  }

  #[inline]
  pub fn set(elements: Vec<RespValue>) -> Self {
    Self::Set(elements)
  }

  #[inline]
  pub fn push(elements: Vec<RespValue>) -> Self {
    Self::Push(elements)
  }

  #[inline]
  pub fn is_null(&self) -> bool {
    matches!(self, Self::Null)
  }

  #[inline]
  pub fn is_ok(&self) -> bool {
    matches!(self, Self::Simple(s) if s == "OK")
  }

  #[inline]
  pub fn is_error(&self) -> bool {
    matches!(self, Self::Error(_))
  }

  #[inline]
  pub fn as_str(&self) -> Option<&str> {
    match self {
      Self::Simple(s) | Self::Error(s) => Some(s.as_str()),
      Self::Blob(b) => from_utf8(b).ok(),
      _ => None,
    }
  }

  #[inline]
  pub fn as_bytes(&self) -> Option<&[u8]> {
    match self {
      Self::Blob(b) => Some(b.as_slice()),
      Self::Simple(s) | Self::Error(s) => Some(s.as_bytes()),
      _ => None,
    }
  }

  #[inline]
  pub fn as_i64(&self) -> Option<i64> {
    match self {
      Self::Int(i) => Some(*i),
      _ => None,
    }
  }

  #[inline]
  pub fn as_f64(&self) -> Option<f64> {
    match self {
      Self::Float(f) => Some(*f),
      Self::Int(i) => Some(*i as f64),
      _ => None,
    }
  }

  #[inline]
  pub fn as_bool(&self) -> Option<bool> {
    match self {
      Self::Bool(b) => Some(*b),
      _ => None,
    }
  }

  #[inline]
  pub fn as_slice(&self) -> Option<&[RespValue]> {
    match self {
      Self::Arr(v) | Self::Set(v) | Self::Push(v) => Some(v.as_slice()),
      _ => None,
    }
  }

  #[inline]
  pub fn as_map(&self) -> Option<&[(RespValue, RespValue)]> {
    match self {
      Self::Map(v) => Some(v.as_slice()),
      _ => None,
    }
  }

  #[inline]
  pub fn into_bytes(self) -> Option<Vec<u8>> {
    match self {
      Self::Blob(b) => Some(b),
      Self::Simple(s) | Self::Error(s) => Some(s.into_bytes()),
      _ => None,
    }
  }

  #[inline]
  pub fn into_string(self) -> Option<String> {
    match self {
      Self::Simple(s) | Self::Error(s) => Some(s),
      Self::Blob(b) => String::from_utf8(b).ok(),
      _ => None,
    }
  }

  #[inline]
  pub fn into_vec(self) -> Option<Vec<RespValue>> {
    match self {
      Self::Arr(v) | Self::Set(v) | Self::Push(v) => Some(v),
      _ => None,
    }
  }

  #[inline]
  pub fn type_name(&self) -> &'static str {
    match self {
      Self::Simple(_) => "simple",
      Self::Error(_) => "error",
      Self::Int(_) => "integer",
      Self::Float(_) => "double",
      Self::Bool(_) => "boolean",
      Self::Blob(_) => "blob",
      Self::Null => "null",
      Self::Arr(_) => "array",
      Self::Map(_) => "map",
      Self::Set(_) => "set",
      Self::Push(_) => "push",
    }
  }

  #[inline]
  pub fn count(&self) -> usize {
    match self {
      Self::Arr(v) | Self::Set(v) | Self::Push(v) => v.len(),
      Self::Map(v) => v.len(),
      Self::Null => 0,
      _ => 1,
    }
  }

  /// 计算 RESP3 序列化后的确切字节长度，用于精准单次分配
  pub fn serialized_len(&self) -> usize {
    match self {
      Self::Simple(s) | Self::Error(s) => 1 + s.len() + 2,
      Self::Int(i) => 1 + digits_len_i64(*i) + 2,
      Self::Float(f) => {
        let mut buf = zmij::Buffer::new();
        1 + buf.format(*f).len() + 2
      }
      Self::Bool(_) => 4,
      Self::Blob(b) => 1 + digits_count(b.len()) + 2 + b.len() + 2,
      Self::Null => 3,
      Self::Arr(elements) | Self::Set(elements) | Self::Push(elements) => {
        1 + digits_count(elements.len())
          + 2
          + elements.iter().map(Self::serialized_len).sum::<usize>()
      }
      Self::Map(pairs) => {
        1 + digits_count(pairs.len())
          + 2
          + pairs
            .iter()
            .map(|(k, v)| k.serialized_len() + v.serialized_len())
            .sum::<usize>()
      }
    }
  }

  /// 计算 RESP2 序列化后的确切字节长度
  pub fn resp2_serialized_len(&self) -> usize {
    match self {
      Self::Simple(s) | Self::Error(s) => 1 + s.len() + 2,
      Self::Int(i) => 1 + digits_len_i64(*i) + 2,
      Self::Float(f) => {
        let mut buf = zmij::Buffer::new();
        let f_str = buf.format(*f);
        1 + digits_count(f_str.len()) + 2 + f_str.len() + 2
      }
      Self::Bool(_) => 4,
      Self::Blob(b) => 1 + digits_count(b.len()) + 2 + b.len() + 2,
      Self::Null => 5,
      Self::Arr(elements) | Self::Set(elements) | Self::Push(elements) => {
        1 + digits_count(elements.len())
          + 2
          + elements
            .iter()
            .map(Self::resp2_serialized_len)
            .sum::<usize>()
      }
      Self::Map(pairs) => {
        1 + digits_count(pairs.len() * 2)
          + 2
          + pairs
            .iter()
            .map(|(k, v)| k.resp2_serialized_len() + v.resp2_serialized_len())
            .sum::<usize>()
      }
    }
  }

  /// 严格按照 RESP3 规范序列化为字节流
  pub fn serialize(&self, buf: &mut Vec<u8>) {
    let mut itoa_buf = itoa::Buffer::new();
    match self {
      Self::Simple(s) => {
        buf.push(b'+');
        buf.extend_from_slice(s.as_bytes());
        buf.extend_from_slice(b"\r\n");
      }
      Self::Error(s) => {
        buf.push(b'-');
        buf.extend_from_slice(s.as_bytes());
        buf.extend_from_slice(b"\r\n");
      }
      Self::Int(i) => {
        buf.push(b':');
        buf.extend_from_slice(itoa_buf.format(*i).as_bytes());
        buf.extend_from_slice(b"\r\n");
      }
      Self::Float(f) => {
        buf.push(b',');
        let mut zmij_buf = zmij::Buffer::new();
        buf.extend_from_slice(zmij_buf.format(*f).as_bytes());
        buf.extend_from_slice(b"\r\n");
      }
      Self::Bool(b) => {
        buf.push(b'#');
        buf.push(if *b { b't' } else { b'f' });
        buf.extend_from_slice(b"\r\n");
      }
      Self::Blob(bytes) => {
        buf.push(b'$');
        buf.extend_from_slice(itoa_buf.format(bytes.len()).as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(bytes);
        buf.extend_from_slice(b"\r\n");
      }
      Self::Null => {
        buf.extend_from_slice(b"_\r\n");
      }
      Self::Arr(elements) => {
        buf.push(b'*');
        buf.extend_from_slice(itoa_buf.format(elements.len()).as_bytes());
        buf.extend_from_slice(b"\r\n");
        for elem in elements {
          elem.serialize(buf);
        }
      }
      Self::Map(pairs) => {
        buf.push(b'%');
        buf.extend_from_slice(itoa_buf.format(pairs.len()).as_bytes());
        buf.extend_from_slice(b"\r\n");
        for (k, v) in pairs {
          k.serialize(buf);
          v.serialize(buf);
        }
      }
      Self::Set(elements) => {
        buf.push(b'~');
        buf.extend_from_slice(itoa_buf.format(elements.len()).as_bytes());
        buf.extend_from_slice(b"\r\n");
        for elem in elements {
          elem.serialize(buf);
        }
      }
      Self::Push(elements) => {
        buf.push(b'>');
        buf.extend_from_slice(itoa_buf.format(elements.len()).as_bytes());
        buf.extend_from_slice(b"\r\n");
        for elem in elements {
          elem.serialize(buf);
        }
      }
    }
  }

  /// 序列化为独立的字节向量 (RESP3)
  pub fn serialize_to_vec(&self) -> Vec<u8> {
    let mut buf = Vec::with_capacity(self.serialized_len());
    self.serialize(&mut buf);
    buf
  }

  /// 序列化为 RESP2 兼容格式
  pub fn serialize_resp2(&self, buf: &mut Vec<u8>) {
    let mut itoa_buf = itoa::Buffer::new();
    match self {
      Self::Simple(s) => {
        buf.push(b'+');
        buf.extend_from_slice(s.as_bytes());
        buf.extend_from_slice(b"\r\n");
      }
      Self::Error(s) => {
        buf.push(b'-');
        buf.extend_from_slice(s.as_bytes());
        buf.extend_from_slice(b"\r\n");
      }
      Self::Int(i) => {
        buf.push(b':');
        buf.extend_from_slice(itoa_buf.format(*i).as_bytes());
        buf.extend_from_slice(b"\r\n");
      }
      Self::Float(f) => {
        let mut zmij_buf = zmij::Buffer::new();
        let f_str = zmij_buf.format(*f);
        buf.push(b'$');
        buf.extend_from_slice(itoa_buf.format(f_str.len()).as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(f_str.as_bytes());
        buf.extend_from_slice(b"\r\n");
      }
      Self::Bool(b) => {
        buf.push(b':');
        buf.extend_from_slice(if *b { b"1\r\n" } else { b"0\r\n" });
      }
      Self::Blob(bytes) => {
        buf.push(b'$');
        buf.extend_from_slice(itoa_buf.format(bytes.len()).as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(bytes);
        buf.extend_from_slice(b"\r\n");
      }
      Self::Null => {
        buf.extend_from_slice(b"$-1\r\n");
      }
      Self::Arr(elements) | Self::Set(elements) | Self::Push(elements) => {
        buf.push(b'*');
        buf.extend_from_slice(itoa_buf.format(elements.len()).as_bytes());
        buf.extend_from_slice(b"\r\n");
        for elem in elements {
          elem.serialize_resp2(buf);
        }
      }
      Self::Map(pairs) => {
        buf.push(b'*');
        buf.extend_from_slice(itoa_buf.format(pairs.len() * 2).as_bytes());
        buf.extend_from_slice(b"\r\n");
        for (k, v) in pairs {
          k.serialize_resp2(buf);
          v.serialize_resp2(buf);
        }
      }
    }
  }

  /// 序列化为独立的 RESP2 字节向量
  pub fn to_resp2_bytes(&self) -> Vec<u8> {
    let mut buf = Vec::with_capacity(self.resp2_serialized_len());
    self.serialize_resp2(&mut buf);
    buf
  }

  /// 序列化写入 BytesMut (RESP3)
  pub fn serialize_to_bytes_mut(&self, buf: &mut BytesMut) {
    let mut itoa_buf = itoa::Buffer::new();
    match self {
      Self::Simple(s) => {
        buf.extend_from_slice(b"+");
        buf.extend_from_slice(s.as_bytes());
        buf.extend_from_slice(b"\r\n");
      }
      Self::Error(s) => {
        buf.extend_from_slice(b"-");
        buf.extend_from_slice(s.as_bytes());
        buf.extend_from_slice(b"\r\n");
      }
      Self::Int(i) => {
        buf.extend_from_slice(b":");
        buf.extend_from_slice(itoa_buf.format(*i).as_bytes());
        buf.extend_from_slice(b"\r\n");
      }
      Self::Float(f) => {
        buf.extend_from_slice(b",");
        let mut zmij_buf = zmij::Buffer::new();
        buf.extend_from_slice(zmij_buf.format(*f).as_bytes());
        buf.extend_from_slice(b"\r\n");
      }
      Self::Bool(b) => {
        buf.extend_from_slice(if *b { b"#t\r\n" } else { b"#f\r\n" });
      }
      Self::Blob(bytes) => {
        buf.extend_from_slice(b"$");
        buf.extend_from_slice(itoa_buf.format(bytes.len()).as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(bytes);
        buf.extend_from_slice(b"\r\n");
      }
      Self::Null => {
        buf.extend_from_slice(b"_\r\n");
      }
      Self::Arr(elements) => {
        buf.extend_from_slice(b"*");
        buf.extend_from_slice(itoa_buf.format(elements.len()).as_bytes());
        buf.extend_from_slice(b"\r\n");
        for elem in elements {
          elem.serialize_to_bytes_mut(buf);
        }
      }
      Self::Map(pairs) => {
        buf.extend_from_slice(b"%");
        buf.extend_from_slice(itoa_buf.format(pairs.len()).as_bytes());
        buf.extend_from_slice(b"\r\n");
        for (k, v) in pairs {
          k.serialize_to_bytes_mut(buf);
          v.serialize_to_bytes_mut(buf);
        }
      }
      Self::Set(elements) => {
        buf.extend_from_slice(b"~");
        buf.extend_from_slice(itoa_buf.format(elements.len()).as_bytes());
        buf.extend_from_slice(b"\r\n");
        for elem in elements {
          elem.serialize_to_bytes_mut(buf);
        }
      }
      Self::Push(elements) => {
        buf.extend_from_slice(b">");
        buf.extend_from_slice(itoa_buf.format(elements.len()).as_bytes());
        buf.extend_from_slice(b"\r\n");
        for elem in elements {
          elem.serialize_to_bytes_mut(buf);
        }
      }
    }
  }

  /// 写入任意 std::io::Write
  pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
    let mut itoa_buf = itoa::Buffer::new();
    match self {
      Self::Simple(s) => {
        writer.write_all(b"+")?;
        writer.write_all(s.as_bytes())?;
        writer.write_all(b"\r\n")?;
      }
      Self::Error(s) => {
        writer.write_all(b"-")?;
        writer.write_all(s.as_bytes())?;
        writer.write_all(b"\r\n")?;
      }
      Self::Int(i) => {
        writer.write_all(b":")?;
        writer.write_all(itoa_buf.format(*i).as_bytes())?;
        writer.write_all(b"\r\n")?;
      }
      Self::Float(f) => {
        writer.write_all(b",")?;
        let mut zmij_buf = zmij::Buffer::new();
        writer.write_all(zmij_buf.format(*f).as_bytes())?;
        writer.write_all(b"\r\n")?;
      }
      Self::Bool(b) => {
        writer.write_all(if *b { b"#t\r\n" } else { b"#f\r\n" })?;
      }
      Self::Blob(bytes) => {
        writer.write_all(b"$")?;
        writer.write_all(itoa_buf.format(bytes.len()).as_bytes())?;
        writer.write_all(b"\r\n")?;
        writer.write_all(bytes)?;
        writer.write_all(b"\r\n")?;
      }
      Self::Null => {
        writer.write_all(b"_\r\n")?;
      }
      Self::Arr(elements) => {
        writer.write_all(b"*")?;
        writer.write_all(itoa_buf.format(elements.len()).as_bytes())?;
        writer.write_all(b"\r\n")?;
        for elem in elements {
          elem.write_to(writer)?;
        }
      }
      Self::Map(pairs) => {
        writer.write_all(b"%")?;
        writer.write_all(itoa_buf.format(pairs.len()).as_bytes())?;
        writer.write_all(b"\r\n")?;
        for (k, v) in pairs {
          k.write_to(writer)?;
          v.write_to(writer)?;
        }
      }
      Self::Set(elements) => {
        writer.write_all(b"~")?;
        writer.write_all(itoa_buf.format(elements.len()).as_bytes())?;
        writer.write_all(b"\r\n")?;
        for elem in elements {
          elem.write_to(writer)?;
        }
      }
      Self::Push(elements) => {
        writer.write_all(b">")?;
        writer.write_all(itoa_buf.format(elements.len()).as_bytes())?;
        writer.write_all(b"\r\n")?;
        for elem in elements {
          elem.write_to(writer)?;
        }
      }
    }
    Ok(())
  }
}

impl From<i64> for RespValue {
  #[inline]
  fn from(i: i64) -> Self {
    Self::Int(i)
  }
}

impl From<i32> for RespValue {
  #[inline]
  fn from(i: i32) -> Self {
    Self::Int(i as i64)
  }
}

impl From<u64> for RespValue {
  #[inline]
  fn from(u: u64) -> Self {
    if u <= i64::MAX as u64 {
      Self::Int(u as i64)
    } else {
      Self::Blob(itoa::Buffer::new().format(u).as_bytes().to_vec())
    }
  }
}

impl From<usize> for RespValue {
  #[inline]
  fn from(u: usize) -> Self {
    Self::from(u as u64)
  }
}

impl From<bool> for RespValue {
  #[inline]
  fn from(b: bool) -> Self {
    Self::Bool(b)
  }
}

impl From<f64> for RespValue {
  #[inline]
  fn from(f: f64) -> Self {
    Self::Float(f)
  }
}

impl From<Vec<u8>> for RespValue {
  #[inline]
  fn from(b: Vec<u8>) -> Self {
    Self::Blob(b)
  }
}

impl From<&[u8]> for RespValue {
  #[inline]
  fn from(b: &[u8]) -> Self {
    Self::Blob(b.to_vec())
  }
}

impl From<Vec<RespValue>> for RespValue {
  #[inline]
  fn from(elements: Vec<RespValue>) -> Self {
    Self::Arr(elements)
  }
}

/// 解析 RESP 协议帧（基于字节切片的高性能单次扫描零拷贝解析）
pub fn parse_resp(src: &mut BytesMut) -> io::Result<Option<RespValue>> {
  if src.is_empty() {
    return Ok(None);
  }

  match parse_resp_slice(src)? {
    Some((val, consumed)) => {
      src.advance(consumed);
      Ok(Some(val))
    }
    None => Ok(None),
  }
}

/// 核心切片解析函数：返回 (解析出的拥有型值, 消耗的字节数)
pub fn parse_resp_slice(src: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
  if src.is_empty() {
    return Ok(None);
  }

  match src[0] {
    b'+' => parse_simple_slice(src),
    b'-' => parse_error_slice(src),
    b':' => parse_int_slice(src),
    b',' => parse_float_slice(src),
    b'#' => parse_bool_slice(src),
    b'$' => parse_blob_slice(src),
    b'_' => parse_null_slice(src),
    b'*' => parse_arr_slice(src),
    b'%' => parse_map_slice(src),
    b'~' => parse_set_slice(src),
    b'>' => parse_push_slice(src),
    b'!' => parse_bulk_error_slice(src),
    b'=' => parse_verbatim_slice(src),
    b'(' => parse_bignum_slice(src),
    _ => parse_inline_slice(src),
  }
}

/// 零拷贝借用型解析函数：返回 (解析出的借用视图, 消耗的字节数)
pub fn parse_resp_borrow<'a>(src: &'a [u8]) -> io::Result<Option<(RespBorrow<'a>, usize)>> {
  if src.is_empty() {
    return Ok(None);
  }

  match src[0] {
    b'+' => parse_simple_borrow(src),
    b'-' => parse_error_borrow(src),
    b':' => parse_int_borrow(src),
    b',' => parse_float_borrow(src),
    b'#' => parse_bool_borrow(src),
    b'$' => parse_blob_borrow(src),
    b'_' => parse_null_borrow(src),
    b'*' => parse_arr_borrow(src),
    b'%' => parse_map_borrow(src),
    b'~' => parse_set_borrow(src),
    b'>' => parse_push_borrow(src),
    b'!' => parse_bulk_error_borrow(src),
    b'=' => parse_verbatim_borrow(src),
    b'(' => parse_bignum_borrow(src),
    _ => parse_inline_borrow(src),
  }
}

/// 基于 memchr SIMD 加速的 CRLF 扫描定位
#[inline]
pub fn find_crlf(src: &[u8]) -> Option<usize> {
  let mut offset = 0;
  while let Some(pos) = memchr::memchr(b'\r', &src[offset..]) {
    let idx = offset + pos;
    if idx + 1 < src.len() {
      if src[idx + 1] == b'\n' {
        return Some(idx);
      }
      offset = idx + 1;
    } else {
      return None;
    }
  }
  None
}

/// 高性能无分配 i64 解析
#[inline]
pub fn parse_i64_fast(bytes: &[u8]) -> Option<i64> {
  if bytes.is_empty() {
    return None;
  }
  let (neg, digits) = match bytes[0] {
    b'-' => (true, &bytes[1..]),
    b'+' => (false, &bytes[1..]),
    _ => (false, bytes),
  };
  if digits.is_empty() || digits.len() > 19 {
    return None;
  }
  if digits.len() < 19 {
    let mut val: u64 = 0;
    for &d in digits {
      let digit = d.wrapping_sub(b'0');
      if digit > 9 {
        return None;
      }
      val = val * 10 + digit as u64;
    }
    if neg {
      Some((val as i64).wrapping_neg())
    } else {
      Some(val as i64)
    }
  } else {
    let mut val: u64 = 0;
    for &d in digits {
      let digit = d.wrapping_sub(b'0');
      if digit > 9 {
        return None;
      }
      val = val.checked_mul(10)?.checked_add(digit as u64)?;
    }
    if neg {
      if val > (i64::MIN.unsigned_abs()) {
        return None;
      }
      Some((val as i64).wrapping_neg())
    } else {
      if val > i64::MAX as u64 {
        return None;
      }
      Some(val as i64)
    }
  }
}

/// 高性能无分配 u64 解析
#[inline]
pub fn parse_u64_fast(bytes: &[u8]) -> Option<u64> {
  if bytes.is_empty() || bytes.len() > 20 {
    return None;
  }
  if bytes.len() < 20 {
    let mut val: u64 = 0;
    for &d in bytes {
      let digit = d.wrapping_sub(b'0');
      if digit > 9 {
        return None;
      }
      val = val * 10 + digit as u64;
    }
    Some(val)
  } else {
    let mut val: u64 = 0;
    for &d in bytes {
      let digit = d.wrapping_sub(b'0');
      if digit > 9 {
        return None;
      }
      val = val.checked_mul(10)?.checked_add(digit as u64)?;
    }
    Some(val)
  }
}

/// 基于 fast_float 的零拷贝 f64 解析，支持 inf / nan
#[inline]
pub fn parse_f64_fast(bytes: &[u8]) -> Option<f64> {
  if bytes.is_empty() {
    return None;
  }
  if bytes.eq_ignore_ascii_case(b"inf")
    || bytes.eq_ignore_ascii_case(b"+inf")
    || bytes.eq_ignore_ascii_case(b"infinity")
    || bytes.eq_ignore_ascii_case(b"+infinity")
  {
    return Some(f64::INFINITY);
  }
  if bytes.eq_ignore_ascii_case(b"-inf") || bytes.eq_ignore_ascii_case(b"-infinity") {
    return Some(f64::NEG_INFINITY);
  }
  if bytes.eq_ignore_ascii_case(b"nan")
    || bytes.eq_ignore_ascii_case(b"-nan")
    || bytes.eq_ignore_ascii_case(b"+nan")
  {
    return Some(f64::NAN);
  }
  fast_float::parse(bytes).ok()
}

fn parse_simple_slice(src: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let line = &src[1..pos];
    let s = from_utf8(line)
      .map_err(|e| io::Error::new(ErrorKind::InvalidData, e.to_string()))?
      .to_string();
    Ok(Some((RespValue::Simple(s), pos + 2)))
  } else {
    Ok(None)
  }
}

fn parse_simple_borrow<'a>(src: &'a [u8]) -> io::Result<Option<(RespBorrow<'a>, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let line = &src[1..pos];
    let s = from_utf8(line).map_err(|e| io::Error::new(ErrorKind::InvalidData, e.to_string()))?;
    Ok(Some((RespBorrow::Simple(s), pos + 2)))
  } else {
    Ok(None)
  }
}

fn parse_error_slice(src: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let line = &src[1..pos];
    let s = from_utf8(line)
      .map_err(|e| io::Error::new(ErrorKind::InvalidData, e.to_string()))?
      .to_string();
    Ok(Some((RespValue::Error(s), pos + 2)))
  } else {
    Ok(None)
  }
}

fn parse_error_borrow<'a>(src: &'a [u8]) -> io::Result<Option<(RespBorrow<'a>, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let line = &src[1..pos];
    let s = from_utf8(line).map_err(|e| io::Error::new(ErrorKind::InvalidData, e.to_string()))?;
    Ok(Some((RespBorrow::Error(s), pos + 2)))
  } else {
    Ok(None)
  }
}

fn parse_int_slice(src: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let line = &src[1..pos];
    let i = parse_i64_fast(line)
      .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Invalid integer format"))?;
    Ok(Some((RespValue::Int(i), pos + 2)))
  } else {
    Ok(None)
  }
}

fn parse_int_borrow<'a>(src: &'a [u8]) -> io::Result<Option<(RespBorrow<'a>, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let line = &src[1..pos];
    let i = parse_i64_fast(line)
      .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Invalid integer format"))?;
    Ok(Some((RespBorrow::Int(i), pos + 2)))
  } else {
    Ok(None)
  }
}

fn parse_float_slice(src: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let line = &src[1..pos];
    let f = parse_f64_fast(line)
      .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Invalid float format"))?;
    Ok(Some((RespValue::Float(f), pos + 2)))
  } else {
    Ok(None)
  }
}

fn parse_float_borrow<'a>(src: &'a [u8]) -> io::Result<Option<(RespBorrow<'a>, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let line = &src[1..pos];
    let f = parse_f64_fast(line)
      .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Invalid float format"))?;
    Ok(Some((RespBorrow::Float(f), pos + 2)))
  } else {
    Ok(None)
  }
}

fn parse_bool_slice(src: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
  if let Some(pos) = find_crlf(src) {
    if pos != 2 {
      return Err(io::Error::new(
        ErrorKind::InvalidData,
        "Invalid RESP3 bool length",
      ));
    }
    let b = match src[1] {
      b't' => true,
      b'f' => false,
      _ => {
        return Err(io::Error::new(
          ErrorKind::InvalidData,
          "Invalid RESP3 boolean char",
        ));
      }
    };
    Ok(Some((RespValue::Bool(b), 4)))
  } else {
    Ok(None)
  }
}

fn parse_bool_borrow<'a>(src: &'a [u8]) -> io::Result<Option<(RespBorrow<'a>, usize)>> {
  if let Some(pos) = find_crlf(src) {
    if pos != 2 {
      return Err(io::Error::new(
        ErrorKind::InvalidData,
        "Invalid RESP3 bool length",
      ));
    }
    let b = match src[1] {
      b't' => true,
      b'f' => false,
      _ => {
        return Err(io::Error::new(
          ErrorKind::InvalidData,
          "Invalid RESP3 boolean char",
        ));
      }
    };
    Ok(Some((RespBorrow::Bool(b), 4)))
  } else {
    Ok(None)
  }
}

fn parse_null_slice(src: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
  if let Some(pos) = find_crlf(src) {
    Ok(Some((RespValue::Null, pos + 2)))
  } else {
    Ok(None)
  }
}

fn parse_null_borrow<'a>(src: &'a [u8]) -> io::Result<Option<(RespBorrow<'a>, usize)>> {
  if let Some(pos) = find_crlf(src) {
    Ok(Some((RespBorrow::Null, pos + 2)))
  } else {
    Ok(None)
  }
}

fn parse_blob_slice(src: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let len = parse_i64_fast(&src[1..pos])
      .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Invalid blob length"))?;

    if len < 0 {
      return Ok(Some((RespValue::Null, pos + 2)));
    }

    let len = len as usize;
    let data_start = pos + 2;
    let total_needed = data_start
      .checked_add(len)
      .and_then(|x| x.checked_add(2))
      .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Blob length overflow"))?;

    if src.len() < total_needed {
      return Ok(None);
    }

    if &src[data_start + len..total_needed] != b"\r\n" {
      return Err(io::Error::new(
        ErrorKind::InvalidData,
        "Expected CRLF after blob string",
      ));
    }

    let data = src[data_start..data_start + len].to_vec();
    Ok(Some((RespValue::Blob(data), total_needed)))
  } else {
    Ok(None)
  }
}

fn parse_blob_borrow<'a>(src: &'a [u8]) -> io::Result<Option<(RespBorrow<'a>, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let len = parse_i64_fast(&src[1..pos])
      .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Invalid blob length"))?;

    if len < 0 {
      return Ok(Some((RespBorrow::Null, pos + 2)));
    }

    let len = len as usize;
    let data_start = pos + 2;
    let total_needed = data_start
      .checked_add(len)
      .and_then(|x| x.checked_add(2))
      .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Blob length overflow"))?;

    if src.len() < total_needed {
      return Ok(None);
    }

    if &src[data_start + len..total_needed] != b"\r\n" {
      return Err(io::Error::new(
        ErrorKind::InvalidData,
        "Expected CRLF after blob string",
      ));
    }

    let data = &src[data_start..data_start + len];
    Ok(Some((RespBorrow::Blob(data), total_needed)))
  } else {
    Ok(None)
  }
}

fn parse_bulk_error_slice(src: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
  if let Some((blob_val, consumed)) = parse_blob_slice(src)? {
    match blob_val {
      RespValue::Blob(b) => {
        let msg = String::from_utf8(b).unwrap_or_else(|e| format!("{e:?}"));
        Ok(Some((RespValue::Error(msg), consumed)))
      }
      RespValue::Null => Ok(Some((RespValue::Null, consumed))),
      other => Ok(Some((other, consumed))),
    }
  } else {
    Ok(None)
  }
}

fn parse_bulk_error_borrow<'a>(src: &'a [u8]) -> io::Result<Option<(RespBorrow<'a>, usize)>> {
  if let Some((blob_val, consumed)) = parse_blob_borrow(src)? {
    match blob_val {
      RespBorrow::Blob(b) => {
        let msg =
          from_utf8(b).map_err(|e| io::Error::new(ErrorKind::InvalidData, e.to_string()))?;
        Ok(Some((RespBorrow::Error(msg), consumed)))
      }
      RespBorrow::Null => Ok(Some((RespBorrow::Null, consumed))),
      other => Ok(Some((other, consumed))),
    }
  } else {
    Ok(None)
  }
}

fn parse_verbatim_slice(src: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
  if let Some((blob_val, consumed)) = parse_blob_slice(src)? {
    match blob_val {
      RespValue::Blob(mut b) => {
        if b.len() >= 4 && b[3] == b':' {
          b.drain(0..4);
        }
        Ok(Some((RespValue::Blob(b), consumed)))
      }
      other => Ok(Some((other, consumed))),
    }
  } else {
    Ok(None)
  }
}

fn parse_verbatim_borrow<'a>(src: &'a [u8]) -> io::Result<Option<(RespBorrow<'a>, usize)>> {
  if let Some((blob_val, consumed)) = parse_blob_borrow(src)? {
    match blob_val {
      RespBorrow::Blob(b) => {
        let payload = if b.len() >= 4 && b[3] == b':' {
          &b[4..]
        } else {
          b
        };
        Ok(Some((RespBorrow::Blob(payload), consumed)))
      }
      other => Ok(Some((other, consumed))),
    }
  } else {
    Ok(None)
  }
}

fn parse_bignum_slice(src: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let digits = &src[1..pos];
    if let Some(i) = parse_i64_fast(digits) {
      Ok(Some((RespValue::Int(i), pos + 2)))
    } else {
      Ok(Some((RespValue::Blob(digits.to_vec()), pos + 2)))
    }
  } else {
    Ok(None)
  }
}

fn parse_bignum_borrow<'a>(src: &'a [u8]) -> io::Result<Option<(RespBorrow<'a>, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let digits = &src[1..pos];
    if let Some(i) = parse_i64_fast(digits) {
      Ok(Some((RespBorrow::Int(i), pos + 2)))
    } else {
      Ok(Some((RespBorrow::Blob(digits), pos + 2)))
    }
  } else {
    Ok(None)
  }
}

fn parse_arr_slice(src: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let count = parse_i64_fast(&src[1..pos])
      .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Invalid array length"))?;

    if count < 0 {
      return Ok(Some((RespValue::Null, pos + 2)));
    }

    let count = count as usize;
    let initial_cap = count.min(1024);
    let mut elements = Vec::with_capacity(initial_cap);
    let mut consumed = pos + 2;

    for _ in 0..count {
      if consumed >= src.len() {
        return Ok(None);
      }
      match parse_resp_slice(&src[consumed..])? {
        Some((elem, n)) => {
          elements.push(elem);
          consumed += n;
        }
        None => return Ok(None),
      }
    }

    Ok(Some((RespValue::Arr(elements), consumed)))
  } else {
    Ok(None)
  }
}

fn parse_arr_borrow<'a>(src: &'a [u8]) -> io::Result<Option<(RespBorrow<'a>, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let count = parse_i64_fast(&src[1..pos])
      .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Invalid array length"))?;

    if count < 0 {
      return Ok(Some((RespBorrow::Null, pos + 2)));
    }

    let count = count as usize;
    let initial_cap = count.min(1024);
    let mut elements = Vec::with_capacity(initial_cap);
    let mut consumed = pos + 2;

    for _ in 0..count {
      if consumed >= src.len() {
        return Ok(None);
      }
      match parse_resp_borrow(&src[consumed..])? {
        Some((elem, n)) => {
          elements.push(elem);
          consumed += n;
        }
        None => return Ok(None),
      }
    }

    Ok(Some((RespBorrow::Arr(elements), consumed)))
  } else {
    Ok(None)
  }
}

fn parse_map_slice(src: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let count = parse_i64_fast(&src[1..pos])
      .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Invalid map length"))?;

    if count < 0 {
      return Ok(Some((RespValue::Null, pos + 2)));
    }

    let count = count as usize;
    let initial_cap = count.min(1024);
    let mut pairs = Vec::with_capacity(initial_cap);
    let mut consumed = pos + 2;

    for _ in 0..count {
      if consumed >= src.len() {
        return Ok(None);
      }
      let (k, nk) = match parse_resp_slice(&src[consumed..])? {
        Some(res) => res,
        None => return Ok(None),
      };
      consumed += nk;

      if consumed >= src.len() {
        return Ok(None);
      }
      let (v, nv) = match parse_resp_slice(&src[consumed..])? {
        Some(res) => res,
        None => return Ok(None),
      };
      consumed += nv;

      pairs.push((k, v));
    }

    Ok(Some((RespValue::Map(pairs), consumed)))
  } else {
    Ok(None)
  }
}

fn parse_map_borrow<'a>(src: &'a [u8]) -> io::Result<Option<(RespBorrow<'a>, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let count = parse_i64_fast(&src[1..pos])
      .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Invalid map length"))?;

    if count < 0 {
      return Ok(Some((RespBorrow::Null, pos + 2)));
    }

    let count = count as usize;
    let initial_cap = count.min(1024);
    let mut pairs = Vec::with_capacity(initial_cap);
    let mut consumed = pos + 2;

    for _ in 0..count {
      if consumed >= src.len() {
        return Ok(None);
      }
      let (k, nk) = match parse_resp_borrow(&src[consumed..])? {
        Some(res) => res,
        None => return Ok(None),
      };
      consumed += nk;

      if consumed >= src.len() {
        return Ok(None);
      }
      let (v, nv) = match parse_resp_borrow(&src[consumed..])? {
        Some(res) => res,
        None => return Ok(None),
      };
      consumed += nv;

      pairs.push((k, v));
    }

    Ok(Some((RespBorrow::Map(pairs), consumed)))
  } else {
    Ok(None)
  }
}

fn parse_set_slice(src: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let count = parse_i64_fast(&src[1..pos])
      .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Invalid set length"))?;

    if count < 0 {
      return Ok(Some((RespValue::Null, pos + 2)));
    }

    let count = count as usize;
    let initial_cap = count.min(1024);
    let mut elements = Vec::with_capacity(initial_cap);
    let mut consumed = pos + 2;

    for _ in 0..count {
      if consumed >= src.len() {
        return Ok(None);
      }
      match parse_resp_slice(&src[consumed..])? {
        Some((elem, n)) => {
          elements.push(elem);
          consumed += n;
        }
        None => return Ok(None),
      }
    }

    Ok(Some((RespValue::Set(elements), consumed)))
  } else {
    Ok(None)
  }
}

fn parse_set_borrow<'a>(src: &'a [u8]) -> io::Result<Option<(RespBorrow<'a>, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let count = parse_i64_fast(&src[1..pos])
      .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Invalid set length"))?;

    if count < 0 {
      return Ok(Some((RespBorrow::Null, pos + 2)));
    }

    let count = count as usize;
    let initial_cap = count.min(1024);
    let mut elements = Vec::with_capacity(initial_cap);
    let mut consumed = pos + 2;

    for _ in 0..count {
      if consumed >= src.len() {
        return Ok(None);
      }
      match parse_resp_borrow(&src[consumed..])? {
        Some((elem, n)) => {
          elements.push(elem);
          consumed += n;
        }
        None => return Ok(None),
      }
    }

    Ok(Some((RespBorrow::Set(elements), consumed)))
  } else {
    Ok(None)
  }
}

fn parse_push_slice(src: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let count = parse_i64_fast(&src[1..pos])
      .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Invalid push length"))?;

    if count < 0 {
      return Ok(Some((RespValue::Null, pos + 2)));
    }

    let count = count as usize;
    let initial_cap = count.min(1024);
    let mut elements = Vec::with_capacity(initial_cap);
    let mut consumed = pos + 2;

    for _ in 0..count {
      if consumed >= src.len() {
        return Ok(None);
      }
      match parse_resp_slice(&src[consumed..])? {
        Some((elem, n)) => {
          elements.push(elem);
          consumed += n;
        }
        None => return Ok(None),
      }
    }

    Ok(Some((RespValue::Push(elements), consumed)))
  } else {
    Ok(None)
  }
}

fn parse_push_borrow<'a>(src: &'a [u8]) -> io::Result<Option<(RespBorrow<'a>, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let count = parse_i64_fast(&src[1..pos])
      .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "Invalid push length"))?;

    if count < 0 {
      return Ok(Some((RespBorrow::Null, pos + 2)));
    }

    let count = count as usize;
    let initial_cap = count.min(1024);
    let mut elements = Vec::with_capacity(initial_cap);
    let mut consumed = pos + 2;

    for _ in 0..count {
      if consumed >= src.len() {
        return Ok(None);
      }
      match parse_resp_borrow(&src[consumed..])? {
        Some((elem, n)) => {
          elements.push(elem);
          consumed += n;
        }
        None => return Ok(None),
      }
    }

    Ok(Some((RespBorrow::Push(elements), consumed)))
  } else {
    Ok(None)
  }
}

fn parse_inline_slice(src: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let line = &src[..pos];
    let mut parts = Vec::new();
    let mut i = 0;
    while i < line.len() {
      while i < line.len() && (line[i] == b' ' || line[i] == b'\t') {
        i += 1;
      }
      if i >= line.len() {
        break;
      }
      if line[i] == b'"' || line[i] == b'\'' {
        let quote = line[i];
        i += 1;
        let mut arg = Vec::new();
        let mut escaped = false;
        while i < line.len() && (line[i] != quote || escaped) {
          if escaped {
            match line[i] {
              b'n' => arg.push(b'\n'),
              b'r' => arg.push(b'\r'),
              b't' => arg.push(b'\t'),
              b'b' => arg.push(0x08),
              b'a' => arg.push(0x07),
              b'x' if i + 2 < line.len() => {
                if let Ok(val) =
                  u8::from_str_radix(from_utf8(&line[i + 1..i + 3]).unwrap_or(""), 16)
                {
                  arg.push(val);
                  i += 2;
                } else {
                  arg.push(b'x');
                }
              }
              other => arg.push(other),
            }
            escaped = false;
          } else if line[i] == b'\\' && quote == b'"' {
            escaped = true;
          } else {
            arg.push(line[i]);
          }
          i += 1;
        }
        if i < line.len() && line[i] == quote {
          i += 1;
        }
        parts.push(RespValue::Blob(arg));
      } else {
        let start = i;
        while i < line.len() && line[i] != b' ' && line[i] != b'\t' {
          i += 1;
        }
        parts.push(RespValue::Blob(line[start..i].to_vec()));
      }
    }
    Ok(Some((RespValue::Arr(parts), pos + 2)))
  } else {
    Ok(None)
  }
}

fn parse_inline_borrow<'a>(src: &'a [u8]) -> io::Result<Option<(RespBorrow<'a>, usize)>> {
  if let Some(pos) = find_crlf(src) {
    let line = &src[..pos];
    let mut parts = Vec::new();
    let mut i = 0;
    while i < line.len() {
      while i < line.len() && (line[i] == b' ' || line[i] == b'\t') {
        i += 1;
      }
      if i >= line.len() {
        break;
      }
      if line[i] == b'"' || line[i] == b'\'' {
        let quote = line[i];
        i += 1;
        let start = i;
        while i < line.len() && line[i] != quote {
          if line[i] == b'\\' && quote == b'"' && i + 1 < line.len() {
            i += 1;
          }
          i += 1;
        }
        let slice = &line[start..i];
        if i < line.len() && line[i] == quote {
          i += 1;
        }
        parts.push(RespBorrow::Blob(slice));
      } else {
        let start = i;
        while i < line.len() && line[i] != b' ' && line[i] != b'\t' {
          i += 1;
        }
        parts.push(RespBorrow::Blob(&line[start..i]));
      }
    }
    Ok(Some((RespBorrow::Arr(parts), pos + 2)))
  } else {
    Ok(None)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_resp_serialization_and_parsing_round_trip() {
    let test_cases = vec![
      RespValue::Simple("OK".to_string()),
      RespValue::Error("ERR unknown command".to_string()),
      RespValue::Int(10086),
      RespValue::Int(-42),
      RespValue::Int(0),
      RespValue::Int(i64::MAX),
      RespValue::Int(i64::MIN),
      RespValue::Float(1.25),
      RespValue::Bool(true),
      RespValue::Bool(false),
      RespValue::Blob(b"hello world\x00\xff".to_vec()),
      RespValue::Blob(vec![]),
      RespValue::Null,
      RespValue::Arr(vec![
        RespValue::Simple("PING".to_string()),
        RespValue::Int(1),
      ]),
      RespValue::Map(vec![
        (RespValue::Simple("k1".to_string()), RespValue::Int(1)),
        (RespValue::Simple("k2".to_string()), RespValue::Int(2)),
      ]),
      RespValue::Set(vec![RespValue::Int(1), RespValue::Int(2)]),
      RespValue::Push(vec![
        RespValue::Simple("pubsub".to_string()),
        RespValue::Blob(b"msg".to_vec()),
      ]),
    ];

    for val in test_cases {
      let serialized = val.serialize_to_vec();
      assert_eq!(serialized.len(), val.serialized_len());

      let parsed = parse_resp_slice(&serialized).unwrap();
      assert_eq!(parsed, Some((val.clone(), serialized.len())));

      let parsed_borrow = parse_resp_borrow(&serialized).unwrap();
      assert!(parsed_borrow.is_some());
      let (borrow_val, consumed) = parsed_borrow.unwrap();
      assert_eq!(consumed, serialized.len());
      assert_eq!(borrow_val.into_owned(), val);
    }
  }

  #[test]
  fn test_float_parsing_edge_cases() {
    assert_eq!(parse_f64_fast(b"1.25"), Some(1.25));
    assert_eq!(parse_f64_fast(b"-0.5"), Some(-0.5));
    assert_eq!(parse_f64_fast(b"inf"), Some(f64::INFINITY));
    assert_eq!(parse_f64_fast(b"+inf"), Some(f64::INFINITY));
    assert_eq!(parse_f64_fast(b"-inf"), Some(f64::NEG_INFINITY));
    assert!(parse_f64_fast(b"nan").unwrap().is_nan());
  }

  #[test]
  fn test_resp2_compatibility() {
    let val = RespValue::Float(1.25);
    let resp2_bytes = val.to_resp2_bytes();
    assert_eq!(resp2_bytes, b"$4\r\n1.25\r\n");

    let null_val = RespValue::Null;
    assert_eq!(null_val.to_resp2_bytes(), b"$-1\r\n");

    let bool_val = RespValue::Bool(true);
    assert_eq!(bool_val.to_resp2_bytes(), b":1\r\n");
  }

  #[test]
  fn test_inline_command_parsing() {
    let cmd = b"SET mykey \"hello world\" 'single quoted' foo\r\n";
    let (val, consumed) = parse_resp_slice(cmd).unwrap().unwrap();
    assert_eq!(consumed, cmd.len());
    if let RespValue::Arr(parts) = val {
      assert_eq!(parts.len(), 5);
      assert_eq!(parts[0], RespValue::Blob(b"SET".to_vec()));
      assert_eq!(parts[1], RespValue::Blob(b"mykey".to_vec()));
      assert_eq!(parts[2], RespValue::Blob(b"hello world".to_vec()));
      assert_eq!(parts[3], RespValue::Blob(b"single quoted".to_vec()));
      assert_eq!(parts[4], RespValue::Blob(b"foo".to_vec()));
    } else {
      panic!("Expected Arr for inline command");
    }
  }
}
