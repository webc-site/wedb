use std::{result, str};

use crate::error::{Error, Result};

/// libs/common/RespReadUtils.cs:MaxArgumentLengthBytes
///
/// 单个 RESP 元素的长度上限（对齐 Redis 默认 512MB 字符串上限）。
/// C# 语义：超上界按"数据未到齐"返回 false 而非抛错（头已照常消费后才
/// 短路判定）——此处保持同一判定次序与返回语义，调用方据 Ok(false) 走
/// 不完整重试路径
pub const MAX_ARGUMENT_LENGTH_BYTES: i32 = 512 * 1_024 * 1_024;

/// garnet/libs/common/RespReadUtils.cs:TryReadUInt64
#[inline]
pub fn try_read_u64(ptr: &mut &[u8], value: &mut u64, bytes_read: &mut usize) -> Result<bool> {
  *bytes_read = 0;
  *value = 0;
  let mut read_head = *ptr;
  let mut i = 0;

  // Fast path for the first 19 digits.
  while i < 19 {
    if read_head.is_empty() {
      break;
    }

    let next_digit = (read_head[0] as u32).wrapping_sub(b'0' as u32);
    if next_digit > 9 {
      break;
    }

    *value = (10 * *value) + next_digit as u64;
    read_head = &read_head[1..];
    i += 1;
  }

  // Parse remaining digits, while checking for overflows.
  while !read_head.is_empty() {
    let next_digit = (read_head[0] as u32).wrapping_sub(b'0' as u32);
    if next_digit > 9 {
      break;
    }

    if (*value == 1844674407370955161 && next_digit > 5) || (*value > 1844674407370955161) {
      return Err(Error::IntegerOverflow { offset: i });
    }

    *value = (10 * *value) + next_digit as u64;
    read_head = &read_head[1..];
    i += 1;
  }

  *bytes_read = i;
  *ptr = read_head;
  Ok(true)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadSignedLengthHeader
#[inline]
pub fn try_read_signed_length_header(
  length: &mut i32,
  ptr: &mut &[u8],
  expected_sigil: u8,
) -> Result<bool> {
  *length = -1;
  if ptr.len() < 3 {
    return Ok(false);
  }

  let mut read_head = &ptr[1..];
  let negative = read_head[0] == b'-';

  // Special case '_\r\n' (RESP3 NULL value)
  if ptr[0] == b'_' && read_head.len() >= 2 && &read_head[0..2] == b"\r\n" {
    *length = -1;
    *ptr = &read_head[2..];
    return Ok(true);
  }

  // String length headers must start with a '$', array headers with '*'
  if ptr[0] != expected_sigil {
    return Err(Error::UnexpectedToken(ptr[0]));
  }

  // Special case: '$-1' (NULL value)
  if negative {
    if ptr.len() < 5 {
      return Ok(false);
    }

    if &read_head[0..4] == b"-1\r\n" {
      *ptr = &read_head[4..];
      return Ok(true);
    }
    read_head = &read_head[1..];
  }

  // Parse length
  let mut value = 0;
  let mut digits_read = 0;
  if !try_read_u64(&mut read_head, &mut value, &mut digits_read)? {
    return Ok(false);
  }

  if digits_read == 0 {
    let unexpected = read_head.first().copied().unwrap_or(0);
    return Err(Error::UnexpectedToken(unexpected));
  }

  // Validate length
  if value > i32::MAX as u64 && (!negative || value > (i32::MAX as u64) + 1) {
    return Err(Error::IntegerOverflow {
      offset: digits_read,
    });
  }

  // Convert to signed value（经 i64 取负：value == 2^31 时 as i32 即 i32::MIN，
  // 若在 i32 域直接取负 debug 构建会溢出 panic）
  *length = if negative {
    -(value as i64) as i32
  } else {
    value as i32
  };

  // Ensure terminator has been received
  if read_head.len() < 2 {
    return Ok(false);
  }

  if &read_head[0..2] != b"\r\n" {
    let unexpected = read_head.first().copied().unwrap_or(0);
    return Err(Error::UnexpectedToken(unexpected));
  }

  *ptr = &read_head[2..];
  Ok(true)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadUnsignedLengthHeader
#[inline]
pub fn try_read_unsigned_length_header(
  length: &mut i32,
  ptr: &mut &[u8],
  expected_sigil: u8,
) -> Result<bool> {
  if !try_read_signed_length_header(length, ptr, expected_sigil)? {
    return Ok(false);
  }

  if *length < 0 {
    return Err(Error::InvalidStringLength(*length));
  }

  Ok(true)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadUnsignedArrayLength
#[inline]
pub fn try_read_unsigned_array_length(length: &mut i32, ptr: &mut &[u8]) -> Result<bool> {
  try_read_unsigned_length_header(length, ptr, b'*')
}

/// garnet/libs/common/RespReadUtils.cs:TryReadSignedArrayLength
#[inline]
pub fn try_read_signed_array_length(length: &mut i32, ptr: &mut &[u8]) -> Result<bool> {
  try_read_signed_length_header(length, ptr, b'*')
}

/// garnet/libs/common/RespReadUtils.cs:TryReadSignedMapLength
#[inline]
pub fn try_read_signed_map_length(length: &mut i32, ptr: &mut &[u8]) -> Result<bool> {
  try_read_signed_length_header(length, ptr, b'%')
}

/// garnet/libs/common/RespReadUtils.cs:TryReadSignedSetLength
#[inline]
pub fn try_read_signed_set_length(length: &mut i32, ptr: &mut &[u8]) -> Result<bool> {
  try_read_signed_length_header(length, ptr, b'~')
}

/// garnet/libs/common/RespReadUtils.cs:TryReadVerbatimStringLength
#[inline]
pub fn try_read_verbatim_string_length(length: &mut i32, ptr: &mut &[u8]) -> Result<bool> {
  try_read_signed_length_header(length, ptr, b'=')
}

/// garnet/libs/common/RespReadUtils.cs:TrySliceWithLengthHeader
#[inline]
pub fn try_slice_with_length_header<'a>(result: &mut &'a [u8], ptr: &mut &'a [u8]) -> Result<bool> {
  *result = &[];

  let mut length = 0;
  if !try_read_unsigned_length_header(&mut length, ptr, b'$')?
    || exceeds_max_argument_length(length)?
  {
    return Ok(false);
  }

  let skip_len = length as usize + 2;
  if ptr.len() < skip_len {
    return Ok(false);
  }

  if &ptr[length as usize..skip_len] != b"\r\n" {
    return Err(Error::UnexpectedToken(ptr[length as usize]));
  }

  *result = &ptr[0..length as usize];
  *ptr = &ptr[skip_len..];

  Ok(true)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadByteArrayWithLengthHeader
#[inline]
pub fn try_read_byte_array_with_length_header(
  result: &mut Vec<u8>,
  ptr: &mut &[u8],
) -> Result<bool> {
  result.clear();
  let mut result_span = &[][..];
  if !try_slice_with_length_header(&mut result_span, ptr)? {
    return Ok(false);
  }

  result.extend_from_slice(result_span);
  Ok(true)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadSpanWithLengthHeader
#[inline]
pub fn try_read_span_with_length_header<'a>(
  result: &mut &'a [u8],
  ptr: &mut &'a [u8],
) -> Result<bool> {
  *result = &[];

  if ptr.len() < 3 {
    return Ok(false);
  }

  let mut length = 0;
  if !try_read_unsigned_length_header(&mut length, ptr, b'$')?
    || exceeds_max_argument_length(length)?
  {
    return Ok(false);
  }

  let skip_len = length as usize + 2;
  if ptr.len() < skip_len {
    return Ok(false);
  }

  if &ptr[length as usize..skip_len] != b"\r\n" {
    return Err(Error::UnexpectedToken(ptr[length as usize]));
  }

  *result = &ptr[0..length as usize];
  *ptr = &ptr[skip_len..];

  Ok(true)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadStringWithLengthHeader
#[inline]
pub fn try_read_string_with_length_header(result: &mut String, ptr: &mut &[u8]) -> Result<bool> {
  let mut result_span = &[][..];
  // 1:1 parity (bug fixed: checked return value)
  if !try_read_span_with_length_header(&mut result_span, ptr)? {
    return Ok(false);
  }

  *result = String::from_utf8_lossy(result_span).into_owned();
  Ok(true)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadPtrWithSignedLengthHeader
#[inline]
pub fn try_read_ptr_with_signed_length_header<'a>(
  result: &mut Option<&'a [u8]>,
  ptr: &mut &'a [u8],
) -> Result<bool> {
  let mut length = 0;
  if !try_read_signed_length_header(&mut length, ptr, b'$')? || exceeds_max_argument_length(length)?
  {
    return Ok(false);
  }

  if length < 0 {
    *result = None;
    return Ok(true);
  }

  let skip_len = length as usize + 2;
  if ptr.len() < skip_len {
    return Ok(false);
  }

  if &ptr[length as usize..skip_len] != b"\r\n" {
    return Err(Error::UnexpectedToken(ptr[length as usize]));
  }

  *result = Some(&ptr[0..length as usize]);
  *ptr = &ptr[skip_len..];
  Ok(true)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadAsSpan
#[inline]
pub fn try_read_as_span<'a>(result: &mut &'a [u8], ptr: &mut &'a [u8]) -> Result<bool> {
  *result = &[];

  if ptr.len() < 2 {
    return Ok(false);
  }

  let mut i = 0;
  while i < ptr.len() - 1 {
    if ptr[i] == b'\r' && ptr[i + 1] == b'\n' {
      *result = &ptr[0..i];
      *ptr = &ptr[i + 2..];
      return Ok(true);
    }
    i += 1;
  }

  Ok(false)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadString
#[inline]
pub fn try_read_string(result: &mut String, ptr: &mut &[u8]) -> Result<bool> {
  let mut result_span = &[][..];
  if !try_read_as_span(&mut result_span, ptr)? {
    return Ok(false);
  }

  *result = String::from_utf8_lossy(result_span).into_owned();
  Ok(true)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadSimpleString
#[inline]
pub fn try_read_simple_string(result: &mut String, ptr: &mut &[u8]) -> Result<bool> {
  if ptr.len() < 2 {
    return Ok(false);
  }

  if ptr[0] != b'+' {
    return Err(Error::UnexpectedToken(ptr[0]));
  }

  *ptr = &ptr[1..];
  try_read_string(result, ptr)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadErrorAsSpan
#[inline]
pub fn try_read_error_as_span<'a>(result: &mut &'a [u8], ptr: &mut &'a [u8]) -> Result<bool> {
  *result = &[];
  if ptr.len() < 2 {
    return Ok(false);
  }

  if ptr[0] != b'-' {
    return Ok(false); // Note: C# returns false instead of throwing for this one!
  }

  *ptr = &ptr[1..];
  try_read_as_span(result, ptr)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadIntegerAsSpan
#[inline]
pub fn try_read_integer_as_span<'a>(result: &mut &'a [u8], ptr: &mut &'a [u8]) -> Result<bool> {
  *result = &[];
  if ptr.len() < 2 {
    return Ok(false);
  }

  if ptr[0] != b':' {
    return Err(Error::UnexpectedToken(ptr[0]));
  }

  *ptr = &ptr[1..];
  try_read_as_span(result, ptr)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadIntegerAsString
#[inline]
pub fn try_read_integer_as_string(result: &mut String, ptr: &mut &[u8]) -> Result<bool> {
  let mut result_span = &[][..];
  let success = try_read_integer_as_span(&mut result_span, ptr)?;
  if success {
    *result = String::from_utf8_lossy(result_span).into_owned();
  }
  Ok(success)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadStringArrayWithLengthHeader
#[inline]
pub fn try_read_string_array_with_length_header(
  result: &mut Vec<String>,
  ptr: &mut &[u8],
) -> Result<bool> {
  result.clear();

  let mut length = 0;
  if !try_read_unsigned_array_length(&mut length, ptr)? {
    return Ok(false);
  }

  for _ in 0..length {
    if ptr.is_empty() {
      return Ok(false);
    }

    let mut item = String::new();
    match ptr[0] {
      // bulk string 元素
      b'$' => {
        if !try_read_string_with_length_header(&mut item, ptr)? {
          return Ok(false);
        }
      }
      // 简单字符串元素（CLUSTER RESERVE VECTOR_SET_CONTEXTS 应答的
      // 十进制上下文形态，C# TryWriteInt64AsSimpleString 同源）
      b'+' => {
        if !try_read_simple_string(&mut item, ptr)? {
          return Ok(false);
        }
      }
      _ => {
        if !try_read_integer_as_string(&mut item, ptr)? {
          return Ok(false);
        }
      }
    }
    result.push(item);
  }

  Ok(true)
}

/// C# 侧 5 处长度消费辅助（RespReadUtils.cs:700/763/870/935/1158）共用的
/// `length > MaxArgumentLengthBytes` 判定；正数才可能越界，负值（NULL）放行
#[inline]
fn exceeds_max_argument_length(length: i32) -> Result<bool> {
  Ok(length > MAX_ARGUMENT_LENGTH_BYTES)
}

/// 整包应答解析错误形态（脚本 GET/SET 特例回包解析专用）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyError {
  /// `-ERR...\r\n` 错误应答（脚本执行错误）
  ErrorReply,
  /// 其余不可识别形态（协议错误）
  Malformed,
}

/// 解析整包 RESP 批量串/null 应答（零拷贝返回载荷切片）
///
/// `$len\r\n<payload>\r\n` → `Ok(Some(载荷))`；`$-1\r\n` → `Ok(None)`；
/// `-...\r\n` → `Err(ReplyError::ErrorReply)`；其余 → `Err(ReplyError::Malformed)`
pub fn parse_bulk_reply(reply: &[u8]) -> result::Result<Option<&[u8]>, ReplyError> {
  if reply.first() == Some(&b'$') {
    let text = str::from_utf8(&reply[1..]).map_err(|_| ReplyError::Malformed)?;
    let Some(crlf) = text.find("\r\n") else {
      return Err(ReplyError::Malformed);
    };
    let len: isize = text[..crlf].parse().map_err(|_| ReplyError::Malformed)?;
    if len < 0 {
      return Ok(None);
    }
    let start = 1 + crlf + 2;
    let end = start + len as usize;
    if reply.len() >= end {
      return Ok(Some(&reply[start..end]));
    }
    return Err(ReplyError::Malformed);
  }
  if reply.first() == Some(&b'-') {
    return Err(ReplyError::ErrorReply);
  }
  Err(ReplyError::Malformed)
}

/// 解析整包 RESP 简单串应答（SET 特例回包：`+OK`）
///
/// `+...\r\n` → `Ok(())`；`-...\r\n` → `Err(ReplyError::ErrorReply)`；
/// 其余 → `Err(ReplyError::Malformed)`
pub fn parse_simple_reply(reply: &[u8]) -> result::Result<(), ReplyError> {
  match reply.first() {
    Some(b'+') => Ok(()),
    Some(b'-') => Err(ReplyError::ErrorReply),
    _ => Err(ReplyError::Malformed),
  }
}
