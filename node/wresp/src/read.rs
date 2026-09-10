use std::str;

use crate::error::{Error, Result};

/// garnet/libs/common/RespReadUtils.cs:TryReadSign
#[inline(always)]
pub fn try_read_sign(input: &[u8], is_negative: &mut bool) -> bool {
  if let Some(&b) = input.first() {
    if b == b'-' {
      *is_negative = true;
      return true;
    }
    if b == b'+' {
      *is_negative = false;
      return true;
    }
  }
  false
}

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

/// garnet/libs/common/RespReadUtils.cs:TryReadInt64Safe
#[inline]
pub fn try_read_i64_safe(
  ptr: &mut &[u8],
  value: &mut i64,
  bytes_read: &mut usize,
  sign_read: &mut bool,
  overflow: &mut bool,
  allow_leading_zeros: bool,
) -> Result<bool> {
  *bytes_read = 0;
  *value = 0;
  *overflow = false;

  // Parse optional leading sign
  let mut is_negative = false;
  *sign_read = try_read_sign(ptr, &mut is_negative);
  if *sign_read {
    *ptr = &(*ptr)[1..];
    *bytes_read = 1;
  }

  if !allow_leading_zeros {
    // Do not allow leading zeros
    if ptr.len() > 1 && ptr[0] == b'0' {
      return Ok(false);
    }
  }

  // Parse digits as u64
  let mut number = 0;
  let mut digits_read = 0;
  // 刻意差异：C# 对仅有符号（零数字）的输入会以 value=0、bytesRead=1 返回
  // true（TryReadUInt64 恒 Ok，零数字不报错）；此处按"未读到数字即失败"降级，
  // 拒绝 "+" / "-" 空数字形式，避免空 bulk-string 数字（"$0\r\n\r\n"）被
  // 静默解析为 0
  if !try_read_u64(ptr, &mut number, &mut digits_read)? || digits_read == 0 {
    return Ok(false);
  }

  // Check for overflows and convert digits to i64, if possible
  if is_negative {
    if number > (i64::MAX as u64) + 1 {
      *overflow = true;
      return Ok(false);
    }
    *value = -1 - (number.wrapping_sub(1) as i64);
  } else {
    if number > i64::MAX as u64 {
      *overflow = true;
      return Ok(false);
    }
    *value = number as i64;
  }

  *bytes_read += digits_read;
  Ok(true)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadInt64
#[inline]
pub fn try_read_i64(
  ptr: &mut &[u8],
  value: &mut i64,
  bytes_read: &mut usize,
  allow_leading_zeros: bool,
) -> Result<bool> {
  let mut sign_read = false;
  let mut overflow = false;

  if try_read_i64_safe(
    ptr,
    value,
    bytes_read,
    &mut sign_read,
    &mut overflow,
    allow_leading_zeros,
  )? {
    return Ok(true);
  }

  if overflow {
    let digits_read = if sign_read {
      *bytes_read - 1
    } else {
      *bytes_read
    };
    return Err(Error::IntegerOverflow {
      offset: digits_read,
    });
  }

  Ok(false)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadInt32Safe
#[inline]
pub fn try_read_i32_safe(
  ptr: &mut &[u8],
  value: &mut i32,
  bytes_read: &mut usize,
  sign_read: &mut bool,
  overflow: &mut bool,
  allow_leading_zeros: bool,
) -> Result<bool> {
  let mut val64 = 0;
  if !try_read_i64_safe(
    ptr,
    &mut val64,
    bytes_read,
    sign_read,
    overflow,
    allow_leading_zeros,
  )? {
    return Ok(false);
  }

  if val64 > i32::MAX as i64 || val64 < i32::MIN as i64 {
    *overflow = true;
    return Ok(false);
  }

  *value = val64 as i32;
  Ok(true)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadInt32
#[inline]
pub fn try_read_i32(
  ptr: &mut &[u8],
  value: &mut i32,
  bytes_read: &mut usize,
  allow_leading_zeros: bool,
) -> Result<bool> {
  let mut sign_read = false;
  let mut overflow = false;

  if try_read_i32_safe(
    ptr,
    value,
    bytes_read,
    &mut sign_read,
    &mut overflow,
    allow_leading_zeros,
  )? {
    return Ok(true);
  }

  if overflow {
    let digits_read = if sign_read {
      *bytes_read - 1
    } else {
      *bytes_read
    };
    return Err(Error::IntegerOverflow {
      offset: digits_read,
    });
  }

  Ok(false)
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

/// garnet/libs/common/RespReadUtils.cs:TryReadInt32WithLengthHeader
#[inline]
pub fn try_read_i32_with_length_header(value: &mut i32, ptr: &mut &[u8]) -> Result<bool> {
  *value = 0;

  let mut number_length = 0;
  if !try_read_unsigned_length_header(&mut number_length, ptr, b'$')? {
    return Ok(false);
  }

  if ptr.len() < (number_length as usize + 2) {
    return Ok(false);
  }

  let mut bytes_read = 0;
  if !try_read_i32(ptr, value, &mut bytes_read, true)? {
    return Ok(false);
  }

  if bytes_read != number_length as usize {
    return Err(Error::NotANumber);
  }

  if ptr.len() < 2 || &ptr[0..2] != b"\r\n" {
    let unexpected = ptr.first().copied().unwrap_or(0);
    return Err(Error::UnexpectedToken(unexpected));
  }

  *ptr = &ptr[2..];
  Ok(true)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadInt64WithLengthHeader
#[inline]
pub fn try_read_i64_with_length_header(value: &mut i64, ptr: &mut &[u8]) -> Result<bool> {
  *value = 0;

  let mut number_length = 0;
  if !try_read_unsigned_length_header(&mut number_length, ptr, b'$')? {
    return Ok(false);
  }

  if ptr.len() < (number_length as usize + 2) {
    return Ok(false);
  }

  let mut bytes_read = 0;
  if !try_read_i64(ptr, value, &mut bytes_read, true)? {
    return Ok(false);
  }

  if bytes_read != number_length as usize {
    return Err(Error::NotANumber);
  }

  if ptr.len() < 2 || &ptr[0..2] != b"\r\n" {
    let unexpected = ptr.first().copied().unwrap_or(0);
    return Err(Error::UnexpectedToken(unexpected));
  }

  *ptr = &ptr[2..];
  Ok(true)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadUInt64WithLengthHeader
#[inline]
pub fn try_read_u64_with_length_header(value: &mut u64, ptr: &mut &[u8]) -> Result<bool> {
  *value = 0;

  let mut number_length = 0;
  if !try_read_unsigned_length_header(&mut number_length, ptr, b'$')? {
    return Ok(false);
  }

  if ptr.len() < (number_length as usize + 2) {
    return Ok(false);
  }

  let mut bytes_read = 0;
  if !try_read_u64(ptr, value, &mut bytes_read)? {
    return Ok(false);
  }

  if bytes_read != number_length as usize {
    return Err(Error::NotANumber);
  }

  if ptr.len() < 2 || &ptr[0..2] != b"\r\n" {
    let unexpected = ptr.first().copied().unwrap_or(0);
    return Err(Error::UnexpectedToken(unexpected));
  }

  *ptr = &ptr[2..];
  Ok(true)
}

/// garnet/libs/common/RespReadUtils.cs:TrySkipByteArrayWithLengthHeader
#[inline]
pub fn try_skip_byte_array_with_length_header(ptr: &mut &[u8]) -> Result<bool> {
  let mut length = 0;
  if !try_read_unsigned_length_header(&mut length, ptr, b'$')? {
    return Ok(false);
  }

  let skip_len = length as usize + 2;
  if ptr.len() < skip_len {
    return Ok(false);
  }

  if &ptr[length as usize..skip_len] != b"\r\n" {
    return Err(Error::UnexpectedToken(ptr[length as usize]));
  }

  *ptr = &ptr[skip_len..];
  Ok(true)
}

/// garnet/libs/common/RespReadUtils.cs:TrySliceWithLengthHeader
#[inline]
pub fn try_slice_with_length_header<'a>(result: &mut &'a [u8], ptr: &mut &'a [u8]) -> Result<bool> {
  *result = &[];

  let mut length = 0;
  if !try_read_unsigned_length_header(&mut length, ptr, b'$')? {
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

/// garnet/libs/common/RespReadUtils.cs:TryReadBoolWithLengthHeader
#[inline]
pub fn try_read_bool_with_length_header(result: &mut bool, ptr: &mut &[u8]) -> Result<bool> {
  *result = false;

  if ptr.len() < 7 {
    return Ok(false);
  }

  // Fast path: RESP string header should have length 1
  if ptr.starts_with(b"$1\r\n") {
    *ptr = &ptr[4..];
  } else {
    let mut length = 0;
    if !try_read_unsigned_length_header(&mut length, ptr, b'$')? {
      return Ok(false);
    }

    if length != 1 {
      return Err(Error::InvalidStringLength(length));
    }
  }

  *result = ptr[0] == b'1';

  if ptr.len() < 3 || &ptr[1..3] != b"\r\n" {
    let unexpected = ptr.get(1).copied().unwrap_or(0);
    return Err(Error::UnexpectedToken(unexpected));
  }

  *ptr = &ptr[3..];
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
  if !try_read_unsigned_length_header(&mut length, ptr, b'$')? {
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
  if !try_read_signed_length_header(&mut length, ptr, b'$')? {
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

/// garnet/libs/common/RespReadUtils.cs:TryReadStringResponseWithLengthHeader
#[inline]
pub fn try_read_string_response_with_length_header(
  result: &mut Option<String>,
  ptr: &mut &[u8],
) -> Result<bool> {
  *result = None;

  let mut result_span = None;
  if !try_read_ptr_with_signed_length_header(&mut result_span, ptr)? {
    return Ok(false);
  }

  if let Some(span) = result_span {
    *result = Some(String::from_utf8_lossy(span).into_owned());
  }

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

/// garnet/libs/common/RespReadUtils.cs:TryReadErrorAsString
#[inline]
pub fn try_read_error_as_string(result: &mut String, ptr: &mut &[u8]) -> Result<bool> {
  if ptr.len() < 2 {
    return Ok(false);
  }

  if ptr[0] != b'-' {
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
    if ptr[0] == b'$' {
      if !try_read_string_with_length_header(&mut item, ptr)? {
        return Ok(false);
      }
    } else {
      if !try_read_integer_as_string(&mut item, ptr)? {
        return Ok(false);
      }
    }
    result.push(item);
  }

  Ok(true)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadDoubleWithLengthHeader
#[inline]
pub fn try_read_double_with_length_header(
  result: &mut f64,
  parsed: &mut bool,
  ptr: &mut &[u8],
) -> Result<bool> {
  *result = 0.0;
  *parsed = false;

  let mut result_bytes = &[][..];
  if !try_slice_with_length_header(&mut result_bytes, ptr)? {
    return Ok(false);
  }

  if let Ok(s) = str::from_utf8(result_bytes)
    && let Ok(val) = s.parse::<f64>()
  {
    *result = val;
    *parsed = true;
  }

  Ok(true)
}

/// garnet/libs/common/RespReadUtils.cs:TryReadPtrWithLengthHeader
#[inline]
pub fn try_read_ptr_with_length_header<'a>(
  result: &mut &'a [u8],
  len: &mut i32,
  ptr: &mut &'a [u8],
) -> Result<bool> {
  *result = &[];

  if !try_read_unsigned_length_header(len, ptr, b'$')? {
    return Ok(false);
  }

  let skip_len = *len as usize + 2;
  if ptr.len() < skip_len {
    return Ok(false);
  }

  if &ptr[*len as usize..skip_len] != b"\r\n" {
    return Err(Error::UnexpectedToken(ptr[*len as usize]));
  }

  *result = &ptr[0..*len as usize];
  *ptr = &ptr[skip_len..];

  Ok(true)
}

/// garnet/libs/common/RespReadUtils.cs:GetSerializedRecordSpan
#[inline]
pub fn get_serialized_record_span<'a>(
  record_span: &mut &'a [u8],
  ptr: &mut &'a [u8],
) -> Result<bool> {
  if ptr.len() < 4 {
    *record_span = &[];
    return Ok(false);
  }

  let record_length = i32::from_le_bytes(ptr[0..4].try_into().unwrap());
  *ptr = &ptr[4..];

  if record_length < 0 || record_length as usize > ptr.len() {
    *record_span = &[];
    return Ok(false);
  }

  *record_span = &ptr[0..record_length as usize];
  *ptr = &ptr[record_length as usize..];

  Ok(true)
}
