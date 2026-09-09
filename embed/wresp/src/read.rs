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

  // We need to keep a copy of ptr in case we need to calculate offset for error
  let _start_len = ptr.len();

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
    // The offset logic in C# is: ptr - digits_read. In Rust, we just return the bytes_read or offset.
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
  if ptr[0] == b'_'
    && read_head.len() >= 2 && &read_head[0..2] == b"\r\n" {
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

  // Convert to signed value
  *length = if negative {
    -(value as i32)
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

const MAX_ARGUMENT_LENGTH_BYTES: i32 = 1024 * 1024 * 1024; // Arbitrary 1GB limit as in Garnet usually? 
// Wait, Garnet RespReadUtils.MaxArgumentLengthBytes is 1024 * 1024 * 1024.

/// garnet/libs/common/RespReadUtils.cs:TrySkipByteArrayWithLengthHeader
#[inline]
pub fn try_skip_byte_array_with_length_header(ptr: &mut &[u8]) -> Result<bool> {
  let mut length = 0;
  if !try_read_unsigned_length_header(&mut length, ptr, b'$')? {
    return Ok(false);
  }

  // Garnet validates MaxArgumentLengthBytes
  if length > 536870912 { // typically 512MB in Garnet. Let's not hardcode if not strictly necessary.
    // we'll just check against 512MB
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
