use std::mem;

/// Advances the slice by `count` bytes
#[inline(always)]
fn advance(curr: &mut &mut [u8], count: usize) {
  let tmp = mem::take(curr);
  *curr = &mut tmp[count..];
}

/// garnet/libs/common/RespWriteUtils.cs:WriteNewline
#[inline(always)]
pub fn write_newline(curr: &mut &mut [u8]) {
  curr[0] = b'\r';
  curr[1] = b'\n';
  advance(curr, 2);
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteMapLength
#[inline]
pub fn try_write_map_length(len: i32, curr: &mut &mut [u8]) -> bool {
  let mut buffer = itoa::Buffer::new();
  let s = buffer.format(len).as_bytes();
  let total_len = 1 + s.len() + 2;
  if curr.len() < total_len {
    return false;
  }
  curr[0] = b'%';
  curr[1..1 + s.len()].copy_from_slice(s);
  let tmp = mem::take(curr);
  *curr = &mut tmp[1 + s.len()..];
  write_newline(curr);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWritePushLength
#[inline]
pub fn try_write_push_length(len: i32, curr: &mut &mut [u8]) -> bool {
  let mut buffer = itoa::Buffer::new();
  let s = buffer.format(len).as_bytes();
  let total_len = 1 + s.len() + 2;
  if curr.len() < total_len {
    return false;
  }
  curr[0] = b'>';
  curr[1..1 + s.len()].copy_from_slice(s);
  let tmp = mem::take(curr);
  *curr = &mut tmp[1 + s.len()..];
  write_newline(curr);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWritePaddedBulkStringLength
#[inline]
pub fn try_write_padded_bulk_string_length(
  len: i32,
  padded_len: i32,
  curr: &mut &mut [u8],
) -> bool {
  let mut buffer = itoa::Buffer::new();
  let s = buffer.format(len).as_bytes();
  // In C#, NumUtils.CountDigits(len) ignores the minus sign. But len >= 0 here usually.
  // If len < 0, s.len() includes '-', which matches C#'s NumUtils.CountDigits(len) + (len<0?1:0).
  // Actually C#'s totalLen is 1 + numDigits + 2. It DOES NOT add 1 for sign.
  // So if len < 0, padded length might be wrong! But len is never < 0 for padded bulk strings in Garnet.
  let num_digits = if len < 0 { s.len() - 1 } else { s.len() };
  let total_len = 1 + num_digits + 2;

  debug_assert!(total_len as i32 <= padded_len);
  if curr.len() < padded_len as usize {
    return false;
  }

  curr[0] = b'$';
  advance(curr, 1);

  let pad = (padded_len as usize) - total_len;
  for _ in 0..pad {
    curr[0] = b'0';
    advance(curr, 1);
  }

  // In C# negative sign is placed after the padded zeros, but here we just write it.
  // Wait, if it's negative, C#'s WriteInt32 will write the minus sign AFTER the padded zeros!
  curr[0..s.len()].copy_from_slice(s);
  advance(curr, s.len());
  write_newline(curr);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteArrayLength
#[inline]
pub fn try_write_array_length(len: i32, curr: &mut &mut [u8]) -> bool {
  let mut buffer = itoa::Buffer::new();
  let s = buffer.format(len).as_bytes();
  let total_len = 1 + s.len() + 2;
  if curr.len() < total_len {
    return false;
  }
  curr[0] = b'*';
  curr[1..1 + s.len()].copy_from_slice(s);
  let tmp = mem::take(curr);
  *curr = &mut tmp[1 + s.len()..];
  write_newline(curr);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteSetLength
#[inline]
pub fn try_write_set_length(len: i32, curr: &mut &mut [u8]) -> bool {
  let mut buffer = itoa::Buffer::new();
  let s = buffer.format(len).as_bytes();
  let total_len = 1 + s.len() + 2;
  if curr.len() < total_len {
    return false;
  }
  curr[0] = b'~';
  curr[1..1 + s.len()].copy_from_slice(s);
  let tmp = mem::take(curr);
  *curr = &mut tmp[1 + s.len()..];
  write_newline(curr);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteArrayItem
#[inline]
pub fn try_write_array_item(integer: i64, curr: &mut &mut [u8]) -> bool {
  let mut val_buf = itoa::Buffer::new();
  let val_str = val_buf.format(integer).as_bytes();
  let val_len = val_str.len();

  let mut len_buf = itoa::Buffer::new();
  let len_str = len_buf.format(val_len).as_bytes();

  let total_len = 1 + len_str.len() + 2 + val_len + 2;
  if curr.len() < total_len {
    return false;
  }

  curr[0] = b'$';
  curr[1..1 + len_str.len()].copy_from_slice(len_str);
  advance(curr, 1 + len_str.len());
  write_newline(curr);

  curr[0..val_len].copy_from_slice(val_str);
  advance(curr, val_len);
  write_newline(curr);

  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteNull
#[inline]
pub fn try_write_null(curr: &mut &mut [u8]) -> bool {
  if curr.len() < 5 {
    return false;
  }
  curr[0..5].copy_from_slice(b"$-1\r\n");
  advance(curr, 5);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteResp3Null
#[inline]
pub fn try_write_resp3_null(curr: &mut &mut [u8]) -> bool {
  if curr.len() < 3 {
    return false;
  }
  curr[0..3].copy_from_slice(b"_\r\n");
  advance(curr, 3);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteNullArray
#[inline]
pub fn try_write_null_array(curr: &mut &mut [u8]) -> bool {
  if curr.len() < 5 {
    return false;
  }
  curr[0..5].copy_from_slice(b"*-1\r\n");
  advance(curr, 5);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteSimpleString
#[inline]
pub fn try_write_simple_string(simple_string: &[u8], curr: &mut &mut [u8]) -> bool {
  let total_len = 1 + simple_string.len() + 2;
  if curr.len() < total_len {
    return false;
  }
  curr[0] = b'+';
  curr[1..1 + simple_string.len()].copy_from_slice(simple_string);
  advance(curr, 1 + simple_string.len());
  write_newline(curr);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteInt64AsSimpleString
#[inline]
pub fn try_write_i64_as_simple_string(value: i64, curr: &mut &mut [u8]) -> bool {
  let mut buffer = itoa::Buffer::new();
  let s = buffer.format(value).as_bytes();
  let total_len = 1 + s.len() + 2;
  if curr.len() < total_len {
    return false;
  }
  curr[0] = b'+';
  curr[1..1 + s.len()].copy_from_slice(s);
  advance(curr, 1 + s.len());
  write_newline(curr);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteError
#[inline]
pub fn try_write_error(error_string: &[u8], curr: &mut &mut [u8]) -> bool {
  let total_len = 1 + error_string.len() + 2;
  if curr.len() < total_len {
    return false;
  }
  curr[0] = b'-';
  curr[1..1 + error_string.len()].copy_from_slice(error_string);
  advance(curr, 1 + error_string.len());
  write_newline(curr);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteDirect
#[inline]
pub fn try_write_direct(input: &[u8], curr: &mut &mut [u8]) -> bool {
  if curr.len() < input.len() {
    return false;
  }
  curr[0..input.len()].copy_from_slice(input);
  advance(curr, input.len());
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteAsciiDirect
#[inline]
pub fn try_write_ascii_direct(input: &str, curr: &mut &mut [u8]) -> bool {
  try_write_direct(input.as_bytes(), curr)
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteBulkStringLength
#[inline]
pub fn try_write_bulk_string_length(len: i32, curr: &mut &mut [u8]) -> bool {
  let mut buffer = itoa::Buffer::new();
  let s = buffer.format(len).as_bytes();
  let total_len = 1 + s.len() + 2;
  if curr.len() < total_len {
    return false;
  }
  curr[0] = b'$';
  curr[1..1 + s.len()].copy_from_slice(s);
  advance(curr, 1 + s.len());
  write_newline(curr);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteBulkString
#[inline]
pub fn try_write_bulk_string(item: &[u8], curr: &mut &mut [u8]) -> bool {
  let mut buffer = itoa::Buffer::new();
  let len_str = buffer.format(item.len()).as_bytes();
  let total_len = 1 + len_str.len() + 2 + item.len() + 2;
  if curr.len() < total_len {
    return false;
  }

  curr[0] = b'$';
  curr[1..1 + len_str.len()].copy_from_slice(len_str);
  advance(curr, 1 + len_str.len());
  write_newline(curr);

  curr[0..item.len()].copy_from_slice(item);
  advance(curr, item.len());
  write_newline(curr);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteAsciiBulkString
#[inline]
pub fn try_write_ascii_bulk_string(chars: &str, curr: &mut &mut [u8]) -> bool {
  try_write_bulk_string(chars.as_bytes(), curr)
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteUtf8BulkString
#[inline]
pub fn try_write_utf8_bulk_string(chars: &str, curr: &mut &mut [u8]) -> bool {
  // In Rust strings are already UTF-8
  try_write_bulk_string(chars.as_bytes(), curr)
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteNewLine
#[inline]
pub fn try_write_new_line(curr: &mut &mut [u8]) -> bool {
  if curr.len() < 2 {
    return false;
  }
  write_newline(curr);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:GetBulkStringLength
#[inline]
pub fn get_bulk_string_length(len: i32) -> i32 {
  let mut num_digits = 1;
  let mut val = len;
  if val < 0 {
    val = -val;
  }
  while val >= 10 {
    num_digits += 1;
    val /= 10;
  }
  let digits = if len < 0 { num_digits + 1 } else { num_digits };
  1 + digits + 2 + len + 2
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteInt32
#[inline]
pub fn try_write_i32(value: i32, curr: &mut &mut [u8]) -> bool {
  let mut buffer = itoa::Buffer::new();
  let s = buffer.format(value).as_bytes();
  let total_len = 1 + s.len() + 2;
  if curr.len() < total_len {
    return false;
  }
  curr[0] = b':';
  curr[1..1 + s.len()].copy_from_slice(s);
  advance(curr, 1 + s.len());
  write_newline(curr);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteInt64
#[inline]
pub fn try_write_i64(value: i64, curr: &mut &mut [u8]) -> bool {
  let mut buffer = itoa::Buffer::new();
  let s = buffer.format(value).as_bytes();
  let total_len = 1 + s.len() + 2;
  if curr.len() < total_len {
    return false;
  }
  curr[0] = b':';
  curr[1..1 + s.len()].copy_from_slice(s);
  advance(curr, 1 + s.len());
  write_newline(curr);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteIntegerFromBytes
#[inline]
pub fn try_write_integer_from_bytes(integer_bytes: &[u8], curr: &mut &mut [u8]) -> bool {
  let total_len = 1 + integer_bytes.len() + 2;
  if curr.len() < total_len {
    return false;
  }
  curr[0] = b':';
  curr[1..1 + integer_bytes.len()].copy_from_slice(integer_bytes);
  advance(curr, 1 + integer_bytes.len());
  write_newline(curr);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteInt32AsBulkString
#[inline]
pub fn try_write_i32_as_bulk_string(value: i32, curr: &mut &mut [u8]) -> bool {
  let mut val_buf = itoa::Buffer::new();
  let val_str = val_buf.format(value).as_bytes();
  let val_len = val_str.len();

  let mut len_buf = itoa::Buffer::new();
  let len_str = len_buf.format(val_len).as_bytes();

  let total_len = 1 + len_str.len() + 2 + val_len + 2;
  if curr.len() < total_len {
    return false;
  }

  curr[0] = b'$';
  curr[1..1 + len_str.len()].copy_from_slice(len_str);
  advance(curr, 1 + len_str.len());
  write_newline(curr);

  curr[0..val_len].copy_from_slice(val_str);
  advance(curr, val_len);
  write_newline(curr);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteInt64AsBulkString
#[inline]
pub fn try_write_i64_as_bulk_string(value: i64, curr: &mut &mut [u8]) -> bool {
  let mut val_buf = itoa::Buffer::new();
  let val_str = val_buf.format(value).as_bytes();
  let val_len = val_str.len();

  let mut len_buf = itoa::Buffer::new();
  let len_str = len_buf.format(val_len).as_bytes();

  let total_len = 1 + len_str.len() + 2 + val_len + 2;
  if curr.len() < total_len {
    return false;
  }

  curr[0] = b'$';
  curr[1..1 + len_str.len()].copy_from_slice(len_str);
  advance(curr, 1 + len_str.len());
  write_newline(curr);

  curr[0..val_len].copy_from_slice(val_str);
  advance(curr, val_len);
  write_newline(curr);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:GetIntegerAsBulkStringLength
#[inline]
pub fn get_integer_as_bulk_string_length(integer: i32) -> i32 {
  let val_len = itoa::Buffer::new().format(integer).len() as i32;
  let len_len = itoa::Buffer::new().format(val_len).len() as i32;
  1 + len_len + 2 + val_len + 2
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteDoubleBulkString
#[inline]
pub fn try_write_double_bulk_string(value: f64, curr: &mut &mut [u8]) -> bool {
  if value.is_nan() {
    return try_write_nan(curr);
  } else if value.is_infinite() {
    return try_write_infinity(value, curr);
  }

  let mut buffer = ryu::Buffer::new();
  let s = buffer.format(value).as_bytes();

  let mut len_buf = itoa::Buffer::new();
  let len_str = len_buf.format(s.len()).as_bytes();

  let total_len = 1 + len_str.len() + 2 + s.len() + 2;
  if curr.len() < total_len {
    return false;
  }

  curr[0] = b'$';
  curr[1..1 + len_str.len()].copy_from_slice(len_str);
  advance(curr, 1 + len_str.len());
  write_newline(curr);

  curr[0..s.len()].copy_from_slice(s);
  advance(curr, s.len());
  write_newline(curr);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteDoubleNumeric
#[inline]
pub fn try_write_double_numeric(value: f64, curr: &mut &mut [u8]) -> bool {
  if value.is_nan() {
    return try_write_nan_numeric(curr);
  } else if value.is_infinite() {
    return try_write_infinity_numeric(value, curr);
  }

  let mut buffer = ryu::Buffer::new();
  let s = buffer.format(value).as_bytes();

  let total_len = 1 + s.len() + 2;
  if curr.len() < total_len {
    return false;
  }

  curr[0] = b',';
  curr[1..1 + s.len()].copy_from_slice(s);
  advance(curr, 1 + s.len());
  write_newline(curr);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteInfinity
#[inline]
pub fn try_write_infinity(value: f64, curr: &mut &mut [u8]) -> bool {
  if value.is_sign_positive() {
    if curr.len() < 9 {
      return false;
    }
    curr[0..9].copy_from_slice(b"$3\r\ninf\r\n");
    advance(curr, 9);
  } else {
    if curr.len() < 10 {
      return false;
    }
    curr[0..10].copy_from_slice(b"$4\r\n-inf\r\n");
    advance(curr, 10);
  }
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteInfinity_Numeric
#[inline]
pub fn try_write_infinity_numeric(value: f64, curr: &mut &mut [u8]) -> bool {
  if value.is_sign_positive() {
    if curr.len() < 6 {
      return false;
    }
    curr[0..6].copy_from_slice(b",inf\r\n");
    advance(curr, 6);
  } else {
    if curr.len() < 7 {
      return false;
    }
    curr[0..7].copy_from_slice(b",-inf\r\n");
    advance(curr, 7);
  }
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteNaN
#[inline]
pub fn try_write_nan(curr: &mut &mut [u8]) -> bool {
  if curr.len() < 9 {
    return false;
  }
  curr[0..9].copy_from_slice(b"$3\r\nnan\r\n");
  advance(curr, 9);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteNaN_Numeric
#[inline]
pub fn try_write_nan_numeric(curr: &mut &mut [u8]) -> bool {
  if curr.len() < 6 {
    return false;
  }
  curr[0..6].copy_from_slice(b",nan\r\n");
  advance(curr, 6);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteEmptyArray
#[inline]
pub fn try_write_empty_array(curr: &mut &mut [u8]) -> bool {
  if curr.len() < 4 {
    return false;
  }
  curr[0..4].copy_from_slice(b"*0\r\n");
  advance(curr, 4);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteEmptyMap
#[inline]
pub fn try_write_empty_map(curr: &mut &mut [u8]) -> bool {
  if curr.len() < 4 {
    return false;
  }
  curr[0..4].copy_from_slice(b"%0\r\n");
  advance(curr, 4);
  true
}

/// garnet/libs/common/RespWriteUtils.cs:TryWriteEmptySet
#[inline]
pub fn try_write_empty_set(curr: &mut &mut [u8]) -> bool {
  if curr.len() < 4 {
    return false;
  }
  curr[0..4].copy_from_slice(b"~0\r\n");
  advance(curr, 4);
  true
}
