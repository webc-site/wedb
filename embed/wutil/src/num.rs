/// garnet/libs/common/NumUtils.cs:BytesPerULong
pub const BYTES_PER_ULONG: i32 = 8;

/// garnet/libs/common/NumUtils.cs:CountDigits
pub fn count_digits(value: i64, is_negative: &mut bool) -> i32 {
  if value == i64::MIN {
    *is_negative = true;
    return 19;
  }

  *is_negative = false;
  let mut val = value;
  if val < 0 {
    *is_negative = true;
    val = -val;
  }

  if val < 10 {
    return 1;
  }
  if val < 100 {
    return 2;
  }
  if val < 1000 {
    return 3;
  }
  if val < 100_000_000 {
    if val < 1000000 {
      if val < 10000 {
        return 4;
      }
      return 5 + if val >= 100000 { 1 } else { 0 };
    }
    return 7 + if val >= 10_000_000 { 1 } else { 0 };
  }
  if val < 1000000000 {
    return 9;
  }
  if val < 10000000000 {
    return 10;
  }
  if val < 100000000000 {
    return 11;
  }
  if val < 1000000000000 {
    return 12;
  }
  if val < 10000000000000 {
    return 13;
  }
  if val < 100000000000000 {
    return 14;
  }
  if val < 1000000000000000 {
    return 15;
  }
  if val < 10000000000000000 {
    return 16;
  }
  if val < 100000000000000000 {
    return 17;
  }
  if val < 1000000000000000000 {
    return 18;
  }
  19
}

/// garnet/libs/common/NumUtils.cs:CountCharsInDouble
pub fn count_chars_in_double(
  mut value: f64,
  integer_digits: &mut i32,
  sign_size: &mut u8,
  fractional_digits: &mut i32,
) -> i32 {
  if value == 0.0 {
    *integer_digits = 1;
    *sign_size = 0;
    *fractional_digits = 0;
    return 1;
  }

  *sign_size = if value < 0.0 { 1 } else { 0 };
  value = value.abs();
  *integer_digits = if value < 10.0 {
    1
  } else {
    value.log10() as i32 + 1
  };

  *fractional_digits = 0;
  while *fractional_digits <= 14 {
    let rounded =
      (value * 10_f64.powi(*fractional_digits)).round() / 10_f64.powi(*fractional_digits);
    if (value - rounded).abs() > 2.0 * f64::EPSILON {
      *fractional_digits += 1;
    } else {
      break;
    }
  }

  let dot_size = if *fractional_digits != 0 { 1 } else { 0 };
  *sign_size as i32 + *integer_digits + dot_size + *fractional_digits
}

/// garnet/libs/common/NumUtils.cs:TryParse
pub fn try_parse_i32(source: &[u8], value: &mut i32) -> bool {
  if let Ok(s) = std::str::from_utf8(source)
    && let Ok(v) = s.parse::<i32>()
  {
    *value = v;
    return true;
  }
  false
}

/// garnet/libs/common/NumUtils.cs:TryParse
pub fn try_parse_i64(source: &[u8], value: &mut i64) -> bool {
  if let Ok(s) = std::str::from_utf8(source)
    && let Ok(v) = s.parse::<i64>()
  {
    *value = v;
    return true;
  }
  false
}

/// garnet/libs/common/NumUtils.cs:TryParse
pub fn try_parse_f32(source: &[u8], value: &mut f32) -> bool {
  if let Ok(s) = std::str::from_utf8(source)
    && let Ok(v) = s.parse::<f32>()
  {
    *value = v;
    return true;
  }
  false
}

/// garnet/libs/common/NumUtils.cs:TryParse
pub fn try_parse_f64(source: &[u8], value: &mut f64) -> bool {
  if let Ok(s) = std::str::from_utf8(source)
    && let Ok(v) = s.parse::<f64>()
  {
    *value = v;
    return true;
  }
  false
}

/// garnet/libs/common/NumUtils.cs:TryParseWithInfinity
pub fn try_parse_with_infinity(source: &[u8], value: &mut f64) -> bool {
  if try_parse_f64(source, value) {
    return true;
  }

  // Fallback to infinity parsing (equivalent to TryReadInfinity in RESP)
  // Note: RespReadUtils::TryReadInfinity will need to be implemented separately or
  // we inline the logic here.
  if source.eq_ignore_ascii_case(b"inf") || source.eq_ignore_ascii_case(b"+inf") {
    *value = f64::INFINITY;
    return true;
  }
  if source.eq_ignore_ascii_case(b"-inf") {
    *value = f64::NEG_INFINITY;
    return true;
  }

  false
}

/// garnet/libs/common/NumUtils.cs:GetNextOffset
pub fn get_next_offset(value: &mut u64) -> i32 {
  let offset = value.trailing_zeros() as i32;
  *value &= !(1_u64 << offset);
  offset
}
