use core::str::from_utf8;
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

/// 小数部分最长判定位数（对标 C# 循环上限，10^-14 精度内收敛）
const MAX_FRACTIONAL_DIGITS: i32 = 14;

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

  // 每轮的缩放幂只计算一次，左右各复用（原实现每轮重复 powi 两次）
  *fractional_digits = 0;
  let mut scale = 10_f64.powi(0);
  while *fractional_digits <= MAX_FRACTIONAL_DIGITS {
    let rounded = (value * scale).round() / scale;
    if (value - rounded).abs() > 2.0 * f64::EPSILON {
      *fractional_digits += 1;
      scale *= 10.0;
    } else {
      break;
    }
  }

  let dot_size = if *fractional_digits != 0 { 1 } else { 0 };
  *sign_size as i32 + *integer_digits + dot_size + *fractional_digits
}

/// 生成 `TryParse` 系列：UTF-8 解码 + 类型解析，成功写入 `value` 返回 true
macro_rules! try_parse {
  ($(#[$meta:meta])* $name:ident, $ty:ty) => {
    $(#[$meta])*
    pub fn $name(source: &[u8], value: &mut $ty) -> bool {
      if let Ok(s) = from_utf8(source)
        && let Ok(v) = s.parse::<$ty>()
      {
        *value = v;
        return true;
      }
      false
    }
  };
}

try_parse!(
  /// garnet/libs/common/NumUtils.cs:TryParse
  try_parse_i32,
  i32
);

try_parse!(
  /// garnet/libs/common/NumUtils.cs:TryParse
  try_parse_i64,
  i64
);

try_parse!(
  /// garnet/libs/common/NumUtils.cs:TryParse
  try_parse_f32,
  f32
);

try_parse!(
  /// garnet/libs/common/NumUtils.cs:TryParse
  try_parse_f64,
  f64
);

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
///
/// 提取最低置位偏移并原位清除该位。`value == 0` 时 C# 的 `1UL << 64`
/// 按位宽取模等价于左移 0 位、值不变返回 64；Rust 移位 64 位在 debug
/// 构建直接 panic，须显式跳过清位对齐 C# 行为（保持 0，返回 64）
pub fn get_next_offset(value: &mut u64) -> i32 {
  let offset = value.trailing_zeros() as i32;
  if offset < 64 {
    *value &= !(1_u64 << offset);
  }
  offset
}
