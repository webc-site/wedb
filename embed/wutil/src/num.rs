use core::str::from_utf8;
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
