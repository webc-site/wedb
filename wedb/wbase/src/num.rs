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

/// 生成整数 `TryParse` 系列：UTF-8 解码 + 类型解析，成功写入 `value` 返回 true
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

/// 生成浮点 `TryParse` 系列（Utf8Parser 整体消费语义：inf/infinity/nan 词形
/// 不属于数值文法，一律拒绝；纯数值溢出的 ±inf 保留），成功写入 `value` 返回 true
macro_rules! try_parse_float {
  ($(#[$meta:meta])* $name:ident, $ty:ty) => {
    $(#[$meta])*
    pub fn $name(source: &[u8], value: &mut $ty) -> bool {
      if let Ok(s) = from_utf8(source)
        && let Ok(v) = s.parse::<$ty>()
        && !v.is_nan()
        && !(v.is_infinite() && !source.iter().any(u8::is_ascii_digit))
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

try_parse_float!(
  /// garnet/libs/common/NumUtils.cs:TryParse
  try_parse_f32,
  f32
);

try_parse_float!(
  /// garnet/libs/common/NumUtils.cs:TryParse
  try_parse_f64,
  f64
);

/// garnet/libs/common/NumUtils.cs:TryParseWithInfinity
///
/// C# 双分支：[`try_parse_f64`]（Utf8Parser 整体消费）成功即返回；
/// 失败回落 RespReadUtils.TryReadInfinity 白名单（inf/+inf/-inf，
/// 大小写不敏感）。NaN 恒拒绝
pub fn try_parse_with_infinity(source: &[u8], value: &mut f64) -> bool {
  if try_parse_f64(source, value) {
    return true;
  }

  // RespReadUtils.TryReadInfinity 词形：inf / +inf / -inf（忽略大小写）
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

/// C# 严格整数解析（对照 RespReadUtils.TryReadInt64Safe allowLeadingZeros: false 语义）：
/// 可选 +/- 号；首数字 '0' 且后续仍有数字即拒绝（"0"/"-0"
/// 合法，"007" 非法）；须为纯数字且整体消费；负值域至 i64::MIN；
/// 溢出返回 None
#[inline]
pub fn strict_i64(raw: &[u8]) -> Option<i64> {
  let (digits, negative) = match raw {
    [b'+', rest @ ..] => (rest, false),
    [b'-', rest @ ..] => (rest, true),
    rest => (rest, false),
  };
  if digits.is_empty() || (digits.len() > 1 && digits[0] == b'0') {
    return None;
  }
  let mut number: u64 = 0;
  for &d in digits {
    if !d.is_ascii_digit() {
      return None;
    }
    number = number.checked_mul(10)?.checked_add(u64::from(d - b'0'))?;
  }
  if negative {
    if number == i64::MIN.unsigned_abs() {
      Some(i64::MIN)
    } else {
      let positive = i64::try_from(number).ok()?;
      Some(-positive)
    }
  } else {
    i64::try_from(number).ok()
  }
}

/// 同 [`strict_i64`] 的 i32 值域版（C# int.MaxValue 上限语义）
#[inline]
pub fn strict_i32(raw: &[u8]) -> Option<i32> {
  i32::try_from(strict_i64(raw)?).ok()
}

/// inf/infinity 词形符号判定（inf/+inf/-inf/infinity/+infinity/-infinity，
/// 大小写不敏感）：Some(true) → +∞，Some(false) → -∞
#[inline]
fn infinity_sign(raw: &[u8]) -> Option<bool> {
  let (body, positive) = match raw {
    [b'+', rest @ ..] => (rest, true),
    [b'-', rest @ ..] => (rest, false),
    rest => (rest, true),
  };
  (body.eq_ignore_ascii_case(b"inf") || body.eq_ignore_ascii_case(b"infinity"))
    .then_some(positive)
}

/// 生成严格浮点解析函数（C# ParseUtils.cs:TryReadDouble/TryReadFloat 口径）：
/// [`try_parse_f32`]/[`try_parse_f64`]（Utf8Parser 整体消费语义，NaN 恒拒绝）
/// 成功即返回；失败且 `can_be_infinite` 时落 inf 词形白名单
///（TryReadInfinity 扩展词表，含 infinity 全拼，大小写不敏感）
macro_rules! strict_float {
  ($(#[$meta:meta])* $name:ident, $ty:ty, $parse:ident) => {
    $(#[$meta])*
    #[inline]
    pub fn $name(raw: &[u8], can_be_infinite: bool) -> Option<$ty> {
      let mut value = 0.0;
      if $parse(raw, &mut value) {
        return Some(value);
      }
      if can_be_infinite {
        return match infinity_sign(raw) {
          Some(true) => Some(<$ty>::INFINITY),
          Some(false) => Some(<$ty>::NEG_INFINITY),
          None => None,
        };
      }
      None
    }
  };
}

strict_float!(
  /// C# ParseUtils.cs:TryReadDouble：严格解析 f64；`can_be_infinite` 放行
  /// inf 词形白名单（inf/+inf/-inf/infinity/+infinity/-infinity）
  strict_f64,
  f64,
  try_parse_f64
);

strict_float!(
  /// 同 [`strict_f64`] 的 f32 版（C# ParseUtils.cs:TryReadFloat）
  strict_f32,
  f32,
  try_parse_f32
);

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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn strict_i64_strict() {
    assert_eq!(strict_i64(b"42"), Some(42));
    assert_eq!(strict_i64(b"-7"), Some(-7));
    assert_eq!(strict_i64(b"+3"), Some(3));
    assert_eq!(strict_i64(b"0"), Some(0));
    assert_eq!(strict_i64(b"-0"), Some(0));
    assert_eq!(strict_i64(b"+0"), Some(0));
    assert_eq!(strict_i64(b"9223372036854775807"), Some(i64::MAX));
    assert_eq!(strict_i64(b"-9223372036854775808"), Some(i64::MIN));
    assert_eq!(strict_i64(b"007"), None);
    assert_eq!(strict_i64(b"-007"), None);
    assert_eq!(strict_i64(b"abc"), None);
    assert_eq!(strict_i64(b""), None);
    assert_eq!(strict_i64(b"1 2"), None);
    assert_eq!(strict_i64(b" 1"), None);
    assert_eq!(strict_i64(b"5 "), None);
    assert_eq!(strict_i64(b"1x"), None);
    assert_eq!(strict_i64(b"9223372036854775808"), None);
    assert_eq!(strict_i64(b"-9223372036854775809"), None);
  }

  #[test]
  fn strict_i32_strict() {
    assert_eq!(strict_i32(b"42"), Some(42));
    assert_eq!(strict_i32(b"-7"), Some(-7));
    assert_eq!(strict_i32(b"2147483647"), Some(i32::MAX));
    assert_eq!(strict_i32(b"-2147483648"), Some(i32::MIN));
    assert_eq!(strict_i32(b"2147483648"), None);
    assert_eq!(strict_i32(b"-2147483649"), None);
    assert_eq!(strict_i32(b"007"), None);
    assert_eq!(strict_i32(b"01"), None);
  }

  #[test]
  fn strict_f64_infinity_forms() {
    // 数值溢出得的 ±inf 保留（Utf8Parser 口径）
    assert_eq!(strict_f64(b"1e999", false), Some(f64::INFINITY));
    // NaN 恒拒绝
    assert_eq!(strict_f64(b"nan", true), None);
    // inf 词形仅 can_be_infinite 放行
    assert_eq!(strict_f64(b"inf", true), Some(f64::INFINITY));
    assert_eq!(strict_f64(b"INF", true), Some(f64::INFINITY));
    assert_eq!(strict_f64(b"+inf", true), Some(f64::INFINITY));
    assert_eq!(strict_f64(b"+Inf", true), Some(f64::INFINITY));
    assert_eq!(strict_f64(b"-inf", true), Some(f64::NEG_INFINITY));
    assert_eq!(strict_f64(b"-INF", true), Some(f64::NEG_INFINITY));
    assert_eq!(strict_f64(b"infinity", true), Some(f64::INFINITY));
    assert_eq!(strict_f64(b"+infinity", true), Some(f64::INFINITY));
    assert_eq!(strict_f64(b"-INFINITY", true), Some(f64::NEG_INFINITY));
    assert_eq!(strict_f64(b"inf", false), None);
    assert_eq!(strict_f64(b"infinity", false), None);
    assert_eq!(strict_f64(b"info", true), None);
    assert_eq!(strict_f64(b"", true), None);
  }

  #[test]
  fn strict_f32_infinity_forms() {
    assert_eq!(strict_f32(b"1e999", true), Some(f32::INFINITY));
    assert_eq!(strict_f32(b"inf", true), Some(f32::INFINITY));
    assert_eq!(strict_f32(b"-infinity", true), Some(f32::NEG_INFINITY));
    assert_eq!(strict_f32(b"nan", true), None);
    assert_eq!(strict_f32(b"inf", false), None);
  }
}
