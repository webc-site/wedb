//! 内存与存储容量单位解析工具
//!
//! 支持解析带单位尺寸字符串（如 "1k", "64mb", "4gb", "1t", "2p"），
//! 以及 2 的幂对齐与人类可读格式化。

/// 常见单位乘数表：[(单位字符, 乘数)]，编译期计算常量，消除运行时 pow 调用
const SUFFIX_MUL: [(u8, i64); 5] = [
  (b'k', 1024),
  (b'm', 1024 * 1024),
  (b'g', 1024 * 1024 * 1024),
  (b't', 1024 * 1024 * 1024 * 1024),
  (b'p', 1024 * 1024 * 1024 * 1024 * 1024),
];

/// libs/client/Utility.cs:ParseSize
///
/// 解析内存尺寸字符串（如 "1gb"、"64mb"、"512kb"）
///
/// 返回 `(解析出的字节数, 消费的字符数)`。
#[inline]
pub fn parse_size(value: &str) -> (i64, usize) {
  parse_size_bytes(value.as_bytes())
}

/// [`parse_size`] 的字节切片形态，零拷贝解析
pub fn parse_size_bytes(value: &[u8]) -> (i64, usize) {
  let mut result: i64 = 0;
  let mut bytes_read = 0usize;

  for (i, &c) in value.iter().enumerate() {
    if c.is_ascii_digit() {
      result = result.wrapping_mul(10).wrapping_add(i64::from(c - b'0'));
      bytes_read += 1;
    } else if let Some((_, mul)) = SUFFIX_MUL.iter().find(|(s, _)| s.eq_ignore_ascii_case(&c)) {
      result = result.wrapping_mul(*mul);
      bytes_read += 1;
      if i + 1 < value.len() && value[i + 1].eq_ignore_ascii_case(&b'b') {
        bytes_read += 1;
      }
      return (result, bytes_read);
    }
  }
  (result, bytes_read)
}

/// 尝试全量解析内存尺寸字符串
///
/// 仅当全量输入被有效消费时返回 `Some(字节数)`，否则返回 `None`。
#[inline]
pub fn try_parse_size(value: &str) -> Option<i64> {
  try_parse_size_bytes(value.as_bytes())
}

/// [`try_parse_size`] 的字节切片形态
#[inline]
pub fn try_parse_size_bytes(value: &[u8]) -> Option<i64> {
  let (size, chars_read) = parse_size_bytes(value);
  (chars_read == value.len()).then_some(size)
}

/// 下取 2 的幂
#[inline]
#[must_use]
pub const fn previous_power_of_2(mut v: i64) -> i64 {
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  v |= v >> 32;
  v - (v >> 1)
}

/// 上取 2 的幂
#[inline]
#[must_use]
pub const fn next_power_of_2(mut v: i64) -> i64 {
  v = v.wrapping_sub(1);
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  v |= v >> 32;
  v.wrapping_add(1)
}

/// 精确计算 2 的幂的 log2（v <= 0 时安全返回 0）
#[inline]
#[must_use]
pub const fn log2_exact(v: i64) -> i32 {
  if v <= 0 { 0 } else { v.ilog2() as i32 }
}

/// libs/client/Utility.cs:PrettySize
///
/// 尺寸字节的人类可读形式（自动选择 k/m/g/t/p 单位）
#[must_use]
pub fn pretty_size(value: i64) -> String {
  const SUFFIX: [char; 5] = ['k', 'm', 'g', 't', 'p'];
  fn round12(v: f64) -> f64 {
    let scaled = v * 1e12;
    let bumped = if scaled >= 0.0 {
      scaled + 0.5
    } else {
      scaled - 0.5
    };
    bumped.floor() / 1e12
  }

  let mut v = value as f64;
  let mut exp: i32 = 0;
  while v - v.floor() > 0.0 {
    if exp >= 18 {
      break;
    }
    exp += 3;
    v *= 1024.0;
    v = round12(v);
  }
  while v.floor().abs() >= 1000.0 {
    if exp <= -18 {
      break;
    }
    exp -= 3;
    v /= 1024.0;
    v = round12(v);
  }
  if exp > 0 {
    let c = SUFFIX[(exp / 3 - 1) as usize];
    format!("{v}{c}")
  } else if exp < 0 {
    let idx = (-exp / 3 - 1) as usize;
    if idx < SUFFIX.len() {
      let c = SUFFIX[idx];
      format!("{v}{c}")
    } else {
      format!("{v}")
    }
  } else {
    format!("{v}")
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_parse_size() {
    assert_eq!(parse_size("1k"), (1024, 2));
    assert_eq!(parse_size("64kb"), (64 * 1024, 4));
    assert_eq!(parse_size("16m"), (16 * 1024 * 1024, 3));
    assert_eq!(parse_size("1gb"), (1024 * 1024 * 1024, 3));
    assert_eq!(try_parse_size("1gb"), Some(1024 * 1024 * 1024));
    assert_eq!(try_parse_size(""), Some(0));
    assert_eq!(try_parse_size("invalid"), None);
  }

  #[test]
  fn test_powers_of_2() {
    assert_eq!(previous_power_of_2(1000), 512);
    assert_eq!(previous_power_of_2(1024), 1024);
    assert_eq!(next_power_of_2(1000), 1024);
    assert_eq!(next_power_of_2(1024), 1024);
    assert_eq!(log2_exact(1024), 10);
  }
}
