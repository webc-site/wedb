//! 对象层共享解析工具（对标 Garnet.common NumUtils/ParseUtils 与
//! libs/server/SessionParseStateExtensions.cs 中对象命令用到的词法解析）

use std::str;

use crate::objects::{
  sortedset::sorted_set_object::{ExpireOption, SortedSetAddOption},
  sortedsetgeo::geo_hash::{GeoDistanceUnitType, GeoHash},
};

/// 比较字节切片是否相等（忽略 ASCII 大小写）
///
/// 对标 CmdStrings.EqualsUpperCaseSpanIgnoringCase
#[inline]
pub fn equals_ignore_case(a: &[u8], b: &[u8]) -> bool {
  a.eq_ignore_ascii_case(b)
}

/// 严格解析双精度浮点（整体须为合法数字），支持 "inf"/"+inf"/"-inf" 无穷量词
///
/// 对标 Garnet.common NumUtils.TryParseWithInfinity
/// （Utf8Parser 全量消费 + RespReadUtils.TryReadInfinity 回退，量词大小写不敏感）
#[inline]
pub fn try_parse_with_infinity(v: &[u8]) -> Option<f64> {
  // RespReadUtils.TryReadInfinity 词形：3 字节 inf / 4 字节 ±inf（忽略大小写）
  match v.len() {
    3 if equals_ignore_case(v, b"inf") => return Some(f64::INFINITY),
    4 if equals_ignore_case(v, b"+inf") => return Some(f64::INFINITY),
    4 if equals_ignore_case(v, b"-inf") => return Some(f64::NEG_INFINITY),
    _ => {}
  }
  // Utf8Parser 全量消费路径：不接受 inf/nan 词形（Rust 解析器的
  // "Infinity" 等扩展词形与 NaN 一并排除；纯数值溢出的 ±inf 保留）
  let d = str::from_utf8(v).ok()?.parse::<f64>().ok()?;
  if d.is_nan() || (d.is_infinite() && !v.iter().any(u8::is_ascii_digit)) {
    return None;
  }
  Some(d)
}

/// 严格解析 i64（对标 parseState.TryGetLong → RespReadUtils.TryReadInt64Safe：
/// 可带 +/- 号，禁前导零，整体消费）
#[inline]
pub fn try_get_long(v: &[u8]) -> Option<i64> {
  let s = str::from_utf8(v).ok()?;
  let digits = match s.as_bytes().first() {
    Some(b'+') | Some(b'-') => &s[1..],
    _ => s,
  };
  // 禁前导零（"0" 本身与 "-0" 合法），且仅接受纯数字
  if digits.len() > 1 && digits.starts_with('0') {
    return None;
  }
  if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
    return None;
  }
  s.parse().ok()
}

/// 严格解析 i32（对标 parseState.TryGetInt）
#[inline]
pub fn try_get_int(v: &[u8]) -> Option<i32> {
  try_get_long(v).and_then(|v| i32::try_from(v).ok())
}

/// 解析 ZADD 选项词元（XX/NX/LT/GT/CH/INCR）
#[inline]
pub fn try_get_sorted_set_add_option(v: &[u8]) -> Option<SortedSetAddOption> {
  let opt = if equals_ignore_case(v, b"XX") {
    SortedSetAddOption::XX
  } else if equals_ignore_case(v, b"NX") {
    SortedSetAddOption::NX
  } else if equals_ignore_case(v, b"LT") {
    SortedSetAddOption::LT
  } else if equals_ignore_case(v, b"GT") {
    SortedSetAddOption::GT
  } else if equals_ignore_case(v, b"CH") {
    SortedSetAddOption::CH
  } else if equals_ignore_case(v, b"INCR") {
    SortedSetAddOption::INCR
  } else {
    return None;
  };
  Some(opt)
}

/// 解析过期选项词元（NX/XX/GT/LT）
#[inline]
pub fn try_get_expire_option(v: &[u8]) -> Option<ExpireOption> {
  match crate::session_parse_state_extensions::expire_option_from_token(v)? {
    crate::session_parse_state_extensions::ExpireOption::Nx => Some(ExpireOption::NX),
    crate::session_parse_state_extensions::ExpireOption::Xx => Some(ExpireOption::XX),
    crate::session_parse_state_extensions::ExpireOption::Gt => Some(ExpireOption::GT),
    crate::session_parse_state_extensions::ExpireOption::Lt => Some(ExpireOption::LT),
    _ => None,
  }
}

/// 解析 GEO 距离单位词元（m/km/mi/ft）
#[inline]
pub fn try_get_geo_distance_unit(v: &[u8]) -> Option<GeoDistanceUnitType> {
  crate::session_parse_state_extensions::geo_distance_unit(v)
}

/// 解析 (longitude, latitude) 坐标对，须均合法且在 WGS-84 范围内
#[inline]
pub fn try_get_geo_lon_lat(lon: &[u8], lat: &[u8]) -> Option<(f64, f64)> {
  let longitude = try_parse_with_infinity(lon)?;
  let latitude = try_parse_with_infinity(lat)?;
  if !(geo_longitude_in_range(longitude) && geo_latitude_in_range(latitude)) {
    return None;
  }
  Some((longitude, latitude))
}

#[inline]
fn geo_longitude_in_range(lon: f64) -> bool {
  (GeoHash::LONGITUDE_MIN..=GeoHash::LONGITUDE_MAX).contains(&lon)
}

#[inline]
fn geo_latitude_in_range(lat: f64) -> bool {
  (GeoHash::LATITUDE_MIN..=GeoHash::LATITUDE_MAX).contains(&lat)
}

/// 当前时刻的 .NET Ticks（公历 0001-01-01 起的 100ns 数）
///
/// 对标 C# `DateTimeOffset.UtcNow.Ticks`（Garnet 过期结构的时间基准）
#[inline]
pub fn now_ticks() -> i64 {
  let now_ms = coarsetime::Clock::now_since_epoch().as_millis() as i64;
  (now_ms + 62_135_596_800_000) * 10_000
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn infinity_forms() {
    assert_eq!(try_parse_with_infinity(b"inf"), Some(f64::INFINITY));
    assert_eq!(try_parse_with_infinity(b"INF"), Some(f64::INFINITY));
    assert_eq!(try_parse_with_infinity(b"+inf"), Some(f64::INFINITY));
    assert_eq!(try_parse_with_infinity(b"-INF"), Some(f64::NEG_INFINITY));
    assert_eq!(try_parse_with_infinity(b"+Inf"), Some(f64::INFINITY));
    assert_eq!(try_parse_with_infinity(b"-inf"), Some(f64::NEG_INFINITY));
    assert_eq!(try_parse_with_infinity(b"nan"), None);
    assert_eq!(try_parse_with_infinity(b"NaN"), None);
    // Rust 解析器接受的 "Infinity" 扩展词形为 Utf8Parser 所不容 → 拒绝
    assert_eq!(try_parse_with_infinity(b"Infinity"), None);
    assert_eq!(try_parse_with_infinity(b"abc"), None);
    assert_eq!(try_parse_with_infinity(b"1.5"), Some(1.5));
    assert_eq!(try_parse_with_infinity(b"1e3"), Some(1000.0));
    // 尾随垃圾拒绝
    assert_eq!(try_parse_with_infinity(b"1.5x"), None);
  }

  #[test]
  fn ints_strict() {
    assert_eq!(try_get_long(b"42"), Some(42));
    assert_eq!(try_get_long(b"-1"), Some(-1));
    assert_eq!(try_get_long(b"+7"), Some(7));
    assert_eq!(try_get_long(b"0"), Some(0));
    assert_eq!(try_get_long(b"-0"), Some(0));
    // 前导零拒绝（对标 TryReadInt64Safe allowLeadingZeros:false）
    assert_eq!(try_get_long(b"007"), None);
    assert_eq!(try_get_long(b"-007"), None);
    assert_eq!(try_get_long(b"1.5"), None);
    assert_eq!(try_get_long(b""), None);
    assert_eq!(try_get_long(b"+"), None);
    assert_eq!(try_get_int(b"2147483647"), Some(i32::MAX));
    assert_eq!(try_get_int(b"2147483648"), None);
  }

  #[test]
  fn options_tokens() {
    assert_eq!(
      try_get_sorted_set_add_option(b"incr"),
      Some(SortedSetAddOption::INCR)
    );
    assert_eq!(try_get_sorted_set_add_option(b"zz"), None);
    assert_eq!(try_get_expire_option(b"gt"), Some(ExpireOption::GT));
    assert_eq!(try_get_expire_option(b"nx"), Some(ExpireOption::NX));
    assert_eq!(try_get_expire_option(b""), None);
    assert_eq!(
      try_get_geo_distance_unit(b"KM"),
      Some(GeoDistanceUnitType::Km)
    );
    assert_eq!(try_get_geo_distance_unit(b"parsecs"), None);
  }

  #[test]
  fn geo_lon_lat_ranges() {
    let parsed = try_get_geo_lon_lat(b"2.3522", b"48.8566");
    assert_eq!(parsed, Some((2.3522, 48.8566)));
    assert_eq!(try_get_geo_lon_lat(b"181", b"0"), None);
    assert_eq!(try_get_geo_lon_lat(b"0", b"91"), None);
    assert_eq!(try_get_geo_lon_lat(b"abc", b"0"), None);
  }

  #[test]
  fn ticks_monotonic_and_large() {
    let t = now_ticks();
    assert!(t > 638_000_000_000_000_000);
    assert!(now_ticks() >= t);
  }
}
