//! 对象层共享解析工具（对标 Garnet.common NumUtils/ParseUtils 与
//! libs/server/SessionParseStateExtensions.cs 中对象命令用到的词法解析）

use wbase::num;
use wresp::strict_i64;

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
/// 单一实现位于 `wbase::num::try_parse_with_infinity`
/// （对标 Garnet.common NumUtils.TryParseWithInfinity），此处仅按对象层签名转接
#[inline]
pub fn try_parse_with_infinity(v: &[u8]) -> Option<f64> {
  let mut value = 0.0;
  num::try_parse_with_infinity(v, &mut value).then_some(value)
}

/// 严格解析 i64（单一实现为 [`strict_i64`]，对标 parseState.TryGetLong →
/// RespReadUtils.TryReadInt64Safe：可带 +/- 号，禁前导零，整体消费）
#[inline]
pub fn try_get_long(v: &[u8]) -> Option<i64> {
  strict_i64(v)
}

/// 严格解析 i32（对标 parseState.TryGetInt）
#[inline]
pub fn try_get_int(v: &[u8]) -> Option<i32> {
  try_get_long(v).and_then(|v| i32::try_from(v).ok())
}

/// 解析 ZADD 选项词元（XX/NX/LT/GT/CH/INCR）
#[inline]
pub fn try_get_sorted_set_add_option(v: &[u8]) -> Option<SortedSetAddOption> {
  match v.len() {
    2 => {
      if equals_ignore_case(v, b"XX") {
        Some(SortedSetAddOption::XX)
      } else if equals_ignore_case(v, b"NX") {
        Some(SortedSetAddOption::NX)
      } else if equals_ignore_case(v, b"LT") {
        Some(SortedSetAddOption::LT)
      } else if equals_ignore_case(v, b"GT") {
        Some(SortedSetAddOption::GT)
      } else if equals_ignore_case(v, b"CH") {
        Some(SortedSetAddOption::CH)
      } else {
        None
      }
    }
    4 => {
      if equals_ignore_case(v, b"INCR") {
        Some(SortedSetAddOption::INCR)
      } else {
        None
      }
    }
    _ => None,
  }
}

/// 严格解析 f64（支持无穷大字面量）
#[inline]
pub fn strict_f64(raw: &[u8], _can_be_infinite: bool) -> Option<f64> {
  try_parse_with_infinity(raw)
}

/// 解析过期选项词元（NX/XX/GT/LT）
#[inline]
pub fn try_get_expire_option(v: &[u8]) -> Option<ExpireOption> {
  if v.len() != 2 {
    return None;
  }
  if equals_ignore_case(v, b"NX") {
    Some(ExpireOption::NX)
  } else if equals_ignore_case(v, b"XX") {
    Some(ExpireOption::XX)
  } else if equals_ignore_case(v, b"GT") {
    Some(ExpireOption::GT)
  } else if equals_ignore_case(v, b"LT") {
    Some(ExpireOption::LT)
  } else {
    None
  }
}

/// 解析 GEO 距离单位词元（m/km/mi/ft）
#[inline]
pub fn try_get_geo_distance_unit(v: &[u8]) -> Option<GeoDistanceUnitType> {
  match v.len() {
    1 => {
      if equals_ignore_case(v, b"M") {
        Some(GeoDistanceUnitType::M)
      } else {
        None
      }
    }
    2 => {
      if equals_ignore_case(v, b"KM") {
        Some(GeoDistanceUnitType::Km)
      } else if equals_ignore_case(v, b"MI") {
        Some(GeoDistanceUnitType::Mi)
      } else if equals_ignore_case(v, b"FT") {
        Some(GeoDistanceUnitType::Ft)
      } else {
        None
      }
    }
    _ => None,
  }
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
#[cfg(test)]
mod tests {
  use wbase::time::now_ticks;

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
