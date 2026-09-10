//! 对象层共享解析工具（对标 Garnet.common NumUtils/ParseUtils 与
//! libs/server/SessionParseStateExtensions.cs 中对象命令用到的词法解析）

use wbase::{convert::utc_now_ticks, num};

use crate::{
  objects::{
    sortedset::sorted_set_object::{ExpireOption, SortedSetAddOption},
    sortedsetgeo::geo_hash::{GeoDistanceUnitType, GeoHash},
  },
  resp::parser::session_parse_state::strict_i64,
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
///
/// 对标 libs/server/SessionParseStateExtensions.cs:TryGetSortedSetAddOption
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
///
/// 对标 libs/server/SessionParseStateExtensions.cs:TryGetExpireOption
#[inline]
pub fn try_get_expire_option(v: &[u8]) -> Option<ExpireOption> {
  let opt = if equals_ignore_case(v, b"NX") {
    ExpireOption::NX
  } else if equals_ignore_case(v, b"XX") {
    ExpireOption::XX
  } else if equals_ignore_case(v, b"GT") {
    ExpireOption::GT
  } else if equals_ignore_case(v, b"LT") {
    ExpireOption::LT
  } else {
    return None;
  };
  Some(opt)
}

/// 解析 GEO 距离单位词元（m/km/mi/ft）
///
/// 对标 libs/server/SessionParseStateExtensions.cs:TryGetGeoDistanceUnit
#[inline]
pub fn try_get_geo_distance_unit(v: &[u8]) -> Option<GeoDistanceUnitType> {
  let unit = if equals_ignore_case(v, b"m") {
    GeoDistanceUnitType::M
  } else if equals_ignore_case(v, b"km") {
    GeoDistanceUnitType::Km
  } else if equals_ignore_case(v, b"mi") {
    GeoDistanceUnitType::Mi
  } else if equals_ignore_case(v, b"ft") {
    GeoDistanceUnitType::Ft
  } else {
    return None;
  };
  Some(unit)
}

/// 解析 (longitude, latitude) 坐标对，须均合法且在 WGS-84 范围内
///
/// 对标 libs/server/SessionParseStateExtensions.cs:TryGetGeoLonLat
/// （C# 借 Garnet.common ParseUtils.TryParseAsDouble 逐一解析后做范围校验）
#[inline]
pub fn try_get_geo_lon_lat(lon: &[u8], lat: &[u8]) -> Option<(f64, f64)> {
  let longitude = try_parse_with_infinity(lon)?;
  let latitude = try_parse_with_infinity(lat)?;
  if !(geo_longitude_in_range(longitude) && geo_latitude_in_range(latitude)) {
    return None;
  }
  Some((longitude, latitude))
}

/// libs/server/SessionParseStateExtensions.cs:TryGetGeoLonLat 的经度范围检查
#[inline]
fn geo_longitude_in_range(lon: f64) -> bool {
  (GeoHash::LONGITUDE_MIN..=GeoHash::LONGITUDE_MAX).contains(&lon)
}

/// libs/server/SessionParseStateExtensions.cs:TryGetGeoLonLat 的纬度范围检查
#[inline]
fn geo_latitude_in_range(lat: f64) -> bool {
  (GeoHash::LATITUDE_MIN..=GeoHash::LATITUDE_MAX).contains(&lat)
}

/// 当前时刻的 .NET Ticks（公历 0001-01-01 起的 100ns 数）
///
/// 对标 C# `DateTimeOffset.UtcNow.Ticks`（Garnet 过期结构的时间基准）；
/// 单一实现为 `wbase::convert::utc_now_ticks`，此处按对象层既有签名转接
#[inline]
pub fn now_ticks() -> i64 {
  utc_now_ticks()
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
