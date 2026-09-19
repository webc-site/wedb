//! 集合层共享解析工具

use wbase::num;
use wresp::options::equals_ignore_case;

use crate::geo::{GeoDistanceUnitType, GeoHash};

/// 严格解析双精度浮点，支持 "inf"/"+inf"/"-inf"
#[inline]
pub fn try_parse_with_infinity(v: &[u8]) -> Option<f64> {
  let mut value = 0.0;
  num::try_parse_with_infinity(v, &mut value).then_some(value)
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

/// libs/server/SessionParseStateExtensions.cs:TryGetGeoLonLat 的两态失败形态
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GeoLonLatError {
  /// 经纬度不可解析为浮点（含 inf 词形）
  NotFloat,
  /// 越界坐标对（携带解析值，供错误文案回显）
  OutOfRange(f64, f64),
}

/// 解析 (longitude, latitude) 坐标对（C# TryGetGeoLonLat：非法浮点与越界
/// 分两态报告，错误文案由会话层组装）
#[inline]
pub fn try_get_geo_lon_lat(lon: &[u8], lat: &[u8]) -> Result<(f64, f64), GeoLonLatError> {
  let Some(longitude) = try_parse_with_infinity(lon) else {
    return Err(GeoLonLatError::NotFloat);
  };
  let Some(latitude) = try_parse_with_infinity(lat) else {
    return Err(GeoLonLatError::NotFloat);
  };
  if !(geo_longitude_in_range(longitude) && geo_latitude_in_range(latitude)) {
    return Err(GeoLonLatError::OutOfRange(longitude, latitude));
  }
  Ok((longitude, latitude))
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
  }

  #[test]
  fn geo_lon_lat_reports_two_failure_kinds() {
    // 合法坐标对
    assert_eq!(
      try_get_geo_lon_lat(b"13.361389", b"38.115556"),
      Ok((13.361_389, 38.115_556))
    );
    // 非浮点 → NotFloat（C# RESP_ERR_NOT_VALID_FLOAT 态）
    assert_eq!(
      try_get_geo_lon_lat(b"abc", b"38.0"),
      Err(GeoLonLatError::NotFloat)
    );
    assert_eq!(
      try_get_geo_lon_lat(b"nan", b"38.0"),
      Err(GeoLonLatError::NotFloat)
    );
    // 越界 → OutOfRange 携带解析值（C# GenericErrLonLat 回显态；C# GeoHash
    // 边界 ±180/±90）
    assert_eq!(
      try_get_geo_lon_lat(b"181.0", b"38.0"),
      Err(GeoLonLatError::OutOfRange(181.0, 38.0))
    );
    assert_eq!(
      try_get_geo_lon_lat(b"13.0", b"-90.1"),
      Err(GeoLonLatError::OutOfRange(13.0, -90.1))
    );
    // inf 词形可解析但必然越界
    assert_eq!(
      try_get_geo_lon_lat(b"inf", b"38.0"),
      Err(GeoLonLatError::OutOfRange(f64::INFINITY, 38.0))
    );
  }
}
