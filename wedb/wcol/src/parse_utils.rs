//! 集合层共享解析工具

use wbase::num;
use wresp::{equals_ignore_case, strict_i64};

use crate::geo::{GeoDistanceUnitType, GeoHash};

/// 严格解析双精度浮点，支持 "inf"/"+inf"/"-inf"
#[inline]
pub fn try_parse_with_infinity(v: &[u8]) -> Option<f64> {
  let mut value = 0.0;
  num::try_parse_with_infinity(v, &mut value).then_some(value)
}

/// 严格解析 i64
#[inline]
pub fn try_get_long(v: &[u8]) -> Option<i64> {
  strict_i64(v)
}

/// 严格解析 i32
#[inline]
pub fn try_get_int(v: &[u8]) -> Option<i32> {
  try_get_long(v).and_then(|v| i32::try_from(v).ok())
}

/// 严格解析 f64
#[inline]
pub fn strict_f64(raw: &[u8], _can_be_infinite: bool) -> Option<f64> {
  try_parse_with_infinity(raw)
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
}
