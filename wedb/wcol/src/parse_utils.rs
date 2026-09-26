//! 集合层共享解析工具
//!
//! 自研依据: 集合参数解析工具（C# 对应 *Ops.cs 参数解析）

use wbase::{eq_ascii_case, num};

use crate::geo::{GeoAddOptions, GeoDistanceUnitType, GeoHash};

/// 解析 GEOADD 选项词元（CH/NX/XX）
#[inline]
pub const fn try_get_geo_add_option(v: &[u8]) -> Option<GeoAddOptions> {
  if v.len() != 2 {
    return None;
  }
  if eq_ascii_case(v, b"CH") {
    Some(GeoAddOptions::CH)
  } else if eq_ascii_case(v, b"NX") {
    Some(GeoAddOptions::NX)
  } else if eq_ascii_case(v, b"XX") {
    Some(GeoAddOptions::XX)
  } else {
    None
  }
}

/// 解析 GEO 距离单位词元（m/km/mi/ft）
#[inline]
pub const fn try_get_geo_distance_unit(v: &[u8]) -> Option<GeoDistanceUnitType> {
  match v.len() {
    1 if eq_ascii_case(v, b"M") => Some(GeoDistanceUnitType::M),
    2 => {
      if eq_ascii_case(v, b"KM") {
        Some(GeoDistanceUnitType::Km)
      } else if eq_ascii_case(v, b"MI") {
        Some(GeoDistanceUnitType::Mi)
      } else if eq_ascii_case(v, b"FT") {
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
  let Some(longitude) = num::strict_f64(lon, true) else {
    return Err(GeoLonLatError::NotFloat);
  };
  let Some(latitude) = num::strict_f64(lat, true) else {
    return Err(GeoLonLatError::NotFloat);
  };
  if !(geo_longitude_in_range(longitude) && geo_latitude_in_range(latitude)) {
    return Err(GeoLonLatError::OutOfRange(longitude, latitude));
  }
  Ok((longitude, latitude))
}

#[inline]
pub const fn geo_longitude_in_range(lon: f64) -> bool {
  lon >= GeoHash::LONGITUDE_MIN && lon <= GeoHash::LONGITUDE_MAX
}

#[inline]
pub const fn geo_latitude_in_range(lat: f64) -> bool {
  lat >= GeoHash::LATITUDE_MIN && lat <= GeoHash::LATITUDE_MAX
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_try_get_geo_add_option() {
    assert_eq!(try_get_geo_add_option(b"ch"), Some(GeoAddOptions::CH));
    assert_eq!(try_get_geo_add_option(b"CH"), Some(GeoAddOptions::CH));
    assert_eq!(try_get_geo_add_option(b"nx"), Some(GeoAddOptions::NX));
    assert_eq!(try_get_geo_add_option(b"NX"), Some(GeoAddOptions::NX));
    assert_eq!(try_get_geo_add_option(b"xx"), Some(GeoAddOptions::XX));
    assert_eq!(try_get_geo_add_option(b"XX"), Some(GeoAddOptions::XX));
    assert_eq!(try_get_geo_add_option(b"xyz"), None);
    assert_eq!(try_get_geo_add_option(b""), None);
  }

  #[test]
  fn test_try_get_geo_distance_unit() {
    assert_eq!(
      try_get_geo_distance_unit(b"m"),
      Some(GeoDistanceUnitType::M)
    );
    assert_eq!(
      try_get_geo_distance_unit(b"M"),
      Some(GeoDistanceUnitType::M)
    );
    assert_eq!(
      try_get_geo_distance_unit(b"km"),
      Some(GeoDistanceUnitType::Km)
    );
    assert_eq!(
      try_get_geo_distance_unit(b"KM"),
      Some(GeoDistanceUnitType::Km)
    );
    assert_eq!(
      try_get_geo_distance_unit(b"mi"),
      Some(GeoDistanceUnitType::Mi)
    );
    assert_eq!(
      try_get_geo_distance_unit(b"MI"),
      Some(GeoDistanceUnitType::Mi)
    );
    assert_eq!(
      try_get_geo_distance_unit(b"ft"),
      Some(GeoDistanceUnitType::Ft)
    );
    assert_eq!(
      try_get_geo_distance_unit(b"FT"),
      Some(GeoDistanceUnitType::Ft)
    );
    assert_eq!(try_get_geo_distance_unit(b"yd"), None);
    assert_eq!(try_get_geo_distance_unit(b""), None);
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
