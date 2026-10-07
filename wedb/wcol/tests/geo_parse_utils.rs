#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wcol::{
  GeoAddOptions, GeoDistanceUnitType,
  parse_utils::{
    GeoLonLatError, try_get_geo_add_option, try_get_geo_distance_unit, try_get_geo_lon_lat,
  },
};

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
