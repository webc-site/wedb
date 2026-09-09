//! 地理空间编码（对标 libs/server/Objects/SortedSetGeo/GeoHash.cs）
//!
//! 坐标量化借道 IEEE-754 双精度二进制表示直接读取 `floor(2^32 * x)`，
//! Morton 编码用位交织（C# 的 BMI2/AVX512 PDEP/PEXT 硬件加速路径在
//! Rust 侧统一走等价的位运算展开，语义一致）。

use std::f64::consts::PI;

/// 距离单位（对标 libs/server/Objects/SortedSetGeo/GeoSearchOptions.cs:GeoDistanceUnitType）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum GeoDistanceUnitType {
  /// 米
  #[default]
  M = 0,
  /// 千米
  Km,
  /// 英里
  Mi,
  /// 英尺
  Ft,
}

/// GeoHash 全部 16 个方法收敛为无状态实现（C# 为 static class），本类型仅作命名空间
pub struct GeoHash;

impl GeoHash {
  /// 最小经度（WGS 84 / Pseudo-Mercator, EPSG:3857 约束）
  pub const LONGITUDE_MIN: f64 = -180.0;
  /// 最大经度
  pub const LONGITUDE_MAX: f64 = 180.0;
  /// 最小纬度（按 EPSG:3857 应为 ±85.05112878，C# 注释承认此处"不精确"，1:1 保留）
  pub const LATITUDE_MIN: f64 = -90.0;
  /// 最大纬度
  pub const LATITUDE_MAX: f64 = 90.0;
  /// GeoHash 有效精度位数
  pub const BITS_OF_PRECISION: u32 = 52;
  /// GeoHash 标准文本表示长度
  pub const CODE_LENGTH: usize = 11;

  /// (latitude, longitude) → 52 位唯一整数；越界返回 -1
  ///
  /// libs/server/Objects/SortedSetGeo/GeoHash.cs:GeoToLongValue
  pub fn geo_to_long_value(latitude: f64, longitude: f64) -> i64 {
    if !(Self::LATITUDE_MIN..=Self::LATITUDE_MAX).contains(&latitude)
      || !(Self::LONGITUDE_MIN..=Self::LONGITUDE_MAX).contains(&longitude)
    {
      return -1;
    }

    const LAT_TO_UNIT_RANGE_RECIPROCAL: f64 = 1.0 / 180.0;
    const LON_TO_UNIT_RANGE_RECIPROCAL: f64 = 1.0 / 360.0;

    let lat_quantized = Self::quantize(latitude, LAT_TO_UNIT_RANGE_RECIPROCAL);
    let lon_quantized = Self::quantize(longitude, LON_TO_UNIT_RANGE_RECIPROCAL);

    // Morton 编码后左对齐到 52 位精度
    let result = Self::morton_encode(lat_quantized, lon_quantized);
    (result >> (u64::BITS - Self::BITS_OF_PRECISION)) as i64
  }

  /// 坐标量化：把 value 映射到 [0,1) 区间后乘 2^32，
  /// 借 `1.5 + value * reciprocal` 落在 [1.0, 2.0) 的 IEEE-754 尾数直接读取量化结果
  ///
  /// libs/server/Objects/SortedSetGeo/GeoHash.cs:Quantize（C# 为局部函数）
  #[inline]
  pub fn quantize(value: f64, range_reciprocal: f64) -> u32 {
    let y = (value.mul_add(range_reciprocal, 1.5).to_bits()) >> 20;

    // 尾数舍入到 2.0 上界的角落地步：返回 u32::MAX
    if y == (2.0f64.to_bits() >> 20) {
      u32::MAX
    } else {
      y as u32
    }
  }

  /// 52 位 GeoHash 整数 → (latitude, longitude)（取包围盒中心点）
  ///
  /// libs/server/Objects/SortedSetGeo/GeoHash.cs:GetCoordinatesFromLong
  pub fn get_coordinates_from_long(hash: i64) -> (f64, f64) {
    let full_hash = (hash as u64) << (u64::BITS - Self::BITS_OF_PRECISION);
    let (lat_quantized, lon_quantized) = Self::morton_decode(full_hash);

    // 反量化得到包围盒下界，加上半误差得到中心点
    let min_latitude = Self::dequantize(lat_quantized, Self::LATITUDE_MAX);
    let min_longitude = Self::dequantize(lon_quantized, Self::LONGITUDE_MAX);
    let (latitude_error, longitude_error) = Self::get_geo_error_by_precision();

    (
      min_latitude + latitude_error / 2.0,
      min_longitude + longitude_error / 2.0,
    )
  }

  /// 32 位量化值 → 区间 [-range_max, range_max) 内的坐标
  ///
  /// libs/server/Objects/SortedSetGeo/GeoHash.cs:Dequantize（C# 为局部函数）
  #[inline]
  pub fn dequantize(quantized_value: u32, range_max: f64) -> f64 {
    // 重建 [1.0, 2.0) 区间的 IEEE-754 表示
    let value = f64::from_bits(((quantized_value as u64) << 20) | (1023_u64 << 52));
    (range_max + range_max).mul_add(value - 1.0, -range_max)
  }

  /// Morton 编码（Z-order 曲线）：x 交织到偶数位，y 交织到奇数位
  ///
  /// libs/server/Objects/SortedSetGeo/GeoHash.cs:MortonEncode
  #[inline]
  pub fn morton_encode(x: u32, y: u32) -> u64 {
    Self::spread(x) | (Self::spread(y) << 1)
  }

  /// 32 位整数按位展开到 64 位的偶数位
  ///
  /// libs/server/Objects/SortedSetGeo/GeoHash.cs:Spread（C# 为局部函数）
  #[inline]
  pub fn spread(x: u32) -> u64 {
    let mut y = x as u64;
    y = (y | (y << 16)) & 0x0000_FFFF_0000_FFFF;
    y = (y | (y << 8)) & 0x00FF_00FF_00FF_00FF;
    y = (y | (y << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
    y = (y | (y << 2)) & 0x3333_3333_3333_3333;
    (y | (y << 1)) & 0x5555_5555_5555_5555
  }

  /// Morton 解码：偶数位/奇数位分别压缩为 (x, y) 两个 32 位坐标
  ///
  /// libs/server/Objects/SortedSetGeo/GeoHash.cs:MortonDecode
  #[inline]
  pub fn morton_decode(x: u64) -> (u32, u32) {
    (Self::squash(x), Self::squash(x >> 1))
  }

  /// 64 位值的偶数位压缩为 32 位整数
  ///
  /// libs/server/Objects/SortedSetGeo/GeoHash.cs:Squash（C# 为局部函数）
  #[inline]
  pub fn squash(x: u64) -> u32 {
    let mut y = x & 0x5555_5555_5555_5555;
    y = (y | (y >> 1)) & 0x3333_3333_3333_3333;
    y = (y | (y >> 2)) & 0x0F0F_0F0F_0F0F_0F0F;
    y = (y | (y >> 4)) & 0x00FF_00FF_00FF_00FF;
    y = (y | (y >> 8)) & 0x0000_FFFF_0000_FFFF;
    y = (y | (y >> 16)) & 0x0000_0000_FFFF_FFFF;
    y as u32
  }

  /// 52 位 GeoHash 整数 → 11 字符 base-32 标准文本表示
  ///
  /// 第 11 字符需 55 位精度而存储只有 52 位，与标准编码器兼容起见恒为 '0'
  ///
  /// libs/server/Objects/SortedSetGeo/GeoHash.cs:GetGeoHashCode
  pub fn get_geo_hash_code(hash: i64) -> [u8; Self::CODE_LENGTH] {
    const BASE32_CHARS: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let mut hash = hash;
    let mut code = [0; Self::CODE_LENGTH];
    code[Self::CODE_LENGTH - 1] = b'0';

    for slot in code.iter_mut().take(Self::CODE_LENGTH - 1) {
      *slot = BASE32_CHARS[((hash >> (Self::BITS_OF_PRECISION - 5)) & 0x1F) as usize];
      hash <<= 5;
    }
    code
  }

  /// Haversine 公式求两点球面距离（米）
  ///
  /// libs/server/Objects/SortedSetGeo/GeoHash.cs:Distance
  pub fn distance(source_lat: f64, source_lon: f64, target_lat: f64, target_lon: f64) -> f64 {
    // WGS-84 基准地球半径
    const EARTH_RADIUS_IN_METERS: f64 = 6_372_797.560_856;

    let lon_radians = Self::degrees_to_radians(source_lon - target_lon);
    let lon_haversine = (lon_radians / 2.0).sin().powi(2);

    let lat_radians = Self::degrees_to_radians(source_lat - target_lat);
    let lat_haversine = (lat_radians / 2.0).sin().powi(2);

    let tmp =
      Self::degrees_to_radians(source_lat).cos() * Self::degrees_to_radians(target_lat).cos();

    2.0 * (lat_haversine + tmp * lon_haversine).sqrt().asin() * EARTH_RADIUS_IN_METERS
  }

  /// 角度 → 弧度
  ///
  /// libs/server/Objects/SortedSetGeo/GeoHash.cs:DegreesToRadians（C# 为局部函数）
  #[inline]
  pub fn degrees_to_radians(degrees: f64) -> f64 {
    degrees * PI / 180.0
  }

  /// 判断点是否落在以中心点为圆心、radius 为半径的圆内；
  /// 命中时返回球面距离（对标 C# `ref double distance` 出参 + bool 返回）
  ///
  /// libs/server/Objects/SortedSetGeo/GeoHash.cs:IsPointWithinRadius
  pub fn is_point_within_radius(
    radius: f64,
    lat_center_point: f64,
    lon_center_point: f64,
    lat: f64,
    lon: f64,
  ) -> Option<f64> {
    let distance = Self::distance(lat_center_point, lon_center_point, lat, lon);
    (distance < radius).then_some(distance)
  }

  /// 判断点是否落在轴对齐矩形（宽 width_mts × 高 height_mts，中心给定）内；
  /// 命中时返回到中心的真实距离
  ///
  /// libs/server/Objects/SortedSetGeo/GeoHash.cs:GetDistanceWhenInRectangle
  pub fn get_distance_when_in_rectangle(
    width_mts: f64,
    height_mts: f64,
    lat_center_point: f64,
    lon_center_point: f64,
    lat2: f64,
    lon2: f64,
  ) -> Option<f64> {
    let lon_distance = Self::distance(lat2, lon2, lat2, lon_center_point);
    let lat_distance = Self::distance(lat2, lon2, lat_center_point, lon2);
    if lon_distance > width_mts / 2.0 || lat_distance > height_mts / 2.0 {
      return None;
    }
    Some(Self::distance(
      lat_center_point,
      lon_center_point,
      lat2,
      lon2,
    ))
  }

  /// 按精度位数计算纬度/经度量化误差
  ///
  /// libs/server/Objects/SortedSetGeo/GeoHash.cs:GetGeoErrorByPrecision
  #[inline]
  pub fn get_geo_error_by_precision() -> (f64, f64) {
    const LAT_BITS: i32 = GeoHash::BITS_OF_PRECISION as i32 / 2;
    const LONG_BITS: i32 = GeoHash::BITS_OF_PRECISION as i32 - LAT_BITS;

    let lat_error = 180.0 * 2.0_f64.powi(-LAT_BITS);
    let long_error = 360.0 * 2.0_f64.powi(-LONG_BITS);
    (lat_error, long_error)
  }

  /// 千米/英尺/英里 → 米
  ///
  /// libs/server/Objects/SortedSetGeo/GeoHash.cs:ConvertValueToMeters
  #[inline]
  pub fn convert_value_to_meters(value: f64, unit: GeoDistanceUnitType) -> f64 {
    match unit {
      GeoDistanceUnitType::Km => value / 0.001,
      GeoDistanceUnitType::Ft => value / 3.280_84,
      GeoDistanceUnitType::Mi => value / 0.000_621_371,
      GeoDistanceUnitType::M => value,
    }
  }

  /// 米 → 千米/英尺/英里
  ///
  /// libs/server/Objects/SortedSetGeo/GeoHash.cs:ConvertMetersToUnits
  #[inline]
  pub fn convert_meters_to_units(value: f64, unit: GeoDistanceUnitType) -> f64 {
    match unit {
      GeoDistanceUnitType::Km => value * 0.001,
      GeoDistanceUnitType::Ft => value * 3.280_84,
      GeoDistanceUnitType::Mi => value * 0.000_621_371,
      GeoDistanceUnitType::M => value,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::str::from_utf8;

  /// 已知坐标 → GeoHash 文本前缀黄金值（与 Redis GEOHASH 输出一致）
  #[test]
  fn geo_hash_code_golden() {
    let sf = GeoHash::geo_to_long_value(37.7749, -122.4194); // San Francisco
    assert_ne!(sf, -1);
    let code_bytes = GeoHash::get_geo_hash_code(sf);
    let code = from_utf8(&code_bytes).unwrap();
    // Redis: GEOHASH sf → "9q8yy..." 族
    assert!(code.starts_with("9q8yy"), "{code}");

    let tokyo = GeoHash::geo_to_long_value(35.6762, 139.6503);
    let code_bytes = GeoHash::get_geo_hash_code(tokyo);
    let code = from_utf8(&code_bytes).unwrap();
    // 东京圈的 geohash 前缀（xn76）
    assert!(code.starts_with("xn76"), "{code}");
  }

  /// 编码 → 解码往返：恢复坐标与输入的误差不超过量化误差
  #[test]
  fn round_trip_coordinates() {
    let (lat_err, lon_err) = GeoHash::get_geo_error_by_precision();
    for (lat, lon) in [
      (0.0, 0.0),
      (37.7749, -122.4194),
      (-33.8688, 151.2093),
      (89.9, 179.9),
      (-45.0, -90.0),
    ] {
      let hash = GeoHash::geo_to_long_value(lat, lon);
      assert!(hash >= 0);
      let (lat2, lon2) = GeoHash::get_coordinates_from_long(hash);
      assert!((lat2 - lat).abs() <= lat_err, "{lat} vs {lat2}");
      assert!((lon2 - lon).abs() <= lon_err, "{lon} vs {lon2}");
    }
  }

  /// 越界坐标返回 -1
  #[test]
  fn out_of_range_coordinates() {
    assert_eq!(GeoHash::geo_to_long_value(90.1, 0.0), -1);
    assert_eq!(GeoHash::geo_to_long_value(-90.1, 0.0), -1);
    assert_eq!(GeoHash::geo_to_long_value(0.0, 180.1), -1);
    assert_eq!(GeoHash::geo_to_long_value(0.0, -180.1), -1);
    // 边界值合法
    assert_ne!(GeoHash::geo_to_long_value(90.0, 180.0), -1);
    assert_ne!(GeoHash::geo_to_long_value(-90.0, -180.0), -1);
  }

  /// Morton 编解码往返
  #[test]
  fn morton_round_trip() {
    for (x, y) in [(0u32, 0u32), (u32::MAX, 1), (0x1234_5678, 0x9abc_def0)] {
      let m = GeoHash::morton_encode(x, y);
      assert_eq!(GeoHash::morton_decode(m), (x, y));
    }
    // 交织模式：x → 偶数位，最低位是 x 的 bit0
    assert_eq!(GeoHash::morton_encode(1, 0), 1);
    assert_eq!(GeoHash::morton_encode(0, 1), 2);
  }

  /// Haversine 距离黄金值：北京—上海约 1067 km
  #[test]
  fn distance_golden() {
    let d = GeoHash::distance(39.9042, 116.4074, 31.2304, 121.4737);
    assert!((d - 1_067_000.0).abs() < 5_000.0, "{d}");
    // 零距离
    assert_eq!(GeoHash::distance(10.0, 20.0, 10.0, 20.0), 0.0);
  }

  /// 半径/矩形包含判断
  #[test]
  fn radius_and_rectangle() {
    // 1 度纬度 ≈ 111.19 km
    let within = GeoHash::is_point_within_radius(150_000.0, 0.0, 0.0, 1.0, 0.0);
    assert!(within.is_some());
    let outside = GeoHash::is_point_within_radius(100_000.0, 0.0, 0.0, 1.0, 0.0);
    assert!(outside.is_none());

    // 矩形 300km x 300km：纬度 1 度（111km）在半高 150km 内；纬度 2 度（222km）
    // 超出 200km 矩形的半高 100km
    let inside = GeoHash::get_distance_when_in_rectangle(300_000.0, 300_000.0, 0.0, 0.0, 1.0, 0.0);
    assert!(inside.is_some());
    let outside = GeoHash::get_distance_when_in_rectangle(200_000.0, 200_000.0, 0.0, 0.0, 2.0, 0.0);
    assert!(outside.is_none());
  }

  /// 单位换算
  #[test]
  fn unit_conversion() {
    use GeoDistanceUnitType as U;
    assert_eq!(GeoHash::convert_value_to_meters(1.0, U::Km), 1000.0);
    assert!((GeoHash::convert_value_to_meters(1.0, U::Mi) - 1609.344).abs() < 0.01);
    assert!((GeoHash::convert_value_to_meters(1.0, U::Ft) - 0.3048).abs() < 1e-6);
    assert_eq!(GeoHash::convert_meters_to_units(1000.0, U::Km), 1.0);
    assert_eq!(GeoHash::convert_value_to_meters(5.0, U::M), 5.0);
  }
}
