//! 地理空间编码（对标 libs/server/Objects/SortedSetGeo/GeoHash.cs）

use std::f64::consts::PI;

/// 距离单位
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

/// GeoHash 全部算法收敛为无状态实现
pub struct GeoHash;

impl GeoHash {
  pub const LONGITUDE_MIN: f64 = -180.0;
  pub const LONGITUDE_MAX: f64 = 180.0;
  pub const LATITUDE_MIN: f64 = -90.0;
  pub const LATITUDE_MAX: f64 = 90.0;
  pub const BITS_OF_PRECISION: u32 = 52;
  pub const CODE_LENGTH: usize = 11;

  /// 在 garnet 中的相对路径:libs/server/Objects/SortedSetGeo/GeoHash.cs:GeoToLongValue
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

    let result = Self::morton_encode(lat_quantized, lon_quantized);
    (result >> (u64::BITS - Self::BITS_OF_PRECISION)) as i64
  }

  #[inline]
  /// 对标 GeoHash.cs GeoToLongValue 的内嵌局部函数 Quantize（C# 局部函数
  /// 不被扫描器索引，映射归并到父方法 GeoToLongValue，避免重复登记）
  pub fn quantize(value: f64, range_reciprocal: f64) -> u32 {
    let y = (value.mul_add(range_reciprocal, 1.5).to_bits()) >> 20;
    if y == (2.0f64.to_bits() >> 20) {
      u32::MAX
    } else {
      y as u32
    }
  }

  /// 在 garnet 中的相对路径:libs/server/Objects/SortedSetGeo/GeoHash.cs:GetCoordinatesFromLong
  pub fn get_coordinates_from_long(hash: i64) -> (f64, f64) {
    let full_hash = (hash as u64) << (u64::BITS - Self::BITS_OF_PRECISION);
    let (lat_quantized, lon_quantized) = Self::morton_decode(full_hash);

    let min_latitude = Self::dequantize(lat_quantized, Self::LATITUDE_MAX);
    let min_longitude = Self::dequantize(lon_quantized, Self::LONGITUDE_MAX);
    let (latitude_error, longitude_error) = Self::get_geo_error_by_precision();

    (
      min_latitude + latitude_error / 2.0,
      min_longitude + longitude_error / 2.0,
    )
  }

  #[inline]
  /// 对标 GeoHash.cs GetCoordinatesFromLong 的内嵌局部函数 Dequantize
  ///（C# 局部函数不被扫描器索引，映射归并到父方法）
  pub fn dequantize(quantized_value: u32, range_max: f64) -> f64 {
    let value = f64::from_bits(((quantized_value as u64) << 20) | (1023_u64 << 52));
    (range_max + range_max).mul_add(value - 1.0, -range_max)
  }

  #[inline]
  /// 在 garnet 中的相对路径:libs/server/Objects/SortedSetGeo/GeoHash.cs:MortonEncode
  pub fn morton_encode(x: u32, y: u32) -> u64 {
    Self::spread(x) | (Self::spread(y) << 1)
  }

  #[inline]
  /// 对标 GeoHash.cs MortonEncode 的内嵌局部函数 Spread（C# 局部函数
  /// 不被扫描器索引，映射归并到父方法）
  pub fn spread(x: u32) -> u64 {
    let mut y = x as u64;
    y = (y | (y << 16)) & 0x0000_FFFF_0000_FFFF;
    y = (y | (y << 8)) & 0x00FF_00FF_00FF_00FF;
    y = (y | (y << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
    y = (y | (y << 2)) & 0x3333_3333_3333_3333;
    (y | (y << 1)) & 0x5555_5555_5555_5555
  }

  #[inline]
  /// 在 garnet 中的相对路径:libs/server/Objects/SortedSetGeo/GeoHash.cs:MortonDecode
  pub fn morton_decode(x: u64) -> (u32, u32) {
    (Self::squash(x), Self::squash(x >> 1))
  }

  #[inline]
  /// 对标 GeoHash.cs MortonDecode 的内嵌局部函数 Squash（C# 局部函数
  /// 不被扫描器索引，映射归并到父方法）
  pub fn squash(x: u64) -> u32 {
    let mut y = x & 0x5555_5555_5555_5555;
    y = (y | (y >> 1)) & 0x3333_3333_3333_3333;
    y = (y | (y >> 2)) & 0x0F0F_0F0F_0F0F_0F0F;
    y = (y | (y >> 4)) & 0x00FF_00FF_00FF_00FF;
    y = (y | (y >> 8)) & 0x0000_FFFF_0000_FFFF;
    y = (y | (y >> 16)) & 0x0000_0000_FFFF_FFFF;
    y as u32
  }

  /// 在 garnet 中的相对路径:libs/server/Objects/SortedSetGeo/GeoHash.cs:GetGeoHashCode
  ///（C# 返回 string，rust 为零分配 [u8; 11] 定长码）
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

  /// 在 garnet 中的相对路径:libs/server/Objects/SortedSetGeo/GeoHash.cs:Distance
  pub fn distance(source_lat: f64, source_lon: f64, target_lat: f64, target_lon: f64) -> f64 {
    const EARTH_RADIUS_IN_METERS: f64 = 6_372_797.560_856;

    let lon_radians = Self::degrees_to_radians(source_lon - target_lon);
    let lon_haversine = (lon_radians / 2.0).sin().powi(2);

    let lat_radians = Self::degrees_to_radians(source_lat - target_lat);
    let lat_haversine = (lat_radians / 2.0).sin().powi(2);

    let tmp =
      Self::degrees_to_radians(source_lat).cos() * Self::degrees_to_radians(target_lat).cos();

    2.0 * (lat_haversine + tmp * lon_haversine).sqrt().asin() * EARTH_RADIUS_IN_METERS
  }

  #[inline]
  /// 对标 GeoHash.cs Distance 的内嵌局部函数 DegreesToRadians（C# 局部
  /// 函数不被扫描器索引，映射归并到父方法）
  pub fn degrees_to_radians(degrees: f64) -> f64 {
    degrees * PI / 180.0
  }

  /// 在 garnet 中的相对路径:libs/server/Objects/SortedSetGeo/GeoHash.cs:IsPointWithinRadius
  ///（C# ref double distance 出参 → Option<f64>：入圈 None/Some(距离) 反转，None = 不在圈内）
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

  /// 在 garnet 中的相对路径:libs/server/Objects/SortedSetGeo/GeoHash.cs:GetDistanceWhenInRectangle
  ///（C# bool + ref double distance → Option<f64>）
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

  #[inline]
  /// 在 garnet 中的相对路径:libs/server/Objects/SortedSetGeo/GeoHash.cs:GetGeoErrorByPrecision
  pub fn get_geo_error_by_precision() -> (f64, f64) {
    const LAT_BITS: i32 = GeoHash::BITS_OF_PRECISION as i32 / 2;
    const LONG_BITS: i32 = GeoHash::BITS_OF_PRECISION as i32 - LAT_BITS;

    let lat_error = 180.0 * 2.0_f64.powi(-LAT_BITS);
    let long_error = 360.0 * 2.0_f64.powi(-LONG_BITS);
    (lat_error, long_error)
  }

  #[inline]
  /// 在 garnet 中的相对路径:libs/server/Objects/SortedSetGeo/GeoHash.cs:ConvertValueToMeters
  pub fn convert_value_to_meters(value: f64, unit: GeoDistanceUnitType) -> f64 {
    match unit {
      GeoDistanceUnitType::Km => value / 0.001,
      GeoDistanceUnitType::Ft => value / 3.280_84,
      GeoDistanceUnitType::Mi => value / 0.000_621_371,
      GeoDistanceUnitType::M => value,
    }
  }

  #[inline]
  /// 在 garnet 中的相对路径:libs/server/Objects/SortedSetGeo/GeoHash.cs:ConvertMetersToUnits
  pub fn convert_meters_to_units(value: f64, unit: GeoDistanceUnitType) -> f64 {
    match unit {
      GeoDistanceUnitType::Km => value * 0.001,
      GeoDistanceUnitType::Ft => value * 3.280_84,
      GeoDistanceUnitType::Mi => value * 0.000_621_371,
      GeoDistanceUnitType::M => value,
    }
  }
}
