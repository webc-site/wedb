//! 地理空间索引与计算模块

pub mod geo_hash;

use bitflags::bitflags;
pub use geo_hash::{GeoDistanceUnitType, GeoHash};

bitflags! {
  /// GEOADD 选项
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
  pub struct GeoAddOptions: u8 {
    /// 无选项
    const NONE = 0;
    /// 仅新增
    const NX = 1 << 0;
    /// 仅更新
    const XX = 1 << 1;
    /// 返回"新增+变更"总数
    const CH = 1 << 2;
  }
}

/// GEOSEARCH 结果排序方向
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum GeoOrder {
  /// 不排序
  #[default]
  None = 0,
  /// 距离升序
  Ascending,
  /// 距离降序
  Descending,
}

/// GEOSEARCH 圆心来源
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum GeoOriginType {
  #[default]
  Undefined = 0,
  /// 显式经纬度
  FromLonLat,
  /// 既有成员
  FromMember,
}

/// GEOSEARCH 形状
///
/// libs/server/Objects/SortedSetGeo/GeoSearchOptions.cs:GeoSearchType
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum GeoSearchType {
  #[default]
  Undefined = 0,
  /// 圆形（BYRADIUS）
  ByRadius,
  /// 轴对齐矩形（BYBOX）
  ByBox,
}

/// GEOSEARCH 选项束
///
/// libs/server/Objects/SortedSetGeo/GeoSearchOptions.cs:GeoSearchOptions
#[derive(Debug, Clone, Default)]
pub struct GeoSearchOptions {
  pub search_type: GeoSearchType,
  pub unit: GeoDistanceUnitType,
  /// 半径（BYRADIUS）；BYBOX 时复用为高度（box_height 的 C# getter/setter 即 radius）
  pub radius: f64,
  /// 矩形宽（BYBOX）
  pub box_width: f64,
  pub count_value: i64,
  pub origin: GeoOriginType,
  /// FROMMEMBER 的成员名
  pub from_member: Vec<u8>,
  /// 圆心坐标
  pub lon: f64,
  pub lat: f64,
  pub with_coord: bool,
  pub with_hash: bool,
  pub with_count_any: bool,
  pub with_dist: bool,
  pub sort: GeoOrder,
}
