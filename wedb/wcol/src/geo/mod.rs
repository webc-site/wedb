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
