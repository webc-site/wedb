//! INFO 各段指标的填充与序列化
//!（对标 libs/server/Metrics/Info/GarnetInfoMetrics.cs:GarnetInfoMetrics）。
//!
//! 目录化拆分（门面：模块声明 + 转出口，外部路径不变）：
//! - [`snapshots`]：库/读缓存/AOF/全局指标/服务器五张快照 struct
//! - [`provider`]：InfoProvider 数据源 trait + GarnetInfoMetrics 聚合实现
//! - [`tables`]：段集合常量与五张 const 行表
//! - [`sections`]：populate_* 段填充函数族

mod provider;
mod sections;
mod snapshots;
mod tables;

pub use self::{
  provider::{GarnetInfoMetrics, InfoProvider},
  snapshots::{AofSnapshot, DbSnapshot, GlobalMetricsSnapshot, ReadCacheSnapshot, ServerFacts},
  tables::{ALL_INFO_SET, DEFAULT_HEX_ID, DEFAULT_INFO},
};
