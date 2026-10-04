//! Wedb 向量数据提供者（对标微软官方 diskann-garnet 的 provider.rs）
//!
//! 实现 DiskANN 官方底层抽象：
//! - [`data_provider::WedbProvider`]：外部/内部 ID 映射桥接与存储底座适配
//! - [`dynamic_quant::DynamicQuantization`]：动态量化透明自适应状态机（全精度/量化双轨检索与剪枝策略）
//! - [`cache`]：邻接表与起点缓存管理
//! - [`callbacks`]：属性与向量持久化回调交互

pub(crate) mod cache;
pub(crate) mod callbacks;
pub mod data_provider;
pub(crate) mod dynamic_quant;

use webc_diskann::graph::config::defaults::MAX_OCCLUSION_SIZE;

/// filtered_ids 池容量单点：命中对 + 长度头余量（上游 MAX_OCCLUSION_SIZE=750
/// 的两倍，data_provider / dynamic_quant 三处池建共享）
pub(crate) const FILTERED_IDS_POOL_CAP: usize = MAX_OCCLUSION_SIZE.get() as usize * 2;

pub use data_provider::{DistanceComputer, QueryComputer};
pub(crate) use data_provider::{ToDistanceComputer, WedbProvider};
pub(crate) use dynamic_quant::DynamicQuantization;
