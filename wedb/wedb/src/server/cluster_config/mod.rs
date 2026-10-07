//! 集群拓扑视图 `ClusterConfig`：类型定义与构造面。查询/变更方法按主题分域：
//! [`local_node`] 本地节点查询与本地 worker 状态、[`topology`] worker 拓扑查找
//! 与合并、[`slots`] slot 状态读写。
//!
//! 对位 garnet/libs/cluster/Server/ClusterConfig.cs。

pub mod serializer;

mod local_node;
mod slots;
mod topology;

use std::{array::from_fn, net::SocketAddr};

pub use serializer::CLUSTER_CONFIG_VERSION;
use wbase::hash_slot::CLUSTER_SLOT_COUNT;

// worker id 常量定义域在 [`crate::server::worker`]，此处转出口维持
// 槽位/配置方法群的单一引用路径
pub use crate::server::{
  cluster::ClusterPreferredEndpointType,
  worker::{LOCAL_WORKER_ID, RESERVED_WORKER_ID},
};
use crate::server::{hash_slot::HashSlot, worker::Worker};

/// worker 宣告地址 + 端口换算为 `SocketAddr`；地址非法（含 0 号位 "unassigned"）
/// 返回 None。收敛 cluster_config 域内 5 处 `address.parse().ok() → SocketAddr::new(ip, port as u16)`
/// 的地址解析与端口换算成对样板（语义逐字节等价：地址按 `IpAddr` 解析、端口 `as u16`；
/// 端口域 0..=65535 由序列化器解码门前置保证，见 serializer `from_byte_array`，
/// 本地写入面仅收监听器合法端口，`as u16` 换算恒安全）
fn socket_of(w: &Worker) -> Option<SocketAddr> {
  let ip = w.address.parse().ok()?;
  Some(SocketAddr::new(ip, w.port as u16))
}

/// libs/cluster/Server/ClusterConfig.cs
/// libs/cluster/Server/ClusterConfig.cs:Copy
///
/// C# `Copy()` 以两次 `Array.Copy` 逐元素深拷贝 slotMap/workers 后重建实例，
/// rust 侧同义承接为 `#[derive(Clone)]`（`slot_map: Box<[HashSlot; N]>` 与
/// `workers: Vec<Worker>` 均为深拷贝语义），活调用点
/// `failover_session.rs` `cm.current_config().clone()`，故不单设 `copy()` 件。
#[derive(Clone)]
pub struct ClusterConfig {
  pub slot_map: Box<[HashSlot; CLUSTER_SLOT_COUNT]>,
  pub workers: Vec<Worker>,
}

impl Default for ClusterConfig {
  fn default() -> Self {
    Self::new()
  }
}

impl ClusterConfig {
  /// libs/cluster/Server/ClusterConfig.cs:ClusterConfig
  pub fn new() -> Self {
    // 数组索引即为槽位号，闭包参数显式命名为 _slot_idx
    let slot_map = Box::new(from_fn(|_slot_idx| HashSlot::default()));
    let workers = vec![Worker::default(); 2];
    let mut config = Self { slot_map, workers };
    config.initialize_unassigned_worker();
    config
  }

  /// libs/cluster/Server/ClusterConfig.cs:InitializeUnassignedWorker
  fn initialize_unassigned_worker(&mut self) {
    self.workers[RESERVED_WORKER_ID] = Worker::unassigned();
  }
}
