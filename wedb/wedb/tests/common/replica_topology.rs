//! 副本复制域拓扑装配单源（provider + rm + 集群配置面）
//!
//! 收口 replica_recover_clamp_partial_resync / replica_replay_truncate_clamp /
//! replica_background_replay / replica_driver_store_generation /
//! migrate_source_vector_set_replica_converge 五册逐字同形的拓扑预置装配：
//! provider 初始化复制管理器、集群配置预置副本角色本地 worker + 主端地址簿
//! Worker 行、双向写回（与 common/mod.rs 的 provider_with_role 同族，差异面
//! 为显式 push 主端 Worker 行）。provider 构造形由调用方传入（default 裸壳 /
//! new 全组件），装配只增补复制管理器与集群配置。宿主册直挂（沿用
//! primary_assets 先例）：
//!
//! ```text
//! #[path = "common/replica_topology.rs"]
//! mod replica_topology_core;
//! use replica_topology_core::replica_topology_with;
//! ```
//!
//! 不进 common/mod.rs 聚合面——按册直挂裁项，避免不消费册招 per-binary
//! dead_code（primary_assets 先例警示）。

use std::sync::Arc;

use hipstr::HipStr;
use wedb::server::{
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  replication::replication_manager::ReplicationManager,
  worker::{LocalWorkerSpec, NodeRole, Worker},
};

/// 副本复制域拓扑装配（拓扑预置：副本角色 + 主端地址簿；process_append_log
/// 按主端 id 注册驱动）。返回（provider, rm）：provider 集群配置已写回
///（set_store / set_wal / set_primary_tasks 等由册自装配），rm 为复制管理器
/// 句柄（rm ready 断言在场）
///
/// - `provider`：构造形由调用方选定（`Arc::new(ClusterProvider::default())`
///   裸壳 / `ClusterProvider::new()` 全组件）
/// - `replica_id`：本地 worker 身份（副本角色，端口 7001，挂靠 `primary_id`）
/// - `primary_id`：主端地址簿行身份（端口 7000，replication_offset 归零初值）
/// - `primary_hostname`：主端地址簿行主机名（None = 不带）
pub fn replica_topology_with(
  provider: Arc<ClusterProvider>,
  replica_id: u128,
  primary_id: u128,
  primary_hostname: Option<&str>,
) -> (Arc<ClusterProvider>, Arc<ReplicationManager>) {
  provider.initialize_replication_manager(1, None, false);
  let rm = provider.replication_manager().expect("rm ready");

  let cm = Arc::new(ClusterManager::new(Arc::clone(&provider)));
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: replica_id,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Replica,
    replica_of_node_id: Some(primary_id),
    hostname: None,
  });
  config.workers.push(Worker {
    nodeid: Some(primary_id),
    address: "127.0.0.1".into(),
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: primary_hostname.map(HipStr::from),
  });
  *cm.current_config.write() = config;
  *provider.cluster_manager.write() = Some(cm);
  (provider, rm)
}
