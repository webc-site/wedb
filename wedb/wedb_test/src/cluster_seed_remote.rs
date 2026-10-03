//! 对端地址簿行追加单源（主角色 Worker 行 + 行号返回）
//!
//! 收口 cluster_failover / failover_epoch_drain_failclose /
//! failover_timeout_bounds 三册逐字同形的「push 对端行 + 取追加后 worker_id」
//! 装配（`assign_slots` 指派槽位需行号）。消费面经
//! `wedb_test::cluster_seed_remote` 引用（原 common/ 直挂面已收口进本 crate）。
//!

use wedb::server::{
  cluster_config::ClusterConfig,
  worker::{NodeRole, Worker},
};

/// 追加一台主角色对端 Worker 行（replication_offset 归零初值、hostname 空），
/// 返回该行 worker_id（追加后末位下标）
pub fn push_remote_worker(
  config: &mut ClusterConfig,
  node_id: u128,
  address: &str,
  port: i32,
  config_epoch: i64,
) -> u16 {
  config.workers.push(Worker {
    nodeid: Some(node_id),
    address: address.into(),
    port,
    config_epoch,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  });
  (config.workers.len() - 1) as u16
}
