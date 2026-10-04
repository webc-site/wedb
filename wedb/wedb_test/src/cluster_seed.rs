//! 集群 config 预置单源（本地 worker + 对端行 + 全槽 Stable 本地，一次写锁）
//!
//! 收口 cluster_management / cluster_failover / failover_primary_probe /
//! failover_timeout_bounds / primary_task_role_gate / cluster_slot_verify 系 /
//! cluster_swapdb_slot_state / scan_key_gate / replication_assembly_e2e 等多册
//! 册逐字同形的 `current_config.write() + initialize_local_worker +
//! [workers.push] + [slot_map 全量 Stable]` 预置装配。差异面（节点身份、
//! 端口、角色、纪元、对端、槽位形态）全部显式参。消费面经 `wedb_test::cluster_seed` 引用（原 common/ 直挂面
//! 已收口进本 crate）。
//!

use wbase::hash_slot::CLUSTER_SLOT_COUNT;
use wedb::server::{
  cluster_config::ClusterConfig,
  hash_slot::{HashSlot, SlotState},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};

/// 现行 config 预置：本地 worker 写入 + 可选对端行追加 + 可选全槽 Stable
/// 本地填充（同一写锁内完成，与各册原 `{ let mut config = ... }` 作用域同义）
///
/// - `peer`：Some((node_id, port, config_epoch)) = 追加一台主角色对端行
///   （replication_offset 归零初值、hostname 空，各册原形态）
/// - `fill_slots`：true = 槽位图全量 Stable 本地（真 RESP 写命令门评走
///   Stable 本地臂）
///
/// 全部调用点角色恒为主角色、无 replica_of，两参就地写死不再外露。
pub fn seed_local_worker(
  config: &mut ClusterConfig,
  node_id: u128,
  port: i32,
  config_epoch: i64,
  peer: Option<(u128, i32, i64)>,
  fill_slots: bool,
) {
  config.initialize_local_worker(LocalWorkerSpec {
    node_id,
    address: "127.0.0.1",
    port,
    config_epoch,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: None,
  });
  if let Some((peer_id, peer_port, peer_epoch)) = peer {
    config.workers.push(Worker {
      nodeid: Some(peer_id),
      address: "127.0.0.1".into(),
      port: peer_port,
      config_epoch: peer_epoch,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      replication_offset: 0,
      hostname: None,
    });
  }
  if fill_slots {
    for s in 0..CLUSTER_SLOT_COUNT {
      config.slot_map[s] = HashSlot {
        worker_id: LOCAL_WORKER_ID as u16,
        state: SlotState::Stable,
      };
    }
  }
}
