//! 主端推流资产装配（primary_assets 叶子单源）
//!
//! 仅被消费面宿主册以 `#[path]` 子模直挂；不进 common/mod.rs 聚合面——
//! 宿主二进制按册裁项，全量聚合会让不消费本面的册招 per-binary dead_code。

use std::sync::Arc;

use wedb::server::{
  cluster_provider::PrimaryReplicationAssets,
  replication::{
    aof_replication_pump::AofReplicationPump, replica_sync_session::ReplicaSyncSession,
    replication_manager::ReplicationManager,
  },
};

use crate::common::NodeStorage;

/// 主端发起无盘全量同步装配（推流泵与驱动同册，PrimaryReplicationAssets 形态）
pub fn primary_assets(
  node: &NodeStorage,
  rm: &Arc<ReplicationManager>,
) -> PrimaryReplicationAssets {
  PrimaryReplicationAssets {
    wal: Arc::clone(&node.wal),
    pump: Arc::new(AofReplicationPump::new(Arc::clone(
      &rm.aof_sync_driver_store,
    ))),
    sync_session: Arc::new(ReplicaSyncSession::new(Arc::clone(rm))),
  }
}
