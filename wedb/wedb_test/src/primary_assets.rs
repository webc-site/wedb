//! 主端推流资产装配（primary_assets 叶子单源）
//!
//! 消费面经 `wedb_test::primary_assets` 引用（原 common/ #[path] 直挂面已
//! 收口进本 crate；per-binary dead_code 面随 crate 化消解）。

use std::sync::Arc;

use wedb::server::{
  cluster_provider::PrimaryReplicationAssets,
  replication::{
    aof_replication_pump::AofReplicationPump, replica_sync_session::ReplicaSyncSession,
    replication_manager::ReplicationManager,
  },
};

use crate::node_storage::NodeStorage;

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
