//! 副本接收会话挂接单源（CLUSTER APPENDLOG 落盘重放面）
//!
//! 收口 diskless 系与检查点导入系逐字同形的 set_replica_replication_session
//! 三行装配。消费面经 `wedb_test::replica_attach` 引用（原 common/ 直挂面已收口进本 crate）。

use std::sync::Arc;

use waof::WalLog;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::ClusterProvider,
  replication::cluster_replication_session::ClusterReplicationSession,
};

/// 副本接收会话挂接（CLUSTER APPENDLOG 落盘重放，对标宿主 wire 装配面）
pub fn attach_replica_session(provider: &Arc<ClusterProvider>, wal: &Arc<WalLog<SegmentedDevice>>) {
  provider.set_replica_replication_session(Some(Arc::new(ClusterReplicationSession::new(
    Arc::clone(provider),
    Arc::clone(wal),
    None,
  ))));
}
