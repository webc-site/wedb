//! 副本端会话供应器单源（GarnetServer 会话泵按消费面取接收会话）
//!
//! 收口 appendlog_reject_disconnect / replica_background_replay /
//! replication_stream_e2e / replication_end_to_end 四册逐字同形的
//! `struct SessionProvider(ClusterReplicationSession) + SessionProviderFace`
//! 实现：每连接克隆同一接收会话（线格式与网络发送端 ID 测试场景不区分）。
//! 消费面经 `wedb_test::replica_session_face` 引用（原 common/ 直挂面已收口进本 crate）。
//!

use std::sync::Arc;

use wdev::SegmentedDevice;
use wedb::server::replication::cluster_replication_session::ClusterReplicationSession;
use wnode::{SessionProviderFace, WireFormat, servers::ConsumerRegistry};

/// 副本端会话供应器（持接收会话，每连接克隆分发）
pub struct ReplicaSessionProvider {
  pub session: ClusterReplicationSession<SegmentedDevice>,
  pub registry: Arc<ConsumerRegistry>,
}

#[allow(non_snake_case)]
pub fn ReplicaSessionProvider(
  session: ClusterReplicationSession<SegmentedDevice>,
) -> ReplicaSessionProvider {
  ReplicaSessionProvider {
    session,
    registry: Arc::new(ConsumerRegistry::default()),
  }
}

impl SessionProviderFace for ReplicaSessionProvider {
  type Consumer = ClusterReplicationSession<SegmentedDevice>;

  fn get_session(
    &self,
    // 满足 SessionProviderFace trait 签名契约；测试场景无需区分线格式与网络发送端 ID
    _wire_format: WireFormat,
    _network_sender_id: u64,
  ) -> Option<ClusterReplicationSession<SegmentedDevice>> {
    Some(self.session.clone())
  }

  fn consumer_registry(&self) -> Option<Arc<ConsumerRegistry>> {
    Some(Arc::clone(&self.registry))
  }
}
