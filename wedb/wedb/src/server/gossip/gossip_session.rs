use std::sync::Arc;

use crate::{
  error::{Error, Result},
  server::{
    cluster_config::{CLUSTER_CONFIG_VERSION, ClusterConfig},
    cluster_provider::ClusterProvider,
    gossip::gossip_stats::GossipStats,
  },
};

/// libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterGossip
pub struct GossipSession {
  cluster_provider: Arc<ClusterProvider>,
  stats: Arc<GossipStats>,
}

impl GossipSession {
  pub fn new(cluster_provider: Arc<ClusterProvider>, stats: Arc<GossipStats>) -> Self {
    Self {
      cluster_provider,
      stats,
    }
  }

  pub fn handle_gossip(&self, data: &[u8], with_meet: bool) -> Result<Vec<u8>> {
    self.stats.update_gossip_bytes_recv(data.len() as i64);

    let cluster_mgr = self
      .cluster_provider
      .cluster_manager()
      .ok_or_else(|| Error::Gossip("ClusterManager not initialized".into()))?;

    let mut config_changed = false;
    // 对标 C# NetworkClusterGossip 的 activeSession.RemoteNodeId：入站 gossip
    // 呈现的远端节点 id（EnsureReplication 的会话归属判定用）
    let mut active_remote_node_id: Option<String> = None;

    if !data.is_empty() {
      if ClusterConfig::try_peek_version(data) != Some(CLUSTER_CONFIG_VERSION) {
        log::warn!("Received gossip with incompatible config version");
      } else if let Ok(other) = ClusterConfig::from_byte_array(data) {
        let is_known = other
          .local_node_id()
          .is_some_and(|id| cluster_mgr.current_config().is_known(id));

        if with_meet || is_known {
          active_remote_node_id = other.local_node_id().map(String::from);
          config_changed = cluster_mgr.try_merge(&other, true);
          if with_meet {
            self.stats.update_meet_requests_succeed();
          }
        }
      }
    }

    // 对标 C# NetworkClusterGossip: EnsureReplication 完整判定链
    //（节流 / REPLICA 角色 / IsReplicating 状态面 / failover 抑制 / 重连发起）
    self
      .cluster_provider
      .ensure_replication(active_remote_node_id.as_deref());

    if config_changed || with_meet {
      let current = cluster_mgr.current_config();
      let resp = current.to_byte_array();
      self.stats.update_gossip_bytes_send(resp.len() as i64);
      Ok(resp)
    } else {
      Ok(Vec::new())
    }
  }
}
