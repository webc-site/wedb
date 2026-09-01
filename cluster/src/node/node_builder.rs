use std::sync::Arc;

use super::cluster;
use crate::conf::{ClusterMode, Conf, Endpoint, FjallConf, RaftConf, RedisConf};
use crate::error::Result;
use crate::node::RaftNode;

pub struct RaftNodeBuilder {
  node_id: u64,
  mode: ClusterMode,
  raft: RaftConf,
  fjall: FjallConf,
  redis: RedisConf,
}

impl RaftNodeBuilder {
  pub fn new(node_id: u64, endpoint: Endpoint) -> Self {
    Self {
      node_id,
      mode: ClusterMode::default(),
      raft: RaftConf {
        endpoint: endpoint.clone(),
        advertise_endpoint: endpoint,
        ..Default::default()
      },
      fjall: FjallConf::new(format!("./data/node_{node_id}")),
      redis: RedisConf::default(),
    }
  }

  pub fn with_advertise_endpoint(mut self, endpoint: Endpoint) -> Self {
    self.raft.advertise_endpoint = endpoint;
    self
  }

  pub fn with_join(mut self, join: Vec<String>) -> Self {
    self.raft.join = join;
    self
  }

  pub fn with_data_path(mut self, path: impl Into<String>) -> Self {
    self.fjall.data_path = path.into();
    self
  }

  pub fn with_redis_addr(mut self, addr: impl Into<String>) -> Self {
    self.redis.addr = addr.into();
    self
  }

  pub fn with_heartbeat_interval(mut self, interval_ms: u64) -> Self {
    self.raft.heartbeat_interval = Some(interval_ms);
    self
  }

  pub fn with_election_timeout(mut self, min_ms: u64, max_ms: u64) -> Self {
    self.raft.election_timeout_min = Some(min_ms);
    self.raft.election_timeout_max = Some(max_ms);
    self
  }

  pub fn with_redis_enabled(mut self, enabled: bool) -> Self {
    self.redis.enabled = enabled;
    self
  }

  pub fn with_mode(mut self, mode: ClusterMode) -> Self {
    self.mode = mode;
    self
  }

  pub fn with_cache_size(mut self, cache_size: usize) -> Self {
    self.fjall.cache_size = Some(cache_size);
    self
  }

  pub fn build_conf(&self) -> Conf {
    Conf {
      node_id: self.node_id,
      mode: self.mode,
      topology: None,
      raft: self.raft.clone(),
      fjall: self.fjall.clone(),
      redis: self.redis.clone(),
    }
  }

  pub async fn build(self) -> Result<Arc<RaftNode>> {
    let conf = self.build_conf();
    Self::from_conf(&conf).await
  }

  pub async fn from_conf(conf: &Conf) -> Result<Arc<RaftNode>> {
    cluster::start_cluster(conf).await
  }
}
