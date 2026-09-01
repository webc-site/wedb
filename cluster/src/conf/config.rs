use super::endpoint::Endpoint;
use serde::{Deserialize, Serialize};

/// 节点地理位置与故障域拓扑配置
#[derive(Debug, Clone, Default, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
#[serde(default)]
pub struct TopologyConf {
  pub region: Option<String>,
  pub zone: Option<String>,
  pub rack: Option<String>,
  pub host: Option<String>,
  pub weight: Option<u32>,
  /// 显式配置当前节点的 Redis 端口（支持异构端口部署，无需依赖与 Raft 端口差值推算）
  pub redis_port: Option<u16>,
}

/// 集群运行模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, bitcode::Encode, bitcode::Decode)]
pub enum ClusterMode {
  /// 纯分片多主直写模式（默认模式，1:1 对标 Apache Kvrocks / Redis Cluster，各节点独立直写本地存储引擎，超高吞吐）
  #[default]
  Sharding,
  /// Raft 强一致共识多副本模式（多数派 Quorum 提交，RPO=0 强一致保障）
  Raft,
}

#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
pub struct Conf {
  pub node_id: u64,
  pub mode: ClusterMode,
  pub topology: Option<TopologyConf>,
  pub raft: RaftConf,
  pub fjall: FjallConf,
  pub redis: RedisConf,
}

#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
pub struct RaftConf {
  pub endpoint: Endpoint,
  pub advertise_endpoint: Endpoint,
  pub join: Vec<String>,
  pub heartbeat_interval: Option<u64>,
  pub election_timeout_min: Option<u64>,
  pub election_timeout_max: Option<u64>,
}

pub use wedb_embed::Conf as FjallConf;

#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
pub struct RedisConf {
  pub addr: String,
  pub enabled: bool,
}

impl Default for RaftConf {
  fn default() -> Self {
    Self {
      endpoint: Endpoint::new("127.0.0.1", 4910),
      advertise_endpoint: Endpoint::new("127.0.0.1", 4910),
      join: Vec::new(),
      heartbeat_interval: Some(50),
      election_timeout_min: Some(150),
      election_timeout_max: Some(300),
    }
  }
}

impl Default for RedisConf {
  fn default() -> Self {
    Self {
      addr: "127.0.0.1:6379".to_string(),
      enabled: true,
    }
  }
}

impl RaftConf {
  pub fn to_raft_config(&self) -> zenoh_raft::Config {
    let mut config = zenoh_raft::Config::default();
    let hb = self.heartbeat_interval.unwrap_or(50);
    config.heartbeat_interval = hb;

    // 使用 fastrand 在合理的区间内动态抖动选举超时，无需外部繁琐配置
    let (min, max) = match (self.election_timeout_min, self.election_timeout_max) {
      (Some(min), Some(max)) => (min, max),
      (Some(min), None) => (min, min.saturating_mul(2)),
      (None, Some(max)) => (max.saturating_div(2).max(hb * 2), max),
      (None, None) => {
        let base_min = hb.saturating_mul(3);
        let jitter_min = base_min + fastrand::u64(0..=hb);
        let jitter_max = jitter_min + fastrand::u64(hb..=hb.saturating_mul(3));
        (jitter_min, jitter_max)
      }
    };

    config.election_timeout_min = min;
    config.election_timeout_max = max.max(min + 10);
    config.max_payload_entries = 1000;
    config.purge_batch_size = 2048;
    config.snapshot_max_chunk_size = 512 * 1024;
    config
  }
}
