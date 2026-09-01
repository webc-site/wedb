#![recursion_limit = "256"]

pub mod conf;
pub mod error;
pub mod node;
pub mod redis;
pub mod service;
pub mod util;

use compio::signal;
pub use conf::{Conf, FjallConf, RaftConf, RedisConf, TopologyConf};
pub use error::{Error, Result};
pub use node::{LeaderHandler, RaftNode, RaftNodeBuilder};
pub use redis::RedisServer;
use std::net::SocketAddr;

pub async fn run_cluster_node(
  node_id: u64,
  redis_addr: &str,
  raft_addr: &str,
  data_dir: &str,
  join: Vec<String>,
) -> aok::Result<()> {
  run_cluster_node_with_topology(node_id, redis_addr, raft_addr, data_dir, join, None).await
}

pub async fn run_cluster_node_with_conf(conf: &Conf) -> aok::Result<()> {
  for addr in &conf.raft.join {
    if let Err(e) = addr.parse::<SocketAddr>() {
      return Err(aok::anyhow!(
        "Invalid join address '{addr}': cannot parse as socket address ({e})"
      ));
    }
  }

  let node = RaftNodeBuilder::from_conf(conf).await?;
  log::info!("Cluster node #{} started", conf.node_id);

  let redis_server = if conf.redis.enabled {
    Some(RedisServer::start(node.clone(), conf.redis.addr.clone()).await?)
  } else {
    None
  };

  signal::ctrl_c().await?;
  if let Some(s) = redis_server {
    s.shutdown().await?;
  }
  node.shutdown().await?;
  Ok(())
}

pub async fn run_cluster_node_with_topology(
  node_id: u64,
  redis_addr: &str,
  raft_addr: &str,
  data_dir: &str,
  join: Vec<String>,
  topology: Option<conf::TopologyConf>,
) -> aok::Result<()> {
  let endpoint: wedb_raft::Endpoint = raft_addr.parse()?;
  let conf = conf::Conf {
    node_id,
    mode: conf::ClusterMode::default(),
    topology,
    raft: conf::RaftConf {
      endpoint: endpoint.clone(),
      advertise_endpoint: endpoint,
      join,
      heartbeat_interval: None,
      election_timeout_min: None,
      election_timeout_max: None,
    },
    fjall: conf::FjallConf::new(data_dir),
    redis: conf::RedisConf {
      addr: redis_addr.to_string(),
      enabled: true,
    },
  };
  run_cluster_node_with_conf(&conf).await
}
