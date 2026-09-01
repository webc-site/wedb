use std::collections::BTreeMap;
use std::future::Future;
use std::io::Error as IoError;
use std::net::SocketAddr;
use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, error, info, warn};
use zenoh::handlers::FifoChannelHandler;
use zenoh::qos::{CongestionControl, Priority};
use zenoh::query::Query;
use zenoh::sample::SampleKind;
use zenoh_raft::error::{InitializeError, RaftError};

use super::RaftNode;
use crate::conf::Conf;
use crate::error::{Error, Result};
use crate::service::RaftServiceImpl;
use crate::util::sleep;
use wedb_raft::network::{
  FORWARD_BROADCAST_KEY, LIVELINESS_SUB_PATTERN, raft_append_key, raft_forward_key,
  raft_liveliness_key, raft_snapshot_key, raft_vote_key,
};
use wedb_raft::types::{
  ForwardRequest, ForwardResponse, JoinRequest, Node, RequestPayload, decode, encode,
};

#[inline]
fn spawn_query_worker<F, Fut>(rx: FifoChannelHandler<Query>, handler: F)
where
  F: Fn(Query) -> Fut + Send + Sync + 'static,
  Fut: Future<Output = ()> + Send + 'static,
{
  let handler = Arc::new(handler);
  zenoh_runtime::ZRuntime::Application.spawn(async move {
    while let Ok(query) = rx.recv_async().await {
      let h = handler.clone();
      zenoh_runtime::ZRuntime::Application.spawn(async move {
        h(query).await;
      });
    }
  });
}

impl RaftNode {
  pub(crate) async fn start_raft_service(raft_node: Arc<Self>) -> Result<()> {
    let node_id = raft_node.conf.node_id;
    let raft_service = Arc::new(RaftServiceImpl::new(raft_node.clone()));

    let append_key = raft_append_key(node_id);
    let vote_key = raft_vote_key(node_id);
    let snapshot_key = raft_snapshot_key(node_id);
    let forward_key = raft_forward_key(node_id);

    let q_append = raft_node
      .session
      .declare_queryable(&append_key)
      .await
      .map_err(|e| Error::internal(format!("Failed to declare queryable {append_key}: {e}")))?;
    let q_vote = raft_node
      .session
      .declare_queryable(&vote_key)
      .await
      .map_err(|e| Error::internal(format!("Failed to declare queryable {vote_key}: {e}")))?;
    let q_snapshot = raft_node
      .session
      .declare_queryable(&snapshot_key)
      .await
      .map_err(|e| Error::internal(format!("Failed to declare queryable {snapshot_key}: {e}")))?;
    let q_forward = raft_node
      .session
      .declare_queryable(&forward_key)
      .await
      .map_err(|e| Error::internal(format!("Failed to declare queryable {forward_key}: {e}")))?;

    let s = raft_service.clone();
    spawn_query_worker(q_append.clone(), move |q| {
      let s = s.clone();
      async move { s.handle_append_entries_query(q).await }
    });

    let s = raft_service.clone();
    spawn_query_worker(q_vote.clone(), move |q| {
      let s = s.clone();
      async move { s.handle_vote_query(q).await }
    });

    let s = raft_service.clone();
    spawn_query_worker(q_snapshot.clone(), move |q| {
      let s = s.clone();
      async move { s.handle_snapshot_query(q).await }
    });

    let s = raft_service.clone();
    spawn_query_worker(q_forward.clone(), move |q| {
      let s = s.clone();
      async move { s.handle_forward_query(q).await }
    });

    let liveliness_key = raft_liveliness_key(node_id);
    let liveliness_token = raft_node
      .session
      .liveliness()
      .declare_token(&liveliness_key)
      .await
      .map_err(|e| {
        Error::internal(format!(
          "Failed to declare liveliness token {liveliness_key}: {e}"
        ))
      })?;

    let liveliness_sub = raft_node
      .session
      .liveliness()
      .declare_subscriber(LIVELINESS_SUB_PATTERN)
      .await
      .map_err(|e| Error::internal(format!("Failed to declare liveliness subscriber: {e}")))?;

    let rx_live = liveliness_sub.clone();
    let node_live = raft_node.clone();
    zenoh_runtime::ZRuntime::Application.spawn(async move {
      while let Ok(sample) = rx_live.recv_async().await {
        if sample.kind() == SampleKind::Delete {
          let key = sample.key_expr().as_str();
          if let Some(dead_node_str) = key
            .strip_prefix("wedb/raft/")
            .and_then(|s| s.strip_suffix("/liveliness"))
            && let Ok(dead_id) = dead_node_str.parse::<u64>()
            && dead_id != node_live.conf.node_id
            && let Some(leader) = node_live.raft().current_leader().await
            && leader == dead_id
          {
            log::warn!("Leader {dead_id} liveliness token lost, triggering proactive election");
            let _ = node_live.raft().trigger().elect(true).await;
          }
        }
      }
    });

    *raft_node._queryable.lock().unwrap() = Some(Box::new((
      q_append,
      q_vote,
      q_snapshot,
      q_forward,
      liveliness_token,
      liveliness_sub,
    )));
    info!(
      "Raft Zenoh service listening on wedb/raft/{node_id}/{{append_entries,vote,snapshot,forward,liveliness}}"
    );
    Ok(())
  }
}

pub(crate) async fn start_cluster(conf: &Conf) -> Result<Arc<RaftNode>> {
  let raft_node = RaftNode::create(conf).await?;

  RaftNode::start_raft_service(raft_node.clone()).await?;

  raft_node.init_or_join_cluster().await?;

  Ok(raft_node)
}

impl RaftNode {
  async fn init_or_join_cluster(&self) -> Result<()> {
    if self.is_empty_cluster()? && self.conf.raft.join.is_empty() {
      self.init_cluster().await?;
    } else {
      self.join_cluster().await?;
    }

    Ok(())
  }

  pub fn is_empty_cluster(&self) -> Result<bool> {
    let sm = &self.state_machine;
    let last_applied_log = sm.get_last_applied_log_id()?;
    let nodes = sm.get_nodes()?;
    Ok(last_applied_log.is_none() && nodes.is_empty())
  }

  pub fn is_in_cluster(&self) -> Result<bool> {
    let node_id = *self.raft.node_id();
    Ok(self.state_machine.contains_node(node_id)?)
  }

  async fn init_cluster(&self) -> Result<()> {
    let mut nodes = BTreeMap::new();
    let node_id = *self.raft.node_id();
    let endpoint = self.conf.raft.advertise_endpoint.clone();
    let node = Node {
      node_id,
      endpoint: endpoint.clone(),
    };

    let sm = &self.state_machine;
    sm.add_node(node.clone())?;

    nodes.insert(node_id, node);

    info!("Initializing single node cluster with node_id: {node_id}, endpoint: {endpoint}");

    match self.raft.initialize(nodes).await {
      Ok(_) => {
        info!("Cluster initialized successfully");
        Ok(())
      }
      Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {
        info!("Cluster already initialized, skipping");
        Ok(())
      }
      Err(e) => {
        error!("Failed to initialize cluster: {e}");
        Err(Error::internal(format!(
          "Failed to initialize cluster: {e}"
        )))
      }
    }
  }

  async fn join_cluster(&self) -> Result<()> {
    if self.is_in_cluster()? {
      info!("Node already in cluster, loaded available nodes from local metadata database");
      return Ok(());
    }

    let mut join_nodes = self.conf.raft.join.clone();

    // 若 CLI/配置未提供 join，但本地元数据库已有记录，启动时自动加载并尝试通信
    if join_nodes.is_empty() {
      let sm_nodes = self.state_machine.get_nodes()?;
      for node in sm_nodes.values() {
        let ep = node.endpoint.to_string();
        if !join_nodes.contains(&ep) {
          join_nodes.push(ep);
        }
      }
    }

    if join_nodes.is_empty() {
      info!(
        "No join endpoints configured and no cached nodes in local metadata database, skipping join"
      );
      return Ok(());
    }

    self.do_join_cluster_with_addrs(&join_nodes).await?;
    Ok(())
  }

  async fn do_join_cluster_with_addrs(&self, addrs: &[String]) -> Result<()> {
    let conf = &self.conf;
    let mut errors = Vec::with_capacity(addrs.len());
    let raft_address = conf.raft.endpoint.to_string();
    let raft_advertise_address = conf.raft.advertise_endpoint.to_string();

    for addr in addrs {
      // 严格校验 Join 节点地址格式必须为合法的 SocketAddr (IP:PORT)
      if let Err(e) = addr.parse::<SocketAddr>() {
        return Err(Error::conf(format!(
          "Invalid join address '{addr}': must be standard IP:PORT format ({e})"
        )));
      }

      if addr == &raft_address || addr == &raft_advertise_address {
        debug!("Ignore join cluster via self node address {addr}");
        continue;
      }

      match self.join_with_retry(addr).await {
        Ok(()) => return Ok(()),
        Err(e) => errors.push(e),
      }
    }

    let node_id = self.raft().node_id();
    let err_summary = errors
      .iter()
      .map(|e| e.to_string())
      .collect::<Vec<_>>()
      .join(", ");
    Err(Error::internal(format!(
      "Fail to join node-{node_id} to cluster via {addrs:?}, errors: {err_summary}"
    )))
  }

  async fn join_with_retry(&self, addr: &str) -> Result<()> {
    let mut last_err = None;
    for attempt in 0..5 {
      match self.join_via(addr).await {
        Ok(()) => {
          info!("Successfully joined cluster via {addr}");
          return Ok(());
        }
        Err(e) if e.is_retryable() => {
          let delay = Duration::from_millis(100 * (attempt + 1));
          warn!("Retryable error connecting to {addr}, retrying in {delay:?}: {e}");
          sleep(delay).await;
          last_err = Some(e);
        }
        Err(e) => return Err(e),
      }
    }
    Err(last_err.unwrap_or_else(|| Error::internal("Join retry attempts exhausted")))
  }

  async fn join_via(&self, _addr: &str) -> Result<()> {
    let conf = &self.conf;

    let join_req = JoinRequest {
      node_id: conf.node_id,
      endpoint: conf.raft.advertise_endpoint.clone(),
    };

    let req = ForwardRequest::new(RequestPayload::Join(join_req));
    let data = encode(&req).map_err(|e| Error::internal(format!("encode failed: {e}")))?;

    // 向集群广播/路由 Join 查询
    let replies = self
      .session
      .get(FORWARD_BROADCAST_KEY)
      .congestion_control(CongestionControl::Block)
      .priority(Priority::InteractiveHigh)
      .express(true)
      .payload(data)
      .timeout(Duration::from_secs(5))
      .await
      .map_err(|e| Error::retryable(IoError::other(format!("Zenoh get error: {e}"))))?;

    let mut last_err = None;
    while let Ok(reply) = replies.recv_async().await {
      let sample = match reply.result() {
        Ok(s) => s,
        Err(e) => {
          last_err = Some(Error::retryable(IoError::other(format!(
            "Zenoh reply error: {e:?}"
          ))));
          continue;
        }
      };

      let reply_res: StdResult<ForwardResponse, String> =
        match decode(sample.payload().to_bytes().as_ref()) {
          Ok(r) => r,
          Err(e) => {
            last_err = Some(Error::internal(format!("decode failed: {e}")));
            continue;
          }
        };

      match reply_res {
        Ok(_) => {
          let node = Node {
            node_id: conf.node_id,
            endpoint: conf.raft.advertise_endpoint.clone(),
          };
          self.state_machine.add_node(node)?;
          return Ok(());
        }
        Err(err_str) => {
          last_err = Some(Error::internal(format!("Join failed: {err_str}")));
        }
      }
    }

    Err(
      last_err.unwrap_or_else(|| {
        Error::retryable(IoError::other("No replies received for join request"))
      }),
    )
  }
}
