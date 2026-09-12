use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::{
  runtime::spawn,
  time::{sleep, timeout},
};
use log::{error, info, warn};

use crate::{
  error::{Error, Result},
  server::{
    cluster_config::{CLUSTER_CONFIG_VERSION, ClusterConfig},
    cluster_provider::ClusterProvider,
    gossip::{
      connection_store::ConnectionStore, gossip_stats::GossipStats, node_connection::NodeConnection,
    },
  },
};

/// libs/cluster/Server/Gossip/Gossip.cs:ClusterManager
pub struct GossipManager {
  cluster_provider: Arc<ClusterProvider>,
  pub stats: Arc<GossipStats>,
  pub connection_store: Arc<ConnectionStore>,
  pub gossip_delay: Duration,
  pub gossip_sample_percent: i32,
  is_running: AtomicBool,
}

impl GossipManager {
  pub fn new(cluster_provider: Arc<ClusterProvider>) -> Self {
    Self {
      cluster_provider,
      stats: Arc::new(GossipStats::new()),
      connection_store: Arc::new(ConnectionStore::new()),
      gossip_delay: Duration::from_millis(100),
      gossip_sample_percent: 100,
      is_running: AtomicBool::new(false),
    }
  }

  pub fn is_running(&self) -> bool {
    self.is_running.load(Ordering::Acquire)
  }

  /// 启动 Gossip 后台心跳与通信协程
  pub fn start(self: &Arc<Self>) {
    if self.is_running.swap(true, Ordering::AcqRel) {
      return;
    }
    let this = Arc::clone(self);
    spawn(async move {
      while this.is_running() {
        this.gossip_step_async().await;
        sleep(this.gossip_delay).await;
      }
    })
    .detach();
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:TryMeetAsync
  pub async fn try_meet_async(&self, address: &str, port: i32) -> Result<()> {
    self.stats.update_meet_requests_recv();
    let cluster_mgr = self
      .cluster_provider
      .cluster_manager()
      .ok_or_else(|| Error::Gossip("ClusterManager not initialized".into()))?;

    let config_bytes = {
      if let Some(rm) = self.cluster_provider.replication_manager() {
        cluster_mgr.lazy_update_local_replication_offset(rm.get_replication_offset(0));
      }
      let config = cluster_mgr.current_config();
      config.to_byte_array()
    };

    let temp_node_id = format!("{}:{}", address, port);
    let auth_user = self.cluster_provider.cluster_username();
    let auth_pwd = self.cluster_provider.cluster_password();
    let conn = self.connection_store.get_or_add_with_auth(
      &temp_node_id,
      address,
      port,
      auth_user.as_deref(),
      auth_pwd.as_deref(),
    );

    let meet_timeout = self.gossip_delay.max(Duration::from_secs(3));
    match timeout(meet_timeout, conn.try_meet_async(&config_bytes)).await {
      Ok(Ok(resp)) if !resp.is_empty() => {
        let other = match ClusterConfig::from_byte_array(&resp) {
          Ok(c) => c,
          Err(e) => {
            self.stats.update_meet_requests_failed();
            warn!(
              "MEET response deserialization failed from {}:{}: {:?}",
              address, port, e
            );
            return Err(e);
          }
        };

        if let Some(target_id) = other.local_node_id() {
          info!("MEET {} {}:{} successful", target_id, address, port);
          cluster_mgr.try_merge(&other, true);
          self.connection_store.try_remove(&temp_node_id);
          self.connection_store.get_or_add_with_auth(
            target_id,
            address,
            port,
            auth_user.as_deref(),
            auth_pwd.as_deref(),
          );
        }
        self.stats.update_meet_requests_succeed();
        Ok(())
      }
      Ok(Ok(_)) => {
        self.stats.update_meet_requests_succeed();
        Ok(())
      }
      Ok(Err(e)) => {
        self.stats.update_meet_requests_failed();
        error!("MEET {}:{} failed: {:?}", address, port, e);
        Err(e)
      }
      Err(_) => {
        self.stats.update_meet_requests_failed();
        error!("MEET {}:{} timed out", address, port);
        Err(Error::Gossip(format!("MEET {address}:{port} timed out")))
      }
    }
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:GossipMainAsync
  pub async fn gossip_step_async(&self) {
    let Some(cluster_mgr) = self.cluster_provider.cluster_manager() else {
      return;
    };

    // 1. 清理过期封禁节点连接并移除处于封禁中的连接（对标 C# DisposeBannedWorkerConnectionsAsync）
    cluster_mgr.cleanup_ban_list();
    {
      let ban_list = cluster_mgr.worker_ban_list.read();
      for nid in ban_list.keys() {
        self.connection_store.try_remove(nid);
      }
    }

    // 2. 同步当前已知的集群节点并建立连接
    let auth_user = self.cluster_provider.cluster_username();
    let auth_pwd = self.cluster_provider.cluster_password();
    {
      let config = cluster_mgr.current_config();
      for w in &config.workers[1..=config.num_workers()] {
        if let Some(ref nid) = w.nodeid
          && !cluster_mgr.is_banned(nid)
          && !config.local_node_id().is_some_and(|lid| lid == nid)
        {
          self.connection_store.get_or_add_with_auth(
            nid,
            &w.address,
            w.port,
            auth_user.as_deref(),
            auth_pwd.as_deref(),
          );
        }
      }
    }

    // 3. 广播或者抽样 Gossip
    if self.gossip_sample_percent >= 100 {
      self.broadcast_gossip_async().await;
    } else {
      self.sample_gossip_async().await;
    }
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:BroadcastGossipSendAsync
  pub async fn broadcast_gossip_async(&self) {
    let mut offset = 0;
    while let Some(conn) = self.connection_store.get_connection_at_offset(offset) {
      if self.gossip_to_peer_async(&conn).await {
        offset += 1;
      }
    }
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:GossipSampleSendAsync
  pub async fn sample_gossip_async(&self) {
    let total = self.connection_store.count();
    if total == 0 {
      return;
    }
    let count = ((total as f64 * (self.gossip_sample_percent as f64 / 100.0)).ceil() as usize)
      .clamp(1, total);
    let start_time = coarsetime::Clock::now_since_epoch().as_millis() as i64;
    for _ in 0..count {
      let mut min_send = start_time;
      let mut curr_node = None;
      for _ in 0..3 {
        if let Some(conn) = self.connection_store.get_random_connection() {
          let send = conn.last_send.load(Ordering::Acquire);
          if send < min_send {
            min_send = send;
            curr_node = Some(conn);
          }
        }
      }
      if let Some(conn) = curr_node {
        self.gossip_to_peer_async(&conn).await;
      } else {
        break;
      }
    }
  }

  /// libs/cluster/Server/Gossip/GarnetServerNode.cs:TryGossip
  async fn gossip_to_peer_async(&self, conn: &NodeConnection) -> bool {
    let Some(cluster_mgr) = self.cluster_provider.cluster_manager() else {
      return false;
    };

    let (config_bytes, current_epoch, is_full) = {
      if let Some(rm) = self.cluster_provider.replication_manager() {
        cluster_mgr.lazy_update_local_replication_offset(rm.get_replication_offset(0));
      }
      let config = cluster_mgr.current_config();
      let epoch = config.local_node_config_epoch();
      let first_send = !conn.has_sent_full.swap(true, Ordering::SeqCst);
      if first_send || conn.last_sent_epoch.load(Ordering::Relaxed) != epoch {
        (config.to_byte_array(), epoch, true)
      } else {
        (Vec::new(), epoch, false)
      }
    };

    if is_full {
      self.stats.gossip_full_send.fetch_add(1, Ordering::Relaxed);
      self
        .stats
        .update_gossip_bytes_send(config_bytes.len() as i64);
    } else {
      self.stats.gossip_empty_send.fetch_add(1, Ordering::Relaxed);
    }

    match timeout(self.gossip_delay, conn.try_gossip_async(&config_bytes)).await {
      Ok(Ok(resp)) => {
        conn.last_sent_epoch.store(current_epoch, Ordering::Relaxed);
        self
          .stats
          .gossip_success_count
          .fetch_add(1, Ordering::Relaxed);
        if !resp.is_empty() {
          self.stats.update_gossip_bytes_recv(resp.len() as i64);
          if ClusterConfig::try_peek_version(&resp) == Some(CLUSTER_CONFIG_VERSION) {
            if let Ok(other) = ClusterConfig::from_byte_array(&resp)
              && let Some(other_id) = other.local_node_id()
            {
              if cluster_mgr.current_config().is_known(other_id) {
                cluster_mgr.try_merge(&other, true);
              } else {
                warn!("Received gossip from unknown node: {other_id}");
              }
            }
          } else {
            warn!("Received gossip response with incompatible config version");
          }
        }
        true
      }
      Ok(Err(e)) => {
        self
          .stats
          .gossip_failed_count
          .fetch_add(1, Ordering::Relaxed);
        warn!("GOSSIP to remote node {} failed: {e:?}", conn.node_id);
        self.connection_store.try_remove(&conn.node_id);
        false
      }
      Err(_) => {
        self
          .stats
          .gossip_timeout_count
          .fetch_add(1, Ordering::Relaxed);
        warn!("GOSSIP to remote node {} timeout!", conn.node_id);
        self.connection_store.try_remove(&conn.node_id);
        false
      }
    }
  }

  /// 终止 Gossip 轮询并断开所有节点连接
  pub fn dispose(&self) {
    self.is_running.store(false, Ordering::Release);
    self.connection_store.dispose();
  }
}
