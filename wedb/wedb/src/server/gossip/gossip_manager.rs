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
use wbase::time::now_ms;

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
  is_running: AtomicBool,
}

impl GossipManager {
  pub fn new(cluster_provider: Arc<ClusterProvider>) -> Self {
    Self {
      cluster_provider,
      stats: Arc::new(GossipStats::new()),
      connection_store: Arc::new(ConnectionStore::new()),
      is_running: AtomicBool::new(false),
    }
  }

  pub fn is_running(&self) -> bool {
    self.is_running.load(Ordering::Acquire)
  }

  /// gossip 周期（对标 clusterManager.gossipDelay =
  /// TimeSpan.FromSeconds(serverOptions.GossipDelay)，live 读装配槽位）
  #[inline]
  fn gossip_delay(&self) -> Duration {
    Duration::from_millis(self.cluster_provider.gossip_delay_ms())
  }

  /// gossip 抽样百分比（对标 ClusterManager.GossipSamplePercent）
  #[inline]
  fn gossip_sample_percent(&self) -> i32 {
    self.cluster_provider.gossip_sample_percent()
  }

  /// Gossip.cs TryStartGossipTasks 的实现体（C# 方法在 ClusterManager，
  /// 映射见 ClusterManager::try_start_gossip_tasks）
  ///
  /// 启动先对全部已知 worker 跑一轮 MEET（对标 RunMeetTask = Task.Run
  /// (TryMeetAsync)，恢复上线的节点据此尽快重入集群），再起 gossip 主循环
  pub fn start(self: &Arc<Self>) {
    if self.is_running.swap(true, Ordering::AcqRel) {
      return;
    }
    if let Some(cluster_mgr) = self.cluster_provider.cluster_manager() {
      let config = cluster_mgr.current_config();
      // worker id 自 2 起（0 保留 unassigned、1 本地），对标 for i = 2; i <= NumWorkers
      for worker_id in 2..=config.num_workers() {
        let (address, port) = config.get_worker_address(worker_id as u16);
        let this = Arc::clone(self);
        spawn(async move {
          if let Err(e) = this.try_meet_async(&address, port).await {
            warn!("Startup MEET {address}:{port} failed: {e:?}");
          }
        })
        .detach();
      }
    }
    let this = Arc::clone(self);
    spawn(async move {
      while this.is_running() {
        this.gossip_step_async().await;
        sleep(this.gossip_delay()).await;
      }
    })
    .detach();
  }

  /// libs/cluster/Server/Gossip/Gossip.cs:TryMeetAsync
  ///
  /// 先按地址查已知 nodeId 复用现有连接，查不到才以 "address:port" 临时 key
  /// 新建（created 语义：失败路径由本调用负责回收，成功后以正式 nodeId 交接
  /// 给 gossip 主循环）
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

    let temp_node_id = format!("{address}:{port}");
    let known_id = {
      let config = cluster_mgr.current_config();
      config.get_worker_node_id_from_address(address, port)
    };
    let conn_key = known_id.as_deref().unwrap_or(&temp_node_id);
    let (conn, created) = match self.connection_store.get_connection(conn_key) {
      Some(conn) => (conn, false),
      None => {
        let auth_user = self.cluster_provider.cluster_username();
        let auth_pwd = self.cluster_provider.cluster_password();
        let conn = self.connection_store.get_or_add_with_auth(
          conn_key,
          address,
          port,
          auth_user.as_deref(),
          auth_pwd.as_deref(),
        );
        (conn, true)
      }
    };

    // meet 超时用集群节点超时（对标 GarnetServerNode.TryMeetAsync
    // WaitAsync(clusterTimeout)）
    let meet_timeout = Duration::from_millis(self.cluster_provider.cluster_node_timeout_ms());
    // 应答验证（对标 Gossip.cs:196-205：先验线格式版本再反序列化；
    // Ok(None) = 空应答，不计成败）
    let validated: Result<Option<ClusterConfig>> =
      match timeout(meet_timeout, conn.try_meet_async(&config_bytes)).await {
        Ok(Ok(resp)) if !resp.is_empty() => {
          if ClusterConfig::try_peek_version(&resp) != Some(CLUSTER_CONFIG_VERSION) {
            warn!("MEET response has incompatible config version from {address}:{port}");
            Err(Error::Gossip("incompatible config version".into()))
          } else {
            match ClusterConfig::from_byte_array(&resp) {
              Ok(other) => Ok(Some(other)),
              Err(e) => {
                warn!("MEET response deserialization failed from {address}:{port}: {e:?}");
                Err(e)
              }
            }
          }
        }
        Ok(Ok(_)) => Ok(None),
        Ok(Err(e)) => {
          error!("MEET {address}:{port} failed: {e:?}");
          Err(e)
        }
        Err(_) => {
          error!("MEET {address}:{port} timed out");
          Err(Error::Gossip(format!("MEET {address}:{port} timed out")))
        }
      };

    match validated {
      Ok(Some(other)) => {
        let Some(target_id) = other.local_node_id().map(String::from) else {
          warn!("MEET response from {address}:{port} missing local node id");
          if created {
            self.connection_store.try_remove(conn_key);
          }
          self.stats.update_meet_requests_failed();
          return Err(Error::Gossip(format!(
            "MEET {address}:{port} response missing node id"
          )));
        };

        info!("MEET {target_id} {address}:{port} successful");
        // meet 由管理员发起，merge 无需再校验信任
        //（对标 C# Merge without a check because node is trusted as meet was issued by admin）
        cluster_mgr.try_merge(&other, true);
        self.stats.update_meet_requests_succeed();

        // created 连接交接：移除临时 key，以正式 nodeId 入库
        //（对标 created && !AddConnectionAsync(gsn) → Dispose；入库后归
        // gossip 主循环所有）
        if created {
          self.connection_store.try_remove(conn_key);
          let auth_user = self.cluster_provider.cluster_username();
          let auth_pwd = self.cluster_provider.cluster_password();
          self.connection_store.get_or_add_with_auth(
            &target_id,
            address,
            port,
            auth_user.as_deref(),
            auth_pwd.as_deref(),
          );
        }
        Ok(())
      }
      // 空应答：created 连接不入库即弃（C# 中未 Add 的 gsn 悬空由 GC 回收，
      // rust 连接已入 store，移除对齐）
      Ok(None) => {
        if created {
          self.connection_store.try_remove(conn_key);
        }
        Ok(())
      }
      Err(e) => {
        self.stats.update_meet_requests_failed();
        if created {
          self.connection_store.try_remove(conn_key);
        }
        Err(e)
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
    //（对标 C# Gossip.cs:InitConnectionsAsync 经 GetWorkerInfoForGossip
    // 收集节点三元组；封禁与自己跳过由本层过滤）
    let auth_user = self.cluster_provider.cluster_username();
    let auth_pwd = self.cluster_provider.cluster_password();
    {
      let config = cluster_mgr.current_config();
      let local_id = config.local_node_id().map(String::from);
      for (nid, address, port) in config.get_worker_info_for_gossip() {
        if !cluster_mgr.is_banned(&nid) && local_id.as_deref() != Some(nid.as_str()) {
          self.connection_store.get_or_add_with_auth(
            &nid,
            &address,
            port,
            auth_user.as_deref(),
            auth_pwd.as_deref(),
          );
        }
      }
    }

    // 3. 广播或者抽样 Gossip
    if self.gossip_sample_percent() >= 100 {
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
    let percent = self.gossip_sample_percent();
    let count = ((total as f64 * (percent as f64 / 100.0)).ceil() as usize).clamp(1, total);
    let start_time = now_ms() as i64;
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
  ///
  /// 增量判定键为配置演化版本号（对标 GetMostRecentConfig 的 lastConfig
  /// 引用比较：配置演化才发全量，否则发空包 ping）
  async fn gossip_to_peer_async(&self, conn: &NodeConnection) -> bool {
    let Some(cluster_mgr) = self.cluster_provider.cluster_manager() else {
      return false;
    };

    let (config_bytes, config_version, is_full) = {
      if let Some(rm) = self.cluster_provider.replication_manager() {
        cluster_mgr.lazy_update_local_replication_offset(rm.get_replication_offset(0));
      }
      let config = cluster_mgr.current_config();
      let version = cluster_mgr.config_version();
      let first_send = !conn.has_sent_full.swap(true, Ordering::SeqCst);
      if first_send || conn.last_sent_config_version.load(Ordering::Relaxed) != version {
        (config.to_byte_array(), version, true)
      } else {
        (Vec::new(), version, false)
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

    match timeout(self.gossip_delay(), conn.try_gossip_async(&config_bytes)).await {
      Ok(Ok(resp)) => {
        conn
          .last_sent_config_version
          .store(config_version, Ordering::Relaxed);
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
