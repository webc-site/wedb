use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, Ordering},
  },
  time::Duration,
};

use compio::time::timeout;
use parking_lot::Mutex;
use waof::AofAddress;
use wbase::time::{Instant, InstantDuration as CoarsetimeDuration, now_instant};

use crate::{
  client::GarnetClient,
  server::{
    cluster_config::{CLUSTER_CONFIG_VERSION, ClusterConfig},
    cluster_provider::ClusterProvider,
    failover::{failover_option::FailoverOption, failover_status::FailoverStatus},
    replication::recovery_status::RecoveryStatus,
  },
};

/// failover 超时缺省值（对齐 C#：入参为 default 时取 600 秒）
const DEFAULT_FAILOVER_TIMEOUT: Duration = Duration::from_secs(600);

/// libs/cluster/Server/Failover/FailoverSession.cs:FailoverSession
///
/// 会话被后台任务驱动的同时还要对外暴露状态查询（C# 中 status 属性为
/// volatile 读），故全部字段内部可变化、方法一律 `&self`；状态以
/// `AtomicU8` 承载（`FailoverStatus` 为 `#[repr(u8)]`）
pub struct FailoverSession {
  cluster_provider: Arc<ClusterProvider>,
  cluster_timeout: Duration,
  failover_timeout: Duration,
  option: FailoverOption,
  clients: Mutex<Vec<Option<Arc<GarnetClient>>>>,
  failover_deadline: Instant,
  status: AtomicU8,
  old_config: ClusterConfig,
  primary_client: Mutex<Option<Arc<GarnetClient>>>,
  /// 中断标志（对标 C# CancellationTokenSource.Cancel）
  aborted: AtomicBool,
}

impl FailoverSession {
  /// libs/cluster/Server/Failover/FailoverSession.cs:FailoverSession
  pub fn new(
    cluster_provider: Arc<ClusterProvider>,
    option: FailoverOption,
    cluster_timeout: Duration,
    failover_timeout: Duration,
    is_replica_session: bool,
    host_address: &str,
    host_port: i32,
  ) -> Self {
    let old_config = cluster_provider
      .cluster_manager()
      .map(|cm| cm.current_config().clone())
      .unwrap_or_default();

    let mut clients = Vec::new();
    if !is_replica_session {
      let auth_user = cluster_provider.cluster_username();
      let auth_pwd = cluster_provider.cluster_password();
      let endpoints = if host_port == -1 {
        Some(old_config.get_local_node_primary_endpoints(true))
      } else if host_port == 0 {
        Some(old_config.get_local_node_replica_endpoints())
      } else {
        None
      };

      if let Some(endpoints) = endpoints {
        for ep in endpoints {
          let client = if auth_user.is_some() || auth_pwd.is_some() {
            Arc::new(GarnetClient::with_auth(
              ep.to_string(),
              auth_user.clone(),
              auth_pwd.clone(),
            ))
          } else {
            Arc::new(GarnetClient::with_endpoint(ep.to_string()))
          };
          clients.push(Some(client));
        }
      } else if !host_address.is_empty() && host_port > 0 {
        let ep = format!("{}:{}", host_address, host_port);
        let client = if auth_user.is_some() || auth_pwd.is_some() {
          Arc::new(GarnetClient::with_auth(ep, auth_user, auth_pwd))
        } else {
          Arc::new(GarnetClient::with_endpoint(ep))
        };
        clients.push(Some(client));
      } else {
        clients.push(Some(Arc::new(GarnetClient::new())));
      }
    }

    let failover_timeout = if failover_timeout.is_zero() {
      DEFAULT_FAILOVER_TIMEOUT
    } else {
      failover_timeout
    };

    Self {
      cluster_provider,
      cluster_timeout,
      failover_timeout,
      option,
      clients: Mutex::new(clients),
      failover_deadline: now_instant() + CoarsetimeDuration::from(failover_timeout),
      status: AtomicU8::new(FailoverStatus::BeginFailover as u8),
      old_config,
      primary_client: Mutex::new(None),
      aborted: AtomicBool::new(false),
    }
  }

  /// libs/cluster/Server/Failover/FailoverSession.cs:clusterTimeout
  #[inline]
  pub fn cluster_timeout(&self) -> Duration {
    self.cluster_timeout
  }

  /// libs/cluster/Server/Failover/FailoverSession.cs:failoverTimeout
  #[inline]
  pub fn failover_timeout(&self) -> Duration {
    self.failover_timeout
  }

  /// libs/cluster/Server/Failover/FailoverSession.cs:status
  #[inline]
  pub fn status(&self) -> FailoverStatus {
    FailoverStatus::from_repr(self.status.load(Ordering::Acquire)).unwrap_or_default()
  }

  /// 是否已被中断（对标 C# CancellationToken.IsCancellationRequested）
  #[inline]
  pub fn is_aborted(&self) -> bool {
    self.aborted.load(Ordering::Acquire)
  }

  /// Setter for status (FailoverSession.cs status.set)
  #[inline]
  fn set_status(&self, status: FailoverStatus) {
    self.status.store(status as u8, Ordering::Release);
  }

  /// libs/cluster/Server/Failover/FailoverSession.cs:FailoverTimeout
  pub fn failover_timeout_reached(&self) -> bool {
    now_instant() > self.failover_deadline
  }

  /// libs/cluster/Server/Failover/FailoverSession.cs:Dispose
  pub fn dispose(&self) {
    self.aborted.store(true, Ordering::Release);
    self.dispose_connections();
  }

  /// libs/cluster/Server/Failover/FailoverSession.cs:DisposeConnections
  fn dispose_connections(&self) {
    let mut clients = self.clients.lock();
    for slot in clients.iter_mut() {
      if let Some(c) = slot.take() {
        c.dispose();
      }
    }
    drop(clients);
    if let Some(c) = self.primary_client.lock().take() {
      c.dispose();
    }
  }

  // --- PrimaryFailoverSession.cs ---

  /// libs/cluster/Server/Failover/PrimaryFailoverSession.cs:CheckReplicaSyncAsync
  async fn check_replica_sync_async(&self, gclient: &GarnetClient) -> String {
    if !gclient.is_connected() {
      gclient.connect_async().await;
    }
    let local_offset = self
      .cluster_provider
      .replication_manager()
      .map(|rm| rm.get_current_replication_offset())
      .unwrap_or_default();
    let offset_str = local_offset.to_aof_string();
    // C# WaitAsync(clusterTimeout, cts.Token)：超时按失败返回空串（该副本不入选）
    timeout(
      self.cluster_timeout,
      gclient.execute_cluster_fail_replication_offset_async(&offset_str),
    )
    .await
    .unwrap_or_default()
  }

  /// libs/cluster/Server/Failover/PrimaryFailoverSession.cs:WaitForFirstReplicaSyncAsync
  ///
  /// 对齐 C# 流程：遍历候选副本连接并做同步位点探测；当副本位点追平
  /// 本地位点时即选取该副本执行接管
  async fn wait_for_first_replica_sync_async(&self) -> Option<Arc<GarnetClient>> {
    let clients: Vec<Arc<GarnetClient>> = self
      .clients
      .lock()
      .iter()
      .filter_map(|c| c.clone())
      .collect();
    if clients.is_empty() {
      return None;
    }

    let local_offset = self
      .cluster_provider
      .replication_manager()
      .map(|rm| rm.get_current_replication_offset())
      .unwrap_or_default();

    for client in clients {
      let resp = self.check_replica_sync_async(&client).await;
      if let Some(primary_offset) = AofAddress::from_string(&resp)
        && primary_offset.equals_all(&local_offset)
      {
        return Some(client);
      }
    }
    None
  }

  /// libs/cluster/Server/Failover/PrimaryFailoverSession.cs:InitiateReplicaTakeOverAsync
  async fn initiate_replica_take_over_async(&self, gclient: &GarnetClient) -> bool {
    if !gclient.is_connected() {
      gclient.connect_async().await;
    }
    // C# WaitAsync(clusterTimeout, cts.Token)：超时按失败返回
    timeout(
      self.cluster_timeout,
      gclient.failover(FailoverOption::Takeover),
    )
    .await
    .unwrap_or(false)
  }

  /// libs/cluster/Server/Failover/PrimaryFailoverSession.cs:BeginAsyncPrimaryFailoverAsync
  pub async fn begin_async_primary_failover_async(&self) -> bool {
    if self.is_aborted() {
      self.set_status(FailoverStatus::NoFailover);
      return false;
    }
    self.set_status(FailoverStatus::IssuingPauseWrites);
    if let Some(cm) = self.cluster_provider.cluster_manager() {
      let first_replica = {
        let current = cm.current_config();
        current
          .local_node_id()
          .and_then(|local_id| current.get_replica_ids(local_id).first().cloned())
      };
      if let Some(first_replica) = first_replica {
        cm.try_stop_writes(&first_replica);
        // 推进纪元流转等待静止
        self
          .cluster_provider
          .bump_and_wait_for_epoch_transition_async()
          .await;
      } else {
        self.set_status(FailoverStatus::NoFailover);
        return false;
      }
    }
    self.set_status(FailoverStatus::WaitingForSync);

    let new_primary = self.wait_for_first_replica_sync_async().await;
    let success = if let Some(np) = new_primary {
      self.set_status(FailoverStatus::TakingOverAsPrimary);
      self.initiate_replica_take_over_async(&np).await
    } else {
      false
    };

    // C# finally：无论成败状态归位 NO_FAILOVER
    self.set_status(FailoverStatus::NoFailover);
    success
  }

  // --- ReplicaFailoverSession.cs ---

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:CreateConnectionAsync
  ///
  /// 恒新建专用连接（对齐 C# 每次 `new GarnetClient`）：绝不复用 gossip
  /// connection_store 的共享 client——failover 用毕 dispose 会毒化 gossip
  /// 连接池（store 仍持已释放 client，心跳报错移除重建造成控制面抖动），
  /// 且 gossip 心跳与 failover 并发共用单工连接会应答错位；connection_store
  /// 只作地址簿，endpoint 反查走 old_config
  async fn create_connection_async(&self, node_id: &str) -> Option<Arc<GarnetClient>> {
    let auth_user = self.cluster_provider.cluster_username();
    let auth_pwd = self.cluster_provider.cluster_password();
    let client = if let Some(endpoint) = self.old_config.get_endpoint_from_node_id(node_id) {
      if auth_user.is_some() || auth_pwd.is_some() {
        Arc::new(GarnetClient::with_auth(
          endpoint.to_string(),
          auth_user,
          auth_pwd,
        ))
      } else {
        Arc::new(GarnetClient::with_endpoint(endpoint.to_string()))
      }
    } else if auth_user.is_some() || auth_pwd.is_some() {
      Arc::new(GarnetClient::with_auth(
        "127.0.0.1:6379".to_string(),
        auth_user,
        auth_pwd,
      ))
    } else {
      Arc::new(GarnetClient::new())
    };
    // C# ReconnectAsync().WaitAsync(failoverTimeout, cts.Token)，
    // 失败/超时 catch → Dispose → null：由 is_connected 复查裁决成败
    if !client.is_connected() {
      let _ = timeout(self.failover_timeout, client.reconnect_async()).await;
      if !client.is_connected() {
        client.dispose();
        return None;
      }
    }
    Some(client)
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:GetConnectionAsync
  async fn get_connection_async(&self, node_id: &str) -> Option<Arc<GarnetClient>> {
    self.create_connection_async(node_id).await
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:PauseWritesAndWaitForSyncAsync
  async fn pause_writes_and_wait_for_sync_async(&self) -> bool {
    let primary_id = self
      .old_config
      .local_node_primary_id()
      .unwrap_or("")
      .to_string();
    let Some(client) = self.get_connection_async(&primary_id).await else {
      return false;
    };

    // 缓存连接供后续接管与复位复用
    *self.primary_client.lock() = Some(Arc::clone(&client));

    // 要求主端停止写入（C# WaitAsync(failoverTimeout, cts.Token)：超时按失败）
    self.set_status(FailoverStatus::IssuingPauseWrites);
    let local_id = self.old_config.local_node_id().unwrap_or("").as_bytes();
    let resp = match timeout(
      self.failover_timeout,
      client.execute_cluster_fail_stop_writes_async(local_id),
    )
    .await
    {
      Ok(resp) => resp,
      Err(_) => {
        log::warn!("CLUSTER FAILSTOPWRITES 应答超时 primary_id={primary_id}");
        String::new()
      }
    };
    // 解析失败按 C# FormatException → catch 路径处理：放弃本次 failover
    let Some(primary_offset) = AofAddress::from_string(&resp) else {
      return false;
    };

    // 等待本地位点追平主端（超时即放弃，绝不在缺口上接管）。
    // 基于 crossfire::oneshot 事件驱动等待，零轮询零空转
    self.set_status(FailoverStatus::WaitingForSync);
    if let Some(rm) = self.cluster_provider.replication_manager() {
      // C# `if (FailoverTimeout)`（ReplicaFailoverSession.cs:97）：位点
      // 等待前先做超时终判（failover_timeout_reached 接线）
      if self.failover_timeout_reached() {
        return false;
      }
      let remaining = (self.failover_deadline - now_instant()).as_millis();
      let timeout = Duration::from_millis(remaining);
      if !rm
        .wait_for_replication_offset_async(&primary_offset, timeout)
        .await
      {
        return false;
      }
    } else {
      return false;
    }
    !self.is_aborted()
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:TakeOverAsPrimaryAsync
  async fn take_over_as_primary_async(&self) -> bool {
    self.set_status(FailoverStatus::TakingOverAsPrimary);
    let rm = self.cluster_provider.replication_manager();
    let mut acquired_lock = false;
    if let Some(ref rm) = rm {
      if !rm.begin_recovery(RecoveryStatus::ClusterFailover, false) {
        return false;
      }
      acquired_lock = true;
    }
    // 推进纪元流转等待静止
    self
      .cluster_provider
      .bump_and_wait_for_epoch_transition_async()
      .await;

    let success = if let Some(cm) = self.cluster_provider.cluster_manager()
      && !cm.try_take_over_for_primary()
    {
      false
    } else {
      if let Some(ref rm) = rm {
        rm.try_update_for_failover();
        rm.reset_replica_replay_driver_store();
        rm.initialize_checkpoint_store();
      }
      self.cluster_provider.reset_sequence_number_generator();
      // 推进纪元流转等待静止
      self
        .cluster_provider
        .bump_and_wait_for_epoch_transition_async()
        .await;
      true
    };

    if acquired_lock && let Some(ref rm) = rm {
      rm.end_recovery(RecoveryStatus::NoRecovery, false);
    }

    success
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:BroadcastConfigAndRequestAttachAsync
  async fn broadcast_config_and_request_attach_async(
    &self,
    replica_id: &str,
    config_byte_array: &[u8],
  ) {
    let old_primary_id = self.old_config.local_node_primary_id().unwrap_or("");
    // 旧主复用停写连接，其余节点新建连接（对齐 C# 分支）
    let client = if old_primary_id == replica_id {
      self.primary_client.lock().clone()
    } else {
      self.get_connection_async(replica_id).await
    };
    let Some(client) = client else {
      return;
    };

    // 强推新配置（C# WaitAsync(failoverTimeout, cts.Token)：超时按空应答跳过合并）
    let resp = match timeout(
      self.failover_timeout,
      client.gossip_async(config_byte_array),
    )
    .await
    {
      Ok(resp) => resp.unwrap_or_default(),
      Err(_) => {
        log::warn!("failover gossip 应答超时 replica_id={replica_id}");
        Vec::new()
      }
    };
    if !resp.is_empty() {
      if let Some(gm) = self.cluster_provider.gossip_manager() {
        gm.stats.update_gossip_bytes_recv(resp.len() as i64);
      }
      if let Some(cm) = self.cluster_provider.cluster_manager()
        && ClusterConfig::try_peek_version(&resp) == Some(CLUSTER_CONFIG_VERSION)
        && let Ok(other) = ClusterConfig::from_byte_array(&resp)
        && let Some(other_id) = other.local_node_id()
        && cm.current_config().is_known(other_id)
        && !cm.try_merge(&other, true)
      {
        log::debug!("Gossip 配置合并跳过（版本未超前）");
      }
    }
    let local_address = self.old_config.local_node_ip();
    let local_port = self.old_config.local_node_port();
    // 要求副本挂接（C# WaitAsync(failoverTimeout, cts.Token)：超时按失败）
    let resp = match timeout(
      self.failover_timeout,
      client.replica_of(local_address, local_port),
    )
    .await
    {
      Ok(resp) => resp,
      Err(_) => {
        log::warn!("failover REPLICAOF 应答超时 replica_id={replica_id}");
        String::new()
      }
    };
    if resp.starts_with("-ERR") {
      log::warn!("发送 REPLICAOF 命令失败: {resp}");
    }
    // 对齐 C# finally：连接用毕即释放
    client.dispose();
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:IssueAttachReplicasAsync
  async fn issue_attach_replicas_async(&self) {
    let old_primary_id = self
      .old_config
      .local_node_primary_id()
      .unwrap_or("")
      .to_string();
    let (new_config, config_byte_array) = self
      .cluster_provider
      .cluster_manager()
      .map(|cm| {
        let cfg = cm.current_config();
        (cfg.clone(), cfg.to_byte_array())
      })
      .unwrap_or_default();
    // 旧主的全部副本都需改挂新主（对齐 C# newConfig.GetReplicaIds(oldPrimaryId)）
    let mut replica_ids = new_config.get_replica_ids(&old_primary_id);

    // DEFAULT 选项下旧主降级为新主的副本
    if self.option == FailoverOption::Default && !old_primary_id.is_empty() {
      replica_ids.push(old_primary_id);
    }

    if let Some(local_id) = new_config.local_node_id() {
      replica_ids.retain(|id| id != local_id);
    }

    for replica_id in replica_ids {
      self
        .broadcast_config_and_request_attach_async(&replica_id, &config_byte_array)
        .await;
    }
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:PrimaryNeedsReset
  fn primary_needs_reset(&self) -> bool {
    matches!(
      self.status(),
      FailoverStatus::WaitingForSync | FailoverStatus::TakingOverAsPrimary
    )
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:BeginAsyncReplicaFailoverAsync
  ///
  /// C# finally 语义：停写已被主端确认（状态已过 WAITING_FOR_SYNC）而
  /// 接管失败时，必须回发 stop-writes 复位主端，否则槽位无主、集群陷入
  /// 不一致；最后释放主端连接并把状态归位 NO_FAILOVER
  pub async fn begin_async_replica_failover_async(&self) -> bool {
    let mut failover_succeeded = false;

    if self.is_aborted() {
      self.reset_if_needed(failover_succeeded).await;
      return false;
    }

    if self.option == FailoverOption::Default && !self.pause_writes_and_wait_for_sync_async().await
    {
      self.reset_if_needed(failover_succeeded).await;
      return false;
    }

    if self.is_aborted() {
      self.reset_if_needed(failover_succeeded).await;
      return false;
    }

    if !self.take_over_as_primary_async().await {
      self.reset_if_needed(failover_succeeded).await;
      return false;
    }

    failover_succeeded = true;
    self.issue_attach_replicas_async().await;
    self.reset_if_needed(failover_succeeded).await;
    true
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:finally(reset primary)
  async fn reset_if_needed(&self, failover_succeeded: bool) {
    // 锁只包克隆本身，绝不跨 await 持有
    let primary = self.primary_client.lock().clone();
    if self.primary_needs_reset()
      && !failover_succeeded
      && let Some(ref c) = primary
    {
      // C# WaitAsync(failoverTimeout, cts.Token)：超时按失败空应答
      let resp = match timeout(
        self.failover_timeout,
        c.execute_cluster_fail_stop_writes_async(&[]),
      )
      .await
      {
        Ok(resp) => resp,
        Err(_) => {
          log::warn!("复位主端 CLUSTER FAILSTOPWRITES 应答超时");
          String::new()
        }
      };
      if resp.starts_with("-ERR") {
        log::warn!("执行 CLUSTER FAILSTOPWRITES 失败: {resp}");
      }
    }
    if let Some(c) = self.primary_client.lock().take() {
      c.dispose();
    }
    self.set_status(FailoverStatus::NoFailover);
  }
}
