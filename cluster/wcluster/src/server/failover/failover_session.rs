use std::{
  sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
  },
  time::{Duration, Instant},
};

use compio::time::sleep;
use parking_lot::Mutex;
use wedb_standalone::aof::aof_address::AofAddress;

use crate::{
  client::GarnetClient,
  server::{
    cluster_config::ClusterConfig,
    cluster_provider::ClusterProvider,
    failover::{failover_option::FailoverOption, failover_status::FailoverStatus},
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
  _cluster_provider: Arc<ClusterProvider>,
  _cluster_timeout: Duration,
  _failover_timeout: Duration,
  option: FailoverOption,
  clients: Mutex<Vec<Option<Arc<GarnetClient>>>>,
  failover_deadline: Instant,
  status: AtomicU8,
  old_config: ClusterConfig,
  primary_client: Mutex<Option<Arc<GarnetClient>>>,
}

impl FailoverSession {
  /// libs/cluster/Server/Failover/FailoverSession.cs:FailoverSession
  pub fn new(
    cluster_provider: Arc<ClusterProvider>,
    option: FailoverOption,
    cluster_timeout: Duration,
    failover_timeout: Duration,
    is_replica_session: bool,
    _host_address: &str,
    _host_port: i32,
  ) -> Self {
    let old_config = ClusterConfig::new(); // mock

    let mut clients = Vec::new();
    if !is_replica_session {
      clients.push(Some(Arc::new(GarnetClient::new())));
    }

    let failover_timeout = if failover_timeout.is_zero() {
      DEFAULT_FAILOVER_TIMEOUT
    } else {
      failover_timeout
    };

    Self {
      _cluster_provider: cluster_provider,
      _cluster_timeout: cluster_timeout,
      _failover_timeout: failover_timeout,
      option,
      clients: Mutex::new(clients),
      failover_deadline: Instant::now() + failover_timeout,
      status: AtomicU8::new(FailoverStatus::BeginFailover as u8),
      old_config,
      primary_client: Mutex::new(None),
    }
  }

  /// libs/cluster/Server/Failover/FailoverSession.cs:status
  #[inline]
  pub fn status(&self) -> FailoverStatus {
    FailoverStatus::from_repr(self.status.load(Ordering::Acquire)).unwrap_or_default()
  }

  /// libs/cluster/Server/Failover/FailoverSession.cs:status setter
  #[inline]
  fn set_status(&self, status: FailoverStatus) {
    self.status.store(status as u8, Ordering::Release);
  }

  /// libs/cluster/Server/Failover/FailoverSession.cs:FailoverTimeout
  pub fn failover_timeout_reached(&self) -> bool {
    Instant::now() > self.failover_deadline
  }

  /// libs/cluster/Server/Failover/FailoverSession.cs:Dispose
  pub fn dispose(&self) {
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
    if !gclient.is_connected {
      gclient.connect_async().await;
    }
    gclient
      .execute_cluster_fail_replication_offset_async(0)
      .await
  }

  /// libs/cluster/Server/Failover/PrimaryFailoverSession.cs:WaitForFirstReplicaSyncAsync
  ///
  /// 对齐 C# 流程：取首个副本连接并做一次同步位点探测；应答解析失败或
  /// 位点未覆盖本地位点时不得启动接管（宁可放弃 failover 也不可丢提交）
  async fn wait_for_first_replica_sync_async(&self) -> Option<Arc<GarnetClient>> {
    let client = self.clients.lock().first()?.clone()?;
    let resp = self.check_replica_sync_async(&client).await;
    let primary_offset = AofAddress::from_string(&resp)?;
    // 本地复制位点占位：ReplicationManager 接线后取 ReplicationOffset
    let local_offset = AofAddress::default();
    primary_offset.equals_all(&local_offset).then_some(client)
  }

  /// libs/cluster/Server/Failover/PrimaryFailoverSession.cs:InitiateReplicaTakeOverAsync
  async fn initiate_replica_take_over_async(&self, gclient: &GarnetClient) -> bool {
    if !gclient.is_connected {
      gclient.connect_async().await;
    }
    gclient.failover(FailoverOption::Takeover).await
  }

  /// libs/cluster/Server/Failover/PrimaryFailoverSession.cs:BeginAsyncPrimaryFailoverAsync
  pub async fn begin_async_primary_failover_async(&self) -> bool {
    self.set_status(FailoverStatus::IssuingPauseWrites);
    // mock TryStopWrites + BumpAndWaitForEpochTransition
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
  async fn create_connection_async(&self, _node_id: &str) -> Option<Arc<GarnetClient>> {
    let client = Arc::new(GarnetClient::new());
    if !client.is_connected {
      client.reconnect_async().await;
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

    // 要求主端停止写入
    self.set_status(FailoverStatus::IssuingPauseWrites);
    let local_id = self.old_config.local_node_id().unwrap_or("").as_bytes();
    let resp = client
      .execute_cluster_fail_stop_writes_async(local_id)
      .await;
    // 解析失败按 C# FormatException → catch 路径处理：放弃本次 failover
    let Some(primary_offset) = AofAddress::from_string(&resp) else {
      return false;
    };

    // 等待本地位点追平主端（超时即放弃，绝不在缺口上接管）。
    // 轮询间隔 1ms：对齐 C# Task.Yield 轮询，但在 compio 定时器上挂起，
    // 不空转烧核
    self.set_status(FailoverStatus::WaitingForSync);
    while primary_offset.any_greater_than(0) {
      // 本地复制位点占位：ReplicationManager 接线后取 ReplicationOffset
      if self.failover_timeout_reached() {
        return false;
      }
      sleep(Duration::from_millis(1)).await;
    }
    true
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:TakeOverAsPrimaryAsync
  async fn take_over_as_primary_async(&self) -> bool {
    self.set_status(FailoverStatus::TakingOverAsPrimary);
    true
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

    let _resp = client.gossip_async(config_byte_array).await;
    let local_address = self.old_config.local_node_ip();
    let local_port = self.old_config.local_node_port();
    let _ = client.replica_of(local_address, local_port).await;
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
    // 旧主的全部副本都需改挂新主（对齐 C# GetReplicaIds(oldPrimaryId)）
    let mut replica_ids = self.old_config.get_replica_ids(&old_primary_id);
    let config_byte_array = vec![];

    // DEFAULT 选项下旧主降级为新主的副本
    if self.option == FailoverOption::Default {
      replica_ids.push(old_primary_id);
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

    if self.option == FailoverOption::Default && !self.pause_writes_and_wait_for_sync_async().await
    {
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
      let _ = c.execute_cluster_fail_stop_writes_async(&[]).await;
    }
    if let Some(c) = self.primary_client.lock().take() {
      c.dispose();
    }
    self.set_status(FailoverStatus::NoFailover);
  }
}

#[cfg(test)]
mod tests {
  use compio::{runtime::Runtime, time::sleep as tsleep};

  use super::*;

  /// 零超时入参填充缺省 600 秒；显式小超时按期到达
  #[test]
  fn failover_timeout_default_and_expiry() -> aok::Void {
    Runtime::new()?.block_on(async {
      let mk = |timeout| {
        FailoverSession::new(
          Arc::new(ClusterProvider {}),
          FailoverOption::Default,
          Duration::ZERO,
          timeout,
          true,
          "",
          -1,
        )
      };
      // 缺省填充：deadline 远在将来
      assert!(!mk(Duration::ZERO).failover_timeout_reached());
      // 显式 1ms：休眠后翻转
      let short = mk(Duration::from_millis(1));
      tsleep(Duration::from_millis(20)).await;
      assert!(short.failover_timeout_reached());
      aok::OK
    })
  }

  /// DEFAULT 副本 failover 全链路：停写追平→接管→广播→C# finally 状态归位。
  /// 配置面改写（槽位转移/转主）依赖 ReplicationManager/ClusterManager 接线，
  /// 当前会话仅驱动状态机
  #[test]
  fn replica_default_flow_completes_and_resets_status() -> aok::Void {
    Runtime::new()?.block_on(async {
      let s = FailoverSession::new(
        Arc::new(ClusterProvider {}),
        FailoverOption::Default,
        Duration::ZERO,
        Duration::from_secs(1),
        true,
        "",
        -1,
      );
      assert!(s.begin_async_replica_failover_async().await);
      assert_eq!(s.status(), FailoverStatus::NoFailover, "终态归位");
      aok::OK
    })
  }

  /// 主端发起的 failover：副本位点探测成功后状态同样必须归位
  /// NO_FAILOVER（对齐 C# finally）
  #[test]
  fn primary_flow_ends_no_failover() -> aok::Void {
    Runtime::new()?.block_on(async {
      let s = FailoverSession::new(
        Arc::new(ClusterProvider {}),
        FailoverOption::Takeover,
        Duration::ZERO,
        Duration::from_secs(1),
        false,
        "10.0.0.2",
        7002,
      );
      assert!(s.begin_async_primary_failover_async().await);
      assert_eq!(s.status(), FailoverStatus::NoFailover);
      aok::OK
    })
  }

  /// 连接释放幂等：重复 dispose 不 panic（take 语义清空槽位）
  #[test]
  fn dispose_is_idempotent() {
    // is_replica_session=false 时会话预置一条连接，二次释放走空槽路径
    let s = FailoverSession::new(
      Arc::new(ClusterProvider {}),
      FailoverOption::Takeover,
      Duration::ZERO,
      Duration::from_secs(1),
      false,
      "",
      -1,
    );
    s.dispose();
    s.dispose();
  }
}
