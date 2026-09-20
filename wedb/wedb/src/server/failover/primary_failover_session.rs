use std::{sync::Arc, time::Duration};

use compio::{runtime::spawn, time::timeout};
use crossfire::mpmc;
use waof::AofAddress;

use super::failover_session::new_failover_client;
use crate::{
  client::GarnetClient,
  server::{
    cluster_provider::ClusterProvider,
    failover::{
      failover_option::FailoverOption, failover_session::FailoverSession,
      failover_status::FailoverStatus,
    },
    wait_async,
  },
};

/// 主端 failover 会话宿主（对标 libs/cluster/Server/Failover/
/// PrimaryFailoverSession.cs：C# 以 partial class 把主端流程写进
/// FailoverSession，rust 无继承，以组合基座承载）
pub(super) struct PrimaryFailoverSession {
  pub(super) base: FailoverSession,
}

impl PrimaryFailoverSession {
  /// 主端身份构造：预建探测连接（原基座构造 !is_replica_session 分支迁入，
  /// 对标 C# 基类 ctor 的主端连接初始化）
  pub(super) fn new(
    cluster_provider: Arc<ClusterProvider>,
    option: FailoverOption,
    cluster_timeout: Option<Duration>,
    failover_timeout: Duration,
    host_address: &str,
    host_port: i32,
  ) -> Self {
    let base = FailoverSession::new(
      cluster_provider.clone(),
      option,
      cluster_timeout,
      failover_timeout,
    );

    let mut clients = Vec::new();
    let endpoints = if host_port == -1 {
      Some(base.old_config.get_local_node_primary_endpoints(true))
    } else if host_port == 0 {
      Some(base.old_config.get_local_node_replica_endpoints())
    } else {
      None
    };

    // 探测客户端统一经 new_failover_client 构造（凭证 + TLS 单源透传，
    // C# FailoverSession.cs:75/:80 两重载的收敛形态）
    if let Some(endpoints) = endpoints {
      clients.extend(endpoints.into_iter().map(|ep| {
        Some(Arc::new(new_failover_client(
          &cluster_provider,
          ep.to_string(),
        )))
      }));
    } else if !host_address.is_empty() && host_port > 0 {
      let ep = format!("{}:{}", host_address, host_port);
      clients.push(Some(Arc::new(new_failover_client(&cluster_provider, ep))));
    } else {
      clients.push(Some(Arc::new(GarnetClient::new())));
    }
    *base.clients.lock() = clients;

    Self { base }
  }

  /// 会话收口（转发基座 Dispose）
  pub(super) fn dispose(&self) {
    self.base.dispose();
  }

  /// libs/cluster/Server/Failover/PrimaryFailoverSession.cs:CheckReplicaSyncAsync
  ///
  /// 单副本同步位点探测：未连先建链（C# ConnectAsync 裸等待），应答限时
  /// cluster_timeout（C# WaitAsync(clusterTimeout, cts.Token)，None = 无限），
  /// 超时按失败返回空串（该副本不入选）。关联函数形态：并发探测任务内以
  /// `Arc<GarnetClient>` 调用，不捕获会话
  async fn probe_replica_sync(
    gclient: &GarnetClient,
    offset: &str,
    cluster_timeout: Option<Duration>,
  ) -> String {
    if !gclient.is_connected() {
      gclient.connect_async().await;
    }
    wait_async(
      cluster_timeout,
      gclient.execute_cluster_fail_replication_offset_async(offset),
    )
    .await
    .unwrap_or_default()
  }

  /// libs/cluster/Server/Failover/PrimaryFailoverSession.cs:WaitForFirstReplicaSyncAsync
  ///
  /// 对齐 C# Task.WhenAny 竞速语义：全体候选副本并发发起位点探测，与
  /// DelayToDefaultAsync(failover_timeout) 哨兵竞速——最先应答的副本验位点
  /// 追平方当选，哨兵先至记 error 返回 None（整体超时）。C# 单副本分支
  ///（syncTask vs Task.Delay(failoverTimeout)）语义同一竞速，统一实现。
  ///
  /// 差异：C# 多副本分支胜出者位点不匹配时会继续 `await tasks[i]` 等完
  /// 全部任务（含哨兵拖满 failover_timeout）才返回 null，属实现瑕疵不对
  /// 标；本实现与 C# 单副本分支一致即刻判定。败者探测不显式取消（WhenAny
  /// 不取消败者）：胜出返回后结果通道关闭，未决任务随发送失败即收，各自
  /// 受单次 cluster_timeout 约束自然消亡
  async fn wait_for_first_replica_sync_async(&self) -> Option<Arc<GarnetClient>> {
    let mut clients: Vec<Arc<GarnetClient>> = self
      .base
      .clients
      .lock()
      .iter()
      .filter_map(|c| c.clone())
      .collect();
    if clients.is_empty() {
      return None;
    }

    let local_offset = self
      .base
      .cluster_provider
      .replication_manager()
      .map(|rm| rm.get_current_replication_offset())
      .unwrap_or_default();
    // 探测载荷预取：C# 各任务启动时各自取 ReplicationOffset，同刻取值等价
    let offset_str = local_offset.to_aof_string();
    let (offset_tx, offset_rx) = mpmc::unbounded_async::<(usize, String)>();
    for (idx, client) in clients.iter().enumerate() {
      let offset_tx = offset_tx.clone();
      let client = Arc::clone(client);
      let offset = offset_str.clone();
      let cluster_timeout = self.base.cluster_timeout;
      spawn(async move {
        let resp = Self::probe_replica_sync(&client, &offset, cluster_timeout).await;
        // 无界通道发送即刻完成；接收端已关闭（竞速已出结果）则随 Err 收尾
        let _ = offset_tx.send((idx, resp));
      })
      .detach();
    }
    drop(offset_tx);

    // WhenAny：最先应答副本胜出（completedTask == tasks[i]），哨兵先至即超时
    match timeout(self.base.failover_timeout, offset_rx.recv()).await {
      Ok(Ok((idx, resp))) => {
        if AofAddress::from_string(&resp).is_some_and(|o| o.equals_all(&local_offset)) {
          Some(clients.swap_remove(idx))
        } else {
          None
        }
      }
      _ => {
        log::error!("WaitForReplicasSync timeout");
        None
      }
    }
  }

  /// libs/cluster/Server/Failover/PrimaryFailoverSession.cs:InitiateReplicaTakeOverAsync
  async fn initiate_replica_take_over_async(&self, gclient: &GarnetClient) -> bool {
    if !gclient.is_connected() {
      gclient.connect_async().await;
    }
    // C# WaitAsync(clusterTimeout, cts.Token)：超时按失败返回（None = 无限）
    wait_async(
      self.base.cluster_timeout,
      gclient.failover(FailoverOption::Takeover),
    )
    .await
    .unwrap_or(false)
  }

  /// libs/cluster/Server/Failover/PrimaryFailoverSession.cs:BeginAsyncPrimaryFailoverAsync
  pub(super) async fn begin_async_primary_failover_async(&self) -> bool {
    if self.base.is_aborted() {
      self.base.set_status(FailoverStatus::NoFailover);
      return false;
    }
    self.base.set_status(FailoverStatus::IssuingPauseWrites);
    if let Some(cm) = self.base.cluster_provider.cluster_manager() {
      let first_replica = {
        let current = cm.current_config();
        current
          .local_node_id()
          .and_then(|local_id| current.get_replica_ids(local_id).into_iter().next())
      };
      if let Some(first_replica) = first_replica {
        cm.try_stop_writes(first_replica);
        // 推进纪元流转等待静止
        self
          .base
          .cluster_provider
          .bump_and_wait_for_epoch_transition_async()
          .await;
      } else {
        self.base.set_status(FailoverStatus::NoFailover);
        return false;
      }
    }
    self.base.set_status(FailoverStatus::WaitingForSync);

    let new_primary = self.wait_for_first_replica_sync_async().await;
    let success = if let Some(np) = new_primary {
      self.base.set_status(FailoverStatus::TakingOverAsPrimary);
      self.initiate_replica_take_over_async(&np).await
    } else {
      false
    };

    // C# finally：无论成败状态归位 NO_FAILOVER
    self.base.set_status(FailoverStatus::NoFailover);
    success
  }
}
