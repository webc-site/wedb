use std::{sync::Arc, time::Duration};

use coarsetime::Instant;
use compio::time::timeout;
use futures_util::future::join_all;
use waof::AofAddress;
use wbase::hex::hex_str_u128;

use super::failover_session::new_failover_client;
use crate::{
  client::GarnetClient,
  server::{
    cluster_config::{CLUSTER_CONFIG_VERSION, ClusterConfig},
    cluster_provider::ClusterProvider,
    failover::{
      failover_option::FailoverOption, failover_session::FailoverSession,
      failover_status::FailoverStatus,
    },
    replication::recovery_status::RecoveryStatus,
  },
};

/// 从端 failover 会话宿主（对标 libs/cluster/Server/Failover/
/// ReplicaFailoverSession.cs：C# 以 partial class 把从端流程写进
/// FailoverSession，rust 无继承，以组合基座承载）
pub(super) struct ReplicaFailoverSession {
  pub(super) base: FailoverSession,
}

impl ReplicaFailoverSession {
  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:ReplicaFailoverSession
  ///
  /// 从端身份构造：不预建探测连接（C# 基类 ctor isReplicaSession=true
  /// 缺省口径，clients 为空数组）
  pub(super) fn new(
    cluster_provider: Arc<ClusterProvider>,
    option: FailoverOption,
    cluster_timeout: Option<Duration>,
    failover_timeout: Duration,
  ) -> Self {
    Self {
      base: FailoverSession::new(cluster_provider, option, cluster_timeout, failover_timeout),
    }
  }

  /// 会话收口（转发基座 Dispose）
  pub(super) fn dispose(&self) {
    self.base.dispose();
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:CreateConnectionAsync
  ///
  /// 恒新建专用连接（对齐 C# 每次 `new GarnetClient`）：绝不复用 gossip
  /// connection_store 的共享 client——failover 用毕 dispose 会毒化 gossip
  /// 连接池（store 仍持已释放 client，心跳报错移除重建造成控制面抖动），
  /// 且 gossip 心跳与 failover 并发共用单工连接会应答错位；connection_store
  /// 只作地址簿，endpoint 反查走 old_config
  async fn create_connection_async(&self, node_id: u128) -> Option<Arc<GarnetClient>> {
    // 探测客户端统一经 new_failover_client 构造（凭证 + TLS 单源透传，
    // C# FailoverSession.cs:80 IPEndPoint 重载的收敛形态）
    let client = if let Some(endpoint) = self.base.old_config.get_endpoint_from_node_id(node_id) {
      Arc::new(new_failover_client(
        &self.base.cluster_provider,
        endpoint.to_string(),
      ))
    } else {
      Arc::new(GarnetClient::new())
    };
    // C# ReconnectAsync().WaitAsync(failoverTimeout, cts.Token)，
    // 失败/超时/打断 catch → Dispose → null：由 is_connected 复查裁决成败
    if !client.is_connected() {
      let connected = matches!(
        timeout(
          self.base.failover_timeout,
          self.base.race_abort(client.reconnect_async()),
        )
        .await,
        Ok(Some(Ok(())))
      ) && client.is_connected();
      if !connected {
        client.dispose();
        return None;
      }
    }
    Some(client)
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:GetConnectionAsync
  async fn get_connection_async(&self, node_id: u128) -> Option<Arc<GarnetClient>> {
    self.create_connection_async(node_id).await
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:PauseWritesAndWaitForSyncAsync
  async fn pause_writes_and_wait_for_sync_async(&self) -> bool {
    let Some(primary_id) = self.base.old_config.local_node_primary_id() else {
      return false;
    };
    let Some(client) = self.get_connection_async(primary_id).await else {
      return false;
    };

    // 缓存连接供后续接管与复位复用
    *self.base.primary_client.lock() = Some(Arc::clone(&client));

    // 要求主端停止写入（C# WaitAsync(failoverTimeout, cts.Token)：
    // 超时/abort 打断按失败）
    self.base.set_status(FailoverStatus::IssuingPauseWrites);
    // 协议帧参数：本端节点 id 仅在跨节点命令面渲染 hex
    let local_id = self
      .base
      .old_config
      .local_node_id()
      .map_or_else(String::new, hex_str_u128);
    let resp = match timeout(
      self.base.failover_timeout,
      self
        .base
        .race_abort(client.execute_cluster_fail_stop_writes_async(local_id.as_bytes())),
    )
    .await
    {
      Ok(Some(resp)) => resp,
      // 超时/中止打断对位 C# WaitAsync 抛出 → ReplicaFailoverSession.cs
      // :107-112 catch → false 放弃本次 failover，绝不允许以空应答续流程
      _ => {
        log::warn!(
          "CLUSTER FAILSTOPWRITES 应答超时或被打断 primary_id={}",
          hex_str_u128(primary_id)
        );
        return false;
      }
    };
    // 空串应答 = client.rs 断连/错误帧 unwrap_or_default 坍缩形或主端空应答
    // 形：AofAddress::from_string("") 为 Some(零位点)，若按有效应答消费即把
    // 「主端未达成停写」误判为「位点为零、副本已追平」，在缺口上直入接管
    // （C# 世界里「无有效应答」与「位点为零」绝不混同）——一律判败
    if resp.is_empty() {
      log::warn!(
        "CLUSTER FAILSTOPWRITES 无有效应答（断连坍缩/主端空应答）primary_id={}",
        hex_str_u128(primary_id)
      );
      return false;
    }
    // 解析失败按 C# FormatException → catch 路径处理：放弃本次 failover
    let Some(primary_offset) = AofAddress::from_string(&resp) else {
      return false;
    };

    // 等待本地位点追平主端（超时即放弃，绝不在缺口上接管）。
    // 基于 crossfire::oneshot 事件驱动等待，零轮询零空转
    self.base.set_status(FailoverStatus::WaitingForSync);
    let Some(rm) = self.base.cluster_provider.replication_manager() else {
      return false;
    };
    // C# `if (FailoverTimeout)`（ReplicaFailoverSession.cs:97）：位点
    // 等待前先做超时终判（failover_timeout_reached 接线）
    if self.base.failover_timeout_reached() {
      return false;
    }
    let remaining = (self.base.failover_deadline - Instant::now()).as_millis();
    // 中断面接入：abort 即唤醒位点等待（事件驱动对位 C# cts.Cancel）；
    // listener 预注册传入（listen() 创建即插入等待链，杜绝取消通知落空）
    let mut abort = self.base.abort_event.listen();
    if !rm
      .wait_for_replication_offset_async_with_abort(
        &primary_offset,
        Some(Duration::from_millis(remaining)),
        &mut abort,
      )
      .await
    {
      return false;
    }
    !self.base.is_aborted()
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:TakeOverAsPrimaryAsync
  async fn take_over_as_primary_async(&self) -> bool {
    self.base.set_status(FailoverStatus::TakingOverAsPrimary);
    let rm = self.base.cluster_provider.replication_manager();
    let mut acquired_lock = false;
    if let Some(ref rm) = rm {
      if !rm.begin_recovery(RecoveryStatus::ClusterFailover, false) {
        return false;
      }
      acquired_lock = true;
    }
    // 推进纪元流转等待静止
    self
      .base
      .cluster_provider
      .bump_and_wait_for_epoch_transition_async()
      .await;

    // C# ReplicaFailoverSession.cs:139-143：TryTakeOverForPrimary 失败即中止接管
    // 返回 false。偏离声明：rust 的 cluster_manager 允许缺席（单机模式），None
    // 与接管失败同判拒绝；原 `if let Some(cm) = ... && !cm.try_take_over...`
    // 组合式在 None 时模式不匹配整条件为 false，穿透 else 成功分支造成未配置
    // 集群管理器也虚假接管（启动 Primary 任务、推进纪元、误报完成）
    let success = if let Some(cm) = self.base.cluster_provider.cluster_manager() {
      if !cm.try_take_over_for_primary() {
        log::warn!("CLUSTER FAILOVER: cannot takeover from old primary");
        false
      } else {
        if let Some(ref rm) = rm {
          rm.try_update_for_failover();
          rm.reset_replica_replay_driver_store();
          rm.initialize_checkpoint_store();
        }
        self.base.cluster_provider.reset_sequence_number_generator();
        // 推进纪元流转等待静止
        self
          .base
          .cluster_provider
          .bump_and_wait_for_epoch_transition_async()
          .await;
        // 接管即恢复 Primary 类后台任务（C# ReplicaFailoverSession.cs:164
        // StartPrimaryTasks 对译：Resume all background maintenance that were
        // possibly shutdown when this node became a replica）
        self.base.cluster_provider.resume_primary_tasks();
        true
      }
    } else {
      log::warn!("CLUSTER FAILOVER: cannot takeover without cluster manager");
      false
    };

    // C# finally(:171-176)：成败都只在持锁时解一次恢复锁，无双解/漏解
    if acquired_lock && let Some(ref rm) = rm {
      rm.end_recovery(RecoveryStatus::NoRecovery, false);
    }

    success
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:BroadcastConfigAndRequestAttachAsync
  async fn broadcast_config_and_request_attach_async(
    &self,
    replica_id: u128,
    config_byte_array: &[u8],
  ) {
    let old_primary_id = self.base.old_config.local_node_primary_id();
    // 旧主复用停写连接，其余节点新建连接（对齐 C# 分支）
    let client = if old_primary_id == Some(replica_id) {
      self.base.primary_client.lock().clone()
    } else {
      self.get_connection_async(replica_id).await
    };
    let Some(client) = client else {
      return;
    };

    // 强推新配置（C# ReplicaFailoverSession.cs:205 WaitAsync(failoverTimeout,
    // cts.Token) 双臂：超时半边由 timeout 承接、取消半边经 race_abort 承接——
    // 打断与超时同按空应答跳过合并，行为形与 pause 臂一致）。预算注：
    // gossip/attach 两臂各自独立限 failover_timeout 串行（非按 deadline 取
    // 剩余额度，与 C# 同形），静默对端单节点最坏 2×failover_timeout，
    // deadline 取剩口径不在本票扩案
    let resp = match timeout(
      self.base.failover_timeout,
      self.base.race_abort(client.gossip_async(config_byte_array)),
    )
    .await
    {
      Ok(Some(resp)) => resp.unwrap_or_default(),
      _ => {
        log::warn!(
          "failover gossip 应答超时或被打断 replica_id={}",
          hex_str_u128(replica_id)
        );
        Vec::new()
      }
    };
    if !resp.is_empty() {
      if let Some(gm) = self.base.cluster_provider.gossip_manager() {
        gm.stats.update_gossip_bytes_recv(resp.len() as i64);
      }
      if let Some(cm) = self.base.cluster_provider.cluster_manager()
        && ClusterConfig::try_peek_version(&resp) == Some(CLUSTER_CONFIG_VERSION)
        && let Ok(other) = ClusterConfig::from_byte_array(&resp)
        && let Some(other_id) = other.local_node_id()
      {
        // 同步读守卫先落 bool 再进挂起门：current_config() 的读守卫绝不
        // 跨 try_merge().await（读锁等待 active_merge_lock 期间）持有
        let known = cm.current_config().is_known(other_id);
        if known && !cm.try_merge(&other, true).await {
          log::debug!("Gossip 配置合并跳过（版本未超前）");
        }
      }
    }
    let local_address = self.base.old_config.local_node_ip();
    let local_port = self.base.old_config.local_node_port();
    // 要求副本挂接（C# ReplicaFailoverSession.cs:245 WaitAsync(failoverTimeout,
    // cts.Token) 双臂：超时半边由 timeout 承接、取消半边经 race_abort 承接——
    // 首臂已吞掉中止通知，此臂凭 race_abort 的持久标志前置判同样即时落断；
    // 打断/超时同按失败记警收口）
    let resp = match timeout(
      self.base.failover_timeout,
      self
        .base
        .race_abort(client.replica_of(local_address, local_port)),
    )
    .await
    {
      Ok(Some(resp)) => resp,
      _ => {
        log::warn!(
          "failover REPLICAOF 应答超时或被打断 replica_id={}",
          hex_str_u128(replica_id)
        );
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
    let Some(old_primary_id) = self.base.old_config.local_node_primary_id() else {
      return;
    };
    let (new_config, config_byte_array) = self
      .base
      .cluster_provider
      .cluster_manager()
      .map(|cm| {
        let cfg = cm.current_config();
        (cfg.clone(), cfg.to_byte_array())
      })
      .unwrap_or_default();
    // 旧主的全部副本都需改挂新主（对齐 C# newConfig.GetReplicaIds(oldPrimaryId)）
    let mut replica_ids = new_config.get_replica_ids(old_primary_id);

    // DEFAULT 选项下旧主降级为新主的副本
    if self.base.option == FailoverOption::Default {
      replica_ids.push(old_primary_id);
    }

    if let Some(local_id) = new_config.local_node_id() {
      replica_ids.retain(|&id| id != local_id);
    }

    let tasks: Vec<_> = replica_ids
      .iter()
      .map(|&replica_id| {
        self.broadcast_config_and_request_attach_async(replica_id, &config_byte_array)
      })
      .collect();
    join_all(tasks).await;
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:PrimaryNeedsReset
  fn primary_needs_reset(&self) -> bool {
    matches!(
      self.base.status(),
      FailoverStatus::WaitingForSync | FailoverStatus::TakingOverAsPrimary
    )
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:BeginAsyncReplicaFailoverAsync
  ///
  /// C# finally 语义：停写已被主端确认（状态已过 WAITING_FOR_SYNC）而
  /// 接管失败时，必须回发 stop-writes 复位主端，否则槽位无主、集群陷入
  /// 不一致；最后释放主端连接并把状态归位 NO_FAILOVER
  pub(super) async fn begin_async_replica_failover_async(&self) -> bool {
    let mut failover_succeeded = false;

    if self.base.is_aborted() {
      self.reset_if_needed(failover_succeeded).await;
      return false;
    }

    // C# 331-334 对 DEFAULT/FORCE 的多数派投票选举为 TODO 未实现（Garnet
    // 上游无任何投票代码），本实现对齐上游行为：不做自造共识，防脑裂由
    // DEFAULT 选项的停写确认 + 位点追平（pause_writes_and_wait_for_sync_async）
    // 承接
    if self.base.option == FailoverOption::Default
      && !self.pause_writes_and_wait_for_sync_async().await
    {
      self.reset_if_needed(failover_succeeded).await;
      return false;
    }

    if self.base.is_aborted() {
      self.reset_if_needed(failover_succeeded).await;
      return false;
    }

    if !self.take_over_as_primary_async().await {
      self.reset_if_needed(failover_succeeded).await;
      return false;
    }

    failover_succeeded = true;
    self.issue_attach_replicas_async().await;
    // 恢复 Primary 类后台任务（C# 346-348 SuspendReplicaOnlyTasksAsync +
    // StartPrimaryTasks 对译；rust 无副本专属任务，仅恢复侧生效）
    self.base.cluster_provider.resume_primary_tasks();
    self.reset_if_needed(failover_succeeded).await;
    true
  }

  /// libs/cluster/Server/Failover/ReplicaFailoverSession.cs:finally(reset primary)
  async fn reset_if_needed(&self, failover_succeeded: bool) {
    // 锁只包克隆本身，绝不跨 await 持有
    let primary = self.base.primary_client.lock().clone();
    if self.primary_needs_reset()
      && !failover_succeeded
      && let Some(ref c) = primary
    {
      // C# WaitAsync(failoverTimeout, cts.Token)：超时按失败空应答。
      // 复位不接 abort 中断面：abort 后主端恢复写正是本次收口的落点，
      // 须有界等待确认送达；C# 以已取消 token 弃置复位应答（请求甚至
      // 可能未发出，自记 incoherent state），属 C# 缺陷不对标
      let resp = match timeout(
        self.base.failover_timeout,
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
    if let Some(c) = self.base.primary_client.lock().take() {
      c.dispose();
    }
    self.base.set_status(FailoverStatus::NoFailover);
  }
}
