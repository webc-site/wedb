use std::{sync::Arc, time::Duration};

use coarsetime::Instant;
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
    // 端点三态选集（对标 C# FailoverSession.cs:66-68）：`host_port == -1`
    // 臂在全收 C# 跳验+错向语义后系纯防御镜像——命令面 TO 端口必过三闸、
    // -1 查无此端即拒（deviations §117b，failover.rs TO 校验臂），本臂自
    // 命令面不可达，仅承接会话直构入口（测试/内部编排）的显式 -1 选集
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
    offset: &AofAddress,
    cluster_timeout: Option<Duration>,
  ) -> String {
    if !gclient.is_connected() {
      let _ = gclient.connect_async().await;
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
  /// 对齐 C# Task.WhenAny 竞速语义：全体候选副本并发发起位点探测，在
  /// failover_deadline 剩余窗口内循环消费 offset_rx，每轮与
  /// self.base.race_abort 竞速（C# WaitAsync(clusterTimeout, cts.Token)
  /// 的取消半边，超时半边由剩余窗口承接）。维护 pending_replicas 待应答
  /// 计数：位点不匹配或空应答（建连失败/探测超时的快速落点）仅记调试日志、
  /// 递减计数继续等下一副本——契约语义是等首个追平位点的健康副本，单副本
  /// 局部异常不构成全盘失败。当且仅当位点完全一致的副本应答时返回该客户端
  /// 当选；全部副本应答完毕无一匹配、整体超时或收到中止信号返回 None。
  ///
  /// 差异：C# 多副本分支胜出者位点不匹配时逐个 `await tasks[i]` 等完全部
  /// 任务（含 DelayToDefaultAsync 哨兵拖满 failover_timeout）才返回 null，
  /// 属实现瑕疵不对标；本实现逐应答递减计数，计数归零即收。败者探测不显式
  /// 取消（WhenAny 不取消败者）：提前返回后结果通道关闭，未决任务随发送
  /// 失败即收，各自受单次 cluster_timeout 约束自然消亡
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
    // 探测载荷预取：C# 各任务启动时各自取 ReplicationOffset，同刻取值等价；
    // 上线形为带 1 字节长度前缀二进制（AofAddress 本体直传，载荷编码单点在
    // execute_cluster_fail_replication_offset_async，对标 C# 实参直传
    // PrimaryFailoverSession.cs:22 → GarnetClientExtensions.cs:61 ToByteArray）
    let offset = local_offset;
    let (offset_tx, offset_rx) = mpmc::unbounded_async::<(usize, String)>();
    for (idx, client) in clients.iter().enumerate() {
      let offset_tx = offset_tx.clone();
      let client = Arc::clone(client);
      let cluster_timeout = self.base.cluster_timeout;
      spawn(async move {
        let resp = Self::probe_replica_sync(&client, &offset, cluster_timeout).await;
        // 无界通道发送即刻完成；接收端已关闭（竞速已出结果）则随 Err 收尾
        let _ = offset_tx.send((idx, resp));
      })
      .detach();
    }
    drop(offset_tx);

    // 待应答副本计数：每收一副本终态应答（匹配返回/不匹配递减）递减一，
    // 归零即全体耗尽无一匹配
    let mut pending_replicas = clients.len();
    loop {
      // 中止标志面终判：aborted 持久置位，覆盖应答唤醒与下一轮 listener
      // 注册之间的通知落空窗口（C# cts.Token 在任何 await 点持续可查）
      if self.base.is_aborted() {
        return None;
      }
      if pending_replicas == 0 {
        log::debug!("全部副本应答完毕且无一位点追平，放弃故障转移");
        return None;
      }
      // 整体预算按 failover_deadline 剩余窗口限时（C# 哨兵
      // DelayToDefaultAsync(failoverTimeout) 的终局半边），先判再取差，
      // 杜绝 deadline - now 负差 panic
      if self.base.failover_timeout_reached() {
        log::error!("WaitForReplicasSync timeout");
        return None;
      }
      let remaining = (self.base.failover_deadline - Instant::now()).as_millis();
      // WhenAny 竞速逐轮再现：本轮应答 vs race_abort 中止 vs 剩余窗口超时
      match timeout(
        Duration::from_millis(remaining),
        self.base.race_abort(offset_rx.recv()),
      )
      .await
      {
        Ok(Some(Ok((idx, resp)))) => {
          pending_replicas -= 1;
          if AofAddress::from_string(&resp).is_some_and(|o| o.equals_all(&local_offset)) {
            return Some(clients.swap_remove(idx));
          }
          // 位点不匹配/空应答副本只记调试日志，绝不误杀整体流程
          log::debug!("副本 {idx} 位点不匹配或空应答，继续等待其余 {pending_replicas} 个副本追平");
        }
        // 中止信号打断等待（C# WaitAsync 取消臂 OperationCanceledException）
        Ok(None) => {
          log::debug!("位点探测等待被中止信号打断，放弃故障转移");
          return None;
        }
        // 全体探测任务已收 senders 关闭：不会再有新应答（含副本任务中途
        // panic 携带 sender 消亡的兜底）
        Ok(Some(Err(_))) => return None,
        Err(_) => {
          log::error!("WaitForReplicasSync timeout");
          return None;
        }
      }
    }
  }

  /// libs/cluster/Server/Failover/PrimaryFailoverSession.cs:InitiateReplicaTakeOverAsync
  async fn initiate_replica_take_over_async(&self, gclient: &GarnetClient) -> bool {
    if !gclient.is_connected() {
      let _ = gclient.connect_async().await;
    }
    // C# WaitAsync(clusterTimeout, cts.Token) 双臂：超时（None = 无限）与
    // 中止打断均按 catch 记失败返回 false，绝不在中止态下盲等应答
    self
      .base
      .race_abort(wait_async(
        self.base.cluster_timeout,
        gclient.failover(FailoverOption::Takeover),
      ))
      .await
      .flatten()
      .unwrap_or(false)
  }

  /// libs/cluster/Server/Failover/PrimaryFailoverSession.cs:BeginAsyncPrimaryFailoverAsync
  ///
  /// 回滚闭环（C# 上游缺失，PR #1670 立论：让渡槽位但从节点未接管 = 集群
  /// 槽位无主非法状态）：try_stop_writes 让渡后任一失败路径（位点同步超时、
  /// 接管应答失败、中止信号打断）统一经 try_restore_stop_writes 赎回槽位
  /// 并恢复 PRIMARY 角色，再推进纪元流转等待全会话静止刷新视图；成功接管
  /// 或未让渡（无候选副本）不回滚
  pub(super) async fn begin_async_primary_failover_async(&self) -> bool {
    if self.base.is_aborted() {
      self.base.set_status(FailoverStatus::NoFailover);
      return false;
    }
    self.base.set_status(FailoverStatus::IssuingPauseWrites);
    let mut stopped_writes = None;
    let mut drain_settled = true;
    if let Some(cm) = self.base.cluster_provider.cluster_manager() {
      let first_replica = {
        let current = cm.current_config();
        current
          .local_node_id()
          .and_then(|local_id| current.get_replica_ids(local_id).into_iter().next())
      };
      if let Some(first_replica) = first_replica {
        cm.try_stop_writes(first_replica);
        stopped_writes = Some(cm);
        // 排空栅栏返值承判（C# PrimaryFailoverSession.cs:113-117 原语恒真、
        // 静止达成才进 WaitForFirstReplicaSync 探测再下发 TAKEOVER；rust 有界
        // 化后 false = 静止未达成，滞留批内在途写仍可越过随后采样的位点——
        // deviations §95 判败同族，与停写应答面同原语同害同判）：未达成即不
        // 探测、不下发，判败落下方 `!success && stopped_writes` 既有赎回臂回滚
        drain_settled = self
          .base
          .cluster_provider
          .bump_and_wait_for_epoch_transition_async()
          .await;
      } else {
        self.base.set_status(FailoverStatus::NoFailover);
        return false;
      }
    }

    let new_primary = if drain_settled {
      self.base.set_status(FailoverStatus::WaitingForSync);
      self.wait_for_first_replica_sync_async().await
    } else {
      None
    };
    let success = if let Some(np) = new_primary {
      // 探测出口与接管下发之间落地的中止必须放弃推进：TAKEOVER 是无条件
      // 夺主指令，中止态下发即双主并写脑裂（C# 靠 WaitAsync(cts.Token)
      // 取消臂抛异常拦截，rust 下发前置查标志面）
      if self.base.is_aborted() {
        false
      } else {
        self.base.set_status(FailoverStatus::TakingOverAsPrimary);
        self.initiate_replica_take_over_async(&np).await
      }
    } else {
      false
    };

    // 未成功接管且已让渡：赎回槽位恢复主节点原状，推进纪元流转
    if !success
      && let Some(cm) = stopped_writes
      && cm.try_restore_stop_writes()
    {
      // 此臂栅栏弃返值判净（与 r25 迁移族归位臂同形）：排空方向为「停写
      // 恢复可写」的松绑，后续动作仅状态归位与返回 false，无放行危险动作；
      // 残余批会话持旧保守视图拒写，批边界自收敛
      self
        .base
        .cluster_provider
        .bump_and_wait_for_epoch_transition_async()
        .await;
    }

    // C# finally：无论成败状态归位 NO_FAILOVER
    self.base.set_status(FailoverStatus::NoFailover);
    success
  }
}
