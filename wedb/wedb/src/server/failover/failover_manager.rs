use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::runtime::spawn;
use event_listener::Event;
use parking_lot::RwLock;
use wbase::supervise::supervise_task;

use crate::server::{
  cluster_provider::ClusterProvider,
  failover::{
    failover_option::FailoverOption, failover_status::FailoverStatus,
    primary_failover_session::PrimaryFailoverSession,
    replica_failover_session::ReplicaFailoverSession,
  },
};

/// 监督快照里的任务名（wbase::supervise 归组键，INFO bg_task_health 可见）
const FAILOVER_REPLICA_TASK: &str = "failover_replica";
/// 同上（主端 failover 任务）
const FAILOVER_PRIMARY_TASK: &str = "failover_primary";

/// failover 会话宿主：C# FailoverSession 为 partial class 单类型，
/// rust 按基类 + 主/从派生拆为两个组合宿主后，FailoverManager 的单会话
/// 槽位以 enum 承载（仅类型分流分发，非 trait 对象、非泛型抽象）
enum FailoverSessionHost {
  Primary(Arc<PrimaryFailoverSession>),
  Replica(Arc<ReplicaFailoverSession>),
}

impl FailoverSessionHost {
  fn status(&self) -> FailoverStatus {
    match self {
      Self::Primary(s) => s.base.status(),
      Self::Replica(s) => s.base.status(),
    }
  }

  fn dispose(&self) {
    match self {
      Self::Primary(s) => s.dispose(),
      Self::Replica(s) => s.dispose(),
    }
  }
}

/// libs/cluster/Server/Failover/FailoverManager.cs:FailoverManager
///
/// 同一时刻至多一个 failover 会话在跑：任务锁为原子标志（C#
/// SingleWriterMultiReaderLock 的 try-lock 语义），在发起时同步获取、由
/// 后台会话任务完成时释放。会话经 compio 后台任务驱动，终态回写
/// `last_failover_status`——对齐 C# Task.Run 中
/// `lastFailoverStatus = success ? FAILOVER_COMPLETED : FAILOVER_ABORTED`。
pub struct FailoverManager {
  cluster_provider: Arc<ClusterProvider>,
  current_failover_session: RwLock<Option<FailoverSessionHost>>,
  failover_task_lock: AtomicBool,
  pub last_failover_status: RwLock<FailoverStatus>,
  event: Event,
}

impl FailoverManager {
  /// libs/cluster/Server/Failover/FailoverManager.cs:FailoverManager
  pub fn new(cluster_provider: Arc<ClusterProvider>) -> Self {
    Self {
      cluster_provider,
      current_failover_session: RwLock::new(None),
      failover_task_lock: AtomicBool::new(false),
      last_failover_status: RwLock::new(FailoverStatus::NoFailover),
      event: Event::new(),
    }
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:clusterTimeout
  ///
  /// C# 为动态属性，每次发起 failover 时即时拉取 runtimeConfig 的
  /// CLUSTER_NODE_TIMEOUT（GetTimeSpan，非正值 = 无限超时），
  /// CONFIG SET cluster-node-timeout 对后续会话生效；本实现同语义，
  /// 从 cluster_provider.cluster_node_timeout() 单点即时取值（gossip 同源；
  /// 写口两个：boot.rs 命令行播种 + CONFIG SET 调停投影
  ///（wconf `apply_cluster_node_timeout_update` → `set_cluster_node_timeout_ms`），
  /// 热更即时生效），None 承载 0 = 无限的 C# 语义
  #[inline]
  fn cluster_timeout(&self) -> Option<Duration> {
    self.cluster_provider.cluster_node_timeout()
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:Dispose
  pub fn dispose(&self) {
    self.reset();
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:TryAbortReplicaFailover
  pub fn try_abort_replica_failover(&self) {
    self.reset();
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:Reset
  ///
  /// 对齐 C# Reset 的 ABORT 解锁逃生门（:54 WriteUnlock）：任务锁在此无条件
  /// 复位。运维 ABORT / panic 后封锁的恢复口——正常路径下监督收尾
  ///（[`Self::settle_failover`]）已释放锁，此处复位为幂等；会话尚在跑时
  /// ABORT 解锁即 C# 同形竞态（新 failover 可并发启动），收尾的锁释放以
  /// `store(false)` 承接（锁无持有者 token，late 收尾可能放行第三个并发
  /// 会话，C# Task.Run 形态同），不回退 r26-autofailover 判据面
  fn reset(&self) {
    if let Some(s) = self.current_failover_session.write().take() {
      s.dispose();
    }
    self.failover_task_lock.store(false, Ordering::Release);
  }

  /// failover 会话收尾唯一收口（Ok / Err / panic 三态必达）：终态落账 +
  /// 会话销毁 + 注册表清位 + 任务锁释放 + 等待者唤醒
  ///
  /// C# 对偶为 Task.Run 体内的
  /// `lastFailoverStatus = success ? COMPLETED : ABORTED` + Dispose + 锁释放；
  /// panic 臂由监督任务体补跑本收尾（`success=false` 即 ABORTED，与 C#
  /// 异常终止同口径），杜绝锁永真、状态永卡 BeginFailover、
  /// wait_failover_done 永挂
  fn settle_failover(&self, host: FailoverSessionHost, success: bool) {
    *self.last_failover_status.write() = if success {
      FailoverStatus::FailoverCompleted
    } else {
      FailoverStatus::FailoverAborted
    };
    host.dispose();
    *self.current_failover_session.write() = None;
    self.failover_task_lock.store(false, Ordering::Release);
    self.event.notify(usize::MAX);
  }
  /// libs/cluster/Server/Failover/FailoverManager.cs:GetFailoverStatus
  pub fn get_failover_status(&self) -> String {
    let session = self.current_failover_session.read();
    session
      .as_ref()
      .map_or(FailoverStatus::NoFailover, |s| s.status())
      .get_failover_status()
      .to_string()
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:GetLastFailoverStatus
  pub fn get_last_failover_status(&self) -> String {
    self
      .last_failover_status
      .read()
      .get_failover_status()
      .to_string()
  }

  /// failover 是否处于进行中的任一阶段
  ///
  /// 对标 C# EnsureReplication 的 failoverStatus 抑制判定
  ///（ReplicationManager.cs:214-220：BEGIN_FAILOVER / ISSUING_PAUSE_WRITES /
  /// WAITING_FOR_SYNC / FAILOVER_IN_PROGRESS / TAKING_OVER_AS_PRIMARY）
  pub fn is_failover_in_progress(&self) -> bool {
    let in_progress = matches!(
      *self.last_failover_status.read(),
      FailoverStatus::BeginFailover
        | FailoverStatus::IssuingPauseWrites
        | FailoverStatus::WaitingForSync
        | FailoverStatus::FailoverInProgress
        | FailoverStatus::TakingOverAsPrimary
    );
    if in_progress {
      return true;
    }
    let session = self.current_failover_session.read();
    session.as_ref().is_some_and(|s| {
      matches!(
        s.status(),
        FailoverStatus::BeginFailover
          | FailoverStatus::IssuingPauseWrites
          | FailoverStatus::WaitingForSync
          | FailoverStatus::FailoverInProgress
          | FailoverStatus::TakingOverAsPrimary
      )
    })
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:TryStartReplicaFailover
  pub fn try_start_replica_failover(
    self: &Arc<Self>,
    option: FailoverOption,
    failover_timeout: Duration,
  ) -> bool {
    if self.failover_task_lock.swap(true, Ordering::Acquire) {
      return false;
    }

    *self.last_failover_status.write() = FailoverStatus::BeginFailover;

    let session = Arc::new(ReplicaFailoverSession::new(
      self.cluster_provider.clone(),
      option,
      self.cluster_timeout(),
      failover_timeout,
    ));
    *self.current_failover_session.write() =
      Some(FailoverSessionHost::Replica(Arc::clone(&session)));

    let this = Arc::clone(self);
    let manager = Arc::clone(self);
    let task_host = FailoverSessionHost::Replica(Arc::clone(&session));
    spawn(async move {
      // 会话体经 wbase supervise_task 顶层监督（单点一次成型）：Ok/Err/panic
      // 三态必经 settle_failover 收尾四件，panic 臂以 ABORTED 终态补跑（对标
      // C# Task.Run 体内异常存于 Task 对象、收尾必达）
      let run = async move {
        let success = session.begin_async_replica_failover_async().await;
        this.settle_failover(FailoverSessionHost::Replica(session), success);
      };
      if supervise_task(FAILOVER_REPLICA_TASK, run).await.is_err() {
        manager.settle_failover(task_host, false);
      }
    })
    .detach();
    true
  }

  /// libs/cluster/Server/Failover/FailoverManager.cs:TryStartPrimaryFailover
  ///
  /// C# 上游遗漏 lastFailoverStatus 维护（replica 路径有），票面要求对齐
  /// replica 同形：启动前置 BEGIN_FAILOVER，收尾按成败 COMPLETED/ABORTED
  pub fn try_start_primary_failover(
    self: &Arc<Self>,
    replica_address: &str,
    replica_port: i32,
    option: FailoverOption,
    timeout: Duration,
  ) -> bool {
    if self.failover_task_lock.swap(true, Ordering::Acquire) {
      return false;
    }

    *self.last_failover_status.write() = FailoverStatus::BeginFailover;

    let session = Arc::new(PrimaryFailoverSession::new(
      self.cluster_provider.clone(),
      option,
      self.cluster_timeout(),
      timeout,
      replica_address,
      replica_port,
    ));
    *self.current_failover_session.write() =
      Some(FailoverSessionHost::Primary(Arc::clone(&session)));

    let this = Arc::clone(self);
    let manager = Arc::clone(self);
    let task_host = FailoverSessionHost::Primary(Arc::clone(&session));
    spawn(async move {
      // 会话体经 wbase supervise_task 顶层监督（单点一次成型）：Ok/Err/panic
      // 三态必经 settle_failover 收尾四件，panic 臂以 ABORTED 终态补跑
      let run = async move {
        let success = session.begin_async_primary_failover_async().await;
        if !success {
          log::warn!("主节点异步故障转移流程未成功完成");
        }
        this.settle_failover(FailoverSessionHost::Primary(session), success);
      };
      if supervise_task(FAILOVER_PRIMARY_TASK, run).await.is_err() {
        manager.settle_failover(task_host, false);
      }
    })
    .detach();
    true
  }

  /// 异步等待当前故障转移任务结束（基于事件驱动，零轮询零空转）
  pub async fn wait_failover_done(&self) {
    loop {
      let listener = {
        if !self.failover_task_lock.load(Ordering::Acquire) {
          return;
        }
        self.event.listen()
      };
      listener.await;
    }
  }
}
