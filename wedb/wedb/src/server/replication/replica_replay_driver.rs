use std::{
  future::Future,
  pin::Pin,
  sync::{
    Arc, Weak,
    atomic::{AtomicBool, AtomicI64, Ordering},
  },
  task::{Context, Poll},
};

use event_listener::{Event, EventListener};
use parking_lot::Mutex;
use wnode::resp::slow_path::SlowWait;

use crate::server::replication::{
  driver_registry::DriverLifecycle, replica_replay_task::ReplayAssets,
  replication_manager::ReplicationManager,
};

/// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:ReplicaReplayDriver
///
/// 副本节点单子日志 AOF 数据消费与重放驱动器。资产在场时持背景重放任务面
/// （applied 位点语义，应用后经本驱动权威面回推）；无资产退化形态（测试
/// 装配 / 未接存储）仅保留位点记账，enqueued 语义由会话承担。
#[derive(Debug)]
pub struct ReplicaReplayDriver {
  pub physical_sublog_idx: usize,
  /// 已重放位点（内部重放推进记账；背景重放任务应用后回推
  /// ReplicationManager 权威位点的挂点，见 replica_replay_task）
  pub(crate) replayed_offset: AtomicI64,
  /// 待应用时间脉冲序列号（C# pendingPulseSequenceNumber）
  pending_pulse_sequence_number: AtomicI64,
  /// 已应用时间脉冲序列号（C# appliedPulseSequenceNumber）
  applied_pulse_sequence_number: AtomicI64,
  /// 驱动生命周期面（创建 → dispose；DriverRegistry 活跃判定源）
  is_active: AtomicBool,
  /// 重放所有权面（C# activeWorkerMonitor 的 TryEnter/Exit 语义：false =
  /// 空闲可获取，true = 会话线程持有）
  replay_owner: AtomicBool,
  /// 重放应用资产（None = 退化形态：无存储应用链）
  assets: Option<Arc<ReplayAssets>>,
  /// 复制管理器弱引用（位点权威面回推与追平守卫；弱引用杜绝
  /// rm → store → driver → rm 强引用环）
  rm: Weak<ReplicationManager>,
  /// 背景重放任务面（Some = 任务已启动；C# replayIterator 非空语义）
  background: Mutex<Option<BackgroundReplay>>,
  /// 位点推进事件（ThrottlePrimary 等待者的协程唤醒源）
  progress: Event,
}

/// 背景重放任务句柄（stop 置位即终止常驻循环）
#[derive(Debug)]
struct BackgroundReplay {
  stop: Arc<AtomicBool>,
}

/// 主端节流挂起体（C# ThrottlePrimary 的 Thread.Yield 自旋在 compio 线程
/// 每核模型下的协程化承接：会话消费面为同步 trait 无法内联 await，改由
/// 网络泵经 take_slow_wait 取走本等待体挂起协程——compio 挂起不占线程，
/// 位点推进 / 驱动处置即唤醒）
struct ThrottleWait {
  driver: Arc<ReplicaReplayDriver>,
  /// 节流门限（C# runtimeConfig AOF_REPLAY_MAX_LAG_BYTES 装配帧快照）
  max_lag: i64,
  /// 位点推进监听（先注册后复查，杜绝错过唤醒；Some = 已注册在册）
  listener: Option<EventListener>,
}

impl Future for ThrottleWait {
  type Output = Vec<u8>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.get_mut();
    loop {
      // 先注册监听再复查放行条件（注册先于检查，位点推进通知必不丢失）
      let listener = this
        .listener
        .get_or_insert_with(|| this.driver.progress.listen());
      if this.driver.throttle_released(this.max_lag) {
        return Poll::Ready(Vec::new());
      }
      if Pin::new(listener).poll(cx).is_ready() {
        // 位点推进或处置广播：丢弃在册监听，循环顶重注册并复查收敛
        this.listener = None;
      } else {
        return Poll::Pending;
      }
    }
  }
}

impl DriverLifecycle for ReplicaReplayDriver {
  #[inline]
  fn is_active(&self) -> bool {
    self.is_active.load(Ordering::Acquire)
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:Dispose
  fn dispose(&self) {
    self.is_active.store(false, Ordering::Release);
    // 终止背景重放任务（对标 C# cts 取消 + Dispose 链）
    if let Some(task) = self.background.lock().take() {
      task.stop.store(true, Ordering::Release);
    }
    // 唤醒节流等待者：处置后 lag 永不收敛，解除挂起放行断流处置
    self.notify_progress();
    self.suspend_replay();
  }
}

impl ReplicaReplayDriver {
  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:ReplicaReplayDriver
  pub(crate) fn new(
    physical_sublog_idx: usize,
    assets: Option<Arc<ReplayAssets>>,
    rm: Weak<ReplicationManager>,
  ) -> Self {
    Self {
      physical_sublog_idx,
      replayed_offset: AtomicI64::new(0),
      pending_pulse_sequence_number: AtomicI64::new(0),
      applied_pulse_sequence_number: AtomicI64::new(0),
      is_active: AtomicBool::new(true),
      replay_owner: AtomicBool::new(false),
      assets,
      rm,
      background: Mutex::new(None),
      progress: Event::new(),
    }
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:ResumeReplay
  ///
  /// 获取重放所有权（TryEnter 语义：空闲即成功，已被持有即失败）
  pub fn resume_replay(&self) -> bool {
    self
      .replay_owner
      .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
      .is_ok()
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:SuspendReplay
  pub fn suspend_replay(&self) {
    self.replay_owner.store(false, Ordering::Release);
  }

  /// 背景重放任务是否已启动（C# replayIterator 非空语义）
  #[inline]
  pub fn background_replay_started(&self) -> bool {
    self.background.lock().is_some()
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:InitializeBackgroundReplayTask
  ///
  /// 启动背景重放任务（幂等：仅首帧启动，对标 replayIterator == null 门控；
  /// startAddress = 首帧 previousAddress，扫描自衔接点起补齐全部增量）。
  /// 无重放资产（退化装配）时空转：位点保持会话落盘面 enqueued 直推形态。
  pub fn initialize_background_replay_task(self: &Arc<Self>, start_address: i64) {
    let Some(assets) = self.assets.clone() else {
      return;
    };
    let mut background = self.background.lock();
    if background.is_some() {
      return;
    }
    let stop = Arc::new(AtomicBool::new(false));
    if !super::replica_replay_task::spawn(
      Arc::clone(self),
      assets,
      Weak::clone(&self.rm),
      start_address,
      Arc::clone(&stop),
    ) {
      log::error!(
        "背景重放线程启动失败 sublogIdx: {}",
        self.physical_sublog_idx
      );
      return;
    }
    *background = Some(BackgroundReplay { stop });
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:ThrottlePrimary
  ///
  /// 主端背压挂起体装配：背景重放已启动且落后字节数（本地日志尾 - 复制
  /// 位点）超过 maxLag 时返回 Some 等待体，由网络泵 await 挂起协程等待重放
  /// 应用追平（maxLag == -1 禁用；0 = 同步形态，锁步至完全追平放行下一
  /// 帧）。C# Thread.Yield 让出时间片自旋（网络线程与重放线程独立）在
  /// compio 线程每核单线程 runtime 中会冻结同 runtime 全部任务，折叠为
  /// 事件通知协程挂起——等待语义等价、零线程阻塞；处置（dispose）即放行。
  /// 已收敛 / 退化装配返回 None 零开销直通（C# 循环条件首查即假）。
  pub fn throttle_wait(self: &Arc<Self>, max_lag_bytes: i32) -> Option<SlowWait> {
    if max_lag_bytes == -1 || !self.background_replay_started() {
      return None;
    }
    let max_lag = i64::from(max_lag_bytes);
    if self.throttle_released(max_lag) {
      return None;
    }
    Some(SlowWait::new(ThrottleWait {
      driver: Arc::clone(self),
      max_lag,
      listener: None,
    }))
  }

  /// 节流放行判定（C# 循环条件取反：lag 收敛至门限内或驱动已处置）
  #[inline]
  fn throttle_released(&self, max_lag: i64) -> bool {
    self.current_lag() <= max_lag || !self.is_active()
  }

  /// 当前落后字节数（本地日志尾 - 复制位点；资产缺失为 0 直通）
  fn current_lag(&self) -> i64 {
    let Some(assets) = self.assets.as_ref() else {
      return 0;
    };
    let Some(rm) = self.rm.upgrade() else {
      return 0;
    };
    let sublog_idx = self.physical_sublog_idx;
    assets
      .aof
      .log()
      .get_tail_address(sublog_idx)
      .saturating_sub(rm.get_sublog_replication_offset(sublog_idx))
  }

  /// 位点推进通知（背景重放批终点推进后调用，唤醒 ThrottlePrimary 挂起者）
  pub(crate) fn notify_progress(&self) {
    self.progress.notify(usize::MAX);
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:SignalTimeAdvance
  ///
  /// 处理时间脉冲心跳：pending 原子单调记录（过期脉冲直退）；会话线程持有
  /// 重放权（背景重放未启动，对标 sessionThreadOwnsReplay =
  /// maxLag == 0 || replayIterator == null）时就地应用，否则由背景重放循环
  /// 每轮 Throttle 消化。
  pub fn signal_time_advance(&self, sequence_number: i64) {
    if sequence_number <= self.pending_pulse_sequence_number.load(Ordering::Acquire) {
      return;
    }
    self
      .pending_pulse_sequence_number
      .store(sequence_number, Ordering::Release);

    if !self.background_replay_started() && self.resume_replay() {
      self.try_apply_pending_pulse();
      self.suspend_replay();
    }
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:Throttle
  #[inline]
  pub fn throttle(&self) {
    self.try_apply_pending_pulse();
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:TryApplyPendingPulse
  ///
  /// 应用待决时间脉冲：pending 单调越限且复制位点追平日志尾（C#
  /// GetSublogReplicationOffset != TailAddress 守卫——读一致性时间只在
  /// 全量应用后推进）才生效；applied 随应用推进。
  fn try_apply_pending_pulse(&self) {
    let pending = self.pending_pulse_sequence_number.load(Ordering::Acquire);
    if pending <= self.applied_pulse_sequence_number.load(Ordering::Acquire) {
      return;
    }
    let (Some(assets), Some(rm)) = (self.assets.as_ref(), self.rm.upgrade()) else {
      return;
    };
    let sublog_idx = self.physical_sublog_idx;
    if rm.get_sublog_replication_offset(sublog_idx) != assets.aof.log().get_tail_address(sublog_idx)
    {
      return;
    }
    self.apply_pulse(pending);
    self
      .applied_pulse_sequence_number
      .store(pending, Ordering::Release);
  }

  /// libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:ApplyPulse
  ///
  /// 折叠形态：一颗脉冲直推读一致时间——本物理子日志的**全部**虚拟子日志一并
  /// 推进（全槽入口见 ReadConsistencyManager::advance_physical_sublog_time）。
  /// C# 侧单任务形态直推 (physical, 0) 即完备，因为哈希→回放任务下标恒 0，该
  /// 物理子日志只有 (physical, 0) 一槽；本仓把并行重放折叠为单消费者，读侧路由
  /// 仍按回放任务数分槽，故补维不可省：只推 0 槽会让停在非零槽的一致读等待者
  /// 永无放行源。
  ///
  /// 语义安全：脉冲只在复制位点追平日志尾时应用（try_apply_pending_pulse
  /// 守卫），即该物理子日志全部条目均已应用进存储，推进其全部虚拟子日志前沿
  /// 与 C# 「各任务推自有槽」的可观测效果同序。
  fn apply_pulse(&self, sequence_number: i64) {
    let Some(assets) = self.assets.as_ref() else {
      return;
    };
    if let Some(rcm) = assets.aof.read_consistency_manager() {
      rcm.advance_physical_sublog_time(self.physical_sublog_idx, sequence_number);
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// 所有权面与生命周期：重放权 TryEnter/Exit 语义 + dispose 终止背景面
  #[test]
  fn test_replay_driver_ownership_lifecycle() {
    let driver = Arc::new(ReplicaReplayDriver::new(0, None, Weak::new()));
    assert_eq!(driver.physical_sublog_idx, 0);
    assert!(driver.is_active());

    // 重放权：空闲可获取，持有中重复获取失败，释放后可再获取
    assert!(driver.resume_replay());
    assert!(!driver.resume_replay());
    driver.suspend_replay();
    assert!(driver.resume_replay());
    driver.suspend_replay();

    // 退化形态（无资产）：背景重放不可启动，节流装配直通（None = 无需挂起）
    driver.initialize_background_replay_task(0);
    assert!(!driver.background_replay_started());
    assert!(driver.throttle_wait(0).is_none());

    // dispose：生命周期面关闭 + 节流等待者解除
    driver.dispose();
    assert!(!driver.is_active());
    assert!(driver.throttle_wait(0).is_none());
  }

  /// 时间脉冲：pending 单调记录（过期直退）；退化形态（无资产）applied
  /// 不误推进
  #[test]
  fn test_signal_time_advance_monotonic() {
    let driver = ReplicaReplayDriver::new(0, None, Weak::new());
    driver.signal_time_advance(42);
    driver.signal_time_advance(41);
    driver.signal_time_advance(43);
    assert_eq!(
      driver.pending_pulse_sequence_number.load(Ordering::Acquire),
      43,
      "pending 单调记录最新脉冲"
    );
    assert_eq!(
      driver.applied_pulse_sequence_number.load(Ordering::Acquire),
      0,
      "无应用面时 applied 不推进"
    );
    // 内部重放记账位点保持
    assert_eq!(driver.replayed_offset.load(Ordering::Acquire), 0);
  }

  /// throttle_wait 装配门控真值面（对标 C# maxLag != -1 && tail - offset >
  /// maxLag）：-1 禁用直通；资产缺失（lag=0）任何门限直通
  #[test]
  fn test_throttle_wait_gating() {
    let driver = Arc::new(ReplicaReplayDriver::new(0, None, Weak::new()));
    // -1 = 禁用
    assert!(driver.throttle_wait(-1).is_none());
    // 背景未启动（C# replayIterator == null）直通
    assert!(driver.throttle_wait(0).is_none());
    assert!(driver.throttle_wait(1024).is_none());
  }
}
