//! 主侧复制背压闸门（对标 libs/server/AOF/AofBackpressure.cs:AofBackpressure）。
//!
//! 以"每子日志已发布（ship）水位 + 每子日志字节预算"实现：
//! 追加方在尾部地址领先水位超过预算时等待复制端推进发布水位。
//! 采用 128B 缓存行对齐原子整型消除跨核伪共享与相邻行预取颠簸，
//! 日志句柄装配期一次性绑定（OnceLock，对标 C# SetLog volatile 单次写引用），
//! 校验路径零锁。等待臂两形态：同步臂 `wait_slow` 服务命令执行栈内的追加
//! 闸（自旋上限 + 有界 park 轮询，C# `Thread.Sleep(PollIntervalMs)` 轮询自醒
//! 同构——notify 即时唤醒外，每 [`PARK_POLL_INTERVAL`] 周期自醒独立复查
//! 水位原子面，放行不依赖任何发布方 notify 到达）；异步臂 `wait_async`
//! 服务异步上下文，event_listener 挂起不占线程。
//!
//! ⚠️ 残余冻结面（如实声明）：同步臂 park 冻结宿主 compio worker 执行器，
//! 与追加任务同执行器的发布任务（推流泵/溢流泵/副本连接会话，宿主为
//! acceptor worker）在冻结期不可推进——全 worker 均有追加方过闸且推流泵
//! 宿主同被冻结时，落网水位停滞，仅余他执行器发布方（装配主运行时的
//! 节流循环不执行命令任务、不受追加闸冻结）与周期自醒复查承接：预算
//! 热改约（CONFIG SET）/ 停机 dispose / 他线程水位推进任一落地即放行。
//! 推流泵完整迁出追加 worker 受 compio 线程本地 spawn（跨线程 spawn 须
//! Send + 类型擦除）与扫描设备运行时亲和限制，见票 zcode-r41-wakeup。

use std::{
  hint::spin_loop,
  sync::{
    OnceLock, Weak,
    atomic::{AtomicBool, AtomicI64, Ordering},
  },
  time::Duration,
};

use event_listener::{Event, Listener};

use super::garnet_log::GarnetLog;

/// 同步慢路径自旋上限次数（微秒级极短毛刺退避）
const SYNC_SPIN_LIMIT: u32 = 16;

/// 同步慢路径有界 park 轮询间隔（对标 C# `AofBackpressure.PollIntervalMs = 1`：
/// 等待者周期自醒独立复查水位，放行不依赖发布方 notify 到达）
const PARK_POLL_INTERVAL: Duration = Duration::from_millis(1);

/// 发布水位告警阈值比例分母（对标 C# AofBackpressure.cs:SetBudget 的
/// `publishDeltaBytes = Math.Max(perSublogBudget / 8, 1)`：每子日志预算的
/// 1/8 为发布停滞告警阈值，下限 1 字节）
const PUBLISH_DELTA_RATIO: i64 = 8;

/// 子日志尾地址句柄（弱引用防与 GarnetLog 引用环，避免虚表开销）。
pub(crate) enum LogTailHandle {
  GarnetWeak(Weak<GarnetLog>),
}

impl LogTailHandle {
  /// 取指定子日志尾地址。
  ///
  /// 生产装配唯一入口是 [`Self::GarnetWeak`]，经
  /// [`AofBackpressure::set_weak_log`] 在 GarnetAppendOnlyFile 构造期绑定。
  #[inline]
  pub fn get_tail_address(&self, sublog_idx: usize) -> Option<i64> {
    match self {
      Self::GarnetWeak(weak) => weak.upgrade().map(|arc| arc.get_tail_address(sublog_idx)),
    }
  }
}

use wbase::align::CachePadded;

/// libs/server/AOF/AofBackpressure.cs:AofBackpressure
///
/// 主侧复制背压闸门。
pub struct AofBackpressure {
  /// 每子日志字节预算（禁用时为 i64::MAX）。
  pub(crate) per_sublog_budget: AtomicI64,
  /// 发布水位推进告警阈值。
  publish_delta_bytes: AtomicI64,
  /// 是否启用。
  enabled: AtomicBool,
  /// 子日志数。
  sublog_count: usize,
  /// 各子日志已发布水位（128B 缓存行对齐，固定容量切片）。
  shipped_watermark: Box<[CachePadded<AtomicI64>]>,
  /// 关停标志：置位后所有等待方立即放行。
  disposed: AtomicBool,
  /// 拥有日志（尾地址反查）：装配期一次性绑定，查询路径零锁
  ///（libs/server/AOF/AofBackpressure.cs:SetLog volatile 引用同语义）。
  log: OnceLock<LogTailHandle>,
  /// 轻量无锁事件通知驱动（用于异步挂起与快速唤醒，不阻塞线程）。
  event: Event,
}

impl AofBackpressure {
  /// libs/server/AOF/AofBackpressure.cs:AofBackpressure（构造）。
  pub fn new(sublog_count: usize, aof_sync_max_lag_bytes: i64) -> Self {
    let shipped_watermark = (0..sublog_count)
      .map(|_| CachePadded::new(AtomicI64::new(i64::MAX)))
      .collect::<Vec<_>>()
      .into_boxed_slice();

    let gate = Self {
      per_sublog_budget: AtomicI64::new(i64::MAX),
      publish_delta_bytes: AtomicI64::new(1),
      enabled: AtomicBool::new(false),
      sublog_count,
      shipped_watermark,
      disposed: AtomicBool::new(false),
      log: OnceLock::new(),
      event: Event::new(),
    };
    gate.set_budget(aof_sync_max_lag_bytes);
    gate
  }

  /// libs/server/AOF/AofBackpressure.cs:SetBudget
  pub fn set_budget(&self, aof_sync_max_lag_bytes: i64) {
    if aof_sync_max_lag_bytes > 0 {
      let per_sublog_budget = (aof_sync_max_lag_bytes / self.sublog_count as i64).max(1);
      self
        .per_sublog_budget
        .store(per_sublog_budget, Ordering::Release);
      self.publish_delta_bytes.store(
        (per_sublog_budget / PUBLISH_DELTA_RATIO).max(1),
        Ordering::Release,
      );
      self.enabled.store(true, Ordering::Release);
    } else {
      self.per_sublog_budget.store(i64::MAX, Ordering::Release);
      self.publish_delta_bytes.store(1, Ordering::Release);
      self.enabled.store(false, Ordering::Release);
    }
    self.event.notify(usize::MAX);
  }

  #[inline]
  pub fn enabled(&self) -> bool {
    self.enabled.load(Ordering::Acquire)
  }

  #[inline]
  pub fn publish_delta_bytes(&self) -> i64 {
    self.publish_delta_bytes.load(Ordering::Relaxed)
  }

  /// 检查指定子日志是否满足放行条件。
  ///
  /// 优化：优先以 `captured_tail` 判定，避免在快照已达标时无谓调用 `query_tail_address`。
  #[inline]
  pub fn is_released(&self, sublog_idx: usize, captured_tail: i64) -> bool {
    if self.disposed.load(Ordering::Acquire) || !self.enabled() {
      return true;
    }
    let watermark = self.shipped_watermark[sublog_idx].load(Ordering::Acquire);
    let budget = self.per_sublog_budget.load(Ordering::Relaxed);

    // 优先以进入时的快照判定（99.9% 场景免去 query_tail_address 读锁与查表开销）
    if captured_tail.wrapping_sub(watermark) <= budget {
      return true;
    }

    // 自愈慢路径：若 tail 发生回退（truncate/reset），重读实时尾地址
    let live_tail = self.query_tail_address(sublog_idx).unwrap_or(captured_tail);
    live_tail.wrapping_sub(watermark) <= budget
  }

  /// libs/server/AOF/AofBackpressure.cs:Wait
  #[inline]
  pub fn wait(&self, sublog_idx: usize, tail_address: i64) {
    if !self.enabled() {
      return;
    }
    let watermark = self.shipped_watermark[sublog_idx].load(Ordering::Acquire);
    if tail_address.wrapping_sub(watermark) <= self.per_sublog_budget.load(Ordering::Relaxed) {
      return;
    }
    self.wait_slow(sublog_idx, tail_address);
  }

  /// 异步背压等待。
  ///
  /// 快路径内联校验；慢路径通过 `event_listener::Event` 异步挂起，不阻塞 compio worker 线程。
  #[inline]
  pub async fn wait_async(&self, sublog_idx: usize, tail_address: i64) {
    if !self.enabled() {
      return;
    }
    let watermark = self.shipped_watermark[sublog_idx].load(Ordering::Acquire);
    if tail_address.wrapping_sub(watermark) <= self.per_sublog_budget.load(Ordering::Relaxed) {
      return;
    }
    self.wait_slow_async(sublog_idx, tail_address).await;
  }

  #[inline]
  fn query_tail_address(&self, sublog_idx: usize) -> Option<i64> {
    self.log.get()?.get_tail_address(sublog_idx)
  }

  /// libs/server/AOF/AofBackpressure.cs:WaitSlow（`Thread.Sleep(PollIntervalMs)`
  /// 轮询自醒形态的 compio 投影：自旋上限退避微毛刺，有界 park 承接长等待）
  pub fn wait_slow(&self, sublog_idx: usize, captured_tail: i64) {
    for _ in 0..SYNC_SPIN_LIMIT {
      if self.is_released(sublog_idx, captured_tail) {
        return;
      }
      spin_loop();
    }
    while !self.is_released(sublog_idx, captured_tail) {
      let listener = self.event.listen();
      if self.is_released(sublog_idx, captured_tail) {
        break;
      }
      // 有界 park：notify 即时唤醒 + 每 PARK_POLL_INTERVAL 周期自醒独立复查。
      // 等待臂运行于 compio worker 命令执行栈内，park 冻结宿主执行器全部
      // 任务（含同执行器发布任务，见模块头残余冻结面声明）——周期自醒
      // 保证等待方不被任何单点 notify 绑死：水位原子推进（他执行器发布方
      // /预算热改约/停机 dispose）落地即自行放行，与 C# 轮询自醒同构
      listener.wait_timeout(PARK_POLL_INTERVAL);
    }
  }

  /// 异步慢路径：当 lag > budget 时，注册监听并在水位推进前异步挂起，完全无锁、不占线程。
  async fn wait_slow_async(&self, sublog_idx: usize, captured_tail: i64) {
    while !self.is_released(sublog_idx, captured_tail) {
      let listener = self.event.listen();
      if self.is_released(sublog_idx, captured_tail) {
        break;
      }
      listener.await;
    }
  }

  /// libs/server/AOF/AofBackpressure.cs:AnyStalled
  #[inline]
  pub fn any_stalled(&self) -> bool {
    if !self.enabled() {
      return false;
    }
    let budget = self.per_sublog_budget.load(Ordering::Relaxed);
    let handle = self.log.get();
    for (i, watermark) in self.shipped_watermark.iter().enumerate() {
      let tail = handle.and_then(|h| h.get_tail_address(i)).unwrap_or(0);
      if tail.wrapping_sub(watermark.load(Ordering::Acquire)) > budget {
        return true;
      }
    }
    false
  }

  /// 装配期一次性绑定日志句柄（重复绑定拒绝并告警；对标 C# SetLog 直写引用）
  #[inline]
  pub fn set_weak_log(&self, log: Weak<GarnetLog>) {
    if self.log.set(LogTailHandle::GarnetWeak(log)).is_err() {
      log::warn!("AofBackpressure 日志句柄重复绑定，拒绝");
    }
  }

  /// libs/server/AOF/AofBackpressure.cs:PublishShippedAddress
  #[inline]
  pub fn publish_shipped_address(&self, sublog_idx: usize, min_shipped_address: i64) {
    self.shipped_watermark[sublog_idx].store(min_shipped_address, Ordering::Release);
    self.event.notify(usize::MAX);
  }

  /// libs/server/AOF/AofBackpressure.cs:Dispose
  #[inline]
  pub fn dispose(&self) {
    self.disposed.store(true, Ordering::Release);
    self.event.notify(usize::MAX);
  }

  /// 测试观测面：读取当前在册事件监听器数
  #[inline]
  pub fn total_listeners(&self) -> usize {
    self.event.total_listeners()
  }

  /// 测试观测面：读取每子日志字节预算
  #[inline]
  pub fn per_sublog_budget(&self) -> i64 {
    self.per_sublog_budget.load(Ordering::Relaxed)
  }
}
