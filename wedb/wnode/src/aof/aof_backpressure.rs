//! 主侧复制背压闸门（对标 libs/server/AOF/AofBackpressure.cs:AofBackpressure）。
//!
//! 以"每子日志已发布（ship）水位 + 每子日志字节预算"实现：
//! 追加方在尾部地址领先水位超过预算时等待复制端推进发布水位。
//! 采用 128B 缓存行对齐原子整型消除跨核伪共享与相邻行预取颠簸，
//! 日志句柄装配期一次性绑定（OnceLock，对标 C# SetLog volatile 单次写引用），
//! 校验路径零锁；使用 event_listener 无锁事件驱动异步挂起与快速唤醒，不阻塞 compio worker 线程。

use std::{
  hint::spin_loop,
  sync::{
    Arc, OnceLock, Weak,
    atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
  },
};

use event_listener::{Event, Listener};

use super::garnet_log::GarnetLog;

/// 同步慢路径自旋上限次数（微秒级极短毛刺退避）
const SYNC_SPIN_LIMIT: u32 = 16;

/// 子日志尾地址句柄（弱引用防与 GarnetLog 引用环，避免虚表开销）。
pub enum LogTailHandle {
  GarnetWeak(Weak<GarnetLog>),
  Counter(Arc<AtomicU64>),
}

impl LogTailHandle {
  /// 取指定子日志尾地址。
  ///
  /// `Counter` 支为测试替身（单测与集成测试以计数器模拟尾地址推进）；
  /// 生产装配唯一入口是 [`Self::GarnetWeak`]，经
  /// [`AofBackpressure::set_weak_log`] 在 GarnetAppendOnlyFile 构造期绑定。
  #[inline]
  pub fn get_tail_address(&self, sublog_idx: usize) -> Option<i64> {
    match self {
      Self::GarnetWeak(weak) => weak.upgrade().map(|arc| arc.get_tail_address(sublog_idx)),
      Self::Counter(c) => Some(c.load(Ordering::Relaxed) as i64),
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
      self
        .publish_delta_bytes
        .store((per_sublog_budget / 8).max(1), Ordering::Release);
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

  /// libs/server/AOF/AofBackpressure.cs:WaitSlow
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
      listener.wait();
    }
  }

  /// 异步慢路径：当 lag > budget 时，注册监听并在水位推进前异步挂起，完全无锁、不占线程。
  pub async fn wait_slow_async(&self, sublog_idx: usize, captured_tail: i64) {
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

  #[inline]
  pub fn set_counter_log(&self, counter: Arc<AtomicU64>) {
    if self.log.set(LogTailHandle::Counter(counter)).is_err() {
      log::warn!("AofBackpressure 日志句柄重复绑定，拒绝");
    }
  }

  /// 获取指定子日志当前已发布的水位线
  #[inline]
  pub fn get_shipped_watermark(&self, sublog_idx: usize) -> i64 {
    self.shipped_watermark[sublog_idx].load(Ordering::Acquire)
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
}

#[cfg(test)]
mod tests {
  use std::{
    sync::{
      Arc,
      atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
    },
    thread,
  };

  use compio::runtime::Runtime;

  use super::AofBackpressure;

  #[test]
  fn budget_enables_and_disables_gate() {
    let gate = AofBackpressure::new(2, 1024);
    assert!(gate.enabled());
    assert_eq!(gate.publish_delta_bytes(), 1024 / 2 / 8);
    assert_eq!(gate.per_sublog_budget.load(AtomicOrdering::Relaxed), 512);

    // 预算 <= 0 禁用。
    let off = AofBackpressure::new(2, -1);
    assert!(!off.enabled());
    assert!(!off.any_stalled());
  }

  #[test]
  fn wait_passes_within_budget_and_stalls_beyond() {
    let tail = Arc::new(AtomicU64::new(0));
    let gate = AofBackpressure::new(1, 100);
    assert_eq!(gate.publish_delta_bytes(), 100 / 8);
    gate.set_counter_log(tail.clone());

    // 水位 i64::MAX（无复制端）直接放行。
    gate.wait(0, 10_000);
    assert!(!gate.any_stalled());

    // 复制端附着：水位 0，尾 50 在预算 100 内。
    gate.publish_shipped_address(0, 0);
    tail.store(50, AtomicOrdering::Relaxed);
    assert!(!gate.any_stalled());

    // 尾 200 超预算 100：出现滞后。
    tail.store(200, AtomicOrdering::Relaxed);
    assert!(gate.any_stalled());

    // 水位推进到预算内即解除。
    gate.publish_shipped_address(0, 150);
    assert!(!gate.any_stalled());
  }

  #[test]
  fn dispose_releases_all() {
    let gate = AofBackpressure::new(1, 100);
    gate.publish_shipped_address(0, 0);
    // 尾地址超预算且无复制端推进：处于滞留态。
    assert!(!gate.is_released(0, 1_000_000));

    gate.dispose();
    // 关停置位后立即放行，同步慢路径不阻塞。
    assert!(gate.is_released(0, 1_000_000));
    gate.wait_slow(0, 1_000_000);
  }

  #[test]
  fn sync_cross_thread_event_wake() {
    let tail = Arc::new(AtomicU64::new(200));
    let gate = Arc::new(AofBackpressure::new(1, 100));
    gate.set_counter_log(tail);
    gate.publish_shipped_address(0, 0);

    let gate_clone = gate.clone();
    let thread_handle = thread::spawn(move || {
      while gate_clone.event.total_listeners() == 0 {
        thread::yield_now();
      }
      gate_clone.publish_shipped_address(0, 150);
    });

    gate.wait(0, 200);
    thread_handle.join().unwrap();
  }

  #[test]
  fn async_wait_event_driven_wake() {
    let tail = Arc::new(AtomicU64::new(200));
    let gate = Arc::new(AofBackpressure::new(1, 100));
    gate.set_counter_log(tail);
    gate.publish_shipped_address(0, 0);

    let gate_clone = gate.clone();
    let thread_handle = thread::spawn(move || {
      while gate_clone.event.total_listeners() == 0 {
        thread::yield_now();
      }
      gate_clone.publish_shipped_address(0, 150);
    });

    Runtime::new().unwrap().block_on(async {
      gate.wait_async(0, 200).await;
    });

    thread_handle.join().unwrap();
  }

  #[test]
  fn async_wait_dispose_wake() {
    let tail = Arc::new(AtomicU64::new(500));
    let gate = Arc::new(AofBackpressure::new(1, 100));
    gate.set_counter_log(tail);
    gate.publish_shipped_address(0, 0);

    let done = Arc::new(AtomicBool::new(false));
    let done_clone = done.clone();
    let gate_clone = gate.clone();

    let thread_handle = thread::spawn(move || {
      while gate_clone.event.total_listeners() == 0 {
        thread::yield_now();
      }
      gate_clone.dispose();
      done_clone.store(true, AtomicOrdering::Release);
    });

    Runtime::new().unwrap().block_on(async {
      gate.wait_async(0, 500).await;
    });

    assert!(done.load(AtomicOrdering::Acquire));
    thread_handle.join().unwrap();
  }

  #[test]
  fn multi_sublog_async_wait() {
    let tail = Arc::new(AtomicU64::new(300));
    let gate = Arc::new(AofBackpressure::new(2, 200));
    gate.set_counter_log(tail);
    gate.publish_shipped_address(0, 0);
    gate.publish_shipped_address(1, 0);

    let gate_clone = gate.clone();
    let thread_handle = thread::spawn(move || {
      while gate_clone.event.total_listeners() == 0 {
        thread::yield_now();
      }
      // sublog 1 水位推进到 250 (300 - 250 = 50 <= budget 100)
      gate_clone.publish_shipped_address(1, 250);
    });

    Runtime::new().unwrap().block_on(async {
      gate.wait_async(1, 300).await;
    });

    thread_handle.join().unwrap();
  }
}
