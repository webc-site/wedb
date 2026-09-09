//! 主侧复制背压闸门（对标 libs/server/AOF/AofBackpressure.cs:AofBackpressure）。
//!
//! 以"每子日志已发布（ship）水位 + 每子日志字节预算"实现：
//! 追加方在尾部地址领先水位超过预算时自旋等待复制端发布水位。

use std::sync::{
  Arc,
  atomic::{AtomicBool, AtomicI64, Ordering},
};

use parking_lot::Mutex;

/// 慢路径轮询间隔（毫秒）。
///（libs/server/AOF/AofBackpressure.cs:PollIntervalMs）
pub const POLL_INTERVAL_MS: u64 = 1;

/// 子日志尾地址提供方（解耦 GarnetLog；等价 C# 的 log 字段反查）。
pub trait LogTail: Send + Sync {
  /// 返回指定子日志的尾地址。
  fn get_tail_address(&self, sublog_idx: usize) -> i64;
}

/// 主侧复制背压闸门。
pub struct AofBackpressure {
  /// 每子日志字节预算（禁用时为 i64::MAX）。
  per_sublog_budget: AtomicI64,
  /// 发布水位推进告警阈值。
  publish_delta_bytes: AtomicI64,
  /// 是否启用。
  enabled: AtomicBool,
  /// 子日志数。
  sublog_count: usize,
  /// 各子日志已发布水位（padded 原子量）。
  shipped_watermark: Vec<AtomicI64>,
  /// 关停标志：置位后所有等待方立即放行。
  disposed: AtomicBool,
  /// 拥有日志（尾地址反查），构造后经 set_log 注入。
  log: Mutex<Option<Arc<dyn LogTail>>>,
}

impl AofBackpressure {
  /// libs/server/AOF/AofBackpressure.cs:AofBackpressure（构造）。
  ///
  /// 水位初始为 i64::MAX（无复制端附着时直接放行），预算取自
  /// aof_sync_max_lag_bytes。
  pub fn new(sublog_count: usize, aof_sync_max_lag_bytes: i64) -> Self {
    let shipped_watermark = (0..sublog_count)
      .map(|_| AtomicI64::new(i64::MAX))
      .collect();
    let gate = Self {
      per_sublog_budget: AtomicI64::new(i64::MAX),
      publish_delta_bytes: AtomicI64::new(1),
      enabled: AtomicBool::new(false),
      sublog_count,
      shipped_watermark,
      disposed: AtomicBool::new(false),
      log: Mutex::new(None),
    };
    gate.set_budget(aof_sync_max_lag_bytes);
    gate
  }

  /// libs/server/AOF/AofBackpressure.cs:SetBudget
  ///
  /// 预算 > 0 时启用：每子日志预算 = max(1, total / 子日志数)，
  /// 发布增量 = max(1, 每子日志预算 / 8)。
  pub fn set_budget(&self, aof_sync_max_lag_bytes: i64) {
    if aof_sync_max_lag_bytes > 0 {
      let per_sublog_budget = (aof_sync_max_lag_bytes / self.sublog_count as i64).max(1);
      self
        .per_sublog_budget
        .store(per_sublog_budget, Ordering::Relaxed);
      self
        .publish_delta_bytes
        .store((per_sublog_budget / 8).max(1), Ordering::Relaxed);
      self.enabled.store(true, Ordering::Relaxed);
    } else {
      self.per_sublog_budget.store(i64::MAX, Ordering::Relaxed);
      self.publish_delta_bytes.store(1, Ordering::Relaxed);
      self.enabled.store(false, Ordering::Relaxed);
    }
  }

  /// 是否启用。
  pub fn enabled(&self) -> bool {
    self.enabled.load(Ordering::Relaxed)
  }

  /// 发布水位推进告警阈值（C# PublishDeltaBytes 属性）。
  pub fn publish_delta_bytes(&self) -> i64 {
    self.publish_delta_bytes.load(Ordering::Relaxed)
  }

  /// libs/server/AOF/AofBackpressure.cs:Wait
  ///
  /// 快路径：尾部地址与水位的差未超预算直接放行；
  /// 慢路径（`wait_slow`）轮询直至预算内或关停。
  pub fn wait(&self, sublog_idx: usize, tail_address: i64) {
    if !self.enabled() {
      return;
    }
    let watermark = self.shipped_watermark[sublog_idx].load(Ordering::Acquire);
    if tail_address - watermark <= self.per_sublog_budget.load(Ordering::Relaxed) {
      return;
    }
    self.wait_slow(sublog_idx, tail_address);
  }

  /// libs/server/AOF/AofBackpressure.cs:WaitSlow
  ///
  /// 轮询水位直至预算内（实时尾地址经日志反查刷新）或关停。
  pub fn wait_slow(&self, sublog_idx: usize, captured_tail: i64) {
    while !self.disposed.load(Ordering::Relaxed) {
      let live_tail = self
        .log
        .lock()
        .as_ref()
        .map_or(captured_tail, |log| log.get_tail_address(sublog_idx));
      let watermark = self.shipped_watermark[sublog_idx].load(Ordering::Acquire);
      if live_tail - watermark <= self.per_sublog_budget.load(Ordering::Relaxed) {
        break;
      }
      std::thread::sleep(std::time::Duration::from_millis(POLL_INTERVAL_MS));
    }
  }

  /// libs/server/AOF/AofBackpressure.cs:AnyStalled
  ///
  /// 闸门启用且任一子日志当前滞后（实时尾 − 已发布水位）超预算。
  pub fn any_stalled(&self) -> bool {
    if !self.enabled() {
      return false;
    }
    let log = self.log.lock();
    for (i, watermark) in self.shipped_watermark.iter().enumerate() {
      let tail = log.as_ref().map_or(0, |log| log.get_tail_address(i));
      if tail - watermark.load(Ordering::Acquire) > self.per_sublog_budget.load(Ordering::Relaxed) {
        return true;
      }
    }
    false
  }

  /// libs/server/AOF/AofBackpressure.cs:SetLog
  ///
  /// 注入拥有日志的反查引用（仅构造期调用，不参与门控路径）。
  pub fn set_log(&self, log: Arc<dyn LogTail>) {
    *self.log.lock() = Some(log);
  }

  /// libs/server/AOF/AofBackpressure.cs:PublishShippedAddress
  ///
  /// 发布子日志跨副本的最小已发布地址（无副本附着时为 i64::MAX，直接放行）。
  /// 陈旧值安全：只会高估追加方滞后。
  pub fn publish_shipped_address(&self, sublog_idx: usize, min_shipped_address: i64) {
    self.shipped_watermark[sublog_idx].store(min_shipped_address, Ordering::Release);
  }

  /// libs/server/AOF/AofBackpressure.cs:Dispose
  ///
  /// 永久放行所有滞后的追加方（服务器停机）。
  pub fn dispose(&self) {
    self.disposed.store(true, Ordering::Relaxed);
  }
}

#[cfg(test)]
mod tests {
  use std::sync::{Arc, atomic::AtomicU64};

  use super::{AofBackpressure, LogTail};

  struct FakeLog {
    tail: AtomicU64,
  }

  impl LogTail for FakeLog {
    fn get_tail_address(&self, _sublog_idx: usize) -> i64 {
      self.tail.load(std::sync::atomic::Ordering::Relaxed) as i64
    }
  }

  #[test]
  fn budget_enables_and_disables_gate() {
    let gate = AofBackpressure::new(2, 1024);
    assert!(gate.enabled());
    assert_eq!(gate.publish_delta_bytes(), 1024 / 2 / 8);
    assert_eq!(
      gate
        .per_sublog_budget
        .load(std::sync::atomic::Ordering::Relaxed),
      512
    );

    // 预算 <= 0 禁用。
    let off = AofBackpressure::new(2, -1);
    assert!(!off.enabled());
    assert!(!off.any_stalled());
  }

  #[test]
  fn wait_passes_within_budget_and_stalls_beyond() {
    let log = Arc::new(FakeLog {
      tail: AtomicU64::new(0),
    });
    let gate = AofBackpressure::new(1, 100);
    assert_eq!(gate.publish_delta_bytes(), 100 / 8);
    gate.set_log(log.clone());

    // 水位 i64::MAX（无复制端）直接放行。
    gate.wait(0, 10_000);
    assert!(!gate.any_stalled());

    // 复制端附着：水位 0，尾 50 在预算 100 内。
    gate.publish_shipped_address(0, 0);
    log.tail.store(50, std::sync::atomic::Ordering::Relaxed);
    assert!(!gate.any_stalled());

    // 尾 200 超预算 100：出现滞后。
    log.tail.store(200, std::sync::atomic::Ordering::Relaxed);
    assert!(gate.any_stalled());

    // 水位推进到预算内即解除。
    gate.publish_shipped_address(0, 150);
    assert!(!gate.any_stalled());
  }

  #[test]
  fn dispose_releases_all() {
    let gate = AofBackpressure::new(1, 100);
    gate.publish_shipped_address(0, 0);
    gate.dispose();
    // 关停后慢路径立即返回（不阻塞测试）。
    gate.wait_slow(0, 1_000_000);
  }
}
