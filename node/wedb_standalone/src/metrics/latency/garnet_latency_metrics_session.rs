use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use parking_lot::RwLock;

use super::{
  latency_metrics_entry_session::LatencyMetricsEntrySession,
  latency_metrics_type::LatencyMetricsType,
};

/// 会话侧延迟指标：双缓冲直方图组，版本由监视器迭代次数奇偶切换。
///（对标 libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:GarnetLatencyMetricsSession）
///
/// C# 直接回读 `monitor.monitor_iterations`；Rust 侧以共享原子迭代计数承接，
/// 由 GarnetServerMonitor 持有并驱动。C# 的 SingleWriterMultiReaderLock dispose
/// 锁以 `RwLock<Option<..>>` 承接：释放（Return）置空，读写方按 None 优雅退出。
pub struct GarnetLatencyMetricsSession {
  /// 监视器迭代时钟（奇偶即版本 0/1）。
  monitor_iterations: Arc<AtomicU64>,
  /// 全部延迟类别的会话条目；释放后为 None（对齐 C# Return 后的 null）。
  metrics: RwLock<Option<Vec<LatencyMetricsEntrySession>>>,
}

impl GarnetLatencyMetricsSession {
  /// 默认统计的全部延迟类别（对齐 C# defaultLatencyTypes）。
  pub const DEFAULT_LATENCY_TYPES: &[LatencyMetricsType] = &LatencyMetricsType::ALL;

  /// libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:GarnetLatencyMetricsSession（构造 + Init）。
  pub fn new(
    monitor_iterations: Arc<AtomicU64>,
    latency_types: &'static [LatencyMetricsType],
  ) -> Self {
    Self {
      monitor_iterations,
      metrics: RwLock::new(Some(
        latency_types
          .iter()
          .map(|_| LatencyMetricsEntrySession::new())
          .collect(),
      )),
    }
  }

  /// 当前版本（迭代次数 % 2）。
  #[inline]
  pub fn version(&self) -> usize {
    (self.monitor_iterations.load(Ordering::Relaxed) % 2) as usize
  }

  /// 上一版本（合并方向）。
  #[inline]
  pub fn prior_version(&self) -> usize {
    1 - self.version()
  }

  /// 指标快照克隆（合并方经此访问，对齐 C# 同程序集直读字段）。
  pub fn metrics_snapshot(&self) -> Option<Vec<LatencyMetricsEntrySession>> {
    self.metrics.read().clone()
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:Return
  ///
  /// 归还全部条目并置空（服务器停机中会话释放时的优雅路径）。
  pub fn return_to_pool(&self) {
    if let Some(mut entries) = self.metrics.write().take() {
      for entry in &mut entries {
        entry.return_to_pool();
      }
    }
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:Start
  ///
  /// 记录指定类别的起始时间戳。`now_ticks` 为当前 Stopwatch tick（单调时钟）。
  #[inline]
  pub fn start(&self, cmd: LatencyMetricsType, now_ticks: u64) {
    if let Some(entry) = self
      .metrics
      .write()
      .as_deref_mut()
      .and_then(|m| m.get_mut(cmd.idx()))
    {
      entry.start(now_ticks);
    }
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:Get
  ///
  /// 读取指定类别的起始时间戳。
  #[inline]
  pub fn get(&self, cmd: LatencyMetricsType) -> u64 {
    self
      .metrics
      .read()
      .as_deref()
      .and_then(|m| m.get(cmd.idx()))
      .map_or(0, |entry| entry.start_timestamp)
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:StopAndSwitch
  ///
  /// 停止旧类别并将起始时间戳转移给新类别后记录（同一操作跨类别切换）。
  #[inline]
  pub fn stop_and_switch(
    &self,
    old_cmd: LatencyMetricsType,
    new_cmd: LatencyMetricsType,
    now_ticks: u64,
  ) {
    let mut metrics = self.metrics.write();
    let Some(entries) = metrics.as_deref_mut() else {
      return;
    };
    let (old_idx, new_idx) = (old_cmd.idx(), new_cmd.idx());
    if old_idx == new_idx {
      if let Some(entry) = entries.get_mut(old_idx) {
        entry.start_timestamp = 0;
        entry.record_value(self.version(), now_ticks);
      }
      return;
    }
    let pair = if old_idx < new_idx {
      let (left, right) = entries.split_at_mut(new_idx);
      left.get_mut(old_idx).zip(right.first_mut())
    } else {
      let (left, right) = entries.split_at_mut(old_idx);
      right.first_mut().zip(left.get_mut(new_idx))
    };
    let Some((old_entry, new_entry)) = pair else {
      return;
    };
    new_entry.start_timestamp = old_entry.start_timestamp;
    old_entry.start_timestamp = 0;
    new_entry.record_value(self.version(), now_ticks);
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:Stop
  ///
  /// 结束指定类别的进行中操作并按当前版本记录。
  #[inline]
  pub fn stop(&self, cmd: LatencyMetricsType, now_ticks: u64) {
    let ver = self.version();
    if let Some(entry) = self
      .metrics
      .write()
      .as_deref_mut()
      .and_then(|m| m.get_mut(cmd.idx()))
    {
      entry.record_value(ver, now_ticks);
    }
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:RecordValue
  ///
  /// 直接记录一段耗时（按当前版本）。
  #[inline]
  pub fn record_value(&self, cmd: LatencyMetricsType, elapsed: i64) {
    let ver = self.version();
    if let Some(entry) = self
      .metrics
      .write()
      .as_deref_mut()
      .and_then(|m| m.get_mut(cmd.idx()))
    {
      entry.record_elapsed(ver, elapsed);
    }
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:ResetAll
  ///
  /// 重置全部类别（按上一版本缓冲）。
  pub fn reset_all(&self) {
    for cmd in Self::DEFAULT_LATENCY_TYPES {
      self.reset(*cmd);
    }
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:Reset
  ///
  /// 重置指定类别的上一版本缓冲；指标已释放时忽略。
  pub fn reset(&self, cmd: LatencyMetricsType) {
    let ver = self.prior_version();
    if let Some(entry) = self
      .metrics
      .write()
      .as_deref_mut()
      .and_then(|m| m.get_mut(cmd.idx()))
    {
      entry.latency[ver].reset();
    }
  }
}
