use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use hdrhistogram::Histogram;
use parking_lot::Mutex;

use super::{
  garnet_latency_metrics::GarnetLatencyMetrics,
  latency_metrics_entry_session::LatencyMetricsEntrySession,
  latency_metrics_type::LatencyMetricsType,
};

/// 存储执行域侧的 PENDING_LAT 计量槽。
///
/// C# 的 storageSession 与会话共持同一个 `GarnetLatencyMetricsSession`
///（Metrics.cs 分部类的 readonly 字段），pending 计时因此能无锁直写会话槽。
/// rust 的会话延迟表为连接任务独占（见 `garnet_latency_metrics_session`），
/// 而执行域经 `Arc<dyn GarnetApiFace>` 只能以 `&self` 记点，故本槽只留
/// PENDING_LAT 一个类别：样本先落每连接一份的直方图，版本翻转时按引用并入
/// 全局表。网络批处理热路径不经过本类型。
pub struct PendingLatencyMeter {
  /// 监视器迭代时钟（与会话延迟表同源，绝对迭代计数即判定基准）。
  monitor_iterations: Arc<AtomicU64>,
  /// 归并出口（对标 C# `monitor.GlobalMetrics.globalLatencyMetrics`）。
  global: Arc<Mutex<GarnetLatencyMetrics>>,
  /// 本窗累积（互斥仅覆盖异步闭环记点与结算，均为慢路径）。
  histogram: Mutex<Histogram<u64>>,
  /// 上次结算时的绝对迭代计数，与会话延迟表同一判定基准；只存奇偶会漏检
  /// 偶数个周期的跨越（ABA 回绕盲区），致 pending 样本永久滞留本地。
  last_iteration: AtomicU64,
}

impl PendingLatencyMeter {
  /// 以全局出口与时钟建槽；直方图界与会话条目同源
  ///（C# `LatencyMetricsEntrySession` 的两界 + 2 位有效数字）。
  pub fn new(monitor_iterations: Arc<AtomicU64>, global: Arc<Mutex<GarnetLatencyMetrics>>) -> Self {
    let last_iteration = monitor_iterations.load(Ordering::Relaxed);
    Self {
      monitor_iterations,
      global,
      histogram: Mutex::new(
        Histogram::new_with_bounds(
          LatencyMetricsEntrySession::HISTOGRAM_LOWER_BOUND,
          LatencyMetricsEntrySession::HISTOGRAM_UPPER_BOUND,
          2,
        )
        .expect("直方图边界为编译期常量，构造必成功"),
      ),
      last_iteration: AtomicU64::new(last_iteration),
    }
  }

  /// 记入一段 pending 耗时：越界收敛上界对齐 C# 会话条目；0 值丢弃系对 C#
  /// 真实调用链的修正——C# 对位路径是单参 `RecordValue(int ver)`
  ///（Storage/Session/Metrics.cs:StopPendingMetrics → GarnetLatencyMetricsSession.cs:Stop
  /// → LatencyMetricsEntrySession.cs:40-50），elapsed==0 因 LOWER_BOUND=1
  /// 非法区间落 HISTOGRAM_UPPER_BOUND（100s 上界巨值）系原型缺陷，丢弃裁决
  /// 见 doc/zh/deviations.md「零耗时 pending 样本」。时钟跨迭代周期即先把
  /// 上一窗样本并入全局再记本点。
  #[inline]
  pub fn record(&self, elapsed: i64) {
    if elapsed == 0 {
      return;
    }
    let value = if LatencyMetricsEntrySession::is_valid_range(elapsed) {
      elapsed as u64
    } else {
      LatencyMetricsEntrySession::HISTOGRAM_UPPER_BOUND
    };
    let mut histogram = self.histogram.lock();
    if self.rolled() {
      self.settle(&mut histogram);
    }
    // 记录失败仅可能因越界，已收敛上界，故忽略返回值。
    let _ = histogram.record(value);
  }

  /// 会话/执行域释放收口：把未结算样本并入全局。
  pub fn flush(&self) {
    self.settle(&mut self.histogram.lock());
  }

  /// 未结算样本数（观测面）。
  pub fn pending_samples(&self) -> u64 {
    self.histogram.lock().len()
  }

  /// 时钟是否跨越迭代周期（同时把基准推进到当前绝对迭代号）。swap 使
  /// 同一周期内并发记点恰有一路触发结算，其余路见基准已新即直接记点。
  #[inline]
  fn rolled(&self) -> bool {
    let curr = self.monitor_iterations.load(Ordering::Relaxed);
    self.last_iteration.swap(curr, Ordering::Relaxed) != curr
  }

  /// 按引用并入全局出口后清零本槽（调用方持 histogram 锁）。
  fn settle(&self, histogram: &mut Histogram<u64>) {
    self
      .global
      .lock()
      .merge_histogram(LatencyMetricsType::PendingLat, histogram);
  }
}

impl Drop for PendingLatencyMeter {
  /// 最后一个持有者释放（连接执行域随会话析构）时结算残余样本，对位 C#
  /// Dispose 尾部监视器侧的会话延迟归并臂——rust 该臂改由属主线程发起。
  fn drop(&mut self) {
    self.flush();
  }
}
