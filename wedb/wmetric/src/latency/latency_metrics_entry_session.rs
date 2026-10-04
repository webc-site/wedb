use hdrhistogram::Histogram;
use wbase::convert::stopwatch::seconds;

/// 会话侧延迟条目：双缓冲直方图 + 进行中操作的起始时间戳。
///
/// 版本 0/1 由监视器迭代计数奇偶切换；条目由所属会话独占，记点一律 `&mut self`。
///（对标 libs/server/Metrics/Latency/LatencyMetricsEntrySession.cs:LatencyMetricsEntrySession）
pub struct LatencyMetricsEntrySession {
  /// 进行中操作的起始 Stopwatch tick（0 = 无进行中操作）。
  pub start_timestamp: u64,
  /// 双版本延迟直方图。
  pub latency: [Histogram<u64>; 2],
}

impl Default for LatencyMetricsEntrySession {
  fn default() -> Self {
    Self::new()
  }
}

impl LatencyMetricsEntrySession {
  /// 直方图下界：1 tick。
  pub const HISTOGRAM_LOWER_BOUND: u64 = 1;
  /// 直方图上界：100 秒（tick 计量）。
  pub const HISTOGRAM_UPPER_BOUND: u64 = seconds(100);

  /// libs/server/Metrics/Latency/LatencyMetricsEntrySession.cs:LatencyMetricsEntrySession（构造）。
  pub fn new() -> Self {
    let hist = Self::new_histogram;
    Self {
      start_timestamp: 0,
      latency: [hist(), hist()],
    }
  }

  /// 以 C# 直方图参数（下界 [`Self::HISTOGRAM_LOWER_BOUND`]、上界
  /// [`Self::HISTOGRAM_UPPER_BOUND`]、2 位有效数字）新建直方图。
  ///
  /// 全仓延迟直方图的界定义与构造收口本单点（会话条目双缓冲、全局单缓冲、
  /// pending 槽三处共用）。
  pub fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(Self::HISTOGRAM_LOWER_BOUND, Self::HISTOGRAM_UPPER_BOUND, 2)
      .expect("直方图边界为编译期常量，构造必成功")
  }

  /// libs/server/Metrics/Latency/LatencyMetricsEntrySession.cs:Return
  ///
  /// 归还双缓冲直方图（池化语义在 Rust 侧为重置）。
  pub fn return_to_pool(&mut self) {
    self.latency[0].reset();
    self.latency[1].reset();
  }

  /// libs/server/Metrics/Latency/LatencyMetricsEntrySession.cs:Start
  ///
  /// 记录起始时间戳（以当前单调时钟为准，由调用方传入 tick）。
  #[inline]
  pub fn start(&mut self, now_ticks: u64) {
    self.start_timestamp = now_ticks;
  }

  /// libs/server/Metrics/Latency/LatencyMetricsEntrySession.cs:RecordValue(ver)
  ///
  /// 以当前时钟结束进行中的操作并记入版本 `ver`；无进行中操作时忽略。
  /// 与双参重载不同：此处 elapsed == 0 时 `IsValidRange` 必假，按 C# 语义
  /// 饱和记入上界（而非跳过）。
  #[inline]
  pub fn record_value(&mut self, ver: usize, now_ticks: u64) {
    if self.start_timestamp == 0 {
      return;
    }
    let elapsed = now_ticks.saturating_sub(self.start_timestamp) as i64;
    // 0 值分叉：elapsed == 0 时 IsValidRange 必假，按 C# 语义饱和记入上界（非跳过）。
    Self::clamp_record(&mut self.latency[ver], elapsed);
    self.start_timestamp = 0;
  }

  /// 对应 RecordValue(ver, elapsed) 耗时记录重载。直接记录一段耗时。
  ///
  /// 0 值分叉：elapsed == 0 直接丢弃（与单参 record_value 的饱和语义不同）。
  #[inline]
  pub fn record_elapsed(&mut self, ver: usize, elapsed: i64) {
    if elapsed == 0 {
      return;
    }
    Self::clamp_record(&mut self.latency[ver], elapsed);
  }

  /// 越界收敛记点单点（clamp-record）：合法区间原值记入，越界值饱和收敛到
  /// 直方图上界（对标 C# IsValidRange 假臂落 HISTOGRAM_UPPER_BOUND）。
  ///
  /// 0 值分叉（饱和记入 / 丢弃）由调用点先行裁决，本单点不感知；
  /// pending 槽（[`super::pending_latency_meter`]）同经此单点收敛。
  #[inline]
  pub fn clamp_record(hist: &mut Histogram<u64>, elapsed: i64) {
    let value = if Self::is_valid_range(elapsed) {
      elapsed as u64
    } else {
      Self::HISTOGRAM_UPPER_BOUND
    };
    // 记录失败仅可能因越界，已收敛上界，故忽略返回值。
    let _ = hist.record(value);
  }

  /// libs/server/Metrics/Latency/LatencyMetricsEntrySession.cs:IsValidRange
  ///
  /// 耗时是否落在直方图可表达区间 [1, 100s) 内。
  #[inline]
  pub fn is_valid_range(value: i64) -> bool {
    value >= Self::HISTOGRAM_LOWER_BOUND as i64 && (value as u64) < Self::HISTOGRAM_UPPER_BOUND
  }
}
