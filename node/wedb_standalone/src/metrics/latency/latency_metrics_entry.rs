use hdrhistogram::Histogram;

use self::time_stamp::seconds;

/// 服务器侧延迟直方图条目（单缓冲）。
///（对标 libs/server/Metrics/Latency/LatencyMetricsEntry.cs:LatencyMetricsEntry）
pub struct LatencyMetricsEntry {
  /// 延迟直方图（Stopwatch tick 计量，2 位有效数字）。
  pub latency: Histogram<u64>,
}

impl Default for LatencyMetricsEntry {
  fn default() -> Self {
    Self::new()
  }
}

impl LatencyMetricsEntry {
  /// 直方图下界：1 tick。
  pub const HISTOGRAM_LOWER_BOUND: u64 = 1;
  /// 直方图上界：100 秒（tick 计量，对齐 C# TimeStamp.Seconds(100)）。
  pub const HISTOGRAM_UPPER_BOUND: u64 = seconds(100);

  /// libs/server/Metrics/Latency/LatencyMetricsEntry.cs:LatencyMetricsEntry（构造）。
  pub fn new() -> Self {
    Self {
      latency: Histogram::new_with_bounds(
        Self::HISTOGRAM_LOWER_BOUND,
        Self::HISTOGRAM_UPPER_BOUND,
        2,
      )
      .expect("直方图边界为编译期常量，构造必成功"),
    }
  }

  /// libs/server/Metrics/Latency/LatencyMetricsEntry.cs:Return
  ///
  /// 归还池化直方图（Rust 侧为重置，语义等价于可复用）。
  pub fn return_to_pool(&mut self) {
    self.latency.reset();
  }
}

/// Stopwatch tick ↔ 微秒换算辅助（服务端延迟值以 tick 计量）。
pub mod time_stamp {
  /// Stopwatch tick 频率：10_000_000/s（.NET TimeSpan.TicksPerSecond）。
  pub const TICKS_PER_SECOND: u64 = 10_000_000;
  /// tick → 微秒除数（对齐 OutputScalingFactor.TimeStampToMicroseconds）。
  pub const TICKS_PER_MICROSECOND: u64 = TICKS_PER_SECOND / 1_000_000;
  /// tick → 秒除数（对齐 OutputScalingFactor.TimeStampToSeconds）。
  pub const TICKS_PER_SECOND_UNIT: u64 = TICKS_PER_SECOND;

  /// 秒 → tick（对齐 Garnet.common TimeStamp.Seconds）。
  #[inline]
  pub const fn seconds(secs: u64) -> u64 {
    secs * TICKS_PER_SECOND
  }
}
