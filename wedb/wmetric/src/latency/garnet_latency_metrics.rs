use std::mem::size_of;

use hdrhistogram::Histogram;
use itoa::Buffer;
use wbase::convert::stopwatch::{TICKS_PER_MICROSECOND, seconds};
use wresp::{
  ext::RespVecExt,
  metrics::{MetricsItem, fmt_n2_into},
};

use super::{
  latency_metrics_entry_session::LatencyMetricsEntrySession,
  latency_metrics_type::LatencyMetricsType,
};

/// RespServerSession 汇总的延迟指标（服务器侧，单缓冲直方图）。
///（对标 libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:GarnetLatencyMetrics）
pub struct GarnetLatencyMetrics {
  /// 各延迟类别的直方图，下标即类别判别值。
  pub metrics: Vec<Histogram<u64>>,
}

impl GarnetLatencyMetrics {
  /// 默认统计的全部延迟类别（对齐 C# defaultLatencyTypes = Enum.GetValues）。
  pub const DEFAULT_LATENCY_TYPES: &[LatencyMetricsType] = &LatencyMetricsType::ALL;

  /// libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:GarnetLatencyMetrics（构造）
  /// libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:Init（构造内建直方图表，C# Init 分配语义并入 new）。
  pub fn new(latency_types: &'static [LatencyMetricsType]) -> Self {
    Self {
      metrics: latency_types
        .iter()
        .map(|_| Self::new_histogram())
        .collect(),
    }
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:Return
  ///
  /// 归还全部池化直方图（Rust 侧语义为重置 + 释放）。
  pub fn return_to_pool(&mut self) {
    for hist in &mut self.metrics {
      hist.reset();
    }
    self.metrics.clear();
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:Merge
  ///
  /// 按引用并入会话侧延迟条目组的 `ver` 槽（仅计入非空槽）并就地清零，
  /// 使每扇窗口的样本恰计一次。C# 由监视器跨线程裸读 `lm.metrics` 并自取
  /// `lm.PriorVersion`；rust 会话延迟表为属主线程独占，版本槽由属主线程
  /// 显式带入（版本翻转的退役槽、会话释放的两槽残余）。同界直方图走桶数组
  /// 流加，全程零堆分配，不再有持锁克隆整组直方图的快照面。
  pub fn merge(&mut self, entries: &mut [LatencyMetricsEntrySession], ver: usize) {
    for (dst, src) in self.metrics.iter_mut().zip(entries) {
      let slot = &mut src.latency[ver];
      if slot.is_empty() {
        continue;
      }
      // 记录失败仅可能因越界，两侧同界故不会发生。
      let _ = dst.add(&*slot);
      slot.reset();
    }
  }

  /// 并入单类别直方图（执行域 PENDING_LAT 计量槽的结算口）：按引用流加后
  /// 清零来源，无深拷贝。
  pub fn merge_histogram(&mut self, cmd: LatencyMetricsType, src: &mut Histogram<u64>) {
    if src.is_empty() {
      return;
    }
    let Some(dst) = self.metrics.get_mut(cmd.idx()) else {
      return;
    };
    let _ = dst.add(&*src);
    src.reset();
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:Reset
  ///
  /// 重置指定类别的直方图；指标已释放时提前返回以优雅退出。
  pub fn reset(&mut self, cmd: LatencyMetricsType) {
    let Some(hist) = self.metrics.get_mut(cmd.idx()) else {
      return;
    };
    hist.reset();
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:GetPercentiles
  ///
  /// 指定类别的分位数列表：calls/min/5th/50th/mean/95th/99th/99.9th。
  /// tick 类别以微秒展示（"N2" 千分位两位小数），值类别原样展示。
  fn get_percentiles(&self, idx: usize) -> Option<Vec<MetricsItem>> {
    let hist = self.metrics.get(idx)?;
    if hist.is_empty() {
      return None;
    }

    let is_ticks = LatencyMetricsType::IS_TICKS[idx];
    let mut str_buf = String::with_capacity(16);
    let mut items = Vec::with_capacity(8);

    items.push(MetricsItem::new("calls", hist.len().to_string()));

    const PERCENTILES: [(&str, f64); 7] = [
      ("min", 0.0),
      ("5th", 5.0),
      ("50th", 50.0),
      ("mean", -1.0),
      ("95th", 95.0),
      ("99th", 99.0),
      ("99.9th", 99.9),
    ];

    for (name, p) in PERCENTILES {
      str_buf.clear();
      if p < 0.0 {
        format_mean_value(hist.mean(), is_ticks, &mut str_buf);
      } else {
        format_percentile_value(value_at_percentile_cs(hist, p), is_ticks, &mut str_buf);
      }
      items.push(MetricsItem::new(name, str_buf.clone()));
    }

    Some(items)
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:GetRespHistogram
  ///
  /// 单类别直方图的 RESP 编码；无样本返回 false。
  pub fn get_resp_histogram(
    &self,
    idx: usize,
    event_type: LatencyMetricsType,
    response: &mut Vec<u8>,
  ) -> bool {
    let Some(hist) = self.metrics.get(idx) else {
      return false;
    };
    if hist.is_empty() {
      return false;
    }

    let is_ticks = LatencyMetricsType::IS_TICKS[idx];
    let cmd_type = event_type.cs_name();
    response.write_resp_bulk_string(cmd_type.as_bytes());
    response.write_resp_array_len(6);
    response.write_resp_bulk_string(b"calls");
    response.write_resp_int(hist.len() as i64);
    response.write_resp_bulk_string(b"size");
    // C# GetEstimatedFootprintInBytes 口径：512 头部开销 + 8 × 计数数组全长
    // （CountsArrayLength 同源，HistogramBase.cs:431）。C# Linux Stopwatch.Frequency=1e9
    // 时桶几何随频域放大，属在册 tick 域选择，本行仅对齐同频域常数面。
    response.write_resp_int(
      (hist.distinct_values() as i64) * (size_of::<u64>() as i64) + Self::FOOTPRINT_HEADER_BYTES,
    );
    response.write_resp_bulk_string(if is_ticks {
      b"histogram_usec" as &[u8]
    } else {
      b"histogram_cnt"
    });
    response.write_resp_array_len(14);

    let mut str_buf = String::with_capacity(16);
    const PERCENTILES: [(&str, f64); 7] = [
      ("min", 0.0),
      ("5th", 5.0),
      ("50th", 50.0),
      ("mean", -1.0),
      ("95th", 95.0),
      ("99th", 99.0),
      ("99.9th", 99.9),
    ];

    for (name, p) in PERCENTILES {
      response.write_resp_bulk_string(name.as_bytes());
      str_buf.clear();
      if p < 0.0 {
        format_mean_value(hist.mean(), is_ticks, &mut str_buf);
      } else {
        format_percentile_value(value_at_percentile_cs(hist, p), is_ticks, &mut str_buf);
      }
      response.write_resp_bulk_string(str_buf.as_bytes());
    }
    true
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:GetRespHistograms
  ///
  /// 多类别直方图的 RESP 编码；无任何样本回 `*0\r\n`。
  pub fn get_resp_histograms(&self, events: &[LatencyMetricsType], output: &mut Vec<u8>) {
    let non_empty_count = events
      .iter()
      .filter(|e| self.metrics.get(e.idx()).is_some_and(|h| !h.is_empty()))
      .count();

    if non_empty_count == 0 {
      output.write_resp_array_len(0);
      return;
    }

    output.write_resp_array_len(non_empty_count * 2);
    for &event_type in events {
      let idx = event_type.idx();
      self.get_resp_histogram(idx, event_type, output);
    }
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:GetLatencyMetrics
  ///
  /// 指定类别的分位数指标（对齐 C# 单类别重载；多类别重载的 rust 消费面
  /// 由 RESP LATENCY HISTOGRAM 以循环单类别承接，重载不转写）。
  pub fn get_latency_metrics(&self, latency_metrics_type: LatencyMetricsType) -> Vec<MetricsItem> {
    self
      .get_percentiles(latency_metrics_type.idx())
      .unwrap_or_default()
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:Dump
  ///
  /// 调试输出：有样本时打印微秒分位数（log 面向诊断，不落 stdout）。
  pub fn dump(&self, idx: usize) {
    let Some(hist) = self.metrics.get(idx) else {
      return;
    };
    if !hist.is_empty() {
      let mean = hist.mean() / TICKS_PER_MICROSECOND as f64;
      log::info!(
        "min (us); 5th (us); median (us); avg (us); 95th (us); 99th (us); 99.9th (us); cnt\n{}; {}; {}; {mean:.1}; {}; {}; {}; {}",
        value_at_percentile_cs(hist, 0.0) / TICKS_PER_MICROSECOND,
        value_at_percentile_cs(hist, 5.0) / TICKS_PER_MICROSECOND,
        value_at_percentile_cs(hist, 50.0) / TICKS_PER_MICROSECOND,
        value_at_percentile_cs(hist, 95.0) / TICKS_PER_MICROSECOND,
        value_at_percentile_cs(hist, 99.0) / TICKS_PER_MICROSECOND,
        value_at_percentile_cs(hist, 99.9) / TICKS_PER_MICROSECOND,
        hist.len()
      );
    }
  }

  /// 直方图上界：100 秒（tick 计量，对标 C# TimeStamp.Seconds(100)）
  const HISTOGRAM_UPPER_BOUND: u64 = seconds(100);

  /// 直方图保守脚印估计的头部开销字节数（对标 HistogramBase.cs:431 的 512 头部项）
  const FOOTPRINT_HEADER_BYTES: i64 = 512;

  /// 以 C# 直方图参数（1..100s, 2 位有效数字）新建。
  fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, Self::HISTOGRAM_UPPER_BOUND, 2)
      .expect("直方图边界为编译期常量，构造必成功")
  }
}

/// metrics/HdrHistogram/HistogramBase.cs:GetValueAtPercentile
///
/// C# 分位取秩单点：秩 `((p/100)*TotalCount + 0.5)` 截断就近（半位向上）且钳 >=1
/// （:371-372），命中桶恒回 highest_equivalent 桶上沿等价值（:381-383，含 p=0 的
/// min 行）。库原生 value_at_percentile 为 ceil 取秩、quantile==0 取 lowest 桶
/// 下沿，两点皆与 C# 分叉，故三读出点统一改经此单点。仅遍历有计数桶与 C# 逐桶
/// 累计在零计数桶上等价（零桶不贡献累计）。
#[inline]
fn value_at_percentile_cs(hist: &Histogram<u64>, p: f64) -> u64 {
  // 对齐 C# :370 Math.Min(percentile, 100.0) 截断到 100%。
  let requested = p.min(100.0);
  // 对齐 :371-372：+0.5 截断就近、钳 >=1（正数域 as u64 即向零截断）。
  let rank = (((requested / 100.0) * hist.len() as f64) + 0.5) as u64;
  let rank = rank.max(1);
  let mut running = 0u64;
  for v in hist.iter_recorded() {
    running += v.count_at_value();
    if running >= rank {
      return hist.highest_equivalent(v.value_iterated_to());
    }
  }
  // 非空直方图且 rank ∈ [1, TotalCount] 时不可达；对齐 C# "should not reach
  // here"，以最大记录桶上沿兜底避免 panic（调用方均已保证非空）。
  hist.highest_equivalent(hist.max())
}

#[inline]
fn format_percentile_value(v: u64, is_ticks: bool, out: &mut String) {
  if is_ticks {
    fmt_n2_into(v as f64 / TICKS_PER_MICROSECOND as f64, out);
  } else {
    let mut buf = Buffer::new();
    out.push_str(buf.format(v));
  }
}

#[inline]
fn format_mean_value(v: f64, is_ticks: bool, out: &mut String) {
  if is_ticks {
    fmt_n2_into(v / TICKS_PER_MICROSECOND as f64, out);
  } else {
    use std::fmt::Write as _;
    let _ = write!(out, "{v:.2}");
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_global_latency_metrics_resp_and_reset() {
    let mut metrics = GarnetLatencyMetrics::new(GarnetLatencyMetrics::DEFAULT_LATENCY_TYPES);
    let mut out = Vec::new();
    metrics.get_resp_histograms(&LatencyMetricsType::ALL, &mut out);
    assert_eq!(out, b"*0\r\n");

    // 记录样本
    let _ = metrics.metrics[LatencyMetricsType::NetRsLat.idx()].record(100);
    out.clear();
    metrics.get_resp_histograms(&LatencyMetricsType::ALL, &mut out);
    let str_out = String::from_utf8_lossy(&out);
    assert!(str_out.starts_with("*2\r\n"));
    assert!(str_out.contains("NET_RS_LAT"));
    assert!(str_out.contains("histogram_usec"));
    // size 行 = 512 头部 + 8 × 计数数组全长（C# GetEstimatedFootprintInBytes 同式）
    assert!(str_out.contains(&format!(
      "$4\r\nsize\r\n:{}\r\n",
      metrics.metrics[LatencyMetricsType::NetRsLat.idx()].distinct_values() as i64
        * size_of::<u64>() as i64
        + GarnetLatencyMetrics::FOOTPRINT_HEADER_BYTES
    )));

    // 重置
    metrics.reset(LatencyMetricsType::NetRsLat);
    out.clear();
    metrics.get_resp_histograms(&LatencyMetricsType::ALL, &mut out);
    assert_eq!(out, b"*0\r\n");
  }
}
