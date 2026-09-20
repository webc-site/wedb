use std::mem::size_of;

use hdrhistogram::Histogram;
use itoa::Buffer;
use wbase::convert::stopwatch::TICKS_PER_MICROSECOND;
use wresp::{
  ext::RespVecExt,
  metrics::{MetricsItem, fmt_n2_into},
};

use super::{
  garnet_latency_metrics_session::GarnetLatencyMetricsSession,
  latency_metrics_type::LatencyMetricsType,
};

/// RespServerSession 汇总的延迟指标（服务器侧，单缓冲直方图）。
///（对标 libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:GarnetLatencyMetrics）
#[derive(Clone)]
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
  /// 并入会话侧延迟指标的"上一版本"缓冲（仅计入 TotalCount > 0 的类别）。
  /// 会话指标为空（服务器停机中仍有会话释放）时提前返回以优雅退出。
  pub fn merge(&mut self, lm: &GarnetLatencyMetricsSession) {
    let Some(session_metrics) = lm.metrics_snapshot() else {
      return;
    };
    let ver = lm.prior_version();
    self.merge_session_snapshot(&session_metrics, ver);
  }

  /// 并入一份会话双缓冲快照的指定版本直方图（Merge 的快照形态，
  /// 供监视器在会话仍活跃时复用）。
  pub fn merge_session_snapshot(
    &mut self,
    session_metrics: &[super::latency_metrics_entry_session::LatencyMetricsEntrySession],
    ver: usize,
  ) {
    for (dst, src) in self.metrics.iter_mut().zip(session_metrics.iter()) {
      if !src.latency[ver].is_empty() {
        let _ = dst.add(&src.latency[ver]);
      }
    }
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
        format_percentile_value(hist.value_at_percentile(p), is_ticks, &mut str_buf);
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
    // C# GetEstimatedFootprintInBytes 的等价估算：桶数 × 每桶 8 字节。
    response.write_resp_int((hist.distinct_values() as i64) * (size_of::<u64>() as i64));
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
        format_percentile_value(hist.value_at_percentile(p), is_ticks, &mut str_buf);
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
        hist.value_at_percentile(0.0) / TICKS_PER_MICROSECOND,
        hist.value_at_percentile(5.0) / TICKS_PER_MICROSECOND,
        hist.value_at_percentile(50.0) / TICKS_PER_MICROSECOND,
        hist.value_at_percentile(95.0) / TICKS_PER_MICROSECOND,
        hist.value_at_percentile(99.0) / TICKS_PER_MICROSECOND,
        hist.value_at_percentile(99.9) / TICKS_PER_MICROSECOND,
        hist.len()
      );
    }
  }

  /// 以 C# 直方图参数（1..100s, 2 位有效数字）新建。
  fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(
      1,
      super::latency_metrics_entry::LatencyMetricsEntry::HISTOGRAM_UPPER_BOUND,
      2,
    )
    .expect("直方图边界为编译期常量，构造必成功")
  }
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

    // 重置
    metrics.reset(LatencyMetricsType::NetRsLat);
    out.clear();
    metrics.get_resp_histograms(&LatencyMetricsType::ALL, &mut out);
    assert_eq!(out, b"*0\r\n");
  }
}
