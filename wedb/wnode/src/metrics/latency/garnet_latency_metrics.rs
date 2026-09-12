use std::mem::size_of;

use hdrhistogram::Histogram;

use super::{
  garnet_latency_metrics_session::GarnetLatencyMetricsSession,
  latency_metrics_entry::time_stamp::TICKS_PER_MICROSECOND,
  latency_metrics_type::LatencyMetricsType,
};
use crate::metrics::{metrics_item::MetricsItem, resp_write_utils::RespWriteUtils};

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

  /// libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:GarnetLatencyMetrics（构造 + Init）。
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

    let ticks = LatencyMetricsType::IS_TICKS[idx];
    let raw = |p: f64| hist.value_at_percentile(p);
    let fmt_value = |v: u64| {
      if ticks {
        fmt_n2(v as f64 / TICKS_PER_MICROSECOND as f64)
      } else {
        v.to_string()
      }
    };
    let fmt_mean = |v: f64| {
      if ticks {
        fmt_n2(v / TICKS_PER_MICROSECOND as f64)
      } else {
        format!("{v:.2}")
      }
    };

    Some(vec![
      MetricsItem::new("calls", hist.len().to_string()),
      MetricsItem::new("min", fmt_value(raw(0.0))),
      MetricsItem::new("5th", fmt_value(raw(5.0))),
      MetricsItem::new("50th", fmt_value(raw(50.0))),
      MetricsItem::new("mean", fmt_mean(hist.mean())),
      MetricsItem::new("95th", fmt_value(raw(95.0))),
      MetricsItem::new("99th", fmt_value(raw(99.0))),
      MetricsItem::new("99.9th", fmt_value(raw(99.9))),
    ])
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:GetRespHistogram
  ///
  /// 单类别直方图的 RESP 编码；无样本返回 false。
  pub fn get_resp_histogram(
    &self,
    idx: usize,
    event_type: LatencyMetricsType,
    response: &mut String,
  ) -> bool {
    let Some(hist) = self.metrics.get(idx) else {
      return false;
    };
    if hist.is_empty() {
      return false;
    }
    let Some(p) = &self.get_percentiles(idx) else {
      return false;
    };

    let cmd_type = event_type.cs_name();
    response.push_str(&RespWriteUtils::bulk_string(cmd_type));
    response.push_str("*6\r\n");
    response.push_str(&RespWriteUtils::bulk_string("calls"));
    response.push_str(&format!(":{}\r\n", p[0].value));
    response.push_str(&RespWriteUtils::bulk_string("size"));
    response.push_str(&format!(
      ":{}\r\n", // C# GetEstimatedFootprintInBytes 的等价估算：桶数 × 每桶 8 字节。
      (hist.distinct_values() as u64) * (size_of::<u64>() as u64)
    ));
    response.push_str(&RespWriteUtils::bulk_string(
      if LatencyMetricsType::IS_TICKS[idx] {
        "histogram_usec"
      } else {
        "histogram_cnt"
      },
    ));
    response.push_str(&format!("*{}\r\n", (p.len() - 1) * 2));
    for item in &p[1..] {
      response.push_str(&RespWriteUtils::bulk_string(&item.name));
      response.push_str(&RespWriteUtils::bulk_string(&item.value));
    }
    true
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:GetRespHistograms
  ///
  /// 多类别直方图的 RESP 编码；无任何样本返回 `*0\r\n`。
  pub fn get_resp_histograms(&self, events: &[LatencyMetricsType]) -> String {
    let mut cmd_count = 0;
    let mut response = String::new();

    for event_type in events {
      let idx = event_type.idx();
      let mut cmd_histogram = String::new();
      if self.get_resp_histogram(idx, *event_type, &mut cmd_histogram) {
        response.push_str(&cmd_histogram);
        cmd_count += 1;
      }
    }

    if cmd_count == 0 {
      "*0\r\n".into()
    } else {
      format!("*{}\r\n", cmd_count * 2) + response.as_str()
    }
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetrics.cs:GetLatencyMetrics
  ///
  /// 指定类别的分位数指标（对齐 C# 单类别重载）。
  pub fn get_latency_metrics(&self, latency_metrics_type: LatencyMetricsType) -> Vec<MetricsItem> {
    self
      .get_percentiles(latency_metrics_type.idx())
      .unwrap_or_default()
  }

  /// 对应 C# GetLatencyMetrics 多类别重载，迭代产出有样本类别的 (类别, 分位数指标) 序列。
  pub fn get_latency_metrics_multi(
    &self,
    latency_metrics_types: &[LatencyMetricsType],
  ) -> Vec<(LatencyMetricsType, Vec<MetricsItem>)> {
    latency_metrics_types
      .iter()
      .filter_map(|&event_type| {
        self
          .get_percentiles(event_type.idx())
          .map(|items| (event_type, items))
      })
      .collect()
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

/// .NET "N2" 格式：千分位分组 + 两位小数（四舍五入）。
pub fn fmt_n2(v: f64) -> String {
  let rounded = format!("{v:.2}");
  let (int_part, frac_part) = rounded.split_once('.').unwrap_or((rounded.as_str(), "00"));
  let (sign, digits) = if let Some(d) = int_part.strip_prefix('-') {
    ("-", d)
  } else {
    ("", int_part)
  };
  let mut grouped = String::with_capacity(digits.len() + digits.len() / 3 + 4);
  grouped.push_str(sign);
  for (i, c) in digits.chars().enumerate() {
    if i > 0 && (digits.len() - i) % 3 == 0 {
      grouped.push(',');
    }
    grouped.push(c);
  }
  grouped.push('.');
  grouped.push_str(frac_part);
  grouped
}
