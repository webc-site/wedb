use std::mem::size_of;

use hdrhistogram::Histogram;
use itoa::Buffer;
use wbase::convert::stopwatch::TICKS_PER_MICROSECOND;
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
        .map(|_| LatencyMetricsEntrySession::new_histogram())
        .collect(),
    }
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

    items.push(MetricsItem::from_u64("calls", hist.len()));

    for (name, p) in PERCENTILES {
      str_buf.clear();
      format_percentile_entry(hist, p, is_ticks, &mut str_buf);
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
    // size 行实口径：512 + 8 × distinct_values（已记录不同值个数）。
    // 与 C# GetEstimatedFootprintInBytes 的口径关系未经盘上亲验，本仓以
    // distinct_values 为真值源；登记锚 deviations.md §194（§166b 仅承载
    // 10MHz 刻度域选择，不作为 size 行数组全长推论依据）。
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

    for (name, p) in PERCENTILES {
      response.write_resp_bulk_string(name.as_bytes());
      str_buf.clear();
      format_percentile_entry(hist, p, is_ticks, &mut str_buf);
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

  /// 直方图保守脚印估计的头部开销字节数（登记锚 deviations.md §194）
  const FOOTPRINT_HEADER_BYTES: i64 = 512;
}

/// 分位展示序列（mean 以负哨兵 p 标注，C# GetPercentiles / GetRespHistogram
/// 两口展开的同一串分位收敛为单点定义）
const PERCENTILES: [(&str, f64); 7] = [
  ("min", 0.0),
  ("5th", 5.0),
  ("50th", 50.0),
  ("mean", -1.0),
  ("95th", 95.0),
  ("99th", 99.0),
  ("99.9th", 99.9),
];

/// 单分位值格式化（`p < 0` 哨兵 = mean 行，ticks 类别走 "N2" 微秒域）
#[inline]
fn format_percentile_entry(hist: &Histogram<u64>, p: f64, is_ticks: bool, out: &mut String) {
  if p < 0.0 {
    format_mean_value(hist.mean(), is_ticks, out);
  } else {
    format_percentile_value(value_at_percentile_cs(hist, p), is_ticks, out);
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
