//! wmetric-latency-percentile-rank-min-equivalent（P2）契约对齐锁：
//! LATENCY 百分位取秩与 min 行等价值双点对齐 C#
//! metrics/HdrHistogram/HistogramBase.cs:GetValueAtPercentile（:368-389）——
//! :371 取秩 `((p/100)*TotalCount + 0.5)` 截断就近、:372 钳 >=1、
//! :381-383 命中桶恒回 HighestEquivalentValue（含 p=0 的 min 行取桶上沿）。
//! 库原生 value_at_percentile 为 ceil 取秩、quantile==0 取 lowest 桶下沿，
//! 本测试锁定两处分叉点改经 value_at_percentile_cs 单点后与 C# 同值。

use wmetric::{GarnetLatencyMetrics, LatencyMetricsType};
use wresp::metrics::MetricsItem;

/// 取 items 中指定指标名的值。
fn item_value(items: &[MetricsItem], name: &str) -> String {
  items
    .iter()
    .find(|i| i.name == name)
    .unwrap_or_else(|| panic!("缺少指标行 {name}"))
    .value
    .clone()
}

/// min 行无条件触发：单样本 1000 tick，C# 钳秩 1 命中首桶后恒回
/// HighestEquivalentValue。digits=2 几何下 1000 的等价区间 [1000,1004)，
/// 上沿 1003 tick → 100.30 µs；库原生 quantile==0 会回桶下沿呈 100.00 µs。
#[test]
fn min_row_reports_highest_equivalent_value() {
  let mut metrics = GarnetLatencyMetrics::new(GarnetLatencyMetrics::DEFAULT_LATENCY_TYPES);
  let idx = LatencyMetricsType::NetRsLat.idx();
  // 公开读出面 get_latency_metrics 承接私有单点 value_at_percentile_cs 的语义。
  metrics.metrics[idx].record(1000).unwrap();

  let items = metrics.get_latency_metrics(LatencyMetricsType::NetRsLat);
  assert_eq!(item_value(&items, "calls"), "1");
  assert_eq!(item_value(&items, "min"), "100.30");
  // p=0 钳秩 1 后 5th 等所有分位同落首桶上沿。
  assert_eq!(item_value(&items, "5th"), "100.30");
}

/// 取秩分叉例证：N=1006、p=5 → x = 0.05*1006 = 50.3(小数位∈(0,0.5))，
/// C# floor(50.3+0.5)=50，库原生 ceil(50.3)=51。样本 1..=1006 各一条，
/// 秩 50 命中值 50（digits=2 下 <2048 前段逐值独立桶，宽 1）→ 5.00 µs；
/// 若仍走库语义则呈 5.10 µs。
#[test]
fn rank_rounds_to_nearest_not_ceil() {
  let mut metrics = GarnetLatencyMetrics::new(GarnetLatencyMetrics::DEFAULT_LATENCY_TYPES);
  let idx = LatencyMetricsType::NetRsLat.idx();
  for v in 1..=1006_u64 {
    metrics.metrics[idx].record(v).unwrap();
  }

  let items = metrics.get_latency_metrics(LatencyMetricsType::NetRsLat);
  assert_eq!(item_value(&items, "calls"), "1006");
  assert_eq!(item_value(&items, "5th"), "5.00");
  // 50th：x = 503 恰整数，双侧同秩 503（+0.5 截断 = 503，ceil = 503），
  // 值 503 落等价区间 [500,504) → 上沿 503 tick → 50.30 µs，回归既有语义。
  assert_eq!(item_value(&items, "50th"), "50.30");
  // min 行：值 1 → 宽 1 → highest(1)=1 tick → 0.10 µs。
  assert_eq!(item_value(&items, "min"), "0.10");
}

/// RESP 读出面（get_resp_histogram）与 MetricsItem 面同源：
/// 同一单样本 1000 tick，histogram_usec 帧内 min 行须呈 100.30。
#[test]
fn resp_histogram_min_row_matches() {
  let mut metrics = GarnetLatencyMetrics::new(GarnetLatencyMetrics::DEFAULT_LATENCY_TYPES);
  let idx = LatencyMetricsType::NetRsLat.idx();
  metrics.metrics[idx].record(1000).unwrap();

  let mut out = Vec::new();
  assert!(metrics.get_resp_histogram(idx, LatencyMetricsType::NetRsLat, &mut out));
  let frame = String::from_utf8_lossy(&out);
  assert!(frame.contains("min"), "帧内应有 min 行: {frame}");
  // min 值 bulk string：$6\r\n100.30\r\n（库语义为 $6\r\n100.00\r\n，可判别）。
  assert!(
    frame.contains("$6\r\n100.30\r\n"),
    "min 行须为桶上沿 100.30: {frame}"
  );
}
