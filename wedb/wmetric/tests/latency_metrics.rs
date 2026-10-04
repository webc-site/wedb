use std::mem::size_of;

use wmetric::{GarnetLatencyMetrics, LatencyMetricsType};

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
  // size 行 = 512 头部 + 8 × distinct_values（实口径真值源，登记锚 deviations.md §194）
  assert!(str_out.contains(&format!(
    "$4\r\nsize\r\n:{}\r\n",
    metrics.metrics[LatencyMetricsType::NetRsLat.idx()].distinct_values() as i64
      * size_of::<u64>() as i64
      + 512
  )));

  // 重置
  metrics.reset(LatencyMetricsType::NetRsLat);
  out.clear();
  metrics.get_resp_histograms(&LatencyMetricsType::ALL, &mut out);
  assert_eq!(out, b"*0\r\n");
}

#[test]
fn test_latency_metrics_size_row_distinct_values_differential() {
  let mut metrics = GarnetLatencyMetrics::new(GarnetLatencyMetrics::DEFAULT_LATENCY_TYPES);
  let mut out = Vec::new();
  let idx = LatencyMetricsType::NetRsLat.idx();

  // 1. 同一数值重复灌入多次（10 次 100）
  for _ in 0..10 {
    let _ = metrics.metrics[idx].record(100);
  }
  let distinct_1 = metrics.metrics[idx].distinct_values() as i64;

  metrics.get_resp_histogram(idx, LatencyMetricsType::NetRsLat, &mut out);
  let str_out_1 = String::from_utf8_lossy(&out);
  let marker = "$4\r\nsize\r\n:";
  let start_1 = str_out_1.find(marker).expect("must contain size field") + marker.len();
  let end_1 = str_out_1[start_1..]
    .find("\r\n")
    .expect("must end with crlf");
  let size_1: i64 = str_out_1[start_1..start_1 + end_1]
    .parse()
    .expect("valid int");
  assert_eq!(size_1, 512 + 8 * distinct_1);

  // 2. 灌入分散的另外 5 个不同数值
  let new_values = [200, 300, 400, 500, 600];
  for &v in &new_values {
    let _ = metrics.metrics[idx].record(v);
  }
  let distinct_n = metrics.metrics[idx].distinct_values() as i64;

  out.clear();
  metrics.get_resp_histogram(idx, LatencyMetricsType::NetRsLat, &mut out);
  let str_out_n = String::from_utf8_lossy(&out);
  let start_n = str_out_n.find(marker).expect("must contain size field") + marker.len();
  let end_n = str_out_n[start_n..]
    .find("\r\n")
    .expect("must end with crlf");
  let size_n: i64 = str_out_n[start_n..start_n + end_n]
    .parse()
    .expect("valid int");
  assert_eq!(size_n, 512 + 8 * distinct_n);

  // 3. 差分断言：锁定 size 随 distinct_values 单调对应（Δsize = 8 × Δdistinct_values），
  // 锁死实现口径（distinct_values 为真值源）防后续误回改。
  assert_eq!(size_n - size_1, 8 * (distinct_n - distinct_1));
}
