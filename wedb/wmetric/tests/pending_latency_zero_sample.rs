//! PENDING_LAT 零耗时样本丢弃钉死测试（doc/zh/deviations.md 条款 25）
//!
//! C# 真实 pending 调用链为单参 `RecordValue(int ver)`
//!（Storage/Session/Metrics.cs:StopPendingMetrics →
//! GarnetLatencyMetricsSession.cs:Stop → LatencyMetricsEntrySession.cs:40-50），
//! elapsed==0 因 LOWER_BOUND=1 非法区间落 HISTOGRAM_UPPER_BOUND（100 秒上界
//! 巨值）系原型缺陷；rust `PendingLatencyMeter::record` 丢弃不计为登记裁决。
//! 本文件钉死零值不计行为，防对标复核反向对齐 C# 缺陷。
//!
//! 自研回归锁: 零样本挂起延迟

use std::sync::Arc;

use wmetric::{GarnetServerMonitor, LatencyMetricsType, PendingLatencyMeter};

/// 以监视器同源时钟 + 全局出口建 pending 计量槽（生产装配同构）。
fn meter(monitor: &GarnetServerMonitor) -> PendingLatencyMeter {
  PendingLatencyMeter::new(
    Arc::clone(&monitor.monitor_iterations),
    monitor
      .global_latency_metrics()
      .expect("延迟监视开启时全局延迟出口在位"),
  )
}

/// 全局 PENDING_LAT 直方图 calls 计数（空表视为 0）。
fn pending_calls(monitor: &GarnetServerMonitor) -> u64 {
  let global = monitor
    .global_latency_metrics()
    .expect("延迟监视开启时全局延迟出口在位");
  global
    .lock()
    .get_latency_metrics(LatencyMetricsType::PendingLat)
    .iter()
    .find(|item| item.name == "calls")
    .and_then(|item| item.value.parse().ok())
    .unwrap_or(0)
}

#[test]
fn record_zero_discards_sample_and_keeps_counts() {
  let monitor = GarnetServerMonitor::new(1, true, true, false);
  let m = meter(&monitor);

  // 亚微秒异步闭环：elapsed 差值饱和 0，丢弃不入本槽（C# 同形样本记 100s
  // 上界，rust 刻意分叉——裁决见 doc/zh/deviations.md「25. 零耗时 pending 样本」）
  m.record(0);
  m.record(0);
  m.record(0);
  assert_eq!(m.pending_samples(), 0, "零耗时样本应丢弃，不入本槽");

  // 合法样本照常入槽（丢弃臂不扩大化：非零值不受零值裁决影响）
  m.record(1000);
  assert_eq!(m.pending_samples(), 1);

  // flush 并入全局：仅合法样本计数，零值样本恒不出现
  m.flush();
  assert_eq!(
    pending_calls(&monitor),
    1,
    "全局表仅见合法样本，零耗时样本不计"
  );
}

#[test]
fn pure_zero_traffic_leaves_no_global_samples() {
  // 纯零值流量 + Drop 收口（执行域随会话析构触发 flush）：全局 PENDING_LAT
  // 无样本，LATENCY HISTOGRAM 不出幽灵上界桶（C# 同流量会记 100s 巨值样本）
  let monitor = GarnetServerMonitor::new(1, true, true, false);
  {
    let m = meter(&monitor);
    m.record(0);
    m.record(0);
  } // Drop → flush
  assert_eq!(pending_calls(&monitor), 0, "纯零值流量全局零样本");
}
