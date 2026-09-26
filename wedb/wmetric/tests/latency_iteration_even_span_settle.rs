//! 延迟直方图绝对迭代判定——偶数跨周期结算回归测试
//!（对标 libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs 的
//! `Version = monitor_iterations % 2` 与 libs/server/Metrics/
//! GarnetServerMonitor.cs:MainMonitorTaskAsync 按迭代周期推进的归并边界；
//! rust 侧翻转检测以绝对迭代计数为准，修复奇偶折叠的 ABA 回绕盲区）
//!
//! 修复前必红的可证伪点（旧实现只存折叠奇偶 0/1，偶数步长跨周期时
//! `now == version` 判不翻转）：
//! - 会话步长 2：旧实现退役槽不结算（全局 0 条 vs 新 1 条），且新旧样本
//!   混写同槽（旧当前槽 2 条 vs 新 1 条）；
//! - 会话步长 4：旧实现 slot1 的在窗样本滞留并与新样本混写（旧全局 1 条
//!   vs 新 2 条、旧 slot1 2 条 vs 新 1 条）；
//! - PendingLatencyMeter 步长 2：旧实现 `rolled()` 恒假，pending 样本永久
//!   滞留本地（旧全局 0 条、本地 2 条 vs 新全局 1 条、本地 1 条）。
//!
//! 自研依据: 延迟迭代区间收敛（C# 对应 LatencyMetrics）

use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use parking_lot::Mutex;
use wmetric::{
  GarnetLatencyMetrics, GarnetLatencyMetricsSession, LatencyMetricsType, PendingLatencyMeter,
};

/// 时钟 + 全局延迟表（会话与计量槽共用的监视器同源组件，直装不起线程）。
struct Env {
  iterations: Arc<AtomicU64>,
  global: Arc<Mutex<GarnetLatencyMetrics>>,
}

fn env(start: u64) -> Env {
  Env {
    iterations: Arc::new(AtomicU64::new(start)),
    global: Arc::new(Mutex::new(GarnetLatencyMetrics::new(
      GarnetLatencyMetrics::DEFAULT_LATENCY_TYPES,
    ))),
  }
}

fn session(env: &Env) -> GarnetLatencyMetricsSession {
  GarnetLatencyMetricsSession::new(
    Arc::clone(&env.iterations),
    Some(Arc::clone(&env.global)),
    GarnetLatencyMetricsSession::DEFAULT_LATENCY_TYPES,
  )
}

/// 全局延迟表指定类别样本数。
fn global_calls(env: &Env, cmd: LatencyMetricsType) -> u64 {
  env.global.lock().metrics[cmd.idx()].len()
}

/// 会话指定类别指定版本槽样本数。
fn slot_calls(session: &GarnetLatencyMetricsSession, cmd: LatencyMetricsType, ver: usize) -> u64 {
  session.metrics[cmd.idx()].latency[ver].len()
}

/// 用例一：会话步长 2——奇偶回绕（ABA）不得掩盖跨周期，旧槽恰结算一次。
#[test]
fn test_session_even_step_two_span_settles_retired_slot() {
  let env = env(0);
  let mut s = session(&env);

  // 窗口 0：一条样本落 slot 0，不入全局
  s.record_value(LatencyMetricsType::NetRsLat, 10);
  assert_eq!(slot_calls(&s, LatencyMetricsType::NetRsLat, 0), 1);
  assert_eq!(global_calls(&env, LatencyMetricsType::NetRsLat), 0);

  // 时钟推进 2：迭代 2 与迭代 0 奇偶相同，旧折叠判定在此漏检
  env.iterations.store(2, Ordering::Relaxed);
  s.record_value(LatencyMetricsType::NetRsLat, 20);

  // 两样本跨周期各归其位：窗口 0 的 1 条并入全局，窗口 2 的 1 条留在当前槽
  assert_eq!(
    global_calls(&env, LatencyMetricsType::NetRsLat),
    1,
    "偶数跨周期退役槽未结算（奇偶折叠盲区）"
  );
  assert_eq!(
    slot_calls(&s, LatencyMetricsType::NetRsLat, 0),
    1,
    "跨周期样本混写同槽：当前槽应只剩本窗 1 条"
  );
  assert_eq!(slot_calls(&s, LatencyMetricsType::NetRsLat, 1), 0);
  assert_eq!(s.version(), 0);

  // 释放收口：本窗残余恰计一次，不重复
  s.return_to_pool();
  assert_eq!(global_calls(&env, LatencyMetricsType::NetRsLat), 2);
}

/// 用例二：会话步长 4 跨多周期——两槽均属过期窗口，一律结算，样本不丢不重。
#[test]
fn test_session_even_step_four_span_settles_both_slots() {
  let env = env(0);
  let mut s = session(&env);

  // 窗口 0：slot 0 一条
  s.record_value(LatencyMetricsType::NetRsLat, 10);
  // 窗口 1（步进 1，奇偶翻转的正常基线路径）：slot 0 结算入全局，本条落 slot 1
  env.iterations.store(1, Ordering::Relaxed);
  s.record_value(LatencyMetricsType::NetRsLat, 20);
  assert_eq!(global_calls(&env, LatencyMetricsType::NetRsLat), 1);
  assert_eq!(slot_calls(&s, LatencyMetricsType::NetRsLat, 1), 1);

  // 窗口 5：步进 4，奇偶仍为 1——旧折叠判定漏检，slot 1 旧样本将与新样本混写
  env.iterations.store(5, Ordering::Relaxed);
  s.record_value(LatencyMetricsType::NetRsLat, 30);

  assert_eq!(
    global_calls(&env, LatencyMetricsType::NetRsLat),
    2,
    "步进 4 跨周期时过期槽未结算（奇偶折叠盲区）"
  );
  assert_eq!(
    slot_calls(&s, LatencyMetricsType::NetRsLat, 1),
    1,
    "步进 4 后当前槽混入过期样本"
  );
  assert_eq!(slot_calls(&s, LatencyMetricsType::NetRsLat, 0), 0);
  assert_eq!(s.version(), 1);

  s.return_to_pool();
  assert_eq!(global_calls(&env, LatencyMetricsType::NetRsLat), 3);
}

/// 用例三：PendingLatencyMeter 步长 2——rolled 以绝对迭代 swap 判定，
/// 偶数跨周期样本照常并入全局 PENDING_LAT，同周期内不重复结算。
#[test]
fn test_pending_meter_even_step_two_span_settles_globally() {
  let env = env(0);
  let meter = PendingLatencyMeter::new(Arc::clone(&env.iterations), Arc::clone(&env.global));

  // 窗口 0：一条本地滞留（未跨周期不结算）
  meter.record(10);
  assert_eq!(meter.pending_samples(), 1);
  assert_eq!(global_calls(&env, LatencyMetricsType::PendingLat), 0);

  // 窗口 2：步进 2，奇偶相同——旧 `swap(奇偶) != 奇偶` 恒假，样本永不清算
  env.iterations.store(2, Ordering::Relaxed);
  meter.record(20);
  assert_eq!(
    global_calls(&env, LatencyMetricsType::PendingLat),
    1,
    "偶数跨周期 pending 样本未并入全局（奇偶折叠盲区）"
  );
  assert_eq!(meter.pending_samples(), 1, "结算后本地窗应只剩本窗 1 条");

  // 同周期再记：swap 见基准已新，不得重复结算
  meter.record(30);
  assert_eq!(global_calls(&env, LatencyMetricsType::PendingLat), 1);
  assert_eq!(meter.pending_samples(), 2);

  // 步进 2 至窗口 4：两样本一并结算，全局共 3 条
  env.iterations.store(4, Ordering::Relaxed);
  meter.record(40);
  assert_eq!(global_calls(&env, LatencyMetricsType::PendingLat), 3);
  assert_eq!(meter.pending_samples(), 1);

  // 释放收口（Drop 前显式 flush）：残余恰计一次
  meter.flush();
  assert_eq!(global_calls(&env, LatencyMetricsType::PendingLat), 4);
  assert_eq!(meter.pending_samples(), 0);
  drop(meter);
  assert_eq!(global_calls(&env, LatencyMetricsType::PendingLat), 4);
}
