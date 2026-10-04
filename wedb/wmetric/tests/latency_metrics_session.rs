use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use hdrhistogram::Histogram;
use parking_lot::Mutex;
use wmetric::{GarnetLatencyMetrics, GarnetLatencyMetricsSession, LatencyMetricsType};

/// 监视器时钟 + 全局出口（会话归并目标）。
struct Env {
  iterations: Arc<AtomicU64>,
  global: Arc<Mutex<GarnetLatencyMetrics>>,
}

fn env() -> Env {
  Env {
    iterations: Arc::new(AtomicU64::new(0)),
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

/// 全局出口指定类别的样本数。
fn global_calls(env: &Env, cmd: LatencyMetricsType) -> u64 {
  env
    .global
    .lock()
    .metrics
    .get(cmd.idx())
    .map_or(0, Histogram::len)
}

#[test]
fn test_session_lifecycle_and_switching() {
  let env = env();
  let mut session = session(&env);

  assert_eq!(session.version(), 0);

  // 1. start + stop
  session.start(LatencyMetricsType::NetRsLat, 100);
  assert_eq!(session.get(LatencyMetricsType::NetRsLat), 100);
  session.stop(LatencyMetricsType::NetRsLat, 200);
  assert_eq!(session.get(LatencyMetricsType::NetRsLat), 0);
  assert_eq!(
    session.metrics[LatencyMetricsType::NetRsLat.idx()].latency[0].len(),
    1
  );

  // 2. stop_and_switch（同类别输入：C# 即读即清，清戳不记样本）
  session.start(LatencyMetricsType::NetRsLat, 300);
  session.stop_and_switch(
    LatencyMetricsType::NetRsLat,
    LatencyMetricsType::NetRsLat,
    500,
  );
  assert_eq!(session.get(LatencyMetricsType::NetRsLat), 0);
  assert_eq!(
    session.metrics[LatencyMetricsType::NetRsLat.idx()].latency[0].len(),
    1
  );

  // 3. stop_and_switch（跨命令切换：戳转移后记入新类别）
  session.start(LatencyMetricsType::NetRsLat, 600);
  session.stop_and_switch(
    LatencyMetricsType::NetRsLat,
    LatencyMetricsType::NetRsLatAdmin,
    800,
  );
  assert_eq!(
    session.metrics[LatencyMetricsType::NetRsLatAdmin.idx()].latency[0].len(),
    1
  );
  assert_eq!(session.get(LatencyMetricsType::NetRsLat), 0);

  // 4. record_value
  session.record_value(LatencyMetricsType::NetRsBytes, 1024);
  assert_eq!(
    session.metrics[LatencyMetricsType::NetRsBytes.idx()].latency[0].len(),
    1
  );

  // 5. 版本翻转：退役槽并入全局后清零（slot0 累计 NET_RS_LAT 样本仅步骤 1
  // 一条：步骤 2 同类别清戳不记账，步骤 3 戳已转移）
  env.iterations.fetch_add(1, Ordering::Relaxed);
  session.start(LatencyMetricsType::NetRsLat, 900);
  assert_eq!(session.version(), 1);
  assert_eq!(global_calls(&env, LatencyMetricsType::NetRsLat), 1);
  assert_eq!(
    session.metrics[LatencyMetricsType::NetRsLat.idx()].latency[0].len(),
    0
  );

  // 6. 会话释放：残余槽同样计入全局且不再重复
  session.stop(LatencyMetricsType::NetRsLat, 1_300);
  session.return_to_pool();
  assert_eq!(global_calls(&env, LatencyMetricsType::NetRsLat), 2);
  assert_eq!(
    session.metrics[LatencyMetricsType::NetRsLat.idx()].latency[1].len(),
    0
  );
}
