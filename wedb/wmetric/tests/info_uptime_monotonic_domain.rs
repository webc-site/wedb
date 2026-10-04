//! INFO server 段 uptime 时钟域回归（对标 libs/server/StoreWrapper.cs:
//! StoreWrapper.startupTimestamp 与 libs/server/Metrics/Info/
//! GarnetInfoMetrics.cs:PopulateServerInfo 的 Stopwatch 单调域口径）
//!
//! 判别机理：uptime 是「过了多久」的区间语义，必须锚在 `wbase::time` 单调
//! 刻度域（`now_stopwatch_ticks`）。本用例把启动事实钉在真实的单调刻度读数
//! 上、睡一个已知的墙钟无关间隔后再取 uptime，断言其值与真实经过区间一致
//! 且受上限约束。旧实现把该读数当墙钟 Unix 秒与 `now_secs()` 作差，得到
//! 十亿年级别的纪元偏移假值，与真实区间完全脱钩——该断言必红，构成确定性的
//! 时钟域判别（不依赖任何时钟注入，无需假 mock）。
//!
//! 自研依据: uptime 单调域（C# 对应 CoarseTimeProvider 时钟面）

use std::{thread::sleep, time::Duration};

use wbase::time::now_stopwatch_ticks;
use wmetric::{DbSnapshot, GarnetInfoMetrics, GlobalMetricsSnapshot, InfoProvider, ServerFacts};
use wresp::metrics::{InfoMetricsType, MetricsItem};

/// uptime 判别数据源：启动事实固定为取时刻的单调刻度读数
struct UptimeProvider {
  startup: u64,
}

impl InfoProvider for UptimeProvider {
  fn server_facts(&self) -> ServerFacts {
    ServerFacts {
      version: "1.0.0".into(),
      run_id: "run123".into(),
      redis_protocol_version: "7.0".into(),
      enable_cluster: false,
      enable_aof: false,
      metrics_sampling_frequency: 0,
      latency_monitor: false,
      command_stats_monitor: false,
      startup_stopwatch_ticks: self.startup,
      log_dir: String::new(),
    }
  }

  fn databases(&self) -> Vec<DbSnapshot> {
    Vec::new()
  }

  fn global_metrics(&self) -> Option<GlobalMetricsSnapshot> {
    None
  }

  fn command_stats(&self) -> Vec<(String, u64, u64, u64)> {
    Vec::new()
  }

  fn keyspace_stats(&self, _db_id: i32) -> (u64, u64) {
    (0, 0)
  }

  fn replication_info(&self) -> Option<Vec<MetricsItem>> {
    None
  }

  fn gossip_stats(&self, _metrics_disabled: bool) -> Vec<MetricsItem> {
    Vec::new()
  }

  fn buffer_pool_stats(&self) -> Vec<(String, String)> {
    Vec::new()
  }

  fn checkpoint_info(&self) -> Option<Vec<MetricsItem>> {
    None
  }

  fn hlog_scan_dump(&self) -> Vec<(String, String)> {
    Vec::new()
  }

  fn safe_aof_address(&self) -> i64 {
    0
  }
}

/// 从 Server 段取指定指标并解析为 i64
fn metric_i64(items: &[MetricsItem], name: &str) -> i64 {
  items
    .iter()
    .find(|i| i.name.as_ref() == name)
    .unwrap_or_else(|| panic!("Server 段缺少指标 {name}"))
    .value
    .parse()
    .expect("指标值应为 i64 字面量")
}

/// uptime 必须落在真实经过区间内：与墙钟纪元读数无耦合
#[test]
fn uptime_anchored_in_monotonic_tick_domain() {
  const SLEEP_MS: u64 = 1_100;
  let start = now_stopwatch_ticks();
  sleep(Duration::from_millis(SLEEP_MS));

  let mut metrics = GarnetInfoMetrics::new();
  let items = metrics
    .get_metric(
      InfoMetricsType::Server,
      0,
      &UptimeProvider { startup: start },
    )
    .expect("Server 段应存在");

  let uptime = metric_i64(&items, "uptime_in_seconds");
  // 下界：真实经过 ≥ 1.1s，单调域 floor 后恒 ≥ 1
  assert!(
    uptime >= 1,
    "uptime 应随单调刻度推进反映真实经过时长，回拨钳零或脱钩即失败，实测 {uptime}"
  );
  // 上界：与真实区间一致（1.1s 间隔容差至 3s）；旧实现按墙钟差得 ~1.7e9 假值，必红
  assert!(
    uptime <= 3,
    "uptime 应等于真实经过区间、与墙钟纪元偏移无耦合，实测 {uptime}（虚增即墙钟域耦合）"
  );
  // days 同源换算：进程内秒级区间必为 0；旧实现 uptime/86400 ≈ 2 万，必红
  assert_eq!(
    metric_i64(&items, "uptime_in_days"),
    0,
    "uptime_in_days 应与 uptime 同源换算"
  );
}
