//! GarnetInfoMetrics 多段聚合与段头映射测试（对标 C# GetInfoMetrics / GetRespInfo）

use wmetric::{DbSnapshot, GarnetInfoMetrics, GlobalMetricsSnapshot, InfoProvider, ServerFacts};
use wresp::metrics::{InfoMetricsType, MetricsItem};

struct MockInfoProvider;

impl InfoProvider for MockInfoProvider {
  fn server_facts(&self) -> ServerFacts {
    ServerFacts {
      version: "1.0.0".into(),
      run_id: "test-run-id".into(),
      redis_protocol_version: "7.0".into(),
      enable_cluster: false,
      enable_aof: false,
      metrics_sampling_frequency: 1,
      latency_monitor: true,
      command_stats_monitor: false,
      startup_stopwatch_ticks: 0,
      log_dir: "/tmp".into(),
    }
  }

  fn databases(&self) -> Vec<DbSnapshot> {
    vec![DbSnapshot {
      id: 0,
      current_version: 1,
      index_bucket_count: 1024,
      ..Default::default()
    }]
  }

  fn global_metrics(&self) -> Option<GlobalMetricsSnapshot> {
    Some(GlobalMetricsSnapshot {
      total_connections_active: 5,
      total_connections_received: 10,
      total_connections_disposed: 5,
      rejected_connections: 3,
      ..Default::default()
    })
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

#[test]
fn test_get_section_header_parts() {
  assert_eq!(
    GarnetInfoMetrics::get_section_header_parts(InfoMetricsType::Server),
    ("Server", false)
  );
  assert_eq!(
    GarnetInfoMetrics::get_section_header_parts(InfoMetricsType::Store),
    ("Store", true)
  );
  assert_eq!(
    GarnetInfoMetrics::get_section_header_parts(InfoMetricsType::Stats),
    ("Stats", false)
  );
  assert_eq!(
    GarnetInfoMetrics::get_section_header_parts(InfoMetricsType::Persistence),
    ("Persistence", true)
  );
}

#[test]
fn test_get_info_metrics_multi() {
  let provider = MockInfoProvider;
  let mut info = GarnetInfoMetrics::new();
  let sections = [InfoMetricsType::Server, InfoMetricsType::Stats];
  let res = info.get_info_metrics(&sections, 0, &provider);

  assert_eq!(res.len(), 2);
  assert_eq!(res[0].0, InfoMetricsType::Server);
  assert_eq!(res[1].0, InfoMetricsType::Stats);

  let server_items = &res[0].1;
  assert!(
    server_items
      .iter()
      .any(|item| item.name == "garnet_version" && item.value == "1.0.0")
  );

  let stats_items = &res[1].1;
  assert!(
    stats_items
      .iter()
      .any(|item| item.name == "total_connections_active" && item.value == "5")
  );
  // 拒连行渲染（容量门拒连计数经快照透出 INFO STATS）
  assert!(
    stats_items
      .iter()
      .any(|item| item.name == "rejected_connections" && item.value == "3")
  );
  // 行位锁定 C# GarnetInfoMetrics.cs:194：rejected_connections 紧随
  // total_connections_disposed 之后
  let disposed_idx = stats_items
    .iter()
    .position(|item| item.name == "total_connections_disposed")
    .expect("disposed 行在位");
  assert_eq!(
    stats_items[disposed_idx + 1].name,
    "rejected_connections",
    "rejected_connections 行位须紧随 total_connections_disposed"
  );
}
