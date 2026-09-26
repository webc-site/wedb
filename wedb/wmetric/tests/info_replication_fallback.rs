//! INFO REPLICATION 兜底分支测试（工单 wmetric-hexid-dual-mechanism-cluster-runid-unwired）
//!
//! 验证点 (a)：无集群提供方时，master_replid 与 master_replid2 恒为全零 40 hex 常量，
//! 且同一实例连续多次 INFO 调用输出严格一致，杜绝假拓扑告警与 diff 抖动。
//! 对标 C# GarnetInfoMetrics.cs:167-168 与 Generator.cs:DefaultHexId。

use wmetric::{
  DbSnapshot, GarnetInfoMetrics, GlobalMetricsSnapshot, InfoProvider, ServerFacts,
  info::garnet_info_metrics::DEFAULT_HEX_ID,
};
use wresp::metrics::{InfoMetricsType, MetricsItem};

struct StandaloneProvider;

impl InfoProvider for StandaloneProvider {
  fn server_facts(&self) -> ServerFacts {
    ServerFacts {
      version: "1.0.0".into(),
      run_id: "test_run_id".into(),
      redis_protocol_version: "7.0".into(),
      enable_cluster: false,
      enable_aof: false,
      metrics_sampling_frequency: 0,
      latency_monitor: false,
      command_stats_monitor: false,
      startup_stopwatch_ticks: 0,
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

fn extract_field<'a>(text: &'a str, field: &str) -> Option<&'a str> {
  let prefix = format!("{field}:");
  text.lines().find_map(|line| line.strip_prefix(&prefix))
}

#[test]
fn test_replication_fallback_replids_equal_and_constant_across_calls() {
  let provider = StandaloneProvider;

  // 连续两次取 INFO replication 段
  let mut metrics1 = GarnetInfoMetrics::new();
  let text1 = metrics1.get_resp_info(&[InfoMetricsType::Replication], 0, &provider);

  let mut metrics2 = GarnetInfoMetrics::new();
  let text2 = metrics2.get_resp_info(&[InfoMetricsType::Replication], 0, &provider);

  let replid_call1 = extract_field(&text1, "master_replid").expect("master_replid 必须存在");
  let replid2_call1 = extract_field(&text1, "master_replid2").expect("master_replid2 必须存在");
  let replid_call2 = extract_field(&text2, "master_replid").expect("master_replid 必须存在");
  let replid2_call2 = extract_field(&text2, "master_replid2").expect("master_replid2 必须存在");

  // 同一次输出内两值相等且对齐 DefaultHexId
  assert_eq!(replid_call1, DEFAULT_HEX_ID);
  assert_eq!(replid2_call1, DEFAULT_HEX_ID);
  assert_eq!(replid_call1, "0000000000000000000000000000000000000000");

  // 跨调用恒定不变（危害一回归），输出体完全一致
  assert_eq!(replid_call1, replid_call2);
  assert_eq!(replid2_call1, replid2_call2);
  assert_eq!(text1, text2, "连续两次 INFO 输出体必须完全相同");
}
