//! INFO 多非法段回显末个回归（工单 wmetric-invalid-item-echo-first-vs-last）
//!
//! 对标 C# InfoCommand.cs:NetworkINFO 解析循环 :43-44「置位覆写不中断」：
//! 多非法段输入回显末个非法段，而非首个。锁测两形——纯非法与合法段穿插。

use wmetric::{GarnetInfoMetrics, InfoCommand, InfoProvider};
use wresp::metrics::MetricsItem;

/// 空数据源：非法段在解析期即回错，provider 不参与渲染
struct NullProvider;

impl InfoProvider for NullProvider {
  fn server_facts(&self) -> wmetric::ServerFacts {
    wmetric::ServerFacts {
      version: "1.0.0".into(),
      run_id: "run".into(),
      redis_protocol_version: "7.0".into(),
      enable_cluster: false,
      enable_aof: false,
      metrics_sampling_frequency: 0,
      latency_monitor: false,
      command_stats_monitor: false,
      startup_stopwatch_ticks: 0,
      log_dir: "/tmp/log".into(),
    }
  }

  fn databases(&self) -> Vec<wmetric::DbSnapshot> {
    Vec::new()
  }

  fn global_metrics(&self) -> Option<wmetric::GlobalMetricsSnapshot> {
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

/// 渲染入口：非法段解析期即回错，输出只含错误帧
fn render(args: &[&[u8]]) -> Vec<u8> {
  let mut info = GarnetInfoMetrics::new();
  let mut out = Vec::new();
  InfoCommand::network_info(args, 0, &NullProvider, &mut info, &mut |_| {}, 2, &mut out);
  out
}

/// INFO SERVER BOGUS1 BOGUS2：回显末个非法段 BOGUS2
#[test]
fn invalid_section_echoes_last_occurrence() {
  let out = render(&[b"BOGUS1", b"BOGUS2"]);
  assert_eq!(out, b"-ERR Invalid section BOGUS2. Try INFO HELP\r\n");

  // 非法项在前、合法段 SERVER 在后：末个非法项仍胜出
  let out = render(&[b"BOGUS1", b"BOGUS2", b"SERVER"]);
  assert_eq!(out, b"-ERR Invalid section BOGUS2. Try INFO HELP\r\n");
}

/// 混合形 SERVER BOGUS1 SERVER BOGUS2：回显末个非法段且不误回数据帧
#[test]
fn mixed_valid_and_invalid_sections_echo_last_invalid() {
  let out = render(&[b"SERVER", b"BOGUS1", b"SERVER", b"BOGUS2"]);
  assert_eq!(out, b"-ERR Invalid section BOGUS2. Try INFO HELP\r\n");

  // 单非法段保持原状：回显唯一非法项
  let out = render(&[b"SERVER", b"BOGUS"]);
  assert_eq!(out, b"-ERR Invalid section BOGUS. Try INFO HELP\r\n");

  // 全合法段不受影响：正常出数据帧（非错误帧）
  let out = render(&[b"SERVER"]);
  assert!(!out.starts_with(b"-ERR"), "合法段不应回错: {out:?}");
  assert!(
    out
      .windows(b"# Server\r\n".len())
      .any(|w| w == b"# Server\r\n"),
    "应含 Server 段头: {out:?}"
  );
}
