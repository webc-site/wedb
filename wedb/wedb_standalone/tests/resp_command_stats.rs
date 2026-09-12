use wnode::{
  metrics::{
    command_stats::CommandStats,
    info::{
      garnet_info_metrics::{
        DbSnapshot, GarnetInfoMetrics, GlobalMetricsSnapshot, InfoProvider, ServerFacts,
      },
      info_command::InfoCommand,
    },
    metrics_item::MetricsItem,
  },
  types::RespCommand,
};

struct TestInfoProvider {
  command_stats_monitor: bool,
  stats: Vec<(String, u64, u64)>,
}

impl InfoProvider for TestInfoProvider {
  fn server_facts(&self) -> ServerFacts {
    ServerFacts {
      version: "1.0.0".into(),
      run_id: "run123".into(),
      redis_protocol_version: "7.0".into(),
      enable_cluster: false,
      enable_aof: false,
      metrics_sampling_frequency: 10,
      latency_monitor: false,
      command_stats_monitor: self.command_stats_monitor,
      startup_timestamp_unix_secs: 0,
      log_dir: "/tmp/log".into(),
    }
  }

  fn databases(&self) -> Vec<DbSnapshot> {
    vec![DbSnapshot {
      id: 0,
      current_version: 7,
      ..DbSnapshot::default()
    }]
  }

  fn max_database_id(&self) -> i32 {
    0
  }

  fn global_metrics(&self) -> Option<GlobalMetricsSnapshot> {
    None
  }

  fn command_stats(&self) -> Vec<(String, u64, u64)> {
    self.stats.clone()
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

fn execute_info(provider: &TestInfoProvider, section: Option<&[u8]>) -> String {
  let mut out = String::new();
  let mut info = GarnetInfoMetrics::new();
  let mut reset_flag = |_| {};
  let args: Vec<&[u8]> = section.into_iter().collect();
  InfoCommand::network_info(&args, 0, provider, &mut info, &mut reset_flag, &mut out);
  out
}

fn parse_command_stat_field(line: &str, field_name: &str) -> u64 {
  let search_key = format!("{field_name}=");
  let start_idx = line.find(&search_key).expect("Field not found in line") + search_key.len();
  let end_idx = line[start_idx..]
    .find(',')
    .map(|i| start_idx + i)
    .unwrap_or(line.len());
  let value_str = &line[start_idx..end_idx];
  if value_str.contains('.') {
    value_str.parse::<f64>().unwrap() as u64
  } else {
    value_str.parse::<u64>().unwrap()
  }
}

fn parse_command_stat_field_string<'a>(line: &'a str, field_name: &str) -> &'a str {
  let search_key = format!("{field_name}=");
  let start_idx = line.find(&search_key).expect("Field not found in line") + search_key.len();
  let end_idx = line[start_idx..]
    .find(',')
    .map(|i| start_idx + i)
    .unwrap_or(line.len());
  &line[start_idx..end_idx]
}

/// test/standalone/Garnet.test/RespCommandStatsTests.cs:CommandStatsDisabledByDefaultTest
#[test]
fn command_stats_disabled_by_default_test() {
  let provider = TestInfoProvider {
    command_stats_monitor: false,
    stats: Vec::new(),
  };
  let info = execute_info(&provider, Some(b"COMMANDSTATS"));
  assert!(
    info.contains("Commandstats"),
    "Expected Commandstats section header"
  );
  assert!(
    !info.contains("cmdstat_set"),
    "Expected no cmdstat entries when disabled"
  );
}

/// test/standalone/Garnet.test/RespCommandStatsTests.cs:CommandStatsCallsTrackingTest
#[test]
fn command_stats_calls_tracking_test() {
  let set_count = 10u64;
  let get_count = 5u64;
  let provider = TestInfoProvider {
    command_stats_monitor: true,
    stats: vec![("set".into(), set_count, 0), ("get".into(), get_count, 0)],
  };
  let info = execute_info(&provider, Some(b"COMMANDSTATS"));
  let lines: Vec<&str> = info.split("\r\n").collect();

  let set_line = lines
    .iter()
    .find(|l| l.starts_with("cmdstat_set:"))
    .expect("Expected cmdstat_set entry");
  assert_eq!(parse_command_stat_field(set_line, "calls"), set_count);

  let get_line = lines
    .iter()
    .find(|l| l.starts_with("cmdstat_get:"))
    .expect("Expected cmdstat_get entry");
  assert_eq!(parse_command_stat_field(get_line, "calls"), get_count);
}

/// test/standalone/Garnet.test/RespCommandStatsTests.cs:CommandStatsFailedCallsTest
#[test]
fn command_stats_failed_calls_test() {
  let mut stats = CommandStats::new();
  stats.increment_failed(RespCommand::Setrange);
  let entry = stats.get_entry(RespCommand::Setrange);
  assert!(entry.failed_calls >= 1);
}

/// test/standalone/Garnet.test/RespCommandStatsTests.cs:CommandStatsUsecFieldsZeroTest
#[test]
fn command_stats_usec_fields_zero_test() {
  let provider = TestInfoProvider {
    command_stats_monitor: true,
    stats: vec![("set".into(), 100, 0)],
  };
  let info = execute_info(&provider, Some(b"COMMANDSTATS"));
  let lines: Vec<&str> = info.split("\r\n").collect();
  let set_line = lines
    .iter()
    .find(|l| l.starts_with("cmdstat_set:"))
    .expect("Expected cmdstat_set entry");

  assert_eq!(parse_command_stat_field(set_line, "usec"), 0);
  assert_eq!(
    parse_command_stat_field_string(set_line, "usec_per_call"),
    "0.00"
  );
}

/// test/standalone/Garnet.test/RespCommandStatsTests.cs:CommandStatsMultipleCommandsTest
#[test]
fn command_stats_multiple_commands_test() {
  let provider = TestInfoProvider {
    command_stats_monitor: true,
    stats: vec![
      ("set".into(), 1, 0),
      ("get".into(), 1, 0),
      ("del".into(), 1, 0),
      ("ping".into(), 1, 0),
    ],
  };
  let info = execute_info(&provider, Some(b"COMMANDSTATS"));
  let lines: Vec<&str> = info.split("\r\n").collect();

  assert!(lines.iter().any(|l| l.starts_with("cmdstat_set:")));
  assert!(lines.iter().any(|l| l.starts_with("cmdstat_get:")));
  assert!(lines.iter().any(|l| l.starts_with("cmdstat_del:")));
  assert!(lines.iter().any(|l| l.starts_with("cmdstat_ping:")));
}

/// test/standalone/Garnet.test/RespCommandStatsTests.cs:CommandStatsSuccessRateTest
#[test]
fn command_stats_success_rate_test() {
  let mut stats = CommandStats::new();
  for _ in 0..5 {
    stats.increment_calls(RespCommand::Set);
  }
  let entry = stats.get_entry(RespCommand::Set);
  let success_rate = if entry.calls > 0 {
    (entry.calls - entry.failed_calls - entry.rejected_calls) as f64 / entry.calls as f64
  } else {
    0.0
  };
  assert!((success_rate - 1.0).abs() < 0.001);
}

/// test/standalone/Garnet.test/RespCommandStatsTests.cs:CommandStatsInfoServerShowsMonitorStatus
#[test]
fn command_stats_info_server_shows_monitor_status() {
  let provider = TestInfoProvider {
    command_stats_monitor: true,
    stats: Vec::new(),
  };
  let info = execute_info(&provider, Some(b"SERVER"));
  assert!(info.contains("commandstats_monitor:enabled"));
}

/// test/standalone/Garnet.test/RespCommandStatsTests.cs:CommandStatsFormatMatchesRedisConvention
#[test]
fn command_stats_format_matches_redis_convention() {
  let provider = TestInfoProvider {
    command_stats_monitor: true,
    stats: vec![("set".into(), 1, 0)],
  };
  let info = execute_info(&provider, Some(b"COMMANDSTATS"));
  let lines: Vec<&str> = info.split("\r\n").collect();
  let set_line = lines
    .iter()
    .find(|l| l.starts_with("cmdstat_set:"))
    .expect("Expected cmdstat_set entry");

  assert!(set_line.contains("calls="));
  assert!(set_line.contains("usec="));
  assert!(set_line.contains("usec_per_call="));
  assert!(set_line.contains("rejected_calls="));
  assert!(set_line.contains("failed_calls="));
}
