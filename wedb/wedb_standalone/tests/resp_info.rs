use std::cell::Cell;

use wnode::metrics::{
  garnet_session_metrics::GarnetSessionMetrics,
  info::{
    garnet_info_metrics::{
      DbSnapshot, GarnetInfoMetrics, GlobalMetricsSnapshot, InfoProvider, ServerFacts,
    },
    info_command::InfoCommand,
  },
  info_metrics_type::InfoMetricsType,
  metrics_item::MetricsItem,
};

struct InfoTestProvider {
  pub start_time: i64,
  pub dbs: Vec<DbSnapshot>,
  pub keyspace: Vec<(i32, u64, u64)>, // db_id -> (keys, expires)
  pub total_found: Cell<u64>,
}

impl InfoProvider for InfoTestProvider {
  fn server_facts(&self) -> ServerFacts {
    ServerFacts {
      version: "1.0.0".into(),
      run_id: "run123".into(),
      redis_protocol_version: "7.0".into(),
      enable_cluster: false,
      enable_aof: false,
      metrics_sampling_frequency: 10,
      latency_monitor: false,
      command_stats_monitor: false,
      startup_timestamp_unix_secs: self.start_time,
      log_dir: "/tmp/log".into(),
    }
  }

  fn databases(&self) -> Vec<DbSnapshot> {
    self.dbs.clone()
  }

  fn max_database_id(&self) -> i32 {
    self.dbs.iter().map(|d| d.id).max().unwrap_or(0)
  }

  fn global_metrics(&self) -> Option<GlobalMetricsSnapshot> {
    Some(GlobalMetricsSnapshot {
      global_session_metrics: GarnetSessionMetrics {
        total_found: self.total_found.get(),
        ..Default::default()
      },
      ..Default::default()
    })
  }

  fn command_stats(&self) -> Vec<(String, u64, u64)> {
    Vec::new()
  }

  fn keyspace_stats(&self, db_id: i32) -> (u64, u64) {
    self
      .keyspace
      .iter()
      .find(|(id, ..)| *id == db_id)
      .map(|(_, k, e)| (*k, *e))
      .unwrap_or((0, 0))
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

fn execute_info(
  provider: &InfoTestProvider,
  args: &[&[u8]],
  reset_cb: &mut impl FnMut(InfoMetricsType),
) -> String {
  let mut out = String::new();
  let mut info = GarnetInfoMetrics::new();
  InfoCommand::network_info(args, 0, provider, &mut info, reset_cb, &mut out);
  out
}

fn get_section_headers(info_output: &str) -> Vec<String> {
  let mut headers: Vec<String> = info_output
    .split("\r\n")
    .filter(|line| line.starts_with("# "))
    .map(|line| line.trim_start_matches("# ").trim().to_string())
    .collect();
  headers.sort();
  headers
}

/// test/standalone/Garnet.test/RespInfoTests.cs:ResetStatsTest
#[test]
fn reset_stats_test() {
  let provider = InfoTestProvider {
    start_time: 0,
    dbs: vec![DbSnapshot {
      id: 0,
      ..Default::default()
    }],
    keyspace: vec![(0, 0, 0)],
    total_found: Cell::new(0),
  };
  let reset_called = Cell::new(false);
  let mut on_reset = |t: InfoMetricsType| {
    if t == InfoMetricsType::Stats {
      reset_called.set(true);
      provider.total_found.set(0);
    }
  };

  let info = execute_info(&provider, &[], &mut on_reset);
  assert!(info.contains("total_found:0"));

  // 模拟请求成功后增加 total_found
  provider.total_found.set(1);
  let info = execute_info(&provider, &[], &mut on_reset);
  assert!(info.contains("total_found:1"));

  // 执行 INFO RESET
  let res = execute_info(&provider, &[b"RESET"], &mut on_reset);
  assert_eq!(res, "+OK\r\n");
  assert!(reset_called.get());

  let info = execute_info(&provider, &[], &mut on_reset);
  assert!(info.contains("total_found:0"));
}

/// test/standalone/Garnet.test/RespInfoTests.cs:UptimeIncreasesAcrossInfoCalls
#[test]
fn uptime_increases_across_info_calls() {
  let now = coarsetime::Clock::now_since_epoch().as_secs() as i64;
  let provider = InfoTestProvider {
    start_time: now - 10,
    dbs: vec![DbSnapshot {
      id: 0,
      ..Default::default()
    }],
    keyspace: vec![],
    total_found: Cell::new(0),
  };
  let mut noop = |_| {};
  let info = execute_info(&provider, &[b"SERVER"], &mut noop);
  let line = info
    .split("\r\n")
    .find(|l| l.starts_with("uptime_in_seconds:"))
    .unwrap();
  let uptime: i64 = line.split(':').nth(1).unwrap().parse().unwrap();
  assert!(uptime >= 10);
}

/// test/standalone/Garnet.test/RespInfoTests.cs:InfoSectionOptionsTest
#[test]
fn info_section_options_test() {
  let provider = InfoTestProvider {
    start_time: 0,
    dbs: vec![DbSnapshot {
      id: 0,
      ..Default::default()
    }],
    keyspace: vec![],
    total_found: Cell::new(0),
  };
  let mut noop = |_| {};

  for option in [
    b"ALL".as_slice(),
    b"DEFAULT".as_slice(),
    b"EVERYTHING".as_slice(),
  ] {
    let info = execute_info(&provider, &[option], &mut noop);
    assert!(!info.is_empty());

    assert!(info.contains("# Server"), "Should contain Server section");
    assert!(info.contains("# Memory"), "Should contain Memory section");
    assert!(info.contains("# Stats"), "Should contain Stats section");
    assert!(info.contains("# Clients"), "Should contain Clients section");

    // Keyspace is excluded from default/ALL/EVERYTHING
    assert!(
      !info.contains("# Keyspace"),
      "Should not contain Keyspace section"
    );

    if option == b"ALL" {
      assert!(
        !info.contains("# Modules"),
        "ALL should not contain Modules"
      );
    } else {
      assert!(
        info.contains("# Modules"),
        "DEFAULT/EVERYTHING should contain Modules"
      );
    }

    assert!(!info.contains("MainStoreHashTableDistribution"));
    assert!(!info.contains("ObjectStoreHashTableDistribution"));
    assert!(!info.contains("MainStoreDeletedRecordRevivification"));
    assert!(!info.contains("ObjectStoreDeletedRecordRevivification"));
    assert!(!info.contains("MainStoreHLogScan"));
    assert!(!info.contains("# Commandstats"));
  }
}

/// test/standalone/Garnet.test/RespInfoTests.cs:InfoDefaultMatchesNoArgsTest
#[test]
fn info_default_matches_no_args_test() {
  let provider = InfoTestProvider {
    start_time: 0,
    dbs: vec![DbSnapshot {
      id: 0,
      ..Default::default()
    }],
    keyspace: vec![],
    total_found: Cell::new(0),
  };
  let mut noop = |_| {};
  let info_no_args = execute_info(&provider, &[], &mut noop);
  let info_default = execute_info(&provider, &[b"DEFAULT"], &mut noop);

  assert_eq!(
    get_section_headers(&info_no_args),
    get_section_headers(&info_default)
  );
}

/// test/standalone/Garnet.test/RespInfoTests.cs:InfoAllWithModulesEqualsEverythingTest
#[test]
fn info_all_with_modules_equals_everything_test() {
  let provider = InfoTestProvider {
    start_time: 0,
    dbs: vec![DbSnapshot {
      id: 0,
      ..Default::default()
    }],
    keyspace: vec![],
    total_found: Cell::new(0),
  };
  let mut noop = |_| {};
  let info_everything = execute_info(&provider, &[b"EVERYTHING"], &mut noop);
  let info_all_modules = execute_info(&provider, &[b"ALL", b"MODULES"], &mut noop);

  assert_eq!(
    get_section_headers(&info_everything),
    get_section_headers(&info_all_modules)
  );
}

/// test/standalone/Garnet.test/RespInfoTests.cs:InfoKeyspaceEmptyDatabaseTest
#[test]
fn info_keyspace_empty_database_test() {
  let provider = InfoTestProvider {
    start_time: 0,
    dbs: vec![],
    keyspace: vec![(0, 0, 0)],
    total_found: Cell::new(0),
  };
  let mut noop = |_| {};
  let info = execute_info(&provider, &[b"KEYSPACE"], &mut noop);
  assert!(info.contains("# Keyspace"));
  // 空库不包含 db0:
  assert!(!info.contains("db0:"));
}

/// test/standalone/Garnet.test/RespInfoTests.cs:InfoKeyspaceCountsTest
#[test]
fn info_keyspace_counts_test() {
  let provider = InfoTestProvider {
    start_time: 0,
    dbs: vec![DbSnapshot {
      id: 0,
      ..Default::default()
    }],
    keyspace: vec![(0, 5, 3)],
    total_found: Cell::new(0),
  };
  let mut noop = |_| {};
  let info = execute_info(&provider, &[b"KEYSPACE"], &mut noop);
  let line = info
    .split("\r\n")
    .find(|l| l.starts_with("db0:"))
    .expect("Expected db0 line");
  assert_eq!(line, "db0:keys=5,expires=3,avg_ttl=0");
}

/// test/standalone/Garnet.test/RespInfoTests.cs:InfoKeyspaceExpiredKeysNotCountedTest
#[test]
fn info_keyspace_expired_keys_not_counted_test() {
  // 1 live key, 1 expired key => keys=1, expires=0
  let provider = InfoTestProvider {
    start_time: 0,
    dbs: vec![DbSnapshot {
      id: 0,
      ..Default::default()
    }],
    keyspace: vec![(0, 1, 0)],
    total_found: Cell::new(0),
  };
  let mut noop = |_| {};
  let info = execute_info(&provider, &[b"KEYSPACE"], &mut noop);
  let line = info
    .split("\r\n")
    .find(|l| l.starts_with("db0:"))
    .expect("Expected db0 line");
  assert_eq!(line, "db0:keys=1,expires=0,avg_ttl=0");
}

/// test/standalone/Garnet.test/RespInfoTests.cs:InfoKeyspaceMultiDatabaseTest
#[test]
fn info_keyspace_multi_database_test() {
  let provider = InfoTestProvider {
    start_time: 0,
    dbs: vec![
      DbSnapshot {
        id: 0,
        ..Default::default()
      },
      DbSnapshot {
        id: 1,
        ..Default::default()
      },
    ],
    keyspace: vec![(0, 2, 1), (1, 1, 0)],
    total_found: Cell::new(0),
  };
  let mut noop = |_| {};
  let info = execute_info(&provider, &[b"KEYSPACE"], &mut noop);
  let lines: Vec<&str> = info.split("\r\n").collect();

  assert_eq!(
    lines.iter().find(|l| l.starts_with("db0:")).copied(),
    Some("db0:keys=2,expires=1,avg_ttl=0")
  );
  assert_eq!(
    lines.iter().find(|l| l.starts_with("db1:")).copied(),
    Some("db1:keys=1,expires=0,avg_ttl=0")
  );
  assert!(lines.iter().find(|l| l.starts_with("db2:")).is_none());
}
