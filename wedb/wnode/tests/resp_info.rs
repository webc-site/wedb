use std::{cell::Cell, str::from_utf8, thread::sleep, time::Duration};

use wbase::time::now_stopwatch_ticks;
use wmetric::{
  GarnetSessionMetrics,
  info::{
    garnet_info_metrics::{
      DbSnapshot, GarnetInfoMetrics, GlobalMetricsSnapshot, InfoProvider, ServerFacts,
    },
    info_command::InfoCommand,
  },
};
use wresp::metrics::{InfoMetricsType, MetricsItem};

struct InfoTestProvider {
  pub start_ticks: u64,
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
      // AOF 开关与 db.aof 面同源（生产装配同径 wnode/src/resp/garnet_api/mod.rs
      // :241-246：aof_memory_size_bytes 与 aof 两项都由同一个 db.aof 派生；C# 侧
      // 亦单源——GarnetInfoMetrics.cs:92 `db.AppendOnlyFile != null ? …`、
      // :368/:527 的 EnableAOF 与 AppendOnlyFile 实例同时成立）。夹具此前把
      // enable_aof 硬编 false，导致同一快照既声明 aof: Some 又被 MEMORY/PERSISTENCE
      // 两段按 AOF 关判读，两处断言互斥，故此处按面在场推导。
      enable_aof: self.dbs.iter().any(|db| db.aof.is_some()),
      metrics_sampling_frequency: 10,
      latency_monitor: false,
      command_stats_monitor: false,
      startup_stopwatch_ticks: self.start_ticks,
      log_dir: "/tmp/log".into(),
    }
  }

  fn databases(&self) -> Vec<DbSnapshot> {
    self.dbs.clone()
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

  fn command_stats(&self) -> Vec<(String, u64, u64, u64)> {
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

fn execute_info_at(
  provider: &InfoTestProvider,
  db_id: i32,
  args: &[&[u8]],
  reset_cb: &mut impl FnMut(InfoMetricsType),
) -> String {
  let out = execute_info_raw(provider, db_id, args, reset_cb, 2);
  String::from_utf8(out).expect("INFO 应答应为 UTF-8 文本")
}

fn execute_info_raw(
  provider: &InfoTestProvider,
  db_id: i32,
  args: &[&[u8]],
  reset_cb: &mut impl FnMut(InfoMetricsType),
  resp_protocol_version: u8,
) -> Vec<u8> {
  let mut out = Vec::new();
  let mut info = GarnetInfoMetrics::new();
  InfoCommand::network_info(
    args,
    db_id,
    provider,
    &mut info,
    reset_cb,
    resp_protocol_version,
    &mut out,
  );
  out
}

fn execute_info(
  provider: &InfoTestProvider,
  args: &[&[u8]],
  reset_cb: &mut impl FnMut(InfoMetricsType),
) -> String {
  execute_info_at(provider, 0, args, reset_cb)
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
    start_ticks: 0,
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
  // 启动事实取当前单调刻度读数（uptime 自取时点起算），与 C# 服务端
  // Stopwatch.GetElapsedTime(startupTimestamp) 同域
  let provider = InfoTestProvider {
    start_ticks: now_stopwatch_ticks(),
    dbs: vec![DbSnapshot {
      id: 0,
      ..Default::default()
    }],
    keyspace: vec![],
    total_found: Cell::new(0),
  };
  let mut noop = |_| {};
  let uptime = |info: String| -> i64 {
    let line = info
      .split("\r\n")
      .find(|l| l.starts_with("uptime_in_seconds:"))
      .unwrap();
    line.split(':').nth(1).unwrap().parse().unwrap()
  };
  let first = uptime(execute_info(&provider, &[b"SERVER"], &mut noop));
  assert!(first >= 0);

  sleep(Duration::from_millis(1_100));

  let second = uptime(execute_info(&provider, &[b"SERVER"], &mut noop));
  assert!(
    second > first,
    "uptime_in_seconds should increase between INFO calls"
  );
}

/// test/standalone/Garnet.test/RespInfoTests.cs:InfoSectionOptionsTest
#[test]
fn info_section_options_test() {
  let provider = InfoTestProvider {
    start_ticks: 0,
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
    start_ticks: 0,
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
    start_ticks: 0,
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
    start_ticks: 0,
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
    start_ticks: 0,
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
    start_ticks: 0,
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
    start_ticks: 0,
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

/// STORE 族行表按库号取行（对标 C# PopulateStoreStats 的 MaxDatabaseId + 1
/// 预分配与 GetDatabasesSnapshot 同源填充）：库号超 16 不截断，命中已填行
/// 取本行事实；wedb 单物理存储下空洞库与越界库一律回落至唯一物理首行
///（db 0），段头仍按活跃库号呈现。
#[test]
fn info_store_rows_follow_databases_above_16() {
  let provider = InfoTestProvider {
    start_ticks: 0,
    dbs: vec![
      DbSnapshot {
        id: 0,
        current_version: 1,
        ..Default::default()
      },
      DbSnapshot {
        id: 20,
        current_version: 21,
        ..Default::default()
      },
      DbSnapshot {
        id: 40,
        current_version: 41,
        ..Default::default()
      },
    ],
    keyspace: vec![],
    total_found: Cell::new(0),
  };
  let mut noop = |_| {};

  // 当前库 40：出段头且填入本行快照事实
  let info = execute_info_at(&provider, 40, &[b"STORE"], &mut noop);
  assert!(info.contains("# Store_DB_40"));
  assert!(info.contains("CurrentVersion:41"));

  // 当前库 20：同一份行表按库 id 精确命中
  let info = execute_info_at(&provider, 20, &[b"STORE"], &mut noop);
  assert!(info.contains("# Store_DB_20"));
  assert!(info.contains("CurrentVersion:21"));

  // 空洞库 7：本行未填，回落唯一物理首行（db 0），段头按活跃库号呈现
  let info = execute_info_at(&provider, 7, &[b"STORE"], &mut noop);
  assert!(info.contains("# Store_DB_7"));
  assert!(info.contains("CurrentVersion:1"));

  // 越界库 41：行表长度 = 最大库号 + 1，越界亦回落物理首行
  let info = execute_info_at(&provider, 41, &[b"STORE"], &mut noop);
  assert!(info.contains("# Store_DB_41"));
  assert!(info.contains("CurrentVersion:1"));

  // 取行收口与 RESP 渲染同源：精确命中本行、空洞/越界回落物理首行
  let mut metrics = GarnetInfoMetrics::new();
  let store_40 = metrics
    .get_metric(InfoMetricsType::Store, 40, &provider)
    .expect("db 40 行应存在");
  assert_eq!(
    store_40
      .iter()
      .find(|i| i.name.as_ref() == "CurrentVersion")
      .map(|i| i.value.as_str()),
    Some("41")
  );
  let store_7 = metrics
    .get_metric(InfoMetricsType::Store, 7, &provider)
    .expect("空洞库 7 应回落物理首行");
  assert!(
    store_7
      .iter()
      .any(|i| i.name.as_ref() == "CurrentVersion" && i.value == "1")
  );
  let store_41 = metrics
    .get_metric(InfoMetricsType::Store, 41, &provider)
    .expect("越界库 41 应回落物理首行");
  assert!(
    store_41
      .iter()
      .any(|i| i.name.as_ref() == "CurrentVersion" && i.value == "1")
  );
}

/// 单物理存储多库形态（对标 wnode `SingleDatabaseManager::get_databases_snapshot`
/// 恒返回唯一 db 0 快照）：活跃库号 ≥ 1 时 STORE / PERSISTENCE 段回落物理首行、
/// 出完整字段集，段头按活跃库号呈现；活跃库 0 输出与回落行逐字节一致。
#[test]
fn info_store_sections_nonempty_for_nonzero_active_db() {
  let db = DbSnapshot {
    id: 0,
    current_version: 7,
    index_bucket_count: 64,
    index_bucket_size_bytes: 64,
    index_memory_size_bytes: 4096,
    index_overflow_bucket_count: 3,
    index_overflow_memory_size_bytes: 192,
    index_total_memory_size_bytes: 4288,
    log_tail_address: 1024,
    log_flushed_until_address: 512,
    aof_memory_size_bytes: 1024,
    aof: Some(wmetric::AofSnapshot {
      committed_begin_address: 64,
      committed_until_address: 768,
      flushed_until_address: 512,
      begin_address: 64,
      tail_address: 1024,
      flush_failures: 0,
    }),
    hash_distribution_dump: "Number of hash buckets: 64\n".into(),
    revivification_dump: "Puts: 3\n".into(),
    ..Default::default()
  };
  let provider = InfoTestProvider {
    start_ticks: 0,
    // 唯一物理快照行（db 0）
    dbs: vec![db.clone()],
    keyspace: vec![],
    total_found: Cell::new(0),
  };
  let mut noop = |_| {};

  // 活跃库 1：STORE 段头呈现 DB_1 且字段集完整（对标 C# Store_DB_n 全字段）
  let store_1 = execute_info_at(&provider, 1, &[b"STORE"], &mut noop);
  assert!(store_1.contains("# Store_DB_1"), "{store_1}");
  assert!(store_1.contains("CurrentVersion:7"), "{store_1}");
  assert!(store_1.contains("Log.TailAddress:1024"), "{store_1}");
  // 活跃库 0：回落行与精确命中同源，输出逐字节一致（仅段头库号不同）
  let store_0 = execute_info_at(&provider, 0, &[b"STORE"], &mut noop);
  assert!(
    store_0.replace("Store_DB_0", "Store_DB_1") == store_1,
    "活跃库 0/1 的 STORE 段体应逐字节同源"
  );

  // 活跃库 1：PERSISTENCE 段（AOF 开）六地址齐全
  let persist_1 = execute_info_at(&provider, 1, &[b"PERSISTENCE"], &mut noop);
  assert!(persist_1.contains("# Persistence_DB_1"), "{persist_1}");
  assert!(
    persist_1.contains("CommittedUntilAddress:768"),
    "{persist_1}"
  );

  // 活跃库 1：STOREHASHTABLE / STOREREVIV 转储直出非空
  let hash_1 = execute_info_at(&provider, 1, &[b"STOREHASHTABLE"], &mut noop);
  assert!(
    hash_1.contains("# StoreHashTableDistribution_DB_1"),
    "{hash_1}"
  );
  assert!(hash_1.contains("Number of hash buckets: 64"), "{hash_1}");
  let reviv_1 = execute_info_at(&provider, 1, &[b"STOREREVIV"], &mut noop);
  assert!(
    reviv_1.contains("# StoreDeletedRecordRevivification_DB_1"),
    "{reviv_1}"
  );
  assert!(reviv_1.contains("Puts: 3"), "{reviv_1}");
}

/// 存储域快照通道填充链路（f20-info-snapshot）：STORE 段字段表 + MEMORY
/// store_* 聚合 + PERSISTENCE 六地址（AOF 开/关）+ STOREHASHTABLE /
/// STOREREVIV 转储直出——对标 C# GarnetInfoMetrics.cs 的
/// GetDatabaseStoreStats / PopulateMemoryInfo / GetDatabasePersistenceStats /
/// PopulateStoreHashDistribution / PopulateStoreRevivInfo
#[test]
fn info_store_snapshot_channel_populates_segments() {
  let db = DbSnapshot {
    id: 0,
    current_version: 7,
    index_bucket_count: 64,
    index_bucket_size_bytes: 64,
    index_memory_size_bytes: 4096,
    index_overflow_bucket_count: 3,
    index_overflow_memory_size_bytes: 192,
    index_total_memory_size_bytes: 4288,
    log_page_size_bytes: 4096,
    log_max_allocated_page_count: 16,
    log_allocated_page_count: 16,
    log_max_memory_size_bytes: 65536,
    log_memory_size_bytes: 65536,
    log_heap_size_bytes: 65536,
    log_begin_address: 64,
    log_head_address: 128,
    log_safe_readonly_address: 256,
    log_flushed_until_address: 512,
    log_tail_address: 1024,
    // AOF 面在场（见上 enable_aof 推导）：aof_memory_size_bytes 是 provider 侧
    // 上报的 AOF 日志内存，对位 C# GarnetInfoMetrics.cs:92
    // `db.AppendOnlyFile != null ? …Log.MemorySizeBytes.AggregateDiff(0) : 0` 的
    // 入参形态，由生产装配 garnet_api/mod.rs:241-244 从同一个 db.aof 派生
    aof_memory_size_bytes: 1024,
    aof: Some(wmetric::AofSnapshot {
      committed_begin_address: 64,
      committed_until_address: 768,
      flushed_until_address: 512,
      begin_address: 64,
      tail_address: 1024,
      flush_failures: 0,
    }),
    // 读缓存段：C# PopulateMemoryInfo 的第三加项 store_readcache_memory_size
    //（GarnetInfoMetrics.cs:103,:107 取 db.Store.ReadCache.MemorySizeBytes /
    // SizeTracker.readCacheTracker.TotalSize），缺省 None 时该加项恒 0
    read_cache: Some(wmetric::ReadCacheSnapshot {
      page_size_bytes: 4096,
      max_allocated_page_count: 8,
      allocated_page_count: 4,
      max_memory_size_bytes: 32768,
      memory_size_bytes: 200,
      heap_size_bytes: 200,
      ..Default::default()
    }),
    hash_distribution_dump: "Number of hash buckets: 64\n".into(),
    revivification_dump: "Puts: 3\n".into(),
    ..Default::default()
  };
  let provider = InfoTestProvider {
    start_ticks: 0,
    dbs: vec![db],
    keyspace: vec![],
    total_found: Cell::new(0),
  };
  let mut noop = |_| {};

  // STORE 段：快照字段逐项入表
  let info = execute_info(&provider, &[b"STORE"], &mut noop);
  assert!(info.contains("# Store_DB_0"));
  assert!(info.contains("CurrentVersion:7"));
  assert!(info.contains("IndexBucketCount:64"));
  assert!(info.contains("IndexTotalMemorySizeBytes:4288"));
  assert!(info.contains("Log.TailAddress:1024"));
  assert!(info.contains("Log.FlushedUntilAddress:512"));

  // MEMORY 段：store_* 聚合三项加和（C# GarnetInfoMetrics.cs:111
  // total = store_index_size + store_mainlog_memory_size +
  // store_readcache_memory_size）；aof_memory_size 起点按 EnableAOF 取 0/-1
  //（:85 enableAof ? 0 : -1L），再逐库累加 AOF 日志内存（:92）
  let info = execute_info(&provider, &[b"MEMORY"], &mut noop);
  assert!(info.contains("store_index_size:4288"));
  assert!(info.contains("store_mainlog_memory_size:65536"));
  assert!(info.contains("store_readcache_memory_size:200"));
  assert!(info.contains("total_main_store_size:70024"));
  assert!(info.contains("aof_memory_size:1024"));

  // PERSISTENCE 段：AOF 开启出六地址（段门 C# :527 `if
  // (!storeWrapper.serverOptions.EnableAOF) return;` 同形，AOF 关整段省略，
  // 见 info_persistence_omitted_when_aof_disabled）
  let info = execute_info(&provider, &[b"PERSISTENCE"], &mut noop);
  assert!(info.contains("# Persistence_DB_0"));
  assert!(info.contains("CommittedBeginAddress:64"));
  assert!(info.contains("CommittedUntilAddress:768"));
  assert!(info.contains("FlushedUntilAddress:512"));
  assert!(info.contains("BeginAddress:64"));
  assert!(info.contains("TailAddress:1024"));
  assert!(info.contains("SafeAofAddress:0"));

  // STOREHASHTABLE / STOREREVIV：转储文本非空直出（无名首项裸出值）
  let info = execute_info(&provider, &[b"STOREHASHTABLE"], &mut noop);
  assert!(info.contains("# StoreHashTableDistribution_DB_0"));
  assert!(info.contains("Number of hash buckets: 64"));
  let info = execute_info(&provider, &[b"STOREREVIV"], &mut noop);
  assert!(info.contains("# StoreDeletedRecordRevivification_DB_0"));
  assert!(info.contains("Puts: 3"));
}

/// AOF 关闭时 PERSISTENCE 段整段省略（wmetric 缺省形态，绝不虚报）
#[test]
fn info_persistence_omitted_when_aof_disabled() {
  let provider = InfoTestProvider {
    start_ticks: 0,
    dbs: vec![DbSnapshot {
      id: 0,
      ..Default::default()
    }],
    keyspace: vec![],
    total_found: Cell::new(0),
  };
  let mut noop = |_| {};
  let info = execute_info(&provider, &[b"PERSISTENCE"], &mut noop);
  assert!(!info.contains("Persistence_DB_0"));
  // AOF 关时 MEMORY 的 aof_memory_size 停 -1 哨兵且不被累加项覆盖
  //（C# GarnetInfoMetrics.cs:85 `enableAof ? 0 : -1L`，本夹具 aof 面不在场故
  // :92 加项为 0）
  let info = execute_info(&provider, &[b"MEMORY"], &mut noop);
  assert!(info.contains("aof_memory_size:-1"));
}

/// RESP3 会话 INFO 非空段以 verbatim string（=<len>\r\ntxt:<info>\r\n）出帧；
/// len 包含 txt: 4 字节元数据；RESP2 保持 $<len>\r\n<info>\r\n 批量串；
/// 空段 RESP_EMPTY（$0\r\n\r\n）恒定不动。
#[test]
fn info_resp3_verbatim_string_and_resp2_bulk_regression() {
  let provider = InfoTestProvider {
    start_ticks: 0,
    dbs: vec![DbSnapshot {
      id: 0,
      ..Default::default()
    }],
    keyspace: vec![],
    total_found: Cell::new(0),
  };
  let mut noop = |_| {};

  // 1. RESP2 回归：非空段保持 $ bulk 不变
  let out_resp2 = execute_info_raw(&provider, 0, &[b"SERVER"], &mut noop, 2);
  assert!(
    out_resp2.starts_with(b"$"),
    "RESP2 应以 bulk string ($) 开头"
  );
  let crlf_pos2 = out_resp2
    .windows(2)
    .position(|w| w == b"\r\n")
    .expect("CRLF required");
  let len_str2 = from_utf8(&out_resp2[1..crlf_pos2]).expect("len string utf8");
  let len2: usize = len_str2.parse().expect("valid bulk length");
  let payload2 = &out_resp2[crlf_pos2 + 2..out_resp2.len() - 2];
  assert_eq!(
    len2,
    payload2.len(),
    "RESP2 bulk length 必须等于 payload 字节数"
  );
  assert!(
    payload2.starts_with(b"# Server\r\n"),
    "payload 内容应包含 Server 段"
  );

  // 2. RESP3: 非空段首帧为 =<len>\r\ntxt:；len 含 txt: 4 字节元数据
  let out_resp3 = execute_info_raw(&provider, 0, &[b"SERVER"], &mut noop, 3);
  assert!(
    out_resp3.starts_with(b"="),
    "RESP3 应以 verbatim string (=) 开头"
  );
  let crlf_pos3 = out_resp3
    .windows(2)
    .position(|w| w == b"\r\n")
    .expect("CRLF required");
  let len_str3 = from_utf8(&out_resp3[1..crlf_pos3]).expect("len string utf8");
  let total_len3: usize = len_str3.parse().expect("valid verbatim length");
  assert_eq!(
    &out_resp3[crlf_pos3 + 2..crlf_pos3 + 6],
    b"txt:",
    "RESP3 verbatim 前缀必须为 txt:"
  );
  let text3 = &out_resp3[crlf_pos3 + 6..out_resp3.len() - 2];
  assert_eq!(
    total_len3,
    4 + text3.len(),
    "RESP3 verbatim 长度必须等于 4(txt:) + text.len()"
  );
  assert_eq!(text3, payload2, "RESP3 与 RESP2 的正文文本内容必须完全一致");

  // 3. 空段回归：RESP2 与 RESP3 均恒定回 RESP_EMPTY（$0\r\n\r\n）
  let empty_resp2 = execute_info_raw(&provider, 0, &[b"PERSISTENCE"], &mut noop, 2);
  assert_eq!(empty_resp2, b"$0\r\n\r\n", "RESP2 空段应回 RESP_EMPTY");
  let empty_resp3 = execute_info_raw(&provider, 0, &[b"PERSISTENCE"], &mut noop, 3);
  assert_eq!(empty_resp3, b"$0\r\n\r\n", "RESP3 空段应回 RESP_EMPTY");
}
