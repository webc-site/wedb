//! 按库段取行回落回归（工单 wmetric-info-per-db-sections-nonzero-active-db-empty-body）
//!
//! 对标 C# GarnetInfoMetrics.GetRespInfo / GetMetricInternal 的 `storeInfo[dbId]`
//! 取行链路。wedb 单物理存储多库前缀隔离，InfoProvider.databases 仅暴露唯一
//! 物理首行（db 0）；旧实现按活跃库号 `get(db_id)` 越界回 None，Store /
//! Persistence / HlogScan / StoreHashtable / StoreReviv 段在非零活跃库下恒出
//! 空体。本用例钉住修复后的取行收口：非零活跃库段体非空且字段语义正确，
//! 活跃库 0 与非零库段体逐字节同源（仅段头库号不同）。
//!
//! 自研依据: INFO 行回退（C# 对应 RespInfoTests.cs INFO 段）

use wmetric::{
  AofSnapshot, DbSnapshot, GarnetInfoMetrics, GlobalMetricsSnapshot, InfoProvider, ServerFacts,
};
use wresp::metrics::{InfoMetricsType, MetricsItem};

/// 单物理存储快照数据源：databases 恒返回唯一 db 0 物理行
struct SingleStoreProvider;

impl InfoProvider for SingleStoreProvider {
  fn server_facts(&self) -> ServerFacts {
    ServerFacts {
      version: "1.0.0".into(),
      run_id: "run".into(),
      redis_protocol_version: "7.0".into(),
      enable_cluster: false,
      // 与 db.aof 面同源：AOF 在场 → PERSISTENCE 段出六地址
      enable_aof: true,
      metrics_sampling_frequency: 0,
      latency_monitor: false,
      command_stats_monitor: false,
      startup_stopwatch_ticks: 0,
      log_dir: "/tmp/log".into(),
    }
  }

  fn databases(&self) -> Vec<DbSnapshot> {
    vec![DbSnapshot {
      id: 0,
      current_version: 7,
      index_total_memory_size_bytes: 4288,
      log_tail_address: 1024,
      aof: Some(AofSnapshot {
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
    }]
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
    vec![("MainStore dump live=1".into(), String::new())]
  }
  fn safe_aof_address(&self) -> i64 {
    99
  }
}

/// 渲染单段文本
fn render(section: InfoMetricsType, db_id: i32) -> String {
  let mut info = GarnetInfoMetrics::new();
  info.get_resp_info(&[section], db_id, &SingleStoreProvider)
}

/// 段体（去段头）
fn body(text: &str) -> &str {
  let nl = text.find("\r\n").expect("应有段头");
  &text[nl + 2..]
}

/// 非零活跃库：五段均回落唯一物理首行、出完整字段集
#[test]
fn nonzero_active_db_store_sections_populated() {
  let store = render(InfoMetricsType::Store, 3);
  assert!(store.contains("# Store_DB_3\r\n"), "{store}");
  assert!(store.contains("CurrentVersion:7"), "{store}");
  assert!(store.contains("Log.TailAddress:1024"), "{store}");

  let persist = render(InfoMetricsType::Persistence, 3);
  assert!(persist.contains("# Persistence_DB_3\r\n"), "{persist}");
  assert!(persist.contains("CommittedUntilAddress:768"), "{persist}");
  assert!(persist.contains("SafeAofAddress:99"), "{persist}");

  let hash = render(InfoMetricsType::StoreHashtable, 3);
  assert!(
    hash.contains("# StoreHashTableDistribution_DB_3\r\n"),
    "{hash}"
  );
  assert!(
    hash.contains("Number of hash buckets: 64"),
    "STOREHASHTABLE 段体非空: {hash}"
  );

  let reviv = render(InfoMetricsType::StoreReviv, 3);
  assert!(
    reviv.contains("# StoreDeletedRecordRevivification_DB_3\r\n"),
    "{reviv}"
  );
  assert!(reviv.contains("Puts: 3"), "STOREREVIV 段体非空: {reviv}");

  let hlog = render(InfoMetricsType::HlogScan, 3);
  assert!(hlog.contains("# MainStoreHLogScan_DB_3\r\n"), "{hlog}");
  assert!(
    hlog.contains("MainStore_HLog_0:MainStore dump live=1"),
    "HLOGSCAN 段体非空: {hlog}"
  );
}

/// 活跃库 0 与库 3 的段体逐字节同源（仅段头库号不同）——取行回落不改变内容
#[test]
fn active_db_zero_and_nonzero_bodies_identical() {
  for section in [
    InfoMetricsType::Store,
    InfoMetricsType::Persistence,
    InfoMetricsType::StoreHashtable,
    InfoMetricsType::StoreReviv,
    InfoMetricsType::HlogScan,
  ] {
    let db0 = render(section, 0);
    let db3 = render(section, 3);
    assert_eq!(body(&db0), body(&db3), "段 {section:?} 段体应同源");
  }
}

/// get_metric 取行与 RESP 渲染同源：非零活跃库返回物理首行指标集
#[test]
fn get_metric_nonzero_active_db_returns_physical_row() {
  let mut metrics = GarnetInfoMetrics::new();
  let store = metrics
    .get_metric(InfoMetricsType::Store, 5, &SingleStoreProvider)
    .expect("db 5 STORE 应回落物理首行");
  assert!(
    store
      .iter()
      .any(|i| i.name.as_ref() == "CurrentVersion" && i.value == "7")
  );
  let persist = metrics
    .get_metric(InfoMetricsType::Persistence, 5, &SingleStoreProvider)
    .expect("db 5 PERSISTENCE 应回落物理首行");
  assert!(
    persist
      .iter()
      .any(|i| i.name.as_ref() == "TailAddress" && i.value == "1024")
  );
}
