//! sync 持久化语义回归测试
//!
//! 对标上游 redb-bench crates/redb-bench/src/lib.rs:201-216,1020:
//! connect 默认 sync=true (持久写契约)；nosync 段 set_sync(false) 后恢复 set_sync(true)

use bench::{
  engines::{
    redb_engine::RedbEngine,
    sqlite_engine::{SqliteConnection, SqliteEngine},
  },
  harness::run_benchmark,
  traits::{BenchDatabase, BenchDatabaseConnection, BenchReadTransaction, BenchWriteTransaction},
  types::{BASE_TIMEOUT_SECS, BenchmarkConfig, ResultType},
};

#[test]
fn sqlite_sync_switch_maps_to_full_and_off() {
  let dir = tempfile::tempdir().unwrap();
  let engine = SqliteEngine::open(&dir.path().join("b.sqlite"), 1024 * 1024).unwrap();
  let mut conn = engine.connect();
  let pragma = |conn: &SqliteConnection| {
    conn
      .conn
      .query_row("PRAGMA synchronous", [], |r| r.get::<_, i64>(0))
      .unwrap()
  };
  // connect 默认持久写契约: synchronous = FULL (2)
  assert_eq!(pragma(&conn), 2);
  // nosync 口径: OFF (0)，完全不 fsync，对齐 redb Durability::None
  assert!(conn.set_sync(false));
  assert_eq!(pragma(&conn), 0);
  // 段后恢复: FULL (2)
  assert!(conn.set_sync(true));
  assert_eq!(pragma(&conn), 2);
}

#[test]
fn sqlite_writes_commit_under_both_sync_modes() {
  let dir = tempfile::tempdir().unwrap();
  let engine = SqliteEngine::open(&dir.path().join("b.sqlite"), 1024 * 1024).unwrap();
  let mut conn = engine.connect();
  for sync in [false, true] {
    assert!(conn.set_sync(sync));
    let mut txn = conn.write_transaction();
    txn.insert(b"k1", b"v1").unwrap();
    txn.commit().unwrap();
  }
  let mut rd = conn.read_transaction();
  assert_eq!(rd.get(b"k1"), Some(b"v1".to_vec()));
}

/// nosync 段恢复持久写后，后续段落 (removals 等) 全链路正常
#[test]
fn redb_run_benchmark_keeps_sections_after_sync_restore() {
  let dir = tempfile::tempdir().unwrap();
  let engine = RedbEngine::open(&dir.path().join("b.redb"), 1024 * 1024).unwrap();
  let cfg = BenchmarkConfig {
    key_size: 8,
    value_size: 8,
    cache_size: 1024 * 1024,
    bulk_elements: 2_000,
    individual_writes: 20,
    batch_writes: 5,
    batch_size: 10,
    nosync_writes: 50,
    num_reads: 100,
    read_iterations: 1,
    num_scans: 10,
    scan_len: 5,
    scan_iterations: 1,
    removals: 10,
    rng_seed: 3,
    timeout_secs: BASE_TIMEOUT_SECS,
  };
  let results = run_benchmark(engine, dir.path(), &cfg);
  let keys: Vec<&str> = results.iter().map(|(k, _)| k.as_str()).collect();
  for expect in [
    "bulk_load",
    "individual_writes",
    "batch_writes",
    "nosync_writes",
    "len",
    "random_reads",
    "random_range_reads",
    "random_reads_4",
    "random_reads_8",
    "random_reads_16",
    "random_reads_32",
    "removals",
    "uncompacted_size",
    "compacted_size",
    "memory",
  ] {
    assert!(keys.contains(&expect), "缺少段落 {expect}");
  }
  // redb 支持 set_sync：nosync 段为真实吞吐；恢复持久写后 removals 段亦非 NA
  for (k, v) in &results {
    if matches!(k.as_str(), "nosync_writes" | "removals") {
      assert!(!matches!(v, ResultType::NA), "{k} 不应为 NA");
    }
  }
}
