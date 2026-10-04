//! 防优化消费屏障回归测试（票 bench-read-loop-no-consume-barrier）
//!
//! 对标上游 redb-bench crates/redb-bench/src/lib.rs:236-246,266-277 的
//! 「读结果消费 + 校验和断言」口径：已知值集读写校验和往返断言，
//! 杜绝 range_scan 返回 (0,0) 空桩与 harness 读链在 lto=fat 下被优化消除。
//! 反证自检（人工执行）：临时注释掉读循环体内 get 调用重编后，
//! run_benchmark 的校验和/命中数断言必须失败。

#[cfg(feature = "wkv")]
use bench::engines::wkv_engine::WkvEngine;
use bench::{
  engines::redb_engine::RedbEngine,
  harness::run_benchmark,
  traits::{BenchDatabase, BenchDatabaseConnection, BenchReadTransaction, BenchWriteTransaction},
  types::{BASE_TIMEOUT_SECS, BenchmarkConfig, ResultType},
};

/// redb range_scan 真实累加值首字节校验和（有序范围扫语义基准形态）
#[test]
fn redb_range_scan_returns_real_first_byte_checksum() {
  let dir = tempfile::tempdir().unwrap();
  let engine = RedbEngine::open(&dir.path().join("b.redb"), 1024 * 1024).unwrap();
  let conn = engine.connect();
  {
    let mut txn = conn.write_transaction();
    for i in 0..3u8 {
      txn.insert(&[b'a' + i], &[10 + i, 0, 0, 0]).unwrap();
    }
    txn.commit().unwrap();
  }
  let mut rd = conn.read_transaction();
  let (scanned, sum) = rd.range_scan(b"a", 3);
  assert_eq!(scanned, 3);
  assert_eq!(sum, 10 + 11 + 12, "校验和须为逐条值首字节真实累加");
  assert_eq!(rd.get(b"b"), Some(vec![11, 0, 0, 0]));
}

/// wkv range_scan 非空桩：锚定起始键记录地址沿 hlog 正向步进，真实累加校验和
#[cfg(feature = "wkv")]
#[test]
fn wkv_range_scan_accumulates_real_checksum_not_stub() {
  let dir = tempfile::tempdir().unwrap();
  let engine = WkvEngine::open(&dir.path().join("b.wkv"), 4 * 1024 * 1024, 2_000).unwrap();
  let conn = engine.connect();
  {
    let mut txn = conn.write_transaction();
    for i in 0..10u8 {
      txn.insert(&[b'k', i], &[i + 1, 0, 0]).unwrap();
    }
    txn.commit().unwrap();
  }
  let mut rd = conn.read_transaction();
  // k2..k4 三条日志记录正向扫：值首字节 3+4+5
  let (scanned, sum) = rd.range_scan(b"k\x02", 3);
  assert_eq!(scanned, 3, "存在锚键时须真实步进记录，不得回 (0,0) 空桩");
  assert_eq!(sum, 3 + 4 + 5);
  // 无锚键如实回 (0,0)，由 harness 单机制折算 N/A
  let (miss_scanned, miss_sum) = rd.range_scan(b"k\xFF", 3);
  assert_eq!((miss_scanned, miss_sum), (0, 0));
}

/// wkv 全段 run_benchmark 闭环：消费屏障断言全通过且范围扫行不再记 N/A
#[cfg(feature = "wkv")]
#[test]
fn wkv_run_benchmark_passes_consume_barriers_with_real_range_row() {
  let dir = tempfile::tempdir().unwrap();
  let engine = WkvEngine::open(&dir.path().join("b2.wkv"), 4 * 1024 * 1024, 2_000).unwrap();
  let cfg = BenchmarkConfig {
    key_size: 8,
    value_size: 8,
    cache_size: 4 * 1024 * 1024,
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
  let range = results
    .iter()
    .find(|(k, _)| k == "random_range_reads")
    .expect("random_range_reads 行必产出");
  assert!(
    !matches!(range.1, ResultType::NA),
    "wkv 范围扫已补真实累加，不应再走 (0,0) 空桩 N/A 通道"
  );
}
