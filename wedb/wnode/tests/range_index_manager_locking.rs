//! 范围索引存根 span 原语与删除独占锁集成测试（自 src 内嵌测试迁出）
//!
//! 对标 libs/server/Resp/RangeIndex/RangeIndexManager.{Index,Locking}.cs：
//! 存根写入 / 零拷贝读 / 标志清除，以及跨线程条带独占锁互斥。

use std::{
  sync::{Arc, mpsc},
  thread,
  time::Duration,
};

use tempfile::tempdir;
use wbftree::{RANGE_INDEX_STUB_SIZE, RangeIndexManager, RangeIndexStub, TreeTuning};
use wnode::resp::rangeindex::{
  range_index_manager_index::RangeIndexManagerIndex,
  range_index_manager_locking::RangeIndexManagerLocking,
};

const DISK: u8 = 0;

fn tuning(cache: usize, min: usize, max: usize, key_len: usize, leaf: usize) -> TreeTuning {
  TreeTuning {
    cache_size: cache,
    min_record_size: min,
    max_record_size: max,
    max_key_len: key_len,
    leaf_page_size: leaf,
  }
}

// ======================== Index 分片（存根 span 原语） ========================

/// RangeIndexManager.Index.cs:CreateIndex/ReadIndex 往返
#[test]
fn create_then_read_roundtrip() {
  let mut span = [0u8; 64];
  RangeIndexManagerIndex::create_index(
    tuning(16 * 1024 * 1024, 64, 1024, 128, 4096),
    DISK,
    0xDEAD_BEEF,
    &mut span,
  )
  .unwrap();

  let stub = RangeIndexManagerIndex::read_index(&span).expect("stub decodable");
  assert_eq!(stub.tree_handle, 0xDEAD_BEEF);
  assert_eq!(stub.cache_size, 16 * 1024 * 1024);
  assert_eq!(stub.min_record_size, 64);
  assert_eq!(stub.max_record_size, 1024);
  assert_eq!(stub.max_key_len, 128);
  assert_eq!(stub.leaf_page_size, 4096);
  assert_eq!(stub.storage_backend, DISK);
  // 新建存根：标志位与序列化阶段号全零
  assert_eq!(stub.flags, 0);
  assert_eq!(stub.serialization_phase, 0);
  assert!(!stub.is_flushed());
}

/// span 长度不足时拒绝（C# Debug.Assert 的显式化）
#[test]
fn create_index_rejects_short_span() {
  let mut span = [0u8; RANGE_INDEX_STUB_SIZE - 1];
  let err =
    RangeIndexManagerIndex::create_index(TreeTuning::default(), DISK, 0, &mut span).unwrap_err();
  assert!(err.to_string().contains("value span too small"));
}

/// 非存根 span 读返回 None（C# 由调用方保证长度的 Option 化）
#[test]
fn read_index_returns_none_for_non_stub_span() {
  assert!(RangeIndexManagerIndex::read_index(&[]).is_none());
  assert!(RangeIndexManagerIndex::read_index(&[0u8; 8]).is_none());
}

/// ClearFlushedFlag 只翻转 Flushed 位，其余字段不受影响
#[test]
fn clear_flushed_flag_flips_only_that_bit() {
  let mut span = [0u8; 64];
  RangeIndexManagerIndex::create_index(tuning(1, 8, 64, 16, 512), DISK, 7, &mut span).unwrap();
  // 先置 Flushed（引擎 slice 原语），确认读回为真
  RangeIndexStub::slice_set_flushed(&mut span, true).unwrap();
  assert!(
    RangeIndexManagerIndex::read_index(&span)
      .unwrap()
      .is_flushed()
  );

  RangeIndexManagerIndex::clear_flushed_flag(&mut span).unwrap();
  let stub = RangeIndexManagerIndex::read_index(&span).unwrap();
  assert!(!stub.is_flushed());
  // 其余字段不受影响
  assert_eq!(stub.tree_handle, 7);
  assert_eq!(stub.max_key_len, 16);

  // 短span拒绝
  let mut short = [0u8; 4];
  assert!(RangeIndexManagerIndex::clear_flushed_flag(&mut short).is_err());
}

// ======================== Locking 分片（删除独占锁） ========================

fn engine() -> (tempfile::TempDir, Arc<RangeIndexManager>) {
  let dir = tempdir().unwrap();
  let e = Arc::new(RangeIndexManager::new(dir.path().join("ri"), dir.path().join("cpr")).unwrap());
  (dir, e)
}

/// 持锁期间他线程同条带写锁被互斥，释放后可获取
#[test]
fn exclusive_lock_serializes_cross_thread() {
  let (_dir, engine) = engine();
  let key_hash = RangeIndexManager::key_hash_of(b"del-key");

  {
    let _guard = RangeIndexManagerLocking::acquire_exclusive_for_delete(&engine, key_hash);
    // 持锁期间，他线程无法取得同条带写锁（100ms 内未获锁即证明互斥）
    let engine2 = Arc::clone(&engine);
    let (tx, rx) = mpsc::channel();
    let h = thread::spawn(move || {
      let g = RangeIndexManagerLocking::acquire_exclusive_for_delete(&engine2, key_hash);
      tx.send(()).unwrap();
      drop(g);
    });
    assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
    // 释放本线程锁后他线程应能获取
    drop(_guard);
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    h.join().unwrap();
  }
}

/// acquire_exclusive_for_key 派生同哈希命中同条带
#[test]
fn acquire_exclusive_for_key_derives_same_stripe() {
  let (_dir, engine) = engine();
  let g1 = RangeIndexManagerLocking::acquire_exclusive_for_delete(
    &engine,
    RangeIndexManager::key_hash_of(b"k"),
  );
  let engine2 = Arc::clone(&engine);
  let (tx, rx) = mpsc::channel();
  let h = thread::spawn(move || {
    let g2 = RangeIndexManagerLocking::acquire_exclusive_for_key(&engine2, b"k");
    tx.send(()).unwrap();
    drop(g2);
  });
  // g1 占用时，acquire_exclusive_for_key 派生同哈希并命中同条带，必被互斥阻塞
  assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
  drop(g1);
  rx.recv_timeout(Duration::from_secs(5)).unwrap();
  h.join().unwrap();
}
