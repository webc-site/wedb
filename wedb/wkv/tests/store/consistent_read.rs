//! 自研依据: 区间索引快照一致读（C# 对应面 test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs 的快照扫描）
use std::sync::{
  Arc,
  atomic::{AtomicI64, AtomicUsize, Ordering},
};

use wdev::SegmentedDevice;
use wkv::{ConsistentReadFunctions, Result, StoreConfig, StoreResult, WedbStore};
use wval::{KeyTag, NamespaceDbCodec};

struct MockFunctions {
  pre_count: AtomicUsize,
  post_count: AtomicUsize,
  last_hash: AtomicI64,
  pre_batch_count: AtomicUsize,
  post_batch_count: AtomicUsize,
  retries: AtomicUsize,
}

impl ConsistentReadFunctions for MockFunctions {
  fn pre_single_key_consistent_read(&self, hash: i64) -> Result<()> {
    self.pre_count.fetch_add(1, Ordering::SeqCst);
    self.last_hash.store(hash, Ordering::SeqCst);
    Ok(())
  }

  fn post_single_key_consistent_read_callback(&self) {
    self.post_count.fetch_add(1, Ordering::SeqCst);
  }

  /// 满足 ConsistentReadCallback trait 契约，测试桩仅统计预批次调用次数
  fn pre_batch_key_consistent_read_callback(&self, _keys: &[&[u8]]) -> Result<()> {
    self.pre_batch_count.fetch_add(1, Ordering::SeqCst);
    Ok(())
  }

  /// 满足 ConsistentReadCallback trait 契约，测试桩仅按计数重试，无需按批次条数判断
  fn post_batch_key_consistent_read_callback(&self, _batch_size: usize) -> bool {
    self.post_batch_count.fetch_add(1, Ordering::SeqCst);
    if self.retries.load(Ordering::SeqCst) > 0 {
      self.retries.fetch_sub(1, Ordering::SeqCst);
      false
    } else {
      true
    }
  }
}

#[compio::test]
async fn test_consistent_read_lifecycle() {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("cr.log")).unwrap());
  let config = StoreConfig::new(64, 4096, 64, 0.5).unwrap();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  let fns = MockFunctions {
    pre_count: AtomicUsize::new(0),
    post_count: AtomicUsize::new(0),
    last_hash: AtomicI64::new(0),
    pre_batch_count: AtomicUsize::new(0),
    post_batch_count: AtomicUsize::new(0),
    retries: AtomicUsize::new(0),
  };

  let ctx = session.consistent_read(&fns);

  session.upsert(b"hello", b"world").await.unwrap();
  let val = ctx.read(b"hello").await.unwrap();
  assert_eq!(val.as_deref(), Some(b"world".as_slice()));
  assert_eq!(fns.pre_count.load(Ordering::SeqCst), 1);
  assert_eq!(fns.post_count.load(Ordering::SeqCst), 1);
  // 触发哈希域 = 记录物理键域（[NsVarint][DbVarint][KeyTag][用户键]）：
  // 与回放侧草图入账键同字节同哈希，独立编码交叉核对，并锁定与用户键
  // 直哈希（旧缺陷域）不相等
  let record_key = NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, b"hello");
  assert_eq!(
    fns.last_hash.load(Ordering::SeqCst),
    whasher::fast_hash_i64(record_key.as_slice()),
    "一致读触发哈希须落在记录物理键域"
  );
  assert_ne!(
    fns.last_hash.load(Ordering::SeqCst),
    whasher::fast_hash_i64(b"hello"),
    "用户键直哈希不再是本域（跨域错读回归哨兵）"
  );

  let sync_val = ctx.try_read_sync(b"hello", |v| v.to_vec()).unwrap();
  assert_eq!(sync_val, StoreResult::Success(b"world".to_vec()));
  assert_eq!(fns.pre_count.load(Ordering::SeqCst), 2);
  assert_eq!(fns.post_count.load(Ordering::SeqCst), 2);

  {
    let batch = session.enter_batch();
    let unprot_val = ctx
      .try_read_sync_unprotected(b"hello", |v| v.to_vec())
      .unwrap();
    assert_eq!(unprot_val, StoreResult::Success(b"world".to_vec()));
    drop(batch);
  }
  assert_eq!(fns.pre_count.load(Ordering::SeqCst), 3);
  assert_eq!(fns.post_count.load(Ordering::SeqCst), 3);

  let sync_size_val = ctx
    .try_read_tag_sync_with_size(b"hello", KeyTag::String, |v, sz| (v.to_vec(), sz))
    .unwrap();
  assert!(matches!(sync_size_val, StoreResult::Success((ref v, sz)) if v == b"world" && sz > 0));
  assert_eq!(fns.pre_count.load(Ordering::SeqCst), 4);
  assert_eq!(fns.post_count.load(Ordering::SeqCst), 4);

  let async_size_val = ctx
    .read_tag_with_size(b"hello", KeyTag::String, |v, sz| (v.to_vec(), sz))
    .await
    .unwrap();
  assert!(matches!(async_size_val, Some((ref v, sz)) if v == b"world" && sz > 0));
  assert_eq!(fns.pre_count.load(Ordering::SeqCst), 5);
  assert_eq!(fns.post_count.load(Ordering::SeqCst), 5);

  // 批量预取一致读校验（模拟 1 次重试后成功）
  session.upsert(b"k1", b"v1").await.unwrap();
  session.upsert(b"k2", b"v2").await.unwrap();
  fns.retries.store(1, Ordering::SeqCst);

  let mut collected = Vec::new();
  ctx
    .read_batch_with(&[b"k1".as_slice(), b"k2".as_slice()], |idx, opt| {
      collected.push((idx, opt.map(|v| v.to_vec())));
    })
    .await
    .unwrap();

  assert_eq!(fns.pre_batch_count.load(Ordering::SeqCst), 2);
  assert_eq!(fns.post_batch_count.load(Ordering::SeqCst), 2);
  assert_eq!(collected.len(), 2);
  assert_eq!(collected[0], (0, Some(b"v1".to_vec())));
  assert_eq!(collected[1], (1, Some(b"v2".to_vec())));
}
