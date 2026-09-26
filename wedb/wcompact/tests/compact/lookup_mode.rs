//! Lookup 模式紧缩：活键保留、死键（墓碑/被覆盖）淘汰、陈旧重复槽位清理与边界拒绝

use std::{
  io,
  result::Result,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use aok::{OK, Void};
use tempfile::tempdir;
use wcompact::{CompactionFunctions, CompactionStats, CompactionType, Error, LogCompactor};

use super::support::{FixtureSession, FixtureStore, HashIndexTestOps};

/// 常规判活迁移：覆盖旧版本弃迁、新版本复制、墓碑判死并清理索引
#[compio::test]
async fn lookup_preserves_live_and_drops_dead() -> Void {
  let dir = tempdir()?;
  let store = FixtureStore::open(dir.path().join("lookup.db"))?;
  let s = store.session()?;

  // 记录布局：k1 两代版本、k2 存活+墓碑、k3 单条存活
  let k1 = b"key:overwrite";
  let k2 = b"key:tombstoned";
  let k3 = b"key:plain";
  store.put(&s, k1, b"v1").await?;
  store.put(&s, k1, b"v2_longer_value").await?;
  store.put(&s, k2, b"doomed").await?;
  store.del(&s, k2).await?;
  store.put(&s, k3, b"kept").await?;

  let tail = store.hlog.tail_address();
  store.seal_read_only(tail);

  let compactor = LogCompactor::new(Arc::clone(&store));
  let stats = compactor.compact(tail, CompactionType::Lookup).await?;

  let expected = CompactionStats {
    scanned_records: 5,
    live_copied: 2,
    // k1 旧版本与 k2 存活记录均因索引已指向更新记录（墓碑）而弃迁
    superseded: 2,
    dead_dropped: 1,
    retained: 0,
    bytes_freed: stats.bytes_freed,
    new_begin_address: stats.new_begin_address,
  };
  assert_eq!(stats, expected, "Lookup 统计必须精确守恒: {stats:?}");

  // 终态数据校验：活键为最新值、墓碑键不可见
  assert_eq!(
    store.get(&s, k1).await?.as_deref(),
    Some(b"v2_longer_value".as_slice())
  );
  assert_eq!(store.get(&s, k2).await?, None, "墓碑键必须不可见");
  assert_eq!(
    store.get(&s, k3).await?.as_deref(),
    Some(b"kept".as_slice())
  );

  // 墓碑键的索引陈旧引用必须被同步清理
  assert!(
    store.index.lookup_candidates(k2).is_empty(),
    "判死键的索引引用必须清理"
  );

  // 紧缩点推进到日志尾部
  assert_eq!(stats.new_begin_address, tail, "紧缩区间必须推进至尾部");

  OK
}

/// 陈旧重复槽位清理：同键多候选（含指向历史版本的陈旧槽位）在判活探查中被收敛
#[compio::test]
async fn lookup_cleans_stale_duplicate_slots() -> Void {
  let dir = tempdir()?;
  let store = FixtureStore::open(dir.path().join("stale_slot.db"))?;
  let s = store.session()?;

  let k = b"key:duplicated";
  let addr_v1 = store.put(&s, k, b"v1").await?;
  let addr_v2 = store.put(&s, k, b"v2").await?;
  // 手工注入陈旧重复候选：索引同时存在指向 v1 历史版本的槽位
  store.index.insert(k, addr_v1)?;
  assert_eq!(
    store.index.lookup_candidates(k).len(),
    2,
    "注入后必须为双候选"
  );

  let tail = store.hlog.tail_address();
  store.seal_read_only(tail);

  let compactor = LogCompactor::new(Arc::clone(&store));
  let stats = compactor.compact(tail, CompactionType::Lookup).await?;
  assert_eq!(stats.scanned_records, 2);
  assert_eq!(stats.live_copied, 1, "仅最新版本复制");
  assert_eq!(stats.superseded, 1, "v1 历史版本计为弃迁");

  // 陈旧重复槽位已被清理：仅剩迁移后的唯一新地址，数据完好
  let candidates = store.index.lookup_candidates(k);
  assert_eq!(
    candidates.len(),
    1,
    "陈旧重复槽位必须收敛为单候选: {candidates:?}"
  );
  assert_eq!(store.get(&s, k).await?.as_deref(), Some(b"v2".as_slice()));
  assert!(
    !candidates.contains(addr_v1) && !candidates.contains(addr_v2),
    "迁移后候选必须指向尾部新副本"
  );

  OK
}

/// 边界拒绝：until_address 超出安全只读区必须报错；空区间返回空统计零副作用
#[compio::test]
async fn lookup_rejects_out_of_range_and_empty_range() -> Void {
  let dir = tempdir()?;
  let store = FixtureStore::open(dir.path().join("boundary.db"))?;
  let s = store.session()?;

  store.put(&s, b"key:x", b"v").await?;
  let tail = store.hlog.tail_address();
  store.seal_read_only(tail);

  let compactor = LogCompactor::new(Arc::clone(&store));
  // 越界紧缩拒绝（seal 后 safe_ro == tail，对标 C# 内核 SafeReadOnlyAddress 硬校验）
  let err = compactor.compact(tail + 1, CompactionType::Lookup).await;
  assert!(
    matches!(
      err,
      Err(Error::UntilAddressOutOfRange {
        until_address,
        safe_read_only_address,
      }) if until_address == tail + 1 && safe_read_only_address == tail
    ),
    "越界紧缩必须报 UntilAddressOutOfRange: {err:?}"
  );

  // 空区间：until == begin 直接返回空统计
  let begin = store.hlog.begin_address();
  let stats = compactor.compact(begin, CompactionType::Lookup).await?;
  assert!(stats.is_empty(), "空区间必须返回空统计: {stats:?}");
  assert_eq!(stats.new_begin_address, begin);

  OK
}

struct FailDropCompactionFunctions {
  fail: AtomicBool,
}

impl CompactionFunctions<FixtureStore> for FailDropCompactionFunctions {
  type Error = io::Error;

  async fn is_deleted(
    &self,
    _session: &FixtureSession,
    key: &[u8],
    _val: &[u8],
    _now: i64,
  ) -> bool {
    key.starts_with(b"dead:")
  }

  async fn on_dropped(&self, _session: &FixtureSession, _key: &[u8]) -> Result<(), Self::Error> {
    if self.fail.load(Ordering::Relaxed) {
      Err(io::Error::other("mock on_dropped failure"))
    } else {
      Ok(())
    }
  }
}

/// 判死清退失败臂：on_dropped 失败时跳过摘槽、截断点回退至记录边界，记录保留至下轮
#[compio::test]
async fn lookup_drop_dead_failure_retains_slot_and_clamps_truncation() -> Void {
  let dir = tempdir()?;
  let store = FixtureStore::open(dir.path().join("drop_fail.db"))?;
  let s = store.session()?;

  let k_dead = b"dead:k1";
  let k_live = b"live:k2";
  let dead_addr = store.put(&s, k_dead, b"v_dead").await?;
  let _live_addr = store.put(&s, k_live, b"v_live").await?;

  let tail = store.hlog.tail_address();
  store.seal_read_only(tail);

  let cf = FailDropCompactionFunctions {
    fail: AtomicBool::new(true),
  };
  let compactor = LogCompactor::new(Arc::clone(&store));
  let stats = compactor
    .compact_with_filter(tail, CompactionType::Lookup, &cf)
    .await?;

  // 第一轮：清退失败，跳过摘槽，截断回退至 dead_addr
  assert_eq!(stats.dead_dropped, 0, "清退失败记录不得计入 dead_dropped");
  assert_eq!(stats.retained, 1, "清退失败记录必须计入 retained");
  assert_eq!(stats.live_copied, 1, "活记录照常迁移");
  assert!(
    stats.new_begin_address <= dead_addr,
    "截断点必须回退至失败记录起始边界之前，实际 {:#x}, dead_addr {:#x}",
    stats.new_begin_address,
    dead_addr
  );
  assert!(
    !store.index.lookup_candidates(k_dead).is_empty(),
    "清退失败时索引槽位必须保留，不得摘除成悬挂槽"
  );

  // 故障解除，下一轮紧缩完成清退并推进截断
  cf.fail.store(false, Ordering::Relaxed);
  let stats2 = compactor
    .compact_with_filter(tail, CompactionType::Lookup, &cf)
    .await?;

  assert_eq!(stats2.dead_dropped, 1, "重试成功必须计入 dead_dropped");
  assert_eq!(stats2.retained, 0, "重试成功不得计入 retained");
  assert!(
    store.index.lookup_candidates(k_dead).is_empty(),
    "清退成功后索引槽位必须被摘除"
  );

  OK
}
