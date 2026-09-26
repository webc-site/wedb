//! Scan 模式死亡判定通道：业务谓词、墓碑、TTL 过期、孤儿 TTL 各覆盖一例，
//! 并附 TTL 存活正对照
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/test.hlog/MoreLogCompactionTests.cs（Scan 模式阶段 2 提前跳出）

use std::{
  convert::Infallible,
  io::Error,
  result::Result,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
};

use aok::{OK, Void};
use tempfile::tempdir;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wcompact::{CompactionFunctions, CompactionType, LogCompactor};

use super::support::{FixtureCompactionFunctions, FixtureSession, FixtureStore, str_key, ttl_key};

/// 用户谓词业务过滤（命中 kill: 前缀判死，模拟宿主注入的自定义谓词）
struct KillPrefixCompactionFunctions;

impl CompactionFunctions<FixtureStore> for KillPrefixCompactionFunctions {
  type Error = Infallible;

  async fn is_deleted(
    &self,
    _session: &FixtureSession,
    key: &[u8],
    _val: &[u8],
    _now: i64,
  ) -> bool {
    // 物理键解出用户键后按前缀判死
    key
      .strip_prefix(b"str:")
      .is_some_and(|user| user.starts_with(b"kill:"))
  }
}

/// 判定用户谓词通道：谓词命中键被判死丢弃并清理索引，未命中键照常迁移
#[compio::test]
async fn scan_channel_user_predicate() -> Void {
  let dir = tempdir()?;
  let store = FixtureStore::open(dir.path().join("predicate.db"))?;
  let s = store.session()?;

  let keep = str_key(b"keep:me");
  let kill = str_key(b"kill:me");
  store.put(&s, &keep, b"survivor").await?;
  store.put(&s, &kill, b"victim").await?;

  let tail = store.hlog.tail_address();
  store.seal_read_only(tail);

  let compactor = LogCompactor::new(Arc::clone(&store));
  let stats = compactor
    .compact_with_filter(tail, CompactionType::Scan, &KillPrefixCompactionFunctions)
    .await?;

  assert_eq!(stats.scanned_records, 2, "两条记录均须被扫描");
  assert_eq!(stats.dead_dropped, 1, "谓词命中键必须判死");
  assert_eq!(stats.live_copied, 1, "未命中键必须迁移");
  assert_eq!(store.get(&s, &kill).await?, None, "谓词判死键必须不可见");
  assert!(
    store.index.lookup_candidates(&kill).is_empty(),
    "谓词判死键的索引引用必须清理"
  );
  assert_eq!(
    store.get(&s, &keep).await?.as_deref(),
    Some(b"survivor".as_slice())
  );

  OK
}

/// 墓碑通道：同键的存活记录被区间内墓碑取代（弃迁），墓碑自身判死丢弃
#[compio::test]
async fn scan_channel_tombstone() -> Void {
  let dir = tempdir()?;
  let store = FixtureStore::open(dir.path().join("tombstone.db"))?;
  let s = store.session()?;

  let k = str_key(b"gone");
  store.put(&s, &k, b"doomed").await?;
  store.del(&s, &k).await?;

  let tail = store.hlog.tail_address();
  store.seal_read_only(tail);

  let compactor = LogCompactor::new(Arc::clone(&store));
  let stats = compactor
    .compact_with_filter(tail, CompactionType::Scan, &FixtureCompactionFunctions)
    .await?;

  assert_eq!(stats.scanned_records, 2);
  // 同键新版本（墓碑）在区间内出现：旧候选计为弃迁；墓碑自身判死丢弃
  assert_eq!(stats.superseded, 1, "被墓碑取代的旧版本必须弃迁: {stats:?}");
  assert_eq!(stats.dead_dropped, 1, "墓碑必须判死丢弃");
  assert_eq!(stats.live_copied, 0);
  assert_eq!(store.get(&s, &k).await?, None);
  assert!(
    store.index.lookup_candidates(&k).is_empty(),
    "墓碑键的索引引用必须清理"
  );

  OK
}

/// TTL 过期通道：数据记录与过期 TTL 记录双双判死（数据记录经宿主 TTL 探测淘汰）
#[compio::test]
async fn scan_channel_ttl_expired() -> Void {
  let dir = tempdir()?;
  let store = FixtureStore::open(dir.path().join("ttl_expired.db"))?;
  let s = store.session()?;

  let now = now_ticks();
  let k = str_key(b"expiring");
  store.put(&s, &k, b"payload").await?;
  store
    .put_ttl(&s, b"expiring", now - TICKS_PER_SECOND)
    .await?;

  let tail = store.hlog.tail_address();
  store.seal_read_only(tail);

  let compactor = LogCompactor::new(Arc::clone(&store));
  let stats = compactor
    .compact_with_filter(tail, CompactionType::Scan, &FixtureCompactionFunctions)
    .await?;

  assert_eq!(stats.scanned_records, 2);
  assert_eq!(
    stats.dead_dropped, 2,
    "数据记录与过期 TTL 记录均判死: {stats:?}"
  );
  assert_eq!(stats.live_copied, 0);
  assert_eq!(
    store.get(&s, &k).await?,
    None,
    "过期 TTL 的数据键必须不可见"
  );
  assert!(
    store
      .index
      .lookup_candidates(&ttl_key(b"expiring"))
      .is_empty(),
    "过期 TTL 记录的索引引用必须清理"
  );

  OK
}

/// 孤儿 TTL 通道：宿主主键已不存在的 TTL 记录判死丢弃（绝不迁移）
#[compio::test]
async fn scan_channel_orphan_ttl() -> Void {
  let dir = tempdir()?;
  let store = FixtureStore::open(dir.path().join("orphan_ttl.db"))?;
  let s = store.session()?;

  // 只写 TTL 记录、不写宿主主键：构成无主孤儿
  let now = now_ticks();
  store
    .put_ttl(&s, b"orphan", now + TICKS_PER_SECOND * 60)
    .await?;

  let tail = store.hlog.tail_address();
  store.seal_read_only(tail);

  let compactor = LogCompactor::new(Arc::clone(&store));
  let stats = compactor
    .compact_with_filter(tail, CompactionType::Scan, &FixtureCompactionFunctions)
    .await?;

  assert_eq!(stats.scanned_records, 1);
  assert_eq!(
    stats.dead_dropped, 1,
    "无主孤儿 TTL 记录必须判死: {stats:?}"
  );
  assert_eq!(stats.live_copied, 0);
  assert!(
    store
      .index
      .lookup_candidates(&ttl_key(b"orphan"))
      .is_empty(),
    "孤儿 TTL 的索引引用必须清理"
  );

  OK
}

/// TTL 存活正对照：未过期 TTL 及其宿主主键全部判活迁移，TTL 记录逐字节保真
#[compio::test]
async fn scan_channel_alive_ttl_survives() -> Void {
  let dir = tempdir()?;
  let store = FixtureStore::open(dir.path().join("ttl_alive.db"))?;
  let s = store.session()?;

  let now = now_ticks();
  let expiry = now + TICKS_PER_SECOND * 60;
  let k = str_key(b"living");
  store.put(&s, &k, b"payload").await?;
  store.put_ttl(&s, b"living", expiry).await?;

  let tail = store.hlog.tail_address();
  store.seal_read_only(tail);

  let compactor = LogCompactor::new(Arc::clone(&store));
  let stats = compactor
    .compact_with_filter(tail, CompactionType::Scan, &FixtureCompactionFunctions)
    .await?;

  assert_eq!(
    stats.live_copied, 2,
    "宿主主键与存活 TTL 记录均须迁移: {stats:?}"
  );
  assert_eq!(stats.dead_dropped, 0);
  assert_eq!(
    store.get(&s, &k).await?.as_deref(),
    Some(b"payload".as_slice())
  );

  // TTL 记录迁移后载荷保真
  let ttl_candidates = store.index.lookup_candidates(&ttl_key(b"living"));
  assert_eq!(ttl_candidates.len(), 1, "存活 TTL 记录必须恰有一个候选");
  let rec = store
    .hlog
    .read_record(ttl_candidates.first().expect("非空"))
    .await?;
  assert_eq!(
    rec.value()?,
    expiry.to_be_bytes().as_slice(),
    "TTL 到期戳必须逐字节保真"
  );

  OK
}

struct ScanFailDropCompactionFunctions {
  fail: AtomicBool,
}

impl CompactionFunctions<FixtureStore> for ScanFailDropCompactionFunctions {
  type Error = Error;

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
      Err(Error::other("mock on_dropped failure"))
    } else {
      Ok(())
    }
  }
}

/// Scan 模式判死清退失败臂：on_dropped 失败时跳过摘槽、截断点回退至记录边界，记录保留至下轮
#[compio::test]
async fn scan_drop_dead_failure_retains_slot_and_clamps_truncation() -> Void {
  let dir = tempdir()?;
  let store = FixtureStore::open(dir.path().join("scan_drop_fail.db"))?;
  let s = store.session()?;

  let k_dead = b"dead:k1";
  let k_live = b"live:k2";
  let dead_addr = store.put(&s, k_dead, b"v_dead").await?;
  let _live_addr = store.put(&s, k_live, b"v_live").await?;

  let tail = store.hlog.tail_address();
  store.seal_read_only(tail);

  let cf = ScanFailDropCompactionFunctions {
    fail: AtomicBool::new(true),
  };
  let compactor = LogCompactor::new(Arc::clone(&store));
  let stats = compactor
    .compact_with_filter(tail, CompactionType::Scan, &cf)
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
    .compact_with_filter(tail, CompactionType::Scan, &cf)
    .await?;

  assert_eq!(stats2.dead_dropped, 1, "重试成功必须计入 dead_dropped");
  assert_eq!(stats2.retained, 0, "重试成功不得计入 retained");
  assert!(
    store.index.lookup_candidates(k_dead).is_empty(),
    "清退成功后索引槽位必须被摘除"
  );

  OK
}
