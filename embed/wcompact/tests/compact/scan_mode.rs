//! Scan 模式四通道死亡判定：用户谓词、墓碑、TTL 过期、孤儿 TTL 各覆盖一例，
//! 并附 TTL 存活正对照

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wbase::time::now_ms;
use wcompact::{CompactionType, LogCompactor};
use wval::NamespaceDbCodec;

use super::support::{FixtureStore, str_key, ttl_key};

/// 判定用户谓词通道：谓词命中键被判死丢弃并清理索引，未命中键照常迁移
#[test]
fn scan_channel_user_predicate() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
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
      .compact_with_filter(tail, CompactionType::Scan, |key, _| {
        // 物理键解出用户键后按前缀判死
        NamespaceDbCodec::decode_tagged_key(key)
          .is_ok_and(|(_, _, _, user)| user.starts_with(b"kill:"))
      })
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

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 墓碑通道：同键的存活记录被区间内墓碑取代（弃迁），墓碑自身判死丢弃
#[test]
fn scan_channel_tombstone() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let store = FixtureStore::open(dir.path().join("tombstone.db"))?;
    let s = store.session()?;

    let k = str_key(b"gone");
    store.put(&s, &k, b"doomed").await?;
    store.del(&s, &k).await?;

    let tail = store.hlog.tail_address();
    store.seal_read_only(tail);

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor.compact(tail, CompactionType::Scan).await?;

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

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// TTL 过期通道：数据记录与过期 TTL 记录双双判死（数据记录经宿主 TTL 探测淘汰）
#[test]
fn scan_channel_ttl_expired() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let store = FixtureStore::open(dir.path().join("ttl_expired.db"))?;
    let s = store.session()?;

    let now = now_ms();
    let k = str_key(b"expiring");
    store.put(&s, &k, b"payload").await?;
    store.put_ttl(&s, b"expiring", now - 1000).await?;

    let tail = store.hlog.tail_address();
    store.seal_read_only(tail);

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor.compact(tail, CompactionType::Scan).await?;

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

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 孤儿 TTL 通道：宿主主键已不存在的 TTL 记录判死丢弃（绝不迁移）
#[test]
fn scan_channel_orphan_ttl() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let store = FixtureStore::open(dir.path().join("orphan_ttl.db"))?;
    let s = store.session()?;

    // 只写 TTL 记录、不写宿主主键：构成无主孤儿
    let now = now_ms();
    store.put_ttl(&s, b"orphan", now + 60_000).await?;

    let tail = store.hlog.tail_address();
    store.seal_read_only(tail);

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor.compact(tail, CompactionType::Scan).await?;

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

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// TTL 存活正对照：未过期 TTL 及其宿主主键全部判活迁移，TTL 记录逐字节保真
#[test]
fn scan_channel_alive_ttl_survives() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let store = FixtureStore::open(dir.path().join("ttl_alive.db"))?;
    let s = store.session()?;

    let now = now_ms();
    let expiry = now + 60_000;
    let k = str_key(b"living");
    store.put(&s, &k, b"payload").await?;
    store.put_ttl(&s, b"living", expiry).await?;

    let tail = store.hlog.tail_address();
    store.seal_read_only(tail);

    let compactor = LogCompactor::new(Arc::clone(&store));
    let stats = compactor.compact(tail, CompactionType::Scan).await?;

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

    aok::Result::<()>::Ok(())
  })?;
  OK
}
