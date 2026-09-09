//! keyspace 统计集成测试（对标 Garnet GetKeyspaceStats / INFO KEYSPACE）
//!
//! 覆盖：无 TTL 键计数、有 TTL 未过期计入 expireCount、已过期键两栏均不计、
//! 删除后的键不计、同键多版本去重、集合键经元记录计数、多 ns/db 隔离计数，
//! 以及全量驱逐至磁盘冷区后统计口径不变。

use std::{sync::Arc, time::Duration};

use aok::{OK, Void};
use compio::{runtime::Runtime, time::sleep};
use tempfile::{TempDir, tempdir};
use wbase::time::now_ms;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, TtlOpt, WedbStore};
use wval::{CollectionType, MetaValue};

/// 构造独立临时库（4KB 页 / 16 页，GC 关闭避免后台物理删除干扰断言）
async fn open_store(tag: &str) -> aok::Result<(TempDir, Arc<WedbStore<SegmentedDevice>>)> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("keyspace_{tag}.db")),
  )?);
  let mut config = StoreConfig::new(1024, 4096, 16, 0.5)?;
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device)?);
  Ok((dir, store))
}

/// 测试 1: 基础计数语义——无 TTL 键计入 keyCount、有 TTL 未过期计入
/// expireCount、删除后的键不计、同键多版本只计一次
#[test]
fn test_keyspace_basic_counts() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_store("basic").await?;
    let session = store.new_session()?;

    // 空库：两栏均为 0
    assert_eq!(store.keyspace_stats().await?, (0, 0));

    // 无 TTL 键：计入 keyCount，不计入 expireCount
    for k in ["k1", "k2", "k3"] {
      session.upsert(k.as_bytes(), b"v").await?;
    }
    assert_eq!(store.keyspace_stats().await?, (3, 0));

    // 有 TTL 未过期：同时计入两栏
    assert_eq!(
      session
        .expire_at(b"k2", now_ms() + 60_000, TtlOpt::NONE)
        .await?,
      1
    );
    assert_eq!(store.keyspace_stats().await?, (3, 1));

    // 同键覆盖写产生多个日志版本，统计仍只计一次
    session.upsert(b"k1", b"v2").await?;
    session.upsert(b"k1", b"v3").await?;
    assert_eq!(store.keyspace_stats().await?, (3, 1));

    // 删除后的键不计（墓碑屏蔽历史版本）
    assert!(session.delete(b"k3").await?);
    assert_eq!(store.keyspace_stats().await?, (2, 1));

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 2: 已过期键两栏均不计（GC 关闭、过期记录未物理清除时同样排除）
#[test]
fn test_keyspace_expired_not_counted() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_store("expired").await?;
    let session = store.new_session()?;

    session.upsert(b"dead", b"v").await?;
    session.upsert(b"alive", b"v").await?;
    assert_eq!(
      session
        .expire_at(b"dead", now_ms() + 50, TtlOpt::NONE)
        .await?,
      1
    );
    sleep(Duration::from_millis(120)).await;

    // 过期记录仍在日志中，统计须排除：存活 1 个且无 TTL
    assert_eq!(store.keyspace_stats().await?, (1, 0));

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 3: 集合键经元记录计数（size > 0），幽灵元记录（size = 0）不计，
/// 集合 TTL 计入 expireCount
#[test]
fn test_keyspace_collections() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_store("collections").await?;
    let session = store.new_session()?;

    // 存活集合键（size = 2）计 1 个，无 TTL
    let meta = MetaValue::new(1, CollectionType::Hash, 0, 2);
    session.save_meta(b"coll", &meta).await?;
    assert_eq!(store.keyspace_stats().await?, (1, 0));

    // 集合键设 TTL 计入 expireCount
    assert_eq!(
      session
        .expire_at(b"coll", now_ms() + 60_000, TtlOpt::NONE)
        .await?,
      1
    );
    assert_eq!(store.keyspace_stats().await?, (1, 1));

    // 幽灵元记录（size = 0）不计入 keyCount
    let ghost = MetaValue::new(2, CollectionType::Hash, 0, 0);
    let meta_k = session.session_meta_key(b"ghost");
    session.upsert_raw(&meta_k, &ghost.to_bytes()).await?;
    assert_eq!(store.keyspace_stats().await?, (1, 1));

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 4: 多 ns/db 隔离计数——同名用户键在不同 (ns, db) 各计一次，
/// TTL 亦按隔离槽位独立判定
#[test]
fn test_keyspace_ns_db_isolation() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_store("ns_db").await?;
    let session = store.new_session()?;

    // (ns=0, db=0): a, b
    session.upsert(b"a", b"v").await?;
    session.upsert(b"b", b"v").await?;
    // (ns=0, db=1): a, c
    session.set_active_db(1);
    session.upsert(b"a", b"v").await?;
    session.upsert(b"c", b"v").await?;
    // (ns=1, db=0): a, d
    session.set_context(1, 0);
    session.upsert(b"a", b"v").await?;
    session.upsert(b"d", b"v").await?;

    assert_eq!(store.keyspace_stats().await?, (6, 0));

    // 仅 (0,1) 的 a 设 TTL：隔离计数 +1，其余同名键不受影响
    session.set_context(0, 1);
    assert_eq!(
      session
        .expire_at(b"a", now_ms() + 60_000, TtlOpt::NONE)
        .await?,
      1
    );
    assert_eq!(store.keyspace_stats().await?, (6, 1));

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 测试 5: 全量驱逐至磁盘冷区后统计口径不变（[begin, tail) 全区间扫描覆盖磁盘段）
#[test]
fn test_keyspace_after_evict() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_store("evict").await?;
    let session = store.new_session()?;

    for i in 0..8 {
      let key = format!("k{i}");
      session.upsert(key.as_bytes(), b"value").await?;
    }
    assert_eq!(
      session
        .expire_at(b"k0", now_ms() + 60_000, TtlOpt::NONE)
        .await?,
      1
    );

    // 全量刷盘并驱逐：全部记录滑入磁盘冷区
    store.flush_and_evict_all().await?;
    assert_eq!(store.keyspace_stats().await?, (8, 1));

    aok::Result::<()>::Ok(())
  })?;
  OK
}
