//! 键空间枚举与活键判定一致性集成测试（zcode-r43-keyspace 与 zcode-r3-perf-dbsize-materialize）
//!
//! 覆盖：
//! 1. 冷区 TTL 记录 Degrade 降级复判：DBSIZE/KEYS/SCAN/EXISTS/TYPE/GET/INFO KEYSPACE 七口径一致性
//! 2. KeyTag::Meta 死元记录桩判死：DBSIZE 与 KEYS 恒不计死桩
//! 3. 语义锁与流式计数：同一库态 DBSIZE == len(KEYS)，多版本覆写与去重一致
//! 4. 槽位键删除：delete_slot_keys 无过期过滤对齐 C# DeleteSlotKeysScan，has_keys_in_slots 保守探测

use std::sync::Arc;

use wbase::{convert::TICKS_PER_SECOND, hash_slot::slot_of, time::now_ticks};
use wnode::storage::session::{
  common::ttl_sync::put_ttl_sync,
  storage_session::{StorageSession, version_map_watch_hook},
};
use wtest_base::open_test_store;
use wtxn::{DEFAULT_VERSION_MAP_SIZE, WatchVersionMap};
use wval::{GarnetObjectType, MetaValue};

fn storage_session<'s, D: wdev::Device>(
  session: &'s wkv::StoreSession<D>,
) -> StorageSession<'s, D> {
  let version_map = Arc::new(WatchVersionMap::new(DEFAULT_VERSION_MAP_SIZE));
  let _ = session
    .store
    .set_watch_hook(version_map_watch_hook(Arc::clone(&version_map)));
  StorageSession::new(session.enter_batch())
}

/// 发现一：冷区 TTL 记录 Degrade 降级复判闭环，七大口径对到期键一致性
#[compio::test]
async fn test_keyspace_cold_ttl_seven_way_consistency() -> aok::Void {
  let (_dir, store) = open_test_store("cold_ttl.db")?;
  let session = store.new_session()?;
  let ss = storage_session(&session);

  // 1. 写入普通活键 k_alive 与 带过期键 k_expired
  ss.upsert_string(b"k_alive", b"v_alive").await?;
  ss.upsert_string(b"k_expired", b"v_expired").await?;

  // 设置 k_expired 的过期时间为过去（已到期）
  {
    let batch = session.enter_batch();
    put_ttl_sync(&batch, b"k_expired", now_ticks() - 10 * TICKS_PER_SECOND)?;
  }

  // 2. 推进检查点并刷盘驱逐，使 TTL 旁路记录落入冷区（触发 RecordOnDisk -> TtlGate::Degrade）
  store.flush_and_evict_all().await?;

  // 3. 不进行点读（不触发读路径惰性清除），核验七大口径完全一致：
  // 口径 1: DBSIZE
  assert_eq!(ss.db_size().await?, 1, "DBSIZE 应仅计入活键");

  // 口径 2: KEYS *
  let keys = ss.db_keys(b"*").await?;
  assert_eq!(keys, vec![b"k_alive".to_vec()], "KEYS 应仅返回活键");

  // 口径 3: SCAN 0
  let (cursor, scan_keys) = ss.scan_cursor(b"*", true, 0, 10, None).await?;
  assert_eq!(cursor, 0);
  assert_eq!(scan_keys, vec![b"k_alive".to_vec()], "SCAN 应仅扫描出活键");

  // 口径 4: EXISTS (contains_key)
  assert!(session.contains_key(b"k_alive").await?, "k_alive 存在");
  assert!(
    !session.contains_key(b"k_expired").await?,
    "k_expired 对 EXISTS 不可见"
  );

  // 口径 5: GET (read)
  assert_eq!(session.read(b"k_alive").await?, Some(b"v_alive".to_vec()));
  assert_eq!(
    session.read(b"k_expired").await?,
    None,
    "k_expired 对 GET 回 nil"
  );

  // 口径 6: TYPE (load_meta)
  assert_eq!(
    session.load_meta(b"k_expired").await?,
    None,
    "k_expired TYPE 为 none"
  );

  // 口径 7: INFO KEYSPACE (keyspace_stats)
  let stats = store.keyspace_stats(0).await?;
  assert_eq!(
    stats.iter().find(|(db, ..)| *db == 0).map(|(_, k, _)| *k),
    Some(1)
  );
  Ok(())
}

/// 发现一：KeyTag::Meta 死元记录桩（size == 0 且非 RangeIndex）判死，DBSIZE 与 KEYS 恒不计死桩
#[compio::test]
async fn test_dead_meta_stub_ignored_in_dbsize_and_keys() -> aok::Void {
  let (_dir, store) = open_test_store("dead_meta.db")?;
  let session = store.new_session()?;
  let ss = storage_session(&session);

  // 1. 注入死 Meta 桩（size == 0 且非 RangeIndex，is_live() == false）
  let dead_meta = MetaValue::new(999, GarnetObjectType::Hash, 0);
  assert!(!dead_meta.is_live(), "size==0 且非 RI 应判死");
  let dead_meta_bytes = dead_meta.to_bytes();
  let dead_meta_key = session.session_meta_key(b"dead_hash_key");
  session.upsert_raw(&dead_meta_key, &dead_meta_bytes).await?;

  // 2. 注入存活 Meta 记录（size > 0）
  let live_meta = MetaValue::new(1000, GarnetObjectType::Hash, 3);
  assert!(live_meta.is_live(), "size > 0 应判活");
  let live_meta_bytes = live_meta.to_bytes();
  let live_meta_key = session.session_meta_key(b"live_hash_key");
  session.upsert_raw(&live_meta_key, &live_meta_bytes).await?;

  // 3. 断言 DBSIZE 与 KEYS 均不计死桩，仅计活 Meta 记录
  assert_eq!(ss.db_size().await?, 1, "DBSIZE 严禁计入死 Meta 桩");
  let keys = ss.db_keys(b"*").await?;
  assert_eq!(keys, vec![b"live_hash_key".to_vec()]);

  let (cursor, scan_keys) = ss.scan_cursor(b"*", true, 0, 10, None).await?;
  assert_eq!(cursor, 0);
  assert_eq!(scan_keys, vec![b"live_hash_key".to_vec()]);
  Ok(())
}

/// 发现二与 r3-perf：同一库态 DBSIZE == len(KEYS) 语义锁与流式计数
#[compio::test]
async fn test_dbsize_equals_keys_len_semantic_lock_and_streaming() -> aok::Void {
  let (_dir, store) = open_test_store("semantic_lock.db")?;
  let session = store.new_session()?;
  let ss = storage_session(&session);

  // 1. 写入若干键，并进行多次覆写与更新（使底层日志产生多个旧版本记录）
  for i in 0..100 {
    let k = format!("key:{i}");
    ss.upsert_string(k.as_bytes(), b"v1").await?;
    // 覆写部分键
    if i % 2 == 0 {
      ss.upsert_string(k.as_bytes(), b"v2").await?;
    }
    if i % 4 == 0 {
      ss.upsert_string(k.as_bytes(), b"v3").await?;
    }
  }

  // 2. 删除部分键
  for i in 80..100 {
    let k = format!("key:{i}");
    ss.delete_string(k.as_bytes()).await?;
  }

  // 3. 设置部分键过期
  for i in 60..80 {
    let k = format!("key:{i}");
    let batch = session.enter_batch();
    put_ttl_sync(&batch, k.as_bytes(), now_ticks() - TICKS_PER_SECOND)?;
  }

  // 此时活键应为 0..60 共 60 个
  let dbsize = ss.db_size().await?;
  let keys = ss.db_keys(b"*").await?;

  // 语义锁断言：同一库态下 DBSIZE == len(KEYS) 恒成立
  assert_eq!(dbsize, keys.len(), "DBSIZE 与 len(KEYS) 必须严格相等");
  assert_eq!(dbsize, 60);

  // 4. 刷盘冷化后再次检验流式计数与一致性
  store.flush_and_evict_all().await?;
  let dbsize_cold = ss.db_size().await?;
  let keys_cold = ss.db_keys(b"*").await?;
  assert_eq!(dbsize_cold, keys_cold.len());
  assert_eq!(dbsize_cold, 60);
  Ok(())
}

/// 发现三：槽位键删除 delete_slot_keys 无过期过滤对齐 C# DeleteSlotKeysScan 语义
#[compio::test]
async fn test_delete_slot_keys_no_expiry_filter_and_has_keys_in_slots() -> aok::Void {
  let (_dir, store) = open_test_store("del_slot.db")?;
  let session = store.new_session()?;
  let ss = storage_session(&session);
  let slot = slot_of(0, 0);

  // 1. 写入 1 个活键和 1 个过期键
  ss.upsert_string(b"active_key", b"v").await?;
  ss.upsert_string(b"expired_key", b"v").await?;
  {
    let batch = session.enter_batch();
    put_ttl_sync(&batch, b"expired_key", now_ticks() - TICKS_PER_SECOND)?;
  }

  // 2. has_keys_in_slots 保守探测：到期未回收键亦计为存在（不漏报）
  assert!(
    ss.has_keys_in_slots(&[slot]).await?,
    "槽位内有键，has_keys_in_slots 应为 true"
  );

  // 3. delete_slot_keys 删除枚举无过期过滤：到期未回收键一并纳入删除与计数
  // （对齐 C# DeleteSlotKeysScan：every matched live key is deleted, including expired-but-not-yet-tombstoned records）
  let deleted = ss.delete_slot_keys(&[slot]).await?;
  assert_eq!(deleted, 2, "delete_slot_keys 应删除活键与到期键，计数为 2");

  // 4. 删除后槽位活键归零；has_keys_in_slots 因墓碑保留仍为 true（保守误报方向，对齐函数文档）
  assert_eq!(ss.count_keys_in_slot(slot).await?, 0);
  assert_eq!(ss.db_size().await?, 0);
  assert!(ss.get_keys_in_slot(slot, 10).await?.is_empty());
  assert!(
    ss.has_keys_in_slots(&[slot]).await?,
    "保留墓碑（保守误报方向，对齐函数文档）"
  );
  Ok(())
}
