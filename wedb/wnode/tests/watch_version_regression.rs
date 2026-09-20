//! WATCH 版本表写面推进回归测试
//!
//! 对标 C# 三套 Methods 写面挂点 functionsState.watchVersionMap.IncrementVersion
//!（garnet/libs/server/Storage/Functions/ 下 MainStore/UnifiedStore/ObjectStore 的
//! UpsertMethods/RMWMethods/DeleteMethods）：任意会话经 RESP 快路径
//!（BatchStoreSession 直写 wkv）或慢路径（StorageSession 降级异步）修改键后，
//! WATCH 登记的版本必须失配，EXEC 校验（TransactionManager::run 内
//! TxnWatchedKeysContainer.validate_watch_version）必须判事务失效。

use std::{sync::Arc, time::Duration};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreResult, WedbStore};
use wnode::storage::session::{
  common::ttl_sync::{del_ttl_sync, put_ttl_sync},
  storage_session::{StorageSession, version_map_watch_hook},
};
use wtest_base::test_store_config;
use wtxn::{TransactionManager, TxnKeyEntryComparison, TxnLockTable, WatchVersionMap};
use wval::KeyTag;

type TestStore = WedbStore<SegmentedDevice>;

/// 键哈希（与版本表分桶同一哈希面）
fn h(key: &[u8]) -> u64 {
  TxnKeyEntryComparison::key_hash(key) as u64
}

/// 测试环境：真实引擎 store + 共享版本表 + 引擎级写面钩子接线
fn setup() -> (Arc<TestStore>, Arc<WatchVersionMap>) {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("watch.db")).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let map = Arc::new(WatchVersionMap::new(1 << 10));
  store.set_watch_hook(version_map_watch_hook(Arc::clone(&map)));
  (store, map)
}

/// 同步面测试入口
fn with_env(f: impl FnOnce(Arc<TestStore>, Arc<WatchVersionMap>)) {
  let (store, map) = setup();
  f(store, map);
}

/// 新开登记了单键 WATCH 的事务管理器（对标 RESP WATCH 后待 EXEC 的会话）
fn watched(map: &Arc<WatchVersionMap>, key: &[u8]) -> TransactionManager {
  let mut txn = TransactionManager::new(TxnLockTable::new(), Arc::clone(map), None);
  txn.watch(key);
  txn
}

/// EXEC 校验（失效返回 false，对标 C# Run 返回 false 回 nil 数组）
fn exec(txn: &mut TransactionManager) -> bool {
  txn.run(false, false, Duration::ZERO)
}

/// 快路径 SET 必须失效他-session 的 WATCH（原缺陷：写面不推进版本表，EXEC 恒通过）
#[test]
fn fast_path_set_invalidates_watch() {
  with_env(|store, map| {
    let mut txn = watched(&map, b"wkey");
    // 他会话快路径写（RESP SET/INCR/APPEND 等同一写入口）
    let writer = store.new_session().unwrap();
    let batch = writer.enter_batch();
    batch.try_upsert_sync(b"wkey", b"v1").unwrap().unwrap();
    drop(batch);
    assert!(!exec(&mut txn), "快路径 SET 后 WATCH 事务必须失效");
    assert_ne!(map.read_version(h(b"wkey")), 0, "版本表必须已推进");
  });
}

/// 快路径 DEL 与 TTL 写/删必须推进版本表
#[test]
fn fast_path_delete_and_ttl_invalidates_watch() {
  with_env(|store, map| {
    let writer = store.new_session().unwrap();
    let batch = writer.enter_batch();

    // DEL：对齐 C# InitialDeleter 无条件推进（缺席键墓碑也计入）
    batch.try_upsert_sync(b"del:key", b"v").unwrap().unwrap();
    let mut txn = watched(&map, b"del:key");
    batch.try_delete_sync(b"del:key").unwrap().unwrap();
    assert!(!exec(&mut txn), "快路径 DEL 后 WATCH 事务必须失效");

    // EXPIRE（TTL 写）：键元数据实际变化才推进
    batch.try_upsert_sync(b"exp:key", b"v").unwrap().unwrap();
    let mut txn = watched(&map, b"exp:key");
    assert!(put_ttl_sync(&batch, b"exp:key", i64::MAX / 2).unwrap());
    assert!(!exec(&mut txn), "TTL 写后 WATCH 事务必须失效");

    // PERSIST（TTL 删）：真实删除才推进
    let mut txn = watched(&map, b"exp:key");
    assert!(del_ttl_sync(&batch, b"exp:key").unwrap());
    assert!(!exec(&mut txn), "TTL 删后 WATCH 事务必须失效");

    // 无 TTL 记录的删除为纯查找探针（零写入），对齐 InPlaceDeleter 的
    // !Modified 条件：不得推进版本（WATCH 不得被误杀）
    let before = map.read_version(h(b"fresh:key"));
    let mut txn = watched(&map, b"fresh:key");
    assert!(
      del_ttl_sync(&batch, b"fresh:key").unwrap(),
      "未命中视同闭环"
    );
    assert!(exec(&mut txn), "零写入 PERSIST 不得推进版本表");
    assert_eq!(map.read_version(h(b"fresh:key")), before);
  });
}

/// 对象信封域写（ZADD/HSET/SADD 等回写面）必须推进版本表
#[test]
fn envelope_write_invalidates_watch() {
  with_env(|store, map| {
    let mut txn = watched(&map, b"obj:key");
    let writer = store.new_session().unwrap();
    let batch = writer.enter_batch();
    batch
      .try_upsert_tag_sync(b"obj:key", KeyTag::ObjectEnvelope, b"\x00payload")
      .unwrap()
      .unwrap();
    assert!(!exec(&mut txn), "对象回写后 WATCH 事务必须失效");
  });
}

/// 慢路径异步写必须推进版本表（StorageSession.upsert_tag 降级闭环面）
#[test]
fn slow_path_upsert_invalidates_watch() {
  Runtime::new().unwrap().block_on(async {
    let (store, map) = setup();
    let session = store.new_session().unwrap();
    let batch = session.enter_batch();
    let storage = StorageSession::new(batch);
    storage
      .upsert_tag(b"slow:key", KeyTag::String, b"v")
      .await
      .unwrap();
    let mut txn = watched(&map, b"slow:key");
    storage
      .upsert_tag(b"slow:key", KeyTag::String, b"v2")
      .await
      .unwrap();
    assert!(!exec(&mut txn), "慢路径写后 WATCH 事务必须失效");
  });
}

/// 慢路径 TTL 变更（expire_at_ticks）必须推进版本表
#[test]
fn slow_path_expire_invalidates_watch() {
  Runtime::new().unwrap().block_on(async {
    let (store, map) = setup();
    let session = store.new_session().unwrap();
    let batch = session.enter_batch();
    let storage = StorageSession::new(batch);
    storage
      .upsert_tag(b"ttl:key", KeyTag::String, b"v")
      .await
      .unwrap();
    let mut txn = watched(&map, b"ttl:key");
    storage
      .expire_at_ticks(b"ttl:key", i64::MAX / 2)
      .await
      .unwrap();
    assert!(!exec(&mut txn), "慢路径 EXPIRE 后 WATCH 事务必须失效");
  });
}

/// 版本推进恰一次：无 TTL 变化的纯 SET 单次写恰 +1（杜绝快慢双计）
#[test]
fn version_advances_exactly_once_per_write() {
  with_env(|store, map| {
    let writer = store.new_session().unwrap();
    let batch = writer.enter_batch();
    batch.try_upsert_sync(b"cnt:key", b"v1").unwrap().unwrap();
    assert_eq!(map.read_version(h(b"cnt:key")), 1);
    batch.try_upsert_sync(b"cnt:key", b"v2").unwrap().unwrap();
    assert_eq!(map.read_version(h(b"cnt:key")), 2);
  });
}

/// 未挂钩子的引擎写路径零影响（OnceLock 空槽安全旁路）
#[test]
fn no_hook_write_unaffected() {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("nohook.db")).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  batch.try_upsert_sync(b"plain:key", b"v").unwrap().unwrap();
  let got = batch.try_read_sync(b"plain:key", |v| v.to_vec()).unwrap();
  assert_eq!(got, StoreResult::Success(b"v".to_vec()));
}
