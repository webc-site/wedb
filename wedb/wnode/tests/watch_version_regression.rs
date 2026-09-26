//! WATCH 版本表写面推进回归测试
//!
//! 对标 C# 三套 Methods 写面挂点 functionsState.watchVersionMap.IncrementVersion
//!（garnet/libs/server/Storage/Functions/ 下 MainStore/UnifiedStore/ObjectStore 的
//! UpsertMethods/RMWMethods/DeleteMethods）：任意会话经 RESP 快路径
//!（BatchStoreSession 直写 wkv）或慢路径（StorageSession 降级异步）修改键后，
//! WATCH 登记的版本必须失配，EXEC 校验（TransactionManager::run 内
//! TxnWatchedKeysContainer.validate_watch_version）必须判事务失效。

use std::{sync::Arc, time::Duration};

use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreResult, WedbStore};
use wnode::storage::session::{
  common::ttl_sync::{del_ttl_sync, put_ttl_sync},
  storage_session::{StorageSession, version_map_watch_hook},
};
use wtest_base::test_store_config;
use wtxn::{LockType, TransactionManager, TxnKeyEntryComparison, TxnLockTable, WatchVersionMap};
use wval::{KeyTag, SessionPrefixBuf};

type TestStore = WedbStore<SegmentedDevice>;

/// 键哈希（与版本表分桶同一哈希面：根域 scoped，与默认 (0,0) 写会话同源）
fn h(key: &[u8]) -> u64 {
  TxnKeyEntryComparison::scoped_key_hash(SessionPrefixBuf::ROOT.as_slice(), key) as u64
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

/// 新开登记了单键 WATCH 的事务管理器（对标 RESP WATCH 后待 EXEC 的会话；
/// 归属域 = 根 (0,0)，与本用例默认写会话前缀同源）
fn watched(map: &Arc<WatchVersionMap>, key: &[u8]) -> TransactionManager {
  let mut txn = TransactionManager::new(TxnLockTable::new(), Arc::clone(map), None);
  txn.watch(SessionPrefixBuf::ROOT.as_slice(), key);
  txn
}

/// EXEC 校验（失效返回 false，对标 C# Run 返回 false 回 nil 数组）
fn exec(txn: &mut TransactionManager) -> bool {
  txn.run(
    SessionPrefixBuf::ROOT.as_slice(),
    false,
    false,
    Duration::ZERO,
  )
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

/// 跨租户/跨库同名键经真实引擎写面互不串扰（本票核心回归，对标 C#
/// 每库独持 WatchVersionMap 的物理隔离 libs/server/GarnetDatabase.cs:156）：
/// 根域 (ns0,db0) WATCH 的会话，其 EXEC 只应被同域写入失效；异租户或异库
/// 对同一字面键的引擎写推进必须落在正交的归属域槽位上。
#[test]
fn cross_domain_write_does_not_spuriously_abort_watch() {
  with_env(|store, map| {
    // Case 1：异租户（ns9,db0）SET 同字面键 → 根域 WATCH 不得假性中止
    let mut txn = watched(&map, b"shared:key");
    let alien_ns = store.new_session().unwrap();
    assert!(alien_ns.set_context(9, 0), "异租户上下文绑定应成功");
    // 版本轨=逻辑域种子：异租户 bump 落其逻辑 (9,0) 槽（换号代际不入种子）
    let alien_prefix = alien_ns.session_logical_prefix();
    let batch = alien_ns.enter_batch();
    batch
      .try_upsert_sync(b"shared:key", b"from-ns9")
      .unwrap()
      .unwrap();
    drop(batch);
    assert!(
      exec(&mut txn),
      "跨租户同名键写入不得使根域 WATCH 事务假性中止"
    );
    assert_eq!(
      map.read_version(h(b"shared:key")),
      0,
      "根域版本槽必须未被异租户写入触碰"
    );
    assert_ne!(
      map
        .read_version(
          TxnKeyEntryComparison::scoped_key_hash(alien_prefix.as_slice(), b"shared:key") as u64
        ),
      0,
      "异租户写入必须在自己的归属域槽位真实推进（非全局旁路）"
    );

    // Case 2：同租户异库（ns0,db7）SET 同字面键 → 根域 WATCH 不得假性中止
    let mut txn = watched(&map, b"shared:key");
    let alien_db = store.new_session().unwrap();
    assert!(alien_db.set_context(0, 7), "异库上下文绑定应成功");
    // 版本轨=逻辑域种子：异库 bump 落其逻辑 (0,7) 槽
    let alien_db_prefix = alien_db.session_logical_prefix();
    let batch = alien_db.enter_batch();
    batch
      .try_upsert_sync(b"shared:key", b"from-db7")
      .unwrap()
      .unwrap();
    drop(batch);
    assert!(
      exec(&mut txn),
      "跨库同名键写入不得使根域 WATCH 事务假性中止"
    );
    assert_ne!(
      map.read_version(TxnKeyEntryComparison::scoped_key_hash(
        alien_db_prefix.as_slice(),
        b"shared:key"
      ) as u64),
      0,
      "异库写入必须在自己的归属域槽位真实推进"
    );

    // Case 3：同域（ns0,db0）SET 同字面键 → 必须准确失效（隔离不得吞掉真冲突）
    let mut txn = watched(&map, b"shared:key");
    let sibling = store.new_session().unwrap();
    let batch = sibling.enter_batch();
    batch
      .try_upsert_sync(b"shared:key", b"from-root")
      .unwrap()
      .unwrap();
    drop(batch);
    assert!(!exec(&mut txn), "同域写入必须使 WATCH 校验失配、事务中止");
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
#[compio::test]
async fn slow_path_upsert_invalidates_watch() {
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
}

/// 慢路径 TTL 变更（expire_at_ticks）必须推进版本表
#[compio::test]
async fn slow_path_expire_invalidates_watch() {
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

/// 慢路径标签删除（delete_tag 同步快路径命中）必须推进版本表
///
/// 原缺陷：`StorageSession::delete_tag` 同步快路径 Ok 分支直返，遗漏
/// bump_watch_version——底层 `try_delete_tag_sync`/`delete_raw` 均为物理键
/// 原语无用户键收口，AOF 回放 StoreDelete（ObjectEnvelope/ACL 墓碑）与业务
/// 标签删除命中内存快路径时 WATCH 无法感知脏写（对标 C# MainStore
/// DeleteMethods.InitialDeleter 无条件 IncrementVersion，DeleteMethods.cs:16）
#[compio::test]
async fn delete_tag_fast_path_invalidates_watch() {
  let (store, map) = setup();
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  // 预置带标签整值记录（AOF 回放 store_delete 同一入口面）
  storage
    .upsert_tag(b"tag:key", KeyTag::ObjectEnvelope, b"\x00payload")
    .await
    .unwrap();
  let mut txn = watched(&map, b"tag:key");
  assert!(
    storage
      .delete_tag(b"tag:key", KeyTag::ObjectEnvelope)
      .await
      .unwrap(),
    "在场记录删除应返回 true"
  );
  assert!(!exec(&mut txn), "标签删除快路径命中后 WATCH 事务必须失效");
  assert_ne!(map.read_version(h(b"tag:key")), 0, "版本表必须已推进");
}

/// 缺席键标签删除（Ok(false) 缺席观测）同样推进——对标 C# InitialDeleter
/// 无条件 IncrementVersion（缺席键墓碑追加同向计入，与快路径 DEL 口径一致）
#[compio::test]
async fn delete_tag_absent_key_still_advances_watch() {
  let (store, map) = setup();
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  let mut txn = watched(&map, b"absent:tag");
  assert!(
    !storage
      .delete_tag(b"absent:tag", KeyTag::ObjectEnvelope)
      .await
      .unwrap(),
    "缺席键删除应返回 false"
  );
  assert!(
    !exec(&mut txn),
    "缺席观测同样计入版本（C# InitialDeleter 无条件）"
  );
}

/// 标签删除版本推进恰一次（杜绝快路径命中后外围收口与底层原语双计）
#[compio::test]
async fn delete_tag_advances_exactly_once() {
  let (store, map) = setup();
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  storage
    .upsert_tag(b"tag:cnt", KeyTag::ObjectEnvelope, b"\x00payload")
    .await
    .unwrap();
  assert_eq!(map.read_version(h(b"tag:cnt")), 1);
  assert!(
    storage
      .delete_tag(b"tag:cnt", KeyTag::ObjectEnvelope)
      .await
      .unwrap()
  );
  assert_eq!(map.read_version(h(b"tag:cnt")), 2, "单次删除恰 +1");
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

/// 置换引擎通过钩子束重挂 WATCH 版本推进（发现二回归：原换机后新引擎未挂钩子致 WATCH 乐观锁失效）
#[test]
fn swap_store_hook_bundle_rebinds_watch_version() {
  let dir = tempdir().unwrap();
  let dev1 = Arc::new(SegmentedDevice::single_file(dir.path().join("base.db")).unwrap());
  let dev2 = Arc::new(SegmentedDevice::single_file(dir.path().join("swapped.db")).unwrap());
  let config = test_store_config();
  let _base_store = Arc::new(WedbStore::open(config.clone(), dev1).unwrap());
  let swapped_store = Arc::new(WedbStore::open(config, dev2).unwrap());
  let map = Arc::new(WatchVersionMap::new(1 << 10));

  // 模拟宿主钩子束
  let bundle = {
    let map = Arc::clone(&map);
    move |store: &Arc<TestStore>| {
      store.set_watch_hook(version_map_watch_hook(Arc::clone(&map)));
    }
  };

  // 换入前未挂钩子，写新引擎版本表不推进
  let s2 = swapped_store.new_session().unwrap();
  let b2 = s2.enter_batch();
  b2.try_upsert_sync(b"k1", b"v0").unwrap().unwrap();
  drop(b2);
  assert_eq!(
    map.read_version(h(b"k1")),
    0,
    "换入未挂钩子前写操作不推进版本"
  );

  // 消费钩子束重挂
  bundle(&swapped_store);

  let mut txn = watched(&map, b"k1");
  let b2 = s2.enter_batch();
  b2.try_upsert_sync(b"k1", b"v1").unwrap().unwrap();
  drop(b2);
  assert!(
    !exec(&mut txn),
    "钩子束重挂后写操作必须推进版本并使 WATCH 失效"
  );
  assert_ne!(map.read_version(h(b"k1")), 0, "版本表已推进");
}

// ═════ swapnum（FLUSHDB/FLUSHNS/FLUSHALL 换号）后版本轨/锁轨专项
//（票 task/ing/wtxn-watch-version-slot-freeze-after-swapnum.md 方案 4/5）═════

/// WATCH k → FLUSHDB → SET k v2 → EXEC 必须中止（现形反判，本票核心）：
/// 版本轨=逻辑域种子，换号只换逻辑→物理解析、不改逻辑身份——换号后同
/// 逻辑键写入的 bump 与在途 WATCH 冻结槽恒命中，复现 C# 每库版本表实例
/// 终身持有形态的「改后写必 abort」（libs/server/GarnetDatabase.cs:156）。
/// 原缺陷：WATCH 登记冻结旧代物理域种子，flush 后改写落新代槽，旧槽永久
/// 停留、EXEC 假通过（乐观锁静默丢失）。附带「槽位域不越代」断言：跨换号
/// bump 恰一次推进同一逻辑槽（非新槽从 0 起）。
#[compio::test]
async fn flushdb_then_write_aborts_inflight_watch() {
  let (store, map) = setup();
  let writer = store.new_session().unwrap();
  let batch = writer.enter_batch();
  batch.try_upsert_sync(b"swap:key", b"v1").unwrap().unwrap();
  drop(batch);

  let mut txn = watched(&map, b"swap:key");
  let before = map.read_version(h(b"swap:key"));

  // FLUSHDB：O(1) 换虚拟库号（唯一漏斗 slow.rs flush_command_slow 的存储段）
  store.flush_database(0, 0).await.unwrap();

  // 换号后同逻辑键改写：物理落新代域，版本轨 bump 落逻辑 (0,0) 冻结槽
  let batch = writer.enter_batch();
  batch.try_upsert_sync(b"swap:key", b"v2").unwrap().unwrap();
  drop(batch);

  assert!(
    !exec(&mut txn),
    "FLUSHDB 后同逻辑键写入必须使在途 WATCH 事务中止"
  );
  assert_eq!(
    map.read_version(h(b"swap:key")),
    before + 1,
    "换号 bump 必落同一逻辑域槽恰一次推进（槽位域不越代、不另建新槽）"
  );
}

/// 裸 WATCH k → FLUSHDB → EXEC 必须成功且读空（护判净 6：flush 本身零触
/// 版本表，C# DatabaseManagerBase.cs:FlushDatabase :301-310 同形——无换号
/// 后改写则不误杀在途 WATCH）
#[compio::test]
async fn bare_watch_flushdb_exec_succeeds() {
  let (store, map) = setup();
  let mut txn = watched(&map, b"bare:key");
  store.flush_database(0, 0).await.unwrap();
  assert!(
    exec(&mut txn),
    "裸 FLUSHDB（窗内无同逻辑键写）不得使 WATCH 假性中止（判净 6 形态）"
  );
  // 逻辑入口读为空：换号即空由物理换代承接，版本轨零扰动
  let reader = store.new_session().unwrap();
  let batch = reader.enter_batch();
  assert!(
    matches!(
      batch.try_read_sync(b"bare:key", |v| v.to_vec()).unwrap(),
      StoreResult::NotFound
    ),
    "flush 后逻辑键读必为空"
  );
}

/// WATCH k → FLUSHNS（整空间换号）→ SET k v2 → EXEC 必须中止（同形一例）
#[compio::test]
async fn flushns_then_write_aborts_inflight_watch() {
  let (store, map) = setup();
  let writer = store.new_session().unwrap();
  let batch = writer.enter_batch();
  batch.try_upsert_sync(b"ns:key", b"v1").unwrap().unwrap();
  drop(batch);

  let mut txn = watched(&map, b"ns:key");
  store.flush_namespace(0).await.unwrap();
  let batch = writer.enter_batch();
  batch.try_upsert_sync(b"ns:key", b"v2").unwrap().unwrap();
  drop(batch);
  assert!(
    !exec(&mut txn),
    "FLUSHNS 换空间后同逻辑键写入必须使在途 WATCH 事务中止"
  );
}

/// 裸 WATCH k → FLUSHNS → EXEC 必须成功（空间级换号同守判净 6）
#[compio::test]
async fn bare_watch_flushns_exec_succeeds() {
  let (store, map) = setup();
  let mut txn = watched(&map, b"ns:bare");
  store.flush_namespace(0).await.unwrap();
  assert!(exec(&mut txn), "裸 FLUSHNS 不得使 WATCH 假性中止");
}

/// WATCH k → FLUSHALL（全库清空 + 映射重置）→ SET k v2 → EXEC 必须中止
#[compio::test]
async fn flushall_then_write_aborts_inflight_watch() {
  let (store, map) = setup();
  let writer = store.new_session().unwrap();
  let batch = writer.enter_batch();
  batch.try_upsert_sync(b"all:key", b"v1").unwrap().unwrap();
  drop(batch);

  let mut txn = watched(&map, b"all:key");
  store.flush_all_databases().await.unwrap();
  let writer = store.new_session().unwrap();
  let batch = writer.enter_batch();
  batch.try_upsert_sync(b"all:key", b"v2").unwrap().unwrap();
  drop(batch);
  assert!(
    !exec(&mut txn),
    "FLUSHALL 后同逻辑键写入必须使在途 WATCH 事务中止"
  );
}

/// 直设物理域写臂（AOF 回放守卫 / 内置 GC / 分层降阶共用形态）显式透传
/// 逻辑域后，bump 必落版本轨逻辑槽：副本/后台在途 WATCH 同必 abort（方案 1
/// 「直设臂扩形显式携带逻辑域、禁映射反查」的透传闭环；对位键域映射经
/// version_domain_of 单点——活域正查即映射真值、死域回孤域替身）
#[compio::test]
async fn direct_set_domain_write_bumps_logical_slot() {
  let (store, map) = setup();
  let writer = store.new_session().unwrap();
  let batch = writer.enter_batch();
  batch.try_upsert_sync(b"dc:key", b"v1").unwrap().unwrap();
  drop(batch);

  let mut txn = watched(&map, b"dc:key");
  let (old_vns, old_vdb) = store.vdb.get_virtual_ids(0, 0);
  let (vns, new_vdb) = store.flush_database(0, 0).await.unwrap();

  // 直设回放形写臂：物理域直设 + 逻辑域显式携带（set_virtual_context 扩形）
  let replay = store.new_session().unwrap();
  replay.set_virtual_context(vns, new_vdb, 0, 0);
  let batch = replay.enter_batch();
  batch.try_upsert_sync(b"dc:key", b"v2").unwrap().unwrap();
  drop(batch);
  assert!(
    !exec(&mut txn),
    "直设臂写入的 bump 必落逻辑域冻结槽，在途 WATCH 中止"
  );
  // 换算单点两态：活域正查即映射真值；换号退役死域回物理对孤域替身
  assert_eq!(store.vdb.version_domain_of(vns, new_vdb), (0, 0));
  assert_eq!(
    store.vdb.version_domain_of(old_vns, old_vdb),
    (old_vns, old_vdb)
  );
}

/// 锁轨=物理域现算（方案 2）：WATCH 后 FLUSHDB 换代，EXEC 并入锁集按
/// **当前**物理前缀现算落新代桶——他会话在新代桶持排他锁时必须拦下本
/// EXEC（假失配反证：消费登记期冻结 hash 则锁旧代空桶、假通过提交，
/// 换号后锁面死旧代桶残面/假性失互斥两面俱灭）；版本轨零推进以隔离变量
#[compio::test]
async fn swapnum_exec_locks_current_physical_domain() {
  let (store, map) = setup();
  let table = TxnLockTable::new();

  // T1 在 WATCH 时刻登记（逻辑轨种子），窗内无任何写入
  let mut watcher = TransactionManager::new(table.clone(), Arc::clone(&map), None);
  watcher.watch(SessionPrefixBuf::ROOT.as_slice(), b"lock:track");

  // FLUSHDB 换代：新代物理前缀 ≠ 登记时刻物理投影
  let (vns, new_vdb) = store.flush_database(0, 0).await.unwrap();
  let phys_new = SessionPrefixBuf::new(vns, new_vdb);

  // T2 按当前物理域持同字面键排他锁（模拟换号后同域并发事务）
  let mut contender = TransactionManager::new(table, Arc::clone(&map), None);
  contender.save_key_entry_to_lock(phys_new.as_slice(), b"lock:track", LockType::Exclusive);
  assert!(
    contender.run(phys_new.as_slice(), true, true, Duration::ZERO),
    "T2 新代域锁登记应无障碍取得"
  );

  // T1 EXEC：WATCH 键并锁现算落新代同桶 → 必须被拦（快速失败复位）
  assert!(
    !watcher.run(phys_new.as_slice(), false, true, Duration::ZERO),
    "锁轨=物理域现算：EXEC 并入锁必撞新代在持桶闩，不得假通过"
  );
  contender.commit(false).unwrap();
  // 释放后 T1 可正常起步（版本轨未被 T2 锁触，无假 abort）
  assert!(
    watcher.run(phys_new.as_slice(), false, true, Duration::ZERO),
    "闩释放后 EXEC 应通过（锁轨拦截非版本轨误杀）"
  );
  watcher.commit(false).unwrap();
}

/// 改判臂 DEL 型换号后语义如常（票验证点 5 六型之「DEL」）：WATCH k（旧代
/// 登记）→ FLUSHDB → 新代域标签删除（缺席键，C# InitialDeleter 无条件推进
/// DeleteMethods.cs:16 口径）→ EXEC 必须中止。原缺陷形态下删除 bump 落新代
/// 物理槽、冻结旧槽永不受触而假通过；修复后换号后删除臂与在途 WATCH 恒共
/// 逻辑槽——缺席与否均计入，正合 C# 无条件推进语义
#[compio::test]
async fn flushdb_then_delete_aborts_inflight_watch() {
  let (store, map) = setup();
  let writer = store.new_session().unwrap();
  let batch = writer.enter_batch();
  batch.try_upsert_sync(b"dels:key", b"v1").unwrap().unwrap();
  drop(batch);

  let mut txn = watched(&map, b"dels:key");
  let before = map.read_version(h(b"dels:key"));

  store.flush_database(0, 0).await.unwrap();

  // 换号后新代域删除臂（键已随换代不存在，仍无条件推进逻辑槽）
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  assert!(
    !storage
      .delete_tag(b"dels:key", KeyTag::String)
      .await
      .unwrap(),
    "换号后缺席删除返回 false 视同闭环"
  );
  assert!(
    !exec(&mut txn),
    "FLUSHDB 后删除臂必须使在途 WATCH 事务中止（DEL 型改判臂不冻结）"
  );
  assert_eq!(
    map.read_version(h(b"dels:key")),
    before + 1,
    "换号后删除 bump 落同一逻辑槽恰一次推进（不越代另建新槽）"
  );
}

/// 改判臂 TTL 型换号后语义如常（票验证点 5 六型之「TTL」）：旧代写入 →
/// FLUSHDB → 新代重建同逻辑键（版本轨跨代连续，槽位域不越代）→ 新代域内
/// 登记 WATCH 快照现值 → EXPIRE 写臂必须再推进同一逻辑槽使 EXEC 中止。
/// 隔离变量：窗口起点置于换号后，专钉 TTL 臂本身的换号后落槽命中，不复用
/// 换号窗内 upsert 推进
#[compio::test]
async fn flushdb_then_ttl_write_aborts_inflight_watch() {
  let (store, map) = setup();
  let writer = store.new_session().unwrap();
  let batch = writer.enter_batch();
  batch.try_upsert_sync(b"ttls:key", b"v1").unwrap().unwrap();
  drop(batch);

  store.flush_database(0, 0).await.unwrap();

  // 换号后同逻辑键重建：bump 落旧代已推进的逻辑槽（跨代连续，恰 +1）
  let before = map.read_version(h(b"ttls:key"));
  let batch = writer.enter_batch();
  batch.try_upsert_sync(b"ttls:key", b"v2").unwrap().unwrap();
  drop(batch);
  assert_eq!(
    map.read_version(h(b"ttls:key")),
    before + 1,
    "换号前后同逻辑键 bump 恒落一槽（重建写入推进不越代）"
  );

  // 新代域内 WATCH 快照现值，TTL 写臂必须命中同槽使 EXEC 中止
  let mut txn = watched(&map, b"ttls:key");
  let batch = writer.enter_batch();
  assert!(
    put_ttl_sync(&batch, b"ttls:key", i64::MAX / 2).unwrap(),
    "换号后重建键 TTL 写应命中"
  );
  drop(batch);
  assert!(
    !exec(&mut txn),
    "FLUSHDB 换代后 TTL 写臂必须使在途 WATCH 事务中止（TTL 型改判臂不冻结）"
  );
}
