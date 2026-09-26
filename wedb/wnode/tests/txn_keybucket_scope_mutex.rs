//! 事务键锁与 wkv 读改写窗 / TTL 键闩同桶互斥回归
//!（票 wtxn-wkv-keybucket-hash-scope-desync，P2）
//!
//! 缺陷形态：wtxn 事务键锁唯一经 `scoped_key_hash`（会话物理前缀种子）寻桶，
//! wkv 读改写窗与 TTL 键闩按裸 `user_key` `fast_hash` 寻桶——同一 `HashIndex`
//! 锁内存上同键落不同桶，事务态让闩前提不成立，EXEC 重放臂与非事务窗对同键
//! 两不相拦（静默丢写）。修复口径：三面统一经 [`whasher::scoped_hash`] 单点
//!（会话物理前缀种子，与 wtxn 构造口逐字同构）寻桶。
//!
//! 判据（锁位真值全走 windex 桶字，无 mock 无 sleep）：
//! 1. 同域同键：事务持锁期内非事务窗 `try` 必败（EXEC 重放让闩前提闭环为真）、
//!    TTL 键闩（EXPIRE/PERSIST 臂）锁忙上浮 `LockTimeout`；
//! 2. 反向：非事务窗持闩期内事务取锁必败（封堵非事务窗持闩期 EXEC 臂裸奔）；
//! 3. 跨租户同名键：窗×窗、事务×事务互不误拦（口径统一的消假斥收益锁定）。

use std::{sync::Arc, time::Duration};

use aok::{OK, Void};
use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use whasher::scoped_hash;
use wkv::{Error, TtlOpt, WedbStore};
use wtest_base::open_test_store;
use wtxn::{LockType, TransactionManager, TxnLockTable, WatchVersionMap};
use wval::SessionPrefixBuf;

/// 生产同构锁表：索引装载闭包直接现取 store 当前 HashIndex 版本
///（wnode `build_txn_lock_table` 的测试等价物，与 wkv 窗/TTL 同一份锁内存；
/// 无扩容屏障面，屏障用例归 wtxn 侧）
fn lock_table_on(store: &Arc<WedbStore<SegmentedDevice>>) -> TxnLockTable {
  let index_store = Arc::clone(store);
  TxnLockTable::from_loader(move || index_store.index.load_full())
}

/// 以指定锁表句柄构造事务管理器（同句柄 = 同一引擎实例锁面）
fn manager_on(table: &TxnLockTable) -> TransactionManager {
  TransactionManager::new(table.clone(), Arc::new(WatchVersionMap::new(64)), None)
}

/// 排他登记 + 线程臂取锁（`fail_fast = false`：空争用下抢占式取锁直至成功）
fn lock_exclusive(txn: &mut TransactionManager, prefix: &SessionPrefixBuf, key: &[u8]) -> bool {
  txn.save_key_entry_to_lock(prefix.as_slice(), key, LockType::Exclusive);
  txn.run(prefix.as_slice(), true, false, Duration::ZERO)
}

/// 排他登记 + 限时单次尝试取锁（争用即快速失败）
fn try_lock_exclusive(txn: &mut TransactionManager, prefix: &SessionPrefixBuf, key: &[u8]) -> bool {
  txn.save_key_entry_to_lock(prefix.as_slice(), key, LockType::Exclusive);
  txn.run(prefix.as_slice(), true, true, Duration::from_millis(1))
}

/// 判据一/二：事务×非事务窗、事务×TTL 键闩同域同键互斥双向闭环
///
/// 正向即票面危害链的消除证明：EXEC 重放臂（事务态让闩，见
/// `SessionLocking::Transactional`）的正确性前提 = 事务已在同一份锁内存同一
/// 桶上持排他闩——持锁期内非事务窗 try 必败、EXPIRE/PERSIST 键闩锁忙即为此
/// 前提的可观测闭环；反向封堵非事务窗持闩期 EXEC 臂裸奔（修复前两口径异桶
/// 双向都不相拦，静默丢写）
#[compio::test]
async fn txn_lock_excludes_window_and_ttl_key_latch_both_ways() -> Void {
  let (_dir, store) = open_test_store("txn-wkv-scope-mutex.db")?;
  let session = store.new_session()?;
  let prefix = session.session_prefix();
  let key = b"txn:wkv:mutex";
  let table = lock_table_on(&store);

  // 正向：事务持锁期内，非事务窗同键取窗必败、EXPIRE/PERSIST 键闩锁忙
  let mut txn = manager_on(&table);
  assert!(
    lock_exclusive(&mut txn, &prefix, key),
    "空争用事务取锁必成（夹具前提）"
  );
  {
    let batch = session.enter_batch();
    assert!(
      batch.try_rmw_window(key).is_none(),
      "事务持锁期非事务窗同键取窗必败（EXEC 重放让闩前提闭环）"
    );
  }
  // TTL 键闩同桶互斥：EXPIRE/PERSIST 臂锁忙上浮 LockTimeout
  assert!(
    matches!(
      session.expire_at(key, i64::MAX, TtlOpt::NONE).await,
      Err(Error::Index(_))
    ),
    "事务持锁期 EXPIRE 键闩（TTL 臂 scoped 口径）必须锁忙上浮 LockTimeout"
  );
  assert!(
    matches!(session.persist(key).await, Err(Error::Index(_))),
    "事务持锁期 PERSIST 键闩必须锁忙上浮 LockTimeout"
  );
  txn.commit(false)?;

  // 放锁后：取窗与 EXPIRE 键闩恢复可得（互斥来自同桶闩而非机制卡死）
  {
    let batch = session.enter_batch();
    assert!(batch.try_rmw_window(key).is_some(), "放锁后取窗必成");
  }
  assert!(
    session.expire_at(key, i64::MAX, TtlOpt::NONE).await.is_ok(),
    "放锁后 EXPIRE 键闩必可取（键不存在回 -2 亦为锁面可得证明）"
  );

  // 反向：非事务窗持闩期内事务限时取锁必败；退窗后取锁即成
  let batch = session.enter_batch();
  let window = batch
    .try_rmw_window(key)
    .expect("空争用取窗必成（夹具前提）");
  let mut contender = manager_on(&table);
  assert!(
    !try_lock_exclusive(&mut contender, &prefix, key),
    "非事务窗持闩期事务取锁必败（封堵 EXEC 臂裸奔）"
  );
  drop(window);
  let mut contender = manager_on(&table);
  assert!(
    try_lock_exclusive(&mut contender, &prefix, key),
    "退窗后事务取锁必成"
  );
  contender.commit(false)?;
  OK
}

/// 判据三：跨租户同名键窗×窗、事务×事务互不误拦（消假斥收益锁定，禁回退）
#[test]
fn cross_tenant_same_key_window_and_txn_no_false_mutex() -> Void {
  let (_dir, store) = open_test_store("txn-wkv-scope-cross-tenant.db")?;
  let key = b"shared:key";
  let rt = Runtime::new().unwrap();
  let (p1, p2) = rt.block_on(async {
    let s1 = store.new_session().unwrap();
    let s2 = store.new_session().unwrap();
    assert!(s1.set_context(1, 0), "租户 1 上下文物化必成");
    assert!(s2.set_context(2, 0), "租户 2 上下文物化必成");
    (s1.session_prefix(), s2.session_prefix())
  });
  let index = store.active_index();
  assert_ne!(
    index.bucket_index_for_hash(scoped_hash(p1.as_slice(), key)),
    index.bucket_index_for_hash(scoped_hash(p2.as_slice(), key)),
    "夹具前提：跨租户同名键 scoped 桶必须互异"
  );

  // 窗×窗：租户 1 持窗期内租户 2 同名键取窗必成（裸哈希口径下同桶互相误拦）
  let (w1_taken, w2_taken) = rt.block_on(async {
    let s1 = store.new_session().unwrap();
    let s2 = store.new_session().unwrap();
    s1.set_context(1, 0);
    s2.set_context(2, 0);
    let batch1 = s1.enter_batch();
    let w1 = batch1.try_rmw_window(key).is_some();
    let w2 = s2.enter_batch().try_rmw_window(key).is_some();
    (w1, w2)
  });
  assert!(w1_taken, "夹具前提：租户 1 持窗必成");
  assert!(w2_taken, "租户 1 持窗期租户 2 同名键取窗必成（窗域消假斥）");

  // 事务×事务：租户 1 持排他锁期，租户 2 同名键限时取锁必成
  let table = lock_table_on(&store);
  let mut txn1 = manager_on(&table);
  assert!(lock_exclusive(&mut txn1, &p1, key));
  let mut txn2 = manager_on(&table);
  assert!(
    try_lock_exclusive(&mut txn2, &p2, key),
    "他租户持锁不得阻塞本租户同名键事务（事务域消假斥不回退）"
  );
  txn2.commit(false)?;
  txn1.commit(false)?;
  OK
}
