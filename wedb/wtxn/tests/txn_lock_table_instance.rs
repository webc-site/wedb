//! 锁表实例归属集成测试（对标 C# 锁表随 store 实例创建、会话自所属 store 取表：
//! Tsavorite.cs:105 字段声明、Tsavorite.cs:228 `LockTable = new OverflowBucketLockTable(this)`、
//! SessionFunctionsWrapper.cs:30 `LockTable => _clientSession.store.LockTable`）
//!
//! 覆盖两点：
//! - 两个引擎实例（两套事务管理器、两张锁表）并存时锁互不干扰：A 实例持某键
//!   排他锁期间，B 实例同键事务照常取锁并提交；
//! - 同一实例内（同句柄派生的两会话）冲突仍被正确检测：占用方未释放时竞争方
//!   快速失败，且失败方已取前缀闩按逆序回滚，不留残锁。
//!
//! 原实现锁面为 txn_key_entry.rs 的进程级 `GLOBAL_LOCK_TABLE`，上述第一点
//! 在该实现下必然失败（跨库假共享），故本文件即回归护栏。
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/BasicLockTests.cs + libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/TsavoriteLockTable.cs

use std::{sync::Arc, time::Duration};

use wtxn::{
  LockType, TransactionManager, TxnKeyEntries, TxnKeyEntryComparison, TxnLockTable, TxnState,
  WatchVersionMap,
};
use wval::SessionPrefixBuf;

/// 编译期口径：锁面结构与事务管理器天然 `Send + Sync`
/// （不再依赖手写的 `unsafe impl Send/Sync`——全部成员均为按构造可搬运类型）
fn assert_send_sync<T: Send + Sync>() {}

/// 以指定锁表句柄构造事务管理器（同句柄 = 同一引擎实例锁面，
/// 对标 C# 同一 store 的各会话取同一 `store.LockTable`）
fn manager_on(table: TxnLockTable) -> TransactionManager {
  TransactionManager::new(table, Arc::new(WatchVersionMap::new(64)), None)
}

#[test]
fn lock_faces_are_thread_safe_by_derivation() {
  assert_send_sync::<TxnLockTable>();
  assert_send_sync::<TxnKeyEntries>();
  assert_send_sync::<TransactionManager>();
}

/// 键哈希所在主桶（与事务侧同一位面：`TxnKeyEntryComparison::scoped_key_hash` →
/// `TxnLockTable::bucket_index_for_hash`，即 `hash & size_mask`）
fn bucket_of(table: &TxnLockTable, key: &[u8]) -> usize {
  table.bucket_index_for_hash(TxnKeyEntryComparison::scoped_key_hash(
    SessionPrefixBuf::ROOT.as_slice(),
    key,
  ))
}

/// 两实例并存：A 持某键排他锁期间，B 同键照常加锁并提交，互不阻塞
#[test]
fn second_instance_commits_same_key_while_first_holds_lock() {
  const KEY: &[u8] = b"cross-instance-key";

  let table_a = TxnLockTable::new();
  let table_b = TxnLockTable::new();
  // 两实例结构同构，同一键映射到同一主桶下标：隔离性只能由「表归实例持有」
  // 保证（各自独立 HashIndex 桶内存），而非哈希错位侥幸
  let bucket_a = bucket_of(&table_a, KEY);
  let bucket_b = bucket_of(&table_b, KEY);
  assert_eq!(bucket_a, bucket_b, "同构实例同键应映射同主桶下标");

  let mut txn_a = manager_on(table_a.clone());
  txn_a.save_key_entry_to_lock(SessionPrefixBuf::ROOT.as_slice(), KEY, LockType::Exclusive);
  assert!(
    txn_a.run(
      SessionPrefixBuf::ROOT.as_slice(),
      true,
      false,
      Duration::ZERO
    ),
    "A 实例自身加锁应成功"
  );
  assert!(
    !table_a.try_lock_exclusive(bucket_a),
    "A 实例持锁期间本实例主桶必须持闩"
  );

  // B 实例同键：完整事务三段式（加锁 → 提交 → 复位）不得被 A 干扰
  let mut txn_b = manager_on(table_b.clone());
  txn_b.save_key_entry_to_lock(SessionPrefixBuf::ROOT.as_slice(), KEY, LockType::Exclusive);
  assert!(
    txn_b.run(
      SessionPrefixBuf::ROOT.as_slice(),
      true,
      true,
      Duration::from_millis(1)
    ),
    "B 实例同键事务不得被 A 实例持锁阻塞"
  );
  assert_eq!(txn_b.state, TxnState::Running);
  txn_b.commit(false).unwrap();
  assert_eq!(txn_b.state, TxnState::None, "B 实例应正常提交收尾");
  assert!(
    table_b.try_lock_exclusive(bucket_b),
    "B 实例提交后本实例主桶必须空闲"
  );
  table_b.unlock_exclusive(bucket_b);

  // A 实例锁面不受 B 影响：仍持闩，且自身提交路径照常
  assert!(
    !table_a.try_lock_exclusive(bucket_a),
    "另一实例的提交不得释放本实例闩位"
  );
  txn_a.commit(false).unwrap();
  assert!(table_a.try_lock_exclusive(bucket_a), "A 提交后主桶必须空闲");
  table_a.unlock_exclusive(bucket_a);
}

/// 同实例两会话：占用未释放时竞争方快速失败，释放后方可通过
#[test]
fn same_instance_sessions_still_conflict() {
  const KEY: &[u8] = b"intra-instance-key";

  let table = TxnLockTable::new();
  let bucket = bucket_of(&table, KEY);

  let mut holder = manager_on(table.clone());
  holder.save_key_entry_to_lock(SessionPrefixBuf::ROOT.as_slice(), KEY, LockType::Exclusive);
  assert!(holder.run(
    SessionPrefixBuf::ROOT.as_slice(),
    true,
    false,
    Duration::ZERO
  ));

  let mut contender = manager_on(table.clone());
  contender.save_key_entry_to_lock(SessionPrefixBuf::ROOT.as_slice(), KEY, LockType::Exclusive);
  assert!(
    !contender.run(
      SessionPrefixBuf::ROOT.as_slice(),
      true,
      true,
      Duration::from_millis(5)
    ),
    "同实例内并发事务必须被锁表拦下"
  );
  assert_eq!(contender.state, TxnState::None, "锁失败路径须复位事务状态");
  assert!(
    !table.try_lock_exclusive(bucket),
    "竞争失败不得误放持有者闩位"
  );

  holder.commit(false).unwrap();
  assert!(
    contender.run(
      SessionPrefixBuf::ROOT.as_slice(),
      true,
      true,
      Duration::from_millis(5)
    ),
    "持有者释放后竞争方必须可通过"
  );
  contender.commit(false).unwrap();
  assert!(table.try_lock_exclusive(bucket), "双方提交后主桶必须空闲");
  table.unlock_exclusive(bucket);
}

/// 部分失败回滚：多键计划中后段主桶被占时，前缀已取闩必须逆序释放
/// （对标 C# `DoTransactionalUnlock(keys[..keyIdx])` 的回滚义务；
/// 无守卫闩形态下漏放即永久残锁，故单列断言）
#[test]
fn partial_acquire_rolls_back_prefix_latches() {
  // 两个显式哈希分属不同主桶（hash & 1023），且低桶先被加锁（计划按桶升序）
  const LOW: i64 = 0;
  const HIGH: i64 = 1;

  let table = TxnLockTable::new();
  let low_bucket = table.bucket_index_for_hash(LOW);
  let high_bucket = table.bucket_index_for_hash(HIGH);
  assert_ne!(low_bucket, high_bucket, "用例前提：两键分属不同主桶");

  let mut blocker = TxnKeyEntries::new(1, table.clone());
  blocker.add_key(HIGH, LockType::Exclusive);
  blocker.lock_all_keys();

  let mut contender = TxnKeyEntries::new(2, table.clone());
  contender.add_key(LOW, LockType::Exclusive);
  contender.add_key(HIGH, LockType::Exclusive);
  assert!(
    !contender.try_lock_all_keys(Duration::from_millis(5)),
    "后段主桶被占，整计划必须失败"
  );
  assert!(
    !table.try_lock_exclusive(high_bucket),
    "阻塞者闩位不得被竞争失败误放"
  );

  // 前缀回滚后低主桶可被他人正常获取（回滚语义即由该探针验证）
  let mut probe = TxnKeyEntries::new(1, table.clone());
  probe.add_key(LOW, LockType::Exclusive);
  assert!(probe.try_lock_all_keys(Duration::from_millis(1)));
  probe.unlock_all_keys();

  blocker.unlock_all_keys();
  assert!(contender.try_lock_all_keys(Duration::from_millis(1)));
  contender.unlock_all_keys();
}

/// 句柄克隆共享同一锁面、不同实例句柄彼此独立（对标 C# 会话取同一
/// `store.LockTable` 引用而非各自新建）
#[test]
fn cloned_handle_shares_one_lock_face_and_new_handle_is_isolated() {
  let table = TxnLockTable::new();
  let shared = table.clone();
  let bucket = table.bucket_index_for_hash(1234);

  assert!(table.try_lock_shared(bucket));
  assert!(
    !shared.try_lock_exclusive(bucket),
    "克隆句柄必须指向同一索引的同一主桶"
  );
  table.unlock_shared(bucket);
  assert!(
    shared.try_lock_exclusive(bucket),
    "释放后克隆句柄视角下主桶必须空闲"
  );
  shared.unlock_exclusive(bucket);

  let other = TxnLockTable::new();
  assert!(other.try_lock_exclusive(bucket), "新实例句柄自带独立锁面");
  other.unlock_exclusive(bucket);
}
