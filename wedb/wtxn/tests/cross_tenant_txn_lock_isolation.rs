//! 跨租户/跨库事务键锁域隔离回归
//!（票 task/todo/wtxn-lock-keys-bare-key-cross-tenant-contention.md）
//!
//! C# 对位：多库隔离由 MultiDatabaseManager 每库独立 Tsavorite 实例与独立
//! `OverflowBucketLockTable` 承载（libs/server/Databases/MultiDatabaseManager.cs），
//! 事务键登记与加锁（libs/server/Transaction/TxnKeyManager.cs:LockKeys /
//! SaveKeyEntryToLock、libs/server/Transaction/TxnKeyEntry.cs:AddKey）天然库内
//! 闭环，同名键跨库零锁争用。
//!
//! rust 单物理引擎共享全服一张锁面（doc/zh/db.md 前缀刚性隔离），锁哈希必须
//! 自带会话物理前缀 `[NsVarint][DbVarint]` 归属维度
//! （[`TxnKeyEntryComparison::scoped_key_hash`] 全仓唯一构造口）：跨租户/跨库
//! 同名键离散到不同主桶，杜绝桶闩假性互斥；同域同名键同桶互斥语义保持不变。
//!
//! 自研依据: doc/zh/db.md 命名空间多租户前缀隔离（跨租户锁域互不干扰）

use std::{
  sync::{Arc, Barrier},
  thread,
  time::Duration,
};

use wbase::store_type::StoreType;
use wtxn::{
  LockType, TransactionManager, TxnCommandKeys, TxnKeyEntries, TxnKeyEntryComparison, TxnKeySpec,
  TxnLockTable, WatchVersionMap,
};
use wtxn_test::MockTxnSession;
use wval::SessionPrefixBuf;

/// 根域会话前缀
fn root() -> SessionPrefixBuf {
  SessionPrefixBuf::ROOT
}

/// 归属域键哈希（与锁登记同位面：`scoped_key_hash` 单点构造）
fn scoped(prefix: &SessionPrefixBuf, key: &[u8]) -> i64 {
  TxnKeyEntryComparison::scoped_key_hash(prefix.as_slice(), key)
}

/// 哈希所在主桶（默认锁表 1024 桶，`hash & size_mask`）
fn bucket_of(table: &TxnLockTable, hash: i64) -> usize {
  table.bucket_index_for_hash(hash)
}

/// 以指定锁表句柄构造事务管理器（同句柄 = 同一引擎实例锁面）
fn manager_on(table: &TxnLockTable) -> TransactionManager {
  TransactionManager::new(table.clone(), Arc::new(WatchVersionMap::new(64)), None)
}

/// 不同命名空间 / 数据库下同名键的键哈希与主桶下标互不相同
///
/// C# 每库独立锁表下同名键物理隔离的语义投影：哈希经前缀种子域正交
/// （fast_hash 固定种子，跨进程确定性），锁面桶定位随哈希离散，租户
/// (ns 1, db 0) 与 (ns 2, db 0)、(ns 0, db 1) 互不共桶。
#[test]
fn scoped_hash_splits_same_key_across_domains() {
  const KEY: &[u8] = b"key1";
  let domains = [
    SessionPrefixBuf::new(0, 0),
    SessionPrefixBuf::new(1, 0),
    SessionPrefixBuf::new(2, 0),
    SessionPrefixBuf::new(0, 1),
  ];
  let table = TxnLockTable::new();

  // 同域重复派生恒等（确定性哈希，锁登记与 WATCH 版本表同槽前提）
  for domain in &domains {
    assert_eq!(scoped(domain, KEY), scoped(domain, KEY));
  }

  // 跨域同名键：哈希与主桶下标两两互异（C# 独立锁表的隔离语义投影）
  for (i, domain_a) in domains.iter().enumerate() {
    for domain_b in &domains[i + 1..] {
      let hash_a = scoped(domain_a, KEY);
      let hash_b = scoped(domain_b, KEY);
      assert_ne!(hash_a, hash_b, "不同归属域同名键 {KEY:?} 哈希必须正交");
      assert_ne!(
        bucket_of(&table, hash_a),
        bucket_of(&table, hash_b),
        "不同归属域同名键 {KEY:?} 必须离散到不同主桶"
      );
    }
  }
}

/// 并发两租户事务排他锁定同名键：双方均立即成功，无跨租户桶闩互斥
#[test]
fn concurrent_exclusive_locks_same_key_across_tenants_immediately_succeed() {
  const KEY: &[u8] = b"key1";
  let table = TxnLockTable::new();
  let tenant_a = SessionPrefixBuf::new(1, 0);
  let tenant_b = SessionPrefixBuf::new(2, 0);
  assert_ne!(
    bucket_of(&table, scoped(&tenant_a, KEY)),
    bucket_of(&table, scoped(&tenant_b, KEY)),
    "跨租户同名键必须不同主桶，桶闩互不争用"
  );

  // 双租户同刻并发取锁：barrier 定序同刻入闸，各自单次尝试必须立即成功
  let barrier = Arc::new(Barrier::new(2));
  let arm = |prefix: SessionPrefixBuf| {
    let table = table.clone();
    let barrier = Arc::clone(&barrier);
    thread::spawn(move || {
      let mut entries = TxnKeyEntries::new(1, table);
      entries.add_key(scoped(&prefix, KEY), LockType::Exclusive);
      barrier.wait();
      assert!(
        entries.try_lock_all_keys_once(),
        "租户 {prefix:?} 排他锁同名键 {KEY:?} 不得被他租户阻塞"
      );
      entries.unlock_all_keys();
    })
  };
  let handle_a = arm(tenant_a);
  let handle_b = arm(tenant_b);
  handle_a.join().expect("租户 A 锁线程不得 panic");
  handle_b.join().expect("租户 B 锁线程不得 panic");

  // 同域对照：同租户同名键第二事务必须被拦（证明隔离来自归属域离散，
  // 而非锁机制失效的侥幸）
  let mut holder = manager_on(&table);
  holder.save_key_entry_to_lock(tenant_a.as_slice(), KEY, LockType::Exclusive);
  assert!(holder.run(tenant_a.as_slice(), true, false, Duration::ZERO));

  let mut contender = manager_on(&table);
  contender.save_key_entry_to_lock(tenant_a.as_slice(), KEY, LockType::Exclusive);
  assert!(
    !contender.run(tenant_a.as_slice(), true, true, Duration::from_millis(1)),
    "同租户同名键并发事务必须被桶闩拦下"
  );

  // 跨租户旁证：另一租户同名键事务在同一持锁窗口内照常取锁并提交
  let mut outsider = manager_on(&table);
  outsider.save_key_entry_to_lock(tenant_b.as_slice(), KEY, LockType::Exclusive);
  assert!(
    outsider.run(tenant_b.as_slice(), true, true, Duration::from_millis(1)),
    "他租户持锁不得阻塞本租户同名键事务"
  );
  outsider.commit(false).unwrap();
  contender.commit(false).unwrap();
  holder.commit(false).unwrap();
}

/// lock_keys 会话链路按会话前缀分域登记：同名键在不同会话前缀下产出互异哈希，
/// 且与 scoped_key_hash 直算值严格一致（登记链路与比较器单点同源）
#[test]
fn lock_keys_partitions_key_hashes_by_session_prefix() {
  let table = TxnLockTable::new();
  let keys = TxnCommandKeys {
    store_type: StoreType::Main,
    key_specs: vec![TxnKeySpec::new(0, 0, 1, false)],
  };

  let sessions = [
    MockTxnSession::with_args(&[b"key1"]),
    MockTxnSession::with_args_and_prefix(&[b"key1"], SessionPrefixBuf::new(1, 0)),
    MockTxnSession::with_args_and_prefix(&[b"key1"], SessionPrefixBuf::new(0, 1)),
  ];
  let registered: Vec<Vec<i64>> = sessions
    .iter()
    .map(|session| {
      let mut txn = manager_on(&table);
      txn.lock_keys(session, &keys);
      txn.key_entries.key_hashes().collect()
    })
    .collect();

  // 缺省会话 = 根域；各域登记哈希与直算一致（链路透传无漂移）
  assert_eq!(registered[0], vec![scoped(&root(), b"key1")]);
  assert_eq!(
    registered[1],
    vec![scoped(&SessionPrefixBuf::new(1, 0), b"key1")]
  );
  assert_eq!(
    registered[2],
    vec![scoped(&SessionPrefixBuf::new(0, 1), b"key1")]
  );

  // 跨域同名键登记哈希互不相同（ lock_keys 逐键携带会话前缀）
  assert_ne!(registered[0], registered[1]);
  assert_ne!(registered[0], registered[2]);
  assert_ne!(registered[1], registered[2]);
}
