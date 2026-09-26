//! 事务键锁表集成测试（每引擎实例一张表、取放闩按主桶下标进出，
//! 锁位由 windex 哈希桶 entry word 高位承载——对标 C# `HashBucket` 桶头闩位域）
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/BasicLockTests.cs + libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/TsavoriteLockTable.cs

use std::{thread, time::Duration};

use wtxn::{LockType, TxnKeyEntries, TxnLockTable};

#[test]
fn shared_latches_coexist_exclusive_excludes() {
  let table = TxnLockTable::new();
  let b = table.bucket_index_for_hash(1);
  assert!(table.try_lock_shared(b));
  assert!(
    table.try_lock_shared(b),
    "共享闩必须可叠加（对标 C# 同桶多共享持有者）"
  );
  assert!(!table.try_lock_exclusive(b), "共享闩持有期间独占闩必须被拒");
  table.unlock_shared(b);
  assert!(
    !table.try_lock_exclusive(b),
    "仍有共享闩持有时独占闩必须被拒"
  );
  table.unlock_shared(b);
  assert!(table.try_lock_exclusive(b), "共享闩全释放后独占闩必须可取");
  table.unlock_exclusive(b);
  assert!(
    table.try_lock_exclusive(b),
    "释放后桶必须回到空闲态（无闩位泄漏）"
  );
  table.unlock_exclusive(b);
}

#[test]
fn distinct_buckets_do_not_conflict() {
  let table = TxnLockTable::new();
  let b0 = table.bucket_index_for_hash(0);
  let b1 = table.bucket_index_for_hash(1);
  assert_ne!(b0, b1, "0/1 哈希须落在不同主桶");
  assert!(table.try_lock_exclusive(b0));
  assert!(table.try_lock_exclusive(b1), "不同主桶不得互相阻塞");
  table.unlock_exclusive(b1);
  table.unlock_exclusive(b0);
}

/// 句柄克隆共享同一锁面（对标 C# `struct OverflowBucketLockTable` 持 store 引用）；
/// 跨线程搬运无需手写 `Send`/`Sync`
#[test]
fn latch_contention_is_visible_across_threads() {
  fn assert_send_sync<T: Send + Sync>(_: &T) {}

  let table = TxnLockTable::new();
  assert_send_sync(&table);
  let b = table.bucket_index_for_hash(5);
  assert!(table.try_lock_exclusive(b));
  let probe = table.clone();
  let handle = thread::spawn(move || {
    assert!(
      !probe.try_lock_exclusive(b),
      "同实例锁表的跨线程持闩必须可见"
    );
    assert!(!probe.try_lock_shared(b), "独占闩持有期间共享闩亦必须被拒");
  });
  handle.join().expect("子线程不 panic");
  table.unlock_exclusive(b);
}

/// 实例隔离回归：两张锁表互不影响，同表内同键事务互斥
/// （原实现为进程级全局单例，此断言在改前必然失败）
#[test]
fn lock_tables_are_isolated_per_instance() {
  const KEY: i64 = 7;

  let instance_a = TxnLockTable::new();
  let instance_b = TxnLockTable::new();

  let mut holder = TxnKeyEntries::new(4, instance_a.clone());
  holder.add_key(KEY, LockType::Exclusive);
  holder.lock_all_keys();

  let mut other_instance = TxnKeyEntries::new(4, instance_b.clone());
  other_instance.add_key(KEY, LockType::Exclusive);
  assert!(
    other_instance.try_lock_all_keys(Duration::from_millis(1)),
    "另一引擎实例的锁表不得被本实例持闩阻塞"
  );
  other_instance.unlock_all_keys();

  let mut same_instance = TxnKeyEntries::new(4, instance_a.clone());
  same_instance.add_key(KEY, LockType::Exclusive);
  assert!(
    !same_instance.try_lock_all_keys(Duration::from_millis(1)),
    "同实例内并发事务不得穿透锁表"
  );

  holder.unlock_all_keys();
  assert!(
    same_instance.try_lock_all_keys(Duration::from_millis(1)),
    "同实例持闩者释放后必须可取"
  );
  same_instance.unlock_all_keys();
}

/// 闩位不泄漏：持锁者未显式解锁即析构时由 `TxnKeyEntries::Drop` 兜底解绑
#[test]
fn dropped_entries_release_held_latches() {
  const KEY: i64 = 11;
  let table = TxnLockTable::new();
  let bucket = table.bucket_index_for_hash(KEY);
  {
    let mut holder = TxnKeyEntries::new(4, table.clone());
    holder.add_key(KEY, LockType::Exclusive);
    holder.lock_all_keys();
    assert!(
      !table.try_lock_exclusive(bucket),
      "持锁期间同桶独占闩必须被拒"
    );
  }
  assert!(table.try_lock_exclusive(bucket), "丢弃持锁者不得泄漏闩位");
  table.unlock_exclusive(bucket);
}
