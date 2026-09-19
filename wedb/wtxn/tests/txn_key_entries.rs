use std::{cmp::Ordering, thread, time::Duration};

use wtxn::{LockType, TxnKeyEntries, TxnKeyEntry, TxnKeyEntryComparison, TxnLockTable};

/// 在同一引擎实例锁表上构造加锁集合
fn entries(table: &TxnLockTable, pairs: &[(i64, LockType)]) -> TxnKeyEntries {
  let mut e = TxnKeyEntries::new(4, table.clone());
  for &(hash, ty) in pairs {
    e.add_key(hash, ty);
  }
  e
}

#[test]
fn compare_orders_by_hash_then_lock_strength() {
  let index = TxnLockTable::new().pin();
  let a = TxnKeyEntry::new(1, LockType::Shared);
  let b = TxnKeyEntry::new(2, LockType::Shared);
  let x = TxnKeyEntry::new(3, LockType::Exclusive);
  let s = TxnKeyEntry::new(3, LockType::Shared);
  assert_eq!(
    TxnKeyEntryComparison::compare(index.as_ref(), &a, &b),
    Ordering::Less
  );
  assert_eq!(
    TxnKeyEntryComparison::compare(index.as_ref(), &x, &s),
    Ordering::Less
  );
  assert_eq!(
    TxnKeyEntryComparison::compare(index.as_ref(), &s, &x),
    Ordering::Greater
  );
}

#[test]
fn lock_and_unlock_roundtrip() {
  let table = TxnLockTable::new();
  let mut e = entries(&table, &[(1, LockType::Exclusive), (2, LockType::Shared)]);
  e.lock_all_keys();
  assert!(e.count() > 0);
  e.unlock_all_keys();
  assert_eq!(e.count(), 0);
}

#[test]
fn try_lock_contention_fails_and_releases() {
  const KEY_A: i64 = 9;
  const KEY_B: i64 = 1 << 21;

  let table = TxnLockTable::new();
  let mut holder = entries(&table, &[(KEY_A, LockType::Exclusive)]);
  holder.lock_all_keys();

  // 竞争桶被占：本轮取闩失败，已取前缀逆序回滚后整计划重试至超时
  let mut contender = entries(
    &table,
    &[(KEY_A, LockType::Exclusive), (KEY_B, LockType::Shared)],
  );
  assert!(!contender.try_lock_all_keys(Duration::from_millis(5)));
  // 未被争的桶必须立即可取（失败回滚未泄漏前缀闩）
  let mut probe = entries(&table, &[(KEY_B, LockType::Exclusive)]);
  assert!(probe.try_lock_all_keys(Duration::from_millis(5)));
  probe.unlock_all_keys();

  holder.unlock_all_keys();
  assert!(contender.try_lock_all_keys(Duration::from_millis(5)));
}

/// 跨桶交错键集：排序归并保证全局取闩序一致，双向竞争不死锁
#[test]
fn crossing_bucket_orders_lock_without_deadlock() {
  const SETS: [&[(i64, LockType)]; 2] = [
    &[
      (0x3FF0_0000, LockType::Exclusive),
      (0x4000_0000, LockType::Exclusive),
    ],
    &[
      (0x0000_0000, LockType::Exclusive),
      (0x7FF0_0000, LockType::Exclusive),
    ],
  ];
  let table = TxnLockTable::new();
  let handles: Vec<_> = SETS
    .into_iter()
    .map(|set| {
      let table = table.clone();
      thread::spawn(move || {
        for _ in 0..64 {
          let mut e = entries(&table, set);
          e.lock_all_keys();
          e.unlock_all_keys();
        }
      })
    })
    .collect();
  for handle in handles {
    handle.join().expect("竞争事务不得死锁");
  }
  // 收尾全表空闲：排序加锁 + 逆序释放不得遗留闩位（逐桶可重取即无残留）
  let index = table.pin();
  for bucket in 0..index.size {
    assert!(table.try_lock_exclusive(bucket), "桶 {bucket} 残留闩位");
    table.unlock_exclusive(bucket);
  }
}

#[test]
fn lockset_string_matches_csharp_shape() {
  let table = TxnLockTable::new();
  let e = entries(&table, &[(-3, LockType::Exclusive), (5, LockType::Shared)]);
  assert_eq!(e.get_lockset(), "-3:x5:s (phase: none))");
}
