//! 事务键比较器（对标 libs/server/Transaction/TxnKeyEntryComparison.cs:TxnKeyComparison）
//!
//! 以 gxhash 键哈希直接承接（与 [`crate::txn_lock_table::TxnLockTable`] 的条带定位共用位面）。

use std::cmp::Ordering;

use gxhash::gxhash64;

use super::{txn_key_entry::TxnKeyEntry, txn_lock_table::TxnLockTable};

/// 键哈希种子（进程内排序 / 条带定位自洽即可，无须跨进程稳定）
const KEY_HASH_SEED: i64 = 0;

/// 事务键比较器
///
/// 在 garnet 中的相对路径:libs/server/Transaction/TxnKeyEntryComparison.cs:TxnKeyComparison
pub struct TxnKeyEntryComparison;

impl TxnKeyEntryComparison {
  /// 键字节哈希（C# UnifiedTransactionalContext.GetKeyHash 的本域承接）
  ///
  /// 返回 i64 位面以对齐 C# `long keyHash`（Display 带符号形态）。
  #[inline]
  pub fn key_hash(key: &[u8]) -> i64 {
    gxhash64(key, KEY_HASH_SEED) as i64
  }

  /// 键哈希比较：排序用全序
  ///
  /// 在 garnet 中的相对路径:libs/server/Transaction/TxnKeyEntryComparison.cs:Compare
  ///
  /// 加锁前按条带下标升序、哈希升序排序，保证多事务以相同顺序过锁表，消除死锁。
  /// 等哈希按锁类型升序（排他在前：LockType Exclusive = 1 < Shared = 2），使后续去重合并取最强锁型。
  #[inline]
  pub fn compare(key1: &TxnKeyEntry, key2: &TxnKeyEntry) -> Ordering {
    let stripe_a = TxnLockTable::stripe_index_for_hash(key1.key_hash);
    let stripe_b = TxnLockTable::stripe_index_for_hash(key2.key_hash);
    stripe_a
      .cmp(&stripe_b)
      .then_with(|| key1.key_hash.cmp(&key2.key_hash))
      .then_with(|| key1.lock_type.cmp(&key2.lock_type))
  }
}

#[cfg(test)]
mod tests {
  use super::{super::txn_key_entry::LockType, *};

  #[test]
  fn compare_orders_by_hash_then_lock_strength() {
    let a = TxnKeyEntry::new(1, LockType::Shared);
    let b = TxnKeyEntry::new(2, LockType::Shared);
    let x = TxnKeyEntry::new(3, LockType::Exclusive);
    let s = TxnKeyEntry::new(3, LockType::Shared);
    assert_eq!(TxnKeyEntryComparison::compare(&a, &b), Ordering::Less);
    assert_eq!(TxnKeyEntryComparison::compare(&x, &s), Ordering::Less);
    assert_eq!(TxnKeyEntryComparison::compare(&s, &x), Ordering::Greater);
  }

  #[test]
  fn key_hash_is_signed_bitface_of_gxhash() {
    let hash = TxnKeyEntryComparison::key_hash(b"some-key");
    assert_ne!(hash, 0);
  }
}
