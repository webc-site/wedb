//! 事务键比较器（对标 libs/server/Transaction/TxnKeyEntryComparison.cs:TxnKeyComparison）
//!
//! 排序键为 windex 当前索引版本下的主桶下标（`hash & size_mask`），与
//! [`crate::txn_lock_table::TxnLockTable`] 的桶定位同源，杜绝排序按 A 粒度、加锁按 B 粒度的分叉。

use std::cmp::Ordering;

use whasher::fast_hash;
use windex::HashIndex;

use super::txn_key_entry::TxnKeyEntry;

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
    fast_hash(key) as i64
  }

  /// 键哈希比较：按当前索引版本主桶下标升序的全序
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:KeyHashComparer
  ///
  /// 加锁前按主桶下标升序排序（同 C# `KeyHashComparer` 取 `size_mask` 现算桶下标），保证多事务
  /// 以相同顺序过锁表，消除死锁；同桶按锁型升序（排他在前：LockType Exclusive = 1 < Shared = 2），
  /// 使后续去重合并取最强锁型。`index` 为本笔事务钉定的索引版本，与 [`TxnLockTable::pin`] 同源。
  #[inline]
  pub fn compare(index: &HashIndex, key1: &TxnKeyEntry, key2: &TxnKeyEntry) -> Ordering {
    let bucket_a = index.bucket_index_for_hash(key1.key_hash as u64);
    let bucket_b = index.bucket_index_for_hash(key2.key_hash as u64);
    bucket_a
      .cmp(&bucket_b)
      .then_with(|| key1.key_hash.cmp(&key2.key_hash))
      .then_with(|| key1.lock_type.cmp(&key2.lock_type))
  }
}
