//! 事务键比较器（对标 libs/server/Transaction/TxnKeyEntryComparison.cs:TxnKeyComparison）
//!
//! 排序键为 windex 当前索引版本下的主桶下标（`hash & size_mask`），与
//! [`crate::txn_lock_table::TxnLockTable`] 的桶定位同源，杜绝排序按 A 粒度、加锁按 B 粒度的分叉。

use std::cmp::Ordering;

use whasher::scoped_hash;
use windex::HashIndex;

use super::txn_key_entry::TxnKeyEntry;

/// 事务键比较器
///
/// 在 garnet 中的相对路径:libs/server/Transaction/TxnKeyEntryComparison.cs:TxnKeyComparison
pub struct TxnKeyEntryComparison;

impl TxnKeyEntryComparison {
  /// 归属域键哈希（C# UnifiedTransactionalContext.GetKeyHash 的本域承接；锁登记、
  /// WATCH 版本表分槽与写面推进全仓唯一构造口）
  ///
  /// C# 每库独持存储实例与锁表（libs/server/GarnetDatabase.cs:156 每库独持
  /// `WatchVersionMap`、MultiDatabaseManager 各库独立 `OverflowBucketLockTable`），
  /// 同名键天然域内隔离；rust 单物理引擎多租户/多库共享全服单表单锁面
  /// （doc/zh/db.md 前缀刚性隔离），键身份必须自带完整归属维度。
  ///
  /// 双轨分置（两轨经本单点构槽、前缀入参各取一域，禁共口互染）：
  /// - **版本轨=逻辑域**：写面推进（wnode `version_map_watch_hook`，前缀出自
  ///   wkv `StoreSession::session_logical_prefix`）与 WATCH 登记
  ///   （[`TxnWatchedKeysContainer::add_watch`](super::txn_watched_keys_container::TxnWatchedKeysContainer::add_watch)）
  ///   取会话逻辑前缀 `[NsVarint][DbVarint]`（ns/db 逻辑真值，不含 FLUSHDB
  ///   换号虚拟代际）——换号只换逻辑→物理解析不改逻辑身份，换号前后同逻辑键
  ///   恒落同槽，对在途 WATCH 复现 C# 改后写必 abort；
  /// - **锁轨=物理域**：事务锁登记（[`super::txn_key_manager::TransactionManager`]
  ///   锁登记链与 EXEC WATCH 键并锁现算）取会话物理前缀（`StoreSession::
  ///   session_prefix` 真值源，含换号态），与桶闩所在现域同源。
  ///
  /// 哈希构造经 [`whasher::scoped_hash`] 全仓单点（本函数为其 i64 位面视角）：
  /// 以前缀哈希为种子域混入键体（`fast_hash_with_seed(key, fast_hash(prefix))`），
  /// 不同 (ns, db) 同名键落位正交，零堆分配；返回 i64 位面以对齐 C#
  /// `long keyHash`（Display 带符号形态）。两轨消费必须且只能经本函数（底层
  /// [`whasher::scoped_hash`] 单点）构槽，杜绝第二手前缀拼接——wkv rmw 窗与
  /// TTL 键闩同经该单点寻桶（票 wtxn-wkv-keybucket-hash-scope-desync 口径
  /// 统一），三面同键同桶互斥。
  #[inline]
  pub fn scoped_key_hash(prefix: &[u8], key: &[u8]) -> i64 {
    scoped_hash(prefix, key) as i64
  }

  /// 键哈希比较：按当前索引版本主桶下标升序的全序
  ///
  /// 在 garnet 中的相对路径:libs/server/Transaction/TxnKeyEntryComparison.cs:Compare
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:KeyHashComparer
  ///
  /// 比较器 IComparer 实现方法同挂此处（C# Compare 委托 KeyHashComparer，rust 折叠单点）：
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:Compare
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
