//! 事务键锁表（对标 Tsavorite.core LockTable.ts 的 wkv 侧承接）
//!
//! C# 事务锁由 Tsavorite 事务上下文（TransactionalContext.Lock / TryLock /
//! Unlock）在哈希桶级读写锁上完成；wkv 未暴露事务锁面，本域自建条带化
//! 锁表：键哈希定位 2 的幂条带，共享锁取读、排他锁取写，排序加锁消除
//! 死锁。守卫由 [`crate::txn_key_entry::TxnKeyEntries`] 持有至 UnlockAllKeys。

use std::{sync::Arc, time::Duration};

use parking_lot::{
  RawRwLock, RwLock,
  lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard},
};

/// 条带数：1024 个读写锁桶（C# Tsavorite 锁表默认 64K 桶的内存适配，
/// 条带内冲突由桶级并发语义承接）
pub const STRIPE_COUNT: u64 = 1 << 10;

/// 一把已持有的条带锁守卫
pub enum TxnKeyLockGuard {
  /// 共享锁守卫
  Shared(ArcRwLockReadGuard<RawRwLock, ()>),
  /// 排他锁守卫
  Exclusive(ArcRwLockWriteGuard<RawRwLock, ()>),
}

/// 条带化键锁表
pub struct TxnLockTable {
  /// 条带数组
  stripes: Vec<Arc<RwLock<()>>>,
}

impl Default for TxnLockTable {
  fn default() -> Self {
    Self::new()
  }
}

impl TxnLockTable {
  /// 构造锁表
  pub fn new() -> Self {
    Self {
      stripes: (0..STRIPE_COUNT)
        .map(|_| Arc::new(RwLock::new(())))
        .collect(),
    }
  }

  /// 键哈希定位条带下标（取哈希位面高段，减少相邻哈希同桶）
  #[inline]
  pub fn stripe_index_for_hash(key_hash: i64) -> usize {
    ((key_hash as u64 >> 20) as usize) & (STRIPE_COUNT as usize - 1)
  }

  /// 键哈希定位条带下标
  #[inline]
  pub fn stripe_index(&self, key_hash: i64) -> usize {
    Self::stripe_index_for_hash(key_hash)
  }

  /// 键哈希定位条带
  #[inline]
  fn stripe_for(&self, key_hash: i64) -> &Arc<RwLock<()>> {
    &self.stripes[Self::stripe_index_for_hash(key_hash)]
  }

  /// 阻塞获取一把键锁；`exclusive` 决定锁型
  pub fn lock_key(&self, key_hash: i64, exclusive: bool) -> TxnKeyLockGuard {
    let stripe = self.stripe_for(key_hash).clone();
    if exclusive {
      TxnKeyLockGuard::Exclusive(stripe.write_arc())
    } else {
      TxnKeyLockGuard::Shared(stripe.read_arc())
    }
  }

  /// 限时尝试获取一把键锁；超时返回 None
  pub fn try_lock_key_for(
    &self,
    key_hash: i64,
    exclusive: bool,
    timeout: Duration,
  ) -> Option<TxnKeyLockGuard> {
    let stripe = self.stripe_for(key_hash).clone();
    if exclusive {
      stripe
        .try_write_arc_for(timeout)
        .map(TxnKeyLockGuard::Exclusive)
    } else {
      stripe
        .try_read_arc_for(timeout)
        .map(TxnKeyLockGuard::Shared)
    }
  }

  /// 阻塞加排他锁（返回 RAII 守卫；锁释放由守卫 drop 承接）
  pub fn lock_exclusive(&self, key_hash: i64) -> TxnKeyLockGuard {
    self.lock_key(key_hash, true)
  }

  /// 阻塞加共享锁（返回 RAII 守卫；锁释放由守卫 drop 承接）
  pub fn lock_shared(&self, key_hash: i64) -> TxnKeyLockGuard {
    self.lock_key(key_hash, false)
  }

  /// 带超时的尝试加排他锁
  pub fn try_lock_exclusive_for(
    &self,
    key_hash: i64,
    timeout: Duration,
  ) -> Option<TxnKeyLockGuard> {
    self.try_lock_key_for(key_hash, true, timeout)
  }

  /// 带超时的尝试加共享锁
  pub fn try_lock_shared_for(&self, key_hash: i64, timeout: Duration) -> Option<TxnKeyLockGuard> {
    self.try_lock_key_for(key_hash, false, timeout)
  }
}

#[cfg(test)]
mod tests {
  use std::thread;

  use super::*;

  #[test]
  fn shared_locks_coexist_exclusive_excludes() {
    let table = TxnLockTable::new();
    let g1 = table.lock_key(1, false);
    let g2 = table.lock_key(1, false);
    assert!(matches!(g1, TxnKeyLockGuard::Shared(_)));
    assert!(matches!(g2, TxnKeyLockGuard::Shared(_)));
    // 同条带排他不可获取
    assert!(
      table
        .try_lock_key_for(1, true, Duration::from_millis(1))
        .is_none()
    );
    drop(g1);
    drop(g2);
    assert!(
      table
        .try_lock_key_for(1, true, Duration::from_millis(1))
        .is_some()
    );
  }

  #[test]
  fn distinct_stripes_do_not_conflict() {
    let table = TxnLockTable::new();
    let _g = table.lock_key(0, true);
    assert!(
      table
        .try_lock_key_for(i64::MAX, true, Duration::from_millis(1))
        .is_some()
    );
  }

  #[test]
  fn lock_is_contention_visible_across_threads() {
    let table = Arc::new(TxnLockTable::new());
    let guard = table.lock_key(5, true);
    let t_table = Arc::clone(&table);
    let handle = thread::spawn(move || {
      assert!(
        t_table
          .try_lock_key_for(5, true, Duration::from_millis(5))
          .is_none()
      );
    });
    handle.join().expect("子线程不 panic");
    drop(guard);
  }
}
