//! 事务键锁表（对标 Tsavorite.core LockTable.ts 的 wkv 侧承接）
//!
//! C# 事务锁由 Tsavorite 事务上下文（TransactionalContext.Lock / TryLock /
//! Unlock）在哈希桶级读写锁上完成；wkv 未暴露事务锁面，本域自建条带化
//! 锁表：键哈希定位 2 的幂条带，共享锁取读、排他锁取写，排序加锁消除
//! 死锁。守卫由 [`crate::txn_key_entry::TxnKeyEntries`] 持有至 UnlockAllKeys。

use std::time::Duration;

use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use wbase::striped::StripedRwLock;

/// 条带数：1024 个读写锁桶（C# Tsavorite 锁表默认 64K 桶的内存适配，
/// 条带内冲突由桶级并发语义承接）
pub const STRIPE_COUNT: usize = 1 << 10;

/// 一把已持有的条带锁守卫
///
/// Send/Sync 不变量：守卫仅绑定单一条件带的读写锁，()` 占位数据无线程
/// 亲和状态；parking_lot 读写锁的释放不要求原加锁线程，跨线程转移守卫
/// 只改变"谁来 drop"，锁语义不变
#[derive(Debug)]
pub enum TxnKeyLockGuard<'a> {
  /// 共享锁守卫
  Shared(RwLockReadGuard<'a, ()>),
  /// 排他锁守卫
  Exclusive(RwLockWriteGuard<'a, ()>),
}

/// 条带化键锁表
#[derive(Debug)]
pub struct TxnLockTable {
  /// 条带读写锁
  stripes: StripedRwLock<(), STRIPE_COUNT>,
}

impl Default for TxnLockTable {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl TxnLockTable {
  /// 构造锁表
  #[inline]
  pub fn new() -> Self {
    Self {
      stripes: StripedRwLock::new(),
    }
  }

  /// 键哈希定位条带下标（取哈希位面高段，减少相邻哈希同桶）
  #[inline]
  pub const fn stripe_index_for_hash(key_hash: i64) -> usize {
    ((key_hash as u64 >> 20) as usize) & (STRIPE_COUNT - 1)
  }

  /// 键哈希定位条带下标
  #[inline]
  pub const fn stripe_index(&self, key_hash: i64) -> usize {
    Self::stripe_index_for_hash(key_hash)
  }

  /// 按条带下标阻塞获取锁；`exclusive` 决定锁型
  #[inline]
  pub fn lock_stripe(&self, stripe_idx: usize, exclusive: bool) -> TxnKeyLockGuard<'_> {
    if exclusive {
      TxnKeyLockGuard::Exclusive(self.stripes.write_at(stripe_idx))
    } else {
      TxnKeyLockGuard::Shared(self.stripes.read_at(stripe_idx))
    }
  }

  /// 按条带下标限时尝试获取锁；超时返回 None
  #[inline]
  pub fn try_lock_stripe_for(
    &self,
    stripe_idx: usize,
    exclusive: bool,
    timeout: Duration,
  ) -> Option<TxnKeyLockGuard<'_>> {
    if exclusive {
      self
        .stripes
        .try_write_at_for(stripe_idx, timeout)
        .map(TxnKeyLockGuard::Exclusive)
    } else {
      self
        .stripes
        .try_read_at_for(stripe_idx, timeout)
        .map(TxnKeyLockGuard::Shared)
    }
  }

  /// 阻塞获取一把键锁；`exclusive` 决定锁型
  #[inline]
  pub fn lock_key(&self, key_hash: i64, exclusive: bool) -> TxnKeyLockGuard<'_> {
    self.lock_stripe(Self::stripe_index_for_hash(key_hash), exclusive)
  }

  /// 限时尝试获取一把键锁；超时返回 None
  #[inline]
  pub fn try_lock_key_for(
    &self,
    key_hash: i64,
    exclusive: bool,
    timeout: Duration,
  ) -> Option<TxnKeyLockGuard<'_>> {
    self.try_lock_stripe_for(Self::stripe_index_for_hash(key_hash), exclusive, timeout)
  }

  /// 阻塞加排他锁（返回 RAII 守卫；锁释放由守卫 drop 承接）
  pub fn lock_exclusive(&self, key_hash: i64) -> TxnKeyLockGuard<'_> {
    self.lock_key(key_hash, true)
  }

  /// 阻塞加共享锁（返回 RAII 守卫；锁释放由守卫 drop 承接）
  pub fn lock_shared(&self, key_hash: i64) -> TxnKeyLockGuard<'_> {
    self.lock_key(key_hash, false)
  }
}

#[cfg(test)]
mod tests {
  use std::{sync::Arc, thread};

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
