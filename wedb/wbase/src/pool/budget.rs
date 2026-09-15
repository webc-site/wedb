//! 双层字节预算管理器 (对标 C# `BudgetState`)
//!
//! 基于 `AtomicI64` 实现原子配额预留与释放，支持 small/large 双层预算强隔离。

use std::sync::atomic::{
  AtomicI64,
  Ordering::{AcqRel, Acquire, Release},
};

/// 字节预算 (对标 C# `BudgetState`：CAS 预留 + 恰好一次释放)
pub(crate) struct Budget {
  total: i64,
  used: AtomicI64,
}

impl Budget {
  #[inline]
  pub(crate) const fn new(total: i64) -> Self {
    Self {
      total,
      used: AtomicI64::new(0),
    }
  }

  /// 尝试预留指定字节配额，若预算耗尽返回 false (对标 C# `BudgetState.TryReserve`)
  #[inline]
  pub(crate) fn try_reserve(&self, bytes: i64) -> bool {
    let mut cur = self.used.load(Acquire);
    loop {
      let next = cur.saturating_add(bytes);
      if next > self.total {
        return false;
      }
      match self.used.compare_exchange_weak(cur, next, AcqRel, Acquire) {
        Ok(_) => return true,
        Err(v) => cur = v,
      }
    }
  }

  /// 返还先前预留的字节配额 (对标 C# `BudgetState.Release`)
  #[inline]
  pub(crate) fn release(&self, bytes: i64) {
    self.used.fetch_sub(bytes, Release);
  }

  /// 当前已预留字节数 (对标 C# `BudgetState.Used`)
  #[inline]
  pub(crate) fn used(&self) -> i64 {
    self.used.load(Acquire)
  }

  /// 总预算上限字节数
  #[inline]
  pub(crate) const fn total(&self) -> i64 {
    self.total
  }
}
