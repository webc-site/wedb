//! 事务键条目与加锁集合（对标 libs/server/Transaction/TxnKeyEntry.cs）
//!
//! 排序后按归并计划取锁——同主桶条目合并为最强锁型，持锁记录为桶下标序列，
//! 解锁为其逆序对偶；锁源为构造期注入的引擎实例锁表句柄，其桶闩即 windex 哈希桶
//! 内嵌闩（对标 C# `TxnKeyEntries(int, TransactionalContext)` 取会话所属 store 的锁表，
//! 持锁集合对标 C# ActiveLocks 持 HashBucketRef，rust 以钉定的 `Arc<HashIndex>` + 桶下标承接）。

use std::{fmt, sync::Arc, thread, time::Duration};

use coarsetime::Instant;
use itoa::Buffer;
use smallvec::SmallVec;
use windex::HashIndex;

use super::{txn_key_entry_comparison::TxnKeyEntryComparison, txn_lock_table::TxnLockTable};

/// libs/server/Transaction/TxnKeyEntry.cs:LockType
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum LockType {
  None = 0,
  Exclusive = 1,
  Shared = 2,
}

/// Entry for a key to lock and unlock in transactions
///
/// 在 garnet 中的相对路径:libs/server/Transaction/TxnKeyEntry.cs:TxnKeyEntry
#[derive(Debug, Clone, Copy)]
pub struct TxnKeyEntry {
  pub key_hash: i64,
  pub lock_type: LockType,
}

impl TxnKeyEntry {
  pub fn new(key_hash: i64, lock_type: LockType) -> Self {
    Self {
      key_hash,
      lock_type,
    }
  }
}

impl fmt::Display for TxnKeyEntry {
  /// 在 garnet 中的相对路径:libs/server/Transaction/TxnKeyEntry.cs:ToString
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let key_hash_sign = if self.key_hash < 0 { "-" } else { "" };
    let lock_str = match self.lock_type {
      LockType::None => "-",
      LockType::Shared => "s",
      LockType::Exclusive => "x",
    };
    let mut buf = Buffer::new();
    let num_str = buf.format(self.key_hash.unsigned_abs());
    write!(f, "{key_hash_sign}{num_str}:{lock_str}")
  }
}

/// 归并后的加锁计划项，同时是持锁期间的桶记录（纯数据，持锁状态由 windex 桶内嵌闩承载）
#[derive(Debug, Clone, Copy)]
struct LockPlanSlot {
  bucket: usize,
  exclusive: bool,
}

/// 事务键加锁集合（libs/server/Transaction/TxnKeyEntry.cs:TxnKeyEntries）
pub struct TxnKeyEntries {
  /// 所属引擎实例的锁表句柄（对标 C# 条目集持 store 事务上下文）
  lock_table: TxnLockTable,
  /// 待加锁键序列（内联 8 槽位，覆盖绝大多数常规事务，消除堆分配）
  keys: SmallVec<[TxnKeyEntry; 8]>,
  unified_store_key_locked: bool,
  /// 锁阶段标记（0 无 / 1 加锁中 / 2 解锁中；GetLockset 展示用）
  pub phase: i32,
  /// 已持有桶记录（内联 4 槽位，覆盖绝大多数事务，消除堆分配）
  held: SmallVec<[LockPlanSlot; 4]>,
  /// 本笔事务钉定的索引版本（取锁与放锁共用同一 `HashIndex`，跨 resize 不串锁、不漏闩）
  latch: Option<Arc<HashIndex>>,
}

impl TxnKeyEntries {
  /// 构造加锁集合（对标 C# `TxnKeyEntries(int initialCount, TransactionalContext)`：
  /// 锁表面由所属引擎实例的锁表句柄注入）
  pub fn new(initial_count: usize, lock_table: TxnLockTable) -> Self {
    Self {
      lock_table,
      keys: if initial_count <= 8 {
        SmallVec::new()
      } else {
        SmallVec::with_capacity(initial_count)
      },
      unified_store_key_locked: false,
      phase: 0,
      held: SmallVec::new(),
      latch: None,
    }
  }

  /// 所属引擎实例的锁表句柄
  #[inline]
  pub fn lock_table(&self) -> &TxnLockTable {
    &self.lock_table
  }

  /// 是否全为读锁（无排他条目）
  pub fn is_read_only(&self) -> bool {
    !self.keys.iter().any(|k| k.lock_type == LockType::Exclusive)
  }

  pub fn count(&self) -> usize {
    self.keys.len()
  }

  /// 待加锁键条目哈希迭代器
  #[inline]
  pub fn key_hashes(&self) -> impl Iterator<Item = i64> + '_ {
    self.keys.iter().map(|k| k.key_hash)
  }

  /// 追加待锁键
  ///
  /// 在 garnet 中的相对路径:libs/server/Transaction/TxnKeyEntry.cs:AddKey
  pub fn add_key(&mut self, key_hash: i64, lock_type: LockType) {
    self.keys.push(TxnKeyEntry {
      key_hash,
      lock_type,
    });
  }

  /// 归并加锁计划：按主桶下标与键哈希全序升序排序，并在单次线性扫描中合并同桶锁位取最强锁型
  ///
  /// `index` 为本笔事务钉定的索引版本（与 [`Self::acquire_plan`] 取锁同源，桶下标即 `hash & size_mask`）。
  fn lock_plan(&mut self, index: &HashIndex) -> SmallVec<[LockPlanSlot; 4]> {
    if self.keys.is_empty() {
      return SmallVec::new();
    }
    self
      .keys
      .sort_unstable_by(|a, b| TxnKeyEntryComparison::compare(index, a, b));

    let mut plan: SmallVec<[LockPlanSlot; 4]> = SmallVec::with_capacity(self.keys.len());
    for entry in &self.keys {
      let bucket = index.bucket_index_for_hash(entry.key_hash as u64);
      let exclusive = entry.lock_type == LockType::Exclusive;
      if let Some(last) = plan.last_mut()
        && last.bucket == bucket
      {
        last.exclusive |= exclusive;
        continue;
      }
      plan.push(LockPlanSlot { bucket, exclusive });
    }
    plan
  }

  /// 逆序释放已登记的桶闩并清空持锁记录
  /// （对标 C# TransactionalContext.cs:DoTransactionalUnlock 的
  /// 「Unlock has to be done in the reverse order of locking」反向遍历）
  fn release_held(&mut self) {
    let Some(index) = self.latch.clone() else {
      self.held.clear();
      return;
    };
    for slot in self.held.drain(..).rev() {
      if slot.exclusive {
        index.bucket(slot.bucket).unlock_exclusive();
      } else {
        index.bucket(slot.bucket).unlock_shared();
      }
    }
  }

  /// 单轮取闩（对标 C# TransactionalContext.cs:DoTransactionalLock 的键集主体：
  /// 按升序计划逐桶 try 取闩，任一桶失败即逆序回滚本轮已取前缀并返回 false）
  fn acquire_plan(&mut self, plan: &[LockPlanSlot]) -> bool {
    self.held.clear();
    let index = self
      .latch
      .clone()
      .expect("取锁前必已钉定本笔事务的索引版本");
    for slot in plan {
      let bucket = index.bucket(slot.bucket);
      let taken = if slot.exclusive {
        bucket.try_lock_exclusive()
      } else {
        bucket.try_lock_shared()
      };
      if taken {
        self.held.push(*slot);
        continue;
      }
      // C# 失败分支：DoTransactionalUnlock(keys[..keyIdx]) 后整计划重试
      self.release_held();
      return false;
    }
    true
  }

  /// 阻塞加锁全部键（libs/server/Transaction/TxnKeyEntry.cs:LockAllKeys）
  ///
  /// 对标 C# TransactionalContext.cs:Lock 的 `while (!lockAcquired)` 外层：
  /// 整计划失败已由 [`Self::acquire_plan`] 回滚，此处让权重试不持任何闩，无死锁。
  pub fn lock_all_keys(&mut self) {
    self.phase = 1;
    let index = self.lock_table.pin();
    let plan = self.lock_plan(&index);
    if !plan.is_empty() {
      self.latch = Some(index);
      while !self.acquire_plan(&plan) {
        thread::yield_now();
      }
      self.unified_store_key_locked = true;
    }
    self.phase = 0;
  }

  /// 限时尝试加锁全部键（libs/server/Transaction/TxnKeyEntry.cs:TryLockAllKeys）
  ///
  /// 对标 C# TransactionalContext.cs:TryLock(keys, timeout)：失败回滚已取闩后
  /// 在超时预算内整计划重试；`lock_timeout` 为零表单次尝试（快速失败路径）。
  pub fn try_lock_all_keys(&mut self, lock_timeout: Duration) -> bool {
    self.phase = 1;
    let index = self.lock_table.pin();
    let plan = self.lock_plan(&index);
    if !plan.is_empty() {
      self.latch = Some(index);
      let start = Instant::now();
      loop {
        if self.acquire_plan(&plan) {
          self.unified_store_key_locked = true;
          self.phase = 0;
          return true;
        }
        if lock_timeout.is_zero() || Duration::from(start.elapsed()) >= lock_timeout {
          self.unified_store_key_locked = false;
          self.latch = None;
          self.phase = 0;
          return false;
        }
        thread::yield_now();
      }
    }
    self.phase = 0;
    true
  }

  /// 解锁全部键（libs/server/Transaction/TxnKeyEntry.cs:UnlockAllKeys）
  ///
  /// 释放已持桶闩（逆序）、放钉定索引并清空锁集；未持锁时为空操作（幂等，供 Drop 复用）。
  pub fn unlock_all_keys(&mut self) {
    self.phase = 2;
    if self.unified_store_key_locked {
      self.release_held();
    }
    self.held.clear();
    self.keys.clear();
    self.latch = None;
    self.unified_store_key_locked = false;
    self.phase = 0;
  }

  /// 锁集展示串（慢日志 / CLIENT INFO 用）
  ///
  /// 在 garnet 中的相对路径:libs/server/Transaction/TxnKeyEntry.cs:GetLockset
  pub fn get_lockset(&self) -> String {
    use std::fmt::Write as _;
    if self.keys.is_empty() {
      return String::new();
    }
    let mut sb = String::with_capacity(self.keys.len() * 16 + 24);
    for entry in &self.keys {
      let _ = write!(sb, "{entry}");
    }
    let phase_str = match self.phase {
      0 => "none",
      1 => "lock",
      _ => "unlock",
    };
    let _ = write!(sb, " (phase: {phase_str}))");
    sb
  }
}

/// RAII 解绑：条目集析构即放闩（对标 C# UnlockAllKeys 的 finally 语义，
/// panic 展开亦不泄漏闩字）
impl Drop for TxnKeyEntries {
  #[inline]
  fn drop(&mut self) {
    self.unlock_all_keys();
  }
}
