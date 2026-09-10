//! 事务键条目与加锁集合（对标 libs/server/Transaction/TxnKeyEntry.cs）
//!
//! C# 的 LockAllKeys / TryLockAllKeys / UnlockAllKeys 经 UnifiedTransactionalContext
//! 在 Tsavorite 锁表上落锁；Rust 侧锁面由本域 [`TxnLockTable`] 承接：
//! 排序（[`super::txn_key_entry_comparison::TxnKeyEntryComparison`]）后按
//! 归并计划取锁——同哈希条目合并为最强锁型，守卫持有至解锁。

use std::{
  fmt,
  sync::{Arc, LazyLock},
  time::Duration,
};

use gxhash::HashMap as GxHashMap;

use super::txn_lock_table::{TxnKeyLockGuard, TxnLockTable};

/// 进程级共享锁表（C# Tsavorite 锁表挂在统一存储会话上，进程内所有
/// 事务键条目共享同一锁空间；lua 域经 TxnKeyEntries 直取本表）
static GLOBAL_LOCK_TABLE: LazyLock<Arc<TxnLockTable>> =
  LazyLock::new(|| Arc::new(TxnLockTable::new()));

/// libs/server/Transaction/TxnKeyEntry.cs:LockType
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum LockType {
  None = 0,
  Shared = 1,
  Exclusive = 2,
}

/// Entry for a key to lock and unlock in transactions
///
/// libs/server/Transaction/TxnKeyEntry.cs:TxnKeyEntry
/// （C# 9 字节显式布局 `[StructLayout(Size = 9)]` 的字段对，Rust 侧按
/// 值语义展开为 16 字节对齐结构，无跨域二进制布局需求）
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
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    // unsigned_abs：i64::MIN 的 abs() 在 debug 构建会溢出 panic
    let key_hash_sign = if self.key_hash < 0 { "-" } else { "" };
    let lock_str = match self.lock_type {
      LockType::None => "-",
      LockType::Shared => "s",
      LockType::Exclusive => "x",
    };
    write!(
      f,
      "{}{}:{}",
      key_hash_sign,
      self.key_hash.unsigned_abs(),
      lock_str
    )
  }
}

/// 归并后的加锁计划项：同一锁条带取最强锁型（C# Tsavorite 桶锁语义：
/// 同桶键共享同一锁位，桶内持排他即覆盖共享）
struct LockPlanSlot {
  key_hash: i64,
  exclusive: bool,
}

/// 事务键加锁集合（libs/server/Transaction/TxnKeyEntry.cs:TxnKeyEntries）
pub struct TxnKeyEntries {
  keys: Vec<TxnKeyEntry>,
  unified_store_key_locked: bool,
  /// 锁阶段标记（0 无 / 1 加锁中 / 2 解锁中；GetLockset 展示用）
  pub phase: i32,
  /// 已持有的锁守卫（C# Tsavorite 会话锁位的托管等价）
  held_locks: Vec<TxnKeyLockGuard>,
}

impl TxnKeyEntries {
  pub fn new(initial_count: usize) -> Self {
    Self {
      keys: Vec::with_capacity(initial_count),
      unified_store_key_locked: false,
      phase: 0,
      held_locks: Vec::new(),
    }
  }

  /// 是否全为读锁（无排他条目）
  pub fn is_read_only(&self) -> bool {
    !self.keys.iter().any(|k| k.lock_type == LockType::Exclusive)
  }

  pub fn count(&self) -> usize {
    self.keys.len()
  }

  /// 指定序号键的哈希
  ///
  /// libs/server/Transaction/TxnKeyEntry.cs:GetKeyHash
  ///
  /// 哈希即 GarnetLog 键哈希的位面（C# 注释同义），AOF 子日志路由可直接用。
  pub fn get_key_hash(&self, index: usize) -> i64 {
    self.keys[index].key_hash
  }

  /// 追加待锁键
  ///
  /// libs/server/Transaction/TxnKeyEntry.cs:AddKey
  ///
  /// C# 在此经 UnifiedTransactionalContext.GetKeyHash 计算哈希；Rust 侧
  /// 哈希由调用方（[`super::transaction_manager::TransactionManager`]）经
  /// 比较器统一计算后传入。
  pub fn add_key(&mut self, key_hash: i64, lock_type: LockType) {
    self.keys.push(TxnKeyEntry {
      key_hash,
      lock_type,
    });
  }

  /// 归并加锁计划：按键哈希 + 锁型降序排序，同条带（桶）取最强锁型；
  /// 计划按条带下标升序输出——条带集相同的任意两事务获得同一取锁全序，
  /// 消除"哈希序与条带序跨 2^30 段相反"导致的死锁窗口
  fn lock_plan(&mut self) -> Vec<LockPlanSlot> {
    self
      .keys
      .sort_by(super::txn_key_entry_comparison::TxnKeyEntryComparison::compare);
    let mut plan: Vec<LockPlanSlot> = Vec::with_capacity(self.keys.len());
    let mut slot_by_stripe: GxHashMap<usize, usize> = GxHashMap::default();
    for entry in &self.keys {
      let stripe = GLOBAL_LOCK_TABLE.stripe_index(entry.key_hash);
      let exclusive = entry.lock_type == LockType::Exclusive;
      match slot_by_stripe.get(&stripe) {
        // 同条带已有锁位：保留排他强度
        Some(&slot_idx) => plan[slot_idx].exclusive |= exclusive,
        None => {
          slot_by_stripe.insert(stripe, plan.len());
          plan.push(LockPlanSlot {
            key_hash: entry.key_hash,
            exclusive,
          });
        }
      }
    }
    plan.sort_unstable_by_key(|slot| GLOBAL_LOCK_TABLE.stripe_index(slot.key_hash));
    plan
  }

  /// 测试观测口：归并计划的条带取锁序列
  #[cfg(test)]
  fn test_lock_plan_stripes(&mut self) -> Vec<usize> {
    self
      .lock_plan()
      .iter()
      .map(|slot| GLOBAL_LOCK_TABLE.stripe_index(slot.key_hash))
      .collect()
  }

  /// 阻塞加锁全部键（libs/server/Transaction/TxnKeyEntry.cs:LockAllKeys）
  ///
  /// 排序在本方法内进行（C# 注释：须在稳定哈希表上排序并加锁）。
  pub fn lock_all_keys(&mut self) {
    self.phase = 1;
    let plan = self.lock_plan();
    if !plan.is_empty() {
      let lock_table = Arc::clone(&*GLOBAL_LOCK_TABLE);
      self.held_locks.extend(
        plan
          .into_iter()
          .map(|slot| lock_table.lock_key(slot.key_hash, slot.exclusive)),
      );
      self.unified_store_key_locked = true;
    }
    self.phase = 0;
  }

  /// 限时尝试加锁全部键（libs/server/Transaction/TxnKeyEntry.cs:TryLockAllKeys）
  ///
  /// 部分失败时自动释放已取锁并返回 false（C# TryLock 同语义）。
  pub fn try_lock_all_keys(&mut self, lock_timeout: Duration) -> bool {
    self.phase = 1;
    let plan = self.lock_plan();
    if !plan.is_empty() {
      let lock_table = Arc::clone(&*GLOBAL_LOCK_TABLE);
      for slot in plan {
        match lock_table.try_lock_key_for(slot.key_hash, slot.exclusive, lock_timeout) {
          Some(guard) => self.held_locks.push(guard),
          None => {
            // 部分失败：退还全部已取锁
            self.held_locks.clear();
            self.unified_store_key_locked = false;
            self.phase = 0;
            return false;
          }
        }
      }
      self.unified_store_key_locked = true;
    }
    self.phase = 0;
    true
  }

  /// 解锁全部键（libs/server/Transaction/TxnKeyEntry.cs:UnlockAllKeys）
  pub fn unlock_all_keys(&mut self) {
    self.phase = 2;
    if self.unified_store_key_locked && !self.keys.is_empty() {
      // 守卫 drop 即释放锁位
      self.held_locks.clear();
    }
    self.keys.clear();
    self.unified_store_key_locked = false;
    self.phase = 0;
  }

  /// 锁集展示串（慢日志 / CLIENT INFO 用）
  ///
  /// libs/server/Transaction/TxnKeyEntry.cs:GetLockset
  pub fn get_lockset(&self) -> String {
    let mut sb = String::new();
    for entry in &self.keys {
      // C# 的 delimiter 恒为空串：条目间本就无分隔符
      sb.push_str(&entry.to_string());
    }
    if !sb.is_empty() {
      let phase_str = match self.phase {
        0 => "none",
        1 => "lock",
        _ => "unlock",
      };
      // C# 插值串字面量末尾即含两个右括号（"(phase: none))"），1:1 保留
      sb.push_str(&format!(" (phase: {phase_str}))"));
    }
    sb
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::transaction::txn_key_entry_comparison::TxnKeyEntryComparison;

  fn entries(pairs: &[(i64, LockType)]) -> TxnKeyEntries {
    let mut e = TxnKeyEntries::new(4);
    for &(hash, ty) in pairs {
      e.add_key(hash, ty);
    }
    e
  }

  #[test]
  fn add_and_read_back() {
    let mut e = TxnKeyEntries::new(2);
    let hash = TxnKeyEntryComparison::key_hash(b"k");
    e.add_key(hash, LockType::Exclusive);
    assert_eq!(e.count(), 1);
    assert_eq!(e.get_key_hash(0), hash);
    assert!(!e.is_read_only());
  }

  #[test]
  fn shared_keys_are_read_only() {
    let e = entries(&[(1, LockType::Shared), (2, LockType::Shared)]);
    assert!(e.is_read_only());
  }

  #[test]
  fn lock_and_unlock_roundtrip() {
    let mut e = entries(&[(1, LockType::Exclusive), (2, LockType::Shared)]);
    e.lock_all_keys();
    assert!(e.count() > 0);
    e.unlock_all_keys();
    assert_eq!(e.count(), 0);
  }

  #[test]
  fn try_lock_contention_fails_and_releases() {
    // 条带定位取哈希高 20 位偏移：9 → 条带 0，2^21 → 条带 2（不同桶）
    const KEY_A: i64 = 9;
    const KEY_B: i64 = 1 << 21;

    let mut holder = entries(&[(KEY_A, LockType::Exclusive)]);
    holder.lock_all_keys();

    // 同批含争夺键（A）与空闲键（B）：A 争夺失败须整体回退（含未取的 B）
    let mut contender = entries(&[(KEY_A, LockType::Exclusive), (KEY_B, LockType::Shared)]);
    assert!(!contender.try_lock_all_keys(Duration::from_millis(5)));
    let mut probe = entries(&[(KEY_B, LockType::Exclusive)]);
    assert!(probe.try_lock_all_keys(Duration::from_millis(5)));
    probe.unlock_all_keys();

    holder.unlock_all_keys();
    assert!(contender.try_lock_all_keys(Duration::from_millis(5)));
  }

  #[test]
  fn duplicate_hashes_collapse_to_strongest_lock() {
    // 同哈希先共享后排他：取一次排他，不自我死锁
    let mut e = entries(&[(7, LockType::Shared), (7, LockType::Exclusive)]);
    e.lock_all_keys();
    e.unlock_all_keys();
  }

  #[test]
  fn lock_plan_is_ordered_by_stripe_not_hash() {
    // 跨 2^30 段构造"哈希序与条带序相反"：0x3FF00000 → 条带 1023，
    // 0x40000000 → 条带 0；哈希升序下 1023 在前，归序后必须条带升序
    let mut e = entries(&[
      (0x3FF0_0000, LockType::Exclusive),
      (0x4000_0000, LockType::Exclusive),
    ]);
    assert_eq!(e.test_lock_plan_stripes(), vec![0, 1023]);
  }

  #[test]
  fn crossing_stripe_orders_lock_without_deadlock() {
    use std::thread;

    // 两事务条带集同为 {0, 1023}、哈希序相反：若按哈希序取条带锁，
    // 并发反复过锁必然出现环死锁；归序后同序串行化，循环无悬停
    let sets: Vec<Vec<(i64, LockType)>> = vec![
      vec![
        (0x3FF0_0000, LockType::Exclusive),
        (0x4000_0000, LockType::Exclusive),
      ],
      vec![
        (0x0000_0000, LockType::Exclusive),
        (0x7FF0_0000, LockType::Exclusive),
      ],
    ];
    let handles: Vec<_> = sets
      .into_iter()
      .map(|set| {
        thread::spawn(move || {
          for _ in 0..64 {
            let mut e = entries(&set);
            e.lock_all_keys();
            e.unlock_all_keys();
          }
        })
      })
      .collect();
    for handle in handles {
      handle.join().expect("竞争事务不得死锁");
    }
  }

  #[test]
  fn lockset_string_matches_csharp_shape() {
    let e = entries(&[(-3, LockType::Exclusive), (5, LockType::Shared)]);
    assert_eq!(e.get_lockset(), "-3:x5:s (phase: none))");
  }
}
