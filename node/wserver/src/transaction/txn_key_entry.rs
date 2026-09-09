use std::{fmt, time::Duration};

/// libs/server/Transaction/TxnKeyEntry.cs:LockType
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LockType {
  None = 0,
  Shared = 1,
  Exclusive = 2,
}

/// Entry for a key to lock and unlock in transactions
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

pub struct TxnKeyEntries {
  keys: Vec<TxnKeyEntry>,
  unified_store_key_locked: bool,
  pub phase: i32,
}

impl TxnKeyEntries {
  pub fn new(initial_count: usize) -> Self {
    Self {
      keys: Vec::with_capacity(initial_count),
      unified_store_key_locked: false,
      phase: 0,
    }
  }

  pub fn is_read_only(&self) -> bool {
    !self.keys.iter().any(|k| k.lock_type == LockType::Exclusive)
  }

  pub fn count(&self) -> usize {
    self.keys.len()
  }

  /// libs/server/Transaction/TxnKeyEntry.cs:GetKeyHash
  pub fn get_key_hash(&self, index: usize) -> i64 {
    self.keys[index].key_hash
  }

  /// libs/server/Transaction/TxnKeyEntry.cs:AddKey
  pub fn add_key(&mut self, key_hash: i64, lock_type: LockType) {
    self.keys.push(TxnKeyEntry {
      key_hash,
      lock_type,
    });
  }

  pub fn lock_all_keys(&mut self) {
    self.phase = 1;
    self.keys.sort_by_key(|k| k.key_hash);
    if !self.keys.is_empty() {
      // lock logic here
      self.unified_store_key_locked = true;
    }
    self.phase = 0;
  }

  /// libs/server/Transaction/TxnKeyEntry.cs:TryLockAllKeys
  pub fn try_lock_all_keys(&mut self, _lock_timeout: Duration) -> bool {
    self.phase = 1;
    self.keys.sort_by_key(|k| k.key_hash);
    if !self.keys.is_empty() {
      // C# TryLock（部分失败自动解锁）；wkv 事务锁后端接入前的占位恒成功，
      // 接入后由实际 TryLock 结果赋值并按失败置 phase=0 返回 false
      self.unified_store_key_locked = true;
    }
    self.phase = 0;
    true
  }

  /// libs/server/Transaction/TxnKeyEntry.cs:UnlockAllKeys
  pub fn unlock_all_keys(&mut self) {
    self.phase = 2;
    if self.unified_store_key_locked && !self.keys.is_empty() {
      // unlock logic here
    }
    self.keys.clear();
    self.unified_store_key_locked = false;
    self.phase = 0;
  }

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
