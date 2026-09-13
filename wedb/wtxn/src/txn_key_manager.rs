//! 事务键管理面（对标 libs/server/Transaction/TxnKeyManager.cs —— C# partial
//! TransactionManager，此处为跨文件 `impl` 块）
//!
//! C# 的 respSession.clusterSession / parseState 访问经会话引用；Rust 侧
//! 会话以 `&impl TxnSession` 逐调用传入，集群面（槽校验缓存 / 迭代
//! 槽校验）在集群域接线前按 C# 的 `!clusterEnabled` 早退路径落地。

pub use crate::txn_key_spec::TxnKeySpec;
use crate::{
  StoreType,
  transaction_manager::{TransactionManager, TxnAofLog},
  txn_key_entry::{LockType, TxnKeyEntries},
  txn_key_entry_comparison::TxnKeyEntryComparison,
  txn_session::TxnSession,
};

/// 排队命令的键登记元数据（C# SimpleRespCommandInfo 中 LockKeys 所需子集
/// 的本域投影）
#[derive(Debug, Clone)]
pub struct TxnCommandKeys {
  /// 命令存储类别（并入事务存储面）
  pub store_type: StoreType,
  /// 键规格列表（C# cmdInfo.KeySpecs）
  pub key_specs: Vec<TxnKeySpec>,
}

impl<L: TxnAofLog> TransactionManager<L> {
  /// 锁登记内核（对应 SaveKeyEntryToLock 主体实现；跨文件 impl 域的字段拆借路径共用，
  /// 避免 WATCH 键并集路径与容器借用冲突）
  pub(crate) fn register_key_lock(
    key_entries: &mut TxnKeyEntries,
    perform_writes: &mut bool,
    key: &[u8],
    lock_type: LockType,
  ) {
    // 排他锁型标记事务含写操作（决定 AOF 事务条目是否记录）
    *perform_writes |= lock_type == LockType::Exclusive;
    key_entries.add_key(TxnKeyEntryComparison::key_hash(key), lock_type);
  }

  /// 登记待锁键
  ///
  /// libs/server/Transaction/TxnKeyManager.cs:SaveKeyEntryToLock
  ///
  /// 排他锁型标记事务含写操作（决定 AOF 事务条目是否记录）。
  pub fn save_key_entry_to_lock(&mut self, key: &[u8], lock_type: LockType) {
    Self::register_key_lock(
      &mut self.key_entries,
      &mut self.perform_writes,
      key,
      lock_type,
    );
  }

  /// 按命令键规格锁定键
  ///
  /// libs/server/Transaction/TxnKeyManager.cs:LockKeys
  ///
  /// 并入存储面后逐规格展开键窗口：登记锁集（共享/排他按键规格只读位）
  /// 并同步集群槽校验键列表。
  pub fn lock_keys(&mut self, session: &(impl TxnSession + ?Sized), command_keys: &TxnCommandKeys) {
    if command_keys.key_specs.is_empty() {
      return;
    }

    self.add_transaction_store_type(command_keys.store_type);

    let arg_count = session.arg_count();
    for key_spec in &command_keys.key_specs {
      let last_idx = if key_spec.last_idx < 0 {
        // 负索引：-1 表最后一个参数（C# searchArgs 的 endIdx 语义）
        arg_count as i64 + key_spec.last_idx
      } else {
        key_spec.last_idx
      };
      if last_idx < 0 {
        continue;
      }
      let last_idx = (last_idx as usize).min(arg_count.saturating_sub(1));
      if key_spec.first_idx > last_idx || key_spec.first_idx >= arg_count {
        continue;
      }
      let lock_type = if key_spec.read_only {
        LockType::Shared
      } else {
        LockType::Exclusive
      };
      let step = key_spec.step.max(1);
      for idx in (key_spec.first_idx..=last_idx).step_by(step) {
        let key_bytes = session.get_arg(idx);
        self.save_key_entry_to_lock(key_bytes, lock_type);
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use super::*;
  use crate::{
    transaction_manager::TransactionStoreTypes, txn_session::MockTxnSession,
    watch_version_map::WatchVersionMap,
  };

  fn manager() -> TransactionManager {
    TransactionManager::new(Arc::new(WatchVersionMap::new(64)), None)
  }

  #[test]
  fn save_key_entry_marks_perform_writes_on_exclusive() {
    let mut txn = manager();
    txn.save_key_entry_to_lock(b"a", LockType::Shared);
    assert!(!txn.perform_writes);
    txn.save_key_entry_to_lock(b"b", LockType::Exclusive);
    assert!(txn.perform_writes);
    assert_eq!(txn.key_entries.count(), 2);
  }

  #[test]
  fn lock_keys_expands_window_and_registers_keys() {
    let mut txn = manager();
    let session = MockTxnSession::with_args(&[b"k1", b"k2", b"k3"]);
    let keys = TxnCommandKeys {
      store_type: StoreType::Main,
      key_specs: vec![TxnKeySpec::new(0, 2, 1, false)],
    };
    txn.lock_keys(&session, &keys);
    assert_eq!(txn.key_entries.count(), 3);
    assert!(txn.perform_writes);
    assert!(txn.store_types.contains(TransactionStoreTypes::Main));
  }

  #[test]
  fn lock_keys_negative_index_pins_to_last_arg() {
    let mut txn = manager();
    let session = MockTxnSession::with_args(&[b"k1", b"k2", b"k3"]);
    let keys = TxnCommandKeys {
      store_type: StoreType::Main,
      key_specs: vec![TxnKeySpec::new(2, -1, 1, true)],
    };
    txn.lock_keys(&session, &keys);
    // -1 收敛到末参（k3），只读 → 共享锁
    assert_eq!(txn.key_entries.count(), 1);
    assert!(!txn.perform_writes);
  }

  #[test]
  fn lock_keys_negative_index_underflow_locks_nothing() {
    let mut txn = manager();
    let session = MockTxnSession::with_args(&[b"k1"]);
    let keys = TxnCommandKeys {
      store_type: StoreType::Main,
      key_specs: vec![TxnKeySpec::new(0, -2, 1, false)],
    };
    txn.lock_keys(&session, &keys);
    assert_eq!(txn.key_entries.count(), 0);
  }
}
