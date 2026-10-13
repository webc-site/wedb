#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalContext.cs（负索引钉尾参、排序登记）
mod common;

use common::manager;
use wbase::store_type::StoreType;
use wtxn::{LockType, TxnCommandKeys, TxnKeySpec};
use wtxn_test::MockTxnSession;
use wval::SessionPrefixBuf;

/// 根域会话前缀（测试与非会话面缺省域单点，与生产恒有前缀形态一致）
fn root() -> SessionPrefixBuf {
  SessionPrefixBuf::ROOT
}

#[test]
fn save_key_entry_registers_to_txn_keys_and_expands_on_preamble() {
  let mut txn = manager();
  txn.save_key_entry_to_lock(b"a", LockType::Shared);
  assert!(txn.is_read_only());
  assert_eq!(txn.key_entries.count(), 0);
  assert_eq!(txn.txn_keys.len(), 1);

  txn.save_key_entry_to_lock(b"b", LockType::Exclusive);
  assert!(!txn.is_read_only());
  assert_eq!(txn.key_entries.count(), 0);
  assert_eq!(txn.txn_keys.len(), 2);

  // 展开期绑定物理哈希并置位写标志
  txn.register_run_preamble(root().as_slice(), true);
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
  // 排队期仅登记 txn_keys（单机/集群统一），key_entries 零物理冻结
  assert_eq!(txn.key_entries.count(), 0);
  assert_eq!(txn.txn_keys.len(), 3);
  assert!(!txn.is_read_only());

  // 展开期以物理前缀展开
  txn.register_run_preamble(root().as_slice(), true);
  assert_eq!(txn.key_entries.count(), 3);
  assert!(txn.perform_writes);
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
  assert_eq!(txn.key_entries.count(), 0);
  assert_eq!(txn.txn_keys.len(), 1);
  assert!(txn.is_read_only());

  txn.register_run_preamble(root().as_slice(), false);
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
  assert_eq!(txn.txn_keys.len(), 0);
}
