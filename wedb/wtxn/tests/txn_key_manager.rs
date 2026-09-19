mod common;

use common::manager;
use wbase::store_type::StoreType;
use wtxn::{LockType, TransactionStoreTypes, TxnCommandKeys, TxnKeySpec};
use wtxn_test::MockTxnSession;

#[test]
fn save_key_entry_marks_perform_writes_on_exclusive() {
  let mut txn = manager();
  txn.save_key_entry_to_lock(b"a", LockType::Shared);
  assert!(!txn.perform_writes);
  txn.save_key_entry_to_lock(b"b", LockType::Exclusive);
  assert!(txn.perform_writes);
  assert_eq!(txn.key_entries.count(), 2);
  // 单机形态零登记
  assert!(txn.txn_keys.is_empty());

  // 集群启用登记
  txn.cluster_enabled = true;
  txn.save_key_entry_to_lock(b"c", LockType::Shared);
  assert_eq!(txn.txn_keys.len(), 1);
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
  // 单机形态（cluster_enabled false）：零登记零堆分配
  assert!(txn.txn_keys.is_empty());
  assert!(txn.perform_writes);
  assert!(txn.store_types.contains(TransactionStoreTypes::Main));

  // 集群模式（cluster_enabled true）：登记全部展开键
  let mut cluster_txn = manager();
  cluster_txn.cluster_enabled = true;
  cluster_txn.lock_keys(&session, &keys);
  assert_eq!(cluster_txn.key_entries.count(), 3);
  assert_eq!(cluster_txn.txn_keys.len(), 3);
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
  assert!(txn.txn_keys.is_empty());
  assert!(!txn.perform_writes);

  let mut cluster_txn = manager();
  cluster_txn.cluster_enabled = true;
  cluster_txn.lock_keys(&session, &keys);
  assert_eq!(cluster_txn.key_entries.count(), 1);
  assert_eq!(cluster_txn.txn_keys.len(), 1);
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
