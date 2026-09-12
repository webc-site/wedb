use std::{sync::Arc, time::Duration};

use wnode::{resp::resp_server_session::RespServerSession, types::RespCommand};
use wresp::ArgSlice;
use wtxn::{
  LockType, StoreType, TransactionManager, TransactionStoreTypes, TxnCommandKeys,
  TxnKeyEntryComparison, TxnKeySpec, TxnProcHandle, TxnProcResolver, TxnQueuedCommandInfo,
  TxnState, WatchVersionMap,
};

fn manager() -> TransactionManager {
  TransactionManager::new(Arc::new(WatchVersionMap::new(64)), None)
}

fn session_with_args(args: &[&[u8]]) -> (RespServerSession, Vec<u8>) {
  let mut buffer: Vec<u8> = Vec::new();
  for arg in args {
    buffer.extend_from_slice(arg);
  }
  let mut slices = Vec::with_capacity(args.len());
  let mut offset = 0usize;
  for arg in args {
    slices.push(ArgSlice::new(
      unsafe { buffer.as_ptr().add(offset) },
      arg.len(),
    ));
    offset += arg.len();
  }
  let mut session = RespServerSession::default();
  session.parse_state.initialize_with_args(&slices);
  (session, buffer)
}

/// test/standalone/Garnet.test.scripting/TransactionTests.cs:TxnSetTest
#[test]
fn txn_set_test() {
  let mut txn = manager();
  let mut session = RespServerSession::default();

  // MULTI
  assert!(txn.network_multi(&mut session));
  assert_eq!(session.output, b"+OK\r\n");
  assert_eq!(txn.state, TxnState::Started);
  session.output.clear();

  // QUEUED SET mykey1 val1
  let (mut sess_k1, _b1) = session_with_args(&[b"mykey1", b"abcdefg1"]);
  let set_info = TxnQueuedCommandInfo {
    name: "set".into(),
    arity: 3,
    allowed_in_txn: true,
    is_sub_command: false,
    keys: Some(TxnCommandKeys {
      store_type: StoreType::Main,
      key_specs: vec![TxnKeySpec::new(0, 0, 1, false)],
    }),
  };
  assert!(txn.network_skip(&mut sess_k1, RespCommand::Set, Some(&set_info)));
  assert_eq!(sess_k1.output, b"+QUEUED\r\n");
  assert_eq!(txn.operation_cnt_txn, 1);

  // QUEUED SET mykey2 val2
  let (mut sess_k2, _b2) = session_with_args(&[b"mykey2", b"abcdefg2"]);
  assert!(txn.network_skip(&mut sess_k2, RespCommand::Set, Some(&set_info)));
  assert_eq!(sess_k2.output, b"+QUEUED\r\n");
  assert_eq!(txn.operation_cnt_txn, 2);

  // EXEC (1st call starts running and outputs *2)
  assert!(txn.network_exec(&mut session));
  assert_eq!(session.output, b"*2\r\n");
  assert_eq!(txn.state, TxnState::Running);

  // EXEC (2nd call at end of replay commits)
  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::None);
}

/// test/standalone/Garnet.test.scripting/TransactionTests.cs:TxnExecuteTest
#[test]
fn txn_execute_test() {
  let mut txn = manager();
  let mut session = RespServerSession::default();

  assert!(txn.network_multi(&mut session));
  assert_eq!(session.output, b"+OK\r\n");
  session.output.clear();

  let (mut s1, _b1) = session_with_args(&[b"mykey1", b"abcdefg1"]);
  let set_info = TxnQueuedCommandInfo {
    name: "set".into(),
    arity: 3,
    allowed_in_txn: true,
    is_sub_command: false,
    keys: Some(TxnCommandKeys {
      store_type: StoreType::Main,
      key_specs: vec![TxnKeySpec::new(0, 0, 1, false)],
    }),
  };
  assert!(txn.network_skip(&mut s1, RespCommand::Set, Some(&set_info)));
  assert_eq!(s1.output, b"+QUEUED\r\n");

  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::Running);
  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::None);
}

/// test/standalone/Garnet.test.scripting/TransactionTests.cs:TxnGetTest
#[test]
fn txn_get_test() {
  let mut txn = manager();
  let mut session = RespServerSession::default();

  assert!(txn.network_multi(&mut session));
  session.output.clear();

  let get_info = TxnQueuedCommandInfo {
    name: "get".into(),
    arity: 2,
    allowed_in_txn: true,
    is_sub_command: false,
    keys: Some(TxnCommandKeys {
      store_type: StoreType::Main,
      key_specs: vec![TxnKeySpec::new(0, 0, 1, true)],
    }),
  };

  let (mut s1, _b1) = session_with_args(&[b"mykey1"]);
  assert!(txn.network_skip(&mut s1, RespCommand::Get, Some(&get_info)));
  assert_eq!(s1.output, b"+QUEUED\r\n");

  let (mut s2, _b2) = session_with_args(&[b"mykey2"]);
  assert!(txn.network_skip(&mut s2, RespCommand::Get, Some(&get_info)));
  assert_eq!(s2.output, b"+QUEUED\r\n");

  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::Running);
  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::None);
}

/// test/standalone/Garnet.test.scripting/TransactionTests.cs:TxnGetSetTest
#[test]
fn txn_get_set_test() {
  let mut txn = manager();
  let mut session = RespServerSession::default();

  assert!(txn.network_multi(&mut session));
  session.output.clear();

  let get_info = TxnQueuedCommandInfo {
    name: "get".into(),
    arity: 2,
    allowed_in_txn: true,
    is_sub_command: false,
    keys: Some(TxnCommandKeys {
      store_type: StoreType::Main,
      key_specs: vec![TxnKeySpec::new(0, 0, 1, true)],
    }),
  };
  let (mut s1, _b1) = session_with_args(&[b"mykey1"]);
  assert!(txn.network_skip(&mut s1, RespCommand::Get, Some(&get_info)));
  assert_eq!(s1.output, b"+QUEUED\r\n");

  let set_info = TxnQueuedCommandInfo {
    name: "set".into(),
    arity: 3,
    allowed_in_txn: true,
    is_sub_command: false,
    keys: Some(TxnCommandKeys {
      store_type: StoreType::Main,
      key_specs: vec![TxnKeySpec::new(0, 0, 1, false)],
    }),
  };
  let (mut s2, _b2) = session_with_args(&[b"mykey2", b"abcdefg2"]);
  assert!(txn.network_skip(&mut s2, RespCommand::Set, Some(&set_info)));
  assert_eq!(s2.output, b"+QUEUED\r\n");

  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::Running);
  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::None);
}

/// test/standalone/Garnet.test.scripting/TransactionTests.cs:SimpleWatchTest
#[test]
fn simple_watch_test() {
  let map = Arc::new(WatchVersionMap::new(64));
  let mut txn = TransactionManager::new(Arc::clone(&map), None::<()>);
  let mut session = RespServerSession::default();

  // WATCH key1
  txn.watch(b"key1");
  assert!(txn.watch_container.validate_watch_version());

  // MULTI
  assert!(txn.network_multi(&mut session));
  assert_eq!(session.output, b"+OK\r\n");
  session.output.clear();

  // Concurrent modification to key1
  map.increment_version(TxnKeyEntryComparison::key_hash(b"key1") as u64);

  // EXEC should abort
  assert!(txn.network_exec(&mut session));
  assert_eq!(session.output, b"*-1\r\n");
  assert_eq!(txn.state, TxnState::None);

  // Next transaction should commit
  session.output.clear();
  assert!(txn.network_multi(&mut session));
  assert_eq!(session.output, b"+OK\r\n");
  session.output.clear();

  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::Running);
  assert!(txn.network_exec(&mut session));
  assert_eq!(txn.state, TxnState::None);
}

/// test/standalone/Garnet.test.scripting/TransactionTests.cs:WatchNonExistentKey
#[test]
fn watch_non_existent_key() {
  let map = Arc::new(WatchVersionMap::new(64));
  let mut txn = TransactionManager::new(Arc::clone(&map), None::<()>);
  let mut session = RespServerSession::default();

  // WATCH key1 (non-existent, version = 0)
  txn.watch(b"key1");

  // MULTI
  assert!(txn.network_multi(&mut session));
  session.output.clear();

  // key1 is created/modified concurrently -> version becomes 1
  map.increment_version(TxnKeyEntryComparison::key_hash(b"key1") as u64);

  // EXEC should abort
  assert!(txn.network_exec(&mut session));
  assert_eq!(session.output, b"*-1\r\n");
  assert_eq!(txn.state, TxnState::None);
}

/// test/standalone/Garnet.test.scripting/RespTransactionProcTests.cs:TransactionProcTest1
#[test]
fn transaction_proc_test1() {
  struct MockResolver;
  impl TxnProcResolver<RespServerSession> for MockResolver {
    fn get_custom_transaction_procedure(&self, txn_id: u8) -> Option<TxnProcHandle> {
      (txn_id == 7).then(|| TxnProcHandle {
        name: "mock-proc".into(),
        arity: 2,
      })
    }
    fn try_transaction_proc(
      &mut self,
      _txn_id: u8,
      _txn_manager: &mut TransactionManager,
      session: &mut RespServerSession,
    ) -> bool {
      session.output.extend_from_slice(b"+SUCCESS\r\n");
      true
    }
  }

  let mut txn = manager();
  let (mut session, _buffer) = session_with_args(&[b"7", b"a1"]);
  assert!(txn.network_runtxp(&mut session, &mut MockResolver));
  assert_eq!(session.output, b"+SUCCESS\r\n");
}

/// test/standalone/Garnet.test.scripting/TransactionTests.cs:TxnCommandCoverage
#[test]
fn txn_command_coverage() {
  let mut txn = manager();
  assert_eq!(txn.state, TxnState::None);
  assert!(!txn.is_skipping_operations());
  txn.state = TxnState::Started;
  assert!(txn.is_skipping_operations());
  txn.state = TxnState::Aborted;
  assert!(txn.is_skipping_operations());
  txn.state = TxnState::Running;
  assert!(!txn.is_skipping_operations());

  // Store type mapping
  let mut txn = manager();
  txn.add_transaction_store_type(StoreType::Main);
  txn.add_transaction_store_type(StoreType::Object);
  assert!(txn.store_types.contains(TransactionStoreTypes::Main));
  assert!(txn.store_types.contains(TransactionStoreTypes::Object));
  assert!(!txn.store_types.contains(TransactionStoreTypes::Unified));
  txn.add_transaction_store_type(StoreType::All);
  assert!(txn.store_types.contains(TransactionStoreTypes::Unified));

  // Nested guard is null when already running
  let mut txn = manager();
  txn.save_key_entry_to_lock(b"k", LockType::Shared);
  assert!(txn.run(false, false, Duration::ZERO));
  {
    let guard = txn.promote_to_transaction(TransactionStoreTypes::Main, b"j", LockType::Shared);
    assert_eq!(guard.state(), TxnState::None);
  }
  assert_eq!(txn.state, TxnState::Running);
}
