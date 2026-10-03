//! 排队期锁集登记跨事务零残留语义锁（deviations §121 锁面）
//!
//! 立案面：MULTI 排队窗内每条入队命令的键在 network_skip 即经 lock_keys →
//! save_key_entry_to_lock 以（裸键字节, lock_type）登记进 txn_keys 缓冲
//! （对标 C# TxnRespCommands.cs:197 LockKeys→TxnKeyEntry.cs:87 AddKey 的
//! 即时登记形态；哈希条目延至 EXEC 展开期按当前物理前缀现算入 key_entries，
//! 见 wtxn-multi-queued-lock-hash-stale-across-generation 换代防穿透票）；
//! DISCARD 臂与 EXEC 的 Aborted EXECABORT 臂均须经 TransactionManager::reset
//! 的**无条件** unlock_all_keys + txn_keys.clear
//! （wtxn/src/transaction_manager.rs reset 单点收口）。
//!
//! C# 原型缺陷形（严禁回改的登记反证）：TransactionManager.cs:211
//! Reset(bool isRunning) 仅 isRunning=true 臂调 keyEntries.UnlockAllKeys()
//! （:217），而 NetworkDISCARD（TxnRespCommands.cs:217）与 NetworkEXEC 的
//! Aborted EXECABORT 臂（:54）均走 Reset(false)——排队期登记的键条目跨事务
//! 滞留 keyEntries，后果为下一笔事务 LockAllKeys 过度取锁、IsReadOnly 被残留
//! Exclusive 条目污染（集群槽校验 readOnly 误判）、ComputeSublogAccessVector
//! 子日志参与面虚报。rust 现状为正确侧，本域以真实会话链钉桩现形：
//! 注册面真实（非 mock），断言全部取自会话所挂 TransactionManager 真值源的
//! txn_keys 缓冲 / key_entries 观测面与落库数据面。

use std::sync::Arc;

use wacl::{AccessControlList, GarnetAclAuthenticator};
use wnode::resp::{
  garnet_api::StoreGarnetApi,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::feed_session_parked as feed;
use wtest_base::open_test_store;
use wtxn::{LockType, TxnLockTable, TxnState, WatchVersionMap};

/// EXECABORT 收口错误帧（C# CmdStrings.RESP_ERR_EXEC_ABORT）
const EXEC_ABORT: &[u8] = b"-EXECABORT Transaction discarded because of previous errors.\r\n";

/// 挂真实存储执行域、ACL 认证器与事务组件的会话（TempDir 保活数据文件）
fn session() -> (RespServerSession, tempfile::TempDir) {
  let (dir, store) = open_test_store("wnode-txn-lockset-residual.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());
  let mut s = RespServerSession::new(
    1,
    RespServerSessionOptions {
      default_user: "default".into(),
      ..RespServerSessionOptions::default()
    },
  );
  s.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(acl))));
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));
  s.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  (s, dir)
}

/// 会话所挂事务管理器真值源（本域锁集观测面）
fn txn(s: &RespServerSession) -> &wtxn::TransactionManager {
  s.txn_manager.as_ref().expect("事务组件已挂载")
}

/// 排队缓冲裸键快照（登记面：排队期裸键+锁型入 txn_keys，EXEC 展开期现算哈希）
fn queued_keys(s: &RespServerSession) -> Vec<Vec<u8>> {
  txn(s).txn_keys.iter().map(|k| k.to_vec()).collect()
}

/// 票面复现链 DISCARD 形：MULTI; SET a 1; DISCARD; MULTI; SET b 2; EXEC 后，
/// 断言锁集登记跨事务零残留——第二笔登记键集仅含 b（count==1 且逐哈希等值），
/// DISCARD 收口形钉 transaction_manager.rs:219-220 无条件 unlock_all_keys
#[test]
fn multi_discard_lockset_zero_residual_next_txn_registers_only_new_key() {
  let (mut s, _dir) = session();

  // 1. 排队期登记面真实：MULTI; SET resid_a → txn_keys 即时含 resid_a 裸键+排他型
  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$7\r\nresid_a\r\n$1\r\n1\r\n"
    ),
    b"+OK\r\n+QUEUED\r\n"
  );
  assert_eq!(
    queued_keys(&s),
    vec![b"resid_a".to_vec()],
    "排队即登记（C# :197 同形：裸键+锁型入缓冲，哈希延至 EXEC 展开）"
  );
  assert_eq!(txn(&s).txn_keys.get_lock_type(0), Some(LockType::Exclusive));
  assert!(
    !txn(&s).txn_keys.is_read_only(),
    "SET 登记排他条目，登记面前置校验"
  );
  assert_eq!(txn(&s).key_entries.count(), 0, "排队期哈希条目恒空");

  // 2. DISCARD 经 reset 无条件收口：缓冲与锁集零残留（C# Reset(false)
  //    不清形态此处将滞留 resid_a 排他条目）
  assert_eq!(feed(&mut s, b"*1\r\n$7\r\nDISCARD\r\n"), b"+OK\r\n");
  assert_eq!(txn(&s).state, TxnState::None);
  assert_eq!(s.txn_state, TxnState::None, "双态同复位");
  assert_eq!(txn(&s).txn_keys.len(), 0, "DISCARD 后排队缓冲零残留");
  assert_eq!(txn(&s).key_entries.count(), 0, "DISCARD 后锁集登记零残留");
  assert!(
    txn(&s).key_entries.is_read_only(),
    "只读态不被前笔排他残留污染"
  );

  // 3. 同会话第二笔：MULTI; SET resid_b 排队后登记键集仅含 b（裸键逐位等值）
  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$7\r\nresid_b\r\n$1\r\n2\r\n"
    ),
    b"+OK\r\n+QUEUED\r\n"
  );
  assert_eq!(
    queued_keys(&s),
    vec![b"resid_b".to_vec()],
    "第二笔登记面恰为 b，无 a 混入"
  );

  // 4. EXEC 正常提交：仅 b 落库，被丢弃的 a 从未执行；提交后登记面再度归零
  assert_eq!(feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"), b"*1\r\n+OK\r\n");
  assert_eq!(
    feed(&mut s, b"*2\r\n$3\r\nGET\r\n$7\r\nresid_a\r\n"),
    b"$-1\r\n",
    "DISCARD 丢弃队列严禁执行"
  );
  assert_eq!(
    feed(&mut s, b"*2\r\n$3\r\nGET\r\n$7\r\nresid_b\r\n"),
    b"$1\r\n2\r\n"
  );
  assert_eq!(txn(&s).key_entries.count(), 0, "提交经同一 reset 收口");
  assert_eq!(txn(&s).txn_keys.len(), 0, "提交后排队缓冲同面归零");
}

/// 票面复现链 EXECABORT 形：MULTI; SET a 1; 错误命令; EXEC 后锁集零残留；
/// 且后笔只读事务的 is_read_only 不被前笔 Exclusive 残留污染（C# 残留形将
/// 令 GetSlotVerificationInput 只读误判，本锁钉死该两面）
#[test]
fn execabort_lockset_zero_residual_readonly_not_poisoned_by_prev_exclusive() {
  let (mut s, _dir) = session();

  // 1. MULTI; SET abort_x 排队（排他登记在位）→ 未知命令中止：登记面确认
  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$7\r\nabort_x\r\n$1\r\n1\r\n*1\r\n$6\r\nFOOBAR\r\n",
    ),
    b"+OK\r\n+QUEUED\r\n-ERR unknown command\r\n"
  );
  assert_eq!(s.txn_state, TxnState::Aborted);
  assert_eq!(queued_keys(&s), vec![b"abort_x".to_vec()]);
  assert_eq!(txn(&s).txn_keys.get_lock_type(0), Some(LockType::Exclusive));

  // 2. EXEC 走 Aborted EXECABORT 臂经 reset 无条件收口：缓冲与锁集零残留
  //   （C# NetworkEXEC Aborted 臂 :54 走 Reset(false) 不清，abort_x 将滞留）
  assert_eq!(feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"), EXEC_ABORT);
  assert_eq!(txn(&s).state, TxnState::None);
  assert_eq!(s.txn_state, TxnState::None);
  assert_eq!(txn(&s).txn_keys.len(), 0, "EXECABORT 后排队缓冲零残留");
  assert_eq!(txn(&s).key_entries.count(), 0, "EXECABORT 后锁集登记零残留");
  assert!(
    txn(&s).key_entries.is_read_only(),
    "只读态不被前笔排他残留污染"
  );
  assert_eq!(
    feed(&mut s, b"*2\r\n$3\r\nGET\r\n$7\r\nabort_x\r\n"),
    b"$-1\r\n",
    "中止队列严禁执行"
  );

  // 3. 后笔只读事务：MULTI; GET ro_y 排队——缓冲 is_read_only 恒真（C# 残留形下
  //    前笔 SET 的 Exclusive 条目滞留将使本判据假阴，集群槽校验 readOnly 误判）
  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*2\r\n$3\r\nGET\r\n$4\r\nro_y\r\n"
    ),
    b"+OK\r\n+QUEUED\r\n"
  );
  assert_eq!(queued_keys(&s), vec![b"ro_y".to_vec()], "登记面仅本笔键");
  assert_eq!(txn(&s).txn_keys.get_lock_type(0), Some(LockType::Shared));
  assert!(
    txn(&s).txn_keys.is_read_only(),
    "只读登记不被前笔排他残留污染"
  );

  // 4. EXEC 正常：缺键回 nil 数组元素，提交后登记面归零
  assert_eq!(feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"), b"*1\r\n$-1\r\n");
  assert_eq!(txn(&s).key_entries.count(), 0);
  assert_eq!(txn(&s).txn_keys.len(), 0);
}
