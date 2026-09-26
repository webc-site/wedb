//! 排队期锁集登记跨事务零残留语义锁（deviations §121 锁面）
//!
//! 立案面：MULTI 排队窗内每条入队命令的键在 network_skip 即经 lock_keys →
//! save_key_entry_to_lock 登记进 key_entries（对标 C# TxnRespCommands.cs:197
//! LockKeys→TxnKeyEntry.cs:87 AddKey 的即时登记形态）；DISCARD 臂与 EXEC
//! 的 Aborted EXECABORT 臂均须经 TransactionManager::reset 的
//! **无条件** unlock_all_keys（wtxn/src/transaction_manager.rs:219-220 →
//! wtxn/src/txn_key_entry.rs:290 清 keys/held/plan/latch）单点收口。
//!
//! C# 原型缺陷形（严禁回改的登记反证）：TransactionManager.cs:211
//! Reset(bool isRunning) 仅 isRunning=true 臂调 keyEntries.UnlockAllKeys()
//! （:217），而 NetworkDISCARD（TxnRespCommands.cs:217）与 NetworkEXEC 的
//! Aborted EXECABORT 臂（:54）均走 Reset(false)——排队期登记的键条目跨事务
//! 滞留 keyEntries，后果为下一笔事务 LockAllKeys 过度取锁、IsReadOnly 被残留
//! Exclusive 条目污染（集群槽校验 readOnly 误判）、ComputeSublogAccessVector
//! 子日志参与面虚报。rust 现状为正确侧，本域以真实会话链钉桩现形：
//! 注册面真实（非 mock），断言全部取自会话所挂 TransactionManager 真值源的
//! key_entries 观测面与落库数据面。

use std::sync::Arc;

use compio::runtime::Runtime;
use wacl::{AccessControlList, GarnetAclAuthenticator};
use wnode::resp::{
  garnet_api::StoreGarnetApi,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::drain_output;
use wtest_base::open_test_store;
use wtxn::{TxnKeyEntryComparison, TxnLockTable, TxnSession, TxnState, WatchVersionMap};

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

/// 喂一整批帧并冲出应答（消费循环 + 停车臂同步闭环的泵替身）
fn feed(s: &mut RespServerSession, frame: &[u8]) -> Vec<u8> {
  s.recv_buffer.extend_from_slice(frame);
  let mut resp_buf = Vec::new();
  let consumed = s.try_consume_messages();
  assert!(consumed.is_some(), "帧应被完整消费: {frame:?}");
  s.take_output_into(&mut resp_buf);
  Runtime::new()
    .unwrap()
    .block_on(wnode_test::drive_pending_parks(s, &mut resp_buf, true));
  s.output.extend_from_slice(&resp_buf);
  drain_output(s)
}

/// 会话所挂事务管理器真值源（本域锁集观测面）
fn txn(s: &RespServerSession) -> &wtxn::TransactionManager {
  s.txn_manager.as_ref().expect("事务组件已挂载")
}

/// 会话物理归属前缀下键的锁集登记哈希（与生产 lock_keys 同源单点现算，
/// 供「第二笔登记键集仅含本笔键」的逐哈希精确断言）
fn scoped_hash(s: &RespServerSession, key: &[u8]) -> i64 {
  let prefix = s.session_prefix();
  TxnKeyEntryComparison::scoped_key_hash(prefix.as_slice(), key)
}

/// 锁集登记的键哈希快照
fn locked_hashes(s: &RespServerSession) -> Vec<i64> {
  txn(s).key_entries.key_hashes().collect()
}

/// 票面复现链 DISCARD 形：MULTI; SET a 1; DISCARD; MULTI; SET b 2; EXEC 后，
/// 断言锁集登记跨事务零残留——第二笔登记键集仅含 b（count==1 且逐哈希等值），
/// DISCARD 收口形钉 transaction_manager.rs:219-220 无条件 unlock_all_keys
#[test]
fn multi_discard_lockset_zero_residual_next_txn_registers_only_new_key() {
  let (mut s, _dir) = session();

  // 1. 排队期登记面真实：MULTI; SET resid_a → key_entries 即时含 resid_a 排他条目
  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$7\r\nresid_a\r\n$1\r\n1\r\n"
    ),
    b"+OK\r\n+QUEUED\r\n"
  );
  assert_eq!(
    locked_hashes(&s),
    vec![scoped_hash(&s, b"resid_a")],
    "排队即登记（C# :197 同形）"
  );
  assert_eq!(txn(&s).key_entries.count(), 1);
  assert!(
    !txn(&s).key_entries.is_read_only(),
    "SET 登记排他条目，登记面前置校验"
  );
  assert!(!txn(&s).key_entries.get_lockset().is_empty());

  // 2. DISCARD 经 reset 无条件 unlock_all_keys：锁集零残留（C# Reset(false)
  //    不清形态此处将滞留 resid_a 排他条目）
  assert_eq!(feed(&mut s, b"*1\r\n$7\r\nDISCARD\r\n"), b"+OK\r\n");
  assert_eq!(txn(&s).state, TxnState::None);
  assert_eq!(s.txn_state, TxnState::None, "双态同复位");
  assert_eq!(txn(&s).key_entries.count(), 0, "DISCARD 后锁集登记零残留");
  assert!(locked_hashes(&s).is_empty(), "残留键哈希必清空");
  assert!(
    txn(&s).key_entries.is_read_only(),
    "只读态不被前笔排他残留污染"
  );
  assert_eq!(txn(&s).key_entries.get_lockset(), "", "锁集展示串同步清空");

  // 3. 同会话第二笔：MULTI; SET resid_b 排队后登记键集仅含 b（count==1 且逐哈希）
  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$7\r\nresid_b\r\n$1\r\n2\r\n"
    ),
    b"+OK\r\n+QUEUED\r\n"
  );
  assert_eq!(
    locked_hashes(&s),
    vec![scoped_hash(&s, b"resid_b")],
    "第二笔登记面恰为 b，无 a 混入"
  );

  // 4. EXEC 正常提交：仅 b 落库，被丢弃的 a 从未执行；提交后锁集再度归零
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
  assert_eq!(locked_hashes(&s), vec![scoped_hash(&s, b"abort_x")]);

  // 2. EXEC 走 Aborted EXECABORT 臂经 reset 无条件 unlock_all_keys：零残留
  //   （C# NetworkEXEC Aborted 臂 :54 走 Reset(false) 不清，abort_x 将滞留）
  assert_eq!(feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"), EXEC_ABORT);
  assert_eq!(txn(&s).state, TxnState::None);
  assert_eq!(s.txn_state, TxnState::None);
  assert_eq!(txn(&s).key_entries.count(), 0, "EXECABORT 后锁集登记零残留");
  assert!(locked_hashes(&s).is_empty());
  assert!(
    txn(&s).key_entries.is_read_only(),
    "只读态不被前笔排他残留污染"
  );
  assert_eq!(
    feed(&mut s, b"*2\r\n$3\r\nGET\r\n$7\r\nabort_x\r\n"),
    b"$-1\r\n",
    "中止队列严禁执行"
  );

  // 3. 后笔只读事务：MULTI; GET ro_y 排队——is_read_only 恒真（C# 残留形下
  //    前笔 SET 的 Exclusive 条目滞留将使本判据假阴，集群槽校验 readOnly 误判）
  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*2\r\n$3\r\nGET\r\n$4\r\nro_y\r\n"
    ),
    b"+OK\r\n+QUEUED\r\n"
  );
  assert_eq!(
    locked_hashes(&s),
    vec![scoped_hash(&s, b"ro_y")],
    "登记面仅本笔键"
  );
  assert!(
    txn(&s).key_entries.is_read_only(),
    "只读登记不被前笔排他残留污染"
  );

  // 4. EXEC 正常：缺键回 nil 数组元素，提交后锁集归零
  assert_eq!(feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"), b"*1\r\n$-1\r\n");
  assert_eq!(txn(&s).key_entries.count(), 0);
}
