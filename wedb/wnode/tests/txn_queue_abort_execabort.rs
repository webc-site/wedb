//! MULTI 排队期入队失败（未知命令/未知子命令、ACL 权限拒绝、参数数量错误）
//! 必须中止事务：EXEC 回 -EXECABORT 且严禁重放队列任何命令（Redis
//! All-or-Nothing 契约，对标 C# 排队失败臂 txnManager.Abort 语义
//! garnet/libs/server/Transaction/TxnRespCommands.cs:NetworkSKIP 与
//! TransactionManager.cs:Abort；C# 原型 RespServerSession.cs:649
//! `cmd != INVALID` 门与 :684 权限门的绕过为同源缺陷，本域修复消除）
//!
//! 修复前缺陷路径（wedb/wnode resp_server_session/core.rs:process_messages）：
//! 未知命令解析臂回 Invalid 被门旁路、ACL/脚本拒绝臂写错误后直接跳分支，
//! 均不置中止 → network_exec 误判 Started 回退重放，队列中的写命令违规执行。

use std::sync::Arc;

use compio::runtime::Runtime;
use wacl::{AccessControlList, GarnetAclAuthenticator};
use wnode::resp::{
  garnet_api::StoreGarnetApi,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::drain_output;
use wtest_base::open_test_store;
use wtxn::{TxnLockTable, TxnState, WatchVersionMap};

/// EXECABORT 收口错误帧（C# CmdStrings.RESP_ERR_EXEC_ABORT）
const EXEC_ABORT: &[u8] = b"-EXECABORT Transaction discarded because of previous errors.\r\n";
/// ACL 拒绝错误帧（C# CmdStrings.RESP_ERR_NOPERM）
const NOPERM: &[u8] = b"-NOPERM this user has no permissions to run the command\r\n";

/// 挂真实存储执行域、ACL 认证器与事务组件的会话（TempDir 保活数据文件）
fn session() -> (RespServerSession, tempfile::TempDir) {
  let (dir, store) = open_test_store("wnode-txn-queue-abort.db").unwrap();
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

/// 双态镜像读数（会话镜像 / 事务管理器真值源）
fn states(s: &RespServerSession) -> (TxnState, TxnState) {
  (
    s.txn_state,
    s.txn_manager.as_ref().expect("事务组件已挂载").state,
  )
}

/// 票面验证点 1：MULTI → 未知命令 → SET → EXEC。未知命令被
/// `cmd != Invalid` 门旁路是修复前主缺陷臂——修复后入队失败即中止，
/// EXEC 回 EXECABORT，SET 不落库；收口后会话恢复正常事务能力
#[test]
fn multi_unknown_command_aborts_and_exec_returns_execabort() {
  let (mut s, _dir) = session();

  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*1\r\n$6\r\nFOOBAR\r\n*3\r\n$3\r\nSET\r\n$7\r\nabort_k\r\n$1\r\nv\r\n",
    ),
    b"+OK\r\n-ERR unknown command\r\n+QUEUED\r\n",
    "MULTI +OK；未知命令回错误帧；其后 SET 按 Redis 契约仍 +QUEUED（Aborted 窗内继续排队，EXEC 整单丢弃）"
  );
  // 双态同置 Aborted（修复前会话镜像滞留 Started、管理器亦 Started 不中止）
  assert_eq!(states(&s), (TxnState::Aborted, TxnState::Aborted));

  assert_eq!(feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"), EXEC_ABORT);
  assert_eq!(
    states(&s),
    (TxnState::None, TxnState::None),
    "EXECABORT 收口复位"
  );

  // 违规 SET 未执行：键不存在
  assert_eq!(
    feed(&mut s, b"*2\r\n$3\r\nGET\r\n$7\r\nabort_k\r\n"),
    b"$-1\r\n"
  );

  // EXECABORT 后新事务正常（残留队列未被重放、状态未污染）
  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$6\r\nnext_k\r\n$1\r\nw\r\n*1\r\n$4\r\nEXEC\r\n",
    ),
    b"+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n"
  );
  assert_eq!(
    feed(&mut s, b"*2\r\n$3\r\nGET\r\n$6\r\nnext_k\r\n"),
    b"$1\r\nw\r\n"
  );
}

/// 票面验证点 2：MULTI → 未知子命令 → EXEC。未知子命令与未知命令同臂
/// （array_parse_command 写 specific_error 后回 Invalid），同样必须中止
#[test]
fn multi_unknown_subcommand_exec_returns_execabort() {
  let (mut s, _dir) = session();

  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*2\r\n$6\r\nOBJECT\r\n$6\r\nFOOBAR\r\n*3\r\n$3\r\nSET\r\n$4\r\nsubk\r\n$1\r\nv\r\n",
    ),
    b"+OK\r\n-ERR unknown subcommand 'FOOBAR'.\r\n+QUEUED\r\n"
  );
  assert_eq!(states(&s), (TxnState::Aborted, TxnState::Aborted));

  assert_eq!(feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"), EXEC_ABORT);
  assert_eq!(
    feed(&mut s, b"*2\r\n$3\r\nGET\r\n$4\r\nsubk\r\n"),
    b"$-1\r\n",
    "中止事务的队列严禁执行"
  );
}

/// 票面验证点 3：MULTI → ACL 拒绝命令 → SET → EXEC。权限拒绝臂（NOPERM）
/// 在排队窗内同属入队失败，必须中止且队列不落库
#[test]
fn multi_acl_denied_command_exec_returns_execabort() {
  let (mut s, _dir) = session();

  // 收权：default 去 TYPE（同会话句柄 CAS 换新即时生效）
  assert_eq!(
    feed(
      &mut s,
      b"*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$7\r\ndefault\r\n$5\r\n-type\r\n"
    ),
    b"+OK\r\n"
  );

  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*2\r\n$4\r\nTYPE\r\n$6\r\nperm_k\r\n*3\r\n$3\r\nSET\r\n$6\r\nperm_k\r\n$1\r\nv\r\n",
    ),
    [&b"+OK\r\n"[..], NOPERM, b"+QUEUED\r\n"].concat()
  );
  assert_eq!(states(&s), (TxnState::Aborted, TxnState::Aborted));

  assert_eq!(feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"), EXEC_ABORT);
  assert_eq!(
    feed(&mut s, b"*2\r\n$3\r\nGET\r\n$6\r\nperm_k\r\n"),
    b"$-1\r\n",
    "权限拒绝中止后 SET 不得落盘"
  );
}

/// 票面验证点 4：MULTI → 参数数量错误命令 → EXEC。NetworkSKIP 元数臂的
/// self.abort() 修复前仅动管理器状态、会话镜像滞留 Started——修复后
/// 双态同置 Aborted 且 EXEC 回 EXECABORT
#[test]
fn multi_wrong_arity_command_syncs_session_mirror_and_exec_aborts() {
  let (mut s, _dir) = session();

  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*2\r\n$3\r\nSET\r\n$5\r\nonlyk\r\n",
    ),
    b"+OK\r\n-ERR wrong number of arguments for 'SET' command\r\n"
  );
  // 修复前此处双态脱节：镜像 Started / 管理器 Aborted
  assert_eq!(states(&s), (TxnState::Aborted, TxnState::Aborted));

  assert_eq!(feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"), EXEC_ABORT);
  assert_eq!(
    feed(&mut s, b"*2\r\n$3\r\nGET\r\n$5\r\nonlyk\r\n"),
    b"$-1\r\n"
  );
}

/// 发现三锁用例：MULTI → 内联垃圾行 → SET → EXEC。内联垃圾行落 Invalid
/// 触发 queue_failure 统一中止，EXEC 恒回 EXECABORT，SET 严禁落盘
#[test]
fn multi_inline_garbage_aborts_and_exec_returns_execabort() {
  let (mut s, _dir) = session();

  // MULTI + 内联垃圾行 + SET
  // 内联垃圾行跳行返回 Invalid，不向客户端写应答，但置 queue_failure 中止事务
  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\nGARBAGE_INLINE_LINE\r\n*3\r\n$3\r\nSET\r\n$8\r\ninline_k\r\n$1\r\nv\r\n",
    ),
    b"+OK\r\n+QUEUED\r\n",
    "MULTI +OK；内联垃圾行无应答；其后 SET 仍 +QUEUED（处于 Aborted 窗内）"
  );
  assert_eq!(states(&s), (TxnState::Aborted, TxnState::Aborted));

  assert_eq!(feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"), EXEC_ABORT);
  assert_eq!(
    states(&s),
    (TxnState::None, TxnState::None),
    "EXECABORT 收口复位"
  );

  // 违规 SET 未执行：键不存在
  assert_eq!(
    feed(&mut s, b"*2\r\n$3\r\nGET\r\n$8\r\ninline_k\r\n"),
    b"$-1\r\n"
  );
}
