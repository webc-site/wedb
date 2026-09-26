//! MULTI 排队期 SELECT 负数库号必须中止（票 task/ing/zcode-r133c-selectdb.md 案一，
//! 修复转写漏项：rust 排队臂误用 parse_db_index 之 Ok 档，int32 域内负数落
//! OutOfRange 短路直落 write_queued，误回 +QUEUED 部分提交；C# 原型
//! `parseState.TryGetInt(0, out var index) && index != activeDbId` 判据系 int32
//! 文法档，负数字面量解析成功且恒异于非负 activeDbId，MULTI 内 SELECT -1 必落
//! 中止臂整笔回 -EXECABORT，对标
//! garnet/libs/server/Transaction/TxnRespCommands.cs:163-168 NetworkSKIP SELECT 臂）
//!
//! 真协议帧 + 真存储 + 真事务状态机闭环，无假 mock：排队帧、双态镜像
//! （会话/管理器）、EXEC 应答、键落库面逐字节收口。

use std::sync::Arc;

use compio::runtime::Runtime;
use wnode::resp::{
  garnet_api::StoreGarnetApi,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::drain_output;
use wtest_base::open_test_store;
use wtxn::{TxnLockTable, TxnState, WatchVersionMap};

/// EXECABORT 收口错误帧（C# CmdStrings.RESP_ERR_EXEC_ABORT）
const EXEC_ABORT: &[u8] = b"-EXECABORT Transaction discarded because of previous errors.\r\n";
/// 事务内 SELECT 中止帧（C# CmdStrings.RESP_ERR_SELECT_IN_TXN_UNSUPPORTED）
const SELECT_IN_TXN: &[u8] = b"-ERR SELECT is currently unsupported inside a transaction.\r\n";
/// not-integer 帧（C# CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER，含尾句点）
const NOT_INTEGER: &[u8] = b"-ERR value is not an integer or out of range.\r\n";

/// 挂真实存储执行域与事务组件的会话（TempDir 保活数据文件）
fn session() -> (RespServerSession, tempfile::TempDir) {
  let (dir, store) = open_test_store("wnode-txn-select-neg.db").unwrap();
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
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

/// 案一主用例：MULTI → SELECT -1 → SET → EXEC。负数库号在 C# TryGetInt 档
/// 解析成功且 ≠ activeDbId（恒非负），必落中止臂：排队帧即回
/// SELECT_IN_TXN_UNSUPPORTED 并双态同置 Aborted，EXEC 整笔回 -EXECABORT，
/// SET 严禁落库（修复前误回 +QUEUED，EXEC 部分提交 [重放错误帧, +OK]，
/// All-or-Nothing 语义与 C# 相反）
#[test]
fn multi_select_negative_db_aborts_and_exec_returns_execabort() {
  let (mut s, _dir) = session();

  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*2\r\n$6\r\nSELECT\r\n$2\r\n-1\r\n*3\r\n$3\r\nSET\r\n$5\r\nneg_k\r\n$1\r\n1\r\n",
    ),
    [&b"+OK\r\n"[..], SELECT_IN_TXN, b"+QUEUED\r\n"].concat(),
    "SELECT -1 当时回中止帧（非 +QUEUED）；其后 SET 按 Redis 契约仍排队入 Aborted 窗"
  );
  assert_eq!(states(&s), (TxnState::Aborted, TxnState::Aborted));

  assert_eq!(feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"), EXEC_ABORT);
  assert_eq!(
    states(&s),
    (TxnState::None, TxnState::None),
    "EXECABORT 收口复位"
  );
  assert_eq!(
    feed(&mut s, b"*2\r\n$3\r\nGET\r\n$5\r\nneg_k\r\n"),
    b"$-1\r\n",
    "中止事务的队列严禁执行（负数臂按 C# 整笔弃）"
  );
}

/// 对照组 1（不回退）：MULTI → SELECT 5（正数跨库）现形维持中止臂
#[test]
fn multi_select_positive_cross_db_still_aborts() {
  let (mut s, _dir) = session();

  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*2\r\n$6\r\nSELECT\r\n$1\r\n5\r\n*3\r\n$3\r\nSET\r\n$5\r\npos_k\r\n$1\r\n1\r\n",
    ),
    [&b"+OK\r\n"[..], SELECT_IN_TXN, b"+QUEUED\r\n"].concat()
  );
  assert_eq!(states(&s), (TxnState::Aborted, TxnState::Aborted));
  assert_eq!(feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"), EXEC_ABORT);
  assert_eq!(
    feed(&mut s, b"*2\r\n$3\r\nGET\r\n$5\r\npos_k\r\n"),
    b"$-1\r\n"
  );
}

/// 对照组 2（不回退）：MULTI → SELECT 3000000000（超 int32 字面量）双侧同
/// C# TryGetInt 失败档，保持落排队；EXEC 重放期执行臂单独落 not-integer 帧
/// （§32 已登记执行臂口径），SET 照常执行提交——负数档与超档文法档判据分离
#[test]
fn multi_select_over_i32_literal_stays_queued_and_exec_runs_arm() {
  let (mut s, _dir) = session();

  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*2\r\n$6\r\nSELECT\r\n$10\r\n3000000000\r\n*3\r\n$3\r\nSET\r\n$6\r\nover_k\r\n$1\r\n1\r\n",
    ),
    b"+OK\r\n+QUEUED\r\n+QUEUED\r\n",
    "超 int32 字面量不入中止臂（C# TryGetInt 失败档同形）"
  );
  assert_eq!(states(&s), (TxnState::Started, TxnState::Started));

  assert_eq!(
    feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"),
    [&b"*2\r\n"[..], NOT_INTEGER, b"+OK\r\n"].concat(),
    "重放遍 SELECT 执行臂单独落 not-integer 帧，SET 正常提交"
  );
  assert_eq!(states(&s), (TxnState::None, TxnState::None));
  assert_eq!(
    feed(&mut s, b"*2\r\n$3\r\nGET\r\n$6\r\nover_k\r\n"),
    b"$1\r\n1\r\n"
  );
}

/// 对照组 3：同库 SELECT（0 与负零 -0，strict_i32 归零后 == active_db_id）
/// 不入中止臂，排队后重放臂正常 +OK——判据仅裁「异库」面
#[test]
fn multi_select_same_db_and_negative_zero_stay_queued() {
  let (mut s, _dir) = session();

  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n*2\r\n$6\r\nSELECT\r\n$2\r\n-0\r\n*3\r\n$3\r\nSET\r\n$4\r\nz_k1\r\n$1\r\n1\r\n",
    ),
    b"+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n"
  );
  assert_eq!(states(&s), (TxnState::Started, TxnState::Started));
  assert_eq!(
    feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"),
    b"*3\r\n+OK\r\n+OK\r\n+OK\r\n"
  );
  assert_eq!(
    feed(&mut s, b"*2\r\n$3\r\nGET\r\n$4\r\nz_k1\r\n"),
    b"$1\r\n1\r\n"
  );
}
