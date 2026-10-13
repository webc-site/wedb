#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 事务排队与槽校验中 BITOP 伪子命令键规格回归测试
//!
//! 对标 C# 三处锚点的行为真值（键窗口一律按归一后命令 + isSubCommand -2 偏移）：
//! - TxnRespCommands.cs:115-128（NetworkSKIP：NormalizeForACLs 先行、
//!   `commandInfo.IsSubCommand || cmd == RespCommand.BITOP` 元数偏移）
//! - TxnKeyManager.cs:63-88（LockKeys：dest 排他 / src* 共享登记）
//! - RespServerSessionSlotVerify.cs:41-58、:92-109（CanServeSlot 两臂：归一
//!   重绑定后 isSubCommand 命中，dest 进入槽校验与集群事务校验键集）
//!
//! 期望值全部由 RespCommandsInfo.json 的 BITOP 表（Arity -4、KeySpec0
//! BeginSearch.Index 2 OW、KeySpec1 Index 3 LastKey -1 RO）与 RESP 帧协议
//! 公式推导：BITOP 操作 token（AND/NOT/…）被解析器消费后，args[0] 即 dest。

use std::{mem::take, sync::Arc};

use parking_lot::Mutex;
use wbase::map::HashSet;
use wnode::{
  ClusterSessionFace,
  cluster_session::{ClusterSlotVerificationInput, SlotVerifyGate},
  resp::{
    garnet_api::StoreGarnetApi,
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
};
use wnode_test::feed_session as feed;
use wresp::{catalog::extract_keys_from_slice, command::RespCommand};
use wtest_base::open_test_store;
use wtxn::{LockType, TxnLockTable, TxnState, WatchVersionMap};

/// 桩切面重定向应答（跨槽 dest 拦截判据帧）
const MOVED: &[u8] = b"-MOVED 0 stub:1\r\n";

/// 一次槽校验调用的快照（切面按会话侧传入的键规格与 is_sub_command 实际提取的键位）
#[derive(Debug, PartialEq)]
struct VerifyCall {
  is_sub_command: bool,
  read_only: bool,
  keys: Vec<Vec<u8>>,
}

/// 记录型集群切面桩：键提取复刻真实集群臂的判据源
/// （wedb cluster_session/slot_verify.rs 与 C# MultiKeySlotVerify 同形：
/// key_specs 空 = 入参键表直用，非空 = 按键规格 + is_sub_command 提取），
/// 命中 remote 集合的键即判跨槽回 MOVED
struct StubClusterSession {
  remote: Mutex<HashSet<Vec<u8>>>,
  calls: Mutex<Vec<VerifyCall>>,
  no_response_calls: Mutex<Vec<VerifyCall>>,
}

impl StubClusterSession {
  fn new() -> Self {
    Self {
      remote: Mutex::new(HashSet::default()),
      calls: Mutex::new(Vec::new()),
      no_response_calls: Mutex::new(Vec::new()),
    }
  }

  fn set_remote(&self, keys: &[&[u8]]) {
    *self.remote.lock() = keys.iter().map(|k| k.to_vec()).collect();
  }

  /// 取走并清空两路记录（相位隔离：布景命令的校验不参与断言）
  fn take_calls(&self) -> Vec<VerifyCall> {
    take(&mut *self.calls.lock())
  }

  fn take_no_response_calls(&self) -> Vec<VerifyCall> {
    take(&mut *self.no_response_calls.lock())
  }
}

fn extract(input: &ClusterSlotVerificationInput<'_>, args: &[&[u8]]) -> Vec<Vec<u8>> {
  if input.key_specs.is_empty() {
    args.iter().map(|k| k.to_vec()).collect()
  } else {
    extract_keys_from_slice(args, input.key_specs, input.is_sub_command)
      .iter()
      .map(|k| k.to_vec())
      .collect()
  }
}

impl StubClusterSession {
  /// 判定核：按会话侧传入的键规格提取键位并裁跨槽（两臂共用，C# 判定/渲染两分同构）
  fn evaluate(
    &self,
    input: &ClusterSlotVerificationInput<'_>,
    args: &[&[u8]],
  ) -> (VerifyCall, bool) {
    let keys = extract(input, args);
    let unservable = {
      let remote = self.remote.lock();
      keys.iter().any(|k| remote.contains(k.as_slice()))
    };
    (
      VerifyCall {
        is_sub_command: input.is_sub_command,
        read_only: input.read_only,
        keys,
      },
      unservable,
    )
  }
}

impl ClusterSessionFace for StubClusterSession {
  fn set_read_only_session(&self) {}
  fn set_read_write_session(&self) {}
  fn local_current_epoch(&self) -> i64 {
    0
  }
  fn acquire_current_epoch(&self) {}
  fn release_current_epoch(&self) {}

  fn network_multi_key_slot_verify(
    &self,
    input: &ClusterSlotVerificationInput<'_>,
    args: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> SlotVerifyGate {
    let (call, unservable) = self.evaluate(input, args);
    self.calls.lock().push(call);
    if unservable {
      output.extend_from_slice(MOVED);
      return SlotVerifyGate::Redirected;
    }
    SlotVerifyGate::Serve
  }

  fn network_multi_key_slot_verify_no_response(
    &self,
    input: &ClusterSlotVerificationInput<'_>,
    args: &[&[u8]],
  ) -> bool {
    let (call, unservable) = self.evaluate(input, args);
    self.no_response_calls.lock().push(call);
    unservable
  }

  fn process_cluster_commands(
    &self,
    _cmd: RespCommand,
    _args: &[&[u8]],
    output: &mut Vec<u8>,
    _slot: u16,
  ) -> bool {
    output.extend_from_slice(b"+STUBCLUSTER\r\n");
    true
  }

  fn dispose(&self) {}
}

/// 挂真实存储执行域与事务组件的会话（返回 TempDir 保活数据文件）
fn session_with_store() -> (RespServerSession, tempfile::TempDir) {
  let (dir, store) = open_test_store("txn-bitop-keyspec.db").unwrap();
  let mut s = RespServerSession::new(0, RespServerSessionOptions::default());
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));
  s.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  (s, dir)
}

fn txn(s: &RespServerSession) -> &wtxn::TransactionManager {
  s.txn_manager.as_ref().expect("事务组件已挂载")
}

/// C# 契约 1：BITOP NOT dest src（2 参数，归一后 isSubCommand 元数偏移生效）
/// 合法排队并成功提交；修复前 arity -3 判 2 < 3 误报参数错 → EXECABORT
#[test]
fn bitop_not_queues_and_executes_in_txn() {
  let (mut s, _dir) = session_with_store();
  // 布景：SET src ab（非事务直发）
  assert_eq!(
    feed(&mut s, b"*3\r\n$3\r\nSET\r\n$3\r\nsrc\r\n$2\r\nab\r\n"),
    b"+OK\r\n"
  );

  // MULTI -> BITOP NOT dest src -> EXEC（整批流水）
  let out = feed(
    &mut s,
    b"*1\r\n$5\r\nMULTI\r\n*4\r\n$5\r\nBITOP\r\n$3\r\nNOT\r\n$4\r\ndest\r\n$3\r\nsrc\r\n*1\r\n$4\r\nEXEC\r\n",
  );
  assert_eq!(
    out, b"+OK\r\n+QUEUED\r\n*1\r\n:2\r\n",
    "BITOP NOT 必须 +QUEUED 并以结果长度 2 提交（arity -4 经子命令偏移后为最少 2 参数）"
  );

  // 目的键真值：NOT(ab) = 9E DD
  assert_eq!(
    feed(&mut s, b"*2\r\n$3\r\nGET\r\n$4\r\ndest\r\n"),
    b"$2\r\n\x9e\x9d\r\n"
  );
}

/// C# 契约 2：缺源参数的 BITOP NOT dest（1 参数）仍须判元数错并中止事务；
/// 错误回显名取归一后命令名 BITOP（C# GetRespCommandName(normalized)，
/// 修复前误回显内部枚举名 BITOP_NOT）
#[test]
fn bitop_not_without_src_still_aborts_txn() {
  let (mut s, _dir) = session_with_store();
  let out = feed(
    &mut s,
    b"*1\r\n$5\r\nMULTI\r\n*3\r\n$5\r\nBITOP\r\n$3\r\nNOT\r\n$4\r\ndest\r\n*1\r\n$4\r\nEXEC\r\n",
  );
  assert_eq!(
    out,
    b"+OK\r\n-ERR wrong number of arguments for 'BITOP' command\r\n\
      -EXECABORT Transaction discarded because of previous errors.\r\n",
    "元数下界收紧到 2 后，1 参数仍须报 'BITOP' 参数错并中止事务"
  );
}

/// C# 契约 3：BITOP AND dest s1 s2 排队期键登记——dest 排他锁（KeySpec0
/// Index 2 OW，-2 偏移命中 args[0]）、src1/src2 共享锁（KeySpec1 Index 3
/// LastKey -1 RO）；修复前窗口错位成 src1 排他 + src2 共享，dest 裸奔
#[test]
fn bitop_and_txn_locks_dest_exclusive_srcs_shared() {
  let (mut s, _dir) = session_with_store();
  assert_eq!(
    feed(&mut s, b"*3\r\n$3\r\nSET\r\n$2\r\ns1\r\n$2\r\nab\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed(&mut s, b"*3\r\n$3\r\nSET\r\n$2\r\ns2\r\n$4\r\nabcd\r\n"),
    b"+OK\r\n"
  );

  feed(&mut s, b"*1\r\n$5\r\nMULTI\r\n");
  assert_eq!(
    feed(
      &mut s,
      b"*5\r\n$5\r\nBITOP\r\n$3\r\nAND\r\n$4\r\ndest\r\n$2\r\ns1\r\n$2\r\ns2\r\n",
    ),
    b"+QUEUED\r\n"
  );

  // 排队完成、EXEC 前：裸键+锁型按 C# LockKeys 槽序入 txn_keys 缓冲；
  // 哈希条目延至 EXEC 展开期按当前物理前缀现算（换代防穿透，
  // wtxn-multi-queued-lock-hash-stale-across-generation）
  assert_eq!(txn(&s).state, TxnState::Started);
  let queued: Vec<(Vec<u8>, LockType)> = txn(&s)
    .txn_keys
    .iter_with_lock()
    .map(|(k, t)| (k.to_vec(), t))
    .collect();
  assert_eq!(
    queued,
    vec![
      (b"dest".to_vec(), LockType::Exclusive),
      (b"s1".to_vec(), LockType::Shared),
      (b"s2".to_vec(), LockType::Shared),
    ],
    "dest 居首排他、src1/src2 随行共享（KeySpec0 命中 args[0] 槽序 + OW/RO 锁型），修复前 dest 整条链路缺席"
  );
  assert!(
    !txn(&s).is_read_only(),
    "含排他登记即非只读（key_entries 与 txn_keys 双面合取）"
  );
  // 排队期哈希条目恒空：展开期才入 key_entries 并同步置位 perform_writes
  assert_eq!(
    txn(&s).key_entries.count(),
    0,
    "排队期 key_entries 延后展开"
  );
  assert!(
    !txn(&s).perform_writes,
    "perform_writes 同步延后至展开期置位"
  );

  // EXEC 提交：结果 = max(src) 长度 4；AND 耗尽补零 → "ab" + 0x00 0x00
  assert_eq!(feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"), b"*1\r\n:4\r\n");
  assert_eq!(
    feed(&mut s, b"*2\r\n$3\r\nGET\r\n$4\r\ndest\r\n"),
    b"$4\r\nab\x00\x00\r\n"
  );
}

/// C# 契约 4（集群形态）：排队期 dest 进入事务集群校验键集（txn_keys 序
/// dest, s1, s2），EXEC 前 VerifyClusterTxnKeys 全键送切面校验
#[test]
fn cluster_txn_bitop_dest_enters_exec_verify_set() {
  let (mut s, _dir) = session_with_store();
  let stub = Arc::new(StubClusterSession::new());
  s.attach_cluster_session(stub.clone());

  assert_eq!(
    feed(&mut s, b"*3\r\n$3\r\nSET\r\n$2\r\ns1\r\n$2\r\nab\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed(&mut s, b"*3\r\n$3\r\nSET\r\n$2\r\ns2\r\n$4\r\nabcd\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*5\r\n$5\r\nBITOP\r\n$3\r\nAND\r\n$5\r\ncdest\r\n$2\r\ns1\r\n$2\r\ns2\r\n",
    ),
    b"+OK\r\n+QUEUED\r\n"
  );

  let keyed: Vec<&[u8]> = txn(&s).txn_keys.iter().collect();
  assert_eq!(
    keyed,
    vec![&b"cdest"[..], &b"s1"[..], &b"s2"[..]],
    "dest 必须进入集群事务校验键集（修复前仅 [s1, s2]）"
  );
  stub.take_calls();

  assert_eq!(feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"), b"*1\r\n:4\r\n");
  // EXEC 前校验恰一次，键表含 dest
  assert_eq!(
    stub.take_calls(),
    vec![VerifyCall {
      is_sub_command: false,
      read_only: false,
      keys: vec![b"cdest".to_vec(), b"s1".to_vec(), b"s2".to_vec()],
    }],
    "VerifyClusterTxnKeys 直传键表（无键规格臂）且覆盖 dest"
  );
}

/// C# 契约 5（集群形态直发命令）：CanServeSlot 臂归一重绑定后 isSubCommand
/// 命中 -2 偏移，dest 纳入槽校验；dest 跨槽即整命令 MOVED 拦截，不执行
#[test]
fn cluster_direct_bitop_dest_cross_slot_redirects() {
  let (mut s, _dir) = session_with_store();
  let stub = Arc::new(StubClusterSession::new());
  stub.set_remote(&[b"rdest"]);
  s.attach_cluster_session(stub.clone());

  assert_eq!(
    feed(&mut s, b"*3\r\n$3\r\nSET\r\n$2\r\ns1\r\n$2\r\nab\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed(&mut s, b"*3\r\n$3\r\nSET\r\n$2\r\ns2\r\n$4\r\nabcd\r\n"),
    b"+OK\r\n"
  );
  stub.take_calls();

  // BITOP AND rdest s1 s2：修复前提取 [s1, s2] → 放行误执行（应答 :4）
  assert_eq!(
    feed(
      &mut s,
      b"*5\r\n$5\r\nBITOP\r\n$3\r\nAND\r\n$5\r\nrdest\r\n$2\r\ns1\r\n$2\r\ns2\r\n",
    ),
    MOVED,
    "dest 跨槽必须整命令重定向拦截"
  );
  assert_eq!(
    stub.take_calls(),
    vec![VerifyCall {
      is_sub_command: true,
      read_only: false,
      keys: vec![b"rdest".to_vec(), b"s1".to_vec(), b"s2".to_vec()],
    }],
    "归一重绑定后 is_sub_command 命中，槽校验键位含 dest 且位移正确（-2 偏移）"
  );
}

/// C# 契约 6（集群形态事务）：EXEC 前校验键集覆盖 dest，dest 迁移/跨槽态
/// 下 EXEC 直接回 MOVED 拦截，事务不进入执行
#[test]
fn cluster_txn_exec_redirects_when_dest_cross_slot() {
  let (mut s, _dir) = session_with_store();
  let stub = Arc::new(StubClusterSession::new());
  stub.set_remote(&[b"xdest"]);
  s.attach_cluster_session(stub.clone());

  assert_eq!(
    feed(&mut s, b"*3\r\n$3\r\nSET\r\n$2\r\ns1\r\n$2\r\nab\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed(&mut s, b"*3\r\n$3\r\nSET\r\n$2\r\ns2\r\n$4\r\nabcd\r\n"),
    b"+OK\r\n"
  );
  assert_eq!(
    feed(
      &mut s,
      b"*1\r\n$5\r\nMULTI\r\n*5\r\n$5\r\nBITOP\r\n$3\r\nAND\r\n$5\r\nxdest\r\n$2\r\ns1\r\n$2\r\ns2\r\n",
    ),
    b"+OK\r\n+QUEUED\r\n"
  );
  stub.take_calls();

  assert_eq!(
    feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"),
    MOVED,
    "dest 跨槽时 EXEC 必须前置于执行被拦截（修复前键集缺 dest → 放行）"
  );
  assert_eq!(
    stub.take_calls(),
    vec![VerifyCall {
      is_sub_command: false,
      read_only: false,
      keys: vec![b"xdest".to_vec(), b"s1".to_vec(), b"s2".to_vec()],
    }]
  );
  assert_eq!(s.txn_state, TxnState::None);
  assert_eq!(txn(&s).state, TxnState::None);
}

/// C# 契约 7（无应答前视臂）：CanServeSlotNoResponse 归一重绑定后同走
/// -2 偏移，键提取覆盖 dest（RespServerSessionSlotVerify.cs:92 首行
/// NormalizeForACLs 的 rust 承接）
#[test]
fn cluster_no_response_bitop_arm_covers_dest() {
  let mut s = RespServerSession::new(0, RespServerSessionOptions::default());
  let stub = Arc::new(StubClusterSession::new());
  s.attach_cluster_session(stub.clone());

  s.recv_buffer
    .extend_from_slice(b"*5\r\n$5\r\nBITOP\r\n$3\r\nAND\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
  s.bytes_read = s.recv_buffer.len();
  assert_eq!(s.parse_command(), Some(RespCommand::BitopAnd));
  assert_eq!(s.parse_state.count, 3);

  // 全键本地：可服务
  assert!(s.can_serve_slot_no_response(RespCommand::BitopAnd));
  assert_eq!(
    stub.take_no_response_calls(),
    vec![VerifyCall {
      is_sub_command: true,
      read_only: false,
      keys: vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
    }],
    "无应答臂键提取须含 dest（args[0]），is_sub_command 归一后命中"
  );

  // dest 跨槽：本臂回不可服务裁决（true），仍零渲染
  stub.set_remote(&[b"a"]);
  assert!(!s.can_serve_slot_no_response(RespCommand::BitopAnd));
}
