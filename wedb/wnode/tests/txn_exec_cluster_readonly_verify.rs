//! EXEC 集群槽校验只读态贯通回归测试（对标 C# NetworkEXEC →
//! TransactionManager.GetSlotVerificationInput `readOnly = keyEntries.IsReadOnly`
//! → ClusterSlotVerify 各消费臂）
//!
//! 修复前 `verify_cluster_txn_keys` 在 `with_txn_manager` 借出窗口内回读会话
//! `txn_manager`（恒 None），`read_only` 恒 false：
//! 1. 副本 READONLY 会话的只读事务被恒 MOVED（C# ClusterSlotVerify.cs:29
//!    SingleKeyReadSlotVerify `IsLocal(enableReplicaReads: readOnlySession)` 放行）；
//! 2. MIGRATING TRANSMITTING 传输窗只读事务被判不可访问 → Wait → TRYAGAIN
//!    （C# MigrateSessionKeyAccess.cs:57 `TRANSMITTING => readOnly` 读放行）。
//!
//! 修复后只读态由被借出的 TransactionManager 本体单点求值经形参传入，
//! 切面按 `input.read_only` 复演 C# 判定核即可裁决真值。
//!
//! 桩切面判定核逐条复刻 wedb cluster_manager_slot_gate 的三处消费点语义
//! （replica_reads 合取、TRANSMITTING 读放行、扫描窗 !read_only 拦门），
//! 非假 mock：会话侧键表/只读位/应答帧全走真实链路。

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
use wnode_test::drain_output;
use wresp::{catalog::extract_keys_from_slice, command::RespCommand};
use wtest_base::open_test_store;
use wtxn::{TxnLockTable, TxnState, WatchVersionMap};

/// 一次槽校验调用的快照（记录会话侧贯通传入的只读态与键表）
#[derive(Debug, Clone, PartialEq)]
struct VerifyCall {
  read_only: bool,
  keys: Vec<Vec<u8>>,
}

/// C# ClusterSlotVerify.cs:29 副本读放行臂的判定核投影：
/// `replica_reads = input.read_only && read_only_session`，只读事务 +
/// READONLY 会话本地服务，否则 MOVED 重定向至主
fn replica_gate(input: &ClusterSlotVerificationInput<'_>, read_only_session: bool) -> bool {
  input.read_only && read_only_session
}

/// 复演集群切面判定核的测试桩：`remote_owned` 模拟槽位属主为异地主节点
/// （副本形态），`transmitting` 模拟 MIGRATING TRANSMITTING 传输窗键集
/// （C# MigrateSessionKeyAccess.cs:57），`scan_window` 模拟无盘全量同步
/// 扫描窗（只读放行、写拦门）
struct StubClusterSession {
  remote_owned: Mutex<HashSet<Vec<u8>>>,
  transmitting: Mutex<HashSet<Vec<u8>>>,
  scan_window: Mutex<HashSet<Vec<u8>>>,
  read_only_session: Mutex<bool>,
  calls: Mutex<Vec<VerifyCall>>,
}

impl StubClusterSession {
  fn new() -> Self {
    Self {
      remote_owned: Mutex::new(HashSet::default()),
      transmitting: Mutex::new(HashSet::default()),
      scan_window: Mutex::new(HashSet::default()),
      read_only_session: Mutex::new(false),
      calls: Mutex::new(Vec::new()),
    }
  }

  fn set_remote_owned(&self, keys: &[&[u8]]) {
    *self.remote_owned.lock() = keys.iter().map(|k| k.to_vec()).collect();
  }

  fn set_transmitting(&self, keys: &[&[u8]]) {
    *self.transmitting.lock() = keys.iter().map(|k| k.to_vec()).collect();
  }

  fn set_scan_window(&self, keys: &[&[u8]]) {
    *self.scan_window.lock() = keys.iter().map(|k| k.to_vec()).collect();
  }

  fn take_calls(&self) -> Vec<VerifyCall> {
    take(&mut *self.calls.lock())
  }

  /// 判定核：键表逐键过三窗（事务臂键表直用；单命令臂按键规格提取）
  fn evaluate(
    &self,
    input: &ClusterSlotVerificationInput<'_>,
    args: &[&[u8]],
  ) -> (VerifyCall, Option<GateDecision>) {
    let keys: Vec<Vec<u8>> = if input.key_specs.is_empty() {
      args.iter().map(|k| k.to_vec()).collect()
    } else {
      extract_keys_from_slice(args, input.key_specs, input.is_sub_command)
        .iter()
        .map(|k| k.to_vec())
        .collect()
    };
    let call = VerifyCall {
      read_only: input.read_only,
      keys: keys.clone(),
    };
    let remote = self.remote_owned.lock();
    let transmitting = self.transmitting.lock();
    let scan = self.scan_window.lock();
    let decision = keys.iter().find_map(|k| {
      let ks = k.as_slice();
      if remote.contains(ks) && !replica_gate(input, *self.read_only_session.lock()) {
        return Some(GateDecision::Moved);
      }
      if transmitting.contains(ks) {
        // C# MigrateSessionKeyAccess TRANSMITTING：读放行，写 Wait（传输窗
        // 数据未齐，等待推进；事务域 Wait 投影为 TRYAGAIN）
        return if input.read_only {
          None
        } else {
          Some(GateDecision::Wait)
        };
      }
      if scan.contains(ks) && !input.read_only {
        // C# 无盘全量同步扫描窗：读命令放行（读不产 AOF 记录），写拦门
        return Some(GateDecision::Wait);
      }
      None
    });
    (call, decision)
  }
}

enum GateDecision {
  Moved,
  Wait,
}

impl ClusterSessionFace for StubClusterSession {
  fn set_read_only_session(&self) {
    *self.read_only_session.lock() = true;
  }
  fn set_read_write_session(&self) {
    *self.read_only_session.lock() = false;
  }
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
    let (call, decision) = self.evaluate(input, args);
    self.calls.lock().push(call);
    match decision {
      None => SlotVerifyGate::Serve,
      Some(GateDecision::Moved) => {
        output.extend_from_slice(b"-MOVED 0 127.0.0.1:7379\r\n");
        SlotVerifyGate::Redirected
      }
      // Wait 态真实切面登记挂起等待体；事务域由会话 Wait 臂统一写 TRYAGAIN
      Some(GateDecision::Wait) => SlotVerifyGate::Wait,
    }
  }

  fn network_multi_key_slot_verify_no_response(
    &self,
    input: &ClusterSlotVerificationInput<'_>,
    args: &[&[u8]],
  ) -> bool {
    let (call, decision) = self.evaluate(input, args);
    self.calls.lock().push(call);
    decision.is_some()
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

/// 挂真实存储执行域与事务组件的会话 + 桩切面（返回 TempDir 保活数据文件）
fn session_with_cluster_stub() -> (
  RespServerSession,
  Arc<StubClusterSession>,
  tempfile::TempDir,
) {
  let (dir, store) = open_test_store("txn-exec-cluster-readonly.db").unwrap();
  let mut s = RespServerSession::new(0, RespServerSessionOptions::default());
  s.set_garnet_api(Arc::new(StoreGarnetApi::new(store.new_session().unwrap())));
  s.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  let stub = Arc::new(StubClusterSession::new());
  s.attach_cluster_session(stub.clone());
  (s, stub, dir)
}

/// 喂一整批帧并冲出应答（消费循环同步闭环）
fn feed(s: &mut RespServerSession, frame: &[u8]) -> Vec<u8> {
  s.recv_buffer.extend_from_slice(frame);
  let consumed = s.try_consume_messages();
  assert!(consumed.is_some(), "帧应被完整消费: {frame:?}");
  drain_output(s)
}

const MULTI: &[u8] = b"*1\r\n$5\r\nMULTI\r\n";
const EXEC: &[u8] = b"*1\r\n$4\r\nEXEC\r\n";

/// 帧拼接（流水整批）
fn frames(parts: &[&[u8]]) -> Vec<u8> {
  parts.concat()
}

fn get_cmd(key: &[u8]) -> Vec<u8> {
  let mut v = format!("*2\r\n$3\r\nGET\r\n${}\r\n", key.len()).into_bytes();
  v.extend_from_slice(key);
  v.extend_from_slice(b"\r\n");
  v
}

fn set_cmd(key: &[u8], val: &[u8]) -> Vec<u8> {
  let mut v = format!("*3\r\n$3\r\nSET\r\n${}\r\n", key.len()).into_bytes();
  v.extend_from_slice(key);
  v.extend_from_slice(format!("\r\n${}\r\n", val.len()).as_bytes());
  v.extend_from_slice(val);
  v.extend_from_slice(b"\r\n");
  v
}

/// 断言点 1（票面测试验证点 1，对标 C# ClusterSlotVerify.cs:29）：
/// 副本 READONLY 会话 MULTI + GET + EXEC 只读事务本地服务，不 MOVED；
/// 修复前 read_only 恒 false → replica_gate 恒拒 → 恒 MOVED（本测试红）
#[test]
fn replica_readonly_txn_exec_served_locally() {
  let (mut s, stub, _dir) = session_with_cluster_stub();
  // 布景：rkey 槽位属主为异地主节点（本地为副本）
  stub.set_remote_owned(&[b"rkey"]);
  assert_eq!(
    feed(&mut s, b"*1\r\n$8\r\nREADONLY\r\n").as_slice(),
    b"+OK\r\n"
  );
  stub.take_calls();

  let out = feed(&mut s, &frames(&[MULTI, &get_cmd(b"rkey"), EXEC]));
  assert_eq!(
    out, b"+OK\r\n+QUEUED\r\n*1\r\n$-1\r\n",
    "只读事务 EXEC 必须本地服务（GET 未命中回 nil），修复前恒 MOVED"
  );
  assert_eq!(
    stub.take_calls(),
    vec![VerifyCall {
      read_only: true,
      keys: vec![b"rkey".to_vec()],
    }],
    "EXEC 前校验须把事务真只读态贯通至切面（借窗内回读恒 false 即回归）"
  );
  assert_eq!(s.txn_state, TxnState::None);
}

/// 对照组：同副本 READONLY 会话的写事务（MULTI + SET + EXEC）read_only
/// 恒 false，仍按 C# 语义 MOVED 重定向，修复不放水
#[test]
fn replica_readonly_write_txn_exec_still_moved() {
  let (mut s, stub, _dir) = session_with_cluster_stub();
  stub.set_remote_owned(&[b"wkey"]);
  feed(&mut s, b"*1\r\n$8\r\nREADONLY\r\n");
  stub.take_calls();

  let out = feed(&mut s, &frames(&[MULTI, &set_cmd(b"wkey", b"v"), EXEC]));
  assert_eq!(
    out, b"+OK\r\n+QUEUED\r\n-MOVED 0 127.0.0.1:7379\r\n",
    "含排他锁的事务非只读，副本上必须重定向"
  );
  assert_eq!(
    stub.take_calls(),
    vec![VerifyCall {
      read_only: false,
      keys: vec![b"wkey".to_vec()],
    }]
  );
  assert_eq!(s.txn_state, TxnState::None);
}

/// 断言点 2（票面测试验证点 2，对标 C# MigrateSessionKeyAccess.cs:57）：
/// MIGRATING TRANSMITTING 传输窗 MULTI + GET + EXEC 只读事务放行，
/// 不 TRYAGAIN；修复前恒按写裁决 → Wait → TRYAGAIN（本测试红）
#[test]
fn migrating_transmitting_readonly_txn_exec_not_tryagain() {
  let (mut s, stub, _dir) = session_with_cluster_stub();
  stub.set_transmitting(&[b"tkey"]);
  stub.take_calls();

  let out = feed(&mut s, &frames(&[MULTI, &get_cmd(b"tkey"), EXEC]));
  assert_eq!(
    out, b"+OK\r\n+QUEUED\r\n*1\r\n$-1\r\n",
    "TRANSMITTING 窗只读事务 EXEC 必须读放行（C# readOnly => true 臂）"
  );
  assert_eq!(
    stub.take_calls(),
    vec![VerifyCall {
      read_only: true,
      keys: vec![b"tkey".to_vec()],
    }]
  );
  assert_eq!(s.txn_state, TxnState::None);

  // 对照组：同窗写事务仍 Wait → TRYAGAIN，事务复位
  stub.take_calls();
  let out = feed(&mut s, &frames(&[MULTI, &set_cmd(b"tkey", b"v"), EXEC]));
  assert_eq!(
    out, b"+OK\r\n+QUEUED\r\n-TRYAGAIN Multiple keys request during rehashing of slot\r\n",
    "写事务在传输窗仍须 TRYAGAIN（修复只贯通真值，不改写路径裁决）"
  );
  assert_eq!(
    stub.take_calls(),
    vec![VerifyCall {
      read_only: false,
      keys: vec![b"tkey".to_vec()],
    }]
  );
  assert_eq!(s.txn_state, TxnState::None);
}

/// 断言点 3（危害面 (3)，扫描窗读放行设计意图）：无盘全量同步扫描窗内
/// 只读事务不被拦门，写事务拦门
#[test]
fn scan_window_readonly_txn_exec_passes_write_blocks() {
  let (mut s, stub, _dir) = session_with_cluster_stub();
  stub.set_scan_window(&[b"skey"]);
  stub.take_calls();

  let out = feed(&mut s, &frames(&[MULTI, &get_cmd(b"skey"), EXEC]));
  assert_eq!(
    out, b"+OK\r\n+QUEUED\r\n*1\r\n$-1\r\n",
    "扫描窗只读事务放行（读不产 AOF 记录），修复前被恒拦"
  );
  assert_eq!(
    stub.take_calls(),
    vec![VerifyCall {
      read_only: true,
      keys: vec![b"skey".to_vec()],
    }]
  );
}

/// 混合事务（GET + SET）非只读：键条目含排他即 read_only false
/// （C# KeyList.IsReadOnly 任一 Exclusive 即 false），切面按写裁决
#[test]
fn mixed_txn_exec_reports_not_read_only() {
  let (mut s, stub, _dir) = session_with_cluster_stub();
  stub.set_transmitting(&[b"mkey"]);
  stub.take_calls();

  let out = feed(
    &mut s,
    &frames(&[MULTI, &get_cmd(b"mkey"), &set_cmd(b"mkey", b"1"), EXEC]),
  );
  assert_eq!(
    out,
    b"+OK\r\n+QUEUED\r\n+QUEUED\r\n-TRYAGAIN Multiple keys request during rehashing of slot\r\n",
    "含 SET 的事务任一 Exclusive 条目即非只读，传输窗仍须 TRYAGAIN"
  );
  assert_eq!(
    stub.take_calls(),
    vec![VerifyCall {
      read_only: false,
      // txn_keys 自动去重（C# SaveKeyArgSlice 同键单条目），GET/SET 共键只登记一次
      keys: vec![b"mkey".to_vec()],
    }]
  );
  assert_eq!(s.txn_state, TxnState::None);
}
