//! 事务闭环集成测试：MULTI → QUEUED → EXEC 原子应用与 WATCH 失效中止
//!
//! 驱动形态：宿主分派器实现 [`RespCommandDispatch`]——事务族命令直入
//! [`TransactionManager`] 的 RESP 命令面，数据命令按事务状态分流（排队
//! 窗口走 [`TransactionManager::network_skip`] 校验登记，执行窗口真实
//! 应用）；宿主循环与 `RespServerSession::process_messages` 同构：
//! 解析一条 → 分派 → 光标推进，EXEC 的光标回退由此自然触发排队重放。

use std::sync::Arc;

use gxhash::HashMap as GxHashMap;

use crate::{
  resp::resp_server_session::{RespCommandDispatch, RespServerSession},
  storage::session::storage_session::StoreType,
  transaction::{
    transaction_manager::{TransactionManager, TxnProcedure, TxnState},
    txn_key_entry::LockType,
    txn_key_entry_comparison::TxnKeyEntryComparison,
    txn_key_manager::{TxnCommandKeys, TxnKeySpec},
    txn_resp_commands::{TxnProcHandle, TxnProcResolver, TxnQueuedCommandInfo},
    watch_version_map::WatchVersionMap,
  },
  types::RespCommand,
};

fn key_hash(key: &[u8]) -> i64 {
  TxnKeyEntryComparison::key_hash(key)
}

/// 单键键规格
fn key_spec(read_only: bool) -> TxnKeySpec {
  TxnKeySpec::new(0, 0, 1, read_only)
}

/// 宿主分派模拟：SET/DEL/GET 真应用，事务族命令路由事务管理器
struct HostDispatch {
  txn: TransactionManager,
  store: GxHashMap<Box<[u8]>, Box<[u8]>>,
}

impl HostDispatch {
  fn new(txn: TransactionManager) -> Self {
    Self {
      txn,
      store: GxHashMap::default(),
    }
  }

  fn set_info() -> TxnQueuedCommandInfo {
    TxnQueuedCommandInfo {
      name: "set".into(),
      arity: 3,
      allowed_in_txn: true,
      is_sub_command: false,
      keys: Some(TxnCommandKeys {
        store_type: StoreType::Main,
        key_specs: vec![key_spec(false)],
      }),
    }
  }

  fn get_info() -> TxnQueuedCommandInfo {
    TxnQueuedCommandInfo {
      name: "get".into(),
      arity: 2,
      allowed_in_txn: true,
      is_sub_command: false,
      keys: Some(TxnCommandKeys {
        store_type: StoreType::Main,
        key_specs: vec![key_spec(true)],
      }),
    }
  }

  fn get_apply(&self, session: &mut RespServerSession, args: &[&[u8]]) {
    match args.first().and_then(|key| self.store.get(*key)) {
      Some(value) => {
        session
          .output
          .extend_from_slice(format!("${}\r\n", value.len()).as_bytes());
        session.output.extend_from_slice(value);
        session.output.extend_from_slice(b"\r\n");
      }
      None => session.output.extend_from_slice(b"$-1\r\n"),
    }
  }
}

impl RespCommandDispatch for HostDispatch {
  fn dispatch(&mut self, session: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) {
    let skipping = self.txn.is_skipping_operations();
    match cmd {
      RespCommand::Multi => {
        self.txn.network_multi(session);
      }
      RespCommand::Exec => {
        self.txn.network_exec(session);
      }
      RespCommand::Discard => {
        self.txn.network_discard(session);
      }
      RespCommand::Watch | RespCommand::Watchms | RespCommand::Watchos => {
        // WATCH 不入排队面，直投监视（C# WATCH 在事务窗口内被 SKIP 拒绝）
        let _ = self.txn.common_watch(session, StoreType::All);
      }
      RespCommand::Unwatch => {
        self.txn.network_unwatch(session);
      }
      RespCommand::Set => {
        if skipping {
          let info = Self::set_info();
          self.txn.network_skip(session, cmd, Some(&info));
        } else {
          if args.len() == 2 {
            self.store.insert(args[0].into(), args[1].into());
          }
          session.output.extend_from_slice(b"+OK\r\n");
        }
      }
      RespCommand::Get => {
        if skipping {
          let info = Self::get_info();
          self.txn.network_skip(session, cmd, Some(&info));
        } else {
          self.get_apply(session, args);
        }
      }
      _ => {
        if skipping {
          self.txn.network_skip(session, cmd, None);
        }
      }
    }
  }
}

/// 最小 RESP 数组解析（集成测试专用；生产路径走 resp parser 域）。
///
/// 返回的 `args` 与 ArgSlice 指向 `tokens`（调用方存活期内有效）。
fn parse_one(session: &mut RespServerSession, tokens: &mut Vec<Vec<u8>>) -> Option<RespCommand> {
  let buffer = session.recv_buffer.clone();
  let mut cursor = session.read_head;

  let read_line = |buffer: &[u8], cursor: &mut usize| -> Option<Vec<u8>> {
    let start = *cursor;
    let mut idx = start;
    while idx + 1 < buffer.len() {
      if buffer[idx] == b'\r' && buffer[idx + 1] == b'\n' {
        let line = buffer[start..idx].to_vec();
        *cursor = idx + 2;
        return Some(line);
      }
      idx += 1;
    }
    None
  };

  let head = read_line(&buffer, &mut cursor)?;
  if head.first() != Some(&b'*') {
    return None;
  }
  let count = crate::objects::parse_utils::try_get_int(&head[1..])?;

  tokens.clear();
  for _ in 0..count.max(0) {
    let len_line = read_line(&buffer, &mut cursor)?;
    if len_line.first() != Some(&b'$') {
      return None;
    }
    let len = crate::objects::parse_utils::try_get_int(&len_line[1..])? as usize;
    if cursor + len + 2 > buffer.len() {
      return None;
    }
    tokens.push(buffer[cursor..cursor + len].to_vec());
    cursor += len + 2;
  }
  session.end_read_head = cursor;

  let name = String::from_utf8_lossy(tokens.first()?).to_uppercase();
  let cmd = match name.as_str() {
    "MULTI" => RespCommand::Multi,
    "EXEC" => RespCommand::Exec,
    "DISCARD" => RespCommand::Discard,
    "WATCH" => RespCommand::Watch,
    "UNWATCH" => RespCommand::Unwatch,
    "SET" => RespCommand::Set,
    "GET" => RespCommand::Get,
    "RUNTXP" => RespCommand::Runtxp,
    _ => RespCommand::None,
  };

  let arg_slices = tokens[1..]
    .iter()
    .map(|token| crate::arg_slice::ArgSlice::new(token.as_ptr(), token.len()))
    .collect::<Vec<_>>();
  session.parse_state.initialize_with_args(&arg_slices);
  Some(cmd)
}

/// 宿主循环：解析一条 → 分派 → 光标推进（与 process_messages 同构）
fn drive_manual(session: &mut RespServerSession, dispatch: &mut HostDispatch, wire: &[u8]) {
  session.recv_buffer.clear();
  session.recv_buffer.extend_from_slice(wire);
  session.bytes_read = session.recv_buffer.len();
  session.read_head = 0;
  // 输出跨批累积（网络发送面在真实宿主分批冲刷；测试同序断言）

  let mut tokens: Vec<Vec<u8>> = Vec::new();
  while session.bytes_read.saturating_sub(session.read_head) >= 4 {
    let before = session.read_head;
    let Some(cmd) = parse_one(session, &mut tokens) else {
      session.read_head = before;
      session.end_read_head = before;
      break;
    };
    let args = tokens[1..]
      .iter()
      .map(Vec::as_slice)
      .collect::<Vec<&[u8]>>();
    dispatch.dispatch(session, cmd, &args);
    // EXEC 排队回退落在 end_read_head；循环据此前进或重放
    session.read_head = session.end_read_head;
  }
}

/// RESP 数组编码
fn resp_array(tokens: &[&[u8]]) -> Vec<u8> {
  let mut wire = format!("*{}\r\n", tokens.len()).into_bytes();
  for token in tokens {
    wire.extend_from_slice(format!("${}\r\n", token.len()).as_bytes());
    wire.extend_from_slice(token);
    wire.extend_from_slice(b"\r\n");
  }
  wire
}

fn manager() -> TransactionManager {
  TransactionManager::new(Arc::new(WatchVersionMap::new(64)), None, false)
}

/// MULTI → SET ×2 → EXEC：+OK / +QUEUED ×2 / 数组头 + 重放执行应答，数据落库
#[test]
fn full_multi_exec_round_trip() {
  let mut dispatch = HostDispatch::new(manager());
  let mut session = RespServerSession::default();

  let mut wire = Vec::new();
  wire.extend_from_slice(&resp_array(&[b"SET", b"pre", b"x"])); // 事务外直执行
  wire.extend_from_slice(&resp_array(&[b"MULTI"]));
  wire.extend_from_slice(&resp_array(&[b"SET", b"a", b"1"]));
  wire.extend_from_slice(&resp_array(&[b"SET", b"b", b"2"]));
  wire.extend_from_slice(&resp_array(&[b"EXEC"]));

  drive_manual(&mut session, &mut dispatch, &wire);

  let out = String::from_utf8_lossy(&session.output);
  assert_eq!(
    out, "+OK\r\n+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n+OK\r\n",
    "事务外 SET + MULTI OK + 两条 QUEUED + EXEC 数组头 + 重放两条 SET 应答"
  );
  // 数据原子应用：EXEC 后两键就位
  assert_eq!(
    dispatch.store.get(b"a".as_slice()).map(Box::as_ref),
    Some(b"1".as_slice())
  );
  assert_eq!(
    dispatch.store.get(b"b".as_slice()).map(Box::as_ref),
    Some(b"2".as_slice())
  );
  assert_eq!(dispatch.txn.state, TxnState::None);
}

/// WATCH 的键在 EXEC 前被并发写入 → EXEC 中止（空数组），事务丢弃
#[test]
fn watch_invalidation_aborts_exec() {
  let version_map = Arc::new(WatchVersionMap::new(64));
  let mut dispatch = HostDispatch::new(TransactionManager::new(
    Arc::clone(&version_map),
    None,
    false,
  ));
  let mut session = RespServerSession::default();

  let watch_and_write = {
    let mut w = Vec::new();
    w.extend_from_slice(&resp_array(&[b"WATCH", b"sk"]));
    w.extend_from_slice(&resp_array(&[b"SET", b"sk", b"outside"]));
    w
  };
  let txn_part = {
    let mut w = Vec::new();
    w.extend_from_slice(&resp_array(&[b"MULTI"]));
    w.extend_from_slice(&resp_array(&[b"SET", b"sk", b"txn"]));
    w.extend_from_slice(&resp_array(&[b"EXEC"]));
    w
  };

  drive_manual(&mut session, &mut dispatch, &watch_and_write);
  // 并发写方推进被监视键版本（C# Tsavorite 函数面 IncrementVersion 的接线位）
  version_map.increment_version(key_hash(b"sk") as u64);
  drive_manual(&mut session, &mut dispatch, &txn_part);

  let out = String::from_utf8_lossy(&session.output);
  assert_eq!(out, "+OK\r\n+OK\r\n+OK\r\n+QUEUED\r\n*-1\r\n");
  // 事务内的写被丢弃：值保持并发写的结果
  assert_eq!(
    dispatch.store.get(b"sk".as_slice()).map(Box::as_ref),
    Some(b"outside".as_slice())
  );
  assert_eq!(dispatch.txn.state, TxnState::None);
}

/// WATCH 的键未被改动 → EXEC 正常执行
#[test]
fn watch_intact_key_lets_exec_proceed() {
  let version_map = Arc::new(WatchVersionMap::new(64));
  let mut dispatch = HostDispatch::new(TransactionManager::new(version_map, None, false));
  let mut session = RespServerSession::default();

  let mut wire = Vec::new();
  wire.extend_from_slice(&resp_array(&[b"WATCH", b"stable"]));
  wire.extend_from_slice(&resp_array(&[b"MULTI"]));
  wire.extend_from_slice(&resp_array(&[b"SET", b"stable", b"fresh"]));
  wire.extend_from_slice(&resp_array(&[b"EXEC"]));

  drive_manual(&mut session, &mut dispatch, &wire);

  let out = String::from_utf8_lossy(&session.output);
  assert_eq!(out, "+OK\r\n+OK\r\n+QUEUED\r\n*1\r\n+OK\r\n");
  assert_eq!(
    dispatch.store.get(b"stable".as_slice()).map(Box::as_ref),
    Some(b"fresh".as_slice())
  );
}

/// 事务内嵌套 MULTI 报错中止 → EXEC 报 EXECABORT 且排队写不落库
#[test]
fn nested_multi_then_exec_aborts_with_execabort() {
  let mut dispatch = HostDispatch::new(manager());
  let mut session = RespServerSession::default();

  let mut wire = Vec::new();
  wire.extend_from_slice(&resp_array(&[b"MULTI"]));
  wire.extend_from_slice(&resp_array(&[b"SET", b"a", b"1"]));
  wire.extend_from_slice(&resp_array(&[b"MULTI"])); // 嵌套 → 报错 + Abort
  wire.extend_from_slice(&resp_array(&[b"EXEC"])); // Aborted → EXECABORT

  drive_manual(&mut session, &mut dispatch, &wire);

  let out = String::from_utf8_lossy(&session.output);
  assert!(out.contains("+QUEUED"));
  assert!(out.contains("ERR MULTI calls can not be nested"));
  assert!(out.contains("EXECABORT"));
  assert!(dispatch.store.get(b"a".as_slice()).is_none());
  assert_eq!(dispatch.txn.state, TxnState::None);
}

/// DISCARD 清空排队；后续 EXEC 报 EXEC without MULTI
#[test]
fn discard_clears_queue() {
  let mut dispatch = HostDispatch::new(manager());
  let mut session = RespServerSession::default();

  let mut wire = Vec::new();
  wire.extend_from_slice(&resp_array(&[b"MULTI"]));
  wire.extend_from_slice(&resp_array(&[b"SET", b"a", b"1"]));
  wire.extend_from_slice(&resp_array(&[b"DISCARD"]));
  wire.extend_from_slice(&resp_array(&[b"EXEC"]));

  drive_manual(&mut session, &mut dispatch, &wire);

  let out = String::from_utf8_lossy(&session.output);
  assert!(out.starts_with("+OK\r\n+QUEUED\r\n+OK\r\n"));
  assert!(out.contains("ERR EXEC without MULTI"));
  assert!(dispatch.store.get(b"a".as_slice()).is_none());
}

/// 只读事务（GET）走共享锁：EXEC 正常返回执行应答
#[test]
fn read_only_txn_runs_with_shared_locks() {
  let mut dispatch = HostDispatch::new(manager());
  let ro_key: Box<[u8]> = Box::from(b"ro".as_slice());
  let ro_val: Box<[u8]> = Box::from(b"v".as_slice());
  dispatch.store.insert(ro_key, ro_val);
  let mut session = RespServerSession::default();

  let mut wire = Vec::new();
  wire.extend_from_slice(&resp_array(&[b"MULTI"]));
  wire.extend_from_slice(&resp_array(&[b"GET", b"ro"]));
  wire.extend_from_slice(&resp_array(&[b"EXEC"]));

  drive_manual(&mut session, &mut dispatch, &wire);

  let out = String::from_utf8_lossy(&session.output);
  assert_eq!(out, "+OK\r\n+QUEUED\r\n*1\r\n$1\r\nv\r\n");
}

/// 无 MULTI 的 EXEC / DISCARD 报错；UNWATCH 幂等 +OK
#[test]
fn txn_commands_outside_multi_error() {
  let mut dispatch = HostDispatch::new(manager());
  let mut session = RespServerSession::default();

  let mut wire = Vec::new();
  wire.extend_from_slice(&resp_array(&[b"EXEC"]));
  wire.extend_from_slice(&resp_array(&[b"DISCARD"]));
  wire.extend_from_slice(&resp_array(&[b"UNWATCH"]));

  drive_manual(&mut session, &mut dispatch, &wire);

  let out = String::from_utf8_lossy(&session.output);
  assert!(out.contains("ERR EXEC without MULTI"));
  assert!(out.contains("ERR DISCARD without MULTI"));
  assert!(out.ends_with("+OK\r\n"));
}

/// RUNTXP 经解析器路由：自定义过程三段式经事务管理器执行
#[test]
fn runtxp_runs_custom_procedure_in_transaction() {
  struct EchoProc;
  impl TxnProcedure for EchoProc {
    fn id(&self) -> u8 {
      3
    }
    fn prepare(&mut self, txn: &mut TransactionManager) -> bool {
      txn.save_key_entry_to_lock(b"proc-key", LockType::Exclusive);
      true
    }
    fn main(&mut self, _txn: &mut TransactionManager, output: &mut Vec<u8>) {
      output.extend_from_slice(b"PROC-DONE");
    }
    fn finalize(&mut self, _txn: &mut TransactionManager, _output: &mut Vec<u8>) {}
  }

  struct ProcResolver;
  impl TxnProcResolver for ProcResolver {
    fn get_custom_transaction_procedure(&self, txn_id: u8) -> Option<TxnProcHandle> {
      (txn_id == 3).then(|| TxnProcHandle {
        name: "echo-proc".into(),
        arity: 2,
      })
    }
    fn try_transaction_proc(
      &mut self,
      _txn_id: u8,
      txn_manager: &mut TransactionManager,
      session: &mut RespServerSession,
    ) -> bool {
      txn_manager.run_transaction_proc(&mut EchoProc, &mut session.output, false)
    }
  }

  let mut txn = manager();
  let mut session = RespServerSession::default();
  let wire = resp_array(&[b"RUNTXP", b"3", b"p1"]);
  session.recv_buffer = wire;
  session.bytes_read = session.recv_buffer.len();
  session.read_head = 0;

  let mut tokens = Vec::new();
  assert_eq!(
    parse_one(&mut session, &mut tokens),
    Some(RespCommand::Runtxp)
  );
  let mut resolver = ProcResolver;
  assert!(txn.network_runtxp(&mut session, &mut resolver));
  assert_eq!(session.output, b"PROC-DONE");
  assert_eq!(txn.state, TxnState::None);
  // 过程键经排他锁登记后随提交释放
  assert_eq!(txn.key_entries.count(), 0);
}
