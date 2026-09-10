//! 事务 RESP 命令面（对标 libs/server/Transaction/TxnRespCommands.cs —— C#
//! RespServerSession partial，Rust 侧为 [`TransactionManager`] 的跨文件
//! `impl` 块，逐调用传 `&mut RespServerSession`）
//!
//! 光标模型映射：C# `readHead/endReadHead` 对应会话 `read_head/end_read_head`；
//! NetworkMULTI 记录的 `txnStartHead` 在 C# 取自命令解析后的 readHead
//! （即下一条命令起点），托管模型下等价于 MULTI 处理完的 `end_read_head`。
//! EXEC 据此回退光标重放排队命令：第一遍（Started）排队校验，重放遍
//! （Running）真执行，末尾 EXEC 再次进入本面触发提交。

use std::time::Duration;

use super::{
  transaction_manager::{TransactionManager, TxnState},
  txn_key_manager::TxnCommandKeys,
};
use crate::{
  objects::parse_utils::try_get_int,
  resp::{
    cmd_strings::{
      GENERIC_ERR_WRONG_NUM_ARGS, RESP_ERR_GENERIC_UNK_CMD, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
      RESP_OK,
    },
    resp_server_session::RespServerSession,
  },
  storage::session::storage_session::StoreType,
  types::RespCommand,
};

/// libs/server/Resp/CmdStrings.cs:RESP_QUEUED
const RESP_QUEUED: &[u8] = b"+QUEUED\r\n";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_NESTED_MULTI
const RESP_ERR_GENERIC_NESTED_MULTI: &str = "ERR MULTI calls can not be nested";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_EXEC_ABORT
const RESP_ERR_EXEC_ABORT: &str = "EXECABORT Transaction discarded because of previous errors.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_EXEC_WO_MULTI
const RESP_ERR_GENERIC_EXEC_WO_MULTI: &str = "ERR EXEC without MULTI";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_DISCARD_WO_MULTI
const RESP_ERR_GENERIC_DISCARD_WO_MULTI: &str = "ERR DISCARD without MULTI";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_WATCH_IN_MULTI
const RESP_ERR_GENERIC_WATCH_IN_MULTI: &str = "ERR WATCH inside MULTI is not allowed";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_SELECT_IN_TXN_UNSUPPORTED
const RESP_ERR_SELECT_IN_TXN_UNSUPPORTED: &str =
  "ERR SELECT is currently unsupported inside a transaction.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_SWAPDB_IN_TXN_UNSUPPORTED
const RESP_ERR_SWAPDB_IN_TXN_UNSUPPORTED: &str =
  "ERR SWAPDB is currently unsupported inside a transaction.";
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_NO_TRANSACTION_PROCEDURE
const RESP_ERR_NO_TRANSACTION_PROCEDURE: &str = "ERR Could not get transaction procedure";
/// libs/server/Resp/CmdStrings.cs:GenericErrWrongNumArgsTxn
const GENERIC_ERR_WRONG_NUM_ARGS_TXN: &str =
  "ERR Invalid number of parameters to stored proc {0}, expected {1}, actual {2}";
/// libs/server/Resp/CmdStrings.cs:GenericErrCommandDisallowedWithOption
const GENERIC_ERR_COMMAND_DISALLOWED_WITH_OPTION: &str = "ERR {0} command not allowed. If the {1} option is set to \"local\", you can run it from a local connection, otherwise you need to set this option in the configuration file, and then restart the server.";

/// 排队命令元数据（C# SimpleRespCommandInfo 中 NetworkSKIP 所需子集的本域
/// 投影；宿主从 resp 命令信息域构建）
#[derive(Debug, Clone)]
pub struct TxnQueuedCommandInfo {
  /// 命令名（错误回显用）
  pub name: String,
  /// 元数（C# Arity；0 不校验 / 正值精确 / 负值至少）
  pub arity: i32,
  /// 是否允许出现在事务内（C# AllowedInTxn）
  pub allowed_in_txn: bool,
  /// 是否子命令（键参数窗口额外偏移；C# IsSubCommand，BITOP 同此论）
  pub is_sub_command: bool,
  /// 键登记元数据（C# KeySpecs 的检索窗口投影；None = 无键面）
  pub keys: Option<TxnCommandKeys>,
}

/// 自定义事务过程句柄（C# CustomTransactionProcedure 的元数据投影；
/// 执行体经 [`TxnProcResolver::try_transaction_proc`] 回调承接）
pub struct TxnProcHandle {
  /// 过程名
  pub name: String,
  /// 元数（C# arity；0 不校验 / 正值精确 / 负值至少）
  pub arity: i32,
}

/// 自定义事务过程解析面（C# customCommandManagerSession 的 RUNTXP 相关投影；
/// custom 域注册表接入时实现）
pub trait TxnProcResolver {
  /// 取注册的自定义事务过程（C# GetCustomTransactionProcedure；未注册为
  /// None，对应 C# 抛异常路径）
  fn get_custom_transaction_procedure(&self, txn_id: u8) -> Option<TxnProcHandle>;
  /// 执行过程三段式（C# TryTransactionProc → RunTransactionProc）；输出
  /// 写入会话输出缓冲
  fn try_transaction_proc(
    &mut self,
    txn_id: u8,
    txn_manager: &mut TransactionManager,
    session: &mut RespServerSession,
  ) -> bool;
}

/// 写协议错误应答
fn write_error(session: &mut RespServerSession, message: &str) {
  session.abort_error_message(message);
}

/// 写 `*N\r\n` 数组头（C# RespWriteUtils.WriteArrayLength）
fn write_array_length(session: &mut RespServerSession, count: usize) {
  session
    .output
    .extend_from_slice(format!("*{count}\r\n").as_bytes());
}

/// 写空数组 `*-1\r\n`（C# WriteNullArray）
fn write_null_array(session: &mut RespServerSession) {
  session.output.extend_from_slice(b"*-1\r\n");
}

impl TransactionManager {
  /// MULTI（libs/server/Transaction/TxnRespCommands.cs:NetworkMULTI）
  pub fn network_multi(&mut self, session: &mut RespServerSession) -> bool {
    if self.state != TxnState::None {
      write_error(session, RESP_ERR_GENERIC_NESTED_MULTI);
      self.abort();
      return true;
    }
    // C# txnStartHead = readHead（下一条命令起点）；托管模型取 MULTI
    // 处理完的 end_read_head
    self.txn_start_head = session.end_read_head;
    self.state = TxnState::Started;
    self.operation_cnt_txn = 0;
    // C# 记录 recvBufferPtr 供 EXEC 键指针修正；托管缓冲无重分配
    self.save_key_recv_buffer_ptr = None;

    session.output.extend_from_slice(RESP_OK);
    true
  }

  /// EXEC（libs/server/Transaction/TxnRespCommands.cs:NetworkEXEC）
  pub fn network_exec(&mut self, session: &mut RespServerSession) -> bool {
    // 执行中再次 EXEC：越过并提交（重放遍的收尾）
    if self.state == TxnState::Running {
      self.commit(false);
      return true;
    }

    // 中止态：EXECABORT
    if self.state == TxnState::Aborted {
      write_error(session, RESP_ERR_EXEC_ABORT);
      self.reset(false);
      self.watch_container.reset();
      return true;
    }

    // Started：回退光标重放排队命令
    if self.state == TxnState::Started {
      let orig_read_head = session.end_read_head;
      session.end_read_head = self.txn_start_head;

      if self.cluster_enabled {
        // 集群槽校验（C# clusterSession.NetworkMultiKeySlotVerify）；集群面
        // 接线前取输入并视为通过，失败路径与 C# 同款回退光标重置。
        let _ = self.get_slot_verification_input(session.session_asking);
      }

      let start_txn = self.run(false, false, Duration::ZERO);

      if start_txn {
        write_array_length(session, self.operation_cnt_txn);
      } else {
        session.end_read_head = orig_read_head;
        write_null_array(session);
      }
      return true;
    }

    // 无 MULTI 的 EXEC
    write_error(session, RESP_ERR_GENERIC_EXEC_WO_MULTI);
    true
  }

  /// 排队第一遍：跳过命令仅做校验与键登记
  ///
  /// libs/server/Transaction/TxnRespCommands.cs:NetworkSKIP
  ///
  /// `info` 为 None = 未知命令 / 不允许入事务（C# 同错回退）。
  pub fn network_skip(
    &mut self,
    session: &mut RespServerSession,
    cmd: RespCommand,
    info: Option<&TxnQueuedCommandInfo>,
  ) -> bool {
    // 元数据可用性（NormalizeForACLs 在托管面为恒等）
    let Some(command_info) = info.filter(|info| info.allowed_in_txn) else {
      write_error(session, RESP_ERR_GENERIC_UNK_CMD);
      self.abort();
      return true;
    };

    // 元数校验：命令名 token 不计入 parse_state.Count（±1 偏移）；子命令
    // 再偏移一次（BITOP 的操作位亦被解析器消费，同 C# 论）
    let count = session.parse_state.count;
    let mut arity = if command_info.arity > 0 {
      command_info.arity - 1
    } else {
      command_info.arity + 1
    };
    if command_info.is_sub_command || cmd == RespCommand::Bitop {
      arity = if arity > 0 { arity - 1 } else { arity + 1 };
    }
    let invalid_num_args = if arity > 0 {
      count != arity as usize
    } else {
      (count as i64) < -(arity as i64)
    };

    // WATCH 系不允许入事务（仅报错，不中止事务）
    let is_watch = matches!(
      cmd,
      RespCommand::Watch | RespCommand::Watchms | RespCommand::Watchos
    );
    // SELECT / SWAPDB 事务内不支持（跨库事务未启用）
    let is_multi_db_command = matches!(cmd, RespCommand::Select | RespCommand::Swapdb);

    if invalid_num_args || is_watch || is_multi_db_command {
      if is_watch {
        write_error(session, RESP_ERR_GENERIC_WATCH_IN_MULTI);
        return true;
      }

      if invalid_num_args {
        write_error(
          session,
          &GENERIC_ERR_WRONG_NUM_ARGS.replace("{0}", &command_info.name),
        );
        self.abort();
        return true;
      }

      match cmd {
        RespCommand::Swapdb => {
          write_error(session, RESP_ERR_SWAPDB_IN_TXN_UNSUPPORTED);
          self.abort();
          return true;
        }
        RespCommand::Select => {
          // C#：TryGetInt(0) 成功且 index != activeDbId 才报错中止
          if count > 0
            && let Some(index) = try_get_int(session.parse_state.get_arg_slice_by_ref(0).as_slice())
            && i64::from(index) != session.active_db_id
          {
            write_error(session, RESP_ERR_SELECT_IN_TXN_UNSUPPORTED);
            self.abort();
            return true;
          }
        }
        _ => {}
      }
    }

    if cmd == RespCommand::Debug && !session.can_run_debug() {
      let message = GENERIC_ERR_COMMAND_DISALLOWED_WITH_OPTION
        .replace("{0}", "DEBUG")
        .replace("{1}", "enable-debug-command");
      write_error(session, &message);
      self.abort();
      return true;
    }

    if self.cluster_enabled {
      // C#：接收缓冲重分配时把既有键拷入 scratch；托管键列表自带副本
      self.copy_existing_keys_to_scratch_buffer();
    }

    // 键登记（C# LockKeys(commandInfo, isSubCommand)）
    if let Some(keys) = &command_info.keys {
      self.lock_keys(session, keys);
    }

    session.output.extend_from_slice(RESP_QUEUED);
    self.operation_cnt_txn += 1;
    true
  }

  /// DISCARD（libs/server/Transaction/TxnRespCommands.cs:NetworkDISCARD）
  pub fn network_discard(&mut self, session: &mut RespServerSession) -> bool {
    if self.state == TxnState::None {
      write_error(session, RESP_ERR_GENERIC_DISCARD_WO_MULTI);
      return true;
    }
    session.output.extend_from_slice(RESP_OK);
    self.reset(false);
    self.watch_container.reset();
    true
  }

  /// WATCH 系共同实现
  ///
  /// libs/server/Transaction/TxnRespCommands.cs:CommonWATCH
  pub fn common_watch(&mut self, session: &mut RespServerSession, store_type: StoreType) -> bool {
    let count = session.parse_state.count;
    // 至少一个键（C# 以未格式化模板直接回错，1:1 保留）
    if count == 0 {
      write_error(session, GENERIC_ERR_WRONG_NUM_ARGS);
      return true;
    }

    self.add_transaction_store_type(store_type);

    for c in 0..count {
      let key = session.parse_state.get_arg_slice_by_ref(c);
      self.watch(key.as_slice());
    }

    session.output.extend_from_slice(RESP_OK);
    true
  }

  /// WATCH MS key [key ..]
  ///（libs/server/Transaction/TxnRespCommands.cs:NetworkWATCH_MS）
  pub fn network_watch_ms(&mut self, session: &mut RespServerSession) -> bool {
    self.common_watch(session, StoreType::Main)
  }

  /// WATCH OS key [key ..]
  ///（libs/server/Transaction/TxnRespCommands.cs:NetworkWATCH_OS）
  pub fn network_watch_os(&mut self, session: &mut RespServerSession) -> bool {
    self.common_watch(session, StoreType::Object)
  }

  /// WATCH key [key ...]
  ///（libs/server/Transaction/TxnRespCommands.cs:NetworkWATCH）
  pub fn network_watch(&mut self, session: &mut RespServerSession) -> bool {
    self.common_watch(session, StoreType::All)
  }

  /// UNWATCH（libs/server/Transaction/TxnRespCommands.cs:NetworkUNWATCH）
  pub fn network_unwatch(&mut self, session: &mut RespServerSession) -> bool {
    if self.state == TxnState::None {
      self.watch_container.reset();
    }
    session.output.extend_from_slice(RESP_OK);
    true
  }

  /// RUNTXP 快路径（libs/server/Transaction/TxnRespCommands.cs:NetworkRUNTXPFast）
  ///
  /// C# 从接收缓冲元数据槽直读参数计数；托管解析态已携带计数，直通慢路径。
  pub fn network_runtxp_fast(
    &mut self,
    session: &mut RespServerSession,
    resolver: &mut dyn TxnProcResolver,
  ) -> bool {
    self.network_runtxp(session, resolver)
  }

  /// RUNTXP id arg [arg ...]
  ///（libs/server/Transaction/TxnRespCommands.cs:NetworkRUNTXP）
  pub fn network_runtxp(
    &mut self,
    session: &mut RespServerSession,
    resolver: &mut dyn TxnProcResolver,
  ) -> bool {
    let count = session.parse_state.count;
    if count < 1 {
      session.abort_wrong_num_args("runtxp");
      return true;
    }

    let first = session.parse_state.get_arg_slice_by_ref(0);
    let Some(tx_id) = try_get_int(first.as_slice()) else {
      write_error(session, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return true;
    };

    // 取过程（C# GetCustomTransactionProcedure 抛异常 → 同错回退）
    let Some(proc) = resolver.get_custom_transaction_procedure(tx_id as u8) else {
      write_error(session, RESP_ERR_NO_TRANSACTION_PROCEDURE);
      return true;
    };

    // 元数校验：首参为过程 id，不计入过程参数
    if (proc.arity > 0 && count != proc.arity as usize)
      || (proc.arity < 0 && (count as i64) < -(proc.arity as i64))
    {
      let expected_params = if proc.arity > 0 {
        proc.arity - 1
      } else {
        -proc.arity - 1
      };
      write_error(
        session,
        &GENERIC_ERR_WRONG_NUM_ARGS_TXN
          .replace("{0}", &tx_id.to_string())
          .replace("{1}", &expected_params.to_string())
          .replace("{2}", &(count - 1).to_string()),
      );
      return true;
    }

    // 执行过程三段式（C# TryTransactionProc）
    resolver.try_transaction_proc(tx_id as u8, self, session);
    true
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use super::{
    super::{
      transaction_manager::TransactionManager, txn_key_manager::TxnKeySpec,
      watch_version_map::WatchVersionMap,
    },
    *,
  };
  use crate::{arg_slice::ArgSlice, resp::resp_server_session::RespServerSession};

  fn manager() -> TransactionManager {
    TransactionManager::new(Arc::new(WatchVersionMap::new(64)), None, false)
  }

  /// 带参数解析态的会话（参数缓冲先聚合后取指针，避免扩容失效）
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

  #[test]
  fn multi_then_nested_multi_aborts() {
    let mut txn = manager();
    let mut session = RespServerSession::default();
    assert!(txn.network_multi(&mut session));
    assert_eq!(txn.state, TxnState::Started);
    assert_eq!(session.output, b"+OK\r\n");

    session.output.clear();
    assert!(txn.network_multi(&mut session));
    assert_eq!(txn.state, TxnState::Aborted);
    assert_eq!(
      session.output,
      format!("-{RESP_ERR_GENERIC_NESTED_MULTI}\r\n").as_bytes()
    );
  }

  #[test]
  fn exec_without_multi_errors() {
    let mut txn = manager();
    let mut session = RespServerSession::default();
    assert!(txn.network_exec(&mut session));
    assert_eq!(
      session.output,
      format!("-{RESP_ERR_GENERIC_EXEC_WO_MULTI}\r\n").as_bytes()
    );
  }

  #[test]
  fn discard_without_multi_errors() {
    let mut txn = manager();
    let mut session = RespServerSession::default();
    assert!(txn.network_discard(&mut session));
    assert_eq!(
      session.output,
      format!("-{RESP_ERR_GENERIC_DISCARD_WO_MULTI}\r\n").as_bytes()
    );
    assert_eq!(txn.state, TxnState::None);

    // 事务内 DISCARD 复位
    session.output.clear();
    assert!(txn.network_multi(&mut session));
    session.output.clear();
    assert!(txn.network_discard(&mut session));
    assert_eq!(session.output, b"+OK\r\n");
    assert_eq!(txn.state, TxnState::None);
  }

  #[test]
  fn skip_queues_and_counts() {
    let mut txn = manager();
    let (mut session, _buffer) = session_with_args(&[b"k1", b"v1"]);
    let info = TxnQueuedCommandInfo {
      name: "set".into(),
      arity: 3, // SET k v → 3 token（含命令名）
      allowed_in_txn: true,
      is_sub_command: false,
      keys: Some(TxnCommandKeys {
        store_type: StoreType::Main,
        key_specs: vec![TxnKeySpec::new(0, 0, 1, false)],
      }),
    };
    assert!(txn.network_skip(&mut session, RespCommand::Set, Some(&info)));
    assert_eq!(session.output, RESP_QUEUED);
    assert_eq!(txn.operation_cnt_txn, 1);
    assert_eq!(txn.key_entries.count(), 1);
    assert!(txn.perform_writes);
  }

  #[test]
  fn skip_unknown_command_aborts() {
    let mut txn = manager();
    let (mut session, _buffer) = session_with_args(&[]);
    assert!(txn.network_skip(&mut session, RespCommand::Invalid, None));
    assert_eq!(txn.state, TxnState::Aborted);
    assert!(String::from_utf8_lossy(&session.output).contains("unknown command"));
  }

  #[test]
  fn skip_watch_inside_multi_errors_without_abort() {
    let mut txn = manager();
    let (mut session, _buffer) = session_with_args(&[b"k"]);
    txn.state = TxnState::Started;
    let watch_info = TxnQueuedCommandInfo {
      name: "watch".into(),
      arity: -2,
      allowed_in_txn: true,
      is_sub_command: false,
      keys: None,
    };
    assert!(txn.network_skip(&mut session, RespCommand::Watch, Some(&watch_info)));
    assert_eq!(txn.state, TxnState::Started); // 未中止
    assert_eq!(txn.operation_cnt_txn, 0); // 也未排队
    assert!(String::from_utf8_lossy(&session.output).contains("WATCH inside MULTI is not allowed"));
  }

  #[test]
  fn skip_wrong_arity_aborts() {
    let mut txn = manager();
    let (mut session, _buffer) = session_with_args(&[b"only-key"]);
    let info = TxnQueuedCommandInfo {
      name: "rpush".into(),
      arity: -3, // 至少 2 参数
      allowed_in_txn: true,
      is_sub_command: false,
      keys: None,
    };
    assert!(txn.network_skip(&mut session, RespCommand::Rpush, Some(&info)));
    assert_eq!(txn.state, TxnState::Aborted);
    assert!(String::from_utf8_lossy(&session.output).contains("wrong number of arguments"));
  }

  #[test]
  fn swapdb_inside_multi_aborts() {
    let mut txn = manager();
    let (mut session, _buffer) = session_with_args(&[b"0", b"1"]);
    let info = TxnQueuedCommandInfo {
      name: "swapdb".into(),
      arity: 3,
      allowed_in_txn: true,
      is_sub_command: false,
      keys: None,
    };
    assert!(txn.network_skip(&mut session, RespCommand::Swapdb, Some(&info)));
    assert_eq!(txn.state, TxnState::Aborted);
    assert!(String::from_utf8_lossy(&session.output).contains("SWAPDB is currently unsupported"));
  }

  #[test]
  fn unwatch_resets_watches_when_idle() {
    let mut txn = manager();
    txn.watch(b"k");
    let mut session = RespServerSession::default();
    assert!(txn.network_unwatch(&mut session));
    assert_eq!(session.output, b"+OK\r\n");
    assert!(txn.watch_container.validate_watch_version());
  }

  /// RUNTXP 解析器：注册 id=7 的过程（元数 2 = id + 1 过程参数）
  struct MockResolver;
  impl TxnProcResolver for MockResolver {
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
      session.output.extend_from_slice(b"PROC-MAIN");
      true
    }
  }

  #[test]
  fn runtxp_routes_through_resolver() {
    let mut txn = manager();
    let (mut session, _buffer) = session_with_args(&[b"7", b"a1"]);
    assert!(txn.network_runtxp(&mut session, &mut MockResolver));
    assert_eq!(session.output, b"PROC-MAIN");
  }

  #[test]
  fn runtxp_unknown_procedure_errors() {
    let mut txn = manager();
    let (mut session, _buffer) = session_with_args(&[b"99"]);
    assert!(txn.network_runtxp(&mut session, &mut MockResolver));
    assert!(
      String::from_utf8_lossy(&session.output).contains("Could not get transaction procedure")
    );
  }

  #[test]
  fn runtxp_non_integer_id_errors() {
    let mut txn = manager();
    let (mut session, _buffer) = session_with_args(&[b"abc"]);
    assert!(txn.network_runtxp(&mut session, &mut MockResolver));
    assert!(String::from_utf8_lossy(&session.output).contains("not an integer"));
  }
}
