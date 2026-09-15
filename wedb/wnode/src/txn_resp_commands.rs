//! 事务 RESP 命令面（对标 libs/server/Transaction/TxnRespCommands.cs —— C#
//! RespServerSession partial 的宿主侧承接，经 [`TxnRespCommandsExt`] 扩展
//! [`TransactionManager`]；逐调用传 `&mut impl TxnSession`）
//!
//! 光标模型映射：C# `readHead/endReadHead` 对应会话 `read_head/end_read_head`；
//! NetworkMULTI 记录的 `txnStartHead` 在 C# 取自命令解析后的 readHead
//! （即下一条命令起点），托管模型下等价于 MULTI 处理完的 `end_read_head`。
//! EXEC 据此回退光标重放排队命令：第一遍（Started）排队校验，重放遍
//! （Running）真执行，末尾 EXEC 再次进入本面触发提交。

use std::{sync::Arc, time::Duration};

use smallvec::SmallVec;
use wbase::num::strict_i32;
use wcustom::{CustomTransactionProcedure, SharedCustomCommandManager};
use wresp::{
  RespCommand,
  cmd_strings::{
    GENERIC_ERR_WRONG_NUM_ARGS, RESP_ERR_DEBUG_DISALLOWED, RESP_ERR_GENERIC_UNK_CMD,
    RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
  },
};
use wtxn::{
  StoreType, TransactionManager, TxnAofLog, TxnProcHandle, TxnProcResolver, TxnQueuedCommandInfo,
  TxnSession, TxnSlotVerifyFace, TxnState,
};

use crate::{
  aof::replaycoordinator::stored_proc_replay::stored_proc_args, cluster_session::ClusterSession,
  resp::resp_server_session::RespServerSession,
};

/// 迭代式槽位校验适配（会话集群切面 + ASKING 会话态绑定；C#
/// VerifyKeyOwnership 内取 respSession.SessionAsking 的等价物）
struct IterativeSlotVerifyAdapter {
  /// 会话集群切面句柄
  cluster: ClusterSession,
  /// 会话 ASKING 剩余计数（C# sessionAsking，非零即导入槽放行）
  session_asking: u8,
}

impl TxnSlotVerifyFace for IterativeSlotVerifyAdapter {
  fn reset_cached_slot_verification_result(&self) {
    self.cluster.reset_cached_slot_verification_result();
  }

  fn network_iterative_slot_verify(&self, key: &[u8], read_only: bool) -> bool {
    self
      .cluster
      .network_iterative_slot_verify(key, read_only, self.session_asking != 0)
  }

  fn write_cached_slot_verification_message(&self, output: &mut Vec<u8>) {
    self.cluster.write_cached_slot_verification_message(output);
  }
}

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
/// libs/server/Custom/CustomRespCommands.cs:TryTransactionProc 失败文案
const RESP_ERR_TRANSACTION_FAILED: &str = "ERR Transaction failed.";

/// 事务 RESP 命令面（C# RespServerSession partial 的扩展 trait 形态；
/// RESP 应答字节序列与 C# 1:1）
pub trait TxnRespCommandsExt<L: TxnAofLog = ()> {
  /// MULTI（libs/server/Transaction/TxnRespCommands.cs:NetworkMULTI）
  fn network_multi(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool;

  /// EXEC（libs/server/Transaction/TxnRespCommands.cs:NetworkEXEC）
  fn network_exec(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool;

  /// 排队第一遍：跳过命令仅做校验与键登记
  ///
  /// libs/server/Transaction/TxnRespCommands.cs:NetworkSKIP
  ///
  /// `info` 为 None = 未知命令 / 不允许入事务（C# 同错回退）。
  fn network_skip(
    &mut self,
    session: &mut (impl TxnSession + ?Sized),
    cmd: RespCommand,
    info: Option<&TxnQueuedCommandInfo>,
  ) -> bool;

  /// DISCARD（libs/server/Transaction/TxnRespCommands.cs:NetworkDISCARD）
  fn network_discard(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool;

  /// WATCH 系共同实现
  ///
  /// libs/server/Transaction/TxnRespCommands.cs:CommonWATCH
  fn common_watch(
    &mut self,
    session: &mut (impl TxnSession + ?Sized),
    store_type: StoreType,
  ) -> bool;

  /// WATCH MS key [key ..]
  ///（libs/server/Transaction/TxnRespCommands.cs:NetworkWATCH_MS）
  fn network_watch_ms(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool;

  /// WATCH OS key [key ..]
  ///（libs/server/Transaction/TxnRespCommands.cs:NetworkWATCH_OS）
  fn network_watch_os(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool;

  /// WATCH key [key ...]
  ///（libs/server/Transaction/TxnRespCommands.cs:NetworkWATCH）
  fn network_watch(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool;

  /// UNWATCH（libs/server/Transaction/TxnRespCommands.cs:NetworkUNWATCH）
  fn network_unwatch(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool;

  /// RUNTXP 快路径（libs/server/Transaction/TxnRespCommands.cs:NetworkRUNTXPFast）
  ///
  /// C# 从接收缓冲元数据槽直读参数计数；托管解析态已携带计数，直通慢路径。
  fn network_runtxp_fast<S: TxnSession + ?Sized>(
    &mut self,
    session: &mut S,
    resolver: &mut (impl TxnProcResolver<S, L> + ?Sized),
  ) -> bool;

  /// RUNTXP id arg [arg ...]
  ///（libs/server/Transaction/TxnRespCommands.cs:NetworkRUNTXP）
  fn network_runtxp<S: TxnSession + ?Sized>(
    &mut self,
    session: &mut S,
    resolver: &mut (impl TxnProcResolver<S, L> + ?Sized),
  ) -> bool;
}

impl<L: TxnAofLog> TxnRespCommandsExt<L> for TransactionManager<L> {
  fn network_multi(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool {
    if self.state != TxnState::None {
      session.write_error(RESP_ERR_GENERIC_NESTED_MULTI);
      self.abort();
      session.set_txn_state(TxnState::Aborted);
      return true;
    }
    // C# txnStartHead = readHead（下一条命令起点）；托管模型取 MULTI
    // 处理完的 end_read_head
    self.session_id = session.session_id();
    self.txn_start_head = session.end_read_head();
    self.state = TxnState::Started;
    session.set_txn_state(TxnState::Started);
    self.operation_cnt_txn = 0;

    session.write_ok();
    true
  }

  fn network_exec(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool {
    // 执行中再次 EXEC：越过并提交（重放遍的收尾）
    if self.state == TxnState::Running {
      self.commit(false);
      session.set_txn_state(TxnState::None);
      return true;
    }

    // 中止态：EXECABORT
    if self.state == TxnState::Aborted {
      session.write_error(RESP_ERR_EXEC_ABORT);
      self.reset(false);
      self.watch_container.reset();
      session.set_txn_state(TxnState::None);
      return true;
    }

    // Started：回退光标重放排队命令
    if self.state == TxnState::Started {
      self.session_id = session.session_id();
      let orig_read_head = session.end_read_head();
      session.set_end_read_head(self.txn_start_head);

      for key in self.watch_container.save_keys_to_key_list() {
        Self::push_txn_key(&mut self.txn_keys, key);
      }

      if !self.txn_keys.is_empty() {
        let verified = {
          let key_slices: SmallVec<[&[u8]; 8]> = self.txn_keys.iter().map(|k| k.as_ref()).collect();
          session.verify_cluster_txn_keys(&key_slices)
        };
        if !verified {
          self.reset(false);
          self.watch_container.reset();
          session.set_end_read_head(orig_read_head);
          session.set_txn_state(TxnState::None);
          return true;
        }
      }

      let start_txn = self.run(false, false, Duration::ZERO);

      if start_txn {
        session.set_txn_state(TxnState::Running);
        session.write_array_len(self.operation_cnt_txn);
      } else {
        session.set_txn_state(TxnState::None);
        session.set_end_read_head(orig_read_head);
        session.write_null_array();
      }
      return true;
    }

    // 无 MULTI 的 EXEC
    session.write_error(RESP_ERR_GENERIC_EXEC_WO_MULTI);
    true
  }

  fn network_skip(
    &mut self,
    session: &mut (impl TxnSession + ?Sized),
    cmd: RespCommand,
    info: Option<&TxnQueuedCommandInfo>,
  ) -> bool {
    // 元数据可用性（NormalizeForACLs 在托管面为恒等）
    let Some(command_info) = info.filter(|info| info.allowed_in_txn) else {
      session.write_error(RESP_ERR_GENERIC_UNK_CMD);
      self.abort();
      return true;
    };

    // 元数校验：command_info.arity == 0 表不校验；非 0 时命令名 token 不计入
    // parse_state.Count（±1 偏移）；子命令再偏移一次（BITOP 的操作位亦被解析器消费）
    let count = session.arg_count();
    let invalid_num_args = if command_info.arity != 0 {
      let mut arity = if command_info.arity > 0 {
        command_info.arity - 1
      } else {
        command_info.arity + 1
      };
      if command_info.is_sub_command || cmd == RespCommand::Bitop {
        arity = if arity > 0 { arity - 1 } else { arity + 1 };
      }
      if arity > 0 {
        count != arity as usize
      } else {
        (count as i64) < -(arity as i64)
      }
    } else {
      false
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
        session.write_error(RESP_ERR_GENERIC_WATCH_IN_MULTI);
        return true;
      }

      if invalid_num_args {
        session.abort_wrong_num_args(&command_info.name);
        self.abort();
        return true;
      }

      match cmd {
        RespCommand::Swapdb => {
          session.write_error(RESP_ERR_SWAPDB_IN_TXN_UNSUPPORTED);
          self.abort();
          return true;
        }
        RespCommand::Select => {
          // C#：TryGetInt(0) 成功且 index != activeDbId 才报错中止
          if count > 0
            && let Some(index) = strict_i32(session.get_arg(0))
            && index != session.active_db_id()
          {
            session.write_error(RESP_ERR_SELECT_IN_TXN_UNSUPPORTED);
            self.abort();
            return true;
          }
        }
        _ => {}
      }
    }

    if cmd == RespCommand::Debug && !session.can_run_debug() {
      session.write_error(RESP_ERR_DEBUG_DISALLOWED);
      self.abort();
      return true;
    }

    // 键登记（C# LockKeys(commandInfo, isSubCommand)）
    if let Some(keys) = &command_info.keys {
      self.lock_keys(session, keys);
    }

    session.write_queued();
    self.operation_cnt_txn += 1;
    true
  }

  fn network_discard(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool {
    if self.state == TxnState::None {
      session.write_error(RESP_ERR_GENERIC_DISCARD_WO_MULTI);
      return true;
    }
    session.write_ok();
    self.reset(false);
    self.watch_container.reset();
    session.set_txn_state(TxnState::None);
    true
  }

  /// WATCH 系共同实现
  ///
  /// libs/server/Transaction/TxnRespCommands.cs:CommonWATCH
  fn common_watch(
    &mut self,
    session: &mut (impl TxnSession + ?Sized),
    store_type: StoreType,
  ) -> bool {
    let count = session.arg_count();
    // 至少一个键（C# 以未格式化模板直接回错，1:1 保留）
    if count == 0 {
      session.write_error(GENERIC_ERR_WRONG_NUM_ARGS);
      return true;
    }

    self.add_transaction_store_type(store_type);

    (0..count).for_each(|c| {
      let key = session.get_arg(c);
      self.watch(key);
    });

    session.write_ok();
    true
  }

  /// WATCH MS key [key ..]
  ///（libs/server/Transaction/TxnRespCommands.cs:NetworkWATCH_MS）
  fn network_watch_ms(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool {
    self.common_watch(session, StoreType::Main)
  }

  /// WATCH OS key [key ..]
  ///（libs/server/Transaction/TxnRespCommands.cs:NetworkWATCH_OS）
  fn network_watch_os(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool {
    self.common_watch(session, StoreType::Object)
  }

  /// WATCH key [key ...]
  ///（libs/server/Transaction/TxnRespCommands.cs:NetworkWATCH）
  fn network_watch(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool {
    self.common_watch(session, StoreType::All)
  }

  fn network_unwatch(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool {
    if self.state == TxnState::None {
      self.watch_container.reset();
      self.txn_keys.clear();
    }
    session.write_ok();
    true
  }

  fn network_runtxp_fast<S: TxnSession + ?Sized>(
    &mut self,
    session: &mut S,
    resolver: &mut (impl TxnProcResolver<S, L> + ?Sized),
  ) -> bool {
    self.network_runtxp(session, resolver)
  }

  fn network_runtxp<S: TxnSession + ?Sized>(
    &mut self,
    session: &mut S,
    resolver: &mut (impl TxnProcResolver<S, L> + ?Sized),
  ) -> bool {
    self.session_id = session.session_id();
    let count = session.arg_count();
    if count < 1 {
      session.abort_wrong_num_args("RUNTXP");
      return true;
    }

    let first = session.get_arg(0);
    let Some(tx_id) = strict_i32(first) else {
      session.write_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return true;
    };

    // 过程 id 为注册字节位面（C# 注册表按其命中；越界 id 必不命中，走同
    // C# GetCustomTransactionProcedure 抛异常的 NO TRANSACTION PROCEDURE 错）
    let Ok(tx_id) = u8::try_from(tx_id) else {
      session.write_error(RESP_ERR_NO_TRANSACTION_PROCEDURE);
      return true;
    };

    // 取过程（C# GetCustomTransactionProcedure 抛异常 → 同错回退）
    let Some(proc) = resolver.get_custom_transaction_procedure(tx_id) else {
      session.write_error(RESP_ERR_NO_TRANSACTION_PROCEDURE);
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
      session.write_proc_param_error(tx_id, expected_params, count - 1);
      return true;
    }

    // 执行过程三段式（C# TryTransactionProc）
    resolver.try_transaction_proc(tx_id, self, session);
    true
  }
}

/// RUNTXP 过程解析器（C# customCommandManagerSession.GetCustomTransactionProcedure
/// + RespServerSession.TryTransactionProc 的会话侧承接）
pub(crate) struct SessionTxnProcResolver {
  /// 自定义命令注册表（C# storeWrapper.customCommandManager；未装配为 None）
  pub registry: Option<SharedCustomCommandManager>,
}

impl TxnProcResolver<RespServerSession> for SessionTxnProcResolver {
  fn get_custom_transaction_procedure(&self, txn_id: u8) -> Option<TxnProcHandle> {
    let registry = self.registry.as_ref()?;
    let guard = registry.read();
    let txn = guard.try_get_custom_transaction_procedure(txn_id)?;
    Some(TxnProcHandle {
      name: txn.name.clone(),
      arity: txn.arity,
    })
  }

  fn try_transaction_proc(
    &mut self,
    txn_id: u8,
    txn_manager: &mut TransactionManager,
    session: &mut RespServerSession,
  ) -> bool {
    // C# TryTransactionProc（libs/server/Custom/CustomRespCommands.cs:18）：
    // 指标计数 → 过程输入构造 → 三段式执行 → 输出落线（空输出成功回 +OK，
    // 失败回 "ERR Transaction failed."）
    if let Some(metrics) = &mut session.session_metrics {
      metrics.incr_total_transaction_commands_received(1);
    }

    // 实例化过程体（C# GetCustomTransactionProcedure 的 entry.proc()；
    // 工厂闭包自持存储句柄，AOF 回放 / 会话执行同径重建）
    let Some(mut proc) = self
      .registry
      .as_ref()
      .and_then(|registry| {
        registry
          .read()
          .try_get_custom_transaction_procedure(txn_id)
          .and_then(|txn| txn.factory)
      })
      .map(|factory| factory())
    else {
      session.write_error(RESP_ERR_NO_TRANSACTION_PROCEDURE);
      return true;
    };

    // 过程输入绑定（C# procInput 引用参数；首参为过程 id，不计入参数窗口）
    let args: Vec<Vec<u8>> = (1..session.arg_count())
      .map(|c| session.get_arg(c).to_vec())
      .collect();
    proc.bind_args(&args);

    // procInput 全量编码随 AOF StoredProcedure 条目落盘（C# Log 同款语义；
    // 回放侧 stored_proc_args::decode 重建参数序列）
    let mut proc_input = Vec::new();
    stored_proc_args::encode(&args, &mut proc_input);

    // 迭代式槽位校验切面注入（C# TxnKeyManager 构造期持 respSession 引用
    // 的等价形态：集群切面在场时把校验面交给事务管理器，单机 None 剥离；
    // session_asking 在此绑定，对标 C# VerifyKeyOwnership 取 respSession.SessionAsking）
    if let Some(cluster) = &session.cluster_session {
      txn_manager.set_slot_verifier(Arc::new(IterativeSlotVerifyAdapter {
        cluster: cluster.clone(),
        session_asking: session.session_asking,
      }));
    }

    let mut output = Vec::new();
    if txn_manager.run_transaction_proc(&mut proc, &proc_input, &mut output, false) {
      if output.is_empty() {
        session.write_ok();
      } else {
        session.output.extend_from_slice(&output);
      }
    } else {
      if let Some(metrics) = &mut session.session_metrics {
        metrics.incr_total_transaction_execution_failed(1);
      }
      if output.is_empty() {
        session.write_error(RESP_ERR_TRANSACTION_FAILED);
      } else {
        session.output.extend_from_slice(&output);
      }
    }
    true
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use itoa::Buffer;
  use wtxn::{LockType, TxnSession, WatchVersionMap};

  use super::*;

  #[derive(Debug, Default)]
  pub struct MockTxnSession {
    pub id: i32,
    pub args: Vec<Vec<u8>>,
    pub txn_state: TxnState,
    pub end_read_head: usize,
    pub protocol_version: u8,
    pub active_db: i32,
    pub allow_debug: bool,
    pub output: Vec<u8>,
    pub command_error_written: bool,
    pub cluster_slot_verify_fail: bool,
  }

  impl MockTxnSession {
    pub fn new() -> Self {
      Self::default()
    }
  }

  impl TxnSession for MockTxnSession {
    #[inline]
    fn session_id(&self) -> i32 {
      self.id
    }
    #[inline]
    fn arg_count(&self) -> usize {
      self.args.len()
    }
    #[inline]
    fn get_arg(&self, idx: usize) -> &[u8] {
      &self.args[idx]
    }
    #[inline]
    fn txn_state(&self) -> TxnState {
      self.txn_state
    }
    #[inline]
    fn set_txn_state(&mut self, state: TxnState) {
      self.txn_state = state;
    }
    #[inline]
    fn end_read_head(&self) -> usize {
      self.end_read_head
    }
    #[inline]
    fn set_end_read_head(&mut self, head: usize) {
      self.end_read_head = head;
    }
    #[inline]
    fn resp_protocol_version(&self) -> u8 {
      self.protocol_version
    }
    #[inline]
    fn active_db_id(&self) -> i32 {
      self.active_db
    }
    #[inline]
    fn can_run_debug(&self) -> bool {
      self.allow_debug
    }
    #[inline]
    fn write_ok(&mut self) {
      self.output.extend_from_slice(b"+OK\r\n");
    }
    #[inline]
    fn write_queued(&mut self) {
      self.output.extend_from_slice(b"+QUEUED\r\n");
    }
    #[inline]
    fn write_null_array(&mut self) {
      if self.protocol_version >= 3 {
        self.output.extend_from_slice(b"_\r\n");
      } else {
        self.output.extend_from_slice(b"*-1\r\n");
      }
    }
    #[inline]
    fn write_array_len(&mut self, count: usize) {
      let mut buf = Buffer::new();
      self.output.push(b'*');
      self.output.extend_from_slice(buf.format(count).as_bytes());
      self.output.extend_from_slice(b"\r\n");
    }
    #[inline]
    fn write_error(&mut self, message: &str) {
      self.output.extend_from_slice(b"-");
      self.output.extend_from_slice(message.as_bytes());
      self.output.extend_from_slice(b"\r\n");
    }
    #[inline]
    fn abort_wrong_num_args(&mut self, cmd_name: &str) {
      self
        .output
        .extend_from_slice(b"-ERR wrong number of arguments for '");
      self.output.extend_from_slice(cmd_name.as_bytes());
      self.output.extend_from_slice(b"' command\r\n");
    }
    #[inline]
    fn write_proc_param_error(&mut self, tx_id: u8, expected: i32, actual: usize) {
      let mut b0 = Buffer::new();
      let mut b1 = Buffer::new();
      let mut b2 = Buffer::new();
      self
        .output
        .extend_from_slice(b"-ERR Invalid number of parameters to stored proc ");
      self.output.extend_from_slice(b0.format(tx_id).as_bytes());
      self.output.extend_from_slice(b", expected ");
      self
        .output
        .extend_from_slice(b1.format(expected).as_bytes());
      self.output.extend_from_slice(b", actual ");
      self.output.extend_from_slice(b2.format(actual).as_bytes());
      self.output.extend_from_slice(b"\r\n");
      self.command_error_written = true;
    }

    #[inline]
    fn verify_cluster_txn_keys(&mut self, _keys: &[&[u8]]) -> bool {
      if self.cluster_slot_verify_fail {
        self
          .output
          .extend_from_slice(b"-CROSSSLOT Keys in request don't hash to the same slot\r\n");
        return false;
      }
      true
    }
  }

  fn manager() -> TransactionManager {
    TransactionManager::new(Arc::new(WatchVersionMap::new(64)), None)
  }

  #[test]
  fn exec_cluster_slot_verify_failure_resets_txn_and_watch() {
    let mut txn = manager();
    let mut session = MockTxnSession::new();
    session.cluster_slot_verify_fail = true;

    // WATCH key1
    txn.watch(b"key1");
    assert_eq!(txn.txn_keys.len(), 1);

    // MULTI
    assert!(txn.network_multi(&mut session));
    assert_eq!(txn.state, TxnState::Started);
    session.output.clear();

    // 记录待锁键
    txn.save_key_entry_to_lock(b"key2", LockType::Exclusive);
    assert_eq!(txn.txn_keys.len(), 2);

    // EXEC 触发集群槽位校验失败
    assert!(txn.network_exec(&mut session));
    assert_eq!(
      session.output,
      b"-CROSSSLOT Keys in request don't hash to the same slot\r\n"
    );
    assert_eq!(txn.state, TxnState::None);
    assert_eq!(session.txn_state, TxnState::None);
    assert!(txn.txn_keys.is_empty());
    assert!(txn.watch_container.save_keys_to_lock().next().is_none());
  }

  #[test]
  fn multi_exec_discard_lifecycle() {
    let mut txn = manager();
    let mut session = MockTxnSession::new();

    // 1. MULTI 启动
    assert!(txn.network_multi(&mut session));
    assert_eq!(txn.state, TxnState::Started);
    assert_eq!(session.txn_state, TxnState::Started);
    assert_eq!(session.output, b"+OK\r\n");
    session.output.clear();

    // 重复 MULTI 报错并置 Aborted
    assert!(txn.network_multi(&mut session));
    assert_eq!(txn.state, TxnState::Aborted);
    assert_eq!(session.txn_state, TxnState::Aborted);
    assert!(session.output.starts_with(b"-ERR MULTI"));
    session.output.clear();

    // 2. DISCARD 恢复
    assert!(txn.network_discard(&mut session));
    assert_eq!(txn.state, TxnState::None);
    assert_eq!(session.txn_state, TxnState::None);
    assert_eq!(session.output, b"+OK\r\n");
    session.output.clear();

    // 3. WATCH 与 UNWATCH
    session.args = vec![b"key1".to_vec(), b"key2".to_vec()];
    assert!(txn.network_watch(&mut session));
    assert_eq!(session.output, b"+OK\r\n");
    session.output.clear();

    assert!(txn.network_unwatch(&mut session));
    assert_eq!(session.output, b"+OK\r\n");
  }

  #[test]
  fn runtxp_resolves_registered_proc_end_to_end() {
    use std::sync::Arc;

    use parking_lot::RwLock;
    use wcustom::{CustomCommandManager, CustomTxnProc, DefaultTxnProc};

    fn mock_factory() -> CustomTxnProc {
      CustomTxnProc::Default(DefaultTxnProc { id: 0 })
    }

    let registry = Arc::new(RwLock::new(CustomCommandManager::new()));
    let id = registry
      .write()
      .register_transaction("mock-proc", Some(mock_factory), None, None)
      .unwrap();

    let mut s = RespServerSession::default();
    s.attach_transaction_components(Arc::new(WatchVersionMap::new(64)));
    s.attach_custom_command_manager(registry);

    // RUNTXP <id>：注册过的空事务过程 → 提交回 +OK
    //（test/standalone/Garnet.test.scripting/RespTransactionProcTests.cs:TransactionProcTest1）
    let frame = format!("*2\r\n$6\r\nRUNTXP\r\n$1\r\n{id}\r\n");
    assert!(s.try_consume_messages(frame.as_bytes()).is_some());
    assert_eq!(s.take_output(), b"+OK\r\n");

    // 未注册 id → C# GetCustomTransactionProcedure 抛异常同款错误
    assert!(
      s.try_consume_messages(b"*2\r\n$6\r\nRUNTXP\r\n$1\r\n9\r\n")
        .is_some()
    );
    assert_eq!(
      s.take_output(),
      b"-ERR Could not get transaction procedure\r\n"
    );
  }
}
