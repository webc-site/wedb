//! 事务 RESP 命令面（对标 libs/server/Transaction/TxnRespCommands.cs —— C#
//! RespServerSession partial，Rust 侧为 [`TransactionManager`] 的跨文件
//! `impl` 块，逐调用传 `&mut impl TxnSession`）
//!
//! 光标模型映射：C# `readHead/endReadHead` 对应会话 `read_head/end_read_head`；
//! NetworkMULTI 记录的 `txnStartHead` 在 C# 取自命令解析后的 readHead
//! （即下一条命令起点），托管模型下等价于 MULTI 处理完的 `end_read_head`。
//! EXEC 据此回退光标重放排队命令：第一遍（Started）排队校验，重放遍
//! （Running）真执行，末尾 EXEC 再次进入本面触发提交。

use std::time::Duration;

use wresp::{
  RespCommand,
  cmd_strings::{
    GENERIC_ERR_WRONG_NUM_ARGS, RESP_ERR_DEBUG_DISALLOWED, RESP_ERR_GENERIC_UNK_CMD,
    RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
  },
  strict_i32,
};

use crate::{
  StoreType,
  transaction_manager::{TransactionManager, TxnAofLog},
  txn_key_manager::TxnCommandKeys,
  txn_session::TxnSession,
  txn_state::TxnState,
};

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
pub trait TxnProcResolver<S: ?Sized, L: TxnAofLog = ()> {
  /// 取注册的自定义事务过程（C# GetCustomTransactionProcedure；未注册为
  /// None，对应 C# 抛异常路径）
  fn get_custom_transaction_procedure(&self, txn_id: u8) -> Option<TxnProcHandle>;
  /// 执行过程三段式（C# TryTransactionProc → RunTransactionProc）；输出
  /// 写入会话输出缓冲
  fn try_transaction_proc(
    &mut self,
    txn_id: u8,
    txn_manager: &mut TransactionManager<L>,
    session: &mut S,
  ) -> bool;
}

impl<L: TxnAofLog> TransactionManager<L> {
  /// MULTI（libs/server/Transaction/TxnRespCommands.cs:NetworkMULTI）
  pub fn network_multi(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool {
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

  /// EXEC（libs/server/Transaction/TxnRespCommands.cs:NetworkEXEC）
  pub fn network_exec(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool {
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

  /// 排队第一遍：跳过命令仅做校验与键登记
  ///
  /// libs/server/Transaction/TxnRespCommands.cs:NetworkSKIP
  ///
  /// `info` 为 None = 未知命令 / 不允许入事务（C# 同错回退）。
  pub fn network_skip(
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

  /// DISCARD（libs/server/Transaction/TxnRespCommands.cs:NetworkDISCARD）
  pub fn network_discard(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool {
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
  pub fn common_watch(
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
  pub fn network_watch_ms(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool {
    self.common_watch(session, StoreType::Main)
  }

  /// WATCH OS key [key ..]
  ///（libs/server/Transaction/TxnRespCommands.cs:NetworkWATCH_OS）
  pub fn network_watch_os(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool {
    self.common_watch(session, StoreType::Object)
  }

  /// WATCH key [key ...]
  ///（libs/server/Transaction/TxnRespCommands.cs:NetworkWATCH）
  pub fn network_watch(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool {
    self.common_watch(session, StoreType::All)
  }

  /// UNWATCH（libs/server/Transaction/TxnRespCommands.cs:NetworkUNWATCH）
  pub fn network_unwatch(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool {
    if self.state == TxnState::None {
      self.watch_container.reset();
    }
    session.write_ok();
    true
  }

  /// RUNTXP 快路径（libs/server/Transaction/TxnRespCommands.cs:NetworkRUNTXPFast）
  ///
  /// C# 从接收缓冲元数据槽直读参数计数；托管解析态已携带计数，直通慢路径。
  pub fn network_runtxp_fast<S: TxnSession + ?Sized>(
    &mut self,
    session: &mut S,
    resolver: &mut (impl TxnProcResolver<S, L> + ?Sized),
  ) -> bool {
    self.network_runtxp(session, resolver)
  }

  /// RUNTXP id arg [arg ...]
  ///（libs/server/Transaction/TxnRespCommands.cs:NetworkRUNTXP）
  pub fn network_runtxp<S: TxnSession + ?Sized>(
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

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use super::*;
  use crate::{txn_session::MockTxnSession, watch_version_map::WatchVersionMap};

  fn manager() -> TransactionManager {
    TransactionManager::new(Arc::new(WatchVersionMap::new(64)), None)
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
}
