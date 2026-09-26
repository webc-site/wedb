//! 事务 RESP 命令面（对标 libs/server/Transaction/TxnRespCommands.cs —— C#
//! RespServerSession partial 的宿主侧承接，经 [`TxnRespCommandsExt`] 扩展
//! [`TransactionManager`]；逐调用传 `&mut impl TxnSession`）
//!
//! 光标模型映射：C# `readHead/endReadHead` 对应会话 `read_head/end_read_head`；
//! NetworkMULTI 记录的 `txnStartHead` 在 C# 取自命令解析后的 readHead
//! （即下一条命令起点），托管模型下等价于 MULTI 处理完的 `end_read_head`。
//! EXEC 据此回退光标重放排队命令：第一遍（Started）排队校验，重放遍
//! （Running）真执行，末尾 EXEC 再次进入本面触发提交。

use smallvec::SmallVec;
use wbase::{num::strict_i32, store_type::StoreType};
use wresp::{
  catalog::normalize_for_acls,
  cmd_strings::{
    GENERIC_ERR_WRONG_NUM_ARGS, RESP_ERR_DEBUG_DISALLOWED, RESP_ERR_EXEC_ABORT,
    RESP_ERR_GENERIC_DISCARD_WO_MULTI, RESP_ERR_GENERIC_EXEC_WO_MULTI,
    RESP_ERR_GENERIC_NESTED_MULTI, RESP_ERR_GENERIC_UNK_CMD, RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
    RESP_ERR_GENERIC_WATCH_IN_MULTI, RESP_ERR_HELLO_IN_TXN_UNSUPPORTED,
    RESP_ERR_NO_TRANSACTION_PROCEDURE, RESP_ERR_SELECT_IN_TXN_UNSUPPORTED,
    RESP_ERR_SWAPDB_IN_TXN_UNSUPPORTED,
  },
  command::RespCommand,
};
use wtxn::{ExecRun, TransactionManager, TxnQueuedCommandInfo, TxnSession, TxnState};

use super::basic_commands::parse_hello_args;

/// libs/server/Custom/CustomRespCommands.cs:TryTransactionProc 失败文案
///
/// 本锚仅承失败文案，TryTransactionProc 函数体不转写（ignore/server.yml 已登）
const RESP_ERR_TRANSACTION_FAILED: &str = "ERR Transaction failed.";

/// 元数按命令名 token 偏一（C# parseState.Count 不计命令名：正 arity -1、
/// 负 arity +1，同向收敛）
#[inline]
const fn arity_shift(arity: i32) -> i32 {
  if arity > 0 { arity - 1 } else { arity + 1 }
}

/// 中止即双态同置单点：管理器 abort + 会话镜像一并 Aborted，杜绝会话镜像
/// 滞留 Started 的双态脱节（返回 true 供调用点直接 `return`）
#[inline]
fn abort_state(txn: &mut TransactionManager, session: &mut (impl TxnSession + ?Sized)) -> bool {
  txn.abort();
  session.set_txn_state(TxnState::Aborted);
  true
}

/// 校验失败统一出口：写错误帧 → 中止双态同置
#[inline]
fn reject_abort(
  txn: &mut TransactionManager,
  session: &mut (impl TxnSession + ?Sized),
  error: &str,
) -> bool {
  session.write_error(error);
  abort_state(txn, session)
}

/// 事务收尾会话侧清零：集群槽位裁决复位 + 会话镜像归 None
#[inline]
fn end_session_txn(session: &mut (impl TxnSession + ?Sized)) {
  session.reset_cluster_slot_verification_result();
  session.set_txn_state(TxnState::None);
}

/// 事务全清：管理器与会话登记复位 + 监视容器复位 + 会话侧清零
#[inline]
fn reset_txn_none(txn: &mut TransactionManager, session: &mut (impl TxnSession + ?Sized)) {
  txn.reset();
  txn.watch_container.reset();
  end_session_txn(session);
}

/// 事务 RESP 命令面（C# RespServerSession partial 的扩展 trait 形态；
/// RESP 应答字节序列与 C# 1:1）
pub trait TxnRespCommandsExt {
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
  fn network_runtxp_fast<S: TxnSession + ?Sized>(&mut self, session: &mut S) -> bool;

  /// RUNTXP id arg [arg ...]
  ///（libs/server/Transaction/TxnRespCommands.cs:NetworkRUNTXP）
  fn network_runtxp<S: TxnSession + ?Sized>(&mut self, session: &mut S) -> bool;
}

impl TxnRespCommandsExt for TransactionManager {
  fn network_multi(&mut self, session: &mut (impl TxnSession + ?Sized)) -> bool {
    session.reset_cluster_slot_verification_result();
    if self.state != TxnState::None {
      return reject_abort(self, session, RESP_ERR_GENERIC_NESTED_MULTI);
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
    // 执行中再次 EXEC：越过并提交（重放遍的收尾）；提交失败写错误响应
    //（C# Commit 异常沿 EXEC 调用栈上抛的 RESP 投影，客户端感知未确认）
    if self.state == TxnState::Running {
      if self.commit(false).is_err() {
        session.write_error(RESP_ERR_TRANSACTION_FAILED);
      }
      end_session_txn(session);
      return true;
    }

    // 中止态：EXECABORT
    if self.state == TxnState::Aborted {
      session.write_error(RESP_ERR_EXEC_ABORT);
      reset_txn_none(self, session);
      return true;
    }

    // Started：回退光标重放排队命令
    if self.state == TxnState::Started {
      self.session_id = session.session_id();
      let orig_read_head = session.end_read_head();
      session.set_end_read_head(self.txn_start_head);

      if self.cluster_enabled {
        let Self {
          watch_container,
          txn_keys,
          ..
        } = self;
        for key in watch_container.save_keys_to_key_list() {
          txn_keys.push(key);
        }

        if !self.txn_keys.is_empty() {
          let verified = {
            let key_slices: SmallVec<[&[u8]; 16]> = self.txn_keys.iter().collect();
            // 只读态在被借出的本体上单点求值（C# NetworkEXEC 直调
            // txnManager.GetSlotVerificationInput 读 keyEntries.IsReadOnly 同构）
            let read_only = self.is_read_only();
            session.verify_cluster_txn_keys(&key_slices, read_only)
          };
          if !verified {
            reset_txn_none(self, session);
            session.set_end_read_head(orig_read_head);
            return true;
          }
        }
      }

      // compio 安全事务起点：首轮单次尝试取闩。成功走锁后收尾；争用则键集
      // 原样保留、登记既有唯一慢臂（单次让步 + 重驱本 EXEC），绝不在此自旋
      // 饿死同 worker 挂起持闩的阻塞命令（对标 C# 抢占线程池的 Lock 外层 while）。
      // WATCH 键并锁取本会话当前物理前缀（锁轨=物理域运行期取值）
      match self.run_exec(session.session_prefix().as_slice()) {
        ExecRun::Started => {
          session.set_txn_state(TxnState::Running);
          session.write_array_length(self.operation_cnt_txn);
        }
        ExecRun::Contended => {
          // 慢臂已登记且置 pending_rearm：游标回退至本 EXEC 命令起点的重驱
          // 归消费循环 rearm 臂单点承接（同槽位迁移等待面同一机制），本轮
          // 不落任何应答、保持 Started 态，待取锁成功后再写数组头并入重放遍。
          session.park_exec_lock_wait();
        }
        ExecRun::Aborted => {
          end_session_txn(session);
          session.set_end_read_head(orig_read_head);
          session.write_null_array();
        }
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
    // C# TxnRespCommands.cs:115 首行 `cmd = cmd.NormalizeForACLs()`：BITOP_AND /
    // SETEXNX 等伪子命令枚举先归一，其后的 BITOP 判据与 WATCH / SELECT /
    // DEBUG 臂一律消费归一值
    let cmd = normalize_for_acls(cmd);
    // 元数据可用性
    let Some(command_info) = info.filter(|info| info.allowed_in_txn) else {
      // 中止即双态同置：reject_abort 单点承接（下方各 abort 调用点同一机制）
      return reject_abort(self, session, RESP_ERR_GENERIC_UNK_CMD);
    };

    // 元数校验：command_info.arity == 0 表不校验；非 0 时命令名 token 不计入
    // parse_state.Count（±1 偏移）；子命令再偏移一次（BITOP 的操作位亦被解析器
    // 消费，is_sub_command 已含归一后 `cmd == Bitop` 并判，C# :125 同构）
    let count = session.arg_count();
    let invalid_num_args = if command_info.arity != 0 {
      let mut arity = arity_shift(command_info.arity);
      if command_info.is_sub_command || cmd == RespCommand::Bitop {
        arity = arity_shift(arity);
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
    // SELECT / SWAPDB / HELLO 事务内不支持（跨库事务未启用 / 事务内禁 HELLO AUTH 换租）
    let is_multi_db_command = matches!(
      cmd,
      RespCommand::Select | RespCommand::Swapdb | RespCommand::Hello
    );

    if invalid_num_args || is_watch || is_multi_db_command {
      if is_watch {
        session.write_error(RESP_ERR_GENERIC_WATCH_IN_MULTI);
        return true;
      }

      if invalid_num_args {
        session.abort_wrong_num_args(command_info.name);
        return abort_state(self, session);
      }

      match cmd {
        RespCommand::Swapdb => {
          return reject_abort(self, session, RESP_ERR_SWAPDB_IN_TXN_UNSUPPORTED);
        }
        RespCommand::Select => {
          // C# TxnRespCommands.cs:163 `parseState.TryGetInt(0, out var index) &&
          // index != activeDbId`：TryGetInt 系 int32 文法档，负数字面量解析成功，
          // 而 activeDbId 恒非负，故负数必落中止臂。判据取与 TryGetInt 同档的
          // strict_i32（Some 即含负数），严禁误用 parse_db_index 之 Ok 档——其
          // OutOfRange 档短路负数即本臂转写漏项（Ok 档非负限制系执行臂错误帧
          // 口径，§32 已裁，不动 parse_db_index 本体）。超 int32 字面量 C#
          // TryGetInt 解析失败不入本分支，同旧保持落排队
          if count > 0
            && let Some(index) = strict_i32(session.get_arg(0))
            && (index as i64) != (session.active_db_id() as i64)
          {
            return reject_abort(self, session, RESP_ERR_SELECT_IN_TXN_UNSUPPORTED);
          }
        }
        RespCommand::Hello => {
          // 事务内禁 HELLO 携 AUTH 换租（§58a，对齐 SELECT 异库中止先例与 AUTH
          // 禁入事务契约；C# HELLO 无 NoMulti、NetworkHELLO 无事务门，中止面
          // 须恰等于「可解析出合法 AUTH 选项组」之形集）：判据走位序文法单源
          // parse_hello_args（与执行臂共用，值位 AUTH / 尾随不足两参等不可触
          // 认证形不越权预断，错形由执行臂落 syntax error / 版本错）；两态
          // 收口：排队放行形（无 AUTH Ok 组）在重放窗经 core.rs 同步快臂直出
          // HELLO 应答，与本臂中止面互为补集，无第二拒绝面
          let has_auth_group = {
            let args: SmallVec<[&[u8]; 8]> = (0..count).map(|i| session.get_arg(i)).collect();
            matches!(parse_hello_args(&args), Ok(hello) if hello.auth.is_some())
          };
          if has_auth_group {
            return reject_abort(self, session, RESP_ERR_HELLO_IN_TXN_UNSUPPORTED);
          }
        }
        _ => {}
      }
    }

    if cmd == RespCommand::Debug && !session.can_run_debug() {
      return reject_abort(self, session, RESP_ERR_DEBUG_DISALLOWED);
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
    reset_txn_none(self, session);
    true
  }

  /// WATCH 系共同实现
  ///
  /// libs/server/Transaction/TxnRespCommands.cs:CommonWATCH
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:WATCH
  ///（C# StorageSession.WATCH 直转 txnManager.AddTransactionStoreType + Watch，
  /// rust 同构：add_transaction_store_type + TxnSession.watch 登记容器）
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

    // 前缀外提（transpile 循环前缀外提准则）：多键 WATCH 单次读取会话**逻辑**
    // 归属前缀（版本轨=逻辑域种子，与写面推进 bump_watch_version 的逻辑投影
    // 同源单点；锁轨=物理域另置，见 TxnSession::session_prefix 双轨声明）
    let prefix = session.watch_prefix();
    let prefix = prefix.as_slice();
    (0..count).for_each(|c| {
      let key = session.get_arg(c);
      self.watch(prefix, key);
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

  fn network_runtxp_fast<S: TxnSession + ?Sized>(&mut self, session: &mut S) -> bool {
    self.network_runtxp(session)
  }

  fn network_runtxp<S: TxnSession + ?Sized>(&mut self, session: &mut S) -> bool {
    self.session_id = session.session_id();
    let count = session.arg_count();
    if count < 1 {
      session.abort_wrong_num_args("RUNTXP");
      return true;
    }

    // 过程 id 须为 int32 文法档数值（C# TryGetInt 同档）
    if strict_i32(session.get_arg(0)).is_none() {
      session.write_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return true;
    }

    // 自定义过程均已清理，不再提供支持
    session.write_error(RESP_ERR_NO_TRANSACTION_PROCEDURE);
    true
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use wtxn::{LockType, TxnLockTable, WatchVersionMap};
  use wtxn_test::MockTxnSession;
  use wval::SessionPrefixBuf;

  use super::*;

  fn manager() -> TransactionManager {
    TransactionManager::new(
      TxnLockTable::new(),
      Arc::new(WatchVersionMap::new(64)),
      None,
    )
  }

  #[test]
  fn exec_cluster_slot_verify_failure_resets_txn_and_watch() {
    let mut txn = manager();
    txn.cluster_enabled = true;
    let mut session = MockTxnSession::new();
    session.cluster_slot_verify_fail = true;

    // WATCH key1
    txn.watch(SessionPrefixBuf::ROOT.as_slice(), b"key1");
    assert_eq!(txn.txn_keys.len(), 1);

    // MULTI
    assert!(txn.network_multi(&mut session));
    assert_eq!(txn.state, TxnState::Started);
    session.output.clear();

    // 记录待锁键
    txn.save_key_entry_to_lock(
      SessionPrefixBuf::ROOT.as_slice(),
      b"key2",
      LockType::Exclusive,
    );
    assert_eq!(txn.txn_keys.len(), 2);

    // EXEC 触发集群槽位校验失败
    assert!(txn.network_exec(&mut session));
    assert_eq!(
      session.output,
      b"-TRYAGAIN Multiple keys request during rehashing of slot\r\n"
    );
    assert_eq!(txn.state, TxnState::None);
    assert_eq!(session.txn_state, TxnState::None);
    assert!(txn.txn_keys.is_empty());
    assert!(
      txn
        .watch_container
        .save_lock_hashes(SessionPrefixBuf::ROOT.as_slice())
        .next()
        .is_none()
    );
  }

  #[test]
  fn exec_standalone_skips_cluster_slot_verify_and_zero_alloc() {
    let mut txn = manager();
    // 默认 cluster_enabled == false
    assert!(!txn.cluster_enabled);
    let mut session = MockTxnSession::new();
    // 即使会话标记 fail，单机形态下跳过集群槽位校验
    session.cluster_slot_verify_fail = true;

    txn.watch(SessionPrefixBuf::ROOT.as_slice(), b"key1");
    assert!(txn.txn_keys.is_empty());

    assert!(txn.network_multi(&mut session));
    txn.save_key_entry_to_lock(
      SessionPrefixBuf::ROOT.as_slice(),
      b"key2",
      LockType::Exclusive,
    );
    assert!(txn.txn_keys.is_empty());

    assert!(txn.network_exec(&mut session));
    assert_eq!(session.txn_state, TxnState::Running);
    assert!(txn.txn_keys.is_empty());
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
