//! 事务面（对标 libs/server/Transaction/TxnRespCommands.cs：MULTI / EXEC /
//! DISCARD / WATCH 族会话侧路由与排队元数据投影，及
//! wtxn::TxnSession 会话 trait 承接）。

use itoa::Buffer;
use wresp::{
  catalog::{normalize_for_acls, try_get_simple_resp_command_info},
  cmd_strings::{
    cluster as cluster_cs, {self as cs},
  },
  command::RespCommand,
  key_spec::KeySpecificationFlags,
};
use wtxn::{TransactionManager, TxnCommandKeys, TxnKeySpec, TxnQueuedCommandInfo, TxnState};

use super::core::{RespServerSession, collect_arg_views};
use crate::{
  cluster_session::{ClusterSlotVerificationInput, SlotVerifyGate},
  resp::resp_commands_info_data::resp_command_to_cs_name,
};

impl RespServerSession {
  /// libs/server/Resp/RespServerSession.cs:EnterAndGetResponseObject
  ///
  /// 拿取响应对象（rust 托管缓冲：进入会话输出期）
  pub fn enter_and_get_response_object(&mut self) {
    self.output.clear();
  }

  /// libs/server/Resp/RespServerSession.cs:ExitAndReturnResponseObject
  pub fn exit_and_return_response_object(&mut self) {
    // C# 归还响应对象并将 dcurr/dend 清零；托管缓冲保留待复用
  }

  /// 事务门分派（C# ProcessMessages 的 `txnManager.state != TxnState.None` 分支）
  pub(super) fn process_transactional_command(&mut self, cmd: RespCommand) -> bool {
    if self.txn_state == TxnState::Running {
      // C# ProcessBasicCommands(cmd, ref transactionalApi)：事务执行态直通
      return self.process_basic_commands(cmd);
    }
    match cmd {
      RespCommand::Exec => self.network_exec(),
      RespCommand::Multi => self.network_multi(),
      RespCommand::Discard => self.network_discard(),
      RespCommand::Quit => {
        self.to_dispose = true;
        self.output.extend_from_slice(cs::RESP_OK);
        true
      }
      _ => self.network_skip(cmd),
    }
  }

  /// MULTI（会话侧路由，实现委托 wtxn::TransactionManager）
  pub(super) fn network_multi(&mut self) -> bool {
    use crate::resp::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_multi(session))
  }

  /// EXEC（会话侧路由，实现委托 wtxn::TransactionManager）
  pub(super) fn network_exec(&mut self) -> bool {
    use crate::resp::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_exec(session))
  }

  /// DISCARD（会话侧路由，实现委托 wtxn::TransactionManager）
  pub(super) fn network_discard(&mut self) -> bool {
    use crate::resp::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_discard(session))
  }

  /// WATCH（会话侧路由，实现委托 wtxn::TransactionManager）
  pub(super) fn network_watch(&mut self) -> bool {
    use crate::resp::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_watch(session))
  }

  /// WATCHMS（会话侧路由，实现委托 wtxn::TransactionManager）
  pub(super) fn network_watch_ms(&mut self) -> bool {
    use crate::resp::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_watch_ms(session))
  }

  /// WATCHOS（会话侧路由，实现委托 wtxn::TransactionManager）
  pub(super) fn network_watch_os(&mut self) -> bool {
    use crate::resp::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_watch_os(session))
  }

  /// UNWATCH（会话侧路由，实现委托 wtxn::TransactionManager）
  pub(super) fn network_unwatch(&mut self) -> bool {
    use crate::resp::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_unwatch(session))
  }

  /// RUNTXP（会话侧路由，实现委托 wtxn::TransactionManager）；过程体经
  /// 编译期静态派发解析实例化执行（C# NetworkRUNTXP + TryTransactionProc）
  pub(super) fn network_runtxp(&mut self) -> bool {
    use crate::resp::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_runtxp(session))
  }

  /// 事务管理器暂借共同骨架（take → 调用 → 归还，规避 &mut self 双重借用）。
  /// 未接线（None）= 宿主装配缺口，明确报错不静默
  fn with_txn_manager(
    &mut self,
    f: impl FnOnce(&mut TransactionManager, &mut Self) -> bool,
  ) -> bool {
    let Some(mut txn) = self.txn_manager.take() else {
      self.abort_error_message(cs::RESP_ERR_GENERIC_UNK_CMD);
      return true;
    };
    txn.cluster_enabled = self.cluster_session.is_some();
    let ok = f(&mut txn, self);
    self.txn_manager = Some(txn);
    ok
  }

  /// 排队命令（会话侧路由，实现委托 wtxn::TransactionManager）；
  /// 命令元数据取自 resp 命令信息域（C# SimpleRespCommandInfo 同源）
  fn network_skip(&mut self, cmd: RespCommand) -> bool {
    use crate::resp::txn_resp_commands::TxnRespCommandsExt;
    let info = self.txn_queued_command_info(cmd);
    self.with_txn_manager(|txn, session| txn.network_skip(session, cmd, info.as_ref()))
  }

  /// 当前排队命令元数据（C# SimpleRespCommandInfo → TxnQueuedCommandInfo 投影；
  /// 键窗口按解析态参数即时解析，对齐 C# LockKeys 的 parseState 取数形态）
  fn txn_queued_command_info(&self, cmd: RespCommand) -> Option<TxnQueuedCommandInfo> {
    let info = try_get_simple_resp_command_info(normalize_for_acls(cmd))?;
    let name = resp_command_to_cs_name(cmd);
    let args = collect_arg_views(&self.parse_state, &self.recv_buffer);
    let key_specs: Vec<TxnKeySpec> = info
      .key_specs
      .iter()
      .filter_map(|spec| {
        let (first_idx, last_idx, step) =
          spec.get_key_search_args_slice(&args, info.is_sub_command)?;
        Some(TxnKeySpec::new(
          first_idx,
          last_idx as i64,
          step,
          spec.flags.contains(KeySpecificationFlags::RO),
        ))
      })
      .collect();
    let keys = (!key_specs.is_empty()).then_some(TxnCommandKeys {
      store_type: info.store_type,
      key_specs,
    });
    Some(TxnQueuedCommandInfo {
      name,
      arity: i32::from(info.arity),
      allowed_in_txn: info.allowed_in_txn,
      is_sub_command: info.is_sub_command,
      keys,
    })
  }
}

impl wtxn::TxnSession for RespServerSession {
  #[inline]
  fn session_id(&self) -> i32 {
    self.id as i32
  }

  #[inline]
  fn arg_count(&self) -> usize {
    self.parse_state.count
  }

  #[inline]
  fn get_arg(&self, idx: usize) -> &[u8] {
    self.parse_state.arg_in(&self.recv_buffer, idx)
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
    self.resp_protocol_version
  }

  #[inline]
  fn active_db_id(&self) -> u64 {
    self.active_db_id
  }

  #[inline]
  fn can_run_debug(&self) -> bool {
    self.can_run_debug()
  }

  #[inline]
  fn write_ok(&mut self) {
    self.output.extend_from_slice(cs::RESP_OK);
  }

  #[inline]
  fn write_queued(&mut self) {
    self.output.extend_from_slice(cs::RESP_QUEUED);
  }

  #[inline]
  fn write_null_array(&mut self) {
    self.write_null_array();
  }

  #[inline]
  fn write_array_length(&mut self, count: usize) {
    self.writer2().write_array_length(count);
  }

  #[inline]
  fn write_error(&mut self, message: &str) {
    self.abort_error_message(message);
  }

  #[inline]
  fn abort_wrong_num_args(&mut self, cmd_name: &str) {
    self.abort_wrong_num_args(cmd_name);
  }

  #[inline]
  fn write_proc_param_error(&mut self, tx_id: u8, expected: i32, actual: usize) {
    // C# CmdStrings.RESP_ERR_INVALID_NUM_PROC_PARAMS + string.Format 后
    // AbortWithErrorMessage；帧由 write_error_raw 单点成帧（含清洗）
    let mut b0 = Buffer::new();
    let mut b1 = Buffer::new();
    let mut b2 = Buffer::new();
    let msg = format!(
      "ERR Invalid number of parameters to stored proc {}, expected {}, actual {}",
      b0.format(tx_id),
      b1.format(expected),
      b2.format(actual)
    );
    cs::write_error_raw(&mut self.output, &msg);
    self.command_error_written = true;
  }

  fn reset_cluster_slot_verification_result(&mut self) {
    if let Some(ref cluster) = self.cluster_session {
      cluster.reset_cached_slot_verification_result();
    }
  }

  fn park_iterative_slot_wait(&mut self) -> bool {
    let Some(cluster) = &self.cluster_session else {
      return false;
    };
    let Some(slow) = cluster.take_pending_slow() else {
      return false;
    };
    self.pending_slow = Some(slow);
    self.pending_rearm = true;
    true
  }

  fn verify_cluster_txn_keys(&mut self, keys: &[&[u8]]) -> bool {
    let Some(ref cluster) = self.cluster_session else {
      return true;
    };
    cluster.reset_cached_slot_verification_result();
    let input = ClusterSlotVerificationInput {
      slot: self.active_db_slot(),
      key_specs: &[],
      is_sub_command: false,
      read_only: self
        .txn_manager
        .as_ref()
        .is_some_and(|tm| tm.is_read_only()),
      session_asking: self.session_asking,
      wait_for_stable_slot: false,
    };
    match cluster.network_multi_key_slot_verify(&input, keys, &mut self.output) {
      SlotVerifyGate::Serve => true,
      SlotVerifyGate::Redirected => false,
      SlotVerifyGate::Wait => {
        // 事务域无挂起重评形态（EXEC 已回退游标重放排队命令）：丢弃切面
        // 等待体，按 C# VerifyKeysInRange 迁移中混合态应 TRYAGAIN，客户端
        // 重试 EXEC（文案走集群域单点常量，帧由错误帧单点成帧）
        let _ = cluster.take_pending_slow();
        cs::write_error_raw(&mut self.output, cluster_cs::ERR_TRYAGAIN);
        false
      }
    }
  }
}
