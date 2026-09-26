//! 事务面（对标 libs/server/Transaction/TxnRespCommands.cs：MULTI / EXEC /
//! DISCARD / WATCH 族会话侧路由与排队元数据投影，及
//! wtxn::TxnSession 会话 trait 承接）。

use itoa::Buffer;
use wbase::future::yield_now;
use wresp::{
  catalog::{extract_keys_from_slice, normalize_for_acls, try_get_simple_resp_command_info},
  cmd_strings::{
    cluster as cluster_cs, {self as cs},
  },
  command::RespCommand,
  key_spec::KeySpecificationFlags,
};
use wtxn::{
  TransactionManager, TxnCommandKeys, TxnKeySpec, TxnQueuedCommandInfo, TxnSession as _, TxnState,
};
use wval::SessionPrefixBuf;

use super::core::{RespServerSession, collect_arg_views};
use crate::{
  cluster_session::{ClusterSlotVerificationInput, SlotVerifyGate},
  resp::slow_path::SlowWait,
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

  /// RUNTXP（会话侧路由，实现委托 wtxn::TransactionManager）；过程面已整体
  /// 清理，仅保校验三门＋无条件 NO_TRANSACTION_PROCEDURE（C# NetworkRUNTXP；
  /// TryTransactionProc 不转写，ignore/server.yml 已登记）
  pub(super) fn network_runtxp(&mut self) -> bool {
    use crate::resp::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_runtxp(session))
  }

  /// 事务排队期入队失败的统一兜底中止臂（对标 C# TransactionManager.Abort +
  /// 会话镜像，libs/server/Resp/RespServerSession.cs 的 ProcessMessages 排队失败
  /// 置 Aborted 诸臂的 rust 收口）。
  ///
  /// 消费主循环未知命令/未知子命令（Invalid，错误帧已由解析器落线）与
  /// ACL/脚本权限拒绝（NOPERM/NOAUTH/NOSCRIPT 已落线）共用本单点：排队期
  /// 将事务管理器与会话镜像同置 [`TxnState::Aborted`]，使随后 EXEC 必回
  /// EXECABORT（C# TxnRespCommands.NetworkEXEC 同构），严禁重放队列任何
  /// 命令，保全 All-or-Nothing
  pub(super) fn abort_pending_transaction(&mut self) {
    // 仅排队窗（Started/Aborted）中止：None 非事务不处理；Running 为 EXEC
    // 重放执行态，错误帧由执行命令臂各自落线，中途置 Aborted 会撕裂已
    // 开始的数组应答。Aborted 幂等重入（C# Abort 无条件置位同构）
    if !matches!(self.txn_state, TxnState::Started | TxnState::Aborted) {
      return;
    }
    self.with_txn_manager(|txn, session| {
      txn.abort();
      <Self as wtxn::TxnSession>::set_txn_state(session, TxnState::Aborted);
      true
    });
  }

  /// 事务管理器暂借共同骨架（take → 调用 → 归还，规避 &mut self 双重借用）。
  /// 未接线（None）= 宿主装配缺口，明确报错不静默
  ///
  /// 闭环防御（C# 会话经 txnManager.state 单真值源直读、无镜像；rust 双态
  /// 持有须本单点对齐）：归还后将会话镜像 txn_state 与管理器 state 对齐，
  /// 杜绝 network_skip 任一 abort() 分支漏同步会话镜像致双态脱节
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
    // 框架层单机制自动同步：以管理器 state 为真值源镜像回会话
    self.txn_state = txn.state;
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
    // C# TxnRespCommands.cs:115 首行 `cmd = cmd.NormalizeForACLs()`：BITOP_AND /
    // SETEXNX 等伪子命令枚举先归一为真实命令，回显名与键窗口一律取归一值
    let cmd = normalize_for_acls(cmd);
    let info = try_get_simple_resp_command_info(cmd)?;
    // BITOP 的操作 token（AND/NOT/…）已被解析器消费，键窗口同子命令走 -2 偏移；
    // json 表中 BITOP 为顶级命令，is_sub_command 恒 false，须显式并判（C#
    // TxnRespCommands.cs:125 `commandInfo.IsSubCommand || cmd == RespCommand.BITOP`）
    let is_sub_command = info.is_sub_command || cmd == RespCommand::Bitop;
    let name = cmd.to_cs_name();
    let args = collect_arg_views(&self.parse_state, &self.recv_buffer);
    let key_specs: Vec<TxnKeySpec> = info
      .key_specs
      .iter()
      .filter_map(|spec| {
        let (first_idx, last_idx, step) = spec.get_key_search_args_slice(&args, is_sub_command)?;
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
      is_sub_command,
      keys,
    })
  }

  /// 本命令键窗是否全落本事务已持闩桶域（EXEC 重放段脚本重入的锁器模式选型
  /// 判据，消费点在 garnet_api::exec；rust 自研判据点，C# 内嵌 processor 恒
  /// Basic ephemeral，登记 doc/zh/deviations.md §139）
  ///
  /// 键窗口提取与排队落键段 [`Self::txn_queued_command_info`] 严格同源：伪子
  /// 命令枚举先归一、命令信息同目录单点、子命令/BITOP 并判同口径、键窗经
  /// wresp `extract_keys_from_slice` 同一扫描内核（COMMAND GETKEYS / 槽校验
  /// 共用单点）；前缀取锁轨=物理域单点 [`wtxn::TxnSession::session_prefix`]
  /// （脚本内 SELECT 切换活动库时随存储执行域现值，与写面落键同源）；桶判定
  /// 按登记侧同一 `scoped_key_hash` 唯一构造口（跨库同名键离散域一致）。
  /// 事务管理器缺席 / 命令信息不可解析一律回 false——无从证明覆盖，保守让
  /// 调用窗口自取闩。
  pub(crate) fn txn_locks_cover_cmd(&self, cmd: RespCommand, args: &[&[u8]]) -> bool {
    let Some(txn) = &self.txn_manager else {
      return false;
    };
    let cmd = normalize_for_acls(cmd);
    let Some(info) = try_get_simple_resp_command_info(cmd) else {
      return false;
    };
    // BITOP 操作 token 已被解析器消费，与排队段同判并 is_sub_command（C#
    // TxnRespCommands.cs:125 `commandInfo.IsSubCommand || cmd == RespCommand.BITOP`）
    let is_sub_command = info.is_sub_command || cmd == RespCommand::Bitop;
    let keys = extract_keys_from_slice(args, &info.key_specs, is_sub_command);
    let prefix = self.session_prefix();
    txn.key_entries.covers_user_keys(prefix.as_slice(), keys)
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

  /// 锁轨=物理域：排队命令锁登记与 EXEC WATCH 键并锁取本会话存储执行域
  /// （garnet_api）的 `StoreSession::session_prefix` 单点，与桶闩所在现域
  /// 同源（含 FLUSHDB 换号态）；无执行域的嵌入式形态回根域（与 wkv 未绑定
  /// 会话实前缀恒等）。版本轨另置 [`Self::watch_prefix`]，双轨分置禁共口互染
  #[inline]
  fn session_prefix(&self) -> SessionPrefixBuf {
    self
      .garnet_api
      .as_ref()
      .map_or(SessionPrefixBuf::ROOT, |api| api.session_prefix())
  }

  /// 版本轨=逻辑域：WATCH 登记分槽直取本会话存储执行域（garnet_api）的
  /// `StoreSession::session_logical_prefix` 单点，与本连接一切写入的
  /// `bump_watch_version` 逻辑投影同源，换号代际不入种子（对位 C# 每库
  /// VersionMap 实例终身持有，TransactionManager.cs:179）；无执行域的嵌入式
  /// 形态回根域（无存储面写入，逻辑域恒等 (0,0)）
  #[inline]
  fn watch_prefix(&self) -> SessionPrefixBuf {
    self
      .garnet_api
      .as_ref()
      .map_or(SessionPrefixBuf::ROOT, |api| api.session_logical_prefix())
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

  /// 外部 EXEC 取锁争用的慢臂登记（对标 C# `TransactionalContext.Lock` 外层 while
  /// 的让步语义，compio 投影）：登记一个仅让步一次执行器的 `SlowWait` 空应答体
  /// 并置重驱标志。网络泵 await 该让步体时把控制权交回执行器，令同 worker 上挂起
  /// 持闩的阻塞命令（如 BLPOP）得以被唤醒放闩；让步 resolve（空应答不写线）后，
  /// 消费循环据 `pending_rearm` 回退游标至本 EXEC 命令起点重驱，复入
  /// `TransactionManager::run_exec` 再单次尝试取闩——循环「单次尝试 + 执行器让步」
  /// 直至成功。以此替换会饿死同 worker 的 `thread::yield_now` 同步自旋，保留 C#
  /// 无界等待契约且不丢键集（键集/计划/钉定索引已在 `run_exec` 内跨轮保留）。
  fn park_exec_lock_wait(&mut self) {
    self.pending_slow = Some(SlowWait::new(async move {
      yield_now().await;
      Vec::new()
    }));
    self.pending_rearm = true;
  }

  fn verify_cluster_txn_keys(&mut self, keys: &[&[u8]], read_only: bool) -> bool {
    let Some(ref cluster) = self.cluster_session else {
      return true;
    };
    cluster.reset_cached_slot_verification_result();
    let input = ClusterSlotVerificationInput {
      slot: self.active_db_slot(),
      key_specs: &[],
      is_sub_command: false,
      // 只读态经形参单点传入（C# GetSlotVerificationInput `readOnly =
      // keyEntries.IsReadOnly` 的借窗承接）：本回调在 with_txn_manager 借出
      // 窗口内执行，会话 txn_manager 槽恒 None，回读必恒 false 误判
      read_only,
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
