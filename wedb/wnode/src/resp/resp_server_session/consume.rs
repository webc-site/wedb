//! 消费主循环（对标 libs/server/Resp/RespServerSession.cs:TryConsumeMessages /
//! ProcessMessages：接收缓冲游标模型、批纪元快照、门链分派与批收口平移）。
//! 分派器臂见 [`super::dispatch`]。

use smallvec::SmallVec;
use wresp::{
  argslice::ArgSlice,
  cmd_strings::{self as cs},
  command::{RespCommand, one_if_read, one_if_write},
};
use wtxn::{TransactionManager, TxnState};

use super::core::{
  RespServerSession, SESSION_PARSE_STATE_MAX_RETAINED_ARGS, SESSION_TRIM_INTERVAL,
};
use crate::{
  cluster_session::SlotVerifyGate,
  resp::{acl_commands::AclGateVerdict, parser::resp_command::is_allowed_in_subscription_mode},
};

/// 批内输出水位（在 garnet 中的相对路径:libs/common/NetworkBufferSettings.cs:NetworkBufferSettings
/// ——默认 sendBufferSize = 1 << 17，即 C# 响应缓冲上界）。批内累计应答达此界
/// 即在命令边界停住交泵实写再续消费：RespWriteUtils TryWrite 写不下即
/// SendAndReset 满刷循环（libs/server/Resp/RespServerSession.cs:SendAndReset）
/// 的 rust 投影，粒度为命令边界而非写点（RespWriter 借用 output，写点复查
/// 在借用模型下不可行）
const OUTPUT_WATERMARK_BYTES: usize = 1 << 17;

/// RESP2 协议版本标记（订阅模式放行门判据；对位 C# respProtocolVersion == 2）
const RESP2: u8 = 2;

impl RespServerSession {
  /// 协议违规哨兵消费（try_consume_messages 对应的 C# catch 块）：
  /// 会话指标计数（C# catch 首行
  /// `sessionMetrics?.incr_total_number_resp_server_session_exceptions(1)`）
  /// 后写 `ERR Protocol Error: {msg}` 追加到累积输出尾部（C#
  /// `RespWriteUtils.TryWriteError($"ERR Protocol Error: {ex.Message}")`，
  /// 同批此前命令应答在前、错误在后），放行应答面交由调用方 Send 后断连。
  /// 返回是否发生违规
  fn write_protocol_error(&mut self) -> bool {
    if let Some(msg) = self.parse_violation.take() {
      if let Some(metrics) = &self.session_metrics {
        metrics.incr_total_number_resp_server_session_exceptions(1);
      }
      // 违例留痕日志（对标 C# catch 臂 RespServerSession.cs:525
      // `logger?.Log(ex.LogLevel, ex, "Aborting open session due to RESP parsing error")`，
      // ex.LogLevel 默认 Critical（RespParsingException.cs:20）——仓内无 critical 宏，
      // 取最高档 error；文案主干逐字对拍，携违例明细与 remote_endpoint 上下文
      // 对位 C# logger 随附异常对象形态；不动 take/帧写/返回时序）
      log::error!(
        "Aborting open session due to RESP parsing error: {msg} (remote={})",
        self.remote_endpoint
      );
      // 前缀单点：cs::ERR_PROTOCOL_ERROR_PREFIX（对标 C#
      // RespServerSession.cs:528 catch 块 TryWriteError($"ERR Protocol Error: {ex.Message}")）
      self.abort_error_message(&format!("{}{msg}", cs::ERR_PROTOCOL_ERROR_PREFIX));
      return true;
    }
    false
  }

  /// libs/server/Resp/RespServerSession.cs:TryConsumeMessages
  ///
  /// 唯一消费形态（C# IMessageConsumer 单方法）：接收缓冲驻留会话、由泵经
  /// take/return 直填（网络字节零拷贝直入），解析并分派接收缓冲中自
  /// [`Self::read_head`] 游标起的全部完整命令。游标跨批次持久不回退——
  /// C# `bytesRead = bytesReceived` + `readHead` 持久游标模型；事务排队
  /// 字节（MULTI..EXEC 跨批次）因此驻留接收缓冲，EXEC 据此回退重解析
  /// 排队命令（C# IsSkippingOperations 禁平移同源语义）。
  ///
  /// 分派经 [`crate::resp::garnet_api::GarnetApiFace`] 注入面；协议违规以 None 表达（C# 抛
  /// RespParsingException，catch 块先写 `ERR Protocol Error: {msg}` 到
  /// 累积输出再断连，rust 同序：错误落在 [`Self::output`] 中此前命令
  /// 应答之后，由调用方发出后断连）。
  ///
  /// 返回消费后残余字节数（半包长度；`0` = 整段消费完毕，缓冲清零复位，
  /// 容量保留复用）；`None` = 协议违规 / 致命断流（应答面已落
  /// [`Self::output`]，泵发尽后断连）。
  ///
  /// 批首尾纪元快照取放（C# :490 `clusterSession?.AcquireCurrentEpoch()` /
  /// :576 finally `clusterSession?.ReleaseCurrentEpoch()`）：批内会话持当前
  /// 纪元快照，批外清零——配置过渡静止等待的观测窗口
  pub fn try_consume_messages(&mut self) -> Option<usize> {
    if let Some(cs) = self.cluster_session.as_ref() {
      cs.acquire_current_epoch();
    }
    let remaining = self.try_consume_messages_body();
    if let Some(cs) = self.cluster_session.as_ref() {
      cs.release_current_epoch();
    }
    remaining
  }

  /// 批消费体（[`Self::try_consume_messages`] 的纪元快照保护段）
  fn try_consume_messages_body(&mut self) -> Option<usize> {
    self.bytes_read = self.recv_buffer.len();
    let prev_read_head = self.read_head;

    // 批首复位水位让渡哨兵（上一批由泵取走；直接重入防御性复位）
    self.output_watermark_yield = false;
    self.latency_batch_start();
    self.enter_and_get_response_object();
    let op_count = self.process_messages();
    // 协议违规（C# RespParsingException 传播出 ProcessMessages → catch 块：
    // 写 `ERR Protocol Error: {msg}` 追加在累积应答之后 → Send → 断连）。
    // 游标不回退，None 表达致命错误，应答面（含此前命令累积应答 + 协议
    // 错误）交由调用方发出后断连
    if self.write_protocol_error() {
      self.exit_and_return_response_object();
      return None;
    }

    // 切面致命断流（GarnetException clientResponse:false）：会话指标计数
    //（C# RespServerSession.cs catch(GarnetException) 首行
    // `sessionMetrics?.incr_total_number_resp_server_session_exceptions(1)`，
    // 与协议违规 catch 对称）后不写错误行，发尽累积应答后断连（None 通道
    // 与协议违规共用）
    if self.fatal_disconnect {
      if let Some(metrics) = &self.session_metrics {
        metrics.incr_total_number_resp_server_session_exceptions(1);
      }
      self.exit_and_return_response_object();
      return None;
    }

    // 本轮新增消费字节（EXEC 回退重解析可致游标暂时回退，saturating 兜底）
    let newly_consumed = self.read_head.saturating_sub(prev_read_head);
    self.latency_batch_stop(newly_consumed, op_count);
    // 事务在途（C# IsSkippingOperations / `if (txnSkip) return 0` 对偶
    // 语义）：排队字节与 txn_start_head 偏移必须驻留缓冲供 EXEC 回退重解析，
    // 禁止清零复位与残余平移。收口门并核会话镜像与事务管理器状态
    //（TransactionManager::is_skipping_operations：镜像失步窗口下 Manager 侧
    // Started/Aborted 同样拦截平移）；批内输出水位让渡批不平移——泵实写本
    // 轮应答后立即重入消费（不经网络读取段），残余完整帧随后续批收口
    if self.txn_state == TxnState::None
      && !self
        .txn_manager
        .as_ref()
        .is_some_and(TransactionManager::is_skipping_operations)
      && !self.output_watermark_yield
    {
      // 本批收口的接收基准规格（PR #2157 预算钳制施加面，C#
      // BaseReceiveBufferSize = budget.ClampReceiveBufferSize(configured)：
      // 预算缺省即默认驻留水位，零行为变化）
      let recv_base = self.recv_base_capacity();
      if self.read_head >= self.bytes_read {
        // 整段消费完毕：缓冲清零复位（C# ShiftTransportReceiveBuffer 的
        // bytesLeft == 0 形态）；超大批次容量释放，回归基准驻留水位
        if self.recv_buffer.capacity() > recv_base {
          self.recv_buffer = Vec::with_capacity(recv_base);
          // 换缓冲实例：初始化代际一并失效（新分配禁复用旧代际）
          self.recv_prime_key = None;
        } else {
          self.recv_buffer.clear();
        }
        self.bytes_read = 0;
        self.read_head = 0;
        self.end_read_head = 0;
      } else if self.read_head > 0 {
        // 半包残余平移收口（C# NetworkHandler.cs:ShiftTransportReceiveBuffer：
        // bytesLeft != transportBytesRead 时残余拷贝至头部、
        // transportBytesRead = bytesLeft、transportReadHead = 0——C# 网络层
        // 每批 Process 后在 Rest 态执行的平移，rust 收口单点搬入会话层）。
        // 缺失此收口时已消费前缀单调驻留，长连接下缓冲无界扩容
        let remaining = self.bytes_read - self.read_head;
        self
          .recv_buffer
          .copy_within(self.read_head..self.bytes_read, 0);
        self.recv_buffer.truncate(remaining);
        self.bytes_read = remaining;
        self.read_head = 0;
        self.end_read_head = 0;
        // 超大批次后的容量收敛（C# ShrinkNetworkReceiveBuffer 的对偶：
        // 平移后残余回落基准水位内即收缩，防大容量常驻）
        if self.recv_buffer.capacity() > recv_base && remaining <= recv_base {
          self.recv_buffer.shrink_to(recv_base);
        }
      }
    }
    self.exit_and_return_response_object();

    // 批边界：无参数指针跨界存活，为一条异常宽命令长出的超帽会话缓冲在此
    // 释放（PR #2157，C# finally 块 `--sessionTrimCountdown` 单递减——读解析
    // 态本体在本方法逐批执行会可测劣化代码生成，倒计数驻会话标量）
    self.session_trim_countdown -= 1;
    if self.session_trim_countdown == 0 {
      self.trim_session_buffers();
    }

    if let Some(metrics) = &self.session_metrics {
      metrics.incr_total_net_input_bytes(newly_consumed as u64);
    }
    Some(self.bytes_read.saturating_sub(self.read_head))
  }

  /// 释放自上次 trim 以来一直高于帽且未增长的会话缓冲（PR #2157，C#
  /// RespServerSession.cs:457 TrimSessionBuffers）：解析态根缓冲随客户端
  /// 发过的最宽命令定容并钉死连接终身，一条超高元命令即永久放大会话、
  /// 成本随连接数伸缩。仅当容量高于帽且未比上次 trim 增长时释放——每批
  /// 都需要该容量的会话保留其缓冲。冷构造：每 64 批达一次
  fn trim_session_buffers(&mut self) {
    self.session_trim_countdown = SESSION_TRIM_INTERVAL;

    let length = self.parse_state.root_buffer.len();
    if length > SESSION_PARSE_STATE_MAX_RETAINED_ARGS && length <= self.parse_state_len_at_last_trim
    {
      // C# parseState.ShrinkRootBuffer(threshold)：计数清零、根缓冲重配至帽
      //（保留帽容量而非全释放——C# 同款重分配 retainedCount 数组）。批边界
      // 调用无存活参数指针，重配安全
      self.parse_state.count = 0;
      self.parse_state.offset = 0;
      let mut fresh = SmallVec::new();
      fresh.resize(SESSION_PARSE_STATE_MAX_RETAINED_ARGS, ArgSlice::new(0, 0));
      self.parse_state.root_buffer = fresh;
    }
    self.parse_state_len_at_last_trim = self.parse_state.root_buffer.len();
  }

  /// libs/server/Resp/RespServerSession.cs:ProcessMessages
  ///
  /// 主循环：解析 → ACL 门 + no-script 门（C# :653 CheckACLPermissions(cmd)
  /// && CheckScriptPermissions(cmd)，位图仅在脚本执行窗口挂载——C# 位图挂
  /// 内嵌 processor，脚本内 redis.call 重入同门）→ 订阅模式/事务/槽位门 →
  /// 分派 → 指标；被拒命令 ACL 失败回 NOPERM/NOAUTH、no-script 失败回
  /// NOSCRIPT（C# :688-715，两分支均 IncrementRejected）。命令未完整到达时
  /// 双游标回退到本轮起点（C# `endReadHead = readHead = _origReadHead`）。
  /// 返回本批有效命令数（C# opCount 字段的批内增量，延迟吞吐直方图消费）
  pub fn process_messages(&mut self) -> u64 {
    // 挂起中的阻塞/慢路径/脚本命令未完成前不再消费新命令（C# 网络线程
    // BlockingWait 期间本就读不到后续命令）
    if self.pending_block.is_some() || self.pending_slow.is_some() || self.script_suspend.is_some()
    {
      return 0;
    }

    let mut op_count = 0u64;
    let mut orig_read_head = self.read_head;

    while self.bytes_read.saturating_sub(self.read_head) >= 4 {
      // 解析命令；未完整到达则回退双游标本轮起点并跳出（C# commandReceived）；
      // 协议违规（C# RespParsingException）保持游标原样跳出，错误由消费
      // 入口 catch 等价物 write_protocol_error 落输出，连接由上层关闭
      let cmd = match self.parse_command() {
        Some(cmd) => cmd,
        None => {
          if self.parse_violation.is_some() {
            break;
          }
          self.read_head = orig_read_head;
          self.end_read_head = orig_read_head;
          break;
        }
      };

      // 排队期入队失败统一标记（未知命令/未知子命令、ACL 拒绝、脚本拒绝
      // 三处臂置位），分派链后单点收口中止
      let mut queue_failure = false;
      if cmd != RespCommand::Invalid {
        let orig_output_len = self.output.len();
        // 冷上下文挂起面按命令窗口即抛：只允许本命令的应答组装点消费
        self.cold_ctx = None;
        // C# 门链（RespServerSession.cs:651-653）：noScriptPassed 默认 true，
        // ACL 失败短路（&&）不再查 no-script；no-script 失败回 NOSCRIPT
        //（C# :710），不落 NOPERM/NOAUTH。挂载陈旧须点查刷新时门停车
        //（Parked），游标回退本命令起点，泵刷新后重驱重评
        let acl_permitted = match self.check_acl_permissions(cmd) {
          AclGateVerdict::Permitted => true,
          AclGateVerdict::Denied => false,
          AclGateVerdict::Parked => {
            self.read_head = orig_read_head;
            self.end_read_head = orig_read_head;
            break;
          }
        };
        let mut script_permitted = true;
        if acl_permitted {
          script_permitted = self.check_script_permissions(cmd);
        }
        if acl_permitted && script_permitted {
          // RESP2 订阅模式仅放行 (P|S)SUBSCRIBE/(P|S)UNSUBSCRIBE/PING/QUIT 与
          // rust 补全的 SUNSUBSCRIBE（无 RESET；允许集单点见
          // is_allowed_in_subscription_mode，C# 对位 RespCommand.cs:733-742）
          if self.is_subscription_session
            && self.resp_protocol_version == RESP2
            && !is_allowed_in_subscription_mode(cmd)
          {
            // 对标 libs/server/Resp/RespServerSession.cs:659（string.Format(
            // CmdStrings.GenericPubSubCommandNotAllowed, cmd.ToString())）
            let name = cmd.to_cs_name();
            self.abort_error_message(&cs::GENERIC_PUBSUB_COMMAND_NOT_ALLOWED.replace("{0}", name));
          } else if self.txn_state != TxnState::None {
            // C# 事务门：Running 直通（事务 API 与单机同一执行路径）；
            // Started 排队（EXEC/MULTI/DISCARD/QUIT 特例，余者 NetworkSKIP）
            self.process_transactional_command(cmd);
          } else if self.cluster_session.is_none() {
            // C# 分派链：ProcessBasicCommands → ProcessArrayCommands →
            // ProcessOtherCommands（事务入队/直通形态由分派域承载）；
            // 集群门控：clusterSession == null || CanServeSlot(cmd)
            self.process_basic_commands(cmd);
          } else {
            match self.can_serve_slot(cmd) {
              SlotVerifyGate::Serve => {
                self.process_basic_commands(cmd);
              }
              SlotVerifyGate::Redirected => {} // 重定向/错误已写出
              SlotVerifyGate::Wait => {
                // C# CanOperateOnKey / WaitForSlotToStablize 网络线程内联
                // 自旋的挂起投影：切面已登记等待体，取走转挂会话泵，回退
                // 游标至本命令起点停止消费；等待体驱动至迁移推进/超时后
                // 重评本命令
                if let Some(slow) = self
                  .cluster_session
                  .as_ref()
                  .and_then(|c| c.take_pending_slow())
                {
                  self.pending_slow = Some(slow);
                  self.read_head = orig_read_head;
                  self.end_read_head = orig_read_head;
                  break;
                }
                // 切面未登记等待体（装配缺口）：跳过本命令防消费忙转
              }
            }
          }

          // libs/server/Resp/RespServerSession.cs:683-689（CommandStats 门控：
          // 执行后 calls 必计；失败随 commandErrorWritten 标志计并复位）
          // 【有意偏差】失败判定在 C# commandErrorWritten 之外增补
          // 「输出以 - 开头即计失败」扫描——rust 命令臂存在直接写错误帧
          // 不经 AbortWithErrorMessage 置位的路径，扫描兜底使 failed_calls
          // 不漏计（与 C# 的差异经本注记登记）。停车臂（AUTH/HELLO/ACL 族）
          // 应答在本扫描块之后才于泵侧 await 域组装，本块只计 calls、失败
          // 判据恒假；failed 经泵侧闭环单点 account_parked_auth_acl_failure
          // 按同一判据补计（见其注释），族内绝无双轨双计。
          if let Some(stats) = &self.command_stats {
            let mut stats = stats.lock();
            stats.increment_calls(cmd);
            if self.command_error_written || self.output[orig_output_len..].starts_with(b"-") {
              stats.increment_failed(cmd);
              self.command_error_written = false;
            }
          }
        } else if script_permitted {
          // C# :688-706 else 分支：已认证 → NOPERM；未认证 → NOAUTH。
          // C# 直写错误帧不置 commandErrorWritten——本臂就地复位 abort 置位
          // 标志，拒缴计数不得污染下条放行命令的收尾门 failed 判据
          self.write_acl_permission_error(self.acl_user_handle.is_some());
          // 事务排队期权限拒绝同步中止：收口统一中止臂（下方 queue_failure 臂）
          queue_failure = true;
          // libs/server/Resp/RespServerSession.cs:715（ACL/脚本权限拒绝计数）
          if let Some(stats) = &self.command_stats {
            stats.lock().increment_rejected(cmd);
            self.command_error_written = false;
          }
        } else {
          // C# :708-712 else 分支：NOSCRIPT（C# :715 同计拒绝数）。
          // 同上：就地复位 abort 置位标志
          self.abort_error_message(cs::RESP_ERR_NOSCRIPT);
          // 事务排队期脚本权限拒绝同步中止：收尾统一中止臂
          queue_failure = true;
          if let Some(stats) = &self.command_stats {
            stats.lock().increment_rejected(cmd);
            self.command_error_written = false;
          }
        }
      } else {
        // 未知命令/未知子命令：错误帧已由解析器落线（C# writeErrorOnFailure）。
        // rust 解析器复用 abort_error_message 落帧并置旗标（C# 解析器
        // RespWriteUtils.TryWriteError 直写不置位），就地复位恢复 C# 记账
        // 语义：旗标泄漏会被下一条放行命令的收尾门误计 failed（先例：
        // 上方 NOPERM/NOSCRIPT 拒绝臂就地复位）
        self.command_error_written = false;
        // 事务排队期中止收口统一中止臂（下方 queue_failure 臂）
        queue_failure = true;
        self.contains_slow_command = true;
      }

      // 事务排队期入队失败统一中止臂（票面 1/2 收口，对标 C#
      // TransactionManager.Abort + NetworkEXEC 的 Aborted→EXECABORT 链）：
      // 未知命令/未知子命令（Invalid）、ACL 拒绝（NOPERM/NOAUTH）、脚本拒绝
      //（NOSCRIPT）三处入队失败臂单点收口——错误帧均已由各自臂落线，此处
      // 只置事务中止，不补写应答、不重解析参数。排空回环：解析器装载循环
      // 对 Invalid 返回同样推进 end_read_head 至整帧命令末尾（对标 C#
      // ReadCommandAndReconcileArguments 消费全 token 后返回 INVALID），
      // 按循环尾同一形态把游标推进到 end_read_head 完成整帧消费，不依赖
      // 事务分支替臂补做消费。仅排队窗生效（门在 abort_pending_transaction
      // 内部：None 不处理、Running 重放态不撕裂应答数组）
      if queue_failure {
        self.read_head = self.end_read_head;
        self.abort_pending_transaction();
      }

      // 重驱型挂起（命令处理体内登记的迭代门 Pending 等待体，如 RUNTXP
      // Prepare 段遇迁移推进未决）：回退游标至本命令起点停止消费（区别于
      // 产应答型 pending_slow），等待体由网络泵驱动至迁移推进/超时后重评
      // 重驱本命令
      if self.pending_rearm {
        self.read_head = orig_read_head;
        self.end_read_head = orig_read_head;
        self.pending_rearm = false;
        break;
      }

      // 推进游标处理下一条命令（C# _origReadHead = readHead = endReadHead）
      self.read_head = self.end_read_head;
      orig_read_head = self.read_head;

      // Handle metrics and special cases（对标 C# :726-735）
      op_count += 1;
      // C# :728 `if (slowLogThreshold > 0) HandleSlowLog(cmd)`：阈值批入口缓存
      // 门控，慢日志禁用时不进函数——零配置读、零时钟取、零参数序列化
      if self.slow_log_threshold > 0 {
        self.handle_slow_log(cmd);
      }
      if let Some(metrics) = &self.session_metrics {
        metrics.incr_total_commands_processed(1);
        metrics.add_total_write_commands_processed(one_if_write(cmd));
        metrics.add_total_read_commands_processed(one_if_read(cmd));
      }

      if self.session_asking != 0 {
        self.session_asking -= 1;
      }

      // 阻塞命令 / 慢路径命令 / 脚本挂起 / AUTH·HELLO·ACL 族停车：停止消费本批后续
      // 命令（C# 网络线程阻塞等价物，后续命令由网络泵驱动等待完成后继续
      // 消费）
      //
      // 【事务窗禁停泊对称加固】：冷租户装载与 AUTH/ACL 停泊唯一合法窗口为普通命令窗口（txn_state == None）。
      // 事务窗内（Started/Running/Aborted）严禁认证与冷租户装载停泊，防跨租户重入撕裂。
      if self.pending_block.is_some()
        || self.pending_slow.is_some()
        || self.pending_auth_acl.is_some()
        || self.script_suspend.is_some()
      {
        if self.pending_auth_acl.is_some()
          || (self.pending_slow.is_some() && self.cold_ctx.is_some())
        {
          debug_assert_eq!(
            self.txn_state,
            TxnState::None,
            "唯一合法停泊窗为普通命令窗口，事务窗内严禁冷租户认证与 ACL 停泊"
          );
        }
        break;
      }

      // 批内输出水位让渡（libs/server/Resp/RespServerSession.cs:SendAndReset
      // 满刷循环的命令边界投影）：累计应答达 OUTPUT_WATERMARK_BYTES 即停住，
      // 置位让渡哨兵交网络泵实写本轮应答后立即重入消费（对标 C# Send 后
      // 重取缓冲续写）；游标不回退，残余完整帧驻留接收缓冲待续消费
      if self.output.len() >= OUTPUT_WATERMARK_BYTES {
        self.output_watermark_yield = true;
        break;
      }
    }
    op_count
  }
}
