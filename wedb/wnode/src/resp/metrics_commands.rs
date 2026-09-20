//! LATENCY / SLOWLOG 命令（对标 libs/server/Metrics/Latency/RespLatencyCommands.cs
//! 与 libs/server/Metrics/Slowlog/RespSlowlogCommands.cs —— C# 为 RespServerSession
//! 的 partial，Rust 侧应答编码由 wmetric 纯函数承接，本文件为会话侧参数解析
//! 与注入面（全局延迟指标 / 慢日志容器）的接线层）

use std::sync::Arc;

use wbase::{convert::stopwatch::TICKS_PER_MICROSECOND, time::now_stopwatch_ticks};
use wconf::ServerConfigType;
use wmetric::{
  GarnetServerMonitor, RespLatencyCommands, RespSlowlogCommands, SlowLogContainer,
  slowlog::resp_slowlog_commands::SlowLogContext,
};
use wresp::command::RespCommand;

use super::resp_server_session::{RespServerSession, collect_arg_views};
use crate::session_parse_state_extensions::serialize_snapshot;

impl RespServerSession {
  /// LATENCY / SLOWLOG 族分派（C# ProcessAdminCommands 的对应 arm；
  /// `None` 表示命令不属于本族，调用方继续后续分派）
  pub fn process_metrics_commands(&mut self, cmd: RespCommand) -> Option<bool> {
    let args = collect_arg_views(&self.parse_state, &self.recv_buffer);
    let mut out = Vec::new();
    let result: Result<(), String> = match cmd {
      RespCommand::LatencyHelp => {
        RespLatencyCommands::network_latency_help(args.len(), &mut out).map_err(String::from)
      }
      RespCommand::LatencyHistogram => {
        // C# storeWrapper.monitor?.GlobalMetrics.globalLatencyMetrics
        let fallback;
        let global_metrics = match &self.global_latency_metrics {
          Some(metrics) => Some(metrics),
          None => {
            fallback = GarnetServerMonitor::global().and_then(|m| m.global_latency_metrics());
            fallback.as_ref()
          }
        };
        let guard = global_metrics.map(|m| m.lock());
        let metrics = guard.as_deref();
        RespLatencyCommands::network_latency_histogram(&args, metrics, &mut out);
        Ok(())
      }
      RespCommand::LatencyReset => {
        // C# NetworkLatencyReset 仅置 monitor.resetLatencyMetrics 标志，
        // 全局与会话直方图复位延后到监视器采样轮消费
        //（CleanupGlobalLatencyMetrics）；未安装为 no-op（对齐 monitor != null）
        let monitor = GarnetServerMonitor::global();
        RespLatencyCommands::network_latency_reset(
          &args,
          &mut |event| {
            if let Some(mon) = &monitor {
              mon.set_latency_reset_flag(event);
            }
          },
          &mut out,
        );
        Ok(())
      }
      RespCommand::SlowlogHelp => RespSlowlogCommands::network_slow_log_help(args.len(), &mut out),
      RespCommand::SlowlogGet => RespSlowlogCommands::network_slow_log_get(
        &args,
        self.slow_log_container.as_deref(),
        &mut out,
      ),
      RespCommand::SlowlogLen => RespSlowlogCommands::network_slow_log_len(
        args.len(),
        self.slow_log_container.as_deref(),
        &mut out,
      ),
      RespCommand::SlowlogReset => RespSlowlogCommands::network_slow_log_reset(
        args.len(),
        self.slow_log_container.as_deref(),
        &mut out,
      ),
      _ => return None,
    };
    // 视图借用随 args 析构收尾：写出方（含错误帧臂）需可变借用整个会话，
    // 接收缓冲的字段级借用必须先结束
    drop(args);
    match result {
      Ok(()) => {
        self.output.extend_from_slice(&out);
        Some(true)
      }
      Err(ref message) => {
        self.abort_error_message(message);
        Some(true)
      }
    }
  }

  /// 慢日志记录（C# 主循环尾部 `slowLogThreshold > 0` 时逐命令调用
  /// HandleSlowLog 的调用点承接）
  ///
  /// C# ProcessMessages 循环尾：`slowLogThreshold > 0` 时逐命令记录；阈值
  /// 每批自运行时配置刷新（SLOWLOG_LOG_SLOWER_THAN，微秒 × TickToMicroseconds
  /// 折算 tick），记录后推进批内起始 tick。
  pub fn handle_slow_log(&mut self, cmd: RespCommand) {
    // C# slowLogThreshold > 0 门（0 = 禁用）；配置未启用时不取时钟
    let threshold_us = self
      .runtime_config()
      .get_microseconds(ServerConfigType::SlowlogLogSlowerThan);
    if threshold_us <= 0 {
      return;
    }
    let Some(container) = self.slow_log_container.as_deref() else {
      return;
    };
    let now_ticks = now_stopwatch_ticks();
    // 阈值 µs → tick 走 wbase::convert::stopwatch 单点因子（C# `slowLogThresholdConfig
    // * OutputScalingFactor.TimeStampToMicroseconds`）；threshold_us 已判 > 0
    let slow_log_threshold = threshold_us as u64 * TICKS_PER_MICROSECOND;
    let elapsed = now_ticks.saturating_sub(self.slow_log_start_ticks);
    let arguments = if cmd != RespCommand::Invalid
      && elapsed > slow_log_threshold
      && self.parse_state.count > 0
    {
      Some(serialize_snapshot(&self.parse_state, &self.recv_buffer))
    } else {
      None
    };
    let ctx = SlowLogContext {
      container,
      cmd,
      ticks_stopwatch: now_ticks,
      slow_log_threshold,
      client_ip_port: &self.remote_endpoint,
      client_name: self.client_name.as_deref().unwrap_or(""),
      arguments,
    };
    RespSlowlogCommands::handle_slow_log(&ctx, &mut self.slow_log_start_ticks);
  }
}

/// 慢日志容器构造（libs/server/StoreWrapper.cs:243 —— serverOptions.SlowLogMaxEntries）
pub fn new_slow_log_container(max_entries: i32) -> Arc<SlowLogContainer> {
  Arc::new(SlowLogContainer::new(max_entries))
}
