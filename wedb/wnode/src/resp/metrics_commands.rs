//! LATENCY / SLOWLOG 命令（对标 libs/server/Metrics/Latency/RespLatencyCommands.cs
//! 与 libs/server/Metrics/Slowlog/RespSlowlogCommands.cs —— C# 为 RespServerSession
//! 的 partial，Rust 侧应答编码由 wmetric 纯函数承接，本文件为会话侧参数解析
//! 与注入面（全局延迟指标 / 慢日志容器）的接线层）

use std::sync::Arc;

use wbase::time::now_nanos;
use wconf::ServerConfigType;
use wmetric::{
  GarnetLatencyMetrics, GarnetLatencyMetricsSession, RespLatencyCommands, RespSlowlogCommands,
  SlowLogContainer, latency::latency_metrics_entry::time_stamp::TICKS_PER_MICROSECOND,
  slowlog::resp_slowlog_commands::SlowLogContext,
};
use wresp::RespCommand;

use super::resp_server_session::RespServerSession;
use crate::session_parse_state_extensions::serialize_snapshot;

impl RespServerSession {
  /// LATENCY / SLOWLOG 族分派（C# ProcessAdminCommands 的对应 arm；
  /// `None` 表示命令不属于本族，调用方继续后续分派）
  pub fn process_metrics_commands(&mut self, cmd: RespCommand) -> Option<bool> {
    let args = self.get_arg_slices();
    let mut out = Vec::new();
    let result: Result<(), String> = match cmd {
      RespCommand::LatencyHelp => {
        RespLatencyCommands::network_latency_help(args.len(), &mut out).map_err(String::from)
      }
      RespCommand::LatencyHistogram => {
        // C# storeWrapper.monitor?.GlobalMetrics.globalLatencyMetrics
        let guard = self.global_latency_metrics.as_ref().map(|m| m.lock());
        let metrics = guard.as_deref();
        RespLatencyCommands::network_latency_histogram(&args, metrics, &mut out);
        Ok(())
      }
      RespCommand::LatencyReset => {
        // C# monitor.resetLatencyMetrics[e] = true（监视器迭代期复位）；
        // 全局指标为 Arc 共享，经内部互斥即时复位
        let metrics = self.global_latency_metrics.clone();
        RespLatencyCommands::network_latency_reset(
          &args,
          &mut |event| {
            if let Some(metrics) = &metrics {
              metrics.lock().reset(event);
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
    let Some(container) = self.slow_log_container.clone() else {
      return;
    };
    // Stopwatch tick 域（100ns/tick）：coarsetime 纳秒 / 100
    let now_ticks = (now_nanos() / 100).min(i64::MAX as u64) as i64;
    let arguments = if self.parse_state.count > 0 {
      // C# parseState.SerializeTo 快照
      Some(serialize_snapshot(&self.parse_state))
    } else {
      None
    };
    let ctx = SlowLogContext {
      container: &container,
      cmd,
      now_ticks,
      slow_log_threshold: threshold_us * TICKS_PER_MICROSECOND as i64,
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

/// 全局延迟指标构造（C# GarnetServerMonitor.GlobalMetrics.globalLatencyMetrics）
pub fn new_global_latency_metrics() -> Arc<parking_lot::Mutex<GarnetLatencyMetrics>> {
  Arc::new(parking_lot::Mutex::new(GarnetLatencyMetrics::new(
    GarnetLatencyMetricsSession::DEFAULT_LATENCY_TYPES,
  )))
}
