//! WeDB 服务端指标监控库 (`wmetric`)
//!
//! 对标微软 Garnet 服务监控架构 (Garnet.server/Metrics/)。
//! client/server 共引的类型单点（InfoMetricsType、MetricsItem）在协议层
//! `wresp::metrics`（对位 Garnet.common/Metrics），本 crate 不复出。

// 单导出面：mod 全部 pub(crate)，跨 crate 消费一律走根 re-export（禁止二次导出）。
// slowlog 例外已消：RespSlowlogCommands/SlowLogContext/SlowLogContainer/SlowLogEntry
// 全数根出，slowlog 树 mod 面 pub(crate)。
pub(crate) mod command_stats;
pub(crate) mod garnet_server_metrics;
pub(crate) mod garnet_server_monitor;
pub(crate) mod garnet_session_metrics;
pub(crate) mod info;
pub(crate) mod latency;
pub(crate) mod slowlog;
pub(crate) mod system_metrics;

pub use command_stats::{CommandStats, CommandStatsEntry};
pub use garnet_server_monitor::{
  GarnetServerMonitor, MonitorIterationInputs, ServerSample, SessionSample,
};
pub use garnet_session_metrics::{GarnetSessionMetrics, SessionMetricsHandle};
pub use info::{
  garnet_info_metrics::{
    AofSnapshot, DEFAULT_HEX_ID, DbSnapshot, GarnetInfoMetrics, GlobalMetricsSnapshot,
    InfoProvider, ReadCacheSnapshot, ServerFacts,
  },
  info_command::InfoCommand,
};
pub use latency::{
  garnet_latency_metrics::GarnetLatencyMetrics,
  garnet_latency_metrics_session::GarnetLatencyMetricsSession,
  latency_metrics_entry_session::LatencyMetricsEntrySession,
  latency_metrics_type::LatencyMetricsType, pending_latency_meter::PendingLatencyMeter,
  resp_latency_commands::RespLatencyCommands,
};
pub use slowlog::{
  resp_slowlog_commands::{RespSlowlogCommands, SlowLogContext},
  slow_log_container::SlowLogContainer,
  slowlog_entry::SlowLogEntry,
};
pub use system_metrics::{SystemMetrics, parse_proc_kb};
