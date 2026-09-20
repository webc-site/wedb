//! WeDB 服务端指标监控库 (`wmetric`)
//!
//! 对标微软 Garnet 服务监控架构 (Garnet.server/Metrics/)。
//! client/server 共引的类型单点（InfoMetricsType、MetricsItem）在协议层
//! `wresp::metrics`（对位 Garnet.common/Metrics），本 crate 不复出。

pub mod command_stats;
pub mod garnet_server_metrics;
pub mod garnet_server_monitor;
pub mod garnet_session_metrics;
pub mod info;
pub mod latency;
pub mod slowlog;
pub mod system_metrics;

pub use command_stats::{CommandStats, CommandStatsEntry};
pub use garnet_server_metrics::GarnetServerMetrics;
pub use garnet_server_monitor::{
  GarnetServerMonitor, MonitorIterationInputs, ServerSample, SessionSample,
};
pub use garnet_session_metrics::{GarnetSessionMetrics, SessionMetricsHandle};
pub use info::{
  garnet_info_metrics::{
    ALL_INFO_SET, AofSnapshot, DEFAULT_INFO, DbSnapshot, GarnetInfoMetrics, GlobalMetricsSnapshot,
    InfoProvider, ReadCacheSnapshot, ServerFacts,
  },
  info_command::InfoCommand,
  info_help::InfoHelp,
};
pub use latency::{
  garnet_latency_metrics::GarnetLatencyMetrics,
  garnet_latency_metrics_session::GarnetLatencyMetricsSession,
  latency_metrics_entry::LatencyMetricsEntry,
  latency_metrics_entry_session::LatencyMetricsEntrySession,
  latency_metrics_type::LatencyMetricsType, resp_latency_commands::RespLatencyCommands,
  resp_latency_help::RespLatencyHelp,
};
pub use slowlog::{
  resp_slowlog_commands::RespSlowlogCommands, resp_slowlog_help::RespSlowlogHelp,
  slow_log_container::SlowLogContainer, slowlog_entry::SlowLogEntry,
};
pub use system_metrics::SystemMetrics;
