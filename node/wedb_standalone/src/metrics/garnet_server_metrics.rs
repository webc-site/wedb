use std::sync::Arc;

use parking_lot::Mutex;

use super::{
  command_stats::CommandStats,
  garnet_session_metrics::GarnetSessionMetrics,
  latency::{
    garnet_latency_metrics::GarnetLatencyMetrics, latency_metrics_type::LatencyMetricsType,
  },
};

/// 服务器级指标快照（对标 libs/server/Metrics/GarnetServerMetrics.cs:GarnetServerMetrics）。
///
/// C# 为 struct，可空成员（track* 关闭时不分配）以 `Option` 承接；
/// `global_latency_metrics` 以 `Arc<Mutex<..>>` 承接 C# 的 readonly 共享引用
/// （会话释放路径与采样路径并发合并）。
pub struct GarnetServerMetrics {
  /// 收到的连接总数。
  pub total_connections_received: i64,
  /// 已释放的连接总数。
  pub total_connections_disposed: i64,
  /// 活跃连接总数。
  pub total_connections_active: i64,

  /// 瞬时命令吞吐（命令/s）。
  pub instantaneous_cmd_per_sec: f64,
  /// 瞬时网络入吞吐（KiB/s）。
  pub instantaneous_net_input_tpt: f64,
  /// 瞬时网络出吞吐（KiB/s）。
  pub instantaneous_net_output_tpt: f64,

  /// 全局会话指标。
  pub global_session_metrics: Option<GarnetSessionMetrics>,
  /// 会话指标历史（已释放会话的累计）。
  pub history_session_metrics: Option<GarnetSessionMetrics>,
  /// 全局逐命令延迟指标（track_latency 关闭时为 None）。
  pub global_latency_metrics: Option<Arc<Mutex<GarnetLatencyMetrics>>>,
  /// 全局逐命令使用统计。
  pub global_command_stats: Option<CommandStats>,
  /// 已释放会话的逐命令使用统计历史。
  pub history_command_stats: Option<CommandStats>,
}

impl GarnetServerMetrics {
  /// 吞吐换算单位：KiB（对齐 C# `byteUnit = 1 << 10`）。
  ///（libs/server/Metrics/GarnetServerMetrics.cs:byteUnit）
  pub const BYTE_UNIT: i64 = 1 << 10;

  /// libs/server/Metrics/GarnetServerMetrics.cs:GarnetServerMetrics（构造）。
  ///
  /// `track_stats` / `track_latency` / `track_command_stats` 决定对应成员
  /// 是否就位；延迟指标统计全部默认类别。
  pub fn new(track_stats: bool, track_latency: bool, track_command_stats: bool) -> Self {
    Self {
      total_connections_received: 0,
      total_connections_disposed: 0,
      total_connections_active: 0,
      instantaneous_cmd_per_sec: 0.0,
      instantaneous_net_input_tpt: 0.0,
      instantaneous_net_output_tpt: 0.0,
      global_session_metrics: track_stats.then(GarnetSessionMetrics::default),
      history_session_metrics: track_stats.then(GarnetSessionMetrics::default),
      global_latency_metrics: track_latency.then(|| {
        Arc::new(Mutex::new(GarnetLatencyMetrics::new(
          GarnetLatencyMetrics::DEFAULT_LATENCY_TYPES,
        )))
      }),
      global_command_stats: track_command_stats.then(CommandStats::new),
      history_command_stats: track_command_stats.then(CommandStats::new),
    }
  }

  /// libs/server/Metrics/GarnetServerMetrics.cs:Dispose
  ///
  /// 归还全局延迟指标（池化语义在 Rust 侧为丢弃引用）。
  pub fn dispose(&mut self) {
    self.global_latency_metrics = None;
  }
}

/// 延迟类别集合的类型别名导出（监视器/会话构造共用）。
pub type DefaultLatencyTypes = [LatencyMetricsType; 6];
