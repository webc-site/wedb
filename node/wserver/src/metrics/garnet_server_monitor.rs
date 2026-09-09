use std::{
  future::Future,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  time::Duration,
};

use parking_lot::Mutex;

use super::{
  command_stats::CommandStats,
  garnet_server_metrics::GarnetServerMetrics,
  garnet_session_metrics::GarnetSessionMetrics,
  info_metrics_type::InfoMetricsType,
  latency::{
    garnet_latency_metrics_session::GarnetLatencyMetricsSession,
    latency_metrics_type::LatencyMetricsType,
  },
};

/// 服务器指标监视器：周期采样活跃会话，维护全局指标与瞬时吞吐。
///（对标 libs/server/Metrics/GarnetServerMonitor.cs:GarnetServerMonitor）
///
/// C# 经 `ActiveConsumers()` 直查会话；会话/服务器域尚未落地，此处以
/// [`SessionSample`] / [`ServerSample`] 快照入参承接同等聚合语义。
/// 可变状态收敛在 `Mutex<MonitorState>` 内，支持并发的历史并入
///（对齐 C# SingleWriterMultiReaderLock 的保护范围）。
pub struct GarnetServerMonitor {
  /// INFO RESET 置位的事件复位标志（下标 = InfoMetricsType 判别值）。
  pub reset_event_flags: [bool; InfoMetricsType::ALL.len()],
  /// LATENCY RESET 置位的延迟类别复位标志（下标 = 类别判别值）。
  pub reset_latency_metrics: [bool; LatencyMetricsType::ALL.len()],
  /// 采样周期。
  monitor_sampling_frequency: Duration,
  /// 迭代时钟：与全部会话侧延迟指标共享（奇偶切版本）。
  pub monitor_iterations: Arc<AtomicU64>,
  /// 全局指标 + 累加器 + 上轮瞬时基线。
  state: Mutex<MonitorState>,
}

/// 监视器可变状态（C# 的 globalMetrics / accSessionMetrics / accCommandStats /
/// instant_* 字段组）。
struct MonitorState {
  global_metrics: GarnetServerMetrics,
  acc_session_metrics: GarnetSessionMetrics,
  acc_command_stats: Option<CommandStats>,
  instant_input_net_bytes: u64,
  instant_output_net_bytes: u64,
  instant_commands_processed: u64,
}

/// 单个活跃会话的采样快照。
pub struct SessionSample<'a> {
  /// 会话指标。
  pub metrics: &'a GarnetSessionMetrics,
  /// 会话命令统计（未启用为 None）。
  pub command_stats: Option<&'a CommandStats>,
  /// 会话延迟指标（未启用为 None）。
  pub latency: Option<&'a GarnetLatencyMetricsSession>,
}

/// 单个服务器的采样快照。
pub struct ServerSample<'a> {
  /// 收到的连接数。
  pub total_connections_received: i64,
  /// 已释放的连接数。
  pub total_connections_disposed: i64,
  /// 活跃连接数。
  pub total_connections_active: i64,
  /// 活跃会话采样。
  pub sessions: Vec<SessionSample<'a>>,
}

/// 单轮迭代的外部输入（会话/服务器域复位回调集合）。
pub struct MonitorIterationInputs<'a> {
  /// 全部服务器的采样快照。
  pub servers: &'a [ServerSample<'a>],
  /// 复位全部活跃会话的延迟指标。
  pub reset_all_session_latency: &'a mut dyn FnMut(),
  /// 复位全部活跃会话的会话指标（STATS 复位路径）。
  pub reset_active_sessions: &'a mut dyn FnMut(),
  /// 复位全部活跃会话的命令统计（COMMANDSTATS 复位路径）。
  pub reset_active_command_stats: &'a mut dyn FnMut(),
  /// 复位指定类别的活跃会话延迟指标。
  pub reset_session_latency: &'a mut dyn FnMut(LatencyMetricsType),
}

impl GarnetServerMonitor {
  /// libs/server/Metrics/GarnetServerMonitor.cs:GarnetServerMonitor（构造）。
  ///
  /// `metrics_sampling_frequency_secs` 为采样频率（秒）；`track_stats` /
  /// `track_latency` / `track_command_stats` 决定相应成员是否就位。
  pub fn new(
    metrics_sampling_frequency_secs: u64,
    track_stats: bool,
    track_latency: bool,
    track_command_stats: bool,
  ) -> Self {
    Self {
      reset_event_flags: [false; InfoMetricsType::ALL.len()],
      reset_latency_metrics: [false; LatencyMetricsType::ALL.len()],
      monitor_sampling_frequency: Duration::from_secs(metrics_sampling_frequency_secs),
      monitor_iterations: Arc::new(AtomicU64::new(0)),
      state: Mutex::new(MonitorState {
        global_metrics: GarnetServerMetrics::new(track_stats, track_latency, track_command_stats),
        acc_session_metrics: GarnetSessionMetrics::default(),
        acc_command_stats: track_command_stats.then(CommandStats::new),
        instant_input_net_bytes: 0,
        instant_output_net_bytes: 0,
        instant_commands_processed: 0,
      }),
    }
  }

  /// libs/server/Metrics/GarnetServerMonitor.cs:GlobalMetrics
  ///
  /// 全局延迟指标的共享句柄（会话直查合并用）；未启用为 None。
  pub fn global_latency_metrics(
    &self,
  ) -> Option<Arc<parking_lot::Mutex<super::latency::garnet_latency_metrics::GarnetLatencyMetrics>>>
  {
    self
      .state
      .lock()
      .global_metrics
      .global_latency_metrics
      .clone()
  }

  /// 全局指标连接计数与瞬时吞吐快照（数值随采样变动）。
  pub fn global_metrics_snapshot(&self) -> (i64, i64, i64, f64, f64, f64) {
    let state = self.state.lock();
    let g = &state.global_metrics;
    (
      g.total_connections_received,
      g.total_connections_disposed,
      g.total_connections_active,
      g.instantaneous_cmd_per_sec,
      g.instantaneous_net_input_tpt,
      g.instantaneous_net_output_tpt,
    )
  }

  /// 共享迭代时钟（会话侧延迟指标构造用）。
  pub fn shared_iterations(&self) -> Arc<AtomicU64> {
    self.monitor_iterations.clone()
  }

  /// libs/server/Metrics/GarnetServerMonitor.cs:AddMetricsHistorySessionDispose
  ///
  /// 会话释放时将其指标并入历史（会话指标 / 延迟指标 / 命令统计均可空）；
  /// 延迟指标经"上一版本"缓冲合并（对齐 C# Merge），随后归还会话指标
  ///（对齐 currLatencyMetrics?.Return()）。
  pub fn add_metrics_history_session_dispose(
    &self,
    curr_session_metrics: Option<&GarnetSessionMetrics>,
    curr_latency_metrics: Option<&GarnetLatencyMetricsSession>,
    curr_command_stats: Option<&CommandStats>,
  ) {
    let mut state = self.state.lock();
    if let Some(metrics) = curr_session_metrics
      && let Some(history) = &mut state.global_metrics.history_session_metrics
    {
      history.add(metrics);
    }
    if let Some(latency) = curr_latency_metrics {
      if let Some(global_latency) = &mut state.global_metrics.global_latency_metrics {
        global_latency.lock().merge(latency);
      }
      latency.return_to_pool();
    }
    if let Some(stats) = curr_command_stats
      && let Some(history) = &mut state.global_metrics.history_command_stats
    {
      history.add(stats);
    }
  }

  /// libs/server/Metrics/GarnetServerMonitor.cs:GetAllLocksets
  ///
  /// 汇总各会话事务锁集合的诊断文本；`locksets` 产出
  /// (会话 StoreSessionID, 锁集合文本)，空锁集合跳过。
  pub fn get_all_locksets(locksets: impl Iterator<Item = (i64, String)>) -> String {
    let mut result = String::new();
    for (session_id, lockset) in locksets {
      if !lockset.is_empty() {
        result += &format!("{session_id}: {lockset}\n");
      }
    }
    result
  }

  /// 延迟监视是否启用。
  fn track_latency(state: &MonitorState) -> bool {
    state.global_metrics.global_latency_metrics.is_some()
  }

  /// libs/server/Metrics/GarnetServerMonitor.cs:UpdateInstantaneousMetrics
  ///
  /// 以采样间隔折算瞬时吞吐（KiB/s 与命令/s），并滚动基线计数。
  fn update_instantaneous_metrics(state: &mut MonitorState, elapsed_sec: f64) {
    let elapsed_units = elapsed_sec * GarnetServerMetrics::BYTE_UNIT as f64;
    let g = &mut state.global_metrics;
    g.instantaneous_net_input_tpt = (g
      .global_session_metrics
      .as_ref()
      .map_or(0, GarnetSessionMetrics::get_total_net_input_bytes)
      - state.instant_input_net_bytes) as f64
      / elapsed_units;
    g.instantaneous_net_output_tpt = (g
      .global_session_metrics
      .as_ref()
      .map_or(0, GarnetSessionMetrics::get_total_net_output_bytes)
      - state.instant_output_net_bytes) as f64
      / elapsed_units;
    g.instantaneous_cmd_per_sec = (g
      .global_session_metrics
      .as_ref()
      .map_or(0, GarnetSessionMetrics::get_total_commands_processed)
      - state.instant_commands_processed) as f64
      / elapsed_sec;

    g.instantaneous_net_input_tpt = round2(g.instantaneous_net_input_tpt);
    g.instantaneous_net_output_tpt = round2(g.instantaneous_net_output_tpt);
    g.instantaneous_cmd_per_sec = g.instantaneous_cmd_per_sec.round();

    state.instant_input_net_bytes = g
      .global_session_metrics
      .as_ref()
      .map_or(0, GarnetSessionMetrics::get_total_net_input_bytes);
    state.instant_output_net_bytes = g
      .global_session_metrics
      .as_ref()
      .map_or(0, GarnetSessionMetrics::get_total_net_output_bytes);
    state.instant_commands_processed = g
      .global_session_metrics
      .as_ref()
      .map_or(0, GarnetSessionMetrics::get_total_commands_processed);
  }

  /// libs/server/Metrics/GarnetServerMonitor.cs:AddCurrentServerStats
  ///
  /// 累加单台服务器的活跃会话指标 / 命令统计 / 延迟指标，随后以累加值
  /// 重建全局会话指标与命令统计。
  fn add_current_server_stats(state: &mut MonitorState, server: &ServerSample<'_>) {
    for session in &server.sessions {
      state.acc_session_metrics.add(session.metrics);
      if let (Some(acc), Some(stats)) = (&mut state.acc_command_stats, session.command_stats) {
        acc.add(stats);
      }
      if let (Some(global_latency), Some(latency)) = (
        &state.global_metrics.global_latency_metrics,
        session.latency,
      ) {
        global_latency.lock().merge(latency);
      }
    }

    // 重置全局会话指标后并入本轮累加值。
    if let Some(global_session) = &mut state.global_metrics.global_session_metrics {
      global_session.reset();
      global_session.add(&state.acc_session_metrics);
    }
    if let Some(global_stats) = &mut state.global_metrics.global_command_stats {
      global_stats.reset();
      if let Some(acc) = &state.acc_command_stats {
        global_stats.add(acc);
      }
    }
  }

  /// libs/server/Metrics/GarnetServerMonitor.cs:ResetAndAddGlobalHistory
  ///
  /// 重置累加器并并入历史（每轮采样开始前调用）。
  fn reset_and_add_global_history(state: &mut MonitorState) {
    state.acc_session_metrics.reset();
    if let Some(history) = &state.global_metrics.history_session_metrics {
      state.acc_session_metrics.add(history);
    }
    if let Some(acc) = &mut state.acc_command_stats {
      acc.reset();
      if let Some(history) = &state.global_metrics.history_command_stats {
        acc.add(history);
      }
    }
  }

  /// libs/server/Metrics/GarnetServerMonitor.cs:CleanupGlobalStats
  ///
  /// INFO RESET 触发的清理：STATS 标志复位瞬时吞吐、连接计数、全局/历史
  /// 会话指标（活跃部分经 `reset_active_sessions` 回调下沉到服务器域）；
  /// COMMANDSTATS 标志复位全局/历史命令统计（经 `reset_active_command_stats`）。
  fn cleanup_global_stats(
    state: &mut MonitorState,
    flags: &mut [bool],
    reset_active_sessions: &mut dyn FnMut(),
    reset_active_command_stats: &mut dyn FnMut(),
  ) {
    if flags[InfoMetricsType::Stats as usize] {
      log::info!("Resetting latency metrics for commands");
      state.global_metrics.instantaneous_net_input_tpt = 0.0;
      state.global_metrics.instantaneous_net_output_tpt = 0.0;
      state.global_metrics.instantaneous_cmd_per_sec = 0.0;

      state.global_metrics.total_connections_received = 0;
      state.global_metrics.total_connections_disposed = 0;
      if let Some(global_session) = &mut state.global_metrics.global_session_metrics {
        global_session.reset();
      }
      if let Some(history) = &mut state.global_metrics.history_session_metrics {
        history.reset();
      }

      reset_active_sessions();
      flags[InfoMetricsType::Stats as usize] = false;
    }

    if flags[InfoMetricsType::CommandStats as usize] {
      log::info!("Resetting command stats");
      if let Some(global_stats) = &mut state.global_metrics.global_command_stats {
        global_stats.reset();
      }
      if let Some(history) = &mut state.global_metrics.history_command_stats {
        history.reset();
      }
      reset_active_command_stats();
      flags[InfoMetricsType::CommandStats as usize] = false;
    }
  }

  /// libs/server/Metrics/GarnetServerMonitor.cs:CleanupGlobalLatencyMetrics
  ///
  /// LATENCY RESET 触发的清理：复位全局延迟指标并将复位下沉到活跃会话
  ///（经 `reset_session_latency` 回调）。
  fn cleanup_global_latency_metrics(
    state: &mut MonitorState,
    flags: &mut [bool],
    reset_session_latency: &mut dyn FnMut(LatencyMetricsType),
  ) {
    if !Self::track_latency(state) {
      return;
    }
    for (idx, flagged) in flags.iter_mut().enumerate() {
      if !*flagged {
        continue;
      }
      let Some(event_type) = LatencyMetricsType::ALL.get(idx).copied() else {
        continue;
      };
      log::info!("Resetting server-side stats {event_type:?}");
      reset_session_latency(event_type);
      if let Some(global_latency) = &state.global_metrics.global_latency_metrics {
        global_latency.lock().reset(event_type);
      }
      *flagged = false;
    }
  }

  /// libs/server/Metrics/GarnetServerMonitor.cs:ResetLatencySessionMetrics
  ///
  /// 延迟监视启用时复位全部活跃会话的延迟指标（活跃会话遍历经
  /// `reset_all_session_latency` 回调下沉到服务器域）。
  fn reset_latency_session_metrics(
    state: &MonitorState,
    reset_all_session_latency: &mut dyn FnMut(),
  ) {
    if Self::track_latency(state) {
      reset_all_session_latency();
    }
  }

  /// libs/server/Metrics/GarnetServerMonitor.cs:MainMonitorTaskAsync（单轮迭代体）
  ///
  /// C# 主循环内除 Task.Delay 外的全部步骤。
  fn monitor_iteration(&mut self, inputs: &mut MonitorIterationInputs<'_>) {
    let mut state = self.state.lock();

    // 复位上一版本的会话级延迟指标（即将成为当前版本）。
    Self::reset_latency_session_metrics(&state, inputs.reset_all_session_latency);

    // 版本推进需先于延迟合并（依赖 prior_version 语义）。
    self.monitor_iterations.fetch_add(1, Ordering::Relaxed);

    // 重置累加器并并入历史。
    Self::reset_and_add_global_history(&mut state);

    let (mut total_received, mut total_disposed, mut total_active) = (0i64, 0i64, 0i64);
    for server in inputs.servers {
      total_received += server.total_connections_received;
      total_disposed += server.total_connections_disposed;
      total_active += server.total_connections_active;
      Self::add_current_server_stats(&mut state, server);
    }

    Self::update_instantaneous_metrics(&mut state, self.monitor_sampling_frequency.as_secs_f64());
    state.global_metrics.total_connections_received = total_received;
    state.global_metrics.total_connections_disposed = total_disposed;
    state.global_metrics.total_connections_active = total_active;

    // INFO RESET 清理。
    Self::cleanup_global_stats(
      &mut state,
      &mut self.reset_event_flags,
      inputs.reset_active_sessions,
      inputs.reset_active_command_stats,
    );
    Self::cleanup_global_latency_metrics(
      &mut state,
      &mut self.reset_latency_metrics,
      inputs.reset_session_latency,
    );
  }

  /// libs/server/Metrics/GarnetServerMonitor.cs:MainMonitorTaskAsync
  ///
  /// 周期采样主循环：每轮 `sleep(采样周期)` 后执行一次迭代；
  /// `cancelled` 为取消探测（对齐 CancellationToken），取消即退出
  ///（对齐 C# 取消终止 + done.Set()）。输入每轮经 `resolve` 重新取用
  ///（对齐 C# 直查活跃会话）。
  pub async fn main_monitor_task_async<S, Fut>(
    &mut self,
    mut sleep: S,
    cancelled: impl Fn() -> bool,
    mut resolve: impl FnMut() -> MonitorIterationInputs<'static>,
  ) where
    S: FnMut(Duration) -> Fut,
    Fut: Future<Output = ()>,
  {
    while !cancelled() {
      sleep(self.monitor_sampling_frequency).await;
      self.monitor_iteration(&mut resolve());
    }
  }
}

/// 两位小数四舍五入（对齐 Math.Round(x, 2)）。
#[inline]
fn round2(v: f64) -> f64 {
  (v * 100.0).round() / 100.0
}
