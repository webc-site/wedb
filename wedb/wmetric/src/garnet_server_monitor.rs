use std::{
  future::Future,
  sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
  time::Duration,
};

use parking_lot::Mutex;
use wresp::metrics::InfoMetricsType;

use super::{
  command_stats::CommandStats,
  garnet_server_metrics::GarnetServerMetrics,
  garnet_session_metrics::GarnetSessionMetrics,
  info::garnet_info_metrics::GlobalMetricsSnapshot,
  latency::{
    garnet_latency_metrics_session::GarnetLatencyMetricsSession,
    latency_metrics_type::LatencyMetricsType,
  },
};

/// 服务器指标监视器：周期采样活跃会话，维护全局指标与瞬时吞吐。
///（对标 libs/server/Metrics/GarnetServerMonitor.cs:GarnetServerMonitor）
///
/// C# 经 `ActiveConsumers()` 直查会话；rust 会话体为连接任务独占，采样经
/// [`SessionSample`] / [`ServerSample`] 拥有型快照承接同等聚合语义。
/// 可变状态收敛在 `Mutex<MonitorState>` 内，支持并发的历史并入
///（对齐 C# SingleWriterMultiReaderLock 的保护范围）；
/// 复位标志为原子位（对齐 C# resetEventFlags / resetLatencyMetrics 字典），
/// 全体方法 `&self`——采样循环可在无外层互斥下驱动（C# 为后台 Task）。
pub struct GarnetServerMonitor {
  /// INFO RESET 置位的事件复位标志（下标 = InfoMetricsType 判别值）。
  reset_event_flags: [AtomicBool; InfoMetricsType::ALL.len()],
  /// LATENCY RESET 置位的延迟类别复位标志（下标 = 类别判别值）。
  reset_latency_metrics: [AtomicBool; LatencyMetricsType::ALL.len()],
  /// 采样周期。
  monitor_sampling_frequency: Duration,
  /// 迭代时钟：与全部会话侧延迟指标共享（奇偶切版本）。
  pub monitor_iterations: Arc<AtomicU64>,
  /// 全局指标 + 累加器 + 上轮瞬时基线。
  state: Mutex<MonitorState>,
}

/// 进程级监视器槽（C# storeWrapper.monitor 的单服务器进程级承接；
/// 会话 dispose 归并经 [`GarnetServerMonitor::global`] 直取，免去会话体
/// 持有句柄）
static GLOBAL_MONITOR: OnceLock<Arc<GarnetServerMonitor>> = OnceLock::new();

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

/// 单个活跃会话的采样快照（拥有型：采样循环与活会话任务并发，持有型引用
/// 不可达；rust 承接为逐字段值拷贝，对齐 C# 直读后的瞬时视图）
pub struct SessionSample {
  /// 会话指标。
  pub metrics: GarnetSessionMetrics,
  /// 会话命令统计（未启用为 None）。
  pub command_stats: Option<CommandStats>,
  /// 会话延迟指标（未启用为 None；Arc 活句柄，合并经内部互斥）。
  pub latency: Option<Arc<GarnetLatencyMetricsSession>>,
}

/// 单个服务器的采样快照。
pub struct ServerSample {
  /// 收到的连接数。
  pub total_connections_received: i64,
  /// 已释放的连接数。
  pub total_connections_disposed: i64,
  /// 活跃连接数。
  pub total_connections_active: i64,
  /// 活跃会话采样。
  pub sessions: Vec<SessionSample>,
}

/// 单轮迭代的外部输入（会话/服务器域复位回调集合；拥有型——采样循环与
/// 活会话任务并发，借用型回调无法跨 await 持有）
pub struct MonitorIterationInputs<F1 = fn(), F2 = fn(), F3 = fn(), F4 = fn(LatencyMetricsType)> {
  /// 全部服务器的采样快照。
  pub servers: Vec<ServerSample>,
  /// 复位全部活跃会话的延迟指标。
  pub reset_all_session_latency: F1,
  /// 复位全部活跃会话的会话指标（STATS 复位路径）。
  pub reset_active_sessions: F2,
  /// 复位全部活跃会话的命令统计（COMMANDSTATS 复位路径）。
  pub reset_active_command_stats: F3,
  /// 复位指定类别的活跃会话延迟指标。
  pub reset_session_latency: F4,
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
      reset_event_flags: [const { AtomicBool::new(false) }; InfoMetricsType::ALL.len()],
      reset_latency_metrics: [const { AtomicBool::new(false) }; LatencyMetricsType::ALL.len()],
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

  /// 进程级安装监视器槽（幂等；首次安装生效，返回是否由本次写入）。
  ///
  /// libs/server/StoreWrapper.cs:226（monitor 随 StoreWrapper 构造）的进程级承接
  pub fn install_global(self: &Arc<Self>) -> bool {
    GLOBAL_MONITOR.set(Arc::clone(self)).is_ok()
  }

  /// 取进程级监视器（未安装为 None；C# `storeWrapper.monitor != null` 判定）
  pub fn global() -> Option<Arc<Self>> {
    GLOBAL_MONITOR.get().cloned()
  }

  /// 置位 INFO 段复位标志（C# monitor.resetEventFlags[e] = true）
  pub fn set_info_reset_flag(&self, info_metrics_type: InfoMetricsType) {
    self.reset_event_flags[info_metrics_type.idx()].store(true, Ordering::Relaxed);
  }

  /// 置位延迟类别复位标志（C# monitor.resetLatencyMetrics[e] = true）
  pub fn set_latency_reset_flag(&self, latency_metrics_type: LatencyMetricsType) {
    self.reset_latency_metrics[latency_metrics_type.idx()].store(true, Ordering::Relaxed);
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

  /// 全局指标快照（INFO STATS 数据面读取入口）。
  pub fn snapshot(&self) -> Option<GlobalMetricsSnapshot> {
    let state = self.state.lock();
    let g = &state.global_metrics;
    let global_session_metrics = g.global_session_metrics?;
    Some(GlobalMetricsSnapshot {
      total_connections_received: g.total_connections_received,
      total_connections_disposed: g.total_connections_disposed,
      total_connections_active: g.total_connections_active,
      instantaneous_cmd_per_sec: g.instantaneous_cmd_per_sec,
      instantaneous_net_input_tpt: g.instantaneous_net_input_tpt,
      instantaneous_net_output_tpt: g.instantaneous_net_output_tpt,
      global_session_metrics,
    })
  }

  /// 是否开启逐命令统计追踪（C# serverOptions.CommandStatsMonitor 的监视器侧投影）。
  pub fn tracks_command_stats(&self) -> bool {
    self
      .state
      .lock()
      .global_metrics
      .global_command_stats
      .is_some()
  }

  /// INFO COMMANDSTATS 聚合快照（C# PopulateCommandStatsInfo 的
  /// monitor.GlobalMetrics 读取形态：周期采样开启时 globalCommandStats 已含
  /// history + 活跃会话的上轮采样；仅命令统计开启时取 historyCommandStats，
  /// 活跃会话未归并部分由调用方补并）。
  pub fn command_stats_aggregate(&self) -> Option<CommandStats> {
    let state = self.state.lock();
    let g = &state.global_metrics;
    if let Some(global) = &g.global_command_stats {
      return Some(global.clone());
    }
    g.history_command_stats.clone()
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

  /// 延迟监视是否启用。
  fn track_latency(state: &MonitorState) -> bool {
    state.global_metrics.global_latency_metrics.is_some()
  }

  /// libs/server/Metrics/GarnetServerMonitor.cs:UpdateInstantaneousMetrics
  ///
  /// 以采样间隔折算瞬时吞吐（KiB/s 与命令/s），并滚动基线计数。
  fn update_instantaneous_metrics(state: &mut MonitorState, elapsed_sec: f64) {
    let elapsed_units = elapsed_sec * GarnetServerMetrics::BYTE_UNIT as f64;
    let (curr_input, curr_output, curr_cmds) = state
      .global_metrics
      .global_session_metrics
      .as_ref()
      .map_or((0, 0, 0), |m| {
        (
          m.total_net_input_bytes,
          m.total_net_output_bytes,
          m.total_commands_processed,
        )
      });

    let g = &mut state.global_metrics;
    g.instantaneous_net_input_tpt =
      round2((curr_input - state.instant_input_net_bytes) as f64 / elapsed_units);
    g.instantaneous_net_output_tpt =
      round2((curr_output - state.instant_output_net_bytes) as f64 / elapsed_units);
    g.instantaneous_cmd_per_sec =
      ((curr_cmds - state.instant_commands_processed) as f64 / elapsed_sec).round();

    state.instant_input_net_bytes = curr_input;
    state.instant_output_net_bytes = curr_output;
    state.instant_commands_processed = curr_cmds;
  }

  /// libs/server/Metrics/GarnetServerMonitor.cs:AddCurrentServerStats
  ///
  /// 累加单台服务器的活跃会话指标 / 命令统计 / 延迟指标，随后以累加值
  /// 重建全局会话指标与命令统计。
  fn add_current_server_stats(state: &mut MonitorState, server: &ServerSample) {
    for session in &server.sessions {
      state.acc_session_metrics.add(&session.metrics);
      if let (Some(acc), Some(stats)) = (&mut state.acc_command_stats, &session.command_stats) {
        acc.add(stats);
      }
      if let (Some(global_latency), Some(latency)) = (
        &state.global_metrics.global_latency_metrics,
        &session.latency,
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
  /// libs/server/Metrics/GarnetServerMonitor.cs:CleanupGlobalStats
  ///
  /// INFO RESET 触发的清理：STATS 标志复位瞬时吞吐、连接计数、全局/历史
  /// 会话指标（活跃部分经 `reset_active_sessions` 回调下沉到服务器域）；
  /// COMMANDSTATS 标志复位全局/历史命令统计（经 `reset_active_command_stats`）。
  fn cleanup_global_stats(
    state: &mut MonitorState,
    flags: &[AtomicBool],
    mut reset_active_sessions: impl FnMut(),
    mut reset_active_command_stats: impl FnMut(),
  ) {
    if flags[InfoMetricsType::Stats as usize].load(Ordering::Relaxed) {
      log::info!("Resetting latency metrics for commands");
      state.global_metrics.instantaneous_net_input_tpt = 0.0;
      state.global_metrics.instantaneous_net_output_tpt = 0.0;
      state.global_metrics.instantaneous_cmd_per_sec = 0.0;
      // 瞬时基线同步归零（C# 未复位私有 instant_* 字段系 unchecked ulong
      // 回绕的潜在缺陷，rust u64 下溢即 panic，复位以成立「下轮丢弃旧累计」）
      state.instant_input_net_bytes = 0;
      state.instant_output_net_bytes = 0;
      state.instant_commands_processed = 0;

      state.global_metrics.total_connections_received = 0;
      state.global_metrics.total_connections_disposed = 0;
      if let Some(global_session) = &mut state.global_metrics.global_session_metrics {
        global_session.reset();
      }
      if let Some(history) = &mut state.global_metrics.history_session_metrics {
        history.reset();
      }

      reset_active_sessions();
      flags[InfoMetricsType::Stats as usize].store(false, Ordering::Relaxed);
    }

    if flags[InfoMetricsType::CommandStats as usize].load(Ordering::Relaxed) {
      log::info!("Resetting command stats");
      if let Some(global_stats) = &mut state.global_metrics.global_command_stats {
        global_stats.reset();
      }
      if let Some(history) = &mut state.global_metrics.history_command_stats {
        history.reset();
      }
      reset_active_command_stats();
      flags[InfoMetricsType::CommandStats as usize].store(false, Ordering::Relaxed);
    }
  }

  /// libs/server/Metrics/GarnetServerMonitor.cs:CleanupGlobalLatencyMetrics
  ///
  /// LATENCY RESET 触发的清理：复位全局延迟指标并将复位下沉到活跃会话
  ///（经 `reset_session_latency` 回调）。
  fn cleanup_global_latency_metrics(
    state: &mut MonitorState,
    flags: &[AtomicBool],
    mut reset_session_latency: impl FnMut(LatencyMetricsType),
  ) {
    if !Self::track_latency(state) {
      return;
    }
    for (idx, flagged) in flags.iter().enumerate() {
      if !flagged.load(Ordering::Relaxed) {
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
      flagged.store(false, Ordering::Relaxed);
    }
  }

  /// libs/server/Metrics/GarnetServerMonitor.cs:ResetLatencySessionMetrics
  ///
  /// 延迟监视启用时复位全部活跃会话的延迟指标（活跃会话遍历经
  /// `reset_all_session_latency` 回调下沉到服务器域）。
  fn reset_latency_session_metrics<F: FnMut()>(
    state: &MonitorState,
    mut reset_all_session_latency: F,
  ) {
    if Self::track_latency(state) {
      reset_all_session_latency();
    }
  }

  /// 对应 MainMonitorTaskAsync 的单轮迭代体（C# 主循环内除 Task.Delay 外的全部步骤）。
  fn monitor_iteration<F1, F2, F3, F4>(&self, inputs: &mut MonitorIterationInputs<F1, F2, F3, F4>)
  where
    F1: FnMut(),
    F2: FnMut(),
    F3: FnMut(),
    F4: FnMut(LatencyMetricsType),
  {
    let mut state = self.state.lock();

    // 复位上一版本的会话级延迟指标（即将成为当前版本）。
    Self::reset_latency_session_metrics(&state, &mut inputs.reset_all_session_latency);

    // 版本推进需先于延迟合并（依赖 prior_version 语义）。
    self.monitor_iterations.fetch_add(1, Ordering::Relaxed);

    // 重置累加器并并入历史。
    Self::reset_and_add_global_history(&mut state);

    let (mut total_received, mut total_disposed, mut total_active) = (0i64, 0i64, 0i64);
    for server in &inputs.servers {
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
      &self.reset_event_flags,
      &mut inputs.reset_active_sessions,
      &mut inputs.reset_active_command_stats,
    );
    Self::cleanup_global_latency_metrics(
      &mut state,
      &self.reset_latency_metrics,
      &mut inputs.reset_session_latency,
    );
  }

  /// libs/server/Metrics/GarnetServerMonitor.cs:MainMonitorTaskAsync
  ///
  /// 周期采样主循环：每轮 `sleep(采样周期)` 后执行一次迭代；
  /// `cancelled` 为取消探测（对齐 CancellationToken），取消即退出
  ///（对齐 C# 取消终止 + done.Set()）。输入每轮经 `resolve` 重新取用
  ///（对齐 C# 直查活跃会话）。
  pub async fn main_monitor_task_async<S, Fut, F1, F2, F3, F4>(
    &self,
    mut sleep: S,
    cancelled: impl Fn() -> bool,
    mut resolve: impl FnMut() -> MonitorIterationInputs<F1, F2, F3, F4>,
  ) where
    S: FnMut(Duration) -> Fut,
    Fut: Future<Output = ()>,
    F1: FnMut(),
    F2: FnMut(),
    F3: FnMut(),
    F4: FnMut(LatencyMetricsType),
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
