//! INFO COMMANDSTATS 聚合真源按采样频率选择对标测试
//!（对标 libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateCommandStatsInfo
//! :233 分支键 MetricsSamplingFrequency > 0 与 :244-256 按需聚合臂；
//! 判别单点落 wmetric::GarnetServerMonitor::command_stats_aggregate）
//!
//! 自研回归锁: commandstats 单开（频率缺省 0）形态聚合真源为 history，
//! 周期采样形态真源为 global

use std::{cell::Cell, future::ready};

use wbase::future::block_on;
use wmetric::{
  CommandStats, GarnetServerMonitor, GarnetSessionMetrics, MonitorIterationInputs, ServerSample,
  SessionSample,
};
use wresp::command::RespCommand;

/// 逐命令计数样本（单命令 calls 非零，其余零值）
fn stats_with(cmd: RespCommand, calls: u64) -> CommandStats {
  let mut stats = CommandStats::new();
  for _ in 0..calls {
    stats.increment_calls(cmd);
  }
  stats
}

/// 读取聚合快照内指定命令的 calls 计数（None = 聚合缺席）
fn aggregate_calls(monitor: &GarnetServerMonitor, cmd: RespCommand) -> Option<u64> {
  monitor
    .command_stats_aggregate()
    .map(|agg| agg.entries[cmd as u16 as usize].calls)
}

/// 单轮采样驱动（会话命令统计镜像经 SessionSample 承接，形态对齐
/// instantaneous_metrics_underflow.rs 的 run_rounds）
fn run_sample_round(monitor: &GarnetServerMonitor, session_stats: Option<CommandStats>) {
  let done = Cell::new(0usize);
  block_on(monitor.main_monitor_task_async(
    |_d| ready(()),
    || done.get() >= 1,
    || {
      done.set(done.get() + 1);
      MonitorIterationInputs {
        servers: vec![ServerSample {
          total_connections_received: 1,
          total_connections_disposed: 0,
          total_connections_active: 1,
          sessions: vec![SessionSample {
            metrics: GarnetSessionMetrics::default(),
            command_stats: session_stats.clone(),
          }],
        }],
        reset_active_sessions: || {},
        reset_active_command_stats: || {},
        reset_gossip_stats: || {},
        reset_revivification_stats: || {},
      }
    },
  ));
}

/// 频率 0（commandstats 单开合法形态）：聚合真源为 history——dispose 归并
/// 累计在场即如实读出；缺陷形态（无条件优先恒零 global）下本断言为红
#[test]
fn freq_zero_aggregates_history() {
  let monitor = GarnetServerMonitor::new(0, true, false, true);
  monitor.add_metrics_history_session_dispose(None, Some(&stats_with(RespCommand::Ping, 4)));
  assert_eq!(
    aggregate_calls(&monitor, RespCommand::Ping),
    Some(4),
    "频率 0 形态聚合须取 history（dispose 归并累计）"
  );
}

/// 频率 > 0：聚合真源为 global——采样轮后 global 已含 history + 活跃会话
/// 上轮值（history Set=5 + 会话 Set=2 → 7），补并臂不再叠加
#[test]
fn freq_positive_aggregates_sampled_global() {
  let monitor = GarnetServerMonitor::new(1, true, false, true);
  monitor.add_metrics_history_session_dispose(None, Some(&stats_with(RespCommand::Set, 5)));
  run_sample_round(&monitor, Some(stats_with(RespCommand::Set, 2)));
  assert_eq!(
    aggregate_calls(&monitor, RespCommand::Set),
    Some(7),
    "频率 > 0 形态聚合须取采样后的 global（history + 活跃会话上轮值）"
  );
}

/// 频率 > 0 首采样轮前：global 零表单源直出（C# :233-238 同构——history
/// 已有归并不经本臂回灌，待采样轮并入），既有行为回归钉
#[test]
fn freq_positive_reads_zero_global_before_first_round() {
  let monitor = GarnetServerMonitor::new(1, true, false, true);
  monitor.add_metrics_history_session_dispose(None, Some(&stats_with(RespCommand::Set, 5)));
  assert_eq!(
    aggregate_calls(&monitor, RespCommand::Set),
    Some(0),
    "频率 > 0 形态真源为 global，history 不经本臂单独回灌"
  );
  run_sample_round(&monitor, None);
  assert_eq!(
    aggregate_calls(&monitor, RespCommand::Set),
    Some(5),
    "首采样轮后 global 并入 history 累计"
  );
}
