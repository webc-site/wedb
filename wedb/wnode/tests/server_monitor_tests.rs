//! 服务器监视器采样循环与会话 dispose 指标归并对标测试
//!
//! C# 参照 GarnetServerMonitor 的 Start / MainMonitorTaskAsync 与
//! RespServerSession Dispose 尾部 AddMetricsHistorySessionDispose 归并点
//!（锚点分别落位于 wnode::server::start_server_monitor、
//! wmetric::GarnetServerMonitor 与 resp_server_session::dispose）。

use std::{
  future::ready,
  sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
  },
};

use compio::runtime::Runtime;
use wmetric::{GarnetServerMonitor, InfoMetricsType, MonitorIterationInputs};
use wnode::{
  resp::resp_server_session::{RespServerSession, RespServerSessionOptions},
  servers::consumer_registry::ConsumerRegistry,
};

/// 无活跃会话复位回调（零捕获形态，同 wnode::server 采样装配）
fn no_reset_sessions() {}
fn no_reset_command_stats() {}
fn no_reset_session_latency() {}

/// 驱动 N 轮采样（空复位回调；快照源为注册表）
async fn run_iterations(monitor: &GarnetServerMonitor, registry: &ConsumerRegistry, rounds: u32) {
  let done = Arc::new(AtomicU32::new(0));
  let done_cancel = Arc::clone(&done);
  monitor
    .main_monitor_task_async(
      |_duration| ready(()),
      move || done_cancel.load(Ordering::Relaxed) >= rounds,
      || {
        done.fetch_add(1, Ordering::Relaxed);
        MonitorIterationInputs {
          servers: vec![registry.monitor_sample()],
          reset_all_session_latency: no_reset_session_latency,
          reset_active_sessions: no_reset_sessions,
          reset_active_command_stats: no_reset_command_stats,
          reset_session_latency: |_| {},
        }
      },
    )
    .await;
}

/// 采样循环驱动：迭代时钟推进、连接计数与瞬时吞吐滚动
///（C# MainMonitorTaskAsync + UpdateInstantaneousMetrics）
#[test]
fn monitor_sampling_loop_rolls_metrics() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let monitor = Arc::new(GarnetServerMonitor::new(1, true, false, false));
    let registry = Arc::new(ConsumerRegistry::new());
    let entry = registry.register(1, "127.0.0.1:50000".into(), "127.0.0.1:6379".into());
    entry.add_net_bytes(2048, 1024);

    run_iterations(&monitor, &registry, 1).await;

    assert_eq!(monitor.monitor_iterations.load(Ordering::Relaxed), 1);
    let (received, disposed, active, cmd_per_sec, input_tpt, output_tpt) =
      monitor.global_metrics_snapshot();
    assert_eq!((received, disposed, active), (1, 0, 1));
    // 2048B / (1s × 1KiB) = 2.0；1024B → 1.0（C# byteUnit 换算）
    assert_eq!(input_tpt, 2.0);
    assert_eq!(output_tpt, 1.0);
    assert_eq!(cmd_per_sec, 0.0);

    // 无新增流量的下一轮：瞬时吞吐按「当轮累计 - 上轮基线」回落 0
    //（C# UpdateInstantaneousMetrics 基线滚动语义）
    run_iterations(&monitor, &registry, 1).await;
    let (_, _, _, _, input_tpt, _) = monitor.global_metrics_snapshot();
    assert_eq!(input_tpt, 0.0);

    // INFO RESET STATS 标志经采样轮清位
    monitor.set_info_reset_flag(InfoMetricsType::Stats);
    run_iterations(&monitor, &registry, 1).await;
    assert!(!monitor.info_reset_flag(InfoMetricsType::Stats));
  });
  Ok(())
}

/// dispose 指标归并链路：会话 dispose → 全局监视器历史并入 → 采样轮
/// 重建全局会话指标（C# Dispose 尾部 AddMetricsHistorySessionDispose）
#[test]
fn session_dispose_merges_into_monitor_history() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let monitor = Arc::new(GarnetServerMonitor::new(1, true, true, false));
    assert!(monitor.install_global(), "监视器进程级安装");

    let mut session = RespServerSession::new(
      42,
      RespServerSessionOptions {
        latency_monitor: true,
        metrics_sampling_frequency: true,
        ..RespServerSessionOptions::default()
      },
    );
    // PING 一条命令：会话指标累计网络入出与命令数（出字节随 take_output
    // 计入，对齐网络泵取走应答的真实时序）
    assert!(session.try_consume_messages(b"PING\r\n").is_some());
    let _ = session.take_output();
    assert!(
      session
        .session_metrics
        .as_ref()
        .is_some_and(|m| m.get_total_commands_processed() >= 1)
    );

    // dispose 尾部归并（监视器未装配时为无害空操作，此处已装配）
    session.dispose();

    // 单轮采样：历史并入全局会话指标
    let registry = Arc::new(ConsumerRegistry::new());
    run_iterations(&monitor, &registry, 1).await;

    let (input, output, commands) = monitor.global_totals().expect("stats tracked");
    assert!(input > 0);
    assert!(output > 0);
    assert!(commands >= 1);
  });
  Ok(())
}
