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
    atomic::{AtomicU32, AtomicUsize, Ordering},
  },
};

use compio::runtime::Runtime;
use wmetric::{GarnetServerMonitor, SessionMetricsHandle};
use wnode::{
  resp::resp_server_session::{RespServerSession, RespServerSessionOptions},
  servers::consumer_registry::ConsumerRegistry,
};
use wnode_test::drain_output;
use wresp::metrics::InfoMetricsType;

/// 全局累计三元组（入字节, 出字节, 命令数）：eca90c2f 收口后全局会话指标
/// 唯一读出口是 [`GarnetServerMonitor::snapshot`]，复位断言据此取数
fn global_totals(monitor: &GarnetServerMonitor) -> Option<(u64, u64, u64)> {
  monitor.snapshot().map(|snap| {
    let m = snap.global_session_metrics;
    (
      m.total_net_input_bytes,
      m.total_net_output_bytes,
      m.total_commands_processed,
    )
  })
}

/// 驱动 N 轮采样（复位回调与宿主装配同构：
/// [`ConsumerRegistry::monitor_iteration_inputs`]，C# 监视器直查 servers 的承接）
async fn run_iterations(
  monitor: &GarnetServerMonitor,
  registry: &Arc<ConsumerRegistry>,
  rounds: u32,
) {
  let done = Arc::new(AtomicU32::new(0));
  let done_cancel = Arc::clone(&done);
  monitor
    .main_monitor_task_async(
      |_duration| ready(()),
      move || done_cancel.load(Ordering::Relaxed) >= rounds,
      || {
        done.fetch_add(1, Ordering::Relaxed);
        // gossip 与复活化两臂的独立观测见
        // `run_iterations_with_arm_counters`（复位语义用例）
        registry.monitor_iteration_inputs(|| {}, || {})
      },
    )
    .await;
}

/// 驱动 N 轮采样并按宿主形态注入 gossip / 复活化两臂计数闭包
///（宿主 `start_server_monitor` 每轮重建两臂闭包、各持一份句柄克隆的同构承接）
async fn run_iterations_with_arm_counters(
  monitor: &GarnetServerMonitor,
  registry: &Arc<ConsumerRegistry>,
  rounds: u32,
  gossip_resets: &Arc<AtomicUsize>,
  reviv_resets: &Arc<AtomicUsize>,
) {
  let done = Arc::new(AtomicU32::new(0));
  let done_cancel = Arc::clone(&done);
  monitor
    .main_monitor_task_async(
      |_duration| ready(()),
      move || done_cancel.load(Ordering::Relaxed) >= rounds,
      || {
        done.fetch_add(1, Ordering::Relaxed);
        let gossip = Arc::clone(gossip_resets);
        let reviv = Arc::clone(reviv_resets);
        registry.monitor_iteration_inputs(
          move || {
            gossip.fetch_add(1, Ordering::Relaxed);
          },
          move || {
            reviv.fetch_add(1, Ordering::Relaxed);
          },
        )
      },
    )
    .await;
}

/// 采样循环驱动：迭代时钟推进、连接计数与瞬时吞吐滚动
///（C# MainMonitorTaskAsync + UpdateInstantaneousMetrics）
/// 模拟泵直填一批字节（take → extend → return → consume 的会话侧等价）
fn feed(s: &mut RespServerSession, bytes: &[u8]) -> Option<usize> {
  s.recv_buffer.extend_from_slice(bytes);
  s.try_consume_messages()
}

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
    let snap = monitor.snapshot().expect("采样轮装配全局指标快照");
    assert_eq!(
      (
        snap.total_connections_received,
        snap.total_connections_disposed,
        snap.total_connections_active
      ),
      (1, 0, 1)
    );
    // 2048B / (1s × 1KiB) = 2.0；1024B → 1.0（C# byteUnit 换算）
    assert_eq!(snap.instantaneous_net_input_tpt, 2.0);
    assert_eq!(snap.instantaneous_net_output_tpt, 1.0);
    assert_eq!(snap.instantaneous_cmd_per_sec, 0.0);

    // 无新增流量的下一轮：瞬时吞吐按「当轮累计 - 上轮基线」回落 0
    //（C# UpdateInstantaneousMetrics 基线滚动语义）
    run_iterations(&monitor, &registry, 1).await;
    let snap = monitor.snapshot().expect("采样轮装配全局指标快照");
    assert_eq!(snap.instantaneous_net_input_tpt, 0.0);

    // INFO RESET STATS：标志置位后由采样轮消费丢弃旧累计（对标 C#
    // CleanupGlobalStats：全局会话指标归零，活跃条目镜像经复位回调清零，
    // 标志清位后不再触发）
    monitor.set_info_reset_flag(InfoMetricsType::Stats);
    run_iterations(&monitor, &registry, 1).await;
    assert_eq!(global_totals(&monitor), Some((0, 0, 0)));
    let sample = registry.monitor_sample();
    assert_eq!(
      (
        sample.sessions[0].metrics.total_net_input_bytes,
        sample.sessions[0].metrics.total_net_output_bytes
      ),
      (0, 0),
      "复位回调应清零活跃条目镜像"
    );
    // 再一轮无复位标志：保持归零后的复位基线（不误回灌旧累计）
    run_iterations(&monitor, &registry, 1).await;
    assert_eq!(global_totals(&monitor), Some((0, 0, 0)));
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
        ..RespServerSessionOptions::default()
      },
    );
    // 会话指标句柄经唯一注入口装配（生产由 service.rs 采样门控创建后同口注入）
    session.attach_session_metrics(Some(Arc::new(SessionMetricsHandle::default())));
    // PING 一条命令：会话指标累计网络入出与命令数（出字节随 take_output_into
    // 计入，对齐网络泵取走应答的真实时序）
    assert!(feed(&mut session, b"PING\r\n").is_some());
    let _ = drain_output(&mut session);
    assert!(
      session
        .session_metrics
        .as_ref()
        .is_some_and(|m| m.snapshot().get_total_commands_processed() >= 1)
    );

    // dispose 尾部归并（监视器未装配时为无害空操作，此处已装配）
    session.dispose();

    // 单轮采样：历史并入全局会话指标
    let registry = Arc::new(ConsumerRegistry::new());
    run_iterations(&monitor, &registry, 1).await;

    let gsm = monitor
      .snapshot()
      .expect("stats tracked")
      .global_session_metrics;
    assert!(gsm.total_net_input_bytes > 0);
    assert!(gsm.total_net_output_bytes > 0);
    assert!(gsm.total_commands_processed >= 1);
  });
  Ok(())
}

/// 延迟指标聚合与 LATENCY HISTOGRAM / RESET / HELP 命令端到端单测
#[test]
fn session_latency_metrics_aggregation_and_resp_commands() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    // 构造期注入形态（C# storeWrapper.monitor 构造下传）：进程级单例就位后，
    // 会话构造自动从 global 取 monitor_iterations 与全局延迟表；首装者生效，
    // 晚到测试经 global 复用同一句柄，采样轮驱动与 session 同源
    let local = Arc::new(GarnetServerMonitor::new(1, true, true, false));
    local.install_global();
    let monitor = GarnetServerMonitor::global().expect("进程级监视器就位");
    let registry = Arc::new(ConsumerRegistry::new());
    let entry = registry.register(100, "127.0.0.1:50001".into(), "127.0.0.1:6379".into());

    let mut session = RespServerSession::new(
      100,
      RespServerSessionOptions {
        latency_monitor: true,
        ..RespServerSessionOptions::default()
      },
    );
    entry.attach_latency_metrics(session.latency_metrics.clone());

    // 1. 执行命令并消费，产生延迟与吞吐样本
    assert!(feed(&mut session, b"PING\r\n").is_some());
    let _ = drain_output(&mut session);
    assert!(feed(&mut session, b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n").is_some());
    let _ = drain_output(&mut session);

    // 2. 驱动采样轮，将上一版本双缓冲延迟并入全局延迟指标
    run_iterations(&monitor, &registry, 1).await;

    // 3. 执行 LATENCY HISTOGRAM 命令，校验 RESP 输出
    assert!(feed(&mut session, b"*2\r\n$7\r\nLATENCY\r\n$9\r\nHISTOGRAM\r\n").is_some());
    let out = drain_output(&mut session);
    let resp_str = String::from_utf8_lossy(&out);
    assert!(
      resp_str.contains("NET_RS_LAT"),
      "HISTOGRAM 应包含 NET_RS_LAT 类别: {resp_str}"
    );
    assert!(
      resp_str.contains("histogram_usec"),
      "HISTOGRAM 应包含 histogram_usec: {resp_str}"
    );
    assert!(
      resp_str.contains("calls"),
      "HISTOGRAM 应包含 calls: {resp_str}"
    );

    // 4. 按指定类别过滤查询
    assert!(
      feed(
        &mut session,
        b"*3\r\n$7\r\nLATENCY\r\n$9\r\nHISTOGRAM\r\n$10\r\nNET_RS_LAT\r\n"
      )
      .is_some()
    );
    let out_filter = drain_output(&mut session);
    let filter_str = String::from_utf8_lossy(&out_filter);
    assert!(filter_str.contains("NET_RS_LAT"));

    // 5. 非法事件报错
    assert!(
      feed(
        &mut session,
        b"*3\r\n$7\r\nLATENCY\r\n$9\r\nHISTOGRAM\r\n$7\r\nINVALID\r\n"
      )
      .is_some()
    );
    let out_err = drain_output(&mut session);
    assert!(
      out_err.starts_with(b"-ERR Invalid event INVALID. Try LATENCY HELP\r\n"),
      "错误回显: {out_err:?}"
    );

    // 6. 执行 LATENCY RESET
    assert!(
      feed(
        &mut session,
        b"*3\r\n$7\r\nLATENCY\r\n$5\r\nRESET\r\n$10\r\nNET_RS_LAT\r\n"
      )
      .is_some()
    );
    let out_reset = drain_output(&mut session);
    assert_eq!(out_reset, b":1\r\n");

    // 7. 执行 LATENCY HELP
    assert!(feed(&mut session, b"*2\r\n$7\r\nLATENCY\r\n$4\r\nHELP\r\n").is_some());
    let out_help = drain_output(&mut session);
    assert!(out_help.starts_with(b"*9\r\n"));
  });
  Ok(())
}

/// INFO RESETSTAT 的 gossip 与复活化两臂接线判定：STATS 标志轮各下达一次，
/// 未置标志的轮次与标志清位后的轮次均不受采样影响
/// 对应 GarnetServerMonitor::cleanup_global_stats 的
/// STATS 分支体内 `storeWrapper.clusterProvider?.ResetGossipStats()` 与
/// `storeWrapper.ResetRevivificationStats()` 两条复位臂。本用例观测装配口
///（[`ConsumerRegistry::monitor_iteration_inputs`]）注入的两臂回调触达次数与
/// 触达时机；两臂的真实终点计数分别见 wedb/tests/gossip_manager.rs 的
/// `test_resetstat_arms_zero_gossip_stats` 与 wnode/tests/database_manager.rs
/// 的 `reset_revivification_stats_zeroes_pool_counters`
#[test]
fn resetstat_stats_branch_fires_gossip_and_reviv_arms() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let monitor = Arc::new(GarnetServerMonitor::new(1, true, false, false));
    let registry = Arc::new(ConsumerRegistry::new());
    registry.register(1, "127.0.0.1:50000".into(), "127.0.0.1:6379".into());
    let gossip_resets = Arc::new(AtomicUsize::new(0));
    let reviv_resets = Arc::new(AtomicUsize::new(0));

    // 无复位标志的常规采样轮：两臂静默（C# 两臂同处 STATS 门内）
    run_iterations_with_arm_counters(&monitor, &registry, 2, &gossip_resets, &reviv_resets).await;
    assert_eq!(
      (
        gossip_resets.load(Ordering::Relaxed),
        reviv_resets.load(Ordering::Relaxed)
      ),
      (0, 0),
      "未 RESETSTAT 时两臂不得随采样轮下发"
    );

    // INFO RESETSTAT：置 STATS 标志的一轮同时触达两臂（各一次）
    monitor.set_info_reset_flag(InfoMetricsType::Stats);
    run_iterations_with_arm_counters(&monitor, &registry, 1, &gossip_resets, &reviv_resets).await;
    assert_eq!(
      (
        gossip_resets.load(Ordering::Relaxed),
        reviv_resets.load(Ordering::Relaxed)
      ),
      (1, 1),
      "RESETSTAT 轮应各下达一次两臂复位"
    );

    // 标志清位后的下一轮：一次性消费，不重复下发
    run_iterations_with_arm_counters(&monitor, &registry, 2, &gossip_resets, &reviv_resets).await;
    assert_eq!(
      (
        gossip_resets.load(Ordering::Relaxed),
        reviv_resets.load(Ordering::Relaxed)
      ),
      (1, 1),
      "复位标志清位后两臂不应再被触达"
    );
  });
  Ok(())
}
