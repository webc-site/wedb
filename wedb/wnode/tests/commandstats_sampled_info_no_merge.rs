//! 周期采样（频率 > 0）形态 INFO COMMANDSTATS global 单源不补并对标测试
//!（对标 libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateCommandStatsInfo
//! :233-238——频率 > 0 臂直取 globalCommandStats（采样循环已含 history +
//! 活跃会话上轮值），不遍历活跃会话补并；判别键同为 MetricsSamplingFrequency，
//! 与 commandstats_freq0_info_rows.rs 的 freq0 补并臂互补钉死路由两臂）
//!
//! 自研回归锁: freq>0 既有行为回归——global 单源、读数随采样轮推进

use std::{
  future::ready,
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
  },
};

use wmetric::GarnetServerMonitor;
use wnode::{
  resp::resp_server_session::{RespServerSession, RespServerSessionOptions},
  servers::consumer_registry::ConsumerRegistry,
};
use wnode_test::drain_output;

/// 模拟泵直填一批字节（同步面命令无停车臂，server_monitor_tests.rs feed 同形态）
fn feed(s: &mut RespServerSession, bytes: &[u8]) {
  s.recv_buffer.extend_from_slice(bytes);
  assert!(s.try_consume_messages().is_some(), "帧应被完整消费");
}

/// 驱动 N 轮采样（复位回调与宿主装配同构，server_monitor_tests.rs 同款）
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
        registry.monitor_iteration_inputs(|| {}, || {})
      },
    )
    .await;
}

/// 读出 INFO COMMANDSTATS 中指定命令行的 calls 计数
fn info_ping_calls(info: &[u8]) -> u64 {
  let text = from_utf8(info).unwrap();
  assert!(
    text.contains("# Commandstats"),
    "应出 COMMANDSTATS 段头: {text}"
  );
  let line = text
    .split("\r\n")
    .find(|l| l.starts_with("cmdstat_ping:"))
    .unwrap_or_else(|| panic!("缺少 cmdstat_ping 条目: {text}"));
  let start = line.find("calls=").expect("cmdstat 行含 calls 字段") + "calls=".len();
  let end = line[start..].find(',').map_or(line.len(), |i| start + i);
  line[start..end].parse().expect("calls 为无符号整数")
}

/// freq>0 端到端：读数取 global 单源、活跃会话不补并、随采样轮推进
#[compio::test]
async fn commandstats_sampled_global_single_source() {
  // 进程级监视器槽首装即赢（本文件单用例，安装必成功）
  let monitor = Arc::new(GarnetServerMonitor::new(1, true, false, true));
  assert!(monitor.install_global(), "测试进程监视器槽应首次安装成功");

  let registry = Arc::new(ConsumerRegistry::new());
  assert!(registry.install_global(), "测试进程注册表槽应首次安装成功");

  let mut session = RespServerSession::new(
    1,
    RespServerSessionOptions {
      command_stats_monitor: true,
      ..RespServerSessionOptions::default()
    },
  );
  let entry = registry.register(1, "127.0.0.1:50000".into(), "127.0.0.1:6379".into());
  entry.attach_command_stats(session.command_stats.clone());

  // 采样前执行 2 次 PING，驱动一轮采样并入 global
  for _ in 0..2 {
    feed(&mut session, b"*1\r\n$4\r\nPING\r\n");
    drain_output(&mut session);
  }
  run_iterations(&monitor, &registry, 1).await;
  feed(&mut session, b"*2\r\n$4\r\nINFO\r\n$12\r\ncommandstats\r\n");
  assert_eq!(info_ping_calls(&drain_output(&mut session)), 2);

  // 采样后再执行 1 次 PING（未采样）：补并臂关闭，读数保持上轮 global
  feed(&mut session, b"*1\r\n$4\r\nPING\r\n");
  drain_output(&mut session);
  feed(&mut session, b"*2\r\n$4\r\nINFO\r\n$12\r\ncommandstats\r\n");
  assert_eq!(
    info_ping_calls(&drain_output(&mut session)),
    2,
    "freq>0 形态活跃会话增量不得直读补并（C# :233-238 global 单源）"
  );

  // 下一采样轮推进：读数随 global 重建升至 3（既有采样链路回归不破）
  run_iterations(&monitor, &registry, 1).await;
  feed(&mut session, b"*2\r\n$4\r\nINFO\r\n$12\r\ncommandstats\r\n");
  assert_eq!(info_ping_calls(&drain_output(&mut session)), 3);
}
