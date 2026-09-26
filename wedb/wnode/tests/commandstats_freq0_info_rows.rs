//! commandstats 单开（采样频率缺省 0）形态 INFO COMMANDSTATS 端到端对标测试
//!（对标 libs/server/Metrics/Info/GarnetInfoMetrics.cs:PopulateCommandStatsInfo
//! :244-256 无周期采样按需聚合臂：history + 遍历活跃消费者逐会话补并；
//! 启动校验仅拒 Latency+freq0——GarnetServerOptions.cs:839-840，本形态合法）
//!
//! 自研回归锁: freq0 形态聚合路由按频率真源取 history 并补并活跃会话，
//! 缺陷形态（tracks_command_stats 冒充采样频率、恒零 global 直出）下
//! COMMANDSTATS 段整段缺席即红

use std::{str::from_utf8, sync::Arc};

use wmetric::{CommandStats, GarnetServerMonitor};
use wnode::{
  resp::resp_server_session::{RespServerSession, RespServerSessionOptions},
  servers::consumer_registry::ConsumerRegistry,
};
use wnode_test::drain_output;
use wresp::command::RespCommand;

/// 模拟泵直填一批字节（同步面命令无停车臂，server_monitor_tests.rs feed 同形态）
fn feed(s: &mut RespServerSession, bytes: &[u8]) {
  s.recv_buffer.extend_from_slice(bytes);
  assert!(s.try_consume_messages().is_some(), "帧应被完整消费");
}

/// cmdstat 行断言（INFO COMMANDSTATS 段内的 Redis 约定格式，
/// resp_commandstats_session.rs 同款）
fn assert_cmdstat(info: &[u8], cmd: &str, calls: u64, rejected: u64, failed: u64) {
  let text = from_utf8(info).unwrap();
  let line = text
    .split("\r\n")
    .find(|l| l.starts_with(&format!("cmdstat_{cmd}:")))
    .unwrap_or_else(|| panic!("缺少 cmdstat_{cmd} 条目: {text}"));
  assert_eq!(
    line,
    format!(
      "cmdstat_{cmd}:calls={calls},usec=0,usec_per_call=0.00,rejected_calls={rejected},failed_calls={failed}"
    ),
  );
}

/// freq0 形态端到端：监视器在位（commandstats 开、采样频率 0）+ 活跃会话
/// 在架 + history 有归并累计，INFO COMMANDSTATS 逐名出实数行
#[test]
fn commandstats_freq0_info_rows_end_to_end() {
  // 进程级监视器槽首装即赢（本文件单用例，安装必成功）
  let monitor = Arc::new(GarnetServerMonitor::new(0, true, false, true));
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
  // 会话命令统计句柄挂接注册表条目（生产装配同口，
  // resp_session_consumer.rs:attach 路径），补并臂经活跃消费者枚举可见
  let entry = registry.register(1, "127.0.0.1:50000".into(), "127.0.0.1:6379".into());
  entry.attach_command_stats(session.command_stats.clone());

  // dispose 归并累计入 history（C# historyCommandStats 经
  // AddMetricsHistorySessionDispose 的就位形态，freq0 聚合真源）
  let mut disposed = CommandStats::new();
  for _ in 0..4 {
    disposed.increment_calls(RespCommand::Ping);
  }
  monitor.add_metrics_history_session_dispose(None, Some(&disposed));

  // 活跃会话在架执行：2 次 PING 放行 + 1 次参数错（calls 计、failed 计）
  for _ in 0..2 {
    feed(&mut session, b"*1\r\n$4\r\nPING\r\n");
    assert_eq!(drain_output(&mut session), b"+PONG\r\n");
  }
  feed(&mut session, b"*3\r\n$4\r\nPING\r\n$1\r\nx\r\n$1\r\ny\r\n");
  drain_output(&mut session);

  // INFO COMMANDSTATS：段头在场 + history 4 与活跃会话 3 合并如实出行
  //（缺陷形态聚合恒零被逐命令过滤，整段缺席即红）
  feed(&mut session, b"*2\r\n$4\r\nINFO\r\n$12\r\ncommandstats\r\n");
  let info = drain_output(&mut session);
  let text = from_utf8(&info).unwrap();
  assert!(
    text.contains("# Commandstats"),
    "应出 COMMANDSTATS 段头: {text}"
  );
  assert!(
    !text.contains("monitoring is disabled"),
    "开关在位不得出禁用提示: {text}"
  );
  assert_cmdstat(&info, "ping", 7, 0, 1);
}
