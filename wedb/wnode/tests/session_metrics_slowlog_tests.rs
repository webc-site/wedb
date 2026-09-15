//! 会话级指标统计与慢日志记录端到端单测
//!
//! 验证：
//! 1. 配置开启慢日志阈值后，慢执行命令记录到 SLOWLOG 中；未达阈值命令不记录。
//! 2. 写命令（如 SET）和读命令（如 GET）递增 session_metrics 的
//!    total_write_commands_processed 和 total_read_commands_processed。
//! 3. INFO statistics / INFO stats 能够正确反映递增后的指标值。

use std::{
  future::{Future, ready},
  sync::Arc,
  thread::sleep,
  time::Duration,
};

use tempfile::tempdir;
use wconf::RuntimeServerConfig;
use wdev::SegmentedDevice;
use wedb_test::test_store_config;
use wkv::WedbStore;
use wnode::resp::{
  garnet_api::{GarnetApi, GarnetApiFace, StoreGarnetApi},
  metrics_commands::new_slow_log_container,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wresp::RespCommand;

/// 模拟命令执行器：SET 注入延时，用于精确触发慢日志阈值判定
struct SlowMockApi;

impl GarnetApiFace for SlowMockApi {
  fn exec(&self, session: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) {
    match cmd {
      RespCommand::Set => {
        sleep(Duration::from_millis(50));
        session.output.extend_from_slice(b"+OK\r\n");
      }
      RespCommand::Get => {
        session.output.extend_from_slice(b"$3\r\nbar\r\n");
      }
      RespCommand::ConfigSet => {
        let mut out = Vec::new();
        // mock 会话无存储域（对标 C# null owner）：CONFIG SET 仅落槽位
        let _ = session.network_config_set::<SegmentedDevice>(args, None, &mut out);
        session.output.extend_from_slice(&out);
      }
      _ => {}
    }
  }

  fn exec_slow<'a>(
    &'a self,
    _cmd: RespCommand,
    _args: Vec<Vec<u8>>,
  ) -> impl Future<Output = Vec<u8>> + 'a {
    ready(Vec::new())
  }
}

#[test]
fn test_slowlog_recording_in_session() {
  let mut session = RespServerSession::new(
    1,
    RespServerSessionOptions {
      metrics_sampling_frequency: true,
      ..RespServerSessionOptions::default()
    },
  );
  let config = Arc::new(RuntimeServerConfig::with_defaults());
  session.set_runtime_config(config.clone());
  let container = new_slow_log_container(10);
  session.set_slow_log_container(container);
  session.set_garnet_api(GarnetApi::new(SlowMockApi));

  // 1. 未开启阈值（默认 0 = 禁用）：慢命令不记录
  let frame_set = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
  assert!(session.try_consume_messages(frame_set).is_some());
  let _ = session.take_output();

  // 检查 SLOWLOG LEN = 0
  let frame_len = b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nLEN\r\n";
  assert!(session.try_consume_messages(frame_len).is_some());
  let out = session.take_output();
  assert_eq!(out, b":0\r\n");

  // 2. 配置开启慢日志阈值（30000 微秒 = 30ms），SET 耗时 50ms 会被记录
  let frame_config =
    b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$23\r\nslowlog-log-slower-than\r\n$5\r\n30000\r\n";
  assert!(session.try_consume_messages(frame_config).is_some());
  let _ = session.take_output();

  // 清空可能因并行高负载下执行 CONFIG SET 自身被记录的条目
  let frame_reset = b"*2\r\n$7\r\nSLOWLOG\r\n$5\r\nRESET\r\n";
  assert!(session.try_consume_messages(frame_reset).is_some());
  let _ = session.take_output();

  assert!(session.try_consume_messages(frame_set).is_some());
  let _ = session.take_output();

  // 检查 SLOWLOG LEN = 1
  assert!(session.try_consume_messages(frame_len).is_some());
  let out = session.take_output();
  assert_eq!(out, b":1\r\n");

  // 检查 SLOWLOG GET 输出
  let frame_get_slowlog = b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nGET\r\n";
  assert!(session.try_consume_messages(frame_get_slowlog).is_some());
  let out = session.take_output();
  let text = String::from_utf8_lossy(&out);
  assert!(text.starts_with("*1\r\n*6\r\n"), "out: {text}");
  assert!(
    text.contains("$3\r\nSet\r\n") || text.contains("$3\r\nSET\r\n"),
    "out: {text}"
  );
  assert!(text.contains("$1\r\nk\r\n"), "out: {text}");
  assert!(text.contains("$1\r\nv\r\n"), "out: {text}");

  // 3. 执行快速命令 GET（< 1ms），不应触发慢日志
  let frame_get = b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
  assert!(session.try_consume_messages(frame_get).is_some());
  let _ = session.take_output();

  // SLOWLOG LEN 依然为 1
  assert!(session.try_consume_messages(frame_len).is_some());
  let out = session.take_output();
  assert_eq!(out, b":1\r\n");

  // 4. SLOWLOG RESET
  let frame_reset = b"*2\r\n$7\r\nSLOWLOG\r\n$5\r\nRESET\r\n";
  assert!(session.try_consume_messages(frame_reset).is_some());
  let out = session.take_output();
  assert_eq!(out, b"+OK\r\n");

  assert!(session.try_consume_messages(frame_len).is_some());
  let out = session.take_output();
  assert_eq!(out, b":0\r\n");
}

#[test]
fn test_session_metrics_read_write_counts_and_info_statistics() {
  let mut session = RespServerSession::new(
    2,
    RespServerSessionOptions {
      metrics_sampling_frequency: true,
      ..RespServerSessionOptions::default()
    },
  );
  session.set_garnet_api(GarnetApi::new(SlowMockApi));

  // 初始计数为 0
  {
    let m = session.session_metrics.as_ref().unwrap();
    assert_eq!(m.get_total_commands_processed(), 0);
    assert_eq!(m.get_total_write_commands_processed(), 0);
    assert_eq!(m.get_total_read_commands_processed(), 0);
  }

  // 执行写命令 SET
  let frame_set = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
  assert_eq!(
    session.try_consume_messages(frame_set),
    Some(frame_set.len())
  );
  let out = session.take_output();
  assert_eq!(out, b"+OK\r\n");

  {
    let m = session.session_metrics.as_ref().unwrap();
    assert_eq!(m.get_total_commands_processed(), 1);
    assert_eq!(m.get_total_write_commands_processed(), 1);
    assert_eq!(m.get_total_read_commands_processed(), 0);
  }

  // 执行读命令 GET
  let frame_get = b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
  assert_eq!(
    session.try_consume_messages(frame_get),
    Some(frame_get.len())
  );
  let out = session.take_output();
  assert_eq!(out, b"$3\r\nbar\r\n");

  {
    let m = session.session_metrics.as_ref().unwrap();
    assert_eq!(m.get_total_commands_processed(), 2);
    assert_eq!(m.get_total_write_commands_processed(), 1);
    assert_eq!(m.get_total_read_commands_processed(), 1);
  }

  // 验证 INFO STATISTICS
  let frame_info_stats = b"*2\r\n$4\r\nINFO\r\n$10\r\nSTATISTICS\r\n";
  assert!(session.try_consume_messages(frame_info_stats).is_some());
  let out = session.take_output();
  let text = String::from_utf8_lossy(&out);
  assert!(
    text.contains("total_write_commands_processed:1\r\n"),
    "out: {text}"
  );
  assert!(
    text.contains("total_read_commands_processed:1\r\n"),
    "out: {text}"
  );

  // 验证 INFO STATS
  let frame_info_stats_short = b"*2\r\n$4\r\nINFO\r\n$5\r\nSTATS\r\n";
  assert!(
    session
      .try_consume_messages(frame_info_stats_short)
      .is_some()
  );
  let out = session.take_output();
  let text = String::from_utf8_lossy(&out);
  assert!(
    text.contains("total_write_commands_processed:1\r\n"),
    "out: {text}"
  );
  assert!(
    text.contains("total_read_commands_processed:1\r\n"),
    "out: {text}"
  );

  // 再执行 2 次写和 1 次读
  assert!(session.try_consume_messages(frame_set).is_some());
  let _ = session.take_output();
  assert!(session.try_consume_messages(frame_set).is_some());
  let _ = session.take_output();
  assert!(session.try_consume_messages(frame_get).is_some());
  let _ = session.take_output();

  {
    let m = session.session_metrics.as_ref().unwrap();
    assert_eq!(m.get_total_write_commands_processed(), 3);
    assert_eq!(m.get_total_read_commands_processed(), 2);
  }

  assert!(session.try_consume_messages(frame_info_stats).is_some());
  let out = session.take_output();
  let text = String::from_utf8_lossy(&out);
  assert!(
    text.contains("total_write_commands_processed:3\r\n"),
    "out: {text}"
  );
  assert!(
    text.contains("total_read_commands_processed:2\r\n"),
    "out: {text}"
  );
}

#[test]
fn test_session_metrics_and_slowlog_with_real_store() {
  let dir = tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("test.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线），GC 关闭保持历史语义
  let mut config = test_store_config();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let store_session = store.new_session().unwrap();

  let mut session = RespServerSession::new(
    3,
    RespServerSessionOptions {
      metrics_sampling_frequency: true,
      ..RespServerSessionOptions::default()
    },
  );
  let config = Arc::new(RuntimeServerConfig::with_defaults());
  session.set_runtime_config(config.clone());
  let container = new_slow_log_container(10);
  session.set_slow_log_container(container);
  session.set_garnet_api(StoreGarnetApi::new(store_session));

  // 1. 设置极低阈值 1 微秒：真实存储操作必定超过 1 微秒，触发慢日志
  let frame_config =
    b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$23\r\nslowlog-log-slower-than\r\n$1\r\n1\r\n";
  assert!(session.try_consume_messages(frame_config).is_some());
  let _ = session.take_output();

  let frame_set = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
  assert_eq!(
    session.try_consume_messages(frame_set),
    Some(frame_set.len())
  );
  let out = session.take_output();
  assert_eq!(out, b"+OK\r\n");

  let frame_get = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
  assert_eq!(
    session.try_consume_messages(frame_get),
    Some(frame_get.len())
  );
  let out = session.take_output();
  assert_eq!(out, b"$3\r\nbar\r\n");

  // 验证 session_metrics 读写计数
  {
    let m = session.session_metrics.as_ref().unwrap();
    assert_eq!(m.get_total_write_commands_processed(), 1);
    assert_eq!(m.get_total_read_commands_processed(), 1);
  }

  // 验证 INFO statistics
  let frame_info = b"*2\r\n$4\r\nINFO\r\n$10\r\nSTATISTICS\r\n";
  assert!(session.try_consume_messages(frame_info).is_some());
  let out = session.take_output();
  let text = String::from_utf8_lossy(&out);
  assert!(
    text.contains("total_write_commands_processed:1\r\n"),
    "out: {text}"
  );
  assert!(
    text.contains("total_read_commands_processed:1\r\n"),
    "out: {text}"
  );

  // 验证慢日志已记录至少 1 条
  let frame_slowlog_len = b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nLEN\r\n";
  assert!(session.try_consume_messages(frame_slowlog_len).is_some());
  let out = session.take_output();
  let text = String::from_utf8_lossy(&out);
  assert!(text.starts_with(':'), "out: {text}");
  let count: i64 = text[1..text.len() - 2].parse().unwrap();
  assert!(count >= 1, "count: {count}");

  let frame_slowlog_get = b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nGET\r\n";
  assert!(session.try_consume_messages(frame_slowlog_get).is_some());
  let out = session.take_output();
  let text = String::from_utf8_lossy(&out);
  assert!(text.contains("foo"), "out: {text}");
}
