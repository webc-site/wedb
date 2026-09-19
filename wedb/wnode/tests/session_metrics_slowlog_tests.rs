//! 会话级指标统计与慢日志记录端到端单测
//!
//! 验证：
//! 1. 配置开启慢日志阈值后，慢执行命令记录到 SLOWLOG 中；未达阈值命令不记录。
//! 2. 写命令（如 SET）和读命令（如 GET）递增 session_metrics 的
//!    total_write_commands_processed 和 total_read_commands_processed。
//! 3. INFO statistics / INFO stats 能够正确反映递增后的指标值。
//!
//! 会话指标句柄一律走真实轨：`StorageSessionProvider` 按
//! `metrics_sampling_frequency_secs > 0` 在装配期创建唯一句柄，经
//! `resp_session_consumer.rs:attach_session_metrics` 注入会话与存储执行域
//! （选项侧不再有布尔开关）。

use std::{future::ready, path::Path, sync::Arc, thread::sleep, time::Duration};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wconf::RuntimeServerConfig;
use wdev::SegmentedDevice;
use wmetric::GarnetSessionMetrics;
use wnode::{
  MessageConsumerFace, SessionProviderFace, WireFormat,
  resp::{
    garnet_api::{GarnetApiFace, StoreGarnetApi, TxnProcRun},
    metrics_commands::new_slow_log_container,
    resp_server_session::{RespServerSession, RespServerSessionOptions},
    resp_session_consumer::RespSessionConsumer,
    slow_path::SlowFuture,
  },
  service::StorageSessionProvider,
};
use wresp::command::RespCommand;
use wtest_base::test_store_config;

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

  // 慢日志注入面 stub：本测试只验证 exec 延时与应答，事务过程驱动不在断言面
  fn run_txn_proc(&self, _run: TxnProcRun<'_>) -> bool {
    false
  }

  fn exec_slow(
    self: Arc<Self>,
    _cmd: RespCommand,
    _args: Vec<Vec<u8>>,
    _resp_version: u8,
  ) -> SlowFuture {
    SlowFuture::new(ready(Vec::new()))
  }
}

/// 真实轨装配基座：按 `sampling_secs` 门控会话指标采样的 provider（该字段是
/// 本连接是否采样指标的唯一真值源：句柄唯一创建点在 `service.rs`，逐连接创建
/// 后经 `attach_session_metrics` 注入会话与存储执行域）
fn sampling_provider<F>(
  data_path: &Path,
  sampling_secs: u64,
  decorate: F,
) -> aok::Result<StorageSessionProvider<F>>
where
  F: Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer>,
{
  Ok(
    StorageSessionProvider::open_with_config(test_store_config(), data_path, decorate)?
      .with_metrics_sampling_frequency_secs(sampling_secs),
  )
}

/// 网络泵等价消费（直填接收缓冲 → 唯一入口 → 应答随冲写取出）
/// 返回 (消费后残余, 应答字节)：残余 Some(0) = 完整消费
fn pump(c: &mut RespSessionConsumer, bytes: &[u8]) -> (Option<usize>, Vec<u8>) {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(bytes);
  c.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = c.try_consume_messages_into(&mut resp);
  (remaining, resp)
}

/// 本连接会话指标快照（句柄由装配注入，测试侧只读）
fn snapshot(c: &RespSessionConsumer) -> GarnetSessionMetrics {
  c.session()
    .session_metrics
    .as_ref()
    .expect("真实轨装配期注入会话指标句柄")
    .snapshot()
}

#[test]
fn test_slowlog_recording_in_session() -> aok::Result<()> {
  let dir = tempdir()?;
  let rt = Runtime::new()?;
  rt.block_on(async {
    let provider = sampling_provider(&dir.path().join("slowlog.db"), 1, |sender_id, _api| {
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions::default(),
        Arc::new(SlowMockApi),
      ))
    })?;
    let mut consumer = provider
      .get_session(WireFormat::Ascii, 1)
      .expect("真实轨会话创建");
    let config = Arc::new(RuntimeServerConfig::with_defaults());
    consumer.set_runtime_config(config.clone());
    let container = new_slow_log_container(10);
    consumer.set_slow_log_container(container);

    // 1. 未开启阈值（默认 0 = 禁用）：慢命令不记录
    let frame_set = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
    assert!(pump(&mut consumer, frame_set).0.is_some());

    // 检查 SLOWLOG LEN = 0
    let frame_len = b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nLEN\r\n";
    let (_, out) = pump(&mut consumer, frame_len);
    assert_eq!(out, b":0\r\n");

    // 2. 配置开启慢日志阈值（30000 微秒 = 30ms），SET 耗时 50ms 会被记录
    let frame_config =
      b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$23\r\nslowlog-log-slower-than\r\n$5\r\n30000\r\n";
    assert!(pump(&mut consumer, frame_config).0.is_some());

    // 清空可能因并行高负载下执行 CONFIG SET 自身被记录的条目
    let frame_reset = b"*2\r\n$7\r\nSLOWLOG\r\n$5\r\nRESET\r\n";
    assert!(pump(&mut consumer, frame_reset).0.is_some());

    assert!(pump(&mut consumer, frame_set).0.is_some());

    // 检查 SLOWLOG LEN = 1
    let (_, out) = pump(&mut consumer, frame_len);
    assert_eq!(out, b":1\r\n");

    // 检查 SLOWLOG GET 输出
    let frame_get_slowlog = b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nGET\r\n";
    let (_, out) = pump(&mut consumer, frame_get_slowlog);
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
    assert!(pump(&mut consumer, frame_get).0.is_some());

    // SLOWLOG LEN 依然为 1
    let (_, out) = pump(&mut consumer, frame_len);
    assert_eq!(out, b":1\r\n");

    // 4. SLOWLOG RESET
    let (_, out) = pump(&mut consumer, frame_reset);
    assert_eq!(out, b"+OK\r\n");

    let (_, out) = pump(&mut consumer, frame_len);
    assert_eq!(out, b":0\r\n");
    aok::OK
  })
}

#[test]
fn test_session_metrics_read_write_counts_and_info_statistics() -> aok::Result<()> {
  let dir = tempdir()?;
  let rt = Runtime::new()?;
  rt.block_on(async {
    let provider = sampling_provider(&dir.path().join("metrics.db"), 1, |sender_id, _api| {
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions::default(),
        Arc::new(SlowMockApi),
      ))
    })?;
    // 采样开关只此一处：provider 的 metrics_sampling_frequency_secs
    let mut consumer = provider
      .get_session(WireFormat::Ascii, 2)
      .expect("真实轨会话创建");
    // 累计冲出字节数（出向记账单点 = take_output_into）
    let mut flushed: u64 = 0;

    // 初始计数为 0
    {
      let m = snapshot(&consumer);
      assert_eq!(m.get_total_commands_processed(), 0);
      assert_eq!(m.get_total_write_commands_processed(), 0);
      assert_eq!(m.get_total_read_commands_processed(), 0);
      assert_eq!(m.total_net_output_bytes, 0);
    }

    // 执行写命令 SET
    let frame_set = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
    let (consumed, out) = pump(&mut consumer, frame_set);
    assert_eq!(consumed, Some(0));
    assert_eq!(out, b"+OK\r\n");
    flushed += out.len() as u64;

    {
      let m = snapshot(&consumer);
      assert_eq!(m.get_total_commands_processed(), 1);
      assert_eq!(m.get_total_write_commands_processed(), 1);
      assert_eq!(m.get_total_read_commands_processed(), 0);
      assert_eq!(m.total_net_output_bytes, flushed);
    }

    // 执行读命令 GET
    let frame_get = b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
    let (consumed, out) = pump(&mut consumer, frame_get);
    assert_eq!(consumed, Some(0));
    assert_eq!(out, b"$3\r\nbar\r\n");
    flushed += out.len() as u64;

    {
      let m = snapshot(&consumer);
      assert_eq!(m.get_total_commands_processed(), 2);
      assert_eq!(m.get_total_write_commands_processed(), 1);
      assert_eq!(m.get_total_read_commands_processed(), 1);
      assert_eq!(m.total_net_output_bytes, flushed);
    }

    // 验证 INFO STATISTICS
    let frame_info_stats = b"*2\r\n$4\r\nINFO\r\n$10\r\nSTATISTICS\r\n";
    let (_, out) = pump(&mut consumer, frame_info_stats);
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
    let (_, out) = pump(&mut consumer, frame_info_stats_short);
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
    assert!(pump(&mut consumer, frame_set).0.is_some());
    assert!(pump(&mut consumer, frame_set).0.is_some());
    assert!(pump(&mut consumer, frame_get).0.is_some());

    {
      let m = snapshot(&consumer);
      assert_eq!(m.get_total_write_commands_processed(), 3);
      assert_eq!(m.get_total_read_commands_processed(), 2);
    }

    let (_, out) = pump(&mut consumer, frame_info_stats);
    let text = String::from_utf8_lossy(&out);
    assert!(
      text.contains("total_write_commands_processed:3\r\n"),
      "out: {text}"
    );
    assert!(
      text.contains("total_read_commands_processed:2\r\n"),
      "out: {text}"
    );

    // 执行未知/无效命令（RespCommand::Invalid）：
    // 对标 C# :726-735，total_commands_processed + 1，但读写指标不增加
    let frame_invalid = b"*1\r\n$7\r\nUNKNOWN\r\n";
    let before_total = snapshot(&consumer).get_total_commands_processed();
    let (_, out) = pump(&mut consumer, frame_invalid);
    assert!(out.starts_with(b"-ERR unknown command"));
    {
      let m = snapshot(&consumer);
      assert_eq!(m.get_total_commands_processed(), before_total + 1);
      assert_eq!(m.get_total_write_commands_processed(), 3);
      assert_eq!(m.get_total_read_commands_processed(), 2);
    }
    aok::OK
  })
}

#[test]
fn test_session_metrics_and_slowlog_with_real_store() -> aok::Result<()> {
  let dir = tempdir()?;
  let rt = Runtime::new()?;
  rt.block_on(async {
    // 真实存储执行域由 provider 逐连接派生（对标 C# StoreWrapper 会话装配）
    let provider = sampling_provider(&dir.path().join("test.db"), 1, |sender_id, api| {
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions::default(),
        Arc::new(api),
      ))
    })?;
    let mut consumer = provider
      .get_session(WireFormat::Ascii, 3)
      .expect("真实轨会话创建");
    let config = Arc::new(RuntimeServerConfig::with_defaults());
    consumer.set_runtime_config(config.clone());
    let container = new_slow_log_container(10);
    consumer.set_slow_log_container(container);

    // 1. 设置极低阈值 1 微秒：真实存储操作必定超过 1 微秒，触发慢日志
    let frame_config =
      b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$23\r\nslowlog-log-slower-than\r\n$1\r\n1\r\n";
    assert!(pump(&mut consumer, frame_config).0.is_some());

    let frame_set = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    let (consumed, out) = pump(&mut consumer, frame_set);
    assert_eq!(consumed, Some(0));
    assert_eq!(out, b"+OK\r\n");

    let frame_get = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    let (consumed, out) = pump(&mut consumer, frame_get);
    assert_eq!(consumed, Some(0));
    assert_eq!(out, b"$3\r\nbar\r\n");

    // 验证 session_metrics 读写计数
    {
      let m = snapshot(&consumer);
      assert_eq!(m.get_total_write_commands_processed(), 1);
      assert_eq!(m.get_total_read_commands_processed(), 1);
    }

    // 验证 INFO statistics
    let frame_info = b"*2\r\n$4\r\nINFO\r\n$10\r\nSTATISTICS\r\n";
    let (_, out) = pump(&mut consumer, frame_info);
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
    let (_, out) = pump(&mut consumer, frame_slowlog_len);
    let text = String::from_utf8_lossy(&out);
    assert!(text.starts_with(':'), "out: {text}");
    let count: i64 = text[1..text.len() - 2].parse().unwrap();
    assert!(count >= 1, "count: {count}");

    let frame_slowlog_get = b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nGET\r\n";
    let (_, out) = pump(&mut consumer, frame_slowlog_get);
    let text = String::from_utf8_lossy(&out);
    assert!(text.contains("foo"), "out: {text}");
    aok::OK
  })
}

/// 采样门控两臂（对标 C# StoreWrapper.trackStats = MetricsSamplingFrequency
/// > 0 方置 sessionMetrics）：secs > 0 装配注入句柄，secs == 0 会话保持
/// > 构造期 None（与 C# null 会话指标同形）
#[test]
fn sampling_gate_controls_session_metrics() -> aok::Result<()> {
  let dir = tempdir()?;
  let rt = Runtime::new()?;
  rt.block_on(async {
    let on = sampling_provider(&dir.path().join("gate_on.db"), 1, |sender_id, api| {
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions::default(),
        Arc::new(api),
      ))
    })?;
    let consumer_on = on
      .get_session(WireFormat::Ascii, 11)
      .expect("真实轨会话创建");
    assert!(
      consumer_on.session().session_metrics.is_some(),
      "secs > 0 须注入会话指标句柄"
    );

    let off = sampling_provider(&dir.path().join("gate_off.db"), 0, |sender_id, api| {
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions::default(),
        Arc::new(api),
      ))
    })?;
    let consumer_off = off
      .get_session(WireFormat::Ascii, 12)
      .expect("真实轨会话创建");
    assert!(
      consumer_off.session().session_metrics.is_none(),
      "secs == 0 会话指标保持 None"
    );
    aok::OK
  })
}
