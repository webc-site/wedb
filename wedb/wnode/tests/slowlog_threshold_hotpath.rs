//! 慢日志批次阈值缓存与批起始戳对齐端到端单测
//!（对标 libs/server/Resp/RespServerSession.cs:TryConsumeMessages:484-489 /
//! ProcessMessages:728 与 libs/server/Metrics/Slowlog/RespSlowlogCommands.cs:
//! HandleSlowLog —— 阈值仅批入口单次读运行时配置折算 tick 缓存，命令循环据此
//! 门控；CONFIG SET slowlog-log-slower-than 自下一批次生效，起始 tick 与阈值
//! 严格同步对齐，杜绝「0 初值当纪元量级比较」的虚假巨额耗时入库）
//!
//! 验证一：动态开启（同批 CONFIG SET + 后续快速命令）无虚假慢查询，禁用批
//! 命令循环零时钟获取（起始 tick 保持 0 哨兵）；
//! 验证二：真实慢命令按缓存阈值准确入库，耗时为微秒量级（公式：sleep 下界）；
//! 验证三：缓存阈值 0 短路——热路径不再读取实时配置，与实时配置热更脱钩。

use std::{future::ready, path::Path, sync::Arc, thread::sleep, time::Duration};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbase::{convert::stopwatch::TICKS_PER_MICROSECOND, time::now_stopwatch_ticks};
use wconf::{RuntimeServerConfig, ServerConfigType};
use wdev::SegmentedDevice;
use wnode::{
  MessageConsumerFace, SessionProviderFace, WireFormat,
  resp::{
    garnet_api::GarnetApiFace,
    metrics_commands::new_slow_log_container,
    resp_server_session::{RespServerSession, RespServerSessionOptions},
    resp_session_consumer::RespSessionConsumer,
    slow_path::SlowFuture,
  },
  service::StorageSessionProvider,
};
use wresp::command::RespCommand;
use wtest_base::test_store_config;

const FRAME_SET: &[u8] = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
const FRAME_GET: &[u8] = b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
const FRAME_SLOWLOG_GET: &[u8] = b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nGET\r\n";
const FRAME_SLOWLOG_LEN: &[u8] = b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nLEN\r\n";
/// CONFIG SET slowlog-log-slower-than 10000（10ms）
const FRAME_CONFIG_10MS: &[u8] =
  b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$23\r\nslowlog-log-slower-than\r\n$5\r\n10000\r\n";
/// CONFIG SET slowlog-log-slower-than 30000（30ms）
const FRAME_CONFIG_30MS: &[u8] =
  b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$23\r\nslowlog-log-slower-than\r\n$5\r\n30000\r\n";

/// 协议层 mock 执行域：SET 按注入延时人工耗时（触发超阈判定），GET 即时返回；
/// CONFIG SET 仅落会话共享运行时配置槽位（对标 C# null owner 落槽形态）
struct MockApi {
  set_delay: Option<Duration>,
}

impl GarnetApiFace for MockApi {
  fn exec(&self, session: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) {
    match cmd {
      RespCommand::Set => {
        if let Some(delay) = self.set_delay {
          sleep(delay);
        }
        session.output.extend_from_slice(b"+OK\r\n");
      }
      RespCommand::Get => {
        session.output.extend_from_slice(b"$3\r\nbar\r\n");
      }
      RespCommand::ConfigSet => {
        let mut out = Vec::new();
        let _ = session.network_config_set::<SegmentedDevice>(args, None, &mut out);
        session.output.extend_from_slice(&out);
      }
      _ => {}
    }
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

/// 慢日志用例装配基座：纯 mock 执行域（指标采样关闭，延迟监视默认关，
/// 慢日志域与指标/延迟装配正交）
fn slowlog_consumer(
  data_path: &Path,
  set_delay: Option<Duration>,
  session_id: u64,
) -> aok::Result<RespSessionConsumer> {
  let mock: Arc<dyn GarnetApiFace> = Arc::new(MockApi { set_delay });
  let provider = StorageSessionProvider::open_with_config(
    test_store_config(),
    data_path,
    move |sender_id, _api| {
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions::default(),
        Arc::clone(&mock),
      ))
    },
  )?
  .with_metrics_sampling_frequency_secs(0);
  Ok(
    provider
      .get_session(WireFormat::Ascii, session_id)
      .expect("真实轨会话创建"),
  )
}

/// 网络泵等价消费（直填接收缓冲 → 唯一入口 → 应答随冲写取出）
/// 返回 (消费后残余, 应答字节)
fn pump(c: &mut RespSessionConsumer, bytes: &[u8]) -> (Option<usize>, Vec<u8>) {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(bytes);
  c.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = c.try_consume_messages_into(&mut resp);
  (remaining, resp)
}

/// SLOWLOG LEN 直读（独立批次，快命令不入库）
fn slowlog_len(c: &mut RespSessionConsumer) -> i64 {
  let (_, out) = pump(c, FRAME_SLOWLOG_LEN);
  let text = String::from_utf8_lossy(&out);
  assert!(
    text.starts_with(':') && text.ends_with("\r\n"),
    "out: {text}"
  );
  text[1..text.len() - 2].parse().expect("SLOWLOG LEN 整数帧")
}

/// 钉住单调钟进程锚点并推进：保证「起始 tick 为 0 时误当纪元量级比较」的
/// 旧路径耗时（= 自进程锚点起刻度，≥ 500_000 tick）恒超用例阈值，判据确定
fn pin_clock_anchor_past_threshold() {
  assert!(now_stopwatch_ticks() > 0);
  sleep(Duration::from_millis(50));
}

/// 验证一 + 验证三（禁用批零开销、动态开启无虚假慢查询）：
/// C# TryConsumeMessages:484-489 批入口缓存门控——本批 CONFIG SET 生效后，
/// 批内后续命令与批起点自身均不得以 0 起始戳入判定；下一批次起始戳与阈值
/// 对齐后方正常计量
#[test]
fn dynamic_enable_same_batch_records_no_phantom_entry() -> aok::Result<()> {
  let dir = tempdir()?;
  let rt = Runtime::new()?;
  rt.block_on(async {
    let db = dir.path().join("phantom.db");
    let mut consumer = slowlog_consumer(&db, None, 1)?;
    consumer.set_runtime_config(Arc::new(RuntimeServerConfig::with_defaults()));
    consumer.set_slow_log_container(new_slow_log_container(10));
    pin_clock_anchor_past_threshold();

    // 单批四命令：SET（快）→ CONFIG SET 10ms → SET（快）→ GET（快）
    let mut batch = Vec::new();
    batch.extend_from_slice(FRAME_SET);
    batch.extend_from_slice(FRAME_CONFIG_10MS);
    batch.extend_from_slice(FRAME_SET);
    batch.extend_from_slice(FRAME_GET);
    pump(&mut consumer, &batch);

    // 实时配置已落槽（下一批次入口须折算生效）
    assert_eq!(
      consumer
        .session()
        .runtime_config()
        .get_microseconds(ServerConfigType::SlowlogLogSlowerThan),
      10_000
    );
    // 本批入口缓存阈值 0 → 命令循环零进入慢日志（C# :728 门）：零时钟获取
    //（起始 tick 保持 0 哨兵）、零入库——旧实现逐命令实时读配置，CONFIG SET
    // 自身即以自进程锚点起的纪元量级耗时虚假入库
    assert_eq!(
      consumer.session().slow_log_start_ticks,
      0,
      "禁用批不得取时钟"
    );
    assert_eq!(slowlog_len(&mut consumer), 0, "动态开启批后无虚假慢查询");

    // 下一批次：阈值缓存生效且起始戳同批对齐，快速命令（≪ 10ms）零入库
    pump(&mut consumer, FRAME_SET);
    pump(&mut consumer, FRAME_GET);
    // 缓存阈值 = 10_000 μs × tick/μs 折算（公式推导）
    assert_eq!(
      consumer.session().slow_log_threshold,
      10_000 * TICKS_PER_MICROSECOND
    );
    assert!(
      consumer.session().slow_log_start_ticks > 0,
      "启用批起点须刷新起始戳"
    );
    assert_eq!(slowlog_len(&mut consumer), 0);
    let (_, out) = pump(&mut consumer, FRAME_SLOWLOG_GET);
    assert_eq!(out, b"*0\r\n");
    aok::OK
  })
}

/// 验证二（真实慢查询准确记录）：C# HandleSlowLog —— elapsed 超缓存阈值才入库，
/// 耗时 = 作差 tick / 每微秒刻度（微秒量级，禁纪元量级巨值）
#[test]
fn real_slow_command_recorded_with_microsecond_duration() -> aok::Result<()> {
  let dir = tempdir()?;
  let rt = Runtime::new()?;
  rt.block_on(async {
    // SET 人工耗时 50ms：恒超 30ms 阈值，且耗时下界由 sleep 保证（公式锚）
    let db = dir.path().join("real.db");
    let mut consumer = slowlog_consumer(&db, Some(Duration::from_millis(50)), 2)?;
    consumer.set_runtime_config(Arc::new(RuntimeServerConfig::with_defaults()));
    consumer.set_slow_log_container(new_slow_log_container(10));
    pin_clock_anchor_past_threshold();

    // 批次一：CONFIG SET 30ms —— 入口缓存阈值 0，CONFIG SET 自身零入库
    pump(&mut consumer, FRAME_CONFIG_30MS);
    assert_eq!(slowlog_len(&mut consumer), 0);

    // 批次二：50ms 耗时 SET —— 准确记录且仅此一条
    pump(&mut consumer, FRAME_SET);
    assert_eq!(slowlog_len(&mut consumer), 1);

    let (_, out) = pump(&mut consumer, FRAME_SLOWLOG_GET);
    let text = String::from_utf8_lossy(&out);
    // 单条目六元组帧（协议：id/timestamp/duration/参数数组/ip:port/name）
    assert!(text.starts_with("*1\r\n*6\r\n"), "out: {text}");
    assert!(text.contains("$3\r\nSET\r\n"), "out: {text}");
    assert!(text.contains("$1\r\nk\r\n"), "out: {text}");
    assert!(text.contains("$1\r\nv\r\n"), "out: {text}");
    let ints: Vec<i64> = text
      .split("\r\n")
      .filter_map(|line| line.strip_prefix(':'))
      .filter_map(|v| v.parse().ok())
      .collect();
    assert_eq!(
      ints.len(),
      3,
      "整数段行应恰为 id/timestamp/duration: {text}"
    );
    // 公式：sleep(50ms) 下界 → duration_us ≥ 50_000（100ns 刻度端点截断留
    // 100μs 裕量）；上界 5s —— 哨捕把 0 起始值当纪元比较的数千秒级虚假耗时
    assert!(
      (49_900..5_000_000).contains(&ints[2]),
      "duration_us: {}",
      ints[2]
    );
    aok::OK
  })
}

/// 验证三（禁用状态零配置访问）：handle_slow_log 唯一阈值来源是批次缓存字段，
/// 与实时配置读取彻底脱钩——缓存 0 即短路，零时钟获取、零序列化、零入库
#[test]
fn disabled_cached_threshold_short_circuits_live_config() {
  let mut session = RespServerSession::new(9, RespServerSessionOptions::default());
  let config = Arc::new(RuntimeServerConfig::with_defaults());
  session.set_runtime_config(Arc::clone(&config));
  let container = new_slow_log_container(10);
  session.set_slow_log_container(Arc::clone(&container));

  // 实时配置直接落槽开启（绕过批次入口缓存路径）：缓存阈值保持 0 = 禁用形态
  let mut out = Vec::new();
  let _ = session.network_config_set::<SegmentedDevice>(
    &[b"slowlog-log-slower-than", b"10000"],
    None,
    &mut out,
  );
  assert_eq!(
    config.get_microseconds(ServerConfigType::SlowlogLogSlowerThan),
    10_000
  );
  assert_eq!(session.slow_log_threshold, 0);
  pin_clock_anchor_past_threshold();

  // 旧实现此处实时读配置（> 0）并以 0 起始戳作纪元比较 → 虚假巨耗时入库；
  // 新实现缓存 0 短路：不取时钟（起始 tick 保持 0 哨兵）、零入库
  session.handle_slow_log(RespCommand::Set);
  assert_eq!(session.slow_log_start_ticks, 0, "禁用短路路径不得取时钟");
  assert_eq!(container.count(), 0, "禁用短路路径不得入库");
}
