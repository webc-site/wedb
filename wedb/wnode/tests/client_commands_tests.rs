//! CLIENT LIST / CLIENT KILL / 监视器采样 端到端对标测试
//!
//! C# 参照 Garnet.test/RespTests/ClientsTests 与 libs/server/Resp/
//! ClientCommands.cs 语义：真实服务器 + 活跃消费者注册表 + 双连接交互；
//! 监视器段对标 GarnetServerMonitor.Start / MainMonitorTaskAsync 的
//! 采样-瞬时吞吐滚动与连接计数承接。
//!
//! 注册表与监视器均为进程级单例（CLIENT 族命令经 global 直取），断言收敛
//! 在单个测试函数内顺序执行，避免并行用例互扰。

use std::{
  future::ready,
  io::Error,
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
  },
  thread::sleep,
  time::Duration,
};

use compio::{net::TcpStream, runtime::Runtime};
use tempfile::tempdir;
use wmetric::GarnetServerMonitor;
use wnode::{
  resp::{RespSessionConsumer, resp_server_session::RespServerSessionOptions},
  servers::consumer_registry::ConsumerRegistry,
  service::StorageSessionProvider,
};
use wnode_test::{err_frame, read_reply, send, send_cmd, start_server};
use wresp::cmd_strings::RESP_ERR_GENERIC_SYNTAX_ERROR;
use wtest_base::test_store_config;

/// bulk 帧载荷断言解析器（作用于已收 frame 字节，非 IO 读取——
/// wnode_test::read_bulk_reply 是流级直读，无法对 CLIENT LIST 的
/// 多行文本做逐字段解析，故保留本地字节切片版）
fn bulk_body(frame: &[u8]) -> String {
  let nl = frame.iter().position(|&b| b == b'\n').expect("bulk header");
  let len: usize = from_utf8(&frame[1..nl - 1])
    .expect("utf8 len")
    .parse()
    .expect("bulk len");
  String::from_utf8(frame[nl + 1..nl + 1 + len].to_vec()).expect("utf8 body")
}

/// 整数应答断言解析器（:N\r\n → i64，作用于已收 frame 字节，同 bulk_body）
fn int_reply(frame: &[u8]) -> i64 {
  from_utf8(&frame[1..frame.len() - 2])
    .expect("utf8 int")
    .parse()
    .expect("int reply")
}

/// 驱动 N 轮监视器采样（复位回调与宿主装配同构：
/// [`ConsumerRegistry::monitor_iteration_inputs`]，C# 监视器直查 servers 的承接）
async fn run_sampling_rounds(
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
        registry.monitor_iteration_inputs()
      },
    )
    .await;
}

#[test]
fn client_list_kill_and_monitor_flow() -> aok::Result<()> {
  let dir = tempdir()?;
  let provider = Arc::new(
    StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join("node.db"),
      |sender_id, api| {
        Some(RespSessionConsumer::new(
          sender_id,
          RespServerSessionOptions::default(),
          Arc::new(api),
        ))
      },
    )?
    // 采样走真实轨：装配期按 secs>0 建句柄并经 attach_session_metrics 注入，
    // 会话与存储执行域共持同一 Arc（替代已删的选项侧布尔死轨）
    .with_metrics_sampling_frequency_secs(1),
  );

  let (server, addr) = start_server(provider.clone());

  // 监视器进程级安装（C# StoreWrapper 构造 monitor + Start；频率 1s 作
  // 瞬时吞吐折算基准），未装配时 dispose 归并为无害空操作
  let monitor = Arc::new(GarnetServerMonitor::new(1, true, false, false));
  assert!(monitor.install_global(), "监视器进程级安装");

  let rt = Runtime::new()?;
  rt.block_on(async {
    let mut a = TcpStream::connect(addr).await?;
    let mut b = TcpStream::connect(addr).await?;

    // 预热：泵在建连后首个数据帧才创建并注册会话
    send_cmd(&mut a, &[b"PING"]).await?;
    assert_eq!(read_reply(&mut a).await, b"+PONG\r\n");
    send_cmd(&mut b, &[b"PING"]).await?;
    assert_eq!(read_reply(&mut b).await, b"+PONG\r\n");

    // 命名（CLIENT SETNAME → 注册表条目视图自刷新）
    send_cmd(&mut a, &[b"CLIENT", b"SETNAME", b"alpha"]).await?;
    assert_eq!(read_reply(&mut a).await, b"+OK\r\n");
    send_cmd(&mut b, &[b"CLIENT", b"SETNAME", b"beta"]).await?;
    assert_eq!(read_reply(&mut b).await, b"+OK\r\n");

    // CLIENT LIST：两条记录，字段序对齐 C# WriteClientInfo
    send_cmd(&mut a, &[b"CLIENT", b"LIST"]).await?;
    let lines: Vec<String> = bulk_body(&read_reply(&mut a).await)
      .trim_end_matches('\n')
      .split('\n')
      .map(str::to_string)
      .collect();
    assert_eq!(lines.len(), 2);
    let alpha = lines
      .iter()
      .find(|l| l.contains("name=alpha"))
      .expect("alpha");
    assert!(alpha.contains("id="));
    assert!(alpha.contains(" addr=127.0.0.1:"));
    assert!(alpha.contains(" laddr=127.0.0.1:"));
    assert!(alpha.contains(" age="));
    // C# WriteClientInfo 恒带 user 字段（GetDefaultUserHandle；NoAuth 档
    // 会话经构造期 AuthenticateUser 落到 default 用户）
    assert!(alpha.contains(" user=default"));
    assert!(alpha.contains(" flags=N"));
    assert!(alpha.contains(" db=0"));
    assert!(alpha.contains(" resp=2"));
    assert!(alpha.contains(" lib-name= lib-ver="));
    assert!(
      lines
        .iter()
        .any(|l| l.contains("name=beta") && l.contains("flags=N"))
    );
    let a_addr = alpha
      .split(" addr=")
      .nth(1)
      .expect("addr field")
      .split(' ')
      .next()
      .expect("addr value")
      .to_string();

    // TYPE 过滤（normal 双双命中）
    send_cmd(&mut a, &[b"CLIENT", b"LIST", b"TYPE", b"normal"]).await?;
    assert_eq!(
      bulk_body(&read_reply(&mut a).await)
        .trim_end_matches('\n')
        .split('\n')
        .count(),
      2
    );

    // SLAVE 于 CLIENT LIST 非法（原始大小写保留进文案）
    send_cmd(&mut a, &[b"CLIENT", b"LIST", b"TYPE", b"slave"]).await?;
    assert_eq!(
      read_reply(&mut a).await,
      b"-ERR Unknown client type 'slave'\r\n"
    );

    // 参数错误面（C# 文案逐字对齐）
    send_cmd(&mut a, &[b"CLIENT", b"KILL"]).await?;
    assert_eq!(
      read_reply(&mut a).await,
      b"-ERR wrong number of arguments for 'CLIENT|KILL' command\r\n"
    );
    send_cmd(&mut a, &[b"CLIENT", b"KILL", b"ID", b"abc"]).await?;
    assert_eq!(
      read_reply(&mut a).await,
      b"-ERR client-id should be greater than 0\r\n"
    );
    send_cmd(&mut a, &[b"CLIENT", b"KILL", b"ID", b"1", b"ID", b"2"]).await?;
    assert_eq!(
      read_reply(&mut a).await,
      b"-ERR Filter 'ID' defined multiple times\r\n"
    );
    send_cmd(&mut a, &[b"CLIENT", b"KILL", b"FOO", b"bar"]).await?;
    assert_eq!(
      read_reply(&mut a).await,
      err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR)
    );

    // B 的会话 id（老式 KILL 与 ID 过滤的目标准备）
    send_cmd(&mut b, &[b"CLIENT", b"ID"]).await?;
    let b_id = int_reply(&read_reply(&mut b).await);

    // 老式 ip:port：无匹配 → NO_SUCH_CLIENT
    send_cmd(&mut a, &[b"CLIENT", b"KILL", b"127.0.0.1:1"]).await?;
    assert_eq!(read_reply(&mut a).await, b"-ERR No such client\r\n");

    // ADDR 过滤杀 A（由 B 发起；应答为实际杀掉数）
    send_cmd(&mut b, &[b"CLIENT", b"KILL", b"ADDR", a_addr.as_bytes()]).await?;
    assert_eq!(int_reply(&read_reply(&mut b).await), 1);

    // A 被服务端关闭（挂起读被取消，连接断开）
    assert!(read_reply(&mut a).await.is_empty());

    // LIST 仅剩 beta
    send_cmd(&mut b, &[b"CLIENT", b"LIST"]).await?;
    let remaining = bulk_body(&read_reply(&mut b).await);
    assert!(remaining.contains("name=beta"));
    assert!(!remaining.contains("name=alpha"));

    // ID + SKIPME NO 自杀（默认 SKIPME=yes 时无法命中自身）
    let b_id = b_id.to_string();
    send_cmd(
      &mut b,
      &[b"CLIENT", b"KILL", b"ID", b_id.as_bytes(), b"SKIPME", b"NO"],
    )
    .await?;
    assert_eq!(int_reply(&read_reply(&mut b).await), 1);
    assert!(read_reply(&mut b).await.is_empty());

    // 注销闭环（C# TotalConnectionsReceived/Disposed 语义）
    let mut totals = provider.registry.connection_totals();
    for _ in 0..200 {
      if totals.1 >= 2 {
        break;
      }
      sleep(Duration::from_millis(10));
      totals = provider.registry.connection_totals();
    }
    assert_eq!(totals, (2, 2, 0));

    // —— 监视器采样链路（C# MainMonitorTaskAsync + UpdateInstantaneousMetrics）——
    // 基线：连接 c 一条 PING，泵镜像回包后首轮采样落基线
    let mut c = TcpStream::connect(addr).await?;
    send_cmd(&mut c, &[b"PING"]).await?;
    assert_eq!(read_reply(&mut c).await, b"+PONG\r\n");
    run_sampling_rounds(&monitor, &provider.registry, 1).await;

    // 窗口内恰 5 条命令：单轮采样后瞬时 ops/s 精确滚动（频率 1s 折算），
    // 连接计数与网络字节随采样承接
    for _ in 0..5 {
      send(&mut c, b"PING\r\n").await?;
      assert_eq!(read_reply(&mut c).await, b"+PONG\r\n");
    }
    run_sampling_rounds(&monitor, &provider.registry, 1).await;
    let snap = monitor.snapshot().expect("采样轮装配全局指标快照");
    assert_eq!(snap.instantaneous_cmd_per_sec, 5.0);
    assert!(snap.instantaneous_net_input_tpt > 0.0);
    assert!(snap.instantaneous_net_output_tpt > 0.0);

    // 释放 c：注销后采样轮承接连接计数（dispose 归并已并入历史）
    drop(c);
    let mut totals = provider.registry.connection_totals();
    for _ in 0..200 {
      if totals == (3, 3, 0) {
        break;
      }
      sleep(Duration::from_millis(10));
      totals = provider.registry.connection_totals();
    }
    assert_eq!(totals, (3, 3, 0));
    run_sampling_rounds(&monitor, &provider.registry, 1).await;
    let snap = monitor.snapshot().expect("采样轮装配全局指标快照");
    assert_eq!(
      (
        snap.total_connections_received,
        snap.total_connections_disposed,
        snap.total_connections_active
      ),
      (3, 3, 0)
    );
    Ok::<(), Error>(())
  })?;

  server.stop();
  Ok(())
}
