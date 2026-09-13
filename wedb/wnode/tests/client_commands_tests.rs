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
  num::NonZeroUsize,
  sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
  },
};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
};
use tempfile::tempdir;
use wmetric::{GarnetServerMonitor, MonitorIterationInputs};
use wnode::{
  GarnetServer,
  resp::{RespSessionConsumer, resp_server_session::RespServerSessionOptions},
  service::StorageSessionProvider,
  servers::consumer_registry::ConsumerRegistry,
};

/// 写出命令载荷并透传 IO 结果（compio BufResult → Result 折叠）
async fn send(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
  let BufResult(res, _) = stream.write_all(data.to_vec()).await;
  res
}

/// 以 RESP 数组帧发送命令（服务器仅支持数组帧；内联只有 PING/QUIT 特化，
/// 对齐 C# FastParseInlineCommand）
async fn send_cmd(stream: &mut TcpStream, args: &[&[u8]]) -> std::io::Result<()> {
  let mut frame = format!("*{}\r\n", args.len()).into_bytes();
  for arg in args {
    frame.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
    frame.extend_from_slice(arg);
    frame.extend_from_slice(b"\r\n");
  }
  send(stream, &frame).await
}

/// 读取一条完整 RESP 应答（bulk / 行式帧；对端断开返回已收字节）
async fn read_reply(stream: &mut TcpStream) -> Vec<u8> {
  let mut acc = Vec::new();
  loop {
    let buf = vec![0u8; 8192];
    let BufResult(res, returned) = stream.read(buf).await;
    match res {
      Ok(0) => return acc,
      Ok(n) => {
        acc.extend_from_slice(&returned[..n]);
        if is_complete_frame(&acc) {
          return acc;
        }
      }
      Err(_) => return acc,
    }
  }
}

/// RESP 帧完整性判定（测试用最小实现：行式或 bulk）
fn is_complete_frame(buf: &[u8]) -> bool {
  match buf.first() {
    Some(b'+' | b'-' | b':') => buf.ends_with(b"\r\n"),
    Some(b'$') => {
      let Some(nl) = buf.iter().position(|&b| b == b'\n') else {
        return false;
      };
      let Ok(len) = std::str::from_utf8(&buf[1..nl - 1]).map(str::parse::<usize>) else {
        return false;
      };
      let Ok(len) = len else { return false };
      buf.len() >= nl + 1 + len + 2
    }
    _ => false,
  }
}

/// 取 bulk 帧载荷（$N\r\n<body>）
fn bulk_body(frame: &[u8]) -> String {
  let nl = frame.iter().position(|&b| b == b'\n').expect("bulk header");
  let len: usize = std::str::from_utf8(&frame[1..nl - 1])
    .expect("utf8 len")
    .parse()
    .expect("bulk len");
  String::from_utf8(frame[nl + 1..nl + 1 + len].to_vec()).expect("utf8 body")
}

/// 取整数应答（:N\r\n）
fn int_reply(frame: &[u8]) -> i64 {
  std::str::from_utf8(&frame[1..frame.len() - 2])
    .expect("utf8 int")
    .parse()
    .expect("int reply")
}

/// 无活跃会话复位回调（零捕获形态，同 wnode::server 采样装配）
fn no_reset_sessions() {}
fn no_reset_command_stats() {}
fn no_reset_session_latency() {}

/// 驱动 N 轮监视器采样（空复位回调；快照源为注册表，与宿主装配同构——
/// C# MainMonitorTaskAsync 经 ActiveConsumers 直查的 rust 承接）
async fn run_sampling_rounds(
  monitor: &GarnetServerMonitor,
  registry: &ConsumerRegistry,
  rounds: u32,
) {
  let done = Arc::new(AtomicU32::new(0));
  let done_cancel = Arc::clone(&done);
  monitor
    .main_monitor_task_async(
      |_duration| std::future::ready(()),
      move || done_cancel.load(Ordering::Relaxed) >= rounds,
      || {
        done.fetch_add(1, Ordering::Relaxed);
        MonitorIterationInputs {
          servers: vec![registry.monitor_sample()],
          reset_all_session_latency: Box::new(no_reset_session_latency),
          reset_active_sessions: Box::new(no_reset_sessions),
          reset_active_command_stats: Box::new(no_reset_command_stats),
          reset_session_latency: Box::new(|_| {}),
        }
      },
    )
    .await;
}

#[test]
fn client_list_kill_and_monitor_flow() -> aok::Result<()> {
  let dir = tempdir()?;
  let provider = Arc::new(StorageSessionProvider::open(
    dir.path().join("node.db"),
    |sender_id, api| {
      Some(RespSessionConsumer::new(
        sender_id,
        RespServerSessionOptions {
          metrics_sampling_frequency: true,
          ..RespServerSessionOptions::default()
        },
        api,
      ))
    },
  )?);

  let server = GarnetServer::new(&["127.0.0.1:0".to_string()], 1 << 16, 8, provider.clone());
  server.start(NonZeroUsize::new(1))?;
  let addr = server.local_addr()?;

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
    let alpha = lines.iter().find(|l| l.contains("name=alpha")).expect("alpha");
    assert!(alpha.contains("id="));
    assert!(alpha.contains(" addr=127.0.0.1:"));
    assert!(alpha.contains(" laddr=127.0.0.1:"));
    assert!(alpha.contains(" age="));
    // NoAuth 档会话无认证句柄（rust 域界：无 ACL 实例可取默认用户），
    // user 字段不输出；C# 恒带 GetDefaultUserHandle 为既记域界差异
    assert!(!alpha.contains("user="));
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
    assert_eq!(read_reply(&mut a).await, b"-ERR syntax error\r\n");

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
    send_cmd(&mut b, &[b"CLIENT", b"KILL", b"ID", b_id.as_bytes(), b"SKIPME", b"NO"]).await?;
    assert_eq!(int_reply(&read_reply(&mut b).await), 1);
    assert!(read_reply(&mut b).await.is_empty());

    // 注销闭环（C# TotalConnectionsReceived/Disposed 语义）
    let mut totals = provider.registry.connection_totals();
    for _ in 0..200 {
      if totals.1 >= 2 {
        break;
      }
      std::thread::sleep(std::time::Duration::from_millis(10));
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
    let (_, _, _, cmd_per_sec, input_tpt, output_tpt) = monitor.global_metrics_snapshot();
    assert_eq!(cmd_per_sec, 5.0);
    assert!(input_tpt > 0.0);
    assert!(output_tpt > 0.0);

    // 释放 c：注销后采样轮承接连接计数（dispose 归并已并入历史）
    drop(c);
    let mut totals = provider.registry.connection_totals();
    for _ in 0..200 {
      if totals == (3, 3, 0) {
        break;
      }
      std::thread::sleep(std::time::Duration::from_millis(10));
      totals = provider.registry.connection_totals();
    }
    assert_eq!(totals, (3, 3, 0));
    run_sampling_rounds(&monitor, &provider.registry, 1).await;
    let (received, disposed, active, _, _, _) = monitor.global_metrics_snapshot();
    assert_eq!((received, disposed, active), (3, 3, 0));
    Ok::<(), std::io::Error>(())
  })?;

  server.stop();
  Ok(())
}

