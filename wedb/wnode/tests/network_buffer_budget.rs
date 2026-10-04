//! 网络缓冲预算端到端回归（PR #2157：每连接基准规格随活跃数自适应）
//!
//! 对标 garnet/test/standalone/Garnet.test/NetworkBufferBudgetTests.cs 的
//! 端到端臂（SmallBudgetDrivesThePublishedTargetDown /
//! ZeroBudgetDisablesAdaptationEndToEnd /
//! DefaultConfigPublishesTheConfiguredSizeToInfo /
//! LargePayloadsStillSucceedWhileTheBudgetIsAtItsFloor）：
//! 进程级预算 ÷ 活跃缓冲数低于配置基准规格时，新缓冲基准向下适配至接收
//! 地板；按需增长永不钳制——预算钉在地板时大值读写照常成功。

use std::{io, net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

use compio::{
  BufResult,
  io::{AsyncWrite, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
  time::sleep,
};
use tempfile::tempdir;
use wbase::pool::NetworkBufferBudget;
use wnode::{
  GarnetServer, RespSessionConsumer, resp::resp_server_session::RespServerSessionOptions,
  service::StorageSessionProvider,
};
use wnode_test::read_reply;
use wtest_base::test_store_config;

/// 写出命令载荷（RESP 数组帧）后即 flush
async fn send_cmd(stream: &mut TcpStream, args: &[&[u8]]) -> io::Result<()> {
  let BufResult(res, _) = stream.write_all(wtest_base::resp_frame(args)).await;
  res?;
  stream.flush().await
}

/// bulk 应答负载长度头
fn bulk_len(frame: &[u8]) -> usize {
  let nl = frame.iter().position(|&b| b == b'\n').expect("bulk header");
  String::from_utf8_lossy(&frame[1..nl - 1])
    .parse::<usize>()
    .expect("bulk len")
}

/// 起单 worker 真存储服务器（预算字节数 None = 不装配预算）
async fn spawn_server(
  dir: &tempfile::TempDir,
  budget_bytes: Option<i64>,
) -> (
  SocketAddr,
  Arc<GarnetServer<StorageSessionProvider<wnode_test::SessionFactory>>>,
) {
  let factory: wnode_test::SessionFactory = |sender_id, api| {
    Some(RespSessionConsumer::new(
      sender_id,
      RespServerSessionOptions::default(),
      Arc::new(api),
    ))
  };
  let provider = Arc::new(
    StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join("net-budget.db"),
      factory,
    )
    .unwrap(),
  );
  let server = GarnetServer::new(&["127.0.0.1:0".to_string()], 1 << 16, provider)
    .unwrap()
    .with_network_buffer_budget_bytes(budget_bytes);
  server.start(NonZeroUsize::new(1)).unwrap();
  (server.local_addr().unwrap(), Arc::new(server))
}

/// 等待预算目标落到期望值（连接任务的会话/借出计数异步到位）
async fn wait_target(budget: &NetworkBufferBudget, expect: usize) {
  for _ in 0..200 {
    if budget.target_buffer_size() == expect {
      return;
    }
    sleep(Duration::from_millis(10)).await;
  }
  panic!(
    "预算目标未落到 {expect}（现 {}，活跃 {}）",
    budget.target_buffer_size(),
    budget.live_buffer_count()
  );
}

/// SmallBudgetDrivesThePublishedTargetDown：小预算下活跃连接把已发布目标
/// 压低到接收地板；INFO BPSTATS 出 network_buffer_budget 行
#[test]
fn small_budget_drives_published_target_down() {
  let dir = tempdir().unwrap();
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (addr, server) = spawn_server(&dir, Some(96 * 1024)).await;
    let budget = server
      .buffer_pool()
      .budget()
      .expect("预算装配后池必须挂预算")
      .clone();

    // 基线：无连接时目标为配置基准规格（DefaultConfigPublishesThe-
    // ConfiguredSizeToInfo 的池面臂）
    assert_eq!(budget.target_buffer_size(), 1 << 16);
    assert!(!budget.is_under_pressure());

    // 每连接 2 块（send 借出 + 会话接收缓冲）：2 连接 → 活跃 4 →
    // 96K/4 = 24K → 目标 16K（接收地板）
    let mut c1 = TcpStream::connect(addr).await.unwrap();
    send_cmd(&mut c1, &[b"PING"]).await.unwrap();
    assert_eq!(read_reply(&mut c1).await, b"+PONG\r\n");
    let mut c2 = TcpStream::connect(addr).await.unwrap();
    send_cmd(&mut c2, &[b"PING"]).await.unwrap();
    assert_eq!(read_reply(&mut c2).await, b"+PONG\r\n");
    wait_target(&budget, 1 << 14).await;
    assert!(budget.is_under_pressure());
    assert_eq!(budget.target_receive_buffer_size(), 1 << 14);
    assert_eq!(budget.target_send_buffer_size(), 1 << 15);

    // INFO BPSTATS 线上可见 network_buffer_budget 行
    send_cmd(&mut c1, &[b"INFO", b"bpstats"]).await.unwrap();
    let reply = read_reply(&mut c1).await;
    let text = String::from_utf8_lossy(&reply);
    assert!(
      text.contains("network_buffer_budget"),
      "INFO BPSTATS 须含预算行：{text}"
    );
    assert!(
      text.contains("target_buffer_size=16384"),
      "预算行须携带已压低的目标：{text}"
    );

    // LargePayloadsStillSucceedWhileTheBudgetIsAtItsFloor：目标钉在地板时
    // 大值（128KB，超基准规格两档）照常读写（按需增长永不钳制）
    let big = vec![b'x'; 1 << 17];
    send_cmd(&mut c2, &[b"SET", b"big", &big]).await.unwrap();
    assert_eq!(read_reply(&mut c2).await, b"+OK\r\n");
    send_cmd(&mut c2, &[b"GET", b"big"]).await.unwrap();
    let reply = read_reply(&mut c2).await;
    assert_eq!(bulk_len(&reply), big.len(), "大值须完整往返");

    drop(c1);
    drop(c2);
    server.stop();
  });
}

/// ZeroBudgetDisablesAdaptationEndToEnd：0 预算禁用——池无预算句柄，
/// 大负载后缓冲规格保持配置基准（不收敛出第二套语义）
#[test]
fn zero_budget_disables_adaptation() {
  let dir = tempdir().unwrap();
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (addr, server) = spawn_server(&dir, Some(0)).await;
    assert!(
      server.buffer_pool().budget().is_none(),
      "0 预算即不装配（Disabled 形态）"
    );

    let mut c = TcpStream::connect(addr).await.unwrap();
    send_cmd(&mut c, &[b"PING"]).await.unwrap();
    assert_eq!(read_reply(&mut c).await, b"+PONG\r\n");
    send_cmd(&mut c, &[b"INFO", b"bpstats"]).await.unwrap();
    let reply = read_reply(&mut c).await;
    let text = String::from_utf8_lossy(&reply);
    assert!(
      !text.contains("network_buffer_budget"),
      "禁用形态不出预算行：{text}"
    );

    let big = vec![b'y'; 1 << 17];
    send_cmd(&mut c, &[b"SET", b"big", &big]).await.unwrap();
    assert_eq!(read_reply(&mut c).await, b"+OK\r\n");
    drop(c);
    // 收场等待：连接任务在 worker 运行时异步收尾，借出计数轮询归零
    for _ in 0..200 {
      if server.buffer_pool().borrowed_count() == 0 {
        break;
      }
      sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(server.buffer_pool().borrowed_count(), 0);
    server.stop();
  });
}
