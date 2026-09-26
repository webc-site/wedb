//! ServerBootstrap 连接上限投影端到端（回归 task/ing/
//! wnode-bootstrap-network-connection-limit-unwired）：
//!
//! 全链路对标 C#：Options.cs:399 `network-connection-limit` → Options.cs:948-962
//! 单点投影进 serverOptions → GarnetServer.cs:294 直读
//! `opts.NetworkConnectionLimit` 传入 GarnetServerTcp →
//! GarnetServerTcp.cs:236-241/302-307 容量门。
//!
//! rust 侧投影唯一落点为 [`wnode::ServerBootstrap::run_async`] 直读
//! `NodeArgs::network_connection_limit`：宿主零逐字段装配，任何经标准
//! ServerBootstrap 引导的启动路径（嵌入式/集成测试）配置必须直达网络层。
//! limit=2 时前两条 PING→PONG 正常、第三条被即刻关闭（客户端见 EOF，
//! 非错误应答）；超限拒绝刚发生即 stop，连接持有者存活下须有界时间内
//! 全量收敛优雅退出（回归 task/done/
//! wnode-accept-loop-shutdown-wake-race-after-connlimit-reject.md）。

use std::{
  io::Error,
  mem::take,
  net::TcpListener as StdTcpListener,
  sync::{Arc, mpsc::sync_channel},
  thread::spawn,
  time::Duration,
};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
  time::sleep,
};
use wconf::NodeArgs;
use wnode::{
  MessageConsumerFace, Result as WnodeResult, ServerBootstrap, SessionProviderFace,
  ShutdownCoordinator, WireFormat, servers::ConsumerRegistry,
};

/// 优雅关停收敛的有界时限：wait 50ms 超时兜底下正常毫秒级完成，
/// 丢唤醒挂死回归在此确定性判失败而非无限死等
const GRACEFUL_EXIT_TIMEOUT: Duration = Duration::from_secs(10);

/// 最小回声消费者：PING → +PONG（与 node_test.rs 容量门测试同一协议面）
struct EchoConsumer {
  buf: Vec<u8>,
  head: usize,
}

impl MessageConsumerFace for EchoConsumer {
  fn try_consume_messages_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
    while self.buf[self.head..].starts_with(b"PING\r\n") {
      self.head += 6;
      resp_buf.extend_from_slice(b"+PONG\r\n");
    }
    if self.head >= self.buf.len() {
      self.buf.clear();
      self.head = 0;
      return Some(0);
    }
    Some(self.buf.len() - self.head)
  }
  fn take_recv_scratch(&mut self) -> Vec<u8> {
    take(&mut self.buf)
  }
  fn return_recv_scratch(&mut self, buf: Vec<u8>) {
    self.buf = buf;
  }
  fn dispose(&mut self) {}
}

/// 供给容量门计量的注册表提供者（无注册表即无门，语义同 limit=-1）
struct RegistryProvider {
  registry: Arc<ConsumerRegistry>,
}

impl SessionProviderFace for RegistryProvider {
  type Consumer = EchoConsumer;
  fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<EchoConsumer> {
    Some(EchoConsumer {
      buf: Vec::new(),
      head: 0,
    })
  }
  fn consumer_registry(&self) -> Option<Arc<ConsumerRegistry>> {
    Some(Arc::clone(&self.registry))
  }
}

/// ServerBootstrap 从 NodeArgs 投影连接上限直达网络容量门：
/// limit=2 时第三条连接被即刻关闭（EOF），宿主侧未做任何逐字段装配
#[test]
fn test_bootstrap_projects_network_connection_limit() -> aok::Result<()> {
  // 探测空闲端口（run_async 内部构造 GarnetServer 不外露绑定地址，
  // 端口 0 不可回读，先探后用）
  let probe = StdTcpListener::bind("127.0.0.1:0")?;
  let port = probe.local_addr()?.port();
  drop(probe);

  let dir = tempfile::tempdir()?;
  let node = NodeArgs {
    bind: Some("127.0.0.1".into()),
    port,
    dir: dir.path().to_path_buf(),
    threads: Some(1),
    quiet: true,
    network_connection_limit: 2,
    ..Default::default()
  };

  let coordinator = ShutdownCoordinator::new();
  let coord_for_thread = coordinator.clone();
  // 停机收敛经 channel 有界断言：若唤醒丢失回归则确定性失败而非无限死等
  let (done_tx, done_rx) = sync_channel::<WnodeResult<()>>(0);
  let handle = spawn(move || {
    let res = ServerBootstrap::new(node)
      .with_shutdown_coordinator(coord_for_thread)
      .run_async(|_args, _noop| async {
        Ok(Arc::new(RegistryProvider {
          registry: Arc::new(ConsumerRegistry::new()),
        }))
      });
    let _ = done_tx.send(res);
  });

  let rt = Runtime::new()?;
  rt.block_on(async move {
    // 服务就绪轮询：首条连接 PING → PONG 即占用第 1 个在途额度
    let mut c1 = None;
    for _ in 0..200 {
      if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)).await {
        let _ = stream.write_all(b"PING\r\n".to_vec()).await;
        let BufResult(res, read_buf) = stream.read(vec![0u8; 128]).await;
        if res.is_ok_and(|n| n > 0 && &read_buf[..n] == b"+PONG\r\n") {
          c1 = Some(stream);
          break;
        }
      }
      sleep(Duration::from_millis(50)).await;
    }
    let _c1 = c1.ok_or_else(|| Error::other("服务未在时限内就绪"))?;

    // 第 2 条：占满 limit=2，PING → PONG 正常
    let mut c2 = TcpStream::connect(("127.0.0.1", port)).await?;
    let _ = c2.write_all(b"PING\r\n".to_vec()).await;
    let BufResult(res, read_buf) = c2.read(vec![0u8; 128]).await;
    assert_eq!(&read_buf[..res?], b"+PONG\r\n");

    // 第 3 条：connect 后不做任何写，读侧见立即 EOF（超限臂只关不发，
    // 客户端无任何 RESP 应答可读——C# GarnetServerTcp.cs:302-307）
    let mut c3 = TcpStream::connect(("127.0.0.1", port)).await?;
    let BufResult(res, _) = c3.read(vec![0u8; 16]).await;
    assert_eq!(res?, 0, "超限连接须被即刻关闭（EOF）");

    // 超限拒绝刚发生、连接持有者全部存活，立即受控停机：
    // ShutdownCoordinator::wait 的 50ms 超时兜底保证 accept 哨兵与
    // coord_task 确定性唤醒（曾有跨线程丢唤醒竞态，见 task/done/
    // wnode-accept-loop-shutdown-wake-race-after-connlimit-reject.md），
    // 无需任何排空等待
    coordinator.stop();

    Ok::<(), Error>(())
  })?;

  // 全量收敛断言：worker 线程 accept 循环被打断、wait_for_shutdown 返回、
  // bootstrap 线程在有界时限内投递结果；唤醒丢失回归确定性判失败
  let res = done_rx
    .recv_timeout(GRACEFUL_EXIT_TIMEOUT)
    .map_err(|_| Error::other("优雅关停未在时限内收敛（疑似跨线程唤醒丢失致永久挂起）"))?;
  assert!(res.is_ok(), "服务应优雅退出: {res:?}");
  let _ = handle.join();
  Ok(())
}
