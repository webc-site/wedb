#![cfg(feature = "tls")]

//! TLS 握手超时与在途连接配额回收集成测试
//! （回归 task/ing/wnode-tls-handshake-missing-timeout-and-connection-exhaustion）
//!
//! 在 garnet 中的相对路径： test/standalone/Garnet.test/RespTlsTests.cs
//!
//! 对标面：慢速/静默握手（Slowloris）防护——客户端完成 TCP 三次握手后不发
//! ClientHello，服务端必须在握手超时后熔断：半开套接字被即刻关闭（客户端见
//! EOF）、在途连接配额随 RAII Drop 回落归零、后续正常 TLS 连接可继续接入。
//! 配额耗尽的拒绝臂（第三条连接进容量门即关，C# GarnetServerTcp.cs:302-307）
//! 在挂起窗口内一并验证。
//!
//! 握手超时生产默认 10s 对测试过长，经
//! [`GarnetServer::with_tls_handshake_timeout`] 注入短值收敛等待（该装配位
//! 仅此测试使用，ServerBootstrap 标准启动路径恒取缺省 10s）。

use std::{
  io::{Error, ErrorKind},
  net::SocketAddr,
  num::NonZeroUsize,
  sync::Arc,
  time::Duration,
};

use compio::{BufResult, io::AsyncRead, net::TcpStream, runtime::Runtime, time::timeout};
use compio_tls::TlsConnector;
use wnode::{GarnetServer, servers::ConsumerRegistry};
use wnode_tls_test::{
  RegistryProvider, assert_peer_closed, ping, test_connector, test_server_tls, wait_entries,
};

/// 测试注入的握手超时：短到秒级收敛、长到覆盖前置断言步序
const TEST_HANDSHAKE_TIMEOUT: Duration = Duration::from_millis(1000);
/// 全用例有界时限：握手超时防护失效（无限挂起回归）时确定性失败而非死等
const TEST_DEADLINE: Duration = Duration::from_secs(30);

/// 握手超时熔断主用例：两条静默半开占满 limit=2 配额（第三条即刻被容量门
/// 拒绝关闭）→ 握手超时触发 → 半开套接字关闭、配额归零 → 正常 TLS 客户端
/// 握手 + PING→PONG 重新接入成功
#[test]
fn test_tls_handshake_timeout_releases_connection_quota() -> aok::Result<()> {
  // 服务端与信任锚共用进程内单份自签证书（wnode_tls_test 夹具，纯内存）
  let server_tls = test_server_tls()?;

  let registry = Arc::new(ConsumerRegistry::new());
  let server = GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    4096,
    8,
    Arc::new(RegistryProvider {
      registry: Arc::clone(&registry),
    }),
  )?
  .with_tls_config(server_tls)
  .with_network_connection_limit(2)
  .with_tls_handshake_timeout(TEST_HANDSHAKE_TIMEOUT);
  server.start(NonZeroUsize::new(1))?;
  let addr = server.local_addr()?;

  // 信任自签名的 TLS 客户端连接器
  let connector = test_connector()?;

  let rt = Runtime::new()?;
  rt.block_on(async move {
    timeout(TEST_DEADLINE, run_case(addr, registry, connector))
      .await
      .map_err(|_| Error::new(ErrorKind::TimedOut, "用例整体时限已到"))?
  })?;

  server.stop();
  Ok(())
}

async fn run_case(
  addr: SocketAddr,
  registry: Arc<ConsumerRegistry>,
  connector: TlsConnector,
) -> Result<(), Error> {
  // 1. 两条半开连接：TCP 建立后零字节（不发 ClientHello），握手无限 Pending
  //    占住在途配额；预注册条目入治理面可观测
  let mut c1 = TcpStream::connect(addr).await?;
  let mut c2 = TcpStream::connect(addr).await?;
  wait_entries(&registry, 2).await?;

  // 2. 配额耗尽态：第三条连接进容量门即关，客户端见 EOF、无任何 RESP 应答
  //    （C# GarnetServerTcp.cs:302-307；此断言须在握手超时熔断前落窗）
  let mut c3 = TcpStream::connect(addr).await?;
  assert_peer_closed(&mut c3, "超限连接").await?;
  drop(c3);

  // 3. 握手超时熔断：两条半开套接字被服务端主动关闭（客户端读侧 EOF），
  //    _in_flight 守卫与 handler 随任务收场 Drop，配额与条目表同步回落
  assert_peer_closed(&mut c1, "半开连接1").await?;
  assert_peer_closed(&mut c2, "半开连接2").await?;
  wait_entries(&registry, 0).await?;

  // 4. 配额归零后正常 TLS 连接可完整握手并收发自如（回声 PING → +PONG）
  let tcp = TcpStream::connect(addr).await?;
  let mut tls = connector.connect("localhost", tcp).await?;
  ping(&mut tls).await?;
  let BufResult(res, read_buf) = tls.read(vec![0u8; 128]).await;
  let n = res?;
  if &read_buf[..n] != b"+PONG\r\n" {
    return Err(Error::other(format!(
      "TLS 正常连接应答不符，实际 {:?}",
      &read_buf[..n]
    )));
  }
  Ok(())
}
