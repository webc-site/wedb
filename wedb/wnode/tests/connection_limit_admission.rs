//! 连接准入端到端回归（PR #2157：进程级 maxclients + 拒绝告知 + 拒绝计数）
//!
//! 对标 garnet/test/standalone/Garnet.test/ConnectionLimitTests.cs 核心臂：
//! - RejectedConnectionReceivesAnErrorRatherThanASilentClose（明文对端收到
//!   `-ERR max number of clients reached` 而非无解释复位）
//! - TheErrorArrivesWithoutTheClientSendingAnything
//! - RejectedConnectionsAreCountedInInfoStats（rejected_connections 行）
//! - MaxClientsSetAtRuntimeTakesEffectOnTheAcceptPath（CONFIG SET 写穿）
//! - TheServerRecoversWhenConnectionsDrainAndTheCounterDoesNotUnwind
//!
//! 注册表为进程级单例（CONFIG SET maxclients 调停经 global 直达），本文件
//! 收敛在单测试函数内顺序断言，杜绝跨用例单例互扰（client_info_laddr_tests
//! 同款约束）。

use std::{io, net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWrite, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
  time::sleep,
};
use tempfile::tempdir;
use wconf::{
  RuntimeServerConfig, RuntimeServerOptions, ServerConfigType,
  node_options::DEFAULT_NETWORK_CONNECTION_LIMIT,
};
use wnode::{
  GarnetServer, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  servers::consumer_registry::ConsumerRegistry,
  service::StorageSessionProvider,
  traits::SessionProviderFace,
};
use wnode_test::read_reply;
use wtest_base::test_store_config;

/// C# GarnetServerTcp.cs:MaxClientsReachedError 逐字节对齐 Redis 线上文本
const MAX_CLIENTS_ERR: &[u8] = b"-ERR max number of clients reached\r\n";

/// 写出命令载荷（RESP 数组帧）后即 flush
async fn send_cmd(stream: &mut TcpStream, args: &[&[u8]]) -> io::Result<()> {
  let BufResult(res, _) = stream.write_all(wtest_base::resp_frame(args)).await;
  res?;
  stream.flush().await
}

/// 起单 worker 真存储服务器（初始 maxclients 由 `limit` 装配；factory 泛型
/// 由调用方闭包具名传导）
fn spawn_server<F>(
  dir: &tempfile::TempDir,
  limit: i64,
  factory: F,
) -> (
  SocketAddr,
  Arc<ConsumerRegistry>,
  GarnetServer<StorageSessionProvider<F>>,
)
where
  F: Fn(u64, StoreGarnetApi<wdev::SegmentedDevice>) -> Option<RespSessionConsumer>
    + Send
    + Sync
    + 'static,
{
  let provider = Arc::new(
    StorageSessionProvider::open_with_config(
      test_store_config(),
      dir.path().join("conn-limit.db"),
      factory,
    )
    .unwrap(),
  );
  let registry = SessionProviderFace::consumer_registry(provider.as_ref()).unwrap();
  let server = GarnetServer::new(&["127.0.0.1:0".to_string()], 1 << 16, provider)
    .unwrap()
    .with_network_connection_limit(limit);
  server.start(NonZeroUsize::new(1)).unwrap();
  let addr = server.local_addr().unwrap();
  (addr, registry, server)
}

/// 读到对端关闭为止的全部残余字节（拒绝帧后 FIN）
async fn read_to_end(stream: &mut TcpStream, acc: &mut Vec<u8>) -> io::Result<usize> {
  loop {
    let BufResult(res, ret) = stream.read(vec![0u8; 256]).await;
    match res {
      Ok(0) => return Ok(acc.len()),
      Ok(n) => acc.extend_from_slice(&ret[..n]),
      Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(acc.len()),
      Err(e) => return Err(e),
    }
  }
}

/// 全链路：拒绝告知 → 拒绝计数 → CONFIG SET maxclients 写穿 → 排空恢复
///（计数不棘轮）
#[test]
fn connection_limit_admission_end_to_end() {
  let dir = tempdir().unwrap();
  let (addr, registry, server) = spawn_server(&dir, 2, |sender_id, api| {
    Some(RespSessionConsumer::new(
      sender_id,
      RespServerSessionOptions::default(),
      Arc::new(api),
    ))
  });
  let rt = Runtime::new().unwrap();
  rt.block_on(async move {
    // ── 探针 + 填充至 2：第三条明文连接收到拒绝帧后对端关闭
    let mut probe = TcpStream::connect(addr).await.unwrap();
    send_cmd(&mut probe, &[b"PING"]).await.unwrap();
    assert_eq!(read_reply(&mut probe).await, b"+PONG\r\n");

    let mut filler = TcpStream::connect(addr).await.unwrap();
    send_cmd(&mut filler, &[b"PING"]).await.unwrap();
    assert_eq!(read_reply(&mut filler).await, b"+PONG\r\n");
    assert_eq!(registry.connection_limit(), 2);

    let mut rejected = TcpStream::connect(addr).await.unwrap();
    let mut acc = Vec::new();
    let n = read_to_end(&mut rejected, &mut acc).await.unwrap();
    assert!(n >= MAX_CLIENTS_ERR.len(), "拒绝连接须收到错误帧后关闭");
    assert_eq!(&acc[..MAX_CLIENTS_ERR.len()], MAX_CLIENTS_ERR);

    // 拒绝计数：仅容量门分支计（TheErrorArrivesWithoutTheClientSendingAnything
    // 同款：客户端不送任何字节也应收到帧——上面拒绝连接即未送字节）
    assert_eq!(registry.total_connections_rejected(), 1);

    // ── CONFIG SET maxclients 4 → 立即再放行（MaxClientsSetAtRuntime-
    // TakesEffectOnTheAcceptPath：写穿 accept 容量门，无重启）
    send_cmd(&mut probe, &[b"CONFIG", b"SET", b"maxclients", b"4"])
      .await
      .unwrap();
    assert_eq!(read_reply(&mut probe).await, b"+OK\r\n");
    assert_eq!(registry.connection_limit(), 4);

    let mut third = TcpStream::connect(addr).await.unwrap();
    send_cmd(&mut third, &[b"PING"]).await.unwrap();
    assert_eq!(read_reply(&mut third).await, b"+PONG\r\n");
    assert_eq!(registry.total_connections_rejected(), 1, "放行不增拒绝计数");

    // CONFIG GET maxclients 回显运行时值
    send_cmd(&mut probe, &[b"CONFIG", b"GET", b"maxclients"])
      .await
      .unwrap();
    let reply = read_reply(&mut probe).await;
    let text = String::from_utf8(reply).unwrap();
    assert!(
      text.contains("maxclients") && text.contains("4"),
      "CONFIG GET maxclients 须回显 4：{text}"
    );

    // ── 调低到 2：既有连接不断（Lowering disconnects nobody），新连接被拒
    send_cmd(&mut probe, &[b"CONFIG", b"SET", b"maxclients", b"2"])
      .await
      .unwrap();
    assert_eq!(read_reply(&mut probe).await, b"+OK\r\n");
    send_cmd(&mut probe, &[b"PING"]).await.unwrap();
    assert_eq!(read_reply(&mut probe).await, b"+PONG\r\n");
    let mut refused = TcpStream::connect(addr).await.unwrap();
    let mut acc = Vec::new();
    read_to_end(&mut refused, &mut acc).await.unwrap();
    assert_eq!(&acc[..MAX_CLIENTS_ERR.len()], MAX_CLIENTS_ERR);
    assert_eq!(registry.total_connections_rejected(), 2);

    // ── CONFIG SET maxclients -1 → 不限（MaxClientsCanBeSetToUnlimited）
    send_cmd(&mut probe, &[b"CONFIG", b"SET", b"maxclients", b"-1"])
      .await
      .unwrap();
    assert_eq!(read_reply(&mut probe).await, b"+OK\r\n");
    assert_eq!(registry.connection_limit(), -1);
    for _ in 0..3 {
      let mut extra = TcpStream::connect(addr).await.unwrap();
      send_cmd(&mut extra, &[b"PING"]).await.unwrap();
      assert_eq!(read_reply(&mut extra).await, b"+PONG\r\n");
    }
    assert_eq!(registry.total_connections_rejected(), 2);

    // ── 排空恢复（TheServerRecoversWhenConnectionsDrain...）：拒绝不棘轮，
    // 计数器随连接释放回落，调回有限上限后可再进
    send_cmd(&mut probe, &[b"CONFIG", b"SET", b"maxclients", b"3"])
      .await
      .unwrap();
    assert_eq!(read_reply(&mut probe).await, b"+OK\r\n");
    drop(third);
    drop(refused);
    // 在途计数归零等待：轮询至 active ≤ 3（third 已断）
    for _ in 0..50 {
      if registry.active_handler_count() <= 3 {
        break;
      }
      sleep(Duration::from_millis(20)).await;
    }
    let mut reentry = TcpStream::connect(addr).await.unwrap();
    send_cmd(&mut reentry, &[b"PING"]).await.unwrap();
    assert_eq!(read_reply(&mut reentry).await, b"+PONG\r\n");
  });

  server.stop();
}

/// 启动上限播种：MAXCLIENTS 槽位自 RuntimeServerOptions 播种默认值
///（RuntimeServerConfig.cs:267 `values[MAXCLIENTS] = o.NetworkConnectionLimit`；
/// 默认值链 wconf DEFAULT_NETWORK_CONNECTION_LIMIT = 10000 对齐 Redis）
#[test]
fn maxclients_seeded_from_startup_options() {
  let config = RuntimeServerConfig::new(RuntimeServerOptions::default());
  assert_eq!(
    config.get_int(ServerConfigType::MaxClients),
    DEFAULT_NETWORK_CONNECTION_LIMIT
  );
  assert_eq!(config.get_int(ServerConfigType::MaxClients), 10000);
}
