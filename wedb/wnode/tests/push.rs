#![cfg(feature = "tls")]

//! TLS 订阅连接推送投递端到端测试（真服务端 + 真 broker + TLS 客户端）
//!
//! 在 garnet 中的相对路径： test/standalone/Garnet.test/RespTlsTests.cs
//!
//! 对标 test/standalone/Garnet.test/RespPubSubTests.cs:BasicSUBSCRIBE 的 TLS
//! 面：C# 侧推送写出对 TLS 会话无任何特判——SubscribeBroker.Broadcast
//! （libs/server/PubSub/SubscribeBroker.cs:76-116）在 broker 线程直调
//! session.Publish（libs/server/Resp/PubSubCommands.cs:21-55），TLS 会话的网络
//! 发送器即 NetworkHandler 自身
//!（libs/common/Networking/NetworkHandler.cs:360 GetNetworkSender），写出为
//! sslStream.Write + sslStream.Flush（:612-633 SendResponse）。故 SUBSCRIBE 后
//! 不再发任何输入帧的空闲 TLS 连接也必须即时收到推送帧——rust 侧对应
//! net/handler/drive.rs 的订阅推送双路等待 + net/stream.rs 的 TLS 读写句柄对。

use std::{mem::forget, net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

use aok::OK;
use compio::{
  BufResult,
  io::{AsyncRead, AsyncWrite, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
  time::timeout,
};
use compio_tls::{TlsConnector, TlsStream};
use wnode::{GarnetServer, service::StorageSessionProvider};
use wnode_test::{SessionFactory, session_factory};
use wnode_tls_test::{test_connector, test_server_tls};
use wtest_base::{resp_frame_str, test_store_config};

/// 推送到达等待上限：超时即失败（缺陷形态是空闲 TLS 订阅者永不收到推送）
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// SUBSCRIBE foo 确认帧（RESP2/RESP3 同为数组 ack，C# NetworkSUBSCRIBE 句式）
const SUB_ACK: &[u8] = b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n";
/// RESP2 频道消息推送帧（C# Publish：*3 message 通道 负载）
const PUSH_RESP2: &[u8] = b"*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
/// RESP3 频道消息推送帧（push 头 `>`，见 wresp PUBSUB_PUSH_MSG_PREFIX_RESP3）
const PUSH_RESP3: &[u8] = b">3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
/// PUBLISH 应答：接收者计数 1
const PUBLISHED_ONE: &[u8] = b":1\r\n";

/// 起默认装配服务器（真 broker、真 RESP 会话），可选 TLS 面
///
/// TLS 装配下 wnode 的 accept 循环对每条入站连接做握手（对标 C#
/// GarnetServerTcp.cs:254 new ServerTcpNetworkHandler(..., tlsOptions != null)
/// 与 :290 handler.Start(tlsOptions?.TlsServerOptions)，握手阻塞在
/// NetworkHandler.cs:147 Start），故明文用例必须跑在无 TLS 装配的服务器上；
/// 连接器在 TLS 装配时返回。
fn spawn_server(
  tls: bool,
) -> (
  GarnetServer<StorageSessionProvider<SessionFactory>>,
  String,
  Option<Arc<TlsConnector>>,
) {
  let dir = tempfile::tempdir().unwrap();
  let data_path = dir.path().join("tls-push.db");
  let factory: SessionFactory = session_factory;
  let provider =
    StorageSessionProvider::open_with_config(test_store_config(), data_path, factory).unwrap();
  // 默认装配断言：pubsub 中枢在场（C# DisablePubSub = false 默认启用）
  assert!(provider.pubsub.is_some(), "默认装配必须启用发布订阅中枢");

  let server =
    GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, Arc::new(provider)).unwrap();

  let mut connector = None;
  let server = if tls {
    // 自签名证书进程内单例（wnode_tls_test 夹具，服务端与客户端信任锚同一张证书）
    let tls_config = test_server_tls().unwrap();
    connector = Some(Arc::new(test_connector().unwrap()));
    server.with_tls_config(tls_config)
  } else {
    server
  };

  server.start(NonZeroUsize::new(1)).unwrap();
  let addr = server.local_addr().unwrap().to_string();

  // dir 随句柄存活至测试结束（服务器全生命周期数据文件在场）
  forget(dir);
  (server, addr, connector)
}

/// TLS 装配服务器地址与客户端信任锚
fn spawn_tls_server() -> (
  GarnetServer<StorageSessionProvider<SessionFactory>>,
  String,
  Arc<TlsConnector>,
) {
  let (server, addr, connector) = spawn_server(true);
  (server, addr, connector.expect("TLS 装配必返回连接器"))
}

/// RESP 帧级客户端（传输形态泛型：明文 TCP 与 TLS 共用同一断言骨架）
struct Client<S: AsyncRead + AsyncWrite> {
  stream: S,
  acc: Vec<u8>,
}

/// 明文 TCP 接入（无 TLS 装配的服务器）
async fn connect_tcp(addr: &str) -> Client<TcpStream> {
  let addr: SocketAddr = addr.parse().unwrap();
  Client {
    stream: TcpStream::connect(addr).await.unwrap(),
    acc: Vec::new(),
  }
}

/// TLS 接入（握手就绪即返回）
async fn connect_tls(addr: &str, connector: &TlsConnector) -> Client<TlsStream<TcpStream>> {
  let addr: SocketAddr = addr.parse().unwrap();
  let tcp = TcpStream::connect(addr).await.unwrap();
  Client {
    stream: connector.connect("localhost", tcp).await.unwrap(),
    acc: Vec::new(),
  }
}

impl<S: AsyncRead + AsyncWrite> Client<S> {
  /// 写出一帧并刷出（TLS 下 rustls 把写出攒在连接发送缓冲，flush 才落线缆；
  /// 明文 TCP 的 flush 为空操作，两形态同一路径）
  async fn send(&mut self, frame: &[u8]) {
    let BufResult(res, buf) = self.stream.write_all(frame.to_vec()).await;
    res.expect("帧写出失败");
    drop(buf);
    self.stream.flush().await.expect("帧刷出失败");
  }

  /// 追加读一批网络字节（对端提前关闭即失败）
  async fn fill(&mut self) {
    let BufResult(res, buf) = self.stream.read(vec![0u8; 4096]).await;
    let n = res.expect("连接读失败");
    assert!(n > 0, "对端提前关闭（已收 {} 字节）", self.acc.len());
    self.acc.extend_from_slice(&buf[..n]);
  }

  /// 限时读到 want 帧出现在流中为止，返回 want 之前收到的字节（前置帧）；
  /// want 自身逐字节核对后消耗
  async fn read_until_contains(&mut self, want: &[u8]) -> Vec<u8> {
    let at = timeout(READ_TIMEOUT, async {
      loop {
        if let Some(at) = self.acc.windows(want.len()).position(|w| w == want) {
          return at;
        }
        self.fill().await;
      }
    })
    .await
    .unwrap_or_else(|_| panic!("读帧超时（未收到 {:?}）", String::from_utf8_lossy(want)));
    let prefix = self.acc.drain(..at).collect();
    let got = self.acc.drain(..want.len()).collect::<Vec<u8>>();
    assert_eq!(got, want, "帧字节不符：{:?}", String::from_utf8_lossy(&got));
    prefix
  }
}

/// 订阅 → ack → 另一连接 PUBLISH → 空闲订阅端限时收到 want_push
///
/// 订阅连接在 SUBSCRIBE 之后不再写出任何字节（推送只能由服务端邮箱事件唤醒
/// 连接任务直写），返回值为首个 ack 之前收到的前置帧字节
async fn push_reaches_idle_subscriber<S, P>(
  sub: &mut Client<S>,
  pubr: &mut Client<P>,
  subscribe_batch: &[u8],
  want_push: &[u8],
) -> Vec<u8>
where
  S: AsyncRead + AsyncWrite,
  P: AsyncRead + AsyncWrite,
{
  sub.send(subscribe_batch).await;
  let before_ack = sub.read_until_contains(SUB_ACK).await;
  pubr
    .send(resp_frame_str(&["PUBLISH", "foo", "bar"]).as_slice())
    .await;
  assert!(
    pubr.read_until_contains(PUBLISHED_ONE).await.is_empty(),
    "PUBLISH 应答前不得有多余字节"
  );
  assert!(
    sub.read_until_contains(want_push).await.is_empty(),
    "推送帧前不得有多余字节（订阅端此刻零输入帧）"
  );
  assert!(sub.acc.is_empty(), "推送帧后不得有多余字节");
  before_ack
}

/// TLS 空闲订阅连接即时收到 PUBLISH 推送（RESP2 帧）
///
/// 缺陷复现面：SUBSCRIBE 后该 TLS 连接不再产生输入帧，旧实现让 TLS 退出
/// 双路等待（推送随下一输入帧投递），本用例在 5s 内读不到推送帧即失败。
#[test]
fn tls_idle_subscriber_receives_publish_push_resp2() -> aok::Void {
  let (server, addr, connector) = spawn_tls_server();
  let rt = Runtime::new()?;
  let res = rt.block_on(async {
    let mut sub = connect_tls(&addr, &connector).await;
    let mut pubr = connect_tls(&addr, &connector).await;
    let before_ack = push_reaches_idle_subscriber(
      &mut sub,
      &mut pubr,
      resp_frame_str(&["SUBSCRIBE", "foo"]).as_slice(),
      PUSH_RESP2,
    )
    .await;
    assert!(before_ack.is_empty(), "RESP2 下 ack 前不应有前置帧");
    OK
  });
  server.dispose();
  res
}

/// TLS 空闲订阅连接即时收到 PUBLISH 推送（RESP3 帧，两条连接均走 TLS）
///
/// RESP3 下推送为 push 头帧（C# WritePushLength 同口径）；订阅端 HELLO 3
/// 升级应答 → SUBSCRIBE ack → 推送帧三段字节序自洽。
#[test]
fn tls_idle_subscriber_receives_publish_push_resp3() -> aok::Void {
  let (server, addr, connector) = spawn_tls_server();
  let rt = Runtime::new()?;
  let res = rt.block_on(async {
    let mut sub = connect_tls(&addr, &connector).await;
    let mut pubr = connect_tls(&addr, &connector).await;
    // HELLO 3 与 SUBSCRIBE 同批写出，此后该连接零输入帧
    let mut batch = resp_frame_str(&["HELLO", "3"]);
    batch.extend_from_slice(&resp_frame_str(&["SUBSCRIBE", "foo"]));
    let before_ack = push_reaches_idle_subscriber(&mut sub, &mut pubr, &batch, PUSH_RESP3).await;
    assert!(
      before_ack.starts_with(b"%8\r\n"),
      "HELLO 3 升级应答应为 RESP3 map 头：{:?}",
      String::from_utf8_lossy(&before_ack)
    );
    OK
  });
  server.dispose();
  res
}

/// 明文路径不回归：无 TLS 装配服务器上的明文空闲订阅连接仍即时收到推送
///
/// TLS 接入双路等待后与明文共用同一条投递路径，本用例守住明文面基线
///（对照 wnode/tests/pubsub_assembly_tests.rs 的无 TLS 装配场景）。
#[test]
fn plaintext_idle_subscriber_receives_publish_push() -> aok::Void {
  let (server, addr, _) = spawn_server(false);
  let rt = Runtime::new()?;
  let res = rt.block_on(async {
    let mut sub = connect_tcp(&addr).await;
    let mut pubr = connect_tcp(&addr).await;
    let before_ack = push_reaches_idle_subscriber(
      &mut sub,
      &mut pubr,
      resp_frame_str(&["SUBSCRIBE", "foo"]).as_slice(),
      PUSH_RESP2,
    )
    .await;
    assert!(before_ack.is_empty(), "RESP2 下 ack 前不应有前置帧");
    OK
  });
  server.dispose();
  res
}
