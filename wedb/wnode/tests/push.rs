#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 订阅连接推送投递端到端测试（真服务端 + 真 broker + 明文 TCP 客户端）
//!
//! 在 garnet 中的相对路径： test/standalone/Garnet.test/RespPubSubTests.cs:BasicSUBSCRIBE
//!
//! 明文路径不回归：无 TLS 装配服务器上的明文空闲订阅连接仍即时收到推送。

use std::{mem::forget, net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

use aok::OK;
use compio::{
  BufResult,
  io::{AsyncRead, AsyncWrite, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
  time::timeout,
};
use wnode::{GarnetServer, service::StorageSessionProvider};
use wnode_test::{SessionFactory, session_factory};
use wtest_base::{resp_frame_str, test_store_config};

/// 推送到达等待上限：超时即失败
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// SUBSCRIBE foo 确认帧（RESP2/RESP3 同为数组 ack，C# NetworkSUBSCRIBE 句式）
const SUB_ACK: &[u8] = b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n";
/// RESP2 频道消息推送帧（C# Publish：*3 message 通道 负载）
const PUSH_RESP2: &[u8] = b"*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
/// PUBLISH 应答：接收者计数 1
const PUBLISHED_ONE: &[u8] = b":1\r\n";

/// 起默认装配服务器（真 broker、真 RESP 会话）
fn spawn_server() -> (GarnetServer<StorageSessionProvider<SessionFactory>>, String) {
  let dir = tempfile::tempdir().unwrap();
  let data_path = dir.path().join("plaintext-push.db");
  let factory: SessionFactory = session_factory;
  let provider =
    StorageSessionProvider::open_with_config(test_store_config(), data_path, factory).unwrap();
  assert!(provider.pubsub.is_some(), "默认装配必须启用发布订阅中枢");

  let server = GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, Arc::new(provider)).unwrap();

  server.start(NonZeroUsize::new(1)).unwrap();
  let addr = server.local_addr().unwrap().to_string();

  // dir 随句柄存活至测试结束（服务器全生命周期数据文件在场）
  forget(dir);
  (server, addr)
}

/// RESP 帧级客户端
struct Client<S: AsyncRead + AsyncWrite> {
  stream: S,
  acc: Vec<u8>,
}

/// 明文 TCP 接入
async fn connect_tcp(addr: &str) -> Client<TcpStream> {
  let addr: SocketAddr = addr.parse().unwrap();
  Client {
    stream: TcpStream::connect(addr).await.unwrap(),
    acc: Vec::new(),
  }
}

impl<S: AsyncRead + AsyncWrite> Client<S> {
  async fn send(&mut self, frame: &[u8]) {
    let BufResult(res, buf) = self.stream.write_all(frame.to_vec()).await;
    res.expect("帧写出失败");
    drop(buf);
    self.stream.flush().await.expect("帧刷出失败");
  }

  async fn fill(&mut self) {
    let BufResult(res, buf) = self.stream.read(vec![0u8; 4096]).await;
    let n = res.expect("连接读失败");
    assert!(n > 0, "对端提前关闭（已收 {} 字节）", self.acc.len());
    self.acc.extend_from_slice(&buf[..n]);
  }

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

/// 明文路径不回归：无 TLS 装配服务器上的明文空闲订阅连接仍即时收到推送
#[test]
fn plaintext_idle_subscriber_receives_publish_push() -> aok::Void {
  let (server, addr) = spawn_server();
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
