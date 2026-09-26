//! 发布订阅生产装配端到端集成测试（StorageSessionProvider + 真网络泵）
//!
//! 对标 garnet/test/standalone/Garnet.test/RespPubSubTests.cs 的双客户端
//! 场景：默认装配下（无任何额外开关）SUBSCRIBE / PSUBSCRIBE / PUBLISH
//! 全链路可用——broker 挂载、会话接线、空闲订阅连接的双路等待推送投递
//!（C# 广播线程直写订阅会话网络发送器的 rust 等价物）逐项验证。

use std::{mem::forget, net::SocketAddr, num::NonZeroUsize, sync::Arc, thread, time::Duration};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
  time::{sleep, timeout},
};
use wnode::{GarnetServer, service::StorageSessionProvider};
use wnode_test::{SessionFactory, session_factory};
use wtest_base::test_store_config;

/// 起默认装配服务器（pubsub 默认启用，无任何额外开关）并返回（服务器，地址）
fn spawn_default_server() -> (GarnetServer<StorageSessionProvider<SessionFactory>>, String) {
  let dir = tempfile::tempdir().unwrap();
  let data_path = dir.path().join("pubsub.db");
  let factory: SessionFactory = session_factory;
  let provider =
    StorageSessionProvider::open_with_config(test_store_config(), data_path, factory).unwrap();
  // 默认装配断言：pubsub 中枢在场（C# DisablePubSub = false 默认启用）
  assert!(provider.pubsub.is_some(), "默认装配必须启用发布订阅中枢");
  let server =
    GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, Arc::new(provider)).unwrap();
  server.start(NonZeroUsize::new(1)).unwrap();
  let addr = server.local_addr().unwrap().to_string();
  // dir 随句柄存活至测试结束（服务器全生命周期数据文件在场）
  forget(dir);
  (server, addr)
}

/// 客户端：发帧 / 读满期望字节数（限时，容忍 TCP 分段）
struct Client {
  stream: TcpStream,
  acc: Vec<u8>,
}

impl Client {
  async fn connect(addr: &str) -> Self {
    let stream = TcpStream::connect(addr.parse::<SocketAddr>().unwrap())
      .await
      .unwrap();
    Self {
      stream,
      acc: Vec::new(),
    }
  }

  async fn send(&mut self, frame: &[u8]) {
    let BufResult(res, buf) = self.stream.write_all(frame.to_vec()).await;
    res.unwrap();
    drop(buf);
  }

  /// 读到恰好 expect 字节（超时即失败，杜绝挂死）
  async fn expect(&mut self, expect: usize) -> Vec<u8> {
    timeout(Duration::from_secs(10), async {
      while self.acc.len() < expect {
        let BufResult(res, buf) = self.stream.read(vec![0u8; 4096]).await;
        let n = res.unwrap();
        assert!(n > 0, "对端提前关闭（已收 {} 字节）", self.acc.len());
        self.acc.extend_from_slice(&buf[..n]);
      }
    })
    .await
    .expect("读应答超时");
    self.acc.drain(..expect).collect()
  }
}

/// 装配级跨连接投递验证（C# RespPubSubTests.cs:BasicSUBSCRIBE 的场景由
/// wedb_standalone/tests/resp_pubsub.rs 映射；此处验证 broker 装配 + 双路等待驱动的真 TCP 投递）
#[test]
fn subscribe_publish_cross_connection_delivery() {
  let (server, addr) = spawn_default_server();
  thread::spawn(move || {
    Runtime::new().unwrap().block_on(async move {
      // 订阅端：SUBSCRIBE foo → 确认帧（逐字节对齐 C#：*3 "subscribe" 通道 活跃数）
      const ACK: &[u8] = b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n";
      let mut sub = Client::connect(&addr).await;
      sub.send(b"*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nfoo\r\n").await;
      assert_eq!(sub.expect(ACK.len()).await, ACK);

      // 发布端：PUBLISH foo bar → 应答接收者计数 1
      let mut pubr = Client::connect(&addr).await;
      pubr
        .send(b"*3\r\n$7\r\nPUBLISH\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
        .await;
      assert_eq!(pubr.expect(4).await, b":1\r\n");

      // 订阅端空闲（无任何后续输入）：双路等待唤醒直写推送帧
      //（逐字节对齐 C# Publish：*3 "message" 通道 负载）
      const PUSH: &[u8] = b"*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
      assert_eq!(sub.expect(PUSH.len()).await, PUSH);
    })
  })
  .join()
  .unwrap();
  server.dispose();
}

/// 装配级模式订阅投递验证（C# RespPubSubTests.cs:BasicPSUBSCRIBE 的场景由
/// wedb_standalone/tests/resp_pubsub.rs 映射；此处验证 broker 装配 + 双路等待驱动的真 TCP 投递）
#[test]
fn psubscribe_pattern_publish_delivery() {
  let (server, addr) = spawn_default_server();
  thread::spawn(move || {
    Runtime::new().unwrap().block_on(async move {
      const ACK: &[u8] = b"*3\r\n$10\r\npsubscribe\r\n$6\r\nnews.*\r\n:1\r\n";
      let mut sub = Client::connect(&addr).await;
      sub
        .send(b"*2\r\n$10\r\nPSUBSCRIBE\r\n$6\r\nnews.*\r\n")
        .await;
      assert_eq!(sub.expect(ACK.len()).await, ACK);

      let mut pubr = Client::connect(&addr).await;
      // 命中模式：news.tech
      pubr
        .send(b"*3\r\n$7\r\nPUBLISH\r\n$9\r\nnews.tech\r\n$2\r\nhi\r\n")
        .await;
      assert_eq!(pubr.expect(4).await, b":1\r\n");
      //（逐字节对齐 C# PatternPublish：*4 "pmessage" 模式 通道 负载）
      const PUSH: &[u8] =
        b"*4\r\n$8\r\npmessage\r\n$6\r\nnews.*\r\n$9\r\nnews.tech\r\n$2\r\nhi\r\n";
      assert_eq!(sub.expect(PUSH.len()).await, PUSH);
    })
  })
  .join()
  .unwrap();
  server.dispose();
}

/// 未订阅通道 PUBLISH 返回 0（C# PublishNow 空订阅者计数口径）
#[test]
fn publish_without_subscriber_returns_zero() {
  let (server, addr) = spawn_default_server();
  thread::spawn(move || {
    Runtime::new().unwrap().block_on(async move {
      let mut pubr = Client::connect(&addr).await;
      pubr
        .send(b"*3\r\n$7\r\nPUBLISH\r\n$4\r\nvoid\r\n$1\r\nx\r\n")
        .await;
      assert_eq!(pubr.expect(4).await, b":0\r\n");
    })
  })
  .join()
  .unwrap();
  server.dispose();
}

/// 订阅端断连后订阅摘除（C# Dispose → RemoveSubscription）：
/// 后续 PUBLISH 不再计入幽灵订阅者
#[test]
fn subscriber_disconnect_removes_subscription() {
  let (server, addr) = spawn_default_server();
  thread::spawn(move || {
    Runtime::new().unwrap().block_on(async move {
      const ACK: &[u8] = b"*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n";
      let mut sub = Client::connect(&addr).await;
      sub.send(b"*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nfoo\r\n").await;
      assert_eq!(sub.expect(ACK.len()).await, ACK);
      // 订阅端断连（drop stream 触发读 0 → 会话 dispose → 摘订阅）
      drop(sub);
      // 有界轮询至服务端完成断连感知与订阅摘除（dispose 异步链，5s 上界）：
      // 订阅在册时 PUBLISH 回 :1，摘除生效即回 :0
      let mut pubr = Client::connect(&addr).await;
      let mut removed = false;
      for _ in 0..1000 {
        pubr
          .send(b"*3\r\n$7\r\nPUBLISH\r\n$3\r\nfoo\r\n$1\r\nx\r\n")
          .await;
        if pubr.expect(4).await == b":0\r\n" {
          removed = true;
          break;
        }
        sleep(Duration::from_millis(5)).await;
      }
      assert!(removed, "断连后订阅摘除未在时限内生效");
    })
  })
  .join()
  .unwrap();
  server.dispose();
}
