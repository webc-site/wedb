//! 阻塞等待断连盲区端到端集成测试（真网络泵 + CollectionItemBroker 默认装配）
//!
//! 对标 C# 内核异步事件守护连接生命周期（TcpNetworkHandlerBase.cs:214
//! `bytesTransferred == 0 || SocketError != Success` → Dispose →
//! RespServerSession.Dispose:408 `itemBroker?.HandleSessionDisposed`）：
//! 1. BLPOP 挂起期客户端断连 → 服务端即时感知，观察者注销、会话回收，
//!    随后 LPUSH 的元素不被误弹出丢弃（僵尸连接 / 数据丢失回归防线）；
//! 2. BLPOP 挂起期客户端违规流水线新命令 → 探测字节保全，阻塞结束后续消费。

use std::{mem::forget, net::SocketAddr, num::NonZeroUsize, sync::Arc, thread, time::Duration};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
  time::{sleep, timeout},
};
use wnode::{GarnetServer, service::StorageSessionProvider};
use wnode_test::{SessionFactory, cmd, session_factory};
use wtest_base::{resp_frame_str, test_store_config};

/// 起默认装配服务器（经纪随 StorageSessionProvider 默认在场）并返回（服务器，地址）
fn spawn_default_server() -> (GarnetServer<StorageSessionProvider<SessionFactory>>, String) {
  let dir = tempfile::tempdir().unwrap();
  let data_path = dir.path().join("blocking_disconnect.db");
  let factory: SessionFactory = session_factory;
  let provider =
    StorageSessionProvider::open_with_config(test_store_config(), data_path, factory).unwrap();
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
    timeout_read(&mut self.stream, &mut self.acc, expect).await
  }
}

/// 读满期望字节的独立函数（复用 acc 累积缓冲，限时 10s）
async fn timeout_read(stream: &mut TcpStream, acc: &mut Vec<u8>, expect: usize) -> Vec<u8> {
  timeout(Duration::from_secs(10), async {
    while acc.len() < expect {
      let BufResult(res, buf) = stream.read(vec![0u8; 4096]).await;
      let n = res.unwrap();
      assert!(n > 0, "对端提前关闭（已收 {} 字节）", acc.len());
      acc.extend_from_slice(&buf[..n]);
    }
  })
  .await
  .expect("读应答超时");
  acc.drain(..expect).collect()
}

/// 断连后的观察者注销收口：CLIENT LIST 轮询收敛（EOF → 泵 blocked.abort()
/// → broker SessionDisposed 注销，断连方 `addr=<本端临时端口> ` 条目从注册表
/// 消失即收口。观察位取对端临时端口而非 id：CLIENT 注册表进程级单例，id 在
/// 同进程多服务器下跨服务器复用，端口则进程内唯一；`addr=` 恒有 ` laddr=`
/// 跟随，尾随空格锚定无前缀误配。50×10ms 有界轮询防挂死，先例 57732087
/// CLIENT LIST 断言轮询同型）
async fn wait_disconnect_settled(stream: &mut TcpStream, gone: &str) -> bool {
  for _ in 0..50 {
    if !String::from_utf8_lossy(&cmd(stream, &[b"CLIENT", b"LIST"]).await).contains(gone) {
      return true;
    }
    sleep(Duration::from_millis(10)).await;
  }
  false
}

/// BLPOP 挂起期客户端断连：服务端即时感知回收，后续 LPUSH 元素不被误弹出
/// 丢弃（僵尸观察者抢件即数据丢失的回归防线）
#[test]
fn blpop_disconnect_reclaims_session_and_keeps_item() {
  let (server, addr) = spawn_default_server();
  thread::spawn(move || {
    Runtime::new().unwrap().block_on(async move {
      // 客户端 A：CLIENT ID 握手（回执到站即首帧已处理，注册表条目确定
      // 在场——注销轮询的非空前提），取本端临时端口作注销观察位；
      // BLPOP k 0（无限阻塞挂起）后立即断连（FIN）
      let mut a = Client::connect(&addr).await;
      cmd(&mut a.stream, &[b"CLIENT", b"ID"]).await;
      let a_port = a.stream.local_addr().unwrap().port();
      a.send(&resp_frame_str(&["BLPOP", "k", "0"])).await;
      drop(a);

      // 客户端 B：先以 CLIENT LIST 轮询 A 的注销收口（EOF → 观察者注销 →
      // 会话回收），再 LPUSH 新元素——断连客户端的观察者若滞留经纪，
      // 此元素会被误弹出并因写回 BrokenPipe 丢失
      let mut b = Client::connect(&addr).await;
      let gone = format!("addr=127.0.0.1:{a_port} ");
      assert!(
        wait_disconnect_settled(&mut b.stream, &gone).await,
        "断连会话条目应已从注册表注销（{gone}）"
      );
      b.send(&resp_frame_str(&["LPUSH", "k", "v1"])).await;
      assert_eq!(b.expect(4).await, b":1\r\n");

      // 元素完整保留在集合中（未被死连接抢走）
      b.send(&resp_frame_str(&["LLEN", "k"])).await;
      assert_eq!(b.expect(4).await, b":1\r\n");
      b.send(&resp_frame_str(&["LPOP", "k"])).await;
      assert_eq!(b.expect(8).await, b"$2\r\nv1\r\n");

      // 服务端健康（A 的僵尸连接未拖垮 accept 泵）
      b.send(&resp_frame_str(&["PING"])).await;
      assert_eq!(b.expect(7).await, b"+PONG\r\n");
      drop(b);
      server.dispose();
    });
  })
  .join()
  .unwrap();
}

/// BLPOP 挂起期客户端违规流水线新命令：探测读保全字节，阻塞超时后
/// 按流水线序续消费（C# 阻塞期字节滞留内核缓冲的同语义闭环）
#[test]
fn blpop_pipeline_bytes_during_wait_are_consumed_after_resolve() {
  let (server, addr) = spawn_default_server();
  thread::spawn(move || {
    Runtime::new().unwrap().block_on(async move {
      // 一次流水线发送：BLPOP k 1（1 秒超时挂起）+ PING（阻塞期到站）
      let mut d = Client::connect(&addr).await;
      d.send(&resp_frame_str(&["BLPOP", "k", "1"])).await;
      d.send(&resp_frame_str(&["PING"])).await;

      // 超时空回（RESP2 nil 数组）与 PING 应答按序同连接返回
      let reply = d.expect(12).await;
      assert_eq!(reply, b"*-1\r\n+PONG\r\n");

      // 会话状态健康：集合操作正常
      d.send(&resp_frame_str(&["LPUSH", "k", "v2"])).await;
      assert_eq!(d.expect(4).await, b":1\r\n");
      drop(d);
      server.dispose();
    });
  })
  .join()
  .unwrap();
}
