//! 会话内 panic 隔离回归（panic-abort-policy 裁决验收）
//!
//! 对齐 C# RespServerSession.TryConsumeMessages 最外层 catch(Exception) →
//! Dispose 语义：单会话消费路径 panic 只断本连接，服务器进程、accept 泵与
//! 其他连接全部存活。兜底点在 [`wnode`] 连接泵 process_stream 的
//! catch_unwind（net/handler/drive.rs），前提是 release profile 撤
//! panic="abort"（wedb/Cargo.toml，unwind 形态下 executor 与本兜底才生效）。

use std::{
  mem::take,
  net::SocketAddr,
  num::NonZeroUsize,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
  time::sleep,
};
use wnode::{GarnetServer, MessageConsumerFace, SessionProviderFace, WireFormat};

/// 起服务器并返回地址（参数与 net_pump_consume_tests 同形态）
fn spawn_server<P: SessionProviderFace + 'static>(
  provider: Arc<P>,
) -> (Arc<GarnetServer<P>>, String) {
  let server = Arc::new(
    GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, Arc::clone(&provider)).unwrap(),
  );
  server.start(NonZeroUsize::new(1)).unwrap();
  let addr = server.local_addr().unwrap().to_string();
  (server, addr)
}

/// PING 正常应答，BOOM 帧触发注入 panic（模拟会话内可达 panic）
struct PanicOnBoomConsumer {
  buf: Vec<u8>,
  head: usize,
  disposed: Arc<AtomicBool>,
}

impl MessageConsumerFace for PanicOnBoomConsumer {
  fn try_consume_messages_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
    loop {
      let rest = &self.buf[self.head..];
      if rest.starts_with(b"PING\r\n") {
        self.head += 6;
        resp_buf.extend_from_slice(b"+PONG\r\n");
      } else if rest.starts_with(b"BOOM\r\n") {
        panic!("定向测试注入: 会话内 panic");
      } else if b"PING\r\n".starts_with(rest) || b"BOOM\r\n".starts_with(rest) {
        break; // 半包待续
      } else {
        return None;
      }
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

  fn dispose(&mut self) {
    self.disposed.store(true, Ordering::Release);
  }
}

#[derive(Clone)]
struct PanicProvider {
  disposed: Arc<AtomicBool>,
}

impl SessionProviderFace for PanicProvider {
  type Consumer = PanicOnBoomConsumer;
  fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<PanicOnBoomConsumer> {
    Some(PanicOnBoomConsumer {
      buf: Vec::new(),
      head: 0,
      disposed: Arc::clone(&self.disposed),
    })
  }
}

/// 写出帧并读一条行式应答（+PONG）
async fn ping_pong(stream: &mut TcpStream) {
  let BufResult(res, _) = stream.write_all(b"PING\r\n".to_vec()).await;
  res.unwrap();
  let mut acc = Vec::new();
  loop {
    let BufResult(res, ret) = stream.read(vec![0u8; 64]).await;
    let (n, buf) = (res.unwrap(), ret);
    assert!(n > 0, "连接提前断开");
    acc.extend_from_slice(&buf[..n]);
    if acc.ends_with(b"\r\n") {
      break;
    }
  }
  assert_eq!(acc, b"+PONG\r\n");
}

/// 读到对端 EOF：终止读必为 Ok(0)（FIN 送达），途中不得有应答字节泄漏
async fn read_until_fin(stream: &mut TcpStream) {
  let mut total = 0usize;
  loop {
    let BufResult(res, ret) = stream.read(vec![0u8; 256]).await;
    match res {
      Ok(0) => break,
      Ok(n) => {
        total += n;
        let _ = ret;
      }
      Err(e) => panic!("连接未以 FIN 收场（已收 {total} 字节）: {e}"),
    }
  }
  assert_eq!(total, 0, "panic 会话不得有应答字节泄漏");
}

/// 注入 panic 的会话被隔离断连，既有连接、新连接与 accept 泵全部存活
#[test]
fn session_panic_does_not_kill_process() {
  let provider = Arc::new(PanicProvider {
    disposed: Arc::new(AtomicBool::new(false)),
  });
  let (server, addr) = spawn_server(provider);
  let addr = addr.parse::<SocketAddr>().unwrap();

  Runtime::new().unwrap().block_on(async {
    // 既有连接 B 建立并确认存活基线
    let mut b = TcpStream::connect(addr).await.unwrap();
    ping_pong(&mut b).await;

    // 连接 A 注入 panic：以 FIN 收场且无字节泄漏
    let mut a = TcpStream::connect(addr).await.unwrap();
    let BufResult(res, _) = a.write_all(b"BOOM\r\n".to_vec()).await;
    res.unwrap();
    read_until_fin(&mut a).await;

    // 既有连接 B 仍然可用（泵线程未死）
    ping_pong(&mut b).await;

    // 新连接 C 可正常建连应答（accept 循环未死）
    let mut c = TcpStream::connect(addr).await.unwrap();
    ping_pong(&mut c).await;
  });

  // 服务器可正常收场（进程与停机路径完整）
  server.stop();
}

/// panic 会话的收场尾巴必走 dispose（资源注销与断连语义，C# Dispose 对位）
#[test]
fn session_panic_runs_dispose() {
  let disposed = Arc::new(AtomicBool::new(false));
  let provider = Arc::new(PanicProvider {
    disposed: Arc::clone(&disposed),
  });
  let (server, addr) = spawn_server(provider);
  let addr = addr.parse::<SocketAddr>().unwrap();

  Runtime::new().unwrap().block_on(async {
    let mut a = TcpStream::connect(addr).await.unwrap();
    let BufResult(res, _) = a.write_all(b"BOOM\r\n".to_vec()).await;
    res.unwrap();
    read_until_fin(&mut a).await;

    // FIN 先于 dispose（收场序 shutdown → dispose），轮询等标志翻转
    let mut polls = 0usize;
    while !disposed.load(Ordering::Acquire) {
      assert!(polls < 400, "panic 会话未走 dispose 收尾");
      polls += 1;
      sleep(Duration::from_millis(5)).await;
    }
  });

  server.stop();
}
