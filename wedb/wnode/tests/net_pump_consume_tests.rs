//! 网络泵交错路径回归（NetworkHandler drive_loop 状态机）
//!
//! 覆盖消费泵与消费者的交错行为基线：半包重组、批内流水线、跨批次
//! 游标持久（半包尾随）、大批量冲洗扩容。真 socket 端到端驱动
//!（GarnetServer + compio 客户端），回退形态（拷贝消费）与直读形态
//!（scratch 消费）共用同一组用例，泵重构前后行为字节级等价为验收线。

use std::{mem::take, net::SocketAddr, num::NonZeroUsize, sync::Arc, thread, time::Duration};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
};
use wnode::{GarnetServer, MessageConsumerFace, SessionProviderFace, WireFormat};

/// 严格帧消费桩：仅认 `PING\r\n` 前缀（半包必须等齐，不得误吞/重解析）
struct LineConsumer;
impl MessageConsumerFace for LineConsumer {
  fn try_consume_messages_into(&mut self, req_buffer: &[u8], resp_buf: &mut Vec<u8>) -> usize {
    if req_buffer.starts_with(b"PING\r\n") {
      resp_buf.extend_from_slice(b"+PONG\r\n");
      6
    } else {
      0
    }
  }
  fn dispose(&mut self) {}
}

struct LineProvider;
impl SessionProviderFace for LineProvider {
  type Consumer = LineConsumer;
  fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<LineConsumer> {
    Some(LineConsumer)
  }
}

/// 起服务器并返回地址（缓冲 4096 放大半包/扩容路径触发概率）
fn spawn_server<P: SessionProviderFace + 'static>(provider: Arc<P>) -> (GarnetServer<P>, String) {
  let server = GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, Arc::clone(&provider));
  server.start(NonZeroUsize::new(1)).unwrap();
  let addr = server.local_addr().unwrap().to_string();
  (server, addr)
}

/// 期望字节数读完（累计，容忍 TCP 分段）
async fn read_until(stream: &mut TcpStream, acc: &mut Vec<u8>, expect: usize) {
  while acc.len() < expect {
    let BufResult(res, ret) = stream.read(vec![0u8; 4096]).await;
    let n = res.unwrap();
    assert!(n > 0, "对端提前关闭（已收 {} 字节）", acc.len());
    acc.extend_from_slice(&ret[..n]);
  }
}

/// 客户端分批写帧并核对总应答（批次间隔制造真实分批读）
async fn drive(addr: &str, batches: &[&[u8]], expect: &[u8]) {
  let mut stream = TcpStream::connect(addr.parse::<SocketAddr>().unwrap())
    .await
    .unwrap();
  for (i, batch) in batches.iter().enumerate() {
    if i > 0 {
      thread::sleep(Duration::from_millis(60));
    }
    stream.write_all(batch.to_vec()).await.unwrap();
  }
  let mut acc = Vec::new();
  read_until(&mut stream, &mut acc, expect.len()).await;
  assert_eq!(&acc, expect, "应答字节级等价");
}

/// 半包重组：PING 拆两次到达，泵必须攒齐后才消费，应答恰好一条
#[test]
fn pump_half_packet_reassembly() {
  let (server, addr) = spawn_server(Arc::new(LineProvider));
  Runtime::new()
    .unwrap()
    .block_on(drive(&addr, &[b"PIN", b"G\r\n"], b"+PONG\r\n"));
  server.stop();
}

/// 批内流水线：单批多条命令顺序消费，应答按序拼接
#[test]
fn pump_pipeline_within_batch() {
  let (server, addr) = spawn_server(Arc::new(LineProvider));
  Runtime::new().unwrap().block_on(drive(
    &addr,
    &[b"PING\r\nPING\r\nPING\r\n"],
    b"+PONG\r\n+PONG\r\n+PONG\r\n",
  ));
  server.stop();
}

/// 跨批次游标持久：完整帧消费后残留半包尾随，补齐字节须原位续解析
#[test]
fn pump_partial_tail_across_batches() {
  let (server, addr) = spawn_server(Arc::new(LineProvider));
  Runtime::new().unwrap().block_on(drive(
    &addr,
    &[b"PING\r\nPIN", b"G\r\n"],
    b"+PONG\r\n+PONG\r\n",
  ));
  server.stop();
}

/// 三批次交错：完整帧 / 完整帧+半包尾 / 半包补齐+完整帧
#[test]
fn pump_multi_batch_interleave() {
  let (server, addr) = spawn_server(Arc::new(LineProvider));
  Runtime::new().unwrap().block_on(drive(
    &addr,
    &[b"PING\r\n", b"PING\r\nPIN", b"G\r\nPING\r\n"],
    b"+PONG\r\n+PONG\r\n+PONG\r\n+PONG\r\n",
  ));
  server.stop();
}

/// 大批量冲洗：单批超出接收缓冲（4096）与发送缓冲，吞吐完整无丢失
#[test]
fn pump_large_pipeline_flush() {
  let (server, addr) = spawn_server(Arc::new(LineProvider));
  let req: Vec<u8> = b"PING\r\n".repeat(1000);
  let expect: Vec<u8> = b"+PONG\r\n".repeat(1000);
  Runtime::new()
    .unwrap()
    .block_on(drive(&addr, &[&req], &expect));
  server.stop();
}

// ---- 直读形态（scratch 消费，会话自有缓冲 + 持久游标）----

/// 会话 C# 缓冲模型的最小等价物：缓冲与游标驻留自身，泵直填网络字节
struct ScratchLineConsumer {
  buf: Vec<u8>,
  head: usize,
}

impl ScratchLineConsumer {
  fn new() -> Self {
    Self {
      buf: Vec::new(),
      head: 0,
    }
  }
}

impl MessageConsumerFace for ScratchLineConsumer {
  fn try_consume_messages_into(&mut self, req_buffer: &[u8], resp_buf: &mut Vec<u8>) -> usize {
    if req_buffer.starts_with(b"PING\r\n") {
      resp_buf.extend_from_slice(b"+PONG\r\n");
      6
    } else {
      0
    }
  }

  fn take_recv_scratch(&mut self) -> Option<Vec<u8>> {
    Some(take(&mut self.buf))
  }

  fn return_recv_scratch(&mut self, buf: Vec<u8>) {
    self.buf = buf;
  }

  fn try_consume_scratch_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
    // 消化全部完整帧（对齐真会话 process_messages 单次全量消费语义）
    loop {
      let rest = &self.buf[self.head..];
      if rest.starts_with(b"PING\r\n") {
        self.head += 6;
        resp_buf.extend_from_slice(b"+PONG\r\n");
      } else if b"PING\r\n".starts_with(rest) {
        break; // 半包待续
      } else {
        // 既非完整帧也非半包前缀 → 协议违规（泵断连）
        return None;
      }
    }
    if self.head >= self.buf.len() {
      // 整段消费完毕：清零复位（游标持久模型的会话侧职责）
      self.buf.clear();
      self.head = 0;
      return Some(0);
    }
    Some(self.buf.len() - self.head)
  }

  fn dispose(&mut self) {}
}

struct ScratchLineProvider;
impl SessionProviderFace for ScratchLineProvider {
  type Consumer = ScratchLineConsumer;
  fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<ScratchLineConsumer> {
    Some(ScratchLineConsumer::new())
  }
}

/// 直读形态：半包重组（字节直入会话缓冲，跨批次拼接）
#[test]
fn scratch_pump_half_packet_reassembly() {
  let (server, addr) = spawn_server(Arc::new(ScratchLineProvider));
  Runtime::new()
    .unwrap()
    .block_on(drive(&addr, &[b"PIN", b"G\r\n"], b"+PONG\r\n"));
  server.stop();
}

/// 直读形态：批内流水线 + 跨批次游标持久（半包尾随）
#[test]
fn scratch_pump_pipeline_and_partial_tail() {
  let (server, addr) = spawn_server(Arc::new(ScratchLineProvider));
  Runtime::new().unwrap().block_on(drive(
    &addr,
    &[b"PING\r\nPING\r\nPIN", b"G\r\nPING\r\n"],
    b"+PONG\r\n+PONG\r\n+PONG\r\n+PONG\r\n",
  ));
  server.stop();
}

/// 直读形态：大批量跨多次网络读完整吞吐
#[test]
fn scratch_pump_large_pipeline_flush() {
  let (server, addr) = spawn_server(Arc::new(ScratchLineProvider));
  let req: Vec<u8> = b"PING\r\n".repeat(1000);
  let expect: Vec<u8> = b"+PONG\r\n".repeat(1000);
  Runtime::new()
    .unwrap()
    .block_on(drive(&addr, &[&req], &expect));
  server.stop();
}

/// 协议违规断连：垃圾字节后泵发尽应答即关闭（客户端读到 EOF）
#[test]
fn scratch_pump_violation_disconnects() {
  let (server, addr) = spawn_server(Arc::new(ScratchLineProvider));
  Runtime::new().unwrap().block_on(async {
    let mut stream = TcpStream::connect(addr.parse::<SocketAddr>().unwrap())
      .await
      .unwrap();
    stream
      .write_all(b"PING\r\nGARBAGE\r\n".to_vec())
      .await
      .unwrap();
    // +PONG 应先发出（发尽应答再断连），随后 EOF
    let mut acc = Vec::new();
    loop {
      let BufResult(res, ret) = stream.read(vec![0u8; 4096]).await;
      let n = res.unwrap();
      acc.extend_from_slice(&ret[..n]);
      if n == 0 {
        break;
      }
    }
    assert_eq!(&acc, b"+PONG\r\n", "发尽应答后断连");
  });
  server.stop();
}

// ---- QUIT 待释放哨兵（C# Process 尾部 if (toDispose) DisposeNetworkSender）----

/// QUIT 语义桩：应答 +OK 后置待释放哨兵（对标 RespServerSession 的 Quit 臂
/// + take_dispose_request 通道）
struct QuitConsumer {
  buf: Vec<u8>,
  head: usize,
  pending_dispose: bool,
}

impl QuitConsumer {
  fn new() -> Self {
    Self {
      buf: Vec::new(),
      head: 0,
      pending_dispose: false,
    }
  }
}

impl MessageConsumerFace for QuitConsumer {
  fn try_consume_messages_into(&mut self, req_buffer: &[u8], resp_buf: &mut Vec<u8>) -> usize {
    if req_buffer.starts_with(b"PING\r\n") {
      resp_buf.extend_from_slice(b"+PONG\r\n");
      6
    } else if req_buffer.starts_with(b"QUIT\r\n") {
      resp_buf.extend_from_slice(b"+OK\r\n");
      self.pending_dispose = true;
      6
    } else {
      0
    }
  }

  fn take_dispose_request(&mut self) -> bool {
    self.pending_dispose
  }

  fn take_recv_scratch(&mut self) -> Option<Vec<u8>> {
    Some(take(&mut self.buf))
  }

  fn return_recv_scratch(&mut self, buf: Vec<u8>) {
    self.buf = buf;
  }

  fn try_consume_scratch_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
    loop {
      let rest = &self.buf[self.head..];
      if rest.starts_with(b"PING\r\n") {
        self.head += 6;
        resp_buf.extend_from_slice(b"+PONG\r\n");
      } else if rest.starts_with(b"QUIT\r\n") {
        self.head += 6;
        resp_buf.extend_from_slice(b"+OK\r\n");
        self.pending_dispose = true;
      } else if b"PING\r\n".starts_with(rest) || b"QUIT\r\n".starts_with(rest) {
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

  fn dispose(&mut self) {}
}

struct QuitProvider;
impl SessionProviderFace for QuitProvider {
  type Consumer = QuitConsumer;
  fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<QuitConsumer> {
    Some(QuitConsumer::new())
  }
}

/// QUIT 断连（回退形态）：+OK 应答发出后服务端主动关闭（客户端读到 EOF）
#[test]
fn pump_quit_replies_then_disconnects() {
  let (server, addr) = spawn_server(Arc::new(QuitProvider));
  Runtime::new().unwrap().block_on(async {
    let mut stream = TcpStream::connect(addr.parse::<SocketAddr>().unwrap())
      .await
      .unwrap();
    stream.write_all(b"QUIT\r\n".to_vec()).await.unwrap();
    let mut acc = Vec::new();
    loop {
      let BufResult(res, ret) = stream.read(vec![0u8; 4096]).await;
      let n = res.unwrap();
      acc.extend_from_slice(&ret[..n]);
      if n == 0 {
        break;
      }
    }
    assert_eq!(&acc, b"+OK\r\n", "QUIT 应答发尽后断连");
  });
  server.stop();
}

/// QUIT 断连（直读形态）：先 PING 保持连接，QUIT 后发尽 +OK 即 EOF
#[test]
fn scratch_pump_quit_replies_then_disconnects() {
  let (server, addr) = spawn_server(Arc::new(QuitProvider));
  Runtime::new().unwrap().block_on(async {
    let mut stream = TcpStream::connect(addr.parse::<SocketAddr>().unwrap())
      .await
      .unwrap();
    stream
      .write_all(b"PING\r\nQUIT\r\n".to_vec())
      .await
      .unwrap();
    let mut acc = Vec::new();
    loop {
      let BufResult(res, ret) = stream.read(vec![0u8; 4096]).await;
      let n = res.unwrap();
      acc.extend_from_slice(&ret[..n]);
      if n == 0 {
        break;
      }
    }
    assert_eq!(&acc, b"+PONG\r\n+OK\r\n", "批内应答全部发尽后断连");
  });
  server.stop();
}
