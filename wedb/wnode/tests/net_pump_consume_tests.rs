//! 网络泵交错路径回归（NetworkHandler drive_loop 状态机）
//!
//! 覆盖消费泵与消费者的交错行为基线：半包重组、批内流水线、跨批次
//! 游标持久（半包尾随）、大批量冲洗扩容、WAIT-FOR-COMMIT 档出网前置
//! 等待。真 socket 端到端驱动
//!（GarnetServer + compio 客户端），消费形态唯一（缓冲与游标驻留消费者，
//! 泵直填网络字节）——对标 C# IMessageConsumer 单形态。

use std::{
  future::Future,
  mem::take,
  net::SocketAddr,
  num::NonZeroUsize,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  thread,
  time::Duration,
};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
};
use parking_lot::Mutex;
use wnode::{
  GarnetServer, MessageConsumerFace, SessionProviderFace, WireFormat,
  resp::{
    garnet_api::GarnetApiFace,
    resp_server_session::{RespServerSession, RespServerSessionOptions},
    resp_session_consumer::RespSessionConsumer,
    slow_path::SlowFuture,
  },
};
use wresp::command::RespCommand;

/// 起服务器并返回地址（缓冲 4096 放大半包/扩容路径触发概率）
fn spawn_server<P: SessionProviderFace + 'static>(provider: Arc<P>) -> (GarnetServer<P>, String) {
  let server =
    GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, 8, Arc::clone(&provider)).unwrap();
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
  fn try_consume_messages_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
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

  fn take_recv_scratch(&mut self) -> Vec<u8> {
    take(&mut self.buf)
  }

  fn return_recv_scratch(&mut self, buf: Vec<u8>) {
    self.buf = buf;
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

/// 半包重组：字节直入会话缓冲，跨批次拼接
#[test]
fn pump_half_packet_reassembly() {
  let (server, addr) = spawn_server(Arc::new(ScratchLineProvider));
  Runtime::new()
    .unwrap()
    .block_on(drive(&addr, &[b"PIN", b"G\r\n"], b"+PONG\r\n"));
  server.stop();
}

/// 批内流水线 + 跨批次游标持久（半包尾随）
#[test]
fn pump_pipeline_and_partial_tail() {
  let (server, addr) = spawn_server(Arc::new(ScratchLineProvider));
  Runtime::new().unwrap().block_on(drive(
    &addr,
    &[b"PING\r\nPING\r\nPIN", b"G\r\nPING\r\n"],
    b"+PONG\r\n+PONG\r\n+PONG\r\n+PONG\r\n",
  ));
  server.stop();
}

/// 大批量跨多次网络读完整吞吐
#[test]
fn pump_large_pipeline_flush() {
  let (server, addr) = spawn_server(Arc::new(ScratchLineProvider));
  let req: Vec<u8> = b"PING\r\n".repeat(1000);
  let expect: Vec<u8> = b"+PONG\r\n".repeat(1000);
  Runtime::new()
    .unwrap()
    .block_on(drive(&addr, &[&req], &expect));
  server.stop();
}

/// 协议违规断连：垃圾字节后泵发尽应答即关闭，且以 FIN 优雅收场
///
/// FIN 判据取自客户端一侧：终止读必为 Ok(0)（对端 FIN 送达的 EOF），不得是 Err。
/// 缺关闭序时流随作用域 drop 直接 close(2)，Linux 对该套接字仍有未读字节时发
/// RST 而非 FIN，RST 清空对端接收缓冲——刚 write_all 成功的应答可能在客户端读到
/// 之前被丢掉（客户端读到 ECONNRESET）。
#[test]
fn pump_violation_disconnects() {
  let (server, addr) = spawn_server(Arc::new(ScratchLineProvider));
  Runtime::new().unwrap().block_on(async {
    let mut stream = TcpStream::connect(addr.parse::<SocketAddr>().unwrap())
      .await
      .unwrap();
    stream
      .write_all(b"PING\r\nGARBAGE\r\n".to_vec())
      .await
      .unwrap();
    // +PONG 应先发出（发尽应答再断连），随后以 FIN 表达 EOF
    let mut acc = Vec::new();
    loop {
      let BufResult(res, ret) = stream.read(vec![0u8; 4096]).await;
      let n = match res {
        Ok(n) => n,
        Err(e) => panic!("连接未以 FIN 收场（已收 {} 字节）: {e}", acc.len()),
      };
      if n == 0 {
        break;
      }
      acc.extend_from_slice(&ret[..n]);
    }
    assert_eq!(&acc, b"+PONG\r\n", "发尽应答后以 FIN 断连");
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
  fn take_dispose_request(&mut self) -> bool {
    self.pending_dispose
  }

  fn take_recv_scratch(&mut self) -> Vec<u8> {
    take(&mut self.buf)
  }

  fn return_recv_scratch(&mut self, buf: Vec<u8>) {
    self.buf = buf;
  }

  fn try_consume_messages_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
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

/// QUIT 断连：+OK 应答发出后服务端主动关闭（客户端读到 EOF）
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

/// QUIT 断连批内混合：先 PING 保持连接，QUIT 后发尽 +PONG+OK 即 EOF
#[test]
fn pump_quit_mixed_batch_replies_then_disconnects() {
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

// ---- 致命断连信号（take_fatal_disconnect）----

/// 致命断连测试消费者桩：收到特定指令时置位 fatal_disconnect 信号并支持跟踪 dispose
struct FatalDisconnectConsumer {
  buf: Vec<u8>,
  head: usize,
  fatal_disconnect: bool,
  disposed: Arc<AtomicBool>,
}

impl FatalDisconnectConsumer {
  fn new(disposed: Arc<AtomicBool>) -> Self {
    Self {
      buf: Vec::new(),
      head: 0,
      fatal_disconnect: false,
      disposed,
    }
  }
}

impl MessageConsumerFace for FatalDisconnectConsumer {
  fn take_fatal_disconnect(&mut self) -> bool {
    take(&mut self.fatal_disconnect)
  }

  fn take_recv_scratch(&mut self) -> Vec<u8> {
    take(&mut self.buf)
  }

  fn return_recv_scratch(&mut self, buf: Vec<u8>) {
    self.buf = buf;
  }

  fn try_consume_messages_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
    loop {
      let rest = &self.buf[self.head..];
      if rest.starts_with(b"PING\r\n") {
        self.head += 6;
        resp_buf.extend_from_slice(b"+PONG\r\n");
      } else if rest.starts_with(b"FATAL_WITH_RESP\r\n") {
        self.head += 17;
        resp_buf.extend_from_slice(b"-ERR fatal error\r\n");
        self.fatal_disconnect = true;
      } else if rest.starts_with(b"FATAL_SILENT\r\n") {
        self.head += 14;
        self.fatal_disconnect = true;
      } else if b"PING\r\n".starts_with(rest)
        || b"FATAL_WITH_RESP\r\n".starts_with(rest)
        || b"FATAL_SILENT\r\n".starts_with(rest)
      {
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

  fn dispose(&mut self) {
    self.disposed.store(true, Ordering::SeqCst);
  }
}

struct FatalDisconnectProvider {
  disposed: Arc<AtomicBool>,
}

impl FatalDisconnectProvider {
  fn new(disposed: Arc<AtomicBool>) -> Self {
    Self { disposed }
  }
}

impl SessionProviderFace for FatalDisconnectProvider {
  type Consumer = FatalDisconnectConsumer;
  fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<FatalDisconnectConsumer> {
    Some(FatalDisconnectConsumer::new(Arc::clone(&self.disposed)))
  }
}

/// 致命断连：应答发出后主动退出泵循环断连并触发 dispose
#[test]
fn pump_fatal_disconnect_replies_then_disconnects() {
  let disposed = Arc::new(AtomicBool::new(false));
  let (server, addr) = spawn_server(Arc::new(FatalDisconnectProvider::new(Arc::clone(
    &disposed,
  ))));
  Runtime::new().unwrap().block_on(async {
    let mut stream = TcpStream::connect(addr.parse::<SocketAddr>().unwrap())
      .await
      .unwrap();
    stream
      .write_all(b"FATAL_WITH_RESP\r\n".to_vec())
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
    assert_eq!(&acc, b"-ERR fatal error\r\n", "致命断连前应答完整发出");
  });
  server.stop();
  assert!(
    disposed.load(Ordering::SeqCst),
    "断连后必须走安全 dispose 收尾"
  );
}

/// 致命断连批内混合：先 PING 后 FATAL，发尽全部应答后断连并触发 dispose
#[test]
fn pump_fatal_disconnect_mixed_batch_replies_then_disconnects() {
  let disposed = Arc::new(AtomicBool::new(false));
  let (server, addr) = spawn_server(Arc::new(FatalDisconnectProvider::new(Arc::clone(
    &disposed,
  ))));
  Runtime::new().unwrap().block_on(async {
    let mut stream = TcpStream::connect(addr.parse::<SocketAddr>().unwrap())
      .await
      .unwrap();
    stream
      .write_all(b"PING\r\nFATAL_WITH_RESP\r\n".to_vec())
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
    assert_eq!(
      &acc, b"+PONG\r\n-ERR fatal error\r\n",
      "批内全部应答发出后断连"
    );
  });
  server.stop();
  assert!(
    disposed.load(Ordering::SeqCst),
    "断连后必须走安全 dispose 收尾"
  );
}

/// 致命静默断连：无应答直接退出泵循环断连（对标 APPENDLOG 拒收语义）并触发 dispose
#[test]
fn pump_fatal_disconnect_silent_disconnects() {
  let disposed = Arc::new(AtomicBool::new(false));
  let (server, addr) = spawn_server(Arc::new(FatalDisconnectProvider::new(Arc::clone(
    &disposed,
  ))));
  Runtime::new().unwrap().block_on(async {
    let mut stream = TcpStream::connect(addr.parse::<SocketAddr>().unwrap())
      .await
      .unwrap();
    stream
      .write_all(b"FATAL_SILENT\r\n".to_vec())
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
    assert!(acc.is_empty(), "静默致命断连不应有任何应答写出");
  });
  server.stop();
  assert!(
    disposed.load(Ordering::SeqCst),
    "断连后必须走安全 dispose 收尾"
  );
}

/// 批内流水线：单批多条命令顺序消费，应答按序拼接
#[test]
fn pump_pipeline_within_batch() {
  let (server, addr) = spawn_server(Arc::new(ScratchLineProvider));
  Runtime::new().unwrap().block_on(drive(
    &addr,
    &[b"PING\r\nPING\r\nPING\r\n"],
    b"+PONG\r\n+PONG\r\n+PONG\r\n",
  ));
  server.stop();
}

/// 三批次交错：完整帧 / 完整帧+半包尾 / 半包补齐+完整帧
#[test]
fn pump_multi_batch_interleave() {
  let (server, addr) = spawn_server(Arc::new(ScratchLineProvider));
  Runtime::new().unwrap().block_on(drive(
    &addr,
    &[b"PING\r\n", b"PING\r\nPIN", b"G\r\nPING\r\n"],
    b"+PONG\r\n+PONG\r\n+PONG\r\n+PONG\r\n",
  ));
  server.stop();
}

// ---- WAIT-FOR-COMMIT 持久性档出网前置等待（C# RespServerSession.cs:Send 内
// `if (waitForAofBlocking)` → storeWrapper.WaitForCommitAsync 读点）----

/// GET 应答桩（命令执行域注入点；真实宿主为存储执行域）
struct OkApi;

impl GarnetApiFace for OkApi {
  fn exec(&self, session: &mut RespServerSession, cmd: RespCommand, _args: &[&[u8]]) {
    assert_eq!(cmd, RespCommand::Get);
    session.output.extend_from_slice(b"+OK\r\n");
  }

  fn exec_slow(
    self: Arc<Self>,
    _cmd: RespCommand,
    _args: Vec<Vec<u8>>,
    _resp_version: u8,
  ) -> SlowFuture {
    SlowFuture::new(async { Vec::new() })
  }
}

/// WAIT-FOR-COMMIT 档出网等待观测宿主：`wait_for_commit_async` 记次数与
/// 时间线（真实面对标 `StorageSessionProvider::wait_for_commit_async` 下达
/// AOF 提交落盘等待，见 wnode/src/service.rs）
struct AofWaitProvider {
  /// 会话门控源（enable_aof && wait_for_commit 同置）
  gate: bool,
  waits: Arc<AtomicUsize>,
  timeline: Arc<Mutex<Vec<&'static str>>>,
}

impl AofWaitProvider {
  fn new(gate: bool) -> Self {
    Self {
      gate,
      waits: Arc::new(AtomicUsize::new(0)),
      timeline: Arc::new(Mutex::new(Vec::new())),
    }
  }
}

impl SessionProviderFace for AofWaitProvider {
  type Consumer = RespSessionConsumer;

  fn get_session(&self, _wf: WireFormat, network_sender: u64) -> Option<RespSessionConsumer> {
    Some(RespSessionConsumer::new(
      network_sender,
      RespServerSessionOptions {
        enable_aof: self.gate,
        wait_for_commit: self.gate,
        ..RespServerSessionOptions::default()
      },
      Arc::new(OkApi),
    ))
  }

  fn wait_for_commit_async(&self) -> impl Future<Output = bool> {
    let waits = Arc::clone(&self.waits);
    let timeline = Arc::clone(&self.timeline);
    async move {
      // 记录点即出网前置位：本行必在 socket 写之前执行，客户端收齐应答
      // 的时间线记录必在其后（跨线程同一时间线定序）
      timeline.lock().push("wait");
      waits.fetch_add(1, Ordering::SeqCst);
      true
    }
  }
}

/// 帧批驱动并把「收齐应答」记入时间线
async fn drive_logged(
  addr: &str,
  batches: &[&[u8]],
  expect: &[u8],
  timeline: &Arc<Mutex<Vec<&'static str>>>,
) {
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
  timeline.lock().push("recv");
  assert_eq!(&acc, expect, "应答字节级等价");
}

const GET_FRAME: &[u8] = b"*3\r\n$3\r\nGET\r\n$1\r\nk\r\n$1\r\nv\r\n";
const PING_FRAME: &[u8] = b"*1\r\n$4\r\nPING\r\n";

/// 档位开启：AOF 相关命令（GET）批次的应答出网前先等一次 AOF 提交落盘；
/// AOF 无关命令（PING）批次不等
#[test]
fn pump_waits_aof_commit_before_send() {
  let provider = Arc::new(AofWaitProvider::new(true));
  let (server, addr) = spawn_server(Arc::clone(&provider));
  Runtime::new().unwrap().block_on(drive_logged(
    &addr,
    &[PING_FRAME, GET_FRAME, PING_FRAME],
    b"+PONG\r\n+OK\r\n+PONG\r\n",
    &provider.timeline,
  ));
  server.stop();
  assert_eq!(
    provider.waits.load(Ordering::SeqCst),
    1,
    "仅 AOF 相关命令批次前置等待一次（其后 PING 批由解析期复位标记）"
  );
  assert_eq!(
    provider.timeline.lock().clone(),
    vec!["wait", "recv"],
    "等待严格先于应答出网"
  );
}

/// 档位关闭（缺省）：出网零等待，应答行为不变
#[test]
fn pump_without_aof_commit_wait_never_waits() {
  let provider = Arc::new(AofWaitProvider::new(false));
  let (server, addr) = spawn_server(Arc::clone(&provider));
  Runtime::new().unwrap().block_on(drive_logged(
    &addr,
    &[GET_FRAME],
    b"+OK\r\n",
    &provider.timeline,
  ));
  server.stop();
  assert_eq!(
    provider.waits.load(Ordering::SeqCst),
    0,
    "门关（EnableAOF/WaitForCommit 未同时成立）时解析不维护标记，泵不等"
  );
  assert_eq!(provider.timeline.lock().clone(), vec!["recv"]);
}

/// 同批 latch（C# `waitForAofBlocking = waitForAofBlocking || !cmd.IsAofIndependent()`，
/// 仅在网面无未发数据时复位）：同一网络批内 AOF 相关命令置位后，其后
/// AOF 无关命令的应答随该批同一次出网等待发出；下一净批解析期复位不再等
#[test]
fn pump_aof_wait_latches_within_batch() {
  let provider = Arc::new(AofWaitProvider::new(true));
  let (server, addr) = spawn_server(Arc::clone(&provider));
  let latched = [GET_FRAME, PING_FRAME].concat();
  let clean = PING_FRAME.to_vec();
  Runtime::new().unwrap().block_on(drive_logged(
    &addr,
    &[&latched, &clean],
    b"+OK\r\n+PONG\r\n+PONG\r\n",
    &provider.timeline,
  ));
  server.stop();
  assert_eq!(
    provider.waits.load(Ordering::SeqCst),
    1,
    "整批一次出网等待（latch 覆盖同批后续 AOF 无关命令），其后净批复位不另等"
  );
  assert_eq!(provider.timeline.lock().clone(), vec!["wait", "recv"]);
}
