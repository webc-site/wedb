#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 网络泵交错路径回归（NetworkHandler drive_loop 状态机）
//!
//! 覆盖消费泵与消费者的交错行为基线：半包重组、批内流水线、跨批次
//! 游标持久（半包尾随）、大批量冲洗扩容、WAIT-FOR-COMMIT 档出网前置
//! 等待。真 socket 端到端驱动
//!（GarnetServer + compio 客户端），消费形态唯一（缓冲与游标驻留消费者，
//! 泵直填网络字节）——对标 C# IMessageConsumer 单形态。

use std::{
  future::Future,
  io::ErrorKind,
  mem::take,
  net::SocketAddr,
  num::NonZeroUsize,
  pin::Pin,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  task::{Context, Poll},
  thread,
  time::{Duration, Instant},
};

use compio::{
  BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
  runtime::Runtime,
  time::{sleep, timeout},
};
use parking_lot::Mutex;
use waof::Error;
use wcol::itembroker::{
  collection_item_broker::{CollectionItemBroker, CollectionItemStore, TryGetOutcome},
  collection_item_observer::{CollectionItemResult, ObserverStatus},
  item_broker_face::{BlockedWait, SharedItemBroker},
};
use wnode::{
  GarnetServer, MessageConsumerFace, SessionProviderFace, WireFormat,
  resp::{
    garnet_api::{GarnetApi, GarnetApiFace},
    resp_server_session::{PROBE_PRESERVE_WATERMARK, RespServerSession, RespServerSessionOptions},
    resp_session_consumer::RespSessionConsumer,
    slow_path::{SlowFuture, SlowWait},
  },
  servers::consumer_registry::ConsumerRegistry,
};
use wresp::command::RespCommand;

/// 起服务器并返回地址（缓冲 4096 放大半包/扩容路径触发概率）
fn spawn_server<P: SessionProviderFace + 'static>(provider: Arc<P>) -> (GarnetServer<P>, String) {
  let server =
    GarnetServer::new(&["127.0.0.1:0".to_string()], 4096, Arc::clone(&provider)).unwrap();
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

/// GET/SET 应答桩（命令执行域注入点；真实宿主为存储执行域）
struct OkApi;

impl GarnetApiFace for OkApi {
  fn exec(&self, session: &mut RespServerSession, cmd: RespCommand, _args: &[&[u8]]) {
    // GET/SET 均回 +OK（脚本窗内 redis.call('SET') 重入同落此注入点）
    assert!(matches!(cmd, RespCommand::Get | RespCommand::Set));
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
/// AOF 提交落盘等待，见 wnode/src/service.rs）；`fail_wait` 置位即恒 Err，
/// 注入设备面提交失败
struct AofWaitProvider {
  /// 会话门控源（enable_aof && wait_for_commit 同置）
  gate: bool,
  /// 脚本窗宿主形态（enable_lua：EVAL 走真实 run_lua_command 窗口；
  /// false = 无 Lua 部署形态，session_script_cache 恒 None）
  enable_lua: bool,
  /// 提交失败注入（true = 等待恒 Err，对标 C# CommitFailureException 沿
  /// BlockingWait 抛出）
  fail_wait: bool,
  waits: Arc<AtomicUsize>,
  timeline: Arc<Mutex<Vec<&'static str>>>,
}

impl AofWaitProvider {
  fn new(gate: bool) -> Self {
    Self {
      gate,
      enable_lua: false,
      fail_wait: false,
      waits: Arc::new(AtomicUsize::new(0)),
      timeline: Arc::new(Mutex::new(Vec::new())),
    }
  }

  /// 脚本窗形态宿主（enable_lua 真会话：EVAL 窗经真实 mem::take 换出承接）
  fn new_with_lua(gate: bool) -> Self {
    Self {
      enable_lua: true,
      ..Self::new(gate)
    }
  }

  /// 提交失败注入形态（等待恒 Err）
  fn new_failing(gate: bool) -> Self {
    Self {
      fail_wait: true,
      ..Self::new(gate)
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
        enable_lua: self.enable_lua,
        ..RespServerSessionOptions::default()
      },
      GarnetApi::Face(Arc::new(OkApi)),
    ))
  }

  fn wait_for_commit_async(&self) -> impl Future<Output = waof::Result<bool>> {
    let waits = Arc::clone(&self.waits);
    let timeline = Arc::clone(&self.timeline);
    let fail_wait = self.fail_wait;
    async move {
      // 记录点即出网前置位：本行必在 socket 写之前执行，客户端收齐应答
      // 的时间线记录必在其后（跨线程同一时间线定序）
      timeline.lock().push("wait");
      waits.fetch_add(1, Ordering::SeqCst);
      if fail_wait {
        // 设备面提交失败注错（形态对标 waof WalLog 刷盘失败的错误域）
        return Err(Error::InvalidRecordHeader);
      }
      Ok(true)
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

/// 提交失败注入（设备故障 → 等待 Err；对标 C# RespServerSession.cs:1453
/// `Send` 内 `BlockingWait` 抛 CommitFailureException 后应答不发出，
/// :566 `catch (Exception)` Dispose 断连）：应答零字节不出网，连接收场
#[test]
fn pump_aof_commit_wait_failure_drops_connection_without_reply() {
  let provider = Arc::new(AofWaitProvider::new_failing(true));
  let (server, addr) = spawn_server(Arc::clone(&provider));
  Runtime::new().unwrap().block_on(async {
    let mut stream = TcpStream::connect(addr.parse::<SocketAddr>().unwrap())
      .await
      .unwrap();
    stream.write_all(GET_FRAME.to_vec()).await.unwrap();
    // 服务端等待失败断连：客户端一侧以 EOF/重置收场，且全程零应答字节
    let mut acc: Vec<u8> = Vec::new();
    loop {
      let BufResult(res, buf) = stream.read(vec![0u8; 4096]).await;
      match res {
        Ok(0) | Err(_) => break,
        Ok(n) => acc.extend_from_slice(&buf[..n]),
      }
      assert!(
        acc.is_empty(),
        "提交失败后应答不得出网（C# Dispose 断连语义），实际收到 {acc:?}"
      );
    }
  });
  server.stop();
  assert_eq!(
    provider.waits.load(Ordering::SeqCst),
    1,
    "失败注入下等待仍下达一次"
  );
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

// ---- 脚本窗换出 output 免维护外层 commit-wait 标记（C# 内嵌 processor
// 隔离面，SessionScriptCache.cs:60-64：redis.call 重入独立内嵌会话，外层
// waitForAofBlocking 脚本期不可触）----
//
// 缺陷形态：rust 无内嵌 processor，脚本窗以 mem::take 换出会话 output
//（lua.rs:252/:123），窗内 redis.call 重入解析的 handle_aof_commit_mode
// 复位臂恒见空壳缓冲即误复位外层标记，窗口关闭只还缓冲不还标记，出网臂
// 漏等提交落盘。修复形态：handle_aof_commit_mode 入口按 no_script_bitmap
// （两窗挂/摘单义「窗内」位）整体早退，外层标记只由外层批解析维护。

/// SET 帧（AOF 相关，外层批置位源）
const SET_FRAME: &[u8] = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";

/// EVAL 帧组装（numkeys 0）
fn eval_frame(script: &[u8]) -> Vec<u8> {
  let mut buf = format!("*3\r\n$4\r\nEVAL\r\n${}\r\n", script.len()).into_bytes();
  buf.extend_from_slice(script);
  buf.extend_from_slice(b"\r\n$1\r\n0\r\n");
  buf
}

/// 案 a：「SET + EVAL(return redis.call('PING'))」同批流水线——窗内最后解析
/// 的 PING 属 AOF 独立集，旧形复位臂见换出空壳清掉外层标记致漏等（waits==0）；
/// 修复后窗内双向免维护，标记保持 EVAL 解析期置位，出网前照常等一次
#[test]
fn pump_script_window_readonly_call_keeps_outer_commit_wait() {
  let provider = Arc::new(AofWaitProvider::new_with_lua(true));
  let (server, addr) = spawn_server(Arc::clone(&provider));
  let batch = [SET_FRAME.to_vec(), eval_frame(b"return redis.call('PING')")].concat();
  Runtime::new().unwrap().block_on(drive_logged(
    &addr,
    &[&batch],
    b"+OK\r\n$4\r\nPONG\r\n",
    &provider.timeline,
  ));
  server.stop();
  assert!(
    provider.waits.load(Ordering::SeqCst) >= 1,
    "窗内只读 redis.call 不得复位外层标记：SET 应答出网前须至少等一次提交落盘"
  );
  assert_eq!(
    provider.timeline.lock().first().copied(),
    Some("wait"),
    "时间线等待严格先于应答出网（C# Send 前置闸对齐）"
  );
}

/// 案 b：「SET + EVAL(纯写脚本)」回归——窗内写命令旧形自愈、新形免维护，
/// 出网等待均须下达（判据不回退）
#[test]
fn pump_script_window_write_call_still_waits() {
  let provider = Arc::new(AofWaitProvider::new_with_lua(true));
  let (server, addr) = spawn_server(Arc::clone(&provider));
  let batch = [
    SET_FRAME.to_vec(),
    eval_frame(b"redis.call('SET','sk','sv') return 1"),
  ]
  .concat();
  Runtime::new().unwrap().block_on(drive_logged(
    &addr,
    &[&batch],
    b"+OK\r\n:1\r\n",
    &provider.timeline,
  ));
  server.stop();
  assert!(
    provider.waits.load(Ordering::SeqCst) >= 1,
    "纯写脚本批出网等待不回退"
  );
}

/// 案 c：enable_lua=false + commit-wait 档普通批「SET + PING」——窗外标记维护
/// 不因窗内免维护门失控（锁死判据不得回退为 session_script_cache 两义形：
/// 无 Lua 部署下该字段恒 None，若按「摘除态=窗内」门控将令全档免维护失效）
#[test]
fn pump_commit_wait_latches_with_lua_disabled() {
  let provider = Arc::new(AofWaitProvider::new(true));
  let (server, addr) = spawn_server(Arc::clone(&provider));
  let batch = [SET_FRAME.to_vec(), PING_FRAME.to_vec()].concat();
  Runtime::new().unwrap().block_on(drive_logged(
    &addr,
    &[&batch],
    b"+OK\r\n+PONG\r\n",
    &provider.timeline,
  ));
  server.stop();
  assert!(
    provider.waits.load(Ordering::SeqCst) >= 1,
    "enable_lua=false 档外层批维护照常：SET 置位出网前须等待"
  );
  assert_eq!(
    provider.timeline.lock().clone(),
    vec!["wait", "recv"],
    "等待严格先于应答出网"
  );
}

/// 案 d：无 AOF 档（门关）默认路径全绿——脚本窗照常执行、出网零等待
#[test]
fn pump_script_window_without_aof_gate_never_waits() {
  let provider = Arc::new(AofWaitProvider::new_with_lua(false));
  let (server, addr) = spawn_server(Arc::clone(&provider));
  let batch = [SET_FRAME.to_vec(), eval_frame(b"return redis.call('PING')")].concat();
  Runtime::new().unwrap().block_on(drive_logged(
    &addr,
    &[&batch],
    b"+OK\r\n$4\r\nPONG\r\n",
    &provider.timeline,
  ));
  server.stop();
  assert_eq!(
    provider.waits.load(Ordering::SeqCst),
    0,
    "门关时解析不维护标记、泵不等（默认路径行为不变）"
  );
  assert_eq!(provider.timeline.lock().clone(), vec!["recv"]);
}

// ---- 慢路径终止取消竞速（C# RespServerSession.cs:Dispose 的
// asyncWaiterCancel?.Cancel() + asyncWaiter?.Signal()：会话注销关闭时立即
// 撤销在途异步等待并放弃应答写回，网络执行循环迅速退出注销）----

/// 永不完成的慢执行体（对位 SCAN 冷区回读等长耗时慢命令的确定性探针）：
/// Drop 置位 = 竞速败侧被丢弃即取消的唯一真观测
struct NeverSlow {
  dropped: Arc<AtomicBool>,
}

impl Future for NeverSlow {
  type Output = Vec<u8>;

  fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
    Poll::Pending
  }
}

impl Drop for NeverSlow {
  fn drop(&mut self) {
    self.dropped.store(true, Ordering::SeqCst);
  }
}

/// 慢挂起消费者桩：SLOW 挂起永不完成的慢执行体，PING 常规应答；挂起时刻
/// 经原子位同步给测试驱动（杜绝下杀令先于挂起的竞态）
struct SlowPendingConsumer {
  buf: Vec<u8>,
  head: usize,
  slow: Option<SlowWait>,
  pended: Arc<AtomicBool>,
  dropped: Arc<AtomicBool>,
  disposed: Arc<AtomicBool>,
}

impl MessageConsumerFace for SlowPendingConsumer {
  fn take_recv_scratch(&mut self) -> Vec<u8> {
    take(&mut self.buf)
  }

  fn return_recv_scratch(&mut self, buf: Vec<u8>) {
    self.buf = buf;
  }

  fn take_slow_wait(&mut self) -> Option<SlowWait> {
    take(&mut self.slow)
  }

  fn dispose(&mut self) {
    self.disposed.store(true, Ordering::SeqCst);
  }

  fn try_consume_messages_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
    loop {
      let rest = &self.buf[self.head..];
      if rest.starts_with(b"PING\r\n") {
        self.head += 6;
        resp_buf.extend_from_slice(b"+PONG\r\n");
      } else if rest.starts_with(b"SLOW\r\n") {
        self.head += 6;
        self.slow = Some(SlowWait::new(NeverSlow {
          dropped: Arc::clone(&self.dropped),
        }));
        self.pended.store(true, Ordering::SeqCst);
      } else if b"PING\r\n".starts_with(rest) || b"SLOW\r\n".starts_with(rest) {
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
}

/// 慢挂起测试宿主：真注册表（KILL/注销哨兵与终止竞速的装配前提）
struct SlowPendingProvider {
  registry: Arc<ConsumerRegistry>,
  pended: Arc<AtomicBool>,
  dropped: Arc<AtomicBool>,
  disposed: Arc<AtomicBool>,
}

impl SlowPendingProvider {
  fn new() -> Self {
    Self {
      registry: Arc::new(ConsumerRegistry::new()),
      pended: Arc::new(AtomicBool::new(false)),
      dropped: Arc::new(AtomicBool::new(false)),
      disposed: Arc::new(AtomicBool::new(false)),
    }
  }
}

impl SessionProviderFace for SlowPendingProvider {
  type Consumer = SlowPendingConsumer;

  fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<SlowPendingConsumer> {
    Some(SlowPendingConsumer {
      buf: Vec::new(),
      head: 0,
      slow: None,
      pended: Arc::clone(&self.pended),
      dropped: Arc::clone(&self.dropped),
      disposed: Arc::clone(&self.disposed),
    })
  }

  fn consumer_registry(&self) -> Option<Arc<ConsumerRegistry>> {
    Some(Arc::clone(&self.registry))
  }
}

/// 等待服务端挂起慢执行体（有界自旋；超时即失败，杜绝测试悬挂）
async fn await_pended(pended: &AtomicBool) {
  for _ in 0..600 {
    if pended.load(Ordering::SeqCst) {
      return;
    }
    sleep(Duration::from_millis(5)).await;
  }
  panic!("服务端未在时限内挂起慢执行体");
}

/// 慢命令在途时 CLIENT KILL：终止竞速即刻取消慢执行体（drop 置位，而非
/// 执行至自然完成）、被杀套接字零应答写出、连接退出泵循环完成注销排空
#[test]
fn pump_kill_cancels_pending_slow_wait() {
  let provider = Arc::new(SlowPendingProvider::new());
  let (server, addr) = spawn_server(Arc::clone(&provider));
  Runtime::new().unwrap().block_on(async {
    let mut stream = TcpStream::connect(addr.parse::<SocketAddr>().unwrap())
      .await
      .unwrap();
    stream.write_all(b"SLOW\r\n".to_vec()).await.unwrap();
    await_pended(&provider.pended).await;

    // 他者会话视角下杀令（CLIENT KILL → 条目 kill 位 + 终止广播）
    let entries = provider.registry.active_consumers();
    assert_eq!(entries.len(), 1);
    assert!(entries[0].kill_session());

    // 被杀连接以 EOF 收场，且慢命令应答零字节出网
    let mut acc = Vec::new();
    loop {
      let BufResult(res, ret) = stream.read(vec![0u8; 128]).await;
      let n = res.unwrap();
      if n == 0 {
        break;
      }
      acc.extend_from_slice(&ret[..n]);
    }
    assert!(acc.is_empty(), "被杀连接不得写出慢命令应答");
  });
  server.stop();
  assert!(
    provider.dropped.load(Ordering::SeqCst),
    "在途慢执行体必须被竞速丢弃取消"
  );
  assert!(
    provider.disposed.load(Ordering::SeqCst),
    "断连后必须走安全 dispose 收尾"
  );
  assert_eq!(
    provider.registry.connection_totals(),
    (1, 1, 0),
    "连接必须完成注销排空"
  );
}

/// 慢命令在途时服务端优雅停机：Phase 2 排空（dispose_active_handlers）下杀令
/// 即刻取消慢执行体，排空不被慢命令拖到 5 秒硬超时护栏，连接收场注销
#[test]
fn pump_shutdown_drains_pending_slow_wait() {
  let provider = Arc::new(SlowPendingProvider::new());
  let (server, addr) = spawn_server(Arc::clone(&provider));
  let cli = {
    let provider = Arc::clone(&provider);
    thread::spawn(move || {
      Runtime::new().unwrap().block_on(async move {
        let mut stream = TcpStream::connect(addr.parse::<SocketAddr>().unwrap())
          .await
          .unwrap();
        stream.write_all(b"SLOW\r\n".to_vec()).await.unwrap();
        await_pended(&provider.pended).await;
        // 被排空连接以 EOF 收场，无慢命令应答
        let mut acc = Vec::new();
        loop {
          let BufResult(res, ret) = stream.read(vec![0u8; 128]).await;
          let n = res.unwrap();
          if n == 0 {
            break;
          }
          acc.extend_from_slice(&ret[..n]);
        }
        assert!(acc.is_empty(), "被排空连接不得写出慢命令应答");
      });
    })
  };
  // 主线程等服务端确认挂起后停机（stop 阻塞 join worker，内含 Phase 2 排空）
  let mut armed = false;
  for _ in 0..600 {
    if provider.pended.load(Ordering::SeqCst) {
      armed = true;
      break;
    }
    thread::sleep(Duration::from_millis(5));
  }
  assert!(armed, "服务端未在时限内挂起慢执行体");
  let t0 = Instant::now();
  server.stop();
  assert!(
    t0.elapsed() < Duration::from_secs(4),
    "停机排空耗时 {:?} 疑似落到 5 秒超时强收（在途慢命令未被即刻取消）",
    t0.elapsed()
  );
  cli.join().unwrap();
  assert!(
    provider.dropped.load(Ordering::SeqCst),
    "在途慢执行体必须被竞速丢弃取消"
  );
  assert_eq!(
    provider.registry.connection_totals(),
    (1, 1, 0),
    "连接必须完成注销排空"
  );
}

// ---- 停泊-续跑轮 AOF 出网闸（armed 闩）回归 ----
//
// 缺陷形态（drive/write.rs 出网臂漏等）：停泊轮把此前 AOF 相关命令的应答经
// resolve_*_wait_into → take_output_into 冲入本泵 resp_pooled 并清空会话
// output，随后内层重入消费解析流水线后续 PING 等 AOF 无关命令，
// handle_aof_commit_mode 见 pending_output_len()==0 复位 wait_for_aof_blocking，
// 出网臂读会话字段读到 false 即跳过 wait_for_commit_async，resp_pooled 中
// 已积存的 AOF 相关应答未经 fsync 直发客户端。C# 侧应答滞留 networkSender
// 池化响应缓冲、dcurr==head 复位判据含未出网字节故 Send 恒受闸，不存在此窗口。
// 修复形态：drive_loop 每处冲应答出口以「出会话时点标记」为准就地闩位
// （armed 布尔），出网条件为 armed || 会话字段，成功等待即随轮首 let 复位。
//
// 桩形态：本桩以真实 RespServerSession 的 output/pending_slow/wait_for_aof_blocking
// 三态与 take_output_into 冲出/ handle_aof_commit_mode 复位 语义一一对标，
// 协议以 `GET\r\n`/`PING\r\n` 文本帧驱动（与 ScratchLineConsumer /
// SlowPendingConsumer 桩同一形态惯例，规避真 RESP 帧构造噪声）。

/// 停泊-续跑消费者桩（对标真实 RespSessionConsumer 的 park → resolve →
/// 重入解析流水线后续命令的窗口）：
/// - `GET\r\n`：AOF 相关命令，按 handle_aof_commit_mode 语义置位 wait_for_aof，
///   随后挂 SlowWait 停泊（应答由 resolve_slow_wait_into 直写泵缓冲、绕开会话
///   output，与真实 pump.rs:164-176 同一收尾形态）
/// - `PING\r\n`：AOF 无关命令，按 handle_aof_commit_mode 语义判 output 空即
///   复位 wait_for_aof（对标 resp_command.rs:480-489）；应答写入会话 output
/// - 每轮 try_consume 收尾 take_output_into 冲入泵缓冲（对标
///   resp_session_consumer.rs:158-172）
struct ParkResumeAofConsumer {
  buf: Vec<u8>,
  head: usize,
  slow: Option<SlowWait>,
  /// 会话级应答缓冲（真实 RespServerSession::output 的镜像，与泵缓冲解耦）
  output: Vec<u8>,
  /// 会话出网标记（真实 RespServerSession::wait_for_aof_blocking 的镜像）
  wait_for_aof: bool,
}

impl ParkResumeAofConsumer {
  /// 复刻 resp_command.rs:480-489 handle_aof_commit_mode 语义：output 空即
  /// 复位；后按 !is_aof_independent(cmd) 或置位
  #[inline]
  fn handle_aof_commit_mode(&mut self, is_aof_independent: bool) {
    if self.output.is_empty() {
      self.wait_for_aof = false;
    }
    self.wait_for_aof |= !is_aof_independent;
  }

  /// 复刻 pump.rs:206 take_output_into 语义：会话 output 非空即冲入 out 并清空
  #[inline]
  fn take_output_into(&mut self, out: &mut Vec<u8>) {
    if self.output.is_empty() {
      return;
    }
    out.extend_from_slice(&self.output);
    self.output.clear();
  }
}

impl MessageConsumerFace for ParkResumeAofConsumer {
  fn take_recv_scratch(&mut self) -> Vec<u8> {
    take(&mut self.buf)
  }

  fn return_recv_scratch(&mut self, buf: Vec<u8>) {
    self.buf = buf;
  }

  fn try_consume_messages_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
    loop {
      let rest = &self.buf[self.head..];
      if rest.starts_with(b"GET\r\n") {
        // AOF 相关（is_aof_independent=false）→ 按 handle_aof_commit_mode 置位
        self.handle_aof_commit_mode(false);
        self.head += 5;
        // 挂 SlowWait 停泊，应答由 resolve_slow_wait_into 直写泵缓冲：与真实
        // park_broker_wait / park_cold_context_load 同一「停泊即中断消费、
        // 应答不写会话 output」的形态（对标 core.rs:1125-1140 停车中断）
        self.slow = Some(SlowWait::new(async { b"+OK\r\n".to_vec() }));
        break;
      } else if rest.starts_with(b"PING\r\n") {
        // AOF 无关：复位判据 output 空——停泊续跑轮 output 已被
        // resolve_slow_wait_into 的 take_output_into 清空，此路径必复位
        // wait_for_aof（正是本缺陷触发点）
        self.handle_aof_commit_mode(true);
        self.output.extend_from_slice(b"+PONG\r\n");
        self.head += 6;
      } else if b"GET\r\n".starts_with(rest) || b"PING\r\n".starts_with(rest) {
        break; // 半包待续
      } else {
        return None;
      }
    }
    // 轮尾 take_output_into（对标 resp_session_consumer.rs:158-172）
    self.take_output_into(resp_buf);
    if self.head >= self.buf.len() {
      self.buf.clear();
      self.head = 0;
      return Some(0);
    }
    Some(self.buf.len() - self.head)
  }

  fn take_slow_wait(&mut self) -> Option<SlowWait> {
    take(&mut self.slow)
  }

  /// 对标 pump.rs:164-176：先冲出会话 output 至泵缓冲（清空），再把挂起体
  /// 应答字节按流水线顺序直写泵缓冲。停泊-续跑窗内此调用即「应答出会话时点」，
  /// 本函数的调用点（drive/write.rs 冲出口⑥）就是 armed 闩触发点
  fn resolve_slow_wait_into(&mut self, reply: &[u8], resp_buf: &mut Vec<u8>) {
    self.take_output_into(resp_buf);
    resp_buf.extend_from_slice(reply);
  }

  fn wait_for_aof_blocking(&self) -> bool {
    self.wait_for_aof
  }

  fn dispose(&mut self) {}
}

/// 停泊-续跑 armed 闩测试宿主：与 AofWaitProvider 同一 waits/fail_wait/timeline
/// 观测形态，get_session 返回带 park/resolve 的桩消费者（对标 AofWaitProvider
/// 三态设计，仅消费形态从真实 RespSessionConsumer 换为 ParkResumeAofConsumer）
struct ParkResumeAofProvider {
  /// 提交失败注入（true = 等待恒 Err，与 AofWaitProvider::new_failing 同形态）
  fail_wait: bool,
  waits: Arc<AtomicUsize>,
  timeline: Arc<Mutex<Vec<&'static str>>>,
}

impl ParkResumeAofProvider {
  fn new() -> Self {
    Self {
      fail_wait: false,
      waits: Arc::new(AtomicUsize::new(0)),
      timeline: Arc::new(Mutex::new(Vec::new())),
    }
  }

  fn new_failing() -> Self {
    Self {
      fail_wait: true,
      ..Self::new()
    }
  }
}

impl SessionProviderFace for ParkResumeAofProvider {
  type Consumer = ParkResumeAofConsumer;

  fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<ParkResumeAofConsumer> {
    Some(ParkResumeAofConsumer {
      buf: Vec::new(),
      head: 0,
      slow: None,
      output: Vec::new(),
      wait_for_aof: false,
    })
  }

  fn wait_for_commit_async(&self) -> impl Future<Output = waof::Result<bool>> {
    let waits = Arc::clone(&self.waits);
    let timeline = Arc::clone(&self.timeline);
    let fail_wait = self.fail_wait;
    async move {
      timeline.lock().push("wait");
      waits.fetch_add(1, Ordering::SeqCst);
      if fail_wait {
        return Err(Error::InvalidRecordHeader);
      }
      Ok(true)
    }
  }
}

/// 停泊 + 流水线后续 PING 案：慢臂挂起把 AOF 相关应答直写 resp_pooled 并清空
/// 会话 output，续跑重入解析后续 PING 复位 wait_for_aof_blocking；出网臂若只读
/// 会话字段即漏等（缺陷形），armed 闩按出会话时点捕获即正确下达一次等待
#[test]
fn pump_armed_latch_waits_after_park_resume_pipeline_ping() {
  let provider = Arc::new(ParkResumeAofProvider::new());
  let (server, addr) = spawn_server(Arc::clone(&provider));
  // 停泊命令 + 同批后续 PING：应答字节顺序 +OK\r\n+PONG\r\n（GET 停泊应答 +
  // 续跑轮 PING 应答）
  Runtime::new().unwrap().block_on(drive_logged(
    &addr,
    &[b"GET\r\nPING\r\n"],
    b"+OK\r\n+PONG\r\n",
    &provider.timeline,
  ));
  server.stop();
  assert_eq!(
    provider.waits.load(Ordering::SeqCst),
    1,
    "停泊应答出会话时点标记置位即闩位 armed，续跑轮 PING 复位不掩盖漏等"
  );
  assert_eq!(
    provider.timeline.lock().clone(),
    vec!["wait", "recv"],
    "等待严格先于应答出网（C# Send 前置闸对齐）"
  );
}

/// 同形态 fail_wait 案：armed 闩命中出网闸后，等待 Err → 应答零字节出网即断连
///（对标 pump_aof_commit_wait_failure_drops_connection_without_reply：C#
/// RespServerSession.cs:1453 内 BlockingWait 抛 CommitFailureException 后
/// :566 catch (Exception) Dispose 断连；本形态证明停泊-续跑轮不再破防）
#[test]
fn pump_armed_latch_failure_drops_connection_without_reply() {
  let provider = Arc::new(ParkResumeAofProvider::new_failing());
  let (server, addr) = spawn_server(Arc::clone(&provider));
  Runtime::new().unwrap().block_on(async {
    let mut stream = TcpStream::connect(addr.parse::<SocketAddr>().unwrap())
      .await
      .unwrap();
    stream.write_all(b"GET\r\nPING\r\n".to_vec()).await.unwrap();
    let mut acc: Vec<u8> = Vec::new();
    loop {
      let BufResult(res, buf) = stream.read(vec![0u8; 4096]).await;
      match res {
        Ok(0) | Err(_) => break,
        Ok(n) => acc.extend_from_slice(&buf[..n]),
      }
      assert!(
        acc.is_empty(),
        "armed 闩命中等待失败后应答不得出网（+OK/+PONG 零字节），实际收到 {acc:?}"
      );
    }
  });
  server.stop();
  assert_eq!(
    provider.waits.load(Ordering::SeqCst),
    1,
    "失败注入下等待仍下达一次（armed 命中出网闸）"
  );
}

/// 纯 PING 单轮回归：无停泊、无 AOF 相关命令，会话字段恒 false，armed 亦不置位
/// → 出网零等待（回归既有行为，防误伤 AOF 无关单轮形态）
#[test]
fn pump_armed_latch_pure_ping_never_waits() {
  let provider = Arc::new(ParkResumeAofProvider::new());
  let (server, addr) = spawn_server(Arc::clone(&provider));
  Runtime::new().unwrap().block_on(drive_logged(
    &addr,
    &[b"PING\r\n"],
    b"+PONG\r\n",
    &provider.timeline,
  ));
  server.stop();
  assert_eq!(
    provider.waits.load(Ordering::SeqCst),
    0,
    "纯 AOF 无关命令单轮不触发出网闸（解析期复位无字节可护）"
  );
  assert_eq!(provider.timeline.lock().clone(), vec!["recv"]);
}

// ---- 脚本续跑臂对端活性探测（僵尸观察者回归，工单
// task/ing/wnode-script-suspend-arm-no-peer-liveness-probe-zombie-observer）----
//
// 缺陷形态（脚本臂系 ACL 两臂模板误载）：脚本续跑臂仅 wait_terminate ×
// resume_suspended_script_fut 两路 select，无探测读——脚本内 BLPOP 挂起窗
// 客户端 FIN/RST 不可达，挂起体局部 BlockedWait 因未 drop 不触发 Drop→abort
// 注销，观察者无限期滞留经纪等待队列（僵尸）：元素误弹出后 BrokenPipe 丢弃、
// 同键真实等待者被挤占。修复形态：并入 probe_race 三路竞速（与阻塞/慢臂同
// 一样板），Disposed 胜出即 break 'drive——resume future drop 令挂起体局部随
// 之丢弃，BlockedWait Drop abort 注销（与 dispose 同取消口径）。
//
// 桩形态：真实经纪（CollectionItemBroker + 可武装取件源桩）承载真实观察者
// 生命周期（登记 → 挂队 → 出件/销毁），消费面以 `SUSPEND\r\n` 置脚本挂起、
// resume_suspended_script_fut 覆写为 await blocked.resolve() 的长挂执行体
//（lua.rs resume 窗口的泵可见面投影：future drop 即挂起体丢弃，注销链真实
// 触发，非断言模拟）。

/// 可武装取件源桩：armed 前恒不可取（观察者登记后保持等待态），armed 后回
/// 单元素（LPUSH 语义的确定性注入，经真实经纪主循环取件链出件）
struct ArmedStore {
  armed: Arc<AtomicBool>,
}

impl CollectionItemStore for ArmedStore {
  fn try_get_result(
    &self,
    _ns: u64,
    _db: u64,
    key: &[u8],
    _command: RespCommand,
    _cmd_args: &[Vec<u8>],
    _fail_on_src_type_mismatch: bool,
  ) -> TryGetOutcome {
    if self.armed.load(Ordering::SeqCst) {
      TryGetOutcome::found(CollectionItemResult::single(key.to_vec(), b"v".to_vec()))
    } else {
      TryGetOutcome::none()
    }
  }
}

/// 脚本挂起消费者桩（对标 lua.rs resume_suspended_script 窗口的泵可见面）：
/// - `SUSPEND\r\n`：置脚本挂起（has_script_suspend 真）并挂真实经纪观察者
///   （BlockedWait，脚本内 BLPOP timeout=0 无限等待形态）
/// - `resume_suspended_script_fut`：长挂执行体——await blocked.resolve()，
///   完成后脚本收尾应答直写 resp_buf（真 resume 窗口同通道）；future drop
///   即挂起体局部丢弃 → BlockedWait Drop abort 注销
/// - `PING\r\n`：常规应答（挂起窗到站流水线字节的消费回归锚）
struct ScriptSuspendConsumer {
  buf: Vec<u8>,
  head: usize,
  broker: Arc<SharedItemBroker<ArmedStore>>,
  blocked: Option<BlockedWait<Arc<SharedItemBroker<ArmedStore>>>>,
  pended: Arc<AtomicBool>,
  disposed: Arc<AtomicBool>,
  session_id: usize,
}

impl MessageConsumerFace for ScriptSuspendConsumer {
  fn take_recv_scratch(&mut self) -> Vec<u8> {
    take(&mut self.buf)
  }

  fn return_recv_scratch(&mut self, buf: Vec<u8>) {
    self.buf = buf;
  }

  fn has_script_suspend(&self) -> bool {
    self.blocked.is_some()
  }

  async fn resume_suspended_script_fut<'a>(&'a mut self, resp_buf: &'a mut Vec<u8>) {
    let Some(mut blocked) = self.blocked.take() else {
      return;
    };
    let (_cmd, _result) = blocked.resolve().await;
    // 脚本收尾应答直写驱动方缓冲（真 resume 窗口同通道同序）
    resp_buf.extend_from_slice(b"+SCRIPT_DONE\r\n");
  }

  fn dispose(&mut self) {
    self.disposed.store(true, Ordering::SeqCst);
  }

  fn try_consume_messages_into(&mut self, resp_buf: &mut Vec<u8>) -> Option<usize> {
    loop {
      let rest = &self.buf[self.head..];
      if rest.starts_with(b"PING\r\n") {
        self.head += 6;
        resp_buf.extend_from_slice(b"+PONG\r\n");
      } else if rest.starts_with(b"SUSPEND\r\n") {
        self.head += 9;
        // 挂起即中断消费（真会话 park_script_suspend 同形）：观察者经真实
        // 经纪登记（BLPOP 单键、timeout=0 无限等待），等待由泵层续跑臂驱动
        let observer = self.broker.start_wait(
          RespCommand::Blpop,
          vec![b"k".to_vec()],
          self.session_id,
          Vec::new(),
          (0, 0),
        );
        self.blocked = Some(BlockedWait::new(
          Arc::clone(&self.broker),
          observer,
          RespCommand::Blpop,
          0.0,
        ));
        self.pended.store(true, Ordering::SeqCst);
        break;
      } else if b"PING\r\n".starts_with(rest) || b"SUSPEND\r\n".starts_with(rest) {
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
}

/// 脚本挂起测试宿主：真注册表 + 真经纪（观察者注销链的装配前提）
struct ScriptSuspendProvider {
  registry: Arc<ConsumerRegistry>,
  broker: Arc<SharedItemBroker<ArmedStore>>,
  armed: Arc<AtomicBool>,
  pended: Arc<AtomicBool>,
  disposed: Arc<AtomicBool>,
  session_id: Arc<AtomicUsize>,
}

impl ScriptSuspendProvider {
  fn new() -> Self {
    let armed = Arc::new(AtomicBool::new(false));
    let broker = Arc::new(SharedItemBroker::new(Arc::new(CollectionItemBroker::new(
      ArmedStore {
        armed: Arc::clone(&armed),
      },
    ))));
    Self {
      registry: Arc::new(ConsumerRegistry::new()),
      broker,
      armed,
      pended: Arc::new(AtomicBool::new(false)),
      disposed: Arc::new(AtomicBool::new(false)),
      session_id: Arc::new(AtomicUsize::new(0)),
    }
  }

  /// 释放挂起观察者：armed 置位后经真实经纪更新链唤醒（LPUSH 语义注入——
  /// 主循环试取出件，观察者 ResultSet）。首轮 update 可能先于 NewObserver
  /// 事件消化（键队列未挂）早退，有界重发闭环
  async fn release_suspended(&self) {
    self.armed.store(true, Ordering::SeqCst);
    let sid = self.session_id.load(Ordering::SeqCst);
    for _ in 0..600 {
      self.broker.handle_collection_update((0, 0), b"k");
      match self.broker.try_get_observer(sid) {
        // ResultSet = 出件在途；None = finish_wait 已摘（resolve 已收尾）
        Some(observer) if observer.status() == ObserverStatus::WaitingForResult => {}
        _ => return,
      }
      sleep(Duration::from_millis(5)).await;
    }
    panic!("服务端未在时限内释放挂起观察者");
  }
}

impl SessionProviderFace for ScriptSuspendProvider {
  type Consumer = ScriptSuspendConsumer;

  fn get_session(&self, _wf: WireFormat, network_sender: u64) -> Option<ScriptSuspendConsumer> {
    // 首个会话 ID 记档（单连接测试，观察者注销断言位）
    self
      .session_id
      .store(network_sender as usize, Ordering::SeqCst);
    Some(ScriptSuspendConsumer {
      buf: Vec::new(),
      head: 0,
      broker: Arc::clone(&self.broker),
      blocked: None,
      pended: Arc::clone(&self.pended),
      disposed: Arc::clone(&self.disposed),
      session_id: network_sender as usize,
    })
  }

  fn consumer_registry(&self) -> Option<Arc<ConsumerRegistry>> {
    Some(Arc::clone(&self.registry))
  }
}

/// 案 a：挂起窗对端 close 即 FIN——探测读 Ok(0) 判 Disposed，泵退出、resume
/// future drop 令挂起体 BlockedWait Drop abort，经纪观察者即时注销（无僵尸）
#[test]
fn pump_script_suspend_peer_close_unblocks_observer_and_exits() {
  let provider = Arc::new(ScriptSuspendProvider::new());
  let (server, addr) = spawn_server(Arc::clone(&provider));
  Runtime::new().unwrap().block_on(async {
    let mut stream = TcpStream::connect(addr.parse::<SocketAddr>().unwrap())
      .await
      .unwrap();
    stream.write_all(b"SUSPEND\r\n".to_vec()).await.unwrap();
    await_pended(&provider.pended).await;
    // 挂起窗对端关闭（FIN）：修复前两路 select 无探测读，FIN 无人在场，
    // 观察者滞留经纪成僵尸、泵滞留 resume await 直至 KILL/停机
    drop(stream);
    let sid = provider.session_id.load(Ordering::SeqCst);
    let mut settled = false;
    for _ in 0..600 {
      let unregistered = provider.broker.try_get_observer(sid).is_none();
      if unregistered && provider.disposed.load(Ordering::SeqCst) {
        settled = true;
        break;
      }
      sleep(Duration::from_millis(5)).await;
    }
    assert!(
      settled,
      "挂起窗对端断连须即时注销观察者并收场泵（无僵尸滞留）"
    );
  });
  server.stop();
  assert_eq!(
    provider.registry.connection_totals(),
    (1, 1, 0),
    "连接必须完成注销排空"
  );
}

/// 案 b：挂起窗活连接到站流水线字节——探测读本地累积，续跑完成后收场一次
/// 保全并入会话接收缓冲，重入消费照常应答（字节零丢失）
#[test]
fn pump_script_suspend_window_pipeline_bytes_preserved_and_consumed() {
  let provider = Arc::new(ScriptSuspendProvider::new());
  let (server, addr) = spawn_server(Arc::clone(&provider));
  Runtime::new().unwrap().block_on(async {
    let mut stream = TcpStream::connect(addr.parse::<SocketAddr>().unwrap())
      .await
      .unwrap();
    stream.write_all(b"SUSPEND\r\n".to_vec()).await.unwrap();
    await_pended(&provider.pended).await;
    // 挂起窗内到站的流水线字节（修复前两路 select 无探测读，字节滞留内核
    // 无人在场，续跑应答后 PING 永无应答）
    stream.write_all(b"PING\r\n".to_vec()).await.unwrap();
    provider.release_suspended().await;
    // 脚本收尾应答与挂起窗到站 PING 按序同连接回流（14+7=21 字节）
    let expect: &[u8] = b"+SCRIPT_DONE\r\n+PONG\r\n";
    let mut acc = Vec::new();
    read_until(&mut stream, &mut acc, expect.len()).await;
    assert_eq!(acc, expect, "挂起窗到站字节须照常消费应答");
  });
  server.stop();
}

/// 案 c：无断连回归——挂起体正常 resolve 全链绿（应答回流、连接存活、
/// 服务能力保持）
#[test]
fn pump_script_suspend_resolve_without_disconnect_full_chain() {
  let provider = Arc::new(ScriptSuspendProvider::new());
  let (server, addr) = spawn_server(Arc::clone(&provider));
  Runtime::new().unwrap().block_on(async {
    let mut stream = TcpStream::connect(addr.parse::<SocketAddr>().unwrap())
      .await
      .unwrap();
    stream.write_all(b"SUSPEND\r\n".to_vec()).await.unwrap();
    await_pended(&provider.pended).await;
    provider.release_suspended().await;
    let mut acc = Vec::new();
    read_until(&mut stream, &mut acc, 14).await;
    assert_eq!(acc, b"+SCRIPT_DONE\r\n", "挂起体正常 resolve 应答须回流");
    // 连接存活、服务能力保持（挂起收场不误断活连接）
    stream.write_all(b"PING\r\n".to_vec()).await.unwrap();
    let mut acc = Vec::new();
    read_until(&mut stream, &mut acc, 7).await;
    assert_eq!(acc, b"+PONG\r\n", "挂起收场后连接须照常服务");
  });
  server.stop();
  assert_eq!(
    provider.registry.connection_totals(),
    (1, 1, 0),
    "连接收场后完成注销排空"
  );
}

// ---- 挂起窗探测保全水位门（票 task/ing/wnode-probe-preserve-unbounded-recv-buffer-oom）----
//
// 缺陷形态（回装即转红案 a）：probe_race 活连接保全臂无水位判据——BLPOP 0 形
// 挂起窗（本测试以脚本续跑臂承载，三臂共用 probe_race 竞速单点，门与计数单份）
// 探测读逐轮重建、对端来字本地累积无上界，挂起窗堆驻留随灌入速率×时长线性无界，
// 多连接即 OOM 放大面。C# 同窗无人在场读套接字：网络线程内联阻塞
// （ListCommands.cs:283 `Must block as we're on the network thread` +
// AsyncUtils.BlockingWait），字节滞留内核 SO_RCVBUF 有界、TCP 零窗自然背压
// （TcpNetworkHandlerBase.cs:214 do/while 内核守护停摆），单连接驻留硬顶。
// 修复形态：竞速局部累积达 PROBE_PRESERVE_WATERMARK（core.rs 紧邻
// DEFAULT_RECV_BUFFER_CAPACITY）即停建探测读，循环退化为终止广播 × 执行体两路
// select（ACL 臂同形样板）；停建窗 FIN 不可达系 C# 同形盲窗（脚本臂票裁定第 6
// 条在案），执行体 resolve 后消费段重见流尾照常收场，已累积字节一次保全不弃。
// 否决形（案 b 反证「判 DeadConn 断连收场」误实现）：停建不得杀连接——C# 对灌帧
// 客户端是背压不是断连。
//
// 观测面：条目入向镜像到达即入账（probe_race 就地 add_net_bytes，票
// zcode-r18-netin 契约），挂起窗抽干量 = total_net_input_bytes − SUSPEND 帧长，
// 与竞速局部累积量等价直读；本票「封顶」语义在新形（竞速期本地累积、收场一次
// 保全）下即该增量的水位门。三案各 <1s，不拖慢全量门禁。

/// 探测读单轮字节上界（与 net/handler/buffer.rs 的 MIN_READ_SPACE 同值对表，
/// 该常量为 handler 私有不可导入）
const PROBE_ROUND_MAX: usize = 4096;
/// 挂起窗抽干硬顶：水位 + 触发停建的最后一轮（停建判据在 append 之后）
const PROBE_DRAIN_CAP: usize = PROBE_PRESERVE_WATERMARK + PROBE_ROUND_MAX - 1;
/// SUSPEND 帧长（挂起前独占一批，其后到站字节全走探测竞速）
const SUSPEND_FRAME_LEN: usize = 9;
/// 超水位 flood 规模：封顶线再 +64KB（吸收余量远小于实测环回缓冲容量，
/// 单任务顺序写形不依赖对端读取即可收尾）
const PROBE_FLOOD_SLACK: usize = 1 << 16;
/// PING 帧 6 字节向上取整，保证灌入总量严格越过封顶线 + 余量
const PROBE_FLOOD_FRAMES: usize = (PROBE_DRAIN_CAP + PROBE_FLOOD_SLACK) / 6 + 1;
/// 挂起窗抽干落定余量（缺陷形在此窗内已把全部 flood 抽干，转红判据稳定）
const PROBE_SETTLE: Duration = Duration::from_millis(400);

/// 挂起 + 超水位灌帧的公共前置：SUSPEND 独占首批挂起竞速，其后一次性灌入
/// 超封顶线 +64KB 的 PING 帧流；灌满落定后返回活连接
async fn suspend_and_flood_above_watermark(
  provider: &ScriptSuspendProvider,
  addr: &str,
) -> TcpStream {
  let mut stream = TcpStream::connect(addr.parse::<SocketAddr>().unwrap())
    .await
    .unwrap();
  stream.write_all(b"SUSPEND\r\n".to_vec()).await.unwrap();
  await_pended(&provider.pended).await;
  let flood = b"PING\r\n".repeat(PROBE_FLOOD_FRAMES);
  let budget = timeout(Duration::from_secs(5), stream.write_all(flood)).await;
  budget
    .expect("flood 写入超时：环回缓冲吸收余量不足，测试形制失效")
    .unwrap();
  sleep(PROBE_SETTLE).await;
  stream
}

/// 挂起窗抽干量（条目入向镜像减 SUSPEND 帧；单连接场景专属观测面）
fn window_drained(registry: &ConsumerRegistry) -> usize {
  let sample = registry.monitor_sample();
  assert_eq!(sample.sessions.len(), 1, "场景内仅挂起连接在场");
  let net_in = sample.sessions[0].metrics.total_net_input_bytes as usize;
  assert!(net_in >= SUSPEND_FRAME_LEN);
  net_in - SUSPEND_FRAME_LEN
}

/// 案 a（反证敏感：撤去 probe_race 水位门回装缺陷形即转红）：BLPOP 0 形挂起
/// 与持续灌帧超水位——挂起窗抽干量达水位即封顶（缺陷形此处 = 全部 flood ≈
/// 200KB > 封顶线 135KB），且停建后镜像增量冻结（探测读不再重建）；
/// KILL 收场资源配对归零
#[test]
fn pump_probe_watermark_caps_suspended_window_drain() {
  let provider = Arc::new(ScriptSuspendProvider::new());
  let (server, addr) = spawn_server(Arc::clone(&provider));
  Runtime::new().unwrap().block_on(async {
    let mut stream = suspend_and_flood_above_watermark(&provider, &addr).await;
    let drained = window_drained(&provider.registry);
    assert!(
      drained >= PROBE_PRESERVE_WATERMARK,
      "灌帧超水位，挂起窗抽干须到达水位门（实际 {drained}）"
    );
    assert!(
      drained <= PROBE_DRAIN_CAP,
      "挂起窗入向抽干须封顶于水位+单轮（实际 {drained} > 上限 {}）——缺陷形无门即线性无界",
      PROBE_DRAIN_CAP
    );
    // 停建实证：再等一轮，镜像零增量（缺陷形此窗早已全干，上界断言已红，
    // 冻结断言对两形皆绿，纯锁「停建不复发」语义）
    sleep(Duration::from_millis(200)).await;
    assert_eq!(
      window_drained(&provider.registry),
      drained,
      "超水位后探测读停建"
    );
    // KILL 收场：停建窗终止广播照常可达（两路 select 终止臂）
    let entries = provider.registry.active_consumers();
    assert_eq!(entries.len(), 1);
    assert!(entries[0].kill_session());
    let mut acc = Vec::new();
    loop {
      let BufResult(res, ret) = stream.read(vec![0u8; 128]).await;
      match res {
        // 正常 FIN 收场
        Ok(0) => break,
        Ok(n) => acc.extend_from_slice(&ret[..n]),
        // 服务端带未读 flood 积压关闭即 RST（与 FIN 同为合法断连收场形）
        Err(e)
          if matches!(
            e.kind(),
            ErrorKind::ConnectionReset | ErrorKind::UnexpectedEof
          ) =>
        {
          break;
        }
        Err(e) => panic!("挂起连接收场读取异常: {e}"),
      }
    }
    assert!(acc.is_empty(), "被杀连接不得写出任何应答");
  });
  server.stop();
  assert!(
    provider.disposed.load(Ordering::SeqCst),
    "断连后必须走安全 dispose 收尾"
  );
  assert_eq!(
    provider.registry.connection_totals(),
    (1, 1, 0),
    "挂起收场后资源计数配对归零"
  );
}

/// 案 b（否决「判 DeadConn 断连收场」误实现——回装该形即转红）：停建后执行体
/// resolve——竞速期累积字节收场一次保全照常消费应答（应答全量按序零丢失零
/// 重复），连接存活不误断、服务能力保持
#[test]
fn pump_probe_watermark_resolve_consumes_preserved_and_keeps_connection() {
  let provider = Arc::new(ScriptSuspendProvider::new());
  let (server, addr) = spawn_server(Arc::clone(&provider));
  Runtime::new().unwrap().block_on(async {
    let mut stream = suspend_and_flood_above_watermark(&provider, &addr).await;
    assert!(
      window_drained(&provider.registry) <= PROBE_DRAIN_CAP,
      "前置：已停建"
    );
    provider.release_suspended().await;
    // 停建窗收场：脚本收尾应答先行，挂起窗保全 + 复读段续到的全部 PING 帧
    // 依序逐一应答（14 + 7×N 字节），零丢失零重复
    let expect_len = b"+SCRIPT_DONE\r\n".len() + 7 * PROBE_FLOOD_FRAMES;
    let mut acc = Vec::new();
    read_until(&mut stream, &mut acc, expect_len).await;
    assert_eq!(&acc[..14], b"+SCRIPT_DONE\r\n", "收尾应答按序先行");
    assert_eq!(
      acc[14..].chunks(7).filter(|c| *c == b"+PONG\r\n").count(),
      PROBE_FLOOD_FRAMES,
      "灌入帧须全部照常消费应答（有损/重复即红）"
    );
    // 连接存活不误断：水位触发后照常服务
    stream.write_all(b"PING\r\n".to_vec()).await.unwrap();
    let mut tail = Vec::new();
    read_until(&mut stream, &mut tail, 7).await;
    assert_eq!(tail, b"+PONG\r\n", "停建窗收场后连接须照常服务");
    assert_eq!(
      provider.registry.monitor_sample().sessions.len(),
      1,
      "未被误判定连"
    );
  });
  server.stop();
  assert_eq!(
    provider.registry.connection_totals(),
    (1, 1, 0),
    "连接收场后资源计数配对归零"
  );
}

/// 案 c（回归不回退）：低于水位的短流水线挂起窗——逐轮保全行为零变化，
/// 抽干量恰为全部灌入帧（水位门不误伤正常保全），收场后应答全量回流、
/// 资源配对归零
#[test]
fn pump_probe_below_watermark_preserves_window_rounds_regression() {
  const FLOOD_FRAMES: usize = 500;
  let provider = Arc::new(ScriptSuspendProvider::new());
  let (server, addr) = spawn_server(Arc::clone(&provider));
  Runtime::new().unwrap().block_on(async {
    let mut stream = TcpStream::connect(addr.parse::<SocketAddr>().unwrap())
      .await
      .unwrap();
    stream.write_all(b"SUSPEND\r\n".to_vec()).await.unwrap();
    await_pended(&provider.pended).await;
    stream
      .write_all(b"PING\r\n".repeat(FLOOD_FRAMES))
      .await
      .unwrap();
    sleep(PROBE_SETTLE).await;
    assert_eq!(
      window_drained(&provider.registry),
      6 * FLOOD_FRAMES,
      "低于水位：挂起窗逐轮保全全量抽干，行为零变化"
    );
    provider.release_suspended().await;
    let expect_len = b"+SCRIPT_DONE\r\n".len() + 7 * FLOOD_FRAMES;
    let mut acc = Vec::new();
    read_until(&mut stream, &mut acc, expect_len).await;
    assert_eq!(&acc[..14], b"+SCRIPT_DONE\r\n");
    assert_eq!(
      acc[14..].chunks(7).filter(|c| *c == b"+PONG\r\n").count(),
      FLOOD_FRAMES,
      "保全帧全部照常消费应答"
    );
  });
  server.stop();
  assert_eq!(
    provider.registry.connection_totals(),
    (1, 1, 0),
    "连接收场后资源计数配对归零"
  );
}
