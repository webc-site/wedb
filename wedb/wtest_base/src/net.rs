//! 集成测试网络基建（对标 C# TestUtils.CreateGarnetServer 的假端点与轮询等待）
//!
//! 自研依据: 测试网络基建

use core::fmt;
use std::{
  future::Future,
  net::SocketAddr,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  thread,
  thread::yield_now,
  time::{Duration, Instant},
};

use compio::{
  buf::BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::{TcpListener, TcpStream},
  runtime::spawn,
  time::sleep,
};
use waof::AofAddress;
use wresp::resp_memory_writer::write_bulk_string_to;

/// 假端点装配单点：绑定随机端口 + accept 循环（每连接派生独立任务，随测试
/// runtime 退出自动终止）。连接服务体由 `per_connection` 逐连接构造
///（Arc 状态在闭包内 clone，产出 owned future 挂 runtime；compio 单线程
/// runtime 靶端 future 无 Send 约束）
async fn bind_fake_node<G, F>(per_connection: G) -> SocketAddr
where
  G: Fn(TcpStream) -> F + 'static,
  F: Future<Output = ()> + 'static,
{
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap();
  spawn(async move {
    while let Ok((stream, _)) = listener.accept().await {
      spawn(per_connection(stream)).detach();
    }
  })
  .detach();
  addr
}

/// 连接级帧泵单点：读-积累-逐帧交付（解析走 [`parse_frame_slices`] 单一面），
/// 每帧载荷交 `on_frame` 应答（返回 false = 写出失败，立即收口）；对端断开
/// 即返回。四类靶端（Silent/Gossip/Failover/StopWrites）的同一读泵收口于此
async fn serve_frames(
  mut stream: TcpStream,
  mut on_frame: impl AsyncFnMut(&mut TcpStream, &[&[u8]]) -> bool,
) {
  let mut acc: Vec<u8> = Vec::new();
  let mut buf = vec![0u8; 8192];
  loop {
    let BufResult(res, next) = stream.read(buf).await;
    buf = next;
    let n = match res {
      Ok(n) if n > 0 => n,
      _ => {
        log::debug!("[serve_frames] read end: {res:?}");
        return;
      }
    };
    log::debug!("[serve_frames] recv {n} bytes");
    acc.extend_from_slice(&buf[..n]);
    let mut consumed = 0;
    while let Some((frame_len, payloads)) = parse_frame_slices(&acc[consumed..]) {
      consumed += frame_len;
      if !on_frame(&mut stream, &payloads).await {
        return;
      }
    }
    if consumed > 0 {
      acc.drain(..consumed);
    }
  }
}

/// 默认轮询等待超时（5 秒）
pub const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
/// 默认轮询等待步进（10 毫秒）
pub const DEFAULT_WAIT_STEP: Duration = Duration::from_millis(10);

/// 轮询等待条件成立（指定步进与超时；超时返回 false）
pub async fn wait_for_step(
  mut cond: impl FnMut() -> bool,
  timeout: Duration,
  step: Duration,
) -> bool {
  let start = Instant::now();
  while !cond() {
    if start.elapsed() >= timeout {
      return false;
    }
    sleep(step).await;
  }
  true
}

/// 轮询等待条件成立（默认 10ms 步进；超时返回 false）
pub async fn wait_for(cond: impl FnMut() -> bool, timeout: Duration) -> bool {
  wait_for_step(cond, timeout, DEFAULT_WAIT_STEP).await
}

/// 同步轮询等待条件成立（指定步进与超时；超时返回 false）
///
/// 当 `step` 为 0 时，每轮调用 `std::thread::yield_now()` 让出时间片；
/// 否则调用 `std::thread::sleep(step)`。
pub fn wait_for_step_sync(
  mut cond: impl FnMut() -> bool,
  timeout: Duration,
  step: Duration,
) -> bool {
  let start = Instant::now();
  while !cond() {
    if start.elapsed() >= timeout {
      return false;
    }
    if step.is_zero() {
      yield_now();
    } else {
      thread::sleep(step);
    }
  }
  true
}

/// 同步断言等待条件成立，超时时触发带自定义信息的 panic（纯泛型函数）
pub fn wait_assert_sync(
  cond: impl FnMut() -> bool,
  timeout: Duration,
  step: Duration,
  msg: impl fmt::Display,
) {
  if !wait_for_step_sync(cond, timeout, step) {
    panic!("wait_assert_sync 超时（{:?}）：{}", timeout, msg);
  }
}

/// 同步断言等待条件成立（默认 10ms 步进）
#[inline]
pub fn wait_until_sync(cond: impl FnMut() -> bool, timeout: Duration, msg: impl fmt::Display) {
  wait_assert_sync(cond, timeout, DEFAULT_WAIT_STEP, msg);
}

/// 同步断言等待条件成立（0 步进，纯 yield_now 让渡）
#[inline]
pub fn wait_yield_sync(cond: impl FnMut() -> bool, timeout: Duration, msg: impl fmt::Display) {
  wait_assert_sync(cond, timeout, Duration::ZERO, msg);
}

/// 转换为 Duration 的辅助特征（供 `wait_until!` 宏消费）
pub trait IntoDuration {
  fn into_duration(self) -> Duration;
}

impl IntoDuration for Duration {
  #[inline]
  fn into_duration(self) -> Duration {
    self
  }
}

impl IntoDuration for &str {
  #[inline]
  fn into_duration(self) -> Duration {
    parse_duration_token(self)
  }
}

/// 安全尝试解析时长标记（如 "5s", "500ms", "10us", "100ns", "1m"）
pub fn try_parse_duration_token(s: &str) -> Option<Duration> {
  let s = s.trim();
  let split_idx = s.find(|c: char| c.is_alphabetic() || c == 'µ')?;
  let (num_part, unit_part) = s.split_at(split_idx);
  let num: u64 = num_part.trim().parse().ok()?;
  let unit = unit_part.trim();
  match unit {
    "ns" | "nanos" => Some(Duration::from_nanos(num)),
    "us" | "µs" | "micros" => Some(Duration::from_micros(num)),
    "ms" | "millis" => Some(Duration::from_millis(num)),
    "s" | "sec" | "secs" => Some(Duration::from_secs(num)),
    "m" | "min" | "mins" => Some(Duration::from_secs(num.checked_mul(60)?)),
    _ => None,
  }
}

/// 解析常用时长标记（如 "5s", "500ms", "10us", "100ns", "1m"；非法输入 panic）
pub fn parse_duration_token(s: &str) -> Duration {
  try_parse_duration_token(s).unwrap_or_else(|| panic!("无法解析的时长表达式: {s}"))
}

/// 握手靶端：逐请求应答式假节点，对前 `ready_replies` 个完整 RESP 命令帧
/// 各回一个 `+OK\r\n`，此后沉默吞帧
///
/// 对齐真实服务端"一请求一应答"的应答节奏（逐帧应答、不预发、不合包）：
/// `ready_replies` 覆盖握手命令数即建链成功；此后对控制命令不再应答，
/// 模拟"能握手、不回包"的半死对端（超时治理路径的靶点）
pub struct SilentNode {
  addr: SocketAddr,
  /// 握手额度用尽后静默吞下的帧计数（控制命令挂起在途的可观测证据，
  /// 如 failover 公告臂 GOSSIP 已入靶端）
  silent_frames: Arc<AtomicUsize>,
  /// 是否已有连接因对端断开而读侧收口（per-node 连接释放的可观测证据）
  peer_closed: Arc<AtomicBool>,
}

impl SilentNode {
  /// 绑定随机端口并启动 accept 循环（随测试 runtime 退出自动终止）
  pub async fn bind(ready_replies: usize) -> Self {
    let silent_frames = Arc::new(AtomicUsize::new(0));
    let peer_closed = Arc::new(AtomicBool::new(false));
    let (frames, closed) = (Arc::clone(&silent_frames), Arc::clone(&peer_closed));
    let addr = bind_fake_node(move |stream| {
      silent_serve(
        stream,
        ready_replies,
        Arc::clone(&frames),
        Arc::clone(&closed),
      )
    })
    .await;
    Self {
      addr,
      silent_frames,
      peer_closed,
    }
  }

  /// 假端点监听端口
  pub fn port(&self) -> u16 {
    self.addr.port()
  }

  /// 握手额度用尽后被静默吞下的帧数
  pub fn silent_frame_count(&self) -> usize {
    self.silent_frames.load(Ordering::Acquire)
  }

  /// 是否已有连接因对端断开而收口
  pub fn peer_closed(&self) -> bool {
    self.peer_closed.load(Ordering::Acquire)
  }
}

/// 单连接服务循环（走 [`serve_frames`] 帧泵）：前 `ready_replies` 帧回 `+OK`，
/// 其余静默吞掉并计数（连接保持，直至对端断开，断开置标记）
async fn silent_serve(
  stream: TcpStream,
  ready_replies: usize,
  silent_frames: Arc<AtomicUsize>,
  peer_closed: Arc<AtomicBool>,
) {
  let mut replies_left = ready_replies;
  serve_frames(stream, async |stream, _payloads| {
    if replies_left > 0 {
      replies_left -= 1;
      stream.write_all(b"+OK\r\n").await.is_ok()
    } else {
      silent_frames.fetch_add(1, Ordering::Release);
      true
    }
  })
  .await;
  peer_closed.store(true, Ordering::Release);
}

#[inline]
fn parse_decimal_usize(bytes: &[u8]) -> Option<usize> {
  if bytes.is_empty() {
    return None;
  }
  let mut val: usize = 0;
  for &b in bytes {
    if !b.is_ascii_digit() {
      return None;
    }
    val = val.checked_mul(10)?.checked_add((b - b'0') as usize)?;
  }
  Some(val)
}

/// RESP2 数组帧遍历核：解析 `*N\r\n` + N 个 `$len\r\npayload\r\n`，
/// 逐段将 bulk 载荷交给 `sink`，返回帧总字节数；不完整返回 None。
///
/// 两处公开入口 `try_parse_frame` / `parse_frame_slices` 共用此单一游标，
/// 保证对半包 / 粘包 / 非法头的返回判定逐字节等价。
fn scan_frame<'a, F: FnMut(&'a [u8])>(buf: &'a [u8], mut sink: F) -> Option<usize> {
  if buf.first() != Some(&b'*') {
    return None;
  }
  let header_rel = buf.iter().position(|b| *b == b'\n')?;
  let header_end = header_rel + 1;
  // 下溢守卫：`*\n` / `*\r\n` 畸形头无 argc 数字段（须为 `\r\n` 结尾且数字非空）
  if header_end < 4 || buf[header_end - 2] != b'\r' {
    return None;
  }
  let argc = parse_decimal_usize(&buf[1..header_end - 2])?;
  let mut pos = header_end;
  for _ in 0..argc {
    if buf.get(pos) != Some(&b'$') {
      return None;
    }
    let len_rel = buf[pos + 1..].iter().position(|b| *b == b'\n')?;
    let len_line_end = pos + 1 + len_rel + 1;
    // 下溢守卫：`$\n` / `$\r\n` 畸形头无 len 数字段
    if len_line_end < pos + 4 || buf[len_line_end - 2] != b'\r' {
      return None;
    }
    let len = parse_decimal_usize(&buf[pos + 1..len_line_end - 2])?;
    let payload_start = len_line_end;
    let payload_end = payload_start.checked_add(len)?;
    if payload_end.checked_add(2)? > buf.len() || &buf[payload_end..payload_end + 2] != b"\r\n" {
      return None;
    }
    sink(&buf[payload_start..payload_end]);
    pos = payload_end + 2;
  }
  Some(pos)
}

/// 从缓冲解析一个完整 RESP2 数组帧（`*N\r\n` + N 个 `$len\r\npayload\r\n`），
/// 返回帧总字节数（零堆分配）；不完整返回 None
pub fn try_parse_frame(buf: &[u8]) -> Option<usize> {
  scan_frame(buf, |_| {})
}

/// 从缓冲解析一个完整 RESP2 数组帧，返回（帧总字节数，各 bulk 载荷的零拷贝切片借用）；
/// 不完整返回 None
pub fn parse_frame_slices(buf: &[u8]) -> Option<(usize, Vec<&[u8]>)> {
  let mut payloads = Vec::new();
  let len = scan_frame(buf, |p| payloads.push(p))?;
  Some((len, payloads))
}

/// 从缓冲解析一个完整 RESP2 数组帧，返回（帧总字节数，各 bulk 载荷的 owned 副本）；
/// 不完整返回 None
pub fn parse_frame(buf: &[u8]) -> Option<(usize, Vec<Vec<u8>>)> {
  parse_frame_slices(buf)
    .map(|(len, payloads)| (len, payloads.into_iter().map(<[u8]>::to_vec).collect()))
}

/// 假端点 bulk string 应答装配（`$<len>\r\n<payload>\r\n`）：帧字节一律出 wresp
/// 写出面，本 crate 不再自拼第二套 RESP 编码
fn bulk_reply(payload: &[u8]) -> Vec<u8> {
  let mut out = Vec::with_capacity(payload.len().saturating_add(16));
  write_bulk_string_to(&mut out, payload);
  out
}

/// gossip 靶端：对 `CLUSTER GOSSIP [WITHMEET] <blob>` 请求回预置 bulk string
/// 载荷（gossip 增量判定、MEET 应答验证的受控对端），其余命令逐帧回 `+OK`
pub struct GossipNode {
  addr: SocketAddr,
}

impl GossipNode {
  /// 绑定随机端口；reply 为 gossip 请求的 bulk string 应答载荷
  pub async fn bind(reply: Arc<Vec<u8>>) -> Self {
    let addr = bind_fake_node(move |stream| gossip_serve(stream, Arc::clone(&reply))).await;
    Self { addr }
  }

  /// 假端点监听端口
  pub fn port(&self) -> u16 {
    self.addr.port()
  }
}

/// gossip 服务循环（走 [`serve_frames`] 帧泵）：CLUSTER GOSSIP 帧回预置 bulk
/// 载荷，其余帧回 `+OK`
async fn gossip_serve(stream: TcpStream, reply: Arc<Vec<u8>>) {
  serve_frames(stream, async |stream, payloads| {
    let is_gossip = payloads.len() >= 2 && payloads[0] == b"CLUSTER" && payloads[1] == b"GOSSIP";
    if is_gossip {
      stream.write_all(bulk_reply(&reply)).await.is_ok()
    } else {
      stream.write_all(b"+OK\r\n").await.is_ok()
    }
  })
  .await;
}

/// failover 靶端：握手后对 `CLUSTER FAILREPLICATIONOFFSET <带 1B 长度前缀
/// 二进制位点>` 校验线形解码后经 `reply_delay` 延迟回预置 bulk string 位点
/// 应答（慢副本模拟），对
/// `CLUSTER FAILOVER` 记录接管标记并回 `+OK`（bind_rejecting_takeover 变体
/// 回 -ERR），其余命令逐帧回 `+OK`（首副本探测竞速取快者、整体超时哨兵、
/// 接管失败回滚的受控对端）
pub struct FailoverNode {
  addr: SocketAddr,
  takeover: Arc<AtomicBool>,
}

impl FailoverNode {
  /// 绑定随机端口并启动 accept 循环；offset_reply 为位点应答 bulk 载荷，
  /// reply_delay 为位点应答前的人为延迟（ZERO 即刻应答）
  pub async fn bind(offset_reply: Arc<String>, reply_delay: Duration) -> Self {
    Self::bind_with(offset_reply, reply_delay, b"+OK\r\n").await
  }

  /// 位点追平但接管应答失败的变体：对 `CLUSTER FAILOVER` 回 -ERR
  ///（模拟从节点接管失败，主端回滚闭环的受控对端）
  pub async fn bind_rejecting_takeover(offset_reply: Arc<String>, reply_delay: Duration) -> Self {
    Self::bind_with(offset_reply, reply_delay, b"-ERR takeover rejected\r\n").await
  }

  async fn bind_with(
    offset_reply: Arc<String>,
    reply_delay: Duration,
    takeover_reply: &'static [u8],
  ) -> Self {
    let takeover = Arc::new(AtomicBool::new(false));
    let loop_takeover = Arc::clone(&takeover);
    let addr = bind_fake_node(move |stream| {
      failover_serve(
        stream,
        Arc::clone(&offset_reply),
        reply_delay,
        Arc::clone(&loop_takeover),
        takeover_reply,
      )
    })
    .await;
    Self { addr, takeover }
  }

  /// 假端点监听端口
  pub fn port(&self) -> u16 {
    self.addr.port()
  }

  /// 是否已收到 CLUSTER FAILOVER 接管命令
  pub fn takeover_received(&self) -> bool {
    self.takeover.load(Ordering::Acquire)
  }
}

/// failover 服务循环（走 [`serve_frames`] 帧泵）：FAILREPLICATIONOFFSET 经
/// reply_delay 延迟回 bulk 位点、FAILOVER 记接管标记并回 takeover_reply
///（+OK 或 -ERR），其余帧回 `+OK`（一请求一应答节奏）
async fn failover_serve(
  stream: TcpStream,
  offset_reply: Arc<String>,
  reply_delay: Duration,
  takeover: Arc<AtomicBool>,
  takeover_reply: &'static [u8],
) {
  serve_frames(stream, async |stream, payloads| {
    let is_cmd = |name: &str| {
      payloads.len() >= 2 && payloads[0] == b"CLUSTER" && payloads[1] == name.as_bytes()
    };
    if is_cmd("FAILREPLICATIONOFFSET") {
      // 请求载荷线形锁：真实主端发带 1 字节长度前缀二进制
      // （waof AofAddress::to_aof_binary，对标 C# GarnetClientExtensions.cs:61
      // ToByteArray），解码失败即回错误帧——靶端不校验形则发收交点被掩蔽
      let payload_ok = payloads
        .get(2)
        .is_some_and(|p| AofAddress::from_aof_binary(p).is_some());
      if !payload_ok {
        stream
          .write_all(b"-ERR invalid failreplicationoffset payload\r\n")
          .await
          .is_ok()
      } else {
        if reply_delay > Duration::ZERO {
          sleep(reply_delay).await;
        }
        stream
          .write_all(bulk_reply(offset_reply.as_bytes()))
          .await
          .is_ok()
      }
    } else if is_cmd("FAILOVER") {
      takeover.store(true, Ordering::Release);
      stream.write_all(takeover_reply).await.is_ok()
    } else {
      stream.write_all(b"+OK\r\n").await.is_ok()
    }
  })
  .await;
}

/// 停写靶端：握手后对 `CLUSTER FAILSTOPWRITES <node_id>` 回预置 bulk string
/// 位点应答（模拟主端已确认停写且位点领先，副本位点等待无从追平），对空载荷
/// 停写（复位）记录标记并回 `+OK`，其余命令逐帧回 `+OK`
///（abort 打断位点等待、主端恢复写验证的受控对端）
pub struct StopWritesNode {
  addr: SocketAddr,
  reset: Arc<AtomicBool>,
}

impl StopWritesNode {
  /// 绑定随机端口并启动 accept 循环；offset_reply 为停写确认应答的 bulk 载荷
  pub async fn bind(offset_reply: Arc<String>) -> Self {
    let reset = Arc::new(AtomicBool::new(false));
    let loop_reset = Arc::clone(&reset);
    let addr = bind_fake_node(move |stream| {
      stop_writes_serve(stream, Arc::clone(&offset_reply), Arc::clone(&loop_reset))
    })
    .await;
    Self { addr, reset }
  }

  /// 假端点监听端口
  pub fn port(&self) -> u16 {
    self.addr.port()
  }

  /// 是否已收到空载荷停写复位（主端恢复写标记）
  pub fn reset_received(&self) -> bool {
    self.reset.load(Ordering::Acquire)
  }
}

/// 停写服务循环（走 [`serve_frames`] 帧泵）：非空载荷 FAILSTOPWRITES 回 bulk
/// 位点应答、空载荷（复位）记标记回 `+OK`，其余帧回 `+OK`（一请求一应答节奏）
async fn stop_writes_serve(stream: TcpStream, offset_reply: Arc<String>, reset: Arc<AtomicBool>) {
  serve_frames(stream, async |stream, payloads| {
    let is_stop_writes =
      payloads.len() >= 3 && payloads[0] == b"CLUSTER" && payloads[1] == b"FAILSTOPWRITES";
    if is_stop_writes && payloads[2].is_empty() {
      reset.store(true, Ordering::Release);
      stream.write_all(b"+OK\r\n").await.is_ok()
    } else if is_stop_writes {
      stream
        .write_all(bulk_reply(offset_reply.as_bytes()))
        .await
        .is_ok()
    } else {
      stream.write_all(b"+OK\r\n").await.is_ok()
    }
  })
  .await;
}
