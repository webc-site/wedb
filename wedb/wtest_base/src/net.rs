//! 集成测试网络基建（对标 C# TestUtils.CreateGarnetServer 的假端点与轮询等待）

use std::{
  net::SocketAddr,
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::{Duration, Instant},
};

use compio::{
  buf::BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::{TcpListener, TcpStream},
  runtime::spawn,
  time::sleep,
};
use wresp::resp_memory_writer::write_bulk_string_to;

/// 轮询等待条件成立（10ms 步进；超时返回 false）
pub async fn wait_for(cond: impl Fn() -> bool, timeout: Duration) -> bool {
  let start = Instant::now();
  while !cond() {
    if start.elapsed() > timeout {
      return false;
    }
    sleep(Duration::from_millis(10)).await;
  }
  true
}

/// 握手靶端：逐请求应答式假节点，对前 `ready_replies` 个完整 RESP 命令帧
/// 各回一个 `+OK\r\n`，此后沉默吞帧
///
/// 对齐真实服务端"一请求一应答"的应答节奏（逐帧应答、不预发、不合包）：
/// `ready_replies` 覆盖握手命令数即建链成功；此后对控制命令不再应答，
/// 模拟"能握手、不回包"的半死对端（超时治理路径的靶点）
pub struct SilentNode {
  addr: SocketAddr,
}

impl SilentNode {
  /// 绑定随机端口并启动 accept 循环（随测试 runtime 退出自动终止）
  pub async fn bind(ready_replies: usize) -> Self {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    spawn(async move {
      while let Ok((stream, _)) = listener.accept().await {
        spawn(async move { serve(stream, ready_replies).await }).detach();
      }
    })
    .detach();
    Self { addr }
  }

  /// 假端点监听端口
  pub fn port(&self) -> u16 {
    self.addr.port()
  }
}

/// 单连接服务循环：逐帧解析 RESP2 数组命令，前 `ready_replies` 帧回 `+OK`，
/// 其余静默吞掉（连接保持，直至对端断开）
async fn serve(mut stream: TcpStream, ready_replies: usize) {
  let mut acc: Vec<u8> = Vec::new();
  let mut replies_left = ready_replies;
  let mut buf = vec![0u8; 4096];
  loop {
    let BufResult(res, next) = stream.read(buf).await;
    buf = next;
    let n = match res {
      Ok(n) if n > 0 => n,
      _ => {
        log::debug!("[serve] read end: {res:?}");
        break;
      }
    };
    log::debug!("[serve] recv {n} bytes");
    acc.extend_from_slice(&buf[..n]);
    while let Some(frame_len) = try_parse_frame(&acc) {
      log::debug!("[serve] parsed frame {frame_len}");
      acc.drain(..frame_len);
      if replies_left == 0 {
        continue;
      }
      replies_left -= 1;
      if stream.write_all(b"+OK\r\n".to_vec()).await.is_err() {
        return;
      }
    }
  }
}

/// 从缓冲解析一个完整 RESP2 数组帧（`*N\r\n` + N 个 `$len\r\npayload\r\n`），
/// 返回帧总字节数；不完整返回 None
fn try_parse_frame(buf: &[u8]) -> Option<usize> {
  parse_frame(buf).map(|(len, _)| len)
}

/// 从缓冲解析一个完整 RESP2 数组帧，返回（帧总字节数，各 bulk 载荷的 owned 副本）；
/// 不完整返回 None
fn parse_frame(buf: &[u8]) -> Option<(usize, Vec<Vec<u8>>)> {
  if buf.first() != Some(&b'*') {
    return None;
  }
  let header_end = buf.iter().position(|b| *b == b'\n')? + 1;
  let argc: usize = from_utf8(&buf[1..header_end - 2]).ok()?.parse().ok()?;
  let mut pos = header_end;
  let mut payloads = Vec::with_capacity(argc);
  for _ in 0..argc {
    if buf.get(pos) != Some(&b'$') {
      return None;
    }
    let len_line_end = buf[pos + 1..].iter().position(|b| *b == b'\n')? + pos + 2;
    let len: usize = from_utf8(&buf[pos + 1..len_line_end - 2])
      .ok()?
      .parse()
      .ok()?;
    let payload_start = len_line_end;
    let payload_end = payload_start + len;
    if payload_end + 2 > buf.len() {
      return None;
    }
    payloads.push(buf[payload_start..payload_end].to_vec());
    pos = payload_end + 2;
  }
  Some((pos, payloads))
}

/// 假端点 bulk string 应答装配（`$<len>\r\n<payload>\r\n`）：帧字节一律出 wresp
/// 写出面，本 crate 不再自拼第二套 RESP 编码
fn bulk_reply(payload: &[u8]) -> Vec<u8> {
  let mut out = Vec::with_capacity(payload.len() + 16);
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
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    spawn(async move {
      while let Ok((stream, _)) = listener.accept().await {
        let reply = Arc::clone(&reply);
        spawn(async move { gossip_serve(stream, reply).await }).detach();
      }
    })
    .detach();
    Self { addr }
  }

  /// 假端点监听端口
  pub fn port(&self) -> u16 {
    self.addr.port()
  }
}

/// gossip 服务循环：CLUSTER GOSSIP 帧回预置 bulk 载荷，其余帧回 `+OK`
async fn gossip_serve(mut stream: TcpStream, reply: Arc<Vec<u8>>) {
  let mut acc: Vec<u8> = Vec::new();
  let mut buf = vec![0u8; 8192];
  loop {
    let BufResult(res, next) = stream.read(buf).await;
    buf = next;
    let n = match res {
      Ok(n) if n > 0 => n,
      _ => {
        log::debug!("[gossip_serve] read end: {res:?}");
        break;
      }
    };
    acc.extend_from_slice(&buf[..n]);
    while let Some((frame_len, payloads)) = parse_frame(&acc) {
      acc.drain(..frame_len);
      let is_gossip = payloads.len() >= 2 && payloads[0] == b"CLUSTER" && payloads[1] == b"GOSSIP";
      let resp = if is_gossip {
        bulk_reply(&reply)
      } else {
        b"+OK\r\n".to_vec()
      };
      if stream.write_all(resp).await.is_err() {
        return;
      }
    }
  }
}

/// failover 靶端：握手后对 `CLUSTER FAILREPLICATIONOFFSET <offset>` 经
/// `reply_delay` 延迟回预置 bulk string 位点应答（慢副本模拟），对
/// `CLUSTER FAILOVER` 记录接管标记并回 `+OK`，其余命令逐帧回 `+OK`
///（首副本探测竞速取快者、整体超时哨兵验证的受控对端）
pub struct FailoverNode {
  addr: SocketAddr,
  takeover: Arc<AtomicBool>,
}

impl FailoverNode {
  /// 绑定随机端口并启动 accept 循环；offset_reply 为位点应答 bulk 载荷，
  /// reply_delay 为位点应答前的人为延迟（ZERO 即刻应答）
  pub async fn bind(offset_reply: Arc<String>, reply_delay: Duration) -> Self {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let takeover = Arc::new(AtomicBool::new(false));
    let loop_takeover = Arc::clone(&takeover);
    spawn(async move {
      while let Ok((stream, _)) = listener.accept().await {
        let (offset_reply, takeover) = (Arc::clone(&offset_reply), Arc::clone(&loop_takeover));
        spawn(async move { failover_serve(stream, offset_reply, reply_delay, takeover).await })
          .detach();
      }
    })
    .detach();
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

/// failover 服务循环：FAILREPLICATIONOFFSET 经 reply_delay 延迟回 bulk 位点、
/// FAILOVER 记接管标记并回 `+OK`，其余帧回 `+OK`（一请求一应答节奏）
async fn failover_serve(
  mut stream: TcpStream,
  offset_reply: Arc<String>,
  reply_delay: Duration,
  takeover: Arc<AtomicBool>,
) {
  let mut acc: Vec<u8> = Vec::new();
  let mut buf = vec![0u8; 8192];
  loop {
    let BufResult(res, next) = stream.read(buf).await;
    buf = next;
    let n = match res {
      Ok(n) if n > 0 => n,
      _ => {
        log::debug!("[failover_serve] read end: {res:?}");
        break;
      }
    };
    acc.extend_from_slice(&buf[..n]);
    while let Some((frame_len, payloads)) = parse_frame(&acc) {
      acc.drain(..frame_len);
      let is_cmd = |name: &str| {
        payloads.len() >= 2 && payloads[0] == b"CLUSTER" && payloads[1] == name.as_bytes()
      };
      let resp = if is_cmd("FAILREPLICATIONOFFSET") {
        if reply_delay > Duration::ZERO {
          sleep(reply_delay).await;
        }
        bulk_reply(offset_reply.as_bytes())
      } else {
        if is_cmd("FAILOVER") {
          takeover.store(true, Ordering::Release);
        }
        b"+OK\r\n".to_vec()
      };
      if stream.write_all(resp).await.is_err() {
        return;
      }
    }
  }
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
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let reset = Arc::new(AtomicBool::new(false));
    let loop_reset = Arc::clone(&reset);
    spawn(async move {
      while let Ok((stream, _)) = listener.accept().await {
        let (offset_reply, reset) = (Arc::clone(&offset_reply), Arc::clone(&loop_reset));
        spawn(async move { stop_writes_serve(stream, offset_reply, reset).await }).detach();
      }
    })
    .detach();
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

/// 停写服务循环：非空载荷 FAILSTOPWRITES 回 bulk 位点应答、空载荷（复位）
/// 记标记回 `+OK`，其余帧回 `+OK`（一请求一应答节奏）
async fn stop_writes_serve(
  mut stream: TcpStream,
  offset_reply: Arc<String>,
  reset: Arc<AtomicBool>,
) {
  let mut acc: Vec<u8> = Vec::new();
  let mut buf = vec![0u8; 8192];
  loop {
    let BufResult(res, next) = stream.read(buf).await;
    buf = next;
    let n = match res {
      Ok(n) if n > 0 => n,
      _ => {
        log::debug!("[stop_writes_serve] read end: {res:?}");
        break;
      }
    };
    acc.extend_from_slice(&buf[..n]);
    while let Some((frame_len, payloads)) = parse_frame(&acc) {
      acc.drain(..frame_len);
      let is_stop_writes =
        payloads.len() >= 3 && payloads[0] == b"CLUSTER" && payloads[1] == b"FAILSTOPWRITES";
      let resp = if is_stop_writes && payloads[2].is_empty() {
        reset.store(true, Ordering::Release);
        b"+OK\r\n".to_vec()
      } else if is_stop_writes {
        bulk_reply(offset_reply.as_bytes())
      } else {
        b"+OK\r\n".to_vec()
      };
      if stream.write_all(resp).await.is_err() {
        return;
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::try_parse_frame;

  #[test]
  fn parse_resp_frame() {
    let frame = b"*2\r\n$4\r\nPING\r\n$3\r\nfoo\r\n";
    assert_eq!(try_parse_frame(frame), Some(frame.len()));
    // 不完整帧
    assert_eq!(try_parse_frame(&frame[..frame.len() - 2]), None);
    assert_eq!(try_parse_frame(b"*1\r\n$4\r\nPI"), None);
    // 两帧连排：只解第一帧
    let two = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
    assert_eq!(try_parse_frame(two), Some(14));
  }
}
