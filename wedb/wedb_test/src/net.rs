//! 集成测试网络基建（对标 C# TestUtils.CreateGarnetServer 的假端点与轮询等待）

use std::{
  net::SocketAddr,
  str::from_utf8,
  time::{Duration, Instant},
};

use compio::{
  buf::BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::{TcpListener, TcpStream},
  runtime::spawn,
  time::sleep,
};

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
  if buf.first() != Some(&b'*') {
    return None;
  }
  let header_end = buf.iter().position(|b| *b == b'\n')? + 1;
  let argc: usize = from_utf8(&buf[1..header_end - 2]).ok()?.parse().ok()?;
  let mut pos = header_end;
  for _ in 0..argc {
    if buf.get(pos) != Some(&b'$') {
      return None;
    }
    let len_line_end = buf[pos + 1..].iter().position(|b| *b == b'\n')? + pos + 2;
    let len: usize = from_utf8(&buf[pos + 1..len_line_end - 2])
      .ok()?
      .parse()
      .ok()?;
    pos = len_line_end + len + 2;
    if pos > buf.len() {
      return None;
    }
  }
  Some(pos)
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
