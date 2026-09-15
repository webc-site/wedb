//! Wnode 集成测试共用装配 (`wnode_test`)
//!
//! wnode 层测试夹具：单机会话工厂、服务器启动架、RESP 套接字 IO。
//! 依赖 wnode，故仅限测试 crate 消费（wnode 自身测试经 dev-dependencies 引入）。

use std::{io, net::SocketAddr, num::NonZeroUsize, sync::Arc};

use compio::{
  buf::BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
};
use wdev::SegmentedDevice;
use wnode::{
  GarnetServer, RespSessionConsumer, SessionProviderFace,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
};

/// 单机形态会话工厂（StorageSessionProvider 装饰钩子）
pub type SessionFactory = fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer>;

/// 单连接会话工厂（无集群切面的单机形态）
#[must_use]
pub fn session_factory(
  network_sender_id: u64,
  api: StoreGarnetApi<SegmentedDevice>,
) -> Option<RespSessionConsumer> {
  Some(RespSessionConsumer::new(
    network_sender_id,
    RespServerSessionOptions::default(),
    api,
  ))
}

/// 起一台随机端口服务器并返回（服务器句柄，实际监听地址）
///
/// # Panics
/// 服务器启动或地址解析失败时 panic（测试环境不预期发生）
pub fn start_server<P: SessionProviderFace + 'static>(
  provider: Arc<P>,
) -> (Arc<GarnetServer<P>>, SocketAddr) {
  let server = Arc::new(GarnetServer::new(
    &["127.0.0.1:0".to_string()],
    1 << 16,
    8,
    provider,
  ));
  server.start(NonZeroUsize::new(1)).expect("server start");
  let addr = server.local_addr().expect("local addr");
  (server, addr)
}

/// 写出命令载荷（RESP 数组帧）
pub async fn send_cmd(stream: &mut TcpStream, args: &[&[u8]]) -> io::Result<()> {
  let frame = wedb_test::resp_frame(args);
  let BufResult(res, _) = stream.write_all(frame).await;
  res
}

/// 写出原始字节载荷（内联命令 / 流水线字节流）
pub async fn send(stream: &mut TcpStream, data: &[u8]) -> io::Result<()> {
  let BufResult(res, _) = stream.write_all(data.to_vec()).await;
  res
}

/// 读取一条完整行式应答（+OK / -ERR / :N；对端断开返回已收字节）
pub async fn read_line_reply(stream: &mut TcpStream) -> Vec<u8> {
  let mut acc = Vec::new();
  loop {
    let buf = vec![0u8; 512];
    let BufResult(res, returned) = stream.read(buf).await;
    match res {
      Ok(0) | Err(_) => return acc,
      Ok(n) => {
        acc.extend_from_slice(&returned[..n]);
        // 行式应答：读到行尾即完整
        if acc.ends_with(b"\r\n") {
          return acc;
        }
      }
    }
  }
}

/// 读取一条完整 RESP 应答（行式 / bulk / 数组逐帧累积；对端断开返回已收字节）
pub async fn read_reply(stream: &mut TcpStream) -> Vec<u8> {
  let mut acc = Vec::new();
  loop {
    let buf = vec![0u8; 4096];
    let BufResult(res, returned) = stream.read(buf).await;
    match res {
      Ok(0) | Err(_) => return acc,
      Ok(n) => {
        acc.extend_from_slice(&returned[..n]);
        if complete_len(&acc).is_some() {
          return acc;
        }
      }
    }
  }
}

/// 计算缓冲区首条完整 RESP 应答的字节长度（不完整返回 None）
#[must_use]
pub fn complete_len(data: &[u8]) -> Option<usize> {
  if data.is_empty() {
    return None;
  }
  let kind = data[0];
  let nl = data.iter().position(|&b| b == b'\n')?;
  match kind {
    // 行式帧（+OK / -ERR / :N）：头部行到齐即完整（header 非数字，不可先解析）
    b'+' | b'-' | b':' => Some(nl + 1),
    b'$' => {
      let header: i64 = String::from_utf8_lossy(&data[1..nl])
        .trim_end()
        .parse()
        .ok()?;
      if header < 0 {
        Some(nl + 1)
      } else {
        (data.len() >= nl + 1 + header as usize + 2).then(|| nl + 1 + header as usize + 2)
      }
    }
    b'*' => {
      let header: i64 = String::from_utf8_lossy(&data[1..nl])
        .trim_end()
        .parse()
        .ok()?;
      if header < 0 {
        return Some(nl + 1);
      }
      let mut rest = &data[nl + 1..];
      let mut total = nl + 1;
      for _ in 0..header {
        let used = complete_len(rest)?;
        rest = &rest[used..];
        total += used;
      }
      Some(total)
    }
    _ => None,
  }
}

/// 判断累积字节是否已构成一条完整 RESP 应答
#[must_use]
pub fn reply_complete(data: &[u8]) -> bool {
  complete_len(data).is_some()
}

/// 便利封装：发送 RESP 数组帧命令并取回一条完整应答
pub async fn cmd(stream: &mut TcpStream, args: &[&[u8]]) -> Vec<u8> {
  send_cmd(stream, args).await.expect("send cmd");
  read_reply(stream).await
}

/// 读取 bulk 应答载荷（$N\r\n<body>\r\n；nil 帧返回 None）
pub async fn read_bulk_reply(stream: &mut TcpStream) -> Option<Vec<u8>> {
  let mut acc = Vec::new();
  loop {
    let buf = vec![0u8; 512];
    let BufResult(res, returned) = stream.read(buf).await;
    match res {
      Ok(0) | Err(_) => return None,
      Ok(n) => {
        acc.extend_from_slice(&returned[..n]);
        if acc.starts_with(b"$-1\r\n") {
          return None;
        }
        // 帧完整性：头部行 + 定长载荷到齐
        if let Some(nl) = acc.iter().position(|&b| b == b'\n')
          && let Ok(len) = String::from_utf8_lossy(&acc[1..nl])
            .trim_end()
            .parse::<usize>()
          && acc.len() >= nl + 1 + len + 2
        {
          return Some(acc[nl + 1..nl + 1 + len].to_vec());
        }
      }
    }
  }
}
