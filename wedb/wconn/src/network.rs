//! 客户端网络读写泵：命令帧编码 + 单连接请求/应答循环
//!
//! [`GarnetClient`](crate::GarnetClient) 与
//! [`GarnetClientSession`](crate::GarnetClientSession) 共用同一实现，
//! 对标 libs/client/GarnetClientProcessReplies.cs:ProcessReplies（C# 侧同样的
//! 应答泵在 GarnetClient/GarnetClientSession 各有一份，此处收敛为单一定义）。
//!
//! 循环每轮：限时收一条命令 → 非阻塞清空通道积压 → 一次性批量写入 →
//! 循环读取应答按 FIFO 逐条派发；应答不完整时保留未消费字节等下一个读事件。
//! 命令空闲期以读探测承接对端断链感知（C# networkSender 接收环常驻读的
//! 等价形态）：EOF 即网络泵退出，发送端经 `is_connected` 观测到断连。

use std::{collections::VecDeque, io, time::Duration};

use compio::{
  buf::BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
  time::timeout,
};
use crossfire::{AsyncRx, mpsc, oneshot};
use itoa::Buffer;

use crate::{
  Error, Result,
  parser::{RespReadResponseUtils, read_token_line, unexpected_token},
  types::{ChannelTx, CommandItem, ReplyTx, roundtrip},
};

/// 单次 socket 读取块大小
const READ_CHUNK: usize = 16 * 1024;
/// 应答累积缓冲初始容量
const READ_BUF_CAP: usize = 8 * 1024;
/// 命令空闲等待上限：到期进入 EOF 探测读（对端断链感知粒度）
const RECV_IDLE_PROBE: Duration = Duration::from_millis(250);

async fn exec(tx: &ChannelTx, command: &[&str]) -> Result<String> {
  let (resp_tx, resp_rx) = oneshot::oneshot();
  let item = CommandItem::new_str(command, ReplyTx::Str(resp_tx));
  roundtrip(tx, item, resp_rx).await
}

/// 连接后握手序列：AUTH（用户名优先，缺省密码按空串补齐）+ CLIENT SETINFO/SETNAME
/// （客户端与会话共用同一口径，对标 C# ConnectAsync；SETINFO/SETNAME 同以
/// clientName 非空为前提）
pub(super) async fn handshake(
  tx: &ChannelTx,
  lib_name: &str,
  auth_username: Option<&str>,
  auth_password: Option<&str>,
  client_name: Option<&str>,
) -> Result<()> {
  if let Some(username) = auth_username {
    let pwd = auth_password.unwrap_or("");
    exec(tx, &["AUTH", username, pwd]).await?;
  } else if let Some(pwd) = auth_password {
    exec(tx, &["AUTH", pwd]).await?;
  }
  if let Some(client_name) = client_name {
    exec(tx, &["CLIENT", "SETINFO", "LIB-NAME", lib_name]).await?;
    exec(tx, &["CLIENT", "SETNAME", client_name]).await?;
  }
  Ok(())
}

/// 把通道中此刻已就绪的命令全部取出排入队列（批量写入摊薄 syscall 次数）；
/// 阻塞收第一条由主循环限时承接，此处仅非阻塞清空
fn drain_ready(
  rx: &AsyncRx<mpsc::Array<CommandItem>>,
  first: CommandItem,
  queue: &mut VecDeque<CommandItem>,
) {
  queue.push_back(first);
  while let Ok(item) = rx.try_recv() {
    queue.push_back(item);
  }
}

/// 将一条字符串命令编码为 RESP2 数组帧追加到 out（itoa 直写，无 format! 中间分配）
pub(crate) fn encode_str_command(out: &mut Vec<u8>, cmd: &[&str]) {
  let est = cmd.iter().map(|s| s.len() + 16).sum::<usize>() + 16;
  out.reserve(est);
  let mut num = Buffer::new();
  out.push(b'*');
  out.extend_from_slice(num.format(cmd.len()).as_bytes());
  out.extend_from_slice(b"\r\n");
  for arg in cmd {
    out.push(b'$');
    out.extend_from_slice(num.format(arg.len()).as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(arg.as_bytes());
    out.extend_from_slice(b"\r\n");
  }
}

/// 将一条字节命令编码为 RESP2 数组帧追加到 out
pub(crate) fn encode_bytes_command(out: &mut Vec<u8>, cmd: &[&[u8]]) {
  let est = cmd.iter().map(|s| s.len() + 16).sum::<usize>() + 16;
  out.reserve(est);
  let mut num = Buffer::new();
  out.push(b'*');
  out.extend_from_slice(num.format(cmd.len()).as_bytes());
  out.extend_from_slice(b"\r\n");
  for arg in cmd {
    out.push(b'$');
    out.extend_from_slice(num.format(arg.len()).as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(arg);
    out.extend_from_slice(b"\r\n");
  }
}

fn parse_bytes(data: &mut &[u8]) -> Result<Option<Result<Vec<u8>>>> {
  if data.is_empty() {
    return Ok(None);
  }
  if data.starts_with(b"+OK\r\n") {
    *data = &data[5..];
    return Ok(Some(Ok(b"OK".to_vec())));
  }
  match data[0] {
    b'+' => {
      RespReadResponseUtils::try_read_simple_string(data).map(|s| s.map(|s| Ok(s.into_bytes())))
    }
    b'-' => {
      RespReadResponseUtils::try_read_error_as_string(data).map(|e| e.map(|e| Err(Error::Other(e))))
    }
    b':' => {
      RespReadResponseUtils::try_read_integer_as_string(data).map(|s| s.map(|s| Ok(s.into_bytes())))
    }
    b'$' => RespReadResponseUtils::try_read_byte_array_with_length_header(data)
      .map(|b| b.map(|b| Ok(b.unwrap_or_default()))),
    b'*' | b'~' | b'>' => RespReadResponseUtils::try_read_string_array_with_length_header(data)
      .map(|a| {
        a.map(|a| {
          Ok(
            a.and_then(|mut v| v.drain(..).next())
              .map(String::into_bytes)
              .unwrap_or_default(),
          )
        })
      }),
    b'_' => RespReadResponseUtils::try_read_null(data).map(|n| n.map(|_| Ok(Vec::new()))),
    b',' | b'#' => read_token_line(data, data[0]).map(|s| s.map(|s| Ok(s.as_bytes().to_vec()))),
    b => Err(unexpected_token(b)),
  }
}

/// 将一条标量应答（简单串/错误串/整数/bulk string/array首元素/RESP3 null）解析为 Result<String>；
/// 应答不完整返回 Ok(None)
fn parse_scalar(data: &mut &[u8]) -> Result<Option<Result<String>>> {
  // +OK\r\n 最常见应答快路径（对标 C# ProcessReplyAsString: +OK\r\n 快速推进）
  if data.starts_with(b"+OK\r\n") {
    *data = &data[5..];
    return Ok(Some(Ok("OK".to_string())));
  }
  match data[0] {
    b'+' => RespReadResponseUtils::try_read_simple_string(data).map(|s| s.map(Ok)),
    b'-' => {
      RespReadResponseUtils::try_read_error_as_string(data).map(|e| e.map(|e| Err(Error::Other(e))))
    }
    b':' => RespReadResponseUtils::try_read_integer_as_string(data).map(|s| s.map(Ok)),
    b'$' => RespReadResponseUtils::try_read_string_with_length_header(data)
      .map(|s| s.map(|s| Ok(s.unwrap_or_default()))),
    // 标量分支遇到数组应答：返回首元素（对标 C# ProcessReplyAsString case '*'）
    b'*' => RespReadResponseUtils::try_read_string_array_with_length_header(data)
      .map(|a| a.map(|a| Ok(a.and_then(|mut v| v.drain(..).next()).unwrap_or_default()))),
    // RESP3 null 标量支持
    b'_' => RespReadResponseUtils::try_read_null(data).map(|n| n.map(|_| Ok(String::new()))),
    // RESP3 浮点 / 布尔标量支持
    b',' | b'#' => read_token_line(data, data[0]).map(|s| s.map(|s| Ok(s.to_string()))),
    b => Err(unexpected_token(b)),
  }
}

/// 将一条数组应答解析为 Result<Vec<String>>；不完整返回 Ok(None)
///
/// 标量应答（+/-/:/$）按 C# ProcessReplyAsStringArray 语义包装为单元素数组
fn parse_array(data: &mut &[u8]) -> Result<Option<Result<Vec<String>>>> {
  // +OK\r\n 快路径（对标 C# ProcessReplyAsStringArray）
  if data.starts_with(b"+OK\r\n") {
    *data = &data[5..];
    return Ok(Some(Ok(vec!["OK".to_string()])));
  }
  match data[0] {
    b'*' | b'~' | b'>' => RespReadResponseUtils::try_read_string_array_with_length_header(data)
      .map(|a| a.map(|a| Ok(a.unwrap_or_default()))),
    // 标量分支：包装为单元素数组（对齐 C# ProcessReplyAsStringArray）
    b'+' => RespReadResponseUtils::try_read_simple_string(data).map(|s| s.map(|s| Ok(vec![s]))),
    b':' => RespReadResponseUtils::try_read_integer_as_string(data).map(|s| s.map(|s| Ok(vec![s]))),
    b'$' => RespReadResponseUtils::try_read_string_with_length_header(data)
      .map(|s| s.map(|s| Ok(vec![s.unwrap_or_default()]))),
    b'-' => {
      RespReadResponseUtils::try_read_error_as_string(data).map(|e| e.map(|e| Err(Error::Other(e))))
    }
    // RESP3 null 数组分支：空列表
    b'_' => RespReadResponseUtils::try_read_null(data).map(|n| n.map(|_| Ok(Vec::new()))),
    // RESP3 浮点 / 布尔
    b',' | b'#' => read_token_line(data, data[0]).map(|s| s.map(|s| Ok(vec![s.to_string()]))),
    b => Err(unexpected_token(b)),
  }
}

/// 网络循环主泵（详见模块文档）
pub(super) async fn network_loop(
  mut stream: TcpStream,
  rx: AsyncRx<mpsc::Array<CommandItem>>,
) -> Result<()> {
  let mut queue: VecDeque<CommandItem> = VecDeque::new();
  let mut read_buf: Vec<u8> = Vec::with_capacity(READ_BUF_CAP);
  // 写出与读取块全程复用，避免每轮循环分配清零
  let mut out_buf = Vec::new();
  let mut chunk = vec![0u8; READ_CHUNK];

  loop {
    match timeout(RECV_IDLE_PROBE, rx.recv()).await {
      // 新命令就绪：随后的 try_recv 清空通道积压（批量写入摊薄 syscall 次数）
      Ok(Ok(first)) => drain_ready(&rx, first, &mut queue),
      // 会话句柄全部丢弃：网络泵自然退出
      Ok(Err(_)) => break,
      // 空闲期 EOF 探测：读事件即对端断链 / 服务端主动数据的感知点
      //（C# 接收环常驻读的等价承接；探测读被新命令超时打断属正常路径）
      Err(_) => {
        let BufResult(read_res, return_chunk) = stream.read(chunk).await;
        chunk = return_chunk;
        match read_res {
          Ok(0) => return Err(Error::Other("EOF".into())),
          Ok(n) => read_buf.extend_from_slice(&chunk[..n]),
          Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
            return Err(Error::Other("EOF".into()));
          }
          Err(e) => return Err(e.into()),
        }
        continue;
      }
    }

    for item in &queue {
      out_buf.extend_from_slice(&item.frame);
    }
    let BufResult(write_res, mut buf) = stream.write_all(out_buf).await;
    write_res?;
    buf.clear();
    out_buf = buf;

    // 发出即忘帧写出即完成（协议约定无应答），清出队列避免下轮重复写出
    queue.retain(|item| !matches!(item.resp_tx, ReplyTx::None));

    while !queue.is_empty() {
      let BufResult(read_res, return_chunk) = stream.read(chunk).await;
      chunk = return_chunk;
      let n = read_res?;
      if n == 0 {
        return Err(Error::Other("EOF".into()));
      }
      read_buf.extend_from_slice(&chunk[..n]);

      let mut data = read_buf.as_slice();
      let mut consumed = 0;
      while !data.is_empty() && !queue.is_empty() {
        let before = data.len();
        // 弹出队首派发：解析完整即回传应答，未到齐则原样退回队列等下一个读事件
        let mut front = queue.pop_front().unwrap();
        let complete = match front.resp_tx {
          // 发出即忘：无应答可等，直接完成（保留分支防未 retain 清空的残留项）
          ReplyTx::None => true,
          ReplyTx::Str(tx) => match parse_scalar(&mut data)? {
            Some(reply) => {
              tx.send(reply);
              true
            }
            None => {
              front.resp_tx = ReplyTx::Str(tx);
              queue.push_front(front);
              false
            }
          },
          ReplyTx::Bytes(tx) => match parse_bytes(&mut data)? {
            Some(reply) => {
              tx.send(reply);
              true
            }
            None => {
              front.resp_tx = ReplyTx::Bytes(tx);
              queue.push_front(front);
              false
            }
          },
          ReplyTx::Array(tx) => match parse_array(&mut data)? {
            Some(reply) => {
              tx.send(reply);
              true
            }
            None => {
              front.resp_tx = ReplyTx::Array(tx);
              queue.push_front(front);
              false
            }
          },
        };
        if !complete {
          break; // 应答不完整：保留 read_buf 原样，等下一个读事件整体重试
        }
        consumed += before - data.len();
      }
      read_buf.drain(..consumed);
    }
  }
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn encode_command_frame() {
    let mut out = Vec::new();
    encode_bytes_command(&mut out, &[b"GET", b"k1"]);
    assert_eq!(&out, b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n");

    let mut out_str = Vec::new();
    encode_str_command(&mut out_str, &["GET", "k1"]);
    assert_eq!(&out_str, b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n");
  }
}
