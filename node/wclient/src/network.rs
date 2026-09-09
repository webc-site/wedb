//! 客户端网络读写泵：命令帧编码 + 单连接请求/应答循环
//!
//! [`GarnetClient`](crate::GarnetClient) 与
//! [`GarnetClientSession`](crate::GarnetClientSession) 共用同一实现，
//! 对标 libs/client/GarnetClientProcessReplies.cs:ProcessReplies（C# 侧同样的
//! 应答泵在 GarnetClient/GarnetClientSession 各有一份，此处收敛为单一定义）。
//!
//! 循环每轮：阻塞收一条命令 → 非阻塞清空通道积压 → 一次性批量写入 →
//! 循环读取应答按 FIFO 逐条派发；应答不完整时保留未消费字节等下一个读事件。

use std::collections::VecDeque;

use compio::{
  buf::BufResult,
  io::{AsyncRead, AsyncWriteExt},
  net::TcpStream,
};
use crossfire::{AsyncRx, mpsc};
use itoa::Buffer;

use crate::{CommandItem, Error, RespReadResponseUtils, Result};

/// 单次 socket 读取块大小
const READ_CHUNK: usize = 16 * 1024;
/// 应答累积缓冲初始容量
const READ_BUF_CAP: usize = 8 * 1024;

/// 把通道中此刻已就绪的命令全部取出排入队列（批量写入摊薄 syscall 次数）
async fn drain_channel(
  rx: &AsyncRx<mpsc::Array<CommandItem>>,
  queue: &mut VecDeque<CommandItem>,
) -> Option<()> {
  // 阻塞收第一条：队列为空时没有可写内容，必须等待新命令
  let first = rx.recv().await.ok()?;
  queue.push_back(first);
  while let Ok(item) = rx.try_recv() {
    queue.push_back(item);
  }
  Some(())
}

/// 将一条命令编码为 RESP2 数组帧追加到 out（itoa 直写，无 format! 中间分配）
fn encode_command(out: &mut Vec<u8>, cmd: &[String], num: &mut Buffer) {
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

/// 将一条标量应答（简单串/错误串/整数/bulk string）解析为 Result<String>；
/// 应答不完整返回 Ok(None)（此时 data 可能已前移，调用方须整体重试）
fn parse_scalar(data: &mut &[u8]) -> Result<Option<Result<String>>> {
  match data[0] {
    b'+' => RespReadResponseUtils::try_read_simple_string(data).map(|s| s.map(Ok)),
    b'-' => {
      RespReadResponseUtils::try_read_error_as_string(data).map(|e| e.map(|e| Err(Error::Other(e))))
    }
    b':' => RespReadResponseUtils::try_read_integer_as_string(data).map(|s| s.map(Ok)),
    b'$' => RespReadResponseUtils::try_read_string_with_length_header(data)
      .map(|s| s.map(|s| Ok(s.unwrap_or_default()))),
    _ => Err(unexpected_token(data[0])),
  }
}

/// 将一条数组应答解析为 Result<Vec<String>>；不完整返回 Ok(None)
fn parse_array(data: &mut &[u8]) -> Result<Option<Result<Vec<String>>>> {
  match data[0] {
    b'*' => RespReadResponseUtils::try_read_string_array_with_length_header(data)
      .map(|a| a.map(|a| Ok(a.unwrap_or_default()))),
    b'-' => {
      RespReadResponseUtils::try_read_error_as_string(data).map(|e| e.map(|e| Err(Error::Other(e))))
    }
    _ => Err(unexpected_token(data[0])),
  }
}

#[inline]
fn unexpected_token(b: u8) -> Error {
  Error::Other(format!("Unexpected token {}", b as char))
}

/// 网络循环主泵（详见模块文档）
pub(super) async fn network_loop(
  mut stream: TcpStream,
  rx: AsyncRx<mpsc::Array<CommandItem>>,
) -> Result<()> {
  let mut queue: VecDeque<CommandItem> = VecDeque::new();
  let mut read_buf: Vec<u8> = Vec::with_capacity(READ_BUF_CAP);
  // 读取块全程复用，按 compio 约定每次 read 归还后接着用，避免每读一次分配清零一次
  let mut chunk = vec![0u8; READ_CHUNK];
  let mut num = Buffer::new();

  loop {
    if drain_channel(&rx, &mut queue).await.is_none() {
      break; // 调用端已全部断开
    }

    let mut out_buf = Vec::new();
    for item in &queue {
      let cmd = match item {
        CommandItem::Command { cmd, .. } | CommandItem::CommandForArray { cmd, .. } => cmd,
      };
      encode_command(&mut out_buf, cmd, &mut num);
    }

    if !out_buf.is_empty() {
      let res = stream.write_all(out_buf).await.0;
      res?;
    }

    while !queue.is_empty() {
      let BufResult(res, buf) = stream.read(chunk).await;
      chunk = buf;
      let n = res?;
      if n == 0 {
        return Err(Error::Other("EOF".into()));
      }
      read_buf.extend_from_slice(&chunk[..n]);

      let mut data = read_buf.as_slice();
      let mut consumed = 0;
      while !data.is_empty() && !queue.is_empty() {
        let before = data.len();
        let front = queue.front().unwrap();
        let complete = match front {
          CommandItem::Command { .. } => match parse_scalar(&mut data)? {
            Some(reply) => {
              if let CommandItem::Command { resp_tx, .. } = queue.pop_front().unwrap() {
                resp_tx.send(reply);
              }
              true
            }
            None => false,
          },
          CommandItem::CommandForArray { .. } => match parse_array(&mut data)? {
            Some(reply) => {
              if let CommandItem::CommandForArray { resp_tx, .. } = queue.pop_front().unwrap() {
                resp_tx.send(reply);
              }
              true
            }
            None => false,
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
  use itoa::Buffer;

  use super::encode_command;

  #[test]
  fn encode_command_frame() {
    let cmd = vec!["GET".to_string(), "k1".to_string()];
    let mut out = Vec::new();
    encode_command(&mut out, &cmd, &mut Buffer::new());
    assert_eq!(&out, b"*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n");
  }
}
