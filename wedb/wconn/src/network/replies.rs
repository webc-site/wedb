//! 帧解析与队列派发：RESP 应答逐条解析 + 在途命令队列按帧序认领
//!
//! 在 garnet 中的相对路径: `libs/client/GarnetClientProcessReplies.cs`

use core::str::from_utf8;
use std::collections::VecDeque;

use crossfire::{AsyncRx, mpsc};

use crate::{
  Error, Result,
  parser::RespReadResponseUtils,
  types::{CommandItem, PumpProgress, ReplyTx},
};

/// +OK\r\n 常量前缀与长度
const OK_PREFIX: &[u8] = b"+OK\r\n";
const OK_PREFIX_LEN: usize = OK_PREFIX.len();
const OK_STR: &str = "OK";

/// 解析字节切片应答（单元素语义：一条应答 → 一个字节串）；不完整返回 Ok(None)
///
/// 在 garnet 中的相对路径: libs/client/GarnetClientProcessReplies.cs:ProcessReplyAsMemoryByte
///
/// 对位边界：C# ProcessReplyAsMemoryByte（out MemoryResult<byte>）同为单元素形态
/// （+OK\r\n 快路径、`*` 数组取首元素 result = resultArray[0]）；本函数额外的
/// `,`/`#`/`_`/`~`/`>` 分支是 parse_scalar/parse_array 共有的 RESP3 统一扩展，
/// 非形态分叉。C# ProcessReplyAsMemoryByteArray（out MemoryResult<byte>[]，仅 `*`、
/// 完整数组）服务于 ExecuteForMemoryResultArrayAsync / StringGetAsMemoryAsync 多键
/// 批量通道，rust 无此批量字节 API；数组应答统一由 parse_array（对位
/// ProcessReplyAsStringArray）承接，故 ByteArray 无 1:1 转写对位。
fn parse_bytes(data: &mut &[u8]) -> Result<Option<Result<Vec<u8>>>> {
  if data.is_empty() {
    return Ok(None);
  }
  if data.starts_with(OK_PREFIX) {
    *data = &data[OK_PREFIX_LEN..];
    return Ok(Some(Ok(b"OK".to_vec())));
  }
  match data[0] {
    // 简单串/整数/RESP3 浮点与布尔共用行读取路径
    b'+' | b':' | b',' | b'#' => {
      Ok(RespReadResponseUtils::try_read_token_span(data, data[0])?.map(|s| Ok(s.to_vec())))
    }
    b'-' => {
      Ok(RespReadResponseUtils::try_read_error_as_string(data)?.map(|e| Err(Error::Server(e))))
    }
    b'$' => Ok(
      RespReadResponseUtils::try_read_byte_slice_with_length_header(data)?
        .map(|b| Ok(b.unwrap_or_default().to_vec())),
    ),
    b'*' | b'~' | b'>' => Ok(
      RespReadResponseUtils::try_read_byte_slice_array_with_length_header(data)?.map(|a| {
        Ok(
          a.and_then(|v| v.first().copied())
            .unwrap_or_default()
            .to_vec(),
        )
      }),
    ),
    b'_' => Ok(RespReadResponseUtils::try_read_null(data)?.map(|_| Ok(Vec::new()))),
    b => Err(RespReadResponseUtils::unexpected_token(b)),
  }
}

/// 将一条标量应答（简单串/错误串/整数/bulk string/array首元素/RESP3 null）解析为 Result<String>；
/// 应答不完整返回 Ok(None)
///
/// 在 garnet 中的相对路径: libs/client/GarnetClientProcessReplies.cs:ProcessReplyAsString
fn parse_scalar(data: &mut &[u8]) -> Result<Option<Result<String>>> {
  // +OK\r\n 最常见应答快路径（对标 C# ProcessReplyAsString: +OK\r\n 快速推进）
  if data.starts_with(OK_PREFIX) {
    *data = &data[OK_PREFIX_LEN..];
    return Ok(Some(Ok(OK_STR.to_string())));
  }
  match data[0] {
    b'+' => Ok(RespReadResponseUtils::try_read_simple_string(data)?.map(Ok)),
    b'-' => {
      Ok(RespReadResponseUtils::try_read_error_as_string(data)?.map(|e| Err(Error::Server(e))))
    }
    b':' => Ok(RespReadResponseUtils::try_read_integer_as_string(data)?.map(Ok)),
    b'$' => match RespReadResponseUtils::try_read_byte_slice_with_length_header(data)? {
      None => Ok(None),
      Some(None) => Ok(Some(Ok(String::new()))),
      Some(Some(slice)) => {
        let s = from_utf8(slice)?;
        Ok(Some(Ok(s.to_string())))
      }
    },
    // 标量分支遇到数组应答：返回首元素（对标 C# ProcessReplyAsString case '*'）
    b'*' | b'~' | b'>' => {
      match RespReadResponseUtils::try_read_byte_slice_array_with_length_header(data)? {
        None => Ok(None),
        Some(None) => Ok(Some(Ok(String::new()))),
        Some(Some(arr)) => {
          if let Some(&slice) = arr.first() {
            let s = from_utf8(slice)?;
            Ok(Some(Ok(s.to_string())))
          } else {
            Ok(Some(Ok(String::new())))
          }
        }
      }
    }
    // RESP3 null 标量支持
    b'_' => Ok(RespReadResponseUtils::try_read_null(data)?.map(|_| Ok(String::new()))),
    // RESP3 浮点 / 布尔标量支持
    b',' | b'#' => {
      Ok(RespReadResponseUtils::try_read_token_line(data, data[0])?.map(|s| Ok(s.to_string())))
    }
    b => Err(RespReadResponseUtils::unexpected_token(b)),
  }
}

/// 将一条数组应答解析为 Result<Vec<String>>；不完整返回 Ok(None)
///
/// 标量应答（+/-/:/$）按 C# ProcessReplyAsStringArray 语义包装为单元素数组
///
/// 在 garnet 中的相对路径: libs/client/GarnetClientProcessReplies.cs:ProcessReplyAsStringArray
fn parse_array(data: &mut &[u8]) -> Result<Option<Result<Vec<String>>>> {
  // +OK\r\n 快路径（对标 C# ProcessReplyAsStringArray）
  if data.starts_with(OK_PREFIX) {
    *data = &data[OK_PREFIX_LEN..];
    return Ok(Some(Ok(vec![OK_STR.to_string()])));
  }
  match data[0] {
    b'*' | b'~' | b'>' => RespReadResponseUtils::try_read_string_array_with_length_header(data)
      .map(|a| a.map(|a| Ok(a.unwrap_or_default()))),
    // 标量分支：包装为单元素数组（对齐 C# ProcessReplyAsStringArray）
    b'+' => Ok(RespReadResponseUtils::try_read_simple_string(data)?.map(|s| Ok(vec![s]))),
    b':' => Ok(RespReadResponseUtils::try_read_integer_as_string(data)?.map(|s| Ok(vec![s]))),
    b'$' => match RespReadResponseUtils::try_read_byte_slice_with_length_header(data)? {
      None => Ok(None),
      Some(None) => Ok(Some(Ok(vec![String::new()]))),
      Some(Some(slice)) => {
        let s = from_utf8(slice)?;
        Ok(Some(Ok(vec![s.to_string()])))
      }
    },
    b'-' => {
      Ok(RespReadResponseUtils::try_read_error_as_string(data)?.map(|e| Err(Error::Server(e))))
    }
    // RESP3 null 数组分支：空列表
    b'_' => Ok(RespReadResponseUtils::try_read_null(data)?.map(|_| Ok(Vec::new()))),
    // RESP3 浮点 / 布尔
    b',' | b'#' => Ok(
      RespReadResponseUtils::try_read_token_line(data, data[0])?.map(|s| Ok(vec![s.to_string()])),
    ),
    b => Err(RespReadResponseUtils::unexpected_token(b)),
  }
}

/// 滞留错误应答探测（fire-and-forget 帧拒收感知）：滞留错误应答探测辅助
/// read_buf 残留 `-ERR...\r\n` 完整错误行 = 应答队列已空、无人认领的服务端
/// 错误应答，判流失效。仅认领 `-` 前导的完整行（半包行等待后续字节）；其余
/// 滞留字节不构成失效证据（保守放行，正常记录帧流本无应答）
pub(super) fn orphan_error_reply(read_buf: &[u8]) -> Option<String> {
  if read_buf.first() != Some(&b'-') {
    return None;
  }
  let mut data = read_buf;
  RespReadResponseUtils::try_read_error_as_string(&mut data)
    .ok()
    .flatten()
}

/// 派发 read_buf 中全部完整应答
///
/// 在 garnet 中的相对路径:
/// - `libs/client/ClientSession/GarnetClientSession.cs:TryConsumeMessages`
/// - `libs/client/GarnetClientProcessReplies.cs:ProcessReplies`
///
/// 从在途通道补位队列后逐条解析队首应答，完整即出队回传，以「read_buf 无完整应答或
/// 在途补位枯竭」为退出条件；未消费字节保留 read_buf 原样，等后续读事件
/// 拼接后整体重试。progress 在位时逐应答推进回收计数（对标 C# tcsOffset 随结果处理递增）
pub(super) fn dispatch_replies(
  queue: &mut VecDeque<CommandItem>,
  in_flight_rx: &AsyncRx<mpsc::Array<CommandItem>>,
  read_buf: &mut Vec<u8>,
  progress: Option<&PumpProgress>,
) -> Result<()> {
  let mut data = read_buf.as_slice();
  let mut consumed = 0;
  loop {
    // 在途命令补位（非阻塞）：与写泵帧序 FIFO 严格对齐
    if queue.is_empty() {
      match in_flight_rx.try_recv() {
        Ok(item) => queue.push_back(item),
        // 在途枯竭：无可认领应答，残余字节等命令入队后对齐
        Err(_) => break,
      }
    }
    if data.is_empty() {
      break; // 缓冲排空：完整应答已全部就地认领
    }
    // 弹出队首派发：解析完整即回传应答，未到齐则原样退回队列等后续读事件
    let before = data.len();
    let Some(mut front) = queue.pop_front() else {
      break;
    };
    // 发出即忘项不入在途队列（写泵未记入队），回收计数同样不推进
    let fire_and_forget = matches!(front.resp_tx, ReplyTx::None);
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
    // 认领完成即推进回收计数（tcsOffset 同源语义）
    if let Some(p) = progress
      && !fire_and_forget
    {
      p.record_reclaimed();
    }
    consumed += before - data.len();
  }
  if consumed > 0 {
    if consumed == read_buf.len() {
      read_buf.clear();
    } else {
      read_buf.copy_within(consumed.., 0);
      read_buf.truncate(read_buf.len() - consumed);
    }
  }
  Ok(())
}
