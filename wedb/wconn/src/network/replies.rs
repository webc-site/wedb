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

/// 行应答文本解码：`+`/`:` 为 lossy（对位 C# 侧行串构造形态），`,`/`#` 严格
/// UTF-8（对位 token_line 形态）
fn decode_text(sigil: u8, body: &[u8]) -> Result<String> {
  match sigil {
    b',' | b'#' => Ok(from_utf8(body)?.to_string()),
    _ => Ok(String::from_utf8_lossy(body).into_owned()),
  }
}

/// bulk 载荷文本化（None = null bulk → 空串；严格 UTF-8 保持现网行为）
fn bulk_text(bulk: Option<&[u8]>) -> Result<String> {
  match bulk {
    Some(s) => Ok(from_utf8(s)?.to_string()),
    None => Ok(String::new()),
  }
}

/// 三形应答解析共有骨架：+OK 快路径与 sigil 派发单点
///
/// `parse_bytes`/`parse_scalar`/`parse_array` 在 `+OK` 快路径、行读（`+ : , #`）、
/// `-` 错误行、`$` bulk、`_` RESP3 null 与非法 token 拒绝上同构，仅目标形态
/// 构造不同——本函数承担派发骨架，臂差由四个闭包承接：
/// - `line`：`+ : , #` 经 token_span 读得的行体字节（bytes 形直接借用，文本形按 sigil 解码）
/// - `bulk`：`$` 载荷（None = null bulk）
/// - `array`：`* ~ >` 臂整体交各形（bytes/scalar 走 bulk 元素取首元，array 形走字符串元素整数组，
///   两路元素解析语义不同故不可再合）
/// - `null`：`_` 的各形空值
///
/// `Ok(None)` 统一表「应答未到齐」（各臂子读自滚回帧边界前）；`-` 错误臂三形
/// 同构，单点产出内层 Err
#[inline]
fn dispatch<T>(
  data: &mut &[u8],
  mut line: impl FnMut(u8, &[u8]) -> Result<T>,
  mut bulk: impl FnMut(Option<&[u8]>) -> Result<T>,
  mut array: impl FnMut(&mut &[u8]) -> Result<Option<Result<T>>>,
  mut null: impl FnMut() -> T,
) -> Result<Option<Result<T>>> {
  if data.is_empty() {
    return Ok(None);
  }
  // +OK\r\n 最常见应答快路径（对标 C# 三链的 +OK 快速推进臂）
  if data.starts_with(OK_PREFIX) {
    *data = &data[OK_PREFIX_LEN..];
    return Ok(Some(Ok(line(b'+', b"OK")?)));
  }
  match data[0] {
    // 简单串/整数/RESP3 浮点与布尔共用行读取路径
    b'+' | b':' | b',' | b'#' => {
      let sigil = data[0];
      match RespReadResponseUtils::try_read_token_span(data, sigil)? {
        None => Ok(None),
        Some(body) => Ok(Some(Ok(line(sigil, body)?))),
      }
    }
    b'-' => {
      Ok(RespReadResponseUtils::try_read_error_as_string(data)?.map(|e| Err(Error::Server(e))))
    }
    b'$' => match RespReadResponseUtils::try_read_byte_slice_with_length_header(data)? {
      None => Ok(None),
      Some(b) => Ok(Some(Ok(bulk(b)?))),
    },
    b'*' | b'~' | b'>' => array(data),
    b'_' => Ok(RespReadResponseUtils::try_read_null(data)?.map(|_| Ok(null()))),
    b => Err(RespReadResponseUtils::unexpected_token(b)),
  }
}

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
  // 标量臂直接借用行体/bulk 字节；`* ~ >` 取首元素（对标 C# ProcessReplyAsMemoryByte）
  dispatch(
    data,
    |_, body| Ok(body.to_vec()),
    |b| Ok(b.unwrap_or_default().to_vec()),
    |data| {
      Ok(
        RespReadResponseUtils::try_read_byte_slice_array_with_length_header(data, 1)?.map(|a| {
          Ok(
            a.and_then(|v| v.first().copied())
              .unwrap_or_default()
              .to_vec(),
          )
        }),
      )
    },
    Vec::new,
  )
}

/// 将一条标量应答（简单串/错误串/整数/bulk string/array首元素/RESP3 null）解析为 Result<String>；
/// 应答不完整返回 Ok(None)
///
/// 在 garnet 中的相对路径: libs/client/GarnetClientProcessReplies.cs:ProcessReplyAsString
fn parse_scalar(data: &mut &[u8]) -> Result<Option<Result<String>>> {
  dispatch(
    data,
    decode_text,
    bulk_text,
    // 标量分支遇到数组应答：返回首元素（对标 C# ProcessReplyAsString case '*'）
    |data| {
      Ok(
        match RespReadResponseUtils::try_read_byte_slice_array_with_length_header(data, 1)? {
          None => None,
          Some(None) => Some(Ok(String::new())),
          Some(Some(arr)) => Some(Ok(bulk_text(arr.first().copied())?)),
        },
      )
    },
    String::new,
  )
}

/// 将一条数组应答解析为 Result<Vec<String>>；不完整返回 Ok(None)
///
/// 标量应答（+/-/:/$）按 C# ProcessReplyAsStringArray 语义包装为单元素数组
///
/// 在 garnet 中的相对路径: libs/client/GarnetClientProcessReplies.cs:ProcessReplyAsStringArray
fn parse_array(data: &mut &[u8]) -> Result<Option<Result<Vec<String>>>> {
  dispatch(
    data,
    |sigil, body| Ok(vec![decode_text(sigil, body)?]),
    |b| Ok(vec![bulk_text(b)?]),
    // 数组形走字符串元素整数组臂（与 bytes/scalar 的首元臂元素语义不同源）
    |data| {
      Ok(
        RespReadResponseUtils::try_read_string_array_with_length_header(data, 1)?
          .map(|a| Ok(a.unwrap_or_default())),
      )
    },
    Vec::new,
  )
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

/// 三形应答共有认领骨架：解析一帧，完整即出队队首发往应答通道
///
/// Str/Bytes/Array 三臂仅解析形态与目标通道载荷不同（`TxOneshot` 三种具体
/// 载荷类型异构，无公共 trait，泛型 `R` 承接；避免 dyn），解析→出队→发送
/// 序列单点收敛。队首变体与解析形态严格配对（外层 match 已按变体选形），
/// `deliver` 内 if-let 变体不符属状态机不变式违例，静默丢弃（与原逐臂
/// if-let 同义）；不完整回 `false`，队列与游标原地留守等后续读事件续认领。
#[inline]
fn claim<R>(
  data: &mut &[u8],
  queue: &mut VecDeque<CommandItem>,
  parse: impl FnOnce(&mut &[u8]) -> Result<Option<Result<R>>>,
  deliver: impl FnOnce(CommandItem, Result<R>),
) -> Result<bool> {
  let Some(reply) = parse(data)? else {
    return Ok(false);
  };
  if let Some(item) = queue.pop_front() {
    deliver(item, reply);
  }
  Ok(true)
}

/// 派发游标后全部完整应答
///
/// 在 garnet 中的相对路径:
/// - `libs/client/ClientSession/GarnetClientSession.cs:TryConsumeMessages`
/// - `libs/client/GarnetClientProcessReplies.cs:ProcessReplies`
///
/// 持久消费游标 `read_head` 跨派发调用驻留读泵（对标 C# ProcessReplies 的
/// `readHead` 与兄弟服务端 `RespServerSession.read_head`）：自游标处借出切片
/// 逐条解析队首应答，完整即出队回传并只推进游标、不搬字节，以「游标后无完整
/// 应答或在途补位枯竭」为退出条件；未消费残余原地留在 `read_buf[read_head..]`，
/// 等后续读事件于尾部拼接后自游标续认领。整段消费完（游标追平缓冲尾）才清零
/// 复位缓冲与游标，回收已消费前缀（对标服务端整段 drain → clear）；残余段物理
/// 前移不在派发内做，只在读泵尾部空间不足时一次性执行，热路径帧间零搬移。
/// progress 在位时逐应答推进回收计数（对标 C# tcsOffset 随结果处理递增）
pub fn dispatch_replies(
  queue: &mut VecDeque<CommandItem>,
  in_flight_rx: &AsyncRx<mpsc::Array<CommandItem>>,
  read_buf: &mut Vec<u8>,
  read_head: &mut usize,
  progress: Option<&PumpProgress>,
) -> Result<()> {
  let mut data = &read_buf[*read_head..];
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
      break; // 游标后无未消费字节：完整应答已全部就地认领
    }
    // 发出即忘项不入在途队列（写泵未记入队，派发队列仅由 in_flight_rx 供给）：
    // 队首 None 属状态机不变式违例，fail-fast 退出派发循环交断连路收口
    //（静默记完成即原地空转）
    let before = data.len();
    let complete = match &queue.front().expect("队首在位").resp_tx {
      ReplyTx::None => break,
      ReplyTx::Str(_) => claim(&mut data, queue, parse_scalar, |item, reply| {
        if let CommandItem {
          resp_tx: ReplyTx::Str(tx),
          ..
        } = item
        {
          tx.send(reply);
        }
      })?,
      ReplyTx::Bytes(_) => claim(&mut data, queue, parse_bytes, |item, reply| {
        if let CommandItem {
          resp_tx: ReplyTx::Bytes(tx),
          ..
        } = item
        {
          tx.send(reply);
        }
      })?,
      ReplyTx::Array(_) => claim(&mut data, queue, parse_array, |item, reply| {
        if let CommandItem {
          resp_tx: ReplyTx::Array(tx),
          ..
        } = item
        {
          tx.send(reply);
        }
      })?,
    };
    if !complete {
      break; // 应答不完整：游标停在帧边界不动，残余留游标后等下一个读事件拼接续认领
    }
    // 认领完成即推进回收计数（tcsOffset 同源语义）
    if let Some(p) = progress {
      p.record_reclaimed();
    }
    // 帧认领完成：仅推进持久游标，热路径零物理搬移（对标 C# readHead = ptr - recvBufferPtr）
    *read_head += before - data.len();
  }
  // 整段消费完毕：清零复位缓冲与游标，回收已消费前缀（残余段前移交读泵按尾部空间触发）
  if *read_head == read_buf.len() {
    read_buf.clear();
    *read_head = 0;
  }
  Ok(())
}
