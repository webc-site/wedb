use compio::{net::TcpStream, runtime::spawn};
use crossfire::{mpsc, oneshot};
use itoa::Buffer;

use crate::{
  Error, Result, network,
  types::{CHANNEL_CAP, ChannelTx, CommandItem, ReplyTx, roundtrip},
};

/// libs/client/ClientSession/GarnetClientSession.cs:GarnetClientSession
pub struct GarnetClientSession {
  pub end_point: String,
  auth_username: Option<String>,
  auth_password: Option<String>,
  client_name: Option<String>,

  tx: Option<ChannelTx>,
}

impl GarnetClientSession {
  /// libs/client/ClientSession/GarnetClientSession.cs:GarnetClientSession
  pub fn new(
    endpoint: String,
    auth_username: Option<String>,
    auth_password: Option<String>,
    client_name: Option<String>,
  ) -> Self {
    Self {
      end_point: endpoint,
      auth_username,
      auth_password,
      client_name,
      tx: None,
    }
  }

  /// 请求通道引用（未连接即报错）
  fn channel(&self) -> Result<&ChannelTx> {
    self
      .tx
      .as_ref()
      .ok_or_else(|| Error::Other("Not connected".into()))
  }

  /// libs/client/ClientSession/GarnetClientSession.cs:ConnectAsync
  pub async fn connect_async(&mut self) -> Result<()> {
    let stream = TcpStream::connect(&self.end_point).await?;
    let (tx, rx) = mpsc::bounded_async(CHANNEL_CAP);
    self.tx = Some(tx);

    spawn(async move {
      if let Err(e) = network::network_loop(stream, rx).await {
        log::error!("GarnetClientSession 网络循环退出: {e}");
      }
    })
    .detach();

    let handshake = network::handshake(
      self.channel()?,
      "GarnetClientSession",
      self.auth_username.as_deref(),
      self.auth_password.as_deref(),
      self.client_name.as_deref(),
    )
    .await;
    if handshake.is_err() {
      // 握手失败（AUTH/SETINFO 报错，C# 同样上抛）：丢弃通道使网络泵退出、
      // 连接关闭，避免僵尸连接上继续排队后续命令
      self.tx = None;
    }
    handshake
  }

  /// libs/client/ClientSession/AsyncGarnetClientSession.cs:ExecuteAsync
  /// libs/client/ClientSession/AsyncGarnetClientSession.cs:ExecuteAsyncBatch
  ///
  /// C# ExecuteAsyncBatch(params string[]) 为单命令变长参数入队 + TCS 等待，
  /// 与 ExecuteAsync 同构 (rust 无 TCS 队列，由 network_loop 按需泵出)，
  /// 故两者共用此实现
  pub async fn execute_async(&self, command: &[&str]) -> Result<String> {
    let (resp_tx, resp_rx) = oneshot::oneshot();
    let item = CommandItem::new_str(command, ReplyTx::Str(resp_tx));
    roundtrip(self.channel()?, item, resp_rx).await
  }

  /// libs/client/ClientSession/AsyncGarnetClientSession.cs:ExecuteForMemoryResultWithCancellationAsync
  pub async fn execute_for_bytes_async(&self, command: &[&[u8]]) -> Result<Vec<u8>> {
    let (resp_tx, resp_rx) = oneshot::oneshot();
    let item = CommandItem::new_bytes(command, ReplyTx::Bytes(resp_tx));
    roundtrip(self.channel()?, item, resp_rx).await
  }

  /// libs/client/ClientSession/AsyncGarnetClientSession.cs:ExecuteForArrayAsync
  pub async fn execute_for_array_async(&self, command: &[&str]) -> Result<Vec<String>> {
    let (resp_tx, resp_rx) = oneshot::oneshot();
    let item = CommandItem::new_str(command, ReplyTx::Array(resp_tx));
    roundtrip(self.channel()?, item, resp_rx).await
  }

  /// libs/client/ClientSession/GarnetClientSession.cs:ExecuteClusterAppendLogInit
  ///
  /// AOF 复制流初始化握手（7 元素 RESP 数组：CLUSTER APPENDLOG nodeId
  /// sublogIdx previousAddress currentAddress nextAddress），等待对端 +OK
  ///（三方地址取 -1/-1/-1 时为 C# 规定的初始化消息语义）
  pub async fn execute_cluster_append_log_init(
    &self,
    node_id: &str,
    physical_sublog_idx: usize,
    previous_address: i64,
    current_address: i64,
    next_address: i64,
  ) -> Result<String> {
    let frame = encode_append_log_init_frame(
      node_id,
      physical_sublog_idx,
      previous_address,
      current_address,
      next_address,
    );

    let (resp_tx, resp_rx) = oneshot::oneshot();
    let item = CommandItem {
      frame,
      resp_tx: ReplyTx::Str(resp_tx),
    };
    roundtrip(self.channel()?, item, resp_rx).await
  }

  /// libs/client/ClientSession/GarnetClientSession.cs:ExecuteClusterAppendLog
  ///
  /// 逐记录 AOF 帧转发（8 元素 RESP 数组，末位 payload 为完整记录帧的二进制
  /// bulk string）。发出即忘：不注册应答等待（对端对记录帧不回写应答），
  /// 对标 C# 无 tcs 入队且不递增 numCommands 的路径。通道饱和时返回
  /// Err（调用方持久化重试，对标 C# 发送缓冲满时 Send 内部 Flush 的阻塞
  /// 背压由上层承接）
  pub fn execute_cluster_append_log(
    &self,
    node_id: &str,
    physical_sublog_idx: usize,
    previous_address: i64,
    current_address: i64,
    next_address: i64,
    payload: &[u8],
  ) -> Result<()> {
    let frame = encode_append_log_frame(
      node_id,
      physical_sublog_idx,
      previous_address,
      current_address,
      next_address,
      payload,
    );

    let item = CommandItem {
      frame,
      resp_tx: ReplyTx::None,
    };
    match self.channel()?.try_send(item) {
      Ok(()) => Ok(()),
      Err(crossfire::TrySendError::Full(_)) => Err(Error::Other("Send channel saturated".into())),
      Err(crossfire::TrySendError::Disconnected(_)) => {
        Err(Error::Other("Network loop died".into()))
      }
    }
  }

  /// 连接健康面（对标 C# GarnetClientSession.IsConnected）
  ///
  /// 通道在位且网络泵仍存活：泵任务退出（对端断链 / EOF / 协议错误）即
  /// 判定断连，C# networkSender.IsConnected 的 socket 态感知投影
  pub fn is_connected(&self) -> bool {
    self.tx.as_ref().is_some_and(|tx| !tx.is_disconnected())
  }
}

/// 初始化帧常量前缀（*7 + CLUSTER + APPENDLOG）
const APPEND_LOG_INIT_PREFIX: &[u8] = b"*7\r\n$7\r\nCLUSTER\r\n$9\r\nAPPENDLOG\r\n";
/// 记录帧常量前缀（*8 + CLUSTER + APPENDLOG）
const APPEND_LOG_FRAME_PREFIX: &[u8] = b"*8\r\n$7\r\nCLUSTER\r\n$9\r\nAPPENDLOG\r\n";

/// 编码 CLUSTER APPENDLOG 7 元素初始化 RESP 数组帧
pub fn encode_append_log_init_frame(
  node_id: &str,
  physical_sublog_idx: usize,
  previous_address: i64,
  current_address: i64,
  next_address: i64,
) -> Vec<u8> {
  let mut frame = Vec::with_capacity(80 + node_id.len());
  let mut num = Buffer::new();
  frame.extend_from_slice(APPEND_LOG_INIT_PREFIX);
  write_bulk_bytes(&mut frame, &mut num, node_id.as_bytes());
  write_bulk_int(&mut frame, &mut num, physical_sublog_idx as i64);
  write_bulk_int(&mut frame, &mut num, previous_address);
  write_bulk_int(&mut frame, &mut num, current_address);
  write_bulk_int(&mut frame, &mut num, next_address);
  frame
}

/// 编码 CLUSTER APPENDLOG 8 元素 RESP 数组帧
pub fn encode_append_log_frame(
  node_id: &str,
  physical_sublog_idx: usize,
  previous_address: i64,
  current_address: i64,
  next_address: i64,
  payload: &[u8],
) -> Vec<u8> {
  let mut frame = Vec::with_capacity(88 + node_id.len() + payload.len());
  let mut num = Buffer::new();
  frame.extend_from_slice(APPEND_LOG_FRAME_PREFIX);
  write_bulk_bytes(&mut frame, &mut num, node_id.as_bytes());
  write_bulk_int(&mut frame, &mut num, physical_sublog_idx as i64);
  write_bulk_int(&mut frame, &mut num, previous_address);
  write_bulk_int(&mut frame, &mut num, current_address);
  write_bulk_int(&mut frame, &mut num, next_address);
  write_bulk_bytes(&mut frame, &mut num, payload);
  frame
}

/// 编码 CLUSTER APPENDLOG RESP 数组帧（复制发送面统一门面）
///
/// `payload == None` 即 7 元素初始化帧（C# ExecuteClusterAppendLogInit），
/// `Some(bytes)` 即 8 元素记录帧（C# ExecuteClusterAppendLog）
pub fn encode_cluster_append_log_frame(
  node_id: &str,
  physical_sublog_idx: usize,
  previous_address: i64,
  current_address: i64,
  next_address: i64,
  payload: Option<&[u8]>,
) -> Vec<u8> {
  match payload {
    Some(payload) => encode_append_log_frame(
      node_id,
      physical_sublog_idx,
      previous_address,
      current_address,
      next_address,
      payload,
    ),
    None => encode_append_log_init_frame(
      node_id,
      physical_sublog_idx,
      previous_address,
      current_address,
      next_address,
    ),
  }
}
/// 写 RESP 数组头 `*<n>\r\n`
#[cfg(test)]
fn write_array_header(out: &mut Vec<u8>, num: &mut Buffer, count: usize) {
  out.push(b'*');
  out.extend_from_slice(num.format(count).as_bytes());
  out.extend_from_slice(b"\r\n");
}

/// 写二进制 bulk string `$<len>\r\n<bytes>\r\n`
fn write_bulk_bytes(out: &mut Vec<u8>, num: &mut Buffer, bytes: &[u8]) {
  out.push(b'$');
  out.extend_from_slice(num.format(bytes.len()).as_bytes());
  out.extend_from_slice(b"\r\n");
  out.extend_from_slice(bytes);
  out.extend_from_slice(b"\r\n");
}

/// 写整数 bulk string `$<len>\r\n<digits>\r\n`（对标 RespWriteUtils.TryWriteArrayItem）
fn write_bulk_int(out: &mut Vec<u8>, num: &mut Buffer, val: i64) {
  let mut len_buf = Buffer::new();
  let s = num.format(val);
  out.push(b'$');
  out.extend_from_slice(len_buf.format(s.len()).as_bytes());
  out.extend_from_slice(b"\r\n");
  out.extend_from_slice(s.as_bytes());
  out.extend_from_slice(b"\r\n");
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::parser::RespReadResponseUtils;

  /// append_log 帧编码与二进制数组解析往返：编码含二进制载荷的 8 元素数组，
  /// 经 try_read_byte_array_array_with_length_header 还原逐元素字节
  #[test]
  fn append_log_frame_roundtrip() {
    let mut frame = Vec::new();
    let mut num = Buffer::new();
    write_array_header(&mut frame, &mut num, 8);
    write_bulk_bytes(&mut frame, &mut num, b"CLUSTER");
    write_bulk_bytes(&mut frame, &mut num, b"APPENDLOG");
    write_bulk_bytes(&mut frame, &mut num, b"node-1");
    write_bulk_int(&mut frame, &mut num, 0);
    write_bulk_int(&mut frame, &mut num, 64);
    write_bulk_int(&mut frame, &mut num, -1);
    write_bulk_int(&mut frame, &mut num, 128);
    // 二进制载荷（含非 UTF-8 字节与 \r\n 穿透）
    let payload: &[u8] = &[0x00, 0xff, b'\r', b'\n', 0x80, 0x42];
    write_bulk_bytes(&mut frame, &mut num, payload);

    let mut data = frame.as_slice();
    let parsed = RespReadResponseUtils::try_read_byte_array_array_with_length_header(&mut data)
      .expect("parse ok")
      .expect("complete");
    let items = parsed.expect("non-null");
    assert_eq!(items.len(), 8);
    assert_eq!(items[0], b"CLUSTER");
    assert_eq!(items[1], b"APPENDLOG");
    assert_eq!(items[2], b"node-1");
    assert_eq!(items[3], b"0");
    assert_eq!(items[4], b"64");
    assert_eq!(items[5], b"-1");
    assert_eq!(items[6], b"128");
    assert_eq!(items[7], payload);
    assert!(data.is_empty(), "帧应被完整消费");
  }

  /// 半包不消费：截断帧解析返回 None 且游标回滚
  #[test]
  fn byte_array_array_partial_frame_not_consumed() {
    let mut frame = Vec::new();
    let mut num = Buffer::new();
    write_array_header(&mut frame, &mut num, 2);
    write_bulk_bytes(&mut frame, &mut num, b"CLUSTER");
    write_bulk_bytes(&mut frame, &mut num, b"APPENDLOG");
    // 截去末元素尾 CRLF 制造半包
    let partial = &frame[..frame.len() - 2];

    let mut data = partial;
    let res = RespReadResponseUtils::try_read_byte_array_array_with_length_header(&mut data);
    assert!(matches!(res, Ok(None)));
    assert_eq!(data.len(), partial.len(), "不完整帧游标应回滚");
  }
}
