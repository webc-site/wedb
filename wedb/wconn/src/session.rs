use std::sync::Arc;

use compio::runtime::spawn;
use crossfire::{TrySendError, mpsc, oneshot};
use wbase::pool::LimitedFixedBufferPool;
use wresp::{ext::RespVecExt, resp_memory_writer::write_bulk_string_to};

#[cfg(feature = "tls")]
use crate::tls::ClientTlsConfig;
use crate::{
  Error, Result, network,
  network::stream::OutStream,
  types::{CHANNEL_CAP, ChannelTx, CommandItem, ReplyTx, roundtrip},
};

/// libs/client/ClientSession/GarnetClientSession.cs:GarnetClientSession
///
/// 会话层不引入在途命令超时（C# GarnetClientSession 同样无
/// timeoutMilliseconds 旋钮）：本会话的调用方（集群总线、副本握手与
/// AOF 转发）已在各自协议层包时限，超时旋钮仅在
/// [`GarnetClient`](crate::client::GarnetClient) 上提供
pub struct GarnetClientSession {
  pub end_point: String,
  auth_username: Option<String>,
  auth_password: Option<String>,
  client_name: Option<String>,

  /// 出站 TLS 配置（None = 明文；C# 构造重载的 tlsOptions? 同位语义）
  #[cfg(feature = "tls")]
  tls: Option<Arc<ClientTlsConfig>>,

  /// 网络缓冲池（C# `networkPool` 形参位：见 [`Self::set_network_pool`]）
  network_pool: Option<Arc<LimitedFixedBufferPool>>,

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
      #[cfg(feature = "tls")]
      tls: None,
      network_pool: None,
      tx: None,
    }
  }

  /// 出站 TLS 配置注入（None = 明文）
  ///
  /// 对标 C# GarnetClientSession 构造重载的 `GarnetTlsOptions? tlsOptions`
  /// 形参位（AofSyncTask 等集群出站消费点透传的同一单源配置）
  #[cfg(feature = "tls")]
  pub fn set_tls(&mut self, tls: Option<Arc<ClientTlsConfig>>) {
    self.tls = tls;
  }

  /// 网络缓冲池注入（读泵接收缓冲的取还单点）
  ///
  /// 对标 C# `libs/client/ClientSession/GarnetClientSession.cs:GarnetClientSession`
  /// 的 `networkPool` 形参：AofSyncTask 与副本同步会话传
  /// `ReplicationManager.GetNetworkPool`、迁移会话传
  /// `MigrationManager.GetNetworkPool`（同一池跨连接复用）；
  /// None = 建连时本会话自建（C# `?? CreateBufferPool` 同型回退）
  pub fn set_network_pool(&mut self, pool: Option<Arc<LimitedFixedBufferPool>>) {
    self.network_pool = pool;
  }

  /// 请求通道引用（未连接即报错）
  fn channel(&self) -> Result<&ChannelTx> {
    self.tx.as_ref().ok_or(Error::NotConnected)
  }

  /// libs/client/ClientSession/GarnetClientSession.cs:ConnectAsync
  pub async fn connect_async(&mut self) -> Result<()> {
    // 端点形态分派建连：TCP 臂设 nodelay、Unix 域套接字臂不设（判定单源
    // `wbase::endpoint::uds_path`，与客户端同一条规则）
    let stream = OutStream::connect(&self.end_point).await?;
    // TLS 配置在位即在 TCP 之上完成握手包裹；无配置保持明文字节流
    #[cfg(feature = "tls")]
    let stream = stream
      .with_tls(self.tls.as_deref(), &self.end_point)
      .await?;
    // 会话层无在途准入形参（C# GarnetClientSession 同样无 maxOutstandingTasks）：
    // 命令通道与泵内在途队列均按 CHANNEL_CAP 单源定容
    let (tx, rx) = mpsc::bounded_async(CHANNEL_CAP);
    self.tx = Some(tx);

    // 读泵接收缓冲池解析（注入优先，未注入即本会话自建，C# 同型回退）
    let network_pool = network::resolve_network_pool(self.network_pool.clone());

    spawn(async move {
      if let Err(e) = network::network_loop(stream, rx, None, CHANNEL_CAP, network_pool).await {
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
  /// libs/client/ClientSession/GarnetClientSession.cs:Execute
  /// libs/client/ClientSession/GarnetClientSession.cs:ExecuteBatch
  ///
  /// C# ExecuteAsyncBatch(params string[]) 为单命令变长参数入队 + TCS 等待，
  /// 与 ExecuteAsync 同构 (rust 无 TCS 队列，由 network_loop 按需泵出)，
  /// 故两者共用此实现；同步 Execute 族（无返回值 / Batch 不等结果）在纯
  /// 异步 runtime 下分解为等待形态与本形态
  pub async fn execute_async(&self, command: &[&str]) -> Result<String> {
    let (resp_tx, resp_rx) = oneshot::oneshot();
    let item = CommandItem::new_str(command, ReplyTx::Str(resp_tx));
    roundtrip(self.channel()?, item, resp_rx, None).await
  }

  /// libs/client/GarnetClientAPI/GarnetClientExecuteAPI.cs:ExecuteForMemoryResultWithCancellationAsync
  pub async fn execute_for_bytes_async(&self, command: &[&[u8]]) -> Result<Vec<u8>> {
    let (resp_tx, resp_rx) = oneshot::oneshot();
    let item = CommandItem::new_bytes(command, ReplyTx::Bytes(resp_tx));
    roundtrip(self.channel()?, item, resp_rx, None).await
  }

  /// libs/client/ClientSession/AsyncGarnetClientSession.cs:ExecuteForArrayAsync
  pub async fn execute_for_array_async(&self, command: &[&str]) -> Result<Vec<String>> {
    let (resp_tx, resp_rx) = oneshot::oneshot();
    let item = CommandItem::new_str(command, ReplyTx::Array(resp_tx));
    roundtrip(self.channel()?, item, resp_rx, None).await
  }

  /// libs/client/ClientSession/GarnetClientSessionReplicationExtensions.cs:ExecuteClusterAppendLogInit
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
    roundtrip(self.channel()?, item, resp_rx, None).await
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
      Err(TrySendError::Full(_)) => Err(Error::SendChannelSaturated),
      Err(TrySendError::Disconnected(_)) => Err(Error::ReadPumpExited),
    }
  }

  /// 异步逐记录 AOF 帧转发（通道饱和时被动挂起等待网络泵消费，对标 C# 流式异步 Flush 背压）
  pub async fn execute_cluster_append_log_async(
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
    self
      .channel()?
      .send(item)
      .await
      .map_err(|_| Error::ReadPumpExited)
  }

  /// libs/client/ClientSession/GarnetClientSessionReplicationExtensions.cs:ExecuteClusterAdvanceTime
  ///
  /// 带内 CLUSTER ADVANCE_TIME 时间脉冲（4 元素 RESP 数组：CLUSTER ADVANCE_TIME
  /// sublogIdx sequenceNumber）。无应答期待（fire-and-forget），脉冲与本连接
  /// 上的 APPENDLOG 流量保持同通道 FIFO 保序。通道饱和或断连返回 Err。
  pub fn execute_cluster_advance_time(
    &self,
    physical_sublog_idx: usize,
    sequence_number: i64,
  ) -> Result<()> {
    let item = CommandItem {
      frame: encode_advance_time_frame(physical_sublog_idx, sequence_number),
      resp_tx: ReplyTx::None,
    };
    match self.channel()?.try_send(item) {
      Ok(()) => Ok(()),
      Err(TrySendError::Full(_)) => Err(Error::SendChannelSaturated),
      Err(TrySendError::Disconnected(_)) => Err(Error::ReadPumpExited),
    }
  }

  /// libs/client/ClientSession/GarnetClientSessionReplicationExtensions.cs:ExecuteClusterAttachSync
  ///
  /// 主备同步协商帧（3 元素 RESP 数组：CLUSTER ATTACH_SYNC metadata）。
  /// 副本主动发起时携带副本元数据（应答授予位点），主端 diskless 恢复握手
  /// 时携带 primary 元数据（应答副本恢复位点）。应答为 bulk string 位点串，
  /// 对端 -ERR 错误帧经 Err(Server) 透出
  pub async fn execute_cluster_attach_sync(&self, sync_metadata: &[u8]) -> Result<String> {
    let (resp_tx, resp_rx) = oneshot::oneshot();
    let item = CommandItem {
      frame: encode_attach_sync_frame(sync_metadata),
      resp_tx: ReplyTx::Str(resp_tx),
    };
    roundtrip(self.channel()?, item, resp_rx, None).await
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
/// 时间脉冲帧常量前缀（*4 + CLUSTER + ADVANCE_TIME）
const ADVANCE_TIME_FRAME_PREFIX: &[u8] = b"*4\r\n$7\r\nCLUSTER\r\n$12\r\nADVANCE_TIME\r\n";
/// 主备同步协商帧常量前缀（*3 + CLUSTER + ATTACH_SYNC）
const ATTACH_SYNC_FRAME_PREFIX: &[u8] = b"*3\r\n$7\r\nCLUSTER\r\n$11\r\nATTACH_SYNC\r\n";

/// 编码 CLUSTER ADVANCE_TIME 4 元素 RESP 数组帧（内部对标 ExecuteClusterAdvanceTime 帧格式）
pub fn encode_advance_time_frame(physical_sublog_idx: usize, sequence_number: i64) -> Vec<u8> {
  let mut frame = Vec::with_capacity(64);
  frame.extend_from_slice(ADVANCE_TIME_FRAME_PREFIX);
  let mut writer = frame.resp_writer2();
  writer.write_array_item(physical_sublog_idx as i64);
  writer.write_array_item(sequence_number);
  frame
}

/// 编码 CLUSTER ATTACH_SYNC 3 元素 RESP 数组帧（元数据为二进制 bulk string）
pub fn encode_attach_sync_frame(sync_metadata: &[u8]) -> Vec<u8> {
  let mut frame = Vec::with_capacity(48 + sync_metadata.len());
  frame.extend_from_slice(ATTACH_SYNC_FRAME_PREFIX);
  write_bulk_string_to(&mut frame, sync_metadata);
  frame
}

/// 编码 CLUSTER APPENDLOG 7 元素初始化 RESP 数组帧
pub fn encode_append_log_init_frame(
  node_id: &str,
  physical_sublog_idx: usize,
  previous_address: i64,
  current_address: i64,
  next_address: i64,
) -> Vec<u8> {
  let mut frame = Vec::with_capacity(80 + node_id.len());
  frame.extend_from_slice(APPEND_LOG_INIT_PREFIX);
  write_bulk_string_to(&mut frame, node_id.as_bytes());
  let mut writer = frame.resp_writer2();
  writer.write_array_item(physical_sublog_idx as i64);
  writer.write_array_item(previous_address);
  writer.write_array_item(current_address);
  writer.write_array_item(next_address);
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
  frame.extend_from_slice(APPEND_LOG_FRAME_PREFIX);
  write_bulk_string_to(&mut frame, node_id.as_bytes());
  let mut writer = frame.resp_writer2();
  writer.write_array_item(physical_sublog_idx as i64);
  writer.write_array_item(previous_address);
  writer.write_array_item(current_address);
  writer.write_array_item(next_address);
  writer.write_bulk_string(payload);
  frame
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::parser::RespReadResponseUtils;

  /// attach_sync 帧布局：3 元素数组头 + CLUSTER + ATTACH_SYNC + 二进制元数据
  #[test]
  fn attach_sync_frame_layout() {
    let frame = encode_attach_sync_frame(&[0x00, 0xff, 0x42]);
    assert_eq!(
      frame,
      b"*3\r\n$7\r\nCLUSTER\r\n$11\r\nATTACH_SYNC\r\n$3\r\n\x00\xff\x42\r\n"
    );
  }

  /// append_log 帧编码与二进制数组解析往返：编码含二进制载荷的 8 元素数组，
  /// 经生产数组读臂 try_read_byte_slice_array_with_length_header 还原逐元素字节
  #[test]
  fn append_log_frame_roundtrip() {
    let mut frame = Vec::new();
    frame.write_resp_array_len(8);
    frame.write_resp_bulk_string(b"CLUSTER");
    frame.write_resp_bulk_string(b"APPENDLOG");
    frame.write_resp_bulk_string(b"node-1");
    {
      let mut writer = frame.resp_writer2();
      writer.write_array_item(0);
      writer.write_array_item(64);
      writer.write_array_item(-1);
      writer.write_array_item(128);
    }
    // 二进制载荷（含非 UTF-8 字节与 \r\n 穿透）
    let payload: &[u8] = &[0x00, 0xff, b'\r', b'\n', 0x80, 0x42];
    frame.write_resp_bulk_string(payload);

    let mut data = frame.as_slice();
    let parsed = RespReadResponseUtils::try_read_byte_slice_array_with_length_header(&mut data)
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
  fn byte_slice_array_partial_frame_not_consumed() {
    let mut frame = Vec::new();
    frame.write_resp_array_len(2);
    frame.write_resp_bulk_string(b"CLUSTER");
    frame.write_resp_bulk_string(b"APPENDLOG");
    // 截去末元素尾 CRLF 制造半包
    let partial = &frame[..frame.len() - 2];

    let mut data = partial;
    let res = RespReadResponseUtils::try_read_byte_slice_array_with_length_header(&mut data);
    assert!(matches!(res, Ok(None)));
    assert_eq!(data.len(), partial.len(), "不完整帧游标应回滚");
  }
}
