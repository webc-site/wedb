use std::sync::Arc;

use crossfire::TrySendError;
use wbase::pool::LimitedFixedBufferPool;
use wresp::{ext::RespVecExt, resp_memory_writer::write_bulk_string_to};
#[cfg(feature = "tls")]
use wtls::ClientTlsConfig;

use crate::{
  Error, Result, network,
  types::{
    CHANNEL_CAP, ChannelTx, CommandItem, ReplyTx, roundtrip_array, roundtrip_bytes,
    roundtrip_frame_str, roundtrip_str,
  },
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

  /// 网络缓冲池（构造期注入，对标 C# GarnetClientSession 构造 `networkPool`
  /// 形参位；None = 建连时本会话自建）
  network_pool: Option<Arc<LimitedFixedBufferPool>>,

  tx: Option<ChannelTx>,

  /// 拆连核心件（dispose 幂等位 + 拆连句柄槽的内聚单点，与
  /// [`GarnetClient`](crate::client::GarnetClient) 五面同形共用同一承接件，
  /// 见 [`network::ConnectionCore`]）：幂等位对位 C# Dispose 的
  /// `Interlocked.Increment(ref disposed) > 1` 守卫与 `Disposed` 位，契约锚
  /// 单点见 [`Self::dispose`]；建连期以只读镜像随 [`network::connect_and_spawn`]
  /// 交泵，dispose 后泵收场判为计划内关闭不上抛断连错误
  core: network::ConnectionCore,
}

impl GarnetClientSession {
  /// libs/client/ClientSession/GarnetClientSession.cs:GarnetClientSession
  ///
  /// `network_pool` 对标 C# 构造第 3 形参位 `networkPool`（复制/迁移链传
  /// `ReplicationManager.GetNetworkPool` / `MigrationManager.GetNetworkPool`，
  /// 见 AofSyncTask.cs、MigrateSession.cs）；None = 建连时本会话自建
  ///（C# `?? CreateBufferPool` 同型回退）。池注入无 setter：会话侧在构造
  /// 期一次定死，客户端侧统一经 [`crate::client::GarnetClient::set_network_pool`]
  /// 一份
  pub fn new(
    endpoint: String,
    auth_username: Option<String>,
    auth_password: Option<String>,
    client_name: Option<String>,
    network_pool: Option<Arc<LimitedFixedBufferPool>>,
  ) -> Self {
    Self {
      end_point: endpoint,
      auth_username,
      auth_password,
      client_name,
      #[cfg(feature = "tls")]
      tls: None,
      network_pool,
      tx: None,
      core: network::ConnectionCore::new(),
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

  /// 请求通道引用（未连接即报错）
  fn channel(&self) -> Result<&ChannelTx> {
    self.tx.as_ref().ok_or(Error::NotConnected)
  }

  /// 发出即忘项装配单点（APPENDLOG 两形与 ADVANCE_TIME 脉冲共用）：
  /// frame → 无应答 CommandItem（对标 C# 无 tcs 入队且不递增 numCommands
  /// 的路径）+ 通道引用
  #[inline]
  fn forget_item(&self, frame: Vec<u8>) -> Result<(CommandItem, &ChannelTx)> {
    let tx = self.channel()?;
    Ok((
      CommandItem {
        frame,
        resp_tx: ReplyTx::None,
      },
      tx,
    ))
  }

  /// 同步非阻塞入队单点（wait=false 形）：通道饱和即返 Err（调用方持久化
  /// 重试，对标 C# 发送缓冲满时 Send 内部 Flush 的阻塞背压由上层承接）；
  /// try_send 双错误形态只在此映射一次
  #[inline]
  fn try_send_frame(&self, frame: Vec<u8>) -> Result<()> {
    let (item, tx) = self.forget_item(frame)?;
    match tx.try_send(item) {
      Ok(()) => Ok(()),
      Err(TrySendError::Full(_)) => Err(Error::SendChannelSaturated),
      Err(TrySendError::Disconnected(_)) => Err(Error::ReadPumpExited),
    }
  }

  /// 异步挂起入队单点（wait=true 形）：通道饱和时被动挂起等待网络泵消费
  ///（对标 C# 流式异步 Flush 背压）
  async fn send_frame(&self, frame: Vec<u8>) -> Result<()> {
    let (item, tx) = self.forget_item(frame)?;
    tx.send(item).await.map_err(|_| Error::ReadPumpExited)
  }

  /// libs/client/ClientSession/GarnetClientSession.cs:ConnectAsync
  pub async fn connect_async(&mut self) -> Result<()> {
    // 换代前置守卫（与 [`crate::client::GarnetClient::connect_async`] 同款，
    // rust 显式代际语义自带；拆净旧代 + 换新幂等位代际单点在
    // [`network::ConnectionCore::retire_generation`]，泄漏机理与守卫必要性
    // 详见其文档）：全新实例幂等位本假、句柄槽本空，置位后仍换新槽，零副作用
    self.core.retire_generation();
    // 建连编排单点（connect → with_tls → 通道 → 泵 spawn → 握手）下沉
    // [`network::connect_and_spawn`]；会话层无在途准入形参（C#
    // GarnetClientSession 同样无 maxOutstandingTasks），闸值取
    // `CHANNEL_CAP` 单源定容，progress 恒 None；拆连句柄存本会话真槽、
    // 幂等位换新代际镜像随建连交泵收场分类——C# 会话层确有 Dispose 拆连面
    //（散文指 [`Self::dispose`]，契约锚单点在其文档），句柄随字段存活至
    // dispose/Drop，不再是白付 dup+close 的即刻 drop 出参
    network::connect_and_spawn(
      network::ConnectParams {
        end_point: &self.end_point,
        #[cfg(feature = "tls")]
        tls: self.tls.as_deref(),
        label: "GarnetClientSession",
        auth_username: self.auth_username.as_deref(),
        auth_password: self.auth_password.as_deref(),
        client_name: self.client_name.as_deref(),
      },
      CHANNEL_CAP,
      None,
      self.network_pool.clone(),
      &mut self.tx,
      Some(Arc::clone(&self.core.disposed)),
      Some(&mut self.core.dispose_handle),
    )
    .await
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
    roundtrip_str(self.channel()?, command, None).await
  }

  /// libs/client/GarnetClientAPI/GarnetClientExecuteAPI.cs:ExecuteForMemoryResultWithCancellationAsync
  pub async fn execute_for_bytes_async(&self, command: &[&[u8]]) -> Result<Vec<u8>> {
    roundtrip_bytes(self.channel()?, command, None).await
  }

  /// libs/client/ClientSession/AsyncGarnetClientSession.cs:ExecuteForArrayAsync
  pub async fn execute_for_array_async(&self, command: &[&str]) -> Result<Vec<String>> {
    roundtrip_array(self.channel()?, command, None).await
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
    roundtrip_frame_str(self.channel()?, frame, None).await
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
    self.try_send_frame(frame)
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
    self.send_frame(frame).await
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
    self.try_send_frame(encode_advance_time_frame(
      physical_sublog_idx,
      sequence_number,
    ))
  }

  /// libs/client/ClientSession/GarnetClientSessionReplicationExtensions.cs:ExecuteClusterAttachSync
  ///
  /// 主备同步协商帧（3 元素 RESP 数组：CLUSTER ATTACH_SYNC metadata）。
  /// 副本主动发起时携带副本元数据（应答授予位点），主端 diskless 恢复握手
  /// 时携带 primary 元数据（应答副本恢复位点）。应答为 bulk string 位点串，
  /// 对端 -ERR 错误帧经 Err(Server) 透出
  pub async fn execute_cluster_attach_sync(&self, sync_metadata: &[u8]) -> Result<String> {
    roundtrip_frame_str(
      self.channel()?,
      encode_attach_sync_frame(sync_metadata),
      None,
    )
    .await
  }

  /// 连接健康面（对标 C# GarnetClientSession.IsConnected 的
  /// `socket != null && socket.Connected && !Disposed` 口径）
  ///
  /// 口径单点在 [`network::ConnectionCore::is_connected`]（与
  /// [`GarnetClient::is_connected`](crate::client::GarnetClient) 同形共用）：
  /// dispose 幂等位置位即假（对位 `Disposed` 守卫）；否则通道在位且网络泵
  /// 仍存活：泵任务退出（对端断链 / EOF / 协议错误 / dispose 拆连）即判定断连，
  /// C# networkSender.IsConnected 的 socket 态感知投影
  pub fn is_connected(&self) -> bool {
    self.core.is_connected(self.tx.as_ref())
  }

  /// libs/client/ClientSession/GarnetClientSession.cs:Dispose
  ///
  /// C# :413-419 拆连全臂的 rust 等义面（会话层拆连契约锚单点在此，机制
  /// 实现单点在 [`network::ConnectionCore::dispose`]，与
  /// [`GarnetClient::dispose`](crate::client::GarnetClient) 同形共用）：
  /// - `Interlocked.Increment(ref disposed) > 1` 守卫 ↔ 幂等位 swap，
  ///   二次调用即返；
  /// - `socket?.Dispose()` ↔
  ///   [`network::stream::DisposeHandle::shutdown_both`] 双向
  ///   shutdown(2)：常驻读即刻以 EOF/err 内核落定，读泵沿既有读收场支退出，
  ///   静默对端下不悬挂；
  /// - `networkSender?.ReturnResponseObject()` / `networkHandler?.Dispose()`
  ///   ↔ 泵收场路径：在途 oneshot 随通道销毁断连（在途命令以既有断连错误
  ///   结算）、池借出缓冲 RAII 归池；
  /// - `if (!usingManagedNetworkPool) networkPool.Dispose()` ↔ 池 Arc 自持
  ///   计数回收（本会话自建池随泵退场放尽；注入池不归本会话收口，与 C#
  ///   managed pool 标志同判据）。
  ///
  /// C# `ReconnectAsync` 的 `if (Disposed) throw ObjectDisposedException`
  /// 臂不落地：rust 会话无 reconnect 形（建连一次性，重连由持有方重建会话
  /// 承载，见 wedb replica_wire），`disposed` 位命中后本实例仅余
  /// `is_connected` 恒假与命令面即刻失败两个终态面
  pub fn dispose(&self) {
    self.core.dispose();
  }
}

/// C# 侧收口经持有方显式 Dispose 链（AofSyncTask.Dispose 与驱动 DisposeClient
/// 的 `garnetClient?.Dispose()`）；rust 的最终收口以最后持有者落下承接——
/// Drop 仅转发同一幂等拆连面，达成「dispose/落尽之后 socket 与池缓冲必然
/// 回收」同强契约（wedb 复制层退场链的会话 `dispose()` 接线见 replica_wire
/// TcpSessionWire::disconnect 文档）
impl Drop for GarnetClientSession {
  fn drop(&mut self) {
    self.dispose();
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

/// 数值数组项字节上限（bulk 成帧 `$` + 长度位 ≤2 + CRLF + i64 最长 20 进制位 +
/// CRLF），四帧容量推导共用——整数经 write_integer_as_bulk_string 以 bulk string
/// 帧写出，非 `:` 整数帧
const MAX_INT_ITEM: usize = 1 + 2 + 2 + 20 + 2;
/// bulk string 帧头字节上限（`$` + 长度最长 20 进制位 + 前后 CRLF）
const MAX_BULK_HEAD: usize = 1 + 20 + 2 * 2;

/// 编码 CLUSTER ADVANCE_TIME 4 元素 RESP 数组帧（内部对标 ExecuteClusterAdvanceTime 帧格式）
pub fn encode_advance_time_frame(physical_sublog_idx: usize, sequence_number: i64) -> Vec<u8> {
  let mut frame = Vec::with_capacity(ADVANCE_TIME_FRAME_PREFIX.len() + 2 * MAX_INT_ITEM);
  frame.extend_from_slice(ADVANCE_TIME_FRAME_PREFIX);
  let mut writer = frame.resp_writer2();
  writer.write_integer_as_bulk_string(physical_sublog_idx as i64);
  writer.write_integer_as_bulk_string(sequence_number);
  frame
}

/// 编码 CLUSTER ATTACH_SYNC 3 元素 RESP 数组帧（元数据为二进制 bulk string）
pub fn encode_attach_sync_frame(sync_metadata: &[u8]) -> Vec<u8> {
  let mut frame =
    Vec::with_capacity(ATTACH_SYNC_FRAME_PREFIX.len() + MAX_BULK_HEAD + sync_metadata.len());
  frame.extend_from_slice(ATTACH_SYNC_FRAME_PREFIX);
  write_bulk_string_to(&mut frame, sync_metadata);
  frame
}

#[inline]
fn write_append_log_fields(
  frame: &mut Vec<u8>,
  node_id: &str,
  physical_sublog_idx: usize,
  previous_address: i64,
  current_address: i64,
  next_address: i64,
) {
  write_bulk_string_to(frame, node_id.as_bytes());
  let mut writer = frame.resp_writer2();
  writer.write_integer_as_bulk_string(physical_sublog_idx as i64);
  writer.write_integer_as_bulk_string(previous_address);
  writer.write_integer_as_bulk_string(current_address);
  writer.write_integer_as_bulk_string(next_address);
}

/// 编码 CLUSTER APPENDLOG 7 元素初始化 RESP 数组帧
pub fn encode_append_log_init_frame(
  node_id: &str,
  physical_sublog_idx: usize,
  previous_address: i64,
  current_address: i64,
  next_address: i64,
) -> Vec<u8> {
  let mut frame = Vec::with_capacity(
    APPEND_LOG_INIT_PREFIX.len() + MAX_BULK_HEAD + node_id.len() + 4 * MAX_INT_ITEM,
  );
  frame.extend_from_slice(APPEND_LOG_INIT_PREFIX);
  write_append_log_fields(
    &mut frame,
    node_id,
    physical_sublog_idx,
    previous_address,
    current_address,
    next_address,
  );
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
  let mut frame = Vec::with_capacity(
    APPEND_LOG_FRAME_PREFIX.len()
      + node_id.len()
      + payload.len()
      + 2 * MAX_BULK_HEAD
      + 4 * MAX_INT_ITEM,
  );
  frame.extend_from_slice(APPEND_LOG_FRAME_PREFIX);
  write_append_log_fields(
    &mut frame,
    node_id,
    physical_sublog_idx,
    previous_address,
    current_address,
    next_address,
  );
  frame.resp_writer2().write_bulk_string(payload);
  frame
}
