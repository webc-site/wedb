use std::{sync::Arc, time::Duration};

use compio::{runtime::spawn, time::sleep};
use crossfire::{mpsc, oneshot};
use wbase::pool::LimitedFixedBufferPool;
#[cfg(feature = "tls")]
use wtls::ClientTlsConfig;

use crate::{
  Error, Result, network,
  network::stream::OutStream,
  session::encode_attach_sync_frame,
  types::{ChannelTx, CommandItem, PumpProgress, ReplyTx, roundtrip},
};

/// 在途命令上限形参上限（对标 C# PageOffset.kTaskMask + 1，kTaskBits = 20）
const MAX_OUTSTANDING_TASKS: usize = 1 << 20;

/// libs/client/GarnetClient.cs:GarnetClient
pub struct GarnetClient {
  pub end_point: String,
  auth_username: Option<String>,
  auth_password: Option<String>,
  client_name: Option<String>,

  /// 在途命令准入上限（2 的幂，≤ [`MAX_OUTSTANDING_TASKS`]）：命令通道与
  /// 泵内在途队列按此定容（对标 C# tcsArray 定长槽），在途未回收数达到
  /// 上限即调用方 send 挂起退避（对标 C# InputGateAsync 满即指数退避）
  max_outstanding_tasks: usize,

  /// 在途命令超时（毫秒，0 = 关闭，对标 C# timeoutMilliseconds 默认 0）：
  /// 周期比较泵侧进度（回收序号 / 发出字节 / 入队序号），皆无进展且在途
  /// 非空即拆客户端，全部在途命令按 [`Error::Timeout`] 结算
  timeout_millis: u64,

  /// 泵侧进度计数（超时旋钮开启时随建连创建，供在途往返判定断连原因）
  progress: Option<Arc<PumpProgress>>,

  /// 出站 TLS 配置（None = 明文，对标 C# 构造的 tlsOptions? 缺省语义；
  /// libs/client/GarnetClient.cs:GarnetClient 构造重载第 2 形参位）
  #[cfg(feature = "tls")]
  tls: Option<Arc<ClientTlsConfig>>,

  /// 网络缓冲池（对标 C# `libs/client/ClientSession/GarnetClientSession.cs:GarnetClientSession`
  /// 的 `networkPool` 形参：调用方持池即共用，None 走 C# `?? CreateBufferPool`
  /// 同型的连接自建池，见 [`Self::set_network_pool`]）
  network_pool: Option<Arc<LimitedFixedBufferPool>>,

  tx: Option<ChannelTx>,
}

impl GarnetClient {
  /// libs/client/GarnetClient.cs:GarnetClient
  ///
  /// 页尺寸/缓冲由 compio IoBuf 与 wresp 缓冲承担，超时旋钮在位
  ///（`timeout_millis`，对标 C# timeoutMilliseconds；0 = 关闭）
  ///
  /// `max_outstanding_tasks` 构造校验（对标 C# 构造重载 :167-174 双校验的
  /// ThrowException）：必须为 2 的幂且 ≤ 1<<20，非合即 Err，不做静默取整
  ///（crossfire 有界通道对 0 / 非 2 的幂的静默容错由此在边界拦下）
  ///
  /// C# 的 `recordLatency` 客户端延迟直方图形参不落地：其开关与全部读者只在
  /// C# 基准树（Resp.benchmark `--client-hist`），本仓已整体登记不移植，理由见
  /// js/check/ignore/client.yml 的 GarnetClientMetrics.cs 条目
  /// libs/client/GarnetClient.cs:GarnetClient
  pub fn new(
    endpoint: String,
    auth_username: Option<String>,
    auth_password: Option<String>,
    client_name: Option<String>,
    max_outstanding_tasks: usize,
    timeout_millis: u64,
  ) -> Result<Self> {
    if !max_outstanding_tasks.is_power_of_two() || max_outstanding_tasks > MAX_OUTSTANDING_TASKS {
      return Err(Error::InvalidOutstandingTasks(max_outstanding_tasks));
    }
    Ok(Self {
      end_point: endpoint,
      auth_username,
      auth_password,
      client_name,
      max_outstanding_tasks,
      timeout_millis,
      progress: None,
      #[cfg(feature = "tls")]
      tls: None,
      network_pool: None,
      tx: None,
    })
  }

  /// 在途命令准入上限读数（构造校验后的原值）
  ///
  /// 在 garnet 中的相对路径: libs/client/GarnetClient.cs:GetOutstandingTasksLimit
  pub fn get_outstanding_tasks_limit(&self) -> usize {
    self.max_outstanding_tasks
  }

  /// 出站 TLS 配置注入（None = 明文）
  ///
  /// 对应 C# GarnetClient 构造重载 `GarnetTlsOptions? tlsOptions` 第 2 形参位；
  /// rust 以注入器承载，集群出站连接透传点的同一单源配置
  #[cfg(feature = "tls")]
  pub fn set_tls(&mut self, tls: Option<Arc<ClientTlsConfig>>) {
    self.tls = tls;
  }

  /// 网络缓冲池注入（读泵接收缓冲的取还单点，全仓唯一池注入 setter）
  ///
  /// 对应 C# GarnetClientSession 的 `networkPool` 形参（复制/迁移链传
  /// `ReplicationManager.GetNetworkPool` / `MigrationManager.GetNetworkPool`，
  /// 见 AofSyncTask.cs、MigrateSession.cs）；None = 建连时本客户端自建。
  /// 推流域 [`GarnetClientSession`](crate::session::GarnetClientSession)
  /// 无 setter，池在构造期注入（对标 C# 构造 `networkPool` 形参位）
  pub fn set_network_pool(&mut self, pool: Option<Arc<LimitedFixedBufferPool>>) {
    self.network_pool = pool;
  }

  /// 请求通道引用（未连接即报错）
  fn channel(&self) -> Result<&ChannelTx> {
    self.tx.as_ref().ok_or(Error::NotConnected)
  }

  /// 连接健康面（对标 libs/client/GarnetClient.cs:IsConnected 的 socket 态口径）
  ///
  /// 通道在位且网络泵仍存活：泵任务退出（EOF/断链/协议错误/在途超时）即
  /// rx 全部丢弃，发送端 `is_disconnected` 翻真——零轮询零探测包，与
  /// [`GarnetClientSession::is_connected`](crate::GarnetClientSession) 同一口径
  pub fn is_connected(&self) -> bool {
    self.tx.as_ref().is_some_and(|tx| !tx.is_disconnected())
  }

  /// libs/client/GarnetClient.cs:ConnectAsync
  pub async fn connect_async(&mut self) -> Result<()> {
    // 端点形态分派建连：TCP 臂设 nodelay、Unix 域套接字臂不设（判定单源
    // `wbase::endpoint::uds_path`，与入站监听端点共读同一条规则）
    let stream = OutStream::connect(&self.end_point).await?;
    // TLS 配置在位即在 TCP 之上完成握手包裹（对标 C# ConnectAsync 里
    // SslStream.AuthenticateAsClientAsync 分支）；无配置保持明文字节流
    #[cfg(feature = "tls")]
    let stream = stream
      .with_tls(self.tls.as_deref(), &self.end_point)
      .await?;
    // 在途准入闸：命令通道容量即闸值（无 .max 抬升，形参原值生效；在途满
    // 即调用方 send 挂起退避，对标 C# InputGateAsync）；闸值随网络循环
    // 透传泵内定容，单点定义无第二容量口径
    let gate = self.max_outstanding_tasks;
    let (tx, rx) = mpsc::bounded_async(gate);
    self.tx = Some(tx);

    // 超时旋钮开启即建泵侧进度计数并 spawn 超时检查任务（对标 C#
    // ConnectAsync 内 timeoutMilliseconds > 0 即 Task.Run(TimeoutChecker)）；
    // 网络循环收场即置退休标志，检查任务随之退场（对标 C# Dispose 经
    // timeoutCheckerCts 取消退出，rust 以泵收场信号收场，不持通道引用）
    let progress = (self.timeout_millis > 0).then(|| {
      let progress = Arc::new(PumpProgress::new());
      spawn(timeout_checker(
        Arc::clone(&progress),
        Duration::from_millis(self.timeout_millis),
      ))
      .detach();
      progress
    });
    self.progress = progress.clone();
    // 读泵接收缓冲池解析（注入优先，未注入即本客户端自建，C# 同型回退）
    let network_pool = network::resolve_network_pool(self.network_pool.clone());

    spawn(async move {
      if let Err(e) = network::network_loop(stream, rx, progress, gate, network_pool).await {
        log::error!("GarnetClient 网络循环退出: {e}");
      }
    })
    .detach();

    let handshake = network::handshake(
      self.channel()?,
      "GarnetClient",
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

  /// libs/client/GarnetClient.cs:ReconnectAsync
  pub async fn reconnect_async(&mut self) -> Result<()> {
    self.tx = None;
    self.progress = None;
    self.connect_async().await
  }

  /// libs/client/GarnetClientAPI/GarnetClientExecuteAPI.cs:ExecuteForStringResultAsync
  pub async fn execute_for_string_result_async(&self, command: &[&str]) -> Result<String> {
    let (resp_tx, resp_rx) = oneshot::oneshot();
    let item = CommandItem::new_str(command, ReplyTx::Str(resp_tx));
    roundtrip(self.channel()?, item, resp_rx, self.progress.as_deref()).await
  }

  /// C# ExecuteForMemoryResultWithCancellationAsync 同型（精确锚点见 session.rs:93）
  pub async fn execute_for_bytes_result_async(&self, command: &[&[u8]]) -> Result<Vec<u8>> {
    let (resp_tx, resp_rx) = oneshot::oneshot();
    let item = CommandItem::new_bytes(command, ReplyTx::Bytes(resp_tx));
    roundtrip(self.channel()?, item, resp_rx, self.progress.as_deref()).await
  }

  /// libs/client/GarnetClientAPI/GarnetClientExecuteAPI.cs:ExecuteForStringArrayResultAsync
  pub async fn execute_for_string_array_result_async(
    &self,
    command: &[&str],
  ) -> Result<Vec<String>> {
    let (resp_tx, resp_rx) = oneshot::oneshot();
    let item = CommandItem::new_str(command, ReplyTx::Array(resp_tx));
    roundtrip(self.channel()?, item, resp_rx, self.progress.as_deref()).await
  }

  /// CLUSTER ATTACH_SYNC 主备同步协商帧（GarnetClient 任务面形态）：帧构造
  /// 复用会话层单点 [`encode_attach_sync_frame`](crate::session::encode_attach_sync_frame)，
  /// 不经逐参数编码路径二次拼令；应答为 bulk string 位点串，
  /// 对端 -ERR 经 [`Error::Server`] 透出
  pub async fn execute_cluster_attach_sync(&self, sync_metadata: &[u8]) -> Result<String> {
    let (resp_tx, resp_rx) = oneshot::oneshot();
    let item = CommandItem {
      frame: encode_attach_sync_frame(sync_metadata),
      resp_tx: ReplyTx::Str(resp_tx),
    };
    roundtrip(self.channel()?, item, resp_rx, self.progress.as_deref()).await
  }

  /// libs/client/GarnetClientAPI/GarnetClientExecuteAPI.cs:ExecuteNoResponse
  pub async fn execute_no_response_async(&self, command: &[&[u8]]) -> Result<()> {
    let item = CommandItem::new_bytes(command, ReplyTx::None);
    self
      .channel()?
      .send(item)
      .await
      .map_err(|_| Error::ReadPumpExited)
  }
}

/// 在途命令超时检查：周期比较泵侧三进度，皆无进展且在途非空即判成
///
/// 在 garnet 中的相对路径: libs/client/GarnetClient.cs:TimeoutChecker
///
/// 判成后置泵侧标志收场：读泵在限时读粒度内见位退出、在途 oneshot 随通道
/// 销毁断连，roundtrip 按标志结算 [`Error::Timeout`]——对标 C# 判成
/// Dispose() 拆客户端、在途 TaskCompletionSource 全部置错。网络循环收场
/// 置退休标志后本任务退场（对标 C# Dispose 经 timeoutCheckerCts 取消退出，
/// rust 不持通道引用，不阻断「调用方退场 → 泵自灭」闭环）
async fn timeout_checker(progress: Arc<PumpProgress>, period: Duration) {
  loop {
    if progress.is_retired() {
      return;
    }
    let (reclaimed, sent_bytes, _) = progress.snapshot();
    sleep(period).await;
    let (new_reclaimed, new_sent_bytes, new_enqueued) = progress.snapshot();
    // 新应答被回收：有进展
    if new_reclaimed != reclaimed {
      continue;
    }
    // 新数据已发出：有进展
    if new_sent_bytes != sent_bytes {
      continue;
    }
    // 全部回收完毕（空闲）：无在途可超时
    if new_reclaimed == new_enqueued {
      continue;
    }
    progress.flag_timeout();
    return;
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// 构造闸校验：0 / 非 2 的幂 / 超 1<<20 显式报错，2 的幂放行且探针可读
  ///
  /// 对标 C# 构造重载对 maxOutstandingTasks 的双校验 ThrowException
  ///（libs/client/GarnetClient.cs:167-174）
  #[test]
  fn outstanding_tasks_gate_validation() {
    for bad in [0usize, 33, 1 << 21] {
      match GarnetClient::new("127.0.0.1:0".into(), None, None, None, bad, 0) {
        Err(Error::InvalidOutstandingTasks(v)) => assert_eq!(v, bad),
        _ => panic!("闸值 {bad} 应被拒绝"),
      }
    }
    let client = GarnetClient::new("127.0.0.1:0".into(), None, None, None, 32, 0).unwrap();
    assert_eq!(client.get_outstanding_tasks_limit(), 32);
  }
}
