use std::{sync::Arc, time::Duration};

use wbase::pool::LimitedFixedBufferPool;
#[cfg(feature = "tls")]
use wtls::ClientTlsConfig;

use crate::{
  Error, Result, network,
  session::encode_attach_sync_frame,
  types::{
    ChannelTx, CommandItem, PumpProgress, ReplyTx, roundtrip_array, roundtrip_bytes,
    roundtrip_frame_str, roundtrip_str,
  },
};

/// 在途命令上限形参上限（对标 C# PageOffset.kTaskMask + 1，kTaskBits = 20）
const MAX_OUTSTANDING_TASKS: usize = 1 << 20;

/// 在途命令准入缺省（2 的幂，测试与轻载客户端共用；构造校验约束见 [`GarnetClient::new`]）
pub const DEFAULT_OUTSTANDING_TASKS: usize = 32;

/// 客户端级在途超时关闭哨兵（`timeout_millis = 0`，对标 C# timeoutMilliseconds 默认 0）
pub const NO_TIMEOUT_MILLIS: u64 = 0;

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

  /// 拆连核心件（dispose 幂等位 + 拆连句柄槽的内聚单点，与
  /// [`GarnetClientSession`](crate::session::GarnetClientSession) 五面同形
  /// 共用同一承接件，见 [`network::ConnectionCore`]）：幂等位对位 C#
  /// `libs/client/GarnetClient.cs:Dispose(bool)` :524
  /// `Interlocked.Increment(ref disposed) > 1` 守卫——显式 dispose 与最后
  /// Arc 落下共同触达，一次置位即不再拆连；网络循环以只读镜像收场分类；
  /// 命令面入通道前读此位拒绝（`Self::channel`）；新代建连换新生成且换前必先
  /// 拆净旧代（[`Self::connect_async`]）
  core: network::ConnectionCore,
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
      core: network::ConnectionCore::new(),
    })
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

  /// 请求通道引用（命令入通道前的唯一守卫闸）
  ///
  /// 两级判定，顺序不可换：
  /// 1. dispose 幂等位在位即 [`Error::Disposed`]——对标 C# 发送路径槽位填满后
  ///    的即时判定（六处同形，`libs/client/GarnetClient.cs:InternalExecuteAsync`
  ///    :733 单点，余 :630/:846/:934/:1050/:1165 与之同形），异常型
  ///    `GarnetClientDisposedException`（[`Error::Disposed`] 对位）；
  /// 2. 通道缺失（未建连或握手失败弃道）按 [`Error::NotConnected`]。
  ///
  /// 缺第 1 级时 dispose 后命令照入通道（tx 仍在位），最终被动结算为
  /// `ResponseChannelClosed`/`ReadPumpExited`；发出即忘形更在 250ms 限时窗内
  /// send 即返 `Ok` 而帧永不达对端——错误形制与「已弃用实例」的语义在此收口
  fn channel(&self) -> Result<&ChannelTx> {
    if self.core.is_disposed() {
      return Err(Error::Disposed);
    }
    self.tx.as_ref().ok_or(Error::NotConnected)
  }

  /// 连接健康面（对标 libs/client/GarnetClient.cs:IsConnected 的 socket 态口径）
  ///
  /// 口径单点在 [`network::ConnectionCore::is_connected`]（与
  /// [`GarnetClientSession::is_connected`](crate::session::GarnetClientSession)
  /// 同形共用）：dispose 幂等位置位即假；否则通道在位且网络泵仍存活
  pub fn is_connected(&self) -> bool {
    self.core.is_connected(self.tx.as_ref())
  }

  /// libs/client/GarnetClient.cs:ConnectAsync
  pub async fn connect_async(&mut self) -> Result<()> {
    // 换代前置守卫（rust 显式代际语义自带，C# 一次实例一次连、无换代复用面；
    // 拆净旧代 + 换新幂等位代际单点在 [`network::ConnectionCore::retire_generation`]，
    // 泄漏机理与守卫必要性详见其文档）：门面与 [`Self::reconnect_async`] 均先
    // dispose，本面补的是 pub API 直调二次建连的泄漏守卫。全新实例幂等位本假、
    // 句柄槽本空，置位后仍换新 Arc，零副作用
    self.core.retire_generation();
    // 超时旋钮开启即建泵侧进度计数（对标 C# ConnectAsync 内
    // timeoutMilliseconds > 0 即 Task.Run(TimeoutChecker)）；检查任务 spawn
    // 单点在 [`network::connect_and_spawn`]，时序钉死在 connect 成功之后
    // （建连失败早退路径不残留空转任务）；网络循环收场即置退休标志，检查
    // 任务随之退场（对标 C# Dispose 经
    // timeoutCheckerCts 取消退出，rust 以泵收场信号收场，不持通道引用）。
    // progress 恒为纯超时判成计数，不承载拆连唤醒——拆连唤醒单点在 dispose
    // 面的双向 shutdown 内核完成（C# Dispose(bool) 语义回归，见 [`Self::dispose`]）
    let progress = (self.timeout_millis > 0).then(|| Arc::new(PumpProgress::new()));
    self.progress = progress.clone();
    // 建连编排单点（connect → with_tls → 检查任务 spawn → 通道 → 泵 spawn
    // → 握手）下沉 [`network::connect_and_spawn`]；闸值取在途准入形参原值
    network::connect_and_spawn(
      network::ConnectParams {
        end_point: &self.end_point,
        #[cfg(feature = "tls")]
        tls: self.tls.as_deref(),
        label: "GarnetClient",
        auth_username: self.auth_username.as_deref(),
        auth_password: self.auth_password.as_deref(),
        client_name: self.client_name.as_deref(),
      },
      self.max_outstanding_tasks,
      progress.map(|p| (p, Duration::from_millis(self.timeout_millis))),
      self.network_pool.clone(),
      &mut self.tx,
      Some(Arc::clone(&self.core.disposed)),
      Some(&mut self.core.dispose_handle),
    )
    .await
  }

  /// libs/client/GarnetClient.cs:ReconnectAsync
  pub async fn reconnect_async(&mut self) -> Result<()> {
    // 恒拒守卫（对标 C# `if (Disposed) throw disposeException;` :501）：已
    // Dispose 的实例不得复活——[`Self::connect_async`] 换新生成幂等位，缺此
    // 守卫时 dispose 后重连会把旧代置位抹掉，与 C# 的单向生命周期相反
    if self.core.is_disposed() {
      return Err(Error::Disposed);
    }
    // 对标 C# ReconnectAsync :499-509 先行 socket?.Dispose() 拆旧连再建连
    self.core.dispose();
    self.tx = None;
    self.progress = None;
    self.core.dispose_handle = None;
    self.connect_async().await
  }

  /// libs/client/GarnetClientAPI/GarnetClientExecuteAPI.cs:ExecuteForStringResultAsync
  pub async fn execute_for_string_result_async(&self, command: &[&str]) -> Result<String> {
    roundtrip_str(self.channel()?, command, self.progress.as_deref()).await
  }

  /// C# ExecuteForMemoryResultWithCancellationAsync 同型（精确锚点见 session.rs:93）
  pub async fn execute_for_bytes_result_async(&self, command: &[&[u8]]) -> Result<Vec<u8>> {
    roundtrip_bytes(self.channel()?, command, self.progress.as_deref()).await
  }

  /// libs/client/GarnetClientAPI/GarnetClientExecuteAPI.cs:ExecuteForStringArrayResultAsync
  pub async fn execute_for_string_array_result_async(
    &self,
    command: &[&str],
  ) -> Result<Vec<String>> {
    roundtrip_array(self.channel()?, command, self.progress.as_deref()).await
  }

  /// CLUSTER ATTACH_SYNC 主备同步协商帧（GarnetClient 任务面形态）：帧构造
  /// 复用会话层单点 [`encode_attach_sync_frame`](crate::session::encode_attach_sync_frame)，
  /// 不经逐参数编码路径二次拼令；应答为 bulk string 位点串，
  /// 对端 -ERR 经 [`Error::Server`] 透出
  pub async fn execute_cluster_attach_sync(&self, sync_metadata: &[u8]) -> Result<String> {
    roundtrip_frame_str(
      self.channel()?,
      encode_attach_sync_frame(sync_metadata),
      self.progress.as_deref(),
    )
    .await
  }

  /// libs/client/GarnetClientAPI/GarnetClientExecuteAPI.cs:ExecuteNoResponse
  pub async fn execute_no_response_async(&self, command: &[&[u8]]) -> Result<()> {
    let item = CommandItem::new(command, ReplyTx::None);
    self
      .channel()?
      .send(item)
      .await
      .map_err(|_| Error::ReadPumpExited)
  }

  /// libs/client/GarnetClient.cs:Dispose(bool)
  ///
  /// 无条件拆连面（对标 C# :521-532，与对端是否配合无关）：幂等 swap +
  /// 句柄双向 shutdown 机制单点在 [`network::ConnectionCore::dispose`]（与
  /// [`GarnetClientSession::dispose`](crate::session::GarnetClientSession)
  /// 同形共用，C# 逐臂映射见其文档）；
  /// `timeoutCheckerCts?.Cancel()` 对应面复用既有退休臂（网络循环收场
  /// `progress.retire`，见 [`timeout_checker`](crate::network::timeout_checker)），
  /// 不新增取消通道；progress 语义保持纯超时判成。
  ///
  /// 置位后的命令面分两类：已入通道的在途命令走既有断连错误形态收场——读泵
  /// 退出令在途通道销毁、oneshot 应答端口随之断连，roundtrip 按泵侧标志结算
  /// Timeout（判成在位）或 ResponseChannelClosed（拆连收场），不新开第二套
  /// completion 通道；新提交的命令在 [`Self::channel`] 闸上即刻
  /// [`Error::Disposed`]，不再入通道被动结算。
  pub fn dispose(&self) {
    self.core.dispose();
  }
}

/// C# 侧收口经显式 Dispose 调用链（GarnetServerNode.cs:116-134 gc?.Dispose 等）；
/// rust 的最终收口以最后 Arc 落下承接——Drop 仅转发幂等 dispose 面，
/// 达成「dispose/落尽之后连接资源必然回收」同强契约
///（libs/client/GarnetClient.cs:Dispose(bool) :521-532；gossip 静默对端下
/// 读泵/读半 fd/池缓冲不再滞留，杜绝每轮净漏，见 task/issue 立案）
impl Drop for GarnetClient {
  fn drop(&mut self) {
    self.dispose();
  }
}
