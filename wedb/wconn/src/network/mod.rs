//! 客户端网络全双工读写双泵：命令帧编码 + 发送/接收解耦循环
//!
//! 在 garnet 中的相对路径:
//! - `libs/client/GarnetClient.cs`
//! - `libs/client/ClientSession/GarnetClientSession.cs`
//! - `libs/client/ClientTcpNetworkSender.cs`
//! - `libs/client/GarnetClientProcessReplies.cs`
//!
//! [`GarnetClient`](crate::client::GarnetClient) 与
//! [`GarnetClientSession`](crate::session::GarnetClientSession) 共用同一实现。
//! 子模块按 garnet libs/client 分文件拓扑拆分：
//! - [`encode`]：命令帧编码（ClientTcpNetworkSender.cs:Send /
//!   GarnetClientSession.cs:FormatRESP）；
//! - [`replies`]：帧解析与队列派发（GarnetClientProcessReplies.cs）；
//! - [`pump`]：读写双泵循环（ClientTcpNetworkSender.cs 发送流 +
//!   GarnetClientProcessReplies.cs 读循环）。
//!
//! 对标 libs/client 发送流（GarnetClientTcpNetworkSender 刷 socket）与接收流
//! （libs/client/GarnetClientProcessReplies.cs:ProcessReplies 认领应答）的
//! 解耦模型：`TcpStream::into_split` 拆分读写两半，写泵与读泵并发运行
//! （compio 单线程运行时内双任务，io_uring 并发 SQE），发送持续可推进，
//! 不被在途应答的认领进度串行阻滞；读泵独立读应答并按帧序认领回传
//! oneshot（C# TaskCompletionSource 的零锁等价物）。
//!
//! 退出闭环：读泵 EOF/协议错误退出 → 复位存活标志 → 写泵在限时收命令的
//! 超时窗内感知并退出、丢弃 rx，发送端经 `is_connected` 观测断连；调用方
//! 全部退场 → 写泵命令通道断连、shutdown 写半 → 读泵 EOF 收场，fd 两半
//! 全 drop 关闭连接（此路径属计划内关闭，不上抛错误）。静默对端不配合回
//! 拆时，客户端 dispose 面经 [`stream::DisposeHandle`] 双向 shutdown 令常驻
//! 读即刻落定（C# `GarnetClient.cs:Dispose(bool)` socket 无条件拆 fd 同强
//! 契约），读泵沿既有读收场支退出，收场分类经 dispose 幂等位判为计划内。

mod encode;
mod pump;
pub mod replies;
pub(crate) mod stream;

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use compio::{runtime::spawn, time::sleep};
use crossfire::mpsc;
pub use encode::encode_command;
pub use pump::{READ_CHUNK, read_pump, write_pump};
pub(crate) use pump::{network_loop, resolve_network_pool};
pub use replies::dispatch_replies;
pub use stream::OutStream;
use wbase::pool::LimitedFixedBufferPool;
#[cfg(feature = "tls")]
use wtls::ClientTlsConfig;

use self::stream::DisposeHandle;
use crate::{
  Error, Result,
  types::{ChannelTx, PumpProgress, roundtrip_str},
};

/// 连接拆连核心件（[`GarnetClient`](crate::client::GarnetClient) 与
/// [`GarnetClientSession`](crate::session::GarnetClientSession) 五面同形的
/// 单点承接，模块内结构体字段直接曝光）：dispose 幂等位 + 拆连句柄槽，
/// 拆连契约（幂等 swap、双向 shutdown、换代前置守卫、健康判定口径）单点在
/// 本件，双方组合持有不再各写第二套
pub(crate) struct ConnectionCore {
  /// dispose 幂等位（对标 C# `Dispose` 的
  /// `Interlocked.Increment(ref disposed) > 1` 守卫位）：显式 dispose 与最后
  /// Arc 落下共同触达，一次置位即不再拆连；网络循环以只读镜像收场分类；
  /// 换代建连换新 Arc（[`Self::retire_generation`]）
  pub disposed: Arc<AtomicBool>,

  /// 拆连句柄槽（建连期捕获的底层 socket fd 自持 dup 副本，`Send + Sync`，
  /// 形态理由见 [`stream::DisposeHandle`] 文档）：dispose 面双向 shutdown 的
  /// fd 触达单点，对位 C# Dispose 的 `socket?.Dispose()` 无条件拆连
  pub dispose_handle: Option<DisposeHandle>,
}

impl ConnectionCore {
  /// 全新未连代际（幂等位零位、句柄槽空）
  pub fn new() -> Self {
    Self {
      disposed: Arc::new(AtomicBool::new(false)),
      dispose_handle: None,
    }
  }

  /// dispose 幂等位读取（命令闸拒绝、健康判定与 reconnect 恒拒守卫共读此位）
  #[inline]
  pub fn is_disposed(&self) -> bool {
    self.disposed.load(Ordering::Acquire)
  }

  /// 幂等拆连面（client/session `dispose()` 的单点实现）：首次置位即对句柄
  /// 双向 shutdown（常驻读即刻以 EOF/err 内核落定，读泵沿既有读收场支退出，
  /// 读写两半句柄随泵收场 drop、fd 关闭），二次调用即返
  pub fn dispose(&self) {
    if self.disposed.swap(true, Ordering::AcqRel) {
      return;
    }
    if let Some(handle) = &self.dispose_handle {
      handle.shutdown_both();
    }
  }

  /// 建连换代前置守卫（client/session `connect_async` 开臂同款）：先把旧代走
  /// 一次完整 dispose 面拆净——旧拆连句柄双向 shutdown 令旧读泵即刻落定、
  /// 旧写泵随 tx 弃置收场，句柄随槽位置空而 drop；再换新幂等位代际（旧代
  /// disposed Arc 随旧泵收场退役）。缺此守卫时旧句柄被建连期
  /// `*dispose_slot = Some(..)` 静默覆盖：只关 dup fd、不下发 shutdown，旧连
  /// 拆除全赖旧 tx 断→writer_done 路；旧写泵一旦钉在挂起点（write_all 挂对端
  /// 不读/缓冲满，或在途队列满挂 in_flight send），该路失效——旧读写泵、fd、
  /// 池借出缓冲永不回收，且再无句柄可拆（Drop 兜底只触新代）。全新实例幂等
  /// 位本假、句柄槽本空，置位后仍换新 Arc，零副作用
  pub fn retire_generation(&mut self) {
    self.dispose();
    self.dispose_handle = None;
    self.disposed = Arc::new(AtomicBool::new(false));
  }

  /// 连接健康判定（client/session `is_connected` 的同形口径单点，对标 C#
  /// IsConnected 的 `socket != null && socket.Connected && !Disposed`）：
  /// dispose 幂等位置位即假；否则通道在位且网络泵仍存活——泵任务退出
  ///（EOF/断链/协议错误/在途超时/dispose 拆连）即 rx 全部丢弃，发送端
  /// `is_disconnected` 翻真，零轮询零探测包
  #[inline]
  pub fn is_connected(&self, tx: Option<&ChannelTx>) -> bool {
    !self.is_disposed() && tx.is_some_and(|tx| !tx.is_disconnected())
  }
}

/// 建连参数（client/session 共用编排的差异面收敛：端点、可选出站 TLS 与
/// 握手身份三参）
pub(crate) struct ConnectParams<'a> {
  /// 目标端点（TCP `host:port` 或 Unix 域套接字路径）
  pub end_point: &'a str,
  /// 出站 TLS 配置（None = 明文，对标 C# ConnectAsync 里
  /// SslStream.AuthenticateAsClientAsync 分支）
  #[cfg(feature = "tls")]
  pub tls: Option<&'a ClientTlsConfig>,
  /// 握手 LIB-NAME 与网络循环退出日志共用标识（spawn 闭包捕获需 'static）
  pub label: &'static str,
  /// AUTH 用户名（None 走 C# default 用户回退）
  pub auth_username: Option<&'a str>,
  /// AUTH 密码
  pub auth_password: Option<&'a str>,
  /// CLIENT SETNAME 名（None 跳过 SETINFO/SETNAME）
  pub client_name: Option<&'a str>,
}

/// client/session 建连编排单点：connect（同点捕获拆连句柄）→ with_tls →
/// 有界命令通道 → tx 置位 → 解析缓冲池 → spawn 网络循环 → 握手 → 握手失败拆通道
///
/// [`GarnetClient`](crate::client::GarnetClient) 与
/// [`GarnetClientSession`](crate::session::GarnetClientSession) 的
/// connect_async 同序骨架，差异收敛为形参：
/// - `gate`：命令通道与泵内在途队列共用容量口径（client 传在途准入形参
///   `max_outstanding_tasks`，无 .max 抬升，在途满即调用方 send 挂起退避，
///   对标 C# InputGateAsync；session 传 `CHANNEL_CAP` 单源定容）；
/// - `progress`：泵侧进度计数（client 超时旋钮开启时建并 spawn
///   timeout_checker，session 恒 None——会话层无在途命令超时旋钮）；
/// - `disposed`：dispose 幂等位只读镜像（client/session 均传入，网络循环
///   收场分类面用其把显式拆连判为计划内退出）；
/// - `dispose_slot`：拆连句柄出参槽（[`OutStream::connect`] 捕获，
///   [`GarnetClient`](crate::client::GarnetClient) 与
///   [`GarnetClientSession`](crate::session::GarnetClientSession) 均传真槽
///   存为字段，供各自 dispose 面触达）；None = 无拆连需求的调用方，句柄
///   不落槽即刻回收（dup fd 副本随即 close，不影响泵侧流存活）。
///
/// 建连控制面非热，无内联诉求。
pub(crate) async fn connect_and_spawn(
  params: ConnectParams<'_>,
  gate: usize,
  progress: Option<(Arc<PumpProgress>, Duration)>,
  network_pool: Option<Arc<LimitedFixedBufferPool>>,
  tx_slot: &mut Option<ChannelTx>,
  disposed: Option<Arc<AtomicBool>>,
  dispose_slot: Option<&mut Option<stream::DisposeHandle>>,
) -> Result<()> {
  // 端点形态分派建连：TCP 臂设 nodelay、Unix 域套接字臂不设（判定单源
  // `wbase::endpoint::uds_path`，与入站监听端点共读同一条规则）；拆连句柄
  // 在此同点捕获（TLS 包裹前，dup 副本与原句柄同指一条 socket 本体）
  let (stream, dispose_handle) = OutStream::connect(params.end_point).await?;
  if let Some(slot) = dispose_slot {
    *slot = Some(dispose_handle);
  }
  // TLS 配置在位即在 TCP 之上完成握手包裹；无配置保持明文字节流
  #[cfg(feature = "tls")]
  let stream = stream.with_tls(params.tls, params.end_point).await?;

  // 超时旋钮开启即 spawn 超时检查任务——时序钉死在 connect 成功之后（对标
  // C# ConnectAsync 内 timeoutMilliseconds > 0 即 Task.Run(TimeoutChecker)，
  // 时序位置同在连接建立后）：connect/TLS 失败 `?` 早退路径不 spawn，否则
  // progress 三计数恒 0、退休标志（network_loop 收场唯一置位）永不置位，
  // 检查任务 sleep 周期空转永不退场（gossip 每轮建连失败即泄一个）
  let progress = progress.map(|(p, period)| {
    spawn(timeout_checker(Arc::clone(&p), period)).detach();
    p
  });

  // 命令通道容量即闸值：闸值随网络循环透传泵内定容，单点定义无第二容量口径
  let (tx, rx) = mpsc::bounded_async(gate);
  *tx_slot = Some(tx);

  // 读泵接收缓冲池解析（注入优先，未注入即本端自建，C# 同型回退）
  let network_pool = resolve_network_pool(network_pool);

  spawn(async move {
    if let Err(e) = network_loop(stream, rx, progress, gate, network_pool, disposed).await {
      log::error!("{} 网络循环退出: {e}", params.label);
    }
  })
  .detach();

  let handshake = handshake(
    tx_slot.as_ref().ok_or(Error::NotConnected)?,
    params.label,
    params.auth_username,
    params.auth_password,
    params.client_name,
  )
  .await;
  if handshake.is_err() {
    // 握手失败（AUTH/SETINFO 报错，C# 同样上抛）：丢弃通道使网络泵退出、
    // 连接关闭，避免僵尸连接上继续排队后续命令
    *tx_slot = None;
  }
  handshake
}

/// 执行单条字符串命令往返
#[inline]
async fn exec(tx: &ChannelTx, command: &[&str]) -> Result<String> {
  roundtrip_str(tx, command, None).await
}

/// 在途命令超时检查：周期比较泵侧三进度，皆无进展且在途非空即判成
///
/// 在 garnet 中的相对路径: libs/client/GarnetClient.cs:TimeoutChecker
///
/// spawn 单点在 [`connect_and_spawn`]：connect 成功后才起任务（回归教训：
/// spawn 先于 connect 时，建连失败路径退休标志永不置位，任务空转泄漏）。
/// 判成后置泵侧标志收场：读泵在限时读粒度内见位退出、在途 oneshot 随通道
/// 销毁断连，roundtrip 按标志结算 [`Error::Timeout`]——对标 C# 判成
/// Dispose() 拆客户端、在途 TaskCompletionSource 全部置错。网络循环收场
/// 置退休标志后本任务退场（对标 C# Dispose 经 timeoutCheckerCts 取消退出，
/// rust 不持通道引用，不阻断「调用方退场 → 泵自灭」闭环）
pub async fn timeout_checker(progress: Arc<PumpProgress>, period: Duration) {
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

/// 连接后握手序列：AUTH（用户名优先，缺省密码按空串补齐）+ CLIENT SETINFO/SETNAME
///
/// （C# ConnectAsync 内的 AUTH/CLIENT SETINFO 握手子步骤，客户端与会话共用同一口径，SETINFO/SETNAME 同以 clientName 非空为前提）
pub(super) async fn handshake(
  tx: &ChannelTx,
  lib_name: &str,
  auth_username: Option<&str>,
  auth_password: Option<&str>,
  client_name: Option<&str>,
) -> Result<()> {
  if auth_username.is_some() || auth_password.is_some() {
    let user = auth_username.unwrap_or("default");
    let pass = auth_password.unwrap_or("");
    exec(tx, &["AUTH", user, pass]).await?;
  }
  if let Some(client_name) = client_name {
    exec(tx, &["CLIENT", "SETINFO", "LIB-NAME", lib_name]).await?;
    exec(tx, &["CLIENT", "SETNAME", client_name]).await?;
  }
  Ok(())
}
