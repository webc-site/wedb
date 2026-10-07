use std::{
  future::Future,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, Ordering},
  },
  time::Duration,
};

use coarsetime::{Duration as CoarsetimeDuration, Instant};
use event_listener::Event;
use futures_util::future::{Either, select};
use parking_lot::Mutex;

use crate::{
  client::GarnetClient,
  server::{
    cluster_config::ClusterConfig,
    cluster_provider::ClusterProvider,
    failover::{failover_option::FailoverOption, failover_status::FailoverStatus},
  },
};

/// failover 超时缺省值（对齐 C#：入参为 default 时取 600 秒）
pub const DEFAULT_FAILOVER_TIMEOUT: Duration = Duration::from_secs(600);

/// failover 探测客户端构造（走 [`ClusterProvider::new_outbound_client`] 单点）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Failover/FailoverSession.cs:75
///（`new GarnetClient(endpoint, TlsOptions?.TlsClientOptions, authUsername:..., authPassword:...)`
/// 的统一 rust 形态；:80 的 IPEndPoint 重载同源）。凭证、出站 TLS 与网络池
/// 均由 ClusterProvider 单点承载
pub(super) fn new_failover_client(
  cluster_provider: &ClusterProvider,
  endpoint: String,
) -> GarnetClient {
  cluster_provider.new_outbound_client(endpoint, 0, None)
}

/// libs/cluster/Server/Failover/FailoverSession.cs:FailoverSession
///
/// 会话被后台任务驱动的同时还要对外暴露状态查询（C# 中 status 属性为
/// volatile 读），故全部字段内部可变化、方法一律 `&self`；状态以
/// `AtomicU8` 承载（`FailoverStatus` 为 `#[repr(u8)]`）。
/// C# 以 partial class 把基类底座与主/从流程分写三个文件，rust 无继承：
/// 本类型仅留公共底座（字段 + 超时/状态/abort/dispose），主从流程分居
/// primary_failover_session / replica_failover_session 组合宿主
pub struct FailoverSession {
  pub cluster_provider: Arc<ClusterProvider>,
  /// C# clusterTimeout 属性投影：None = 无限（0 哨兵经
  /// cluster_provider.cluster_node_timeout 单点归一）
  pub cluster_timeout: Option<Duration>,
  pub failover_timeout: Duration,
  pub option: FailoverOption,
  pub clients: Mutex<Vec<Option<Arc<GarnetClient>>>>,
  pub failover_deadline: Instant,
  pub status: AtomicU8,
  pub old_config: ClusterConfig,
  pub primary_client: Mutex<Option<Arc<GarnetClient>>>,
  /// 中断标志（对标 C# CancellationTokenSource.Cancel）
  pub aborted: AtomicBool,
  /// 中断面：dispose 触发即打断在途可中断等待（对标 C# cts.Cancel 事件源）
  pub abort_event: Event,
}

impl FailoverSession {
  /// libs/cluster/Server/Failover/FailoverSession.cs:FailoverSession
  ///
  /// is_replica_session 形参随拆分移除：本构造只建空 clients（副本会话
  /// C# 同口径为空数组），主端探测连接预建迁至 PrimaryFailoverSession::new
  pub fn new(
    cluster_provider: Arc<ClusterProvider>,
    option: FailoverOption,
    cluster_timeout: Option<Duration>,
    failover_timeout: Duration,
  ) -> Self {
    let old_config = cluster_provider
      .cluster_manager()
      .map(|cm| cm.current_config().clone())
      .unwrap_or_default();

    // 偏离声明（C# FailoverSession.cs:85/:86 不对称）：C# 只归一字段 failoverTimeout
    // （:85，供各 op 的 WaitAsync），deadline 却按原始入参算（:86 漏 `this.`，#293
    // 重构前用的是归一后的 this.failoverTimeout），零超时即开局过期，DEFAULT 无
    // TIMEOUT 路径在位点未追平时必中止。rust 单点归一、两处同源，守 C# :38
    // 「End to end timeout for failover」的 600 秒缺省预算
    let failover_timeout = if failover_timeout.is_zero() {
      DEFAULT_FAILOVER_TIMEOUT
    } else {
      failover_timeout
    };

    Self {
      cluster_provider,
      cluster_timeout,
      failover_timeout,
      option,
      clients: Mutex::new(Vec::new()),
      // 取归一值（C# :86 取原始入参，见上方偏离声明）。saturating_add 兜
      // 显式超大 TIMEOUT（如 i64::MAX 毫秒，coarsetime ticks 转换已饱和至
      // tick 上界）与单调钟裸加溢出的场景：C# DateTime.Add 永不溢出，
      // rust 侧 deadline 饱和后依旧远在未来，超时判定语义不变
      //
      // 必败档单独落点：C# 负 TimeSpan 的 deadline 落在过去（FailoverSession.cs:86
      // 取原始入参，:89 WaitAsync 即抛）；rust Duration 无负，命令面传播的
      // 1 纳秒形态即负值档——coarsetime 毫秒粒度会把它截成 now+0，等号
      // 不触发即永不过期，故亚毫秒必败档显式落过去（配置粒度为秒，该区间
      // 无合法正超时来源，零误伤）
      failover_deadline: if failover_timeout < Duration::from_millis(1) {
        Instant::now().saturating_sub(CoarsetimeDuration::from_millis(1))
      } else {
        Instant::now().saturating_add(CoarsetimeDuration::from(failover_timeout))
      },
      status: AtomicU8::new(FailoverStatus::BeginFailover as u8),
      old_config,
      primary_client: Mutex::new(None),
      aborted: AtomicBool::new(false),
      abort_event: Event::new(),
    }
  }

  /// libs/cluster/Server/Failover/FailoverSession.cs:status
  #[inline]
  pub(super) fn status(&self) -> FailoverStatus {
    FailoverStatus::from_repr(self.status.load(Ordering::Acquire)).unwrap_or_default()
  }

  /// 是否已被中断（对标 C# CancellationToken.IsCancellationRequested）
  #[inline]
  pub(super) fn is_aborted(&self) -> bool {
    self.aborted.load(Ordering::Acquire)
  }

  /// Setter for status (FailoverSession.cs status.set)
  #[inline]
  pub(super) fn set_status(&self, status: FailoverStatus) {
    self.status.store(status as u8, Ordering::Release);
  }

  /// libs/cluster/Server/Failover/FailoverSession.cs:FailoverTimeout
  pub fn failover_timeout_reached(&self) -> bool {
    Instant::now() > self.failover_deadline
  }

  /// libs/cluster/Server/Failover/FailoverSession.cs:Dispose
  pub fn dispose(&self) {
    self.aborted.store(true, Ordering::Release);
    // 打断在途可中断等待（对标 C# cts.Cancel；会话单任务驱动，MAX 兜底
    // 多监听者并存）
    self.abort_event.notify(usize::MAX);
    self.dispose_connections();
  }

  /// libs/cluster/Server/Failover/FailoverSession.cs:DisposeConnections
  ///
  /// 仅处置会话自持探测连接（C# 同口径：副本会话 clients 为空数组）。
  /// primary_client 为停写专用连接，归收口路径 reset_if_needed 独占处置
  /// ——此处提前回收会让 abort 后的复位停写无连接可用，主端停写状态悬空
  fn dispose_connections(&self) {
    let mut clients = self.clients.lock();
    for slot in clients.iter_mut() {
      if let Some(c) = slot.take() {
        c.dispose();
      }
    }
  }

  /// 中断事件与 fut 竞速（对标 C# WaitAsync(timeout, cts.Token) 的取消半边：
  /// 超时半边由外层 timeout 承接）：abort 已置位或 abort_event 先至返回
  /// None，fut 正常完成返回 Some。
  ///
  /// C# cts.Token 为持久语义（取消后任何 await 点即刻抛
  /// OperationCanceledException），而 Event 的一次 notify 只捞得着置位时刻
  /// 已在册的监听者：公告臂等同一中止下先后竞速的串行多臂，首臂吞掉通知后
  /// 后续臂必须凭 aborted 标志前置判落断。先 `listen()` 注册再读标志，
  /// 封死「检查→注册」间隙内通知落空的窗口（dispose 先置位后 notify 的
  /// 配序与此恰好互补）
  pub async fn race_abort<T>(&self, fut: impl Future<Output = T>) -> Option<T> {
    let mut fut = Box::pin(fut);
    let mut listener = Box::pin(self.abort_event.listen());
    if self.is_aborted() {
      return None;
    }
    match select(fut.as_mut(), listener.as_mut()).await {
      Either::Left((v, _)) => Some(v),
      Either::Right(((), _)) => None,
    }
  }
}
