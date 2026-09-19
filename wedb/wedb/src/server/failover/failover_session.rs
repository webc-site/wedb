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
  client::{GarnetClient, apply_tls},
  server::{
    cluster_config::ClusterConfig,
    cluster_provider::ClusterProvider,
    failover::{failover_option::FailoverOption, failover_status::FailoverStatus},
  },
};

/// failover 超时缺省值（对齐 C#：入参为 default 时取 600 秒）
const DEFAULT_FAILOVER_TIMEOUT: Duration = Duration::from_secs(600);

/// failover 探测客户端构造（凭证 + 出站 TLS 单源透传）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Failover/FailoverSession.cs:75
///（`new GarnetClient(endpoint, TlsOptions?.TlsClientOptions, authUsername:..., authPassword:...)`
/// 的统一 rust 形态；:80 的 IPEndPoint 重载同源）。凭证缺省与显式 None 的
/// 构造差异在 facade 内部归一，两分支收敛一处
pub(super) fn new_failover_client(
  cluster_provider: &ClusterProvider,
  endpoint: String,
) -> GarnetClient {
  let client = GarnetClient::with_config(
    endpoint,
    cluster_provider.cluster_username(),
    cluster_provider.cluster_password(),
    0,
    None,
  );
  apply_tls!(client, cluster_provider);
  client
}

/// libs/cluster/Server/Failover/FailoverSession.cs:FailoverSession
///
/// 会话被后台任务驱动的同时还要对外暴露状态查询（C# 中 status 属性为
/// volatile 读），故全部字段内部可变化、方法一律 `&self`；状态以
/// `AtomicU8` 承载（`FailoverStatus` 为 `#[repr(u8)]`）。
/// C# 以 partial class 把基类底座与主/从流程分写三个文件，rust 无继承：
/// 本类型仅留公共底座（字段 + 超时/状态/abort/dispose），主从流程分居
/// primary_failover_session / replica_failover_session 组合宿主
pub(super) struct FailoverSession {
  pub(super) cluster_provider: Arc<ClusterProvider>,
  /// C# clusterTimeout 属性投影：None = 无限（0 哨兵经
  /// cluster_provider.cluster_node_timeout 单点归一）
  pub(super) cluster_timeout: Option<Duration>,
  pub(super) failover_timeout: Duration,
  pub(super) option: FailoverOption,
  pub(super) clients: Mutex<Vec<Option<Arc<GarnetClient>>>>,
  pub(super) failover_deadline: Instant,
  pub(super) status: AtomicU8,
  pub(super) old_config: ClusterConfig,
  pub(super) primary_client: Mutex<Option<Arc<GarnetClient>>>,
  /// 中断标志（对标 C# CancellationTokenSource.Cancel）
  pub(super) aborted: AtomicBool,
  /// 中断面：dispose 触发即打断在途可中断等待（对标 C# cts.Cancel 事件源）
  pub(super) abort_event: Event,
}

impl FailoverSession {
  /// libs/cluster/Server/Failover/FailoverSession.cs:FailoverSession
  ///
  /// is_replica_session 形参随拆分移除：本构造只建空 clients（副本会话
  /// C# 同口径为空数组），主端探测连接预建迁至 PrimaryFailoverSession::new
  pub(super) fn new(
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
      // 取归一值（C# :86 取原始入参，见上方偏离声明）
      failover_deadline: Instant::now() + CoarsetimeDuration::from(failover_timeout),
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
  pub(super) fn failover_timeout_reached(&self) -> bool {
    Instant::now() > self.failover_deadline
  }

  /// libs/cluster/Server/Failover/FailoverSession.cs:Dispose
  pub(super) fn dispose(&self) {
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
  /// 超时半边由外层 timeout 承接）：abort_event 先至返回 None，fut 正常
  /// 完成返回 Some
  pub(super) async fn race_abort<T>(&self, fut: impl Future<Output = T>) -> Option<T> {
    let mut fut = Box::pin(fut);
    let mut listener = Box::pin(self.abort_event.listen());
    match select(fut.as_mut(), listener.as_mut()).await {
      Either::Left((v, _)) => Some(v),
      Either::Right(((), _)) => None,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// failover 超时单点归一（next 条3）：零时长（CLUSTER FAILOVER 缺省/显式
  /// 0 的命令面传播形态）→ 600 秒（C# FailoverSession.cs:85
  /// `failoverTimeout == default ? TimeSpan.FromSeconds(600)`）；显式正值
  /// 照传。cluster_timeout 半边承载 0 = 无限的 None 哨兵（next 条4），
  /// 构造器原样透传不归一
  ///
  /// deadline 半边焊住偏离：与 C# :86 不同，归一值同源喂给 deadline，零超时
  /// 不开局过期
  #[test]
  fn zero_failover_timeout_normalizes_to_six_hundred_seconds() {
    let cp = ClusterProvider::new();
    let session = FailoverSession::new(
      Arc::clone(&cp),
      FailoverOption::Default,
      None,
      Duration::ZERO,
    );
    assert_eq!(session.failover_timeout, DEFAULT_FAILOVER_TIMEOUT);
    assert_eq!(DEFAULT_FAILOVER_TIMEOUT, Duration::from_secs(600));
    assert!(session.failover_deadline > Instant::now() + CoarsetimeDuration::from_secs(599));
    assert!(!session.failover_timeout_reached());
    assert_eq!(session.cluster_timeout, None);
    let session = FailoverSession::new(
      cp,
      FailoverOption::Default,
      Some(Duration::from_secs(5)),
      Duration::from_secs(30),
    );
    assert_eq!(session.failover_timeout, Duration::from_secs(30));
    assert!(session.failover_deadline > Instant::now() + CoarsetimeDuration::from_secs(29));
    assert_eq!(session.cluster_timeout, Some(Duration::from_secs(5)));
  }
}
