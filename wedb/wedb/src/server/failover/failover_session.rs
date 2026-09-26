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
  /// 超时半边由外层 timeout 承接）：abort 已置位或 abort_event 先至返回
  /// None，fut 正常完成返回 Some。
  ///
  /// C# cts.Token 为持久语义（取消后任何 await 点即刻抛
  /// OperationCanceledException），而 Event 的一次 notify 只捞得着置位时刻
  /// 已在册的监听者：公告臂等同一中止下先后竞速的串行多臂，首臂吞掉通知后
  /// 后续臂必须凭 aborted 标志前置判落断。先 `listen()` 注册再读标志，
  /// 封死「检查→注册」间隙内通知落空的窗口（dispose 先置位后 notify 的
  /// 配序与此恰好互补）
  pub(super) async fn race_abort<T>(&self, fut: impl Future<Output = T>) -> Option<T> {
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

#[cfg(test)]
mod tests {
  use compio::{
    runtime::{Runtime, spawn},
    time::sleep,
  };

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

  /// 显式超大 failover_timeout（FAILOVER TIMEOUT i64::MAX 毫秒的命令面传播
  /// 形态，C# FailoverCommand.cs:110 正数分支）：deadline 构造饱和不溢出
  /// panic（C# DateTime.Add 天然不溢出，FailoverSession.cs:86），且与常规
  /// 预算的 deadline 排序单调可判、开局不误判超时
  #[test]
  fn huge_failover_timeout_deadline_saturates_monotonic() {
    let cp = ClusterProvider::new();
    let small = FailoverSession::new(
      Arc::clone(&cp),
      FailoverOption::Default,
      None,
      Duration::from_secs(5),
    );
    let huge = FailoverSession::new(
      cp,
      FailoverOption::Default,
      None,
      Duration::from_millis(i64::MAX as u64),
    );
    assert!(
      !huge.failover_timeout_reached() && !small.failover_timeout_reached(),
      "超大超时不得开局误判过期"
    );
    assert!(
      huge.failover_deadline > small.failover_deadline,
      "deadline 排序须单调可判"
    );
  }

  /// i32 界外显式预算不进 600 秒归一臂（deviations §117d 消费面）：命令面
  /// strict_i64（failover.rs 从端秒档/顶层毫秒档）收下 C# TryGetInt int32
  /// 档拒收的 4000000000 形态后，非零直落 failover_timeout 预算原样生效、
  /// 不被归一折叠，开局不误判过期（归一臂单点即上方 is_zero 判定）
  #[test]
  fn i32_overflow_failover_timeout_flows_into_budget_unnormalized() {
    let cp = ClusterProvider::new();
    let session = FailoverSession::new(
      cp,
      FailoverOption::Default,
      None,
      Duration::from_millis(4_000_000_000),
    );
    assert_eq!(
      session.failover_timeout,
      Duration::from_millis(4_000_000_000)
    );
    assert_ne!(
      session.failover_timeout, DEFAULT_FAILOVER_TIMEOUT,
      "界外大值不得被折叠为 600 秒缺省预算（C# int32 拒收档严禁回缩）"
    );
    assert!(!session.failover_timeout_reached());
  }

  /// 负超时即刻失败语义（票 zcode-r26-autofailover 发现三，命令面经
  /// network_cluster_failover 传播 1 纳秒形态）：C# FromSeconds(负) 原样入
  /// 会话，FailoverSession.cs:85 非 default 不归一、:86 deadline 落过去，
  /// ReplicaFailoverSession.cs:89 WaitAsync(负时限) 即抛落 catch 秒败——
  /// 对位断言：非零负传播值绕过 600 秒归一，deadline 开局即过期
  #[test]
  fn negative_failover_timeout_is_immediate_deadline() {
    let cp = ClusterProvider::new();
    let session = FailoverSession::new(cp, FailoverOption::Default, None, Duration::from_nanos(1));
    assert_eq!(
      session.failover_timeout,
      Duration::from_nanos(1),
      "负值传播的必败时长不得被归一为 600 秒缺省预算"
    );
    assert!(
      session.failover_timeout_reached(),
      "deadline 须开局即过期（即刻放弃语义）"
    );
  }

  /// race_abort 取消半边三形态（C# WaitAsync(timeout, cts.Token) 语义面）：
  /// fut 正常完成返回 Some；挂起中被 dispose 打断返回 None；中止置位后才
  /// 进入的调用凭持久标志前置判即刻落 None——C# 取消后任何 await 点持续
  /// 可查，同一中止下串行竞速的多臂（公告臂 gossip→attach）不因首臂吞掉
  /// 唯一一次 notify 而盲等
  #[test]
  fn race_abort_forms() {
    Runtime::new().unwrap().block_on(async {
      let cp = ClusterProvider::new();
      let session = || {
        FailoverSession::new(
          Arc::clone(&cp),
          FailoverOption::Default,
          None,
          Duration::from_secs(5),
        )
      };
      // 正常完成半边：未中止时 fut 结果原样透传
      assert_eq!(session().race_abort(async { 7u8 }).await, Some(7));

      // 在途打断半边：50ms 后 dispose 通知在册监听者，60 秒挂起臂即刻落断
      let session = Arc::new(session());
      {
        let disposer = Arc::clone(&session);
        spawn(async move {
          sleep(Duration::from_millis(50)).await;
          disposer.dispose();
        })
        .detach();
      }
      let start = Instant::now();
      assert!(
        session
          .race_abort(sleep(Duration::from_secs(60)))
          .await
          .is_none(),
        "在途中止应打断挂起臂"
      );
      assert!(start.elapsed() < CoarsetimeDuration::from_secs(5));

      // 持久标志前置判：notify 已被首臂吞掉，迟到进入的第二臂不再盲等事件
      let start = Instant::now();
      assert!(
        session
          .race_abort(sleep(Duration::from_secs(60)))
          .await
          .is_none(),
        "已中止态后任何臂应即刻落断"
      );
      assert!(start.elapsed() < CoarsetimeDuration::from_secs(5));
    });
  }
}
