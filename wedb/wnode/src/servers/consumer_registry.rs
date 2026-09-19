//! 活跃消费者注册表
//!
//! 1:1 对标微软 Garnet libs/server/Servers/GarnetServerBase.cs:GarnetServerBase
//! （activeHandlers / TotalConnectionsReceived·Disposed）与
//! libs/server/Servers/GarnetServerTcp.cs:GarnetServerTcp.ActiveConsumers。
//!
//! rust 会话体（RespServerSession）为连接任务独占（C# 为跨线程裸读），注册表
//! 以「注册快照 + 动态字段镜像视图 + kill 触发位」承接枚举面：
//! - 注册/注销时机 = 网络泵建连/释放（C# activeHandlers.TryAdd / TryRemove）；
//! - CLIENT LIST/KILL 与监视器经 [`ConsumerRegistry::active_consumers`] 枚举；
//! - 动态字段（name/db/resp/type 等）由 CLIENT 族命令执行时自刷新（见
//!   client_commands.rs），避免每次 LIST 扫描会话内部状态。

use std::{
  fmt::Write as _,
  sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
  },
  time::Duration,
};

use compio::time::sleep;
use event_listener::{Event, EventListener};
use log::warn;
use parking_lot::{Mutex, RwLock};
use wbase::{
  map::{ConcurrentMap, new_concurrent_map},
  time::now_ms,
};
use wmetric::{
  CommandStats, GarnetLatencyMetricsSession, GarnetSessionMetrics, LatencyMetricsType,
  MonitorIterationInputs, ServerSample,
};

use crate::session_parse_state_extensions::ClientType;

/// 进程级注册表槽（C# RespServerSession.Server 反查服务器的进程级承接；
/// 单服务器语义，CLIENT 族命令经 [`ConsumerRegistry::global`] 直取）
static GLOBAL_REGISTRY: OnceLock<Arc<ConsumerRegistry>> = OnceLock::new();

/// 停机排空截止线毫秒（rust 防悬挂护栏，取 C# DisposeActiveHandlers
/// DEBUG 滞留诊断同值 5s；到期留痕强收）
const DRAIN_TIMEOUT_MS: u64 = 5_000;
/// 停机排空轮询步长毫秒（对偶 C# 自旋间 Thread.Yield）
const DRAIN_POLL_MS: u64 = 25;

/// 会话动态字段镜像视图（C# CLIENT LIST/KILL 直读的 RespServerSession 动态
/// 字段承接：clientName / clientLib* / _userHandle / activeDatabaseId /
/// respProtocolVersion / isSubscriptionSession·clusterSession）
#[derive(Debug, Clone)]
pub struct ClientView {
  /// 客户端名（C# clientName）
  pub name: Option<String>,
  /// 客户端库名（C# clientLibName）
  pub lib_name: Option<String>,
  /// 客户端库版本（C# clientLibVersion）
  pub lib_ver: Option<String>,
  /// 当前认证用户名（C# _userHandle.User.Name）
  pub user: Option<String>,
  /// 活跃库编号（C# activeDatabaseId）
  pub db: u64,
  /// RESP 协议版本（C# respProtocolVersion）
  pub resp: u8,
  /// 客户端类型（C# WriteClientInfo/IsMatch 的有效类型推导）
  pub client_type: ClientType,
}

impl Default for ClientView {
  fn default() -> Self {
    Self {
      name: None,
      lib_name: None,
      lib_ver: None,
      user: None,
      db: 0,
      resp: 2,
      client_type: ClientType::Normal,
    }
  }
}

impl ClientView {
  /// CLIENT INFO flags 字段（C# WriteClientInfo：集群副本 S / 主 M /
  /// 订阅 P / 普通 N）
  fn flags_char(&self) -> char {
    match self.client_type {
      ClientType::Master => 'M',
      ClientType::Replica | ClientType::Slave => 'S',
      ClientType::Pubsub => 'P',
      _ => 'N',
    }
  }
}

/// 单个活跃消费者的注册条目（C# activeHandlers 中的一个 handler+session 对）
pub struct ConsumerEntry {
  /// 会话 id（C# session Id；即网络发送器 id）
  pub id: i64,
  /// 对端端点（C# networkSender.RemoteEndpointName；CLIENT INFO addr）
  pub remote_endpoint: String,
  /// 本地端点（C# networkSender.LocalEndpointName；CLIENT INFO laddr）
  pub local_endpoint: String,
  /// 注册时刻毫秒（C# CreationTicks 同源毫秒 tick）
  pub creation_ticks: i64,
  /// 动态字段镜像视图
  view: Mutex<ClientView>,
  /// 网络入字节镜像（网络泵逐批累加；监视器瞬时吞吐源）
  net_input_bytes: AtomicU64,
  /// 网络出字节镜像（网络泵逐批累加；监视器瞬时吞吐源）
  net_output_bytes: AtomicU64,
  /// 累计命令数镜像（会话消费者逐批同步；监视器瞬时 ops/s 源）
  commands_processed: AtomicU64,
  /// INFO RESET STATS 复位请求位（监视器置位，属主连接任务在下批镜像同步
  /// 前消费并清零独占的会话指标；C# 监视器原地 GetSessionMetrics.Reset()
  /// 的独占所有权承接）
  session_stats_reset: AtomicBool,
  /// 订阅邮箱溢出丢弃数镜像（会话消费者逐批同步；INFO clients 段
  /// pubsub_dropped 聚合项源。C# 直写模型无丢帧语义，此为 rust 有界邮箱
  /// 背压的域界指标，见 wpubsub subscriber.rs 差异登记）
  pubsub_dropped: AtomicU64,
  /// 逐命令统计句柄镜像（C# 监视器经 ActiveConsumers 直查
  /// RespServerSession.GetCommandStats 的承接：会话体独占，镜像共享句柄，
  /// 会话消费者逐批挂接；监视器采样时克隆快照）
  command_stats: RwLock<Option<Arc<Mutex<CommandStats>>>>,
  /// 延迟指标句柄镜像（C# 监视器经 ActiveConsumers 直查
  /// RespServerSession.LatencyMetrics 的承接：会话体独占，镜像共享句柄，
  /// 会话消费者逐批挂接；监视器采样时克隆快照）
  latency_metrics: RwLock<Option<Arc<GarnetLatencyMetricsSession>>>,
  /// KILL 触发位（C# networkSender.TryClose；首杀即真，重复杀假）
  kill_flag: AtomicBool,
  /// 注销标志（泵已释放该连接；唤醒并退出哨兵任务）
  removed: AtomicBool,
  /// KILL/注销广播事件
  kill_event: Event,
}

impl ConsumerEntry {
  /// KILL 语义位（首杀即真，重复杀假；C# networkSender.TryClose /
  /// RespServerSession.cs:TryKill 的注册表侧投影——被杀会话体归其连接任务
  /// 独占，跨会话只能经此触发位 + 网络泵哨兵关闭连接）
  pub fn kill_session(&self) -> bool {
    let first = !self.kill_flag.swap(true, Ordering::AcqRel);
    if first {
      self.kill_event.notify(usize::MAX);
    }
    first
  }

  /// 是否处于终止态（被杀或已注销；哨兵任务探测）
  pub(crate) fn is_terminating(&self) -> bool {
    self.kill_flag.load(Ordering::Acquire) || self.removed.load(Ordering::Acquire)
  }

  /// 订阅终止广播（防竞态：注册监听后复查）
  pub(crate) fn listen_terminate(&self) -> EventListener {
    self.kill_event.listen()
  }

  /// 网络泵逐批累加字节镜像（wnode 网络泵调用面；监视器瞬时吞吐源）
  pub fn add_net_bytes(&self, input: u64, output: u64) {
    if input != 0 {
      self.net_input_bytes.fetch_add(input, Ordering::Relaxed);
    }
    if output != 0 {
      self.net_output_bytes.fetch_add(output, Ordering::Relaxed);
    }
  }

  /// 会话消费者逐批同步累计命令数（wnode 网络泵调用面；对齐 C# 监视器
  /// 直读 sessionMetrics.TotalCommandsProcessed 的累计口径）
  pub fn set_commands_processed(&self, total: u64) {
    self.commands_processed.store(total, Ordering::Release);
  }

  /// INFO RESET STATS 复位条目（C# CleanupGlobalStats 遍历 ActiveConsumers
  /// 的 GetSessionMetrics.Reset() 承接：镜像字节与命令计数即时清零——网络
  /// 字节以条目为累计主体清零即生效；命令数为会话镜像，置复位位由属主
  /// 连接任务下批同步前清零独占会话指标后回灌复位后增量）
  pub fn reset_session_stats(&self) {
    self.net_input_bytes.store(0, Ordering::Relaxed);
    self.net_output_bytes.store(0, Ordering::Relaxed);
    self.commands_processed.store(0, Ordering::Relaxed);
    self.session_stats_reset.store(true, Ordering::Release);
  }

  /// 连接任务消费会话指标复位位（镜像同步前探测；C# 监视器直改会话指标
  /// 在 rust 独占会话体模型下的握手面）
  pub fn take_session_stats_reset(&self) -> bool {
    self.session_stats_reset.swap(false, Ordering::AcqRel)
  }

  /// 复位共享逐命令统计句柄（C# CleanupGlobalStats COMMANDSTATS 分支的
  /// `((RespServerSession)s).GetCommandStats?.Reset()`；句柄共享，直复位）
  pub fn reset_command_stats(&self) {
    if let Some(handle) = self.command_stats.read().clone() {
      handle.lock().reset();
    }
  }

  /// 会话消费者逐批同步订阅邮箱溢出丢弃数（wnode 网络泵调用面；
  /// INFO clients 段 pubsub_dropped 聚合项源）
  pub fn set_pubsub_dropped(&self, total: u64) {
    self.pubsub_dropped.store(total, Ordering::Release);
  }

  /// 订阅邮箱溢出丢弃数快照
  pub fn pubsub_dropped(&self) -> u64 {
    self.pubsub_dropped.load(Ordering::Relaxed)
  }

  /// 挂接逐命令统计句柄（幂等；C# ActiveConsumers 直查 GetCommandStats 的
  /// 共享句柄承接，CommandStatsMonitor 关闭为空操作）
  pub fn attach_command_stats(&self, stats: Option<Arc<Mutex<CommandStats>>>) {
    if stats.is_some() {
      *self.command_stats.write() = stats;
    }
  }

  /// 挂接延迟指标句柄（幂等；C# ActiveConsumers 直查 LatencyMetrics 的
  /// 共享句柄承接，LatencyMonitor 关闭为空操作）
  pub fn attach_latency_metrics(&self, latency: Option<Arc<GarnetLatencyMetricsSession>>) {
    if latency.is_some() {
      *self.latency_metrics.write() = latency;
    }
  }

  /// 逐命令统计快照（监视器采样面；未挂接为 None）
  pub fn command_stats_snapshot(&self) -> Option<CommandStats> {
    let handle = self.command_stats.read().clone()?;
    Some(handle.lock().clone())
  }

  /// 延迟指标快照（监视器采样面；未挂接为 None）
  pub fn latency_metrics_snapshot(&self) -> Option<Arc<GarnetLatencyMetricsSession>> {
    self.latency_metrics.read().clone()
  }

  /// 读取动态字段镜像（值拷贝，不持锁跨调用）
  pub fn client_view(&self) -> ClientView {
    self.view.lock().clone()
  }

  /// 覆写动态字段镜像（CLIENT 族命令执行时自刷新）
  pub fn update_view(&self, view: ClientView) {
    *self.view.lock() = view;
  }

  /// CLIENT LIST 行格式（注册表条目侧投影；行组装收敛于
  /// [`write_client_info_fields`] 单点，与 CLIENT INFO 共用——对标 C#
  /// LIST/INFO 共函数 WriteClientInfo；行尾不含换行，LIST 行不携带
  /// rust 域扩展字段 pubsub-dropped）
  pub fn write_client_info(&self, into: &mut String, now_milliseconds: i64) {
    let view = self.view.lock();
    let age_sec = (now_milliseconds - self.creation_ticks).max(0) / 1_000;
    write_client_info_fields(
      into,
      self.id,
      &self.remote_endpoint,
      &self.local_endpoint,
      age_sec,
      &view,
    );
  }
}

/// CLIENT 行唯一组装点（对标 C# BasicCommands.cs:WriteClientInfo(:1941)——
/// LIST/INFO 共用一函数）。字段序 id addr laddr [name] age [user] flags
/// db resp lib-name lib-ver：name/user 非空才输出，lib-* 空值输出空串，
/// 行尾不含换行。调用方按视图自决尾部扩展（如 INFO 的 pubsub-dropped）。
pub(crate) fn write_client_info_fields(
  into: &mut String,
  id: i64,
  addr: &str,
  laddr: &str,
  age_sec: i64,
  view: &ClientView,
) {
  let _ = write!(into, "id={} addr={} laddr={}", id, addr, laddr);
  if let Some(name) = &view.name {
    let _ = write!(into, " name={name}");
  }
  let _ = write!(into, " age={age_sec}");
  if let Some(user) = &view.user {
    let _ = write!(into, " user={user}");
  }
  let _ = write!(
    into,
    " flags={} db={} resp={} lib-name={} lib-ver={}",
    view.flags_char(),
    view.db,
    view.resp,
    view.lib_name.as_deref().unwrap_or(""),
    view.lib_ver.as_deref().unwrap_or(""),
  );
}

/// 活跃消费者注册表（单服务器语义；C# GarnetServerBase.activeHandlers）
pub struct ConsumerRegistry {
  /// 活跃条目（键 = 会话 id）。注册/注销为连接生命周期事件（低频），
  /// 枚举为 LIST/KILL/采样（读取）——papaya 无锁并表 + gxhash，读路径零阻塞
  entries: ConcurrentMap<u64, Arc<ConsumerEntry>>,
  /// 收到的连接总数（C# totalConnectionsReceived）
  total_connections_received: AtomicI64,
  /// 已释放的连接总数（C# totalConnectionsDisposed）
  total_connections_disposed: AtomicI64,
  /// 在途网络连接计数（C# activeHandlerCount）：accept 成功即刻递增（先于
  /// handler 装配与 register），连接任务结束经 [`ConnectionGuard`] Drop 归零。
  /// 与 entries.len()（枚举在途量）同宿主单点，承接 accept 容量门与 C#
  /// 排空共用的同一计数面
  active_handler_count: AtomicI64,
}

/// 在途连接守卫（RAII）：accept 成功时经 [`ConsumerRegistry::try_acquire_connection`]
/// 取得，随连接任务结束（正常收尾/TLS 握手失败/handler 构造失败）Drop 归零，
/// 对标 C# activeHandlerCount 的 increment/decrement 配对点
pub struct ConnectionGuard {
  registry: Arc<ConsumerRegistry>,
}

impl Drop for ConnectionGuard {
  fn drop(&mut self) {
    self
      .registry
      .active_handler_count
      .fetch_sub(1, Ordering::AcqRel);
  }
}

impl Default for ConsumerRegistry {
  fn default() -> Self {
    Self::new()
  }
}

impl ConsumerRegistry {
  /// 创建注册表（C# GarnetServerBase 构造：activeHandlers = new()）
  pub fn new() -> Self {
    Self {
      entries: new_concurrent_map(),
      total_connections_received: AtomicI64::new(0),
      total_connections_disposed: AtomicI64::new(0),
      active_handler_count: AtomicI64::new(0),
    }
  }

  /// accept 成功后的在途计量与容量门
  ///
  /// 在 garnet 中的相对路径: libs/server/Servers/GarnetServerTcp.cs:HandleNewConnection
  ///
  /// C# 语义：accept 成功即刻 `Interlocked.Increment(ref activeHandlerCount)`，
  /// `networkConnectionLimit == -1 || currentActiveHandlerCount <= networkConnectionLimit`
  /// 放行建 handler；超限臂（GarnetServerTcp.cs:302-307）先 `Decrement` 再
  /// `AcceptSocket.Dispose()`——即刻关闭新连接且不写任何 RESP 应答。rust 对偶：
  /// 超限回退计数返回 None（调用方 drop stream 即关闭），放行返回 RAII 守卫
  /// （Drop 归零，覆盖 handler 构造失败臂）。limit 取 -1 与现状逐字节一致
  pub fn try_acquire_connection(self: &Arc<Self>, limit: i64) -> Option<ConnectionGuard> {
    let n = self.active_handler_count.fetch_add(1, Ordering::AcqRel) + 1;
    if limit == -1 || n <= limit {
      Some(ConnectionGuard {
        registry: Arc::clone(self),
      })
    } else {
      self.active_handler_count.fetch_sub(1, Ordering::AcqRel);
      None
    }
  }

  /// 在途连接计数快照（accept 门同一计数的观测面）
  #[inline]
  pub fn active_handler_count(&self) -> i64 {
    self.active_handler_count.load(Ordering::Acquire)
  }

  /// 进程级安装（幂等；仅首次生效）。CLIENT 族命令与 dispose 归并经
  /// [`ConsumerRegistry::global`] 直取
  pub fn install_global(self: &Arc<Self>) -> bool {
    GLOBAL_REGISTRY.set(Arc::clone(self)).is_ok()
  }

  /// 取进程级注册表（未安装为 None；对齐 C# `Server is GarnetServerBase` 判定）
  pub fn global() -> Option<Arc<Self>> {
    GLOBAL_REGISTRY.get().cloned()
  }

  /// 注册活跃消费者（网络泵建连时调用；C# HandleNewConnection 的
  /// activeHandlers.TryAdd + IncrementConnectionsReceived）
  pub fn register(
    &self,
    id: i64,
    remote_endpoint: String,
    local_endpoint: String,
  ) -> Arc<ConsumerEntry> {
    let entry = Arc::new(ConsumerEntry {
      id,
      remote_endpoint,
      local_endpoint,
      creation_ticks: now_ms().min(i64::MAX as u64) as i64,
      view: Mutex::new(ClientView::default()),
      net_input_bytes: AtomicU64::new(0),
      net_output_bytes: AtomicU64::new(0),
      commands_processed: AtomicU64::new(0),
      session_stats_reset: AtomicBool::new(false),
      pubsub_dropped: AtomicU64::new(0),
      command_stats: RwLock::new(None),
      latency_metrics: RwLock::new(None),
      kill_flag: AtomicBool::new(false),
      removed: AtomicBool::new(false),
      kill_event: Event::new(),
    });
    self.entries.pin().insert(id as u64, Arc::clone(&entry));
    self
      .total_connections_received
      .fetch_add(1, Ordering::Relaxed);
    entry
  }

  /// 注销（网络泵释放时调用；C# DisposeMessageConsumer 的 TryRemove +
  /// IncrementConnectionsDisposed）。不存在为无害空操作
  pub fn unregister(&self, id: i64) {
    if let Some(entry) = self.entries.pin().remove(&(id as u64)) {
      self
        .total_connections_disposed
        .fetch_add(1, Ordering::Relaxed);
      entry.removed.store(true, Ordering::Release);
      entry.kill_event.notify(usize::MAX);
    }
  }

  /// 按会话 id 查条目
  pub fn get(&self, id: i64) -> Option<Arc<ConsumerEntry>> {
    self.entries.pin().get(&(id as u64)).cloned()
  }

  /// libs/server/Servers/GarnetServerTcp.cs:ActiveConsumers
  ///
  /// 全部活跃消费者条目（任意顺序；C# ConcurrentDictionary 枚举语义）
  pub fn active_consumers(&self) -> Vec<Arc<ConsumerEntry>> {
    self.entries.pin().values().cloned().collect()
  }

  /// 连接计数（received, disposed, active；C# TotalConnections* 与
  /// get_conn_active = activeHandlers.Count）
  pub fn connection_totals(&self) -> (i64, i64, i64) {
    (
      self.total_connections_received.load(Ordering::Relaxed),
      self.total_connections_disposed.load(Ordering::Relaxed),
      self.entries.pin().len() as i64,
    )
  }

  /// INFO RESET STATS 的连接计数复位（C# ResetConnectionsReceived /
  /// ResetConnectionsDiposed：received 置当前活跃数，disposed 清零）
  pub fn reset_connection_totals(&self) {
    let active = self.entries.pin().len() as i64;
    self.total_connections_disposed.store(0, Ordering::Relaxed);
    self
      .total_connections_received
      .store(active, Ordering::Relaxed);
  }

  /// 装配单轮采样输入（C# MainMonitorTaskAsync 直查 servers[] 与
  /// ActiveConsumers 的 rust 承接：本注册表即唯一服务器，复位回调经
  /// [`ConsumerRegistry::active_consumers`] 回访条目共享句柄/镜像）
  ///
  /// 复位语义对齐 C#：STATS 复位回清连接计数 + 逐会话指标；COMMANDSTATS
  /// 复位回清逐会话命令统计句柄；延迟复位（全量/单类）回清逐会话延迟句柄。
  pub fn monitor_iteration_inputs(
    self: &Arc<Self>,
  ) -> MonitorIterationInputs<
    impl FnMut(),
    impl FnMut(),
    impl FnMut(),
    impl FnMut(LatencyMetricsType),
  > {
    let conn_reset = Arc::clone(self);
    let cmdstats_reset = Arc::clone(self);
    let latency_all_reset = Arc::clone(self);
    let latency_event_reset = Arc::clone(self);
    MonitorIterationInputs {
      servers: vec![self.monitor_sample()],
      reset_active_sessions: move || {
        conn_reset.reset_connection_totals();
        for entry in conn_reset.active_consumers() {
          entry.reset_session_stats();
        }
      },
      reset_active_command_stats: move || {
        for entry in cmdstats_reset.active_consumers() {
          entry.reset_command_stats();
        }
      },
      // 全事件复位（C# GarnetServerMonitor.cs:344 遍历会话调
      // libs/server/Resp/RespServerSession.cs:ResetAllLatencyMetrics 的
      // rust 载体：registry 持会话延迟句柄直调，不经会话方法二转）
      reset_all_session_latency: move || {
        for entry in latency_all_reset.active_consumers() {
          if let Some(latency) = entry.latency_metrics_snapshot() {
            latency.reset_all();
          }
        }
      },
      reset_session_latency: move |event| {
        for entry in latency_event_reset.active_consumers() {
          if let Some(latency) = entry.latency_metrics_snapshot() {
            latency.reset(event);
          }
        }
      },
    }
  }

  /// 监视器服务器快照（C# MainMonitorTaskAsync 经 ActiveConsumers 直查的
  /// 服务器域承接：连接计数 + 会话采样）。
  ///
  /// 会话指标镜像承接网络字节、命令计数与延迟指标（网络泵逐批写入/挂接）；逐命令统计
  /// 经共享句柄快照承接，会话内部延迟指标随采样与 dispose 域归并
  pub fn monitor_sample(&self) -> ServerSample {
    let pin = self.entries.pin();
    let sessions: Vec<_> = pin
      .values()
      .map(|entry| wmetric::SessionSample {
        metrics: GarnetSessionMetrics {
          total_net_input_bytes: entry.net_input_bytes.load(Ordering::Relaxed),
          total_net_output_bytes: entry.net_output_bytes.load(Ordering::Relaxed),
          total_commands_processed: entry.commands_processed.load(Ordering::Acquire),
          ..GarnetSessionMetrics::default()
        },
        command_stats: entry.command_stats_snapshot(),
        latency: entry.latency_metrics_snapshot(),
      })
      .collect();
    let active = pin.len() as i64;
    let received = self.total_connections_received.load(Ordering::Relaxed);
    let disposed = self.total_connections_disposed.load(Ordering::Relaxed);
    ServerSample {
      total_connections_received: received,
      total_connections_disposed: disposed,
      total_connections_active: active,
      sessions,
    }
  }

  /// 停机排空活跃连接（C# Phase 2：停监听后、拆存储/集群域前调用）
  ///
  /// 在 garnet 中的相对路径: libs/server/Servers/GarnetServerBase.cs:DisposeActiveHandlers
  ///
  /// C# 语义：计数大于零时对 activeHandlers 全量 handler.Dispose() 并
  /// 轮询等待归零；rust 对偶为全量 [`ConsumerEntry::kill_session`]
  /// （kill 位 + 网络泵终止哨兵打断挂起读），连接泵经 unregister 归零
  /// 计数。空闲长连接永不自关，故必须先下杀令而非被动等。超时是 rust
  /// 侧防悬挂护栏（C# 生产语义无限等），到期留痕滞留端点后强收——宿主
  /// 线程 Runtime 析构兜底取消残余任务。
  ///
  /// 调用点必须位于 compio 运行时内（各 accept worker 线程 block_on 尾部、
  /// Runtime 析构前），本线程连接任务由本线程排空等待驱动跑完；多 worker
  /// 并发调用安全：kill 幂等、计数全局。
  pub async fn dispose_active_handlers(&self) {
    let begin = now_ms();
    loop {
      let entries = self.active_consumers();
      if entries.is_empty() {
        return;
      }
      for entry in &entries {
        entry.kill_session();
      }
      if now_ms() - begin >= DRAIN_TIMEOUT_MS {
        let stuck: Vec<&str> = entries.iter().map(|e| e.remote_endpoint.as_str()).collect();
        warn!("停机排空超时，滞留 {} 条连接强收: {stuck:?}", entries.len());
        return;
      }
      sleep(Duration::from_millis(DRAIN_POLL_MS)).await;
    }
  }
}

#[cfg(test)]
mod tests {
  use std::thread;

  use compio::runtime::spawn;

  use super::*;

  /// 注册/注销闭环与连接计数（C# TotalConnectionsReceived/Disposed 语义）
  #[test]
  fn register_unregister_cycles_counters() {
    let registry = ConsumerRegistry::new();
    let entry = registry.register(1, "127.0.0.1:7000".into(), "127.0.0.1:6379".into());
    assert_eq!(registry.connection_totals(), (1, 0, 1));
    assert_eq!(entry.id, 1);
    assert_eq!(entry.remote_endpoint, "127.0.0.1:7000");
    assert_eq!(registry.get(1).map(|e| e.id), Some(1));

    registry.unregister(1);
    assert_eq!(registry.connection_totals(), (1, 1, 0));
    assert!(registry.get(1).is_none());
    // 重复注销为无害空操作
    registry.unregister(1);
    assert_eq!(registry.connection_totals(), (1, 1, 0));
  }

  /// 首杀即真、重复杀假（C# TryKill 语义），注销广播终止态
  #[test]
  fn kill_is_first_shot_only() {
    let registry = ConsumerRegistry::new();
    let entry = registry.register(7, "127.0.0.1:7001".into(), String::new());
    assert!(!entry.is_terminating());
    assert!(entry.kill_session());
    assert!(!entry.kill_session());
    assert!(entry.is_terminating());

    registry.unregister(7);
    assert!(entry.is_terminating());
  }

  /// CLIENT INFO 行字段序逐项对齐 C# WriteClientInfo
  #[test]
  fn client_info_line_matches_csharp_field_order() {
    let registry = ConsumerRegistry::new();
    let entry = registry.register(9, "127.0.0.1:40000".into(), "127.0.0.1:6379".into());
    entry.update_view(ClientView {
      name: Some("tester".into()),
      user: Some("default".into()),
      lib_name: Some("lib".into()),
      lib_ver: Some("1.0".into()),
      db: 2,
      resp: 3,
      client_type: ClientType::Pubsub,
    });

    let mut line = String::new();
    entry.write_client_info(&mut line, entry.creation_ticks + 5_000);
    assert_eq!(
      line,
      "id=9 addr=127.0.0.1:40000 laddr=127.0.0.1:6379 name=tester age=5 \
       user=default flags=P db=2 resp=3 lib-name=lib lib-ver=1.0"
    );
  }

  /// 监视器快照承接连接计数与网络字节镜像
  #[test]
  fn monitor_sample_carries_counters_and_bytes() {
    let registry = ConsumerRegistry::new();
    let entry = registry.register(3, "127.0.0.1:7002".into(), "127.0.0.1:6379".into());
    entry.add_net_bytes(128, 64);

    let sample = registry.monitor_sample();
    assert_eq!(sample.total_connections_received, 1);
    assert_eq!(sample.total_connections_disposed, 0);
    assert_eq!(sample.total_connections_active, 1);
    assert_eq!(sample.sessions.len(), 1);
    assert_eq!(sample.sessions[0].metrics.get_total_net_input_bytes(), 128);
    assert_eq!(sample.sessions[0].metrics.get_total_net_output_bytes(), 64);
  }

  /// 订阅邮箱溢出丢弃数镜像逐批覆写（INFO clients pubsub_dropped 聚合源）
  #[test]
  fn pubsub_dropped_mirror_tracks_session() {
    let registry = ConsumerRegistry::new();
    let entry = registry.register(4, "127.0.0.1:7003".into(), "127.0.0.1:6379".into());
    assert_eq!(entry.pubsub_dropped(), 0);
    entry.set_pubsub_dropped(7);
    assert_eq!(entry.pubsub_dropped(), 7);
  }

  /// INFO RESET STATS 连接计数复位（C# ResetConnectionsReceived 语义）
  #[test]
  fn reset_totals_keeps_active() {
    let registry = ConsumerRegistry::new();
    registry.register(1, "a".into(), String::new());
    registry.register(2, "b".into(), String::new());
    registry.unregister(1);
    registry.reset_connection_totals();
    assert_eq!(registry.connection_totals(), (1, 0, 1));
  }

  /// 在途守卫容量门（C# GarnetServerTcp.cs:236-241/302-307 语义）：
  /// limit=2 时第三条拒绝，释放后可再进；Drop 配对归零
  #[test]
  fn connection_guard_enforces_limit() {
    let registry = Arc::new(ConsumerRegistry::new());
    let g1 = registry.try_acquire_connection(2).unwrap();
    let g2 = registry.try_acquire_connection(2).unwrap();
    assert_eq!(registry.active_handler_count(), 2);
    // 超限拒绝：计数即刻回退，不留在途泄漏
    assert!(registry.try_acquire_connection(2).is_none());
    assert_eq!(registry.active_handler_count(), 2);

    drop(g1);
    assert_eq!(registry.active_handler_count(), 1);
    // 断言产生的临时守卫语句结束即释放，计数回到 1
    assert!(registry.try_acquire_connection(2).is_some());
    drop(g2);
    // 全部释放归零（无漂移），额度可复用
    assert_eq!(registry.active_handler_count(), 0);
  }

  /// limit=-1 不限（与现状逐字节一致：恒放行）
  #[test]
  fn connection_guard_unlimited_when_minus_one() {
    let registry = Arc::new(ConsumerRegistry::new());
    let guards: Vec<_> = (0..64)
      .map(|_| registry.try_acquire_connection(-1).unwrap())
      .collect();
    assert_eq!(registry.active_handler_count(), 64);
    drop(guards);
    assert_eq!(registry.active_handler_count(), 0);
  }

  /// 并发 acquire/release 下计数不漂（fetch_add 与 Guard Drop 配对原子）
  #[test]
  fn connection_guard_concurrent_no_drift() {
    let registry = Arc::new(ConsumerRegistry::new());
    let guards: Vec<_> = (0..8)
      .map(|_| {
        let reg = Arc::clone(&registry);
        thread::spawn(move || {
          let held: Vec<_> = (0..50).map(|_| reg.try_acquire_connection(-1)).collect();
          drop(held);
        })
      })
      .collect();
    for t in guards {
      t.join().unwrap();
    }
    assert_eq!(registry.active_handler_count(), 0);
  }

  /// 停机排空：全量下杀令后等待注销归零（C# DisposeActiveHandlers 语义），
  /// 不依赖 5 秒超时护栏——模拟泵在收到 kill 广播后注销即快速返回
  #[test]
  fn dispose_active_handlers_drains_after_kill() {
    use compio::runtime::Runtime;

    let registry = Arc::new(ConsumerRegistry::new());
    let entry = registry.register(11, "127.0.0.1:7011".into(), String::new());
    let rt = Runtime::new().unwrap();
    let reg = Arc::clone(&registry);
    rt.block_on(async move {
      // 模拟连接泵（kill.rs 哨兵路径）：终止广播命中后走 dispose 注销
      let pump_entry = Arc::clone(&entry);
      let pump_reg = Arc::clone(&reg);
      spawn(async move {
        loop {
          let listener = pump_entry.listen_terminate();
          if pump_entry.is_terminating() {
            break;
          }
          listener.await;
        }
        pump_reg.unregister(pump_entry.id);
      })
      .detach();
      reg.dispose_active_handlers().await;
    });
    assert_eq!(registry.connection_totals(), (1, 1, 0));
    assert!(registry.get(11).is_none());
  }
}
