//! 活跃消费者注册表
//!
//! 1:1 对标微软 Garnet libs/server/Servers/GarnetServerBase.cs:GarnetServerBase
//! （activeHandlers / TotalConnectionsReceived·Disposed）与
//! libs/server/Servers/GarnetServerTcp.cs:GarnetServerTcp.ActiveConsumers。
//!
//! rust 会话体（RespServerSession）为连接任务独占（C# 为跨线程裸读），注册表
//! 以「注册快照 + 动态字段投影 + kill 触发位」承接枚举面：
//! - 注册/注销时机 = accept 成功预注册 / 连接收场（C# activeHandlers.TryAdd /
//!   TryRemove 时机——TryAdd 在 handler.Start 前，rust 同前移至 TLS 握手与
//!   首字节读取之前，握手期/空闲连接入治理面；C# CLIENT 枚举对 Session 为
//!   null 的条目不可见，rust 以 ClientView 默认投影承接为可见）；
//! - CLIENT LIST/KILL 与监视器经 [`ConsumerRegistry::active_consumers`] 枚举；
//! - 动态字段（name/db/resp/type 等）真值单源在会话字段，条目持一份跨线程可读
//!   投影：他者会话行由网络泵每批汇聚单点整体重导（见
//!   `resp_session_consumer.rs:mirror_session_counters`），本会话行在 LIST/KILL
//!   入口即时刷新（见 `client_commands.rs`）。C# 为跨线程无锁裸读会话活字段，
//!   rust 所有权模型不容无锁裸读，投影即其等价承接。
//! - 会话延迟指标不入采样面：延迟双缓冲槽为连接任务独占，样本由属主线程在
//!   版本翻转点按引用并入全局延迟表，条目因此不持延迟句柄镜像——C# 监视器
//!   直查会话延迟表并跨线程复位会话槽的两臂在 rust 归属转移，镜像只会把
//!   Thread-per-core 零锁不变式退化成写锁竞争。

use std::{
  fmt::Write as _,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64, Ordering},
  },
  time::Duration,
};

use compio::time::sleep;
use event_listener::Event;
use log::warn;
use parking_lot::{Mutex, RwLock};
use wbase::{
  map::{ConcurrentMap, new_concurrent_map},
  time::now_ms_i64,
};
use wmetric::{
  CommandStats, GarnetSessionMetrics, MonitorIterationInputs, ServerSample, SessionMetricsHandle,
};

use crate::session_parse_state_extensions::ClientType;

/// 进程级注册表槽（C# RespServerSession.Server 反查服务器的进程级承接；
/// 单服务器语义，CLIENT 族命令经 [`ConsumerRegistry::global`] 直取）
///
/// 单实例进程契约：本槽为进程级单例（C# 对位 RespServerSession 的 per-server
/// 反向引用，rust 以进程槽承接同一单服务器语义），多实例同进程装配（多库宿主、
/// 测试并行）时后装实例覆盖式接管——先装实例的会话将枚举到后装实例的连接面，
/// 自身连接不可见不可 KILL；覆盖发生在 [`ConsumerRegistry::install_global`]
/// 装配点 warn 留痕。实例级治理面路由（句柄经装配链注入会话）为后续票射程
static GLOBAL_REGISTRY: RwLock<Option<Arc<ConsumerRegistry>>> = RwLock::new(None);

/// 停机排空截止线毫秒（rust 防悬挂护栏，取 C# DisposeActiveHandlers
/// DEBUG 滞留诊断同值 5s；到期留痕强收）
const DRAIN_TIMEOUT_MS: u64 = 5_000;
/// 停机排空轮询步长毫秒（对偶 C# 自旋间 Thread.Yield）
const DRAIN_POLL_MS: u64 = 25;

/// 会话动态字段投影（C# CLIENT LIST/KILL 直读的 RespServerSession 动态
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
  /// pubsub 邮箱满水位拒收累计帧数（rn14 观测盲区收口：慢订阅者丢尾量，
  /// 真值单源为会话邮箱计数器，随本投影同一发布轨跨线程可读；
  /// C# 无此面属 rust 自有分叉 §14 配套）
  pub pubsub_dropped: u64,
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
      pubsub_dropped: 0,
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

/// 消费者分类（按持有执行域与拓扑角色划分，用于在线引擎置换清扫与治理过滤）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum ConsumerType {
  /// 客户端会话消费者（持有旧引擎执行域，置换时须清扫）
  #[default]
  Client = 0,
  /// 复制消费者（跨节点主从同步链路，不持旧引擎执行域，置换时豁免）
  Replication = 1,
  /// 集群总线 / 节点间互连消费者（不持旧引擎执行域，置换时豁免）
  Cluster = 2,
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
  /// 动态字段投影（会话字段真值的跨线程可读快照，每批汇聚重导）
  view: Mutex<ClientView>,
  /// 网络入字节镜像（网络泵逐批累加；监视器瞬时吞吐源）
  net_input_bytes: AtomicU64,
  /// 网络出字节镜像（网络泵逐批累加；监视器瞬时吞吐源）
  net_output_bytes: AtomicU64,
  /// 累计命令数镜像（会话消费者逐批同步；监视器瞬时 ops/s 源）
  commands_processed: AtomicU64,
  /// 逐命令统计句柄镜像（C# 监视器经 ActiveConsumers 直查
  /// RespServerSession.GetCommandStats 的承接：会话体独占，镜像共享句柄，
  /// 会话消费者逐批挂接；监视器采样时克隆快照）
  command_stats: RwLock<Option<Arc<Mutex<CommandStats>>>>,
  /// 会话指标句柄镜像（C# 监视器经 ActiveConsumers 直查
  /// RespServerSession.GetSessionMetrics 的承接：会话体独占，镜像共享句柄，
  /// 会话消费者逐批挂接；监视器采样时克隆全字段快照，复位敏感三计数
  /// 仍读条目镜像——INFO RESET STATS 即时清零生效面）
  session_metrics: RwLock<Option<Arc<SessionMetricsHandle>>>,
  /// KILL 触发位（C# networkSender.TryClose；首杀即真，重复杀假）
  kill_flag: AtomicBool,
  /// 慢路径挂起执行中标志（置换清扫时豁免误杀发起/在途会话）
  in_slow_wait: AtomicBool,
  /// 注销标志（泵已释放该连接；唤醒并退出哨兵任务）
  removed: AtomicBool,
  /// KILL/注销广播事件
  kill_event: Event,
  /// 消费者分类标志（置换清扫过滤源）
  consumer_type: AtomicU8,
}

impl ConsumerEntry {
  /// 读取消费者分类
  #[inline]
  pub fn consumer_type(&self) -> ConsumerType {
    match self.consumer_type.load(Ordering::Acquire) {
      n if n == ConsumerType::Replication as u8 => ConsumerType::Replication,
      n if n == ConsumerType::Cluster as u8 => ConsumerType::Cluster,
      _ => ConsumerType::Client,
    }
  }

  /// 设置消费者分类
  #[inline]
  pub fn set_consumer_type(&self, consumer_type: ConsumerType) {
    self
      .consumer_type
      .store(consumer_type as u8, Ordering::Release);
  }

  /// 是否为持有存储引擎执行域的客户端会话
  #[inline]
  pub fn is_client_session(&self) -> bool {
    self.consumer_type() == ConsumerType::Client
  }

  /// 标记或清除慢路径挂起状态
  #[inline]
  pub fn set_in_slow_wait(&self, val: bool) {
    self.in_slow_wait.store(val, Ordering::Release);
  }

  /// 是否处于慢路径挂起中
  #[inline]
  pub fn is_in_slow_wait(&self) -> bool {
    self.in_slow_wait.load(Ordering::Acquire)
  }

  /// KILL 语义位（首杀即真，重复杀假；C# networkSender.TryClose /
  /// libs/server/Resp/RespServerSession.cs:TryKill 的注册表侧投影——被杀会话体归其连接任务
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

  /// 等待终止（KILL/注销广播命中即返回；防竞态：先注册监听后复查——对齐
  /// ShutdownCoordinator::wait 防竞态模式）
  ///
  /// 在途执行体的统一取消源，对位 C# RespServerSession.Dispose 的注销撤销
  /// （`asyncWaiterCancel?.Cancel()` + `asyncWaiter?.Signal()`：会话注销关闭时立即撤销在途异步等待并放弃
  ///     应答写回）。rust 承接：KILL 哨兵令牌打断挂起读，网络泵阻塞/慢挂起
  ///     与本等待 select 竞速，终止胜出即丢弃执行体退出泵循环
  pub(crate) async fn wait_terminate(&self) {
    loop {
      let listener = self.kill_event.listen();
      if self.is_terminating() {
        return;
      }
      listener.await;
    }
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
  /// 字节以条目为累计主体清零即生效；会话指标句柄共享（与 reset_command_stats
  /// 同一锁句柄直复位模式），全字段原子、reset 为 &self 线程安全口，跨线程
  /// 直清零即生效——空闲连接不再滞留旧值回灌全局）。
  ///
  /// 延迟槽无同款复位臂：C# 的会话延迟复位臂（ResetAllLatencyMetrics）为
  /// 就地清本会话双缓冲槽，rust 属主线程在版本翻转点已把退役槽并入全局后
  /// 就地清零，复位结果等价，无需跨线程握手位
  pub fn reset_session_stats(&self) {
    self.net_input_bytes.store(0, Ordering::Relaxed);
    self.net_output_bytes.store(0, Ordering::Relaxed);
    self.commands_processed.store(0, Ordering::Relaxed);
    if let Some(handle) = self.session_metrics.read().clone() {
      handle.reset();
    }
  }

  /// 复位共享逐命令统计句柄（C# CleanupGlobalStats COMMANDSTATS 分支的
  /// `((RespServerSession)s).GetCommandStats?.Reset()`；句柄共享，直复位）
  pub fn reset_command_stats(&self) {
    if let Some(handle) = self.command_stats.read().clone() {
      handle.lock().reset();
    }
  }

  /// 挂接逐命令统计句柄（幂等；C# ActiveConsumers 直查 GetCommandStats 的
  /// 共享句柄承接，CommandStatsMonitor 关闭为空操作）
  pub fn attach_command_stats(&self, stats: Option<Arc<Mutex<CommandStats>>>) {
    if stats.is_some() {
      *self.command_stats.write() = stats;
    }
  }

  /// 挂接会话指标句柄（幂等；C# 监视器经 ActiveConsumers 直查
  /// GetSessionMetrics 的共享句柄承接，采样关闭为空操作）
  pub fn attach_session_metrics(&self, metrics: Option<Arc<SessionMetricsHandle>>) {
    if metrics.is_some() {
      *self.session_metrics.write() = metrics;
    }
  }

  /// 逐命令统计快照（监视器采样面；未挂接为 None）。
  ///
  /// 消费点：[`ConsumerRegistry::monitor_sample`] 逐会话采样克隆
  pub fn command_stats_snapshot(&self) -> Option<CommandStats> {
    let handle = self.command_stats.read().clone()?;
    Some(handle.lock().clone())
  }

  /// 会话指标全字段快照（监视器采样面；未挂接为 None）。
  ///
  /// 消费点：[`ConsumerRegistry::monitor_sample`] 逐会话采样
  pub fn session_metrics_snapshot(&self) -> Option<GarnetSessionMetrics> {
    Some(self.session_metrics.read().as_ref()?.snapshot())
  }

  /// 读取动态字段投影（值拷贝，不持锁跨调用）
  pub fn client_view(&self) -> ClientView {
    self.view.lock().clone()
  }

  /// 零分配快速读取客户端类型（用于置换清扫等高频遍历判定）
  #[inline]
  pub fn client_type(&self) -> ClientType {
    self.view.lock().client_type
  }

  /// 覆写动态字段投影（网络泵每批汇聚单点整体重导，及 LIST/KILL 入口对自身即时
  /// 刷新；真值单源在会话字段，本投影仅为其跨线程可读快照）
  pub fn update_view(&self, view: ClientView) {
    *self.view.lock() = view;
  }

  /// CLIENT LIST 行格式（注册表条目侧投影；行组装收敛于
  /// [`write_client_info_fields`] 单点，与 CLIENT INFO 共用——对标 C#
  /// LIST/INFO 共函数 WriteClientInfo；行尾不含换行）
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
/// 行尾不含换行。
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
  // rust 自有观测面（C# WriteClientInfo 无对位字段）：仅非零输出,
  // 零丢弃的普通会话行保持与 C# 字段序逐位一致（对标测试按全量
  // 相等断言锚定,此字段缺席即零丢弃的可读形态）
  if view.pubsub_dropped > 0 {
    let _ = write!(into, " pubsub-drop={}", view.pubsub_dropped);
  }
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
  /// 与 entries.len()（枚举在途量）同宿主单点，承接 accept 容量门；生产读取
  /// 面为 [`ConsumerRegistry::dispose_active_handlers`] 的排空判据（C#
  /// DisposeActiveHandlers 轮询 activeHandlerCount 归零的对偶）
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
  /// C# HandleNewConnection 的容量门子步骤（其本体承接方见 server.rs 的
  /// accept 循环），非独立 C# 函数，故不另挂锚点。
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

  /// 进程级安装（覆盖式；测试与单机形态均可安装）。CLIENT 族命令与 dispose 归并经
  /// [`ConsumerRegistry::global`] 直取
  ///
  /// 返回是否为首装（槽原空）：false 即覆盖了既有实例（单实例进程契约
  /// 被打破，装配点必须据返回值 warn 留痕禁静默）
  pub fn install_global(self: &Arc<Self>) -> bool {
    let mut g = GLOBAL_REGISTRY.write();
    let was_empty = g.is_none();
    *g = Some(Arc::clone(self));
    was_empty
  }

  /// 取进程级注册表（未安装为 None；对齐 C# `Server is GarnetServerBase` 判定）
  pub fn global() -> Option<Arc<Self>> {
    GLOBAL_REGISTRY.read().clone()
  }

  /// 复位进程级注册表（测试清理与多服置换用）
  pub fn reset_global() -> Option<Arc<Self>> {
    GLOBAL_REGISTRY.write().take()
  }

  /// 注册活跃消费者（accept 循环预注册调用；C# HandleNewConnection 的
  /// activeHandlers.TryAdd，GarnetServerTcp.cs:256——即刻注册，先于 TLS 握手
  /// 与首字节读取，握手期/不发字节的空闲连接即入 LIST/KILL 治理面）。
  /// received 计数不在本函数（已前移到 accept 成功分支，见
  /// [`ConsumerRegistry::note_connection_received`]）
  pub fn register(
    &self,
    id: i64,
    remote_endpoint: String,
    local_endpoint: String,
  ) -> Arc<ConsumerEntry> {
    self.register_with_type(id, remote_endpoint, local_endpoint, ConsumerType::Client)
  }

  /// 注册带分类的活跃消费者
  pub fn register_with_type(
    &self,
    id: i64,
    remote_endpoint: String,
    local_endpoint: String,
    consumer_type: ConsumerType,
  ) -> Arc<ConsumerEntry> {
    let entry = Arc::new(ConsumerEntry {
      id,
      remote_endpoint,
      local_endpoint,
      creation_ticks: now_ms_i64(),
      view: Mutex::new(ClientView::default()),
      net_input_bytes: AtomicU64::new(0),
      net_output_bytes: AtomicU64::new(0),
      commands_processed: AtomicU64::new(0),
      command_stats: RwLock::new(None),
      session_metrics: RwLock::new(None),
      kill_flag: AtomicBool::new(false),
      in_slow_wait: AtomicBool::new(false),
      removed: AtomicBool::new(false),
      kill_event: Event::new(),
      consumer_type: AtomicU8::new(consumer_type as u8),
    });
    self.entries.pin().insert(id as u64, Arc::clone(&entry));
    entry
  }

  /// 收到连接计数（C# IncrementConnectionsReceived，GarnetServerBase.cs:65；
  /// 其调用点 GarnetServerTcp.cs:288 在 TryAdd 后、handler.Start 前）。
  ///
  /// rust 计数点前移到 accept 成功分支（容量门前）：TLS 握手失败、容量门
  /// 拒绝、发字即断的短命连接全部进统计——超限臂由
  /// [`ConsumerRegistry::note_connection_disposed`] 配对，维持
  /// received - disposed = 活跃条目数 不变量。
  ///
  /// r14-conn.md:11「容量门拒绝除外」括注不采纳、维持现计数禁按 C# 回改，
  /// 裁决归属 deviations §101
  pub fn note_connection_received(&self) {
    self
      .total_connections_received
      .fetch_add(1, Ordering::Relaxed);
  }

  /// disposed 计数单源（C# IncrementConnectionsDisposed）：unregister 正常注销
  /// 与容量门拒绝臂（已计 received 而无注册条目收场）共用；C# 超限连接不计
  /// received 故无此配对，rust 计数点前移到容量门前后以本函数补配对
  pub fn note_connection_disposed(&self) {
    self
      .total_connections_disposed
      .fetch_add(1, Ordering::Relaxed);
  }

  /// 注销（网络泵释放时调用；C# DisposeMessageConsumer 的 TryRemove +
  /// IncrementConnectionsDisposed）。不存在为无害空操作
  pub fn unregister(&self, id: i64) {
    if let Some(entry) = self.entries.pin().remove(&(id as u64)) {
      self.note_connection_disposed();
      entry.removed.store(true, Ordering::Release);
      entry.kill_event.notify(usize::MAX);
    }
  }

  /// 按会话 id 查条目
  pub fn get(&self, id: i64) -> Option<Arc<ConsumerEntry>> {
    self.entries.pin().get(&(id as u64)).cloned()
  }

  /// libs/server/Servers/GarnetServerTcp.cs:ActiveConsumers
  /// libs/server/Servers/GarnetServerBase.cs:ActiveConsumers
  ///（基类抽象声明折叠：注册表即 rust 单一消费者宿主，无 Tcp/基类两层级）
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
  /// [`ConsumerRegistry::active_consumers`] 回访条目共享句柄/镜像；
  /// gossip 与复活化统计两臂的句柄不在本注册表域内，由宿主装配点
  ///（`server.rs` 的 `start_server_monitor`）以回调注入，全仓唯一构造位）
  ///
  /// 复位语义对齐 C#：STATS 复位回清连接计数 + 逐会话指标 + gossip 计数 +
  /// 复活化统计；COMMANDSTATS 复位回清逐会话命令统计句柄。延迟复位无会话
  /// 臂：见本文件头「会话延迟指标不入采样面」。
  pub fn monitor_iteration_inputs<G, R>(
    self: &Arc<Self>,
    reset_gossip_stats: G,
    reset_revivification_stats: R,
  ) -> MonitorIterationInputs<impl FnMut(), impl FnMut(), G, R>
  where
    G: FnMut(),
    R: FnMut(),
  {
    let conn_reset = Arc::clone(self);
    let cmdstats_reset = Arc::clone(self);
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
      // C# CleanupGlobalStats 体内的 clusterProvider?.ResetGossipStats() 与
      // storeWrapper.ResetRevivificationStats() 两臂：句柄由宿主装配点持有，
      // 本处原样承接（单机形态注入空操作闭包，对位 trait 默认实现）
      reset_gossip_stats,
      reset_revivification_stats,
    }
  }

  /// 监视器服务器快照（C# MainMonitorTaskAsync 经 ActiveConsumers 直查的
  /// 服务器域承接：连接计数 + 会话采样）。
  ///
  /// 会话指标镜像承接网络字节与命令计数；逐命令统计经共享句柄快照承接。
  /// 会话内部延迟指标不在快照内：其槽为属主线程独占、按引用直并全局，
  /// 放进快照就意味着每轮深拷贝整张直方图表
  pub fn monitor_sample(&self) -> ServerSample {
    let pin = self.entries.pin();
    let sessions: Vec<_> = pin
      .values()
      .map(|entry| {
        // 会话指标全字段读共享句柄快照（C# GarnetServerMonitor.cs:275 经
        // ActiveConsumers 直读 GetSessionMetrics 全量的承接）；网络字节与
        // 命令数三计数仍读条目镜像（条目即累计主体，INFO RESET STATS 经
        // reset_session_stats 清零条目即生效）
        let metrics = entry.session_metrics_snapshot().unwrap_or_default();
        wmetric::SessionSample {
          metrics: GarnetSessionMetrics {
            total_net_input_bytes: entry.net_input_bytes.load(Ordering::Relaxed),
            total_net_output_bytes: entry.net_output_bytes.load(Ordering::Relaxed),
            total_commands_processed: entry.commands_processed.load(Ordering::Acquire),
            ..metrics
          },
          command_stats: entry.command_stats_snapshot(),
        }
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
  /// C# 语义：以 `activeHandlerCount > 0` 为循环判据（覆盖 TryAdd 后、
  /// Session 建立前的握手期连接），对 activeHandlers 全量 handler.Dispose()
  /// 并轮询等待归零；rust 对偶为全量 [`ConsumerEntry::kill_session`]
  /// （kill 位 + 网络泵终止哨兵打断挂起读，含 TLS 握手期——握手 future 挂
  /// 同一取消令牌），连接泵经 unregister 归零计数。空闲长连接永不自关，
  /// 故必须先下杀令而非被动等。超时是 rust 侧防悬挂护栏（C# 生产语义无限
  /// 等），到期留痕滞留端点后强收——宿主线程 Runtime 析构兜底取消残余任务。
  ///
  /// 收敛判据双条件：entries 空且 [`Self::active_handler_count`] 归零——后者
  /// 是 C# 的正宗判据，前者补齐下杀下达面；预注册后二者同集，count 侧另
  /// 覆盖「容量门计量中与注册间隙」的在途连接，杜绝排空提前返回。
  ///
  /// 调用点必须位于 compio 运行时内（各 accept worker 线程 block_on 尾部、
  /// Runtime 析构前），本线程连接任务由本线程排空等待驱动跑完；多 worker
  /// 并发调用安全：kill 幂等、计数全局。
  pub async fn dispose_active_handlers(&self) {
    let begin = now_ms_i64();
    loop {
      let entries = self.active_consumers();
      if entries.is_empty() && self.active_handler_count.load(Ordering::Acquire) == 0 {
        return;
      }
      for entry in &entries {
        entry.kill_session();
      }
      if now_ms_i64().saturating_sub(begin) >= DRAIN_TIMEOUT_MS as i64 {
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

  /// 在途计数测试域直读（getter 已删，同文件私有字段直取）
  fn active_count(registry: &ConsumerRegistry) -> i64 {
    registry.active_handler_count.load(Ordering::Acquire)
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

  /// 终止等待防竞态臂：已终止条目直接返回（监听注册晚于广播也不挂死）
  #[test]
  fn wait_terminate_returns_when_already_terminated() {
    use compio::runtime::Runtime;

    let registry = ConsumerRegistry::new();
    let entry = registry.register(12, "127.0.0.1:7012".into(), String::new());
    let rt = Runtime::new().unwrap();
    entry.kill_session();
    rt.block_on(entry.wait_terminate());
    registry.unregister(12);
    rt.block_on(entry.wait_terminate());
  }

  /// 在途守卫容量门（C# GarnetServerTcp.cs:236-241/302-307 语义）：
  /// limit=2 时第三条拒绝，释放后可再进；Drop 配对归零
  #[test]
  fn connection_guard_enforces_limit() {
    let registry = Arc::new(ConsumerRegistry::new());
    let g1 = registry.try_acquire_connection(2).unwrap();
    let g2 = registry.try_acquire_connection(2).unwrap();
    assert_eq!(active_count(&registry), 2);
    // 超限拒绝：计数即刻回退，不留在途泄漏
    assert!(registry.try_acquire_connection(2).is_none());
    assert_eq!(active_count(&registry), 2);

    drop(g1);
    assert_eq!(active_count(&registry), 1);
    // 断言产生的临时守卫语句结束即释放，计数回到 1
    assert!(registry.try_acquire_connection(2).is_some());
    drop(g2);
    // 全部释放归零（无漂移），额度可复用
    assert_eq!(active_count(&registry), 0);
  }

  /// limit=-1 不限（与现状逐字节一致：恒放行）
  #[test]
  fn connection_guard_unlimited_when_minus_one() {
    let registry = Arc::new(ConsumerRegistry::new());
    let guards: Vec<_> = (0..64)
      .map(|_| registry.try_acquire_connection(-1).unwrap())
      .collect();
    assert_eq!(active_count(&registry), 64);
    drop(guards);
    assert_eq!(active_count(&registry), 0);
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
    assert_eq!(active_count(&registry), 0);
  }

  /// 停机排空：全量下杀令后等待注销归零（C# DisposeActiveHandlers 语义），
  /// 不依赖 5 秒超时护栏——模拟泵在收到 kill 广播后注销即快速返回
  #[test]
  fn dispose_active_handlers_drains_after_kill() {
    use compio::runtime::Runtime;

    let registry = Arc::new(ConsumerRegistry::new());
    registry.note_connection_received();
    let entry = registry.register(11, "127.0.0.1:7011".into(), String::new());
    let rt = Runtime::new().unwrap();
    let reg = Arc::clone(&registry);
    rt.block_on(async move {
      // 模拟连接泵（kill.rs 哨兵路径）：终止广播命中后走 dispose 注销
      let pump_entry = Arc::clone(&entry);
      let pump_reg = Arc::clone(&reg);
      spawn(async move {
        pump_entry.wait_terminate().await;
        pump_reg.unregister(pump_entry.id);
      })
      .detach();
      reg.dispose_active_handlers().await;
    });
    assert_eq!(registry.connection_totals(), (1, 1, 0));
    assert!(registry.get(11).is_none());
  }

  /// 排空判据双条件：entries 空但 active_handler_count 未归零（容量门计量
  /// 中/注册间隙的在途连接）不得提前返回，count 归零方收敛（C#
  /// DisposeActiveHandlers 以 activeHandlerCount 轮询为判据的对偶）
  #[test]
  fn dispose_waits_for_in_flight_count_drain() {
    use std::time::Duration;

    use compio::{runtime::Runtime, time::sleep};

    let registry = Arc::new(ConsumerRegistry::new());
    let rt = Runtime::new().unwrap();
    let reg = Arc::clone(&registry);
    rt.block_on(async move {
      // 在途守卫持有者（未注册条目）：短延后释放，模拟 accept 与 register
      // 间隙的在途连接收场
      let holder = Arc::clone(&reg);
      spawn(async move {
        sleep(Duration::from_millis(80)).await;
        drop(holder.try_acquire_connection(-1));
      })
      .detach();
      reg.dispose_active_handlers().await;
      // 返回即 count 已归零（若误以 entries 空为唯一判据则提前返回，
      // 此刻 count 仍为 1）
      assert_eq!(reg.active_handler_count.load(Ordering::Acquire), 0);
    });
  }
}
