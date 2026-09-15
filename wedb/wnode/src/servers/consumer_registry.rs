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
};

use event_listener::{Event, EventListener};
use gxhash::HashMap;
use parking_lot::{Mutex, RwLock};
use wbase::time::now_ms;
use wmetric::{GarnetSessionMetrics, ServerSample};

use crate::{session_parse_state_extensions::ClientType, traits::ServerEnumerate};

/// 进程级注册表槽（C# RespServerSession.Server 反查服务器的进程级承接；
/// 单服务器语义，CLIENT 族命令经 [`ConsumerRegistry::global`] 直取）
static GLOBAL_REGISTRY: OnceLock<Arc<ConsumerRegistry>> = OnceLock::new();

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
  pub db: i32,
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

  /// 读取动态字段镜像（值拷贝，不持锁跨调用）
  pub fn client_view(&self) -> ClientView {
    self.view.lock().clone()
  }

  /// 覆写动态字段镜像（CLIENT 族命令执行时自刷新）
  pub fn update_view(&self, view: ClientView) {
    *self.view.lock() = view;
  }

  /// CLIENT INFO 行格式（注册表条目侧投影；C# 会话侧锚点为
  /// write_client_info_state，字段序逐项一致：
  /// id addr [name] age [user] flags db resp lib-name lib-ver——name/user
  /// 非空才输出，lib-* 空值输出空串，行尾不含换行）
  pub fn write_client_info(&self, into: &mut String, now_milliseconds: i64) {
    let view = self.view.lock();
    let age_sec = (now_milliseconds - self.creation_ticks).max(0) / 1_000;
    let _ = write!(
      into,
      "id={} addr={} laddr={}",
      self.id, self.remote_endpoint, self.local_endpoint
    );
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
}

/// 活跃消费者注册表（单服务器语义；C# GarnetServerBase.activeHandlers）
pub struct ConsumerRegistry {
  /// 活跃条目（键 = 会话 id）。注册/注销为连接生命周期事件（低频），
  /// 枚举为 LIST/KILL/采样（读取）——读写锁 + gxhash 足量
  entries: RwLock<HashMap<u64, Arc<ConsumerEntry>>>,
  /// 收到的连接总数（C# totalConnectionsReceived）
  total_connections_received: AtomicI64,
  /// 已释放的连接总数（C# totalConnectionsDisposed）
  total_connections_disposed: AtomicI64,
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
      entries: RwLock::new(HashMap::with_capacity_and_hasher(16, Default::default())),
      total_connections_received: AtomicI64::new(0),
      total_connections_disposed: AtomicI64::new(0),
    }
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
      kill_flag: AtomicBool::new(false),
      removed: AtomicBool::new(false),
      kill_event: Event::new(),
    });
    self.entries.write().insert(id as u64, Arc::clone(&entry));
    self
      .total_connections_received
      .fetch_add(1, Ordering::Relaxed);
    entry
  }

  /// 注销（网络泵释放时调用；C# DisposeMessageConsumer 的 TryRemove +
  /// IncrementConnectionsDisposed）。不存在为无害空操作
  pub fn unregister(&self, id: i64) {
    if let Some(entry) = self.entries.write().remove(&(id as u64)) {
      self
        .total_connections_disposed
        .fetch_add(1, Ordering::Relaxed);
      entry.removed.store(true, Ordering::Release);
      entry.kill_event.notify(usize::MAX);
    }
  }

  /// 按会话 id 查条目
  pub fn get(&self, id: i64) -> Option<Arc<ConsumerEntry>> {
    self.entries.read().get(&(id as u64)).cloned()
  }

  /// libs/server/Servers/GarnetServerTcp.cs:ActiveConsumers
  ///
  /// 全部活跃消费者条目（任意顺序；C# ConcurrentDictionary 枚举语义）
  pub fn active_consumers(&self) -> Vec<Arc<ConsumerEntry>> {
    self.entries.read().values().cloned().collect()
  }

  /// 连接计数（received, disposed, active；C# TotalConnections* 与
  /// get_conn_active = activeHandlers.Count）
  pub fn connection_totals(&self) -> (i64, i64, i64) {
    (
      self.total_connections_received.load(Ordering::Relaxed),
      self.total_connections_disposed.load(Ordering::Relaxed),
      self.entries.read().len() as i64,
    )
  }

  /// INFO RESET STATS 的连接计数复位（C# ResetConnectionsReceived /
  /// ResetConnectionsDiposed：received 置当前活跃数，disposed 清零）
  pub fn reset_connection_totals(&self) {
    let active = self.entries.read().len() as i64;
    self.total_connections_disposed.store(0, Ordering::Relaxed);
    self
      .total_connections_received
      .store(active, Ordering::Relaxed);
  }

  /// 监视器服务器快照（C# MainMonitorTaskAsync 经 ActiveConsumers 直查的
  /// 服务器域承接：连接计数 + 会话采样）。
  ///
  /// 会话指标镜像仅承接网络字节（网络泵逐批写入）；命令计数等会话内部
  /// 计数随会话 dispose 经监视器历史归并（域界差异，见模块注释）
  pub fn monitor_sample(&self) -> ServerSample {
    let entries = self.active_consumers();
    let sessions: Vec<_> = entries
      .iter()
      .map(|entry| wmetric::SessionSample {
        metrics: GarnetSessionMetrics {
          total_net_input_bytes: entry.net_input_bytes.load(Ordering::Relaxed),
          total_net_output_bytes: entry.net_output_bytes.load(Ordering::Relaxed),
          total_commands_processed: entry.commands_processed.load(Ordering::Acquire),
          ..GarnetSessionMetrics::default()
        },
        command_stats: None,
        latency: None,
      })
      .collect();
    let (received, disposed, active) = self.connection_totals();
    ServerSample {
      total_connections_received: received,
      total_connections_disposed: disposed,
      total_connections_active: active,
      sessions,
    }
  }
}

impl ServerEnumerate for ConsumerRegistry {
  fn active_consumers(&self) -> Vec<Arc<ConsumerEntry>> {
    ConsumerRegistry::active_consumers(self)
  }
}

#[cfg(test)]
mod tests {
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
}
