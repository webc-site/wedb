//! 服务器基座（对标 libs/server/Servers/GarnetServerBase.cs:GarnetServerBase）
//!
//! C# 基座以 `ConcurrentDictionary<INetworkHandler, byte>` 承载活跃连接、
//! 以 int 计数兼作关闭哨兵（int.MinValue 单次关闭屏障）；Rust 侧以读写锁
//! 分域的处理器表 + 原子计数承接同一生命周期面：连接收发计数、提供者
//! 注册表、会话创建回填、Dispose 排空。
//!
//! C# 的 ActiveConsumers / ActiveClusterSessions 为抽象（唯一实现
//! GarnetServerTcp 读 handler.Session）；托管模型处理器表即会话表，
//! 因此上收到基座直接实现。

use std::{
  sync::{
    Arc,
    atomic::{
      AtomicBool, AtomicI32, AtomicI64, AtomicU64,
      Ordering::{AcqRel, Acquire, Release},
    },
  },
  thread::yield_now,
};

use gxhash::{HashMap as GxHashMap, HashSet as GxHashSet};
use parking_lot::RwLock;

use super::i_garnet_server::{
  ClusterSessionFace, MessageConsumerFace, ServerEnumerate, ServerError, SessionProviderFace,
  WireFormat,
};

/// 网络缓冲区默认大小（C# BufferSizeUtils.ClientBufferSize(new MaxSizeSettings())
/// 的常量投影：64KB）
pub const DEFAULT_NETWORK_BUFFER_SIZE: usize = 1 << 16;

/// 关闭哨兵（C# activeHandlerCount 被置换为 int.MinValue 后拒绝新连接）
pub const DISPOSED_HANDLER_COUNT: i32 = i32::MIN;

/// Garnet 服务器公共基座
pub struct GarnetServerBase {
  /// 活跃网络处理器：处理器 id -> 会话消费者（C# activeHandlers）
  active_handlers: RwLock<GxHashMap<u64, Arc<dyn MessageConsumerFace>>>,
  /// 处理器 id 分配游标
  next_handler_id: AtomicU64,
  /// 活跃处理器计数（C# activeHandlerCount；Dispose 后置关闭哨兵）
  active_handler_count: AtomicI32,
  /// 会话提供者注册表（C# sessionProviders）
  session_providers: RwLock<GxHashMap<WireFormat, Arc<dyn SessionProviderFace>>>,
  /// 网络缓冲区大小（C# networkBufferSize）
  network_buffer_size: usize,
  /// 监听端点（C# EndPoint 的展示串投影）
  endpoint: String,
  /// 是否已释放（C# Disposed）
  disposed: AtomicBool,
  /// 累计接收连接数（C# totalConnectionsReceived）
  total_connections_received: AtomicI64,
  /// 累计处置连接数（C# totalConnectionsDisposed）
  total_connections_disposed: AtomicI64,
}

impl GarnetServerBase {
  /// 构造基座（C# 构造：endpoint + networkBufferSize，0 取默认缓冲）
  ///
  /// libs/server/Servers/GarnetServerBase.cs:GarnetServerBase
  pub fn new(endpoint: &str, network_buffer_size: usize) -> Self {
    Self {
      active_handlers: RwLock::new(GxHashMap::default()),
      next_handler_id: AtomicU64::new(0),
      active_handler_count: AtomicI32::new(0),
      session_providers: RwLock::new(GxHashMap::default()),
      network_buffer_size: if network_buffer_size == 0 {
        DEFAULT_NETWORK_BUFFER_SIZE
      } else {
        network_buffer_size
      },
      endpoint: endpoint.to_string(),
      disposed: AtomicBool::new(false),
      total_connections_received: AtomicI64::new(0),
      total_connections_disposed: AtomicI64::new(0),
    }
  }

  /// 监听端点
  ///
  /// libs/server/Servers/GarnetServerBase.cs:EndPoint
  pub fn endpoint(&self) -> &str {
    &self.endpoint
  }

  /// 网络缓冲区大小
  ///
  /// libs/server/Servers/GarnetServerBase.cs:NetworkBufferSize
  pub fn network_buffer_size(&self) -> usize {
    self.network_buffer_size
  }

  /// 是否已释放
  ///
  /// libs/server/Servers/GarnetServerBase.cs:Disposed
  pub fn disposed(&self) -> bool {
    self.disposed.load(Acquire)
  }

  /// 接收连接计数 +1
  ///
  /// libs/server/Servers/GarnetServerBase.cs:IncrementConnectionsReceived
  #[inline]
  pub fn increment_connections_received(&self) {
    self.total_connections_received.fetch_add(1, AcqRel);
  }

  /// 处置连接计数 +1
  ///
  /// libs/server/Servers/GarnetServerBase.cs:IncrementConnectionsDisposed
  #[inline]
  pub fn increment_connections_disposed(&self) {
    self.total_connections_disposed.fetch_add(1, AcqRel);
  }

  /// 累计接收连接数
  ///
  /// libs/server/Servers/GarnetServerBase.cs:TotalConnectionsReceived
  pub fn total_connections_received(&self) -> i64 {
    self.total_connections_received.load(Acquire)
  }

  /// 累计处置连接数
  ///
  /// libs/server/Servers/GarnetServerBase.cs:TotalConnectionsDisposed
  pub fn total_connections_disposed(&self) -> i64 {
    self.total_connections_disposed.load(Acquire)
  }

  /// 当前活跃连接数
  ///
  /// libs/server/Servers/GarnetServerBase.cs:get_conn_active
  pub fn get_conn_active(&self) -> i64 {
    self.active_handlers.read().len() as i64
  }

  /// 复位接收连接计数为当前活跃数
  ///（C# 注释：pub/sub 乘数记账；libs/server/Servers/GarnetServerBase.cs:ResetConnectionsReceived）
  pub fn reset_connections_received(&self) {
    let active = self.active_handlers.read().len() as i64;
    self.total_connections_received.store(active, Release);
  }

  /// 复位处置连接计数为 0
  ///
  /// libs/server/Servers/GarnetServerBase.cs:ResetConnectionsDiposed
  ///（C# 属性名拼写笔误 Dispose→Diposed，映射保留）
  pub fn reset_connections_disposed(&self) {
    self.total_connections_disposed.store(0, Release);
  }

  /// 登记新处理器并分配 id（C# activeHandlers.TryAdd + 计数自增）
  ///
  /// 返回处理器 id；已释放（关闭哨兵生效）时拒绝并返回 None。
  pub fn register_handler(&self, consumer: Arc<dyn MessageConsumerFace>) -> Option<u64> {
    if self.disposed.load(Acquire) {
      return None;
    }
    let count = self.active_handler_count.fetch_add(1, AcqRel);
    if count < 0 {
      // 关闭排空已启动：回退计数并拒绝
      self.active_handler_count.fetch_sub(1, AcqRel);
      return None;
    }
    let handler_id = self.next_handler_id.fetch_add(1, AcqRel);
    self.active_handlers.write().insert(handler_id, consumer);
    Some(handler_id)
  }

  /// 移除处理器（C# activeHandlers.TryRemove；处置记账由调用方完成）
  pub fn remove_handler(&self, handler_id: u64) -> bool {
    self.active_handlers.write().remove(&handler_id).is_some()
  }

  /// 在册处理器 id 快照（Dispose 排空 / 运维枚举用）
  pub fn active_handler_ids(&self) -> Vec<u64> {
    self.active_handlers.read().keys().copied().collect()
  }

  /// 注册线格式提供者（重复注册报错）
  ///
  /// libs/server/Servers/GarnetServerBase.cs:Register
  pub fn register(
    &self,
    wire_format: WireFormat,
    backend_provider: Arc<dyn SessionProviderFace>,
  ) -> Result<(), ServerError> {
    self
      .session_providers
      .write()
      .insert(wire_format, backend_provider)
      .map_or(Ok(()), |_| {
        Err(ServerError::WireFormatAlreadyRegistered(wire_format))
      })
  }

  /// 注销线格式提供者
  ///
  /// libs/server/Servers/GarnetServerBase.cs:Unregister
  pub fn unregister(&self, wire_format: WireFormat) -> Option<Arc<dyn SessionProviderFace>> {
    self.session_providers.write().remove(&wire_format)
  }

  /// 提供者表快照
  ///
  /// libs/server/Servers/GarnetServerBase.cs:GetSessionProviders
  pub fn get_session_providers(&self) -> Vec<(WireFormat, Arc<dyn SessionProviderFace>)> {
    self
      .session_providers
      .read()
      .iter()
      .map(|(k, v)| (*k, v.clone()))
      .collect()
  }

  /// 查找线格式提供者（连接到来时的会话类型判定）
  pub fn find_session_provider(
    &self,
    wire_format: WireFormat,
  ) -> Option<Arc<dyn SessionProviderFace>> {
    self.session_providers.read().get(&wire_format).cloned()
  }

  /// 创建会话并回填服务器引用
  ///
  /// libs/server/Servers/GarnetServerBase.cs:AddSession
  ///
  /// C# 对 RespServerSession 回填 respSession.Server = this 以支持会话
  /// 枚举；托管面经 [`MessageConsumerFace::attach_server`] 统一回填。
  pub fn add_session(
    self: &Arc<Self>,
    wire_format: WireFormat,
    backend_provider: &dyn SessionProviderFace,
    network_sender_id: u64,
  ) -> Option<Arc<dyn MessageConsumerFace>> {
    let session = backend_provider.get_session(wire_format, network_sender_id)?;
    session.attach_server(self.clone());
    Some(session)
  }

  /// 排空活跃处理器（C# DisposeActiveHandlers）
  ///
  /// 循环处置全部活跃处理器直至计数归零，随后以关闭哨兵（int.MinValue）
  /// 完成单次关闭屏障；此后 [`Self::register_handler`] 拒绝新连接。
  pub fn dispose_active_handlers(&self) {
    log::trace!("Begin disposing active handlers");
    loop {
      let handler_ids: GxHashSet<u64> = {
        let handlers = self.active_handlers.read();
        handlers.keys().copied().collect()
      };
      if handler_ids.is_empty() {
        break;
      }
      for handler_id in handler_ids {
        if let Some(consumer) = self.active_handlers.write().remove(&handler_id) {
          consumer.dispose();
          self.active_handler_count.fetch_sub(1, AcqRel);
          self.increment_connections_disposed();
        }
      }
      // C# Thread.Yield 等待异步处置收敛的等价让位点
      yield_now();
    }
    let _ = self
      .active_handler_count
      .compare_exchange(0, DISPOSED_HANDLER_COUNT, AcqRel, Acquire);
    log::trace!("End disposing active handlers");
  }

  /// 释放服务器：置位、排空处理器、清空提供者表
  ///
  /// libs/server/Servers/GarnetServerBase.cs:Dispose
  pub fn dispose(&self) {
    self.disposed.store(true, Release);
    self.dispose_active_handlers();
    self.session_providers.write().clear();
  }

  /// 处置单个活跃处理器（连接关闭路径：移除 + 计数 + 处置记账）
  ///
  /// 返回处理器是否在册。C# GarnetServerTcp.DisposeMessageConsumer 的
  /// 记账内核（增量与消费者释放），TCP 层仅补连接面细节。
  pub fn dispose_message_consumer(&self, handler_id: u64) -> bool {
    let consumer = self.active_handlers.write().remove(&handler_id);
    if let Some(consumer) = consumer {
      self.active_handler_count.fetch_sub(1, AcqRel);
      self.increment_connections_disposed();
      consumer.dispose();
      true
    } else {
      false
    }
  }
}

impl ServerEnumerate for GarnetServerBase {
  /// 全部活跃消息消费者
  ///
  /// libs/server/Servers/GarnetServerBase.cs:ActiveConsumers
  fn active_consumers(&self) -> Vec<Arc<dyn MessageConsumerFace>> {
    self.active_handlers.read().values().cloned().collect()
  }

  /// 全部活跃集群会话
  ///
  /// libs/server/Servers/GarnetServerBase.cs:ActiveClusterSessions
  fn active_cluster_sessions(&self) -> Vec<Arc<dyn ClusterSessionFace>> {
    self
      .active_handlers
      .read()
      .values()
      .filter_map(|consumer| consumer.cluster_session())
      .collect()
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Mutex;

  use super::*;

  /// 测试消费者：dispose 计数
  struct TestConsumer {
    disposed: Mutex<usize>,
    cluster: Mutex<Option<Arc<dyn ClusterSessionFace>>>,
  }

  impl TestConsumer {
    fn new() -> Arc<Self> {
      Arc::new(Self {
        disposed: Mutex::new(0),
        cluster: Mutex::new(None),
      })
    }
  }

  impl MessageConsumerFace for TestConsumer {
    fn dispose(&self) {
      *self.disposed.lock().expect("无锁中毒") += 1;
    }
    fn cluster_session(&self) -> Option<Arc<dyn ClusterSessionFace>> {
      self.cluster.lock().expect("无锁中毒").clone()
    }
    fn attach_server(&self, _server: Arc<dyn ServerEnumerate>) {}
  }

  struct TestClusterSession;
  impl ClusterSessionFace for TestClusterSession {
    fn session_id(&self) -> i64 {
      7
    }
  }

  #[test]
  fn connection_counters_track_receive_and_dispose() {
    let base = GarnetServerBase::new("127.0.0.1:6379", 0);
    assert_eq!(base.network_buffer_size(), DEFAULT_NETWORK_BUFFER_SIZE);
    assert_eq!(base.endpoint(), "127.0.0.1:6379");

    let consumer = TestConsumer::new();
    let handler_id = base.register_handler(consumer).expect("未释放可注册");
    base.increment_connections_received();

    assert_eq!(base.get_conn_active(), 1);
    assert_eq!(base.total_connections_received(), 1);

    base.reset_connections_received();
    assert_eq!(base.total_connections_received(), 1); // 活跃数 1

    assert!(base.dispose_message_consumer(handler_id));
    assert_eq!(base.total_connections_disposed(), 1);
    assert_eq!(base.get_conn_active(), 0);
    base.reset_connections_disposed();
    assert_eq!(base.total_connections_disposed(), 0);
  }

  #[test]
  fn register_rejects_duplicate_wire_format() {
    let base = GarnetServerBase::new("ep", 4096);
    struct Provider;
    impl SessionProviderFace for Provider {
      fn get_session(
        &self,
        _wire_format: WireFormat,
        _network_sender_id: u64,
      ) -> Option<Arc<dyn MessageConsumerFace>> {
        None
      }
    }
    base
      .register(WireFormat::Ascii, Arc::new(Provider))
      .expect("首次注册成功");
    assert!(matches!(
      base.register(WireFormat::Ascii, Arc::new(Provider)),
      Err(ServerError::WireFormatAlreadyRegistered(WireFormat::Ascii))
    ));
    assert_eq!(base.get_session_providers().len(), 1);
    assert!(base.unregister(WireFormat::Ascii).is_some());
    assert!(base.unregister(WireFormat::Ascii).is_none());
  }

  #[test]
  fn add_session_attaches_server_backref() {
    let base = Arc::new(GarnetServerBase::new("ep", 0));
    struct Provider;
    impl SessionProviderFace for Provider {
      fn get_session(
        &self,
        _wire_format: WireFormat,
        _network_sender_id: u64,
      ) -> Option<Arc<dyn MessageConsumerFace>> {
        Some(TestConsumer::new())
      }
    }
    let session = base
      .add_session(WireFormat::Ascii, &Provider, 42)
      .expect("会话可创建");
    // C# AddSession 只回填引用不入活跃表；登记后基座可枚举该会话
    assert_eq!(base.active_consumers().len(), 0);
    let _ = base.register_handler(session.clone()).expect("登记成功");
    assert_eq!(base.active_consumers().len(), 1);
    drop(session);
  }

  #[test]
  fn active_cluster_sessions_enumerates_consumers() {
    let base = GarnetServerBase::new("ep", 0);
    let consumer = TestConsumer::new();
    *consumer.cluster.lock().expect("无锁中毒") = Some(Arc::new(TestClusterSession));
    let _ = base.register_handler(consumer);
    let sessions = base.active_cluster_sessions();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].session_id(), 7);
  }

  #[test]
  fn dispose_drains_handlers_and_blocks_new_ones() {
    let base = GarnetServerBase::new("ep", 0);
    let consumer = TestConsumer::new();
    let _ = base.register_handler(consumer.clone());
    assert_eq!(base.get_conn_active(), 1);

    base.dispose();
    assert!(base.disposed());
    assert_eq!(*consumer.disposed.lock().expect("无锁中毒"), 1);
    assert_eq!(base.get_conn_active(), 0);

    // 关闭哨兵生效：新连接被拒绝
    assert!(base.register_handler(TestConsumer::new()).is_none());
  }
}
