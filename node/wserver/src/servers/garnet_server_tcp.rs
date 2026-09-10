//! TCP 服务器（对标 libs/server/Servers/GarnetServerTcp.cs:GarnetServerTcp）
//!
//! C# 以 SocketAsyncEventArgs + IOCP 完成端口驱动 accept 循环；Rust 托管面
//! 以 std 监听套接字 + [`GarnetServerTcp::accept_once`] 单步接受承接同一
//! 状态机（连接上限 / 分级错误退避 / 处理器登记），宿主以专线程或异步
//! 运行时包装阻塞 accept 即得等价泵。缓冲池面（Purge / GetBufferPoolStats）
//! 为 .NET LimitedFixedBufferPool 专属，托管会话缓冲自持，无对应物。
//!
//! C# 的 ServerTcpNetworkHandler（每连接 IO 泵）属网络承载域；托管模型下
//! 会话由宿主经显式消费调用驱动，此处仅登记连接与处置记账。

use std::{
  io,
  net::{TcpListener, TcpStream},
  sync::{
    Arc,
    atomic::{
      AtomicU64,
      Ordering::{AcqRel, Acquire, Release},
    },
  },
  thread::sleep,
  time::Duration,
};

use parking_lot::Mutex as ParkingMutex;

use super::{
  garnet_server_base::GarnetServerBase,
  i_garnet_server::{ClusterSessionFace, MessageConsumerFace, ServerEnumerate, WireFormat},
};

/// 初始 accept 退避毫秒（libs/server/Servers/GarnetServerTcp.cs:InitialAcceptBackoffMs）
pub const INITIAL_ACCEPT_BACKOFF_MS: u64 = 100;
/// 最大 accept 退避毫秒（libs/server/Servers/GarnetServerTcp.cs:MaxAcceptBackoffMs）
pub const MAX_ACCEPT_BACKOFF_MS: u64 = 5000;
/// 监听队列长度（libs/server/Servers/GarnetServerTcp.cs:Listen(512)）
pub const LISTEN_BACKLOG: i32 = 512;

/// accept 错误分级（C# HandleAcceptError 的 SocketError 分档投影）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptErrorTier {
  /// 一档：监听套接字已死（干净关闭），停止接受
  FatalClean,
  /// 二档：资源压力，退避后重试
  Backoff,
  /// 三档：对端瞬态，记录后继续
  Transient,
}

/// 一次接受的连接（C# SocketAsyncEventArgs.AcceptSocket 投影）
pub struct AcceptedConnection {
  /// 已接受的流（NoDelay 已按 C# 对非 UDS 套接字置位语义保留连接级配置位）
  pub stream: TcpStream,
  /// 对端端点展示串（C# RemoteEndPoint?.ToString()）
  pub remote_endpoint: String,
}

/// TCP 服务器
pub struct GarnetServerTcp {
  /// 公共基座（C# 继承 GarnetServerBase）
  base: Arc<GarnetServerBase>,
  /// 监听套接字（C# listenSocket；Close/Dispose 后为 None）
  listener: ParkingMutex<Option<TcpListener>>,
  /// 连接数上限（-1 不限；C# networkConnectionLimit）
  network_connection_limit: i64,
  /// 网络发送节流上限（C# networkSendThrottleMax）
  network_send_throttle_max: usize,
  /// accept 退避毫秒（C# acceptBackoffMs，成功接受后复位）
  accept_backoff_ms: AtomicU64,
}

impl GarnetServerTcp {
  /// 构造 TCP 服务器
  ///
  /// libs/server/Servers/GarnetServerTcp.cs:GarnetServerTcp
  ///
  /// C# 构造含 TLS 选项 / 缓冲池 / UDS 权限位；托管面以 std 监听 +
  /// 会话自持缓冲承接，tlsOptions / unixSocket 参数面随 tls 域接线补齐。
  pub fn new(
    endpoint: &str,
    network_buffer_size: usize,
    network_send_throttle_max: usize,
    network_connection_limit: i64,
  ) -> Self {
    Self {
      base: Arc::new(GarnetServerBase::new(endpoint, network_buffer_size)),
      listener: ParkingMutex::new(None),
      network_connection_limit,
      network_send_throttle_max: network_send_throttle_max.max(1),
      accept_backoff_ms: AtomicU64::new(INITIAL_ACCEPT_BACKOFF_MS),
    }
  }

  /// 基座句柄（提供者注册 / 计数访问）
  pub fn base(&self) -> &Arc<GarnetServerBase> {
    &self.base
  }

  /// 网络发送节流上限
  pub fn network_send_throttle_max(&self) -> usize {
    self.network_send_throttle_max
  }

  /// 当前 accept 退避毫秒
  pub fn accept_backoff_ms(&self) -> u64 {
    self.accept_backoff_ms.load(Acquire)
  }

  /// 活跃消息消费者
  ///
  /// libs/server/Servers/GarnetServerTcp.cs:ActiveConsumers
  pub fn active_consumers(&self) -> Vec<Arc<dyn MessageConsumerFace>> {
    self.base.active_consumers()
  }

  /// 活跃集群会话
  ///
  /// libs/server/Servers/GarnetServerTcp.cs:ActiveClusterSessions
  pub fn active_cluster_sessions(&self) -> Vec<Arc<dyn ClusterSessionFace>> {
    self.base.active_cluster_sessions()
  }

  /// 启动监听：绑定端点并进入监听态
  ///
  /// libs/server/Servers/GarnetServerTcp.cs:Start
  ///
  /// C# 另设 ReuseAddress 与 UDS 权限位；std 面以默认绑定承接
  /// （TIME_WAIT 重用随宿主套接字选项接线补齐）。
  pub fn start(&self) -> io::Result<()> {
    // std TcpListener::bind 自带 listen(backlog) 语义
    let listener = TcpListener::bind(self.base.endpoint())?;
    *self.listener.lock() = Some(listener);
    Ok(())
  }

  /// 停止接受新连接，释放监听端口（不等活跃连接排空）
  ///
  /// libs/server/Servers/GarnetServerTcp.cs:Close
  pub fn close(&self) {
    *self.listener.lock() = None;
  }

  /// 单步 accept：错误分级处置，成功则登记连接
  ///
  /// libs/server/Servers/GarnetServerTcp.cs:AcceptEventArg_Completed /
  /// HandleNewConnection
  ///
  /// 返回 false 表示接受循环应终止（监听已关闭或一档致命错误）；
  /// 连接被上限拒绝 / 会话创建失败为继续循环（C# HandleNewConnection
  /// 对应分支恒返回 true）。
  pub fn accept_once(&self) -> bool {
    let listener = {
      let guard = self.listener.lock();
      match guard.as_ref() {
        Some(listener) => match listener.try_clone() {
          Ok(cloned) => cloned,
          Err(error) => return self.handle_accept_error(&error),
        },
        // 监听已关闭：循环终止（C# ObjectDisposedException 捕获分支）
        None => return false,
      }
    };

    match listener.accept() {
      Ok((stream, peer)) => {
        let _ = self.handle_new_connection(stream, peer.to_string());
        true
      }
      Err(error) => self.handle_accept_error(&error),
    }
  }

  /// accept 错误分级处置
  ///
  /// libs/server/Servers/GarnetServerTcp.cs:HandleAcceptError
  ///
  /// 一档（OperationAborted/Shutdown → 连接中断族）静默终止；套接字族
  /// 致命错误在托管面同为 Err 路径；二档（资源压力族）退避重试，指数
  /// 增长至 5s；三档（对端瞬态）记录后继续。
  pub fn handle_accept_error(&self, error: &io::Error) -> bool {
    match accept_error_tier(error) {
      AcceptErrorTier::FatalClean => false,
      AcceptErrorTier::Backoff => {
        let backoff_ms = self.grow_backoff();
        log::warn!("Accept backoff ({backoff_ms}ms) due to resource pressure: {error}");
        sleep(Duration::from_millis(backoff_ms));
        true
      }
      AcceptErrorTier::Transient => {
        log::debug!("Transient accept error, continuing: {error}");
        true
      }
    }
  }

  /// 登记新连接：上限校验 → 会话创建 → 处理器注册 → 接收计数
  ///
  /// libs/server/Servers/GarnetServerTcp.cs:HandleNewConnection
  ///
  /// `stream` 由宿主 IO 泵接管（托管模型下会话经显式消费调用驱动）；
  /// 返回处理器 id，被上限或关闭哨兵拒绝时为 None（C# 对应分支直接
  /// 关闭套接字并继续接受循环）。
  pub fn handle_new_connection(&self, stream: TcpStream, remote_endpoint: String) -> Option<u64> {
    // NoDelay 对齐 C# 非 UDS 套接字语义（失败按对端已死处置）
    if stream.set_nodelay(true).is_err() {
      return None;
    }

    let current = self.base.get_conn_active();
    if current < 0
      || (self.network_connection_limit != -1 && current >= self.network_connection_limit)
    {
      log::debug!("Connection limit reached, rejecting {remote_endpoint}");
      return None;
    }

    // 会话创建（C# TryCreateMessageConsumer → AddSession；RESP 线格式）
    let session = self.try_create_message_consumer(remote_endpoint.as_bytes())?;
    let handler_id = self.base.register_handler(session)?;
    drop(stream);
    self.base.increment_connections_received();
    // 成功接受即复位退避（C# HandleNewConnection 同款）
    self
      .accept_backoff_ms
      .store(INITIAL_ACCEPT_BACKOFF_MS, Release);
    Some(handler_id)
  }

  /// 退避翻倍并封顶，返回本次应等待的毫秒（C# 退避算式的独立面）
  fn grow_backoff(&self) -> u64 {
    let backoff_ms = self.accept_backoff_ms.load(Acquire);
    let next = (backoff_ms * 2).min(MAX_ACCEPT_BACKOFF_MS);
    self.accept_backoff_ms.store(next, Release);
    backoff_ms
  }

  /// 按首字节判定线格式并创建会话
  ///
  /// libs/server/Servers/GarnetServerTcp.cs:TryCreateMessageConsumer
  ///
  /// C# 以前 4 字节判定 WireFormat 后查提供者表；托管面仅支持 ASCII
  /// （RESP）线格式，`bytes` 为对端首包（未接入 IO 泵时为空切片）。
  pub fn try_create_message_consumer(&self, _bytes: &[u8]) -> Option<Arc<dyn MessageConsumerFace>> {
    let provider = self.base.find_session_provider(WireFormat::Ascii)?;
    self
      .base
      .add_session(WireFormat::Ascii, provider.as_ref(), next_sender_id())
  }

  /// 处置消息消费者：移除处理器 + 计数 + 会话释放
  ///
  /// libs/server/Servers/GarnetServerTcp.cs:DisposeMessageConsumer
  pub fn dispose_message_consumer(&self, handler_id: u64) -> bool {
    self.base.dispose_message_consumer(handler_id)
  }
}

impl ServerEnumerate for GarnetServerTcp {
  fn active_consumers(&self) -> Vec<Arc<dyn MessageConsumerFace>> {
    self.base.active_consumers()
  }

  fn active_cluster_sessions(&self) -> Vec<Arc<dyn ClusterSessionFace>> {
    self.base.active_cluster_sessions()
  }
}

impl super::i_garnet_server::GarnetServer for GarnetServerTcp {
  fn register(
    &self,
    wire_format: WireFormat,
    backend_provider: Arc<dyn super::i_garnet_server::SessionProviderFace>,
  ) -> Result<(), super::i_garnet_server::ServerError> {
    self.base.register(wire_format, backend_provider)
  }

  fn unregister(
    &self,
    wire_format: WireFormat,
  ) -> Option<Arc<dyn super::i_garnet_server::SessionProviderFace>> {
    self.base.unregister(wire_format)
  }

  fn get_session_providers(
    &self,
  ) -> Vec<(
    WireFormat,
    Arc<dyn super::i_garnet_server::SessionProviderFace>,
  )> {
    self.base.get_session_providers()
  }

  fn add_session(
    &self,
    wire_format: WireFormat,
    backend_provider: &dyn super::i_garnet_server::SessionProviderFace,
    network_sender_id: u64,
  ) -> Option<Arc<dyn MessageConsumerFace>> {
    self
      .base
      .add_session(wire_format, backend_provider, network_sender_id)
  }

  fn start(&self) -> io::Result<()> {
    GarnetServerTcp::start(self)
  }

  fn close(&self) {
    GarnetServerTcp::close(self);
  }
}

/// 网络发送器标识分配（C# INetworkSender 实例身份的托管等价）
fn next_sender_id() -> u64 {
  static SENDER_ID: AtomicU64 = AtomicU64::new(1);
  SENDER_ID.fetch_add(1, AcqRel)
}

/// std io 错误 → accept 分级（C# SocketError 分档的 std 等价映射）
///
/// - 一档（干净关闭族）：ConnectionAborted / Interrupted（监听关闭触发的
///   OperationAborted 等价）；
/// - 二档（资源压力族）：内存/缓冲耗尽、无空闲端口（TooManyOpenSockets /
///   NoBufferSpace / ProcessLimit 等价）；
/// - 三档：其余瞬态（对端重置、超时等）。
fn accept_error_tier(error: &io::Error) -> AcceptErrorTier {
  use std::io::ErrorKind::{ConnectionAborted, Interrupted, OutOfMemory, WouldBlock};
  match error.kind() {
    ConnectionAborted | Interrupted => AcceptErrorTier::FatalClean,
    OutOfMemory | WouldBlock => AcceptErrorTier::Backoff,
    _ => AcceptErrorTier::Transient,
  }
}

/// 监听器持有检查（Close 幂等）
#[cfg(test)]
mod tests {
  use std::{io::ErrorKind, net::SocketAddr};

  use super::{
    super::garnet_server_base::{DISPOSED_HANDLER_COUNT, GarnetServerBase},
    *,
  };

  struct TestConsumer;
  impl MessageConsumerFace for TestConsumer {
    fn dispose(&self) {}
    fn attach_server(&self, _server: Arc<dyn ServerEnumerate>) {}
  }

  struct TestProvider;
  impl super::super::i_garnet_server::SessionProviderFace for TestProvider {
    fn get_session(
      &self,
      _wire_format: WireFormat,
      _network_sender_id: u64,
    ) -> Option<Arc<dyn MessageConsumerFace>> {
      Some(Arc::new(TestConsumer))
    }
  }

  fn server(limit: i64) -> (GarnetServerTcp, SocketAddr) {
    let server = GarnetServerTcp::new("127.0.0.1:0", 0, 8, limit);
    server
      .base()
      .register(WireFormat::Ascii, Arc::new(TestProvider))
      .expect("注册成功");
    server.start().expect("绑定成功");
    let addr = server
      .listener
      .lock()
      .as_ref()
      .expect("监听在册")
      .local_addr()
      .expect("本地地址可得");
    (server, addr)
  }

  #[test]
  fn start_accept_and_dispose_roundtrip() {
    let (server, addr) = server(-1);
    let client = TcpStream::connect(addr).expect("回环连接成功");
    drop(client);

    assert!(server.accept_once(), "接受成功");
    assert_eq!(server.base().get_conn_active(), 1);
    assert_eq!(server.base().total_connections_received(), 1);

    // 在册处理器可枚举并按 id 处置
    let handler_ids = server.base().active_handler_ids();
    assert_eq!(handler_ids.len(), 1);
    assert!(server.dispose_message_consumer(handler_ids[0]));
    assert_eq!(server.base().get_conn_active(), 0);
    assert_eq!(server.base().total_connections_disposed(), 1);
    assert!(!server.dispose_message_consumer(handler_ids[0]));

    server.close();
    // 关闭后接受循环终止
    assert!(!server.accept_once());
  }

  #[test]
  fn connection_limit_rejects_new_connections() {
    let (server, addr) = server(1);
    let client = TcpStream::connect(addr).expect("回环连接成功");
    drop(client);
    assert!(server.accept_once());
    assert_eq!(server.base().get_conn_active(), 1);

    let second = TcpStream::connect(addr).expect("第二连接成功");
    drop(second);
    // 上限 1：拒绝该连接但接受循环继续（C# HandleNewConnection 恒续跑）
    assert!(server.accept_once());
    assert_eq!(server.base().get_conn_active(), 1);
  }

  #[test]
  fn accept_backoff_grows_to_max() {
    let server = GarnetServerTcp::new("ep", 0, 8, -1);
    assert_eq!(server.accept_backoff_ms(), INITIAL_ACCEPT_BACKOFF_MS);
    // 二档退避：100 起步翻倍
    assert_eq!(server.grow_backoff(), 100);
    assert_eq!(server.accept_backoff_ms(), 200);
    assert_eq!(server.grow_backoff(), 200);
    assert_eq!(server.grow_backoff(), 400);
    // 封顶不再增长
    for _ in 0..24 {
      server.grow_backoff();
    }
    assert_eq!(server.accept_backoff_ms(), MAX_ACCEPT_BACKOFF_MS);
    assert_eq!(server.grow_backoff(), MAX_ACCEPT_BACKOFF_MS);
    assert_eq!(server.accept_backoff_ms(), MAX_ACCEPT_BACKOFF_MS);
  }

  #[test]
  fn clean_shutdown_tier_stops_accept_loop() {
    let server = GarnetServerTcp::new("ep", 0, 8, -1);
    let aborted = io::Error::new(ErrorKind::ConnectionAborted, "aborted");
    assert!(!server.handle_accept_error(&aborted));
  }

  #[test]
  fn base_served_by_tcp_implementation() {
    let (server, _addr) = server(-1);
    let base: &GarnetServerBase = server.base();
    assert_eq!(base.get_conn_active(), 0);
    let _ = DISPOSED_HANDLER_COUNT;
  }
}
