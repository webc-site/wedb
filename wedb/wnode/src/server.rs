//! 统一节点宿主服务器与生命周期编排
//!
//! 1:1 对标微软 Garnet GarnetServer 与 GarnetServerBase
//!
//! 核心能力：
//! 1. 统一多端点监听：支持 TCP（SO_REUSEPORT 端口复用）与 Unix Domain Socket（UdsGuard 自动治理）；
//! 2. 纯 compio 全异步运行时与多核 Thread-per-Core 驱动（一核心一 Runtime，消除核间锁竞争）；
//! 3. 统一网络缓冲池与慢客户端流控；
//! 4. 三阶段优雅停机与安全关停：
//!    - Phase 1: 广播取消令牌，秒级打断所有核心的 accept 阻塞；
//!    - Phase 2: 关闭监听套接字与 UDS 守卫，阻断新连接；
//!    - Phase 3: 排空在途请求与工作线程，释放缓冲池与会话资源。
//! 5. 统一服务端启动流水线（模板方法模式 ServerBootstrap）与服务器构建器（NodeServerBuilder）。

use std::{
  fs::create_dir_all,
  io,
  net::{Ipv4Addr, SocketAddr, SocketAddrV4},
  num::NonZeroUsize,
  path::Path,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc::sync_channel,
  },
  thread::{Builder as ThreadBuilder, JoinHandle, available_parallelism},
};

use compio::{
  net::TcpListener,
  runtime::{CancelToken, Cancelled, FutureExt, Runtime, spawn},
};
use crossfire::{AsyncRx, mpsc::Array};
use log::{debug, error, info};
use parking_lot::Mutex;

use crate::{
  args::{NodeArgs, ServerArgs},
  buffer_pool::LimitedFixedBufferPool,
  cluster_provider::{ClusterProvider, NoopClusterProvider},
  endpoint::ServerEndpoint,
  net::{ConnectionStream, handler::NetworkHandler, socket_opt::bind_reuseport, uds::UdsGuard},
  shutdown::ShutdownCoordinator,
  signal::wait_shutdown_signal,
  traits::SessionProviderFace,
};

/// 宿主服务器构建器
pub struct NodeServerBuilder<P = ()> {
  endpoints: Vec<String>,
  network_buffer_size: usize,
  network_send_throttle_max: usize,
  threads: Option<NonZeroUsize>,
  session_provider: Option<Arc<P>>,
}

impl Default for NodeServerBuilder<()> {
  fn default() -> Self {
    Self::new()
  }
}

impl NodeServerBuilder<()> {
  /// 创建默认构建器
  pub fn new() -> Self {
    Self {
      endpoints: Vec::new(),
      network_buffer_size: crate::DEFAULT_BUFFER_SIZE,
      network_send_throttle_max: 8,
      threads: None,
      session_provider: None,
    }
  }
}

impl<P> NodeServerBuilder<P> {
  /// 从节点参数初始化端点与线程配置
  pub fn from_node_args(mut self, args: &NodeArgs) -> Self {
    self.endpoints = args.endpoints();
    self.threads = args.threads.and_then(NonZeroUsize::new);
    self
  }

  /// 增加网络监听端点
  pub fn endpoint(mut self, ep: impl Into<String>) -> Self {
    self.endpoints.push(ep.into());
    self
  }

  /// 设置监听端点列表
  pub fn endpoints(mut self, eps: impl IntoIterator<Item = impl Into<String>>) -> Self {
    self.endpoints = eps.into_iter().map(Into::into).collect();
    self
  }

  /// 设置网络缓冲区池大小
  pub fn network_buffer_size(mut self, size: usize) -> Self {
    self.network_buffer_size = size;
    self
  }

  /// 设置慢客户端发送流控阈值
  pub fn network_send_throttle_max(mut self, max: usize) -> Self {
    self.network_send_throttle_max = max;
    self
  }

  /// 设置工作线程数
  pub fn threads(mut self, threads: Option<NonZeroUsize>) -> Self {
    self.threads = threads;
    self
  }

  /// 设置会话提供者
  pub fn session_provider<NewP: SessionProviderFace>(
    self,
    provider: Arc<NewP>,
  ) -> NodeServerBuilder<NewP> {
    NodeServerBuilder {
      endpoints: self.endpoints,
      network_buffer_size: self.network_buffer_size,
      network_send_throttle_max: self.network_send_throttle_max,
      threads: self.threads,
      session_provider: Some(provider),
    }
  }
}

impl<P: SessionProviderFace + 'static> NodeServerBuilder<P> {
  /// 构建宿主服务器实例
  pub fn build(self) -> io::Result<GarnetServer<P>> {
    let session_provider = self
      .session_provider
      .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "未指定 session_provider"))?;
    Ok(GarnetServer::new(
      &self.endpoints,
      self.network_buffer_size,
      self.network_send_throttle_max,
      session_provider,
    ))
  }
}

/// 统一服务端启动流水线（模板方法模式）
///
/// 核心模板步骤：
/// 1. 确保基础与 WAL 数据目录就绪；
/// 2. 启动集群提供者后台管理协程（单机模式下为 Noop 零开销内联消除）；
/// 3. 执行会话提供者组装回调；
/// 4. 基于配置与端点构造 GarnetServer；
/// 5. 驱动 compio 全异步运行时启动服务并阻塞监听系统停机信号；
/// 6. 捕获停机信号后执行双向优雅关机与集群状态持久化刷盘。
pub struct ServerBootstrap<A, C = NoopClusterProvider> {
  args: A,
  cluster_provider: C,
  network_buffer_size: usize,
  network_send_throttle_max: usize,
  banner: String,
}

impl<A: ServerArgs> ServerBootstrap<A, NoopClusterProvider> {
  /// 创建单机服务端启动引导器（默认装配 NoopClusterProvider）
  pub fn new(args: A) -> Self {
    Self {
      args,
      cluster_provider: NoopClusterProvider,
      network_buffer_size: crate::DEFAULT_BUFFER_SIZE,
      network_send_throttle_max: 8,
      banner: "WeDB 数据库服务".into(),
    }
  }
}

impl<A: ServerArgs, C: ClusterProvider> ServerBootstrap<A, C> {
  /// 注入自定义集群提供者（静态泛型消除分支开销）
  pub fn with_cluster_provider<NewC: ClusterProvider>(
    self,
    cluster_provider: NewC,
  ) -> ServerBootstrap<A, NewC> {
    ServerBootstrap {
      args: self.args,
      cluster_provider,
      network_buffer_size: self.network_buffer_size,
      network_send_throttle_max: self.network_send_throttle_max,
      banner: self.banner,
    }
  }

  /// 设置服务启动横幅/标识名称
  pub fn banner(mut self, banner: impl Into<String>) -> Self {
    self.banner = banner.into();
    self
  }

  /// 设置网络读写缓冲池单页大小
  pub fn network_buffer_size(mut self, size: usize) -> Self {
    self.network_buffer_size = size;
    self
  }

  /// 设置慢客户端节流最大在途阈值
  pub fn network_send_throttle_max(mut self, max: usize) -> Self {
    self.network_send_throttle_max = max;
    self
  }

  /// 获取启动参数引用
  #[inline]
  pub fn args(&self) -> &A {
    &self.args
  }

  /// 获取集群提供者引用
  #[inline]
  pub fn cluster_provider(&self) -> &C {
    &self.cluster_provider
  }

  /// 使用直接会话提供者运行服务流水线
  pub fn run_with_provider<P: SessionProviderFace + 'static>(
    self,
    session_provider: Arc<P>,
  ) -> crate::Result<()> {
    self.run(|_, _| Ok(session_provider))
  }

  /// 统一服务端启动流水线（模板方法模式）
  pub fn run<P, F>(self, session_provider_factory: F) -> crate::Result<()>
  where
    P: SessionProviderFace + 'static,
    F: FnOnce(&A, &C) -> crate::Result<Arc<P>>,
  {
    let node_args = self.args.node_args();

    // 1. 确保基础与 WAL 数据目录就绪
    if !node_args.dir.as_os_str().is_empty() {
      let _ = create_dir_all(&node_args.dir);
    }
    let wal_dir = node_args.wal_dir();
    if !wal_dir.as_os_str().is_empty() {
      let _ = create_dir_all(&wal_dir);
    }

    // 2. 启动集群提供者后台管理协程（单机 Noop 零开销内联消除）
    if self.cluster_provider.is_cluster_enabled() {
      self.cluster_provider.start();
    }

    // 3. 执行会话提供者组装回调
    let session_provider = session_provider_factory(&self.args, &self.cluster_provider)?;

    // 4. 基于配置与端点构造 GarnetServer
    let endpoints = node_args.endpoints();
    let server = GarnetServer::new(
      &endpoints,
      self.network_buffer_size,
      self.network_send_throttle_max,
      session_provider,
    );

    // 5. 驱动 compio 全异步运行时启动服务并阻塞监听系统停机信号
    let rt = Runtime::new()?;
    let threads = node_args.threads.and_then(NonZeroUsize::new);
    let banner = self.banner;
    let cluster_provider = self.cluster_provider;

    rt.block_on(async move {
      let run_res = server.run_until_shutdown(threads, &banner).await;
      // 6. 停机清理与集群配置持久化刷盘
      if cluster_provider.is_cluster_enabled() {
        cluster_provider.flush_config();
      }
      run_res
    })
  }
}

/// 统一节点宿主服务器
pub struct GarnetServer<P: SessionProviderFace> {
  /// 配置的端点列表
  endpoints: Vec<ServerEndpoint>,
  /// 实际绑定的本地 TCP 地址列表
  tcp_addrs: Mutex<Vec<SocketAddr>>,
  /// 网络缓冲池
  buffer_pool: Arc<LimitedFixedBufferPool>,
  /// 慢客户端最大在途发送数
  network_send_throttle_max: usize,
  /// 优雅停机协调器
  shutdown_coordinator: ShutdownCoordinator,
  /// 会话提供者
  session_provider: Arc<P>,
  /// 会话 ID 分配器
  session_id_counter: Arc<AtomicU64>,
  /// 工作线程句柄列表
  worker_threads: Mutex<Vec<JoinHandle<()>>>,
}

impl<P: SessionProviderFace + 'static> GarnetServer<P> {
  /// 创建宿主服务器实例
  pub fn new(
    endpoints: &[String],
    network_buffer_size: usize,
    network_send_throttle_max: usize,
    session_provider: Arc<P>,
  ) -> Self {
    const DEFAULT_FALLBACK_ENDPOINT: ServerEndpoint = ServerEndpoint::Tcp(SocketAddr::V4(
      SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 0),
    ));

    let parsed_endpoints: Vec<ServerEndpoint> = endpoints
      .iter()
      .map(|e| ServerEndpoint::parse(e).unwrap_or(DEFAULT_FALLBACK_ENDPOINT))
      .collect();

    let buffer_pool = LimitedFixedBufferPool::new(network_buffer_size, 0);

    Self {
      endpoints: parsed_endpoints,
      tcp_addrs: Mutex::new(Vec::new()),
      buffer_pool,
      network_send_throttle_max: network_send_throttle_max.max(1),
      shutdown_coordinator: ShutdownCoordinator::new(),
      session_provider,
      session_id_counter: Arc::new(AtomicU64::new(1)),
      worker_threads: Mutex::new(Vec::new()),
    }
  }

  /// 获取绑定的本地 TCP 地址
  pub fn local_addrs(&self) -> Vec<SocketAddr> {
    self.tcp_addrs.lock().clone()
  }

  /// 获取首个绑定的本地 TCP 地址
  pub fn local_addr(&self) -> io::Result<SocketAddr> {
    self
      .tcp_addrs
      .lock()
      .first()
      .copied()
      .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "Server not bound"))
  }

  /// 释放服务器（stop 的兼容别名）
  ///
  /// libs/host/GarnetServer.cs:Dispose
  #[inline]
  pub fn dispose(&self) {
    self.stop();
  }

  /// 获取网络缓冲池引用
  #[inline]
  pub fn buffer_pool(&self) -> &Arc<LimitedFixedBufferPool> {
    &self.buffer_pool
  }

  /// 获取停机协调器引用
  #[inline]
  pub fn shutdown_coordinator(&self) -> &ShutdownCoordinator {
    &self.shutdown_coordinator
  }

  /// 启动服务器（Thread-per-Core 多核 SO_REUSEPORT 并发监听）
  ///
  /// libs/host/GarnetServer.cs:Start
  /// libs/host/GarnetServer.cs:InitializeServer
  pub fn start(&self, worker_threads: Option<NonZeroUsize>) -> io::Result<()> {
    let nthreads = worker_threads
      .unwrap_or_else(|| available_parallelism().unwrap_or(const { NonZeroUsize::new(1).unwrap() }))
      .get();

    for endpoint in &self.endpoints {
      match endpoint {
        ServerEndpoint::Tcp(addr) => {
          self.start_tcp_workers(*addr, nthreads)?;
        }
        ServerEndpoint::Unix(_path) => {
          #[cfg(unix)]
          self.start_unix_worker(_path)?;
          #[cfg(not(unix))]
          return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Unix domain socket not supported",
          ));
        }
      }
    }

    Ok(())
  }

  /// 启动多核 TCP Worker 线程（基于 SO_REUSEPORT 端口复用）
  fn start_tcp_workers(&self, target_addr: SocketAddr, nthreads: usize) -> io::Result<()> {
    let (ready_tx, ready_rx) = sync_channel::<io::Result<SocketAddr>>(1);

    let mut handles = Vec::with_capacity(nthreads);

    // Worker 0: 负责首发绑定（特别针对动态端口 :0）并广播实际地址
    {
      let cancel_rx = self.shutdown_coordinator.new_cancel_channel();
      let stopped = self.shutdown_coordinator.stopped_handle();
      let id_gen = Arc::clone(&self.session_id_counter);
      let provider = Arc::clone(&self.session_provider);
      let pool = Arc::clone(&self.buffer_pool);
      let throttle_max = self.network_send_throttle_max;

      let handle = ThreadBuilder::new()
        .name("wnode-worker-0".into())
        .spawn(move || {
          let rt = match Runtime::new() {
            Ok(r) => r,
            Err(e) => {
              let _ = ready_tx.send(Err(io::Error::other(e.to_string())));
              return;
            }
          };

          rt.block_on(async move {
            let listener = match bind_reuseport(target_addr).await {
              Ok(l) => l,
              Err(e) => {
                let _ = ready_tx.send(Err(e));
                return;
              }
            };

            let actual_addr = match listener.local_addr() {
              Ok(a) => a,
              Err(e) => {
                let _ = ready_tx.send(Err(e));
                return;
              }
            };
            let _ = ready_tx.send(Ok(actual_addr));

            run_tcp_accept_loop(
              "Worker-0",
              listener,
              cancel_rx,
              stopped,
              id_gen,
              provider,
              pool,
              throttle_max,
            )
            .await;
          });
        })?;

      handles.push(handle);
    }

    // 等待 Worker 0 绑定成功
    let actual_addr = ready_rx
      .recv()
      .map_err(|_| io::Error::other("Worker-0 初始化失败"))??;

    self.tcp_addrs.lock().push(actual_addr);

    info!(
      "启动 TCP 多核网络监听: {} ({} 核心 SO_REUSEPORT)",
      actual_addr, nthreads
    );

    // 启动其余 Worker 核心 (1..nthreads)
    for core_id in 1..nthreads {
      let (worker_ready_tx, worker_ready_rx) = sync_channel::<io::Result<()>>(1);
      let cancel_rx = self.shutdown_coordinator.new_cancel_channel();
      let stopped = self.shutdown_coordinator.stopped_handle();
      let id_gen = Arc::clone(&self.session_id_counter);
      let provider = Arc::clone(&self.session_provider);
      let pool = Arc::clone(&self.buffer_pool);
      let throttle_max = self.network_send_throttle_max;

      let handle = ThreadBuilder::new()
        .name(format!("wnode-worker-{core_id}"))
        .spawn(move || {
          let rt = match Runtime::new() {
            Ok(r) => r,
            Err(e) => {
              let _ = worker_ready_tx.send(Err(io::Error::other(e.to_string())));
              return;
            }
          };

          rt.block_on(async move {
            let listener = match bind_reuseport(actual_addr).await {
              Ok(l) => l,
              Err(e) => {
                let _ = worker_ready_tx.send(Err(e));
                return;
              }
            };
            let _ = worker_ready_tx.send(Ok(()));

            let worker_name = format!("Worker-{core_id}");
            run_tcp_accept_loop(
              &worker_name,
              listener,
              cancel_rx,
              stopped,
              id_gen,
              provider,
              pool,
              throttle_max,
            )
            .await;
          });
        })?;

      worker_ready_rx
        .recv()
        .map_err(|_| io::Error::other("Worker 初始化失败"))??;
      handles.push(handle);
    }

    self.worker_threads.lock().extend(handles);
    Ok(())
  }

  /// 启动 Unix 域套接字监听 Worker
  #[cfg(unix)]
  fn start_unix_worker(&self, path: &Path) -> io::Result<()> {
    let (ready_tx, ready_rx) = sync_channel::<io::Result<()>>(1);
    let cancel_rx = self.shutdown_coordinator.new_cancel_channel();
    let stopped = self.shutdown_coordinator.stopped_handle();
    let id_gen = Arc::clone(&self.session_id_counter);
    let provider = Arc::clone(&self.session_provider);
    let pool = Arc::clone(&self.buffer_pool);
    let throttle_max = self.network_send_throttle_max;
    let path_buf = path.to_path_buf();

    let handle = ThreadBuilder::new()
      .name("wnode-uds".into())
      .spawn(move || {
        let rt = match Runtime::new() {
          Ok(r) => r,
          Err(e) => {
            let _ = ready_tx.send(Err(io::Error::other(e.to_string())));
            return;
          }
        };

        rt.block_on(async move {
          let (listener, _guard) = match UdsGuard::bind(&path_buf).await {
            Ok(res) => res,
            Err(e) => {
              let _ = ready_tx.send(Err(e));
              return;
            }
          };

          info!("启动 Unix 域套接字监听: {}", path_buf.display());
          let _ = ready_tx.send(Ok(()));

          let cancel_token = CancelToken::new();
          let watcher_cancel = cancel_token.clone();
          spawn(async move {
            let _ = cancel_rx.recv().await;
            watcher_cancel.cancel();
          })
          .detach();

          while !stopped.load(Ordering::Relaxed) {
            let accept_res = listener
              .accept()
              .with_cancel(cancel_token.clone())
              .fail_fast()
              .await;

            match accept_res {
              Ok(Ok((stream, _))) => {
                if stopped.load(Ordering::Relaxed) {
                  break;
                }
                let sender_id = id_gen.fetch_add(1, Ordering::Relaxed);
                let handler = NetworkHandler::new(
                  sender_id,
                  path_buf.display().to_string(),
                  Arc::clone(&pool),
                  throttle_max,
                );
                let provider_clone = Arc::clone(&provider);
                spawn(async move {
                  if let Err(err) = handler
                    .process_stream(ConnectionStream::Unix(stream), provider_clone, sender_id)
                    .await
                  {
                    log::debug!("UDS 连接处理结束: {err}");
                  }
                })
                .detach();
              }
              Ok(Err(e)) => {
                if stopped.load(Ordering::Relaxed) {
                  break;
                }
                error!("UDS 接收连接失败: {e}");
              }
              Err(Cancelled) => {
                debug!("UDS 接收循环收到停机取消信号，退出");
                break;
              }
            }
          }
        });
      })?;

    ready_rx
      .recv()
      .map_err(|_| io::Error::other("UDS Worker 初始化失败"))??;

    self.worker_threads.lock().push(handle);
    Ok(())
  }

  /// 优雅关停服务器
  ///
  /// libs/host/GarnetServer.cs:InternalDispose
  pub fn stop(&self) {
    self.shutdown_coordinator.stop();
    let mut handles = self.worker_threads.lock();
    for handle in handles.drain(..) {
      let _ = handle.join();
    }
    self.buffer_pool.purge();
  }

  /// 启动服务器并监听系统停机信号，实现全生命周期优雅退出闭环
  pub async fn run_until_shutdown(
    &self,
    threads: Option<NonZeroUsize>,
    server_label: &str,
  ) -> crate::Result<()> {
    self.start(threads)?;
    log::info!("{server_label} 网络监听就绪: {:?}", self.local_addrs());
    let sig = wait_shutdown_signal().await?;
    log::info!("捕获停机信号 ({sig}), 正在优雅关机 {server_label}...");
    self.stop();
    log::info!("{server_label} 安全退出完成");
    Ok(())
  }
}

impl<P: SessionProviderFace> Drop for GarnetServer<P> {
  fn drop(&mut self) {
    self.stop();
  }
}

/// 驱动节点服务全生命周期运行（初始化 compio 运行时、监听端点直至系统停机信号）
pub fn run_node<P: SessionProviderFace + 'static>(
  conf: &crate::Conf,
  session_provider: Arc<P>,
  banner: &str,
) -> crate::Result<()> {
  ServerBootstrap::new(conf.clone())
    .banner(banner)
    .run_with_provider(session_provider)
}

struct TcpAcceptContext<P: SessionProviderFace> {
  worker_name: Arc<str>,
  listener: TcpListener,
  cancel_rx: AsyncRx<Array<()>>,
  stopped: Arc<AtomicBool>,
  id_gen: Arc<AtomicU64>,
  provider: Arc<P>,
  pool: Arc<LimitedFixedBufferPool>,
  throttle_max: usize,
}

/// 驱动单个核心的 TCP 连接接入循环
async fn run_tcp_accept_loop<P: SessionProviderFace + 'static>(ctx: TcpAcceptContext<P>) {
  let cancel_token = CancelToken::new();
  let watcher_cancel = cancel_token.clone();
  let cancel_rx = ctx.cancel_rx;
  spawn(async move {
    let _ = cancel_rx.recv().await;
    watcher_cancel.cancel();
  })
  .detach();

  while !ctx.stopped.load(Ordering::Relaxed) {
    let accept_res = ctx
      .listener
      .accept()
      .with_cancel(cancel_token.clone())
      .fail_fast()
      .await;

    match accept_res {
      Ok(Ok((stream, client_addr))) => {
        if ctx.stopped.load(Ordering::Relaxed) {
          break;
        }
        let sender_id = ctx.id_gen.fetch_add(1, Ordering::Relaxed);
        let handler = NetworkHandler::<P::Consumer>::new(
          sender_id,
          client_addr.to_string(),
          Arc::clone(&ctx.pool),
          ctx.throttle_max,
        );
        let provider_clone = Arc::clone(&ctx.provider);
        let worker_name_clone = Arc::clone(&ctx.worker_name);
        spawn(async move {
          if let Err(err) = handler
            .process_stream(ConnectionStream::Tcp(stream), provider_clone, sender_id)
            .await
          {
            log::debug!("{worker_name_clone} 连接处理结束: {err}");
          }
        })
        .detach();
      }
      Ok(Err(e)) => {
        if ctx.stopped.load(Ordering::Relaxed) {
          break;
        }
        error!("{} 接收连接失败: {e}", ctx.worker_name);
      }
      Err(Cancelled) => {
        debug!("{} 收到停机取消信号，退出接入循环", ctx.worker_name);
        break;
      }
    }
  }
}
