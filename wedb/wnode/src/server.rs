//! 统一节点宿主服务器与生命周期编排
//!
//! 1:1 对标微软 Garnet GarnetServer 与 GarnetServerBase
//!
//! 核心能力：
//! 1. 统一多端点监听：支持 TCP（SO_REUSEPORT 端口复用）与 Unix Domain Socket（UdsGuard 自动治理）；
//! 2. 纯 compio 全异步运行时与多核 Thread-per-Core 驱动（一核心一 Runtime，消除核间锁竞争）；
//! 3. 统一网络缓冲池与慢客户端流控；
//! 4. 三阶段优雅停机与安全关停（对标 C# GarnetServer.InternalDispose）：
//!    - Phase 1: 广播取消令牌，秒级打断所有核心的 accept 阻塞，阻断新连接；
//!    - Phase 2: 各 worker 线程退出 accept 循环后、运行时析构前排空活跃
//!      连接（全量下杀令 + 等待归零，超时留痕强收），随后线程退出被 join，
//!      释放缓冲池；
//!    - Phase 3: 排空完成后，上层才执行集群域 dispose 与配置刷盘。
//! 5. 统一服务端启动流水线（模板方法模式 ServerBootstrap，服务端唯一入口：
//!    启动参数 → 运行期事实的投影只此一处，宿主侧无逐字段装配样板）。

use std::{
  fmt,
  fs::create_dir_all,
  future::Future,
  io,
  net::SocketAddr,
  num::NonZeroUsize,
  path::Path,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  thread::{Builder as ThreadBuilder, JoinHandle, available_parallelism},
  time::Duration,
};

use compio::{
  net::TcpListener,
  runtime::{CancelToken, Cancelled, FutureExt, Runtime, spawn},
  time,
};
use crossfire::{mpsc::bounded_async, oneshot::oneshot};
use log::{debug, info, warn};
use parking_lot::Mutex;
use wbase::pool::{DEFAULT_BUFFER_SIZE, LimitedFixedBufferPool};
#[cfg(feature = "tls")]
use wconf::NodeArgs;
use wconf::ServerArgs;
use wmetric::GarnetServerMonitor;

#[cfg(feature = "tls")]
use crate::tls::ServerTlsConfig;
use crate::{
  Error,
  cluster_provider::{ClusterProvider, NoopClusterProvider},
  endpoint::ServerEndpoint,
  net::{
    ConnectionStream,
    handler::NetworkHandler,
    socket_opt::{bind_reuseport, configure_socket},
    uds::UdsGuard,
  },
  servers::consumer_registry::ConsumerRegistry,
  shutdown::ShutdownCoordinator,
  signal::wait_shutdown_signal,
  traits::SessionProviderFace,
};

/// 统一服务端启动流水线（模板方法模式）
///
/// 装配入参整体即 `A: ServerArgs`：指标采样节拍、延迟监视、逐命令统计与 TLS
/// 证书对全在 [`Self::run_async`] 一处从 `NodeArgs` 直读投影，结构体不持其
/// 副本、宿主侧无逐字段 setter（对标 C# StoreWrapper.cs:226-227 与
/// GarnetServer.cs:294 消费方直读 `serverOptions`，配置 → 运行期事实的投影
/// 唯一点在 libs/host/Configuration/Options.cs:948-962 的装配段），
/// 新增启动旋钮只需改投影一处，杜绝漏抄与第二套装配路径。
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
  /// 最大并发网络连接数（-1 = 不限；C# GarnetServerOptions.cs:347
  /// NetworkConnectionLimit，装配经 GarnetServer.cs:294 传入 GarnetServerTcp）
  network_connection_limit: i64,
  banner: String,
  /// 外部停机协调器（可选，便于宿主或测试受控关停）
  shutdown_coordinator: Option<ShutdownCoordinator>,
}

impl<A: ServerArgs> ServerBootstrap<A, NoopClusterProvider> {
  /// 创建单机服务端启动引导器（默认装配 NoopClusterProvider）
  pub fn new(args: A) -> Self {
    Self {
      args,
      cluster_provider: NoopClusterProvider,
      network_buffer_size: DEFAULT_BUFFER_SIZE,
      network_send_throttle_max: 8,
      network_connection_limit: -1,
      banner: "WeDB 数据库服务".into(),
      shutdown_coordinator: None,
    }
  }
}

impl<A: ServerArgs, C: ClusterProvider + Clone> ServerBootstrap<A, C> {
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
      network_connection_limit: self.network_connection_limit,
      banner: self.banner,
      shutdown_coordinator: self.shutdown_coordinator,
    }
  }

  /// 设置停机协调器（便于宿主或测试方受控退出）
  pub fn with_shutdown_coordinator(mut self, coordinator: ShutdownCoordinator) -> Self {
    self.shutdown_coordinator = Some(coordinator);
    self
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

  /// 设置最大并发网络连接数（-1 = 不限；C# Options.cs:399
  /// network-connection-limit / defaults.conf:304 默认 -1）
  pub fn network_connection_limit(mut self, limit: i64) -> Self {
    self.network_connection_limit = limit;
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

  /// 异步装配版启动流水线：会话提供者组装回调可等待设备面恢复
  ///
  /// 对标 C# GarnetServer.Start 的恢复时序（libs/host/GarnetServer.cs:529-535：
  /// `Provider.RecoverAsync().AsTask().GetAwaiter().GetResult()` 先于
  /// `servers[i].Start()` 同步完成——恢复完成前不接受连接）。rust 以异步
  /// 装配回调承接同一时序：恢复在端点 accept 启动之前 await 闭环。
  ///
  /// 装配回调消费 owned 参数（`A`/`C` 的克隆）；停机尾部 `flush_config`
  /// 经 bootstrap 持有的同一 `C` 实例执行（集群宿主传共享句柄如
  /// `Arc<ClusterProvider>`，克隆即同实例）
  pub fn run_async<P, F, Fut>(self, assemble: F) -> crate::Result<()>
  where
    P: SessionProviderFace + 'static,
    F: FnOnce(A, C) -> Fut,
    Fut: Future<Output = crate::Result<Arc<P>>>,
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

    // 2. 驱动 compio 全异步运行时启动服务并阻塞监听系统停机信号。
    //    会话提供者组装回调与集群管理协程均在运行时内执行
    //    （对标 C# GarnetServer.InitializeServer / Start 在运行时内完成存储打开与后台任务拉起）
    let rt = Runtime::new()?;
    let threads = node_args.threads.and_then(NonZeroUsize::new);
    let endpoints = node_args.endpoints();
    // UDS 权限位单点取值（wconf 折算与校验完毕后经装配链透传，绑定侧零校验）
    #[cfg(unix)]
    let unix_socket_perm = node_args.unix_socket_mode();
    // 启动参数 → 运行期服务事实的唯一投影点：监视器三开关与 TLS 一律直读
    // [`NodeArgs`]（对标 C# 消费侧 StoreWrapper.cs:226-227 直读
    // serverOptions.MetricsSamplingFrequency/CommandStatsMonitor/LatencyMonitor、
    // GarnetServer.cs:294 直读 opts.TlsOptions；配置 → options 的一次性投影
    // 在 Options.cs:948-962）。本结构不持其副本、宿主侧零逐字段 setter，
    // 新增启动旋钮只改本处与 wconf 字段
    let metrics_sampling_frequency = node_args.metrics_sampling_frequency_secs;
    let latency_monitor = node_args.latency_monitor;
    let commandstats_monitor = node_args.commandstats_monitor;
    #[cfg(feature = "tls")]
    let tls_config = tls_config_from_node(node_args)?;
    let banner = self.banner;
    let cluster_provider = self.cluster_provider;
    let args = self.args;
    let network_buffer_size = self.network_buffer_size;
    let network_send_throttle_max = self.network_send_throttle_max;
    let network_connection_limit = self.network_connection_limit;
    let shutdown_coordinator = self.shutdown_coordinator;
    let assemble = assemble;
    let cluster_for_assemble = cluster_provider.clone();

    rt.block_on(async move {
      let session_provider = assemble(args, cluster_for_assemble).await?;

      // 活跃消费者注册表（监视器采样源；构造服务器前取用）
      let registry = session_provider.consumer_registry();
      // 监视器复活化统计复位臂的宿主句柄（C# CleanupGlobalStats 直读
      // storeWrapper 的 rust 承接：会话提供者为 storeWrapper 对位面；
      // session_provider 随后移入 GarnetServer，故此处先行取一份共享句柄）
      let monitor_provider = Arc::clone(&session_provider);

      // 3. 基于配置与端点构造 GarnetServer（端点解析失败即中止启动，
      //    对标 C# Options.cs:795-797 配置期拒启时序）
      let mut server = GarnetServer::new(
        &endpoints,
        network_buffer_size,
        network_send_throttle_max,
        session_provider,
      )?
      .with_network_connection_limit(network_connection_limit);
      if let Some(coord) = shutdown_coordinator {
        server = server.with_shutdown_coordinator(coord);
      }
      #[cfg(unix)]
      {
        server = server.with_unix_socket_perm(unix_socket_perm);
      }
      #[cfg(feature = "tls")]
      if let Some(tls) = tls_config {
        server = server.with_tls_config(tls);
      }

      // 4. 启动网络监听端点（对标 C# GarnetServer.cs:532-533 servers[i].Start()）
      server.start(threads)?;

      // 5. 启动指标监视器采样循环（对标 C# StoreWrapper.cs:823 monitor?.Start()）
      if (metrics_sampling_frequency > 0 || commandstats_monitor || latency_monitor)
        && let Some(registry) = registry
      {
        start_server_monitor(
          server.shutdown_coordinator().clone(),
          registry,
          cluster_provider.clone(),
          monitor_provider,
          metrics_sampling_frequency,
          latency_monitor,
          commandstats_monitor,
        );
      }

      // 6. 启动集群提供者后台治理协程：已在 compio 运行时内（gossip/刷盘等
      //    后台任务 spawn 依赖当前运行时），时序对标 C# GarnetServer.Start
      //    （libs/host/GarnetServer.cs:527-535：Recover → Provider.Start →
      //    servers[i].Start），置于端点 accept 等待之前（单机 Noop 零开销
      //    内联消除）
      if cluster_provider.is_cluster_enabled() {
        cluster_provider.start();
      }

      // 7. 阻塞监听系统停机信号并执行优雅关机
      let run_res = server.wait_for_shutdown(&banner).await;
      // 8. 停机清理与集群配置持久化刷盘（C# StoreWrapper.Dispose 第 1 步
      //    clusterProvider?.Dispose() 的宿主承接段；bootstrap 持有集群
      //    提供者而 GarnetServer 不持，故留驻 wait_for_shutdown 之后的
      //    尾部执行——范围索引收口（第 7 步）已在其前的 server.stop()
      //    内落地，rust 相对 C# 集群/范围索引次序有意互换：集群治理面
      //    已随 worker join 停摆，二者无交叉触达，注释即步骤映射凭证）
      if cluster_provider.is_cluster_enabled() {
        cluster_provider.dispose();
        cluster_provider.flush_config();
      }
      run_res
    })
  }
}

/// 统一节点宿主服务器
pub struct GarnetServer<P: SessionProviderFace + 'static> {
  /// 配置的端点列表
  endpoints: Vec<ServerEndpoint>,
  /// 实际绑定的本地 TCP 地址列表
  tcp_addrs: Mutex<Vec<SocketAddr>>,
  /// 网络缓冲池
  buffer_pool: Arc<LimitedFixedBufferPool>,
  /// 慢客户端最大在途发送数
  network_send_throttle_max: usize,
  /// 最大并发网络连接数（-1 = 不限；C# 经 GarnetServer.cs:294 传入
  /// GarnetServerTcp 的 networkConnectionLimit，accept 成功分支计量拒绝）
  network_connection_limit: i64,
  /// 优雅停机协调器
  shutdown_coordinator: ShutdownCoordinator,
  /// 会话提供者
  session_provider: Arc<P>,
  /// 会话 ID 分配器
  session_id_counter: Arc<AtomicU64>,
  /// 工作线程句柄列表
  worker_threads: Mutex<Vec<JoinHandle<()>>>,
  /// TLS 证书配置（纯 Rust 实现；可选特性）
  #[cfg(feature = "tls")]
  tls_config: Option<ServerTlsConfig>,
  /// UDS 套接字文件权限模式位（None = 不设置，沿用 umask 现行为；对标
  /// C# GarnetServer.cs:294 将 opts.UnixSocketPermission 注入
  /// GarnetServerTcp 构造的透传链，值不进端点字符串）
  #[cfg(unix)]
  unix_socket_perm: Option<u32>,
}

impl<P: SessionProviderFace + 'static> GarnetServer<P> {
  /// 创建宿主服务器实例
  ///
  /// 端点解析失败即拒启（对标 libs/host/Configuration/Options.cs:795-797
  /// `TryParseAddressList` 失败或 `endpoints.Length == 0` 抛 GarnetException，
  /// 无任何静默回退端点）：逐端点 `?` 上抛 `Error::AddrParse` 并点名原字符串
  /// （bind 多地址拆分已在 wconf `NodeArgs::endpoints` 单点完成，此处禁再切分）。
  pub fn new(
    endpoints: &[String],
    network_buffer_size: usize,
    network_send_throttle_max: usize,
    session_provider: Arc<P>,
  ) -> crate::Result<Self> {
    let parsed_endpoints: Vec<ServerEndpoint> = endpoints
      .iter()
      .map(|e| ServerEndpoint::parse(e))
      .collect::<crate::Result<Vec<_>>>()?;
    if parsed_endpoints.is_empty() {
      return Err(Error::AddrParse(
        "监听端点列表为空（bind 无有效地址）".to_string(),
      ));
    }

    let buffer_pool = LimitedFixedBufferPool::new(network_buffer_size, 0);

    Ok(Self {
      endpoints: parsed_endpoints,
      tcp_addrs: Mutex::new(Vec::new()),
      buffer_pool,
      network_send_throttle_max: network_send_throttle_max.max(1),
      network_connection_limit: -1,
      shutdown_coordinator: ShutdownCoordinator::new(),
      session_provider,
      session_id_counter: Arc::new(AtomicU64::new(1)),
      worker_threads: Mutex::new(Vec::new()),
      #[cfg(feature = "tls")]
      tls_config: None,
      #[cfg(unix)]
      unix_socket_perm: None,
    })
  }

  /// 设置外部停机协调器
  pub fn with_shutdown_coordinator(mut self, shutdown_coordinator: ShutdownCoordinator) -> Self {
    self.shutdown_coordinator = shutdown_coordinator;
    self
  }

  /// 设置 TLS 证书配置
  #[cfg(feature = "tls")]
  pub fn with_tls_config(mut self, tls_config: impl Into<Option<ServerTlsConfig>>) -> Self {
    self.tls_config = tls_config.into();
    self
  }

  /// 设置最大并发网络连接数（-1 = 不限；C# GarnetServer.cs:294 把
  /// opts.NetworkConnectionLimit 传入 GarnetServerTcp 构造的装配位）
  pub fn with_network_connection_limit(mut self, limit: i64) -> Self {
    self.network_connection_limit = limit;
    self
  }

  /// 注入 UDS 套接字文件权限模式位（端点装配单点注入，同 with_tls_config
  /// 形态；对标 C# GarnetServer.cs:294 构造透传，禁调用方各自读配置字段）
  #[cfg(unix)]
  pub fn with_unix_socket_perm(mut self, perm: Option<u32>) -> Self {
    self.unix_socket_perm = perm;
    self
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
          self.start_unix_worker(_path, self.unix_socket_perm)?;
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
    let (ready_tx, ready_rx) = oneshot();

    let mut handles = Vec::with_capacity(nthreads);

    // Worker 0: 负责首发绑定（特别针对动态端口 :0）并广播实际地址
    {
      let coordinator = self.shutdown_coordinator.clone();
      let id_gen = Arc::clone(&self.session_id_counter);
      let provider = Arc::clone(&self.session_provider);
      let pool = Arc::clone(&self.buffer_pool);
      let throttle_max = self.network_send_throttle_max;
      let conn_limit = self.network_connection_limit;
      #[cfg(feature = "tls")]
      let tls_config = self.tls_config.clone();

      let handle = ThreadBuilder::new()
        .name("wnode-worker-0".into())
        .spawn(move || {
          let rt = match Runtime::new() {
            Ok(r) => r,
            Err(e) => {
              ready_tx.send(Err(io::Error::other(e.to_string())));
              return;
            }
          };

          rt.block_on(async move {
            let bind_res = async {
              let listener = bind_reuseport(target_addr).await?;
              let actual_addr = listener.local_addr()?;
              Ok((listener, actual_addr))
            }
            .await;

            let (listener, _actual_addr) = match bind_res {
              Ok(pair) => {
                ready_tx.send(Ok(pair.1));
                pair
              }
              Err(e) => {
                ready_tx.send(Err(e));
                return;
              }
            };

            let ctx = TcpAcceptContext {
              core_id: 0,
              listener,
              coordinator,
              id_gen,
              provider,
              pool,
              throttle_max,
              conn_limit,
              #[cfg(feature = "tls")]
              tls_config,
            };
            run_tcp_accept_loop(ctx).await;
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
      let (worker_ready_tx, worker_ready_rx) = oneshot();
      let coordinator = self.shutdown_coordinator.clone();
      let id_gen = Arc::clone(&self.session_id_counter);
      let provider = Arc::clone(&self.session_provider);
      let pool = Arc::clone(&self.buffer_pool);
      let throttle_max = self.network_send_throttle_max;
      let conn_limit = self.network_connection_limit;
      #[cfg(feature = "tls")]
      let tls_config = self.tls_config.clone();

      let handle = ThreadBuilder::new()
        .name(format!("wnode-worker-{core_id}"))
        .spawn(move || {
          let rt = match Runtime::new() {
            Ok(r) => r,
            Err(e) => {
              worker_ready_tx.send(Err(io::Error::other(e.to_string())));
              return;
            }
          };

          rt.block_on(async move {
            let listener = match bind_reuseport(actual_addr).await {
              Ok(l) => {
                worker_ready_tx.send(Ok(()));
                l
              }
              Err(e) => {
                worker_ready_tx.send(Err(e));
                return;
              }
            };

            let ctx = TcpAcceptContext {
              core_id,
              listener,
              coordinator,
              id_gen,
              provider,
              pool,
              throttle_max,
              conn_limit,
              #[cfg(feature = "tls")]
              tls_config,
            };
            run_tcp_accept_loop(ctx).await;
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
  fn start_unix_worker(&self, path: &Path, perm: Option<u32>) -> io::Result<()> {
    let (ready_tx, ready_rx) = oneshot();
    let coordinator = self.shutdown_coordinator.clone();
    let id_gen = Arc::clone(&self.session_id_counter);
    let provider = Arc::clone(&self.session_provider);
    let pool = Arc::clone(&self.buffer_pool);
    let throttle_max = self.network_send_throttle_max;
    let conn_limit = self.network_connection_limit;
    let path_buf = path.to_path_buf();

    let handle = ThreadBuilder::new()
      .name("wnode-uds".into())
      .spawn(move || {
        let rt = match Runtime::new() {
          Ok(r) => r,
          Err(e) => {
            ready_tx.send(Err(io::Error::other(e.to_string())));
            return;
          }
        };

        rt.block_on(async move {
          let (listener, _guard) = match UdsGuard::bind(&path_buf, perm).await {
            Ok(res) => {
              info!("启动 Unix 域套接字监听: {}", path_buf.display());
              ready_tx.send(Ok(()));
              res
            }
            Err(e) => {
              ready_tx.send(Err(e));
              return;
            }
          };

          let cancel_token = CancelToken::new();
          let watcher_cancel = cancel_token.clone();
          let coord = coordinator.clone();
          spawn(async move {
            coord.wait().await;
            watcher_cancel.cancel();
          })
          .detach();

          let mut backoff_ms = INITIAL_ACCEPT_BACKOFF_MS;
          while !coordinator.is_stopped() {
            let accept_res = listener
              .accept()
              .with_cancel(cancel_token.clone())
              .fail_fast()
              .await;

            match accept_res {
              Ok(Ok((stream, _))) => {
                backoff_ms = INITIAL_ACCEPT_BACKOFF_MS;
                // 在途容量门与 TCP 循环同一收口（C# UDS 监听同为
                // GarnetServerTcp 形态：accept 成功即刻计量，超限臂即刻
                // 关闭新连接且不写任何 RESP 应答）
                let guard = match provider.consumer_registry() {
                  Some(registry) => match registry.try_acquire_connection(conn_limit) {
                    Some(guard) => Some(guard),
                    None => {
                      debug!("UDS 在途连接达上限 {conn_limit}，关闭新连接");
                      continue;
                    }
                  },
                  None => None,
                };
                if coordinator.is_stopped() {
                  break;
                }
                let sender_id = id_gen.fetch_add(1, Ordering::Relaxed);
                let mut handler = NetworkHandler::new(
                  sender_id,
                  path_buf.display().to_string(),
                  Arc::clone(&pool),
                  throttle_max,
                );
                let provider_clone = Arc::clone(&provider);
                spawn(async move {
                  // 在途守卫随连接任务结束归零
                  let _in_flight = guard;
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
                // accept 失败分档（与 TCP 循环共用同一收口）
                if !handle_accept_error("UDS", e, &coordinator, &cancel_token, &mut backoff_ms)
                  .await
                {
                  break;
                }
              }
              Err(Cancelled) => {
                debug!("UDS 接收循环收到停机取消信号，退出");
                break;
              }
            }
          }

          // 停机 Phase 2：本线程运行时析构前排空活跃连接（C#
          // GarnetServerBase.DisposeActiveHandlers），detach 连接任务
          // 须在本 block_on 尾部驱动至归零，否则随 Runtime 析构被截断
          if let Some(registry) = provider.consumer_registry() {
            registry.dispose_active_handlers().await;
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
  /// coordinator.stop 停监听（Phase 1）→ AOF 背压闸门放行（滞留追加方出口，
  /// 置位幂等）→ pubsub 中枢收口（等消费循环退出并清订阅表）→ join worker
  /// 线程（各线程运行时内已完成活跃连接排空，Phase 2）→ 范围索引收口 →
  /// 释放缓冲池；join 返回即排空结束，上层的集群 dispose 与配置刷盘
  /// （Phase 3）在其后执行
  ///
  /// 闸门放行必须先于 join：rust join 无超时上界，同步 wait_slow 阻塞的是
  /// worker 线程本体（poll 内阻塞，连接任务强杀不可打断）；C# 靠 WaitSlow
  /// 轮询 disposed 与 DrainActiveHandlers 超时兜底，无此前置约束
  ///
  /// libs/host/GarnetServer.cs:InternalDispose
  pub fn stop(&self) {
    // 向量清理协程收敛必须先于停 coordinator/join worker：C# VectorManager.Dispose
    // 逐通道 CompleteAndWaitForConsumerTask 依赖消费运行时仍存活排空积压；此处在
    // 主线程阻塞等待，各 worker 线程 compio 运行时（thread-per-core，独立）继续
    // 驱动协程消费退出，故必须早于下方 join（Phase 2）。
    if !self.session_provider.dispose_vector_cleanup() {
      log::warn!("向量清理协程未在超时内全部收敛，停机继续");
    }
    self.shutdown_coordinator.stop();
    // AOF 背压闸门关停放行（C# AofBackpressure.Dispose：Release all stalled
    // appenders permanently (server shutdown)；无 AOF 形态经 trait 默认
    // None 跳过）
    if let Some(aof) = self.session_provider.aof()
      && let Some(bp) = aof.backpressure()
    {
      bp.dispose();
    }
    // pubsub 中枢收口（C# InternalDispose 的 subscribeBroker?.Dispose()：
    // 等后台消费循环跑完在途批次退出再清订阅表，done.WaitOne 语义）。次序
    // 与 C# 有意差异：C# 排在 Provider.Dispose 之后，rust 的 AOF 完整收口
    // （dispose_async）须 join 后由主运行时直驱（async 设备 IO 进不了同步
    // stop），而消费任务跑在 worker 运行时——本等待需运行时驱动，只能先于
    // join（join = worker 运行时析构屏障）；未装配 pubsub 经 trait 默认跳过
    if !self.session_provider.dispose_pubsub() {
      log::warn!("pubsub 消费任务未在超时内收敛，停机继续");
    }
    let mut handles = self.worker_threads.lock();
    for handle in handles.drain(..) {
      let _ = handle.join();
    }
    // 范围索引停机收口（C# StoreWrapper.Dispose 的
    // rangeIndexManager?.Dispose()，Provider.Dispose 段第七步：时序在
    // clusterProvider/itemBroker/taskManager 之后、databaseManager 之前——
    // 对位即 Phase 2 连接排空（join）之后、引擎 store 兜底析构之前）。
    // 显式步落地后，嵌入式/复用进程形态 stop() 返回即释放全部在线树与
    // native 页缓存，不再依赖各 Arc 引用恰好归零；引擎侧幂等，与
    // WedbStore::drop 兜底并存安全。早于 join 则在途命令仍可触达在建树，
    // 故不可前移；未装配引擎经 trait 默认跳过
    self.session_provider.dispose_range_index();
    self.buffer_pool.purge();
  }

  /// 等待系统停机信号或协调器停机并优雅关机
  pub async fn wait_for_shutdown(&self, server_label: &str) -> crate::Result<()> {
    #[cfg(feature = "tls")]
    let tls_info = if self.tls_config.is_some() {
      " (TLS 启用)"
    } else {
      ""
    };
    #[cfg(not(feature = "tls"))]
    let tls_info = "";
    log::info!(
      "{server_label} 网络监听就绪: {:?}{tls_info}",
      self.local_addrs()
    );

    let coordinator = self.shutdown_coordinator.clone();
    let (tx, rx) = bounded_async::<&'static str>(1);
    let tx_sig = tx.clone();
    let sig_task = spawn(async move {
      let sig = wait_shutdown_signal().await.unwrap_or("信号监听失败");
      let _ = tx_sig.try_send(sig);
    });
    let coord_task = spawn(async move {
      coordinator.wait().await;
      let _ = tx.try_send("停机协调器");
    });

    let sig = rx.recv().await.unwrap_or("未知信号");
    drop(sig_task);
    drop(coord_task);

    log::info!("捕获停机信号 ({sig}), 正在优雅关机 {server_label}...");
    self.stop();
    // AOF 刷盘收口（C# InternalDispose Phase 3 Provider.Dispose →
    // AppendOnlyFile.Dispose）：网络已排空、worker 运行时已析构，主运行时
    // 直驱未提交帧落设备（async 设备 IO 无法在同步 stop() 内完成）
    if let Some(aof) = self.session_provider.aof() {
      aof.dispose_async().await;
    }
    log::info!("{server_label} 安全退出完成");
    Ok(())
  }
}

impl<P: SessionProviderFace + 'static> Drop for GarnetServer<P> {
  fn drop(&mut self) {
    self.stop();
  }
}

/// 启动参数 TLS 证书对 → 服务端 TLS 配置投影（[`ServerBootstrap::run_async`]
/// 的唯一 TLS 入口，对标 C# Options.cs:948-957 EnableTLS 时一处构造
/// GarnetTlsOptions、GarnetServer.cs:294 端点直读 opts.TlsOptions）
///
/// 证书与私钥必须成对：单侧配置在 C# 由 GarnetTlsOptions 构造期拒
/// （CertFileName 空即抛），rust 同判据启动期拒绝，杜绝静默降级明文。
///
/// 入站客户端认证旋钮一并过同一投影（对标 C# GarnetServerTcp.cs:290
/// handler.Start(tlsOptions?.TlsServerOptions) 携带的
/// ClientCertificateRequired + IssuerCertificatePath 两面）
#[cfg(feature = "tls")]
fn tls_config_from_node(node: &NodeArgs) -> crate::Result<Option<ServerTlsConfig>> {
  match (&node.tls_cert, &node.tls_key) {
    (Some(cert), Some(key)) => Ok(Some(ServerTlsConfig::from_pem_files(
      cert,
      key,
      node.tls_client_cert_required,
      node.tls_issuer_cert.as_deref(),
    )?)),
    (Some(_), None) | (None, Some(_)) => Err(Error::InvalidArgument(
      "tls_cert 与 tls_key 必须同时提供".into(),
    )),
    (None, None) => Ok(None),
  }
}

/// 启动服务器指标监视器采样循环
///
/// libs/server/Metrics/GarnetServerMonitor.cs:Start（宿主启动序列
/// StoreWrapper.Start() → monitor?.Start()，频率 > 0 才拉起后台采样任务）。
/// 监视器随宿主进程级安装（dispose 归并直取），停机协调器充当
/// C# CancellationToken——stop 即取消采样循环。
///
/// INFO RESETSTAT 的六件事由本装配点凑齐：会话/连接/命令统计/延迟四臂的
/// 回调在 [`ConsumerRegistry::monitor_iteration_inputs`] 内构造，gossip 与
/// 复活化两臂的句柄（C# `storeWrapper.clusterProvider` 与 `storeWrapper`
/// 本身）在此注入——单机形态的集群句柄即 NoopClusterProvider，其
/// `reset_gossip_stats` 默认空操作正是 C# clusterProvider 为 null 的对位
fn start_server_monitor<C, P>(
  coordinator: ShutdownCoordinator,
  registry: Arc<ConsumerRegistry>,
  cluster_provider: C,
  session_provider: Arc<P>,
  frequency_secs: u64,
  latency_monitor: bool,
  commandstats_monitor: bool,
) where
  C: ClusterProvider + Clone + 'static,
  P: SessionProviderFace + 'static,
{
  // C# GarnetServerMonitor.cs:64 构造（true, opts.LatencyMonitor,
  // opts.CommandStatsMonitor, this）：三追踪开关决定聚合成员是否就位
  let monitor = Arc::new(GarnetServerMonitor::new(
    frequency_secs,
    true,
    latency_monitor,
    commandstats_monitor,
  ));
  monitor.install_global();

  // C# GarnetServerMonitor.cs:Start：周期采样任务仅在配置了采样频率时
  // 拉起（监视器可仅为命令统计历史装配，无周期采样；dispose 归并不依赖
  // 采样循环）
  if frequency_secs > 0 {
    let monitor = Arc::clone(&monitor);
    let gossip_handle = cluster_provider;
    let reviv_handle = session_provider;
    spawn(async move {
      monitor
        .main_monitor_task_async(
          time::sleep,
          || coordinator.is_stopped(),
          || {
            // 每轮重建两臂闭包（输入拥有型：闭包各持一份句柄克隆，
            // 与 C# 每轮直查 storeWrapper.clusterProvider 同构）
            let gossip = gossip_handle.clone();
            let reviv = Arc::clone(&reviv_handle);
            registry.monitor_iteration_inputs(
              move || gossip.reset_gossip_stats(),
              move || reviv.reset_revivification_stats(),
            )
          },
        )
        .await;
    })
    .detach();
  }
  info!("服务器指标监视器已启动: 采样频率 {frequency_secs}s");
}

/// accept 资源压力退避的初值与封顶毫秒数（每接入循环协程栈上持有一份，
/// 与 C# 每监听器一字段同粒度，禁全局可变）
///
/// 在 garnet 中的相对路径: libs/server/Servers/GarnetServerTcp.cs:33-34
const INITIAL_ACCEPT_BACKOFF_MS: u64 = 100;
const MAX_ACCEPT_BACKOFF_MS: u64 = 5000;

/// accept 资源压力判定（对标 C# 档内 SocketError 在 unix 的对应物）：
/// ENOMEM 用 std 稳定归一的 `OutOfMemory` kind；EMFILE(24)/ENFILE(23) 在
/// Linux/macOS 同值、ENOBUFS 异值（105/55），std 未将其归一为稳定 kind，
/// 故以 cfg 编译期 raw errno 集合补判——热路径零字符串比较、零平台运行时分支
#[cfg(target_os = "linux")]
const ACCEPT_RESOURCE_ERRNOS: [i32; 3] = [24, 23, 105];
#[cfg(not(target_os = "linux"))]
const ACCEPT_RESOURCE_ERRNOS: [i32; 3] = [24, 23, 55];

fn is_accept_resource_pressure(e: &io::Error) -> bool {
  e.kind() == io::ErrorKind::OutOfMemory
    || matches!(e.raw_os_error(), Some(errno) if ACCEPT_RESOURCE_ERRNOS.contains(&errno))
}

/// accept 失败分档收口：返回 false 表示接入循环应退出
///
/// 在 garnet 中的相对路径: libs/server/Servers/GarnetServerTcp.cs:HandleAcceptError
///
/// 三档对标：一档致命（C# OperationAborted/Shutdown 静默停循环）对应 rust
/// 停机竞态 `coordinator.is_stopped()`；二档资源压力（C# TooManyOpenSockets /
/// NoBufferSpaceAvailable / ProcessLimit，即 unix 的 EMFILE/ENFILE/ENOBUFS/
/// ENOMEM，见 is_accept_resource_pressure）warning 带退避毫秒 + 指数退避
/// 封顶 5s，成功 accept 后由调用方复位（C#:234）；三档其余（C# default）
/// debug 即续。C# 档内 NetworkDown/SystemNotReady 为监听 socket 不可能经
/// accept 产出的形态（后者 Windows 专属），不纳入判定。
///
/// C# 的 `Thread.Sleep` 阻塞在此换成退避 sleep 与停机取消令牌竞速：本线程
/// reactor 绝不能被阻塞（单线程多协程），且 SIGTERM 最迟在下一次让渡点生效，
/// 不被拖到 5 秒。C#:197-204 的 throw 崩溃档不做——那组 NotSocket 一类是
/// IOCP/Windows 产物，rust 协程持有着 listener 本体，对应形态不可达，
/// panic 爆炸半径亦不等价，只保留 break/退避两种收口。
async fn handle_accept_error(
  target: impl fmt::Display,
  e: io::Error,
  coordinator: &ShutdownCoordinator,
  cancel_token: &CancelToken,
  backoff_ms: &mut u64,
) -> bool {
  // 一档：停机竞态（对标 C# return false）
  if coordinator.is_stopped() {
    return false;
  }
  if is_accept_resource_pressure(&e) {
    // 二档：资源压力，先按当前档位让渡再翻倍封顶（对标 C#:212-216）
    warn!("{target} 接收连接资源压力，退避 {backoff_ms}ms: {e}");
    let slept = time::sleep(Duration::from_millis(*backoff_ms))
      .with_cancel(cancel_token.clone())
      .fail_fast()
      .await;
    *backoff_ms = (*backoff_ms * 2).min(MAX_ACCEPT_BACKOFF_MS);
    return slept.is_ok();
  }
  // 三档：瞬时错误，debug 续不退避（对标 C#:220-222）
  debug!("{target} 瞬时接收错误，继续: {e}");
  true
}

struct TcpAcceptContext<P: SessionProviderFace> {
  core_id: usize,
  listener: TcpListener,
  coordinator: ShutdownCoordinator,
  id_gen: Arc<AtomicU64>,
  provider: Arc<P>,
  pool: Arc<LimitedFixedBufferPool>,
  throttle_max: usize,
  /// 在途连接容量门（-1 = 不限；C# networkConnectionLimit）
  conn_limit: i64,
  #[cfg(feature = "tls")]
  tls_config: Option<ServerTlsConfig>,
}

/// 驱动单个核心的 TCP 连接接入循环
///
/// 在 garnet 中的相对路径: libs/server/Servers/GarnetServerTcp.cs:HandleNewConnection
///
/// accept 成功回调本体承接：复位退避 → 在途容量门 → socket 配置 → 建 handler
/// 并注册 → 拉起连接泵；容量门子步骤对位见
/// [`ConsumerRegistry::try_acquire_connection`]，accept 失败分档见
/// handle_accept_error（对标 C# HandleAcceptError 一/二/三档）
async fn run_tcp_accept_loop<P: SessionProviderFace + 'static>(ctx: TcpAcceptContext<P>) {
  let cancel_token = CancelToken::new();
  let watcher_cancel = cancel_token.clone();
  let coordinator = ctx.coordinator.clone();
  spawn(async move {
    coordinator.wait().await;
    watcher_cancel.cancel();
  })
  .detach();

  // 退避状态协程栈持有（C#:35 acceptBackoffMs，每监听器一份）
  let mut backoff_ms = INITIAL_ACCEPT_BACKOFF_MS;

  while !ctx.coordinator.is_stopped() {
    let accept_res = ctx
      .listener
      .accept()
      .with_cancel(cancel_token.clone())
      .fail_fast()
      .await;

    match accept_res {
      Ok(Ok((stream, client_addr))) => {
        // 成功接入复位退避（C# GarnetServerTcp.cs:234）
        backoff_ms = INITIAL_ACCEPT_BACKOFF_MS;
        // 在途容量门（C# GarnetServerTcp.cs:236-241）：accept 成功即刻计量，
        // 先于 socket 配置与 handler 装配；超限臂（:302-307）回退计数并即刻
        // 关闭新连接且不写任何 RESP 应答——continue 离开作用域 drop stream
        // 即对偶 C# AcceptSocket.Dispose()。守卫 move 进连接任务随其结束
        // （正常收尾/TLS 握手失败）Drop 归零；无注册表的哑桩（trait 默认
        // None）不设门，语义同 limit=-1
        let guard = match ctx.provider.consumer_registry() {
          Some(registry) => match registry.try_acquire_connection(ctx.conn_limit) {
            Some(guard) => Some(guard),
            None => {
              debug!(
                "Worker-{} 在途连接达上限 {}，关闭新连接",
                ctx.core_id, ctx.conn_limit
              );
              continue;
            }
          },
          None => None,
        };
        // 接入侧装配 nodelay + 默认保活（C#:249 仅 NoDelay，保活为 rust 自有面）
        let _ = configure_socket(&stream);
        if ctx.coordinator.is_stopped() {
          break;
        }
        let sender_id = ctx.id_gen.fetch_add(1, Ordering::Relaxed);
        let mut handler = NetworkHandler::<P::Consumer>::new(
          sender_id,
          client_addr.to_string(),
          Arc::clone(&ctx.pool),
          ctx.throttle_max,
        );
        let provider_clone = Arc::clone(&ctx.provider);
        let core_id = ctx.core_id;

        #[cfg(feature = "tls")]
        let tls_acceptor = ctx.tls_config.as_ref().map(|c| c.acceptor().clone());

        spawn(async move {
          // 在途守卫随连接任务结束归零（C# activeHandlerCount 的
          // decrement 挂点在 handler dispose，含握手失败臂）
          let _in_flight = guard;

          #[cfg(feature = "tls")]
          let connection_stream = if let Some(acceptor) = tls_acceptor {
            match acceptor.accept(stream).await {
              Ok(tls_stream) => ConnectionStream::tls(tls_stream),
              Err(err) => {
                log::debug!("Worker-{core_id} TLS 握手失败: {err}");
                return;
              }
            }
          } else {
            ConnectionStream::Tcp(stream)
          };

          #[cfg(not(feature = "tls"))]
          let connection_stream = ConnectionStream::Tcp(stream);

          if let Err(err) = handler
            .process_stream(connection_stream, provider_clone, sender_id)
            .await
          {
            log::debug!("Worker-{core_id} 连接处理结束: {err}");
          }
        })
        .detach();
      }
      Ok(Err(e)) => {
        // accept 失败分档（对标 C# HandleAcceptError 的一/二/三档）
        if !handle_accept_error(
          format_args!("Worker-{}", ctx.core_id),
          e,
          &ctx.coordinator,
          &cancel_token,
          &mut backoff_ms,
        )
        .await
        {
          break;
        }
      }
      Err(Cancelled) => {
        debug!("Worker-{} 收到停机取消信号，退出接入循环", ctx.core_id);
        break;
      }
    }
  }

  // 停机 Phase 2：本线程运行时析构前排空活跃连接（C#
  // GarnetServerBase.DisposeActiveHandlers），detach 连接任务须在本
  // block_on 尾部驱动至归零，否则随 Runtime 析构被截断
  if let Some(registry) = ctx.provider.consumer_registry() {
    registry.dispose_active_handlers().await;
  }
}

#[cfg(test)]
mod tests {
  use std::mem::take;

  use super::*;
  use crate::{
    Error,
    traits::{MessageConsumerFace, WireFormat},
  };

  /// 最小哑消费者：仅满足端点解析拒启断言，不触网络
  struct NullConsumer(Vec<u8>);

  impl MessageConsumerFace for NullConsumer {
    fn try_consume_messages_into(&mut self, _resp_buf: &mut Vec<u8>) -> Option<usize> {
      Some(0)
    }
    fn take_recv_scratch(&mut self) -> Vec<u8> {
      take(&mut self.0)
    }
    fn return_recv_scratch(&mut self, buf: Vec<u8>) {
      self.0 = buf;
    }
    fn dispose(&mut self) {}
  }

  struct NullProvider;

  impl SessionProviderFace for NullProvider {
    type Consumer = NullConsumer;
    fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<NullConsumer> {
      Some(NullConsumer(Vec::new()))
    }
  }

  /// 非法端点拒启且错误信息含原字符串（对标 C# Options.cs:795-797 抛
  /// GarnetException，禁静默回退默认端点）
  #[test]
  fn new_rejects_invalid_endpoint() {
    let err = match GarnetServer::new(
      &["1.2.3.4:65536".to_string()],
      4096,
      8,
      Arc::new(NullProvider),
    ) {
      Ok(_) => panic!("端口越界端点必须拒启"),
      Err(e) => e,
    };
    assert!(matches!(err, Error::AddrParse(_)), "实际错误: {err:?}");
    assert!(
      err.to_string().contains("1.2.3.4:65536"),
      "错误信息须点名原字符串，实际: {err}"
    );
  }

  /// 空端点列表拒启（C# Options.cs:796 `endpoints.Length == 0` 臂）
  #[test]
  fn new_rejects_empty_endpoints() {
    assert!(GarnetServer::new(&[], 4096, 8, Arc::new(NullProvider)).is_err());
  }
}
