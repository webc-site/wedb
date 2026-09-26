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
//!      连接（全量下杀令 + 等待归零，超时留痕强收），随后线程退出被有界
//!      join（防御档上界见 WORKER_JOIN_TIMEOUT_MS，deviations §152），释放
//!      缓冲池；
//!    - Phase 3: 排空完成后，上层才执行集群域 dispose 与配置刷盘。
//! 5. 统一服务端启动流水线（模板方法模式 ServerBootstrap，服务端唯一入口：
//!    启动参数 → 运行期事实的投影只此一处，宿主侧无逐字段装配样板）。

#[cfg(unix)]
use std::path::PathBuf;
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
    mpsc::channel,
  },
  thread,
  thread::{Builder as ThreadBuilder, JoinHandle, available_parallelism},
  time::Duration,
};

#[cfg(unix)]
use compio::net::UnixListener;
use compio::{
  net::TcpListener,
  runtime::{CancelToken, Cancelled, FutureExt, Runtime, spawn},
  time,
};
use crossfire::{
  mpsc::bounded_async,
  oneshot::{RxOneshot, oneshot},
};
use log::{debug, info, warn};
use parking_lot::Mutex;
use wbase::{
  endpoint::ip_is_loopback,
  pool::{DEFAULT_BUFFER_SIZE, LimitedFixedBufferPool},
  supervise::supervise_task,
};
use wconf::{NodeArgs, ServerArgs};
use wmetric::GarnetServerMonitor;
#[cfg(feature = "tls")]
use wtls::ServerTlsConfig;

use crate::{
  Error,
  cluster_provider::{ClusterProvider, NoopClusterProvider},
  datadir_lock::DataDirLock,
  endpoint::ServerEndpoint,
  net::{
    ConnectionStream,
    handler::{NetworkHandler, kill::spawn_kill_watcher},
    socket_opt::{bind_reuseport, configure_socket},
    stream::tcp_local_endpoint,
    uds::UdsGuard,
  },
  servers::consumer_registry::{ConnectionGuard, ConsumerRegistry},
  shutdown::ShutdownCoordinator,
  signal::wait_shutdown_signal,
  traits::{MessageConsumerFace, PeerSource, SessionProviderFace},
};

/// 端点列表为空的拒启错误文本（C# Options.cs:796 `endpoints.Length == 0` 臂；
/// run_async 与 GarnetServer::new 两处判空共用，禁文本二写）
const EMPTY_ENDPOINTS_ERR: &str = "监听端点列表为空（bind 无有效地址）";

/// 统一服务端启动流水线（模板方法模式）
///
/// 装配入参整体即 `A: ServerArgs`：指标采样节拍、延迟监视、逐命令统计、
/// TLS 证书对与连接上限全在 [`Self::run_async`] 一处从 `NodeArgs` 直读投影，
/// 结构体不持其副本、宿主侧无逐字段 setter（对标 C# StoreWrapper.cs:226-227
/// 与 GarnetServer.cs:294 消费方直读 `serverOptions`，配置 → 运行期事实的投影
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

    #[cfg(not(feature = "tls"))]
    guard_no_tls(node_args)?;

    // 1. 确保基础与 WAL 数据目录就绪
    if !node_args.dir.as_os_str().is_empty() {
      let _ = create_dir_all(&node_args.dir);
    }
    let wal_dir = node_args.wal_dir();
    if !wal_dir.as_os_str().is_empty() {
      let _ = create_dir_all(&wal_dir);
    }
    let checkpoint_base_dir = node_args.checkpoint_base_dir();
    if !checkpoint_base_dir.as_os_str().is_empty() {
      let _ = create_dir_all(&checkpoint_base_dir);
    }

    // 1.5 数据目录排他锁（flock）：SO_REUSEPORT 架构偏差下防多实例并发
    //     双写的唯一防线（deviations 第 11 条），设备打开前单点获取。
    //     守卫随本流水线存活（move 进运行时闭包），进程退出含 kill -9
    //     由内核回收 fd 自动释放；空目录形态（嵌入式无目录）无互踩面跳过
    let _datadir_lock = if node_args.dir.as_os_str().is_empty() {
      None
    } else {
      Some(DataDirLock::acquire(
        &node_args.dir,
        &wal_dir,
        Some(&checkpoint_base_dir),
      )?)
    };

    // 2. 驱动 compio 全异步运行时启动服务并阻塞监听系统停机信号。
    //    会话提供者组装回调与集群管理协程均在运行时内执行
    //    （对标 C# GarnetServer.InitializeServer / Start 在运行时内完成存储打开与后台任务拉起）
    let rt = Runtime::new()?;
    let threads = node_args.threads.and_then(NonZeroUsize::new);
    let endpoints = node_args.endpoints()?;
    if endpoints.is_empty() {
      return Err(Error::AddrParse(EMPTY_ENDPOINTS_ERR.to_string()));
    }
    // UDS 权限位单点取值（wconf 折算与校验完毕后经装配链透传，绑定侧零校验）
    #[cfg(unix)]
    let unix_socket_perm = node_args.unix_socket_mode();
    // 启动参数 → 运行期服务事实的唯一投影点：监视器三开关、连接上限与 TLS
    // 一律直读 [`NodeArgs`]（对标 C# 消费侧 StoreWrapper.cs:226-227 直读
    // serverOptions.MetricsSamplingFrequency/CommandStatsMonitor/LatencyMonitor、
    // GarnetServer.cs:294 直读 opts.NetworkConnectionLimit 与 opts.TlsOptions，
    // 配置 → options 的一次性投影在 Options.cs:948-962）。本结构不持其
    // 副本、宿主侧零逐字段 setter，新增启动旋钮只改本处与 wconf 字段
    let metrics_sampling_frequency = node_args.metrics_sampling_frequency_secs;
    let latency_monitor = node_args.latency_monitor;
    let commandstats_monitor = node_args.commandstats_monitor;
    // 连接上限（C# Options.cs:399 network-connection-limit，IntRangeValidation
    // (-1, int.MaxValue)，defaults.conf:304 默认 -1 不限；经 GarnetServer.cs:294
    // 传入 GarnetServerTcp 的同一装配位）
    let network_connection_limit = i64::from(node_args.network_connection_limit);
    // QuietMode 启动静默（C# Options.cs:363-364 QuietMode；消费门禁
    // GarnetServer.cs:174 横幅与 :535 `* Ready to accept connections` 就绪文本）：
    // 置位即不输出监听就绪横幅
    let quiet = node_args.quiet;
    let banner = self.banner;
    let cluster_provider = self.cluster_provider;
    let args = self.args;
    let network_buffer_size = self.network_buffer_size;
    let network_send_throttle_max = self.network_send_throttle_max;
    let shutdown_coordinator = self.shutdown_coordinator;
    let assemble = assemble;
    let cluster_for_assemble = cluster_provider.clone();

    rt.block_on(async move {
      // 排他锁守卫引用一次完成 move 捕获：随闭包存活至停机返回，此后
      // 全程持锁（drop 时点即本流水线退出点，见第 1.5 步装配注释）
      let _datadir_lock = _datadir_lock;
      let session_provider = assemble(args, cluster_for_assemble).await?;

      // 活跃消费者注册表（监视器采样源；构造服务器前取用）
      let registry = session_provider.consumer_registry();
      // 监视器复活化统计复位臂的宿主句柄（C# CleanupGlobalStats 直读
      // storeWrapper 的 rust 承接：会话提供者为 storeWrapper 对位面；
      // session_provider 随后移入 GarnetServer，故此处先行取一份共享句柄）
      let monitor_provider = Arc::clone(&session_provider);

      // 3. 基于配置与端点构造 GarnetServer（端点解析失败即中止启动，
      //    对标 C# Options.cs:795-797 配置期拒启时序）
      // TLS 证书配置随会话提供者装配链单点构造（对标 C# TlsOptions 单实例
      // 共享：网络端点 handler 与 storeWrapper 会话域触达同一 TlsOptions，
      // CONFIG SET cert-file-name 换装证书对全部端点的新握手即时生效）。
      // 取用先于 provider move 进服务器构造
      #[cfg(feature = "tls")]
      let tls_config = session_provider.tls_config();

      // 3.5 监视器进程级安装先于网络监听（对标 C# StoreWrapper.cs:226 monitor
      // 随 StoreWrapper 构造早于 GarnetServer.Start：窗口期连接亦可取得监视器
      // 时钟与全局延迟出口，LATENCY 样本不丢；采样循环 spawn 仍留第 5 步）
      let server_monitor =
        if (metrics_sampling_frequency > 0 || commandstats_monitor || latency_monitor)
          && let Some(registry) = registry
        {
          Some((
            install_server_monitor(
              metrics_sampling_frequency,
              latency_monitor,
              commandstats_monitor,
            ),
            registry,
          ))
        } else {
          None
        };

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
        server = server.with_tls_config((*tls).clone());
      }

      // 4. 启动网络监听端点（对标 C# GarnetServer.cs:532-533 servers[i].Start()）
      server.start(threads)?;

      // 5. 启动指标监视器采样循环（对标 C# StoreWrapper.cs:823 monitor?.Start()）
      if let Some((monitor, registry)) = server_monitor {
        start_server_monitor(
          monitor,
          server.shutdown_coordinator().clone(),
          registry,
          cluster_provider.clone(),
          monitor_provider,
          metrics_sampling_frequency,
        );
      }

      // 6. 启动集群提供者后台治理协程：已在 compio 运行时内（gossip/刷盘等
      //    后台任务 spawn 依赖当前运行时），时序对标 C# GarnetServer.Start
      //    （libs/host/GarnetServer.cs:527-536：Recover → servers[i].Start →
      //    Provider.Start → 就绪横幅）——本步置于 server.start 拉起 accept
      //    之后、就绪横幅之前，与 C# 的 Provider.Start 位置同序（单机 Noop
      //    零开销内联消除）
      if cluster_provider.is_cluster_enabled() {
        cluster_provider.start();
      }

      // 7. 阻塞监听系统停机信号并执行优雅关机
      let run_res = server.wait_for_shutdown(&banner, quiet).await;
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
  /// TLS 握手超时（生产默认 [`TLS_HANDSHAKE_TIMEOUT`] 10s，经
  /// [`Self::with_tls_handshake_timeout`] 仅此一处装配位可注入短值，
  /// 供集成测试有界收敛等待）
  #[cfg(feature = "tls")]
  tls_handshake_timeout: Duration,
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
    let mut parsed_endpoints: Vec<ServerEndpoint> = Vec::new();
    for e in endpoints {
      parsed_endpoints.extend(ServerEndpoint::parse_many(e)?);
    }
    if parsed_endpoints.is_empty() {
      return Err(Error::AddrParse(EMPTY_ENDPOINTS_ERR.to_string()));
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
      #[cfg(feature = "tls")]
      tls_handshake_timeout: TLS_HANDSHAKE_TIMEOUT,
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

  /// 设置 TLS 握手超时（Slowloris 慢速握手防护铁边；生产缺省
  /// [`TLS_HANDSHAKE_TIMEOUT`] 10s，本装配位仅供集成测试注入短超时
  /// 有界收敛验证，ServerBootstrap 标准启动路径不覆盖、恒取缺省值）
  #[cfg(feature = "tls")]
  pub fn with_tls_handshake_timeout(mut self, timeout: Duration) -> Self {
    self.tls_handshake_timeout = timeout;
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
  /// 资源收口语义，不含 AOF 尾刷（边界同 [`Self::stop`] 文档）；落盘停机
  /// 走 [`Self::wait_for_shutdown`] 或 [`Self::dispose_async`]
  ///
  /// libs/host/GarnetServer.cs:Dispose
  #[inline]
  pub fn dispose(&self) {
    self.stop();
  }

  /// 获取会话提供者引用
  #[inline]
  pub fn session_provider(&self) -> &Arc<P> {
    &self.session_provider
  }

  /// 异步关停服务器并执行 AOF 刷盘收口（停机尾唯一收口单点）
  ///
  /// 先执行 [`Self::stop`] 关停网络监听、排空活跃连接并释放运行时资源，
  /// 若存在 AOF 则执行 [`GarnetAppendOnlyFile::dispose_async`] 确保环形缓冲区未提交帧全部落盘。
  ///
  /// libs/host/GarnetServer.cs:Dispose / InternalDispose
  pub async fn dispose_async(&self) -> io::Result<()> {
    self.stop();
    if let Some(aof) = self.session_provider.aof() {
      aof.dispose_async().await;
    }
    Ok(())
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

  /// worker 线程 spawn 前共享句柄单点快照（三接入线程闭包逐字段捕获清单的
  /// 唯一装配位；克隆次序与本方法字段序一致；UDS 侧绑定路径与权限位为循环
  /// 独参，不入本快照）
  fn capture(&self) -> AcceptContext<P> {
    AcceptContext {
      coordinator: self.shutdown_coordinator.clone(),
      id_gen: Arc::clone(&self.session_id_counter),
      provider: Arc::clone(&self.session_provider),
      pool: Arc::clone(&self.buffer_pool),
      throttle_max: self.network_send_throttle_max,
      conn_limit: self.network_connection_limit,
      #[cfg(feature = "tls")]
      tls_config: self.tls_config.clone(),
      #[cfg(feature = "tls")]
      tls_handshake_timeout: self.tls_handshake_timeout,
    }
  }

  /// start 失败臂回收单点：停 coordinator → 逐个有界 join 本端点已 spawn 的
  /// worker（次序与原各臂一致：stop 先于 join，杜绝幽灵 worker 存活；join
  /// 走 [`join_workers_bounded`] 防御档，失败收口臂绝不因滞留 worker 挂死）
  fn reclaim_workers(&self, mut handles: Vec<JoinHandle<()>>) {
    self.shutdown_coordinator.stop();
    join_workers_bounded(&mut handles);
  }

  /// 启动服务器（Thread-per-Core 多核 SO_REUSEPORT 并发监听）
  ///
  /// 返回 `io::Result` 维持本域（监听绑定/套接字失败的天然通道，对标 C#
  /// SocketException 同域外抛，不并入节点错误枚举）
  ///
  /// libs/host/GarnetServer.cs:Start
  /// libs/host/GarnetServer.cs:InitializeServer
  pub fn start(&self, worker_threads: Option<NonZeroUsize>) -> io::Result<()> {
    // TLS 证书定时刷新循环挂表单点（对标 C# GarnetTlsOptions 构造 selector
    // 即 Timer 挂表；rust 构造/start 皆可发生在 compio 运行时域外——
    // run_async 在室内驱动 start，wnode_test::start_server 等 harness 则在
    // 室外（本注释旧文「start 全程在调用方运行时域内执行」经现有 harness
    // 证否已校正）。室外无 executor 可挂，必须 warn 留痕，禁静默失效）
    #[cfg(feature = "tls")]
    if let Some(tls) = &self.tls_config
      && !tls.ensure_refresh_loop()
    {
      log::warn!("证书刷新循环未挂载（调用点无 compio 运行时域），刷新将滞后至下次承接臂");
    }
    let nthreads = worker_threads
      .unwrap_or_else(|| available_parallelism().unwrap_or(const { NonZeroUsize::new(1).unwrap() }))
      .get();

    for endpoint in &self.endpoints {
      let res = match endpoint {
        ServerEndpoint::Tcp(addr) => self.start_tcp_workers(*addr, nthreads),
        ServerEndpoint::Unix(_path) => {
          #[cfg(unix)]
          {
            self.start_unix_worker(_path, self.unix_socket_perm)
          }
          #[cfg(not(unix))]
          {
            Err(io::Error::new(
              io::ErrorKind::Unsupported,
              "Unix domain socket not supported",
            ))
          }
        }
      };

      if let Err(e) = res {
        // 失败臂：对之前已成功启动的端点和 workers 进行全部清理回收（确保返回 Err 时零幽灵监听）
        self.reclaim_workers(self.worker_threads.lock().drain(..).collect());
        self.tcp_addrs.lock().clear();
        return Err(e);
      }
    }

    Ok(())
  }

  /// 启动多核 TCP Worker 线程（基于 SO_REUSEPORT 端口复用）
  fn start_tcp_workers(&self, target_addr: SocketAddr, nthreads: usize) -> io::Result<()> {
    let mut handles = Vec::with_capacity(nthreads);

    // Worker 0: 负责首发绑定（特别针对动态端口 :0）并广播实际地址
    let cap = self.capture();
    let (handle, ready_rx) = spawn_accept_worker(
      "wnode-worker-0".into(),
      move || async move {
        let listener = bind_reuseport(target_addr).await?;
        let actual_addr = listener.local_addr()?;
        Ok((listener, actual_addr))
      },
      move |listener| run_tcp_accept_loop(0, listener, cap),
    )?;

    handles.push(handle);

    // 等待 Worker 0 绑定成功
    let actual_addr = match recv_ready(ready_rx, "Worker-0 初始化失败") {
      Ok(addr) => addr,
      Err(e) => {
        self.reclaim_workers(handles);
        return Err(e);
      }
    };

    self.tcp_addrs.lock().push(actual_addr);

    info!(
      "启动 TCP 多核网络监听: {} ({} 核心 SO_REUSEPORT)",
      actual_addr, nthreads
    );

    // 启动其余 Worker 核心 (1..nthreads)
    for core_id in 1..nthreads {
      let cap = self.capture();
      let (handle, ready_rx) = match spawn_accept_worker(
        format!("wnode-worker-{core_id}"),
        move || async move { Ok((bind_reuseport(actual_addr).await?, ())) },
        move |listener| run_tcp_accept_loop(core_id, listener, cap),
      ) {
        Ok(pair) => pair,
        Err(e) => {
          self.tcp_addrs.lock().retain(|a| *a != actual_addr);
          self.reclaim_workers(handles);
          return Err(e);
        }
      };

      handles.push(handle);

      if let Err(e) = recv_ready(ready_rx, "Worker 初始化失败") {
        self.tcp_addrs.lock().retain(|a| *a != actual_addr);
        self.reclaim_workers(handles);
        return Err(e);
      }
    }

    self.worker_threads.lock().extend(handles);
    Ok(())
  }

  /// 启动 Unix 域套接字监听 Worker
  #[cfg(unix)]
  fn start_unix_worker(&self, path: &Path, perm: Option<u32>) -> io::Result<()> {
    // 绑定路径与循环本端点回退路径各持一份（同值，分别迁入绑定前奏与接入循环）
    let bind_path = path.to_path_buf();
    let loop_path = path.to_path_buf();
    let ctx = self.capture();
    let (handle, ready_rx) = spawn_accept_worker(
      "wnode-uds".into(),
      move || async move {
        let bound = UdsGuard::bind(&bind_path, perm).await?;
        info!("启动 Unix 域套接字监听: {}", bind_path.display());
        Ok((bound, ()))
      },
      move |(listener, sock_guard)| run_uds_accept_loop(ctx, loop_path, listener, sock_guard),
    )?;

    if let Err(e) = recv_ready(ready_rx, "UDS Worker 初始化失败") {
      self.reclaim_workers(vec![handle]);
      return Err(e);
    }

    self.worker_threads.lock().push(handle);
    Ok(())
  }

  /// 优雅关停服务器（资源与网络收口）
  ///
  /// **语义边界声明**：`stop` 与 [`Drop`] 仅为连接与内存等资源收口，**不包含 AOF 尾部刷盘**。
  /// 嵌入宿主若需保证未提交帧安全落盘停机，必须调用 [`Self::dispose_async`] 或 [`Self::wait_for_shutdown`]。
  ///
  /// 三阶段时序对标 C# InternalDispose（servers[i].Close 先于一切通道收敛，
  /// subscribeBroker.Dispose 排在 Provider.Dispose 即排空之后）：coordinator.stop
  /// 停监听阻断新连接（Phase 1，必须最前置——收敛窗口内新连接仍可涌入提交
  /// VADD/VREM 而清理通道已闭，写任务静默丢弃成孤儿索引）→ AOF 背压闸门
  /// 放行（滞留追加方出口，置位幂等）→ 向量清理协程收敛（消费协程栖 worker
  /// 运行时，硬约束仅先于 join）→ 集合项经纪收口（解除全部阻塞等待者，
  /// C# itemBroker?.Dispose() 对位；主循环跑在 worker 运行时，紧随其后的
  /// join 即排空屏障）→ 有界 join worker 线程（各线程运行时内已完成活跃
  /// 连接排空，Phase 2）→ Lua 看门狗收口 → 范围索引收口（Provider.Dispose
  /// 级联尾项）→ pubsub 中枢收口（C# subscribeBroker?.Dispose() 排在
  /// Provider.Dispose 之后，即 join 排空后；rust 无常驻消费任务，置 disposed
  /// 与清订阅表为同步单步）→ 释放缓冲池；join 返回即排空结束，上层的集群
  /// dispose 与配置刷盘（Phase 3）在其后执行
  ///
  /// 闸门放行必须先于 join：同步 wait_slow 阻塞的是 worker 线程本体（poll 内
  /// 阻塞，连接任务强杀不可打断），滞留追加方若不在排空窗口前放行将吃满
  /// join 防御档上界（[`WORKER_JOIN_TIMEOUT_MS`]，deviations §152）；C# 靠 WaitSlow
  /// 轮询 disposed 与 DrainActiveHandlers 超时兜底，无此前置约束
  ///
  /// AOF 语义边界：本方法止步于背压闸门放行（Phase 1/2 资源收口），不含
  /// AOF 尾刷——同步上下文无法直驱 async 设备 IO，环形缓冲未提交帧不入盘。
  /// 落盘停机（C# InternalDispose Phase 3 的 AppendOnlyFile.Dispose 对位）
  /// 须走 [`Self::wait_for_shutdown`] 或 [`Self::dispose_async`]，二者尾部
  /// 共用同一收口单点；裸 [`Self::drop`] 同此边界
  ///
  /// libs/host/GarnetServer.cs:InternalDispose
  pub fn stop(&self) {
    // Phase 1（C# InternalDispose 首步 servers[i].Close() 关监听端口阻断内核
    // backlog 新连接的对位）：协调器广播终止三接入循环，停机取消桥打断在途
    // accept 并 drop 监听套接字——必须先于一切通道收敛，否则收敛窗口内新连接
    // 仍可涌入提交 VADD/VREM，而清理通道已闭致写任务丢弃、孤儿索引泄漏
    self.shutdown_coordinator.stop();
    // AOF 背压闸门关停放行（C# AofBackpressure.Dispose：Release all stalled
    // appenders permanently (server shutdown)；无 AOF 形态经 trait 默认
    // None 跳过）
    if let Some(aof) = self.session_provider.aof()
      && let Some(bp) = aof.backpressure()
    {
      bp.dispose();
    }
    // 向量清理协程收敛（C# Provider.Dispose 级联 VectorManager.Dispose 逐通道
    // CompleteAndWaitForConsumerTask）：硬约束仅先于 join——消费协程栖各 worker
    // 线程 compio 运行时（thread-per-core，独立），主线程此处阻塞等待期间
    // worker 运行时经 dispose_active_handlers 排空臂持续存活并驱动协程消费
    // 退出；新连接已被上方 Phase 1 阻断，收敛期无在途写任务可丢
    if !self.session_provider.dispose_vector_cleanup() {
      log::warn!("向量清理协程未在超时内全部收敛，停机继续");
    }
    // 集合项经纪收口（C# StoreWrapper.Dispose 的 `itemBroker?.Dispose()`，
    // 本函数上方注释枚举 C# 序列所点名的一步）：置取消、解除全部等待观察者
    // （客户端收最终空应答，不依赖 terminate 广播兜底弃答）、投事件唤醒
    // 经纪主循环退出——主循环跑在 worker 运行时，紧随其后的 join 即天然
    // 排空屏障（对应 C# done.Wait() 的排空语义），避免经纪主循环任务与
    // 取件源会话（独立纪元参与者）随 worker 硬杀消亡于 enter_batch 纪元
    // 临界段内
    self.session_provider.dispose_item_broker();
    // Phase 2 收口：有界 join（案三防御档，deviations §152——std join 无超时
    // 不可打断，worker 线程内部同步死锁属 §84 排空护栏未覆的残余挂死面，
    // 到期告警强推后续收尾，绝不上抛挂死停机链）
    let mut handles = self.worker_threads.lock();
    join_workers_bounded(&mut handles);
    // Lua 超时看门狗停机收口（C# StoreWrapper.Dispose 的
    // `luaTimeoutManager?.Dispose()`：置停机位唤醒专属线程并 Join，时序在
    // rangeIndexManager.Dispose 之前——rust join 返回即连接排空完成，在途
    // 脚本已收敛，此刻收口不再需要看门狗；排空前收口会让 drain 中的死循环
    // 脚本失去抢占兜底）。看门狗为独立 OS 线程、不经 worker 运行时驱动，
    // 与 join 屏障无次序耦合；未装配超时形态默认口空操作
    self.session_provider.dispose_lua_timeout();
    // 范围索引停机收口（C# StoreWrapper.Dispose 的
    // rangeIndexManager?.Dispose()，Provider.Dispose 段第七步：时序在
    // clusterProvider/itemBroker/taskManager 之后、databaseManager 之前——
    // 对位即 Phase 2 连接排空（join）之后、引擎 store 兜底析构之前）。
    // 显式步落地后，嵌入式/复用进程形态 stop() 返回即释放全部在线树与
    // native 页缓存，不再依赖各 Arc 引用恰好归零；引擎侧幂等，与
    // WedbStore::drop 兜底并存安全。早于 join 则在途命令仍可触达在建树，
    // 故不可前移；未装配引擎经 trait 默认跳过
    self.session_provider.dispose_range_index();
    // pubsub 中枢收口（C# InternalDispose 的 subscribeBroker?.Dispose() 排在
    // Provider.Dispose 即排空之后，本次对标后移至 join 后）：rust 无 C# 的
    // TsavoriteLog 介质与后台消费循环（SubscribeBroker::dispose 即置 disposed
    // 并清三订阅表，同步单步、无 done.WaitOne 等待面），排空后统一释放，
    // 杜绝旧时序在 worker 排空臂注销订阅/写出在途 PUBLISH 前抢先清表；
    // 未装配 pubsub（--disable-pubsub）经 trait 默认跳过
    self.session_provider.dispose_pubsub();
    self.buffer_pool.purge();
  }

  /// 等待系统停机信号或协调器停机并优雅关机
  ///
  /// `quiet`（C# GarnetServer.cs:535 `if (!opts.QuietMode)`，选项源头
  /// Options.cs:363-364 QuietMode）置位时静默监听就绪横幅文本
  pub async fn wait_for_shutdown(&self, server_label: &str, quiet: bool) -> crate::Result<()> {
    #[cfg(feature = "tls")]
    let tls_info = if self.tls_config.is_some() {
      " (TLS 启用)"
    } else {
      ""
    };
    #[cfg(not(feature = "tls"))]
    let tls_info = "";
    if !quiet {
      log::info!(
        "{server_label} 网络监听就绪: {:?}{tls_info}",
        self.local_addrs()
      );
    }

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
    // 停机尾唯一收口单点（stop 排空 + AOF 尾刷合一）；C# InternalDispose
    // Phase 3 Provider.Dispose → AppendOnlyFile.Dispose 的对位动作经此落位
    self.dispose_async().await?;
    log::info!("{server_label} 安全退出完成");
    Ok(())
  }
}

/// 析构守卫
///
/// **语义边界声明**：Drop 仅同步调用 [`GarnetServer::stop`] 完成网络连接与内存资源收口，
/// **不执行 AOF 异步刷盘**（异步设备 IO 无法在同步 Drop 内安全阻塞驱动）。
/// 如需保证 AOF 尾部帧落盘，停机须显式调用 [`GarnetServer::dispose_async`] 或 [`GarnetServer::wait_for_shutdown`]。
impl<P: SessionProviderFace + 'static> Drop for GarnetServer<P> {
  /// 资源收口（转调 [`GarnetServer::stop`]）＝网络与线程域排空，不含 AOF
  /// 尾刷：环形缓冲未提交帧随停机弃置（async 设备 IO 进不了同步 Drop，
  /// compio poll 栈内严禁 block_on 兜底）。落盘停机须显式走
  /// [`GarnetServer::wait_for_shutdown`] 或 [`GarnetServer::dispose_async`]，
  /// 此处文档即契约，无隐式兜底
  fn drop(&mut self) {
    self.stop();
  }
}

/// 启动参数 TLS 证书对 → 服务端 TLS 配置投影（会话提供者装配链的唯一 TLS
/// 入口，对标 C# Options.cs:948-957 EnableTLS 时一处构造 GarnetTlsOptions，
/// 网络端点（GarnetServer.cs:294 直读 opts.TlsOptions）与会话域共享同一实例）
///
/// 证书与私钥必须成对：单侧配置在 C# 由 GarnetTlsOptions 构造期拒
/// （CertFileName 空即抛），rust 同判据启动期拒绝，杜绝静默降级明文。
///
/// 入站客户端认证旋钮一并过同一投影（对标 C# GarnetServerTcp.cs:290
/// handler.Start(tlsOptions?.TlsServerOptions) 携带的
/// ClientCertificateRequired + IssuerCertificatePath 两面）；
/// tls_cert_refresh_freq 注入定时刷新旋钮（Options.cs:330
/// CertificateRefreshFrequency）
#[cfg(feature = "tls")]
pub(crate) fn tls_config_from_node(node: &NodeArgs) -> crate::Result<Option<ServerTlsConfig>> {
  match (&node.tls_cert, &node.tls_key) {
    (Some(cert), Some(key)) => Ok(Some(ServerTlsConfig::from_pem_files(
      cert,
      key,
      node.tls_client_cert_required,
      node.tls_issuer_cert.as_deref(),
      node.tls_cert_refresh_freq,
    )?)),
    (Some(_), None) | (None, Some(_)) => Err(Error::InvalidArgument(
      "tls_cert 与 tls_key 必须同时提供".into(),
    )),
    (None, None) => Ok(None),
  }
}

/// 未启用 TLS 特性时的启动门禁：检测到任何 TLS 相关配置参数即显式报错拒绝启动，严禁静默降级为明文
#[cfg(not(feature = "tls"))]
fn guard_no_tls(node: &NodeArgs) -> crate::Result<()> {
  if node.has_tls() {
    return Err(Error::InvalidArgument(
      "未启用 tls 特性，但检测到 TLS 配置参数 (--tls-cert / --tls-key / --tls-issuer-cert / --tls-client-target-host)，拒绝启动以防止静默降级为明文".into(),
    ));
  }
  Ok(())
}
/// 监视器进程级安装（构造 + install_global；对标 C# StoreWrapper.cs:226
/// monitor 随 StoreWrapper 构造：早于 GarnetServer.Start 的 servers[i].Start，
/// 任何连接建立时 RespServerSession 即可取得 monitor_iterations 时钟与
/// globalLatencyMetrics 出口——装配点若晚于网络监听，窗口期连接拿零值时钟
/// 与空延迟出口，LATENCY 样本静默丢失）。构造与全局槽安装不依赖 compio
/// 运行时；latency_monitor / commandstats_monitor 决定聚合成员是否就位
/// （C# GarnetServerMonitor.cs:64 构造参数）。
///
/// **单实例进程契约**：进程级监视器槽全局唯一（首装即赢）。若同一进程重复启动多个实例，
/// 后装实例将无法注册其监视器，必须记录 warn 日志留痕。
fn install_server_monitor(
  frequency_secs: u64,
  latency_monitor: bool,
  commandstats_monitor: bool,
) -> Arc<GarnetServerMonitor> {
  let monitor = Arc::new(GarnetServerMonitor::new(
    frequency_secs,
    true,
    latency_monitor,
    commandstats_monitor,
  ));
  // 首装即赢（OnceLock 槽）：同进程第二实例安装被拒必须留痕禁静默——
  // 后装实例的会话 dispose 归并与 INFO/LATENCY 读路径将全部落到首装实例，
  // 指标面跨实例串数据（单实例进程契约声明见 GLOBAL_MONITOR 槽文档）
  if !monitor.install_global() {
    warn!("同进程第二监视器安装被拒（首装即赢，单实例进程契约）：指标面归并首装实例");
  }
  monitor
}

/// 监督快照里的指标监视采样任务名（[`start_server_monitor`] 的监督归组名，
/// 随 INFO bg_task_health 出；对标 C# GarnetServerMonitor.cs:MainMonitorTaskAsync
/// catch LogCritical + finally done.Set() 的死亡留痕契约）
const MONITOR_TASK: &str = "server_monitor";

/// 启动服务器指标监视器采样循环
///
/// libs/server/Metrics/GarnetServerMonitor.cs:Start（宿主启动序列
/// StoreWrapper.Start() → monitor?.Start()，频率 > 0 才拉起后台采样任务）。
/// 监视器本体已由 [`install_server_monitor`] 进程级安装（dispose 归并直取），
/// 停机协调器充当 C# CancellationToken——stop 即取消采样循环。
///
/// 供装配期宿主调用；对本 crate 集成测试开放拉起入口（监督接线验证，与
/// service.rs `spawn_aof_size_limit_task` 同形态）。
///
/// INFO RESETSTAT 的六件事由本装配点凑齐：会话/连接/命令统计/延迟四臂的
/// 回调在 [`ConsumerRegistry::monitor_iteration_inputs`] 内构造，gossip 与
/// 复活化两臂的句柄（C# `storeWrapper.clusterProvider` 与 `storeWrapper`
/// 本身）在此注入——单机形态的集群句柄即 NoopClusterProvider，其
/// `reset_gossip_stats` 默认空操作正是 C# clusterProvider 为 null 的对位
pub fn start_server_monitor<C, P>(
  monitor: Arc<GarnetServerMonitor>,
  coordinator: ShutdownCoordinator,
  registry: Arc<ConsumerRegistry>,
  cluster_provider: C,
  session_provider: Arc<P>,
  frequency_secs: u64,
) where
  C: ClusterProvider + Clone + 'static,
  P: SessionProviderFace + 'static,
{
  // C# GarnetServerMonitor.cs:Start：周期采样任务仅在配置了采样频率时
  // 拉起（监视器可仅为命令统计历史装配，无周期采样；dispose 归并不依赖
  // 采样循环）
  if frequency_secs > 0 {
    let monitor = Arc::clone(&monitor);
    let gossip_handle = cluster_provider;
    let reviv_handle = session_provider;
    // 任务体经 wbase [`supervise_task`] 单点顶层监督（沿同族 primary_tasks.rs
    // 现成形态）：panic 落 log::error（带任务名）并计监督快照 panics（随 INFO
    // bg_task_health 出），死亡与空闲不再不可区分；Err 臂无需复位启动位——
    // start_server_monitor 为装配期一次性拉起（对标 C# MainMonitorTaskAsync
    // catch LogCritical 仅留痕语义），不加自动重拉（防毒丸风暴）
    spawn(async move {
      let _ = supervise_task(
        MONITOR_TASK,
        monitor.main_monitor_task_async(
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
        ),
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

/// TLS 握手超时上限（生产默认，Slowloris 慢速握手防护铁边）：对端完成 TCP
/// 三次握手后不发或极慢分片发送 ClientHello 时，握手 Future 将无限期
/// Pending——无确定收敛边界则在途连接守卫与套接字被永久占死，少量半开连接
/// 即可耗尽 network-connection-limit 配额造成拒绝服务。超时即熔断：连接任务
/// return → _in_flight 守卫 Drop 回落配额 → handler Drop 注销登记 → 握手
/// Future Drop 释放半开套接字（compio Submit 在 Drop 时向驱动撤销在途操作）。
/// C# 原型无对物（GarnetServerTcp.cs:237-308 与 NetworkHandler.cs:147-196
/// 握手无超时），此为工业级 TLS 服务契约的确定性收口补强
#[cfg(feature = "tls")]
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// accept 资源压力判定（对标 C# 档内 SocketError 在各平台的对应物）：
/// ENOMEM 用 std 稳定归一的 `OutOfMemory` kind；EMFILE/ENFILE/ENOBUFS 因 std 未将
/// 其归一为稳定 kind，以 cfg 编译期 raw errno 集合补判——热路径零字符串比较、零平台运行时分支
///
/// Linux: EMFILE=24, ENFILE=23, ENOBUFS=105
/// macOS/BSD: EMFILE=24, ENFILE=23, ENOBUFS=55
/// Windows: WSAEMFILE=10024, WSAENOBUFS=10055（对标 C# TooManyOpenSockets / NoBufferSpaceAvailable）
#[cfg(target_os = "linux")]
const ACCEPT_RESOURCE_ERRNOS: &[i32] = &[24, 23, 105];
#[cfg(any(
  target_os = "macos",
  target_os = "ios",
  target_os = "freebsd",
  target_os = "openbsd",
  target_os = "netbsd",
  target_os = "dragonfly"
))]
const ACCEPT_RESOURCE_ERRNOS: &[i32] = &[24, 23, 55];
#[cfg(windows)]
const ACCEPT_RESOURCE_ERRNOS: &[i32] = &[10024, 10055];
#[cfg(not(any(
  target_os = "linux",
  target_os = "macos",
  target_os = "ios",
  target_os = "freebsd",
  target_os = "openbsd",
  target_os = "netbsd",
  target_os = "dragonfly",
  windows
)))]
const ACCEPT_RESOURCE_ERRNOS: &[i32] = &[];

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

/// accept 成功接入判定结果：通过（携在途守卫；无注册表的哑桩形态为 None，
/// 语义同 limit=-1 不设门）/ 超限拒绝（调用方 continue，drop stream 关闭新连接）
enum Admission {
  Passed(Option<ConnectionGuard>),
  Rejected,
}

/// accept 成功后的接入计量与在途容量门收口（TCP/UDS 双循环共用；C#
/// GarnetServerTcp.cs:236-241 容量门 accept 成功即刻计量、:302-307 超限臂
/// 即刻关闭新连接且不写任何 RESP 应答）：received 计数前移（容量门前——
/// TLS 握手失败、容量门拒绝、发字即断的短命连接全部进统计；C# :288 计数点
/// 在 TryAdd 后 rust 前移一档，r14-conn.md:11「容量门拒绝除外」括注不采纳，
/// 维持现计数，裁决归属 deviations §101）→ 容量门；超限臂 disposed 配对
/// 递增（rust received 前移后的配对点，C# 超限不计 received 故无此步）并
/// debug 留痕。守卫由调用方 move 进连接任务随其结束（正常收尾/TLS 握手
/// 失败）Drop 归零
fn admit_connection(
  registry: &Option<Arc<ConsumerRegistry>>,
  conn_limit: i64,
  target: impl fmt::Display,
) -> Admission {
  let Some(registry) = registry.as_ref() else {
    return Admission::Passed(None);
  };
  registry.note_connection_received();
  match registry.try_acquire_connection(conn_limit) {
    Some(guard) => Admission::Passed(Some(guard)),
    None => {
      registry.note_connection_disposed();
      debug!("{target} 在途连接达上限 {conn_limit}，关闭新连接");
      Admission::Rejected
    }
  }
}

/// accept 成功后的预注册收口（TCP/UDS 两循环共用；C# HandleNewConnection
/// 的 GarnetServerTcp.cs:256 `activeHandlers.TryAdd` 即刻注册、先于 :290
/// `handler.Start`）：条目即刻入 entries——TLS 握手与首字节读取前连接即
/// 处于 CLIENT LIST/KILL 治理面（view 动态字段由会话建立后每批镜像重导
/// 补全），终止哨兵同点挂接，握手与泵读共用同一取消令牌。返回该令牌供
/// TLS 握手臂复用（非 TLS 构建无握手位，令牌仅驻 handler 域）
fn preregister_consumer<C: MessageConsumerFace>(
  registry: &Option<Arc<ConsumerRegistry>>,
  handler: &mut NetworkHandler<C>,
  id: i64,
  remote_endpoint: String,
  local_endpoint: String,
) -> Option<CancelToken> {
  let registry = registry.as_ref()?;
  let entry = registry.register(id, remote_endpoint, local_endpoint);
  let token = CancelToken::new();
  spawn_kill_watcher(Arc::clone(&entry), token.clone());
  handler.attach_consumer(Arc::clone(registry), entry, token.clone());
  Some(token)
}

/// worker 线程 join 有界上界毫秒（P3 防御档，deviations §152 在册）：停机
/// 排空护栏（consumer_registry `DRAIN_TIMEOUT_MS`=5s）到期强收后，同线程
/// Runtime 析构兜底取消残余任务的量级余量。挂死向量绝大多数已由 §84
/// 护栏有界，本档仅覆盖「worker 线程内部同步死锁」残余面
const WORKER_JOIN_TIMEOUT_MS: u64 = 15_000;

/// 单个 worker 线程有界 join（std [`JoinHandle::join`] 无超时、不可打断的
/// 防御承接）：join 移交中介线程，通道 recv_timeout 有界收口。返回
/// `Some(true)`=正常退出、`Some(false)`=worker panic 退出、`None`=到期未收敛
/// （疑线程内死锁，句柄随中介线程迁出——进程收口已脱离挂死的防御取舍）。
///
/// libs/server/Servers/GarnetServerBase.cs:DisposeActiveHandlers（C# 侧
/// 5s 滞留诊断留痕形的 join 面延伸；rust 到期 error 留痕并强行推进）
pub fn join_worker_bounded(handle: JoinHandle<()>, timeout: Duration) -> Option<bool> {
  let (tx, rx) = channel();
  thread::spawn(move || {
    let _ = tx.send(handle.join().is_ok());
  });
  rx.recv_timeout(timeout).ok()
}

/// worker 线程组逐个有界 join（[`GarnetServer::stop`] 与
/// [`GarnetServer::reclaim_workers`] 共用收口单点）：超时/panic 留痕后强推
/// 停机后续步骤（AOF 刷盘收尾、锁守护释放不被滞留 worker 绑架）
fn join_workers_bounded(handles: &mut Vec<JoinHandle<()>>) {
  for (idx, handle) in handles.drain(..).enumerate() {
    match join_worker_bounded(handle, Duration::from_millis(WORKER_JOIN_TIMEOUT_MS)) {
      Some(true) => {}
      Some(false) => warn!("Worker 线程 {idx} panic 退出（有界 join 收口）"),
      None => {
        log::error!(
          "Worker 线程 {idx} 未在 {}ms 内退出（疑线程内部死锁），强推停机后续收尾，残余交 Runtime 析构兜底（deviations §152）",
          WORKER_JOIN_TIMEOUT_MS
        );
      }
    }
  }
}

/// 接入 worker 线程启动骨架（TCP 首发核 / TCP 其余核 / UDS 三处共用）：
/// 新起 compio 运行时 → 运行时内驱动接入前奏（端点绑定，产出移交接入循环的
/// 资源与投递父线程的就绪值）→ 前奏成功即投递就绪值并把资源交给接入循环，
/// 前奏失败即投递错误后收线程；运行时构造失败按原语义投递
/// `io::Error::other(运行时错误 Display)` 文本
fn spawn_accept_worker<R, T, Pr, PrF, Mt, MtF>(
  name: String,
  prelude: Pr,
  main: Mt,
) -> io::Result<(JoinHandle<()>, RxOneshot<io::Result<T>>)>
where
  T: Send + 'static,
  Pr: FnOnce() -> PrF + Send + 'static,
  PrF: Future<Output = io::Result<(R, T)>>,
  Mt: FnOnce(R) -> MtF + Send + 'static,
  MtF: Future<Output = ()>,
{
  let (ready_tx, ready_rx) = oneshot();
  let handle = ThreadBuilder::new().name(name).spawn(move || {
    let rt = match Runtime::new() {
      Ok(rt) => rt,
      Err(e) => {
        ready_tx.send(Err(io::Error::other(e.to_string())));
        return;
      }
    };

    rt.block_on(async move {
      match prelude().await {
        Ok((res, ready)) => {
          ready_tx.send(Ok(ready));
          main(res).await;
        }
        Err(e) => ready_tx.send(Err(e)),
      }
    });
  })?;
  Ok((handle, ready_rx))
}

/// 接入 worker 就绪回执收取（线程失联即通道断开，兜底文本由调用方按原口径
/// 单点传入，禁各臂二写）
fn recv_ready<T>(ready_rx: RxOneshot<io::Result<T>>, gone: &str) -> io::Result<T> {
  match ready_rx.recv() {
    Ok(res) => res,
    Err(_) => Err(io::Error::other(gone)),
  }
}

/// 接入循环每 worker 上下文：共享句柄组由 [`GarnetServer::capture`] 单点快照
/// （随线程闭包整体迁入），核号、监听套接字与 UDS 绑定路径/权限位均于接入
/// 入口补投——消三处逐字段克隆捕获与逐字段装填样板
struct AcceptContext<P: SessionProviderFace> {
  coordinator: ShutdownCoordinator,
  id_gen: Arc<AtomicU64>,
  provider: Arc<P>,
  pool: Arc<LimitedFixedBufferPool>,
  throttle_max: usize,
  /// 在途连接容量门（-1 = 不限；C# networkConnectionLimit）
  conn_limit: i64,
  #[cfg(feature = "tls")]
  tls_config: Option<ServerTlsConfig>,
  /// TLS 握手超时（生产默认 [`TLS_HANDSHAKE_TIMEOUT`]；集成测试经
  /// [`GarnetServer::with_tls_handshake_timeout`] 注入短值收敛等待）
  #[cfg(feature = "tls")]
  tls_handshake_timeout: Duration,
}

/// 接入循环停机取消桥（TCP/UDS 双循环共用）：协调器停机即刻取消本循环
/// 在途 accept 与退避 sleep，使接入臂于停机信号到达当刻收场
fn spawn_shutdown_bridge(coordinator: &ShutdownCoordinator, cancel_token: &CancelToken) {
  let watcher_cancel = cancel_token.clone();
  let coordinator = coordinator.clone();
  spawn(async move {
    coordinator.wait().await;
    watcher_cancel.cancel();
  })
  .detach();
}

/// 连接任务收尾单点（TCP/UDS 双循环共用）：在途守卫随本任务结束归零 →
/// 驱动连接泵 → 泵错 debug 留痕
///
/// 守卫先于 handler 释放的次序由形参/局部析构次序保证（局部先于形参 drop，
/// C# activeHandlerCount 的 decrement 挂点在 handler dispose），勿改形参次序
async fn serve_connection<P: SessionProviderFace + 'static>(
  target: impl fmt::Display,
  mut handler: NetworkHandler<P::Consumer>,
  stream: ConnectionStream,
  provider: Arc<P>,
  sender_id: u64,
  guard: Option<ConnectionGuard>,
) {
  let _in_flight = guard;
  if let Err(err) = handler.process_stream(stream, provider, sender_id).await {
    log::debug!("{target} 连接处理结束: {err}");
  }
}

/// 驱动单个核心的 TCP 连接接入循环
///
/// 在 garnet 中的相对路径: libs/server/Servers/GarnetServerTcp.cs:HandleNewConnection
///
/// accept 成功回调本体承接：复位退避 → 在途容量门 → socket 配置 → 建 handler
/// 并注册 → 拉起连接泵；容量门子步骤对位见
/// [`ConsumerRegistry::try_acquire_connection`]，accept 失败分档见
/// handle_accept_error（对标 C# HandleAcceptError 一/二/三档）
async fn run_tcp_accept_loop<P: SessionProviderFace + 'static>(
  core_id: usize,
  listener: TcpListener,
  ctx: AcceptContext<P>,
) {
  let cancel_token = CancelToken::new();
  spawn_shutdown_bridge(&ctx.coordinator, &cancel_token);

  // 退避状态协程栈持有（C#:35 acceptBackoffMs，每监听器一份）
  let mut backoff_ms = INITIAL_ACCEPT_BACKOFF_MS;

  while !ctx.coordinator.is_stopped() {
    let accept_res = listener
      .accept()
      .with_cancel(cancel_token.clone())
      .fail_fast()
      .await;

    match accept_res {
      Ok(Ok((stream, client_addr))) => {
        // 成功接入复位退避（C# GarnetServerTcp.cs:234）
        backoff_ms = INITIAL_ACCEPT_BACKOFF_MS;
        let registry = ctx.provider.consumer_registry();
        // received 前移与在途容量门单源收口（C# 对位、计数口径与 deviations
        // §101 裁决归属见 admit_connection 文档）
        let guard =
          match admit_connection(&registry, ctx.conn_limit, format_args!("Worker-{core_id}")) {
            Admission::Passed(guard) => guard,
            Admission::Rejected => continue,
          };
        // 接入侧装配 nodelay + 默认保活（C#:249 仅 NoDelay，保活为 rust 自有面）
        let _ = configure_socket(&stream);
        if ctx.coordinator.is_stopped() {
          break;
        }
        let sender_id = ctx.id_gen.fetch_add(1, Ordering::Relaxed);
        // 本地端点构造期捕获（C# TcpNetworkHandlerBase 构造期捕获
        // socket.LocalEndPoint 的 localEndpointName 对标；TLS 臂流型擦除
        // 不透出内层 TCP，取值必须前移到握手前）
        let local_endpoint = tcp_local_endpoint(&stream);
        let remote_endpoint = client_addr.to_string();
        // 回环判定 accept 侧当场折叠（C# TcpNetworkHandlerBase.cs:42-44 的
        // IPEndPoint 臂 IPAddress.IsLoopback：typed SocketAddr 直判，含
        // v4-mapped IPv6 解映射，见 wbase::endpoint::ip_is_loopback）——
        // 展示字符串自此仅供 CLIENT INFO/日志，判定面绝不字符串再解析
        let peer_source = PeerSource::Ip {
          loopback: ip_is_loopback(client_addr),
        };
        let mut handler = NetworkHandler::<P::Consumer>::new(
          sender_id,
          remote_endpoint.clone(),
          peer_source,
          local_endpoint.clone(),
          Arc::clone(&ctx.pool),
          ctx.throttle_max,
        );
        // 预注册（C# :256 TryAdd 即刻注册，先于 :290 handler.Start——TLS
        // 握手与首字节读取前条目即入 entries，握手期/不发字节的空闲连接
        // 均可见可 KILL；view 动态字段由会话建立后每批镜像重导补全）。
        // 返回令牌供 TLS 握手臂复用（非 TLS 构建无握手位，令牌仅驻 handler，
        // 下划线绑定兼作两 feature 形态的零告警单点调用）
        let _kill_token = preregister_consumer(
          &registry,
          &mut handler,
          sender_id as i64,
          remote_endpoint,
          local_endpoint,
        );
        let provider_clone = Arc::clone(&ctx.provider);

        #[cfg(feature = "tls")]
        let tls_acceptor = ctx.tls_config.as_ref().map(|c| c.acceptor().clone());
        #[cfg(feature = "tls")]
        let tls_handshake_timeout = ctx.tls_handshake_timeout;

        spawn(async move {
          #[cfg(feature = "tls")]
          let connection_stream = if let Some(acceptor) = tls_acceptor {
            // TLS 握手挂 KILL/停机令牌与确定性超时双边界（预注册条目治理面
            // 延伸到握手期；Slowloris 慢速握手防护，见 TLS_HANDSHAKE_TIMEOUT）。
            // timeout 置于 with_cancel 内层：令牌触发时 fail_fast 即刻收场且
            // 驱动级撤销握手与睡眠两枚在途操作；超时触发时握手 Future 随
            // timeout 落值被 Drop，compio Submit 的 Drop 臂向驱动撤销挂起读。
            // 两条早退臂同走 RAII 收口：在途守卫先 Drop 回落容量、handler
            // Drop → dispose → 注销配对（次序同 serve_connection 收尾臂），
            // 半开套接字随 Drop 释放
            let handshake = time::timeout(tls_handshake_timeout, acceptor.accept(stream));
            let shaken = match &_kill_token {
              Some(token) => handshake.with_cancel(token.clone()).fail_fast().await,
              None => Ok(handshake.await),
            };
            match shaken {
              // 三层落值：with_cancel → timeout → 握手本体
              Ok(Ok(Ok(tls_stream))) => ConnectionStream::tls(tls_stream),
              Ok(Ok(Err(err))) => {
                log::debug!("Worker-{core_id} TLS 握手失败: {err}");
                drop(guard);
                return;
              }
              Ok(Err(_elapsed)) => {
                log::debug!(
                  "Worker-{core_id} TLS 握手超时 ({tls_handshake_timeout:?})，关闭半开连接"
                );
                drop(guard);
                return;
              }
              Err(Cancelled) => {
                drop(guard);
                return;
              }
            }
          } else {
            ConnectionStream::Tcp(stream)
          };

          #[cfg(not(feature = "tls"))]
          let connection_stream = ConnectionStream::Tcp(stream);

          serve_connection(
            format_args!("Worker-{core_id}"),
            handler,
            connection_stream,
            provider_clone,
            sender_id,
            guard,
          )
          .await;
        })
        .detach();
      }
      Ok(Err(e)) => {
        // accept 失败分档（对标 C# HandleAcceptError 的一/二/三档）
        if !handle_accept_error(
          format_args!("Worker-{core_id}"),
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
        debug!("Worker-{core_id} 收到停机取消信号，退出接入循环");
        break;
      }
    }
  }

  // 停机 Phase 1：即刻关闭监听套接字，阻断新连接进内核 backlog
  // （对标 C# GarnetServer.cs:InternalDispose Phase 1 → GarnetServerTcp.Close()
  //  释放 listenSocket 端口；排空窗口内 TCP 握手不再完成，客户端 connect 立即失败）
  drop(listener);

  // 停机 Phase 2：本线程运行时析构前排空活跃连接（C#
  // GarnetServerBase.DisposeActiveHandlers），detach 连接任务须在本
  // block_on 尾部驱动至归零，否则随 Runtime 析构被截断
  if let Some(registry) = ctx.provider.consumer_registry() {
    registry.dispose_active_handlers().await;
  }
}

/// 驱动 Unix 域套接字连接接入循环
///
/// 与 TCP 循环共用 [`admit_connection`] 接入计量、[`handle_accept_error`]
/// 失败分档、[`spawn_shutdown_bridge`] 停机取消桥与 [`serve_connection`]
/// 连接收尾；端点对取值与恒本地来源、监听套接字与套接字文件守卫的停机
/// Phase 1 关闭为本循环独有（UDS 无 TLS 握手位）
#[cfg(unix)]
async fn run_uds_accept_loop<P: SessionProviderFace + 'static>(
  ctx: AcceptContext<P>,
  path_buf: PathBuf,
  listener: UnixListener,
  sock_guard: UdsGuard,
) {
  let cancel_token = CancelToken::new();
  spawn_shutdown_bridge(&ctx.coordinator, &cancel_token);

  let mut backoff_ms = INITIAL_ACCEPT_BACKOFF_MS;
  while !ctx.coordinator.is_stopped() {
    let accept_res = listener
      .accept()
      .with_cancel(cancel_token.clone())
      .fail_fast()
      .await;

    match accept_res {
      Ok(Ok((stream, _))) => {
        backoff_ms = INITIAL_ACCEPT_BACKOFF_MS;
        let registry = ctx.provider.consumer_registry();
        // 接入计量与在途容量门与 TCP 循环同一单源收口（见 admit_connection 文档）
        let guard = match admit_connection(&registry, ctx.conn_limit, "UDS") {
          Admission::Passed(guard) => guard,
          Admission::Rejected => continue,
        };
        if ctx.coordinator.is_stopped() {
          break;
        }
        let sender_id = ctx.id_gen.fetch_add(1, Ordering::Relaxed);
        // 端点对 accept 侧捕获：
        // 对端 remote_endpoint 经 getpeername 取 as_pathname，未命名对端（如客户端直连）
        // 为空串，对标 C# TcpNetworkHandlerBase.cs remoteEndpoint?.ToString() ?? string.Empty
        // （.NET UnixDomainSocketEndPoint 对未命名对端 ToString 为空串）；
        // 本端 local_endpoint 为监听绑定路径（getsockname 取 as_pathname，失败回退到 path_buf）。
        let remote_endpoint = stream
          .peer_addr()
          .ok()
          .and_then(|addr| addr.as_pathname().map(|p| p.display().to_string()))
          .unwrap_or_default();
        // 来源类型折叠：UDS accept 臂本即知道连接类型（C# 对端为
        // UnixDomainSocketEndPoint 对象即恒本地，未命名对端空串展示
        // 不影响判定），TcpNetworkHandlerBase.cs:42-44 类型臂承接
        let peer_source = PeerSource::Unix;
        let local_endpoint = stream
          .local_addr()
          .ok()
          .and_then(|addr| addr.as_pathname().map(|p| p.display().to_string()))
          .unwrap_or_else(|| path_buf.display().to_string());
        let mut handler = NetworkHandler::new(
          sender_id,
          remote_endpoint.clone(),
          peer_source,
          local_endpoint.clone(),
          Arc::clone(&ctx.pool),
          ctx.throttle_max,
        );
        // 预注册 + 终止哨兵（TCP 循环同一收口：C# :256 TryAdd 先于
        // handler.Start，条目即刻入治理面；UDS 无 TLS 握手位）
        let _ = preregister_consumer(
          &registry,
          &mut handler,
          sender_id as i64,
          remote_endpoint,
          local_endpoint,
        );
        spawn(serve_connection(
          "UDS",
          handler,
          ConnectionStream::Unix(stream),
          Arc::clone(&ctx.provider),
          sender_id,
          guard,
        ))
        .detach();
      }
      Ok(Err(e)) => {
        // accept 失败分档（与 TCP 循环共用同一收口）
        if !handle_accept_error("UDS", e, &ctx.coordinator, &cancel_token, &mut backoff_ms).await {
          break;
        }
      }
      Err(Cancelled) => {
        debug!("UDS 接收循环收到停机取消信号，退出");
        break;
      }
    }
  }

  // 停机 Phase 1：即刻关闭 UDS 监听套接字并移除套接字文件
  // （对标 C# GarnetServer.cs:InternalDispose Phase 1 → Close()；
  //  drop(listener) 关 fd，drop(sock_guard) 触发 UdsGuard::drop 移除套接字文件，
  //  排空窗口内新连接无法建立）
  drop(listener);
  drop(sock_guard);

  // 停机 Phase 2：本线程运行时析构前排空活跃连接（C#
  // GarnetServerBase.DisposeActiveHandlers），detach 连接任务
  // 须在本 block_on 尾部驱动至归零，否则随 Runtime 析构被截断
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

  /// localhost 端点自动展开为 IPv4 + IPv6 双回环端点（对标 C# Format.cs:99-100）
  #[test]
  fn new_expands_localhost_to_dual_endpoints() {
    let server = GarnetServer::new(
      &["localhost:6379".to_string()],
      4096,
      8,
      Arc::new(NullProvider),
    )
    .unwrap();
    assert_eq!(server.endpoints.len(), 2);
    assert_eq!(server.endpoints[0].to_string(), "127.0.0.1:6379");
    assert_eq!(server.endpoints[1].to_string(), "[::1]:6379");
  }

  /// start 失败臂（第二端点失败）必须回收已 spawn worker 并清空监听，杜绝幽灵监听
  #[test]
  fn start_failure_reclaims_all_workers() {
    let temp_dir = tempfile::tempdir().unwrap();
    // 目录路径做套接字绑定必定失败
    let invalid_uds_path = temp_dir.path().to_path_buf();

    let server = GarnetServer::new(
      &[
        "127.0.0.1:0".to_string(),
        format!("unix:{}", invalid_uds_path.display()),
      ],
      4096,
      8,
      Arc::new(NullProvider),
    )
    .unwrap();

    let start_res = server.start(NonZeroUsize::new(2));
    assert!(
      start_res.is_err(),
      "第二端点 UDS 绑定失败必须导致 start 返回 Err"
    );

    // 断言资源干净回收：零存活幽灵监听
    assert!(server.worker_threads.lock().is_empty());
    assert!(server.tcp_addrs.lock().is_empty());
    assert!(server.shutdown_coordinator.is_stopped());
  }

  /// 未启用 TLS 特性时配置 TLS 相关参数拒启（防静默降级为明文）
  #[cfg(not(feature = "tls"))]
  #[test]
  fn test_guard_no_tls_rejects_tls_args() {
    let mut args = NodeArgs::default();
    assert!(guard_no_tls(&args).is_ok());

    args.tls_cert = Some("/tmp/cert.pem".into());
    assert!(guard_no_tls(&args).is_err());
    args.tls_cert = None;

    args.tls_key = Some("/tmp/key.pem".into());
    assert!(guard_no_tls(&args).is_err());
    args.tls_key = None;

    args.tls_issuer_cert = Some("/tmp/ca.pem".into());
    assert!(guard_no_tls(&args).is_err());
    args.tls_issuer_cert = None;

    args.tls_client_target_host = Some("example.com".into());
    assert!(guard_no_tls(&args).is_err());
  }
}
