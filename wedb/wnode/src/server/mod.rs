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

mod accept;
mod bootstrap;
mod lifecycle;
mod monitor;
mod tls;

#[cfg(unix)]
use std::path::PathBuf;
use std::{
  fmt,
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

/// 统一节点宿主服务器
pub struct GarnetServer<P: SessionProviderFace + 'static> {
  /// 配置的端点列表
  ///
  /// [`doc(hidden)`] 测试专用隐藏面：localhost 端点双回环展开断言的观测口
  ///（wnode/tests/server_start_failure_reclaim.rs），生产读取面为
  /// [`Self::local_addr`] 与 accept 循环，非公共 API 契约
  #[doc(hidden)]
  pub endpoints: Vec<ServerEndpoint>,
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
  /// TLS 收场尾帧超时界（生产默认 [`TLS_SHUTDOWN_TIMEOUT`] 1s，经
  /// [`Self::with_tls_shutdown_timeout`] 仅此一处装配位可注入短值，
  /// 供集成测试有界收敛等待）
  #[cfg(feature = "tls")]
  tls_shutdown_timeout: Duration,
  /// UDS 套接字文件权限模式位（None = 不设置，沿用 umask 现行为；对标
  /// C# GarnetServer.cs:294 将 opts.UnixSocketPermission 注入
  /// GarnetServerTcp 构造的透传链，值不进端点字符串）
  #[cfg(unix)]
  unix_socket_perm: Option<u32>,
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
  /// TLS 收场尾帧超时界（生产默认 [`TLS_SHUTDOWN_TIMEOUT`]；集成测试经
  /// [`GarnetServer::with_tls_shutdown_timeout`] 注入短值收敛等待）
  #[cfg(feature = "tls")]
  tls_shutdown_timeout: Duration,
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
      #[cfg(feature = "tls")]
      tls_shutdown_timeout: TLS_SHUTDOWN_TIMEOUT,
      #[cfg(unix)]
      unix_socket_perm: None,
    })
  }

  /// 设置外部停机协调器
  pub fn with_shutdown_coordinator(mut self, shutdown_coordinator: ShutdownCoordinator) -> Self {
    self.shutdown_coordinator = shutdown_coordinator;
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
  fn with_unix_socket_perm(mut self, perm: Option<u32>) -> Self {
    self.unix_socket_perm = perm;
    self
  }

  /// 获取绑定的本地 TCP 地址
  fn local_addrs(&self) -> Vec<SocketAddr> {
    self.tcp_addrs.lock().clone()
  }

  /// 存活 worker 线程句柄计数
  ///
  /// [`doc(hidden)`] 测试专用隐藏面：start 失败臂资源回收断言的观测口
  /// （wnode/tests/server_start_failure_reclaim.rs——start 返回 Err 后
  /// 幽灵 worker 必须清零），生产读取面为 [`Self::stop`] 的收场排纵，
  /// 非公共 API 契约
  #[doc(hidden)]
  pub fn worker_thread_count(&self) -> usize {
    self.worker_threads.lock().len()
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
  /// 对位 C# GarnetServer 的 InternalDispose 私有臂（C# 侧无 DisposeAsync 同名件，
  /// 私有件不挂锚；同步壳的 C# 同名锚留 [`Self::dispose`] 一处）
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
}

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

/// TLS 收场尾帧超时界（生产默认 1s，与 [`TLS_HANDSHAKE_TIMEOUT`] 同域）：连接泵
/// 收场尾 close_notify 尾帧的确定性上界。rustls `Stream::poll_close` 须把连接
/// 发送队列全部写尽 socket 才返回 Ready，黑洞对端（发送缓冲满且持续零窗口）令
/// 其永久 Pending——`dispose` 永不执行即注册表幽灵条目 / 幽灵订阅 / 容量守卫
/// 三泄漏。超时即弃尾帧照常落 dispose 收口，底层 fd 随连接任务收场 Drop 关闭发
/// FIN/RST 兜底（对位 C# TcpNetworkHandlerBase.Dispose 的 `socket.Close` 内核
/// 接管尾帧——syscall 无论对端是否排空即刻返回）。明文臂 shutdown 即刻就绪，
/// 包裹零成本无行为变化。C# 原型无对物（Dispose 链 Shutdown/Close 为 syscall
/// 不存在可悬挂的异步等待点），此为 TLS 服务契约的确定性收口补强；注入位
/// [`GarnetServer::with_tls_shutdown_timeout`] 供集成测试短值收敛
pub(crate) const TLS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// 启动参数 TLS 证书对 → 服务端 TLS 配置投影（会话提供者装配链的唯一 TLS
/// 入口，对标 C# Options.cs:948-957 EnableTLS 时一处构造 GarnetTlsOptions，
/// 网络端点（GarnetServer.cs:294 直读 opts.TlsOptions）与会话域共享同一实例）
///
/// 证书与私钥必须成对：单侧配置在 C# 由 GarnetTlsOptions 构造期拒
/// （CertFileName 空即抛），rust 同判据启动期拒绝，杜绝静默降级明文。
///
/// 证书对与 --unixsocket 组合同拒：UDS 接入环无 TLS 握手位
/// （run_uds_accept_loop 直连 `ConnectionStream::Unix`，不似 TCP 臂
/// acceptor+timeout+kill_token 三件套），组合配置下 UDS 面静默明文、绕开
/// mTLS 身份门；C# UDS 端点同构造器携 TlsOptions 每连接握手
/// （GarnetServer.cs:287-294），rust 裁启动门拒启而非补握手臂（UDS 本地
/// 信任边界下 TLS 增益近零，补臂属双重机制面），与出站侧 wconn with_tls
/// Unix 臂「绝不静默降级为明文」同红线。
///
/// 入站客户端认证旋钮一并过同一投影（对标 C# GarnetServerTcp.cs:290
/// handler.Start(tlsOptions?.TlsServerOptions) 携带的
/// ClientCertificateRequired + IssuerCertificatePath 两面）；
/// tls_cert_refresh_freq 注入定时刷新旋钮（Options.cs:330
/// CertificateRefreshFrequency）
///
/// [`doc(hidden)`] 测试专用隐藏面：证书对 + UDS 组合三臂锁测
///（wnode/tests/server_start_failure_reclaim.rs 的 tls 特性臂），生产调用点
/// 为 boot 装配段单点，非公共 API 契约
#[doc(hidden)]
#[cfg(feature = "tls")]
pub fn tls_config_from_node(node: &NodeArgs) -> crate::Result<Option<ServerTlsConfig>> {
  // UDS + TLS 证书对组合拒启门（与 cfg(not(tls)) 侧 guard_no_tls 对称双门，
  // 文案同「拒绝启动以防止静默降级为明文」红线；证书单侧缺位的组合由下方
  // 成对校验先拒，本门只裁完整证书对形态）
  if node.unixsocket.is_some()
    && matches!(
      (node.tls_cert.as_ref(), node.tls_key.as_ref()),
      (Some(_), Some(_))
    )
  {
    return Err(Error::InvalidArgument(
      "检测到 --tls-cert / --tls-key 与 --unixsocket 同时配置：UDS 监听端点无 TLS 握手位，拒绝启动以防止 UDS 面静默降级为明文（绕开 mTLS 身份门），TLS 与 UDS 端点二选一".into(),
    ));
  }
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
///
/// [`doc(hidden)`] 测试专用隐藏面：TLS 参数拒启四臂锁测
///（wnode/tests/server_start_failure_reclaim.rs），生产调用点为
/// `new` 装配段单点，非公共 API 契约
#[doc(hidden)]
#[cfg(not(feature = "tls"))]
pub fn guard_no_tls(node: &NodeArgs) -> crate::Result<()> {
  if node.has_tls() {
    return Err(Error::InvalidArgument(
      "未启用 tls 特性，但检测到 TLS 配置参数 (--tls-cert / --tls-key / --tls-issuer-cert / --tls-client-target-host)，拒绝启动以防止静默降级为明文".into(),
    ));
  }
  Ok(())
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

pub use lifecycle::join_worker_bounded;
