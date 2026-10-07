//! 统一服务端启动流水线（模板方法模式 ServerBootstrap 的装配与 run_async）
//!
//! 对标 C# libs/host/GarnetServer.cs 的 InitializeServer / Start 启动序列

use super::{monitor::install_server_monitor, *};

impl<A: ServerArgs> ServerBootstrap<A, NoopClusterProvider> {
  /// 创建单机服务端启动引导器（默认装配 NoopClusterProvider）
  pub fn new(args: A) -> Self {
    Self {
      args,
      cluster_provider: NoopClusterProvider,
      network_buffer_size: DEFAULT_BUFFER_SIZE,
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
      wdev::ensure_dir_persistent(&node_args.dir)?;
    }
    let wal_dir = node_args.wal_dir();
    if !wal_dir.as_os_str().is_empty() {
      wdev::ensure_dir_persistent(&wal_dir)?;
    }
    let checkpoint_base_dir = node_args.checkpoint_base_dir();
    if !checkpoint_base_dir.as_os_str().is_empty() {
      wdev::ensure_dir_persistent(&checkpoint_base_dir)?;
    }

    // 1.5 数据目录排他锁（flock）：SO_REUSEPORT 架构偏差下防多实例并发
    //     双写的唯一防线（deviations 第 11 条），设备打开前单点获取。
    //     守卫随本流水线存活（move 进运行时闭包），进程退出含 kill -9
    //     由内核回收 fd 自动释放。锁门谓词与第 1 步写面谓词归一：dir、
    //     wal_dir、checkpoint_base_dir 任一非空即取锁——dir 空串并非无写面
    //     （data_path 折出相对 `wedb.db`、派生 `wal` 与检查点回落全部
    //     相对 cwd 解析真实落盘），由 [`DataDirLock::acquire`] 的 dir 空串
    //     锁 cwd 语义覆盖；三者全空（显式三空配置）与第 1 步 ensure 同
    //     谓词跳过，嵌入式无目录形态语义不变（嵌入式直构本不经本流水线）
    let _datadir_lock = if node_args.dir.as_os_str().is_empty()
      && wal_dir.as_os_str().is_empty()
      && checkpoint_base_dir.as_os_str().is_empty()
    {
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

      let mut server = GarnetServer::new(&endpoints, network_buffer_size, session_provider)?
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
