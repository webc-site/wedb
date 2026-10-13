use std::future::Future;

use super::{
  aof_sink::on_aof_store_event,
  checkpoint::{checkpoint_dir_of, recover_checkpoint_store},
  wal::open_wal,
  *,
};
use crate::Error;

impl<F> StorageSessionProvider<F>
where
  F: Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer>,
{
  /// 冷启动基础装配体（无 AOF 形态）：打开单文件存储引擎、共享经纪与向量
  /// 集合管理器后构造基座（生产无调用方——启动一律经 [`Self::open_from_args`]
  /// 按 (recover, aof) 分派；测试与嵌入式显式注入小预算 [`StoreConfig`]）。
  /// 注册表随装配进程级安装（CLIENT 族命令/dispose 归并直取）；
  /// AOF 门控此形恒 `aof = None`
  pub fn open_with_config(
    config: StoreConfig,
    data_path: impl AsRef<Path>,
    decorate: F,
  ) -> crate::Result<Self> {
    // INFO uptime 起点装配期预热（C# StoreWrapper 构造期赋值 startupTimestamp
    // 对位，先于一切恢复/重放；OnceLock 幂等）
    init_startup_ticks();
    let data_path = data_path.as_ref();
    let (store, broker, vector_manager) = open_node_with_config(config, data_path)?;
    // 嵌入式/测试基座无 NodeArgs，检查点基目录回落数据目录（默认布局）；
    // 生产臂在 open_from_args_with_config / *_and_aof 口按
    // RuntimeServerOptions.checkpoint_base_directory（--checkpoint-dir 真源）覆写
    let checkpoint_dir = checkpoint_dir_of(Path::new(""), data_path);
    Self::from_parts(store, broker, vector_manager, checkpoint_dir, decorate)
  }

  /// 覆盖发布订阅装配（main 层按 NodeArgs 调用：--disable-pubsub 关闭；须在端点
  /// accept 之前调用——首个连接建立后已 attach 的会话持旧中枢）
  fn with_pubsub_config(mut self, disabled: bool) -> Self {
    if disabled && let Some(old) = self.pubsub.take() {
      // 关闭时析构默认装配的中枢（C# DisablePubSub = true 不启动 broker）
      old.dispose();
    }
    self
  }

  /// 覆盖运行时配置与慢日志装配（main 层按 NodeArgs 调用：
  /// [`NodeArgs::runtime_server_options`] 投影 —— --slowlog-log-slower-than /
  /// --slowlog-max-len / --object-scan-count-limit / --aof-commit-ms /
  /// --max-databases 播种 RuntimeServerConfig；须在端点 accept 之前调用）
  pub fn with_runtime_server_options(mut self, options: RuntimeServerOptions) -> Self {
    self.slow_log_container = new_slow_log_container(options.slow_log_max_entries);
    self.runtime_config = Arc::new(RuntimeServerConfig::new(options));
    self
  }

  /// 挂接 Lua 超时管理器停机句柄（宿主装配期注入会话选项投影已装配的
  /// 同一 Arc 实例，对标 C# StoreWrapper 构造期持有 luaTimeoutManager；
  /// None = 超时未启用形态，停机收口自然空操作）
  #[must_use]
  pub fn with_lua_timeout(mut self, manager: Option<Arc<LuaTimeoutManager>>) -> Self {
    self.lua_timeout = manager;
    self
  }

  /// AOF 门控点亮装配（C# StoreWrapper.EnableAOF 语义）：在 [`Self::open_with_config`]
  /// 基础上建独立 WAL 设备 → [`WalLog`] → [`single_log_aof`] 工厂 →
  /// [`NodeService::with_options`] 注册全部 AOF 写监听端口；
  /// [`Self::open_from_args_with_config`] 的 `(false, true)` 臂即走本口。
  ///
  /// `config`：存储引擎配置（生产经 [`Self::open_from_args`] 由 NodeArgs 推导）。
  /// `wal_dir`：自定义 WAL 日志目录（None 默认使用 `<data>/wal`）。
  /// `options`：运行时服务选项（生产为 [`NodeArgs::runtime_server_options`]
  /// 投影）——AOF 三尺寸旋钮（aof-memory / aof-page-size / aof-segment-size）
  /// 先经 [`AofSettings::from_options`] 体检（C# GetAofSettings 先于设备分配
  /// 同序，非法组合在此拒启）再注入物理段设备与日志设置；完整选项全量透传
  /// AOF 门面（C# 完整 serverOptions 直入 GarnetAppendOnlyFile 同位，
  /// commit_frequency_ms 同槽播种运行时配置）。
  /// 返回的基座 `aof()` / `wal()` 在场，宿主据此注入集群面（set_aof /
  /// set_replica_replication_session / set_primary_replication / set_wal）
  pub fn open_with_config_and_aof(
    config: StoreConfig,
    data_path: impl AsRef<Path>,
    wal_dir: Option<&Path>,
    options: RuntimeServerOptions,
    decorate: F,
  ) -> crate::Result<Self> {
    // 实配主存页容量投影（C# GetAofSettings 读同对象 PageSizeBits() 同源）：
    // 先于 config 移入引擎装配取生效值，AofSettings 校验三据此拒启
    // （票 task/todo/wnode-aof-main-page-bits-unwired）
    let mut options = options;
    options.hlog_page_size = config.page_size;
    let mut provider = Self::open_with_config(config, data_path.as_ref(), decorate)?;
    let AssembledAofStack { wal, aof, ri } = Self::assemble_aof_stack(
      data_path.as_ref(),
      wal_dir,
      &options,
      &provider.store,
      &provider.vector_manager,
    )?;
    provider.ri = Some(ri);
    // 检查点基目录以运行时选项为真源（--checkpoint-dir 显式项经
    // NodeArgs::checkpoint_base_dir 投影，缺省回落数据目录；空串回落同口），
    // 覆写嵌入式回落值并换装双件（(false,false) 臂同形，None AOF 位）
    provider.reattach_checkpoint_dir(
      checkpoint_dir_of(
        Path::new(&options.checkpoint_base_directory),
        data_path.as_ref(),
      ),
      Some(Arc::clone(&aof)),
    );
    Ok(Self::mount_aof_domain(provider, aof, wal, options))
  }

  /// AOF 装配同构段（`open_with_config_and_aof` / `open_recovered_with_config_and_aof`
  /// 两臂单点收敛）：hlog 页容量投影须先于 config 移入引擎装配由调用方完成，
  /// 本段承接其余同构五步——AofSettings 三尺寸体检（先于 AOF 设备分配，非法
  /// 组合在此拒启）→ WAL 设备打开 → NodeService 写监听注册 → 范围索引复制面
  /// 单例转交 → 向量域 AOF 直推装配（生产端注入端口 + 重放端承接面）。
  /// 恢复臂先于 AOF 重放装配向量承接面（重放的 VADD/VREM/VSETATTR 条目经
  /// 承接面重建索引）、冷启臂 provider 已在——两臂时序差异留在调用方
  fn assemble_aof_stack(
    data_path: &Path,
    wal_dir: Option<&Path>,
    options: &RuntimeServerOptions,
    store: &SharedStore<SegmentedDevice>,
    vector_manager: &Arc<VectorManager>,
  ) -> crate::Result<AssembledAofStack> {
    // AOF 三尺寸旋钮体检（C# GetAofSettings 同序：先于 AOF 设备分配，
    // 非法组合在此拒启）再注入 open_wal；完整选项全量透传门面
    let aof_settings = AofSettings::from_options(options)?;
    let (_, wal) = open_wal(data_path, wal_dir, &aof_settings)?;
    // 写监听端口注册 + 服务级会话（对标 C# EnableAOF 构造段）；调用方
    // options 全量透传门面，不再经合成 NodeArgs 二次投影
    let node = NodeService::with_options(options, Arc::clone(store), Arc::clone(&wal))
      .map_err(|e| io::Error::other(e.to_string()))?;
    // 范围索引复制面单例转交（C# StoreWrapper.rangeIndexManager 对位：
    // 与事件汇同实例，停机链单点收口）
    let ri = Arc::clone(node.ri());
    // 向量域 AOF 直推装配：生产端注入端口（VADD/VREM/VSETATTR 合成写）+
    // 重放端承接面（AofProcessor 向量分支重放重建索引）
    let aof = Arc::clone(node.aof());
    Self::wire_vector_aof_sink(
      &aof,
      Arc::clone(store.current_version_atomic()),
      vector_manager,
    );
    Ok(AssembledAofStack { wal, aof, ri })
  }

  /// 向量域 AOF 承接面对装（两装配臂同构步单点）：生产端注入端口
  /// （VADD/VREM/VSETATTR 合成写）+ 重放端承接面（AofProcessor 向量分支
  /// 重放重建索引），互为镜像双注册
  fn wire_vector_aof_sink(
    aof: &Arc<GarnetAppendOnlyFile>,
    version_atomic: Arc<AtomicU64>,
    vector_manager: &Arc<VectorManager>,
  ) {
    vector_manager.set_aof_sink(Arc::new(VectorAofSink::new(aof, version_atomic)));
    aof.set_vector_manager(Arc::clone(vector_manager));
  }

  /// AOF 域挂载收口（两装配臂同构步单点）：停机收口角色分派同源注入
  /// （dispose_async 副本纯刷盘不写本地帧）→ AOF/WAL 句柄装载 → 运行时
  /// 服务选项换装
  fn mount_aof_domain(
    mut self,
    aof: Arc<GarnetAppendOnlyFile>,
    wal: Arc<WalLog<SegmentedDevice>>,
    options: RuntimeServerOptions,
  ) -> Self {
    aof.attach_primary_tasks(Arc::clone(&self.primary_tasks));
    self.aof = Some(aof);
    self.wal = Some(wal);
    self.with_runtime_server_options(options)
  }

  /// --recover 恢复装配（无 AOF 形态）：从最新检查点恢复存储句柄后装配基座
  ///
  /// C# RecoverAsync（无 AOF 分支）的变体：仅承接 checkpoint 恢复段，
  /// 完整恢复（checkpoint + AOF 重放）见
  /// [`Self::open_recovered_with_config_and_aof`]；
  /// 恢复在端点 accept 之前完成的时序由
  /// [`crate::server::ServerBootstrap::run_async`] 的装配回调承接
  ///（对标 C# Start 的 `Provider.RecoverAsync()` 同步完成后才
  /// `servers[i].Start()`）。生产无调用方（启动走 [`Self::open_from_args`]
  /// 的 `(true, false)` 臂），恢复装配的 `index_size` 预检要求与快照
  /// StoreMeta 一致，两代装配必须传同一 config
  ///
  /// 检查点目录无有效快照时回退冷启动空库（对标 C# RecoverAsync 对空
  /// 检查点目录的静默语义）
  ///
  /// `checkpoint_base_dir`：检查点基目录（生产为
  /// [`wconf::NodeArgs::checkpoint_base_dir`] 投影，`--checkpoint-dir` 优先
  /// 缺省回落数据目录；对标 C# CheckpointBaseDirectory）
  pub async fn open_recovered_with_config(
    config: StoreConfig,
    data_path: impl AsRef<Path>,
    checkpoint_base_dir: impl AsRef<Path>,
    vector_preview: bool,
    quantization_task_count: usize,
    decorate: F,
  ) -> crate::Result<Self> {
    // INFO uptime 起点装配期预热（恢复臂同 open_recovered_with_config_and_aof，
    // 先于检查点加载；OnceLock 幂等）
    init_startup_ticks();
    let data_path = data_path.as_ref();
    let checkpoint_dir = checkpoint_dir_of(checkpoint_base_dir.as_ref(), data_path);
    let device = Arc::new(SegmentedDevice::new(
      data_path,
      DEFAULT_MAIN_LOG_SEGMENT_SIZE,
      DEFAULT_SECTOR_SIZE,
    )?);
    let store = recover_checkpoint_store(&checkpoint_dir, device, config).await?;
    let (broker, vector_manager) =
      node_components(&store, vector_preview, quantization_task_count)?;
    let provider = Self::from_parts(store, broker, vector_manager, checkpoint_dir, decorate)?;
    // C# RecoverAsync 无 AOF 分支同位：RecoverCheckpointAsync → RecoverVectorSets。
    // 登记表与上下文元数据自旁路记录回建收口（预览关时 sanitize/reconcile
    // 内部门空转，与 C# disabled 恢复语义一致；向量管理器配对已由 from_parts
    // 诞生点单点收口，回建臂经同一 OnceLock 读到在场句柄）
    let recovered = provider.database_manager.recover_vector_sets().await?;
    log::info!("Recovered vector sets: {recovered}");
    Ok(provider)
  }

  /// --recover 恢复装配（AOF 点亮形态）：检查点恢复 + WAL 设备面恢复 + 全量重放；
  /// [`Self::open_from_args_with_config`] 的 `(true, true)` 臂即走本口。
  ///
  /// libs/server/StoreWrapper.cs:RecoverAsync（Recover 分支：
  /// RecoverCheckpointAsync → RecoverAOFAsync → ReplayAOF）。
  /// libs/server/StoreWrapper.cs:RecoverAOFAsync（StoreWrapper 层
  /// RecoverAOFAsync / ReplayAOF 两转发随恢复装配折叠进本流程——下方
  /// `aof.log().recover_async()` 设备面恢复对标 RecoverAOFAsync，恢复出的
  /// `mgr.replay_aof(u64::MAX)` 全量重放对标 ReplayAOF，正式映射锚挂
  /// SingleDatabaseManager 两口）。重放以恢复
  /// store 的版本基线过滤（[`wkv`] checkpoint token 版本——跳过检查点已
  /// 覆盖的旧代条目，未从检查点恢复时版本 0 = 全量重放）；生产写路径
  /// AOF 监听注册在恢复出的存储句柄上（增量继续镜像 WAL）。恢复装配的
  /// `index_size` 预检要求与快照 StoreMeta 一致，两代装配必须传同一 config
  // async body 以 Box::pin 承载：泛型 D（含 R11 注入设备等测试实例化）下该
  // 状态机 layout 巨大，使用方 crate（数十个测试）逐个提 recursion_limit
  // 追不胜追（R27 CI 实证逐层溢出）；冷启动恢复路径一次性装箱，开销无关紧要
  pub fn open_recovered_with_config_and_aof(
    config: StoreConfig,
    data_path: impl AsRef<Path>,
    wal_dir: Option<&Path>,
    options: RuntimeServerOptions,
    vector_preview: bool,
    decorate: F,
  ) -> impl Future<Output = crate::Result<Self>> {
    Box::pin(async move {
      // INFO uptime 起点装配期预热：恢复臂起表必须先于检查点加载与 AOF 重放
      // （C# InitializeServer 构造 StoreWrapper 先于 Start 的 RecoverAsync 对位；
      // OnceLock 幂等，冷启动臂已预热时此处零开销）
      init_startup_ticks();
      let data_path = data_path.as_ref();
      // 实配主存页容量投影（C# GetAofSettings 读同对象 PageSizeBits() 同源）：
      // 先于 config 移入恢复装配取生效值，AofSettings 校验三据此拒启
      let mut options = options;
      options.hlog_page_size = config.page_size;
      let checkpoint_dir =
        checkpoint_dir_of(Path::new(&options.checkpoint_base_directory), data_path);
      let device = Arc::new(SegmentedDevice::new(
        data_path,
        DEFAULT_MAIN_LOG_SEGMENT_SIZE,
        DEFAULT_SECTOR_SIZE,
      )?);
      let store = recover_checkpoint_store(&checkpoint_dir, Arc::clone(&device), config).await?;
      // 向量域先于 AOF 重放装配（重放的 VADD/VREM/VSETATTR 条目经 AOF 门面的
      // 向量承接面重建索引；vm 的存储回调绑恢复出的存储句柄，元素数据随
      let (broker, vector_manager) = node_components(
        &store,
        vector_preview,
        options.vector_set_quantization_task_count.max(0) as usize,
      )?;
      // 写监听端口注册 + 服务级会话（挂到恢复出的存储句柄）+ 范围索引单例
      // 转交（绑定恢复出的存储句柄）+ 向量域承接面对装
      let AssembledAofStack {
        wal,
        aof,
        ri: recovered_ri,
      } = Self::assemble_aof_stack(data_path, wal_dir, &options, &store, &vector_manager)?;
      // AOF 设备面恢复 + 全量重放（版本基线过滤）
      let db = Arc::new(GarnetDatabase::new(
        0,
        Arc::clone(&store),
        device,
        checkpoint_dir.clone(),
        Some(Arc::clone(&aof)),
      ));
      let mgr = Arc::new(SingleDatabaseManager::new(checkpoint_dir.clone(), db));
      mgr.attach_vector_manager(Arc::clone(&vector_manager));
      // C# RecoverCheckpointAndAOFAsync 同位：RecoverCheckpointAsync →
      // RecoverVectorSets → ReplayAOF。登记表与上下文元数据自旁路记录回建
      // 收口在 AOF 重放之前——重放的 VADD 条目随即命中已回建登记（context
      // 原位复用，对齐 C# 索引记录入检查点的原位语义；预览关时
      // sanitize/reconcile 内部门空转，与 C# disabled 恢复语义一致）
      let recovered_vectors = mgr.recover_vector_sets().await?;
      log::info!("Recovered vector sets: {recovered_vectors}");
      // AOF 设备面恢复（C# RecoverAOFAsync → Log.RecoverAsync 磁盘段位点扫描；
      // ValueTask 透明上抛：wnode::Error 具 #[from] waof::Error，`?` 直达装配口
      // 快速失败中止启动，脏位点下绝不续跑重放。C# 侧该类恢复错受
      // FailOnRecoveryError 旗标门控且生效默认关——catch 吞错带已恢复部分续行
      //（Options.cs:637/:1028 GetValueOrDefault 折 false + defaults.conf:497 恒
      // false）；rust 侧该旗标零代码消费、恢复失败恒拒启，系刻意收紧非缺省漏配，
      // 裁决收口见 deviations.md §186）
      aof.log().recover_async().await?;
      // 检查点覆盖位点对齐（C# ReplicationManager.cs:548 RecoverCheckpointAndAOFAsync
      // 同位调用 InitializeIf）：恢复出的检查点对应不可用 AOF 地址（AOF 段过度
      // 截断或丢失致尾位点落后于检查点覆盖地址）时，把 AOF 位点推至安全地址，
      // 重放与复制位点基线保持一致；正常场景尾位点不落后即 no-op
      if let Some((_, meta)) = wcpr::latest_checkpoint_meta(&checkpoint_dir)
        && let Some(covered) = meta.checkpoint_aof_address
      {
        // 从元数据向量按物理子日志逐位还原覆盖位点（对标 C#
        // GetCheckpointCookieMetadata 多子日志分支 AofAddress.Deserialize），物理
        // 日志维度为界截取——严禁 AofAddress::create(size, 标量) 广播：子日志间
        // 地址空间独立，标量广播会把小写入量子日志的尾位点拔高至子日志 0 水位，
        // 制造幽灵空洞（重放区间读未初始化段 → UnexpectedEof 宕机）
        let mut safe = AofAddress::new(aof.log().size() as i32);
        for (i, &addr) in covered.iter().enumerate().take(safe.length() as usize) {
          safe.set(i, addr as i64);
        }
        aof.log().initialize_if(&safe);
      }
      // 全量重放（版本基线过滤）
      let replayed = mgr.replay_aof(u64::MAX).await?;
      log::info!("Recovered AOF: replayed {replayed} entries");
      // 重放后的 AOF 尾地址（对标 C# ReplayAOF 返回值 replayedUntil；宿主
      // 装配尾段据此回填 rm 复制位点——gossip 广播与 failover 判定基线）
      let recovered_aof_tail = aof.log().tail_address();
      let mut provider = Self::from_parts(store, broker, vector_manager, checkpoint_dir, decorate)?;
      provider.database_manager = mgr;
      // 换装点同步注入角色域：AOF 超限臂在任意装配形态下读同一角色位
      provider
        .database_manager
        .attach_primary_tasks(Arc::clone(&provider.primary_tasks));
      let mut provider = Self::mount_aof_domain(provider, aof, wal, options);
      provider.ri = Some(recovered_ri);
      provider.recovered_aof_tail = Some(recovered_aof_tail);
      Ok(provider)
    })
  }

  /// 根据 [`NodeArgs`] 自动分派恢复与 AOF 策略，并串联 requirepass、PubSub 与运行时选项装配
  ///
  /// 封装 (recover, aof) 四路状态分派（对标 GarnetServer 启动时序，四臂均走
  /// 带 [`StoreConfig`] 的装配口，配置由 [`Self::open_from_args`] 一次推导）：
  /// - `(true, true)`: [`Self::open_recovered_with_config_and_aof`] 检查点恢复 + WAL 重放
  /// - `(true, false)`: [`Self::open_recovered_with_config`] 仅检查点恢复
  /// - `(false, true)`: [`Self::open_with_config_and_aof`] 冷启动 + AOF 挂载
  /// - `(false, false)`: [`Self::open_with_config`] 基础冷启动
  ///
  /// 装配完成后依次链式注入认证口令、PubSub 规格与运行时动态配置。
  pub async fn open_from_args(
    node: &NodeArgs,
    data_path: impl AsRef<Path>,
    decorate: F,
  ) -> crate::Result<Self> {
    Self::open_from_args_with_config(store_config_from_node(node)?, node, data_path, decorate).await
  }

  /// [`Self::open_from_args`] 的显式配置变体（测试/嵌入式注入自定义 [`StoreConfig`]）
  pub async fn open_from_args_with_config(
    config: StoreConfig,
    node: &NodeArgs,
    data_path: impl AsRef<Path>,
    decorate: F,
  ) -> crate::Result<Self> {
    let data_path = data_path.as_ref();
    let provider = match (node.recover, node.aof) {
      (true, true) => {
        Self::open_recovered_with_config_and_aof(
          config,
          data_path,
          node.wal_dir.as_deref(),
          node.runtime_server_options(),
          node.enable_vector_set_preview,
          decorate,
        )
        .await?
      }
      (true, false) => {
        Self::open_recovered_with_config(
          config,
          data_path,
          node.checkpoint_base_dir(),
          node.enable_vector_set_preview,
          node.vector_set_quantization_task_count.max(0) as usize,
          decorate,
        )
        .await?
      }
      (false, true) => Self::open_with_config_and_aof(
        config,
        data_path,
        node.wal_dir.as_deref(),
        node.runtime_server_options(),
        decorate,
      )?,
      (false, false) => {
        // 冷启动臂：检查点基目录以 NodeArgs 真源覆写嵌入式回落值（与
        // checkpoint_base_dir() 投影的 runtime_server_options 同源）并换装
        // 双件（(false,true) 臂同形，None AOF 位）
        let mut provider = Self::open_with_config(config, data_path, decorate)?;
        provider.reattach_checkpoint_dir(
          checkpoint_dir_of(&node.checkpoint_base_dir(), data_path),
          None,
        );
        provider
      }
    }
    .with_requirepass(node.requirepass.as_deref())
    .with_pubsub_config(node.disable_pubsub)
    .with_runtime_server_options(node.runtime_server_options())
    .with_metrics_sampling_frequency_secs(node.metrics_sampling_frequency_secs)
    .with_vector_set_preview(node.enable_vector_set_preview)
    // 量化任务数冷启动透传：三件套漏斗构造期恒 0 落核数，本尾段以 NodeArgs
    // 真值单点覆写（案一假旋钮收口；四臂统一，恢复臂构造已注入同值幂等）
    .with_quantization_task_count(node.vector_set_quantization_task_count.max(0) as usize);
    // TLS 证书配置投影（C# Options.cs:948-957 EnableTLS 一处构造
    // GarnetTlsOptions 对位：网络端点与会话域经
    // [`SessionProviderFace::tls_config`] 共享同一实例，CONFIG SET
    // cert-file-name 在线重载与 --cert-refresh-freq 定时刷新共用此单点）。
    // 刷新周期 > 0 的后台循环须在 compio 运行时内拉起，本装配口为 async
    // 且宿主一律在运行时内 await，时序天然满足
    #[cfg(feature = "tls")]
    let provider = match tls_config_from_node(node)? {
      Some(tls) => provider.with_tls_config(tls),
      None => provider,
    };
    // 后台维护任务装配（对标 StoreWrapper.Start → StartPrimaryTasks /
    // StartGenericNodeTasks：AofSizeLimitTask / IndexAutoGrowTask 注册条件）
    Self::wire_background_tasks(provider, node)
  }

  /// 置换槽句柄（供集群节点对齐 C# storeWrapper 同步在线置换引擎）
  pub fn store_swap_slot(&self) -> StoreSwapSlot {
    self.store_swap.clone()
  }

  /// Primary 类后台任务生命周期域句柄（集群装配期注入 ClusterProvider，
  /// 角色切换点批量挂起/恢复 Primary 类任务）
  pub fn primary_tasks(&self) -> Arc<PrimaryTasks> {
    Arc::clone(&self.primary_tasks)
  }

  /// 当前在线引擎（本结构引擎取口唯一入口：置换槽优先、回落装配期初值；
  /// 对标 C# libs/server/StoreWrapper.cs:41 单计算属性——副本检查点导入
  /// 置换后，新会话装配与集群侧经此取到的必为同一新引擎实例）
  pub fn store(&self) -> SharedStore<SegmentedDevice> {
    self
      .store_swap
      .get()
      .unwrap_or_else(|| Arc::clone(&self.store))
  }

  /// AOF 门面（AOF 门控未点亮时 None）
  pub fn aof(&self) -> Option<&Arc<GarnetAppendOnlyFile>> {
    self.aof.as_ref()
  }

  /// 集合项经纪共享句柄（对位 C# StoreWrapper.itemBroker 公开字段；宿主
  /// 停机断言与嵌入式管理面取用）
  pub fn item_broker(&self) -> Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>> {
    Arc::clone(&self.broker)
  }

  /// 物理日志句柄（AOF 门控未点亮时 None；宿主据此装配集群复制数据面：
  /// 副本接收会话落盘目标与主端推流数据源共用同一日志实例）
  pub fn wal(&self) -> Option<&Arc<WalLog<SegmentedDevice>>> {
    self.wal.as_ref()
  }

  /// --recover 重放后的 AOF 尾地址（仅
  /// [`Self::open_recovered_with_config_and_aof`] 形态点亮；对标 C#
  /// RecoverCheckpointAndAOFAsync 尾段
  /// `replicationOffset.SetValue(ref replayedUntil)` 的回填值来源）
  pub fn recovered_aof_tail(&self) -> Option<AofAddress> {
    self.recovered_aof_tail
  }

  /// 上次保存时间（毫秒 Unix 时间戳，对标 C# StoreWrapper.lastSaveTime）
  pub fn last_save_ms(&self) -> u64 {
    self.database_manager.last_save_ms()
  }

  /// 配置 requirepass 认证口令（对标 C# Options.Password / GetAuthenticationSettings）
  ///
  /// 单一固定口令档不设独立认证器：requirepass 落成 default 用户口令入 ACL，
  /// 认证走 ACL 基座统一链路——承接
  /// libs/server/Auth/GarnetPasswordAuthenticator.cs:Authenticate
  /// 的固定口令比对语义（rust 无旁路认证器类型，见 wacl/src/auth/mod.rs 头注）
  pub fn with_requirepass(mut self, pass: Option<&str>) -> Self {
    self.acl = pass.filter(|p| !p.is_empty()).map(|p| {
      // 纯内存创建默认用户，无配置文件 I/O，绝不 panic
      let acl =
        AccessControlList::new(p).expect("failed to create AccessControlList with requirepass");
      Arc::new(acl)
    });
    self
  }

  /// 注入自定义 ACL 访问控制列表
  /// 测试断言面，产线零调用
  #[doc(hidden)]
  pub fn with_acl(mut self, acl: Arc<AccessControlList>) -> Self {
    self.acl = Some(acl);
    self
  }

  /// 注入指标采样频率秒数（对标 C# ServerOptions.MetricsSamplingFrequency →
  /// storeWrapper.trackStats 门控；仅装配链尾段注入，与监视器任务同源）
  pub fn with_metrics_sampling_frequency_secs(mut self, secs: u64) -> Self {
    self.metrics_sampling_frequency_secs = secs;
    self
  }

  /// 注入 Vector Set 预览开关（装配链尾段单次定值；开关投影进向量管理器
  /// `is_enabled`，命令面与量化/清理后台链均以此门控）。
  ///
  ///（C# 对位 VectorManager.cs 构造器的 `IsEnabled = serverOptions.EnableVectorSetPreview` 注入位）
  pub fn with_vector_set_preview(self, enabled: bool) -> Self {
    self
      .vector_manager
      .is_enabled
      .store(enabled, Ordering::Relaxed);
    self
  }

  /// 注入量化任务数生效值（C# 对位 VectorManager.cs:227-228 构造器读
  /// `serverOptions.VectorSetQuantizationTaskCount` 的注入位）：冷启动三件套
  /// 漏斗 `open_node_with_config` 无 NodeArgs，构造期恒 0 落默认核数（假旋钮），
  /// 真实 CLI 值经本装配尾段单点覆写收口，与 [`Self::with_vector_set_preview`]
  /// 同形；入参为 `max(0)` 折叠后的原始值，归一（0→核数、非 0→钳 1024）复用
  /// VectorManager 单点，杜绝第二套判定
  fn with_quantization_task_count(self, raw: usize) -> Self {
    self.vector_manager.set_quantization_task_count(raw);
    self
  }

  /// 注入 TLS 证书热加载共享句柄（C# storeWrapper.serverOptions.TlsOptions
  /// 单实例共享的装配位；网络端点侧经 [`SessionProviderFace::tls_config`]
  /// 取同一实例——CONFIG SET cert-file-name 在线重载即全端点生效。测试/
  /// 嵌入式直构形态用，标准启动链由 [`Self::open_from_args_with_config`]
  /// 从 NodeArgs 单点投影）
  #[cfg(feature = "tls")]
  pub fn with_tls_config(mut self, tls_config: ServerTlsConfig) -> Self {
    self.tls_config = Some(Arc::new(tls_config));
    self
  }

  /// 启动 AOF 周期提交后台任务（C# StoreWrapper.cs:TryStartCommitTask 对标实现）
  ///
  /// 若配置了 commit_frequency_ms > 0 且存在 AOF 句柄，首次调用拉起后台周期
  /// 循环（任务常驻，副本角色由 [`PrimaryTasks`] 角色位门检挂起）。
  pub fn try_start_commit_task(&self) {
    if let Some(aof) = &self.aof {
      self
        .primary_tasks
        .bind_commit_env(aof, &self.runtime_config);
      self.primary_tasks.try_start_commit_task();
    }
  }

  /// 配置 AOF 体积限额（C# StartPrimaryTasks 的 AofSizeLimitTask 注册条件：
  /// limit_bytes > 0；须在端点 accept 之前调用）。检查周期不在此设——运行期
  /// 真值源为 wconf 槽 aof-size-limit-enforce-frequency（node 侧经
  /// runtime_server_options 播种），任务循环每轮现取，CONFIG SET 即时生效
  pub fn with_aof_size_limit(mut self, limit_bytes: u64) -> Self {
    self.aof_size_limit = (limit_bytes > 0).then_some(limit_bytes);
    self
  }

  /// 配置索引自动扩容上限、阈值与周期（C# StartGenericNodeTasks 的
  /// IndexAutoGrowTask 注册条件：max_buckets > 0；须在端点 accept 之前调用）
  pub fn with_index_auto_grow(
    mut self,
    max_buckets: usize,
    resize_threshold: i64,
    frequency_secs: u64,
  ) -> Self {
    self.index_auto_grow =
      (max_buckets > 0).then_some((max_buckets, resize_threshold, frequency_secs));
    self
  }

  /// 拉起 AOF 体积限额后台任务（C# StartPrimaryTasks 的 AofSizeLimitTask 注册段；
  /// 首个会话建立时惰性触发，幂等）
  pub(super) fn try_start_aof_size_limit_task(&self) {
    if let Some(limit) = self.aof_size_limit
      && !self.aof_size_limit_started.swap(true, Ordering::Relaxed)
    {
      spawn_aof_size_limit_task(
        Arc::clone(&self.database_manager),
        limit,
        Some(Arc::clone(&self.runtime_config)),
        Arc::clone(&self.aof_size_limit_started),
      );
    }
  }

  /// 拉起索引自动扩容后台任务（C# StartGenericNodeTasks 的 IndexAutoGrowTask
  /// 注册段；首个会话建立时惰性触发，幂等）
  pub(super) fn try_start_index_auto_grow_task(&self) {
    if let Some((max_buckets, threshold, freq)) = self.index_auto_grow
      && !self.index_auto_grow_started.swap(true, Ordering::Relaxed)
    {
      spawn_index_auto_grow_task(
        Arc::clone(&self.database_manager),
        max_buckets,
        threshold,
        freq,
        Arc::clone(&self.index_auto_grow_started),
      );
    }
  }

  /// 按节点参数装配后台维护任务（AOF 体积限额 / 索引自动扩容）
  ///
  /// libs/server/StoreWrapper.cs:StartGenericNodeTasks
  /// libs/server/StoreWrapper.cs:Start
  ///（StoreWrapper.Start 的总装折叠：monitor / clusterProvider / luaTimeout
  /// 三启动面在 boot 装配序与 [`assemble_lua_timeout`]（构造即 start 专属
  /// 看门狗线程）等散点承接，Primary /
  /// Replica 任务角色分派折叠进 ClusterProvider 装配与 resume_primary_tasks，
  /// StartSizeTrackers 随 CacheSizeTracker 判定不移植；本函数即任务接线段——
  /// AOF 限长、索引自动扩容两通用任务与 C# 同条件注册）
  ///（注册条件对齐：AofSizeLimit 配置且 EnableAOF；IndexMaxMemorySize 配置；
  /// StartPrimaryTasks 的严格映射单点在 ClusterProvider::resume_primary_tasks，
  /// 周期任务角色门控见 primary_tasks 域）
  fn wire_background_tasks(provider: Self, node: &NodeArgs) -> crate::Result<Self> {
    // libs/host/Configuration/Options.cs:869（AofSizeLimit 不能与禁用 AOF 同启）
    if node
      .aof_size_limit
      .as_deref()
      .is_some_and(|s| !s.is_empty())
      && !node.aof
    {
      return Err(Error::InvalidArgument(
        "aof_size_limit cannot be enforced with disabled AOF!".into(),
      ));
    }
    let mut provider = provider;
    if let Some(limit) = node.aof_size_limit_bytes() {
      provider = provider.with_aof_size_limit(limit);
    }
    if let Some(max_buckets) = node.index_max_size_buckets() {
      provider = provider.with_index_auto_grow(
        max_buckets,
        node.index_resize_threshold,
        node.index_resize_frequency_secs,
      );
    }
    Ok(provider)
  }

  /// 引擎在线置换钩子束（宿主注入集群置换漏斗的单次挂载回调；返回闭包捕获
  /// 本提供者的 WATCH 版本表、AOF 门面、RI 复制面与向量集合管理器句柄，对换入
  /// 引擎统一重挂写面钩子，详见方法体注释）
  ///
  /// C# 对位形态：副本检查点恢复为原位恢复
  ///（C# SingleDatabaseManager 的 RecoverCheckpointAsync 臂在同一 database/store
  /// 对象上重建），functionsState.watchVersionMap 与
  /// appendOnlyFile 接线跨恢复全程存活，不存在「钩子丢失」形态。rust 引擎
  /// 实例置换形态下钩子是引擎实例级 [`wkv`] OnceLock：换入新引擎不经重挂则
  /// WATCH 版本推进静默旁路（wkv `bump_watch_version` 钩子缺席零开销跳过 →
  /// 置换后 WATCH 乐观并发整体失效）、AOF per-op 镜像零条目（升主后写入不
  /// 落日志）、缺席删除登记观测断线（向量登记表幽灵驻留新引擎）。OnceLock
  /// 保首，重复挂载安全。
  ///
  /// 治理枚举（工单 zcode-r137c-snaplock2 宗一）：本束必须逐件重挂
  /// [`wkv::EngineHookSlots`] 全举的三件引擎实例级钩（watch_hook、
  /// event_sink、delete_miss_hook），先于投槽令「引擎可见即钩子在场」成立；
  /// wkv 新增引擎实例级 OnceLock 钩时必同步扩 EngineHookSlots 枚举与本束，
  /// 置换锁测（wedb/tests/engine_swap_hook_bundle.rs）逐字段断言即红。
  pub fn engine_swap_hook_bundle(&self) -> EngineSwapHook {
    let watch_version_map = Arc::clone(&self.watch_version_map);
    let aof = self.aof.as_ref().map(Arc::clone);
    // 与装配期事件汇共持同一 RI 复制面单例（AofSinkContext.ri 唯一实例纪律，
    // 见 ri 字段文档）；事件汇内仅读其分块大小槽，引擎绑定面随引擎树释放
    // 走 dispose 链
    let ri = self.ri.as_ref().map(Arc::clone);
    let vector_manager = Arc::clone(&self.vector_manager);
    Arc::new(move |store: &SharedStore<SegmentedDevice>| {
      // WATCH 写面收口重挂（与装配期 from_parts 同一构造：EXEC 校验表按键
      // 推进，置换后新引擎写入持续推进共享版本表）
      store.set_watch_hook(version_map_watch_hook(Arc::clone(&watch_version_map)));
      // AOF per-op 镜像重挂（与装配期 NodeService::assemble 同构：条目版本戳
      // 随写入口合字快照单点传入，绑换入引擎自身版本源——store_version 版本
      // 基线随换面；AOF 未点亮维持无事件汇形态，与 C# `appendOnlyFile != null`
      // 门控同口径）
      if let (Some(aof), Some(ri)) = (&aof, &ri) {
        let ctx = Arc::new(AofSinkContext {
          aof: Arc::clone(aof),
          ri: Arc::clone(ri),
        });
        if !store.set_event_sink(StoreEventSink::new(ctx, on_aof_store_event)) {
          log::warn!("存储事件处理器重复注册");
        }
      }
      // 删除缺席登记观测重挂（与逐连接 get_session 装饰经
      // StoreGarnetApi::with_vector_manager 注入同一构造）：换入引擎的
      // DEL/UNLINK/回放删/紧缩丢向量双域缺席臂自钩子在位起即摘除登记表
      // 幽灵，不再待首连接装饰补挂；OnceLock 保首，装饰面重复注入幂等无害
      store.set_delete_miss_hook(vector_registry_delete_hook(Arc::clone(&vector_manager)));
    })
  }

  /// 提取当前服务基座持有的会话共享依赖集合（对标 C# StoreWrapper 共享依赖组）
  pub fn session_dependencies(&self) -> SessionDependencies {
    let acl_authenticator = self
      .acl
      .as_ref()
      .map(|acl| Arc::new(GarnetAclAuthenticator::new(Arc::clone(acl))));
    SessionDependencies {
      watch_version_map: Arc::clone(&self.watch_version_map),
      lock_table: self.lock_table.clone(),
      store_script_cache: Arc::clone(&self.store_script_cache),
      item_broker: Arc::clone(&self.broker),
      runtime_config: Arc::clone(&self.runtime_config),
      slow_log_container: Arc::clone(&self.slow_log_container),
      acl_authenticator,
      pubsub: self.pubsub.as_ref().map(Arc::clone),
      primary_tasks: Some(Arc::clone(&self.primary_tasks)),
      aof: self.aof.as_ref().map(Arc::clone),
      #[cfg(feature = "tls")]
      tls_config: self.tls_config.clone(),
    }
  }
}
