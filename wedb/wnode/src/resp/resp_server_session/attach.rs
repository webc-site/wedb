//! 装配面（对标 libs/server/API/SessionApi.cs 与 C# 构造函数注入段：存储
//! 执行域、事务组件、发布订阅、ACL 认证器、集群切面、慢日志、Primary 任务
//! 域的宿主注入出口，以及会话构造选项的配置投影）。

use std::sync::Arc;

use wacl::GarnetAclAuthenticator;
use wbase::pool::LimitedFixedBufferPool;
use wconf::{
  ConnectionProtectionOption, LuaLoggingMode as ConfLuaLoggingMode,
  LuaMemoryManagementMode as ConfLuaMemoryMode, NodeArgs, RuntimeServerConfig,
};
use wlua::{LuaLoggingMode, LuaMemoryManagementMode, LuaOptions, LuaTimeoutManager};
use wmetric::SlowLogContainer;
use wpubsub::{session_commands::PubSubSession, subscribe_broker::SubscribeBroker};
use wtxn::{TransactionManager, TxnAofLog, TxnLockTable, WatchVersionMap};

use super::core::RespServerSession;
use crate::{
  aof::garnet_append_only_file::GarnetAppendOnlyFile,
  cluster_provider::ClusterProviderHandle,
  cluster_session::ClusterSession,
  primary_tasks::PrimaryTasks,
  resp::{ItemBroker, garnet_api::GarnetApi, session_dependencies::SessionDependencies},
  service::assemble_lua_timeout,
  traits::PeerSource,
};

/// 会话构造选项（C# 构造函数入参的会话状态子集）
#[derive(Debug, Clone)]
pub struct RespServerSessionOptions {
  /// 逻辑库数上界（装配侧按 max_databases 配置注入，全模式一致；C#
  /// GarnetServer.cs:314 集群形态恒 1，本仓自定义设计删除集群限制）
  pub max_databases: u64,
  /// EnableDebugCommand 配置
  pub enable_debug_command: ConnectionProtectionOption,
  /// 延迟监视开关（C# serverOptions.LatencyMonitor）
  pub latency_monitor: bool,
  /// 逐命令统计开关（挂 CommandStats 表，C# serverOptions.CommandStatsMonitor）
  pub command_stats_monitor: bool,
  /// 默认用户句柄名（C# accessControlList.GetDefaultUserHandle()）
  pub default_user: String,
  /// 是否启用 Lua 脚本（C# storeWrapper.serverOptions.EnableLua + enableScripts）
  pub enable_lua: bool,
  /// Lua 会话选项（C# serverOptions.LuaOptions；enable_lua 时随脚本命令
  /// 装配进 LuaSessionContext，不再写死 default）
  pub lua_options: LuaOptions,
  /// Lua 超时管理器（C# storeWrapper.luaTimeoutManager；服务装配期按
  /// 「EnableLua 且超时非无限」创建，tick 任务周期驱动；None = 无超时）
  pub lua_timeout_manager: Option<Arc<LuaTimeoutManager>>,
  /// AOF 启用开关（C# storeWrapper.serverOptions.EnableAOF）
  pub enable_aof: bool,
  /// 索引自动扩容任务是否在跑（C# storeWrapper.serverOptions
  /// .AdjustedIndexMaxCacheLines > 0：`--index-max-size` 配置即常驻拉起
  /// IndexAutoGrowTask，CONFIG SET index 运行门据此短路拒绝人工扩容）
  pub index_auto_grow_active: bool,
  /// AOF 提交等待开关（C# storeWrapper.serverOptions.WaitForCommit，参数源
  /// `--aof-commit-wait`（NodeArgs::aof_commit_wait）；与 enable_aof 同时
  /// 开启时解析器按命令依赖性维护 AOF 阻塞标记，应答出网前等提交落盘）
  pub wait_for_commit: bool,
}

impl Default for RespServerSessionOptions {
  fn default() -> Self {
    Self {
      max_databases: 16,
      enable_debug_command: ConnectionProtectionOption::No,
      latency_monitor: false,
      command_stats_monitor: false,
      default_user: "default".to_string(),
      enable_lua: false,
      // C# 默认链：LuaOptions 默认（超时无限、Enable 日志、Native）、
      // Timeout == Infinite 不建管理器（zcode-r30-defaults 立项三）。
      lua_options: LuaOptions::default(),
      lua_timeout_manager: None,
      // C# GarnetServerOptions 默认：EnableAOF = false、WaitForCommit = false
      enable_aof: false,
      wait_for_commit: false,
      index_auto_grow_active: false,
    }
  }
}

/// 配置端内存管理模式镜像 → wlua 消费端枚举（对标 C# host 层枚举直入
/// `new LuaOptions(...)` 的装配投影，wconf 不反向依赖 wlua，投影收本层单点）
fn project_memory_mode(mode: ConfLuaMemoryMode) -> LuaMemoryManagementMode {
  match mode {
    ConfLuaMemoryMode::Native => LuaMemoryManagementMode::Native,
    ConfLuaMemoryMode::Tracked => LuaMemoryManagementMode::Tracked,
    ConfLuaMemoryMode::Managed => LuaMemoryManagementMode::Managed,
  }
}

/// 配置端日志模式镜像 → wlua 消费端枚举（同 memory 投影形态）
fn project_log_mode(mode: ConfLuaLoggingMode) -> LuaLoggingMode {
  match mode {
    ConfLuaLoggingMode::Enable => LuaLoggingMode::Enable,
    ConfLuaLoggingMode::Silent => LuaLoggingMode::Silent,
    ConfLuaLoggingMode::Disable => LuaLoggingMode::Disable,
  }
}

impl From<&NodeArgs> for RespServerSessionOptions {
  /// 节点参数 → 会话选项基线的单点投影（对标 C# Options.cs:771
  /// `Options.GetServerOptions` 的选项 → serverOptions 单点装配：单机与集群
  /// 会话共用同一份映射，新增旋钮只此一处）
  ///
  /// C# GarnetServer.cs:314 集群形态 maxDatabases 恒 1，本仓按自定义设计
  /// 删除集群限制，全模式与单机同口径读 max_databases 配置自由切库。
  fn from(node: &NodeArgs) -> Self {
    Self {
      max_databases: node.max_databases.max(0) as u64,
      // C# Options.cs:1018 EnableDebugCommand 投影（参数源
      // `--enable-debug-command`，NodeArgs::enable_debug_command）→ 会话门
      // can_run_debug 三档判定，不落 ..default() 兜底
      enable_debug_command: node.enable_debug_command,
      latency_monitor: node.latency_monitor,
      command_stats_monitor: node.commandstats_monitor,
      enable_lua: node.enable_lua,
      // C# Options.cs:1029 五参数整体装配 `new LuaOptions(...)` 的单点投影：
      // 超时/内存模式/限额/日志模式/沙箱白名单逐字段进 LuaOptions，杜绝恒
      // default 的假旋钮形态。超时负值/过小值已在 wconf 启动校验拒启，装配点
      // 不再 max(0) 钳制；限额 [1K, 2GB] 值域闸复用 wlua get_memory_limit_bytes
      // 单点，本处只投字节量不复刻第二道闸
      lua_options: LuaOptions {
        timeout_millis: node.lua_script_timeout_ms,
        lua_memory_limit_bytes: node.lua_memory_limit_bytes().unwrap_or(0),
        log_mode: project_log_mode(node.lua_logging_mode),
        memory_mode: project_memory_mode(node.lua_memory_management_mode),
        allowed_functions: node.lua_allowed_functions.clone(),
      },
      lua_timeout_manager: assemble_lua_timeout(node.enable_lua, node.lua_script_timeout_ms),
      // C# serverOptions.EnableAOF 投影（单机与集群会话 AOF 门控一致性）
      enable_aof: node.aof,
      // C# storeWrapper.serverOptions.AdjustedIndexMaxCacheLines > 0 等价投影：
      // `--index-max-size` 有效配置即 index_max_size_buckets() 产 Some，
      // IndexAutoGrowTask 常驻，CONFIG SET index 运行门据此拒绝（案二）
      index_auto_grow_active: node.index_max_size_buckets().is_some(),
      // C# serverOptions.WaitForCommit 投影（参数源 `--aof-commit-wait`，
      // Options.cs:253 → :934）：与 enable_aof 同真即会话门
      // `aof_commit_mode_gate` 开启，解析期维护 wait_for_aof_blocking、
      // 应答出网前等 AOF 提交落盘
      wait_for_commit: node.aof_commit_wait,
      ..Self::default()
    }
  }
}

impl RespServerSession {
  /// 注入存储执行域（对标 C# RespServerSession 构造函数的 storeWrapper/
  /// basicGarnetApi 注入形态：命令执行面由宿主装配后注入，单机与集群同径）
  pub fn set_garnet_api(&mut self, api: GarnetApi) {
    // PENDING_LAT 计量槽单点回挂执行域（对标 C# RespServerSession 构造
    // storageSession 时下传 LatencyMetrics 的共享关系：rust 会话延迟表为连接
    // 任务独占、执行域只有 &self 触不到，故 pending 计时单独成每连接一槽；
    // 延迟监视关闭（pending_latency None）不回挂，与 C# null 同形）
    if let Some(meter) = &self.pending_latency {
      api.attach_pending_latency(Arc::clone(meter));
    }
    self.garnet_api = Some(api);
  }

  /// 注入集合项经纪（对标 C# RespServerSession 构造期经 storeWrapper
  /// 暴露的 itemBroker：服务器级共享，阻塞命令经其挂起/唤醒）
  pub fn set_item_broker(&mut self, broker: Arc<ItemBroker>) {
    self.item_broker = Some(broker);
  }

  /// 注入服务器级运行时配置（对标 C# storeWrapper.runtimeConfig：会话
  /// 共享同一实例，CONFIG SET 即时全服务器生效）
  pub fn set_runtime_config(&mut self, config: Arc<RuntimeServerConfig>) {
    self.runtime_config = config;
  }

  /// 当前运行时配置（命令层热更读取入口）
  pub fn runtime_config(&self) -> &Arc<RuntimeServerConfig> {
    &self.runtime_config
  }

  /// 注入 Primary 类后台任务生命周期域（C# storeWrapper 任务域可达面：
  /// CONFIG SET 调停消息经会话触达周期任务启停）
  pub fn set_primary_tasks(&mut self, primary_tasks: Arc<PrimaryTasks>) {
    self.primary_tasks = Some(primary_tasks);
  }

  /// Primary 类后台任务生命周期域（None = 装配未注入的纯协议层形态）
  pub fn primary_tasks(&self) -> Option<&Arc<PrimaryTasks>> {
    self.primary_tasks.as_ref()
  }

  /// AOF 追加日志门面（None = 无 AOF / 装配未注入形态）
  pub fn aof(&self) -> Option<&Arc<GarnetAppendOnlyFile>> {
    self.aof.as_ref()
  }

  /// 注入事务组件（C# 构造函数 `new TransactionManager(storeWrapper.watchversionMap, ...)`
  /// 的依赖倒置形态。`aof_log` 在本口恒为 None，AOF 事务日志句柄由
  /// [`Self::inject_dependencies`] 在注入 `deps.aof` 时单点回填——对标 C#
  /// 构造期 `this.appendOnlyFile = functionsState.appendOnlyFile`
  ///（TransactionManager.cs:173），点亮 MULTI/EXEC 的 TxnStart/TxnCommit
  /// 标记落盘（C# Run :513 / Commit :392 的 EnqueueTxn 门控）。
  /// `lock_table` 为该会话所属引擎实例的锁表句柄（C# 事务上下文经
  /// `_clientSession.store.LockTable` 取表，非本会话自造）
  pub fn attach_transaction_components(
    &mut self,
    watch_version_map: Arc<WatchVersionMap>,
    lock_table: TxnLockTable,
  ) {
    let aof_log = self
      .aof
      .as_ref()
      .map(|a| Arc::clone(a.log()) as Arc<dyn TxnAofLog>);
    let mut txn = TransactionManager::new(lock_table, watch_version_map, aof_log);
    txn.cluster_enabled = self.cluster_session.is_some();
    self.txn_manager = Some(txn);
  }

  /// 注入发布订阅中枢（C# 构造函数 `subscribeBroker` 装配；重建会话接线，
  /// 邮箱容量取 wpubsub 默认值）
  pub fn attach_pubsub(&mut self, broker: Arc<SubscribeBroker>) {
    self.pubsub = PubSubSession::new(broker);
  }

  /// 注入慢日志容器（C# storeWrapper.slowLogContainer，容量取服务器配置）
  pub fn set_slow_log_container(&mut self, container: Arc<SlowLogContainer>) {
    self.slow_log_container = Some(container);
  }

  /// 注入 ACL 认证器（C# 构造函数取 `authenticator ?? AuthSettings 投影` 的
  /// 装配位；rust 认证源单一——requirepass 在装配期直接落成带口令的 default
  /// 用户并裹成 ACL 实例，故此处只有 None（免认证）与 Some（ACL）两态）。
  /// ACL 挂载即置 CanAuthenticate（C# GarnetACLAuthenticator.CanAuthenticate = true），
  /// 并按 C# 构造尾部 AuthenticateUser(defaultUser) 关联 default 用户并尽可能自动认证
  pub fn attach_acl(&mut self, authenticator: Option<Arc<GarnetAclAuthenticator>>) {
    self.authenticator_can_authenticate = authenticator.is_some();
    self.acl_authenticator = authenticator;
    // 认证域切换，先回落未认证态（C# 构造期 _userHandle 为 null），再重放
    // 构造尾部 AuthenticateUser(defaultUser)：nopass default 用户认证成功 →
    // acl_user_handle 挂载（全部命令放行）；带口令 default 用户失败 → 保持
    // 未认证，非 NoAuth 命令按 C# NOAUTH 拒绝
    self.acl_user_handle = None;
    self.acl_mount = None;
    self.authenticate_user("default".as_bytes(), &[]);
  }

  /// 统一单次注入会话共享依赖集合（对标 C# StoreWrapper 共享依赖组会话装配）
  pub fn inject_dependencies(&mut self, deps: SessionDependencies) -> &mut Self {
    self.aof = deps.aof;
    self.attach_transaction_components(deps.watch_version_map, deps.lock_table);
    // 全局脚本缓存绑定（C# 会话经 `session.storeWrapper.storeScriptCache`
    // 直取存储实例字典：构造期占位缓存在此被基座单点实例整体替换，
    // EVAL/EVALSHA/SCRIPT 族自此共享全服同一字典）
    self.store_script_cache = deps.store_script_cache;
    self.set_item_broker(deps.item_broker);
    self.set_runtime_config(deps.runtime_config);
    self.set_slow_log_container(deps.slow_log_container);
    #[cfg(feature = "tls")]
    {
      self.tls_config = deps.tls_config;
    }
    if let Some(tasks) = deps.primary_tasks {
      self.set_primary_tasks(tasks);
    }
    if deps.acl_authenticator.is_some() {
      self.attach_acl(deps.acl_authenticator);
    }
    if let Some(broker) = deps.pubsub {
      self.attach_pubsub(broker);
    }
    self
  }

  /// 挂接集群会话切面（C# 构造函数 `cp?.CreateClusterSession(...)` 的依赖
  /// 倒置形态：集群域实现由宿主构造后注入，单机形态保持 None）
  pub fn attach_cluster_session(&mut self, cluster_session: ClusterSession) {
    self.cluster_session = Some(cluster_session);
    if let Some(txn) = &mut self.txn_manager {
      txn.cluster_enabled = true;
    }
  }

  /// 挂接集群提供者切面（C# 构造函数 `clusterProvider`；只读查询与缓冲池管理）
  pub fn attach_cluster_provider(&mut self, cluster_provider: ClusterProviderHandle) {
    self.cluster_provider = Some(cluster_provider);
  }

  /// 注入网络监听层缓冲池句柄（DEBUG PURGEBP ServerListener 的会话侧
  /// 清理源；C# 经 `storeWrapper.Servers` 直调 `GarnetServerTcp.Purge()`，
  /// rust 侧由泵装配 `NetworkHandler::set_session` 单点注入）
  pub fn attach_buffer_pool(&mut self, pool: Arc<LimitedFixedBufferPool>) {
    self.listener_buffer_pool = Some(pool);
  }

  /// 远端端点描述（接口属性 INetworkSender.cs:RemoteEndpointName 的会话实现，接口映射在 traits.rs）
  ///
  /// 关联远端端点（客户端 IP:Port，对标 C# NetworkSender.RemoteEndpointName）：
  /// 文本仅展示面，本地判定同点落 accept 侧折叠的 [`PeerSource`] 来源类型
  pub fn set_remote_endpoint(&mut self, endpoint: &str, source: PeerSource) {
    self.remote_endpoint.clear();
    self.remote_endpoint.push_str(endpoint);
    self.peer_source = source;
  }

  /// 关联本地端点（监听端点文本，对标 C# CLIENT INFO 读的
  /// networkSender.LocalEndpointName——TCP 为 `ip:port`、Unix 为监听套接字
  /// 路径；TLS 待 accept 侧透传前为空串兜底。取值单源在 stream 抽象层，
  /// 本字段仅其会话侧承接，CLIENT INFO 与注册表条目共读同一次取值）
  pub fn set_local_endpoint(&mut self, endpoint: &str) {
    self.local_endpoint.clear();
    self.local_endpoint.push_str(endpoint);
  }

  /// C# networkSender.IsLocalConnection（TcpNetworkHandlerBase.cs:42-44）：
  /// 直读 accept 侧一次性折叠的 [`PeerSource`] 来源类型——Unix 域套接字对端
  /// 恒本地（含未命名对端，其展示串为空串亦不受影响，对位 C#
  /// UnixDomainSocketEndPoint 恒真臂）；IP 臂读 typed 回环判定布尔
  /// （wbase::endpoint::ip_is_loopback 对位 C# IPAddress.IsLoopback：
  /// 127.0.0.0/8、::1 及 v4-mapped IPv6 解映射）。判定绝不再解析
  /// remote_endpoint 展示字符串（旧字符串前缀判据对 UDS 未命名与
  /// v4-mapped 回环两态漏判，已废）
  pub fn is_local_connection(&self) -> bool {
    self.peer_source.is_local()
  }
}
