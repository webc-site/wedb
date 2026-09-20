//! 装配面（对标 libs/server/API/SessionApi.cs 与 C# 构造函数注入段：存储
//! 执行域、事务组件、发布订阅、ACL 认证器、集群切面、慢日志、Primary 任务
//! 域的宿主注入出口，以及会话构造选项的配置投影）。

use std::sync::Arc;

use wacl::GarnetAclAuthenticator;
use wconf::{ConnectionProtectionOption, NodeArgs, RuntimeServerConfig};
use wlua::{LuaOptions, LuaTimeoutManager};
use wmetric::SlowLogContainer;
use wpubsub::{session_commands::PubSubSession, subscribe_broker::SubscribeBroker};
use wtxn::{TransactionManager, TxnLockTable, WatchVersionMap};

use super::core::RespServerSession;
use crate::{
  aof::garnet_append_only_file::GarnetAppendOnlyFile,
  cluster_provider::ClusterProviderHandle,
  cluster_session::ClusterSession,
  primary_tasks::PrimaryTasks,
  resp::{ItemBroker, garnet_api::GarnetApi, session_dependencies::SessionDependencies},
  service::assemble_lua_timeout,
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
      // C# 默认链：LuaOptions 默认（超时无限、Silent、Native）、
      // Timeout == Infinite 不建管理器。
      lua_options: LuaOptions::default(),
      lua_timeout_manager: None,
      // C# GarnetServerOptions 默认：EnableAOF = false、WaitForCommit = false
      enable_aof: false,
      wait_for_commit: false,
    }
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
      lua_options: LuaOptions {
        timeout_millis: node.lua_script_timeout_ms.max(0),
        ..LuaOptions::default()
      },
      lua_timeout_manager: assemble_lua_timeout(node.enable_lua, node.lua_script_timeout_ms),
      // C# serverOptions.EnableAOF 投影（单机与集群会话 AOF 门控一致性）
      enable_aof: node.aof,
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
    // 会话延迟表单点回挂执行域（对标 C# RespServerSession 构造 storageSession
    // 时下传 LatencyMetrics 的共享关系：rust 执行域先于会话构造，故在挂入
    // 会话时把同一对象回挂，慢路径存储会话据此记 PENDING_LAT；
    // 延迟监视关闭（latency_metrics None）不回挂，与 C# null 同形）
    if let Some(latency) = &self.latency_metrics {
      api.attach_latency_metrics(Arc::clone(latency));
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
  /// 的依赖倒置形态；AOF 事务日志由 wtxn 默认无日志形态承接。
  /// `lock_table` 为该会话所属引擎实例的锁表句柄（C# 事务上下文经
  /// `_clientSession.store.LockTable` 取表，非本会话自造）
  pub fn attach_transaction_components(
    &mut self,
    watch_version_map: Arc<WatchVersionMap>,
    lock_table: TxnLockTable,
  ) {
    self.lock_table = lock_table.clone();
    let mut txn = TransactionManager::new(lock_table, watch_version_map, None);
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
  pub fn attach_acl(
    &mut self,
    authenticator: Option<Arc<parking_lot::Mutex<GarnetAclAuthenticator>>>,
  ) {
    self.authenticator_can_authenticate = authenticator.is_some();
    self.acl_authenticator = authenticator;
    // 认证域切换，先回落未认证态（C# 构造期 _userHandle 为 null），再重放
    // 构造尾部 AuthenticateUser(defaultUser)：nopass default 用户认证成功 →
    // acl_user_handle 挂载（全部命令放行）；带口令 default 用户失败 → 保持
    // 未认证，非 NoAuth 命令按 C# NOAUTH 拒绝
    self.user_handle = None;
    self.acl_user_handle = None;
    self.acl_mount = None;
    self.authenticate_user("default".as_bytes(), &[]);
  }

  /// 统一单次注入会话共享依赖集合（对标 C# StoreWrapper 共享依赖组会话装配）
  pub fn inject_dependencies(&mut self, deps: SessionDependencies) -> &mut Self {
    self.attach_transaction_components(deps.watch_version_map, deps.lock_table);
    self.set_item_broker(deps.item_broker);
    self.set_runtime_config(deps.runtime_config);
    self.set_slow_log_container(deps.slow_log_container);
    self.aof = deps.aof;
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

  /// 远端端点描述（接口属性 INetworkSender.cs:RemoteEndpointName 的会话实现，接口映射在 traits.rs）
  ///
  /// 关联远端端点（客户端 IP:Port，对标 C# NetworkSender.RemoteEndpointName）
  pub fn set_remote_endpoint(&mut self, endpoint: &str) {
    self.remote_endpoint.clear();
    self.remote_endpoint.push_str(endpoint);
  }

  /// C# networkSender.IsLocalConnection：对端为本地回环 / Unix 套接字
  pub fn is_local_connection(&self) -> bool {
    self.remote_endpoint.starts_with("127.0.0.1")
      || self.remote_endpoint.starts_with("[::1]")
      || self.remote_endpoint.starts_with("unix:")
  }
}
