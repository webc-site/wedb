//! RESP 服务器会话（对标 libs/server/Resp/RespServerSession.cs:RespServerSession）
//!
//! C# 会话直接持有网络发送器 / Tsavorite 上下文 / 事务管理器；Rust 侧这些面
//! 分别由并行域（wkv 会话纪元 / transaction / cluster）承载，本结构承接会话
//! 状态本体：Id / 端点 / CreationTicks / RESP 协议版本 / clientName·lib-* /
//! useAsync / 数据库会话映射 / 订阅与事务模式 / 延迟与会话指标 / 接收缓冲
//! 解析游标 / 输出缓冲，以及 C# 的分派 · 发送 · 数据库切换方法族。
//!
//! 输出缓冲模型：C# 的 `dcurr/dend` 指针游标对应 `output: Vec<u8>` 的
//! `len/capacity`；`Send` 对应 `take_output`。

use std::{
  fmt::Display,
  iter,
  mem::{self, size_of},
  sync::{Arc, atomic::AtomicU64},
};

use itoa::Buffer;
use smallvec::SmallVec;
use wacl::{
  AclPassword, GarnetAclAuthenticator, UserHandle,
  auth::settings::acl_authentication_settings::AclAuthenticationSettings,
};
use wbase::time::{now_ms, now_nanos};
use wcol::itembroker::collection_item_observer::CollectionItemResult;
use wconf::{DEFAULT_RESP_VERSION, RuntimeServerConfig};
use wlua::{
  LuaCommands, LuaOptions, LuaSessionContext, LuaTimeoutManager, ScratchBufferNetworkSender,
  ScriptingApi, SessionScriptCache, StoreScriptCache,
};
use wmetric::{
  GarnetInfoMetrics, GarnetLatencyMetrics, GarnetLatencyMetricsSession, GarnetSessionMetrics,
  InfoCommand, LatencyMetricsType, SlowLogContainer,
};
use wpubsub::{PubSubSession, PubSubSessionCommands, SubscribeBroker};
use wresp::{
  MAX_ERROR_MSG_LEN, ReplyError, RespCommand, RespSliceExt, RespVecExt, SessionParseState,
  cmd_strings as cs, cmd_strings::write_map_len_resp2, is_cluster_sub_command, is_data_command,
  is_no_auth, is_read_only, key_spec::KeySpecificationFlags, normalize_for_acls, one_if_read,
  one_if_write, sanitize_error_str,
};
use wtxn::{TransactionManager, TxnCommandKeys, TxnKeySpec, TxnQueuedCommandInfo, WatchVersionMap};

use super::{
  BlockedWait, ItemBroker,
  metrics_commands::now_stopwatch_ticks,
  parser::{
    resp_command::{MAX_RESP_ARRAY_LENGTH, MruCommandCache, is_allowed_in_subscription_mode},
    session_parse_state::MAX_ARGUMENT_LENGTH_BYTES,
  },
  resp_commands_info::{RespCommandFlags, try_get_simple_resp_command_info},
  slow_path::SlowWait,
};
use crate::cluster_session::{ClusterSession, ClusterSlotVerificationInput, SlotVerifyGate};

/// libs/host/GarnetServer.cs:RedisProtocolVersion（HELLO 应答 version 字段）
pub const REDIS_PROTOCOL_VERSION: &str = "7.4.3";

/// 存储执行域未挂载时的拒绝文案（C# 构造必带 storeWrapper 无此态；
/// rust 侧为宿主装配缺口的显式防线）
const ERR_STORE_DOMAIN_NOT_ATTACHED: &str = "ERR store execution domain not attached";

/// 接收缓冲默认驻留容量（对标泵 64KB 池化接收缓冲水位；scratch 直读形态
/// 整段消费完毕后超限容量即释放回归此水位）
const DEFAULT_RECV_BUFFER_CAPACITY: usize = 1 << 16;

use wtxn::TxnState;

use super::garnet_api::GarnetApi;

/// libs/server/Auth/ConnectionProtectionOption.cs:ConnectionProtectionOption
///
/// 连接保护选项
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionProtectionOption {
  /// 允许
  Yes,
  /// 拒绝
  No,
  /// 仅本地连接
  Local,
}

/// 会话侧数据库会话槽（C# GarnetDatabaseSession 的会话状态投影）
///
/// Rust 侧 StorageSession 与 wkv 批处理会话生命周期绑定，无法驻留会话结构；
/// 槽位承接 `ExpandableMap<GarnetDatabaseSession>` 的簿记语义（创建 / 获取 /
/// 交换 / 激活），存储操作本体经活跃库句柄在命令层完成。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseSessionSlot {
  /// 数据库 ID
  pub id: i32,
  /// 槽位创建时刻（毫秒 tick）
  pub created_ticks: i64,
}

/// 会话构造选项（C# 构造函数入参的会话状态子集）
#[derive(Debug, Clone)]
pub struct RespServerSessionOptions {
  /// 是否允许多库（C# storeWrapper.serverOptions.AllowMultiDb）
  pub allow_multi_db: bool,
  /// 最大库数（EnableCluster 时为 2）
  pub max_databases: i32,
  /// EnableDebugCommand 配置
  pub enable_debug_command: ConnectionProtectionOption,
  /// 延迟监视开关（C# serverOptions.LatencyMonitor）
  pub latency_monitor: bool,
  /// 指标采样频率（> 0 时启用会话指标，C# MetricsSamplingFrequency）
  pub metrics_sampling_frequency: bool,
  /// 默认用户句柄名（C# accessControlList.GetDefaultUserHandle()）
  pub default_user: String,
  /// 是否启用 Lua 脚本（C# storeWrapper.serverOptions.EnableLua + enableScripts）
  pub enable_lua: bool,
  /// Lua 会话选项（C# serverOptions.LuaOptions；enable_lua 时随脚本命令
  /// 装配进 LuaSessionContext，不再写死 default）
  pub lua_options: LuaOptions,
  /// Lua 事务模式（C# serverOptions.LuaTransactionMode；脚本 runner 构造
  /// 的 txn_mode 源头）
  pub lua_txn_mode: bool,
  /// Lua 超时管理器（C# storeWrapper.luaTimeoutManager；服务装配期按
  /// 「EnableLua 且超时非无限」创建，tick 任务周期驱动；None = 无超时）
  pub lua_timeout_manager: Option<Arc<LuaTimeoutManager>>,
}

impl Default for RespServerSessionOptions {
  fn default() -> Self {
    Self {
      allow_multi_db: false,
      max_databases: 16,
      enable_debug_command: ConnectionProtectionOption::No,
      latency_monitor: false,
      metrics_sampling_frequency: false,
      default_user: "default".to_string(),
      enable_lua: false,
      // C# 默认链：LuaOptions 默认（超时无限、Silent、Native）、
      // LuaTransactionMode = false、Timeout == Infinite 不建管理器。
      lua_options: LuaOptions::default(),
      lua_txn_mode: false,
      lua_timeout_manager: None,
    }
  }
}

/// 自定义命令引用（C# currentCustomTransaction / CustomProcedure /
/// CustomRawStringCommand / CustomObjCmd 的会话侧投影；注册表本体由
/// custom 域承载）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomCommandRef {
  /// 命令名
  pub name: String,
  /// 命令 ID
  pub id: u16,
  /// arity（0 = 不校验；负值 = 至少 -arity-1 个参数）
  pub arity: i32,
}

/// libs/server/Resp/RespServerSession.cs:RespServerSession
///
/// RESP 服务器会话
pub struct RespServerSession {
  /// 会话标识（CLIENT ID / CLIENT INFO 使用；C# Id）
  pub id: i64,
  /// 会话创建时刻（毫秒 tick；C# CreationTicks = Environment.TickCount64）
  pub creation_ticks: i64,
  /// 对端端点（C# networkSender.RemoteEndpointName；无网络发送器为空串）
  pub remote_endpoint: String,
  /// 本地端点（C# networkSender.LocalEndpointName）
  pub local_endpoint: String,
  /// ASYNC 命令切换的异步处理模式（C# useAsync，NetworkASYNC 置位）
  pub use_async: bool,

  /// RESP 协议版本（RESP2 默认；HELLO \[3\] 升级；C# respProtocolVersion）
  pub resp_protocol_version: u8,
  /// 客户端名（CLIENT SETNAME / HELLO SETNAME；C# clientName）
  pub client_name: Option<String>,
  /// 客户端库名（CLIENT SETINFO LIB-NAME）
  pub client_lib_name: Option<String>,
  /// 客户端库版本（CLIENT SETINFO LIB-VER）
  pub client_lib_version: Option<String>,

  /// 当前认证用户（C# _userHandle；ACL 域为并行转写，此持用户名）
  pub user_handle: Option<String>,
  /// 当前生效用户句柄（C# _userHandle；ACL 档认证成功挂载，None = 未认证）
  pub(crate) acl_user_handle: Option<Arc<UserHandle>>,
  /// 认证器是否支持认证（C# _authenticator.CanAuthenticate；NoAuth 为 false）
  pub authenticator_can_authenticate: bool,

  /// ASKING 跳过计数（C# SessionAsking）
  pub session_asking: u8,
  /// 是否为只读会话（C# readOnlySession）
  pub read_only_session: bool,
  /// 当前活跃库编号（C# activeDatabaseId）
  pub active_db_id: i32,
  /// 是否允许切换非 0 库（C# allowMultiDb）
  pub allow_multi_db: bool,
  /// 最大库数上界（C# serverOptions.MaxDatabases；SELECT/SWAPDB 校验用）
  pub max_databases: i32,
  /// 数据库会话槽位表（下标即 DB 编号；C# databaseSessions）
  database_sessions: Vec<Option<DatabaseSessionSlot>>,
  /// 副本一致读会话是否开启（C# IsConsistentReadSessionActive）
  pub is_consistent_read_session_active: bool,
  /// 是否处于发布订阅模式（C# isSubscriptionSession）
  pub is_subscription_session: bool,
  /// 事务状态镜像（C# txnManager.state 的会话侧视图）
  pub txn_state: TxnState,
  /// 本批消息是否已标记 AOF 阻塞等待（C# waitForAofBlocking）
  pub wait_for_aof_blocking: bool,
  /// 本批是否含慢命令（NET_RS 直方图分桶；C# containsSlowCommand）
  pub contains_slow_command: bool,
  /// 命令执行期写出的错误标记（CommandStats 失败判定；C# commandErrorWritten）
  pub command_error_written: bool,
  /// 会话是否待释放（QUIT；C# toDispose；网络泵发尽应答后断连）
  pub to_dispose: bool,

  /// 会话指标（C# sessionMetrics；采样关闭时为 None）
  pub session_metrics: Option<GarnetSessionMetrics>,
  /// 延迟指标（C# LatencyMetrics；监视关闭时为 None；监视器迭代时钟由
  /// 延迟指标实例内部持有 —— C# LatencyMetrics._monitorIterations）
  latency_metrics: Option<Arc<GarnetLatencyMetricsSession>>,

  /// 解析态（C# parseState）
  pub parse_state: SessionParseState,
  /// 接收缓冲（C# recvBufferPtr 固定接收缓冲的托管等价；parser 分片读写）
  pub recv_buffer: Vec<u8>,
  /// 已接收字节数（C# bytesRead；parser 分片读写）
  pub bytes_read: usize,
  /// 读游标（C# readHead；成功解析后停在命令负载起点）
  pub read_head: usize,
  /// 当前命令尾游标（C# endReadHead）
  pub end_read_head: usize,
  /// 协议违规哨兵：畸形 RESP 帧（C# RespParsingException 抛出即断连），
  /// 携带异常 message 文案（RespParsingException.cs:Throw* 族）。解析层
  /// 置位、[`Self::try_consume_messages`] 消费：先写
  /// `ERR Protocol Error: {msg}` 落累积输出再回传 None，由调用方发尽应答
  /// 后关闭连接（C# RespServerSession.cs:522-537 catch 块顺序）
  pub parse_violation: Option<String>,
  /// 致命断流哨兵（C# GarnetException 且 DisposeSession=true 的
  /// clientResponse:false 形态：集群切面命令处理失败上抛等价）。命令层
  /// 置位、[`Self::try_consume_messages`] 消费：不写错误应答行，发尽累积
  /// 应答后回传 None，由调用方发出后关闭连接（C# RespServerSession.cs
  /// catch(GarnetException) 块 `ex.ClientResponse == false` 分支）
  pub fatal_disconnect: bool,
  /// 输出缓冲（C# networkSender 响应对象 + dcurr/dend 游标的托管等价；分片写）
  pub output: Vec<u8>,
  /// 累计冲洗字节数（Send 累计，测试断言用）
  flushed_bytes: u64,

  /// 当前待执行自定义命令（C# currentCustomTransaction / Procedure 等共用槽）
  pub current_custom_command: Option<(RespCommand, CustomCommandRef)>,
  /// MRU 命令缓存（C# _cachedCmd0/1；resp_command 域维护）
  pub(crate) mru_cache: MruCommandCache,
  /// 会话脚本缓存（EnableLua 时创建；C# sessionScriptCache）
  pub(crate) session_script_cache: Option<SessionScriptCache>,
  /// 全局脚本缓存（C# storeWrapper.storeScriptCache，进程级共享）
  pub(crate) store_script_cache: Arc<StoreScriptCache>,
  /// Lua 会话选项（C# serverOptions.LuaOptions 的会话侧投影；脚本命令
  /// 装配 LuaSessionContext 的取值源）
  pub(crate) lua_options: LuaOptions,
  /// Lua 事务模式（C# serverOptions.LuaTransactionMode；runner 构造下传）
  pub(crate) lua_txn_mode: bool,
  /// 内建命令分派器（宿主持存储执行域注入；None 时命令解析/门控仍闭环，
  /// 落到分派的命令写明错误，绝不静默）。慢路径分派（`Ok(false)` 降级）
  /// 在 exec 内经此克隆句柄构造 [`SlowWait`]
  pub(crate) garnet_api: Option<GarnetApi>,

  /// EnableDebugCommand 镜像（C# storeWrapper.serverOptions.EnableDebugCommand）
  connection_protection_debug: ConnectionProtectionOption,

  /// 集群会话切面（C# clusterSession；None = 单机形态，命令路径与
  /// C# clusterSession == null 分支一致）
  pub(crate) cluster_session: Option<ClusterSession>,

  /// 集合项经纪（C# storeWrapper.itemBroker；None = 装配未注入，阻塞命令
  /// 走立即可取降级路径）
  pub(crate) item_broker: Option<Arc<ItemBroker>>,

  /// 挂起中的阻塞命令等待体（网络泵 take 后 await 驱动；C# 由网络线程
  /// BlockingWait 内联承担）
  pub(crate) pending_block: Option<BlockedWait>,

  /// 挂起中的慢路径执行体（SCAN/KEYS/DBSIZE/CLUSTER RESET 等同步段
  /// 返回 Ok(false) 的命令；网络泵 take 后 await 驱动，应答按流水线
  /// 顺序写回——C# 网络线程同步执行慢命令的 compio 异步等价物）
  pub(crate) pending_slow: Option<SlowWait>,

  /// 运行时配置（C# storeWrapper.runtimeConfig；OBJECT_SCAN_COUNT_LIMIT 等
  /// 热更读取源，CONFIG SET 经同一实例生效）
  pub(crate) runtime_config: Arc<RuntimeServerConfig>,

  /// 事务管理器（C# txnManager；构造期注入 WatchVersionMap 后创建，
  /// None = 宿主未挂事务组件，MULTI 按未接线报错）
  /// C# txnManager；构造期注入 WatchVersionMap 后创建，None = 宿主未挂
  /// 事务组件，MULTI 按未接线报错。锁表守卫使 TransactionManager 类型层面
  /// !Send，而会话消费为单线程串行（compio 任务亲和）：守卫的获取与释放
  /// 同线程，跨线程转移仅发生在无在途事务的消费间隙 —— Send 由该不变量
  /// 人工担保（[`GarnetApi`] 同款论证）
  pub(crate) txn_manager: Option<TransactionManager>,
  /// 自定义命令注册表（C# storeWrapper.customCommandManager；RUNTXP 过程体
  /// 解析用，None = 宿主未装配）
  pub(crate) custom_command_manager: Option<wcustom::SharedCustomCommandManager>,
  /// 发布订阅会话接线（C# subscribeBroker 字段 + numActiveChannels；
  /// 默认无 broker = --pubsub 关闭形态，命令面按同款禁用文案回错）
  pub pubsub: wpubsub::PubSubSession,
  /// 慢日志容器（C# storeWrapper.slowLogContainer；None = 未启用）
  pub(crate) slow_log_container: Option<Arc<SlowLogContainer>>,
  /// 全局延迟指标（C# storeWrapper.monitor.GlobalMetrics.globalLatencyMetrics）
  pub(crate) global_latency_metrics: Option<Arc<parking_lot::Mutex<GarnetLatencyMetrics>>>,
  /// 慢日志批次起始 tick（C# slowLogStartTime；try_consume_messages 入口刷新）
  pub(crate) slow_log_start_ticks: i64,
  /// ACL 认证器（C# _authenticator 的 ACL 档实例；None = NoAuth 档。
  /// authenticate 记录句柄需 &mut，以 parking_lot 互斥承接）
  pub(crate) acl_authenticator: Option<Arc<parking_lot::Mutex<GarnetAclAuthenticator>>>,
  /// ACL 认证设置（C# storeWrapper.aclSettings；ACL LOAD/SAVE 配置文件定位）
  pub(crate) acl_settings: Option<Arc<AclAuthenticationSettings>>,
}

impl RespServerSession {
  /// libs/server/Resp/RespServerSession.cs:RespServerSession（构造）
  ///
  /// 创建默认库会话（DB 0）并置为活跃；认证默认用户（C# 构造尾部
  /// AuthenticateUser(defaultUser)）。
  pub fn new(id: i64, options: RespServerSessionOptions) -> Self {
    let max_slots = options.max_databases.max(1) as usize;
    let mut database_sessions: Vec<Option<DatabaseSessionSlot>> = vec![None; max_slots];
    // 创建默认 DB 会话并注册（C# CreateDatabaseSession(0) + TrySetValue）
    database_sessions[0] = Some(DatabaseSessionSlot {
      id: 0,
      created_ticks: session_now_ms(),
    });

    let latency_metrics = options.latency_monitor.then(|| {
      Arc::new(GarnetLatencyMetricsSession::new(
        Arc::new(AtomicU64::new(0)),
        GarnetLatencyMetricsSession::DEFAULT_LATENCY_TYPES,
      ))
    });

    let mut session = Self {
      id,
      creation_ticks: session_now_ms(),
      remote_endpoint: String::new(),
      local_endpoint: String::new(),
      use_async: false,
      resp_protocol_version: DEFAULT_RESP_VERSION,
      client_name: None,
      client_lib_name: None,
      client_lib_version: None,
      user_handle: None,
      acl_user_handle: None,
      authenticator_can_authenticate: false,
      session_asking: 0,
      read_only_session: false,
      active_db_id: 0,
      allow_multi_db: options.allow_multi_db,
      max_databases: options.max_databases,
      database_sessions,
      is_consistent_read_session_active: false,
      is_subscription_session: false,
      txn_state: TxnState::None,
      wait_for_aof_blocking: false,
      contains_slow_command: false,
      command_error_written: false,
      to_dispose: false,
      session_metrics: options
        .metrics_sampling_frequency
        .then(GarnetSessionMetrics::default),
      latency_metrics,
      parse_state: SessionParseState::new(),
      recv_buffer: Vec::with_capacity(1 << 16),
      bytes_read: 0,
      read_head: 0,
      end_read_head: 0,
      parse_violation: None,
      fatal_disconnect: false,
      output: Vec::with_capacity(1 << 16),
      flushed_bytes: 0,
      current_custom_command: None,
      connection_protection_debug: options.enable_debug_command,
      mru_cache: Default::default(),
      session_script_cache: options.enable_lua.then(|| {
        // C# SessionScriptCache 构造注入 timeoutManager（服务装配期按
        // 「超时非无限」创建的共享单例；None = 无超时形态）。
        let mut cache = SessionScriptCache::default();
        if let Some(manager) = &options.lua_timeout_manager {
          cache.set_timeout_manager(Arc::clone(manager));
        }
        cache
      }),
      store_script_cache: Arc::new(StoreScriptCache::default()),
      lua_options: options.lua_options,
      lua_txn_mode: options.lua_txn_mode,
      garnet_api: None,
      cluster_session: None,
      item_broker: None,
      pending_block: None,
      pending_slow: None,
      runtime_config: RuntimeServerConfig::shared_default(),
      txn_manager: None,
      custom_command_manager: None,
      pubsub: PubSubSession::with_mailbox_capacity(None, 4),
      slow_log_container: None,
      global_latency_metrics: None,
      slow_log_start_ticks: 0,
      acl_authenticator: None,
      acl_settings: None,
    };
    // C# 构造尾部 AuthenticateUser(defaultUser)：NoAuth 档即落到默认用户
    //（GetDefaultUserHandle 兜底）；ACL 档挂载前为空操作
    session.authenticate_user(options.default_user.as_bytes(), &[]);
    session
  }

  /// 注入存储执行域（对标 C# RespServerSession 构造函数的 storeWrapper/
  /// basicGarnetApi 注入形态：命令执行面由宿主装配后注入，单机与集群同径）
  pub fn set_garnet_api(&mut self, api: impl Into<GarnetApi>) {
    self.garnet_api = Some(api.into());
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

  /// 注入事务组件（C# 构造函数 `new TransactionManager(storeWrapper.watchversionMap, ...)`
  /// 的依赖倒置形态；AOF 事务日志由 wtxn 默认无日志形态承接）
  pub fn attach_transaction_components(&mut self, watch_version_map: Arc<WatchVersionMap>) {
    self.txn_manager = Some(TransactionManager::new(watch_version_map, None));
  }

  /// 注入自定义命令注册表（C# storeWrapper.customCommandManager；RUNTXP
  /// 过程体解析与自定义命令族共用）
  pub fn attach_custom_command_manager(&mut self, manager: wcustom::SharedCustomCommandManager) {
    self.custom_command_manager = Some(manager);
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

  /// 注入全局延迟指标（C# storeWrapper.monitor.GlobalMetrics.globalLatencyMetrics）
  pub fn set_global_latency_metrics(
    &mut self,
    metrics: Arc<parking_lot::Mutex<GarnetLatencyMetrics>>,
  ) {
    self.global_latency_metrics = Some(metrics);
  }

  /// 注入 ACL 认证器与设置（C# 构造函数 `_authenticator` + `storeWrapper.serverOptions`
  /// 的 ACL 档装配；None 分量分别承接 NoAuth 档与无 ACL 文件形态）。
  /// ACL 档挂载即置 CanAuthenticate（C# GarnetACLAuthenticator.CanAuthenticate = true），
  /// 并按 C# 构造尾部 AuthenticateUser(defaultUser) 关联 default 用户并尽可能自动认证
  pub fn attach_acl(
    &mut self,
    authenticator: Option<Arc<parking_lot::Mutex<GarnetAclAuthenticator>>>,
    settings: Option<Arc<AclAuthenticationSettings>>,
  ) {
    self.authenticator_can_authenticate = authenticator.is_some();
    self.acl_authenticator = authenticator;
    self.acl_settings = settings;
    // 认证域切换，先回落未认证态（C# 构造期 _userHandle 为 null），再重放
    // 构造尾部 AuthenticateUser(defaultUser)：nopass default 用户认证成功 →
    // acl_user_handle 挂载（全部命令放行）；带口令 default 用户失败 → 保持
    // 未认证，非 NoAuth 命令按 C# NOAUTH 拒绝
    self.user_handle = None;
    self.acl_user_handle = None;
    self.authenticate_user("default".as_bytes(), &[]);
  }

  /// 集合更新唤醒（C# StorageSession ListOps/SortedSetOps 写成功后
  /// `itemBroker?.HandleCollectionUpdate(key)`——阻塞观察者经经纪主循环
  /// 试取指派；无经纪或键无观察者均为无害空操作）
  pub(crate) fn notify_collection_update(&self, key: &[u8]) {
    if let Some(broker) = &self.item_broker {
      broker.handle_collection_update(key);
    }
  }

  /// 经纪注入时挂起阻塞命令（登记观察者 + pending_block，由网络泵驱动；懒求值入参）
  pub(crate) fn park_broker_wait(
    &mut self,
    command: RespCommand,
    timeout: f64,
    keys: impl FnOnce() -> Vec<Vec<u8>>,
    cmd_args: impl FnOnce() -> Vec<Vec<u8>>,
  ) -> bool {
    let Some(broker) = &self.item_broker else {
      return false;
    };
    let observer = broker.start_wait(command, keys(), self.id as usize, cmd_args());
    self.pending_block = Some(BlockedWait::new(
      Arc::clone(broker),
      observer,
      command,
      timeout,
    ));
    true
  }

  /// 取走挂起中的阻塞等待（网络泵专属：await 驱动至完成后经
  /// [`Self::resolve_blocked_wait`] 写回应答）
  pub fn take_blocked_wait(&mut self) -> Option<BlockedWait> {
    self.pending_block.take()
  }

  /// 取走挂起中的慢路径执行体（网络泵专属：await 驱动至完成后把应答
  /// 字节按流水线顺序写回；对照 C# 慢命令在网络线程内同步执行的整段语义）
  pub fn take_slow_wait(&mut self) -> Option<SlowWait> {
    self.pending_slow.take()
  }

  /// 阻塞等待完成后的应答写出：直接追加到目标写缓冲（零中间堆分配与二次拷贝）
  ///（C# 各阻塞命令尾部 BlockingWait 之后的 switch 应答段）
  pub fn resolve_blocked_wait_into(
    &mut self,
    cmd: RespCommand,
    result: CollectionItemResult,
    resp_buf: &mut Vec<u8>,
  ) {
    self.take_output_into(resp_buf);
    let start_len = resp_buf.len();
    super::objects::list_commands::write_collection_item_result(cmd, &result, resp_buf);
    let written = resp_buf.len() - start_len;
    self.flushed_bytes += written as u64;
    if let Some(metrics) = &mut self.session_metrics {
      metrics.incr_total_net_output_bytes(written as u64);
    }
  }

  /// 阻塞等待完成后的应答写出；返回应答字节（向后兼容接口）
  pub fn resolve_blocked_wait(
    &mut self,
    cmd: RespCommand,
    result: CollectionItemResult,
  ) -> Vec<u8> {
    let mut out = Vec::new();
    self.resolve_blocked_wait_into(cmd, result, &mut out);
    out
  }

  /// 挂接集群会话切面（C# 构造函数 `cp?.CreateClusterSession(...)` 的依赖
  /// 倒置形态：集群域实现由宿主构造后注入，单机形态保持 None）
  pub fn attach_cluster_session(&mut self, cluster_session: impl Into<ClusterSession>) {
    self.cluster_session = Some(cluster_session.into());
  }

  /// libs/server/Resp/RespServerSession.cs:Dispose
  ///
  /// 会话析构：释放集群会话切面（C# clusterSession?.Dispose()）；数据库会话
  /// 槽与订阅清理由各自并行域承接；挂起中的阻塞等待经经纪置 SessionDisposed
  ///（C# broker.HandleSessionDisposed）解除网络泵等待
  pub fn dispose(&mut self) {
    if let Some(block) = self.pending_block.take() {
      block.abort();
    }
    // 慢路径执行体无外部登记方，drop future 即取消（compio 任务自清理）
    self.pending_slow.take();
    // 摘除本会话全部订阅（C# Dispose 尾部 subscribeBroker?.RemoveSubscription；
    // 幽灵订阅会令 PUBLISH 计数虚高并向已断连邮箱投递）
    if let Some(broker) = self.pubsub.broker() {
      broker.remove_subscription(self.id as u64);
    }
    if let Some(cluster) = self.cluster_session.take() {
      cluster.dispose();
    }
    self.merge_metrics_history_session_dispose();
  }

  /// libs/server/Resp/RespServerSessionSlotVerify.cs:CanServeSlot
  ///
  /// 数据命令按命令键规格提取键位交集群切面校验槽位归属，MOVED/ASK 等
  /// 重定向错误由切面直写输出缓冲；非数据命令与无键规格命令放行。
  /// 事务运行态跳过（C# NetworkMultiKeySlotVerify 首行判定；事务键校验由
  /// 事务域承接）。自定义命令注册表为空（custom 域并行转写），按 C# 空
  /// 注册表行为不进入数据命令分支。
  fn can_serve_slot(&mut self, cmd: RespCommand) -> SlotVerifyGate {
    if self.txn_state == TxnState::Running || !is_data_command(cmd) {
      return SlotVerifyGate::Serve;
    }
    let Some(info) = try_get_simple_resp_command_info(normalize_for_acls(cmd)) else {
      // C#：命令信息缺失（json 解析失败）时拒绝服务
      return SlotVerifyGate::Redirected;
    };
    if info.key_specs.is_empty() {
      return SlotVerifyGate::Serve;
    }
    let input = ClusterSlotVerificationInput {
      key_specs: &info.key_specs,
      // BITOP 的操作参数被解析器吞掉，键下标需 -2 偏移（同子命令）
      is_sub_command: info.is_sub_command || cmd == RespCommand::Bitop,
      read_only: is_read_only(cmd),
      session_asking: self.session_asking,
      wait_for_stable_slot: matches!(
        cmd,
        RespCommand::Vadd | RespCommand::Vrem | RespCommand::Vsetattr
      ),
    };
    let Some(cluster) = &self.cluster_session else {
      return SlotVerifyGate::Serve;
    };
    let args = self.get_arg_slices();
    cluster.network_multi_key_slot_verify(&input, &args, &mut self.output)
  }

  /// 经注入的存储执行域分派（底层指针与函数指针借用解耦；C# 对应
  /// Process 链末端 ProcessAdminCommands 的兜底形态）
  fn dispatch_via_garnet_api(&mut self, cmd: RespCommand, args: &[&[u8]]) {
    let (ptr, exec) = match &self.garnet_api {
      Some(api) => api.raw_exec_parts(),
      None => {
        // 存储执行域未挂载 = 宿主装配缺口：写明错误并告警，绝不静默吞命令
        log::error!("存储执行域未挂载，命令 {cmd:?} 被拒绝");
        self.abort_error_message(ERR_STORE_DOMAIN_NOT_ATTACHED);
        return;
      }
    };
    unsafe { (exec)(ptr, self, cmd, args) };
  }

  /// libs/server/Resp/RespServerSession.cs:GetLatencyMetrics
  pub fn get_latency_metrics(&self) -> Option<Arc<GarnetLatencyMetricsSession>> {
    self.latency_metrics.clone()
  }

  /// libs/server/Resp/RespServerSession.cs:ResetLatencyMetrics
  pub fn reset_latency_metrics(&self, latency_event: LatencyMetricsType) {
    if let Some(metrics) = &self.latency_metrics {
      metrics.reset(latency_event);
    }
  }

  /// libs/server/Resp/RespServerSession.cs:ResetAllLatencyMetrics
  pub fn reset_all_latency_metrics(&self) {
    if let Some(metrics) = &self.latency_metrics {
      metrics.reset_all();
    }
  }

  /// libs/server/Resp/RespServerSession.cs:GetDatabaseSessionsSnapshot
  ///
  /// 全部已创建的数据库会话（按库 ID 升序）
  pub fn get_database_sessions_snapshot(&self) -> Vec<DatabaseSessionSlot> {
    self.database_sessions.iter().flatten().cloned().collect()
  }

  /// libs/server/Resp/RespServerSession.cs:SetUserHandle
  ///
  /// 挂载用户句柄：同步刷新会话用户名（CLIENT LIST / SLOWLOG 展示面）
  pub fn set_user_handle(&mut self, user_handle: Arc<wacl::UserHandle>) {
    self.user_handle = Some(user_handle.user().name.clone());
    self.acl_user_handle = Some(user_handle);
  }

  /// libs/server/Resp/RespServerSession.cs:UpdateRespProtocolVersion
  pub fn update_resp_protocol_version(&mut self, resp_protocol_version: u8) {
    self.resp_protocol_version = resp_protocol_version;
  }

  /// libs/server/Resp/RespServerSession.cs:AuthenticateUser
  ///
  /// ACL 认证器挂载时按其校验（定位用户 → 口令比对 → 记录句柄
  /// `aclAuthenticator.GetUserHandle()`）；NoAuth 档 CanAuthenticate = false
  /// → 恒返回 false（C# 取 accessControlList.GetDefaultUserHandle 兜底，
  /// rust 无 ACL 实例可取，门控对该档恒放行承接同一网络效果）
  pub fn authenticate_user(&mut self, username: &[u8], password: &[u8]) -> bool {
    if !self.authenticator_can_authenticate {
      // 不支持认证的认证器直接落到默认用户（C# GetDefaultUserHandle 分支）
      if self.user_handle.is_none() {
        self.user_handle = Some("default".to_string());
      }
      return false;
    }
    let Some(acl) = &self.acl_authenticator else {
      return false;
    };
    // 认证器可变态内 &mut（记录用户句柄）；会话消费串行，锁无竞争
    let mut acl = acl.lock();
    let success = acl.authenticate(username, password, acl_password_check);
    if success && let Some(user_handle) = acl.get_user_handle() {
      self.user_handle = Some(user_handle.user().name.clone());
      self.acl_user_handle = Some(Arc::clone(user_handle));
    } else {
      self.user_handle = None;
      self.acl_user_handle = None;
    }
    success
  }

  /// libs/server/Resp/RespServerSession.cs:CanRunDebug
  pub fn can_run_debug(&self) -> bool {
    can_run_with_protection(self.enable_debug_command(), self.is_local_connection())
  }

  /// EnableDebugCommand 配置视图（C# storeWrapper.serverOptions 选项承接）
  fn enable_debug_command(&self) -> ConnectionProtectionOption {
    self.connection_protection_debug
  }

  /// C# networkSender.IsLocalConnection：对端为本地回环 / Unix 套接字
  pub fn is_local_connection(&self) -> bool {
    self.remote_endpoint.starts_with("127.0.0.1")
      || self.remote_endpoint.starts_with("[::1]")
      || self.remote_endpoint.starts_with("unix:")
  }

  /// 协议违规哨兵消费（try_consume_messages 对应的 C# catch 块）：
  /// 会话指标计数（C# catch 首行
  /// `sessionMetrics?.incr_total_number_resp_server_session_exceptions(1)`）
  /// 后写 `ERR Protocol Error: {msg}` 追加到累积输出尾部（C#
  /// `RespWriteUtils.TryWriteError($"ERR Protocol Error: {ex.Message}")`，
  /// 同批此前命令应答在前、错误在后），放行应答面交由调用方 Send 后断连。
  /// 返回是否发生违规
  fn write_protocol_error(&mut self) -> bool {
    if let Some(msg) = self.parse_violation.take() {
      if let Some(metrics) = &mut self.session_metrics {
        metrics.incr_total_number_resp_server_session_exceptions(1);
      }
      self.abort_error_message(&format!("ERR Protocol Error: {msg}"));
      return true;
    }
    false
  }

  /// libs/server/Resp/RespServerSession.cs:TryConsumeMessages
  ///
  /// 解析并分派接收缓冲中的全部完整命令，返回已消费字节数（C# 返回 readHead）。
  /// 分派经 [`GarnetApi`] 注入面；协议违规以 None 表达（C# 抛
  /// RespParsingException，catch 块先写 `ERR Protocol Error: {msg}` 到
  /// 累积输出再断连，rust 同序：错误落在 [`Self::output`] 中此前命令
  /// 应答之后，由调用方发出后断连）。
  ///
  /// 批首尾纪元快照取放（C# :490 `clusterSession?.AcquireCurrentEpoch()` /
  /// :576 finally `clusterSession?.ReleaseCurrentEpoch()`）：批内会话持当前
  /// 纪元快照，批外清零——配置过渡静止等待的观测窗口
  pub fn try_consume_messages(&mut self, req_buffer: &[u8]) -> Option<usize> {
    if let Some(cs) = self.cluster_session.as_ref() {
      cs.acquire_current_epoch();
    }
    let consumed = self.try_consume_messages_body(req_buffer);
    if let Some(cs) = self.cluster_session.as_ref() {
      cs.release_current_epoch();
    }
    consumed
  }

  /// 批消费体（[`Self::try_consume_messages`] 的纪元快照保护段）
  fn try_consume_messages_body(&mut self, req_buffer: &[u8]) -> Option<usize> {
    self.recv_buffer.clear();
    self.recv_buffer.extend_from_slice(req_buffer);
    self.bytes_read = self.recv_buffer.len();
    self.read_head = 0;

    self.enter_and_get_response_object();
    self.process_messages();
    // 协议违规（C# RespParsingException 传播出 ProcessMessages → catch 块：
    // 写 `ERR Protocol Error: {msg}` 追加在累积应答之后 → Send → 断连）。
    // 不回退游标、不计消费字节；None 表达致命错误，应答面（含此前命令
    // 累积应答 + 协议错误）交由调用方发出后断连
    if self.write_protocol_error() {
      self.exit_and_return_response_object();
      return None;
    }
    // 切面致命断流（GarnetException clientResponse:false）：不写错误行，
    // 发尽累积应答后断连（None 通道与协议违规共用）
    if self.fatal_disconnect {
      self.exit_and_return_response_object();
      return None;
    }
    let consumed = self.read_head;
    self.exit_and_return_response_object();

    if let Some(metrics) = &mut self.session_metrics {
      metrics.incr_total_net_input_bytes(consumed as u64);
    }
    Some(consumed)
  }

  /// 泵直读消费入口（scratch 模式，网络泵专属）
  ///
  /// 接收缓冲由泵经 take/return 直填（网络字节零拷贝直入），[`Self::read_head`]
  /// 跨批次持久不回退 —— C# TryConsumeMessages 的 `bytesRead = bytesReceived`+
  /// `readHead` 持久游标模型。事务排队字节（MULTI..EXEC 跨批次）因此驻留
  /// 接收缓冲，EXEC 据此回退重解析排队命令（C# 同源语义，拷贝形态做不到）。
  ///
  /// 返回消费后残余字节数（半包长度；`0` = 整段消费完毕，缓冲清零复位，
  /// 容量保留复用）；`None` = 协议违规（`ERR Protocol Error` 与同批此前
  /// 应答已落 [`Self::output`]，泵发尽后断连）
  pub fn try_consume_pending(&mut self) -> Option<usize> {
    if let Some(cs) = self.cluster_session.as_ref() {
      cs.acquire_current_epoch();
    }
    let remaining = self.try_consume_pending_body();
    if let Some(cs) = self.cluster_session.as_ref() {
      cs.release_current_epoch();
    }
    remaining
  }

  /// 批消费体（[`Self::try_consume_pending`] 的纪元快照保护段）
  fn try_consume_pending_body(&mut self) -> Option<usize> {
    self.bytes_read = self.recv_buffer.len();
    let prev_read_head = self.read_head;

    self.enter_and_get_response_object();
    self.process_messages();
    // 协议违规（C# RespParsingException → catch 块写协议错误 → Send 累积
    // 应答 → DisposeNetworkSender）：游标不回退，None 表达致命错误，
    // 由泵发尽应答（含协议错误）后关闭连接
    if self.write_protocol_error() {
      self.exit_and_return_response_object();
      return None;
    }

    // 切面致命断流（GarnetException clientResponse:false）：不写错误行，
    // 发尽累积应答后断连（None 通道与协议违规共用）
    if self.fatal_disconnect {
      self.exit_and_return_response_object();
      return None;
    }

    // 本轮新增消费字节（EXEC 回退重解析可致游标暂时回退，saturating 兜底）
    let newly_consumed = self.read_head.saturating_sub(prev_read_head);
    // 事务在途（C# IsSkippingOperations / `if (!txnSkip) readHead = 0` 对偶
    // 语义）：排队字节与 txn_start_head 偏移必须驻留缓冲供 EXEC 回退重解析，
    // 禁止清零复位
    if self.read_head >= self.bytes_read && self.txn_state == TxnState::None {
      // 整段消费完毕且无在途事务：缓冲清零复位（平移仅在整段消费完执行，
      // offset/解析指针同时失效安全 —— C# ShiftNetworkReceiveBuffer 的托管
      // 等价）；超大批次容量释放，回归默认驻留水位
      if self.recv_buffer.capacity() > DEFAULT_RECV_BUFFER_CAPACITY {
        self.recv_buffer = Vec::with_capacity(DEFAULT_RECV_BUFFER_CAPACITY);
      } else {
        self.recv_buffer.clear();
      }
      self.bytes_read = 0;
      self.read_head = 0;
      self.end_read_head = 0;
    }
    self.exit_and_return_response_object();

    if let Some(metrics) = &mut self.session_metrics {
      metrics.incr_total_net_input_bytes(newly_consumed as u64);
    }
    Some(self.bytes_read.saturating_sub(self.read_head))
  }

  /// libs/server/Resp/RespServerSession.cs:ProcessMessages
  ///
  /// 主循环：解析 → ACL 门（C# :653 CheckACLPermissions(cmd) &&
  /// CheckScriptPermissions(cmd)，rust 脚本域未维护 no-script 位图，等价 C#
  /// 位图 null 的放行路径，门仅承载 ACL）→ 订阅模式/事务/槽位门 → 分派 →
  /// 指标；被拒命令回 NOPERM/NOAUTH（C# :688-706 else 分支）。命令未完整
  /// 到达时双游标回退到本轮起点（C# `endReadHead = readHead = _origReadHead`）。
  pub fn process_messages(&mut self) {
    // 挂起中的阻塞/慢路径命令未完成前不再消费新命令（C# 网络线程
    // BlockingWait 期间本就读不到后续命令）
    if self.pending_block.is_some() || self.pending_slow.is_some() {
      return;
    }

    self.slow_log_start_ticks = now_stopwatch_ticks();
    let mut orig_read_head = self.read_head;

    while self.bytes_read.saturating_sub(self.read_head) >= 4 {
      // 解析命令；未完整到达则回退双游标本轮起点并跳出（C# commandReceived）；
      // 协议违规（C# RespParsingException）保持游标原样跳出，错误由消费
      // 入口 catch 等价物 write_protocol_error 落输出，连接由上层关闭
      let cmd = match self.parse_command() {
        Some(cmd) => cmd,
        None => {
          if self.parse_violation.is_some() {
            break;
          }
          self.read_head = orig_read_head;
          self.end_read_head = orig_read_head;
          break;
        }
      };

      if cmd != RespCommand::Invalid {
        // C# ACL 门（RespServerSession.cs:653 CheckACLPermissions(cmd) &&
        // CheckScriptPermissions(cmd)）：脚本 no-script 位图域未承接（等价
        // C# 位图 null 的放行路径），门仅承载 ACL
        if self.check_acl_permissions(cmd) {
          // RESP2 订阅模式仅放行 (P|S)SUBSCRIBE/(P|S)UNSUBSCRIBE/PING/QUIT/RESET
          //（libs/server/Resp/Parser/RespCommand.cs:IsAllowedInSubscriptionMode）
          if self.is_subscription_session
            && self.resp_protocol_version == 2
            && !is_allowed_in_subscription_mode(cmd)
          {
            // 对标 libs/server/Resp/RespServerSession.cs:659（string.Format(
            // CmdStrings.GenericPubSubCommandNotAllowed, cmd.ToString())）
            let name = super::resp_commands_info_data::resp_command_to_cs_name(cmd);
            self.write_error_response(&cs::GENERIC_PUBSUB_COMMAND_NOT_ALLOWED.replace("{0}", name));
          } else if self.txn_state != TxnState::None {
            // C# 事务门：Running 直通（事务 API 与单机同一执行路径）；
            // Started 排队（EXEC/MULTI/DISCARD/QUIT 特例，余者 NetworkSKIP）
            self.process_transactional_command(cmd);
          } else if self.cluster_session.is_none() {
            // C# 分派链：ProcessBasicCommands → ProcessArrayCommands →
            // ProcessOtherCommands（事务入队/直通形态由分派域承载）；
            // 集群门控：clusterSession == null || CanServeSlot(cmd)
            self.process_basic_commands(cmd);
          } else {
            match self.can_serve_slot(cmd) {
              SlotVerifyGate::Serve => {
                self.process_basic_commands(cmd);
              }
              SlotVerifyGate::Redirected => {} // 重定向/错误已写出
              SlotVerifyGate::Wait => {
                // C# CanOperateOnKey / WaitForSlotToStabalize 网络线程内联
                // 自旋的挂起投影：切面已登记等待体，取走转挂会话泵，回退
                // 游标至本命令起点停止消费；等待体驱动至迁移推进/超时后
                // 重评本命令
                if let Some(slow) = self
                  .cluster_session
                  .as_ref()
                  .and_then(|c| c.take_pending_slow())
                {
                  self.pending_slow = Some(slow);
                  self.read_head = orig_read_head;
                  self.end_read_head = orig_read_head;
                  break;
                }
                // 切面未登记等待体（装配缺口）：跳过本命令防消费忙转
              }
            }
          }
        } else {
          // C# :688-706 else 分支：已认证 → NOPERM；未认证 → NOAUTH
          self.write_acl_permission_error(self.acl_user_handle.is_some());
        }

        self.handle_slow_log(cmd);

        if let Some(metrics) = &mut self.session_metrics {
          metrics.incr_total_commands_processed(1);
          metrics.add_total_write_commands_processed(one_if_write(cmd));
          metrics.add_total_read_commands_processed(one_if_read(cmd));
          if self.command_error_written {
            self.command_error_written = false;
          }
        }
      } else {
        self.contains_slow_command = true;
      }

      // 推进游标处理下一条命令（C# _origReadHead = readHead = endReadHead）
      self.read_head = self.end_read_head;
      orig_read_head = self.read_head;
      if self.session_asking != 0 {
        self.session_asking -= 1;
      }

      // 阻塞命令 / 慢路径命令挂起：停止消费本批后续命令（C# 网络线程
      // 阻塞等价物，后续命令由网络泵驱动等待完成后继续消费）
      if self.pending_block.is_some() || self.pending_slow.is_some() {
        break;
      }
    }
  }

  /// libs/server/Resp/RespServerSession.cs:EnterAndGetResponseObject
  ///
  /// 拿取响应对象（rust 托管缓冲：进入会话输出期）
  pub fn enter_and_get_response_object(&mut self) {
    self.output.clear();
  }

  /// libs/server/Resp/RespServerSession.cs:ExitAndReturnResponseObject
  pub fn exit_and_return_response_object(&mut self) {
    // C# 归还响应对象并将 dcurr/dend 清零；托管缓冲保留待复用
  }

  /// libs/server/Resp/RespServerSession.cs:SetTransactionMode
  pub fn set_transaction_mode(&mut self, enable: bool) {
    self.txn_state = if enable {
      TxnState::Running
    } else {
      TxnState::None
    };
  }

  /// libs/server/Resp/RespServerSession.cs:MakeUpperCase
  ///
  /// 就地大写化首条命令名；返回是否发生改写。C# 位技巧快路径按
  /// "常见命令已大写" 假设跳过全扫描，语义保留。
  pub fn make_upper_case(&mut self, ptr: usize, len: usize) -> bool {
    let buffer = &mut self.recv_buffer;
    let end = (ptr + len).min(buffer.len());
    // 常见场景：命令名已全大写 → 不改写（返回 false）
    let mut changed = false;
    let mut i = ptr;
    while i < end {
      if buffer[i] > 64 {
        // 找到命令名起点
        while i < end && buffer[i] > 32 && buffer[i] < 123 {
          if buffer[i] > 96 {
            buffer[i] -= 32;
            changed = true;
          }
          i += 1;
        }
        return changed;
      }
      i += 1;
    }
    false
  }

  /// libs/server/Resp/RespServerSession.cs:ProcessBasicCommands
  ///
  /// fast 命令族分派（WARNING: 仅 @fast 命令，慢命令走 OtherCommands）。
  /// 命令实现位于 resp 命令文件（并行域），经 [`GarnetApi`] 注入面
  /// 接入；PING/QUIT/事务族在会话侧闭环。
  pub fn process_basic_commands(&mut self, cmd: RespCommand) -> bool {
    match cmd {
      RespCommand::Ping => {
        if self.parse_state.count == 0 {
          // C# NetworkPING：+PONG
          self.output.extend_from_slice(b"+PONG\r\n");
        } else if self.parse_state.count == 1 {
          // C# NetworkArrayPING: bulk string message
          let msg = self.parse_state.get_arg_slice_by_ref(0).as_slice();
          self.output.write_resp_bulk_string(msg);
        } else {
          self.abort_wrong_num_args("PING");
        }
        true
      }
      RespCommand::Asking => {
        if self.parse_state.count != 0 {
          self.abort_wrong_num_args("ASKING");
          return true;
        }
        self.session_asking = 2;
        self.output.extend_from_slice(b"+OK\r\n");
        true
      }
      RespCommand::Quit => {
        self.to_dispose = true;
        self.output.extend_from_slice(b"+OK\r\n");
        true
      }
      RespCommand::Readonly => {
        self.read_only_session = true;
        // C# NetworkREADONLY：clusterSession?.SetReadOnlySession()
        if let Some(cluster) = &self.cluster_session {
          cluster.set_read_only_session();
        }
        self.output.extend_from_slice(b"+OK\r\n");
        true
      }
      RespCommand::Readwrite => {
        self.read_only_session = false;
        // C# NetworkREADWRITE：clusterSession?.SetReadWriteSession()
        if let Some(cluster) = &self.cluster_session {
          cluster.set_read_write_session();
        }
        self.output.extend_from_slice(b"+OK\r\n");
        true
      }
      // C# ProcessBasicCommands switch：MULTI / EXEC / DISCARD / UNWATCH / RUNTXP
      RespCommand::Multi => self.network_multi(),
      RespCommand::Exec => self.network_exec(),
      RespCommand::Discard => self.network_discard(),
      RespCommand::Unwatch => self.network_unwatch(),
      // C# ProcessBasicCommands：RUNTXP
      //（libs/server/Resp/RespServerSession.cs:RespCommand.RUNTXP）
      RespCommand::Runtxp => self.network_runtxp(),
      // C# 链式回退：fast 表未命中的命令继续走 array → other 分派链
      _ => self.process_array_commands(cmd),
    }
  }

  /// libs/server/Resp/RespServerSession.cs:ProcessArrayCommands
  ///
  /// @fast 数组族会话级命令（WARNING: 仅 @fast，慢命令走 OtherCommands）；
  /// 存储面数组命令经 [`GarnetApi`] 注入面承接。
  pub fn process_array_commands(&mut self, cmd: RespCommand) -> bool {
    match cmd {
      // C# ProcessArrayCommands：WATCH / WATCHMS / WATCHOS
      RespCommand::Watch => self.network_watch(),
      RespCommand::Watchms => self.network_watch_ms(),
      RespCommand::Watchos => self.network_watch_os(),
      // 发布订阅族（C# ProcessArrayCommands 的 pub/sub 段；wire 为会话自持）
      RespCommand::Subscribe => self.process_pubsub_command(cmd, false),
      RespCommand::Ssubscribe => self.process_pubsub_command(cmd, true),
      RespCommand::Psubscribe => self.process_pubsub_command(cmd, false),
      RespCommand::Unsubscribe | RespCommand::Punsubscribe => {
        self.process_pubsub_command(cmd, false)
      }
      RespCommand::Publish => self.process_pubsub_command(cmd, false),
      RespCommand::Spublish => self.process_pubsub_command(cmd, true),
      RespCommand::PubsubChannels | RespCommand::PubsubNumsub | RespCommand::PubsubNumpat => {
        self.process_pubsub_command(cmd, false)
      }
      // C# 链式回退末端：未归类命令走 other（慢命令）分派
      _ => self.process_other_commands(cmd),
    }
  }

  /// 事务门分派（C# ProcessMessages 的 `txnManager.state != TxnState.None` 分支）
  fn process_transactional_command(&mut self, cmd: RespCommand) -> bool {
    if self.txn_state == TxnState::Running {
      // C# ProcessBasicCommands(cmd, ref transactionalApi)：事务执行态直通
      return self.process_basic_commands(cmd);
    }
    match cmd {
      RespCommand::Exec => self.network_exec(),
      RespCommand::Multi => self.network_multi(),
      RespCommand::Discard => self.network_discard(),
      RespCommand::Quit => {
        self.to_dispose = true;
        self.output.extend_from_slice(b"+OK\r\n");
        true
      }
      _ => self.network_skip(cmd),
    }
  }

  /// pub/sub 命令会话侧统一入参（wire 为会话自持接线，参数取解析态）
  fn process_pubsub_command(&mut self, cmd: RespCommand, shard: bool) -> bool {
    let args = self.get_arg_slices();
    match cmd {
      RespCommand::Subscribe => self.network_subscribe(shard, &args),
      RespCommand::Ssubscribe => self.network_subscribe(true, &args),
      RespCommand::Psubscribe => self.network_psubscribe(&args),
      RespCommand::Unsubscribe => self.network_unsubscribe(&args),
      RespCommand::Punsubscribe => self.network_punsubscribe(&args),
      RespCommand::Publish | RespCommand::Spublish => self.network_publish(shard, &args),
      RespCommand::PubsubChannels => self.network_pubsub_channels(&args),
      RespCommand::PubsubNumsub => self.network_pubsub_numsub(&args),
      RespCommand::PubsubNumpat => self.network_pubsub_numpat(&args),
      _ => true,
    }
  }

  /// MULTI（会话侧路由，实现委托 wtxn::TransactionManager）
  fn network_multi(&mut self) -> bool {
    use crate::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_multi(session))
  }

  /// EXEC（会话侧路由，实现委托 wtxn::TransactionManager）
  fn network_exec(&mut self) -> bool {
    use crate::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_exec(session))
  }

  /// DISCARD（会话侧路由，实现委托 wtxn::TransactionManager）
  fn network_discard(&mut self) -> bool {
    use crate::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_discard(session))
  }

  /// WATCH（会话侧路由，实现委托 wtxn::TransactionManager）
  fn network_watch(&mut self) -> bool {
    use crate::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_watch(session))
  }

  /// WATCHMS（会话侧路由，实现委托 wtxn::TransactionManager）
  fn network_watch_ms(&mut self) -> bool {
    use crate::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_watch_ms(session))
  }

  /// WATCHOS（会话侧路由，实现委托 wtxn::TransactionManager）
  fn network_watch_os(&mut self) -> bool {
    use crate::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_watch_os(session))
  }

  /// UNWATCH（会话侧路由，实现委托 wtxn::TransactionManager）
  fn network_unwatch(&mut self) -> bool {
    use crate::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_unwatch(session))
  }

  /// RUNTXP（会话侧路由，实现委托 wtxn::TransactionManager）；过程体经
  /// 会话注册表解析器实例化执行（C# NetworkRUNTXP + TryTransactionProc）
  fn network_runtxp(&mut self) -> bool {
    use crate::txn_resp_commands::{SessionTxnProcResolver, TxnRespCommandsExt};
    let mut resolver = SessionTxnProcResolver {
      registry: self.custom_command_manager.clone(),
    };
    self.with_txn_manager(|txn, session| txn.network_runtxp(session, &mut resolver))
  }

  /// 事务管理器暂借共同骨架（take → 调用 → 归还，规避 &mut self 双重借用）。
  /// 未接线（None）= 宿主装配缺口，明确报错不静默
  fn with_txn_manager(
    &mut self,
    f: impl FnOnce(&mut TransactionManager, &mut Self) -> bool,
  ) -> bool {
    let Some(mut txn) = self.txn_manager.take() else {
      self.abort_error_message(cs::RESP_ERR_GENERIC_UNK_CMD);
      return true;
    };
    let ok = f(&mut txn, self);
    self.txn_manager = Some(txn);
    ok
  }

  /// 排队命令（会话侧路由，实现委托 wtxn::TransactionManager）；
  /// 命令元数据取自 resp 命令信息域（C# SimpleRespCommandInfo 同源）
  fn network_skip(&mut self, cmd: RespCommand) -> bool {
    use crate::txn_resp_commands::TxnRespCommandsExt;
    let info = self.txn_queued_command_info(cmd);
    self.with_txn_manager(|txn, session| txn.network_skip(session, cmd, info.as_ref()))
  }

  /// 当前排队命令元数据（C# SimpleRespCommandInfo → TxnQueuedCommandInfo 投影；
  /// 键窗口按解析态参数即时解析，对齐 C# LockKeys 的 parseState 取数形态）
  fn txn_queued_command_info(&self, cmd: RespCommand) -> Option<TxnQueuedCommandInfo> {
    let info = try_get_simple_resp_command_info(normalize_for_acls(cmd))?;
    let name = super::resp_commands_info_data::resp_command_to_cs_name(cmd);
    let args = self.get_arg_slices();
    let key_specs: Vec<TxnKeySpec> = info
      .key_specs
      .iter()
      .filter_map(|spec| {
        let (first_idx, last_idx, step) =
          spec.get_key_search_args_slice(&args, info.is_sub_command)?;
        Some(TxnKeySpec::new(
          first_idx,
          last_idx as i64,
          step,
          spec.flags.contains(KeySpecificationFlags::RO),
        ))
      })
      .collect();
    let keys = (!key_specs.is_empty()).then_some(TxnCommandKeys {
      store_type: info.store_type,
      key_specs,
    });
    Some(TxnQueuedCommandInfo {
      name: name.to_string(),
      arity: i32::from(info.arity),
      allowed_in_txn: info.allowed_in_txn,
      is_sub_command: info.is_sub_command,
      keys,
    })
  }

  /// libs/server/Resp/RespServerSession.cs:ProcessOtherCommands
  ///
  /// 慢命令族分派（此处可安全放 @slow 命令）。containsSlowCommand 的置位
  /// 单点收敛在存储执行域 `dispatch_slow` 入口——本会话分派链对 fast 存储
  /// 命令（GET/SET 等）保持直通，段位判定与 C# 三段 switch 等效
  pub fn process_other_commands(&mut self, cmd: RespCommand) -> bool {
    // Lua 脚本族（C# NetworkEVAL / NetworkEVALSHA / NetworkScript*）
    if matches!(
      cmd,
      RespCommand::Eval
        | RespCommand::Evalsha
        | RespCommand::ScriptExists
        | RespCommand::ScriptFlush
        | RespCommand::ScriptLoad
    ) {
      return self.run_lua_command(cmd);
    }
    if cmd == RespCommand::ClientId && self.parse_state.count != 0 {
      // C# NetworkCLIENTID 的参数校验在会话侧（AbortWithWrongNumberOfArguments）
      self.abort_wrong_num_args("client|id");
      return true;
    }
    if cmd == RespCommand::ClientId {
      // C# TryWriteInt64(Id)
      self.output.push(b':');
      let mut buffer = Buffer::new();
      self
        .output
        .extend_from_slice(buffer.format(self.id).as_bytes());
      self.output.extend_from_slice(b"\r\n");
      return true;
    }
    if cmd == RespCommand::Echo {
      if self.parse_state.count != 1 {
        self.abort_wrong_num_args("ECHO");
        return true;
      }
      let msg = self.parse_state.get_arg_slice_by_ref(0).as_slice();
      self.output.write_resp_bulk_string(msg);
      return true;
    }
    if cmd == RespCommand::Time {
      // 在 garnet 中的相对路径:libs/server/Resp/BasicCommands.cs:NetworkTIME
      if self.parse_state.count != 0 {
        self.abort_wrong_num_args("TIME");
        return true;
      }
      let now_nanos = now_nanos();
      let secs = now_nanos / 1_000_000_000;
      let usecs = (now_nanos % 1_000_000_000) / 1_000;
      let mut b1 = Buffer::new();
      let s_str = b1.format(secs);
      let mut b2 = Buffer::new();
      let us_str = b2.format(usecs);
      self.output.extend_from_slice(b"*2\r\n");
      self.output.write_resp_bulk_string(s_str.as_bytes());
      self.output.write_resp_bulk_string(us_str.as_bytes());
      return true;
    }
    if cmd == RespCommand::Async {
      // C# NetworkASYNC：委托 basic_commands 单一定义（RESP3 才可用；
      // BARRIER 的批量等待由 compio 单线程模型天然串行化承接）
      let args = self.get_arg_slices();
      let mut output = mem::take(&mut self.output);
      let r = self.apply_async_param(&args, &mut output);
      self.output = output;
      return matches!(r, Ok(true));
    }
    if cmd == RespCommand::ClientInfo {
      let args = self.get_arg_slices();
      return self.write_via_local("client info error", |s, out| {
        s.network_clientinfo(&args, out)
      });
    }
    if cmd == RespCommand::ClientList {
      return self.write_via_local("client list error", |s, out| s.network_clientlist(out));
    }
    if cmd == RespCommand::ClientKill {
      return self.write_via_local("client kill error", |s, out| s.network_clientkill(out));
    }
    if cmd == RespCommand::ClientGetname {
      let args = self.get_arg_slices();
      return self.write_via_local("client getname error", |s, out| {
        s.network_clientgetname(&args, out)
      });
    }
    if cmd == RespCommand::ClientSetname {
      let args = self.get_arg_slices();
      return self.write_via_local("client setname error", |s, out| {
        s.network_clientsetname(&args, out)
      });
    }
    if cmd == RespCommand::ClientSetinfo {
      let args = self.get_arg_slices();
      return self.write_via_local("client setinfo error", |s, out| {
        s.network_clientsetinfo(&args, out)
      });
    }
    if cmd == RespCommand::ClientUnblock {
      let args = self.get_arg_slices();
      return self.write_via_local("client unblock error", |s, out| {
        s.network_clientunblock(&args, out)
      });
    }
    if cmd == RespCommand::Cluster
      || is_cluster_sub_command(cmd)
      || matches!(
        cmd,
        RespCommand::Failover
          | RespCommand::Replicaof
          | RespCommand::Migrate
          | RespCommand::Secondaryof
      )
    {
      // C# 分派：FAILOVER/REPLICAOF/SECONDARYOF 显式路由 + IsClusterSubCommand
      // 区间整体经 NetworkProcessClusterCommand；切面未挂时报集群支持未启用
      return self.write_via_local("cluster command error", |s, out| {
        s.network_process_cluster_command(cmd, out)
      });
    }
    if cmd == RespCommand::Role {
      let args = self.get_arg_slices();
      return self.write_via_local("role error", |s, out| s.network_role(&args, out));
    }
    // C# NetworkCOMMAND（COMMAND 根命令）：带参报未知子命令，无参列全部命令
    if cmd == RespCommand::Command {
      return self.write_via_local("command error", |s, out| s.network_command_root(out));
    }
    // C# NetworkAUTH（ProcessOtherCommands 段）
    if cmd == RespCommand::Auth {
      let args = self.get_arg_slices();
      return self.network_auth_session(&args).unwrap_or(true);
    }
    // LATENCY / SLOWLOG 族（C# Metrics/Latency、Metrics/Slowlog 的会话 partial）
    if let Some(handled) = self.process_metrics_commands(cmd) {
      return handled;
    }
    // ACL 族（C# ProcessAdminCommands 的 ACL 段）
    if let Some(handled) = self.process_acl_commands(cmd) {
      return handled;
    }
    // MONITOR / DEBUG / SAVE 族（C# ProcessAdminCommands）
    if let Some(handled) = self.process_admin_session_commands(cmd) {
      return handled;
    }
    if cmd == RespCommand::Info {
      // libs/server/Metrics/Info/InfoCommand.cs:NetworkINFO（wmetric 段分发：
      // 段解析 + 各信息域填充经 SessionInfoSource 数据源承接）
      let args = self.get_arg_slices();
      let text = {
        let provider = super::info_provider::SessionInfoSource::new(self);
        let mut info = GarnetInfoMetrics::new();
        let mut out = Vec::new();
        InfoCommand::network_info(
          &args,
          self.active_db_id,
          &provider,
          &mut info,
          // C# monitor.resetEventFlags[STATS] 置位；服务器级监视器未装配为 no-op
          &mut |_| {},
          &mut out,
        );
        out
      };
      self.output.extend_from_slice(&text);
      return true;
    }
    // 自定义命令族（C# ProcessOtherCommands 的 RespCommand.CustomTxn /
    // CustomRawStringCmd / CustomProcedure → NetworkCustomTxn /
    // NetworkCustomRawStringCmd / NetworkCustomProcedure；CustomObjCmd 经
    // dispatch_via_garnet_api 的存储执行域承接）
    if matches!(
      cmd,
      RespCommand::Customtxn | RespCommand::Customrawstringcmd | RespCommand::Customprocedure
    ) {
      return self.run_custom_command();
    }
    let args = self.get_arg_slices();
    self.dispatch_via_garnet_api(cmd, &args);
    true
  }

  /// 本地缓冲写出 → 并回会话输出（CLIENT/CLUSTER/ROLE 族 take/log/restore
  /// 共同骨架：take 输出缓冲 → 本地写出 → 失败告警 → 并回）
  fn write_via_local<F, E>(&mut self, ctx: &'static str, f: F) -> bool
  where
    F: FnOnce(&mut Self, &mut Vec<u8>) -> Result<bool, E>,
    E: Display,
  {
    let mut out = mem::take(&mut self.output);
    if let Err(err) = f(self, &mut out) {
      log::warn!("{ctx}: {err}");
    }
    self.output = out;
    true
  }

  /// COMMAND 根命令入口
  pub fn network_command_root(&mut self, output: &mut Vec<u8>) -> wresp::Result<bool> {
    let args = self.get_arg_slices();
    if !args.is_empty() {
      cs::abort_with_unknown_subcommand(output, args[0].as_str_safe(), "COMMAND");
    } else {
      self.write_command_response(output)?;
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkAUTH
  ///
  /// AUTH [<username>] <password>：NoAuth 档回认证器拒绝文案；ACL 档经
  /// [`RespServerSession::authenticate_user`] 校验后按用户名有无回 WRONGPASS 变体
  pub fn network_auth_session(&mut self, parse_state: &[&[u8]]) -> wresp::Result<bool> {
    wresp::check_arg_count!(parse_state, 1..=2, &mut self.output, "AUTH");

    if !self.authenticator_can_authenticate {
      // C# 默认 GarnetNoAuthAuthenticator：CanAuthenticate = false
      cs::write_error_raw(
        self.get_string_output(),
        "ERR Client sent AUTH, but configured authenticator does not accept passwords",
      );
      return Ok(true);
    }

    let username = if parse_state.len() == 2 {
      parse_state[0]
    } else {
      &[]
    };
    let password = parse_state[parse_state.len() - 1];
    if self.authenticate_user(username, password) {
      cs::write_raw(self.get_string_output(), cs::RESP_OK);
    } else if username.is_empty() {
      cs::write_error_raw(
        self.get_string_output(),
        cs::RESP_WRONGPASS_INVALID_PASSWORD,
      );
    } else {
      cs::write_error_raw(
        self.get_string_output(),
        cs::RESP_WRONGPASS_INVALID_USERNAME_PASSWORD,
      );
    }
    Ok(true)
  }

  /// libs/server/Resp/RespServerSession.cs:NetworkCustomTxn / NetworkCustomProcedure /
  /// NetworkCustomRawStringCmd 共同骨架
  ///
  /// arity 校验 → 分派 → 清空当前自定义命令槽。对象命令走
  /// [`Self::network_custom_obj_cmd`] 存储执行域；事务/过程/原始字符串
  /// 命令执行域未接线，按 arity 校验语义闭环后明确报错。
  pub fn network_custom_txn(&mut self) -> bool {
    self.run_custom_command()
  }

  /// libs/server/Resp/RespServerSession.cs:NetworkCustomProcedure
  pub fn network_custom_procedure(&mut self) -> bool {
    self.run_custom_command()
  }

  /// libs/server/Resp/RespServerSession.cs:NetworkCustomRawStringCmd
  pub fn network_custom_raw_string_cmd(&mut self) -> bool {
    self.run_custom_command()
  }

  /// libs/server/Resp/RespServerSession.cs:NetworkCustomObjCmd
  ///
  /// 自定义对象命令执行入口（arity 校验 → 注册表解析 → 存储执行域分派）。
  /// 同步降级（磁盘候选）时保留 `current_custom_command` 槽供慢路径快照
  /// 命令名（C# currentCustomObjectCommand 的承接形态），由执行域
  /// `StoreGarnetApi::exec` 消费后清槽。
  pub fn network_custom_obj_cmd<D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'_, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    use crate::resp::objects::custom_object_commands::{
      CustomObjOutcome, try_custom_object_command,
    };

    let Some((_, custom)) = self.current_custom_command.take() else {
      return Ok(true);
    };
    let CustomCommandRef { name, id, arity } = &custom;
    if !is_command_arity_valid_checked(*arity, self.parse_state.count) {
      cs::abort_with_wrong_number_of_arguments(output, name);
      self.command_error_written = true;
      return Ok(true);
    }
    let Some((&key, args)) = parse_state.split_first() else {
      cs::abort_with_wrong_number_of_arguments(output, name);
      self.command_error_written = true;
      return Ok(true);
    };

    // 注册表解析：(类型扩展 id, 子命令 id) → 命令 + 执行体 + 信封标签
    let manager = self.custom_command_manager.as_deref();
    let resolved = manager.and_then(|mgr| {
      let mgr = mgr.read();
      let cmd = mgr.try_get_custom_object_sub_command((id >> 8) as u8, (id & 0xff) as u8)?;
      let tag = mgr.object_type_tag(cmd.ext_id);
      Some((cmd.clone(), tag))
    });
    let Some((obj_cmd, tag)) = resolved else {
      // 注册表缺失 / 命令未登记：明确报错，绝不静默
      log::error!("自定义对象命令 {name} 执行域未接线，被拒绝");
      cs::write_error_raw(output, cs::RESP_ERR_GENERIC_UNK_CMD);
      self.command_error_written = true;
      return Ok(true);
    };
    let Some(fns) = obj_cmd.functions else {
      log::error!("自定义对象命令 {name} 未注册执行体，被拒绝");
      cs::write_error_raw(output, cs::RESP_ERR_GENERIC_UNK_CMD);
      self.command_error_written = true;
      return Ok(true);
    };

    match try_custom_object_command(store, &obj_cmd, tag, &fns, key, args, output) {
      CustomObjOutcome::Done => Ok(true),
      // 降级异步重放：命令槽回填供 exec 快照命令名，本次不残留输出
      CustomObjOutcome::Degrade => {
        self.current_custom_command = Some((RespCommand::Customobjcmd, custom));
        Ok(false)
      }
    }
  }

  /// 自定义命令共同路径（事务/过程/原始字符串族）：IsCommandArityValid → 清槽
  fn run_custom_command(&mut self) -> bool {
    let Some((_kind, custom)) = self.current_custom_command.take() else {
      return true;
    };
    let count = self.parse_state.count;
    let CustomCommandRef { name, arity, .. } = &custom;
    if !is_command_arity_valid_checked(*arity, count) {
      self.abort_wrong_num_args(name);
      return true;
    }
    // 自定义命令执行域（C# TryTransactionProc / TryCustomProcedure 族，由
    // custom 域注册表承载）尚未接线：按 ProcessAdminCommands 兜底明确报错，
    // 绝不静默
    log::error!("自定义命令执行域未接线，命令 {name} 被拒绝");
    self.abort_error_message(cs::RESP_ERR_GENERIC_UNK_CMD);
    true
  }

  /// libs/server/Resp/RespServerSession.cs:Process（admin 族回退）
  pub fn process(&mut self, cmd: RespCommand) -> bool {
    let args = self.get_arg_slices();
    self.dispatch_via_garnet_api(cmd, &args);
    true
  }

  /// libs/server/Resp/RespServerSession.cs:IsCommandArityValid
  ///
  /// arity = 0 不校验；正值 = 恰好 arity-1 参数；负值 = 至少 -arity-1 参数。
  /// 失败时按 C# GenericErrWrongNumArgs 写出错误应答。
  pub fn is_command_arity_valid(&mut self, cmd_name: &str, arity: i32, count: usize) -> bool {
    if !is_command_arity_valid_checked(arity, count) {
      self.abort_wrong_num_args(cmd_name);
      return false;
    }
    true
  }

  /// libs/common/Parsing/RespParsingException.cs:ThrowUnexpectedToken
  ///
  /// 置协议违规哨兵，文案 `Unexpected character '{escaped}'.`（控制字符
  /// 以 `\xNN` 转义；C# 以异常抛出，rust 以哨兵携带文案供消费入口写
  /// `ERR Protocol Error: {msg}`）
  pub(crate) fn violation_unexpected_token(&mut self, token: u8) {
    let escaped = if token.is_ascii_control() {
      format!("\\x{token:02x}")
    } else {
      (token as char).to_string()
    };
    self.parse_violation = Some(format!("Unexpected character '{escaped}'."));
  }

  /// libs/common/Parsing/RespParsingException.cs:ThrowInvalidStringLength
  ///
  /// 置协议违规哨兵，文案 `Invalid string length '{len}'.`
  pub(crate) fn violation_invalid_string_length(&mut self, len: i64) {
    self.parse_violation = Some(format!("Invalid string length '{len}'."));
  }

  /// libs/common/Parsing/RespParsingException.cs:ThrowExcessiveArgumentCount
  ///
  /// 置协议违规哨兵，文案
  /// `RESP array argument count '{count}' exceeds maximum allowed count of '{max}'.`
  pub(crate) fn violation_excessive_arg_count(&mut self, count: isize) {
    self.parse_violation = Some(format!(
      "RESP array argument count '{count}' exceeds maximum allowed count of '{}'.",
      MAX_RESP_ARRAY_LENGTH
    ));
  }

  /// libs/common/Parsing/RespParsingException.cs:ThrowIntegerOverflow
  ///
  /// 置协议违规哨兵，文案
  /// `Unable to parse integer. The given number is larger than allowed: {digits}`
  ///（C# 携 ASCII 数字串；u64 溢出形态不含触发位，对齐 C# TryReadUInt64
  /// 抛出时 readHead 尚未越过的语义）
  pub(crate) fn violation_integer_overflow(&mut self, digits: &[u8]) {
    let number = String::from_utf8_lossy(digits);
    self.parse_violation = Some(format!(
      "Unable to parse integer. The given number is larger than allowed: {number}"
    ));
  }

  /// get_command 的区间解析内核（C# 映射统一挂 get_command；本函数承担
  /// GetCommand 的 TryReadUnsignedLengthHeader → TryReadSignedLengthHeader
  /// 及值尾检查的违例分支）。
  ///
  /// 从接收缓冲读取命令名（$len\r\n...\r\n）的区间 (start, len)，推进
  /// read_head；字节不足返回 None 等待。长度头/值尾非法（C#
  /// RespReadUtils.TryReadUnsignedLengthHeader → TryReadSignedLengthHeader
  /// 及 GetCommand 值尾检查抛 RespParsingException 断连）置
  /// [`Self::parse_violation`] 哨兵后返回 None，经既有 violation 路径写
  /// `ERR Protocol Error` 后断连：
  /// - 非 `$` sigil / 无数字 / 头尾与值尾终止符不符 → ThrowUnexpectedToken
  /// - 负长度（含 RESP3 NULL `_\r\n` 与 `$-1\r\n` 特例）→
  ///   ThrowInvalidStringLength
  /// - u64 溢出或数值超 int.MaxValue → ThrowIntegerOverflow
  /// - 超 [`MAX_ARGUMENT_LENGTH_BYTES`]：C# GetCommand 无超限断连
  ///   （SessionParseState.Read 同上限仅返回 false），按字节不足等待
  pub fn get_command_range(&mut self) -> Option<(usize, usize)> {
    let buffer: &[u8] = &self.recv_buffer;
    let mut ptr = self.read_head;
    let end = self.bytes_read;

    // 头部至少 3 字节方可判定（C# TryReadSignedLengthHeader 前置
    // ptr+3 > end 返回 false 等待）
    if end - ptr < 3 {
      return None;
    }
    // RESP3 NULL 特例 '_\r\n'：C# TryReadSignedLengthHeader 解析为 -1，
    // Unsigned 包装层抛 ThrowInvalidStringLength(-1)
    if buffer[ptr] == b'_' && &buffer[ptr + 1..ptr + 3] == b"\r\n" {
      self.violation_invalid_string_length(-1);
      return None;
    }
    // 命令名头 sigil 必为 '$'（C# ThrowUnexpectedToken）
    if buffer[ptr] != b'$' {
      let token = buffer[ptr];
      self.violation_unexpected_token(token);
      return None;
    }
    ptr += 1;

    // 负长度（C#：'-' 起不足 4 字节等待——须容纳 '-1\r\n' 整体读取；
    // '$-1\r\n' 特例与任意负值均由 Unsigned 包装层抛 ThrowInvalidStringLength）
    let negative = buffer[ptr] == b'-';
    if negative {
      if end - ptr < 4 {
        return None;
      }
      if &buffer[ptr..ptr + 4] == b"-1\r\n" {
        self.violation_invalid_string_length(-1);
        return None;
      }
      ptr += 1;
    }

    // 读数字长度；u64 溢出（C# TryReadUInt64 慢路径 ThrowIntegerOverflow，
    // 文案数字串不含触发位——C# 抛出时 readHead 未越过该位）
    let start_digit = ptr;
    let mut length = 0u64;
    while ptr < end && buffer[ptr].is_ascii_digit() {
      let digit = u64::from(buffer[ptr] - b'0');
      length = match length.checked_mul(10).and_then(|v| v.checked_add(digit)) {
        Some(v) => v,
        None => {
          let digits = buffer[start_digit..ptr].to_vec();
          self.violation_integer_overflow(&digits);
          return None;
        }
      };
      ptr += 1;
    }
    // 无数字（C# digitsRead == 0 → ThrowUnexpectedToken；头部 3 字节前置
    // 保证 start_digit <= end-2，buffer[ptr] 越界安全）
    if ptr == start_digit {
      let token = buffer[ptr];
      self.violation_unexpected_token(token);
      return None;
    }
    // 数值超 int.MaxValue（C# 负值门限 int.MinValue 绝对值，允许
    // '$-2147483648' 达界后才按负长度报错）→ ThrowIntegerOverflow
    if length > i32::MAX as u64 + u64::from(negative) {
      let digits = buffer[start_digit..ptr].to_vec();
      self.violation_integer_overflow(&digits);
      return None;
    }
    // 头尾 \r\n：不足等待；不符 C# ThrowUnexpectedToken
    if ptr + 2 > end {
      return None;
    }
    if &buffer[ptr..ptr + 2] != b"\r\n" {
      self.violation_unexpected_token(buffer[ptr]);
      return None;
    }
    // 负长度值（C# TryReadUnsignedLengthHeader length<0 →
    // ThrowInvalidStringLength）
    if negative {
      self.violation_invalid_string_length(-(length as i64));
      return None;
    }
    // 超 512MB 上限：按字节不足等待（见文档注释）
    let length = length as usize;
    if length > MAX_ARGUMENT_LENGTH_BYTES {
      return None;
    }
    ptr += 2;

    // 命令值 + 结尾（数据未收齐时不移动 read_head，保证断包可安全重试；
    // 值尾终止符不符 C# GetCommand ThrowUnexpectedToken 断连）
    if ptr + length + 2 > end {
      return None;
    }
    if &buffer[ptr + length..ptr + length + 2] != b"\r\n" {
      self.violation_unexpected_token(buffer[ptr + length]);
      return None;
    }
    let cmd_start = ptr;
    self.read_head = ptr + length + 2;
    Some((cmd_start, length))
  }

  /// 同 get_command_range，并就地大写化
  pub fn get_upper_case_command_range(&mut self) -> Option<(usize, usize)> {
    let (start, len) = self.get_command_range()?;
    self.recv_buffer[start..start + len].make_ascii_uppercase();
    Some((start, len))
  }

  /// libs/server/Resp/RespServerSession.cs:GetCommand
  ///
  /// 从接收缓冲读取命令名（$len\r\n...\r\n），推进 readHead；不完整返回 None
  pub fn get_command(&mut self) -> Option<Vec<u8>> {
    let (start, len) = self.get_command_range()?;
    Some(self.recv_buffer[start..start + len].to_vec())
  }

  /// libs/server/Resp/RespServerSession.cs:GetUpperCaseCommand
  ///
  /// 同 GetCommand，并就地大写化（子命令名用）
  pub fn get_upper_case_command(&mut self) -> Option<Vec<u8>> {
    let (start, len) = self.get_upper_case_command_range()?;
    Some(self.recv_buffer[start..start + len].to_vec())
  }

  /// libs/server/Resp/RespServerSession.cs:SendAndReset
  ///
  /// 冲洗输出缓冲；缓冲无新增字节时返回 false（C# 抛 GarnetException：
  /// 写入超出响应缓冲仍无进展）
  pub fn send_and_reset(&mut self) -> bool {
    if self.output.is_empty() {
      return false;
    }
    self.flushed_bytes += self.output.len() as u64;
    if let Some(metrics) = &mut self.session_metrics {
      metrics.incr_total_net_output_bytes(self.output.len() as u64);
    }
    self.output.clear();
    true
  }

  /// 已发送字节兼容入口（读后即清；向后兼容 take_output）
  pub fn take_sent(&mut self) -> Vec<u8> {
    self.take_output()
  }

  /// 待发送字节（C# dcurr - GetResponseObjectHead）
  pub fn pending_output_len(&self) -> usize {
    self.output.len()
  }

  /// 取走待发送字节（C# Send(networkSender.GetResponseObjectHead())）
  pub fn take_output(&mut self) -> Vec<u8> {
    let pending = mem::take(&mut self.output);
    self.flushed_bytes += pending.len() as u64;
    if let Some(metrics) = &mut self.session_metrics {
      metrics.incr_total_net_output_bytes(pending.len() as u64);
    }
    pending
  }

  /// 零拷贝提取待发送字节追加到目标写缓冲（对标 C# networkSender 目标缓冲直接写出）
  pub fn take_output_into(&mut self, out: &mut Vec<u8>) {
    if self.output.is_empty() {
      return;
    }
    let len = self.output.len();
    if out.is_empty() {
      mem::swap(&mut self.output, out);
      if self.output.capacity() < 4096 {
        self.output.reserve(65536);
      }
    } else {
      out.extend_from_slice(&self.output);
      self.output.clear();
    }
    self.flushed_bytes += len as u64;
    if let Some(metrics) = &mut self.session_metrics {
      metrics.incr_total_net_output_bytes(len as u64);
    }
  }

  /// 取走会话待释放哨兵（C# ProcessMessages 尾部 `if (toDispose)
  /// DisposeNetworkSender(true)` 的信号通道：QUIT 置位，网络泵发尽本轮
  /// 累积应答后据此断连；取走即复位）
  pub fn take_dispose_request(&mut self) -> bool {
    mem::take(&mut self.to_dispose)
  }

  /// libs/server/Resp/RespServerSession.cs:WriteDirectLarge
  ///
  /// 大块直写输出缓冲（rust 托管缓冲天然可扩容，等价一次追加）
  pub fn write_direct_large(&mut self, src: &[u8]) {
    self.output.extend_from_slice(src);
  }

  /// libs/server/Resp/RespServerSession.cs:TrySwitchActiveDatabaseSession
  pub fn try_switch_active_database_session(&mut self, db_id: i32) -> bool {
    if !self.allow_multi_db {
      return false;
    }
    if !self.try_get_or_set_database_session(db_id, -1) {
      return false;
    }
    let Some(slot) = self.database_session(db_id) else {
      return false;
    };
    self.switch_active_database_session(slot);
    true
  }

  /// libs/server/Resp/RespServerSession.cs:TrySwapDatabaseSessions
  pub fn try_swap_database_sessions(&mut self, db_id1: i32, db_id2: i32) -> bool {
    if !self.allow_multi_db {
      return false;
    }
    if db_id1 == db_id2 {
      return true;
    }
    // 注意：dbIdForSessionCreation 交叉设置（数据库已先行交换）
    if !self.try_get_or_set_database_session(db_id1, db_id2) {
      return false;
    }
    if !self.try_get_or_set_database_session(db_id2, db_id1) {
      return false;
    }
    let (a, b) = (self.database_session(db_id1), self.database_session(db_id2));
    let (Some(slot1), Some(slot2)) = (a, b) else {
      return false;
    };
    // 交换映射（保留各自 ID）
    self.set_database_session(
      db_id1,
      DatabaseSessionSlot {
        id: db_id1,
        created_ticks: slot2.created_ticks,
      },
    );
    self.set_database_session(
      db_id2,
      DatabaseSessionSlot {
        id: db_id2,
        created_ticks: slot1.created_ticks,
      },
    );

    // 活跃库跟随（switch_active 仅读 id；槽位刚写入必为 Some，直取免再克隆）
    if self.active_db_id == db_id1 {
      self.switch_active_database_session(DatabaseSessionSlot {
        id: db_id1,
        created_ticks: slot2.created_ticks,
      });
    } else if self.active_db_id == db_id2 {
      self.switch_active_database_session(DatabaseSessionSlot {
        id: db_id2,
        created_ticks: slot1.created_ticks,
      });
    }
    true
  }

  /// libs/server/Resp/RespServerSession.cs:TryGetOrSetDatabaseSession
  ///
  /// 获取或创建库会话槽；`db_id_for_session_creation = -1` 时取 db_id
  pub fn try_get_or_set_database_session(
    &mut self,
    db_id: i32,
    db_id_for_session_creation: i32,
  ) -> bool {
    if db_id < 0 {
      return false;
    }
    let creation_id = if db_id_for_session_creation == -1 {
      db_id
    } else {
      db_id_for_session_creation
    };
    if (db_id as usize) < self.database_sessions.len()
      && self.database_sessions[db_id as usize].is_some()
    {
      return true;
    }
    if creation_id < 0 || (creation_id as usize) >= self.database_sessions.len() {
      // ExpandableMap 容量上限（C# TrySetValueUnsafe 失败路径）
      return false;
    }
    let slot = self.create_database_session(creation_id);
    self.database_sessions[creation_id as usize] = Some(slot);
    true
  }

  /// libs/server/Resp/RespServerSession.cs:CreateDatabaseSession
  ///
  /// 存储会话 / API / 事务管理器由 wkv 会话纪元与并行域承载，此处建槽
  fn create_database_session(&self, db_id: i32) -> DatabaseSessionSlot {
    DatabaseSessionSlot {
      id: db_id,
      created_ticks: session_now_ms(),
    }
  }

  /// libs/server/Resp/RespServerSession.cs:SwitchActiveDatabaseSession
  pub fn switch_active_database_session(&mut self, db_session: DatabaseSessionSlot) {
    self.active_db_id = db_session.id;
  }

  /// 活跃库 ID 的会话槽视图（C# `databaseSessions.Map[activeDbId]`）
  pub fn database_session(&self, db_id: i32) -> Option<DatabaseSessionSlot> {
    if db_id < 0 || (db_id as usize) >= self.database_sessions.len() {
      return None;
    }
    self.database_sessions[db_id as usize].clone()
  }

  fn set_database_session(&mut self, db_id: i32, slot: DatabaseSessionSlot) {
    if db_id >= 0 && (db_id as usize) < self.database_sessions.len() {
      self.database_sessions[db_id as usize] = Some(slot);
    }
  }

  /// libs/server/Resp/RespServerSession.cs:GetStringOutput
  ///
  /// 主存输出视图（dcurr..dend 的托管等价：输出缓冲可写区）。
  /// C# StringOutput 类型由 string_output 域（并行代理）承载后可替换返回型。
  pub fn get_string_output(&mut self) -> &mut Vec<u8> {
    &mut self.output
  }

  /// 写错误应答并置 commandErrorWritten（对标 C# AbortWithErrorMessage）
  pub fn abort_error_message(&mut self, message: &str) {
    let clean = sanitize_error_str(message, MAX_ERROR_MSG_LEN);
    self.output.extend_from_slice(b"-");
    self.output.extend_from_slice(clean.as_bytes());
    self.output.extend_from_slice(b"\r\n");
    self.command_error_written = true;
  }

  /// 参数数量错误应答（对标 C# AbortWithWrongNumberOfArguments）
  pub fn abort_wrong_num_args(&mut self, cmd_name: &str) {
    let clean = sanitize_error_str(cmd_name, cs::MAX_PARAM_NAME_LEN);
    self
      .output
      .extend_from_slice(b"-ERR wrong number of arguments for '");
    self.output.extend_from_slice(clean.as_bytes());
    self.output.extend_from_slice(b"' command\r\n");
    self.command_error_written = true;
  }

  /// C# WriteError（直接错误写出，无 commandErrorWritten 置位路径差异由
  /// 调用方维护；此处统一置位，覆盖面以 WriteError/Abort 族为准）
  fn write_error_response(&mut self, message: &str) {
    self.abort_error_message(message);
  }

  /// 写出 ACL 拦截错误（对标 libs/server/Resp/RespServerSession.cs:697-706）
  ///
  /// 已认证用户无命令权限时写出 RESP_ERR_NOPERM（`-NOPERM this user has no permissions to run the command\r\n`）；
  /// 未认证会话写出 RESP_ERR_NOAUTH（`-NOAUTH Authentication required.\r\n`）。
  pub fn write_acl_permission_error(&mut self, is_authenticated: bool) {
    let err = if is_authenticated {
      cs::RESP_ERR_NOPERM
    } else {
      cs::RESP_ERR_NOAUTH
    };
    self.abort_error_message(err);
  }

  /// 提取参数切片（<=8 元素在栈上分配零堆开销；超出时自动溢出至堆）
  #[inline]
  pub fn get_arg_slices<'a>(&self) -> SmallVec<[&'a [u8]; 8]> {
    let count = self.parse_state.count;
    let mut args = SmallVec::with_capacity(count);
    for i in 0..count {
      args.push(self.parse_state.get_arg_slice_by_ref(i).as_slice());
    }
    args
  }

  /// 汇集解析态参数所有权副本（Lua 脚本上下文等需独立所有权场景用）
  fn collect_args(&self) -> Vec<Vec<u8>> {
    (0..self.parse_state.count)
      .map(|i| self.parse_state.get_arg_slice_by_ref(i).as_slice().to_vec())
      .collect()
  }
}

impl RespServerSession {
  /// CLIENT SETNAME 落库（C# clientName 字段赋值；命令域校验后调用）
  pub fn set_client_name(&mut self, name: Option<&str>) {
    self.client_name = name.map(str::to_string);
  }

  /// CLIENT SETINFO 落库（C# clientLibName / clientLibVersion 字段赋值）
  pub fn set_client_lib_info(&mut self, lib_name: Option<&str>, lib_version: Option<&str>) {
    self.client_lib_name = lib_name.map(str::to_string);
    self.client_lib_version = lib_version.map(str::to_string);
  }

  /// libs/server/Resp/BasicCommands.cs:WriteClientInfo
  ///
  /// 字段序与 C# 逐项对齐：id addr laddr age flags db resp lib-name lib-ver。
  pub fn write_client_info_state(&self, into: &mut String) {
    let age_sec = (session_now_ms() - self.creation_ticks).max(0) / 1000;
    use std::fmt::Write as _;
    let _ = write!(
      into,
      "id={} addr={} laddr={} age={} flags={} db={} resp={} lib-name={} lib-ver={}",
      self.id,
      self.remote_endpoint,
      self.local_endpoint,
      age_sec,
      if self.is_subscription_session {
        "P"
      } else {
        "N"
      },
      self.active_db_id,
      self.resp_protocol_version,
      self.client_lib_name.as_deref().unwrap_or(""),
      self.client_lib_version.as_deref().unwrap_or(""),
    );
  }

  /// 处理 HELLO 命令的会话状态转换：
  ///
  /// 校验 → 认证 → 升级协议 / 落客户端名 → 按会话真实状态组 HELLO 应答 map
  ///（RESP2 退化为双倍数组）。返回 false 表示认证失败（WRONGPASS）。
  pub fn process_hello_command_state(
    &mut self,
    resp_protocol_version: Option<u8>,
    username: &[u8],
    password: &[u8],
    client_name: Option<&str>,
    output: &mut Vec<u8>,
  ) -> bool {
    if !username.is_empty() && !self.authenticate_user(username, password) {
      cs::write_error_raw(output, cs::RESP_WRONGPASS_INVALID_USERNAME_PASSWORD);
      return false;
    }

    if let Some(version) = resp_protocol_version {
      self.update_resp_protocol_version(version);
    }
    if let Some(name) = client_name {
      self.set_client_name(Some(name));
    }

    // 应答 map（RESP2 退化为双倍数组）；字段序对齐 C#：server/version/
    // garnet_version/proto/id/mode/role + modules 空数组；proto/id 直读会话状态；
    // mode/role 集群形态（C# EnableCluster && IsReplica 分支）
    let (mode, role) = match &self.cluster_session {
      None => ("standalone", "master"),
      Some(cluster) => (
        "cluster",
        if cluster.is_replica() {
          "replica"
        } else {
          "master"
        },
      ),
    };
    write_map_len_resp2(output, 8);
    output.write_resp_bulk_string(b"server");
    output.write_resp_bulk_string(b"redis");
    output.write_resp_bulk_string(b"version");
    output.write_resp_bulk_string(REDIS_PROTOCOL_VERSION.as_bytes());
    output.write_resp_bulk_string(b"garnet_version");
    output.write_resp_bulk_string(env!("CARGO_PKG_VERSION").as_bytes());
    output.write_resp_bulk_string(b"proto");
    output.write_resp_int(i64::from(self.resp_protocol_version));
    output.write_resp_bulk_string(b"id");
    output.write_resp_int(self.id);
    output.write_resp_bulk_string(b"mode");
    output.write_resp_bulk_string(mode.as_bytes());
    output.write_resp_bulk_string(b"role");
    output.write_resp_bulk_string(role.as_bytes());
    output.write_resp_bulk_string(b"modules");
    output.extend_from_slice(b"*0\r\n");
    true
  }
}

impl RespServerSession {
  /// Lua 命令（EVAL / EVALSHA / SCRIPT 等）会话侧接线：构建 [`LuaSessionContext`] 并分派
  /// （输出缓冲为脚本期本地缓冲，结束后并入会话输出）。
  fn run_lua_command(&mut self, cmd: RespCommand) -> bool {
    let Some(mut session_cache) = self.session_script_cache.take() else {
      // C# CheckLuaEnabled：未启用直接回错
      self.abort_error_message("ERR Lua is disabled.");
      return true;
    };
    let store_cache = Arc::clone(&self.store_script_cache);
    let args = self.collect_args();
    // 配置链取值先行克隆（api 的 &mut 借用窗口内不可再借 &self）。
    // LuaOptions 标量为主、allowed_functions 常空，克隆零/单次分配。
    let lua_options = self.lua_options.clone();
    let lua_txn_mode = self.lua_txn_mode;
    let mut script_out = Vec::new();
    {
      let mut api = RespScriptingApi(&mut *self);
      let mut ctx = LuaSessionContext {
        args: &args,
        out: &mut script_out,
        session_cache: &mut session_cache,
        store_cache: &store_cache,
        session: &mut api,
        lua_enabled: true,
        // 配置链装配（C# SessionScriptCache 装配面：LuaTransactionMode 与
        // LuaOptions 逐字段下传），不再写死 default。
        txn_mode: lua_txn_mode,
        redis_version: REDIS_PROTOCOL_VERSION,
        lua_options: &lua_options,
      };
      match cmd {
        RespCommand::Eval => LuaCommands::try_eval(&mut ctx),
        RespCommand::Evalsha => LuaCommands::try_evalsha(&mut ctx),
        RespCommand::ScriptExists => LuaCommands::network_script_exists(&mut ctx),
        RespCommand::ScriptFlush => LuaCommands::network_script_flush(&mut ctx),
        RespCommand::ScriptLoad => LuaCommands::network_script_load(&mut ctx),
        _ => true,
      };
    }
    self.session_script_cache = Some(session_cache);
    self.output.extend_from_slice(&script_out);
    true
  }

  /// ACL 门位图段（C# AdminCommands.cs:CheckACLPermissions 主体；&self 纯判定，
  /// 主循环与 Lua redis.call 路径同用——LuaRunner.Functions.cs:2993）
  ///
  /// 无 ACL 认证器（NoAuth / Password 档）：C# default 用户 +@all → 恒放行。
  /// ACL 档：(!IsAuthenticated || !CanAccessCommand) && !IsNoAuth → 拒绝
  ///（per-user CanAccessCommand 位图；IsNoAuth 豁免见
  /// libs/server/Resp/Parser/RespCommand.cs:701-707）。
  pub(crate) fn acl_permits(&self, cmd: RespCommand) -> bool {
    if self.acl_authenticator.is_none() {
      return true;
    }
    let permitted = self
      .acl_user_handle
      .as_ref()
      .is_some_and(|handle| handle.load().can_access_command(cmd));
    permitted || is_no_auth(cmd)
  }

  /// C# CheckACLPermissions（Lua redis.call 路径，LuaRunner.Functions.cs:2993）：
  /// 命令名解析后走同一 ACL 门。无法解析的名字不入 ACL 门（C# INVALID 语义，
  /// 主循环与脚本调用方均先行拒绝）。
  /// 命名避让 admin_commands 域的 ACL 命令处理器（同名 C# 入口）
  pub fn acl_allows_command(&self, command: &str) -> bool {
    match super::resp_commands_info_data::resp_command_from_cs_name(command) {
      Some(cmd) => self.acl_permits(cmd),
      None => true,
    }
  }

  /// 构建 NoScript 命令集位图（对齐 LuaRunner InitializeNoScriptDetails 集合）
  ///
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:InitializeNoScriptDetails
  ///
  /// 从 [`super::resp_commands_info::try_get_resp_commands_info`]（externalOnly
  /// 口径）的 NoScript 标志动态构建：顶层命令与子命令的判别值升序铺开，
  /// 位图按字节粒度置位（C# `ulongIndex = stepped / sizeof(ulong)`、
  /// `bitIndex = stepped % sizeof(ulong)`，除数为 8 字节而非 64 位）。
  /// Garnet 命令数据无 FCALL/FCALL_RO/EVAL_RO/EVALSHA_RO/FUNCTION（Redis 侧
  /// 命令，Garnet RespCommandsInfo 未定义），故无对应位。
  pub fn no_script_details() -> (i32, Vec<u64>) {
    const BITS_PER_WORD: usize = size_of::<u64>(); // C# sizeof(ulong) = 8

    let Some(all_commands) = super::resp_commands_info::try_get_resp_commands_info(true) else {
      // 命令元数据导入失败（C# 抛 InvalidOperationException 路径）；数据为
      // 构建期内嵌资源，运行时不可达，按空位图退化
      return (0, vec![0]);
    };

    let mut no_script: Vec<u16> = all_commands
      .values()
      .flat_map(|info| iter::once(info).chain(info.sub_commands.iter()))
      .filter(|info| info.flags.intersects(RespCommandFlags::NO_SCRIPT))
      .filter(|info| info.command != RespCommand::None)
      .map(|info| info.command as u16)
      .collect();
    no_script.sort_unstable();
    no_script.dedup();

    let Some(&start) = no_script.first() else {
      return (0, vec![0]);
    };
    let end = no_script.last().copied().expect("非空");
    let size = (end - start) as usize + 1;
    let mut num_words = size / BITS_PER_WORD;
    // C# 上游怪癖：余数对 numULongs 而非字宽取模，1:1 保留
    if !size.is_multiple_of(num_words) {
      num_words += 1;
    }

    let mut bitmap = vec![0_u64; num_words];
    for discriminant in no_script {
      let stepped = (discriminant - start) as usize;
      bitmap[stepped / BITS_PER_WORD] |= 1_u64 << (stepped % BITS_PER_WORD);
    }

    (start as i32, bitmap)
  }
}

/// 会话的 [`ScriptingApi`] 适配器（redis.call 落地面）
///
/// C# ProcessCommandFromScripting 把参数格式化为 RESP 请求后重入
/// TryConsumeMessages；rust 侧经同一解析/分派路径，响应字节落入
/// [`ScratchBufferNetworkSender`]。会话脚本缓存已由 [`RespServerSession::run_lua_command`]
/// 暂时摘除，重入路径与脚本期借用互斥。
struct RespScriptingApi<'a>(&'a mut RespServerSession);

impl ScriptingApi for RespScriptingApi<'_> {
  /// 分派 RESP 请求（C# TryConsumeMessages + ScratchBufferNetworkSender 组合）
  fn dispatch_resp(&mut self, request: &[u8], sender: &mut ScratchBufferNetworkSender) {
    let _ = self.0.try_consume_messages(request);
    sender.write_response_bytes(&self.0.output);
    self.0.send_and_reset();
  }

  /// GET 特例（C# api.GET）：RESP 请求闭环后解析批量串/null 应答
  ///（C# 为存储 API 直连，rust 重入会话解析面，须带完整数组头）
  fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, &'static str> {
    let mut request = Vec::with_capacity(key.len() + 21);
    request.extend_from_slice(b"*2\r\n");
    request.write_resp_bulk_string(b"GET");
    request.write_resp_bulk_string(key);
    let mut sender = ScratchBufferNetworkSender::new();
    self.dispatch_resp(&request, &mut sender);
    wresp::parse_bulk_reply(sender.get_response())
      .map(|opt| opt.map(<[u8]>::to_vec))
      .map_err(reply_error_str)
  }

  /// SET 特例（C# api.SET）：+OK 或错误应答（同上，带完整数组头）
  fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), &'static str> {
    let mut request = Vec::with_capacity(key.len() + value.len() + 29);
    request.extend_from_slice(b"*3\r\n");
    request.write_resp_bulk_string(b"SET");
    request.write_resp_bulk_string(key);
    request.write_resp_bulk_string(value);
    let mut sender = ScratchBufferNetworkSender::new();
    self.dispatch_resp(&request, &mut sender);
    wresp::parse_simple_reply(sender.get_response()).map_err(reply_error_str)
  }

  fn resp_protocol_version(&self) -> u8 {
    self.0.resp_protocol_version
  }

  fn update_resp_protocol_version(&mut self, version: u8) {
    self.0.update_resp_protocol_version(version);
  }

  fn check_acl_permissions(&self, command: &str) -> bool {
    self.0.acl_allows_command(command)
  }

  fn set_transaction_mode(&mut self, enabled: bool) {
    self.0.set_transaction_mode(enabled);
  }
}

impl wtxn::TxnSession for RespServerSession {
  #[inline]
  fn session_id(&self) -> i32 {
    self.id as i32
  }

  #[inline]
  fn arg_count(&self) -> usize {
    self.parse_state.count
  }

  #[inline]
  fn get_arg(&self, idx: usize) -> &[u8] {
    self.parse_state.get_arg_slice_by_ref(idx).as_slice()
  }

  #[inline]
  fn txn_state(&self) -> TxnState {
    self.txn_state
  }

  #[inline]
  fn set_txn_state(&mut self, state: TxnState) {
    self.txn_state = state;
  }

  #[inline]
  fn end_read_head(&self) -> usize {
    self.end_read_head
  }

  #[inline]
  fn set_end_read_head(&mut self, head: usize) {
    self.end_read_head = head;
  }

  #[inline]
  fn resp_protocol_version(&self) -> u8 {
    self.resp_protocol_version
  }

  #[inline]
  fn active_db_id(&self) -> i32 {
    self.active_db_id
  }

  #[inline]
  fn can_run_debug(&self) -> bool {
    self.can_run_debug()
  }

  #[inline]
  fn write_ok(&mut self) {
    self.output.extend_from_slice(cs::RESP_OK);
  }

  #[inline]
  fn write_queued(&mut self) {
    self.output.extend_from_slice(b"+QUEUED\r\n");
  }

  #[inline]
  fn write_null_array(&mut self) {
    if self.resp_protocol_version >= 3 {
      self.output.extend_from_slice(b"_\r\n");
    } else {
      self.output.extend_from_slice(b"*-1\r\n");
    }
  }

  #[inline]
  fn write_array_len(&mut self, count: usize) {
    use wresp::RespVecExt;
    self.output.write_resp_array_len(count);
  }

  #[inline]
  fn write_error(&mut self, message: &str) {
    self.abort_error_message(message);
  }

  #[inline]
  fn abort_wrong_num_args(&mut self, cmd_name: &str) {
    self.abort_wrong_num_args(cmd_name);
  }

  #[inline]
  fn write_proc_param_error(&mut self, tx_id: u8, expected: i32, actual: usize) {
    let mut b0 = Buffer::new();
    let mut b1 = Buffer::new();
    let mut b2 = Buffer::new();
    let s0 = b0.format(tx_id);
    let s1 = b1.format(expected);
    let s2 = b2.format(actual);
    self
      .output
      .extend_from_slice(b"-ERR Invalid number of parameters to stored proc ");
    self.output.extend_from_slice(s0.as_bytes());
    self.output.extend_from_slice(b", expected ");
    self.output.extend_from_slice(s1.as_bytes());
    self.output.extend_from_slice(b", actual ");
    self.output.extend_from_slice(s2.as_bytes());
    self.output.extend_from_slice(b"\r\n");
    self.command_error_written = true;
  }

  fn verify_cluster_txn_keys(&mut self, keys: &[&[u8]]) -> bool {
    let Some(ref cluster) = self.cluster_session else {
      return true;
    };
    let input = ClusterSlotVerificationInput {
      key_specs: &[],
      is_sub_command: false,
      read_only: self
        .txn_manager
        .as_ref()
        .is_some_and(|tm| tm.is_read_only()),
      session_asking: self.session_asking,
      wait_for_stable_slot: false,
    };
    match cluster.network_multi_key_slot_verify(&input, keys, &mut self.output) {
      SlotVerifyGate::Serve => true,
      SlotVerifyGate::Redirected => false,
      SlotVerifyGate::Wait => {
        // 事务域无挂起重评形态（EXEC 已回退游标重放排队命令）：丢弃切面
        // 等待体，按 C# VerifyKeysInRange 迁移中混合态应 TRYAGAIN，客户端
        // 重试 EXEC
        let _ = cluster.take_pending_slow();
        self
          .output
          .extend_from_slice(b"-TRYAGAIN Multiple keys request during rehashing of slot\r\n");
        false
      }
    }
  }
}

/// [`wresp::ReplyError`] → [`ScriptingApi`] 的 `&'static str` 错误面映射
fn reply_error_str(err: wresp::ReplyError) -> &'static str {
  match err {
    ReplyError::ErrorReply => "script error",
    ReplyError::Malformed => "protocol error",
  }
}

/// 默认会话（C# internal RespServerSession() 空构造的等价）
impl Default for RespServerSession {
  fn default() -> Self {
    Self::new(0, RespServerSessionOptions::default())
  }
}

impl wpubsub::PubSubSessionCommands for RespServerSession {
  #[inline]
  fn session_id(&self) -> i64 {
    self.id
  }

  #[inline]
  fn output_mut(&mut self) -> &mut Vec<u8> {
    &mut self.output
  }

  #[inline]
  fn resp_protocol_version(&self) -> u8 {
    self.resp_protocol_version
  }

  #[inline]
  fn set_subscription_session(&mut self, is_subscription: bool) {
    self.is_subscription_session = is_subscription;
  }

  #[inline]
  fn abort_wrong_num_args(&mut self, cmd_name: &str) {
    self.abort_wrong_num_args(cmd_name);
  }

  #[inline]
  fn abort_error_message(&mut self, message: &str) {
    self.abort_error_message(message);
  }

  #[inline]
  fn write_null(&mut self) {
    self.write_null();
  }

  #[inline]
  fn write_push_length(&mut self, count: usize) {
    self.write_push_length(count);
  }

  #[inline]
  fn send_and_reset(&mut self) {
    // 托管缓冲模式下由网络消费端统一提取写出，此处保留在 output 中
  }
}

impl RespServerSession {
  /// pub/sub 命令共同骨架：trait 默认实现同时借用会话输出面与自持 wire，
  /// take→调用→归还规避字段级双重借用；占位 wire 零堆分配（Vec::new 不分配）
  fn with_pubsub(
    &mut self,
    f: impl FnOnce(&mut Self, &mut wpubsub::PubSubSession) -> bool,
  ) -> bool {
    let mut wire = mem::replace(
      &mut self.pubsub,
      PubSubSession::with_mailbox_capacity(None, 0),
    );
    let ok = f(self, &mut wire);
    self.pubsub = wire;
    ok
  }

  /// 订阅通道（C# NetworkSUBSCRIBE / NetworkSSUBSCRIBE；wire 为会话自持接线）
  #[inline]
  pub fn network_subscribe(&mut self, shard: bool, args: &[&[u8]]) -> bool {
    self.with_pubsub(|s, wire| PubSubSessionCommands::network_subscribe(s, wire, shard, args))
  }

  /// 模式订阅（C# NetworkPSUBSCRIBE）
  #[inline]
  pub fn network_psubscribe(&mut self, args: &[&[u8]]) -> bool {
    self.with_pubsub(|s, wire| PubSubSessionCommands::network_psubscribe(s, wire, args))
  }

  /// 退订通道（C# NetworkUNSUBSCRIBE）
  #[inline]
  pub fn network_unsubscribe(&mut self, args: &[&[u8]]) -> bool {
    self.with_pubsub(|s, wire| PubSubSessionCommands::network_unsubscribe(s, wire, args))
  }

  /// 退订模式（C# NetworkPUNSUBSCRIBE）
  #[inline]
  pub fn network_punsubscribe(&mut self, args: &[&[u8]]) -> bool {
    self.with_pubsub(|s, wire| PubSubSessionCommands::network_punsubscribe(s, wire, args))
  }

  /// 发布消息（C# NetworkPUBLISH / NetworkSPUBLISH）
  #[inline]
  pub fn network_publish(&mut self, shard: bool, args: &[&[u8]]) -> bool {
    self.with_pubsub(|s, wire| PubSubSessionCommands::network_publish(s, wire, shard, args))
  }

  /// 列出活跃通道（C# NetworkPUBSUB_CHANNELS）
  #[inline]
  pub fn network_pubsub_channels(&mut self, args: &[&[u8]]) -> bool {
    self.with_pubsub(|s, wire| PubSubSessionCommands::network_pubsub_channels(s, wire, args))
  }

  /// 活跃模式订阅数（C# NetworkPUBSUB_NUMPAT）
  #[inline]
  pub fn network_pubsub_numpat(&mut self, args: &[&[u8]]) -> bool {
    self.with_pubsub(|s, wire| PubSubSessionCommands::network_pubsub_numpat(s, wire, args))
  }

  /// 指定通道订阅数（C# NetworkPUBSUB_NUMSUB）
  #[inline]
  pub fn network_pubsub_numsub(&mut self, args: &[&[u8]]) -> bool {
    self.with_pubsub(|s, wire| PubSubSessionCommands::network_pubsub_numsub(s, wire, args))
  }

  /// 会话推送编码收敛点（C# Publish / PatternPublish）
  #[inline]
  pub fn drain_pubsub_frames(&mut self) -> usize {
    let mut wire = mem::replace(
      &mut self.pubsub,
      PubSubSession::with_mailbox_capacity(None, 0),
    );
    let n = PubSubSessionCommands::drain_pubsub_frames(self, &mut wire);
    self.pubsub = wire;
    n
  }
}

/// 连接保护共同判定（调试命令与模块加载使用）
fn can_run_with_protection(option: ConnectionProtectionOption, is_local: bool) -> bool {
  match option {
    ConnectionProtectionOption::Yes => true,
    ConnectionProtectionOption::No => false,
    ConnectionProtectionOption::Local => is_local,
  }
}

/// ACL 口令校验闭包（wacl GarnetAclWithPasswordAuthenticator::authenticate_internal
/// 的同语义承接：ascii_sanitize 为 wacl pub(crate)，按 >0x7F 折 '?' 规范化）
fn acl_password_check(
  user_handle: &Arc<wacl::UserHandle>,
  _username: &[u8],
  password: &[u8],
) -> bool {
  let sanitized: String = password
    .iter()
    .map(|&b| if b <= 0x7F { b as char } else { '?' })
    .collect();
  let hash = AclPassword::from_string(&sanitized);
  let user = user_handle.user();
  user.is_enabled() && user.validate_password(&hash)
}

/// 命令 arity 纯判定逻辑（供 is_command_arity_valid 使用）。
/// arity = 0 不校验；正值 = 恰好 arity-1 参数；
/// 负值 = 至少 |arity|-1 参数（C# `count < -arity - 1` 为非法）
fn is_command_arity_valid_checked(arity: i32, count: usize) -> bool {
  if arity == 0 {
    return true;
  }
  if arity > 0 {
    count == arity as usize - 1
  } else {
    count >= (-arity) as usize - 1
  }
}

/// Environment.TickCount64 等价：会话年龄域毫秒时钟（CreationTicks/age 仅作
/// 相对量，绝对基准无关紧要；单一实现 `wbase::time::now_ms`，此处仅饱和转 i64）
fn session_now_ms() -> i64 {
  now_ms().min(i64::MAX as u64) as i64
}

#[cfg(test)]
mod tests {
  use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

  use compio::time::sleep;
  use parking_lot::Mutex;
  use wacl::{
    self, AccessControlList, AclPassword, GarnetAclAuthenticator as WaclGarnetAclAuthenticator,
    User, UserHandle as WaclUserHandle,
  };
  use waof::AofAddress;
  use wlua::{LuaOptions as WluaLuaOptions, LuaTimeoutManager as WluaLuaTimeoutManager};
  use wpubsub::{self, SubscribeBroker as WpubsubSubscribeBroker};
  use wtxn::{self, TxnState as WtxnTxnState, WatchVersionMap as WtxnWatchVersionMap};

  use super::*;
  use crate::{
    ClusterSessionFace, RoleInfo,
    resp::{garnet_api::GarnetApiFace, resp_commands_info},
    service::spawn_lua_timeout_tick,
  };

  fn session(id: i64) -> RespServerSession {
    RespServerSession::new(id, RespServerSessionOptions::default())
  }

  fn multi_db_session(id: i64) -> RespServerSession {
    RespServerSession::new(
      id,
      RespServerSessionOptions {
        allow_multi_db: true,
        ..RespServerSessionOptions::default()
      },
    )
  }

  #[test]
  fn dispatch_routes_via_garnet_api_injection() {
    // 桩存储执行域：记录命令并写回应答（验证注入面真分派）
    struct EchoApi;
    impl GarnetApiFace for EchoApi {
      fn exec(&self, session: &mut RespServerSession, cmd: RespCommand, _args: &[&[u8]]) {
        session.write_direct_large(b"+OK\r\n");
        assert_eq!(cmd, RespCommand::Get);
      }

      async fn exec_slow(&self, _cmd: RespCommand, _args: Vec<Vec<u8>>) -> Vec<u8> {
        Vec::new()
      }
    }
    let mut s = session(20);
    s.set_garnet_api(Arc::new(EchoApi));
    s.parse_state.initialize(1);
    assert!(s.process_array_commands(RespCommand::Get));
    assert_eq!(String::from_utf8(s.take_output()).unwrap(), "+OK\r\n");
  }

  #[test]
  fn dispatch_without_api_reports_error() {
    // 存储执行域未挂载：明确报错，绝不静默吞命令
    let mut s = session(21);
    s.parse_state.initialize(1);
    assert!(s.process_array_commands(RespCommand::Get));
    assert_eq!(
      String::from_utf8(s.take_output()).unwrap(),
      "-ERR store execution domain not attached\r\n"
    );
  }

  #[test]
  fn client_info_carries_real_state() {
    let mut s = session(7);
    s.remote_endpoint = "127.0.0.1:6380".to_string();
    s.set_client_name(Some("loader"));
    s.set_client_lib_info(Some("redis-py"), Some("5.0.1"));

    // HELLO 3 经命令路径升级协议版本，proto/id 回写会话状态
    let mut out = Vec::new();
    assert!(s.process_hello_command_state(Some(3), b"", b"", None, &mut out));
    let text = String::from_utf8(out).unwrap();
    assert!(
      text.contains("$5\r\nproto\r\n:3\r\n"),
      "resp=3 expected: {text}"
    );
    assert!(
      text.contains("$2\r\nid\r\n:7\r\n"),
      "真实 Id expected: {text}"
    );

    // 升级后的协议版本随会话状态透传 CLIENT INFO
    let mut info = String::new();
    s.write_client_info_state(&mut info);
    assert_eq!(
      info,
      "id=7 addr=127.0.0.1:6380 laddr= age=0 flags=N db=0 resp=3 lib-name=redis-py lib-ver=5.0.1"
    );
  }

  #[test]
  fn hello_rejects_auth_on_noauth() {
    let mut s = session(1);
    let mut out = Vec::new();
    assert!(!s.process_hello_command_state(Some(3), b"alice", b"secret", None, &mut out));
    // C# 带 username 的 HELLO 认证失败 → Invalid username/password combination
    assert_eq!(
      String::from_utf8(out).unwrap(),
      "-WRONGPASS Invalid username/password combination\r\n"
    );
    // 认证失败不升级协议
    assert_eq!(s.resp_protocol_version, DEFAULT_RESP_VERSION);
  }

  #[test]
  fn hello_auth_with_acl_credentials() {
    let (mut s, authenticator) = acl_session(2);
    let acl = Arc::clone(authenticator.lock().get_access_control_list());
    let user = User::new("alice".to_string());
    user.set_enabled(true);
    user.add_password_hash(AclPassword::from_string("secret"));
    acl
      .add_user_handle(Arc::new(WaclUserHandle::new(Arc::new(user))))
      .unwrap();

    // 凭据正确：认证通过，升级协议版本并设置 client name
    let mut out = Vec::new();
    assert!(s.process_hello_command_state(
      Some(3),
      b"alice",
      b"secret",
      Some("myclient"),
      &mut out
    ));
    assert_eq!(s.resp_protocol_version, 3);
    assert_eq!(s.client_name.as_deref(), Some("myclient"));
    assert_eq!(s.user_handle.as_deref(), Some("alice"));
    let text = String::from_utf8(out).unwrap();
    assert!(text.contains("$5\r\nproto\r\n:3\r\n"));

    // 密码错误：认证失败，阻断协议版本修改与 client name 修改
    let mut s2 = session(3);
    s2.attach_acl(Some(authenticator), None);
    let mut out2 = Vec::new();
    assert!(!s2.process_hello_command_state(
      Some(3),
      b"alice",
      b"wrongpwd",
      Some("newclient"),
      &mut out2
    ));
    assert_eq!(
      String::from_utf8(out2).unwrap(),
      "-WRONGPASS Invalid username/password combination\r\n"
    );
    assert_eq!(s2.resp_protocol_version, DEFAULT_RESP_VERSION);
    assert_eq!(s2.client_name, None);
  }

  #[test]
  fn database_sessions_lifecycle() {
    let mut s = multi_db_session(1);
    // 默认库 0 已建且活跃
    assert_eq!(s.active_db_id, 0);
    assert_eq!(s.get_database_sessions_snapshot().len(), 1);

    // 切换到库 2（惰性创建）
    assert!(s.try_switch_active_database_session(2));
    assert_eq!(s.active_db_id, 2);
    assert_eq!(s.get_database_sessions_snapshot().len(), 2);

    // 超出 max_databases → 失败
    assert!(!s.try_switch_active_database_session(999));

    // SWAPDB：交换后活跃会话跟随
    assert!(s.try_swap_database_sessions(0, 2));
    assert_eq!(s.active_db_id, 2);
    assert_eq!(s.database_session(0).unwrap().id, 0);
    assert_eq!(s.database_session(2).unwrap().id, 2);

    // 单库会话禁切换
    let mut single = session(2);
    assert!(!single.try_switch_active_database_session(1));
    assert!(!single.try_swap_database_sessions(0, 1));
  }

  #[test]
  fn arity_validation_writes_error() {
    let mut s = session(4);
    assert!(s.is_command_arity_valid("get", 2, 1));
    assert!(!s.is_command_arity_valid("get", 2, 2));
    let text = String::from_utf8(s.take_output()).unwrap();
    assert_eq!(text, "-ERR wrong number of arguments for 'get' command\r\n");
    // 负 arity：至少 |arity|-1
    assert!(s.is_command_arity_valid("mset", -3, 2));
    assert!(!s.is_command_arity_valid("mset", -3, 1));
    // 0 = 不校验
    assert!(s.is_command_arity_valid("x", 0, 100));
  }

  #[test]
  fn latency_metrics_optional_path() {
    let s = session(5);
    // 默认关闭监视
    assert!(s.get_latency_metrics().is_none());
    s.reset_latency_metrics(LatencyMetricsType::NetRsLat);
    s.reset_all_latency_metrics();

    let enabled = RespServerSession::new(
      6,
      RespServerSessionOptions {
        latency_monitor: true,
        ..RespServerSessionOptions::default()
      },
    );
    let metrics = enabled.get_latency_metrics().expect("监视开启时有实例");
    metrics.start(LatencyMetricsType::NetRsLat, 100);
    assert_eq!(metrics.get(LatencyMetricsType::NetRsLat), 100);
    // Stop 记录耗时并清零进行中起点（C# RecordValue 后 start = 0）
    metrics.stop(LatencyMetricsType::NetRsLat, 150);
    assert_eq!(metrics.get(LatencyMetricsType::NetRsLat), 0);
    enabled.reset_all_latency_metrics();
    assert_eq!(metrics.get(LatencyMetricsType::NetRsLat), 0);
  }

  #[test]
  fn debug_protection() {
    let local = RespServerSessionOptions {
      enable_debug_command: ConnectionProtectionOption::Local,
      ..RespServerSessionOptions::default()
    };
    let mut s = RespServerSession::new(8, local);
    s.remote_endpoint = "127.0.0.1:55555".to_string();
    assert!(s.can_run_debug());

    s.remote_endpoint = "10.0.0.9:1234".to_string();
    assert!(!s.can_run_debug(), "Local 保护拒绝远程");

    let mut closed = RespServerSession::new(
      9,
      RespServerSessionOptions {
        enable_debug_command: ConnectionProtectionOption::No,
        ..RespServerSessionOptions::default()
      },
    );
    closed.remote_endpoint = "127.0.0.1:1".to_string();
    assert!(!closed.can_run_debug());
  }

  #[test]
  fn output_pipeline_and_metrics() {
    let mut s = RespServerSession::new(
      10,
      RespServerSessionOptions {
        metrics_sampling_frequency: true,
        ..RespServerSessionOptions::default()
      },
    );
    s.write_direct_large(b"*2\r\n$3\r\nfoo\r\n");
    assert_eq!(s.pending_output_len(), 13);
    assert!(s.send_and_reset());
    assert!(!s.send_and_reset(), "空缓冲冲洗 = 无进展");
    let total = s.session_metrics.as_ref().unwrap().total_net_output_bytes;
    assert_eq!(total, 13);
  }

  #[test]
  fn custom_command_arity_gate() {
    let mut s = session(11);
    s.parse_state.initialize(2);
    s.current_custom_command = Some((
      RespCommand::Customtxn,
      CustomCommandRef {
        name: "MYTXN".to_string(),
        id: 1,
        arity: 3,
      },
    ));
    // arity 3 → 恰 2 参数 → 通过并清槽（执行域未接线，报 unknown 不静默）
    assert!(s.network_custom_txn());
    assert!(s.current_custom_command.is_none());
    assert!(
      String::from_utf8(s.take_output())
        .unwrap()
        .contains("unknown command")
    );

    // arity 3 但 1 参数 → 参数错误且清槽
    s.parse_state.initialize(1);
    s.current_custom_command = Some((
      RespCommand::Customprocedure,
      CustomCommandRef {
        name: "MYPROC".to_string(),
        id: 2,
        arity: 3,
      },
    ));
    assert!(s.network_custom_procedure());
    assert!(s.current_custom_command.is_none());
    assert!(
      String::from_utf8(s.take_output())
        .unwrap()
        .contains("MYPROC")
    );
  }

  /// 桩切面：记录只读写态与批纪元快照并按开关注写 MOVED
  struct StubClusterSession {
    read_only: AtomicBool,
    redirect: AtomicBool,
    disposed: AtomicBool,
    local_epoch: AtomicI64,
  }

  impl StubClusterSession {
    fn new() -> Self {
      Self {
        read_only: AtomicBool::new(false),
        redirect: AtomicBool::new(false),
        disposed: AtomicBool::new(false),
        local_epoch: AtomicI64::new(0),
      }
    }
  }

  impl ClusterSessionFace for StubClusterSession {
    fn set_read_only_session(&self) {
      self.read_only.store(true, Ordering::Relaxed);
    }

    fn set_read_write_session(&self) {
      self.read_only.store(false, Ordering::Relaxed);
    }

    fn is_internal_write_session(&self) -> bool {
      false
    }

    fn local_current_epoch(&self) -> i64 {
      self.local_epoch.load(Ordering::Relaxed)
    }

    fn acquire_current_epoch(&self) {
      // 桩无 provider 可达面，置位即批内哨兵（LocalCurrentEpoch != 0）
      self.local_epoch.store(1, Ordering::Relaxed);
    }

    fn release_current_epoch(&self) {
      self.local_epoch.store(0, Ordering::Relaxed);
    }

    fn network_multi_key_slot_verify(
      &self,
      _input: &ClusterSlotVerificationInput<'_>,
      _args: &[&[u8]],
      output: &mut Vec<u8>,
    ) -> SlotVerifyGate {
      if self.redirect.load(Ordering::Relaxed) {
        output.extend_from_slice(b"-MOVED 0 stub:1\r\n");
        return SlotVerifyGate::Redirected;
      }
      SlotVerifyGate::Serve
    }

    fn process_cluster_commands(
      &self,
      _cmd: RespCommand,
      _args: &[&[u8]],
      output: &mut Vec<u8>,
    ) -> bool {
      output.extend_from_slice(b"+STUBCLUSTER\r\n");
      true
    }

    fn is_primary(&self) -> bool {
      true
    }

    fn is_replica(&self) -> bool {
      false
    }

    fn get_primary_info(&self) -> (AofAddress, Vec<RoleInfo>) {
      (AofAddress::default(), Vec::new())
    }

    fn get_replica_info(&self) -> RoleInfo {
      RoleInfo::default()
    }

    fn aof_sublog_count(&self) -> usize {
      1
    }

    fn dispose(&self) {
      self.disposed.store(true, Ordering::Relaxed);
    }
  }

  #[test]
  fn readonly_writes_through_cluster_aspect() {
    let stub = Arc::new(StubClusterSession::new());
    let mut s = session(30);
    s.attach_cluster_session(Arc::clone(&stub));
    s.parse_state.initialize(0);
    assert!(s.process_basic_commands(RespCommand::Readonly));
    assert_eq!(String::from_utf8(s.take_output()).unwrap(), "+OK\r\n");
    assert!(stub.read_only.load(Ordering::Relaxed));
    assert!(s.read_only_session);

    s.parse_state.initialize(0);
    assert!(s.process_basic_commands(RespCommand::Readwrite));
    assert!(!stub.read_only.load(Ordering::Relaxed));
    assert!(!s.read_only_session);
  }

  #[test]
  fn cluster_gate_redirects_data_command() {
    let stub = Arc::new(StubClusterSession::new());
    stub.redirect.store(true, Ordering::Relaxed);
    let mut s = session(31);
    s.attach_cluster_session(Arc::clone(&stub));
    // GET key：槽位门重定向（分派不再执行）
    assert!(
      s.try_consume_messages(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
        .is_some()
    );
    assert_eq!(
      String::from_utf8(s.take_output()).unwrap(),
      "-MOVED 0 stub:1\r\n"
    );
  }

  #[test]
  fn cluster_gate_passes_local_data_command() {
    let stub = Arc::new(StubClusterSession::new());
    let mut s = session(32);
    s.attach_cluster_session(Arc::clone(&stub));
    assert!(
      s.try_consume_messages(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
        .is_some()
    );
    // 本地放行 → 无重定向；存储执行域未挂载时明确报错（不再静默空应答）
    let sent = String::from_utf8(s.take_output()).unwrap();
    assert!(!sent.starts_with("-MOVED"));
    assert_eq!(sent, "-ERR store execution domain not attached\r\n");
  }

  #[test]
  fn cluster_command_routes_to_aspect() {
    let stub = Arc::new(StubClusterSession::new());
    let mut s = session(33);
    s.attach_cluster_session(Arc::clone(&stub));
    assert!(
      s.try_consume_messages(b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n")
        .is_some()
    );
    assert_eq!(
      String::from_utf8(s.take_output()).unwrap(),
      "+STUBCLUSTER\r\n"
    );
  }

  #[test]
  fn dispose_releases_cluster_aspect() {
    let stub = Arc::new(StubClusterSession::new());
    let mut s = session(34);
    s.attach_cluster_session(Arc::clone(&stub));
    s.dispose();
    assert!(stub.disposed.load(Ordering::Relaxed));
    assert!(s.cluster_session.is_none());
  }

  #[test]
  fn auth_default_user_fallback() {
    let mut s = session(13);
    // NoAuth：无法认证 → 恒 false，但保持默认用户
    assert!(!s.authenticate_user(b"other", b"pwd"));
    assert_eq!(s.user_handle.as_deref(), Some("default"));
    // 门控对 NoAuth 档恒放行（C# default 用户 +@all 兜底语义）
    assert!(s.acl_permits(RespCommand::Get));
    // 挂载 ACL 认证器（CanAuthenticate = true）后，无权限用户 → 拒绝
    let acl = Arc::new(AccessControlList::new("", None).unwrap());
    s.attach_acl(
      Some(Arc::new(Mutex::new(WaclGarnetAclAuthenticator::new(acl)))),
      None,
    );
    let handle = Arc::new(WaclUserHandle::new(Arc::new(User::new("admin".into()))));
    s.set_user_handle(handle);
    assert_eq!(s.user_handle.as_deref(), Some("admin"));
    // 已认证但用户无命令权限 → 拒绝
    assert!(!s.acl_permits(RespCommand::Get));
  }

  /// ACL 档测试座：default 用户表（+@all nopass）+ 认证器挂载（attach 自动认证）
  fn acl_session(
    id: i64,
  ) -> (
    RespServerSession,
    Arc<parking_lot::Mutex<GarnetAclAuthenticator>>,
  ) {
    let authenticator = Arc::new(Mutex::new(GarnetAclAuthenticator::new(Arc::new(
      AccessControlList::default(),
    ))));
    let mut s = session(id);
    s.attach_acl(Some(Arc::clone(&authenticator)), None);
    (s, authenticator)
  }

  /// C# default 用户行为：+@all nopass，挂载即自动认证，全部命令放行
  #[test]
  fn acl_default_user_allows_all() {
    let (mut s, _) = acl_session(40);
    assert!(s.acl_user_handle.is_some(), "default 用户自动认证挂载");
    assert!(s.check_acl_permissions(RespCommand::Get));
    assert!(s.check_acl_permissions(RespCommand::Set));

    // 主循环整链：放行命令照常进入分派（存储域未挂载 → 非 ACL 错误）
    assert!(
      s.try_consume_messages(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
        .is_some()
    );
    assert_eq!(
      String::from_utf8(s.take_output()).unwrap(),
      "-ERR store execution domain not attached\r\n"
    );
  }

  /// C# 限权用户：ACL SETUSER (+GET) 后位图外命令被拒，回 C# NOPERM 文案
  #[test]
  fn acl_limited_user_filters_commands() {
    let (mut s, authenticator) = acl_session(41);
    // 模拟 ACL SETUSER limited +GET on >pwd：新建用户加入 ACL 用户表
    let acl = Arc::clone(authenticator.lock().get_access_control_list());
    let user = User::new("limited".to_string());
    user.add_command(RespCommand::Get).unwrap();
    user.set_enabled(true);
    user.add_password_hash(AclPassword::from_string("pwd"));
    acl
      .add_user_handle(Arc::new(UserHandle::new(Arc::new(user))))
      .unwrap();

    // AUTH 认证 → 会话按其用户位图过滤命令
    assert!(s.authenticate_user(b"limited", b"pwd"));
    assert!(s.check_acl_permissions(RespCommand::Get));
    assert!(!s.check_acl_permissions(RespCommand::Set), "位图外命令拒绝");

    // 主循环：被拒命令回 C# NOPERM 文案（RespServerSession.cs:697-700）
    assert!(
      s.try_consume_messages(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
        .is_some()
    );
    assert_eq!(
      String::from_utf8(s.take_output()).unwrap(),
      "-NOPERM this user has no permissions to run the command\r\n"
    );

    // 放行命令照常进入分派
    assert!(
      s.try_consume_messages(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
        .is_some()
    );
    assert_eq!(
      String::from_utf8(s.take_output()).unwrap(),
      "-ERR store execution domain not attached\r\n"
    );
  }

  /// C# 未认证会话：ACL 档带口令 default 用户，非 NoAuth 命令回 NOAUTH 文案
  #[test]
  fn acl_unauthenticated_rejects_with_noauth() {
    let authenticator = Arc::new(Mutex::new(GarnetAclAuthenticator::new(Arc::new(
      AccessControlList::new("pwd", None).unwrap(),
    ))));
    let mut s = session(42);
    s.attach_acl(Some(Arc::clone(&authenticator)), None);
    // default 用户带口令 → 挂载自动认证失败，保持未认证
    assert!(s.acl_user_handle.is_none());

    assert!(!s.check_acl_permissions(RespCommand::Get));
    assert!(
      s.try_consume_messages(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
        .is_some()
    );
    assert_eq!(
      String::from_utf8(s.take_output()).unwrap(),
      "-NOAUTH Authentication required.\r\n"
    );

    // NoAuth 豁免（RespCommand.cs:701-707，AUTH..=QUIT）不受 ACL 门拦截
    assert!(s.check_acl_permissions(RespCommand::Quit));
  }

  /// C# NoAuth 认证器（无 ACL）：IsAuthenticated = true 恒放行，接线不挡既有命令
  #[test]
  fn acl_no_auth_authenticator_allows_all() {
    let mut s = session(43);
    assert!(s.acl_user_handle.is_none());
    assert!(s.check_acl_permissions(RespCommand::Get));
    assert!(s.check_acl_permissions(RespCommand::Set));
    assert!(s.acl_allows_command("GET"));
  }

  #[test]
  fn no_script_bitmap_sets_discriminants() {
    // 对标 C# InitializeNoScriptDetails：NoScript 标志动态构建、字节粒度位图
    let (start, bitmap) = RespServerSession::no_script_details();

    let info = |cmd: RespCommand| {
      resp_commands_info::try_get_resp_command_info_by_cmd(cmd, false).expect("命令元数据已导入")
    };
    let stepped = |cmd: RespCommand| u16::from(cmd) as usize - start as usize;

    // NoScript 标志命令置位（含顶层与子命令代表）；字节粒度索引与实现同径
    for cmd in [
      RespCommand::Eval,
      RespCommand::Evalsha,
      RespCommand::Watch,
      RespCommand::Subscribe,
      RespCommand::Async,
      RespCommand::AclCat, // 子命令代表（ACL 顶层无 NoScript，ACL|CAT 有）
    ] {
      assert!(
        info(cmd).flags.intersects(RespCommandFlags::NO_SCRIPT),
        "{cmd:?} 前置：元数据须带 NoScript 标志"
      );
      let byte = stepped(cmd);
      assert_ne!(bitmap[byte / 8] & (1 << (byte % 8)), 0, "{cmd:?} 应置位");
    }
    // 非 NoScript 命令不应置位（取 start 之上界内命令：INFO=265）
    let byte = stepped(RespCommand::Info);
    assert_eq!(bitmap[byte / 8] & (1 << (byte % 8)), 0);
    // 位图覆盖最大判别值（WATCHOS 为 NoScript 集上界）
    assert!(bitmap.len() * 8 > stepped(RespCommand::Watchos));
  }

  #[test]
  fn eval_roundtrip_via_session() {
    let mut s = RespServerSession::new(
      30,
      RespServerSessionOptions {
        enable_lua: true,
        ..RespServerSessionOptions::default()
      },
    );
    // EVAL "return 'pong'" 0 → 脚本结果写回会话输出
    let frame = b"*3\r\n$4\r\nEVAL\r\n$13\r\nreturn 'pong'\r\n$1\r\n0\r\n";
    let consumed = s.try_consume_messages(frame);
    assert!(consumed.is_some());
    let out = s.take_output();
    let text = String::from_utf8_lossy(&out);
    assert!(text.contains("pong"), "脚本结果应回写: {text}");

    // SCRIPT LOAD + EXISTS 走同一会话面（经解析门控而非直调）
    let out = s.take_output();
    assert!(out.is_empty());
    let frame = b"*2\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n";
    let consumed = s.try_consume_messages(frame);
    assert!(consumed.is_some());
    let out = s.take_output();
    assert!(
      String::from_utf8_lossy(&out).contains("ERR"),
      "ScriptLoad 需源码参数: {out:?}"
    );
  }

  #[test]
  fn eval_disabled_rejects() {
    let mut s = RespServerSession::new(
      31,
      RespServerSessionOptions {
        enable_lua: false,
        ..RespServerSessionOptions::default()
      },
    );
    s.parse_state.initialize(2);
    assert!(s.process_other_commands(RespCommand::Eval));
    assert_eq!(
      String::from_utf8(s.take_output()).unwrap(),
      "-ERR Lua is disabled.\r\n"
    );
  }

  /// C# LuaScriptTests.IntentionalTimeout（Ctrl='Timeout' 形态）：死循环
  /// 脚本 + 短超时 → 会话在超时后被中断、返回对齐 C# 的错误文案、
  /// 同会话后续命令可用。tick 以测试线程直驱（生产形态为 compio
  /// tick 任务 spawn_lua_timeout_tick）。
  #[test]
  fn eval_infinite_loop_times_out_and_session_recovers() {
    use std::{
      thread,
      time::{Duration, Instant},
    };

    let manager = Arc::new(WluaLuaTimeoutManager::new(50));
    let mut s = RespServerSession::new(
      32,
      RespServerSessionOptions {
        enable_lua: true,
        lua_timeout_manager: Some(Arc::clone(&manager)),
        ..RespServerSessionOptions::default()
      },
    );

    // tick 线程：5ms 节拍（超时 50ms / 10，对齐 C# frequency）。
    let stop = Arc::new(AtomicBool::new(false));
    let ticker = {
      let (manager, stop) = (Arc::clone(&manager), Arc::clone(&stop));
      thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
          thread::sleep(Duration::from_millis(5));
          manager.tick();
        }
      })
    };

    // EVAL "while true do end" 0（C# TimeoutScript 的死循环形态）。
    let frame = b"*3\r\n$4\r\nEVAL\r\n$17\r\nwhile true do end\r\n$1\r\n0\r\n";
    let start = Instant::now();
    let consumed = s.try_consume_messages(frame);
    let elapsed = start.elapsed();
    stop.store(true, Ordering::Relaxed);
    ticker.join().unwrap();

    // 会话未被挂死：解析正常返回，脚本在超时窗口后被中断（tick 粒度余量）。
    assert!(consumed.is_some());
    assert!(
      elapsed >= Duration::from_millis(50),
      "中断早于配置超时: {elapsed:?}"
    );
    assert!(
      elapsed < Duration::from_secs(10),
      "死循环未被中断（会话挂死）: {elapsed:?}"
    );
    assert_eq!(
      String::from_utf8_lossy(&s.take_output()),
      "-ERR Lua script exceeded configured timeout\r\n"
    );

    // 超时机制不误伤：同会话后续脚本与普通命令均可用。
    let frame = b"*3\r\n$4\r\nEVAL\r\n$9\r\nreturn 42\r\n$1\r\n0\r\n";
    assert!(s.try_consume_messages(frame).is_some());
    assert_eq!(String::from_utf8_lossy(&s.take_output()), ":42\r\n");
    let frame = b"*1\r\n$4\r\nPING\r\n";
    assert!(s.try_consume_messages(frame).is_some());
    assert_eq!(String::from_utf8_lossy(&s.take_output()), "+PONG\r\n");
  }

  /// 生产 tick 接线形态端到端：compio runtime 内 [`spawn_lua_timeout_tick`]
  /// 驱动中断（会话在独立线程执行死循环，runtime 于 sleep await 点推进
  /// tick 任务）。
  #[test]
  fn eval_dead_loop_interrupted_by_runtime_tick_task() {
    use std::{thread, time::Duration};

    use compio::runtime::Runtime;

    let manager = Arc::new(WluaLuaTimeoutManager::new(40));
    let mut s = RespServerSession::new(
      35,
      RespServerSessionOptions {
        enable_lua: true,
        lua_timeout_manager: Some(Arc::clone(&manager)),
        ..RespServerSessionOptions::default()
      },
    );

    let frame = b"*3\r\n$4\r\nEVAL\r\n$17\r\nwhile true do end\r\n$1\r\n0\r\n".to_vec();
    let session_thread = thread::spawn(move || {
      let consumed = s.try_consume_messages(&frame);
      (consumed.is_some(), s.take_output())
    });

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      spawn_lua_timeout_tick(manager);
      while !session_thread.is_finished() {
        sleep(Duration::from_millis(5)).await;
      }
    });

    let (consumed, out) = session_thread.join().unwrap();
    assert!(consumed);
    assert_eq!(
      String::from_utf8_lossy(&out),
      "-ERR Lua script exceeded configured timeout\r\n"
    );
  }

  /// C# Timeout == InfiniteTimeSpan 形态（不建超时管理器）：有限重脚本
  /// 正常完成，超时机制零干扰。
  #[test]
  fn eval_without_timeout_runs_finite_heavy_loop() {
    let mut s = RespServerSession::new(
      33,
      RespServerSessionOptions {
        enable_lua: true,
        ..RespServerSessionOptions::default()
      },
    );
    // sum(1..100_000) = 5_000_050_000。
    let script = b"local s = 0 for i = 1, 100000 do s = s + i end return s";
    let frame = format!(
      "*3\r\n$4\r\nEVAL\r\n${}\r\n{}\r\n$1\r\n0\r\n",
      script.len(),
      String::from_utf8_lossy(script)
    );
    assert!(s.try_consume_messages(frame.as_bytes()).is_some());
    assert_eq!(String::from_utf8_lossy(&s.take_output()), ":5000050000\r\n");
  }

  /// lua_options / txn_mode 装配链最小验证：RespServerSessionOptions →
  /// 会话字段 →（经 run_lua_command）LuaSessionContext 下传 runner 构造；
  /// 事务模式下带键脚本经 run_in_transaction 开合事务窗口仍正常执行。
  #[test]
  fn lua_options_and_txn_mode_assembled_from_config() {
    let mut s = RespServerSession::new(
      34,
      RespServerSessionOptions {
        enable_lua: true,
        lua_txn_mode: true,
        lua_options: wlua::LuaOptions {
          timeout_millis: 1_234,
          ..WluaLuaOptions::default()
        },
        ..RespServerSessionOptions::default()
      },
    );
    assert!(s.lua_txn_mode);
    assert_eq!(s.lua_options.timeout_millis, 1_234);

    // 事务模式 + 1 key：run_in_transaction 窗口内执行（键锁经
    // TxnKeyEntries），脚本正常返回。
    let script = b"return redis.call('PING')";
    let frame = format!(
      "*4\r\n$4\r\nEVAL\r\n${}\r\n{}\r\n$1\r\n1\r\n$1\r\nk\r\n",
      script.len(),
      String::from_utf8_lossy(script)
    );
    assert!(s.try_consume_messages(frame.as_bytes()).is_some());
    // RESP2 下 Lua string 返回 bulk 形态（$4\r\nPONG\r\n）。
    assert_eq!(String::from_utf8_lossy(&s.take_output()), "$4\r\nPONG\r\n");
  }

  #[test]
  fn read_length_prefixed_string_partial_rollback_and_limits() {
    let mut s = session(99);

    // 1. 完整读取并大写化
    let full_frame = b"$4\r\nping\r\n";
    s.recv_buffer.clear();
    s.recv_buffer.extend_from_slice(full_frame);
    s.read_head = 0;
    s.bytes_read = full_frame.len();
    assert_eq!(
      s.get_upper_case_command().as_deref(),
      Some(b"PING".as_slice())
    );
    assert_eq!(s.read_head, full_frame.len());

    // 2. 分包断帧测试：只有前半段 $4\r\npi，不完整时必须返回 None 且 read_head 保持在 0 不动
    let partial_frame = b"$4\r\npi";
    s.recv_buffer.clear();
    s.recv_buffer.extend_from_slice(partial_frame);
    s.read_head = 0;
    s.bytes_read = partial_frame.len();
    assert_eq!(s.get_upper_case_command(), None);
    assert_eq!(s.read_head, 0, "断帧未读全时不得推进游标");

    // 3. 补齐数据后成功解析
    s.recv_buffer.extend_from_slice(b"ng\r\n");
    s.bytes_read = s.recv_buffer.len();
    assert_eq!(
      s.get_upper_case_command().as_deref(),
      Some(b"PING".as_slice())
    );
    assert_eq!(s.read_head, s.recv_buffer.len());

    // 4. 异常帧：无数字长度 $\r\n
    let invalid_frame = b"$\r\n";
    s.recv_buffer.clear();
    s.recv_buffer.extend_from_slice(invalid_frame);
    s.read_head = 0;
    s.bytes_read = invalid_frame.len();
    assert_eq!(s.get_command(), None);
    assert_eq!(s.read_head, 0);
  }

  // ---- 新接线命令族的会话级回归（NoAuth / 未装配 / 禁用语义对齐 C#）----

  fn session_frame(frame: &[u8]) -> (RespServerSession, Vec<u8>) {
    let mut s = session(0);
    let consumed = s.try_consume_messages(frame);
    assert!(consumed.is_some());
    let out = s.take_output();
    (s, out)
  }

  #[test]
  fn auth_noauth_rejects_with_configured_error() {
    // C# NoAuth 认证器：AUTH 回 "does not accept passwords"
    let (_, out) = session_frame(b"*2\r\n$4\r\nAUTH\r\n$3\r\npwd\r\n");
    assert_eq!(
      out,
      b"-ERR Client sent AUTH, but configured authenticator does not accept passwords\r\n"
    );
  }

  #[test]
  fn async_resp2_reports_unsupported() {
    let (_, out) = session_frame(b"*2\r\n$5\r\nASYNC\r\n$2\r\nON\r\n");
    assert_eq!(out, b"-ERR command not supported in RESP2\r\n");
  }

  #[test]
  fn command_root_with_args_reports_unknown_subcommand() {
    let (_, out) = session_frame(b"*2\r\n$7\r\nCOMMAND\r\n$4\r\nNOPE\r\n");
    assert_eq!(out, b"-ERR unknown subcommand 'NOPE'.\r\n");
  }

  #[test]
  fn latency_help_lists_subcommands() {
    let (_, out) = session_frame(b"*2\r\n$7\r\nLATENCY\r\n$4\r\nHELP\r\n");
    assert!(out.starts_with(b"*"), "帮助文本数组: {out:?}");
    assert!(String::from_utf8_lossy(&out).contains("HISTOGRAM"));
  }

  #[test]
  fn latency_histogram_without_monitor_is_empty_array() {
    let (_, out) = session_frame(b"*2\r\n$7\r\nLATENCY\r\n$9\r\nHISTOGRAM\r\n");
    assert_eq!(out, b"*0\r\n");
  }

  #[test]
  fn slowlog_len_without_container_is_zero() {
    let (_, out) = session_frame(b"*2\r\n$7\r\nSLOWLOG\r\n$3\r\nLEN\r\n");
    assert_eq!(out, b":0\r\n");
  }

  #[test]
  fn acl_cat_without_authenticator_reports_disabled() {
    let (_, out) = session_frame(b"*2\r\n$3\r\nACL\r\n$3\r\nCAT\r\n");
    assert_eq!(out, b"-ERR ACL Authenticator is disabled.\r\n");
  }

  #[test]
  fn publish_without_broker_reports_disabled() {
    let (_, out) = session_frame(b"*3\r\n$7\r\nPUBLISH\r\n$1\r\nc\r\n$1\r\nv\r\n");
    assert_eq!(
      out,
      b"-ERR PUBLISH is disabled, enable it with --pubsub option.\r\n"
    );
  }

  #[test]
  fn subscribe_without_broker_reports_disabled() {
    let (_, out) = session_frame(b"*2\r\n$9\r\nSUBSCRIBE\r\n$1\r\nc\r\n");
    assert_eq!(
      out,
      b"-ERR SUBSCRIBE is disabled, enable it with --pubsub option.\r\n"
    );
  }

  #[test]
  fn multi_without_txn_components_reports_error() {
    // 事务组件未注入 = 宿主装配缺口：明确报错不静默
    let (_, out) = session_frame(b"*1\r\n$5\r\nMULTI\r\n");
    assert_eq!(out, b"-ERR unknown command\r\n");
  }

  #[test]
  fn watch_in_txn_gate_is_registered_with_components() {
    use std::sync::Arc as StdArc;
    let mut s = session(0);
    s.attach_transaction_components(StdArc::new(WtxnWatchVersionMap::new(64)));
    // WATCH 无键：参数错误（事务域 abort 文案）
    let consumed = s.try_consume_messages(b"*1\r\n$5\r\nWATCH\r\n");
    assert!(consumed.is_some());
    assert!(
      s.take_output()
        .starts_with(b"-ERR wrong number of arguments")
    );
  }

  #[test]
  fn multi_exec_roundtrip_without_writes() {
    use std::sync::Arc as StdArc;
    let mut s = session(0);
    s.attach_transaction_components(StdArc::new(WtxnWatchVersionMap::new(64)));
    // MULTI → +OK；EXEC 空事务 → 空数组
    assert!(s.try_consume_messages(b"*1\r\n$5\r\nMULTI\r\n").is_some());
    assert_eq!(s.take_output(), b"+OK\r\n");
    // 排队命令（PING 在事务内跳过执行仅登记）
    assert!(s.try_consume_messages(b"*1\r\n$4\r\nPING\r\n").is_some());
    assert_eq!(s.take_output(), b"+QUEUED\r\n");
    assert!(s.try_consume_messages(b"*1\r\n$4\r\nEXEC\r\n").is_some());
    assert_eq!(s.take_output(), b"*1\r\n");
  }

  #[test]
  fn attach_pubsub_enables_publish_path() {
    use std::sync::Arc as StdArc;
    let mut s = session(0);
    s.attach_pubsub(StdArc::new(WpubsubSubscribeBroker::new(4096)));
    assert!(
      s.try_consume_messages(b"*3\r\n$7\r\nPUBLISH\r\n$1\r\nc\r\n$1\r\nv\r\n")
        .is_some()
    );
    // broker 已接线：发布到无订阅者通道回 :0（不再是禁用错误）
    assert_eq!(s.take_output(), b":0\r\n");
  }

  #[test]
  fn abort_error_message_sanitizes_crlf() {
    let mut s = session(100);
    s.abort_error_message("ERR broken\r\nINJECT");
    assert_eq!(s.take_output(), b"-ERR broken\r\n");
    assert!(s.command_error_written);
  }

  // ---- scratch 直读消费（try_consume_pending 持久游标模型，网络泵语义）----

  /// 模拟泵直填一批字节（take → extend → return → consume 的会话侧等价）
  fn pump_feed(s: &mut RespServerSession, bytes: &[u8]) -> Option<usize> {
    s.recv_buffer.extend_from_slice(bytes);
    s.try_consume_pending()
  }

  #[test]
  fn scratch_pending_half_packet_and_full_reset() {
    let mut s = session(0);
    // 批1：半包（PING 前半）→ 残余驻留，游标不动
    assert_eq!(pump_feed(&mut s, b"*1\r\n$4\r\nPI"), Some(10));
    assert_eq!(s.read_head, 0);
    // 批2：补齐 → 整段消费完毕，缓冲清零复位（容量保留）
    assert_eq!(pump_feed(&mut s, b"NG\r\n"), Some(0));
    assert!(s.recv_buffer.is_empty());
    assert_eq!(s.bytes_read, 0);
    assert_eq!(s.read_head, 0);
    assert_eq!(String::from_utf8(s.take_output()).unwrap(), "+PONG\r\n");
    // 复位后再来一批：从零游标重新消费
    assert_eq!(pump_feed(&mut s, b"*1\r\n$4\r\nPING\r\n"), Some(0));
    assert_eq!(String::from_utf8(s.take_output()).unwrap(), "+PONG\r\n");
  }

  #[test]
  fn scratch_pending_pipeline_partial_tail() {
    // 整段消费 + 半包尾随并存：游标推进越过已消费前缀，不丢后续流水线
    let mut s = session(0);
    assert_eq!(
      pump_feed(&mut s, b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPI"),
      Some(10)
    );
    assert_eq!(String::from_utf8(s.take_output()).unwrap(), "+PONG\r\n");
    assert_eq!(s.read_head, 14);
    // 补齐后续解析
    assert_eq!(pump_feed(&mut s, b"NG\r\n"), Some(0));
    assert_eq!(String::from_utf8(s.take_output()).unwrap(), "+PONG\r\n");
    assert!(s.recv_buffer.is_empty());
  }

  #[test]
  fn scratch_pending_multi_exec_cross_batch_replay() {
    // 跨批次事务：排队命令字节驻留接收缓冲，EXEC 回退重解析真执行
    //（C# bytesRead/readHead 持久游标语义；拷贝形态只能得到空数组）
    use std::sync::Arc as StdArc;
    let mut s = session(0);
    s.attach_transaction_components(StdArc::new(WtxnWatchVersionMap::new(64)));

    assert_eq!(pump_feed(&mut s, b"*1\r\n$5\r\nMULTI\r\n"), Some(0));
    assert_eq!(s.take_output(), b"+OK\r\n");
    assert_eq!(pump_feed(&mut s, b"*1\r\n$4\r\nPING\r\n"), Some(0));
    assert_eq!(s.take_output(), b"+QUEUED\r\n");
    // EXEC：先写数组头，随后回退到 MULTI 尾重解析 PING 真执行（+PONG）
    assert_eq!(pump_feed(&mut s, b"*1\r\n$4\r\nEXEC\r\n"), Some(0));
    assert_eq!(s.take_output(), b"*1\r\n+PONG\r\n");
    // 事务收尾：缓冲复位、游标归零
    assert!(s.recv_buffer.is_empty());
    assert_eq!(s.txn_state, WtxnTxnState::None);
  }

  #[test]
  fn scratch_pending_multi_exec_same_batch_matches_redis() {
    // 同批 MULTI..EXEC：回退重解析与跨批次形态应答一致（真 Redis 字节序）
    use std::sync::Arc as StdArc;
    let mut s = session(0);
    s.attach_transaction_components(StdArc::new(WtxnWatchVersionMap::new(64)));
    assert_eq!(
      pump_feed(
        &mut s,
        b"*1\r\n$5\r\nMULTI\r\n*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nEXEC\r\n"
      ),
      Some(0)
    );
    assert_eq!(s.take_output(), b"+OK\r\n+QUEUED\r\n*1\r\n+PONG\r\n");
  }

  #[test]
  fn scratch_pending_violation_signals_none() {
    let mut s = session(0);
    // 畸形数组计数（C# RespParsingException）→ None，泵发尽应答后断连；
    // 协议错误应答已落输出（C# catch 块 ERR Protocol Error 文案对标）
    assert_eq!(pump_feed(&mut s, b"*A\r\nGET\r\n"), None);
    assert!(s.parse_violation.is_none(), "哨兵应被消费复位");
    assert_eq!(
      s.take_output(),
      b"-ERR Protocol Error: Unexpected character 'A'.\r\n"
    );
  }

  /// 致命断流哨兵（GarnetException clientResponse:false 等价）：置位后
  /// try_consume_messages / try_consume_pending 回 None 且不写错误行，
  /// 泵发尽累积应答后断连
  #[test]
  fn fatal_disconnect_signals_none_without_error_line() {
    let mut s = session(0);
    s.fatal_disconnect = true;
    assert!(s.try_consume_messages(b"*1\r\n$4\r\nPING\r\n").is_none());
    // 同批命令应答照常发尽（+PONG），致命断流不追加任何错误行
    assert_eq!(s.take_output(), b"+PONG\r\n");

    // scratch 直读形态同一 None 通道
    assert!(s.try_consume_pending().is_none());
  }

  #[test]
  fn scratch_pending_violation_preserves_prior_replies() {
    let mut s = session(0);
    // 同批：合法 PING 应答在前、协议错误在后（C# 错误追加在累积应答尾部，
    // Send 一次发出后 DisposeNetworkSender 断连）
    assert_eq!(pump_feed(&mut s, b"*1\r\n$4\r\nPING\r\n*A\r\n"), None);
    assert_eq!(
      s.take_output(),
      b"+PONG\r\n-ERR Protocol Error: Unexpected character 'A'.\r\n"
    );
  }

  #[test]
  fn scratch_pending_empty_is_harmless_noop() {
    let mut s = session(0);
    assert_eq!(pump_feed(&mut s, b""), Some(0));
    assert!(s.take_output().is_empty());
  }

  // ---- 命令名层协议违例（get_command_range 畸形帧断连；C# GetCommand 经
  // TryReadUnsignedLengthHeader 抛 RespParsingException 的哨兵等价）----

  #[test]
  fn scratch_pending_cmd_bad_sigil_disconnects() {
    let mut s = session(0);
    // 数组头合法、命令名头 sigil 非 '$'（C# ThrowUnexpectedToken）
    assert_eq!(pump_feed(&mut s, b"*1\r\n:5\r\n"), None);
    assert_eq!(
      s.take_output(),
      b"-ERR Protocol Error: Unexpected character ':'.\r\n"
    );
  }

  #[test]
  fn scratch_pending_cmd_non_digit_length_disconnects() {
    let mut s = session(0);
    // '$' 后无数字（C# digitsRead==0 → ThrowUnexpectedToken；
    // MakeUpperCase 已就地大写化，文案对齐大写后形态）
    assert_eq!(pump_feed(&mut s, b"*1\r\n$abc\r\n"), None);
    assert_eq!(
      s.take_output(),
      b"-ERR Protocol Error: Unexpected character 'A'.\r\n"
    );
  }

  #[test]
  fn scratch_pending_cmd_length_u64_overflow_disconnects() {
    let mut s = session(0);
    // 20 位以上触发 u64 溢出（C# TryReadUInt64 慢路径 ThrowIntegerOverflow，
    // 文案数字串不含触发位）
    let frame = format!("*1\r\n${}\r\n", "9".repeat(23));
    assert_eq!(pump_feed(&mut s, frame.as_bytes()), None);
    assert_eq!(
      s.take_output(),
      b"-ERR Protocol Error: Unable to parse integer. The given number is larger than allowed: 9999999999999999999\r\n"
    );
  }

  #[test]
  fn scratch_pending_cmd_length_int_overflow_disconnects() {
    let mut s = session(0);
    // 数值超 int.MaxValue（C# signed 层 ThrowIntegerOverflow，完整数字串）
    assert_eq!(pump_feed(&mut s, b"*1\r\n$3000000000\r\n"), None);
    assert_eq!(
      s.take_output(),
      b"-ERR Protocol Error: Unable to parse integer. The given number is larger than allowed: 3000000000\r\n"
    );
  }

  #[test]
  fn scratch_pending_cmd_bad_header_terminator_disconnects() {
    let mut s = session(0);
    // 头尾终止符 '\rX'（C# ThrowUnexpectedToken；取违规终止符首字节，
    // 控制字符转义 '\x0d'，与数组头层惯例一致）
    assert_eq!(pump_feed(&mut s, b"*1\r\n$3\rXabc\r\n"), None);
    assert_eq!(
      s.take_output(),
      b"-ERR Protocol Error: Unexpected character '\\x0d'.\r\n"
    );
  }

  #[test]
  fn scratch_pending_cmd_bad_value_terminator_disconnects() {
    let mut s = session(0);
    // 值尾终止符 '\rX'（C# GetCommand ThrowUnexpectedToken，传终止符首字节）
    assert_eq!(pump_feed(&mut s, b"*1\r\n$3\r\nabc\rX"), None);
    assert_eq!(
      s.take_output(),
      b"-ERR Protocol Error: Unexpected character '\\x0d'.\r\n"
    );
  }

  #[test]
  fn scratch_pending_cmd_negative_length_disconnects() {
    // '$-1\r\n' NULL 特例（C# Unsigned 包装层 ThrowInvalidStringLength(-1)）
    let mut s = session(0);
    assert_eq!(pump_feed(&mut s, b"*1\r\n$-1\r\n"), None);
    assert_eq!(
      s.take_output(),
      b"-ERR Protocol Error: Invalid string length '-1'.\r\n"
    );
    // 任意负长度同抛
    let mut s = session(1);
    assert_eq!(pump_feed(&mut s, b"*1\r\n$-5\r\n"), None);
    assert_eq!(
      s.take_output(),
      b"-ERR Protocol Error: Invalid string length '-5'.\r\n"
    );
    // RESP3 NULL '_\r\n' 出现在命令名位（C# 解析为 -1 后同样断连）
    let mut s = session(2);
    assert_eq!(pump_feed(&mut s, b"*1\r\n_\r\n"), None);
    assert_eq!(
      s.take_output(),
      b"-ERR Protocol Error: Invalid string length '-1'.\r\n"
    );
  }

  #[test]
  fn scratch_pending_cmd_subcommand_bad_sigil_disconnects() {
    let mut s = session(0);
    // 子命令名层（get_upper_case_command_range → get_command_range）同形态
    // 断连：哨兵经 hash_lookup_command 的 `?` 透传至既有 violation 路径
    assert_eq!(pump_feed(&mut s, b"*2\r\n$7\r\nCLUSTER\r\n:5\r\n"), None);
    assert_eq!(
      s.take_output(),
      b"-ERR Protocol Error: Unexpected character ':'.\r\n"
    );
  }

  #[test]
  fn scratch_pending_cmd_violation_preserves_prior_replies() {
    let mut s = session(0);
    // 同批：合法 PING 应答在前、命令名层协议错误在后（C# 错误追加在
    // 累积应答尾部，Send 一次发出后断连）
    assert_eq!(pump_feed(&mut s, b"*1\r\n$4\r\nPING\r\n*1\r\n:5\r\n"), None);
    assert_eq!(
      s.take_output(),
      b"+PONG\r\n-ERR Protocol Error: Unexpected character ':'.\r\n"
    );
  }

  #[test]
  fn scratch_pending_cmd_half_packet_keeps_waiting() {
    let mut s = session(0);
    // 半包：命令名值未收齐 → 等待不误断（残余驻留、游标不动、无输出）
    assert_eq!(pump_feed(&mut s, b"*1\r\n$3\r\nab"), Some(10));
    assert!(s.take_output().is_empty());
    assert_eq!(s.read_head, 0);
    // 512MB 超限但未溢出形态：对齐 C# GetCommand 无超限断连，按不完整等待
    let mut s = session(1);
    assert_eq!(pump_feed(&mut s, b"*1\r\n$600000000\r\n"), Some(16));
    assert!(s.take_output().is_empty());
  }
}
