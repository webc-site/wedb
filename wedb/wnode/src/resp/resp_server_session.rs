//! RESP 服务器会话（对标 libs/server/Resp/RespServerSession.cs:RespServerSession）
//!
//! C# 会话直接持有网络发送器 / Tsavorite 上下文 / 事务管理器；Rust 侧这些面
//! 分别由并行域（wkv 会话纪元 / transaction / cluster）承载，本结构承接会话
//! 状态本体：Id / 端点 / CreationTicks / RESP 协议版本 / clientName·lib-* /
//! useAsync / 数据库会话映射 / 订阅与事务模式 / 延迟与会话指标 / 接收缓冲
//! 解析游标 / 输出缓冲，以及 C# 的分派 · 发送 · 数据库切换方法族。
//!
//! 输出缓冲模型：C# 的 `dcurr/dend` 指针游标对应 `output: Vec<u8>` 的
//! `len/capacity`；`Send` 与 `SendAndReset` 合为 [`RespServerSession::take_output_into`]
//! 一枚冲取出面。

use std::{
  fmt::{self, Display, Formatter},
  iter,
  mem::{self, size_of},
  str::from_utf8,
  sync::{Arc, OnceLock, atomic::AtomicU64},
};

use gxhash::HashMap as GxHashMap;
use itoa::Buffer;
use parking_lot::Mutex;
use smallvec::SmallVec;
use wacl::{GarnetAclAuthenticator, UserHandle, acl_password_check};
use wbase::{
  future::blocking_wait,
  hash_slot::slot_of,
  time::{now_ms, now_nanos, now_stopwatch_ticks},
};
use wcol::{
  itembroker::collection_item_observer::CollectionItemResult, object_payload::obj_decode_custom,
};
use wconf::{DEFAULT_RESP_VERSION, NodeArgs, RuntimeServerConfig, ServerConfigType};
use wcustom::{CommandType, CustomObjectFns};
use wdev::Device;
use wkv::{StoreResult, WedbStore};
use wlua::{
  LuaCommands, LuaOptions, LuaSessionContext, LuaTimeoutManager, ScriptingApi, SessionScriptCache,
  StoreScriptCache,
};
use wmetric::{
  CommandStats, GarnetInfoMetrics, GarnetLatencyMetrics, GarnetLatencyMetricsSession,
  GarnetServerMonitor, InfoCommand, LatencyMetricsType, SessionMetricsHandle, SlowLogContainer,
};
use wpubsub::{
  session_commands::{PubSubSession, PubSubSessionCommands},
  subscribe_broker::SubscribeBroker,
};
use wresp::{
  argslice::ArgSlice,
  catalog::{
    RespCommandFlags, is_no_auth, normalize_for_acls, try_get_resp_commands_info,
    try_get_simple_resp_command_info,
  },
  cmd_strings::{self as cs, write_map_len},
  command::{RespCommand, is_cluster_sub_command, one_if_read, one_if_write},
  ext::{MAX_ERROR_MSG_LEN, RespSliceExt, RespVecExt, sanitize_error_str},
  key_spec::KeySpecificationFlags,
  metrics::InfoMetricsType,
  read::{ReplyError, parse_bulk_reply, parse_simple_reply},
  session_parse_state::{MAX_ARGUMENT_LENGTH_BYTES, SessionParseState},
};
use wtxn::{
  TransactionManager, TxnCommandKeys, TxnKeySpec, TxnLockTable, TxnQueuedCommandInfo, TxnState,
  WatchVersionMap,
};
use wval::KeyTag;

use super::{
  BlockedWait, ItemBroker,
  acl_commands::AclAuthOutcome,
  acl_store::AclStore,
  garnet_api::GarnetApi,
  parser::resp_command::{MAX_RESP_ARRAY_LENGTH, MruCommandCache, is_allowed_in_subscription_mode},
  session_dependencies::SessionDependencies,
  slow_path::SlowWait,
};
use crate::{
  aof::garnet_append_only_file::GarnetAppendOnlyFile,
  cluster_provider::ClusterProviderHandle,
  cluster_session::{ClusterSession, ClusterSlotVerificationInput, SlotVerifyGate},
  primary_tasks::PrimaryTasks,
  servers::consumer_registry::write_client_info_fields,
  service::assemble_lua_timeout,
};

/// libs/host/GarnetServer.cs:RedisProtocolVersion（HELLO 应答 version 字段）
pub const REDIS_PROTOCOL_VERSION: &str = "7.4.3";

/// RESP framing of `GET key` up to and including the command token: `*2\r\n$3\r\nGET\r\n`
/// (libs/server/Resp/BasicCommands.cs:GetCommandRespPrefix)
pub const GET_COMMAND_RESP_PREFIX: &[u8] = b"*2\r\n$3\r\nGET\r\n";

/// 存储执行域未挂载时的拒绝文案（C# 构造必带 storeWrapper 无此态；
/// rust 侧为宿主装配缺口的显式防线）
const ERR_STORE_DOMAIN_NOT_ATTACHED: &str = "ERR store execution domain not attached";

/// 接收缓冲默认驻留容量（对标泵 64KB 池化接收缓冲水位；scratch 直读形态
/// 整段消费完毕后超限容量即释放回归此水位）
const DEFAULT_RECV_BUFFER_CAPACITY: usize = 1 << 16;

/// 输出缓冲默认驻留容量（在 garnet 中的相对路径:libs/common/Networking/GarnetTcpNetworkSender.cs:GarnetTcpNetworkSender
/// ——C# 发送侧水位即构造传入的 NetworkBufferSettings.sendBufferSize；rust 会话以
/// 平铺 Vec 承载 output，初始构造与 take_output_into 整段换出后低于下界时的补充
/// 均回归此水位；与接收水位同为 64KB 但语义独立，C# 两域各为独立配置，禁止混用）
const DEFAULT_OUTPUT_BUFFER_CAPACITY: usize = 1 << 16;

/// 批内输出水位（在 garnet 中的相对路径:libs/common/NetworkBufferSettings.cs:NetworkBufferSettings
/// ——默认 sendBufferSize = 1 << 17，即 C# 响应缓冲上界）。批内累计应答达此界
/// 即在命令边界停住交泵实写再续消费：RespWriteUtils TryWrite 写不下即
/// SendAndReset 满刷循环（libs/server/Resp/RespServerSession.cs:SendAndReset）
/// 的 rust 投影，粒度为命令边界而非写点（RespWriter 借用 output，写点复查
/// 在借用模型下不可行）
const OUTPUT_WATERMARK_BYTES: usize = 1 << 17;

/// 连接保护选项单源承接（定义已上收 wconf 配置域供 NodeArgs 旋钮复用；
/// 此处 re-export 维持本模块既有引用路径不变）
///
/// libs/server/Auth/Settings/ConnectionProtectionOption.cs:ConnectionProtectionOption
pub use wconf::ConnectionProtectionOption;

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
  /// Lua 事务模式（C# serverOptions.LuaTransactionMode；脚本 runner 构造
  /// 的 txn_mode 源头）
  pub lua_txn_mode: bool,
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
      // LuaTransactionMode = false、Timeout == Infinite 不建管理器。
      lua_options: LuaOptions::default(),
      lua_txn_mode: false,
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
      lua_txn_mode: node.lua_transaction_mode,
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

/// 自定义命令引用（C# currentCustomTransaction / CustomProcedure /
/// CustomRawStringCommand / CustomObjCmd 的会话侧投影）。扩展命令编译期
/// 静态接线：解析期经 resp::custom_objects 静态清单取全量执行面入槽，
/// 会话侧零克隆零回查。
#[derive(Clone)]
pub struct CustomCommandRef {
  /// 命令名（静态清单规范形，零分配）
  pub name: &'static str,
  /// 命令类型（Read / ReadModifyWrite）
  pub command_type: CommandType,
  /// arity（0 = 不校验；负值 = 至少 -arity-1 个参数）
  pub arity: i32,
  /// 对象信封类型标签（wval::CustomObjectType 分配单点，经 wcustom
  /// CustomObjectEntry 静态描述清单流转）
  pub object_tag: u8,
  /// 静态执行体（编译期函数指针集）
  pub fns: CustomObjectFns,
}

impl fmt::Debug for CustomCommandRef {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    f.debug_struct("CustomCommandRef")
      .field("name", &self.name)
      .field("command_type", &self.command_type)
      .field("arity", &self.arity)
      .field("object_tag", &self.object_tag)
      .finish_non_exhaustive()
  }
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
  pub acl_user_handle: Option<Arc<UserHandle>>,
  /// 认证器是否支持认证（C# _authenticator.CanAuthenticate；免认证形态为 false）
  pub authenticator_can_authenticate: bool,

  /// ASKING 跳过计数（C# SessionAsking）
  pub session_asking: u8,
  /// 是否为只读会话（C# readOnlySession）
  pub read_only_session: bool,
  /// 当前会话所属命名空间（0 为全局超管空间，连接认证时根据 <ns>#user 绑定）
  pub namespace: u64,
  /// 当前活跃库编号（C# activeDatabaseId）
  pub active_db_id: u64,
  /// 最大库数上界（C# serverOptions.MaxDatabases；SELECT/SWAPDB 校验用）
  pub max_databases: u64,
  /// 是否处于发布订阅模式（C# isSubscriptionSession）
  pub is_subscription_session: bool,
  /// 事务状态镜像（C# txnManager.state 的会话侧视图）
  pub txn_state: TxnState,
  /// 本批消息是否已标记 AOF 阻塞等待（C# waitForAofBlocking；解析期
  /// `handle_aof_commit_mode` 维护，网络泵写出段读取——置位则应答出网前
  /// 等 AOF 提交落盘，对标 C# RespServerSession.cs:Send 内的阻塞点）
  pub wait_for_aof_blocking: bool,
  /// 本批是否含慢命令（NET_RS 直方图分桶；C# containsSlowCommand）
  pub contains_slow_command: bool,
  /// 命令执行期写出的错误标记（CommandStats 失败判定；C# commandErrorWritten）
  pub command_error_written: bool,
  /// 会话是否待释放（QUIT；C# toDispose；网络泵发尽应答后断连）
  pub to_dispose: bool,

  /// 会话指标共享句柄（C# sessionMetrics 类实例的 Arc 承接；采样关闭时为 None，
  /// 与存储执行域共持同一对象，写口 &self 原子累加，计数单源）。
  /// 唯一写入点是本类型的 [`Self::attach_session_metrics`]（消费者侧同名转发口
  /// `resp_session_consumer.rs:attach_session_metrics`），创建点唯一在
  /// `service.rs` 装配期按采样频率门控；构造期恒 None，选项侧不再有开关。
  pub session_metrics: Option<Arc<SessionMetricsHandle>>,
  /// 逐命令统计表（C# commandStats；CommandStatsMonitor 关闭时为 None。
  /// 单写者为主循环递增，monitor 采样 / dispose 归并 / INFO 聚合为读者，
  /// 以 parking_lot 互斥承接）
  pub command_stats: Option<Arc<parking_lot::Mutex<CommandStats>>>,
  /// 延迟指标（C# LatencyMetrics；监视关闭时为 None；监视器迭代时钟由
  /// 延迟指标实例内部持有 —— C# LatencyMetrics._monitorIterations）
  pub latency_metrics: Option<Arc<GarnetLatencyMetricsSession>>,

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
  /// 批内输出水位让渡哨兵（OUTPUT_WATERMARK_BYTES 满刷的命令边界
  /// 投影：置位表示本批因输出达水位在命令边界停住、接收缓冲尚有完整帧；
  /// 网络泵取走后实写本轮应答并立即重入消费，批首复位）
  output_watermark_yield: bool,

  /// 当前待执行自定义命令（C# currentCustomTransaction / Procedure 等共用槽）
  pub current_custom_command: Option<(RespCommand, CustomCommandRef)>,
  /// MSETNX 补写标记：快路径写入段遇异步闭环信号降级时置位（NX 判定已
  /// 整体通过、前缀键已持久写入），exec 降级快照据此追加模式尾参，慢路径
  /// 续写全部键值回 :1；判定段降级保持 false（慢路径须先完整裁决存活）。
  /// 进入命令即复位、dispatch 消费后复位，无跨命令残留
  pub msetnx_resume: bool,
  /// 流水线 Scatter-Gather GET 聚合键列表（C# pendingGetOutputArr 的等价承接）
  pub sg_batched_keys: Option<Vec<Vec<u8>>>,
  /// MRU 命令缓存（C# _cachedCmd0/1；resp_command 域维护）
  pub(crate) mru_cache: MruCommandCache,
  /// 会话脚本缓存（EnableLua 时创建；C# sessionScriptCache）
  pub session_script_cache: Option<SessionScriptCache>,
  /// 全局脚本缓存（C# storeWrapper.storeScriptCache，进程级共享）
  pub store_script_cache: Arc<StoreScriptCache>,
  /// Lua 会话选项（C# serverOptions.LuaOptions 的会话侧投影；脚本命令
  /// 装配 LuaSessionContext 的取值源）
  pub lua_options: LuaOptions,
  /// Lua 事务模式（C# serverOptions.LuaTransactionMode；runner 构造下传）
  pub lua_txn_mode: bool,
  /// 内建命令分派器（宿主持存储执行域注入；None 时命令解析/门控仍闭环，
  /// 落到分派的命令写明错误，绝不静默）。慢路径分派（`Ok(false)` 降级）
  /// 在 exec 内经此克隆句柄构造 [`SlowWait`]
  pub garnet_api: Option<GarnetApi>,

  /// EnableDebugCommand 镜像（C# storeWrapper.serverOptions.EnableDebugCommand）
  connection_protection_debug: ConnectionProtectionOption,

  /// AOF 提交等待门控（C# storeWrapper.serverOptions 的 EnableAOF &&
  /// WaitForCommit 投影；解析器按此决定是否维护 wait_for_aof_blocking）
  pub aof_commit_mode_gate: bool,

  /// 集群会话切面（C# clusterSession；None = 单机形态，命令路径与
  /// C# clusterSession == null 分支一致）
  pub cluster_session: Option<ClusterSession>,
  /// 集群提供者切面（C# clusterProvider；只读查询与缓冲池管理）
  pub cluster_provider: Option<ClusterProviderHandle>,

  /// 集合项经纪（C# storeWrapper.itemBroker；None = 装配未注入，阻塞命令
  /// 走立即可取降级路径）
  pub item_broker: Option<Arc<ItemBroker>>,

  /// 挂起中的阻塞命令等待体（网络泵 take 后 await 驱动；C# 由网络线程
  /// BlockingWait 内联承担）
  pub pending_block: Option<BlockedWait>,
  /// 冷上下文挂起面（严格会话 set_context 报告未装载时登记 (ns, db)，
  /// 应答组装点据此挂起 SlowWait 点查装载；派发前置空防跨命令残留）
  cold_ctx: Option<(u64, u64)>,

  /// 挂起中的慢路径执行体（SCAN/KEYS/DBSIZE/CLUSTER RESET 等同步段
  /// 返回 Ok(false) 的命令；网络泵 take 后 await 驱动，应答按流水线
  /// 顺序写回——C# 网络线程同步执行慢命令的 compio 异步等价物）
  pub pending_slow: Option<SlowWait>,

  /// 重驱型挂起标志（槽位门 Wait / 迭代门 Pending 登记等待体时置位）：
  /// 与产应答型 pending_slow 不同，等待体不产出应答，消费循环须回退游标
  /// 至本命令起点，等待体驱动至迁移推进/超时后重评重驱本命令
  pub(crate) pending_rearm: bool,

  /// 运行时配置（C# storeWrapper.runtimeConfig；OBJECT_SCAN_COUNT_LIMIT 等
  /// 热更读取源，CONFIG SET 经同一实例生效）
  pub runtime_config: Arc<RuntimeServerConfig>,

  /// Primary 类后台任务生命周期域（C# storeWrapper 任务域可达面；None =
  /// 纯协议层 mock 形态，CONFIG SET 调停无目标仅落槽位）
  pub primary_tasks: Option<Arc<PrimaryTasks>>,

  /// AOF 追加日志门面（C# storeWrapper.appendOnlyFile；CONFIG SET
  /// aof-sync-max-lag-bytes 背压预算调停的推送位。None = 无 AOF / 纯协议层
  /// mock 形态，调停无目标仅落槽位）
  pub aof: Option<Arc<GarnetAppendOnlyFile>>,

  /// 所属引擎实例锁表句柄（C# SessionFunctionsWrapper.cs:30 的
  /// `_clientSession.store.LockTable` 对位：会话不自建锁表，注入依赖时
  /// 从所属实例取句柄；未注入的纯协议层形态自持一把私有表）。
  /// 事务管理器与脚本键集共用此句柄，Arc 薄克隆、`Send + Sync` 自动成立
  pub lock_table: TxnLockTable,
  /// 事务管理器（C# txnManager；构造期注入 WatchVersionMap 与所属实例锁表
  /// 后创建，None = 宿主未挂事务组件，MULTI 按未接线报错。锁面为无守卫的
  /// 条带闩句柄（对标 C# OverflowBucketLockTable 的 TryLock/Unlock 显式协议），
  /// 持锁记录是纯数据，故类型层面 `Send + Sync` 自动推导成立，
  /// 无需手写 `unsafe impl`，亦不依赖消费线程亲和
  pub txn_manager: Option<TransactionManager>,
  /// 发布订阅会话接线（C# subscribeBroker 字段 + numActiveChannels；
  /// 默认无 broker = --pubsub 关闭形态，命令面按同款禁用文案回错）
  pub pubsub: PubSubSession,
  /// 慢日志容器（C# storeWrapper.slowLogContainer；None = 未启用）
  pub slow_log_container: Option<Arc<SlowLogContainer>>,
  /// 全局延迟指标（C# storeWrapper.monitor.GlobalMetrics.globalLatencyMetrics）
  pub global_latency_metrics: Option<Arc<parking_lot::Mutex<GarnetLatencyMetrics>>>,
  /// 慢日志批次起始 tick（C# slowLogStartTime；try_consume_messages 入口刷新，
  /// 与延迟监视同取 `wbase::time::now_stopwatch_ticks` 单调计时域）
  pub slow_log_start_ticks: u64,
  /// ACL 认证器（C# `_authenticator` 的 ACL 档实例；None = 未装配认证器的
  /// 免认证形态。authenticate 记录句柄需 &mut，以 parking_lot 互斥承接）
  pub acl_authenticator: Option<Arc<parking_lot::Mutex<GarnetAclAuthenticator>>>,
  /// 脚本期 no-script 位图起始判别值（C# AdminCommands.cs:23 noScriptStart）
  no_script_start: i32,
  /// 脚本期 no-script 位图（C# AdminCommands.cs:24 noScriptBitmap；None =
  /// 未进入脚本期，等价 C# 位图 null 的放行路径。挂载后常驻——C# 不摘除，
  /// 位图源为进程级静态，零 Arc 借用）
  pub no_script_bitmap: Option<&'static [u64]>,
}

impl RespServerSession {
  /// libs/server/Resp/RespServerSession.cs:RespServerSession（构造）
  ///
  /// 创建默认库会话（DB 0）并置为活跃；认证默认用户（C# 构造尾部
  /// AuthenticateUser(defaultUser)）。
  pub fn new(id: i64, options: RespServerSessionOptions) -> Self {
    let global_latency_metrics =
      GarnetServerMonitor::global().and_then(|m| m.global_latency_metrics());
    let latency_metrics = options.latency_monitor.then(|| {
      let monitor_iterations = GarnetServerMonitor::global()
        .map(|m| Arc::clone(&m.monitor_iterations))
        .unwrap_or_else(|| Arc::new(AtomicU64::new(0)));
      Arc::new(GarnetLatencyMetricsSession::new(
        monitor_iterations,
        GarnetLatencyMetricsSession::DEFAULT_LATENCY_TYPES,
      ))
    });

    let mut session = Self {
      id,
      creation_ticks: session_now_ms(),
      remote_endpoint: String::new(),
      local_endpoint: String::new(),
      resp_protocol_version: DEFAULT_RESP_VERSION,
      client_name: None,
      client_lib_name: None,
      client_lib_version: None,
      user_handle: None,
      acl_user_handle: None,
      authenticator_can_authenticate: false,
      session_asking: 0,
      read_only_session: false,
      namespace: 0,
      active_db_id: 0,
      max_databases: options.max_databases,
      is_subscription_session: false,
      txn_state: TxnState::None,
      wait_for_aof_blocking: false,
      contains_slow_command: false,
      command_error_written: false,
      to_dispose: false,
      // 会话指标句柄构造期恒 None：C# RespServerSession.cs:264 的构造期单点判定
      // 在本仓被搬到 provider.get_session 按连接装配（须与存储执行域共持同一 Arc，
      // 注入口唯一为 Self::attach_session_metrics）；唯一创建点为 service.rs
      // 采样门控，此处不留选项侧死轨。
      session_metrics: None,
      command_stats: options
        .command_stats_monitor
        .then(|| Arc::new(Mutex::new(CommandStats::new()))),
      latency_metrics,
      parse_state: SessionParseState::new(),
      recv_buffer: Vec::with_capacity(DEFAULT_RECV_BUFFER_CAPACITY),
      bytes_read: 0,
      read_head: 0,
      end_read_head: 0,
      parse_violation: None,
      fatal_disconnect: false,
      output: Vec::with_capacity(DEFAULT_OUTPUT_BUFFER_CAPACITY),
      output_watermark_yield: false,
      current_custom_command: None,
      msetnx_resume: false,
      sg_batched_keys: None,
      connection_protection_debug: options.enable_debug_command,
      aof_commit_mode_gate: options.enable_aof && options.wait_for_commit,
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
      cluster_provider: None,
      item_broker: None,
      pending_block: None,
      cold_ctx: None,
      pending_slow: None,
      pending_rearm: false,
      runtime_config: RuntimeServerConfig::shared_default(),
      primary_tasks: None,
      aof: None,
      lock_table: TxnLockTable::new(),
      txn_manager: None,
      pubsub: PubSubSession::with_mailbox_capacity(None, 4),
      slow_log_container: None,
      global_latency_metrics,
      slow_log_start_ticks: 0,
      acl_authenticator: None,
      no_script_start: 0,
      no_script_bitmap: None,
    };
    // C# 构造尾部 AuthenticateUser(defaultUser)：免认证形态即落到默认用户
    //（GetDefaultUserHandle 兜底）；ACL 档挂载前为空操作
    session.authenticate_user(options.default_user.as_bytes(), &[]);
    session
  }

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

  /// 集合更新唤醒（C# StorageSession ListOps/SortedSetOps 写成功后
  /// `itemBroker?.HandleCollectionUpdate(key)`——阻塞观察者经经纪主循环
  /// 试取指派；无经纪或键无观察者均为无害空操作）
  pub(crate) fn notify_collection_update(&self, key: &[u8]) {
    if let Some(broker) = &self.item_broker {
      broker.handle_collection_update(key);
    }
  }

  /// 取走冷上下文挂起面（应答组装点消费；None = 上下文已物化，直接应答）
  #[inline]
  pub(crate) fn take_cold_ctx(&mut self) -> Option<(u64, u64)> {
    self.cold_ctx.take()
  }

  /// 挂起冷上下文点查装载（严格会话上下文切换的异步闭环）
  ///
  /// 严格会话 `set_context` 报告映射未装载时：预组应答字节交由 SlowWait，
  /// future 点查磁盘 DbMeta 装载既有映射后经 api 重放上下文物化，再原样
  /// 产出应答——应答按流水线序写回，挂起期间本批停止消费，后续命令看到的
  /// 一定是装载后的上下文（磁盘为映射权威，装载不改任何既有映射）
  pub(crate) fn park_cold_context_load<D: wdev::Device>(
    &mut self,
    store: &Arc<WedbStore<D>>,
    ns: u64,
    db: u64,
    reply: Vec<u8>,
  ) {
    let Some(api) = self.garnet_api.clone() else {
      // 无存储执行域（理论不可达：严格会话必经 garnet_api 装配）：直回应答防挂死
      self.output.extend_from_slice(&reply);
      return;
    };
    let store = Arc::clone(store);
    self.pending_slow = Some(SlowWait::new(async move {
      match store.resolve_context(ns, db).await {
        Ok(_) => {
          api.set_context(ns, db);
          reply
        }
        Err(_) => {
          let mut out = Vec::with_capacity(cs::RESP_ERR_SLOW_PATH_STORAGE.len() + 5);
          cs::write_error_raw(&mut out, cs::RESP_ERR_SLOW_PATH_STORAGE);
          out
        }
      }
    }));
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
  /// [`Self::resolve_blocked_wait_into`] 写回应答）
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
  ///
  /// 先经 [`Self::take_output_into`] 冲出会话缓冲内已累积的应答，再把本条
  /// 阻塞命令的应答直写目标缓冲：该段字节绕过 `output`，故出向量另经
  /// [`Self::account_output`] 单点入账（与冲出口同一份实现，两处量取口径）
  pub fn resolve_blocked_wait_into(
    &mut self,
    cmd: RespCommand,
    result: CollectionItemResult,
    resp_buf: &mut Vec<u8>,
  ) {
    self.take_output_into(resp_buf);
    let start_len = resp_buf.len();
    let resp_version = self.resp_protocol_version;
    super::objects::list_commands::write_collection_item_result(
      cmd,
      &result,
      resp_version,
      resp_buf,
    );
    self.account_output((resp_buf.len() - start_len) as u64);
  }

  /// 慢路径完成后的应答写出（C# 慢命令在网络线程同步执行段的应答产出点）：
  /// 与 [`Self::resolve_blocked_wait_into`] 同一收尾形态——先冲出会话已累积
  /// 应答，再把挂起体产出的应答字节按流水线顺序并入目标写缓冲，绕过
  /// `output` 的这段同样经 [`Self::account_output`] 单点入账
  ///
  /// 网络泵与脚本重入两处承接点共用此一枚并入口，杜绝「挂起应答如何落
  /// 缓冲」的第二份实现
  pub fn resolve_slow_wait_into(&mut self, reply: &[u8], resp_buf: &mut Vec<u8>) {
    self.take_output_into(resp_buf);
    let start_len = resp_buf.len();
    resp_buf.extend_from_slice(reply);
    self.account_output((resp_buf.len() - start_len) as u64);
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
    self.cluster_provider = None;
    if let Some(txn) = &mut self.txn_manager {
      txn.cluster_enabled = false;
    }
    self.merge_metrics_history_session_dispose();
  }

  /// 会话当前库的集群槽位（doc/zh/db.md 4.1 库级定槽的会话侧消费面）：
  /// `Slot = Mixer(namespace, active_db) & SLOT_MASK`，唯一真值源
  /// `wbase::hash_slot::slot_of`；键内容一律不参与定槽（键级 CRC16 与
  /// `{...}` 散列标签已废除），故同库多键命令 / MULTI 事务 / Lua 脚本恒共
  /// 一槽，跨槽错误在架构层消失（doc/zh/db.md 4.4）。
  /// SELECT/SWAPDB 切库即时改判此值（⚠️ 集群下切库的属主门禁见 array_commands
  /// 的 network_swapdb 归属判定，本函数不做属主判定）
  #[inline]
  pub fn active_db_slot(&self) -> u16 {
    slot_of(self.namespace, self.active_db_id)
  }

  /// libs/server/Resp/BasicCommands.cs:NextCommandMaybeGet
  ///
  /// 前视探查接收缓冲中下一条命令是否可能为 `GET key`（以 `*2\r\n$3\r\nGET\r\n` 快速前缀比对）
  #[inline]
  pub fn next_command_maybe_get(&self) -> bool {
    let head = self.end_read_head;
    let end = self.bytes_read;
    if head >= end {
      return false;
    }
    self
      .recv_buffer
      .get(head..end)
      .is_some_and(|buf| buf.starts_with(GET_COMMAND_RESP_PREFIX))
  }

  /// libs/server/Resp/BasicCommands.cs:ParseGETAndKey
  ///
  /// 投机式前视解析下一条命令是否为 `GET` 并提取其 `key` 参数。
  /// 命中时推进游标并返回其 key 的 ArgSlice；未命中或校验失败时回退游标并返回 None。
  pub fn parse_get_and_key(&mut self) -> Option<ArgSlice> {
    if !self.next_command_maybe_get() {
      return None;
    }
    let old_end_read_head = self.end_read_head;
    self.read_head = old_end_read_head;
    let Some(cmd) = self.parse_command() else {
      self.read_head = old_end_read_head;
      self.end_read_head = old_end_read_head;
      return None;
    };
    if cmd != RespCommand::Get || self.parse_state.count != 1 {
      self.read_head = old_end_read_head;
      self.end_read_head = old_end_read_head;
      return None;
    }
    if self.cluster_session.is_some() && !self.can_serve_slot_no_response(RespCommand::Get) {
      self.read_head = old_end_read_head;
      self.end_read_head = old_end_read_head;
      return None;
    }
    Some(self.parse_state.get_arg_slice_by_ref(0))
  }

  /// 经注入的存储执行域分派（C# 对应 Process 链末端 ProcessAdminCommands
  /// 的兜底形态；先克隆 Arc 再调用，解 &self.garnet_api 与 &mut session
  /// 的借用相交，单次原子递增对比命令执行开销不可见）
  fn dispatch_via_garnet_api(&mut self, cmd: RespCommand, args: &[&[u8]]) {
    let Some(api) = self.garnet_api.clone() else {
      // 存储执行域未挂载 = 宿主装配缺口：写明错误并告警，绝不静默吞命令
      log::error!("存储执行域未挂载，命令 {cmd:?} 被拒绝");
      self.abort_error_message(ERR_STORE_DOMAIN_NOT_ATTACHED);
      return;
    };
    api.exec(self, cmd, args);
  }

  /// 挂接监视器（对齐 C# `new GarnetLatencyMetricsSession(storeWrapper.monitor)`）
  pub fn attach_monitor(&mut self, monitor: &Arc<wmetric::GarnetServerMonitor>) {
    let latency = Arc::new(GarnetLatencyMetricsSession::new(
      Arc::clone(&monitor.monitor_iterations),
      GarnetLatencyMetricsSession::DEFAULT_LATENCY_TYPES,
    ));
    // 晚装配换表须同步重挂已挂载的执行域（pending 计时与 NET_RS_LAT 必须
    // 落同一对象，见
    // [`crate::resp::garnet_api::GarnetApiFace::attach_latency_metrics`]）
    if let Some(api) = &self.garnet_api {
      api.attach_latency_metrics(Arc::clone(&latency));
    }
    self.latency_metrics = Some(latency);
    self.global_latency_metrics = monitor.global_latency_metrics();
  }

  /// 装配期注入会话指标共享句柄（本会话 `session_metrics` 字段的唯一写入点，
  /// 与 `attach_monitor` 同为晚装配注入口；对标 C# RespServerSession.cs:264
  /// 构造期单点建 sessionMetrics 的判定在本仓搬到装配侧：句柄由 `service.rs`
  /// 按采样频率门控唯一创建，经
  /// [`RespSessionConsumer::attach_session_metrics`](crate::resp::resp_session_consumer::RespSessionConsumer::attach_session_metrics)
  /// 同名转发到本口，使会话与存储执行域共持同一 Arc；None = 采样关闭，
  /// 与 C# null 会话指标同形）
  pub fn attach_session_metrics(&mut self, metrics: Option<Arc<SessionMetricsHandle>>) {
    self.session_metrics = metrics;
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
  /// `aclAuthenticator.GetUserHandle()`）；免认证形态 CanAuthenticate = false
  /// → 恒返回 false（C# 取 accessControlList.GetDefaultUserHandle 兜底，
  /// rust 无 ACL 实例可取，门控对该形态恒放行承接同一网络效果）
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
      let target_ns = acl.get_namespace();
      self.namespace = target_ns;
      if let Some(api) = &self.garnet_api
        && !api.set_context(target_ns, self.active_db_id)
      {
        // 冷租户/冷库：映射未装载，登记挂起面由应答组装点异步点查装载
        self.cold_ctx = Some((target_ns, self.active_db_id));
      }
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

  /// 协议违规哨兵消费（try_consume_messages 对应的 C# catch 块）：
  /// 会话指标计数（C# catch 首行
  /// `sessionMetrics?.incr_total_number_resp_server_session_exceptions(1)`）
  /// 后写 `ERR Protocol Error: {msg}` 追加到累积输出尾部（C#
  /// `RespWriteUtils.TryWriteError($"ERR Protocol Error: {ex.Message}")`，
  /// 同批此前命令应答在前、错误在后），放行应答面交由调用方 Send 后断连。
  /// 返回是否发生违规
  fn write_protocol_error(&mut self) -> bool {
    if let Some(msg) = self.parse_violation.take() {
      if let Some(metrics) = &self.session_metrics {
        metrics.incr_total_number_resp_server_session_exceptions(1);
      }
      self.abort_error_message(&format!("ERR Protocol Error: {msg}"));
      return true;
    }
    false
  }

  /// libs/server/Resp/RespServerSession.cs:TryConsumeMessages
  ///
  /// 唯一消费形态（C# IMessageConsumer 单方法）：接收缓冲驻留会话、由泵经
  /// take/return 直填（网络字节零拷贝直入），解析并分派接收缓冲中自
  /// [`Self::read_head`] 游标起的全部完整命令。游标跨批次持久不回退——
  /// C# `bytesRead = bytesReceived` + `readHead` 持久游标模型；事务排队
  /// 字节（MULTI..EXEC 跨批次）因此驻留接收缓冲，EXEC 据此回退重解析
  /// 排队命令（C# IsSkippingOperations 禁平移同源语义）。
  ///
  /// 分派经 [`GarnetApi`] 注入面；协议违规以 None 表达（C# 抛
  /// RespParsingException，catch 块先写 `ERR Protocol Error: {msg}` 到
  /// 累积输出再断连，rust 同序：错误落在 [`Self::output`] 中此前命令
  /// 应答之后，由调用方发出后断连）。
  ///
  /// 返回消费后残余字节数（半包长度；`0` = 整段消费完毕，缓冲清零复位，
  /// 容量保留复用）；`None` = 协议违规 / 致命断流（应答面已落
  /// [`Self::output`]，泵发尽后断连）。
  ///
  /// 批首尾纪元快照取放（C# :490 `clusterSession?.AcquireCurrentEpoch()` /
  /// :576 finally `clusterSession?.ReleaseCurrentEpoch()`）：批内会话持当前
  /// 纪元快照，批外清零——配置过渡静止等待的观测窗口
  pub fn try_consume_messages(&mut self) -> Option<usize> {
    if let Some(cs) = self.cluster_session.as_ref() {
      cs.acquire_current_epoch();
    }
    let remaining = self.try_consume_messages_body();
    if let Some(cs) = self.cluster_session.as_ref() {
      cs.release_current_epoch();
    }
    remaining
  }

  /// 批消费体（[`Self::try_consume_messages`] 的纪元快照保护段）
  fn try_consume_messages_body(&mut self) -> Option<usize> {
    self.bytes_read = self.recv_buffer.len();
    let prev_read_head = self.read_head;

    // 批首复位水位让渡哨兵（上一批由泵取走；直接重入防御性复位）
    self.output_watermark_yield = false;
    self.latency_batch_start();
    self.enter_and_get_response_object();
    let op_count = self.process_messages();
    // 协议违规（C# RespParsingException 传播出 ProcessMessages → catch 块：
    // 写 `ERR Protocol Error: {msg}` 追加在累积应答之后 → Send → 断连）。
    // 游标不回退，None 表达致命错误，应答面（含此前命令累积应答 + 协议
    // 错误）交由调用方发出后断连
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
    self.latency_batch_stop(newly_consumed, op_count);
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

    if let Some(metrics) = &self.session_metrics {
      metrics.incr_total_net_input_bytes(newly_consumed as u64);
    }
    Some(self.bytes_read.saturating_sub(self.read_head))
  }

  /// 批次消费入口的延迟/慢日志起始装配（C# TryConsumeMessages:481/:486-490）：
  /// 延迟监视开启即启动 NET_RS_LAT 计时；慢日志门开启时起始刻度与延迟
  /// 计时同源（LatencyMetrics.Get(NET_RS_LAT)），无延迟监视直取单调秒表
  /// （C# `Stopwatch.GetTimestamp()`）
  fn latency_batch_start(&mut self) {
    let slow_log_enabled = self
      .runtime_config
      .get_microseconds(ServerConfigType::SlowlogLogSlowerThan)
      > 0;
    let Some(latency) = &self.latency_metrics else {
      if slow_log_enabled {
        self.slow_log_start_ticks = now_stopwatch_ticks();
      }
      return;
    };
    latency.start(LatencyMetricsType::NetRsLat, now_stopwatch_ticks());
    if slow_log_enabled {
      self.slow_log_start_ticks = latency.get(LatencyMetricsType::NetRsLat);
    }
  }

  /// 批次消费出口的延迟停表（C# TryConsumeMessages:586-598）：有成功消费
  /// 字节才记录——慢命令批次切 NET_RS_LAT_ADMIN 桶，随后把字节/命令数
  /// 记入吞吐直方图
  fn latency_batch_stop(&mut self, consumed: usize, op_count: u64) {
    let Some(latency) = &self.latency_metrics else {
      return;
    };
    if consumed == 0 {
      return;
    }
    let now = now_stopwatch_ticks();
    if self.contains_slow_command {
      latency.stop_and_switch(
        LatencyMetricsType::NetRsLat,
        LatencyMetricsType::NetRsLatAdmin,
        now,
      );
      self.contains_slow_command = false;
    } else {
      latency.stop(LatencyMetricsType::NetRsLat, now);
    }
    latency.record_value(LatencyMetricsType::NetRsBytes, consumed as i64);
    latency.record_value(LatencyMetricsType::NetRsOps, op_count as i64);
  }

  /// libs/server/Resp/RespServerSession.cs:ProcessMessages
  ///
  /// 主循环：解析 → ACL 门 + no-script 门（C# :653 CheckACLPermissions(cmd)
  /// && CheckScriptPermissions(cmd)，位图仅在脚本执行窗口挂载——C# 位图挂
  /// 内嵌 processor，脚本内 redis.call 重入同门）→ 订阅模式/事务/槽位门 →
  /// 分派 → 指标；被拒命令 ACL 失败回 NOPERM/NOAUTH、no-script 失败回
  /// NOSCRIPT（C# :688-715，两分支均 IncrementRejected）。命令未完整到达时
  /// 双游标回退到本轮起点（C# `endReadHead = readHead = _origReadHead`）。
  /// 返回本批有效命令数（C# opCount 字段的批内增量，延迟吞吐直方图消费）
  pub fn process_messages(&mut self) -> u64 {
    // 挂起中的阻塞/慢路径命令未完成前不再消费新命令（C# 网络线程
    // BlockingWait 期间本就读不到后续命令）
    if self.pending_block.is_some() || self.pending_slow.is_some() {
      return 0;
    }

    let mut op_count = 0u64;
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
        // 冷上下文挂起面按命令窗口即抛：只允许本命令的应答组装点消费
        self.cold_ctx = None;
        // C# 门链（RespServerSession.cs:651-653）：noScriptPassed 默认 true，
        // ACL 失败短路（&&）不再查 no-script；no-script 失败回 NOSCRIPT
        //（C# :710），不落 NOPERM/NOAUTH
        let acl_permitted = self.check_acl_permissions(cmd);
        let mut script_permitted = true;
        if acl_permitted {
          script_permitted = self.check_script_permissions(cmd);
        }
        if acl_permitted && script_permitted {
          // RESP2 订阅模式仅放行 (P|S)SUBSCRIBE/(P|S)UNSUBSCRIBE/PING/QUIT 与
          // rust 补全的 SUNSUBSCRIBE（无 RESET；允许集单点见
          // is_allowed_in_subscription_mode，C# 对位 RespCommand.cs:733-742）
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

          // libs/server/Resp/RespServerSession.cs:683-689（CommandStats 门控：
          // 执行后 calls 必计；失败随 commandErrorWritten 标志计并复位）
          if let Some(stats) = &self.command_stats {
            let mut stats = stats.lock();
            stats.increment_calls(cmd);
            if self.command_error_written {
              stats.increment_failed(cmd);
              self.command_error_written = false;
            }
          }
        } else if script_permitted {
          // C# :688-706 else 分支：已认证 → NOPERM；未认证 → NOAUTH
          self.write_acl_permission_error(self.acl_user_handle.is_some());
          // libs/server/Resp/RespServerSession.cs:715（ACL/脚本权限拒绝计数）
          if let Some(stats) = &self.command_stats {
            stats.lock().increment_rejected(cmd);
          }
        } else {
          // C# :708-712 else 分支：NOSCRIPT（C# :715 同计拒绝数）
          self.write_error_response(cs::RESP_ERR_NOSCRIPT);
          if let Some(stats) = &self.command_stats {
            stats.lock().increment_rejected(cmd);
          }
        }
      } else {
        self.contains_slow_command = true;
      }

      // 重驱型挂起（命令处理体内登记的迭代门 Pending 等待体，如 RUNTXP
      // Prepare 段遇迁移推进未决）：回退游标至本命令起点停止消费（区别于
      // 产应答型 pending_slow），等待体由网络泵驱动至迁移推进/超时后重评
      // 重驱本命令
      if self.pending_rearm {
        self.read_head = orig_read_head;
        self.end_read_head = orig_read_head;
        self.pending_rearm = false;
        break;
      }

      // 推进游标处理下一条命令（C# _origReadHead = readHead = endReadHead）
      self.read_head = self.end_read_head;
      orig_read_head = self.read_head;

      // Handle metrics and special cases（对标 C# :726-735）
      op_count += 1;
      self.handle_slow_log(cmd);
      if let Some(metrics) = &self.session_metrics {
        metrics.incr_total_commands_processed(1);
        metrics.add_total_write_commands_processed(one_if_write(cmd));
        metrics.add_total_read_commands_processed(one_if_read(cmd));
      }

      if self.session_asking != 0 {
        self.session_asking -= 1;
      }

      // 阻塞命令 / 慢路径命令挂起：停止消费本批后续命令（C# 网络线程
      // 阻塞等价物，后续命令由网络泵驱动等待完成后继续消费）
      if self.pending_block.is_some() || self.pending_slow.is_some() {
        break;
      }

      // 批内输出水位让渡（libs/server/Resp/RespServerSession.cs:SendAndReset
      // 满刷循环的命令边界投影）：累计应答达 OUTPUT_WATERMARK_BYTES 即停住，
      // 置位让渡哨兵交网络泵实写本轮应答后立即重入消费（对标 C# Send 后
      // 重取缓冲续写）；游标不回退，残余完整帧驻留接收缓冲待续消费
      if self.output.len() >= OUTPUT_WATERMARK_BYTES {
        self.output_watermark_yield = true;
        break;
      }
    }
    op_count
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
          let msg = self.parse_state.arg_in(&self.recv_buffer, 0);
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
        self.output.extend_from_slice(cs::RESP_OK);
        true
      }
      RespCommand::Quit => {
        self.to_dispose = true;
        self.output.extend_from_slice(cs::RESP_OK);
        true
      }
      RespCommand::Readonly => {
        self.read_only_session = true;
        // C# NetworkREADONLY：clusterSession?.SetReadOnlySession()
        if let Some(cluster) = &self.cluster_session {
          cluster.set_read_only_session();
        }
        self.output.extend_from_slice(cs::RESP_OK);
        true
      }
      RespCommand::Readwrite => {
        self.read_only_session = false;
        // C# NetworkREADWRITE：clusterSession?.SetReadWriteSession()
        if let Some(cluster) = &self.cluster_session {
          cluster.set_read_write_session();
        }
        self.output.extend_from_slice(cs::RESP_OK);
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
      RespCommand::Unsubscribe | RespCommand::Punsubscribe | RespCommand::Sunsubscribe => {
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
        self.output.extend_from_slice(cs::RESP_OK);
        true
      }
      _ => self.network_skip(cmd),
    }
  }

  /// pub/sub 命令会话侧统一入参（wire 为会话自持接线，参数取解析态）
  fn process_pubsub_command(&mut self, cmd: RespCommand, shard: bool) -> bool {
    let args_buf = self.collect_args();
    let args: Vec<&[u8]> = args_buf.iter().map(Vec::as_slice).collect();
    match cmd {
      RespCommand::Subscribe => self.network_subscribe(shard, &args),
      RespCommand::Ssubscribe => self.network_subscribe(true, &args),
      RespCommand::Psubscribe => self.network_psubscribe(&args),
      RespCommand::Unsubscribe => self.network_unsubscribe(&args),
      RespCommand::Sunsubscribe => self.network_sunsubscribe(&args),
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
    use crate::resp::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_multi(session))
  }

  /// EXEC（会话侧路由，实现委托 wtxn::TransactionManager）
  fn network_exec(&mut self) -> bool {
    use crate::resp::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_exec(session))
  }

  /// DISCARD（会话侧路由，实现委托 wtxn::TransactionManager）
  fn network_discard(&mut self) -> bool {
    use crate::resp::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_discard(session))
  }

  /// WATCH（会话侧路由，实现委托 wtxn::TransactionManager）
  fn network_watch(&mut self) -> bool {
    use crate::resp::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_watch(session))
  }

  /// WATCHMS（会话侧路由，实现委托 wtxn::TransactionManager）
  fn network_watch_ms(&mut self) -> bool {
    use crate::resp::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_watch_ms(session))
  }

  /// WATCHOS（会话侧路由，实现委托 wtxn::TransactionManager）
  fn network_watch_os(&mut self) -> bool {
    use crate::resp::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_watch_os(session))
  }

  /// UNWATCH（会话侧路由，实现委托 wtxn::TransactionManager）
  fn network_unwatch(&mut self) -> bool {
    use crate::resp::txn_resp_commands::TxnRespCommandsExt;
    self.with_txn_manager(|txn, session| txn.network_unwatch(session))
  }

  /// RUNTXP（会话侧路由，实现委托 wtxn::TransactionManager）；过程体经
  /// 编译期静态派发解析实例化执行（C# NetworkRUNTXP + TryTransactionProc）
  fn network_runtxp(&mut self) -> bool {
    use crate::resp::txn_resp_commands::{SessionTxnProcResolver, TxnRespCommandsExt};
    self.with_txn_manager(|txn, session| txn.network_runtxp(session, &mut SessionTxnProcResolver))
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
    txn.cluster_enabled = self.cluster_session.is_some();
    let ok = f(&mut txn, self);
    self.txn_manager = Some(txn);
    ok
  }

  /// 排队命令（会话侧路由，实现委托 wtxn::TransactionManager）；
  /// 命令元数据取自 resp 命令信息域（C# SimpleRespCommandInfo 同源）
  fn network_skip(&mut self, cmd: RespCommand) -> bool {
    use crate::resp::txn_resp_commands::TxnRespCommandsExt;
    let info = self.txn_queued_command_info(cmd);
    self.with_txn_manager(|txn, session| txn.network_skip(session, cmd, info.as_ref()))
  }

  /// 当前排队命令元数据（C# SimpleRespCommandInfo → TxnQueuedCommandInfo 投影；
  /// 键窗口按解析态参数即时解析，对齐 C# LockKeys 的 parseState 取数形态）
  fn txn_queued_command_info(&self, cmd: RespCommand) -> Option<TxnQueuedCommandInfo> {
    let info = try_get_simple_resp_command_info(normalize_for_acls(cmd))?;
    let name = super::resp_commands_info_data::resp_command_to_cs_name(cmd);
    let args_buf = self.collect_args();
    let args: Vec<&[u8]> = args_buf.iter().map(Vec::as_slice).collect();
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
      let msg = self.parse_state.arg_in(&self.recv_buffer, 0);
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
      // C# NetworkASYNC：委托 basic_commands 单一定义（参数校验与降级回包全在彼处）
      let args_buf = self.collect_args();
      let args: Vec<&[u8]> = args_buf.iter().map(Vec::as_slice).collect();
      let mut output = mem::take(&mut self.output);
      let r = self.apply_async_param(&args, &mut output);
      self.output = output;
      return matches!(r, Ok(true));
    }
    if cmd == RespCommand::ClientInfo {
      let args_buf = self.collect_args();
      let args: Vec<&[u8]> = args_buf.iter().map(Vec::as_slice).collect();
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
      let args_buf = self.collect_args();
      let args: Vec<&[u8]> = args_buf.iter().map(Vec::as_slice).collect();
      return self.write_via_local("client getname error", |s, out| {
        s.network_clientgetname(&args, out)
      });
    }
    if cmd == RespCommand::ClientSetname {
      let args_buf = self.collect_args();
      let args: Vec<&[u8]> = args_buf.iter().map(Vec::as_slice).collect();
      return self.write_via_local("client setname error", |s, out| {
        s.network_clientsetname(&args, out)
      });
    }
    if cmd == RespCommand::ClientSetinfo {
      let args_buf = self.collect_args();
      let args: Vec<&[u8]> = args_buf.iter().map(Vec::as_slice).collect();
      return self.write_via_local("client setinfo error", |s, out| {
        s.network_clientsetinfo(&args, out)
      });
    }
    if cmd == RespCommand::ClientUnblock {
      let args_buf = self.collect_args();
      let args: Vec<&[u8]> = args_buf.iter().map(Vec::as_slice).collect();
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
      let args_buf = self.collect_args();
      let args: Vec<&[u8]> = args_buf.iter().map(Vec::as_slice).collect();
      return self.write_via_local("role error", |s, out| s.network_role(&args, out));
    }
    // C# NetworkCOMMAND（COMMAND 根命令）：带参报未知子命令，无参列全部命令
    if cmd == RespCommand::Command {
      return self.write_via_local("command error", |s, out| s.network_command_root(out));
    }
    // C# NetworkAUTH（ProcessOtherCommands 段）与 ACL 族不在此分派：二者须
    // 点查底层存储（ACL 为唯一真源），由存储执行域 `StoreGarnetApi::exec`
    // 在批处理纪元保护区外统一承接（冷记录落盘回读 + 会话本地句柄回写）

    // LATENCY / SLOWLOG 族（C# Metrics/Latency、Metrics/Slowlog 的会话 partial）
    if let Some(handled) = self.process_metrics_commands(cmd) {
      return handled;
    }
    // MONITOR / DEBUG / SAVE 族（C# ProcessAdminCommands）
    if let Some(handled) = self.process_admin_session_commands(cmd) {
      return handled;
    }
    if cmd == RespCommand::Info {
      // libs/server/Metrics/Info/InfoCommand.cs:NetworkINFO（wmetric 段分发：
      // 段解析 + 各信息域填充经 SessionInfoSource 数据源承接）。
      // 纯慢段请求（KEYSPACE 全库扫描计数 / HLOGSCAN 混合日志分布扫描 /
      // STOREHASHTABLE 哈希分布诊断扫描 / STOREREVIV 复活统计转储，C#
      // PopulateKeyspaceInfo → GetKeyspaceStats 专用扫描会话、
      // PopulateHlogScanInfo → HybridLogDistributionScan、
      // PopulateStoreHashDistribution → DumpDistribution、
      // PopulateStoreRevivInfo → DumpRevivificationStats；DEFAULT/ALL 段
      // 集合均不含这些段，普通 INFO 不受影响）——存储域扫描须跨 await，
      // 与 DBSIZE 同构降级慢路径（garnet_api dispatch_slow Info 臂挂
      // SlowWait 闭环）。混合段名（如 INFO server keyspace）不降级，
      // 慢段按 wmetric 缺省形态呈现，避免丢段
      let args_buf = self.collect_args();
      let args: Vec<&[u8]> = args_buf.iter().map(Vec::as_slice).collect();
      let slow_scan_only = !args.is_empty()
        && args.iter().all(|a| {
          matches!(
            InfoMetricsType::from_name(a),
            Some(
              InfoMetricsType::Keyspace
                | InfoMetricsType::HlogScan
                | InfoMetricsType::StoreHashtable
                | InfoMetricsType::StoreReviv
            )
          )
        });
      if slow_scan_only {
        // 放行到函数尾兜底分派（C# ProcessOtherCommands 末端
        // ProcessAdminCommands 形态），由存储执行域承接
      } else {
        let text = {
          let provider = super::info_provider::SessionInfoSource::new(self);
          let mut info = GarnetInfoMetrics::new();
          let mut out = Vec::new();
          InfoCommand::network_info(
            &args,
            self.active_db_id as i32,
            &provider,
            &mut info,
            // C# InfoCommand.cs:63 直写 storeWrapper.monitor.resetEventFlags
            // [STATS]=true，经进程级监视器置位、采样轮消费复位（未安装为
            // no-op，对齐 C# monitor != null 判定）
            &mut |section| {
              if let Some(monitor) = GarnetServerMonitor::global() {
                monitor.set_info_reset_flag(section);
              }
            },
            &mut out,
          );
          out
        };
        self.output.extend_from_slice(&text);
        return true;
      }
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
    let args_buf = self.collect_args();
    let args: Vec<&[u8]> = args_buf.iter().map(Vec::as_slice).collect();
    self.dispatch_via_garnet_api(cmd, &args);
    true
  }

  /// INFO 纯慢段请求（KEYSPACE/HLOGSCAN/STOREHASHTABLE/STOREREVIV）的降级
  /// 判定（rust compio 异步存储域特有降级点，无 C# 对标函数——C#
  /// GetKeyspaceStats / HybridLogDistributionScan / DumpDistribution /
  /// DumpRevivificationStats 网络线程同步执行；rust 存储域扫描须跨
  /// await，与 [`Self::network_dbsize`] 同构降级 Ok(false) 挂 SlowWait
  /// 异步闭环）
  ///
  /// 唯一到达路径：[`Self::process_other_commands`] 放行的纯慢段请求
  /// （DEFAULT/ALL 段集合不含这四段，其余 INFO 请求在会话侧同步闭环）
  pub(crate) fn try_info_keyspace_slow_path(
    &mut self,
    _parse_state: &[&[u8]],
    _output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    Ok(false)
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

  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND
  /// COMMAND 根命令入口
  pub fn network_command_root(&mut self, output: &mut Vec<u8>) -> wresp::Result<bool> {
    if self.parse_state.count > 0 {
      let sub = self.parse_state.arg_in(&self.recv_buffer, 0).as_str_safe();
      cs::abort_with_unknown_subcommand(output, sub, "COMMAND");
    } else {
      self.write_command_response(output)?;
    }
    Ok(true)
  }

  /// 认证成功落位：刷新认证器句柄 + 会话本地句柄 / 命名空间 / 存储域上下文
  ///
  /// 对标 C# 认证器与 RespServerSession 共享同一 `UserHandle` 的语义；rust 侧
  /// 句柄连接本地持有（无全局用户字典），故认证器与会话两处同步刷新。
  fn apply_authenticated_handle(&mut self, user_handle: Arc<wacl::UserHandle>, target_ns: u64) {
    if let Some(acl) = &self.acl_authenticator {
      let mut acl = acl.lock();
      acl.user_handle = Some(Arc::clone(&user_handle));
      acl.namespace = target_ns;
    }
    self.set_user_handle(user_handle);
    self.namespace = target_ns;
    if let Some(api) = &self.garnet_api
      && !api.set_context(target_ns, self.active_db_id)
    {
      // 冷租户/冷库：映射未装载，登记挂起面由应答组装点异步点查装载
      self.cold_ctx = Some((target_ns, self.active_db_id));
    }
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkAUTH
  ///
  /// AUTH [<username>] <password>：免认证形态回认证器拒绝文案；ACL 档命名用户
  /// 经 [`RespServerSession::authenticate_user_via_store`] 点查底层存储（存储
  /// 为唯一真源），成功后连接本地挂 `Arc<UserHandle>`（句柄随连接析构释放，
  /// 无全局用户字典）；`default` / requirepass 回落内存认证器。按用户名有无
  /// 回 WRONGPASS 变体
  pub fn network_auth_session<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &AclStore<'_, D>,
  ) -> wresp::Result<bool> {
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
    // 命名用户：存储点查认证（句柄连接本地持有）
    if !username.is_empty() {
      match self.authenticate_user_via_store(store, username, password) {
        AclAuthOutcome::Success(user_handle, target_ns) => {
          self.apply_authenticated_handle(user_handle, target_ns);
          if let Some((ns, db)) = self.take_cold_ctx() {
            self.park_cold_context_load(&store.storage().store, ns, db, cs::RESP_OK.to_vec());
            return Ok(true);
          }
          cs::write_raw(self.get_string_output(), cs::RESP_OK);
          return Ok(true);
        }
        AclAuthOutcome::StorageError => {
          cs::write_error_raw(self.get_string_output(), cs::RESP_ERR_SLOW_PATH_STORAGE);
          return Ok(true);
        }
        // 存储无记录/口令不匹配：回落内存认证器（default / requirepass 形态）
        AclAuthOutcome::Denied => {}
      }
    }
    if self.authenticate_user(username, password) {
      if let Some((ns, db)) = self.take_cold_ctx() {
        self.park_cold_context_load(&store.storage().store, ns, db, cs::RESP_OK.to_vec());
        return Ok(true);
      }
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
    let Some((_, custom)) = self.current_custom_command.take() else {
      return Ok(true);
    };
    if !is_command_arity_valid_checked(custom.arity, self.parse_state.count) {
      cs::abort_with_wrong_number_of_arguments(output, custom.name);
      self.command_error_written = true;
      return Ok(true);
    }
    let Some((&key, args)) = parse_state.split_first() else {
      cs::abort_with_wrong_number_of_arguments(output, custom.name);
      self.command_error_written = true;
      return Ok(true);
    };

    // 自定义对象执行体的 nil 帧随会话协议（版本在调用点裁决，执行体不自存状态）
    let resp_version = self.resp_protocol_version;
    if custom.name.eq_ignore_ascii_case("JSON.MGET") {
      let (keys, path_slice) = parse_state.split_at(parse_state.len() - 1);
      let path = path_slice[0];
      output.write_resp_array_len(keys.len());
      for &k in keys {
        let res = store.try_read_tag_sync(k, KeyTag::ObjectEnvelope, |raw| {
          if let Some(payload) = obj_decode_custom(raw, custom.object_tag) {
            let mut sub_out = Vec::new();
            (custom.fns.reader)(payload, &[path], &mut sub_out, resp_version);
            Some(sub_out)
          } else {
            None
          }
        });
        match res {
          Ok(StoreResult::Success(Some(sub_out))) => {
            output.extend_from_slice(&sub_out);
          }
          _ => {
            output.write_resp_null_ver(self.resp_protocol_version);
          }
        }
      }
      return Ok(true);
    }

    // 信封类型标签 + 执行面均为编译期静态取用（解析期已入槽，零锁零克隆）
    #[cfg(any(feature = "roaring", feature = "json"))]
    let done = {
      use crate::resp::objects::custom_object_commands::{
        CustomObjOutcome, CustomObjectCall, try_custom_object_command,
      };
      match try_custom_object_command(
        store,
        CustomObjectCall {
          cmd_type: custom.command_type,
          tag: custom.object_tag,
          fns: &custom.fns,
          key,
          args,
          resp_version,
        },
        output,
      ) {
        CustomObjOutcome::Done => true,
        // 降级异步重放：命令槽回填供 exec 快照命令名，本次不残留输出
        CustomObjOutcome::Degrade => {
          self.current_custom_command = Some((RespCommand::Customobjcmd, custom));
          false
        }
      }
    };
    // 未配置扩展特性：解析面无静态清单可命中，本分支不可达兜底明确报错，
    // 绝不静默
    #[cfg(not(any(feature = "roaring", feature = "json")))]
    let done = {
      let _ = (custom, key, args, store);
      log::error!("自定义对象命令执行域未配置，被拒绝");
      cs::write_error_raw(output, cs::RESP_ERR_GENERIC_UNK_CMD);
      self.command_error_written = true;
      true
    };

    if done { Ok(true) } else { Ok(false) }
  }

  /// 自定义命令共同路径（C# ProcessOtherCommands 的 NetworkCustomTxn /
  /// NetworkCustomProcedure / NetworkCustomRawStringCmd 三 local function
  /// 的共同骨架；分派臂 [`Self::process_other_commands`] 直接走本函数）：
  /// IsCommandArityValid → 清槽
  pub fn run_custom_command(&mut self) -> bool {
    let Some((_kind, custom)) = self.current_custom_command.take() else {
      return true;
    };
    let count = self.parse_state.count;
    if !is_command_arity_valid_checked(custom.arity, count) {
      self.abort_wrong_num_args(custom.name);
      return true;
    }
    // 自定义命令执行域（C# TryTransactionProc / TryCustomProcedure 族，由
    // custom 域注册表承载）尚未接线：按 ProcessAdminCommands 兜底明确报错，
    // 绝不静默
    log::error!("自定义命令执行域未接线，命令 {} 被拒绝", custom.name);
    self.abort_error_message(cs::RESP_ERR_GENERIC_UNK_CMD);
    true
  }

  /// libs/server/Resp/RespServerSession.cs:Process（admin 族回退）
  pub fn process(&mut self, cmd: RespCommand) -> bool {
    let args_buf = self.collect_args();
    let args: Vec<&[u8]> = args_buf.iter().map(Vec::as_slice).collect();
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

  /// libs/server/Resp/RespServerSession.cs:GetCommand 的区间解析内核（零拷贝
  /// 返回 recv_buffer 内 (start, len) 视图，对标 C# ReadOnlySpan 返回；本函数
  /// 承担 GetCommand 的 TryReadUnsignedLengthHeader → TryReadSignedLengthHeader
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
          set_integer_overflow(&mut self.parse_violation, &buffer[start_digit..ptr]);
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
      set_integer_overflow(&mut self.parse_violation, &buffer[start_digit..ptr]);
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
  #[inline]
  pub fn get_upper_case_command_range(&mut self) -> Option<(usize, usize)> {
    let (start, len) = self.get_command_range()?;
    self.recv_buffer[start..start + len].make_ascii_uppercase();
    Some((start, len))
  }

  /// 待发送字节（C# dcurr - GetResponseObjectHead）
  #[inline]
  pub fn pending_output_len(&self) -> usize {
    self.output.len()
  }

  /// 出向字节记账单点（C# 唯一出向记账点 Send 内的会话指标出向累计，
  /// `sessionMetrics?.` 空即跳过；C# 侧既无第二枚冲出口，也无会话级
  /// 出向累计字段）
  #[inline]
  fn account_output(&self, bytes: u64) {
    if let Some(metrics) = &self.session_metrics {
      metrics.incr_total_net_output_bytes(bytes);
    }
  }

  /// libs/server/Resp/RespServerSession.cs:SendAndReset
  /// libs/server/Resp/RespServerSession.cs:Send
  ///
  /// 冲取出面单点：把会话累积应答并入目标写缓冲并复位
  ///
  /// C# 的 SendAndReset（判游标前进则 Send + 重取响应对象）与 Send（唯一出向
  /// 记账点）两枚锚点在托管缓冲下的合并实现：`out` 空时整段换出（零拷贝），
  /// 非空时追加后清空，两路产出字节序列一致；冲出量为零即无应答可出网，
  /// C# 「写超响应缓冲仍无进展即抛」探针在可扩容 Vec 下无从成立，故无残留
  pub fn take_output_into(&mut self, out: &mut Vec<u8>) {
    if self.output.is_empty() {
      return;
    }
    let len = self.output.len();
    if out.is_empty() {
      mem::swap(&mut self.output, out);
      if self.output.capacity() < 4096 {
        self.output.reserve(DEFAULT_OUTPUT_BUFFER_CAPACITY);
      }
    } else {
      out.extend_from_slice(&self.output);
      self.output.clear();
    }
    self.account_output(len as u64);
  }

  /// 取走会话待释放哨兵（C# ProcessMessages 尾部 `if (toDispose)
  /// DisposeNetworkSender(true)` 的信号通道：QUIT 置位，网络泵发尽本轮
  /// 累积应答后据此断连；取走即复位）
  pub fn take_dispose_request(&mut self) -> bool {
    mem::take(&mut self.to_dispose)
  }

  /// 取走批内输出水位让渡哨兵（网络泵专属：置位表示本批因累计应答达
  /// OUTPUT_WATERMARK_BYTES 在命令边界停住，接收缓冲尚有完整帧待续消费；
  /// 泵实写本轮应答后立即重入消费，不等下一批网络字节。取走即复位）
  pub fn take_output_watermark_yield(&mut self) -> bool {
    mem::take(&mut self.output_watermark_yield)
  }

  /// libs/server/Resp/RespServerSession.cs:WriteDirectLarge
  ///
  /// 大块直写输出缓冲（rust 托管缓冲天然可扩容，等价一次追加）
  pub fn write_direct_large(&mut self, src: &[u8]) {
    self.output.extend_from_slice(src);
  }

  /// libs/server/Resp/RespServerSession.cs:TrySwitchActiveDatabaseSession
  #[inline]
  pub fn try_switch_active_database_session(&mut self, db_id: u64) -> bool {
    if db_id >= self.max_databases {
      return false;
    }
    self.active_db_id = db_id;
    if let Some(api) = &self.garnet_api
      && !api.set_context(self.namespace, db_id)
    {
      // 冷库：映射未装载，登记挂起面由 SELECT 应答组装点异步点查装载
      self.cold_ctx = Some((self.namespace, db_id));
    }
    true
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
  ///
  /// 帧文本由 `wresp::cmd_strings::abort_with_wrong_number_of_arguments` 单点
  /// 生成，避免手拼 RESP 错误帧与文案散点；本会话承担 `command_error_written`
  /// 副作用（调用方语义保留）。
  pub fn abort_wrong_num_args(&mut self, cmd_name: &str) {
    cs::abort_with_wrong_number_of_arguments(&mut self.output, cmd_name);
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

  /// 汇集解析态参数所有权副本（Lua 脚本上下文等需独立所有权场景用）
  pub(crate) fn collect_args(&self) -> Vec<Vec<u8>> {
    (0..self.parse_state.count)
      .map(|i| self.parse_state.arg_in(&self.recv_buffer, i).to_vec())
      .collect()
  }

  /// 汇集解析态参数借用切片（零堆分配参数分派场景用）
  pub(crate) fn collect_args_borrowed(&self) -> Vec<&[u8]> {
    (0..self.parse_state.count)
      .map(|i| self.parse_state.arg_in(&self.recv_buffer, i))
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
  /// CLIENT INFO 自身行：字段与 CLIENT LIST 同源单机制——视图经
  /// [`current_client_view`](Self::current_client_view) 组装（含条件段
  /// name/user 与集群 M/S flags 臂），行序写出收敛于
  /// [`write_client_info_fields`]（C# LIST/INFO 共函数的 rust 对位）。
  /// 尾部追加 rust 域扩展字段 pubsub-dropped（C# 直写模型无丢帧语义故无此
  /// 字段；rust 有界邮箱背压的会话侧可观测投影，--pubsub 关闭恒 0，见
  /// wpubsub subscriber.rs 背压差异登记）。
  pub fn write_client_info_state(&self, into: &mut String) {
    let age_sec = (session_now_ms() - self.creation_ticks).max(0) / 1000;
    let view = self.current_client_view();
    write_client_info_fields(
      into,
      self.id,
      &self.remote_endpoint,
      &self.local_endpoint,
      age_sec,
      &view,
    );
    use std::fmt::Write as _;
    let _ = write!(
      into,
      " pubsub-dropped={}",
      self.pubsub.dropped_count().unwrap_or(0)
    );
  }

  /// 处理 HELLO 命令的会话状态转换：
  ///
  /// 校验 → 认证 → 升级协议 / 落客户端名 → 按会话真实状态组 HELLO 应答 map
  ///（RESP2 退化为双倍数组）。返回 false 表示认证失败（WRONGPASS）。
  ///
  /// 不移植 C# BasicCommands.cs:1784-1789 的 pending 异步守卫臂
  /// （协议版本变化且 asyncCompleted < asyncStarted 时回
  /// 「存在进行中异步操作不允许协议变更」错误）之因：rust 命令在会话
  /// 循环内 await 到完成，无 C# AsyncProcessor 的 asyncStarted /
  /// asyncCompleted 在途计数面（GET_WithPending 族），守卫不可达。若未来
  /// 引入异步自定义命令面，须连同计数面与本守卫臂一并落地。
  pub fn process_hello_command_state<D: Device>(
    &mut self,
    resp_protocol_version: Option<u8>,
    username: &[u8],
    password: &[u8],
    client_name: Option<&str>,
    store: Option<&AclStore<'_, D>>,
    output: &mut Vec<u8>,
  ) -> bool {
    if !username.is_empty() {
      // 命名用户：存储点查（句柄连接本地持有）；default / requirepass 回落内存认证器
      let mut authenticated = false;
      if let Some(store) = store {
        match self.authenticate_user_via_store(store, username, password) {
          AclAuthOutcome::Success(user_handle, target_ns) => {
            self.apply_authenticated_handle(user_handle, target_ns);
            authenticated = true;
          }
          AclAuthOutcome::StorageError => {
            cs::write_error_raw(output, cs::RESP_ERR_SLOW_PATH_STORAGE);
            return false;
          }
          AclAuthOutcome::Denied => {}
        }
      }
      if !authenticated && !self.authenticate_user(username, password) {
        cs::write_error_raw(output, cs::RESP_WRONGPASS_INVALID_USERNAME_PASSWORD);
        return false;
      }
    }

    if let Some(version) = resp_protocol_version {
      self.update_resp_protocol_version(version);
    }
    if let Some(name) = client_name {
      self.set_client_name(Some(name));
    }

    // 应答 map 按升级后的协议版本写头（C# BasicCommands.cs:1829 WriteMapLength：
    // RESP3 %8、RESP2 双倍数组）；字段序对齐 C#：server/version/
    // garnet_version/proto/id/mode/role + modules 空数组；proto/id 直读会话状态；
    // mode/role 集群形态（C# EnableCluster && IsReplica 分支）
    let (mode, role) = match &self.cluster_session {
      None => ("standalone", "master"),
      Some(_) => (
        "cluster",
        if self
          .cluster_provider
          .as_ref()
          .is_some_and(|p| p.is_replica())
        {
          "replica"
        } else {
          "master"
        },
      ),
    };
    write_map_len(output, 8, self.resp_protocol_version);
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
  ///
  /// 脚本期 no-script 位图在本窗口挂载、结束摘除：C# 位图挂在内嵌 processor
  /// （SessionScriptCache 构造的独立 RespServerSession，仅承接脚本内
  /// redis.call）上，LuaRunner.cs:242 构造期挂载且常驻，外层连接会话位图
  /// 恒 null——主循环命令不受 no-script 门限；rust 无内嵌 processor，脚本
  /// 内 redis.call 经 [`RespScriptingApi`] 重入共享会话，以窗口式挂/摘承接
  /// 同一可观测语义。
  fn run_lua_command(&mut self, cmd: RespCommand) -> bool {
    let Some(mut session_cache) = self.session_script_cache.take() else {
      // 单一 Lua 启用门（对标 libs/server/Lua/LuaCommands.cs:CheckLuaEnabled）：
      // session_script_cache 仅在 enable_lua 时创建，None 即未启用，回 RESP_ERR_LUA_DISABLED。
      self.abort_error_message(cs::RESP_ERR_LUA_DISABLED);
      return true;
    };
    self.attach_no_script_bitmap();
    let store_cache = Arc::clone(&self.store_script_cache);
    let args = self.collect_args();
    // 配置链取值先行克隆（api 的 &mut 借用窗口内不可再借 &self）。
    // LuaOptions 标量为主、allowed_functions 常空，克隆零/单次分配。
    let lua_options = self.lua_options.clone();
    let lua_txn_mode = self.lua_txn_mode;
    // 会话所属实例锁表句柄（C# LuaRunner.cs:266 的
    // `respServerSession.storageSession.unifiedTransactionalContext` 同源：
    // 脚本键锁面与会话事务锁面同表）
    let lock_table = self.lock_table.clone();
    // 脚本窗口隔离（C# 内嵌 processor 自带独立接收缓冲与应答暂存发送器的
    // 等价物）：redis.call 的合成 RESP 请求覆写会话接收窗，故外层批的接收
    // 游标与已产出应答先换出、窗口关闭原样挂回——否则同批 EVAL 之后尚未
    // 消费的命令随覆写凭空消失，前序命令的应答更会被 Lua 应答转换器误读出
    // 成本条 redis.call 的应答
    let outer_output = mem::take(&mut self.output);
    let outer_recv = mem::take(&mut self.recv_buffer);
    let outer_cursors = (self.read_head, self.end_read_head, self.bytes_read);
    let mut script_out = Vec::new();
    {
      let mut api = RespScriptingApi(&mut *self);
      let mut ctx = LuaSessionContext {
        args: &args,
        out: &mut script_out,
        session_cache: &mut session_cache,
        store_cache: &store_cache,
        session: &mut api,
        // 配置链装配（C# SessionScriptCache 装配面：LuaTransactionMode 与
        // LuaOptions 逐字段下传），不再写死 default。
        txn_mode: lua_txn_mode,
        redis_version: REDIS_PROTOCOL_VERSION,
        lua_options: &lua_options,
        lock_table,
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
    // 收尾不变式：脚本窗口退出时挂起态必为 None——每次重入的
    // [`RespScriptingApi::dispatch_resp`] 已就地承接两态并把应答并入脚本应答。
    // 残留挂起体若不摘除，网络泵会把它当成本连接的挂起命令 resolve，产出一帧
    // 不属于任何客户端命令的应答插进出网流（协议流错插的最后一道闸）；此处
    // 写明并就地取消，与 [`Self::dispose`] 同一取消口径
    if let Some(blocked) = self.take_blocked_wait() {
      log::error!("lua script window leaked a blocked wait, cancelled");
      blocked.abort();
    }
    if self.take_slow_wait().is_some() {
      log::error!("lua script window leaked a slow wait, cancelled");
    }
    // 会话窗口挂回：外层游标与接收缓冲复位，水位让渡哨兵复位为派发前的
    // false（EVAL 能被派发即说明前序命令未触水位）
    self.recv_buffer = outer_recv;
    (self.read_head, self.end_read_head, self.bytes_read) = outer_cursors;
    self.output_watermark_yield = false;
    self.output = outer_output;
    // 脚本窗口关闭：外层连接命令恢复 no-script 门豁免（对齐 C# 外层会话
    // 位图恒 null）
    self.no_script_bitmap = None;
    self.session_script_cache = Some(session_cache);
    self.output.extend_from_slice(&script_out);
    true
  }

  /// ACL 门位图段（C# AdminCommands.cs:CheckACLPermissions 主体；&self 纯判定，
  /// 主循环与 Lua redis.call 路径同用——LuaRunner.Functions.cs:2993）
  ///
  /// 无 ACL 认证器（免认证形态）：C# default 用户 +@all → 恒放行。
  /// ACL 档：(!IsAuthenticated || !CanAccessCommand) && !IsNoAuth → 拒绝
  ///（per-user CanAccessCommand 位图；IsNoAuth 豁免见
  /// libs/server/Resp/Parser/RespCommand.cs:701-707）。
  pub fn acl_permits(&self, cmd: RespCommand) -> bool {
    if self.acl_authenticator.is_none() {
      return true;
    }
    let permitted = self
      .acl_user_handle
      .as_ref()
      .is_some_and(|handle| handle.load().can_access_command(cmd));
    permitted || is_no_auth(cmd)
  }

  /// 脚本期 no-script 位图静态源（C# LuaRunner.cs:148 NoScriptDetails，
  /// static readonly 单次构建；进程级缓存，挂载面零 Arc 借用）
  pub(crate) fn no_script_bitmap_source() -> (i32, &'static [u64]) {
    static SOURCE: OnceLock<(i32, Box<[u64]>)> = OnceLock::new();
    let (start, bitmap) = SOURCE.get_or_init(|| {
      let (start, bitmap) = Self::no_script_details();
      (start, bitmap.into_boxed_slice())
    });
    (*start, bitmap)
  }

  /// 挂载脚本期 no-script 位图（C# LuaRunner.cs:242：LuaRunner 构造期
  /// `(noScriptStart, noScriptBitmap) = NoScriptDetails` 的字段赋值动作；
  /// 挂/摘时机由 [`Self::run_lua_command` 的脚本窗口承接，见该处语义说明]）
  pub fn attach_no_script_bitmap(&mut self) {
    if self.no_script_bitmap.is_none() {
      let (start, bitmap) = Self::no_script_bitmap_source();
      self.no_script_start = start;
      self.no_script_bitmap = Some(bitmap);
    }
  }

  /// 子命令判别值 → 顶层命令判别值归一表（C# 门语义对齐：ProcessMessages
  /// 解析产出顶层命令（SCRIPT/ACL/CLUSTER 等的子命令在分派 handler 内二次
  /// 解析），CheckScriptPermissions 查顶层判别值——位图虽含子命令位（构建
  /// 端 InitializeNoScriptDetails 一并收集），顶层查询命中不了子命令位；
  /// rust 解析器直接产出子命令判别值，查位图前须归一，否则 SCRIPT|EXISTS
  /// 等被位图中的子命令位误拦）
  fn no_script_gate_cmd(cmd: RespCommand) -> u16 {
    static TABLE: OnceLock<GxHashMap<u16, u16>> = OnceLock::new();
    let table = TABLE.get_or_init(|| {
      let Some(all_commands) = try_get_resp_commands_info(true) else {
        return GxHashMap::default();
      };
      all_commands
        .values()
        .flat_map(|info| {
          info
            .sub_commands
            .iter()
            .map(|sub| (sub.command as u16, info.command as u16))
        })
        .collect()
    });
    table.get(&(cmd as u16)).copied().unwrap_or(cmd as u16)
  }

  /// libs/server/Resp/RespServerSession.cs:CheckScriptPermissions（实现体
  /// AdminCommands.cs:95-115）
  ///
  /// 位图未挂载（本连接未进入过脚本期）恒放行，等价 C# noScriptBitmap ==
  /// null 路径；挂载后按 C# 字节粒度位检查（除数 8 字节而非 64 位，
  /// [`Self::no_script_details`] 构建端同款怪癖，两端一致故位序吻合）
  pub fn check_script_permissions(&self, cmd: RespCommand) -> bool {
    let Some(bitmap) = self.no_script_bitmap else {
      return true;
    };
    let ix = i32::from(Self::no_script_gate_cmd(cmd)) - self.no_script_start;
    if ix >= 0 {
      let word_ix = ix as usize / size_of::<u64>(); // C# sizeof(ulong) = 8
      if let Some(&word) = bitmap.get(word_ix)
        && word & (1_u64 << (ix as usize % size_of::<u64>())) != 0
      {
        // C# :108 OnACLOrNoScriptFailure：custom 命令环境态清理，rust
        // current_custom_command 在分派段之后才置位，门期无环境态可清
        return false;
      }
    }
    true
  }

  /// 构建 NoScript 命令集位图（对齐 LuaRunner InitializeNoScriptDetails 集合）
  ///
  /// 在 garnet 中的相对路径:libs/server/Lua/LuaRunner.cs:InitializeNoScriptDetails
  ///
  /// 从 [`wresp::catalog::try_get_resp_commands_info`]（externalOnly
  /// 口径）的 NoScript 标志动态构建：顶层命令与子命令的判别值升序铺开，
  /// 位图按字节粒度置位（C# `ulongIndex = stepped / sizeof(ulong)`、
  /// `bitIndex = stepped % sizeof(ulong)`，除数为 8 字节而非 64 位）。
  /// Garnet 命令数据无 FCALL/FCALL_RO/EVAL_RO/EVALSHA_RO/FUNCTION（Redis 侧
  /// 命令，Garnet RespCommandsInfo 未定义），故无对应位。
  pub fn no_script_details() -> (i32, Vec<u64>) {
    const BITS_PER_WORD: usize = size_of::<u64>(); // C# sizeof(ulong) = 8

    let Some(all_commands) = try_get_resp_commands_info(true) else {
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
/// C# ProcessCommandFromScripting 把参数格式化为 RESP 请求后重入内嵌
/// processor 的 TryConsumeMessages；rust 无内嵌 processor，经同一解析/分派
/// 路径重入共享会话，响应字节追加写入调用方传入的 `&mut Vec<u8>`。外层批的
/// 接收窗与输出缓冲由 [`RespServerSession::run_lua_command`] 在脚本窗口换出、
/// 会话脚本缓存同时摘除，故重入路径与脚本期借用互斥且外层字节不被覆写污染。
struct RespScriptingApi<'a>(&'a mut RespServerSession);

impl RespScriptingApi<'_> {
  /// 拼装 RESP 数组请求（数组头 + 命令 + 参数，单点经 RespVecExt）。
  fn resp_request(cmd: &[u8], args: &[&[u8]]) -> Vec<u8> {
    let mut request = Vec::new();
    request.write_resp_array_len(args.len() + 1);
    request.write_resp_bulk_string(cmd);
    for arg in args {
      request.write_resp_bulk_string(arg);
    }
    request
  }
}

impl ScriptingApi for RespScriptingApi<'_> {
  /// 分派 RESP 请求（对标 C# TryConsumeMessages；C# 的
  /// ScratchBufferNetworkSender 占位在 rust 无 INetworkSender 形状约束，
  /// 应答直写 Vec 缓冲）
  fn dispatch_resp(&mut self, request: &[u8], response: &mut Vec<u8>) {
    // 对标 C# LuaRunner.Functions.cs:ProcessCommandFromScripting 尾部
    // `respServerSession.TryConsumeMessages(request.ptr, request.length)`：
    // 脚本格式化缓冲切为接收内容重入消费装配（C# 的 recvBufferPtr 切到
    // reqBuffer + 入口 `if (!txnSkip) readHead = 0` 的游标归零）。C# 切的是
    // 内嵌 processor 的接收窗，外层批字节不受扰动；rust 重入共享会话，外层
    // 接收窗与已产出应答由 [`RespServerSession::run_lua_command`] 在脚本窗口
    // 换出、收尾挂回，此处只覆写窗口内的会话接收窗
    let session = &mut *self.0;
    session.recv_buffer.clear();
    session.recv_buffer.extend_from_slice(request);
    session.read_head = 0;
    session.end_read_head = 0;
    // 消费序与网络泵同构（drive.rs 泵循环的脚本重入投影）：消费 → 水位让渡
    // 冲出应答后续消费 → 挂起态就地驱动闭环。
    //
    // 挂起承接是 C# 重入语义的必需项：C# 侧脚本内命令的磁盘 pending 与阻塞
    // 等待都在 TryConsumeMessages 的调用栈上同步收割，应答齐了才返回，故 C#
    // 不存在「重入返回而命令仍挂起」的形态；rust 侧两态以会话挂起体承载，
    // 本函数返回前必须取走并驱动——留 pending 回会话即令本条 redis.call 拿
    // 空应答（resp_convert 落 UnexpectedError）且后续每条 redis.call 被消费
    // 入口门连锁挡回，残留挂起体更会被网络泵当作本连接的挂起 resolve，把一帧
    // 不属于任何客户端命令的应答插进 EVAL 之后的出网流
    loop {
      if session.try_consume_messages().is_none() {
        break;
      }
      // 批内输出水位让渡：应答先并入 response 再续消费——会话输出缓冲在下一
      // 批入口即被清空，不冲出即丢整段已产出应答（大应答脚本命令曾据此凭空
      // 截断）
      if session.take_output_watermark_yield() {
        session.take_output_into(response);
        continue;
      }
      let mut resumed = false;
      // 阻塞挂起承接：应答经泵同款并入口直写 response，不碰会话出网缓冲
      if let Some(blocked) = session.take_blocked_wait() {
        let (cmd, result) = blocking_wait(blocked.resolve());
        session.resolve_blocked_wait_into(cmd, result, response);
        resumed = true;
      }
      // 慢路径挂起承接（冷键降级 / 槽位门等待）
      if let Some(slow) = session.take_slow_wait() {
        let reply = blocking_wait(slow.resolve());
        session.resolve_slow_wait_into(&reply, response);
        resumed = true;
      }
      if !resumed {
        break;
      }
    }
    session.take_output_into(response);
  }

  /// GET 特例（C# api.GET）：RESP 请求闭环后解析批量串/null 应答
  ///（C# 为存储 API 直连，rust 重入会话解析面，须带完整数组头）
  fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, &'static str> {
    let request = Self::resp_request(b"GET", &[key]);
    let mut response = Vec::new();
    self.dispatch_resp(&request, &mut response);
    parse_bulk_reply(&response)
      .map(|opt| opt.map(<[u8]>::to_vec))
      .map_err(reply_error_str)
  }

  /// SET 特例（C# api.SET）：+OK 或错误应答（同上，带完整数组头）
  fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), &'static str> {
    let request = Self::resp_request(b"SET", &[key, value]);
    let mut response = Vec::new();
    self.dispatch_resp(&request, &mut response);
    parse_simple_reply(&response).map_err(reply_error_str)
  }

  fn resp_protocol_version(&self) -> u8 {
    self.0.resp_protocol_version
  }

  fn update_resp_protocol_version(&mut self, version: u8) {
    self.0.update_resp_protocol_version(version);
  }

  /// 独立缓冲解析（redis.acl_check_cmd 有效性判定的会话单点）
  fn parse_resp_command_buffer(&mut self, buffer: &[u8]) -> Option<RespCommand> {
    self.0.parse_resp_command_buffer(buffer)
  }

  /// ACL 位图门（Lua redis.call / redis.acl_check_cmd 路径，
  /// LuaRunner.Functions.cs:2993 / :2879）
  fn check_acl_permissions(&self, command: RespCommand) -> bool {
    self.0.acl_permits(command)
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
    self.parse_state.arg_in(&self.recv_buffer, idx)
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
  fn active_db_id(&self) -> u64 {
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
    self.write_null_array();
  }

  #[inline]
  fn write_array_length(&mut self, count: usize) {
    self.writer2().write_array_length(count);
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

  fn reset_cluster_slot_verification_result(&mut self) {
    if let Some(ref cluster) = self.cluster_session {
      cluster.reset_cached_slot_verification_result();
    }
  }

  fn park_iterative_slot_wait(&mut self) -> bool {
    let Some(cluster) = &self.cluster_session else {
      return false;
    };
    let Some(slow) = cluster.take_pending_slow() else {
      return false;
    };
    self.pending_slow = Some(slow);
    self.pending_rearm = true;
    true
  }

  fn verify_cluster_txn_keys(&mut self, keys: &[&[u8]]) -> bool {
    let Some(ref cluster) = self.cluster_session else {
      return true;
    };
    cluster.reset_cached_slot_verification_result();
    let input = ClusterSlotVerificationInput {
      slot: self.active_db_slot(),
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

/// [`wresp::read::ReplyError`] → [`ScriptingApi`] 的 `&'static str` 错误面映射
fn reply_error_str(err: ReplyError) -> &'static str {
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

impl PubSubSessionCommands for RespServerSession {
  #[inline]
  fn session_id(&self) -> i64 {
    self.id
  }

  /// 消息域隔离键前缀唯一来源：认证绑定的会话 ns（与存储域物理键同口径）
  #[inline]
  fn namespace(&self) -> u64 {
    self.namespace
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

  #[inline]
  fn has_cluster_session(&self) -> bool {
    self.cluster_session.is_some()
  }

  #[inline]
  fn cluster_publish(&mut self, is_spublish: bool, channel: &[u8], message: &[u8]) {
    let Some(cluster) = &self.cluster_session else {
      return;
    };
    let cmd = if is_spublish {
      RespCommand::Spublish
    } else {
      RespCommand::Publish
    };
    cluster.cluster_publish(cmd, channel, message);
  }
}

impl RespServerSession {
  /// pub/sub 命令共同骨架：trait 默认实现同时借用会话输出面与自持 wire，
  /// take→调用→归还规避字段级双重借用；占位 wire 零堆分配（Vec::new 不分配）
  fn with_pubsub(&mut self, f: impl FnOnce(&mut Self, &mut PubSubSession) -> bool) -> bool {
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

  /// 退订分片通道（rust 补全命令，C# 无对位，声明见
  /// wpubsub::session_commands::PubSubSessionCommands::network_sunsubscribe）
  #[inline]
  pub fn network_sunsubscribe(&mut self, args: &[&[u8]]) -> bool {
    self.with_pubsub(|s, wire| PubSubSessionCommands::network_sunsubscribe(s, wire, args))
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

/// 解析态参数视图批量收集（<=8 元素栈上分配；parse_state 与接收缓冲
/// 显式传参，借用按字段拆分，可与 self.output 等正交字段的可变借用并存）
pub(crate) fn collect_arg_views<'a>(
  parse_state: &SessionParseState,
  recv_buffer: &'a [u8],
) -> SmallVec<[&'a [u8]; 8]> {
  let count = parse_state.count;
  let mut args = SmallVec::with_capacity(count);
  for i in 0..count {
    args.push(parse_state.arg_in(recv_buffer, i));
  }
  args
}

/// libs/common/Parsing/RespParsingException.cs:ThrowIntegerOverflow
///
/// 置协议违规哨兵，文案
/// `Unable to parse integer. The given number is larger than allowed: {digits}`
///（C# 携 ASCII 数字串；u64 溢出形态不含触发位，对齐 C# TryReadUInt64
/// 抛出时 readHead 尚未越过的语义）
#[inline]
fn set_integer_overflow(parse_violation: &mut Option<String>, digits: &[u8]) {
  let number = from_utf8(digits).unwrap_or("");
  *parse_violation = Some(format!(
    "Unable to parse integer. The given number is larger than allowed: {number}"
  ));
}
