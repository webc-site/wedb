//! 会话核心（对标 libs/server/Resp/RespServerSession.cs：结构体本体、构造 /
//! 析构、消费主循环与分派器）。命令名与区间解析见 [`super::parse`]，网络泵
//! 取出 / 挂起 / 记账面见 [`super::pump`]，输出写面在
//! resp_server_session_output（并行域，本单不动）。

use std::{
  fmt, mem,
  sync::{Arc, atomic::AtomicU64},
};

use itoa::Buffer;
use parking_lot::Mutex;
use smallvec::SmallVec;
use wacl::{GarnetAclAuthenticator, UserHandle};
use wbase::{
  hash_slot::slot_of,
  time::{now_ms_i64, now_nanos},
};
use wconf::{ConnectionProtectionOption, DEFAULT_RESP_VERSION, RuntimeServerConfig};
use wlua::{LuaOptions, SessionScriptCache, StoreScriptCache};
use wmetric::{
  CommandStats, GarnetInfoMetrics, GarnetLatencyMetrics, GarnetLatencyMetricsSession,
  GarnetServerMonitor, InfoCommand, SessionMetricsHandle, SlowLogContainer,
};
use wpubsub::session_commands::PubSubSession;
use wresp::{
  cmd_strings::{self as cs},
  command::{RespCommand, is_cluster_sub_command, one_if_read, one_if_write},
  ext::{RespSliceExt, RespVecExt},
  metrics::InfoMetricsType,
  session_parse_state::SessionParseState,
};
use wtxn::{TransactionManager, TxnLockTable, TxnState};

use super::{attach::RespServerSessionOptions, auth::AclMount, custom::CustomCommandRef};
use crate::{
  aof::garnet_append_only_file::GarnetAppendOnlyFile,
  cluster_provider::ClusterProviderHandle,
  cluster_session::{ClusterSession, SlotVerifyGate},
  primary_tasks::PrimaryTasks,
  resp::{
    BlockedWait, ItemBroker,
    garnet_api::GarnetApi,
    info_provider::SessionInfoSource,
    parser::resp_command::{MruCommandCache, is_allowed_in_subscription_mode},
    slow_path::SlowWait,
  },
  servers::consumer_registry::write_client_info_fields,
};

/// libs/host/GarnetServer.cs:RedisProtocolVersion（HELLO 应答 version 字段）
pub const REDIS_PROTOCOL_VERSION: &str = "7.4.3";

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
pub(super) const DEFAULT_OUTPUT_BUFFER_CAPACITY: usize = 1 << 16;

/// 批内输出水位（在 garnet 中的相对路径:libs/common/NetworkBufferSettings.cs:NetworkBufferSettings
/// ——默认 sendBufferSize = 1 << 17，即 C# 响应缓冲上界）。批内累计应答达此界
/// 即在命令边界停住交泵实写再续消费：RespWriteUtils TryWrite 写不下即
/// SendAndReset 满刷循环（libs/server/Resp/RespServerSession.cs:SendAndReset）
/// 的 rust 投影，粒度为命令边界而非写点（RespWriter 借用 output，写点复查
/// 在借用模型下不可行）
const OUTPUT_WATERMARK_BYTES: usize = 1 << 17;

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
  /// ACL 挂载态（跨连接改权收敛的会话侧快照；None = 无 ACL 挂载面）
  pub(super) acl_mount: Option<AclMount>,
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
  pub(super) output_watermark_yield: bool,

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
  /// 内建命令分派器（宿主持存储执行域注入；None 时命令解析/门控仍闭环，
  /// 落到分派的命令写明错误，绝不静默）。慢路径分派（`Ok(false)` 降级）
  /// 在 exec 内经此克隆句柄构造 [`SlowWait`]
  pub garnet_api: Option<GarnetApi>,

  /// EnableDebugCommand 镜像（C# storeWrapper.serverOptions.EnableDebugCommand）
  pub(super) connection_protection_debug: ConnectionProtectionOption,

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
  pub(super) cold_ctx: Option<(u64, u64)>,

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
  pub(super) no_script_start: i32,
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
      acl_mount: None,
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
      // 前缀单点：cs::ERR_PROTOCOL_ERROR_PREFIX（对标 C#
      // RespServerSession.cs:528 catch 块 TryWriteError($"ERR Protocol Error: {ex.Message}")）
      self.abort_error_message(&format!("{}{msg}", cs::ERR_PROTOCOL_ERROR_PREFIX));
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

    // 切面致命断流（GarnetException clientResponse:false）：会话指标计数
    //（C# RespServerSession.cs catch(GarnetException) 首行
    // `sessionMetrics?.incr_total_number_resp_server_session_exceptions(1)`，
    // 与协议违规 catch 对称）后不写错误行，发尽累积应答后断连（None 通道
    // 与协议违规共用）
    if self.fatal_disconnect {
      if let Some(metrics) = &self.session_metrics {
        metrics.incr_total_number_resp_server_session_exceptions(1);
      }
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
        let orig_output_len = self.output.len();
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
            let name = cmd.to_cs_name();
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
          // 【有意偏差】失败判定在 C# commandErrorWritten 之外增补
          // 「输出以 - 开头即计失败」扫描——rust 命令臂存在直接写错误帧
          // 不经 AbortWithErrorMessage 置位的路径，扫描兜底使 failed_calls
          // 不漏计；C# 仅认 commandErrorWritten，rust 口径更完整，刻意
          // 保留不删（与 C# 的差异经本注记登记）。
          if let Some(stats) = &self.command_stats {
            let mut stats = stats.lock();
            stats.increment_calls(cmd);
            if self.command_error_written || self.output[orig_output_len..].starts_with(b"-") {
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

  /// libs/server/Resp/RespServerSession.cs:ProcessBasicCommands
  ///
  /// fast 命令族分派（WARNING: 仅 @fast 命令，慢命令走 OtherCommands）。
  /// 命令实现位于 resp 命令文件（并行域），经 [`GarnetApi`] 注入面
  /// 接入；PING/ASKING/QUIT/事务族在会话侧闭环（分派臂只转调，单一实现
  /// 在 basic_commands）。
  pub fn process_basic_commands(&mut self, cmd: RespCommand) -> bool {
    match cmd {
      RespCommand::Ping => {
        // C# RespServerSession.cs:855：PING→NetworkPING/NetworkArrayPING
        //（帧字面量单点在 basic_commands 方法体的 cs 常量）
        let args = collect_arg_views(&self.parse_state, &self.recv_buffer);
        let mut output = mem::take(&mut self.output);
        let r = self.network_ping(&args, &mut output);
        self.output = output;
        matches!(r, Ok(true))
      }
      RespCommand::Asking => {
        // C# RespServerSession.cs:856：ASKING→NetworkASKING
        let mut output = mem::take(&mut self.output);
        let r = self.network_asking(&mut output);
        self.output = output;
        matches!(r, Ok(true))
      }
      RespCommand::Quit => {
        self.to_dispose = true;
        self.output.extend_from_slice(cs::RESP_OK);
        true
      }
      // C# NetworkREADONLY
      RespCommand::Readonly => self.network_readonly(),
      // C# NetworkREADWRITE
      RespCommand::Readwrite => self.network_readwrite(),
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
      // C# TryWriteInt64(Id)，走 wresp 整数帧单点
      self.output.write_resp_int(self.id);
      return true;
    }
    if cmd == RespCommand::Echo {
      // C# RespServerSession.cs:1089：ECHO→NetworkECHO
      let args = collect_arg_views(&self.parse_state, &self.recv_buffer);
      let mut output = mem::take(&mut self.output);
      let r = self.network_echo(&args, &mut output);
      self.output = output;
      return matches!(r, Ok(true));
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
      self.output.write_resp_array_len(2);
      self.output.write_resp_bulk_string(s_str.as_bytes());
      self.output.write_resp_bulk_string(us_str.as_bytes());
      return true;
    }
    if cmd == RespCommand::Async {
      // C# NetworkASYNC：委托 basic_commands 单一定义（参数校验与降级回包全在彼处）
      let args = collect_arg_views(&self.parse_state, &self.recv_buffer);
      let mut output = mem::take(&mut self.output);
      let r = self.apply_async_param(&args, &mut output);
      self.output = output;
      return matches!(r, Ok(true));
    }
    if cmd == RespCommand::ClientInfo {
      let store = self.collect_args_store();
      let args = store.views();
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
      let store = self.collect_args_store();
      let args = store.views();
      return self.write_via_local("client getname error", |s, out| {
        s.network_clientgetname(&args, out)
      });
    }
    if cmd == RespCommand::ClientSetname {
      let store = self.collect_args_store();
      let args = store.views();
      return self.write_via_local("client setname error", |s, out| {
        s.network_clientsetname(&args, out)
      });
    }
    if cmd == RespCommand::ClientSetinfo {
      let store = self.collect_args_store();
      let args = store.views();
      return self.write_via_local("client setinfo error", |s, out| {
        s.network_clientsetinfo(&args, out)
      });
    }
    if cmd == RespCommand::ClientUnblock {
      let store = self.collect_args_store();
      let args = store.views();
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
      let store = self.collect_args_store();
      let args = store.views();
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
      let args = collect_arg_views(&self.parse_state, &self.recv_buffer);
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
          let provider = SessionInfoSource::new(self);
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
    // 自定义对象命令（Customobjcmd）经 dispatch_via_garnet_api 的存储执行域承接；
    // C# CustomTxn / CustomRawStringCmd / CustomProcedure 三族随动态注册层删除，
    // 解析器不再产出这些枚举，事务过程入口单点收敛到 RUNTXP
    let store = self.collect_args_store();
    let args = store.views();
    self.dispatch_via_garnet_api(cmd, &args);
    true
  }

  /// 本地缓冲写出 → 并回会话输出（CLIENT/CLUSTER/ROLE 族 take/log/restore
  /// 共同骨架：take 输出缓冲 → 本地写出 → 失败告警 → 并回）
  fn write_via_local<F, E>(&mut self, ctx: &'static str, f: F) -> bool
  where
    F: FnOnce(&mut Self, &mut Vec<u8>) -> Result<bool, E>,
    E: fmt::Display,
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

  /// libs/server/Resp/RespServerSession.cs:Process（admin 族回退）
  pub fn process(&mut self, cmd: RespCommand) -> bool {
    let store = self.collect_args_store();
    let args = store.views();
    self.dispatch_via_garnet_api(cmd, &args);
    true
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
  ///
  /// 帧由 `wresp::cmd_strings::write_error_raw` 单点成帧（内含 CRLF 切断与
  /// 长度帽清洗）；本会话只承担 `command_error_written` 副作用。
  pub fn abort_error_message(&mut self, message: &str) {
    cs::write_error_raw(&mut self.output, message);
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

  /// 汇集解析态参数所有权副本（唯一使用者为 Lua 脚本窗口：窗口期接收缓冲
  /// 整体换出覆写、[`crate::resp::resp_server_session`] 的 wlua 会话上下文取
  /// `&[Vec<u8>]`，借用无法跨窗口存活）
  pub(crate) fn collect_args(&self) -> Vec<Vec<u8>> {
    (0..self.parse_state.count)
      .map(|i| self.parse_state.arg_in(&self.recv_buffer, i).to_vec())
      .collect()
  }

  /// 汇集解析态参数为单分配物化（见 [`ArgStore`]；被调方取 `&mut self` 的
  /// 分派落点用）
  ///
  /// 借用能够流到的落点一律直用 [`collect_arg_views`]，不经本入口。
  pub(crate) fn collect_args_store(&self) -> ArgStore {
    let args = (0..self.parse_state.count).map(|i| self.parse_state.arg_in(&self.recv_buffer, i));
    // 两遍索引（首遍只读长度、不触碰字节）换 data 的恰容量单次分配
    let lens: SmallVec<[usize; ARG_INLINE]> = args.clone().map(<[u8]>::len).collect();
    let mut data = Vec::with_capacity(lens.iter().sum());
    for arg in args {
      data.extend_from_slice(arg);
    }
    ArgStore { data, lens }
  }

  /// CLIENT SETNAME 落库（C# clientName 字段赋值；命令域校验后调用）
  pub fn set_client_name(&mut self, name: Option<&str>) {
    self.client_name = name.map(str::to_string);
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
}

/// 默认会话（C# internal RespServerSession() 空构造的等价）
impl Default for RespServerSession {
  fn default() -> Self {
    Self::new(0, RespServerSessionOptions::default())
  }
}

/// 命令 arity 纯判定逻辑（供 is_command_arity_valid 使用）。
/// arity = 0 不校验；正值 = 恰好 arity-1 参数；
/// 负值 = 至少 |arity|-1 参数（C# `count < -arity - 1` 为非法）
pub(super) fn is_command_arity_valid_checked(arity: i32, count: usize) -> bool {
  if arity == 0 {
    return true;
  }
  if arity > 0 {
    count == arity as usize - 1
  } else {
    count >= (-arity) as usize - 1
  }
}

/// Environment.TickCount64 等价：会话年龄域毫秒时钟（统一委托 `wbase::time::now_ms_i64`）
fn session_now_ms() -> i64 {
  now_ms_i64()
}

/// 参数收集的栈上内联容量：不多于此数目的参数完全零堆分配（RESP 命令参数
/// 个数常见 1-3，借用面与物化面共用此单源）
const ARG_INLINE: usize = 8;

/// 解析态参数视图批量收集（借用零拷贝；parse_state 与接收缓冲显式传参，
/// 借用按字段拆分，可与 `self.output` 等正交字段的可变借用并存）
///
/// 唯一的借用收集入口：被调方为 `&self` / 字段级借用（集群切面、只读探针、
/// 纯写出函数）时优先本口。
pub(crate) fn collect_arg_views<'a>(
  parse_state: &SessionParseState,
  recv_buffer: &'a [u8],
) -> SmallVec<[&'a [u8]; ARG_INLINE]> {
  let count = parse_state.count;
  let mut args = SmallVec::with_capacity(count);
  args.extend((0..count).map(|i| parse_state.arg_in(recv_buffer, i)));
  args
}

/// 解析态参数的单分配物化容器
///
/// C# 分派臂把 parseState 直递命令实现，零物化；rust 侧多数命令处理器取
/// `&mut self`（会话方法族），接收缓冲的借用与之不可共存，故在此一次性物化：
/// 全部参数字节落单次分配，视图表借用本容器（栈上 SmallVec）因而与被调方的
/// `&mut self` 正交。借用能够流到的落点直用 [`collect_arg_views`]，不经本容器。
pub(crate) struct ArgStore {
  /// 参数字节按 [`Self::lens`] 顺序紧密排布
  data: Vec<u8>,
  /// 各参数长度
  lens: SmallVec<[usize; ARG_INLINE]>,
}

impl ArgStore {
  /// 参数视图表（零字节拷贝；仅参数数超 [`ARG_INLINE`] 时多一次指针表分配）
  pub(crate) fn views(&self) -> SmallVec<[&[u8]; ARG_INLINE]> {
    let mut off = 0usize;
    self
      .lens
      .iter()
      .map(|&len| {
        let arg = &self.data[off..off + len];
        off += len;
        arg
      })
      .collect()
  }
}
