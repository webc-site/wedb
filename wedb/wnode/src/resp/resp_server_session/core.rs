//! 会话核心（对标 libs/server/Resp/RespServerSession.cs：结构体本体、构造 /
//! 析构、切库与通用写面）。命令名与区间解析见 [`super::parse`]，网络泵
//! 取出 / 挂起 / 记账面见 [`super::pump`]，消费主循环见 [`super::consume`]，
//! 分派器与存储分派见 [`super::dispatch`]，慢路径续跑尾参编解码族见
//! [`super::resume`]，冷上下文挂起待物化面见 [`super::cold_context`]，输出
//! 写面直用 `wresp::resp_memory_writer::RespWriter` 单态化写出。

use std::{
  mem,
  sync::{Arc, atomic::AtomicU64},
};

use parking_lot::Mutex;
use wacl::{GarnetAclAuthenticator, UserHandle};
use wbase::{hash_slot::slot_of, pool::LimitedFixedBufferPool, time::now_ms_i64};
use wconf::{ConnectionProtectionOption, DEFAULT_RESP_VERSION, RuntimeServerConfig};
use wlua::{LuaOptions, SessionScriptCache, StoreScriptCache};
use wmetric::{
  CommandStats, GarnetLatencyMetrics, GarnetLatencyMetricsSession, GarnetServerMonitor,
  PendingLatencyMeter, SessionMetricsHandle, SlowLogContainer,
};
use wpubsub::session_commands::PubSubSession;
use wresp::{
  cmd_strings::{self as cs},
  command::RespCommand,
  session_parse_state::SessionParseState,
};
#[cfg(feature = "tls")]
use wtls::ServerTlsConfig;
use wtxn::{TransactionManager, TxnState};

pub(crate) use super::args::collect_arg_views;
pub(super) use super::cold_context::{ColdAuthCommit, ColdContextPending, ColdHelloCommit};
// 目录化拆分（兄弟模块 flat 形态，对标 C# RespServerSession 分片文件）：
// 续跑尾参编解码族拆至 [`super::resume`]、冷上下文挂起面拆至
// [`super::cold_context`]、消费主循环拆至 [`super::consume`]、分派器拆至
// [`super::dispatch`]、参数汇集面拆至 [`super::args`]；此处 re-export 保持
// `super::core::X` 既有引用路径与 mod.rs 的 pub use 面不变
pub use super::resume::{EtagResume, MsetnxResume, TtlLeg, TtlResume};
use super::{
  attach::RespServerSessionOptions, auth::AclMount, custom::CustomCommandRef, lua::ScriptSuspend,
};
use crate::{
  aof::garnet_append_only_file::GarnetAppendOnlyFile,
  cluster_provider::ClusterProviderHandle,
  cluster_session::ClusterSession,
  primary_tasks::PrimaryTasks,
  resp::{
    BlockedWait, ItemBroker, garnet_api::GarnetApi, parser::resp_command::MruCommandCache,
    slow_path::SlowWait,
  },
  servers::consumer_registry::write_client_info_fields,
  traits::PeerSource,
};

/// libs/host/GarnetServer.cs:RedisProtocolVersion（HELLO 应答 version 字段）
pub const REDIS_PROTOCOL_VERSION: &str = "7.4.3";

/// 接收缓冲默认驻留容量（对标泵 64KB 池化接收缓冲水位；scratch 直读形态
/// 整段消费完毕后超限容量即释放回归此水位；收口复位臂见 [`super::consume`]）
pub(super) const DEFAULT_RECV_BUFFER_CAPACITY: usize = 1 << 16;

/// 挂起窗探测竞速入向保全水位（票 task/ing/wnode-probe-preserve-unbounded-recv-buffer-oom）：
/// 泵层探测竞速 `probe_race`（net/handler/drive.rs）竞速期本地累积量达此界即停建
/// 探测读，挂起窗单连接驻留封顶。C# 阻塞/慢命令在网络线程内联阻塞
/// （libs/server/Resp/Objects/ListCommands.cs:283 `Must block as we're on the network
/// thread` + `AsyncUtils.BlockingWait`），挂起窗无人在场读套接字，对端来字滞留内核
/// SO_RCVBUF 有界、TCP 零窗自然背压（libs/common/Networking/TcpNetworkHandlerBase.cs:214
/// do/while 内核守护循环停摆）；rust 泵挂起窗主动抽干对端来字，须以本水位复刻同款
/// 「单连接挂起窗驻留硬顶」形态。与出向 OUTPUT_WATERMARK_BYTES 同量级但语义独立
pub const PROBE_PRESERVE_WATERMARK: usize = 1 << 17;

/// 输出缓冲默认驻留容量（在 garnet 中的相对路径:libs/common/Networking/GarnetTcpNetworkSender.cs:GarnetTcpNetworkSender
/// ——C# 发送侧水位即构造传入的 NetworkBufferSettings.sendBufferSize；rust 会话以
/// 平铺 Vec 承载 output，初始构造回归此水位；与接收水位同为 64KB 但语义独立，C# 两域各为独立配置，禁止混用）
pub(super) const DEFAULT_OUTPUT_BUFFER_CAPACITY: usize = 1 << 16;

/// libs/server/Resp/RespServerSession.cs:RespServerSession
///
/// RESP 服务器会话
pub struct RespServerSession {
  /// 会话标识（CLIENT ID / CLIENT INFO 使用；C# Id）
  pub id: i64,
  /// 会话出生时刻（毫秒 tick；C# CreationTicks = Environment.单调毫秒。
  /// 源=accept 预注册条目 creation_ticks，经泵 `NetworkHandler::set_session`
  /// 装配期单点回填（CLIENT LIST/INFO/KILL MAXAGE 时基单源，勿按 C# 构造期
  /// 口径回改自取）；无条目形态（嵌入/哑桩）回落下方构造期自取值）
  pub creation_ticks: i64,
  /// 对端端点（C# networkSender.RemoteEndpointName；无网络发送器为空串。
  /// 仅 CLIENT INFO/日志展示文本，本地判定读 [`Self::peer_source`]）
  pub remote_endpoint: String,
  /// 对端来源类型（C# remoteEndpoint 端点对象对位；accept 侧一次性折叠，
  /// is_local_connection 唯一真源。缺省 `Ip { loopback: false }` = 未接网
  /// 非本地，对位 C# networkSender 缺失时的恒假臂）
  pub peer_source: PeerSource,
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

  /// 当前生效用户句柄（C# _userHandle；唯一真源——认证挂载 / 免认证兜底
  /// default 单例，None = 未认证（ACL 档认证失败 / 撤销挂载））
  pub acl_user_handle: Option<Arc<UserHandle>>,
  /// ACL 挂载态（跨连接改权收敛的会话侧快照；None = 无 ACL 挂载面）
  pub(super) acl_mount: Option<AclMount>,
  /// 认证器是否支持认证（C# _authenticator.CanAuthenticate；免认证形态为 false）
  pub authenticator_can_authenticate: bool,

  /// ASKING 跳过计数（C# SessionAsking）
  pub session_asking: u8,
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
  /// 延迟指标（C# LatencyMetrics；监视关闭时为 None。对标 C# 的
  /// `GarnetLatencyMetricsSession` 裸数组直写：本会话独占、记点一律 `&mut`，
  /// 故为拥有型而非 Arc——共享句柄必然要求把无锁写口降级成写锁。
  /// 监视器迭代时钟由实例内部持有 —— C# LatencyMetrics._monitorIterations）
  pub latency_metrics: Option<GarnetLatencyMetricsSession>,
  /// PENDING_LAT 计量槽（C# storageSession 与会话共持同一
  /// `GarnetLatencyMetricsSession` 的 pending 计时臂）：存储执行域只有
  /// `&self`，触不到上面那张属主独占表，故本臂单独成每连接一份的槽，
  /// 经 `Arc` 供执行域记点，版本翻转点按引用并入全局延迟表
  pub pending_latency: Option<Arc<PendingLatencyMeter>>,

  /// 解析态（C# parseState）
  pub parse_state: SessionParseState,
  /// 接收缓冲（C# recvBufferPtr 固定接收缓冲的托管等价；parser 分片读写）
  pub recv_buffer: Vec<u8>,
  /// 接收缓冲空闲段初始化代际（TLS 读整段清零随缓冲付一次的跨读记忆，
  /// `wbase::primed` 契约；换缓冲实例必清，见高水位复位臂）
  pub recv_prime_key: Option<(usize, usize)>,
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
  /// MSETNX 慢路径承接模式（[`MsetnxResume`]）：快路径判定段降级保持
  /// [`MsetnxResume::Replay`]、写入段异步闭环信号降级置
  /// [`MsetnxResume::Continue`]、回滚删除遇降级残留置
  /// [`MsetnxResume::Rollback`]，exec 降级快照据此追加模式尾参
  ///（b"0"/b"1"/b"r"），慢路径消费后复位。进入命令即复位、dispatch 消费后
  /// 复位，无跨命令残留
  pub msetnx_resume: MsetnxResume,
  /// RESTORE / SET 条件写族「值已提交 + TTL 待投」续跑标记（[`TtlResume`]，
  /// 票 wnode-nx-conditional-ttl-degrade-replay-selfhit）：快臂值先落库、
  /// put_ttl_sync 遭环形页翻转降级时置 [`TtlResume::Pending`]，exec 降级快照
  /// 据此追加 9 字节尾参（沿 MSETNX/DEL 尾参先例，经既有 pending_slow 通道
  /// 承载，不新建第二通道），慢臂剥尾参消费后仅窗内补投 TTL 出成功帧，
  /// 杜绝整命令重放自碰已提交值；GET 回旧值形值已提交降级由调用侧重映射为
  /// [`TtlResume::ReplyEcho`]（应答已保留，慢臂补投 TTL 出零字节，票
  /// zcode-r153c-setrangeget 案一）；进入命令即复位、快照消费后复位，无跨命令残留
  pub ttl_resume: TtlResume,
  /// ETag 写族「值腿已提交 + 余腿待投」续跑标记（[`EtagResume`]，票
  /// zcode-r139c-etag2 案一情形 B）：快臂值已同步落库后 put_ttl_sync /
  /// put_etag_sync 遭环形页翻转降级时置 [`EtagResume::Pending`]，exec 降级快照
  /// 据此追加 17 字节尾参（同一 pending_slow 通道），慢臂剥尾参后持窗补投余腿
  /// 出与快臂逐字节一致的成功帧，杜绝重放自碰已提交值致条件重判发散；
  /// 进入命令即复位、快照消费后复位，无跨命令残留
  pub etag_resume: EtagResume,
  /// DEL/UNLINK 快路径降级已删键计数：快路径遇异步闭环信号（复合对象/页翻转等）
  /// 降级时记录当前已删除键数，exec 降级快照据此追加尾参，慢路径续传计数；
  /// 进入命令即复位、dispatch 消费后复位，无跨命令残留
  pub del_deleted_count: i64,
  /// 流水线 Scatter-Gather GET 聚合键列表（C# pendingGetOutputArr 的等价承接）
  pub sg_batched_keys: Option<Vec<Vec<u8>>>,
  /// MRU 命令缓存（C# _cachedCmd0/1；resp_command 域维护）
  pub(crate) mru_cache: MruCommandCache,
  /// 会话脚本缓存（EnableLua 时创建；C# sessionScriptCache）
  pub session_script_cache: Option<SessionScriptCache>,
  /// 全局脚本缓存（C# storeWrapper.storeScriptCache，服务器存储实例级共享）；
  /// 构造期默认值为未注入依赖的纯协议层占位，宿主装配经
  /// [`Self::inject_dependencies`] 以基座单点实例整体替换（lock_table 同款
  /// 构造/注入两段式）
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

  /// 索引自动扩容运行门（C# storeWrapper.serverOptions.AdjustedIndexMaxCacheLines
  /// > 0 投影；`--index-max-size` 配置即常驻拉起 IndexAutoGrowTask，CONFIG SET
  /// > index 据此短路拒绝人工扩容，杜绝与自动扩容并发同入 grow_index_blocking）
  pub index_auto_grow_active: bool,

  /// 集群会话切面（C# clusterSession；None = 单机形态，命令路径与
  /// C# clusterSession == null 分支一致）
  pub cluster_session: Option<ClusterSession>,
  /// 集群提供者切面（C# clusterProvider；只读查询与缓冲池管理）
  pub cluster_provider: Option<ClusterProviderHandle>,
  /// 网络监听层缓冲池句柄（DEBUG PURGEBP ServerListener 清理源；C# 侧经
  /// `storeWrapper.Servers` 遍历宿主 `GarnetServerTcp` 直调 `Purge()`——
  /// rust 侧监听器全连接共享同一 [`LimitedFixedBufferPool`]，泵装配期
  /// `NetworkHandler::set_session` 注入，None = 未接网（纯协议层形态））
  pub listener_buffer_pool: Option<Arc<LimitedFixedBufferPool>>,

  /// 集合项经纪（C# storeWrapper.itemBroker；None = 装配未注入，阻塞命令
  /// 走立即可取降级路径）
  pub item_broker: Option<Arc<ItemBroker>>,

  /// 挂起中的阻塞命令等待体（网络泵 take 后 await 驱动；C# 由网络线程
  /// BlockingWait 内联承担）
  pub pending_block: Option<BlockedWait>,
  /// 冷上下文挂起待物化面（严格会话 set_context 报告未装载时登记目标
  /// (ns, db) 与落位载荷；应答组装点据此挂起 SlowWait 点查装载，装载成功
  /// 应答回写时物化、失败即弃——会话标量在物化前严格保持旧值；派发前置
  /// 空防跨命令残留）
  pub(super) cold_ctx: Option<ColdContextPending>,

  /// 挂起中的慢路径执行体（SCAN/KEYS/DBSIZE/CLUSTER RESET 等同步段
  /// 返回 Ok(false) 的命令；网络泵 take 后 await 驱动，应答按流水线
  /// 顺序写回——C# 网络线程同步执行慢命令的 compio 异步等价物）
  pub pending_slow: Option<SlowWait>,

  /// 脚本内挂起让渡槽（协程化承接：EVAL 内 redis.call 命中阻塞/慢路径时，
  /// 挂起体移出泵可见槽入本槽，网络泵经
  /// [`RespServerSession::resume_suspended_script`] await 续跑脚本协程）。
  /// 与 pending_block/pending_slow 互斥承载，杜绝泵同款挂起分支误驱动
  pub(crate) script_suspend: Option<ScriptSuspend>,

  /// 脚本挂起让渡标记（redis.call 收尾读取后挂起协程；读取即复位，与
  /// script_suspend 同置同清）
  pub(crate) script_yield_tag: Option<i32>,

  /// 重驱型挂起标志（槽位门 Wait / 迭代门 Pending 登记等待体时置位）：
  /// 与产应答型 pending_slow 不同，等待体不产出应答，消费循环须回退游标
  /// 至本命令起点，等待体驱动至迁移推进/超时后重评重驱本命令
  pub pending_rearm: bool,

  /// ACL 挂载刷新停车标志（鉴权预门判挂载陈旧时置位）：与 pending_rearm
  /// 同为重驱型，泵经执行域刷新臂 await 点查（不产应答）后回退游标重解析
  /// 本命令，门链以新挂载重评
  pub(crate) pending_acl_refresh: bool,

  /// AUTH / HELLO / ACL 族命令停车槽（产应答型）：分派漏斗预筛命中即
  /// 快照 (命令, 参数) 登记，本命令游标已推进（与 pending_slow 同形），泵经
  /// 执行域 [`crate::resp::garnet_api::GarnetApi::exec_auth_acl`]
  /// 异步臂 await 闭环后把应答按流水线顺序冲出；参数快照脱离接收缓冲
  /// 生命周期（await 期间缓冲可被复用，与 SlowWait::for_command 同口径）。
  /// 失败计数由泵侧闭环单点
  /// [`RespServerSession::account_parked_auth_acl_failure`] 按同判据补计
  /// （切点恒 0：停车返回即冲空缓冲，闭环应答独占 output[0..]）
  pub(crate) pending_auth_acl: Option<(RespCommand, Vec<Vec<u8>>)>,

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

  /// TLS 证书热加载共享句柄（C# storeWrapper.serverOptions.TlsOptions 的
  /// 会话侧可达面：CONFIG SET cert-file-name / cert-password 经此在线重载
  /// 活跃证书；None = 未装配 TLS，证书对按 "ERR TLS is disabled." 拒绝）
  #[cfg(feature = "tls")]
  pub tls_config: Option<Arc<ServerTlsConfig>>,

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
  /// 慢日志批次级阈值缓存（tick；C# slowLogThreshold——
  /// TryConsumeMessages 批入口单次读运行时配置（µs × 刻度因子）折算缓存，
  /// 0 = 禁用；ProcessMessages 循环体据此门控 HandleSlowLog 调用点，
  /// 慢日志关闭时命令热路径零配置读、零时钟取）
  pub slow_log_threshold: u64,
  /// ACL 认证器（C# `_authenticator` 的 ACL 档实例；None = 未装配认证器的
  /// 免认证形态。无状态纯判定组件，只读共享免锁——对标 C# ServerState 与
  /// RespServerSession 间无锁共享同一 IGarnetAuthenticator 实例）
  pub acl_authenticator: Option<Arc<GarnetAclAuthenticator>>,
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
    // 时钟仅一次全局槽查询 + Arc 克隆；未装监视器（单测/无采样装配）时兜底
    // 零值时钟，与 C# monitor 为 null 时不建延迟实例同形。
    let monitor_iterations = GarnetServerMonitor::global()
      .map(|m| Arc::clone(&m.monitor_iterations))
      .unwrap_or_else(|| Arc::new(AtomicU64::new(0)));
    // 会话延迟表：本会话独占的拥有型实例（无锁直写）
    let latency_metrics = options.latency_monitor.then(|| {
      GarnetLatencyMetricsSession::new(
        Arc::clone(&monitor_iterations),
        global_latency_metrics.clone(),
        GarnetLatencyMetricsSession::DEFAULT_LATENCY_TYPES,
      )
    });
    // PENDING_LAT 槽：执行域以 &self 记点，故为 Arc 共享的独立小槽；
    // 全局延迟表缺失（未开延迟监视）时无出口，不建槽。
    let pending_latency = match (options.latency_monitor, &global_latency_metrics) {
      (true, Some(global)) => Some(Arc::new(PendingLatencyMeter::new(
        Arc::clone(&monitor_iterations),
        Arc::clone(global),
      ))),
      _ => None,
    };

    let mut session = Self {
      id,
      // 构造期自取仅为回落臂（无注册条目形态）；接网会话由泵
      // NetworkHandler::set_session 以 accept 预注册条目时刻覆盖回填（单源）
      creation_ticks: session_now_ms(),
      remote_endpoint: String::new(),
      peer_source: PeerSource::default(),
      local_endpoint: String::new(),
      resp_protocol_version: DEFAULT_RESP_VERSION,
      client_name: None,
      client_lib_name: None,
      client_lib_version: None,
      acl_user_handle: None,
      acl_mount: None,
      authenticator_can_authenticate: false,
      session_asking: 0,
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
      pending_latency,
      parse_state: SessionParseState::new(),
      recv_buffer: Vec::with_capacity(DEFAULT_RECV_BUFFER_CAPACITY),
      recv_prime_key: None,
      bytes_read: 0,
      read_head: 0,
      end_read_head: 0,
      parse_violation: None,
      fatal_disconnect: false,
      output: Vec::with_capacity(DEFAULT_OUTPUT_BUFFER_CAPACITY),
      output_watermark_yield: false,
      current_custom_command: None,
      msetnx_resume: MsetnxResume::Replay,
      ttl_resume: TtlResume::Full,
      etag_resume: EtagResume::Full,
      del_deleted_count: 0,
      sg_batched_keys: None,
      connection_protection_debug: options.enable_debug_command,
      aof_commit_mode_gate: options.enable_aof && options.wait_for_commit,
      index_auto_grow_active: options.index_auto_grow_active,
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
      // 纯协议层占位（lock_table 同款两段式）：生产会话经 inject_dependencies
      // 替换为基座单点实例，跨连接共享才成立
      store_script_cache: Arc::new(StoreScriptCache::default()),
      lua_options: options.lua_options,
      garnet_api: None,
      cluster_session: None,
      cluster_provider: None,
      listener_buffer_pool: None,
      item_broker: None,
      pending_block: None,
      cold_ctx: None,
      pending_slow: None,
      script_suspend: None,
      script_yield_tag: None,
      pending_rearm: false,
      pending_acl_refresh: false,
      pending_auth_acl: None,
      runtime_config: RuntimeServerConfig::shared_default(),
      primary_tasks: None,
      aof: None,
      #[cfg(feature = "tls")]
      tls_config: None,

      txn_manager: None,
      pubsub: PubSubSession::with_mailbox_capacity(None, 4),
      slow_log_container: None,
      global_latency_metrics,
      slow_log_start_ticks: 0,
      slow_log_threshold: 0,
      acl_authenticator: None,
      no_script_start: 0,
      no_script_bitmap: None,
    };
    // C# 构造尾部 AuthenticateUser(defaultUser)：免认证形态即落到默认用户
    //（GetDefaultUserHandle 兜底）；ACL 档挂载前为空操作
    session.authenticate_user(options.default_user.as_bytes(), &[]);
    session
  }

  /// 当前认证用户名（C# `targetSession._userHandle?.User.Name` 的直读投影；
  /// None = 未认证）。展示面（CLIENT LIST/INFO 等经 ClientView）与测试
  /// 断言的唯一取名口，无第二处用户名镜像
  #[inline]
  pub fn user_name(&self) -> Option<&str> {
    self
      .acl_user_handle
      .as_ref()
      .map(|h| h.user().name.as_str())
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

  /// 刷新执行域会话物理前缀（换号清库后对齐虚拟数据库代数）
  #[inline]
  pub fn refresh_active_db(&self) {
    if let Some(api) = &self.garnet_api {
      api.refresh_active_db();
    }
  }

  /// 在途事务废弃收口（网络泵三臂竞速废弃与本会话析构共用的事务收口单点
  /// 转接，法定语义与注释钉版在事务域 `wtxn::TransactionManager::finish_abandoned`）
  ///
  /// EXEC 重放段（[`TxnState::Running`] 直通）命令挂起后，泵层
  /// `NetworkHandler::drive_loop` 的 probe_race 三臂被终止广播（CLIENT KILL /
  /// 停机令牌）或对端 FIN/RST 胜出即丢弃执行体退出泵循环——重放中途废弃令
  /// 尾帧 EXEC 不再被消费、TxnCommit 永不再达，已落 AOF 的 TxnStart 成孤儿
  /// 残组。本方法在废弃时刻经事务管理器补投显式废弃终结符后复位；非
  /// Running 态恒零动作（状态门在管理器单点）。
  ///
  /// 管理器归还后同步会话镜像 [`Self::txn_state`]（与事务面 `with_txn_manager`
  /// 同款双态对齐单机制，杜绝镜像滞留 Running 致门分派误判）。
  pub fn finish_abandoned_txn(&mut self) {
    let Some(txn) = &mut self.txn_manager else {
      return;
    };
    if let Err(e) = txn.finish_abandoned() {
      // 入队失败仅落日志：客户端已废弃断连无从应答，锁面与屏障票据仍随
      // 管理器复位释放（C# 无此面——重放内联必达 Commit，异常上抛由会话
      // catch 断连，本处为 rust 竞速废弃臂的对应承接）
      log::error!(
        "会话 {} 废弃事务投递 AOF 终结符失败，事务已复位: {e}",
        self.id
      );
    }
    self.txn_state = txn.state;
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
    // 慢路径执行体就地取消（drop future 即取消，compio 任务自清理）；
    // 执行体内阻塞等待面的观察者随 ObserverDropGuard 经经纪注销
    //（C# broker.HandleSessionDisposed 对位，不留僵尸观察者）
    self.pending_slow.take();
    // 脚本挂起让渡槽：挂起体就地取消（阻塞观察者经经纪置 SessionDisposed；
    // 慢执行体 drop 即取消），挂起协程随会话缓存的 runner 一并 lua_close
    if let Some(suspend) = self.script_suspend.take()
      && let Some(blocked) = suspend.blocked
    {
      blocked.abort();
    }
    self.script_yield_tag = None;
    // 摘除本会话全部订阅（C# Dispose 尾部 subscribeBroker?.RemoveSubscription；
    // 幽灵订阅会令 PUBLISH 计数虚高并向已断连邮箱投递）
    if let Some(broker) = self.pubsub.broker() {
      broker.remove_subscription(self.id as u64);
    }
    if let Some(cluster) = self.cluster_session.take() {
      cluster.dispose();
    }
    self.cluster_provider = None;
    // 在途事务废弃收口（事务收口单点的会话析构侧调用面）：泵三臂之外的全部
    // 退出路径——QUIT / 对端 EOF / 协议违规 / 致命断连 / 停机排空 / 收场尾巴
    // ——都汇聚到本 dispose，Running 态事务在此补投 AOF 废弃终结符后复位，
    // 杜绝 TxnStart 已落而 TxnCommit 永不再达的孤儿残组（C# 无此面：
    // NetworkEXEC 内联重放必达 Commit）。泵臂已在废弃 instant 先行收口时，
    // 管理器态非 Running，本调用恒零动作、不重复投递
    self.finish_abandoned_txn();
    if let Some(txn) = &mut self.txn_manager {
      txn.cluster_enabled = false;
    }
    self.merge_metrics_history_session_dispose();
  }

  /// 取走 ACL 挂载刷新停车标志（网络泵专属：经执行域刷新臂 await 点查后
  /// 游标已回退，重入消费即重解析重评本命令；取走即复位）
  pub fn take_pending_acl_refresh(&mut self) -> bool {
    mem::take(&mut self.pending_acl_refresh)
  }

  /// 取走 AUTH / HELLO / ACL 族停车快照（网络泵专属：经执行域 exec_auth_acl
  /// 异步臂 await 闭环，应答直写会话输出缓冲后冲出；取走即复位）。
  /// 停车返回即被泵 [`Self::take_output_into`] 冲空缓冲，闭环应答独占
  /// output[0..]——陈旧的停车时水位不具切段意义（跨冲出边界失效），记账
  /// 切点恒 0（见 [`Self::account_parked_auth_acl_failure`]）
  pub fn take_pending_auth_acl(&mut self) -> Option<(RespCommand, Vec<Vec<u8>>)> {
    self.pending_auth_acl.take()
  }

  /// 停车臂失败应答补计单点（网络泵专属：await 闭环完成后、冲出前调用）
  ///
  /// 同步段 CommandStats 门（RespServerSession.cs:683-689 对位）在本命令
  /// 同步迭代内扫描时，停车臂应答尚未组装、判据恒假（calls 已在同步段
  /// 计，此处只补 failed 防双计）；本口按与同步段同一判据（命令错误置位
  /// 或 output 应答段以 `-` 开头）补计 failed_calls 并复位
  /// 置位，消除 HELLO/AUTH/ACL 族失败样本在 INFO commandstats 的系统性
  /// 少计（C# 全错误帧经会话级 WriteError 统一置位、收尾门入账的对位收口）
  pub fn account_parked_auth_acl_failure(&mut self, cmd: RespCommand) {
    if let Some(stats) = &self.command_stats {
      let mut stats = stats.lock();
      // 切点恒 0：停车返回即冲空缓冲，闭环应答独占 output[0..]（陈旧停车
      // 水位跨 take_output_into 冲出边界失效——按其切片轻则帧中段误判漏计
      // failed_calls，重则越界 panic）
      if self.command_error_written || self.output.starts_with(b"-") {
        stats.increment_failed(cmd);
        self.command_error_written = false;
      }
    }
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
  ///
  /// C# 契约：底层库会话确认就绪（success）后才 `SwitchActiveDatabaseSession`
  /// 物化 `activeDbId`，获取失败即返回、原库标量严格保持。rust 投影同口径：
  /// 热库（set_context 同步成功）当场物化标量；冷库（映射未装载）严禁提前
  /// 覆写 `active_db_id`——目标暂存挂起面由 SELECT 应答组装点异步点查装载，
  /// 装载成功应答回写时物化，失败即弃（杜绝加载失败后会话标量与底层物理域
  /// 永久撕裂、后续命令跨库错写）。两物化点均经
  /// [`Self::invalidate_watch_on_db_switch`] 承接 C# `SwitchActiveDatabaseSession`
  /// 的 txnManager 换任 watch 作废面（r133c-selectdb 案二裁决）
  #[inline]
  pub fn try_switch_active_database_session(&mut self, db_id: u64) -> bool {
    if db_id >= self.max_databases {
      return false;
    }
    if let Some(api) = &self.garnet_api
      && !api.set_context(self.namespace, db_id)
    {
      self.cold_ctx = Some(ColdContextPending {
        ns: self.namespace,
        db: db_id,
        auth: None,
        hello: None,
      });
      return true;
    }
    // 热库（或存储执行域未挂载的纯协议层形态：无冷路径即无撕裂面）当场物化
    self.invalidate_watch_on_db_switch(db_id);
    self.active_db_id = db_id;
    true
  }

  /// 切库成功提交点的 WATCH 位点作废单点（r133c-selectdb 案二，方向裁决 a：
  /// 对齐 C# 逐库管理器换任清 watch）
  ///
  /// C# `SwitchActiveDatabaseSession`（libs/server/Resp/RespServerSession.cs:
  /// 1714-1724）字段清单含 `this.txnManager = dbSession.TransactionManager`——
  /// 每库独持事务管理器（libs/server/Resp/GarnetDatabaseSession.cs:46/63），
  /// watch 容器随管理器归库，切库即换任、位点随旧库作废（同时消除 rust 登记期
  /// 烘 hash 跨库恒校验的保守误中止面与旧库 watch 派生 hash 并入新库锁集的
  /// 跨库带外锁条目）。rust 会话级单实例管理器（txn.rs:with_txn_manager 同
  /// 实例 take/put）无法以标量翻转型承接「换任」，本单点以「切库成功提交点
  /// 主动清容器」承接同语义（复用 `TxnWatchedKeysContainer::reset`，与
  /// DISCARD 收尾 txn_resp_commands.rs:334 同型调用）。
  ///
  /// 裁决收口语（方向注记 a，已登 deviations §131）：「回原库不复活」与 C# 非全等——C# 缓存的
  /// dbSession 持原容器，SELECT 回原库位点事实上复活；rust 单实例容器已清，
  /// 回原库不复活。本票裁一次性作废收口，严禁补逐库容器（过度设计，票面明禁）。
  ///
  /// 同库切换 no-op：C# NetworkSELECT 以 `index == activeDbId` 短路不入
  /// Switch（ArrayCommands.cs:143），同库 MULTI 排队期 SELECT 重放臂的在途
  /// WATCH 位点必须保持，故仅异库落点清容器
  #[inline]
  pub(super) fn invalidate_watch_on_db_switch(&mut self, db_id: u64) {
    if db_id != self.active_db_id
      && let Some(txn) = &mut self.txn_manager
    {
      txn.watch_container.reset();
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

  /// CLIENT SETNAME 落库（C# clientName 字段赋值；命令域校验后调用）
  pub fn set_client_name(&mut self, name: Option<&str>) {
    self.client_name = name.map(str::to_string);
  }

  /// libs/server/Resp/BasicCommands.cs:WriteClientInfo
  /// CLIENT INFO 自身行：字段与 CLIENT LIST 同源单机制——视图经
  /// [`current_client_view`](Self::current_client_view) 组装（含条件段
  /// name/user 与集群 M/S flags 臂），行序写出收敛于
  /// [`write_client_info_fields`]（C# LIST/INFO 共函数的 rust 对位）。
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

/// Environment.单调毫秒 等价：会话年龄域毫秒时钟（统一委托 `wbase::time::now_ms_i64`）
fn session_now_ms() -> i64 {
  now_ms_i64()
}
