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
  pool::LimitedFixedBufferPool,
  time::{now_ms_i64, now_nanos},
};
use wconf::{ConnectionProtectionOption, DEFAULT_RESP_VERSION, RuntimeServerConfig};
use wlua::{LuaOptions, SessionScriptCache, StoreScriptCache};
use wmetric::{
  CommandStats, GarnetInfoMetrics, GarnetLatencyMetrics, GarnetLatencyMetricsSession,
  GarnetServerMonitor, InfoCommand, PendingLatencyMeter, SessionMetricsHandle, SlowLogContainer,
};
use wpubsub::session_commands::PubSubSession;
use wresp::{
  cmd_strings::{self as cs},
  command::{RespCommand, is_cluster_sub_command, one_if_read, one_if_write},
  ext::{RespSliceExt, RespVecExt},
  session_parse_state::SessionParseState,
};
#[cfg(feature = "tls")]
use wtls::ServerTlsConfig;
use wtxn::{TransactionManager, TxnState};

use super::{
  attach::RespServerSessionOptions,
  auth::{AclMount, RESP_ERR_AUTH_IN_MULTI},
  custom::CustomCommandRef,
  lua::ScriptSuspend,
};
use crate::{
  aof::garnet_append_only_file::GarnetAppendOnlyFile,
  cluster_provider::ClusterProviderHandle,
  cluster_session::{ClusterSession, SlotVerifyGate},
  primary_tasks::PrimaryTasks,
  resp::{
    BlockedWait, ItemBroker,
    acl_commands::AclGateVerdict,
    basic_commands::parse_hello_args,
    garnet_api::{GarnetApi, is_acl_command},
    info_provider::{SessionInfoSource, info_scan_section},
    parser::resp_command::{MruCommandCache, is_allowed_in_subscription_mode},
    slow_path::SlowWait,
  },
  servers::consumer_registry::write_client_info_fields,
  traits::PeerSource,
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
/// 平铺 Vec 承载 output，初始构造回归此水位；与接收水位同为 64KB 但语义独立，C# 两域各为独立配置，禁止混用）
pub(super) const DEFAULT_OUTPUT_BUFFER_CAPACITY: usize = 1 << 16;

/// 批内输出水位（在 garnet 中的相对路径:libs/common/NetworkBufferSettings.cs:NetworkBufferSettings
/// ——默认 sendBufferSize = 1 << 17，即 C# 响应缓冲上界）。批内累计应答达此界
/// 即在命令边界停住交泵实写再续消费：RespWriteUtils TryWrite 写不下即
/// SendAndReset 满刷循环（libs/server/Resp/RespServerSession.cs:SendAndReset）
/// 的 rust 投影，粒度为命令边界而非写点（RespWriter 借用 output，写点复查
/// 在借用模型下不可行）
const OUTPUT_WATERMARK_BYTES: usize = 1 << 17;

/// TIME 应答 ns→秒/微秒换算刻度（编译期单源，杜绝 1e9 双字面量）
const NANOS_PER_SEC: u64 = 1_000_000_000;

/// TIME 应答微秒段定宽位数（C# utcTime.ToString("ffffff") 补零宽度）
const MICROS_DIGITS: usize = 6;

/// RESP2 协议版本标记（订阅模式放行门判据；对位 C# respProtocolVersion == 2）
const RESP2: u8 = 2;

/// 冷上下文挂起待物化面（严格会话 `set_context` 报告映射未装载时的暂存载荷：
/// 目标 (ns, db) 与认证 / HELLO 元数据落位载荷。会话标量在装载确认前严禁
/// 提前覆写——对标 C# `TryGetOrSetDatabaseSession` 的 success 门（只有底层
/// 就绪才 `SwitchActiveDatabaseSession`），装载失败即弃本载荷，旧标量原样）
pub(super) struct ColdContextPending {
  /// 目标命名空间
  pub(super) ns: u64,
  /// 目标库（纯切库臂物化为 `active_db_id`；认证臂切租不改库）
  pub(super) db: u64,
  /// 认证落位载荷（None = 纯切库 SELECT 臂）
  pub(super) auth: Option<ColdAuthCommit>,
  /// HELLO 元数据落位载荷（协议版本 + 客户端名；随认证臂挂起一并暂存）
  pub(super) hello: Option<ColdHelloCommit>,
}

/// 认证落位暂存载荷（句柄 + 读前采样的挂载代数 + 来源标记）
pub(super) struct ColdAuthCommit {
  pub(super) user_handle: Arc<UserHandle>,
  pub(super) generation: Option<u64>,
  pub(super) from_store: bool,
}

/// HELLO 元数据暂存载荷（冷租户认证挂起未确认前严禁单向脏写的会话元数据）
pub(super) struct ColdHelloCommit {
  pub(super) resp_protocol_version: Option<u8>,
  pub(super) client_name: Option<String>,
}

/// MSETNX 慢路径承接模式（快路径降级时置位，exec 降级快照尾参承载，
/// 慢路径消费后复位；尾参编码见 [`MsetnxResume::tail_byte`]）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MsetnxResume {
  /// 无降级 / 判定段降级：慢路径全量重判（三域存活裁决 + 写入一体重放）
  #[default]
  Replay,
  /// 写入段异步闭环信号降级（环形页翻转）：NX 判定已整体通过、已写键
  /// 保持，慢路径跳过判定续写全部键值回 :1（upsert 同值幂等）
  Continue,
  /// 回滚存在删除降级残留（回滚删除遇环形页翻转被记录）：慢路径持窗
  /// 条件回滚收尾——仅删内容即本命令所写的键（禁盲删吞并发已确认写，
  /// 票 zcode-r37-lockfix 发现 B）后回 :0
  Rollback,
}

impl MsetnxResume {
  /// 快照尾参编码（b"0"/b"1"/b"r"；沿 MSETNX resume 尾参先例的单字节
  /// 模式标记，慢路径臂 [`MsetnxResume::from_tail`] 逆解析）
  pub fn tail_byte(self) -> u8 {
    match self {
      Self::Replay => b'0',
      Self::Continue => b'1',
      Self::Rollback => b'r',
    }
  }

  /// 快照尾参逆解析（未知字节按 Replay 兜底：判定段全量重判语义最保守）
  pub fn from_tail(tail: Option<&[u8]>) -> Self {
    match tail {
      Some(b"1") => Self::Continue,
      Some(b"r") => Self::Rollback,
      _ => Self::Replay,
    }
  }
}

/// RESTORE / SET 条件写族慢路径承接标记（票 wnode-nx-conditional-ttl-degrade-replay-selfhit：
/// 快臂「值已同步提交」后 put_ttl_sync 遭环形页翻转降级时置 Pending，exec 降级快照
/// 尾参承载，慢臂消费后跳过整命令重放、仅窗内补投 TTL 出成功帧）。
/// 杜绝重放自碰本命令已提交值——RESTORE 误回 BUSYKEY、SET NX 误回 nil、
/// SET KEEPTTL 丢 TTL 三形；对标 C# 值与过期内嵌单次 CAS 原子一体落库、
/// 无「值已落、过期未落」中间态（KeyAdminCommands.cs:105/:109、BasicCommands.cs:786）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TtlResume {
  /// 无「值已提交」降级（未降级 / 提交前降级）：慢路径整命令全量重放（既有语义）
  #[default]
  Full,
  /// 值已同步提交、TTL 待投（绝对过期刻度随尾参携带，KEEPTTL 形为回填的旧值刻度）
  Pending(i64),
  /// 快臂已读旧 TTL、值写遇页翻转降级：慢路径重放写值并按此刻度回填 TTL，
  /// 杜绝重读已被快臂 TTL 腿清退的墓碑致静默丢 TTL
  KeepTtl(i64),
  /// GET 回旧值形值已同步提交、应答已由快臂成帧保留（旧值已覆写不可复原，
  /// 禁整命令重放）：慢臂持窗仅补投 TTL、不出任何帧（票 zcode-r153c-setrangeget
  /// 案一，杜绝重放自碰已提交值把新值冒充旧值回显 / NX 反判 / KEEPTTL 静默丢）
  ReplyEcho(i64),
}

impl TtlResume {
  /// 尾参线长单源（1 模式字节 + 8 字节 LE 过期刻度；编码/逆解析共用，杜绝 9 双字面量）
  const TAIL_LEN: usize = 1 + 8;

  /// 快照尾参编码（9 字节：模式字节 b'0'/b'1'/b'2'/b'3' + 8 字节 LE 过期刻度；沿 MSETNX
  /// 单字节模式标记与 DEL 计数 8 字节 LE 尾参同款先例）
  pub fn tail_bytes(self) -> Vec<u8> {
    let (mode, ticks) = match self {
      Self::Full => (b'0', 0i64),
      Self::Pending(ticks) => (b'1', ticks),
      Self::KeepTtl(ticks) => (b'2', ticks),
      Self::ReplyEcho(ticks) => (b'3', ticks),
    };
    let mut tail = Vec::with_capacity(Self::TAIL_LEN);
    tail.push(mode);
    tail.extend_from_slice(&ticks.to_le_bytes());
    tail
  }

  /// 快照尾参逆解析（形态不符按 Full 兜底：全量重放语义最保守）
  pub fn from_tail(tail: Option<&[u8]>) -> Self {
    match tail {
      Some(t) if t.len() == Self::TAIL_LEN => {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&t[1..]);
        let ticks = i64::from_le_bytes(buf);
        match t[0] {
          b'1' => Self::Pending(ticks),
          b'2' => Self::KeepTtl(ticks),
          b'3' => Self::ReplyEcho(ticks),
          _ => Self::Full,
        }
      }
      _ => Self::Full,
    }
  }
}

/// ETag 写族慢路径承接标记（票 zcode-r139c-etag2 案一情形 B：快臂「值腿已同步
/// 提交」后 TTL / etag 余腿遭环形页翻转降级时置 Pending，exec 降级快照尾参承载，
/// 慢臂消费后持窗补投余腿出成功帧）。
///
/// 杜绝整命令重放自碰已提交值——[`TtlResume`] 只携 TTL 刻度，ETag
/// 族的成功帧还须携快臂已裁决的新 etag（Missing / WrongType 臂重放后读态翻成
/// Hit，条件重判即失配回 `[0, 新值]`、etag 侧写永缺），故本族尾参在
/// 同一 pending_slow 通道上多带一枚 etag（MSETNX 1 字节 / DEL 8 字节 /
/// HSCAN 4 字节 / VADD 2 字节同款「每族自带尾参」形态，不新建通道）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EtagResume {
  /// 值腿未提交（RI 门磁盘候选 / upsert 页翻转）或无降级：慢路径整命令全量重放
  #[default]
  Full,
  /// 值腿已同步提交、余腿待投：`new_etag` 为快臂已裁决的 etag 侧写，
  /// `ticks` 非零即 TTL 待投绝对过期刻度（0 = TTL 腿已闭环，仅 etag 待投）
  Pending { ticks: i64, new_etag: i64 },
}

impl EtagResume {
  /// 尾参线长单源（1 模式字节 + 8 字节 LE 新 etag + 8 字节 LE 过期刻度）
  const TAIL_LEN: usize = 1 + 8 + 8;
  /// etag 段右界（模式字节后 8 字节）
  const ETAG_END: usize = 1 + 8;

  /// 快照尾参编码（17 字节：模式字节 b'0'/b'1' + 8 字节 LE 新 etag + 8 字节 LE
  /// 过期刻度；[`TtlResume::tail_bytes`] 同款线形，多带 etag 一枚）
  pub fn tail_bytes(self) -> Vec<u8> {
    let (mode, new_etag, ticks) = match self {
      Self::Full => (b'0', 0i64, 0i64),
      Self::Pending { ticks, new_etag } => (b'1', new_etag, ticks),
    };
    let mut tail = Vec::with_capacity(Self::TAIL_LEN);
    tail.push(mode);
    tail.extend_from_slice(&new_etag.to_le_bytes());
    tail.extend_from_slice(&ticks.to_le_bytes());
    tail
  }

  /// 快照尾参逆解析（形态不符按 Full 兜底：全量重放语义最保守）
  pub fn from_tail(tail: Option<&[u8]>) -> Self {
    match tail {
      Some(t) if t.len() == Self::TAIL_LEN && t[0] == b'1' => {
        let mut etag_buf = [0u8; 8];
        etag_buf.copy_from_slice(&t[1..Self::ETAG_END]);
        let mut ticks_buf = [0u8; 8];
        ticks_buf.copy_from_slice(&t[Self::ETAG_END..]);
        Self::Pending {
          ticks: i64::from_le_bytes(ticks_buf),
          new_etag: i64::from_le_bytes(etag_buf),
        }
      }
      _ => Self::Full,
    }
  }
}

impl ColdContextPending {
  /// 挂起载荷物化单点：SlowWait 成功应答回写时执行，会话标量就此与底层
  /// StoreSession 物理域对齐（物理域切换已在装载 future 内经 `set_context`
  /// 重放完成；本函数只落外层镜像，杜绝双写实现）
  pub(super) fn materialize_into(self, s: &mut RespServerSession) {
    match self.auth {
      Some(commit) => s.materialize_authenticated_handle(
        commit.user_handle,
        self.ns,
        commit.generation,
        commit.from_store,
      ),
      // 冷库切库成功提交点：与热库臂同经 watch 作废单点（C# Switch 换任面
      // 只在确认就绪后发生，r133c-selectdb 案二；auth 换租分支不物化库标量，
      // 无切库面不触）
      None => {
        s.invalidate_watch_on_db_switch(self.db);
        s.active_db_id = self.db;
      }
    }
    if let Some(hello) = self.hello {
      if let Some(version) = hello.resp_protocol_version {
        s.update_resp_protocol_version(version);
      }
      if let Some(name) = hello.client_name {
        s.set_client_name(Some(&name));
      }
    }
  }
}

/// libs/server/Resp/RespServerSession.cs:RespServerSession
///
/// RESP 服务器会话
pub struct RespServerSession {
  /// 会话标识（CLIENT ID / CLIENT INFO 使用；C# Id）
  pub id: i64,
  /// 会话创建时刻（毫秒 tick；C# CreationTicks = Environment.单调毫秒）
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
  pub(crate) pending_rearm: bool,

  /// ACL 挂载刷新停车标志（鉴权预门判挂载陈旧时置位）：与 pending_rearm
  /// 同为重驱型，泵经执行域刷新臂 await 点查（不产应答）后回退游标重解析
  /// 本命令，门链以新挂载重评
  pub(crate) pending_acl_refresh: bool,

  /// AUTH / HELLO / ACL 族命令停车槽（产应答型）：分派漏斗预筛命中即
  /// 快照 (命令, 参数, 停车时输出水位) 登记，本命令游标已推进（与
  /// pending_slow 同形），泵经执行域 [`crate::resp::garnet_api::GarnetApiFace::exec_auth_acl`]
  /// 异步臂 await 闭环后把应答按流水线顺序冲出；参数快照脱离接收缓冲
  /// 生命周期（await 期间缓冲可被复用，与 SlowWait::for_command 同口径）；
  /// 水位 = 停车登记时会话 output 长度（= 本命令应答段起点，同步段扫描
  /// 块在登记时刻应答尚未组装、判据恒假，失败计数由泵侧闭环单点
  /// [`RespServerSession::account_parked_auth_acl_failure`] 按同判据补计）
  pub(crate) pending_auth_acl: Option<(RespCommand, Vec<Vec<u8>>, usize)>,

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
      // 违例留痕日志（对标 C# catch 臂 RespServerSession.cs:525
      // `logger?.Log(ex.LogLevel, ex, "Aborting open session due to RESP parsing error")`，
      // ex.LogLevel 默认 Critical（RespParsingException.cs:20）——仓内无 critical 宏，
      // 取最高档 error；文案主干逐字对拍，携违例明细与 remote_endpoint 上下文
      // 对位 C# logger 随附异常对象形态；不动 take/帧写/返回时序）
      log::error!(
        "Aborting open session due to RESP parsing error: {msg} (remote={})",
        self.remote_endpoint
      );
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
    // 事务在途（C# IsSkippingOperations / `if (txnSkip) return 0` 对偶
    // 语义）：排队字节与 txn_start_head 偏移必须驻留缓冲供 EXEC 回退重解析，
    // 禁止清零复位与残余平移。收口门并核会话镜像与事务管理器状态
    //（TransactionManager::is_skipping_operations：镜像失步窗口下 Manager 侧
    // Started/Aborted 同样拦截平移）；批内输出水位让渡批不平移——泵实写本
    // 轮应答后立即重入消费（不经网络读取段），残余完整帧随后续批收口
    if self.txn_state == TxnState::None
      && !self
        .txn_manager
        .as_ref()
        .is_some_and(TransactionManager::is_skipping_operations)
      && !self.output_watermark_yield
    {
      if self.read_head >= self.bytes_read {
        // 整段消费完毕：缓冲清零复位（C# ShiftTransportReceiveBuffer 的
        // bytesLeft == 0 形态）；超大批次容量释放，回归默认驻留水位
        if self.recv_buffer.capacity() > DEFAULT_RECV_BUFFER_CAPACITY {
          self.recv_buffer = Vec::with_capacity(DEFAULT_RECV_BUFFER_CAPACITY);
          // 换缓冲实例：初始化代际一并失效（新分配禁复用旧代际）
          self.recv_prime_key = None;
        } else {
          self.recv_buffer.clear();
        }
        self.bytes_read = 0;
        self.read_head = 0;
        self.end_read_head = 0;
      } else if self.read_head > 0 {
        // 半包残余平移收口（C# NetworkHandler.cs:ShiftTransportReceiveBuffer：
        // bytesLeft != transportBytesRead 时残余拷贝至头部、
        // transportBytesRead = bytesLeft、transportReadHead = 0——C# 网络层
        // 每批 Process 后在 Rest 态执行的平移，rust 收口单点搬入会话层）。
        // 缺失此收口时已消费前缀单调驻留，长连接下缓冲无界扩容
        let remaining = self.bytes_read - self.read_head;
        self
          .recv_buffer
          .copy_within(self.read_head..self.bytes_read, 0);
        self.recv_buffer.truncate(remaining);
        self.bytes_read = remaining;
        self.read_head = 0;
        self.end_read_head = 0;
        // 超大批次后的容量收敛（C# ShrinkNetworkReceiveBuffer 的对偶：
        // 平移后残余回落默认水位内即收缩，防大容量常驻）
        if self.recv_buffer.capacity() > DEFAULT_RECV_BUFFER_CAPACITY
          && remaining <= DEFAULT_RECV_BUFFER_CAPACITY
        {
          self.recv_buffer.shrink_to(DEFAULT_RECV_BUFFER_CAPACITY);
        }
      }
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
    // 挂起中的阻塞/慢路径/脚本命令未完成前不再消费新命令（C# 网络线程
    // BlockingWait 期间本就读不到后续命令）
    if self.pending_block.is_some() || self.pending_slow.is_some() || self.script_suspend.is_some()
    {
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

      // 排队期入队失败统一标记（未知命令/未知子命令、ACL 拒绝、脚本拒绝
      // 三处臂置位），分派链后单点收口中止
      let mut queue_failure = false;
      if cmd != RespCommand::Invalid {
        let orig_output_len = self.output.len();
        // 冷上下文挂起面按命令窗口即抛：只允许本命令的应答组装点消费
        self.cold_ctx = None;
        // C# 门链（RespServerSession.cs:651-653）：noScriptPassed 默认 true，
        // ACL 失败短路（&&）不再查 no-script；no-script 失败回 NOSCRIPT
        //（C# :710），不落 NOPERM/NOAUTH。挂载陈旧须点查刷新时门停车
        //（Parked），游标回退本命令起点，泵刷新后重驱重评
        let acl_permitted = match self.check_acl_permissions(cmd) {
          AclGateVerdict::Permitted => true,
          AclGateVerdict::Denied => false,
          AclGateVerdict::Parked => {
            self.read_head = orig_read_head;
            self.end_read_head = orig_read_head;
            break;
          }
        };
        let mut script_permitted = true;
        if acl_permitted {
          script_permitted = self.check_script_permissions(cmd);
        }
        if acl_permitted && script_permitted {
          // RESP2 订阅模式仅放行 (P|S)SUBSCRIBE/(P|S)UNSUBSCRIBE/PING/QUIT 与
          // rust 补全的 SUNSUBSCRIBE（无 RESET；允许集单点见
          // is_allowed_in_subscription_mode，C# 对位 RespCommand.cs:733-742）
          if self.is_subscription_session
            && self.resp_protocol_version == RESP2
            && !is_allowed_in_subscription_mode(cmd)
          {
            // 对标 libs/server/Resp/RespServerSession.cs:659（string.Format(
            // CmdStrings.GenericPubSubCommandNotAllowed, cmd.ToString())）
            let name = cmd.to_cs_name();
            self.abort_error_message(&cs::GENERIC_PUBSUB_COMMAND_NOT_ALLOWED.replace("{0}", name));
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
          // 不漏计（与 C# 的差异经本注记登记）。停车臂（AUTH/HELLO/ACL 族）
          // 应答在本扫描块之后才于泵侧 await 域组装，本块只计 calls、失败
          // 判据恒假；failed 经泵侧闭环单点 account_parked_auth_acl_failure
          // 按同一判据补计（见其注释），族内绝无双轨双计。
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
          // 事务排队期权限拒绝同步中止：收口统一中止臂（下方 queue_failure 臂）
          queue_failure = true;
          // libs/server/Resp/RespServerSession.cs:715（ACL/脚本权限拒绝计数）
          if let Some(stats) = &self.command_stats {
            stats.lock().increment_rejected(cmd);
          }
        } else {
          // C# :708-712 else 分支：NOSCRIPT（C# :715 同计拒绝数）
          self.abort_error_message(cs::RESP_ERR_NOSCRIPT);
          // 事务排队期脚本权限拒绝同步中止：收口统一中止臂
          queue_failure = true;
          if let Some(stats) = &self.command_stats {
            stats.lock().increment_rejected(cmd);
          }
        }
      } else {
        // 未知命令/未知子命令：错误帧已由解析器落线（C# writeErrorOnFailure）。
        // 事务排队期中止收口统一中止臂（下方 queue_failure 臂）
        queue_failure = true;
        self.contains_slow_command = true;
      }

      // 事务排队期入队失败统一中止臂（票面 1/2 收口，对标 C#
      // TransactionManager.Abort + NetworkEXEC 的 Aborted→EXECABORT 链）：
      // 未知命令/未知子命令（Invalid）、ACL 拒绝（NOPERM/NOAUTH）、脚本拒绝
      //（NOSCRIPT）三处入队失败臂单点收口——错误帧均已由各自臂落线，此处
      // 只置事务中止，不补写应答、不重解析参数。排空回环：解析器装载循环
      // 对 Invalid 返回同样推进 end_read_head 至整帧命令末尾（对标 C#
      // ReadCommandAndReconcileArguments 消费全 token 后返回 INVALID），
      // 按循环尾同一形态把游标推进到 end_read_head 完成整帧消费，不依赖
      // 事务分支替臂补做消费。仅排队窗生效（门在 abort_pending_transaction
      // 内部：None 不处理、Running 重放态不撕裂应答数组）
      if queue_failure {
        self.read_head = self.end_read_head;
        self.abort_pending_transaction();
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
      // C# :728 `if (slowLogThreshold > 0) HandleSlowLog(cmd)`：阈值批入口缓存
      // 门控，慢日志禁用时不进函数——零配置读、零时钟取、零参数序列化
      if self.slow_log_threshold > 0 {
        self.handle_slow_log(cmd);
      }
      if let Some(metrics) = &self.session_metrics {
        metrics.incr_total_commands_processed(1);
        metrics.add_total_write_commands_processed(one_if_write(cmd));
        metrics.add_total_read_commands_processed(one_if_read(cmd));
      }

      if self.session_asking != 0 {
        self.session_asking -= 1;
      }

      // 阻塞命令 / 慢路径命令 / 脚本挂起 / AUTH·HELLO·ACL 族停车：停止消费本批后续
      // 命令（C# 网络线程阻塞等价物，后续命令由网络泵驱动等待完成后继续
      // 消费）
      //
      // 【事务窗禁停泊对称加固】：冷租户装载与 AUTH/ACL 停泊唯一合法窗口为普通命令窗口（txn_state == None）。
      // 事务窗内（Started/Running/Aborted）严禁认证与冷租户装载停泊，防跨租户重入撕裂。
      if self.pending_block.is_some()
        || self.pending_slow.is_some()
        || self.pending_auth_acl.is_some()
        || self.script_suspend.is_some()
      {
        if self.pending_auth_acl.is_some()
          || (self.pending_slow.is_some() && self.cold_ctx.is_some())
        {
          debug_assert_eq!(
            self.txn_state,
            TxnState::None,
            "唯一合法停泊窗为普通命令窗口，事务窗内严禁冷租户认证与 ACL 停泊"
          );
        }
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
  /// 慢命令族分派（此处可安全放 @slow 命令）。C# ProcessOtherCommands :1065
  /// 入口 `containsSlowCommand = true` 的结构对位：会话侧闭环臂
  ///（[`Self::process_other_session_commands`]）命中即在本单点置位后返回，
  /// 新增会话臂落入伞内自动覆盖，杜绝逐臂补丁漏标；存储命令穿透尾部
  /// [`Self::dispatch_via_garnet_api`]（fast 段直通不置位，slow 段由
  /// `dispatch_slow` 入口既有置位承接——布尔写幂等，与 C# Other/Admin 两点
  /// 入口同义），段位判定与 C# 三段 switch 等效
  pub fn process_other_commands(&mut self, cmd: RespCommand) -> bool {
    if let Some(handled) = self.process_other_session_commands(cmd) {
      // 批延迟切 NET_RS_LAT_ADMIN 桶（latency_batch_stop 批出口消费复位）
      self.contains_slow_command = true;
      return handled;
    }
    // 自定义对象命令（Customobjcmd）经 dispatch_via_garnet_api 的存储执行域承接；
    // C# CustomTxn / CustomRawStringCmd / CustomProcedure 三族随动态注册层删除，
    // 解析器不再产出这些枚举，事务过程入口单点收敛到 RUNTXP
    let store = self.collect_args_store();
    let args = store.views();
    self.dispatch_via_garnet_api(cmd, &args);
    true
  }

  /// 会话侧闭环臂（C# ProcessOtherCommands switch 的会话 partial 段 +
  /// ProcessAdminCommands 的无存储面子集）；`None` = 存储命令（含 INFO
  /// 扫描族段降级放行），放行 [`Self::process_other_commands`] 尾部存储分派。
  /// 派发单点 `match`，臂序严格等同原顺序 if 链（判序/流水线语义不变）；
  /// 物化参写出骨架收敛 [`Self::args_via_local`]，Echo/Async 借用形态
  /// 合不进（见其文档注）
  fn process_other_session_commands(&mut self, cmd: RespCommand) -> Option<bool> {
    let handled = match cmd {
      // Lua 脚本族（C# NetworkEVAL / NetworkEVALSHA / NetworkScript*）
      RespCommand::Eval
      | RespCommand::Evalsha
      | RespCommand::ScriptExists
      | RespCommand::ScriptFlush
      | RespCommand::ScriptLoad => Some(self.run_lua_command(cmd)),
      RespCommand::ClientId if self.parse_state.count != 0 => {
        // C# NetworkCLIENTID 的参数校验在会话侧（AbortWithWrongNumberOfArguments）
        self.abort_wrong_num_args("client|id");
        Some(true)
      }
      RespCommand::ClientId => {
        // C# TryWriteInt64(Id)，走 wresp 整数帧单点
        self.output.write_resp_int(self.id);
        Some(true)
      }
      RespCommand::Echo => {
        // C# RespServerSession.cs:1089：ECHO→NetworkECHO
        let args = collect_arg_views(&self.parse_state, &self.recv_buffer);
        let mut output = mem::take(&mut self.output);
        let r = self.network_echo(&args, &mut output);
        self.output = output;
        Some(matches!(r, Ok(true)))
      }
      RespCommand::Time => {
        // 在 garnet 中的相对路径:libs/server/Resp/BasicCommands.cs:NetworkTIME
        if self.parse_state.count != 0 {
          self.abort_wrong_num_args("TIME");
          return Some(true);
        }
        let now_nanos = now_nanos();
        let secs = now_nanos / NANOS_PER_SEC;
        let usecs = (now_nanos % NANOS_PER_SEC) / 1_000;
        let mut b1 = Buffer::new();
        let s_str = b1.format(secs);
        // C# utcTime.ToString("ffffff")：秒内小数截断至微秒，串恒 6 位零填充、
        // $ 头恒 6。usecs ≤ 999999 恒成立，借 itoa 单点后定长左补零，无运行期格式串
        let mut b2 = Buffer::new();
        let us_str = b2.format(usecs);
        let mut us_buf = [b'0'; MICROS_DIGITS];
        us_buf[MICROS_DIGITS - us_str.len()..].copy_from_slice(us_str.as_bytes());
        self.output.write_resp_array_len(2);
        self.output.write_resp_bulk_string(s_str.as_bytes());
        self.output.write_resp_bulk_string(&us_buf);
        Some(true)
      }
      RespCommand::Async => {
        // C# NetworkASYNC：委托 basic_commands 单一定义（参数校验与降级回包全在彼处）
        let args = collect_arg_views(&self.parse_state, &self.recv_buffer);
        let mut output = mem::take(&mut self.output);
        let r = self.apply_async_param(&args, &mut output);
        self.output = output;
        Some(matches!(r, Ok(true)))
      }
      // CLIENT 族（ctx 告警文案逐字保留）
      RespCommand::ClientInfo => {
        Some(self.args_via_local("client info error", Self::network_clientinfo))
      }
      RespCommand::ClientList => {
        Some(self.write_via_local("client list error", |s, out| s.network_clientlist(out)))
      }
      RespCommand::ClientKill => {
        Some(self.write_via_local("client kill error", |s, out| s.network_clientkill(out)))
      }
      RespCommand::ClientGetname => {
        Some(self.args_via_local("client getname error", Self::network_clientgetname))
      }
      RespCommand::ClientSetname => {
        Some(self.args_via_local("client setname error", Self::network_clientsetname))
      }
      RespCommand::ClientSetinfo => {
        Some(self.args_via_local("client setinfo error", Self::network_clientsetinfo))
      }
      RespCommand::ClientUnblock => {
        Some(self.args_via_local("client unblock error", Self::network_clientunblock))
      }
      // C# 分派：FAILOVER/REPLICAOF/SECONDARYOF 显式路由 + IsClusterSubCommand
      // 区间整体经 NetworkProcessClusterCommand；切面未挂时报集群支持未启用
      _ if cmd == RespCommand::Cluster
        || is_cluster_sub_command(cmd)
        || matches!(
          cmd,
          RespCommand::Failover
            | RespCommand::Replicaof
            | RespCommand::Migrate
            | RespCommand::Secondaryof
        ) =>
      {
        Some(self.write_via_local("cluster command error", |s, out| {
          s.network_process_cluster_command(cmd, out)
        }))
      }
      RespCommand::Role => Some(self.args_via_local("role error", Self::network_role)),
      // C# NetworkCOMMAND（COMMAND 根命令）：带参报未知子命令，无参列全部命令
      RespCommand::Command => {
        Some(self.write_via_local("command error", |s, out| s.network_command_root(out)))
      }
      _ => None,
    };
    if handled.is_some() {
      return handled;
    }
    // C# NetworkAUTH（ProcessOtherCommands 段）与 ACL 族不在此分派：二者须
    // 点查底层存储（ACL 为唯一真源），由存储执行域 `StoreGarnetApi::exec`
    // 在批处理纪元保护区外统一承接（冷记录落盘回读 + 会话本地句柄回写）

    // LATENCY / SLOWLOG 族（C# Metrics/Latency、Metrics/Slowlog 的会话 partial）
    if let Some(handled) = self.process_metrics_commands(cmd) {
      return Some(handled);
    }
    // MONITOR / DEBUG / SAVE 族（C# ProcessAdminCommands）
    if let Some(handled) = self.process_admin_session_commands(cmd) {
      return Some(handled);
    }
    if cmd == RespCommand::Info {
      // libs/server/Resp/RespServerSession.cs:ProcessOtherCommands 的 INFO 段
      //（wmetric 段分发：段解析 + 各信息域填充经 SessionInfoSource 数据源
      // 承接）。扫描族段（KEYSPACE 全库扫描计数 / HLOGSCAN 混合日志分布
      // 扫描 / STOREHASHTABLE 哈希分布诊断扫描 / STOREREVIV 复活统计转储，
      // C# PopulateKeyspaceInfo → GetKeyspaceStats 专用扫描会话、
      // PopulateHlogScanInfo → HybridLogDistributionScan、
      // PopulateStoreHashDistribution → DumpDistribution、
      // PopulateStoreRevivInfo → DumpRevivificationStats 均为逐段实填，
      // DEFAULT/ALL 段集合不含这些段）——rust 存储域扫描须跨 await，与
      // DBSIZE 同构降级慢路径（garnet_api 漏斗挂 SlowWait，扫描行与非扫描
      // 面两路合成 InfoSlowSource 全段集渲染）。降级门为 any 语义：解析后
      // 段集（ALL/DEFAULT 关键字已展开、去重）含任一扫描族段即整请求放行
      // 分派漏斗，混合段请求（如 INFO server keyspace）不再以同步面
      // (0,0) 假计数渲染 KEYSPACE；RESET / HELP / 非法段在 C# 先于任何
      // 段填充短路且从不触达扫描数据，一律留在同步面呈现（段解析单点复用
      // wmetric InfoCommand::parse_sections，与渲染面零分叉）
      let args = collect_arg_views(&self.parse_state, &self.recv_buffer);
      let parsed = InfoCommand::parse_sections(&args);
      if parsed.invalid.is_none()
        && !parsed.reset
        && !parsed.help
        && parsed.sections.iter().any(|s| info_scan_section(*s))
      {
        // 放行到存储分派（C# ProcessOtherCommands 末端 ProcessAdminCommands
        // 形态），由存储执行域承接；执行域未挂载时经 dispatch_via_garnet_api
        // 显式拒绝，绝不静默吞命令
        return None;
      }
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
          self.resp_protocol_version,
          &mut out,
        );
        out
      };
      self.output.extend_from_slice(&text);
      return Some(true);
    }
    // 存储命令放行尾部存储分派（fast 直通，slow 由 dispatch_slow 入口置位）
    None
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

  /// CLIENT/ROLE 族公共骨架：物化参单分配 → [`Self::write_via_local`] → 并回
  /// 输出，臂只余「ctx 文案 + 方法名」。Echo/Async/PING 借用形态不可并入：
  /// 参数视图持有 `recv_buffer` 借用期间，helper 无法再取 `&mut self`
  /// 实参（字段拆分借用正是 `collect_arg_views` 的显式传参契约），
  /// 故保留各处显式 take/restore
  fn args_via_local(
    &mut self,
    ctx: &'static str,
    f: impl FnOnce(&mut Self, &[&[u8]], &mut Vec<u8>) -> wresp::Result<bool>,
  ) -> bool {
    let store = self.collect_args_store();
    self.write_via_local(ctx, |s, out| f(s, &store.views(), out))
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

  /// 经注入的存储执行域分派（C# 对应 Process 链末端 ProcessAdminCommands
  /// 的兜底形态；先克隆 Arc 再调用，解 &self.garnet_api 与 &mut session
  /// 的借用相交，单次原子递增对比命令执行开销不可见）
  ///
  /// AUTH / HELLO / ACL 族预筛停车：存储点查（认证、规则读写、串行锁）
  /// 严禁同步收割，登记 (命令, 参数快照) 交网络泵经执行域 exec_auth_acl
  /// 异步臂闭环；本批消费到此为止（命令游标已推进、应答由泵按流水线顺序
  /// 冲出），后续命令待闭环后继续。事务窗内（EXEC 重放）按 §58d 一态收口：
  /// 携 AUTH 形 HELLO 与 ACL 族各回专属拒绝帧，无 AUTH 形 HELLO 走同步快臂
  /// 直出应答（详见臂内注）
  fn dispatch_via_garnet_api(&mut self, cmd: RespCommand, args: &[&[u8]]) {
    // 预筛以执行域已挂载为前提：未挂载即装配缺口，与余者同走显式拒绝。
    // 事务窗内严禁挂入 pending_auth_acl（普通命令窗口为唯一合法停泊窗）
    if (cmd == RespCommand::Auth || cmd == RespCommand::Hello || is_acl_command(cmd))
      && self.garnet_api.is_some()
    {
      // C# AUTH ∈ ProcessOtherCommands(:1065)、ACL 族 ∈ ProcessAdminCommands
      //（AdminCommands.cs:31）入口置位对位：停车闭环不经 dispatch_slow，
      // 本批延迟须切 NET_RS_LAT_ADMIN
      self.contains_slow_command = true;
      if self.txn_state != TxnState::None {
        // §58d 重放窗一态收口：文法合法且不携 AUTH 的 HELLO 形零存储点查、
        // 零停泊（协议回显/客户端名均会话元数据），经无存储同步快臂直出应答
        // map（对位 C# NetworkHELLO 无事务门正常执行、与 §58a「中止面恰等于
        // 携 AUTH 组」排队裁决面收口为一态）；携 AUTH 形与语法错形维持禁停泊
        // 围栏拒绝帧，ACL 族十子命令规则读写须点查停泊、回专属文案
        if cmd == RespCommand::Hello
          && let Ok(hello) = parse_hello_args(args)
          && hello.auth.is_none()
        {
          let mut out = mem::take(&mut self.output);
          self.commit_hello_state_and_write_reply(
            hello.protocol_version,
            hello.client_name,
            false,
            &mut out,
          );
          self.output = out;
          return;
        }
        let err = if cmd == RespCommand::Auth {
          RESP_ERR_AUTH_IN_MULTI
        } else if is_acl_command(cmd) {
          cs::RESP_ERR_ACL_IN_TXN_UNSUPPORTED
        } else {
          cs::RESP_ERR_HELLO_IN_TXN_UNSUPPORTED
        };
        cs::write_error_raw(&mut self.output, err);
        return;
      }
      self.pending_auth_acl = Some((
        cmd,
        args.iter().map(|a| a.to_vec()).collect::<Vec<_>>(),
        self.output.len(),
      ));
      return;
    }
    let Some(api) = self.garnet_api.clone() else {
      // 存储执行域未挂载 = 宿主装配缺口：写明错误并告警，绝不静默吞命令
      log::error!("存储执行域未挂载，命令 {cmd:?} 被拒绝");
      self.abort_error_message(ERR_STORE_DOMAIN_NOT_ATTACHED);
      return;
    };
    api.exec(self, cmd, args);
  }

  /// 取走 ACL 挂载刷新停车标志（网络泵专属：经执行域刷新臂 await 点查后
  /// 游标已回退，重入消费即重解析重评本命令；取走即复位）
  pub fn take_pending_acl_refresh(&mut self) -> bool {
    mem::take(&mut self.pending_acl_refresh)
  }

  /// 取走 AUTH / HELLO / ACL 族停车快照（网络泵专属：经执行域 exec_auth_acl
  /// 异步臂 await 闭环，应答直写会话输出缓冲后冲出；取走即复位）。伴随
  /// 输出水位 = 本命令应答段起点（供闭环后 [`Self::account_parked_auth_acl_failure`]
  /// 补计失败）
  pub fn take_pending_auth_acl(&mut self) -> Option<(RespCommand, Vec<Vec<u8>>, usize)> {
    self.pending_auth_acl.take()
  }

  /// 停车臂失败应答补计单点（网络泵专属：await 闭环完成后、冲出前调用）
  ///
  /// 同步段 CommandStats 门（RespServerSession.cs:683-689 对位）在本命令
  /// 同步迭代内扫描时，停车臂应答尚未组装、判据恒假（calls 已在同步段
  /// 计，此处只补 failed 防双计）；本口按与同步段同一判据（命令错误置位
  /// 或 `output[start_len..]` 应答段以 `-` 开头）补计 failed_calls 并复位
  /// 置位，消除 HELLO/AUTH/ACL 族失败样本在 INFO commandstats 的系统性
  /// 少计（C# 全错误帧经会话级 WriteError 统一置位、收尾门入账的对位收口）
  pub fn account_parked_auth_acl_failure(&mut self, cmd: RespCommand, start_len: usize) {
    if let Some(stats) = &self.command_stats {
      let mut stats = stats.lock();
      if self.command_error_written || self.output[start_len..].starts_with(b"-") {
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
  fn invalidate_watch_on_db_switch(&mut self, db_id: u64) {
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
