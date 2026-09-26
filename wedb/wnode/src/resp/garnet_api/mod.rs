//! 存储执行域命令分派面（会话核心 → 存储执行的注入点）
//!
//! 对标 libs/server/API/IGarnetApi.cs:IGarnetApi：C# 会话的分派链
//! ProcessBasicCommands → ProcessArrayCommands → ProcessOtherCommands 以
//! 泛型参数 `ref TGarnetApi storageApi` 贯穿存储 API，单机与集群同一
//! 执行路径（集群差异仅槽位门 CanServeSlot 在前）。rust 侧命令实现
//! （resp 命令文件）需要 [`wkv::BatchStoreSession`] 纪元守卫与
//! `D: Device` 泛型，无法驻留会话结构体，故以统一分派句柄
//! [`GarnetApi`]（`Arc<dyn GarnetApiFace>` trait 对象，对标 C# 直持
//! storageApi 引用的虚分派）注入承接：宿主构造会话后经
//! [`RespServerSession::set_garnet_api`] 注入，会话主循环在槽位门放行后
//! 经 [`GarnetApiFace::exec`] 进入存储执行域。

use wcol::itembroker::item_broker_face::ItemBrokerFinisher;
use wtxn::TxnState;

use crate::{aof::GarnetAppendOnlyFile, database::GarnetDatabase, resp::acl_store::AclStore};
mod objects;
mod raw;
mod slow;

use std::{
  future::Future,
  mem,
  pin::Pin,
  sync::{Arc, OnceLock, atomic::AtomicBool},
  task::{Context, Poll},
};

pub(crate) use objects::object_collect_all;
use wbase::{hash_slot::slot_of, map::HashMap};
use wconf::ServerConfigType;
use wdev::Device;
use wkv::{SessionLocking, StoreSession};
use wmetric::{DbSnapshot, InfoCommand, PendingLatencyMeter, SessionMetricsHandle};
use wresp::{
  cmd_strings::{RESP_ERR_ASYNC_REQUIRED, write_error_raw},
  command::{RespCommand, is_vector_set_command},
  metrics::InfoMetricsType,
};
use wval::SessionPrefixBuf;

use crate::{
  database::SingleDatabaseManager,
  resp::{
    EtagResume, MsetnxResume, RespServerSession, TtlResume,
    info_provider::{InfoScanResult, InfoSurface, SessionInfoSource, render_info_slow_reply},
    slow_path::{SlowFuture, SlowWait},
    vector::{
      resp_server_session_vectors::{RespServerSessionVectors, VectorGuardVerdict},
      vector_manager::VectorManager,
      vector_store_callbacks::ActiveVectorSessionGuard,
    },
  },
  storage::session::{
    common::ttl_sync::purge_expired_residue_sync, storage_session::vector_registry_delete_hook,
  },
};

/// 慢路径 poll 边界执行域绑定包装（对标 C# 命令线程 NetworkXxx 期间绑定 /
/// 解绑 `[ThreadStatic] ActiveThreadSession`：rust 慢路径在 [`SlowWait`] 的
/// 每次 poll 同步段内绑定属主连接会话，poll 返回即解绑，守卫绝不跨 poll
/// 存活——compio thread-per-core 下 poll 恒在属主任务线程，绑定/解绑同线程
/// 配对，同线程其他任务在两次 poll 间隙看不到本会话的绑定）
struct SlowPollSessionBound<F, D: Device> {
  /// 执行域宿主（仅取会话引用做绑定，不触达其余字段）
  api: Arc<StoreGarnetApi<D>>,
  /// 被包装的慢路径执行体（结构性 pin：只在 api 之后投影，永不 mov）
  inner: F,
}

impl<F: Future<Output = Vec<u8>>, D: Device> Future for SlowPollSessionBound<F, D> {
  type Output = Vec<u8>;

  #[inline]
  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    // SAFETY: 结构固定投影——api 为 `Arc`（恒 `Unpin`），本 poll 只以 `Pin`
    // 移交 `inner`，全程不移动任何字段，满足 `SlowFuture` 的类型擦除 pin 契约。
    let this = unsafe { self.get_unchecked_mut() };
    let _bound = ActiveVectorSessionGuard::bind(&this.api.session);
    // SAFETY: 同上，`inner` 一经交付即结构性 pin 于本地址
    unsafe { Pin::new_unchecked(&mut this.inner) }.poll(cx)
  }
}

/// SAVE / BGSAVE / LASTSAVE 检查点通道（对标 C# storeWrapper.databaseManager；
/// 服务器级共享常驻单例，由 StorageSessionProvider 装配期注入）
#[derive(Clone)]
pub struct CheckpointCtx<D: Device> {
  /// 逻辑数据库管理器（常驻单例，管理快照与 AOF）
  pub database_manager: Arc<SingleDatabaseManager<D>>,
}

impl<D: Device> CheckpointCtx<D> {
  pub fn new(database_manager: Arc<SingleDatabaseManager<D>>) -> Self {
    Self { database_manager }
  }
}

/// libs/server/API/IGarnetApi.cs:IGarnetApi
///
/// 存储执行域命令分派面（C# IGarnetApi 的 rust 注入投影）
///
/// C# 以泛型静态分发（`where TGarnetApi: IGarnetApi`）；rust 以
/// `Arc<dyn GarnetApiFace>` trait 对象消除 `D: Device` 泛型对会话/
/// 消费者层的传染（对标 C# 直持 storageApi 引用的虚分派）
/// ACL 用户记录点查应答（装箱 future 的输出复杂度收敛单点）
pub type AclRecordFut<'a> =
  Pin<Box<dyn Future<Output = Option<wkv::Result<Option<Vec<u8>>>>> + 'a>>;

pub trait GarnetApiFace: Send + Sync {
  /// libs/server/API/IGarnetApi.cs:IGarnetApi
  ///
  /// 执行一条命令并写回应答到会话输出缓冲（C# ProcessBasicCommands /
  /// ProcessArrayCommands / ProcessOtherCommands 的 switch 承接）。
  /// 未接入分派表的命令按 C# ProcessAdminCommands 尾部兜底写
  /// `ERR unknown command`
  fn exec(&self, session: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]);

  /// AUTH / ACL / HELLO 族命令臂（异步域独占）
  ///
  /// 认证与 ACL 的底层存储点查须在 async 上下文内联收割（冷记录降级为
  /// 异步落盘回读，严禁同步驱动重入 compio 调度器），且认证成功后须回写
  /// 会话本地句柄/命名空间，仅分派段可达（慢路径仅产出应答字节，无会话态
  /// 变更面）。存储为 ACL 唯一真源，见 doc/zh/db.md §3。调用方以
  /// `cmd == Auth || cmd == Hello || is_acl_command(cmd)` 预筛后才进入本臂
  ///（无存储执行域的测试替身走默认实现：恒 false，命令落 [`Self::exec`]
  /// 通用分派，与历史替身行为一致）。
  ///
  /// trait 对象（[`GarnetApi`]）下 async 方法不可 RPITIT，手工装箱：future
  /// 仅在分派调用点内联 `.await`，不跨线程移交
  fn exec_auth_acl<'a>(
    &'a self,
    session: &'a mut RespServerSession,
    cmd: RespCommand,
    args: &'a [&'a [u8]],
  ) -> Pin<Box<dyn Future<Output = bool> + 'a>> {
    let _ = (cmd, args);
    Box::pin(async move {
      let _ = session;
      false
    })
  }

  /// ACL 挂载陈旧的异步刷新臂（重驱型：不产应答，刷新完成后原命令游标
  /// 回退重解析，门链以新挂载重评）
  ///
  /// 跨连接改权收敛预门 [`RespServerSession::refresh_acl_mount_if_stale`]
  /// 的点查臂：挂载代数落后即按会话已绑 `(ns, 用户名)` 点查存储真源重建
  /// 句柄。点查须在批处理纪元保护区外 await（冷记录降级落盘回读），故
  /// 同步门链段只判停车、由泵内联 await 本臂。无存储执行域的测试替身走
  /// 默认实现：免刷新直过（会话挂载与引擎代数本就无源可陈旧）
  ///
  /// trait 对象下 async 方法不可 RPITIT，手工装箱：future 仅在分派调用点
  /// 内联 `.await`，不跨线程移交
  fn exec_acl_refresh<'a>(
    &'a self,
    session: &'a mut RespServerSession,
  ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
    Box::pin(async move {
      let _ = session;
    })
  }

  /// 慢路径异步执行（同步段返回 `Ok(false)` 的命令在异步域闭环）
  ///
  /// 产出该命令的完整应答字节（错误应答含在内），由 [`SlowWait`] 驱动。
  /// `self: Arc<Self>` 接收者：self move 进 future 保 `'static`，future 借用
  /// 线程本地执行域（wdev 设备段句柄为 TLS `Rc` 表，compio 任务线程绑定），
  /// 跨 [`SlowWait`] 的 Send 边界由其安全论证承担
  ///
  /// `resp_version` 为版本源单点流入：调度慢命令时快照 `RespServerSession`
  /// 真实协议版本随参数带入（exec_slow 无会话可达面，与命令名/COUNT 上限等
  /// 快照尾参同口径），供存储会话构造时经 [`StorageSession::with_resp_version`]
  /// 装配，对象族慢路径据此取正确帧型
  fn exec_slow(
    self: Arc<Self>,
    cmd: RespCommand,
    args: Vec<Vec<u8>>,
    resp_version: u8,
  ) -> SlowFuture;

  /// INFO 慢路径异步臂（凡段集含扫描族段的 INFO 请求整请求降级后的闭环）
  ///
  /// 对标 C# InfoCommand.cs:NetworkINFO → GarnetInfoMetrics.GetRespInfo 的
  /// 逐段实填：rust 扫描族段数据面在存储域且须跨 await，拆两路取数——
  /// 非扫描面由分派漏斗在调度点经 [`InfoSurface`] 同步快照（本臂无会话
  /// 可达面，与命令名 / resp_version / 快照尾参同渠道同口径），扫描行由
  /// 存储执行域覆写臂异步产出，两路合成组合数据源后交单源渲染出帧。
  ///
  /// 无存储执行域的测试替身走默认实现：扫描行空集形态渲染（KEYSPACE 无
  /// 行、HLOGSCAN 出 Empty，绝不虚报计数）
  fn exec_slow_info(
    self: Arc<Self>,
    sections: Vec<InfoMetricsType>,
    surface: InfoSurface,
    max_databases: u64,
    active_db: i32,
    resp_version: u8,
  ) -> SlowWait {
    // 默认臂无存储扫描面：self 不参与渲染（扫描行空集形态）、库数上限
    // 无消费点，即时释放句柄不搬进 future（'static 约束免增）
    drop(self);
    let _ = max_databases;
    SlowWait::new(async move {
      render_info_slow_reply(
        &sections,
        &InfoScanResult::default(),
        surface,
        active_db,
        resp_version,
      )
    })
  }

  /// 切换当前会话底层存储上下文（命名空间 + 数据库）
  ///
  /// 返回是否完成上下文物化：严格会话映射未装载（冷租户/冷库）时返回
  /// false，由调用方挂起 [`wkv::WedbStore::resolve_context`] 点查装载后重放
  fn set_context(&self, ns: u64, db: u64) -> bool {
    let _ = (ns, db);
    true
  }

  /// 执行域存储会话的**物理**归属前缀（`[NsVarint][DbVarint]`，锁轨种子与
  /// 一切物理寻址的真值源单点）
  ///
  /// 双轨声明（分置两单点、禁共口互染）：锁轨=物理域——事务锁登记与本前缀
  /// 同源（`StoreSession::session_prefix` 投影，含 FLUSHDB 换号态），与桶闩
  /// 所在现域一致；版本轨=逻辑域——WATCH 分槽与写面推进改取
  /// [`Self::session_logical_prefix`]（对位 C# 每库版本表实例终身持有，
  /// libs/server/GarnetDatabase.cs:156）。缺省根域 = 嵌入式/无存储执行域形态
  /// 的真实身份（wkv 未绑定会话实前缀即 (0,0) 恒等），非兜底假值
  fn session_prefix(&self) -> SessionPrefixBuf {
    SessionPrefixBuf::ROOT
  }

  /// 执行域存储会话的**逻辑**归属前缀（版本轨种子单点：WATCH 分槽与写面
  /// 推进共用）
  ///
  /// 直取 [`StoreSession::session_logical_prefix`](wkv::StoreSession::session_logical_prefix)
  /// （`namespace()`/`active_db()` 逻辑真值投影，不含换号虚拟代际），与 wkv
  /// 写面 `BatchStoreSession::bump_watch_version` 取值同源：同逻辑库在
  /// FLUSHDB/FLUSHNS/SWAPDB 换号前后的写 bump 与在途 WATCH 核验恒落同槽，
  /// 改后写必 abort。缺省根域口径同 [`Self::session_prefix`]
  fn session_logical_prefix(&self) -> SessionPrefixBuf {
    SessionPrefixBuf::ROOT
  }

  /// 引擎级 ACL 变更代数（跨连接改权收敛的唯一判据源，推进口见
  /// [`crate::resp::acl_store::AclStore::write`]）
  ///
  /// 默认 None = 嵌入式 / 测试桩形态无存储执行域，无 ACL 真源可收敛：
  /// 会话侧据此不登记挂载态，鉴权预门零成本旁路
  fn acl_generation(&self) -> Option<u64> {
    None
  }

  /// 按 `(ns, 用户名)` 点查 ACL 用户规则字节（挂载代数陈旧时的句柄重建口）
  ///
  /// 调用约束同 [`crate::resp::acl_store::AclStore::read`]：须在批处理纪元
  /// 保护区外 await（冷记录降级落盘回读）。外层 None = 本执行域无 ACL 存储
  /// 真源面，与 [`Self::acl_generation`] 成对，非存储形态下会话不登记挂载态
  /// 故不可达。trait 对象下 async 方法不可 RPITIT，手工装箱（调用点内联
  /// await，不跨线程移交）
  fn acl_user_record<'a>(&'a self, ns: u64, username: &'a [u8]) -> AclRecordFut<'a> {
    let _ = (ns, username);
    Box::pin(async { None })
  }

  /// 全部库的存储域快照（STORE / PERSISTENCE 段与 MEMORY store_* 项的
  /// 数据面）
  ///
  ///（C# 对位 StoreWrapper.GetDatabasesSnapshot 的编排面，引擎真实现锚在
  /// wkv `WedbStore::store_snapshot`）
  ///
  /// 默认空集：嵌入式 / 测试桩形态无存储执行域注入，与 C#
  /// GetDatabasesSnapshot 无库时返回空数组同构（wmetric 段填充器对空
  /// 快照出空行表，绝不虚报计数）
  fn store_snapshots(&self) -> Vec<DbSnapshot> {
    Vec::new()
  }

  /// 装配期回挂 PENDING_LAT 计量槽（对标 C# RespServerSession 构造 storageSession
  /// 时下传 `LatencyMetrics` 的共享关系：rust 执行域先于会话构造，故由
  /// [`RespServerSession::set_garnet_api`] 在挂入会话时把同一对象回挂执行域，
  /// 供慢路径存储会话承接 PENDING_LAT 计时。会话延迟表本体不回挂：其为连接
  /// 任务独占、执行域只有 `&self` 无法 `&mut` 记点，pending 计时因此单独成
  /// 槽。非存储域执行形态无延迟面，默认空实现即 C# 该面 `latencyMetrics`
  /// 为 null 的同形缺省）
  fn attach_pending_latency(&self, meter: Arc<PendingLatencyMeter>) {
    let _ = meter;
  }
}

/// 存储域系统状态文本（wkv 无 C# SystemState 状态机的 Restoring 中间态：
/// 恢复完成才装配会话，同步 INFO 可达面恒为运行态）
const SYSTEM_STATE_RUNNING: &str = "Running";

/// AOF 地址向量收敛为标量（分片拓扑多值 → INFO 单值投影）
///
/// 尾地址取 max（最新写入推进最远）；水位类地址取 min（全拓扑达成语义，
/// 与 C# 多地址对齐收敛的保守值同构）；内存量取 sum（各子日志常驻环形
/// 缓冲之和）
fn aof_scalar(addr: &waof::AofAddress, take_max: bool) -> i64 {
  let values = (0..addr.length() as usize).filter_map(|i| addr.get(i));
  if take_max {
    values.max().unwrap_or(0)
  } else {
    values.min().unwrap_or(0)
  }
}

/// AOF 向量分量求和（内存量口径）
fn aof_sum(addr: &waof::AofAddress) -> i64 {
  (0..addr.length() as usize)
    .filter_map(|i| addr.get(i))
    .sum()
}

/// AOF 持久化快照投影（waof 六地址真源直读）
///
///（C# 对位 GarnetInfoMetrics.GetDatabasePersistenceStats 的地址字段装配段，
/// 统计真实现锚在 wmetric get_database_persistence_stats）
fn project_aof_snapshot(aof: &GarnetAppendOnlyFile) -> wmetric::AofSnapshot {
  let log = aof.log();
  wmetric::AofSnapshot {
    committed_begin_address: aof_scalar(&log.committed_begin_address(), false),
    committed_until_address: aof_scalar(&log.committed_until_address(), false),
    flushed_until_address: aof_scalar(&log.flushed_until_address(), false),
    begin_address: aof_scalar(&log.begin_address(), false),
    tail_address: aof_scalar(&log.tail_address(), true),
    // 刷盘失败累计（全子日志求和：常驻提交驱动致命故障信号，非零可告警）
    flush_failures: log.sublogs().iter().map(|s| s.flush_failures()).sum(),
  }
}

/// 单库存储域快照投影（wmetric [`DbSnapshot`] 的全仓唯一组装点）
///
///（C# 对位 GarnetInfoMetrics.GetDatabaseStoreStats 的单库字段装配段，
/// 统计真实现锚在 wmetric get_database_store_stats）
///
/// wkv 单物理存储（单 WedbStore 多库前缀隔离），存储域统计按 db 0 形态
/// 呈现（与 HLOGSCAN 段同口径）；whlog 常驻整页分配模型下已分配页 ==
/// 上限 == 整环页数，内存上限 == 当前 == 常驻整环字节；wkv current_version
/// 仅由 checkpoint 拍摄/恢复推进（store/event.rs），LastCheckpointedVersion
/// 读检查点发布确认处与恢复轮登记的独立成功版本（在途/失败轮与 CurrentVersion 分列）；
/// mainlog/readcache 目标内存（C# SizeTracker）rust 无对应机制，
/// None 经 wmetric 出直加当前内存分支。
fn project_db_snapshot<D: Device>(db: &GarnetDatabase<D>) -> DbSnapshot {
  let s = db.store().store_snapshot();
  let log_memory = (s.log_num_pages * s.log_page_size_bytes) as i64;
  let index_memory = (s.index_bucket_count * s.index_bucket_size_bytes) as i64;
  let overflow_memory = (s.index_overflow_bucket_count * s.index_bucket_size_bytes) as i64;
  DbSnapshot {
    id: db.id as i32,
    current_version: s.current_version,
    last_checkpointed_version: s.last_checkpointed_version as i64,
    system_state: SYSTEM_STATE_RUNNING.to_string(),
    index_bucket_count: s.index_bucket_count as i64,
    index_bucket_size_bytes: s.index_bucket_size_bytes as i64,
    index_memory_size_bytes: index_memory,
    index_overflow_bucket_count: s.index_overflow_bucket_count as i64,
    index_overflow_memory_size_bytes: overflow_memory,
    index_total_memory_size_bytes: index_memory + overflow_memory,
    tree_cache_reserved_bytes: s.tree_cache_reserved_bytes as i64,
    tree_cache_budget_bytes: s.tree_cache_budget_bytes as i64,
    log_page_size_bytes: s.log_page_size_bytes as i64,
    log_max_allocated_page_count: s.log_num_pages as i64,
    log_allocated_page_count: s.log_num_pages as i64,
    log_max_memory_size_bytes: log_memory,
    log_memory_size_bytes: log_memory,
    log_heap_size_bytes: log_memory,
    log_begin_address: s.log_begin_address as i64,
    log_head_address: s.log_head_address as i64,
    log_safe_readonly_address: s.log_safe_readonly_address as i64,
    log_flushed_until_address: s.log_flushed_until_address as i64,
    log_tail_address: s.log_tail_address as i64,
    read_cache: s.read_cache.map(|rc| wmetric::ReadCacheSnapshot {
      page_size_bytes: rc.page_size_bytes as i64,
      max_allocated_page_count: rc.num_pages as i64,
      allocated_page_count: rc.num_pages as i64,
      max_memory_size_bytes: rc.memory_size_bytes as i64,
      memory_size_bytes: rc.memory_size_bytes as i64,
      heap_size_bytes: rc.memory_size_bytes as i64,
      // rust ReadCache 无独立 begin 概念：有效起始即滑窗下界 head
      begin_address: rc.head_address as i64,
      head_address: rc.head_address as i64,
      tail_address: rc.tail_address as i64,
    }),
    mainlog_target_size: None,
    readcache_target_size: None,
    aof_memory_size_bytes: db
      .aof
      .as_ref()
      .map_or(0, |aof| aof_sum(&aof.log().memory_size_bytes())),
    aof: db.aof.as_ref().map(|aof| project_aof_snapshot(aof)),
    // 转储文本段（STOREHASHTABLE / STOREREVIV）走慢路径通道，同步面不触发
    hash_distribution_dump: String::new(),
    revivification_dump: String::new(),
    // 管理面在册库快照恒物理行（虚库行仅由慢路径组合源扫描侧合成）
    virtual_db: false,
  }
}

/// libs/server/API/IGarnetApi.cs:IGarnetApi
///
/// 存储执行域命令分派句柄（`Arc<dyn GarnetApiFace>` trait 对象，对标 C#
/// 直持 storageApi 引用的虚分派；消除 `D: Device` 泛型对会话/消费者层传染）
pub type GarnetApi = Arc<dyn GarnetApiFace>;

/// 集合更新唤醒通知回调（`Arc<dyn Fn>` trait 对象，对标 C# itemBroker
/// `HandleCollectionUpdate` 直接方法引用；宿主以 move 闭包捕获经纪句柄构造。
/// 入参 `(ns, db)` 为写会话所属域，裸键经经纪域折叠后命中观察表）
pub type CollectionNotify = Arc<dyn Fn((u64, u64), &[u8]) + Send + Sync>;

/// wkv 存储会话的 [`GarnetApi`] 实现（单机与集群共用执行域）
///
/// 对标 C# RespServerSession 构造时经 storeWrapper 创建的 storageSession
/// （每连接独立会话）；命令执行期进入批处理纪元（C# EnterUnsafe），整个
/// 消费批次内内存直读免逐操作 enter/exit
pub struct StoreGarnetApi<D: Device> {
  /// 底层存储会话
  pub session: StoreSession<D>,
  /// Vector Set 命令处理层（构造期缓存，杜绝每条向量命令克隆 `Arc<VectorManager>`）
  vector_session: Option<RespServerSessionVectors>,
  /// 检查点通道（未注入 = SAVE/BGSAVE 显式拒绝，LASTSAVE 回 0）
  pub(crate) checkpoint: Option<CheckpointCtx<D>>,
  /// HCOLLECT `*` 全库扫描进行标志（C# HashOps.cs:15 _hcollectTaskLock
  /// SingleWriterMultiReaderLock 的单写位投影；true = 扫描在途，重入回
  /// already-in-progress。粒度对齐 C# per storageSession：本执行域即
  /// 每连接独立会话）
  pub hcollect_in_progress: AtomicBool,
  /// ZCOLLECT `*` 全库扫描进行标志（C# SortedSetOps.cs:17 _zcollectTaskLock，
  /// 与 HCOLLECT 独立两把，C# 同）
  pub zcollect_in_progress: AtomicBool,
  /// 集合更新唤醒（C# StorageSession ListOps/SortedSetOps 写成功后
  /// `itemBroker?.HandleCollectionUpdate(key)`；慢路径执行域无会话可达面，
  /// 装配期经类型擦除闭包注入经纪句柄，None = 无经纪/未装配）
  pub(crate) collection_notify: Option<CollectionNotify>,
  /// 集合项经纪等待面（慢路径阻塞闭环：阻塞族命令冷键装载未取到时经
  /// [`wcol::itembroker::item_broker_face::ItemBrokerFinisher`] 登记观察者
  /// 内联等待，None = 经纪未注入的独立会话域）
  pub(crate) item_broker_wait: Option<Arc<dyn ItemBrokerFinisher>>,
  /// 会话指标共享句柄（对标 C# RespServerSession 构造 storageSession 时下传的
  /// sessionMetrics 类引用；慢路径主存储会话经此直写命中/pending 计数，
  /// None = 采样关闭）
  pub(crate) session_metrics: Option<Arc<SessionMetricsHandle>>,
  /// PENDING_LAT 计量槽（对标 C# RespServerSession 构造 storageSession 时下传的
  /// `LatencyMetrics` 类引用在 pending 计时臂上的投影：rust 执行域先于会话
  /// 构造，故由 [`RespServerSession::set_garnet_api`] 装配期回挂会话建好的
  /// 同一槽（构造单点 = C# `new GarnetLatencyMetricsSession(storeWrapper
  /// .monitor)` 的 rust 对位），慢路径存储会话经 [`Self::pending_latency`]
  /// 读此口承接 PENDING_LAT 计时。以 `OnceLock` 承接：回挂点唯一且早于任何
  /// 读点，写一次语义免去互斥；未回挂 = 无延迟监视/非会话宿主，与 C# null
  /// 同形）
  pending_latency: OnceLock<Arc<PendingLatencyMeter>>,
}

impl<D: Device> StoreGarnetApi<D> {
  /// 在 garnet 中的相对路径:Storage/Session/StorageSession.cs:StorageSession
  ///
  /// 构造存储执行域（`session` 为每连接独立的 `store.new_session()` 产物）
  pub fn new(session: StoreSession<D>) -> Self {
    Self {
      session,
      vector_session: None,
      checkpoint: None,
      hcollect_in_progress: AtomicBool::new(false),
      zcollect_in_progress: AtomicBool::new(false),
      collection_notify: None,
      item_broker_wait: None,
      session_metrics: None,
      pending_latency: OnceLock::new(),
    }
  }

  /// 获取底层存储会话引用
  #[inline]
  pub fn session(&self) -> &StoreSession<D> {
    &self.session
  }

  /// 关联集合更新唤醒回调（慢路径写回后唤醒阻塞观察者，对标 C#
  /// itemBroker.HandleCollectionUpdate 的存储可达面；装配期以 move 闭包
  /// 注入，避免泛型传染）
  pub fn with_collection_notify(mut self, notify: Option<CollectionNotify>) -> Self {
    self.collection_notify = notify;
    self
  }

  /// 关联集合项经纪等待面（慢路径阻塞闭环的可等待面：阻塞族命令经慢路径
  /// 装载未取到时在执行域内联登记观察者并竞速超时——C# BlockingWait 的
  /// compio 投影；None = 经纪未注入的独立会话域，慢路径维持立即可取语义）
  pub fn with_item_broker_wait(mut self, broker: Option<Arc<dyn ItemBrokerFinisher>>) -> Self {
    self.item_broker_wait = broker;
    self
  }

  /// 慢路径阻塞等待面只读口（None = 经纪未注入）
  pub(crate) fn item_broker_wait(&self) -> Option<&Arc<dyn ItemBrokerFinisher>> {
    self.item_broker_wait.as_ref()
  }

  /// 关联向量集合管理器（构造期包装为命令处理层，单次 Arc 持有）
  ///
  /// 同处装配删除缺席收口钩子（对标 C# MainStore RemoveKey →
  /// VectorManager.RequestDeletion，GarnetRecordTriggers.OnDispose 的 Deleted 臂）：
  /// wkv 用户键双域删除判未命中后经本钩子摘除登记项，DEL/UNLINK 快慢两臂
  /// 与 GETDEL/重放等一切删除口共用同一存储删除单点，计数与登记清退不再
  /// 口径分裂；OnceLock 保首，同引擎重复注入幂等忽略
  pub fn with_vector_manager(mut self, vector_manager: Arc<VectorManager>) -> Self {
    self
      .session
      .store()
      .set_delete_miss_hook(vector_registry_delete_hook(Arc::clone(&vector_manager)));
    self.vector_session = Some(RespServerSessionVectors::new(vector_manager));
    self
  }

  /// 关联检查点通道（SAVE/BGSAVE/LASTSAVE 经此闭环，对标 C#
  /// storeWrapper.TakeCheckpointAsync 的存储可达面）
  pub fn with_checkpoint_ctx(mut self, ctx: CheckpointCtx<D>) -> Self {
    self.checkpoint = Some(ctx);
    self
  }

  /// 关联会话指标共享句柄（对标 C# storageSession 构造下传 sessionMetrics；
  /// 供 exec_slow 主存储会话绑定，None = 采样关闭）
  pub fn with_session_metrics(mut self, metrics: Option<Arc<SessionMetricsHandle>>) -> Self {
    self.session_metrics = metrics;
    self
  }

  /// 当前 PENDING_LAT 计量槽（未回挂 = None）：慢路径存储会话构造时单点读此
  /// 口，与会话共持同一 Arc（回挂语义见 [`GarnetApiFace::attach_pending_latency`]）
  #[inline]
  pub(crate) fn pending_latency(&self) -> Option<Arc<PendingLatencyMeter>> {
    self.pending_latency.get().cloned()
  }

  /// 本租户逐在册库的向量登记表计数（逻辑库号 → 向量键数）
  ///
  /// INFO keyspace 消费点合并用（引擎侧 `keyspace_stats` 纯物理记录扫描无
  /// VectorManager 访问面，wkv 层不持装配句柄；C# 向量元数据驻留主存、
  /// GetKeyspaceStats 扫描天然计入的旁挂域对位）：按租户在册库快照逐库
  /// 构造会话域前缀直查登记表，零计数库不入表。向量域未装配 / 租户未在册
  /// 返回空表（无键可加）
  pub(crate) fn vector_domain_counts(&self) -> HashMap<u64, usize> {
    let Some(vectors) = &self.vector_session else {
      return HashMap::default();
    };
    let store = self.session.store();
    let Some(vns) = store.vdb.vns_of_ns(self.session.namespace()) else {
      return HashMap::default();
    };
    store
      .vdb
      .registered_dbs(vns)
      .into_iter()
      .filter_map(|(vdb, logic_db)| {
        let count = vectors
          .manager
          .registry_domain_count(SessionPrefixBuf::new(vns, vdb).as_slice());
        (count > 0).then_some((logic_db, count))
      })
      .collect()
  }

  /// 关联常驻单库管理器（对标 C# storeWrapper.databaseManager）
  pub fn with_database_manager(mut self, database_manager: Arc<SingleDatabaseManager<D>>) -> Self {
    self.checkpoint = Some(CheckpointCtx::new(database_manager));
    self
  }

  /// 触发集合更新唤醒通知（裸键随本执行域会话的 (ns, db) 传入）
  #[inline]
  pub(crate) fn notify_collection_update(&self, key: &[u8]) {
    if let Some(broker) = &self.collection_notify {
      broker((self.session.namespace(), self.session.active_db()), key);
    }
  }
}

/// 独占单写者安全论证（本票「解除 StoreGarnetApi 对 StoreSession 的 Sync
/// 传导约束」的许可路线之二）：
///
/// `StoreSession<D>` 经 `Participant: !Sync` 收紧为 `!Sync`（wepoch
/// participant.rs 线程亲和契约），但本执行域实例的**触达面**恒为独占串行：
///   * 每实例由 `get_session` 在连接任务内创建，`GarnetApi` 句柄随
///     `RespSessionConsumer` 移交该连接任务，compio thread-per-core 任务不
///     迁线程——`exec` / `exec_slow` 全部在属主线程的同步段或 poll 边界内
///     触达 `session`（`SlowFuture` 的 `unsafe impl Send` 同一纪律：慢路径
///     执行体由属主任务的 `SlowWait::resolve` 就地驱动，从不他线程 poll）；
///   * `Arc` 克隆只延后 drop 的时点与线程，不参与会话触达；跨线程仅存
///     `Arc` 引用计数的原子操作；
///   * 注册表（CLIENT 枚举面）持投影不持会话（consumer_registry 模块头），
///     他线程无 `&StoreGarnetApi` 可达面。
///
/// 故本类型可满足 `Sync` 标记（共享引用存在）而不发生跨线程并发触达
/// （共享引用被并发使用），与 C# storageSession 随连接任务独占同构。
///
/// 违反纪律即 UB：任何新增跨线程并发触达 `&StoreGarnetApi` 的执行体必须
/// 改为自持会话并走 `bind_dedicated_session` / `ActiveVectorSessionGuard`
/// 私有域通道。
unsafe impl<D: Device> Sync for StoreGarnetApi<D> {}

/// ACL 命令族判定（`RespCommand::Acl*` 集合的 rust 侧谓词，无 C# 直接对应；
/// 这些命令经存储直通面而非批处理存储分派；分派漏斗据此预筛停车进异步臂）
#[inline]
pub(crate) fn is_acl_command(cmd: RespCommand) -> bool {
  matches!(
    cmd,
    RespCommand::AclList
      | RespCommand::AclUsers
      | RespCommand::AclCat
      | RespCommand::AclSetuser
      | RespCommand::AclDeluser
      | RespCommand::AclWhoami
      | RespCommand::AclLoad
      | RespCommand::AclSave
      | RespCommand::AclGenpass
      | RespCommand::AclGetuser
  )
}

impl<D: Device> GarnetApiFace for StoreGarnetApi<D> {
  fn exec_auth_acl<'a>(
    &'a self,
    session: &'a mut RespServerSession,
    cmd: RespCommand,
    args: &'a [&'a [u8]],
  ) -> Pin<Box<dyn Future<Output = bool> + 'a>> {
    Box::pin(async move {
      // AUTH / HELLO / ACL 族：底层存储点查须在批处理纪元保护区外 await——
      // 冷记录落盘回读持纪元守卫等待驱逐会自锁；且认证成功后须回写会话本地
      // 句柄/命名空间，仅本分派段可达（慢路径仅产出应答字节，无会话态变更
      // 面）。存储为 ACL 唯一真源，见 doc/zh/db.md §3
      if cmd == RespCommand::Auth {
        let store = AclStore::new(&self.session);
        let _ = session.network_auth_session(args, &store).await;
        return true;
      }
      if cmd == RespCommand::Hello {
        // HELLO 可携 AUTH 凭据（命名用户经存储点查认证），与 ACL 族同链 async
        // 闭环；应答直写会话输出缓冲（take/并回模式：output 可变借用与
        // session 借用不相交的既有骨架）
        let store = AclStore::new(&self.session);
        let mut output = mem::take(&mut session.output);
        let handled = session.network_hello(args, &store, &mut output).await;
        session.output = output;
        return handled.is_ok();
      }
      if is_acl_command(cmd) {
        let store = AclStore::new(&self.session);
        return session
          .process_acl_commands(cmd, args, &store)
          .await
          .is_some();
      }
      false
    })
  }

  fn exec_acl_refresh<'a>(
    &'a self,
    session: &'a mut RespServerSession,
  ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
    Box::pin(async move {
      // 点查臂在批处理纪元保护区外 await（内存直读优先，冷记录降级异步落盘
      // 回读），刷新完成后泵重驱原命令重评门链
      session.refresh_acl_mount_if_stale().await;
    })
  }

  fn exec(&self, session: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) {
    // 命令分派同步段整段绑定向量存储会话执行域（对标 C# `[ThreadStatic]
    // ActiveThreadSession` 在 NetworkVADD / VSIM 期间的连接线程绑定，且把
    // 缺席删除钩子（DEL 向量键 → 登记表摘除写透）一并纳入绑定段；本段为
    // 纯同步段，段尾自动解绑还原，绝不跨 `.await` 存活）
    let _vector_domain = ActiveVectorSessionGuard::bind(&self.session);
    // 会话锁器模式选型点（对标 C# RespServerSession.ProcessMessages 依
    // `txnManager.state == TxnState.Running` 在 basicApi 与 transactionalApi 间
    // 派发）：事务重放段的本键桶排他闩已由本会话事务在 windex 同一份锁内存上
    // 同一主桶持有（wtxn 锁登记与本窗口寻桶同经 whasher::scoped_hash 前缀种子
    // 单点，票 wtxn-wkv-keybucket-hash-scope-desync），键窗全落持锁桶域时读改
    // 写窗口让闩复用；非事务遍窗口自取闩。RAII 守卫在本分派段退出即还原（降级
    // 慢路径在段外以 Basic 重取闩，与 C# 慢路径重投同址同判据）。脚本重入段不
    // 无条件下传 Running：C# 内嵌 processor 自带独立会话、脚本内命令恒走
    // basicApi ephemeral 自取闩，rust 无内嵌 processor 而重入共享会话（事务镜像
    // 滞留 Running），故按本命令键窗对本事务持锁桶域的会合判定选型——在域内
    // 让闩（复用已持闩，杜绝非重入桶闩自等死锁），在域外 Basic 自取闩（他连接
    // 同键并发经同一锁内存互斥，杜绝无闩盲写丢更新；判定单点见
    // `RespServerSession::txn_locks_cover_cmd`，登记 doc/zh/deviations.md §139）
    let _locking = self.session.push_session_locking(
      if session.txn_state == TxnState::Running && session.txn_locks_cover_cmd(cmd, args) {
        SessionLocking::Transactional
      } else {
        SessionLocking::Basic
      },
    );
    // AUTH / HELLO / ACL 族已上提至泵侧 [`Self::exec_auth_acl`] /
    // [`Self::exec_acl_refresh`] 异步臂（分派漏斗 dispatch_via_garnet_api
    // 预筛停车），同步段 exec 只承接其余命令
    if is_vector_set_command(cmd)
      && let Some(vectors) = &self.vector_session
    {
      let resp3 = session.resp_protocol_version == 3;
      // 向量集命令前置守卫（读写分臂，对标 C# RespServerSessionVectors.cs:
      // 501/944/1360/1362/1417/1453/1489/1534/1573/1664/1717/1786 等）：
      // 键驻留 wkv 值域即拒绝，杜绝与既有非向量键并行建向量集产生双域键或非向量键读命令误报
      if let Some(key) = args.first() {
        let batch = self.session.enter_batch();
        match vectors.vector_key_guard(key, &batch) {
          VectorGuardVerdict::Reject(reply) => {
            reply.encode_resp(&mut session.output, resp3);
            return;
          }
          // Allow / 只读族 Degrade：一律落穿下方 raw::dispatch Ok(false) 臂
          // 挂起 SlowWait（向量十二臂全量挂起化，见下注）
          VectorGuardVerdict::Allow => {
            // VADD 登记创建位过期残留清退快臂（案 zcode-r151c-exwatch 案二）：
            // 守卫对「过期未清退死残留」判缺失放行，窗内同步清退 wkv 值域
            // 过去代残留（TTL 旁路记录 + 判死值记录），清退 bump 与命令体
            // 自身 bump 合并同命令、恒先于任何后续 WATCH 登记，WATCH 假弃
            // 面归零；降级形（失闩/磁盘候选/页翻转）零副作用静默落穿，由
            // network_vector_write_slow VADD 臂同源口经既有 delete 级联承接
            if cmd == RespCommand::Vadd {
              let _ = purge_expired_residue_sync(&batch, key);
            }
          }
          VectorGuardVerdict::Degrade if cmd.is_vector_read_command() => {}
          // 磁盘候选待裁决 / 存储错误（同步段读不准）：写命令保守拒（冷态
          // 不确定键拒写，误拒可 DEL 后重试，双域键一经写即成幽灵——取舍
          // 登记 doc/zh/deviations.md §22）
          VectorGuardVerdict::Degrade => {
            vectors
              .wrong_type_reply()
              .encode_resp(&mut session.output, resp3);
            return;
          }
        }
      }
      // 十二臂全量挂起化（对标 C# VectorStoreOps.cs 十四锁点 using 锁域
      // 全程罩住命令体的 rust 异步承接）：写族（VADD / VSETATTR / VREM：
      // 插入/删除/属性写链为 compio 存储异步操作）+ 读族全部经锁定读面
      // read_vector_index（VSIM / VEMB / VCARD / VDIM / VGETATTR / VINFO /
      // VISMEMBER / VLINKS / VRANDMEMBER：共享守卫跨命令体 await 存活，
      // ptr=0 冷记录在锁内独占重建后降级共享——重建挂起面归一，同步段
      // 无就地应答形态）——Allow 态直接落穿下方 raw::dispatch Ok(false)
      // 臂（与只读 Degrade 同路），参数快照登记 SlowWait 停车，网络泵
      // await exec_slow 向量读/写臂（network_vector_read_slow /
      // network_vector_write_slow）异步闭环。对标 cluster 链 pending_slow
      // 转挂 + acl 链泵侧异步臂先例，不发明新机制；挂起窗口的键域竞态由
      // 慢臂真读复判收口（见 network_vector_write_slow 文档）。
    }
    let batch = self.session.enter_batch();
    let mut output = mem::take(&mut session.output);
    // 命令层约定：Ok(false) = 须异步闭环且本次不残留输出
    let vector = self.vector_mgr();
    if raw::dispatch(session, cmd, args, &batch, vector, &mut output) == Ok(false) {
      // 慢路径分派（单次实现，多命令复用）：挂起 SlowWait 停止本批消费，
      // 网络泵 await 闭环后写回应答；参数快照脱离接收缓冲生命周期。
      // 句柄克隆保 Arc 存活，future 借用的执行域（self.session）在网络泵
      // await 期间有效（消费串行驱动，无并发进入）
      if let Some(api) = &session.garnet_api {
        // INFO 慢路径（凡段集含扫描族段的整请求降级）：类型化调度参数直挂
        // exec_slow_info 单源——非扫描面在此调度点经 InfoSurface 同步快照
        //（exec_slow 无会话可达面，与命令名 / resp_version 快照同渠道同
        // 口径，取代旧 8 字节 LE 库数上限尾参裸字节通道），扫描行由慢臂
        // 异步产出，两路合成组合数据源单源渲染（对标 C# 逐段实填）
        if cmd == RespCommand::Info {
          let parsed = InfoCommand::parse_sections(args);
          let surface = InfoSurface::capture(&SessionInfoSource::new(session), &parsed.sections);
          let max_databases = session.max_databases;
          let active_db = session.active_db_id as i32;
          let resp_version = session.resp_protocol_version;
          session.pending_slow = Some(Arc::clone(api).exec_slow_info(
            parsed.sections,
            surface,
            max_databases,
            active_db,
            resp_version,
          ));
          session.output = output;
          return;
        }
        let mut snapshot = if cmd == RespCommand::Get
          && let Some(sg_keys) = session.sg_batched_keys.take()
        {
          sg_keys
        } else {
          args.iter().map(|a| a.to_vec()).collect()
        };
        // 自定义对象命令快照尾参追加命令名（C# currentCustomObjectCommand
        // 的慢路径承接：exec_slow 无会话可达面，经名回查注册表）
        if cmd == RespCommand::Customobjcmd
          && let Some((_, custom)) = session.current_custom_command.take()
        {
          snapshot.push(custom.name.as_bytes().to_vec());
        }
        // HSCAN/SSCAN/ZSCAN/COSCAN 慢路径快照尾参追加 COUNT 上限（4 字节
        // LE；同款先例；OBJECT_SCAN_COUNT_LIMIT 运行时配置热更即时生效）
        if matches!(
          cmd,
          RespCommand::Hscan | RespCommand::Sscan | RespCommand::Zscan | RespCommand::Coscan
        ) {
          snapshot.push(
            session
              .runtime_config()
              .get_int(ServerConfigType::ObjectScanCountLimit)
              .to_le_bytes()
              .to_vec(),
          );
        }
        // MSETNX 慢路径快照尾参追加续跑模式标记（[`MsetnxResume::tail_byte`]：
        // b"1" = NX 判定已整体通过、已写键保持，慢路径补写续跑；b"r" = 回滚
        // 存在删除降级残留，慢路径持窗条件回滚收尾；b"0" = 判定段降级，慢
        // 路径须先完整异步裁决存活），消费即复位
        if cmd == RespCommand::Msetnx {
          snapshot.push(vec![session.msetnx_resume.tail_byte()]);
          session.msetnx_resume = MsetnxResume::Replay;
        }
        // DEL / UNLINK 慢路径快照尾参追加已删键计数（8 字节 LE；
        // 沿 MSETNX resume 尾参先例），慢路径继承起始计数，消费即复位
        if matches!(cmd, RespCommand::Del | RespCommand::Unlink) {
          snapshot.push(session.del_deleted_count.to_le_bytes().to_vec());
          session.del_deleted_count = 0;
        }
        // RESTORE / SET 条件写族慢路径快照尾参追加「值已提交 + TTL 待投」
        // 续跑标记（[`TtlResume::tail_bytes`]，9 字节：模式字节 + 8 字节 LE
        // 过期刻度；沿 MSETNX/DEL resume 尾参先例经同一 pending_slow 通道承载，
        // 票 wnode-nx-conditional-ttl-degrade-replay-selfhit：快臂值先落库、
        // put_ttl 遭环形页翻转降级时慢臂跳过整命令重放自碰已提交值——
        // RESTORE 误回 BUSYKEY / SET NX 误回 nil / KEEPTTL 丢 TTL，仅补投
        // TTL 出成功帧），消费即复位
        if matches!(
          cmd,
          RespCommand::Restore | RespCommand::Set | RespCommand::Setexnx
        ) {
          snapshot.push(session.ttl_resume.tail_bytes());
          session.ttl_resume = TtlResume::Full;
        }
        // ETag 写族慢路径快照尾参追加「值腿已提交 + 余腿待投」续跑标记
        // （[`EtagResume::tail_bytes`]，17 字节：模式字节 + 8 字节 LE 新 etag +
        // 8 字节 LE 过期刻度；与 RESTORE/SET 同一 pending_slow 通道、同一尾参
        // 纪律，票 zcode-r139c-etag2 案一情形 B：快臂值已同步落库后 TTL / etag
        // 余腿遭环形页翻转降级时，慢臂不得整命令重放自碰已提交值——Missing /
        // WrongType 臂读态翻成 Hit 后条件重判失配回 [0, 新值] 且 etag 侧写永缺、
        // KEEPTTL 形回填读已被 upsert 清退的 None 静默丢 TTL，剥尾参后持窗补投
        // 余腿出与快臂逐字节一致的成功帧），消费即复位
        if matches!(
          cmd,
          RespCommand::Setifmatch | RespCommand::Setifgreater | RespCommand::Setwithetag
        ) {
          snapshot.push(session.etag_resume.tail_bytes());
          session.etag_resume = EtagResume::Full;
        }
        // VADD 慢路径快照尾参追加库级定槽（2 字节 LE；同款先例：
        // exec_slow 无会话可达面，槽位随调度点快照带入，慢臂剥除后交
        // network_vadd；对标 slot_of 的快速臂取值同源）
        if cmd == RespCommand::Vadd {
          snapshot.push(
            slot_of(self.session.namespace(), self.session.active_db())
              .to_le_bytes()
              .to_vec(),
          );
        }
        session.pending_slow = Some(SlowWait::for_command(
          api,
          cmd,
          snapshot,
          session.resp_protocol_version,
        ));
      } else {
        // 执行域未挂载的装配缺口：写明错误，绝不静默吞命令
        write_error_raw(&mut output, RESP_ERR_ASYNC_REQUIRED);
      }
    }
    session.output = output;
  }

  fn exec_slow(
    self: Arc<Self>,
    cmd: RespCommand,
    args: Vec<Vec<u8>>,
    resp_version: u8,
  ) -> SlowFuture {
    // poll 边界绑定包装（FLUSHDB 慢路径的登记表域回收、DEL 系慢路径的缺席
    // 删除钩子等向量臂须见当前执行域会话，见 [`SlowPollSessionBound`]）
    let api = Arc::clone(&self);
    SlowFuture::new(SlowPollSessionBound {
      api,
      inner: async move { self.exec_slow_impl(cmd, args, resp_version).await },
    })
  }

  /// INFO 慢路径覆写臂：扫描行经存储域 [`Self::info_scan_slow`] 异步产出，
  /// 与调度点快照的非扫描面合成组合数据源后单源渲染出帧；poll 边界执行域
  /// 绑定包装与 [`Self::exec_slow`] 同款（存储段 TLS 句柄线程绑定）
  fn exec_slow_info(
    self: Arc<Self>,
    sections: Vec<InfoMetricsType>,
    surface: InfoSurface,
    max_databases: u64,
    active_db: i32,
    resp_version: u8,
  ) -> SlowWait {
    let api = Arc::clone(&self);
    SlowWait::new(SlowPollSessionBound {
      api,
      inner: async move {
        let scan = self.info_scan_slow(&sections, max_databases).await;
        render_info_slow_reply(&sections, &scan, surface, active_db, resp_version)
      },
    })
  }

  #[inline]
  fn set_context(&self, ns: u64, db: u64) -> bool {
    self.session.set_context(ns, db)
  }

  /// 执行域会话物理前缀直取（锁轨种子与物理寻址的 `StoreSession` 单点）
  #[inline]
  fn session_prefix(&self) -> SessionPrefixBuf {
    self.session.session_prefix()
  }

  /// 执行域会话逻辑前缀直取（版本轨种子 `StoreSession` 单点，与写面
  /// bump_watch_version 的逻辑投影同源）
  #[inline]
  fn session_logical_prefix(&self) -> SessionPrefixBuf {
    self.session.session_logical_prefix()
  }

  /// 引擎级 ACL 变更代数（`Arc<WedbStore>` 单标量，同引擎各连接共视同一源）
  #[inline]
  fn acl_generation(&self) -> Option<u64> {
    Some(self.session.store().acl_generation())
  }

  /// ACL 用户规则点查（与会话执行域同一存储会话，ACL 恒驻 db 0 故不经上下文）
  fn acl_user_record<'a>(
    &'a self,
    ns: u64,
    username: &'a [u8],
  ) -> Pin<Box<dyn Future<Output = Option<wkv::Result<Option<Vec<u8>>>>> + 'a>> {
    let store = AclStore::new(&self.session);
    Box::pin(async move { Some(store.read(ns, username).await) })
  }

  /// 装配期回挂 PENDING_LAT 计量槽（[`RespServerSession::set_garnet_api`] 挂入
  /// 会话时调用，执行域持有的永远与会话是同一槽；重复回挂保留首次，装配
  /// 单点在类型层面固化）
  #[inline]
  fn attach_pending_latency(&self, meter: Arc<PendingLatencyMeter>) {
    let _ = self.pending_latency.set(meter);
  }

  /// 库快照逐库投影（经检查点通道的数据库管理面枚举；通道未注入的
  /// 嵌入式形态回空集）
  ///
  ///（C# 对位 StoreWrapper.GetDatabasesSnapshot 的转发面，引擎真实现锚在
  /// wkv `WedbStore::store_snapshot`）
  fn store_snapshots(&self) -> Vec<DbSnapshot> {
    self.checkpoint.as_ref().map_or_else(Vec::new, |ctx| {
      ctx
        .database_manager
        .get_databases_snapshot()
        .iter()
        .map(|db| project_db_snapshot(db))
        .collect()
    })
  }
}
