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

use crate::{aof::GarnetAppendOnlyFile, database::GarnetDatabase, resp::acl_store::AclStore};
mod objects;
mod raw;
mod slow;

use std::{
  mem,
  sync::{Arc, atomic::AtomicBool},
};

pub(crate) use objects::object_collect_all;
use parking_lot::Mutex;
use wbase::hash_slot::slot_of;
use wconf::ServerConfigType;
use wdev::Device;
use wkv::StoreSession;
use wmetric::{DbSnapshot, GarnetLatencyMetricsSession, SessionMetricsHandle};
use wresp::{
  cmd_strings::{RESP_ERR_ASYNC_REQUIRED, write_error_raw},
  command::{RespCommand, is_vector_set_command},
};

use crate::{
  database::SingleDatabaseManager,
  resp::{
    RespServerSession,
    slow_path::{SlowFuture, SlowWait},
    vector::{
      resp_server_session_vectors::RespServerSessionVectors, vector_manager::VectorManager,
    },
  },
  storage::{
    StorageSession,
    session::{storage_session::vector_registry_delete_hook, txn_proc_view::TxnProcView},
  },
};

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
pub trait GarnetApiFace: Send + Sync {
  /// libs/server/API/IGarnetApi.cs:IGarnetApi
  ///
  /// 执行一条命令并写回应答到会话输出缓冲（C# ProcessBasicCommands /
  /// ProcessArrayCommands / ProcessOtherCommands 的 switch 承接）。
  /// 未接入分派表的命令按 C# ProcessAdminCommands 尾部兜底写
  /// `ERR unknown command`
  fn exec(&self, session: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]);

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

  /// 切换当前会话底层存储上下文（命名空间 + 数据库）
  ///
  /// 返回是否完成上下文物化：严格会话映射未装载（冷租户/冷库）时返回
  /// false，由调用方挂起 [`wkv::WedbStore::resolve_context`] 点查装载后重放
  fn set_context(&self, ns: u64, db: u64) -> bool {
    let _ = (ns, db);
    true
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
  /// 保护区外（冷记录降级阻塞回读，持守卫等待驱逐会自锁）。外层 None = 本
  /// 执行域无 ACL 存储真源面，与 [`Self::acl_generation`] 成对，非存储形态
  /// 下会话不登记挂载态故不可达
  fn acl_user_record(&self, ns: u64, username: &[u8]) -> Option<wkv::Result<Option<Vec<u8>>>> {
    let _ = (ns, username);
    None
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

  /// 事务过程三段式驱动（libs/server/Transaction/TransactionManager.cs:188-190
  /// 装配 garnetTxPrepareApi / garnetTxMainApi / garnetTxFinalizeApi 三 api 的
  /// rust 落点：存储执行域在具体类型在场处构造 [`TxnProcView`] 视图并驱动
  /// [`wtxn::TransactionManager::run_transaction_proc`]）
  fn run_txn_proc(&self, run: TxnProcRun<'_>) -> bool;

  /// 装配期回挂会话侧延迟表（对标 C# RespServerSession 构造 storageSession 时
  /// 下传 `LatencyMetrics` 的共享关系：rust 执行域先于会话构造，故由
  /// [`RespServerSession::set_garnet_api`] 在挂入会话时把同一对象回挂执行域，
  /// 供慢路径存储会话承接 PENDING_LAT 计时。非存储域执行形态无延迟面，
  /// 默认空实现即 C# 该面 `latencyMetrics` 为 null 的同形缺省）
  fn attach_latency_metrics(&self, metrics: Arc<GarnetLatencyMetricsSession>) {
    let _ = metrics;
  }
}

/// 事务过程三段式驱动参数（[`GarnetApiFace::run_txn_proc`] 的宿主侧参数束；
/// 过程体经 `&mut dyn TxnProcedure` 擦除——擦除的是唯一静态枚举宿主而非
/// 运行时注册表，与 [`wtxn::SlotVerifyHandle`] 的既有处置同形）
pub struct TxnProcRun<'a> {
  /// 事务管理器
  pub txn_manager: &'a mut wtxn::TransactionManager,
  /// 过程体
  pub proc: &'a mut (dyn wtxn::TxnProcedure + 'a),
  /// 过程输入负载
  pub proc_input: &'a [u8],
  /// 过程输出
  pub output: &'a mut Vec<u8>,
  /// 是否处于 AOF 回放（回放期跳过收尾段与落盘）
  pub is_replaying: bool,
  /// 迭代式槽位校验直穿句柄（None = 单机 / 回放宿主）
  pub verifier: Option<wtxn::SlotVerifyHandle<'a>>,
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
/// 同源直读；mainlog/readcache 目标内存（C# SizeTracker）rust 无对应机制，
/// None 经 wmetric 出直加当前内存分支。
fn project_db_snapshot<D: Device>(db: &GarnetDatabase<D>) -> DbSnapshot {
  let s = db.store.store_snapshot();
  let log_memory = (s.log_num_pages * s.log_page_size_bytes) as i64;
  let index_memory = (s.index_bucket_count * s.index_bucket_size_bytes) as i64;
  let overflow_memory = (s.index_overflow_bucket_count * s.index_bucket_size_bytes) as i64;
  DbSnapshot {
    id: db.id as i32,
    current_version: s.current_version,
    last_checkpointed_version: s.current_version,
    system_state: SYSTEM_STATE_RUNNING.to_string(),
    index_bucket_count: s.index_bucket_count as i64,
    index_bucket_size_bytes: s.index_bucket_size_bytes as i64,
    index_memory_size_bytes: index_memory,
    index_overflow_bucket_count: s.index_overflow_bucket_count as i64,
    index_overflow_memory_size_bytes: overflow_memory,
    index_total_memory_size_bytes: index_memory + overflow_memory,
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
  }
}

/// libs/server/API/IGarnetApi.cs:IGarnetApi
///
/// 存储执行域命令分派句柄（`Arc<dyn GarnetApiFace>` trait 对象，对标 C#
/// 直持 storageApi 引用的虚分派；消除 `D: Device` 泛型对会话/消费者层传染）
pub type GarnetApi = Arc<dyn GarnetApiFace>;

/// 集合更新唤醒通知回调（`Arc<dyn Fn>` trait 对象，对标 C# itemBroker
/// `HandleCollectionUpdate` 直接方法引用；宿主以 move 闭包捕获经纪句柄构造）
pub type CollectionNotify = Arc<dyn Fn(&[u8]) + Send + Sync>;

/// wkv 存储会话的 [`GarnetApi`] 实现（单机与集群共用执行域）
///
/// 对标 C# RespServerSession 构造时经 storeWrapper 创建的 storageSession
/// （每连接独立会话）；命令执行期进入批处理纪元（C# EnterUnsafe），整个
/// 消费批次内内存直读免逐操作 enter/exit
pub struct StoreGarnetApi<D: Device> {
  /// 底层存储会话
  pub(crate) session: StoreSession<D>,
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
  /// 会话指标共享句柄（对标 C# RespServerSession 构造 storageSession 时下传的
  /// sessionMetrics 类引用；慢路径主存储会话经此直写命中/pending 计数，
  /// None = 采样关闭）
  pub(crate) session_metrics: Option<Arc<SessionMetricsHandle>>,
  /// 会话侧延迟表（对标 C# RespServerSession 构造 storageSession 时下传的
  /// `LatencyMetrics` 类引用：rust 执行域先于会话构造，故由
  /// [`RespServerSession::set_garnet_api`] 装配期回挂会话构造时建好的同一
  /// 对象（构造单点 = C# `new GarnetLatencyMetricsSession(storeWrapper
  /// .monitor)` 的 rust 对位），慢路径存储会话经 [`Self::latency_metrics`]
  /// 读此口承接 PENDING_LAT 计时。以互斥槽承接（仅慢路径每命令读一次，
  /// 非热路径）；未回挂 = 无延迟监视/非会话宿主，与 C# null 同形）
  latency_metrics: Mutex<Option<Arc<GarnetLatencyMetricsSession>>>,
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
      session_metrics: None,
      latency_metrics: Mutex::new(None),
    }
  }

  /// 关联集合更新唤醒回调（慢路径写回后唤醒阻塞观察者，对标 C#
  /// itemBroker.HandleCollectionUpdate 的存储可达面；装配期以 move 闭包
  /// 注入，避免泛型传染）
  pub fn with_collection_notify(mut self, notify: Option<CollectionNotify>) -> Self {
    self.collection_notify = notify;
    self
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

  /// 当前会话侧延迟表（未回挂 = None）：慢路径存储会话构造时单点读此口，
  /// 与会话共持同一 Arc（回挂语义见 [`GarnetApiFace::attach_latency_metrics`]）
  #[inline]
  pub(crate) fn latency_metrics(&self) -> Option<Arc<GarnetLatencyMetricsSession>> {
    self.latency_metrics.lock().clone()
  }

  /// 关联常驻单库管理器（对标 C# storeWrapper.databaseManager）
  pub fn with_database_manager(mut self, database_manager: Arc<SingleDatabaseManager<D>>) -> Self {
    self.checkpoint = Some(CheckpointCtx::new(database_manager));
    self
  }

  /// 触发集合更新唤醒通知
  #[inline]
  pub(crate) fn notify_collection_update(&self, key: &[u8]) {
    if let Some(broker) = &self.collection_notify {
      broker(key);
    }
  }
}

/// ACL 命令族判定（`RespCommand::Acl*` 集合的 rust 侧谓词，无 C# 直接对应；
/// 这些命令经存储直通面而非批处理存储分派）
#[inline]
fn is_acl_command(cmd: RespCommand) -> bool {
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
  fn exec(&self, session: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) {
    // AUTH / ACL 族：底层存储点查须在批处理纪元保护区外执行——冷记录落盘
    // 回读经阻塞驱动，持纪元守卫等待驱逐会自锁；且认证成功后须回写会话本地
    // 句柄/命名空间，仅本同步分派段可达（慢路径仅产出应答字节，无会话态
    // 变更面）。存储为 ACL 唯一真源，见 doc/zh/db.md §3
    if cmd == RespCommand::Auth || is_acl_command(cmd) {
      let store = AclStore::new(&self.session);
      if cmd == RespCommand::Auth {
        let _ = session.network_auth_session(args, &store);
        return;
      }
      if session.process_acl_commands(cmd, &store).is_some() {
        return;
      }
    }
    if is_vector_set_command(cmd)
      && let Some(vectors) = &self.vector_session
    {
      let resp3 = session.resp_protocol_version == 3;
      // 向量集全命令 WRONGTYPE 守卫（对标 C# RespServerSessionVectors.cs:
      // 501/944/1360/1362/1417/1453/1489/1534/1573/1664/1717/1786 等）：
      // 键已驻留 wkv 值域即拒绝，杜绝与既有非向量键并行建向量集产生双域键或非向量键读命令误报
      if let Some(key) = args.first() {
        let batch = self.session.enter_batch();
        if let Some(reply) = vectors.abort_vector_set_wrong_type(key, &batch) {
          reply.encode_resp(&mut session.output, resp3);
          return;
        }
      }
      // 登记表域键单次外提（循环前缀外提对位）：本连接 (ns, db) 会话域内
      // 寻址向量登记表，跨库同名键互不可见
      let prefix = self.session.session_prefix();
      let prefix = prefix.as_slice();
      let reply = match cmd {
        RespCommand::Vadd => vectors.network_vadd_impl(
          prefix,
          args,
          slot_of(self.session.namespace(), self.session.active_db()),
          resp3,
        ),
        RespCommand::Vsim => vectors.network_vsim_impl(prefix, args, resp3),
        RespCommand::Vemb => vectors.network_vemb(prefix, args),
        RespCommand::Vcard => vectors.network_vcard(prefix, args),
        RespCommand::Vdim => vectors.network_vdim(prefix, args),
        RespCommand::Vgetattr => vectors.network_vgetattr(prefix, args),
        RespCommand::Vinfo => vectors.network_vinfo(prefix, args),
        RespCommand::Vismember => vectors.network_vismember_impl(prefix, args, resp3),
        RespCommand::Vlinks => vectors.network_vlinks(prefix, args),
        RespCommand::Vrandmember => vectors.network_vrandmember(prefix, args),
        RespCommand::Vrem => vectors.network_vrem(prefix, args),
        RespCommand::Vsetattr => vectors.network_vsetattr_impl(prefix, args, resp3),
        _ => unreachable!(),
      };
      // 应答直写会话输出缓冲（self 借用 vectors 与 session.output 不相交，
      // 免 mem::take/换回的二次搬移）
      reply.encode_resp(&mut session.output, resp3);
      return;
    }
    let batch = self.session.enter_batch();
    let mut output = mem::take(&mut session.output);
    // 命令层约定：Ok(false) = 须异步闭环且本次不残留输出
    let vector = self.vector_session.as_ref().map(|v| v.manager.as_ref());
    if raw::dispatch(session, cmd, args, &batch, vector, &mut output) == Ok(false) {
      // 慢路径分派（单次实现，多命令复用）：挂起 SlowWait 停止本批消费，
      // 网络泵 await 闭环后写回应答；参数快照脱离接收缓冲生命周期。
      // 句柄克隆保 Arc 存活，future 借用的执行域（self.session）在网络泵
      // await 期间有效（消费串行驱动，无并发进入）
      if let Some(api) = &session.garnet_api {
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
        // INFO 慢路径（KEYSPACE/HLOGSCAN 扫描段）快照尾参追加库数上限
        //（8 字节 LE；同款先例）
        if cmd == RespCommand::Info {
          snapshot.push(session.max_databases.to_le_bytes().to_vec());
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
        // MSETNX 慢路径快照尾参追加续跑模式标记（b"1" = NX 判定已整体
        // 通过、前缀键已持久写入，慢路径补写续跑；b"0" = 判定段降级，
        // 慢路径须先完整异步裁决存活），消费即复位
        if cmd == RespCommand::Msetnx {
          snapshot.push(vec![if session.msetnx_resume { b'1' } else { b'0' }]);
          session.msetnx_resume = false;
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
    SlowFuture::new(async move { self.exec_slow_impl(cmd, args, resp_version).await })
  }

  #[inline]
  fn set_context(&self, ns: u64, db: u64) -> bool {
    self.session.set_context(ns, db)
  }

  /// 引擎级 ACL 变更代数（`Arc<WedbStore>` 单标量，同引擎各连接共视同一源）
  #[inline]
  fn acl_generation(&self) -> Option<u64> {
    Some(self.session.store().acl_generation())
  }

  /// ACL 用户规则点查（与会话执行域同一存储会话，ACL 恒驻 db 0 故不经上下文）
  fn acl_user_record(&self, ns: u64, username: &[u8]) -> Option<wkv::Result<Option<Vec<u8>>>> {
    Some(AclStore::new(&self.session).read(ns, username))
  }

  /// 装配期回挂会话延迟表（[`RespServerSession::set_garnet_api`] 挂入会话时
  /// 调用，执行域持有的永远与会话是同一对象）
  #[inline]
  fn attach_latency_metrics(&self, metrics: Arc<GarnetLatencyMetricsSession>) {
    *self.latency_metrics.lock() = Some(metrics);
  }

  /// 事务过程三段式驱动：构造事务过程视图（批处理纪元 + 对象存引擎）后驱动
  /// 三段式——C# TransactionManager.cs:188-190 三 api 装配的本域落点（具体
  /// 类型在场处单点装配，会话/消费者层零泛型传染）
  fn run_txn_proc(&self, run: TxnProcRun<'_>) -> bool {
    let batch = self.session.enter_batch();
    let storage = StorageSession::new(batch);
    let mut view = TxnProcView::new(&storage);
    run.txn_manager.run_transaction_proc(
      run.proc,
      run.proc_input,
      run.output,
      run.is_replaying,
      run.verifier.as_ref(),
      &mut view,
    )
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
