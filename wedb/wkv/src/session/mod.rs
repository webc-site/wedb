//! 客户端会话层（对标 C# Garnet Tsavorite cs/src/core/ClientSession/ClientSession.cs）
//!
//! 三层拆分（纯移动，行为语义不变）：
//! - `mod.rs`：会话核心——StoreSession/BatchStoreSession 定义、纪元参与者生命周期、
//!   context（ns/db）管理与 enter_batch（对标 ClientSession 与 IUnsafeContext/UnsafeContext）；
//! - [`raw`]：纯引擎 KV 面——一切 `*_raw` 物理键操作、unprotected 变体与批量读
//!   （对标 ClientSession 的 Upsert/Read/Delete 快慢路径）；
//! - [`keys`]：键编码域——会话前缀物理键纯函数（对标 C# StorageSession 的键编码）；
//! - [`collection`]：集合元数据与紧凑编码操作（对标 C# StorageSession/MainObjectStore
//!   的元数据与分块存储）。

use std::ptr::eq;

use crate::{
  error,
  session::consistent_read::single_key_around,
  vdb::{DbMetaRecord, ROOT_DBMETA_PREFIX},
};
mod collection;
pub mod consistent_read;
mod keys;
mod raw;
mod rmw_window;
mod swap;
mod vector_cleanup;

use std::{
  future::Future,
  ops::Deref,
  pin::Pin,
  result::Result as StdResult,
  sync::{
    Arc,
    atomic::{
      AtomicBool, AtomicU64,
      Ordering::{Acquire, Relaxed},
    },
  },
};

pub use consistent_read::{ConsistentReadContext, ConsistentReadFunctions};
use parking_lot::Mutex;
pub(crate) use raw::CopyToTailOutcome;
pub use raw::{
  RmwGrow,
  read::{RecordRead, StoreResult},
};
pub(crate) use rmw_window::{INNER_LATCH_RETRY_BUDGET, SessionLockingState};
pub use rmw_window::{
  KeyRef, RMW_PLAN_ACQUIRE_MISS, RMW_PLAN_BUILD_COUNT, RMW_PLAN_PINNED_INDEX, RmwWindow,
  SessionLocking,
};
use smallvec::SmallVec;
use wdev::Device;
use wepoch::{EpochGuard, EpochSuspendGuard, Participant};
use wval::{KeyTag, SessionPrefixBuf};

use crate::{
  error::Result,
  store::{ObjectRmwNotification, TieredCollectionNotification, WedbStore},
};

/// set_context 冷检窗口测试留钩（一次性）：严格冷检通过后、虚 ID 解析前的
/// 间隙内回调，供「GC 空闲析构与 set_context 交错」定向用例放大竞态窗口。
/// 生产路径恒 None，仅一次无争锁读；业务代码禁止触碰
#[doc(hidden)]
pub static TEST_COLD_WINDOW_HOOK: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);

/// WATCH 推进委托（对位 C# Tsavorite 单委托字段 Allocator/WorkQueueLIFO.cs:17
/// `readonly Action<T> work`：wkv 不得依赖 wnode，故以 `Arc<dyn Fn>` 承接同一
/// 抽象度，Send/Sync/Drop 皆由 std 保证，不自立虚表）。
/// 入参 `(会话逻辑前缀, 用户键)`——版本轨=逻辑域种子（双轨分置：锁轨=物理
/// 域种子，见 [`StoreSession::session_prefix`] 与 wtxn 锁登记面，禁共口互染）：
/// C# 版本表按库实例物理隔离（libs/server/GarnetDatabase.cs:156 每库独持
/// WatchVersionMap），rust 共享单表形态下键身份必须自带归属维度
/// （doc/zh/db.md 前缀刚性隔离）；该归属维度取**逻辑域**而非换号物理域——
/// FLUSHDB/FLUSHNS/SWAPDB 换号只换代物理路由、不改逻辑身份，逻辑种子令换号
/// 前后同逻辑键恒落同槽，对在途 WATCH 复现 C#「改后写必命中同槽必 abort」。
/// 与 [`DeleteMissHook`] 同形双参，杜绝第二套擦除代码
type WatchFn = Arc<dyn Fn(&[u8], &[u8]) + Send + Sync>;

/// WATCH 版本推进钩子（收 (会话逻辑前缀, 用户键)——版本轨种子域，宿主上下文
/// 类型由 std 闭包对象擦除，擦除形态见 [`WatchFn`]）
///
/// 对标 C# functionsState.watchVersionMap（libs/server/Transaction/
/// WatchVersionMap.cs）与存储 functions 面的共享装配：每个写面在完成实际
/// 写入后回调一次（MainStore/UnifiedStore/ObjectStore 三套 UpsertMethods/
/// RMWMethods/DeleteMethods 的 InPlaceUpdater/PostInitialWriter/
/// InitialUpdater/InitialDeleter/PostCopyUpdater 挂点语义）
pub struct WatchHook(WatchFn);

impl WatchHook {
  /// 由任意线程安全上下文及静态处理函数构造分发器
  pub fn new<T: Send + Sync + 'static>(ctx: Arc<T>, handler: fn(&T, &[u8], &[u8])) -> Self {
    Self(Arc::new(move |prefix: &[u8], key: &[u8]| {
      handler(&ctx, prefix, key)
    }))
  }

  /// 统一调用推进版本（前缀 = 会话逻辑归属域即版本轨种子域，与
  /// [`Self::new`] 处理器同口径）
  #[inline(always)]
  pub fn call(&self, prefix: &[u8], key: &[u8]) {
    (self.0)(prefix, key);
  }
}

/// 用户键删除缺席观测委托（对位 C# Tsavorite 双参单委托字段
/// Allocator/AllocatorBase.cs:261 `Action<long, long> EvictCallback`）
///
/// 异步形态（手动装箱，dyn 面不可用 RPITIT）：宿主观测臂的登记摘除为真
/// 异步写透（冷区臂 `.await` 引擎异步口，无内联收割），故委托回装箱
/// future；`Send` 约束与全仓回调契约同型——compio thread-per-core 下
/// future 只在其所属任务线程上 poll，宿主会话引用经通行证自持，纯为
/// 类型级要求。
type DeleteMissFn = Arc<
  dyn for<'a> Fn(&'a [u8], &'a [u8]) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>>
    + Send
    + Sync,
>;

/// 用户键删除缺席观测钩子（收 (会话前缀, 用户键)；宿主上下文类型由 std
/// 闭包对象擦除，与 [`WatchHook`] 同一形态、零第二套擦除代码）
///
/// 对标 C# 记录触发器 libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnDispose
/// 的 `DisposeReason.Deleted` 臂：宿主把值域外的键存在态（如向量集登记表）单列于
/// 引擎之外，引擎用户键删除双域（String/ObjectEnvelope）判未命中时回调本钩子收口，
/// 命中即视同删除成功（计数与墓碑口径随存储删除单点统一，RESP 各臂不再各配第二套
/// 清退判据）。`false` = 宿主域亦无此键（缺席删除维持原口径）
pub struct DeleteMissHook(DeleteMissFn);

impl DeleteMissHook {
  /// 由任意闭包构造分发器（闭包返回装箱
  /// future，借用 `prefix`/`key` 切片：生命周期由调用方 await 点框定；
  /// 宿主上下文若需持有状态须自行克隆所有权移入闭包）
  pub fn new<F>(handler: F) -> Self
  where
    F: for<'a> Fn(&'a [u8], &'a [u8]) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>>
      + Send
      + Sync
      + 'static,
  {
    Self(Arc::new(handler))
  }

  /// 统一调用缺席观测：返回宿主域是否实际摘除此键（真异步，调用方
  /// `.await` 闭环；同步快删路径见 `try_delete_sync` 的降级约定。
  /// 返回 future 借用 `prefix`/`key` 切片，不借用 self）
  #[inline(always)]
  pub fn call<'a>(
    &self,
    prefix: &'a [u8],
    key: &'a [u8],
  ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
    (self.0)(prefix, key)
  }
}

/// 引擎实例级 OnceLock 钩子在位快照（换引擎「引擎可见即钩子在场」不变量的
/// 全枚举承载，工单 zcode-r137c-snaplock2 宗一治理臂）：引擎实例级钩子族以
/// 本结构字段全集承载——宿主钩子束（`wnode` engine_swap_hook_bundle）对换入
/// 引擎逐件重挂，`wedb` 置换锁测逐字段断言在位。**新增引擎实例级 OnceLock
/// 钩必须同步扩本枚举**，否则锁测逐字段断言即红，杜绝第四钩再漏。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EngineHookSlots {
  /// WATCH 版本推进钩子（[`WedbStore::set_watch_hook`]）
  pub watch_hook: bool,
  /// 统一存储事件处理器（[`WedbStore::set_event_sink`]，wkv store/event.rs）
  pub event_sink: bool,
  /// 用户键删除缺席观测钩子（[`WedbStore::set_delete_miss_hook`]）
  pub delete_miss_hook: bool,
}

impl<D: Device> WedbStore<D> {
  /// 注入 WATCH 版本推进钩子（装配期一次性调用；重复注入返回 false）
  pub fn set_watch_hook(&self, hook: WatchHook) -> bool {
    self.watch_hook.set(hook).is_ok()
  }

  /// 注入用户键删除缺席观测钩子（装配期一次性调用；重复注入返回 false）
  pub fn set_delete_miss_hook(&self, hook: DeleteMissHook) -> bool {
    self.delete_miss_hook.set(hook).is_ok()
  }

  /// 引擎实例级钩子三件的在位现态只读枚举（见 [`EngineHookSlots`] 治理契约；
  /// 数据面零消费，仅宿主换面断言与观测使用）
  pub fn engine_hook_slots(&self) -> EngineHookSlots {
    EngineHookSlots {
      watch_hook: self.watch_hook.get().is_some(),
      event_sink: self.event_sink.get().is_some(),
      delete_miss_hook: self.delete_miss_hook.get().is_some(),
    }
  }
}

/// 客户端并发会话句柄（绑定一个 LightEpoch 参与者）
///
/// 会话上下文单写者纪律（唯一定义处，doc/zh/db.md §1.2 与本段同源）：
/// 一个执行域内会话为单写者。`namespace` / `active_db` / `active_vns` /
/// `active_vdb` / `is_virtual` / `last_generation` 六字段以 Relaxed 原子承载，
/// 仅因 [`Self::session_prefix`] 慢路径刷新与批处理上下文（[`BatchStoreSession`]
/// 借用 `&Self`）等 `&self` API 面需要内部可变性：Relaxed 单指令读写无锁零
/// 开销，防字节撕裂；但六字段不整体原子更新，（逻辑库，虚拟库，代数）的
/// 跨字段一致性由单写者承接（[`Self::set_context`] 慢路径代数快照收敛论证的
/// 前提即单写者）。故跨任务共享的会话严禁调用 `set_context` /
/// `set_active_db` / `set_virtual_context` 族变更上下文；共享长持有的唯一
/// 许可形态是装配期固化上下文的只读持有者（wnode 向量磁盘回调
/// `WedbVectorStoreCallbacks`，见其类型注释），执行期仅允许 `session_prefix`
/// 幂等换代刷新。`copy_reads_to_tail` 为读面配置位、
/// `strict_ctx` / `bound_vns` 为装配与析构面状态、`session_locking` 为本执行域
/// 命令分派单点写锁器模式位、`purge_window` 为本执行域 purge 链镜像抑制窗位
/// （读写皆在本域，无跨域并发），皆不在本纪律的六字段之列。
pub struct StoreSession<D: Device> {
  pub store: Arc<WedbStore<D>>,
  pub participant: Participant,
  pub copy_reads_to_tail: AtomicBool,
  pub namespace: AtomicU64,
  pub active_db: AtomicU64,
  pub active_vns: AtomicU64,
  pub active_vdb: AtomicU64,
  pub is_virtual: AtomicBool,
  pub last_generation: AtomicU64,
  /// 严格上下文态（RESP 连接会话装配期置位）：`set_context` 在映射未装载时
  /// 禁止同步盲分配，返回 false 交协议层挂起磁盘点查装载后重放（冷租户条款：
  /// 冷租户既有映射在磁盘 DbMeta，盲分配即换新号使旧域判死丢失）
  strict_ctx: AtomicBool,
  /// 当前绑定租户路由快照的 vns（引用归零空闲析构协议；0 = 根域免计数）
  bound_vns: AtomicU64,
  /// 会话锁器模式位（Basic = 自取桶闩 / Transactional = 让闩于事务）：对标 C#
  /// 会话按 api 视图类型编译期选定 `BasicSessionLocker` /
  /// `TransactionalSessionLocker`（见 `session/rmw_window.rs` 模块头），rust 以本位
  /// 承载同一判据，读写单点收口在 `session/rmw_window.rs` 的
  /// [`StoreSession::session_locking`] / [`StoreSession::set_session_locking`] /
  /// [`StoreSession::push_session_locking`]，执行期由命令分派单点与事务过程视图各自置位
  session_locking: SessionLockingState,
  /// purge 链物理写镜像抑制窗位（会话私有，随会话销毁自然消亡，绝无跨会话残留态）：
  /// [`StoreSession::purge_expired`] 窗口内经 [`crate::ttl::PurgeNotifyGuard`]
  /// save/restore 置位，写监听漏斗（`session/raw/mod.rs`）命中本位即跳过本会话级联
  /// 物理写镜像——两条墓碑折叠为单条 TtlPurge 确定性条目（§28 改良，C# 单线程
  /// 存储任务天然互锁无此位）。会话即单写者上下文，其他会话（含并发同键写与
  /// 内置 GC 会话）的清退窗与镜像落否互不相干
  pub(crate) purge_window: AtomicBool,
  /// 副本一致读会话附着态（对标 C# StorageSession.readSessionState 挂各
  /// SessionFunctions 的形态：libs/server/Storage/Session/StorageSession.cs:104-132；
  /// None = 无一致读协议，读路径零开销直通）。装配期一次性附着，其后只读
  read_session_state: Option<Arc<dyn ConsistentReadFunctions>>,
  /// 本会话产生数据条目的 AOF 归组会话 id（对标 C# 会话 `Session.ID`：
  /// libs/server/Storage/Functions/UnifiedStore/UpsertMethods.cs:139
  /// `upsertInfo.SessionID` → MainStore/PrivateMethods.cs:782-790 直传
  /// `Log.Enqueue(.., sessionId, ..)`）。连接级装配期由宿主随连接会话 id 固
  /// 化一次（[`Self::with_aof_session_id`]），执行期只读——数据条目帧头携此值，
  /// 与事务标记面（`TransactionManager.cs:516` `Session.ID`）同键，令重放归组
  /// 口命中组内数据。后台/GC/重放会话未装配即 0（无活动事务组，即到即放）。
  /// 会话单写者纪律外的连接级固化标量，非六字段之列。
  pub aof_session_id: i32,
}

impl<D: Device> StoreSession<D> {
  /// 创建新的客户端会话（默认 ns=0, db=0）
  pub fn new(store: Arc<WedbStore<D>>, participant: Participant) -> Self {
    // 冷读晋升位的真源在 StoreConfig（对标 C# store 级 kvSettings.ReadCopyOptions，
    // GarnetServerOptions.cs:899-900）：会话位只是装配期从真源取的初值快照，
    // 非独立配置源；生产投影单点见 wnode service.rs `apply_hlog_overrides`
    let copy_reads_to_tail = store.config.copy_reads_to_tail;
    let session = Self {
      store,
      participant,
      copy_reads_to_tail: AtomicBool::new(copy_reads_to_tail),
      namespace: AtomicU64::new(0),
      active_db: AtomicU64::new(0),
      active_vns: AtomicU64::new(0),
      active_vdb: AtomicU64::new(0),
      is_virtual: AtomicBool::new(false),
      last_generation: AtomicU64::new(0),
      strict_ctx: AtomicBool::new(false),
      bound_vns: AtomicU64::new(0),
      session_locking: SessionLockingState::new(),
      purge_window: AtomicBool::new(false),
      read_session_state: None,
      aof_session_id: 0,
    };
    session.set_context(0, 0);
    session
  }

  /// 固化本会话的 AOF 归组会话 id（连接装配期一次性置位，执行期只读；
  /// 后台/GC/重放会话不调用即维持 0 = 无会话面语义）。
  #[inline]
  pub fn with_aof_session_id(mut self, aof_session_id: i32) -> Self {
    self.aof_session_id = aof_session_id;
    self
  }

  /// 附着副本一致读会话状态机（装配期一次性调用；对标 C#
  /// NewSession(functions, isConsistentReadSession) 形态——ConsistentReadContext
  /// 由附着态派生，读漏斗按附着态自动触发协议，调用点零 Option 传染）
  pub fn with_read_session_state(
    mut self,
    state: Option<Arc<dyn ConsistentReadFunctions>>,
  ) -> Self {
    self.read_session_state = state;
    self
  }

  /// 一致读会话附着态访问口（None = 未附着，读路径直通）
  #[inline]
  pub fn read_session_state(&self) -> Option<&Arc<dyn ConsistentReadFunctions>> {
    self.read_session_state.as_ref()
  }

  /// 是否为一致读会话（对标 C# StorageSession.IsConsistentReadSession）
  #[inline]
  pub fn is_consistent_read_session(&self) -> bool {
    self.read_session_state.is_some()
  }

  /// 一致读单键协议触发内核（一处定义）：pre 在读前（超时上抛中止），post 在读后
  /// 推进；未附着零开销直通（读漏斗单点触发面，对标 C# 一致读会话的
  /// PreSingleKeyConsistentRead/PostSingleKeyConsistentReadCallback 序列）。
  /// C# 四套 SessionFunctions（Main/Object/Unified/Vector）各自实现的转调壳在
  /// rust 单轨会话下折叠为本漏斗，四处映射一次挂准：
  /// libs/server/Storage/Functions/MainStore/MainSessionFunctions.cs:PreSingleKeyConsistentRead
  /// libs/server/Storage/Functions/ObjectStore/ObjectSessionFunctions.cs:PreSingleKeyConsistentRead
  /// libs/server/Storage/Functions/UnifiedStore/UnifiedSessionFunctions.cs:PreSingleKeyConsistentRead
  /// libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs:PreSingleKeyConsistentRead
  /// 多标签/多探重复触发为良性：post 单调推进，语义仍是「不超过读取时刻的
  /// 前缀上界」。触发哈希经 `hash` 惰性求值，未附着会话连记录物理键编码都不做
  #[inline(always)]
  fn consistent_read_single_key<R>(
    &self,
    hash: impl FnOnce() -> i64,
    f: impl FnOnce() -> R,
  ) -> error::Result<R> {
    match self.read_session_state() {
      Some(fns) => single_key_around(fns.as_ref(), hash(), f),
      None => Ok(f()),
    }
  }

  /// 附着态一致读单键协议触发：前缀取自本会话活跃域
  ///
  /// `tag` 为本次读取触碰的记录域，触发哈希经
  /// [`StoreSession::consistent_read_hash`] 单点取记录物理键域（与回放侧草图
  /// 入账键同键同哈希）
  #[inline]
  pub fn with_session_consistent_read<R>(
    &self,
    user_key: &[u8],
    tag: KeyTag,
    f: impl FnOnce() -> R,
  ) -> error::Result<R> {
    self.consistent_read_single_key(|| self.consistent_read_hash(tag, user_key), f)
  }

  /// 显式前缀附着态一致读单键协议触发（循环前缀外提对位，语义与
  /// [`Self::with_session_consistent_read`] 全等；rust 工程优化无 c# 对应：
  /// 批量命令循环单次外提 `session_prefix()` 消除逐键重读原子变量）
  #[inline]
  pub fn with_session_consistent_read_with_prefix<R>(
    &self,
    prefix: &[u8],
    user_key: &[u8],
    tag: KeyTag,
    f: impl FnOnce() -> R,
  ) -> error::Result<R> {
    self.consistent_read_single_key(
      || Self::consistent_read_hash_with_prefix(prefix, tag, user_key),
      f,
    )
  }

  /// 置位严格上下文态（RESP 连接会话装配期一次性调用）
  ///
  /// 严格态下 [`Self::set_context`] 在映射未装载时拒绝盲分配并返回 false，
  /// 由协议层挂起 [`WedbStore::resolve_context`] 异步点查装载（磁盘为映射
  /// 权威）后重放；非严格会话（内部 / 重放 / 统计）维持纯内存原语语义
  pub fn set_strict_context(&self, strict: bool) {
    self.strict_ctx.store(strict, Relaxed);
  }

  /// 解绑应登记的空闲析构期限毫秒（配置快照换算）
  #[inline]
  fn route_idle_ms(&self) -> u64 {
    self.store.config.gc.route_idle_evict_secs * 1000
  }

  /// 重绑租户路由快照（先绑新者后解旧者，引用计数永不双零）
  #[inline]
  fn rebind_route(&self, vns: u64) {
    let prev = self.bound_vns.load(Relaxed);
    if prev == vns {
      return;
    }
    self.store.vdb.bind_route(vns);
    self.bound_vns.store(vns, Relaxed);
    if prev != 0 {
      self.store.vdb.unbind_route(prev, self.route_idle_ms());
    }
  }

  /// 会话操作纪元入口：会话入口薄包装，直接转发 [`WedbStore::barrier_enter`]
  /// 的 PREPARE_GROW 全事务屏障（AcquireTransactionVersion 的函数级映射唯一见
  /// barrier_enter）。一切会话操作入口必须经此获取纪元保护，严禁绕过屏障直用
  /// [`Participant::enter`]。
  ///
  /// C# 上下文层 Refresh 入口族（重新获取纪元保护）在本 rust 单点的折叠映射
  /// （rust 侧纪元保护为 RAII 守卫，逐操作进入即逐操作刷新，无独立 Refresh 调用面）：
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/BasicContext.cs:Refresh
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/ClientSession.cs:Refresh
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs:Refresh
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/ITsavoriteContext.cs:Refresh
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalContext.cs:Refresh
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalUnsafeContext.cs:Refresh
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/UnsafeContext.cs:Refresh
  #[inline]
  pub(crate) fn enter_gated(&self) -> EpochGuard<'_> {
    self.store.barrier_enter(&self.participant)
  }

  /// 获取当前会话的命名空间
  #[inline(always)]
  pub fn namespace(&self) -> u64 {
    self.namespace.load(Relaxed)
  }

  /// 获取当前会话的活跃数据库编号
  #[inline(always)]
  pub fn active_db(&self) -> u64 {
    self.active_db.load(Relaxed)
  }

  /// 获取当前会话的 19 字节**物理**前缀缓冲（[`Self::virtual_domain`] 的字节
  /// 投影）——记录键编码、事务锁轨种子与一切物理寻址的唯一取点
  #[inline(always)]
  pub fn session_prefix(&self) -> SessionPrefixBuf {
    let (vns, vdb) = self.virtual_domain();
    SessionPrefixBuf::new(vns, vdb)
  }

  /// 获取当前会话的 19 字节**逻辑**前缀缓冲（`namespace()` / `active_db()`
  /// 逻辑真值经 [`SessionPrefixBuf`] 构造的投影）——**版本轨种子单点**
  ///
  /// 双轨分置（禁共口互染）：版本轨=逻辑域、锁轨=物理域。逻辑域不随
  /// FLUSHDB/FLUSHNS/SWAPDB 换号漂移（换号只换逻辑→物理解析），同逻辑库
  /// flush 前后对同字面键的 bump 与 WATCH 登记核验恒落同槽，封堵换号窗内
  /// 冻结旧代物理种子致乐观锁静默丢失的缺口；跨租户/跨库正交性不变
  /// （逻辑 ns/db 仍入种子）。写落域仍是物理域（[`Self::session_prefix`]），
  /// 本投影只供版本轨推进与 WATCH 登记核验消费
  #[inline(always)]
  pub fn session_logical_prefix(&self) -> SessionPrefixBuf {
    SessionPrefixBuf::new(self.namespace.load(Relaxed), self.active_db.load(Relaxed))
  }

  /// 会话当前**物理域** `(active_vns, active_vdb)` 读取单点
  ///
  /// 三口径合一，全仓物理前缀只出自本方法（[`Self::session_prefix`] 即其字节
  /// 投影，入账记录键、AOF 事件携带域、换号回收旁表登记域同源于此；版本轨
  /// 种子除外——版本轨=逻辑域，取 [`Self::session_logical_prefix`] 单点，
  /// 双轨分置禁共口互染）：
  /// - `is_virtual` 为真（经 [`Self::set_virtual_context`] 直设，AOF 回放守卫、
  ///   内置 GC 死域清扫与过期键逐键删除共用该形态）即原样返回直设值，
  ///   零逻辑解析、零虚号分配、零 DbMeta 落盘；
  /// - 否则走逻辑 → 物理解析：代数命中读缓存，落后才经
  ///   [`VirtualDbManager::get_virtual_ids`] 刷新缓存槽。
  ///
  /// 代数快照顺序与 [`Self::set_context`] 同源且不可交换：**先**读当前
  /// 代数（Acquire 载入，与 [`crate::vdb::VirtualDbManager::bump_generation`] 的 Release 配对，
  /// 确保读序不被重排且可见推进代数前已发布的路由表换指与映射变更）、**后**解析虚 ID，
  /// 收尾把「解析前」读到的代数写回缓存槽。解析期间任何换代（启动期 DbMeta 映射重建收尾的
  /// bump、并发 FLUSHDB/FLUSHNS/SWAPDB）都会让缓存槽留在落后一代，下一次本调用必走慢路径重解析；
  /// 反之在解析之后再读代数，会把「新代数 + 旧映射」钉进缓存槽，会话从此固着在磁盘映射已抛弃的
  /// 物理前缀上写数据（幽灵段）。代数与存储侧就绪门禁（见 [`WedbStore::open_shared`]）
  /// 是一对：门禁消掉重建窗口，代数兜住窗口外的换号收敛。
  #[inline(always)]
  pub fn virtual_domain(&self) -> (u64, u64) {
    if self.is_virtual.load(Relaxed) {
      return (self.active_vns.load(Relaxed), self.active_vdb.load(Relaxed));
    }
    // Acquire 与 bump_generation 的 Release 配对，确保先于后续虚 ID 解析且见已发布的路由变更
    let current_gen = self.store.vdb.generation.load(Acquire);
    if self.last_generation.load(Relaxed) == current_gen {
      return (self.active_vns.load(Relaxed), self.active_vdb.load(Relaxed));
    }
    // 慢路径：版本落后，刷新本地缓存
    let ns = self.namespace.load(Relaxed);
    let db = self.active_db.load(Relaxed);
    let (vns, vdb) = self.store.vdb.get_virtual_ids(ns, db);
    self.active_vns.store(vns, Relaxed);
    self.active_vdb.store(vdb, Relaxed);
    self.last_generation.store(current_gen, Relaxed);
    (vns, vdb)
  }

  /// 原子更新当前会话的命名空间与活跃数据库编号，并刷新虚库缓存
  ///
  /// 代数快照顺序与 [`Self::virtual_domain`] 慢路径同源且不可交换：**先**读当前
  /// 代数（Acquire 载入，与 [`crate::vdb::VirtualDbManager::bump_generation`] 的 Release 配对，
  /// 确保读序不被重排且可见推进代数前已发布的路由表换指与映射变更）、**后**解析虚 ID，
  /// 收尾把「解析前」读到的代数写回缓存槽。解析期间任何换代（启动期 DbMeta 映射重建收尾的
  /// bump、并发 FLUSHDB/FLUSHNS/SWAPDB）都会让缓存槽留在落后一代，下一次 [`Self::session_prefix`]
  /// 必走慢路径重解析；反之在解析之后再读代数，会把「新代数 + 旧映射」钉进缓存槽，会话从此
  /// 固着在磁盘映射已抛弃的物理前缀上写数据（幽灵段）。代数与存储侧就绪门禁
  /// （见 [`WedbStore::open_shared`]）是一对：门禁消掉重建窗口，代数兜住窗口外
  /// 的换号收敛。
  ///
  /// 返回是否完成上下文物化：严格会话遇冷库（ns 在册而库映射未装载且路由
  /// 表非权威全量，见 [`VirtualDbManager::is_cold_db`]）时返回 false，不改
  /// 会话任何状态、不分配不持久化——冷租户既有映射以磁盘 DbMeta 为权威，
  /// 须先经 [`WedbStore::resolve_context`] 点查装载再重放本调用（点查未命中
  /// 即真新库，由解析面创建并持久化）。ns 未在册即全新租户，同步创建零挂起。
  ///
  /// 冷检与解析之间的空闲析构窗口封堵：解析前先经 [`VirtualDbManager::bind_route`]
  /// 钉住租户快照（摘除-回插协议保证持引用期间快照绝不被 GC 摘除），钉住后
  /// 复核冷条件——引用未生效（快照恰在冷检与绑定之间被摘除）或钉到并发回建
  /// 空表时，既有映射仍以磁盘为权威，严格会话同样回退挂起面，绝不盲分配
  /// 新号覆写既有租户映射。物化后释放临时钉，会话期绑定由重绑单独持有
  /// （换 ns 时先绑新者后解旧者，引用计数永不双零）
  pub fn set_context(&self, ns: u64, db: u64) -> bool {
    let strict = self.strict_ctx.load(Relaxed);
    if strict && self.store.vdb.is_cold_db(ns, db) {
      return false;
    }
    // 冷检窗口测试留钩（一次性回调，生产恒 None 零负担）：取钩即释锁再回调
    // ——parking_lot 非重入，钩体持锁回调期间重入 set_context（含跨库实例
    // 共读同一静态槽）必自锁
    let hook = TEST_COLD_WINDOW_HOOK.lock().take();
    if let Some(hook) = hook {
      hook();
    }
    // Acquire 与 bump_generation 的 Release 配对，确保先于后续虚 ID 解析且见已发布的路由变更
    let generation = self.store.vdb.generation.load(Acquire);
    let (vns, new_ns) = self.store.vdb.get_or_create_ns(ns);
    if new_ns {
      // 全新租户：路由表为本运行期权威全量，后续新库同步分配免点查甄别
      self.store.vdb.mark_route_authoritative(vns);
    }
    let pinned = self.store.vdb.bind_route(vns);
    if strict && (!pinned || self.store.vdb.is_cold_db(ns, db)) {
      if pinned {
        self.store.vdb.unbind_route(vns, self.route_idle_ms());
      }
      return false;
    }
    self.is_virtual.store(false, Relaxed);
    self.namespace.store(ns, Relaxed);
    self.active_db.store(db, Relaxed);
    let (vdb, new_db) = self.store.vdb.get_or_create_db(ns, db);
    self.active_vns.store(vns, Relaxed);
    self.active_vdb.store(vdb, Relaxed);
    self.last_generation.store(generation, Relaxed);
    self.rebind_route(vns);
    if pinned {
      self.store.vdb.unbind_route(vns, self.route_idle_ms());
    }

    // 映射落盘走换号写路径单点（与 flush/swap 持久化、GC 墓碑删除及点查装载
    // 同一物理布局：固定根域前缀 (ns 0, db 0)，键载荷与值经 DbMetaRecord 单点
    // 编解码，doc/zh/db.md 1.4 与「即时原子提交」段），本同步域无法 await，
    // 仅做原子批同步尝试并显式判定信号：降级（翻页）或引擎硬错误绝不静默
    // 吞掉——告警留痕后维持内存物化，最坏崩溃时该映射丢一次落盘 = 旧域泄漏，
    // 绝不旧号复用撞号（同批携带的 0x05 分配水位与 flush/swap 批同源抬升）
    let mut items: [Option<DbMetaRecord>; 3] = [None, None, None];
    if new_ns && ns != 0 {
      items[0] = Some(DbMetaRecord::NsMap { logic_ns: ns, vns });
    }
    if new_db && (ns != 0 || db != 0) {
      items[1] = Some(DbMetaRecord::DbMap {
        vns,
        logic_db: db,
        vdb,
      });
    }
    if items[0].is_some() || items[1].is_some() {
      // 分配水位收尾（safe-order 末位）：本批既有新号分配即携带，无分配零写放大
      items[2] = Some(DbMetaRecord::NextId {
        next_virtual_id: self.store.vdb.next_virtual_id.load(Relaxed),
      });
      match self.try_persist_dbmeta_sync(&items) {
        Ok(degraded) if !degraded.is_empty() => log::warn!(
          "DbMeta 上下文映射同步批降级未落盘（会话保持内存态）: ns={ns} db={db} 降级 {} 项",
          degraded.len()
        ),
        Err(err) => log::warn!("DbMeta 上下文映射同步批硬错误: ns={ns} db={db} {err}"),
        _ => {}
      }
    }
    true
  }

  /// 直设会话**物理域**（零解析、零分配、零落盘的物理入口），并同存
  /// 该写落域的**逻辑归属域** `(ns, db)`
  ///
  /// 与 [`Self::set_context`] 相对：本方法不做 logic_ns/logic_db → 虚号解析，
  /// 绝不调用 `get_or_create_*`、绝不写 DbMeta，故重放端按条目物理前缀落回
  /// 条目原域（doc/zh/db.md「从库完全继承主库的映射体系，不进行本地二次
  /// 映射」）。置 `is_virtual` 后 [`Self::virtual_domain`] 恒原样返回本对数值。
  ///
  /// 逻辑入参为版本轨种子真值（双轨分置：版本轨=逻辑域、锁轨=物理域）：
  /// 直设形态绕开逻辑→物理解析，[`Self::session_logical_prefix`] 因此无源
  /// 可投影，故调用方必须显式透传其所持逻辑域，写入 `namespace`/`active_db`
  /// 逻辑槽供写面 bump 取用。**禁止**在本方法内部或 bump 链路上经 vns/vdb
  /// 映射表反查——反查与并发换号竞态会重引固着漂移；逻辑域真值只准由调用
  /// 方逐点透传（AOF 回放写漏斗、内置 GC 清扫、分层降阶与后台收集各臂按其
  /// 持有或经条目入账域换算单点 [`crate::vdb::VirtualDbManager::version_domain_of`]
  /// 取版本轨落账域后传入）。
  #[inline]
  pub fn set_virtual_context(&self, vns: u64, vdb: u64, ns: u64, db: u64) {
    self.is_virtual.store(true, Relaxed);
    self.active_vns.store(vns, Relaxed);
    self.active_vdb.store(vdb, Relaxed);
    self.namespace.store(ns, Relaxed);
    self.active_db.store(db, Relaxed);
  }

  /// 设置当前会话的活跃数据库编号并更新会话前缀（物化结果同 [`Self::set_context`]）
  #[inline]
  pub fn set_active_db(&self, db: u64) -> bool {
    self.set_context(self.namespace(), db)
  }

  /// 以会话当前域登记换号回收旁表（一参会话侧单点入口，转调落表内核
  /// [`WedbStore::register_bftree_key`]）
  ///
  /// 对标 C# RangeIndexManager.RegisterIndex/RegisterPending 只收 keyBytes、
  /// 身份在单点内部派生的形态（libs/server/Resp/RangeIndex/
  /// RangeIndexManager.cs:396/:457 走 HashKeyToPrefix + KeyId 自算），杜绝调用
  /// 方逐站手写「两原子 load + 三元组」样板的漏传/换序失败模式。域取自
  /// [`Self::virtual_domain`] 单点（与本会话记录键前缀同一物理域，恢复期直设
  /// 与在线解析两形态自动收敛）；恢复期登记域来自物理键前缀解码、不可取会话
  /// 活跃域，走 store 侧三参内核直调，两形态分工见
  /// [`WedbStore::register_bftree_key`] 文档。
  #[inline]
  pub(crate) fn register_bftree_key(&self, key: &[u8]) {
    let (vns, vdb) = self.virtual_domain();
    self.store.register_bftree_key(vns, vdb, key);
  }

  /// 以会话当前域注销换号回收旁表登记（一参会话侧单点入口，[`Self::register_bftree_key`]
  /// 的逆操作；形态对标 C# UnregisterIndex 自算 keyId，RangeIndexManager.cs:471）
  #[inline]
  pub(crate) fn unregister_bftree_key(&self, key: &[u8]) {
    let (vns, vdb) = self.virtual_domain();
    self.store.unregister_bftree_key(vns, vdb, key);
  }

  /// 获取当前是否开启冷读提升回 Tail
  #[inline]
  pub fn copy_reads_to_tail(&self) -> bool {
    self.copy_reads_to_tail.load(Relaxed)
  }

  /// 是否可能发生复活类并发记录变更（决定 traceback 前是否加 ephemeral 桶锁）
  ///
  /// 对标 Helpers.cs `FindOrCreateTagAndTryEphemeralXLock` /
  /// `FindTagAndTryEphemeralXLock` 的 "Ephemeral must lock the bucket before
  /// traceback, to prevent revivification from yanking the record out from
  /// underneath us" 协议（Helpers.cs:199/216）：C# 不按任何配置开关裁剪该锁，
  /// 取闩形态由锁器型决定（ISessionLocker.cs:38 `BasicSessionLocker
  /// TryLockEphemeralExclusive` 恒实取 LockTable 桶闩；:80
  /// `TransactionalSessionLocker` 只断言事务已持闩），两型皆与 reviv 开关无关。
  /// rust 的锁器分派单点在 `session_locking`（`session/rmw_window.rs` 模块头
  /// 同源叙述），本位只承载危险源本体：upsert/delete 脱钩（elide）虽已常态
  /// 发生，但脱链唯经槽位 CAS 覆写、脱钩槽位绝不被原地复用，无锁回溯至多
  /// CAS 落败重探（与 C# Basic 会话同级）；「回溯期槽位被抽走且携他键复现」
  /// 的拼接风险仅源于复活池开启——判据收敛为 `enable_revivification` 单源，
  /// 无第二口径。
  #[inline]
  pub fn ephemeral_lock_enabled(&self) -> bool {
    self.store.config.enable_revivification
  }

  /// 哈希桶定位/加闩前的分裂协同（全仓会话侧唯一入口，对标 C# 会话操作首行铁律
  /// `if (Ctx.phase == Phase.IN_PROGRESS_GROW) SplitBuckets(hei.hash)`——
  /// InternalRMW.cs:67-72、InternalRead.cs:70-73、InternalUpsert.cs:64-66、
  /// InternalDelete.cs:57-59 一律先协同、后取闩/探针）：扩容期先行迁移键所在分块
  /// 至 SPLIT_COMPLETED，杜绝会话对未迁移新桶加闩与后台迁移内核
  /// （SplitIndex.cs:SplitChunk 断言 `!IsLatched(src_start)`）位碰撞，以及在新桶
  /// 查空导致的读改写盲写覆盖与幽灵读未命中；非扩容期仅一次原子相位读。
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/SplitIndex.cs:SplitBuckets
  #[inline(always)]
  pub(crate) fn ensure_split(&self, key: &[u8]) -> Result<()> {
    self.ensure_split_by_hash(whasher::fast_hash(key))
  }

  /// [`Self::ensure_split`] 的哈希入参对位（调用方已单源算定哈希、全程复用时用，
  /// 严格对标 C# `hei.hash` 一经 `OperationStackContext(keyHash)` 算定不再重算）
  ///
  /// 相位门严格对位 C# 铁律 `phase == IN_PROGRESS_GROW`（非 is_growing 的
  /// PrepareGrow 并集）：全事务屏障下 PrepareGrow 排空后的会话触达面恒静默
  /// （新事务注册被 [`crate::store::resize::IndexResizeState::try_acquire_txn`]
  /// 回绝），放行例外（活跃计数未零）所见仍是完整旧表快照，无须亦不得协同迁移。
  /// 同表观测守卫：dev 全屏障内核已移除 split_single_chunk 的同表回滚旁路
  /// （不变量「InProgressGrow 即活跃表恒为新表」），会话侧唯一入口在此承接
  /// 「相位已发布而活跃表仍为迁移源」的确定性装配矛盾窗（同表即旧表快照裁决，
  /// 不迁移不假推进），杜绝自表向自表迁移的错位写。
  #[inline(always)]
  pub(crate) fn ensure_split_by_hash(&self, hash: u64) -> Result<()> {
    let Some(old) = self.store.resize.old_index.load_full() else {
      return Ok(());
    };
    if eq(Arc::as_ptr(&old), Arc::as_ptr(&self.store.active_index())) {
      return Ok(());
    }
    self.store.split_buckets(hash)
  }

  /// 分裂协同后的链首地址探针单点（[`Self::ensure_split_by_hash`] 对外唯一收口
  /// 形态，体形照 [`crate::ttl`] 的 `has_ttl_key_unprotected` 既有单机制与
  /// `read.rs:read_probe` 点读样板：先协同迁移键所在分块，再重新采样最新活跃
  /// 索引探针，严格对标 C# InternalRead.cs:70-73 入口铁律「先协同、后探针」）
  ///
  /// 供扫描族（SCAN / KEYS / DBSIZE / GETKEYSINSLOT / COUNTKEYSINSLOT 共用的
  /// 活键判定）等纯探针消费面使用，杜绝扩容进行期对未迁分块新桶采得 None 被
  /// 误判链首不符剔除（SCAN 全程漏键被游标固化不可恢复）——点读/TTL 面早已
  /// 协同，扫描面同经本单点后多路径同构。非扩容期仅多一次 old_index 判空原子
  /// load（与点读面同价）。
  ///
  /// 迁移内核错误（溢出桶耗尽 / 环形 Cycle 等）显式上抛，调用方严禁折成
  /// Dead/None 假剔除（§35 口径折叠慢路径存储错误帧）。
  #[inline]
  pub fn find_tag_cooperative(&self, key: &[u8]) -> Result<Option<u64>> {
    let hash = whasher::fast_hash(key);
    self.ensure_split_by_hash(hash)?;
    Ok(self.store.index.load().find_tag_by_hash(hash))
  }

  /// 快速探测标签键是否存在于哈希索引（带纪元保护）
  #[inline(always)]
  pub(crate) fn has_tag_key(&self, key: &[u8]) -> Result<bool> {
    let _guard = self.enter_gated();
    self.has_tag_key_unprotected(key)
  }

  /// 快速探测标签键是否存在于哈希索引（在已有纪元保护下）
  #[inline(always)]
  pub(crate) fn has_tag_key_unprotected(&self, key: &[u8]) -> Result<bool> {
    Ok(self.find_tag_cooperative(key)?.is_some())
  }

  /// 进入批处理纪元保护上下文（严格对标 libs/storage/Tsavorite/cs/src/core/ClientSession/IUnsafeContext.cs:BeginUnsafe）
  ///
  /// 接口的两个实现类同挂此处（rust 以守卫 RAII 一臂承接，EndUnsafe 即守卫 Drop）：
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/UnsafeContext.cs:BeginUnsafe
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalUnsafeContext.cs:BeginUnsafe
  #[inline]
  pub fn enter_batch(&self) -> BatchStoreSession<'_, D> {
    let guard = self.enter_gated();
    BatchStoreSession {
      session: self,
      _guard: guard,
    }
  }

  /// 获取关联存储引擎引用
  #[inline]
  pub fn store(&self) -> &Arc<WedbStore<D>> {
    &self.store
  }

  /// 获取关联纪元参与者引用
  #[inline]
  pub fn participant(&self) -> &Participant {
    &self.participant
  }

  /// 创建一致读会话上下文（对标 libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs）
  #[inline]
  pub fn consistent_read<'a, F: ConsistentReadFunctions + ?Sized>(
    &'a self,
    functions: &'a F,
  ) -> ConsistentReadContext<'a, D, F> {
    ConsistentReadContext::new(self, functions)
  }

  /// 推进键的 WATCH 版本（写面收口内核，引擎级钩子为空时零开销旁路，内部转发 WatchVersionMap::increment_version）
  ///
  /// C# Post RMW 回调族头部的无条件版本推进单点：
  /// libs/server/Storage/Functions/MainStore/RMWMethods.cs:PostCopyUpdater
  /// （与 PostInitialUpdater/PostCopyUpdater(Unified) :119 同序——CAS 挂链成功后
  /// 恰一次 IncrementVersion；RIPROMOTE/向量指针转移臂由 range_index promote
  /// 与 vector_manager 各自承接，不在本内核射程）
  ///
  /// 归属维度收口（双轨分置：版本轨=逻辑域、锁轨=物理域）：本内核单次读取
  /// [`Self::session_logical_prefix`]（`namespace()`/`active_db()` 逻辑真值的
  /// 字节投影），连同用户键交钩子——版本表槽位只随逻辑域定址，不随
  /// FLUSHDB/FLUSHNS/SWAPDB 换号换代，换号后同逻辑键写入的 bump 与在途
  /// WATCH 冻结槽恒命中（对位 C# 每库版本表实例终身持有、改后写必命中同槽
  /// 必 abort，libs/server/GarnetDatabase.cs:156）；跨租户/跨库正交性由逻辑
  /// ns/db 入种子保持，杜绝 C# 每库独持版本表所不存在的跨租户/跨库串扰面
  #[inline]
  pub(crate) fn bump_watch_version(&self, user_key: &[u8]) {
    if let Some(hook) = self.store.watch_hook.get() {
      hook.call(self.session_logical_prefix().as_slice(), user_key);
    }
  }

  /// DbMeta 换号批同步提交内核（固定根域前缀，批内按 safe-order 单点落盘）
  ///
  /// 与 [`Self::persist_dbmeta_batch`] 同键布局；返回需降级异步回放的项下标：
  /// 首条记录即遭遇环形页翻转（整批零写入落地），或第 k 条翻页（前 k 条已
  /// 落地），从 k 起其后全部记录一并转异步回放，批内相对顺序与 safe-order
  /// 一致（绝不让旧下标记录越过降级点先行落盘）。引擎硬错误经 `?` 传播：
  /// 此前已落地的批前缀不回滚，崩溃前缀语义保证最坏旧域泄漏，绝不复活撞号
  pub fn try_persist_dbmeta_sync(&self, items: &[Option<DbMetaRecord>]) -> Result<Vec<usize>> {
    let _guard = self.enter_gated();
    let mut degraded = Vec::new();
    for (idx, rec) in items.iter().enumerate() {
      let Some(rec) = rec else { continue };
      // 一旦有记录降级，其后记录直接并入回放序列，保持批内落盘顺序
      if !degraded.is_empty() {
        degraded.push(idx);
        continue;
      }
      match self.try_upsert_tag_sync_unprotected_with_prefix(
        ROOT_DBMETA_PREFIX.as_slice(),
        rec.key().as_slice(),
        KeyTag::DbMeta,
        rec.value().as_slice(),
      )? {
        Ok(_address) => {}
        Err(_page_id) => degraded.push(idx),
      }
    }
    Ok(degraded)
  }

  /// 原子批持久化 DbMeta 换号记录（doc/zh/db.md「即时原子提交」写面单点）
  ///
  /// items 顺序即落盘 safe-order（新映射 → 旧域退役墓碑 → 分配水位 0x05），
  /// 批内不合并不重排。换号事务全程持 [`WedbStore::lock_dbmeta`]，杜绝并发
  /// 换号事务的记录流交错。同步快路径遇页翻转即降级异步回放，回放逐项 await
  /// 完成后才向调用方返回，命令应答即含全部记录持久化承诺
  pub async fn persist_dbmeta_batch(&self, items: &[Option<DbMetaRecord>]) -> Result<()> {
    let degraded = self.try_persist_dbmeta_sync(items)?;
    if degraded.is_empty() {
      return Ok(());
    }
    log::warn!(
      "DbMeta 换号批同步快路径翻页，{} 项降级异步回放",
      degraded.len()
    );
    for idx in degraded {
      let Some(rec) = items[idx].as_ref() else {
        continue;
      };
      let rec_k = Self::session_tag_key_with_prefix(
        ROOT_DBMETA_PREFIX.as_slice(),
        KeyTag::DbMeta,
        rec.key().as_slice(),
      );
      self
        .upsert_raw(rec_k.as_slice(), rec.value().as_slice())
        .await?;
    }
    Ok(())
  }

  /// 持久化单条 DbMeta 系统元数据（原子批退化形态，safe-order 仅剩本条）
  pub async fn persist_dbmeta(&self, rec: &DbMetaRecord) -> Result<()> {
    self.persist_dbmeta_batch(&[Some(*rec)]).await
  }

  /// 物理删除 DbMeta 系统元数据（快路径优先同步删除，翻页/等待时自动回退异步删除）
  ///
  /// 删除对象固定根域前缀（与 persist 批同布局），不随会话活跃上下文漂移
  pub async fn delete_dbmeta(&self, key: &[u8]) -> Result<()> {
    let deleted = {
      let _guard = self.enter_gated();
      self.try_delete_tag_sync_unprotected_with_prefix(
        ROOT_DBMETA_PREFIX.as_slice(),
        key,
        KeyTag::DbMeta,
      )?
    };
    if deleted.is_err() {
      let rec_k =
        Self::session_tag_key_with_prefix(ROOT_DBMETA_PREFIX.as_slice(), KeyTag::DbMeta, key);
      self.delete_raw(&rec_k).await?;
    }
    Ok(())
  }
}

impl<D: Device> Drop for StoreSession<D> {
  fn drop(&mut self) {
    // 会话析构即解绑租户路由快照：引用归零登记空闲析构期限，GC 轮次到期
    // 摘除释放（连接断开触发租户内存回收的入口；根域免计数）
    let vns = self.bound_vns.load(Relaxed);
    if vns != 0 {
      self.store.vdb.unbind_route(vns, self.route_idle_ms());
    }
  }
}

/// 惰建复用的会话槽位（后台专用扫描会话缓存）
///
/// 对标 Garnet 专用扫描 StorageSession（`KeyspaceScanStorageSession` +
/// `KeyspaceScanLock` / StoreExpiredKeyDeletionDbStorageSession）：取用-归还
/// 两段式，以所有权取还替代在互斥守卫内跨 await；槽位为空或被并发取走时懒建
/// 新会话，异常路径丢弃由下次取用重建。
pub(crate) struct SessionSlot<D: Device> {
  slot: Mutex<Option<StoreSession<D>>>,
}

impl<D: Device> SessionSlot<D> {
  /// 创建空槽位
  pub(crate) const fn new() -> Self {
    Self {
      slot: Mutex::new(None),
    }
  }

  /// 取出（或懒建）专用会话；用毕须 [`Self::restore`](Self::restore) 归还
  pub(crate) fn take(&self, store: &Arc<WedbStore<D>>) -> Result<StoreSession<D>> {
    match self.slot.lock().take() {
      Some(s) => Ok(s),
      None => store.new_session(),
    }
  }

  /// 归还专用会话
  pub(crate) fn restore(&self, session: StoreSession<D>) {
    *self.slot.lock() = Some(session);
  }
}

/// 批处理会话上下文（严格对标 C# Garnet IUnsafeContext 与 UnsafeContext）
///
/// 在处理网络流水线（Pipeline）批量命令时，外层仅进入并持有一次纪元保护，
/// 批处理期间的所有内存直读完全跳过原子 enter/exit，
/// 将纪元保护开销降至绝对零，极大释放多核高并发吞吐。
pub struct BatchStoreSession<'a, D: Device> {
  pub session: &'a StoreSession<D>,
  _guard: EpochGuard<'a>,
}

impl<'a, D: Device> Deref for BatchStoreSession<'a, D> {
  type Target = StoreSession<D>;

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    self.session
  }
}

impl<'a, D: Device> BatchStoreSession<'a, D> {
  /// 挂起本批会话的纪元保护窗口（返回的守卫 Drop 时按原重入深度自动重入）
  ///
  /// 对标 C# Tsavorite 长 I/O 临界区的 epoch.UnsafeSuspendThread / ResumeThread
  /// 协议（libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs 的 OnPagesClosed），
  /// 与会话内前台驱逐窗口的挂起同一内核 wepoch 的 EpochSuspendGuard，全仓只此一处分发。
  ///
  /// C# 上下文层 UnsafeSuspendThread 入口族在本 rust 单点的折叠映射：
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/BasicContext.cs:UnsafeSuspendThread
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/ClientSession.cs:UnsafeSuspendThread
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/SessionFunctionsWrapper.cs:UnsafeSuspendThread
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalContext.cs:UnsafeSuspendThread
  ///
  /// 用途：批会话存活期内需要驱动「自带纪元排空屏障」的存储级动作（副本重放
  /// 检查点臂即其一）时，必须先解除自钉再动手——屏障谓词按全局最旧保护纪元
  /// 判定，批守卫不解除即把该谓词钉死为永假。调用点约定：只在两条记录之间、
  /// 上一次会话操作已完整落库（记录追加 + 索引插入闭环）处挂起，绝不在单条
  /// 操作中途挂起（那才是「页已刷、索引未插」的丢失更新窗口）。
  #[inline]
  pub fn suspend_epoch(&self) -> EpochSuspendGuard<'_> {
    EpochSuspendGuard::new(self.session.participant())
  }

  /// 批会话纪元让步（瞬时挂起窗：立即挂起并即刻按重入深度重入）
  ///
  /// [`Self::suspend_epoch`] 的零持有形态——守卫构造即弃：按当前重入深度逐层
  /// 退出保护区（会话槽位纪元位清 0，`compute_safe_to_reclaim` 扫描不再被本
  /// 会话钉死），随即按原深度重入（首层经 `enter_with_tid` 现场 CAS 公布
  /// **最新**全局纪元，补上重入臂不刷新公布纪元的缺口）。
  ///
  /// 用途：批守卫必须整轮在场（批内同步快路径依赖保护区前提）而批间存在天然
  /// 记录边界的长周期消费面——AOF 逐记录重放即其一：每条记录处理入口调用一次，
  /// 在两条记录之间（上一条已完整落库）给排空屏障一个确定性让步窗，对标 C#
  /// 重放会话逐记录常规 context 的 enter/exit（Tsavorite 重放不经 UnsafeContext
  /// 持整轮保护）。调用点约定与 [`Self::suspend_epoch`] 相同：只在两条记录
  /// 之间让步，绝不在单条操作中途。
  #[inline]
  pub fn epoch_yield(&self) {
    drop(self.suspend_epoch());
  }

  /// 创建一致读会话上下文（对标 libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs）
  #[inline]
  pub fn consistent_read<'b, F: ConsistentReadFunctions + ?Sized>(
    &'b self,
    functions: &'b F,
  ) -> ConsistentReadContext<'b, D, F> {
    ConsistentReadContext::new(self.session, functions)
  }

  /// 纯同步快速路径写入当前会话普通字符串键（严格对标 C# UnsafeContext 的 SET 快路径）
  ///
  /// 语义与 [`StoreSession::try_upsert_sync`] 完全一致且零 enter() 原子开销：
  /// - `Ok(Ok(addr))`：纯内存写入成功（原位更新 / 复活 / 盲追加）；
  /// - `Ok(Err(page_id))`：环形缓冲区翻转（精确 page_id）或 TTL 清除需异步闭环
  ///   （`u64::MAX`），调用方须先 drop 本守卫再降级 `upsert().await`，随后可重回批处理。
  #[inline(always)]
  pub fn try_upsert_sync(&self, key: &[u8], val: &[u8]) -> Result<StdResult<u64, u64>> {
    self.session.try_upsert_sync_unprotected(key, val)
  }

  /// 纯同步快速路径写入当前会话指定标签物理键（零 enter() 原子开销）
  ///
  /// 语义与 [`StoreSession::try_upsert_tag_sync`] 完全一致，语义细节见
  /// [`StoreSession::try_upsert_tag_sync_unprotected`]
  #[inline(always)]
  pub fn try_upsert_tag_sync(
    &self,
    key: &[u8],
    tag: KeyTag,
    val: &[u8],
  ) -> Result<StdResult<u64, u64>> {
    self.session.try_upsert_tag_sync_unprotected(key, tag, val)
  }

  /// 对象信封单次成形快写（零 enter() 原子开销，批处理热路径专用）
  ///
  /// 语义与 [`StoreSession::try_upsert_envelope_sync_fill`] 完全一致（批处理
  /// 纪元已由本守卫持有，直呼 unprotected 内核），返回值语义与其文档一致
  #[inline(always)]
  pub fn try_upsert_envelope_sync_fill(
    &self,
    key: &[u8],
    obj_tag: u8,
    payload: &[u8],
  ) -> Result<StdResult<u64, u64>> {
    let rec_k = self.session.session_tag_key(KeyTag::ObjectEnvelope, key);
    self
      .session
      .try_upsert_envelope_sync_fill_with_prefix(key, &rec_k, obj_tag, payload)
  }

  /// 纯同步快速条件写入当前会话普通字符串键（NX 语义：仅当键不存在时原子写入）
  #[inline(always)]
  pub fn try_insert_sync(&self, key: &[u8], val: &[u8]) -> Result<StdResult<bool, u64>> {
    // 普通字符串即 String 标签特例，收口至同型带标签单源（对位 raw/write 各
    // 同步口的 String→tag 转发形态），杜绝前缀外提样板的第二套实现
    self.try_insert_tag_sync(key, KeyTag::String, val)
  }

  /// 纯同步快速条件写入当前会话指定标签物理键（NX 语义）
  #[inline(always)]
  pub fn try_insert_tag_sync(
    &self,
    key: &[u8],
    tag: KeyTag,
    val: &[u8],
  ) -> Result<StdResult<bool, u64>> {
    let prefix = self.session_prefix();
    self
      .session
      .try_insert_tag_sync_unprotected_with_prefix(prefix.as_slice(), key, tag, val)
  }

  /// 纯同步快速路径删除当前会话指定标签物理键（零 enter() 原子开销）
  #[inline(always)]
  pub fn try_delete_tag_sync(&self, key: &[u8], tag: KeyTag) -> Result<StdResult<bool, u64>> {
    self.session.try_delete_tag_sync_unprotected(key, tag)
  }

  /// 同步读当前会话普通字符串键快路径（TTL 快门控 + 内存直读，零 enter() 原子开销）
  ///
  /// 返回三态见 [`StoreResult`]：
  /// - `Success(r)`：内存命中，闭包零拷贝消费；
  /// - `NotFound`：内存中明确不存在（无候选 / 墓碑 / TTL 已到期）；
  /// - `RecordOnDisk`：须降级全异步 `read_with().await`（数据或 TTL 记录存在磁盘候选，
  ///   `check_expired` 含磁盘路径与物理清除，绝不跨纪元 await）。
  #[inline(always)]
  pub fn try_read_sync<R>(&self, key: &[u8], f: impl FnOnce(&[u8]) -> R) -> Result<StoreResult<R>> {
    self.session.try_read_sync_unprotected(key, f)
  }

  /// 同步读当前会话指定标签物理键快路径并披露记录物理尺寸（TTL 同栈门裁决 + 内存直读）
  ///
  /// MEMORY USAGE 统计内核：[`Self::try_read_tag_sync`] 的带尺寸对位，三态语义一致
  #[inline(always)]
  pub fn try_read_tag_sync_with_size<R>(
    &self,
    key: &[u8],
    tag: KeyTag,
    f: impl FnOnce(&[u8], usize) -> R,
  ) -> Result<StoreResult<R>> {
    self.session.try_read_tag_sync_with_size(key, tag, f)
  }

  /// 同步读当前会话指定标签物理键快路径（TTL 同栈门裁决 + 内存直读，零 enter() 原子开销）
  ///
  /// 返回三态与 [`Self::try_read_sync`] 一致；TTL 门控按用户键同栈裁决
  /// （无 TTL / 未到期放行，已到期快路径 NOTFOUND），与数据记录标签无关
  #[inline(always)]
  pub fn try_read_tag_sync<R>(
    &self,
    key: &[u8],
    tag: KeyTag,
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<StoreResult<R>> {
    self.session.try_read_tag_sync_unprotected(key, tag, f)
  }

  /// 纯同步快速路径物理删除当前会话普通字符串键（零 enter() 原子开销）
  #[inline(always)]
  pub fn try_delete_sync(&self, key: &[u8]) -> Result<StdResult<bool, u64>> {
    self.session.try_delete_sync_unprotected(key)
  }

  /// 写入或更新键值对
  #[inline(always)]
  pub async fn upsert(&self, key: &[u8], val: &[u8]) -> Result<u64> {
    self.session.upsert(key, val).await
  }

  /// 读取键值对
  #[inline(always)]
  pub async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    self.session.read(key).await
  }

  /// 删除键值对
  #[inline(always)]
  pub async fn delete(&self, key: &[u8]) -> Result<bool> {
    self.session.delete(key).await
  }

  /// 异步读取用户键的 TTL 并判定在指定 ticks 是否已过期（无 TTL 视同未过期）
  #[inline(always)]
  pub async fn is_expired_at(&self, user_key: &[u8], now: i64) -> Result<bool> {
    self.session.is_expired_at(user_key, now).await
  }

  /// 批量读取当前会话普通字符串记录（12 项流水线预取）
  #[inline(always)]
  pub async fn read_batch_with<K, F>(&self, keys: &[K], on_item: F) -> Result<()>
  where
    K: AsRef<[u8]>,
    F: FnMut(usize, Option<&[u8]>),
  {
    self.session.read_batch_with(keys, on_item).await
  }

  /// 纯内存批量直读当前会话普通字符串记录（零堆分配与零异步开销）
  #[inline(always)]
  pub fn try_read_batch_in_memory<K, F>(&self, keys: &[K], on_item: F) -> Result<()>
  where
    K: AsRef<[u8]>,
    F: FnMut(usize, Option<&[u8]>),
  {
    self.session.try_read_batch_in_memory(keys, on_item)
  }

  /// 触发对象 RMW 增量日志通知（AOF 入队失败沿写路径上抛拒绝该命令）
  #[inline(always)]
  pub fn notify_object_rmw(&self, notif: &ObjectRmwNotification<'_>) -> Result<()> {
    self
      .store
      .notify_object_rmw(self.session.aof_session_id, notif)
  }

  /// 触发分层稳态写命令镜像通知（AOF 入队失败沿写路径上抛拒绝该命令）
  #[inline(always)]
  pub fn notify_tiered_collection_write(
    &self,
    notif: &TieredCollectionNotification<'_>,
  ) -> Result<()> {
    self
      .store
      .notify_tiered_collection_write(self.session.aof_session_id, notif)
  }

  /// 触发对象信封整值写通知（AOF 入队失败沿写路径上抛拒绝该命令）
  #[inline(always)]
  pub fn notify_envelope_upsert(&self, key: &[u8], val: &[u8]) -> Result<()> {
    self
      .store
      .notify_envelope_upsert(self.session.aof_session_id, key, val)
  }

  /// 推进键的 WATCH 版本（批处理上下文的写面收口出口，供存储层
  /// TTL 同步快路径等旁路物理键原语的调用方按用户键显式推进，内部转发 bump_watch_version）
  #[inline(always)]
  pub fn bump_watch_version(&self, user_key: &[u8]) {
    self.session.bump_watch_version(user_key);
  }

  /// 纯同步快速路径物理删除当前会话普通字符串键的显式前缀变体（循环前缀外提
  /// 对位，语义与 [`Self::try_delete_sync`] 完全一致且零 enter() 原子开销；
  /// rust 工程优化无 c# 对应）
  #[inline(always)]
  pub fn try_delete_sync_with_prefix(
    &self,
    prefix: &[u8],
    key: &[u8],
  ) -> Result<StdResult<bool, u64>> {
    self
      .session
      .try_delete_sync_unprotected_with_prefix(prefix, key)
  }

  /// 批量同步快速路径写入当前会话普通字符串键（批量接口单次折叠，transpile
  /// SKILL 工程准则；rust 工程优化无 c# 对应）
  ///
  /// 单次折叠：纪元守卫复用本上下文外层持有（循环零 enter() 原子开销）、
  /// 会话前缀单次外提（循环零 ns/db 原子变量重读与 Varint 重算）、借用对
  /// 携带命令序下标排序（键序优先、同键按下标升序的全序，仅重排引用，零 KV
  /// 拷贝）后顺序写入——同键相邻、去重保末值恒为命令序末值
  /// （MSET 重复键后者胜语义，先例 ri_set_batch），批量集中命中相邻索引桶
  /// 压降探针缓存缺失。逐键 WATCH 推进与降级信号语义与逐键
  /// [`Self::try_upsert_sync`] 循环等价：任一键遇环形页翻转 / TTL 清退
  /// 异步闭环信号（`Err(page_id)`）立即整体返回，已写键保持（调用方降级
  /// 慢路径整命令重放幂等），剩余键不写
  ///
  /// 折叠先例对标 C# MainStoreOps.cs:MSET_Conditional（全键排他锁内批量
  /// SET）；rust 主存储为 compio 每核单线程 + epoch 无锁写，无条带锁可
  /// 分组，折叠收益为纪元/前缀/编码单次化与写入局部性（注释声明与条目
  /// 「按条带锁分组」的差异）
  ///
  /// 调用契约：批量盲写无地址复验，命令层调用方须以键组读改写窗口
  /// （`try_rmw_window_sorted`）覆盖全部键（快路径 network_mset 先例，
  /// 票 zcode-r32-rmwmatrix 立项一）
  #[inline]
  pub fn try_upsert_batch_sync<I, K, V>(&self, pairs: I) -> Result<StdResult<(), u64>>
  where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
  {
    let prefix = self.session.session_prefix();
    self.try_upsert_batch_sync_with_prefix(prefix.as_slice(), pairs)
  }

  /// 批量同步快速路径写入当前会话普通字符串键的显式前缀变体（支持复用外层已计算好的会话前缀）
  pub fn try_upsert_batch_sync_with_prefix<I, K, V>(
    &self,
    prefix_slice: &[u8],
    pairs: I,
  ) -> Result<StdResult<(), u64>>
  where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
  {
    // 栈上固定容量缓冲承接小批次排序（≤8 元素零堆分配，超限自动溢出至堆）；
    // enumerate 携带命令序下标，与借用对同为栈上三元组，零拷贝不变
    let mut sorted: SmallVec<[(K, V, usize); 8]> = pairs
      .into_iter()
      .enumerate()
      .map(|(i, (k, v))| (k, v, i))
      .collect();
    // 键序优先、同键按命令序下标升序，构成无相等元素的全序；sort_unstable_by
    // 在全序下结果确定，纯键序比较器不承诺相等键相对顺序的缺陷就此封堵
    sorted.sort_unstable_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()).then_with(|| a.2.cmp(&b.2)));
    let mut iter = sorted.into_iter().peekable();
    while let Some((k, v, _)) = iter.next() {
      // 相邻去重保末值：全序排序后同键末位恒为命令序末值（MSET 后者胜语义，
      // 对标 C# ArrayCommands.cs:NetworkMSET 与 MainStoreOps.cs:MSET_Conditional
      // 按命令序逐对 SET）
      if iter
        .peek()
        .is_some_and(|(next_key, ..)| next_key.as_ref() == k.as_ref())
      {
        continue;
      }
      match self.session.try_upsert_tag_sync_unprotected_with_prefix(
        prefix_slice,
        k.as_ref(),
        KeyTag::String,
        v.as_ref(),
      )? {
        Ok(_) => {}
        // 环形页翻转 / TTL 清退异步闭环：立即整体降级（page_id 为首个触发键）
        Err(page_id) => return Ok(Err(page_id)),
      }
    }
    Ok(Ok(()))
  }
}
