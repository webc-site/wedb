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

use crate::{
  error,
  session::consistent_read::{key_hash, single_key_around},
  vdb::{DbMetaRecord, ROOT_DBMETA_PREFIX},
};
mod collection;
pub mod consistent_read;
mod keys;
mod raw;
mod rmw_window;
mod swap;

use std::{
  ops::Deref,
  result::Result as StdResult,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
  },
};

pub use consistent_read::{ConsistentReadContext, ConsistentReadFunctions};
use parking_lot::Mutex;
pub(crate) use raw::CopyToTailOutcome;
pub use raw::read::{RecordRead, StoreResult};
pub(crate) use rmw_window::SessionLockingState;
pub use rmw_window::{RmwWindow, SessionLocking, SessionLockingGuard};
use wdev::Device;
use wepoch::{EpochGuard, Participant};
use wval::{KeyTag, SessionPrefixBuf};

use crate::{
  error::Result,
  store::{ObjectRmwNotification, WedbStore},
};

/// WATCH 版本推进钩子（收用户键；经单态化函数指针擦除宿主上下文类型）
///
/// 对标 C# functionsState.watchVersionMap（libs/server/Transaction/
/// WatchVersionMap.cs）与存储 functions 面的共享装配：每个写面在完成实际
/// 写入后回调一次（MainStore/UnifiedStore/ObjectStore 三套 UpsertMethods/
/// RMWMethods/DeleteMethods 的 InPlaceUpdater/PostInitialWriter/
/// InitialUpdater/InitialDeleter/PostCopyUpdater 挂点语义）
pub struct WatchHook {
  raw: *const (),
  call_fn: fn(*const (), &[u8]),
  drop_fn: unsafe fn(*const ()),
}

unsafe impl Send for WatchHook {}
unsafe impl Sync for WatchHook {}

impl Drop for WatchHook {
  #[inline]
  fn drop(&mut self) {
    unsafe {
      (self.drop_fn)(self.raw);
    }
  }
}

impl WatchHook {
  /// 由任意线程安全上下文及静态处理函数构造分发器
  pub fn new<T: Send + Sync + 'static>(ctx: Arc<T>, handler: fn(&T, &[u8])) -> Self {
    struct State<T> {
      ctx: Arc<T>,
      handler: fn(&T, &[u8]),
    }

    fn call_impl<T>(ptr: *const (), key: &[u8]) {
      let state = unsafe { &*(ptr as *const State<T>) };
      (state.handler)(&state.ctx, key);
    }

    unsafe fn drop_impl<T>(ptr: *const ()) {
      unsafe {
        drop(Box::from_raw(ptr as *mut State<T>));
      }
    }

    let state = Box::new(State { ctx, handler });
    let raw = Box::into_raw(state) as *const ();

    Self {
      raw,
      call_fn: call_impl::<T>,
      drop_fn: drop_impl::<T>,
    }
  }

  /// 统一调用推进版本
  #[inline(always)]
  pub fn call(&self, key: &[u8]) {
    (self.call_fn)(self.raw, key);
  }
}

/// 用户键删除缺席观测钩子（收 (会话前缀, 用户键)；经单态化函数指针擦除宿主
/// 上下文类型，形态与 [`WatchHook`] 同源）
///
/// 对标 C# 记录触发器 libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnDispose
/// 的 `DisposeReason.Deleted` 臂：宿主把值域外的键存在态（如向量集登记表）单列于
/// 引擎之外，引擎用户键删除双域（String/ObjectEnvelope）判未命中时回调本钩子收口，
/// 命中即视同删除成功（计数与墓碑口径随存储删除单点统一，RESP 各臂不再各配第二套
/// 清退判据）。`false` = 宿主域亦无此键（缺席删除维持原口径）
pub struct DeleteMissHook {
  raw: *const (),
  call_fn: fn(*const (), &[u8], &[u8]) -> bool,
  drop_fn: unsafe fn(*const ()),
}

unsafe impl Send for DeleteMissHook {}
unsafe impl Sync for DeleteMissHook {}

impl Drop for DeleteMissHook {
  #[inline]
  fn drop(&mut self) {
    unsafe {
      (self.drop_fn)(self.raw);
    }
  }
}

impl DeleteMissHook {
  /// 由任意线程安全上下文及静态处理函数构造分发器
  pub fn new<T: Send + Sync + 'static>(ctx: Arc<T>, handler: fn(&T, &[u8], &[u8]) -> bool) -> Self {
    struct State<T> {
      ctx: Arc<T>,
      handler: fn(&T, &[u8], &[u8]) -> bool,
    }

    fn call_impl<T>(ptr: *const (), prefix: &[u8], key: &[u8]) -> bool {
      let state = unsafe { &*(ptr as *const State<T>) };
      (state.handler)(&state.ctx, prefix, key)
    }

    unsafe fn drop_impl<T>(ptr: *const ()) {
      unsafe {
        drop(Box::from_raw(ptr as *mut State<T>));
      }
    }

    let state = Box::new(State { ctx, handler });
    let raw = Box::into_raw(state) as *const ();

    Self {
      raw,
      call_fn: call_impl::<T>,
      drop_fn: drop_impl::<T>,
    }
  }

  /// 统一调用缺席观测：返回宿主域是否实际摘除此键
  #[inline(always)]
  pub fn call(&self, prefix: &[u8], key: &[u8]) -> bool {
    (self.call_fn)(self.raw, prefix, key)
  }
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
/// 幂等换代刷新。`copy_reads_to_tail` / `record_elision` 为读面配置位、
/// `strict_ctx` / `bound_vns` 为装配与析构面状态、`session_locking` 为本执行域
/// 命令分派单点写锁器模式位（读写皆在本域，无跨域并发），皆不在本纪律的六字段之列。
pub struct StoreSession<D: Device> {
  pub store: Arc<WedbStore<D>>,
  pub participant: Participant,
  pub copy_reads_to_tail: AtomicBool,
  pub record_elision: AtomicBool,
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
  /// 副本一致读会话附着态（对标 C# StorageSession.readSessionState 挂各
  /// SessionFunctions 的形态：libs/server/Storage/Session/StorageSession.cs:104-132；
  /// None = 无一致读协议，读路径零开销直通）。装配期一次性附着，其后只读
  read_session_state: Option<Arc<dyn ConsistentReadFunctions>>,
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
      record_elision: AtomicBool::new(false),
      namespace: AtomicU64::new(0),
      active_db: AtomicU64::new(0),
      active_vns: AtomicU64::new(0),
      active_vdb: AtomicU64::new(0),
      is_virtual: AtomicBool::new(false),
      last_generation: AtomicU64::new(0),
      strict_ctx: AtomicBool::new(false),
      bound_vns: AtomicU64::new(0),
      session_locking: SessionLockingState::new(),
      read_session_state: None,
    };
    session.set_context(0, 0);
    session
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

  /// 附着态一致读单键协议触发：pre 在读前（超时上抛中止），post 在读后推进；
  /// 未附着零开销直通（读漏斗单点触发面，对标 C# 一致读会话的
  /// PreSingleKeyConsistentRead/PostSingleKeyConsistentReadCallback 序列）。
  /// 多标签/多探重复触发为良性：post 单调推进，语义仍是「不超过读取时刻的
  /// 前缀上界」
  #[inline]
  pub fn with_session_consistent_read<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce() -> R,
  ) -> error::Result<R> {
    match self.read_session_state() {
      Some(fns) => single_key_around(fns.as_ref(), key_hash(user_key), f),
      None => Ok(f()),
    }
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

  /// 获取当前会话的 19 字节前缀缓冲（[`Self::virtual_domain`] 的字节投影）
  #[inline(always)]
  pub fn session_prefix(&self) -> SessionPrefixBuf {
    let (vns, vdb) = self.virtual_domain();
    SessionPrefixBuf::new(vns, vdb)
  }

  /// 会话当前**物理域** `(active_vns, active_vdb)` 读取单点
  ///
  /// 三口径合一，全仓物理前缀只出自本方法（[`Self::session_prefix`] 即其字节
  /// 投影，入账记录键、AOF 事件携带域、换号回收旁表登记域同源于此）：
  /// - `is_virtual` 为真（经 [`Self::set_virtual_context`] 直设，AOF 回放守卫、
  ///   内置 GC 死域清扫与过期键逐键删除共用该形态）即原样返回直设值，
  ///   零逻辑解析、零虚号分配、零 DbMeta 落盘；
  /// - 否则走逻辑 → 物理解析：代数命中读缓存，落后才经
  ///   [`VirtualDbManager::get_virtual_ids`] 刷新缓存槽。
  ///
  /// 代数快照顺序与 [`Self::set_context`] 同源且不可交换：**先**读当前
  /// 代数、**后**解析虚 ID，收尾把「解析前」读到的代数写回缓存槽。解析期间任何
  /// 换代（启动期 DbMeta 映射重建收尾的 bump、并发 FLUSHDB/FLUSHNS/SWAPDB）都会
  /// 让缓存槽留在落后一代，下一次本调用必走慢路径重解析；反之在解析之后再读
  /// 代数，会把「新代数 + 旧映射」钉进缓存槽，会话从此固着在磁盘映射已抛弃的
  /// 物理前缀上写数据（幽灵段）。代数与存储侧就绪门禁（见 [`WedbStore::open_shared`]）
  /// 是一对：门禁消掉重建窗口，代数兜住窗口外的换号收敛。
  #[inline(always)]
  pub fn virtual_domain(&self) -> (u64, u64) {
    if self.is_virtual.load(Relaxed) {
      return (self.active_vns.load(Relaxed), self.active_vdb.load(Relaxed));
    }
    let current_gen = self.store.vdb.generation.load(Relaxed);
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
  /// 代数、**后**解析虚 ID，收尾把「解析前」读到的代数写回缓存槽。解析期间任何
  /// 换代（启动期 DbMeta 映射重建收尾的 bump、并发 FLUSHDB/FLUSHNS/SWAPDB）都会
  /// 让缓存槽留在落后一代，下一次 [`Self::session_prefix`] 必走慢路径重解析；
  /// 反之在解析之后再读代数，会把「新代数 + 旧映射」钉进缓存槽，会话从此
  /// 固着在磁盘映射已抛弃的物理前缀上写数据（幽灵段）。代数与存储侧就绪门禁
  /// （见 [`WedbStore::open_shared`]）是一对：门禁消掉重建窗口，代数兜住窗口外
  /// 的换号收敛。
  ///
  /// 返回是否完成上下文物化：严格会话遇冷库（ns 在册而库映射未装载且路由
  /// 表非权威全量，见 [`VirtualDbManager::is_cold_db`]）时返回 false，不改
  /// 会话任何状态、不分配不持久化——冷租户既有映射以磁盘 DbMeta 为权威，
  /// 须先经 [`WedbStore::resolve_context`] 点查装载再重放本调用（点查未命中
  /// 即真新库，由解析面创建并持久化）。ns 未在册即全新租户，同步创建零挂起。
  /// 绑定租户路由快照（引用归零空闲析构协议），换 ns 时先绑新者后解旧者
  pub fn set_context(&self, ns: u64, db: u64) -> bool {
    if self.strict_ctx.load(Relaxed) && self.store.vdb.is_cold_db(ns, db) {
      return false;
    }
    self.is_virtual.store(false, Relaxed);
    self.namespace.store(ns, Relaxed);
    self.active_db.store(db, Relaxed);
    let generation = self.store.vdb.generation.load(Relaxed);
    let (vns, new_ns) = self.store.vdb.get_or_create_ns(ns);
    if new_ns {
      // 全新租户：路由表为本运行期权威全量，后续新库同步分配免点查甄别
      self.store.vdb.mark_route_authoritative(vns);
    }
    let (vdb, new_db) = self.store.vdb.get_or_create_db(ns, db);
    self.active_vns.store(vns, Relaxed);
    self.active_vdb.store(vdb, Relaxed);
    self.last_generation.store(generation, Relaxed);
    self.rebind_route(vns);

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

  /// 直设会话**物理域**（零解析、零分配、零落盘的物理入口）
  ///
  /// 与 [`Self::set_context`] 相对：本方法不做 logic_ns/logic_db → 虚号解析，
  /// 绝不调用 `get_or_create_*`、绝不写 DbMeta，故重放端按条目物理前缀落回
  /// 条目原域（doc/zh/db.md「从库完全继承主库的映射体系，不进行本地二次
  /// 映射」）。置 `is_virtual` 后 [`Self::virtual_domain`] 恒原样返回本对数值。
  #[inline]
  pub fn set_virtual_context(&self, vns: u64, vdb: u64) {
    self.is_virtual.store(true, Relaxed);
    self.active_vns.store(vns, Relaxed);
    self.active_vdb.store(vdb, Relaxed);
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

  /// 覆写本会话冷读晋升位初值（对标 C# Garnet CopyReadsToTail 的会话承载位）
  ///
  /// 真源在 `StoreConfig::copy_reads_to_tail`（store 级，wconf hlog 段经 wnode
  /// `apply_hlog_overrides` 单点投影），会话位装配期从真源取初值（[`Self::new`]）；
  /// 本覆写口生产零调用，仅测试域用于在同一存储上按会话覆写初值以覆盖两条臂
  #[inline]
  pub fn set_copy_reads_to_tail(&self, enable: bool) {
    self.copy_reads_to_tail.store(enable, Relaxed);
  }

  /// 获取当前是否开启冷读提升回 Tail
  #[inline]
  pub fn copy_reads_to_tail(&self) -> bool {
    self.copy_reads_to_tail.load(Relaxed)
  }

  /// 设置是否开启记录脱钩剔除回收（rust 自有开关；C# RevivificationSettings 无对应剔除标志）
  #[inline]
  pub fn set_record_elision(&self, enable: bool) {
    self.record_elision.store(enable, Relaxed);
  }

  /// 获取当前是否开启记录脱钩剔除回收
  #[inline]
  pub fn record_elision(&self) -> bool {
    self.record_elision.load(Relaxed)
  }

  /// 是否可能发生复活/脱钩类并发记录变更（决定 traceback 前是否加 ephemeral 桶锁）
  ///
  /// 对标 Helpers.cs FindOrCreateTagAndTryEphemeralXLock 的 "Ephemeral must lock the bucket before traceback" 协议；
  /// C# BasicSessionLocker 无条件加锁，此处等效裁剪为仅复活相关配置开启时加锁——复活功能全关时
  /// 记录槽位绝无被抽走的风险，免锁语义等价
  #[inline]
  pub fn ephemeral_lock_enabled(&self) -> bool {
    self.store.config.enable_revivification || self.record_elision()
  }

  /// 进入批处理纪元保护上下文（严格对标 libs/storage/Tsavorite/cs/src/core/ClientSession/IUnsafeContext.cs:BeginUnsafe）
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
  #[inline]
  pub(crate) fn bump_watch_version(&self, user_key: &[u8]) {
    if let Some(hook) = self.store.watch_hook.get() {
      hook.call(user_key);
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
    match deleted {
      Ok(_) => Ok(()),
      Err(_) => {
        let rec_k =
          Self::session_tag_key_with_prefix(ROOT_DBMETA_PREFIX.as_slice(), KeyTag::DbMeta, key);
        self.delete_raw(&rec_k).await?;
        Ok(())
      }
    }
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
    self.store.notify_object_rmw(notif)
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
  /// 排序（仅重排引用，零 KV 拷贝）后顺序写入——同键相邻、去重保末值
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
  pub fn try_upsert_batch_sync<I, K, V>(&self, pairs: I) -> Result<StdResult<(), u64>>
  where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
  {
    let prefix = self.session.session_prefix();
    let prefix_slice = prefix.as_slice();
    // 栈外单次借用对收集（批量路径允许一次分配，先例 ri_set_batch）
    let mut sorted: Vec<(K, V)> = pairs.into_iter().collect();
    sorted.sort_unstable_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
    let mut iter = sorted.into_iter().peekable();
    while let Some((k, v)) = iter.next() {
      // 相邻去重保末值：排序后同键相邻仅末值生效（MSET 后者胜语义）
      if iter
        .peek()
        .is_some_and(|(next_key, _)| next_key.as_ref() == k.as_ref())
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
