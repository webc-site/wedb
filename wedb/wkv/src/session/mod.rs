//! 客户端会话层（对标 C# Garnet Tsavorite cs/src/core/ClientSession/ClientSession.cs）
//!
//! 拆分（纯移动，行为语义不变）：
//! - `mod.rs`：会话核心——StoreSession 定义、纪元参与者生命周期、
//!   context（ns/db）管理与 enter_batch（对标 ClientSession 的会话主体）；
//! - [`batch`]：批处理会话上下文 BatchStoreSession（对标 IUnsafeContext 与
//!   UnsafeContext 的批量快路径面）；
//! - [`hooks`]：引擎实例级钩子族（WatchHook/DeleteMissHook/EngineHookSlots 与
//!   WATCH 版本写面收口，对标 Tsavorite Allocator 单委托字段与 Garnet
//!   记录触发器挂点）；
//! - [`dbmeta`]：DbMeta 换号记录持久化批（doc/zh/db.md「即时原子提交」写面单点）；
//! - [`raw`]：纯引擎 KV 面——一切 `*_raw` 物理键操作、unprotected 变体与批量读
//!   （对标 ClientSession 的 Upsert/Read/Delete 快慢路径）；
//! - [`keys`]：键编码域——会话前缀物理键纯函数（对标 C# StorageSession 的键编码）；
//! - [`collection`]：集合元数据与紧凑编码操作（对标 C# StorageSession/MainObjectStore
//!   的元数据与分块存储）。

use std::ptr::eq;

use crate::vdb::DbMetaRecord;
mod batch;
mod collection;
pub mod consistent_read;
mod dbmeta;
mod hooks;
mod keys;
mod raw;
mod rmw_window;
mod swap;
mod vector_cleanup;

use std::sync::{
  Arc,
  atomic::{
    AtomicBool, AtomicU64,
    Ordering::{Acquire, Relaxed},
  },
};

pub use batch::BatchStoreSession;
pub use consistent_read::{ConsistentReadContext, ConsistentReadFunctions};
#[cfg(debug_assertions)]
#[doc(hidden)]
pub use hooks::TEST_COLD_WINDOW_HOOK;
pub use hooks::{DeleteMissHook, EngineHookSlots, WatchHook};
use parking_lot::Mutex;
pub(crate) use raw::CopyToTailOutcome;
pub use raw::{
  RmwGrow,
  read::{RecordRead, StoreResult},
};
pub(crate) use rmw_window::{INNER_LATCH_RETRY_BUDGET, SessionLockingState};
pub use rmw_window::{
  RMW_KEY_LATCH_ATTEMPTS, RMW_PLAN_ACQUIRE_MISS, RMW_PLAN_BUILD_COUNT, RMW_PLAN_PINNED_INDEX,
  RmwWindow, SessionLocking,
};
#[doc(hidden)]
pub use vector_cleanup::{CONTEXT_TERM_MASK, matches_vector_context};
use wdev::Device;
use wepoch::{EpochGuard, Participant};
use wval::SessionPrefixBuf;

use crate::{error::Result, store::WedbStore};

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
  ///   [`crate::vdb::VirtualDbManager::get_virtual_ids_with_created`] 刷新缓存槽，命中
  ///   新分配即按 set_context 同款 safe-order 组批落 DbMeta 映射（零 await
  ///   同步臂）。
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
    let (vns, vdb, new_ns, new_db) = self.store.vdb.get_virtual_ids_with_created(ns, db);
    self.active_vns.store(vns, Relaxed);
    self.active_vdb.store(vdb, Relaxed);
    self.last_generation.store(current_gen, Relaxed);
    // 重解析新分配即落盘（组批形态与豁免判据同 [`Self::set_context`]：同步域
    // 零 await，仅原子批同步尝试；根域 (ns 0, db 0) 恒不落）——换代后存量会话
    // 的下一条写经本臂给 (new_vns, db) 盲分配新号，若批不落则确认写随常规
    // 重启蒸发、0x05 水位不抬升致旧号复用跨域幽灵（doc/zh/db.md 清库隔离与
    // 「重启自动恢复映射表」承诺面）。降级/硬错误 warn 口径与 set_context
    // 同文（tolerated 旧域泄漏登记见 doc/zh/deviations.md）
    if (new_ns && ns != 0) || (new_db && (ns != 0 || db != 0)) {
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
  /// 表非权威全量，见 [`crate::vdb::VirtualDbManager::is_cold_db`]）时返回 false，不改
  /// 会话任何状态、不分配不持久化——冷租户既有映射以磁盘 DbMeta 为权威，
  /// 须先经 [`WedbStore::resolve_context`] 点查装载再重放本调用（点查未命中
  /// 即真新库，由解析面创建并持久化）。ns 未在册即全新租户，同步创建零挂起。
  ///
  /// **bool 返回值是调用方的强制分叉契约，严禁丢弃**：false 双臂（冷检门与
  /// 下方绑定复核门）均先于一切标量存储返回——装载成功不蕴含重放物化成功，
  /// 重放判点（装载回建快照无绑定方时）可遭空闲析构引擎摘除快照
  ///（`route_idle_evict_secs=0` 即引用归零即期的合法稳态）二次致冷。调用方
  /// 无视返回值直接产出成功应答即撕裂：外层会话镜像（认证租户 / 活跃库 /
  /// ACL 句柄）物化为新租，内层六标量与本物理域仍锚旧租，外层报新租、读写
  /// 穿透旧租存储域，破坏多租户隔离防线且析构不推进换代纪元、撕裂不可自愈。
  /// 协议层消费方（wnode 停泊臂 `park_cold_context_load`）false 即回存储
  /// 错误帧交失败通道弃暂存，与本仓对照臂
  ///（wnode core.rs `try_switch_active_database_session` 的 `!set_context`
  /// 分叉暂存）同口径，对标 C# `TryGetOrSetDatabaseSession` success 门。
  ///
  /// 冷检与解析之间的空闲析构窗口封堵：解析前先经 [`crate::vdb::VirtualDbManager::bind_route`]
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
    #[cfg(debug_assertions)]
    let hook = TEST_COLD_WINDOW_HOOK.lock().take();
    #[cfg(debug_assertions)]
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
  /// [`WedbStore::register_bftree_key`] 文档。死亡域守卫拒绝（域已退役）时
  /// 刚建树经 [`WedbStore::destroy_dead_domain_tree`] 异步卸载销毁（本入口
  /// 为 async fn，销毁的条带停车档离核由本层 await 承接）。
  #[inline]
  pub(crate) async fn register_bftree_key(&self, key: &[u8]) {
    let (vns, vdb) = self.virtual_domain();
    if self.store.register_bftree_key(vns, vdb, key) {
      return;
    }
    self.store.destroy_dead_domain_tree(vns, vdb, key).await;
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
  /// 对位 C# SplitIndex 的 SplitBuckets 分裂件（同名真身锚留 store/resize.rs 的
  /// split_buckets 一处）
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
