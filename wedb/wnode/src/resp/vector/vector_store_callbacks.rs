//! 磁盘存储回调实现（对标 C# VectorManager.Callbacks.cs）
//!
//! 将 DiskANN 向量图的向量数据、邻接表（图拓扑）、量化状态、属性和 ID 映射
//! 通过统一的 `[命名空间字节][键字节]` 物理键落盘到 wkv 存储引擎（Tsavorite 混合日志）。
//!
//! C# 批量读构件 [`VectorReadBatch`] 的两项逐次策略在本端口由引擎单点承接，
//! 故本文件不再自建同名概念：
//!   * 初读尺寸 `InitialIORecordSize`（同文件 :38-57，配合
//!     `SetActiveReadGeometry` :312-339 按项类型预算单 IO 尺寸）→ wkv 冷读侧的
//!     探针 + 精确二次读（whlog `hlog/mod.rs:36` 探针长度、
//!     `hlog/io.rs:141-165` 单记录绝不整页读），故 [`StoreCallbacks::read_multi`]
//!     不消费 `length_hint`；
//!   * 回填拷贝 `ReadCopyOptions`（同文件 :68-85，图内小记录
//!     NeighborList/QuantizedVector/Metadata/InternalIdMap/ExternalIdMap 取
//!     `ReadCopyFrom.AllImmutable` + `CopyTo = StubReadCopyTo`（:295 由
//!     `EnableReadCache` 定为只读缓存或主日志尾部），FullVector/Attributes 取
//!     `None`）→ 引擎冷读回填单点（wkv `session/raw/read.rs:673-690`：启用
//!     `ReadCache` 时挂只读缓存、否则按会话 `copy_reads_to_tail` 晋升尾部），
//!     并以 `read_cache/append.rs:52-54`「单记录超页容量即不入缓存」的尺寸准入
//!     承接 C# 排除大尺寸 FullVector 的同一目的；
//!   * 批量下发与完成收割 `ReadCallbackUnmanaged`（同文件 :356-362 单次
//!     `ReadWithPrefetch` + `hasPending` 为真才 `CompletePending(wait: true)`）→
//!     [`StoreCallbacks::read_multi`] 的窗口化内存批量直读 + 冷候选单次批量收割。
//!
//! # 异步契约（compio 下的 CompletePending 对位）
//!
//! C# 回调栈内 `CompletePending(wait: true)` 同步收割安全（.NET 线程池完成
//! 回调兜底）；compio 一线程一运行时，运行时上下文内嵌套 block_on 即重入
//! 调度器（compio-executor 断言），故本实现全部回调按
//! [`StoreCallbacks`] 的异步契约闭环：冷读/落盘/删除/清扫在 async 回调内
//! `.await` 引擎异步口，全程不经任何同步收割的 block_on 形态。唯一同步
//! 特例是 [`StoreCallbacks::read`]（webc-diskann `DataProvider` 同步 ID
//! 映射契约的承接口，见 trait 注释）：其冷区分支以 VM 同步绑定同型的
//! `wbase::future::inline_wait` 内联收割（不经 `Runtime::block_on` 入口，
//! 与 Luau `redis.call` 回调同一栈位纪律）。
//!
//! 会话引用在回调臂入口一次取出后跨 await 持有：会话本体由调用侧守卫/
//! 连接持有（见下），await 窗口内槽位可能被同线程其他任务重绑，但已取出
//! 的 `&StoreSession` 指向固定会话对象，不受重绑影响。
//!
//! # 会话绑定的执行域私有化
//!
//! C# 侧向量存储回调不持会话：`VectorManager` 只经
//! `[ThreadStatic] static TsavoriteSession ActiveThreadSession`
//! （libs/server/Resp/Vector/VectorManager.cs:254，读写臂见
//! libs/server/Storage/Functions/VectorStore/*.cs 对该静态字段的取用）拿到
//! 「当前线程当前命令」的会话，会话随连接/后台任务各自创建，故天然按执行域
//! 私有、且每库落位（C# 每库一份独立 Tsavorite 日志，向量数据本就域内）。
//!
//! rust 单库多表形态下同一引擎会话承载多域，等价实现即本文件的线程槽 +
//! RAII 守卫：回调实现 [`StoreCallbacks`] 本身**不持会话**（只带 `PhantomData<D>`
//! 与一把跨执行域共享的同键条带互斥锁），故 [`WedbVectorStoreCallbacks`] 自动
//! 满足 `Send + Sync`，`VectorManager` 不再被一份跨连接共享的全局会话拖成
//! `!Sync`；会话改由 [`ActiveVectorSessionGuard`] 在调用栈上绑定，回调臂经
//! [`with_active_vector_session`] 取用当前执行域会话。
//!
//! 会话私有化与同键读改写原子性正交：条带锁挂在共享的回调句柄上（各执行域
//! 拿到的是同一个 `Arc<WedbVectorStoreCallbacks>`），故「读旧 → 算新 → 写回」
//! 的互斥窗口跨线程/跨任务依然成立，与被绑定的会话无关。
//!
//! 绑定落点纪律（compio 为 thread-per-core：任务不迁线程，但同线程多任务在
//! `.await` 点交错，故线程槽只在**同步段**内保持唯一所有者）：
//!   * 命令分派整段绑定（`StoreGarnetApi::exec` 顶）；
//!   * 慢路径每次 `poll` 边界包装绑定（见
//!     [`crate::resp::slow_path`] 的 [`SlowFuture`](crate::resp::slow_path::SlowFuture)
//!     调用侧，绝不把守卫跨 `.await` 持有，否则其他任务的绑定/解绑会破坏 LIFO
//!     栈纪律而错接他人会话）；
//!   * 后台清理/量化/恢复臂每次处理项自持一份专用会话
//!     （[`OwnedActiveVectorSession`]，对标 C# `RunCleanupTaskAsync` 的专用
//!     `dropSession`，libs/server/Resp/Vector/VectorManager.Cleanup.cs:149）。
//!
//! 物理落位口径（与旧「全局根域会话」形态的唯一差异，属 Vector Set 预览特性
//! 可接受的布局变更）：元素记录键前缀
//! `[session_prefix(vns,db)][KeyTag::Vector]…`（wkv `session/keys.rs:44-55`）
//! 随**当前连接的 (namespace, active_db)** 落位，逐面对位 C# 每库独立日志；
//! 登记表记录键自带记录域、上下文元数据键恒用根前缀（本 crate
//! [`index_registry_physical_key`](vector_registry_recovery::index_registry_physical_key)
//! / [`metadata_registry_physical_key`](vector_registry_recovery::metadata_registry_physical_key)），
//! drop 清扫按 context 段全日志匹配（wkv `session/vector_cleanup.rs:65-96`），
//! 三者皆与绑定域无关。

use std::{
  any::{Any, TypeId},
  cell::Cell,
  future::Future,
  marker::PhantomData,
  ops::Deref,
  pin::Pin,
  ptr,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  task::{Context, Poll},
};

use event_listener::Event;
use wdev::Device;
use whasher::fast_hash;
use windex::PREFETCH_WINDOW;
use wkv::{StoreResult, StoreSession};
use wval::TaggedKeyBuf;
use wvector::store::{LengthPrefixedIter, StoreCallbacks, Term};

use super::vector_manager_filter::{evaluate_candidate_filter, with_inline_filter_state};

/// 线程槽值：当前执行域绑定的向量存储会话（裸指针 + 设备类型指纹）。
///
/// 裸指针成员令本类型天然 `!Send + !Sync`，只可栖身 [`thread_local!`] 槽，
/// 跨线程搬运即编译不过。`ptr` 为空即未绑定。
#[derive(Clone, Copy)]
struct BoundSlot {
  ptr: *const (),
  type_id: Option<TypeId>,
}

impl BoundSlot {
  const EMPTY: Self = Self {
    ptr: ptr::null(),
    type_id: None,
  };

  #[inline]
  fn of<D: Device>(session: &StoreSession<D>) -> Self {
    Self {
      ptr: session as *const StoreSession<D> as *const (),
      type_id: Some(TypeId::of::<D>()),
    }
  }
}

thread_local! {
  /// 当前执行域的向量存储会话槽（对标 C# `[ThreadStatic] ActiveThreadSession`）
  static ACTIVE_VECTOR_SESSION: Cell<BoundSlot> = const { Cell::new(BoundSlot::EMPTY) };
}

/// 向量存储会话的本执行域 RAII 守卫：进入即绑定，离开（含 panic 展开）即还原。
///
/// 严格嵌套：守卫记录绑定前的槽值，`Drop` 原样回填，故后台任务的专用会话守卫
/// 叠在命令绑定之上亦可在离开时精确还原外层命令会话。
///
/// **绝不跨 `.await` 持有**（`#[must_use]` + 本类型 `!Send` 只是第一道防线，
/// 真正的栈纪律见模块头）。
#[must_use = "守卫离开作用域即解绑：必须持有至本同步段结束"]
pub struct ActiveVectorSessionGuard<'a, D: Device> {
  /// 绑定前的槽值（`Drop` 回填目标）
  prev: BoundSlot,
  /// 绑定的会话（生命周期 `'a` 由借用检查保证守卫存活期内会话存活）
  session: &'a StoreSession<D>,
}

impl<'a, D: Device> ActiveVectorSessionGuard<'a, D> {
  /// 把 `session` 绑定为当前执行域的向量会话。
  #[inline]
  pub fn bind(session: &'a StoreSession<D>) -> Self {
    let prev = ACTIVE_VECTOR_SESSION.with(|slot| slot.replace(BoundSlot::of(session)));
    Self { prev, session }
  }

  /// 本守卫绑定的会话。
  #[inline]
  pub fn session(&self) -> &'a StoreSession<D> {
    self.session
  }
}

impl<D: Device> Drop for ActiveVectorSessionGuard<'_, D> {
  #[inline]
  fn drop(&mut self) {
    ACTIVE_VECTOR_SESSION.with(|slot| slot.set(self.prev));
  }
}

/// 自持且已绑定的向量会话（后台清理/量化/恢复任务的专用会话形态）。
///
/// 构造即把本会话登记为当前执行域会话；`Drop` **先**还原槽位**再**销毁会话，
/// 故槽位可读到该指针的任一时刻会话必未释放。裸指针成员令本类型 `!Send`，
/// 绑定与还原必在同线程完成。
pub struct OwnedActiveVectorSession<D: Device> {
  prev: BoundSlot,
  /// 堆上稳定地址（守卫自身移动不影响槽位指向）
  session: Box<StoreSession<D>>,
}

impl<D: Device> OwnedActiveVectorSession<D> {
  /// 接管 `session` 并即刻绑定为当前执行域会话。
  #[inline]
  pub fn new(session: StoreSession<D>) -> Self {
    let boxed = Box::new(session);
    let prev = ACTIVE_VECTOR_SESSION.with(|slot| slot.replace(BoundSlot::of(&boxed)));
    Self {
      prev,
      session: boxed,
    }
  }

  /// 专用会话本体。
  #[inline]
  pub fn session(&self) -> &StoreSession<D> {
    &self.session
  }
}

impl<D: Device> Drop for OwnedActiveVectorSession<D> {
  #[inline]
  fn drop(&mut self) {
    ACTIVE_VECTOR_SESSION.with(|slot| slot.set(self.prev));
  }
}

/// 以当前执行域绑定的会话执行闭包（对标 C# 回调内读
/// `[ThreadStatic] ActiveThreadSession`）。
///
/// 生命周期 `'s` 允许闭包把会话引用带出（回调臂「先取会话再跨 await」的
/// 形态）：会话本体由调用侧守卫/连接持有，引用有效期即调用方对执行域的
/// 持有期。未绑定、或绑定的会话设备类型与 `D` 不符即返回 `None`：错绑
/// 绝不静默改用他人会话（调用方须按失败口径报错，见
/// [`report_missing_session`]）。
#[inline]
pub fn with_active_vector_session<'s, D: Device, R>(
  f: impl FnOnce(&'s StoreSession<D>) -> R,
) -> Option<R> {
  let slot = ACTIVE_VECTOR_SESSION.with(Cell::get);
  if slot.type_id != Some(TypeId::of::<D>()) {
    return None;
  }
  // SAFETY: `BoundSlot::of` 只在 `ActiveVectorSessionGuard::bind`（其守卫持
  // `&'a StoreSession<D>`）与 `OwnedActiveVectorSession::new`（其 `Box` 自持该
  // 会话且 `Drop` 先回填槽位再放会话）两处写入；两者皆在 `Drop` 里把槽还原为
  // 绑定前的值，且 `type_id` 与 `D` 逐类型校验通过，故此刻 `slot.ptr` 指向一个
  // 存活且类型正确的 `StoreSession<D>`。`Cell` 的线程局部性保证无跨线程读写；
  // 返回引用的有效期由调用方对执行域（守卫/连接任务）的持有期保证。
  Some(f(unsafe { &*(slot.ptr as *const StoreSession<D>) }))
}

/// 本执行域是否已绑定向量会话（设备类型无关的判定，对标 C#
/// `Debug.Assert(VectorManager.ActiveThreadSession != null)`）。
#[inline]
pub fn active_vector_session_bound() -> bool {
  ACTIVE_VECTOR_SESSION.with(|slot| !slot.get().ptr.is_null())
}

/// 专用向量会话工厂的擦除返回形态：已绑定本执行域、离开作用域自动解绑并
/// 销毁会话的句柄。
///
/// [`VectorManager`](super::vector_manager::VectorManager) 只按存储回调泛型、
/// 不锁定设备类型，故句柄在此擦除为 `Box<dyn Any>`；其析构即
/// [`OwnedActiveVectorSession::drop`]——先还原线程槽、再释放会话。裸指针成员
/// 令本类型天然 `!Send`，绑定与解绑必在同线程的同一同步段内完成。
#[must_use = "守卫离开作用域即解绑并销毁专用会话：必须持有至本同步段结束"]
pub struct ActiveDedicatedVectorSession {
  /// 仅承接所有权：drop 即还原线程槽并销毁专用会话，无须读取
  _inner: Box<dyn Any>,
}

impl ActiveDedicatedVectorSession {
  /// 以已绑定的专用会话构造（仅由工厂返回值封送）。
  #[inline]
  pub fn from_bound<D: Device>(session: OwnedActiveVectorSession<D>) -> Self {
    Self {
      _inner: Box::new(session),
    }
  }
}

/// 专用向量会话工厂（清理/量化/恢复后台臂每次处理项自持一份会话的注入位）。
///
/// 闭包本体与其捕获须 `Send + Sync`（装配期由宿主以存储句柄构造），返回值
/// 无需 `Send`——它只在本线程的同步段内存在。工厂产不出会话（如纪元表满）
/// 即返回 `None`，调用方按失败口径处理，禁止降级为无会话写。
pub type DedicatedVectorSessionFactory =
  Arc<dyn Fn() -> Option<ActiveDedicatedVectorSession> + Send + Sync>;

/// 缺绑/错绑的失败口径（对标 C# `Debug.Assert(ActiveThreadSession != null)`，
/// 但 rust 运行期不可 panic 于后台线程，故 Debug 断言 + 错误日志 + 返回失败）
#[cold]
pub(crate) fn report_missing_session(op: &str) {
  debug_assert!(false, "向量存储回调 {op} 进入时本执行域未绑定同型向量会话");
  log::error!(
    "Vector store callback {op} invoked without an active vector session bound to this execution domain"
  );
}

/// 同键读改写条带互斥锁条带数（2 的幂，对标 Tsavorite 哈希桶分片粒度）
const RMW_STRIPE_COUNT: usize = 1024;

const _: () = assert!(
  RMW_STRIPE_COUNT.is_power_of_two(),
  "RMW 条带数必须为 2 的幂"
);

/// 任务态串行闸（wkv `SerialLock` 同构：认领位 + event_listener 等待队列）。
///
/// 回调异步化后，同键读改写窗口（读旧 → 算新 → 写回）内含冷读/冷写的
/// `.await` 让位点，parking_lot 守卫跨 await 持有会把整条 future 的 `Send`
/// 抹掉，且争用路径线程挂起会饿死同核任务队列（compio thread-per-core，
/// 挂起的线程无法驱动持锁任务完成）；本闸等待方挂任务态零唤醒零 CPU，
/// 守卫为共享引用（`Send + Sync`），可随回调 future 跨 await 存活。
struct SerialGate {
  /// 认领位（true = 有回调在窗口内）
  busy: AtomicBool,
  /// 争用等待队列：释放方 notify(1) 逐个移交
  gate: Event,
}

/// 任务态串行闸守卫（Drop 清认领位并唤醒队首等待者）。
struct SerialGateGuard<'a>(&'a SerialGate);

impl Drop for SerialGateGuard<'_> {
  #[inline]
  fn drop(&mut self) {
    self.0.busy.store(false, Ordering::Release);
    self.0.gate.notify(1);
  }
}

impl SerialGate {
  const fn new() -> Self {
    Self {
      busy: AtomicBool::new(false),
      gate: Event::new(),
    }
  }

  /// 串行获取：快路径一次 CAS 抢占；争用路径「先注册监听、再复核认领位」
  /// 后挂起——注册先于复核是丢失唤醒的唯一防线（与 wkv SerialLock 同一
  /// 论证），顺序颠倒即可能永久睡过一次移交。
  async fn acquire(&self) -> SerialGateGuard<'_> {
    loop {
      if self
        .busy
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
      {
        return SerialGateGuard(self);
      }
      let listener = self.gate.listen();
      if self
        .busy
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
      {
        return SerialGateGuard(self);
      }
      listener.await;
    }
  }
}

/// 128 字节对齐条带（杜绝相邻条带伪共享）。
#[repr(align(128))]
struct CacheAlignedGate(SerialGate);

/// 同键读改写条带互斥（任务态，跨执行域共享；对标 Tsavorite
/// `InternalRMW` 的 `FindOrCreateTagAndTryEphemeralXLock` 桶独占瞬时锁——
/// 窗口从「桶闩瞬时」放宽为「任务态串行闸整段持有」，因 rust 冷臂窗口
/// 含 `.await`，原子瞬时锁在该形态下不可得，互斥语义不变）。
struct StripedSerialLock {
  stripes: Box<[CacheAlignedGate]>,
}

impl StripedSerialLock {
  fn new(stripe_count: usize) -> Self {
    Self {
      stripes: (0..stripe_count)
        .map(|_| CacheAlignedGate(SerialGate::new()))
        .collect(),
    }
  }

  #[inline]
  async fn acquire(&self, hash: u64) -> SerialGateGuard<'_> {
    self.stripes[(hash as usize) & (RMW_STRIPE_COUNT - 1)]
      .0
      .acquire()
      .await
  }
}

/// 基于 wkv 存储会话的真实向量磁盘存储回调
///
/// **不持会话**：本类型只有设备类型指纹与一把跨执行域共享的同键条带互斥锁；
/// 执行期经 [`with_active_vector_session`] 取当前执行域绑定的会话，故
/// `Send + Sync` 自动成立（对标 C# 回调类不持 Tsavorite 会话、改读
/// `[ThreadStatic]`）。会话的创建与绑定归调用侧（命令面绑连接会话，后台臂绑
/// 专用会话，见模块头）。
pub struct WedbVectorStoreCallbacks<D: Device + 'static> {
  /// 设备类型指纹：本类型不持会话，执行期经 [`with_active_vector_session`]
  /// 取当前执行域绑定的会话
  _device: PhantomData<D>,
  /// 同键读改写条带互斥锁（物理键哈希 → 128 字节对齐 RwLock 条带，零每键堆分配）。
  ///
  /// 对标 C# 侧 Tsavorite `InternalRMW` 对记录所在哈希桶取独占瞬时锁
  /// （`FindOrCreateTagAndTryEphemeralXLock`）后才执行 InPlaceUpdater / CopyUpdater
  /// 的原子窗口：外层 VectorSetAdd 故意仅持共享锁放行并发 VADD，同键的
  /// 「读旧 → 算新 → 写回」互斥由本锁承接（图邻接表 `rmw_iid` 追加与 FSM
  /// 位图 `rmw_wid` 标记都经 [`StoreCallbacks::rmw`] 改写，无锁即丢更新）。
  ///
  /// 本锁是**跨执行域共享的同步原语**（非会话状态）：同一份回调句柄以
  /// `Arc` 分发到各线程/各任务，故不同执行域的同键读改写仍串行在同一把条带
  /// 锁上——会话私有化不削弱同键原子窗口。
  rmw_locks: StripedSerialLock,
}

impl<D: Device + 'static> Default for WedbVectorStoreCallbacks<D> {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl<D: Device + 'static> WedbVectorStoreCallbacks<D> {
  /// 构造存储回调绑定：不持会话（会话由调用侧在执行域内绑定），
  /// 仅初始化跨执行域共享的同键条带互斥锁
  pub fn new() -> Self {
    Self {
      _device: PhantomData,
      rmw_locks: StripedSerialLock::new(RMW_STRIPE_COUNT),
    }
  }

  /// 单键读三态甄别内核·异步臂（`rmw`/`filter` 消费；同步特例版
  /// [`Self::read_outcome_inline`] 供 [`StoreCallbacks::read`]/ID 映射使用，
  /// 语义两臂一致）
  ///
  /// 会话与物理键均由调用臂透传：会话取自 [`with_active_vector_session`]
  /// （缺绑判定单点归调用臂），物理键由同一会话构造，杜绝二次取执行域。
  ///
  /// libs/server/Resp/Vector/VectorManager.cs:CompletePending 的合并承接：
  /// C# 私有静态 CompletePending（CompletePendingWithOutputs(wait: true)
  /// 单输出收割）在 rust 无 pending 中间态——冷读经内联收割闭环（同步臂
  /// [`Self::read_outcome_inline`] 或本异步臂 `.await`），完成语义直达调用方。
  ///
  /// C# 向量存 ISessionFunctions 读回调（向量记录域读出 + 交付 DiskANN）的
  /// rust 漏斗：libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs:Reader
  /// ——rust 向量记录为裸字节值（无 IGarnetObject 实体化），命中即经 `f` 交付
  /// 值切片，缺失/失败三态同 C# `!status.Found` 判负口径
  async fn read_outcome_async<F>(
    &self,
    session: SessionRef<'_, D>,
    phys_key: &TaggedKeyBuf,
    mut f: F,
  ) -> ReadOutcome
  where
    F: FnMut(&[u8]) + Send,
  {
    match session.try_read_raw_in_memory(phys_key, |val| f(val)) {
      Ok(StoreResult::Success(_)) => ReadOutcome::Hit,
      Ok(StoreResult::NotFound) => ReadOutcome::NotFound,
      _ => {
        let mut called = false;
        match AssertSessionSend(session.read_raw_with(phys_key, |val| {
          called = true;
          f(val);
        }))
        .await
        {
          Ok(Some(_)) if called => ReadOutcome::Hit,
          Ok(None) => ReadOutcome::NotFound,
          _ => ReadOutcome::Failed,
        }
      }
    }
  }

  /// 条带锁窗口内的写内核（[`StoreCallbacks::write`] 与 [`Self::rmw`] 的公共
  /// 落点：调用方必须已取本执行域会话并持同键条带写锁，保证与并发读改写窗口
  /// 完全互斥）
  async fn write_locked(
    &self,
    session: SessionRef<'_, D>,
    phys_key: &TaggedKeyBuf,
    value: &[u8],
  ) -> bool {
    match session.try_upsert_raw_sync(phys_key, value) {
      Ok(Ok(_)) => true,
      _ => AssertSessionSend(session.upsert_raw(phys_key, value))
        .await
        .is_ok(),
    }
  }
}

/// 会话引用的 `Send` 类型通行证。
///
/// # Safety 契约
///
/// [`StoreCallbacks`] 的 RPITIT 契约要求 future `Send`（diskann glue 的
/// `SendFuture` 链上溯所需），而 [`StoreSession`] 因 wepoch 参与者
/// （thread-local 纪元护栏）天然 `!Sync + !Send`——这是 thread-per-core 的
/// 设计使然，非缺陷。compio 执行器为单线程：任务钉在其 Runtime 线程，
/// `block_on` 亦在当线程驱动，webc-diskann 的并发面（async_tools）在
/// compio feature 下退化为当线程任务并发（跨线程 spawn 需要 `Send` future，
/// 而 `DiskANNIndex` 本身在 compio 下即 `!Send`，上游类型系统已自证）。
/// 故随回调 future 存活的会话引用从不跨线程——`Send` 在本仓执行模型下
/// 纯为类型级通行证，跨线程访问实际不会发生。
pub(crate) struct SessionRef<'a, D: Device>(pub(crate) &'a StoreSession<D>);

// 手写 Clone/Copy：derive 会对 D: Copy 加约束，而引用本身恒可复制
impl<D: Device> Clone for SessionRef<'_, D> {
  #[inline]
  fn clone(&self) -> Self {
    *self
  }
}
impl<D: Device> Copy for SessionRef<'_, D> {}

// SAFETY: 见类型注释（compio 任务不迁线程；会话引用生命周期不超出回调
// future，future 只在其所属任务线程上 poll）。
unsafe impl<D: Device> Send for SessionRef<'_, D> {}

/// wkv 会话异步 future 的 `Send` 类型通行证（同 [`SessionRef`] 的 Safety
/// 契约：compio 任务不迁线程，future 只在会话所属线程上 poll；`Send` 纯为
/// 满足 [`StoreCallbacks`] RPITIT 契约上溯 diskann glue 的静态要求）。
pub(crate) struct AssertSessionSend<F>(pub(crate) F);

impl<F: Future> Future for AssertSessionSend<F> {
  type Output = F::Output;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    // SAFETY: newtype 通行证无内存布局改动，投影到内层 future 原样 poll
    //（Pin 投影：内层字段与外层同生命周期同固定性）
    unsafe { self.map_unchecked_mut(|wrapper| &mut wrapper.0) }.poll(cx)
  }
}

// SAFETY: 见类型注释。
unsafe impl<F: Future> Send for AssertSessionSend<F> {}

impl<D: Device> Deref for SessionRef<'_, D> {
  type Target = StoreSession<D>;

  #[inline]
  fn deref(&self) -> &Self::Target {
    self.0
  }
}

/// 单键读三态（命中 / 缺失 / 存储失败）。
enum ReadOutcome {
  Hit,
  NotFound,
  Failed,
}

impl<D: Device + 'static> StoreCallbacks for WedbVectorStoreCallbacks<D> {
  /// 批量读（对标 C# ReadCallbackUnmanaged:341-363 的 `ReadWithPrefetch` +
  /// `CompletePending`）
  ///
  /// 在 garnet 中的相对路径:libs/server/Resp/Vector/VectorManager.Callbacks.cs:ReadCallbackUnmanaged
  ///
  /// 单次折叠三段：
  ///   1. 前缀外提：整批仅读一次会话 ns/db，消除逐键原子重读与 Varint 重算；
  ///   2. 窗口化纯内存批量直读：与引擎内部分块常量 [`PREFETCH_WINDOW`]
  ///      严格对齐的栈上物理键窗口，每窗口一次纪元进入 + 两级 L1 流水线预取，
  ///      热批全程零堆物化、零异步进入；
  ///   3. 冷候选单次批量收割：仅当本批出现内存未命中时，对全部冷键做一次
  ///      批量 await（引擎内并发下发磁盘读、按下标升序合流），替代原逐键
  ///      收割的 N 次驱动进入。
  ///
  /// 内存批量直读把「明确不存在/墓碑」与「已落盘」统一报为未命中，故缺失键随
  /// 冷批重探一次索引（纯内存路径，无磁盘 I/O）；命中项严格按键流对下标各回调
  /// 一次，未命中不回调（对标 C# `VectorSessionFunctions` 仅 found 时回调）。
  ///
  /// 返回 false 表示冷批收割失败（对标 C# CompletePending 失败态）：本批交付
  /// 不完整，调用方须把本次检索报错，禁止静默吞成半截结果。本执行域未绑会话
  /// 亦返回 false（缺绑不降级）。
  async fn read_multi<F>(&self, context: u64, keys: &[u8], _length_hint: usize, mut f: F) -> bool
  where
    F: FnMut(u32, &[u8]) + Send,
  {
    let Some(session) = with_active_vector_session(|s: &StoreSession<D>| SessionRef(s)) else {
      report_missing_session("read_multi");
      return false;
    };

    let prefix = session.session_prefix();
    let prefix = prefix.as_slice();
    let mut window = [const { TaggedKeyBuf::new() }; PREFETCH_WINDOW];
    // 冷批两列平行（下标 / 物理键）：Vec::new 零分配，纯热批全程不触堆
    let mut cold_idx: Vec<u32> = Vec::new();
    let mut cold_keys: Vec<TaggedKeyBuf> = Vec::new();

    let mut stream = LengthPrefixedIter::new(keys).enumerate();
    loop {
      let mut base = 0u32;
      let mut len = 0;
      while len < PREFETCH_WINDOW {
        let Some((idx, key)) = stream.next() else {
          break;
        };
        if len == 0 {
          base = idx as u32;
        }
        window[len] = StoreSession::<D>::vector_key_with_prefix(prefix, context, key);
        len += 1;
      }
      if len == 0 {
        break;
      }

      // 已交付位图：批量直读中途上抛时只把未交付项并入冷批，杜绝同下标二次回调
      let mut delivered: u32 = 0;
      let res = session.try_read_batch_raw_in_memory(&window[..len], |i, val| {
        delivered |= 1 << i;
        match val {
          Some(v) => f(base + i as u32, v),
          None => {
            cold_idx.push(base + i as u32);
            cold_keys.push(window[i].clone());
          }
        }
      });
      if res.is_err() {
        for (i, phys_key) in window.iter().enumerate().take(len) {
          if delivered & (1 << i) == 0 {
            cold_idx.push(base + i as u32);
            cold_keys.push(phys_key.clone());
          }
        }
      }
    }

    if cold_idx.is_empty() {
      // 对标 C# `hasPending == false` 时整段跳过 CompletePending
      return true;
    }
    match AssertSessionSend(session.read_batch_raw_with(&cold_keys, |i, val| {
      if let Some(v) = val {
        f(cold_idx[i], v);
      }
    }))
    .await
    {
      Ok(_) => true,
      Err(err) => {
        log::error!("VectorStoreCallbacks::read_multi 冷批读失败: {err:?}");
        false
      }
    }
  }

  /// 单键尺寸未知读（对标 C# ReadSizeUnknown:427-474）
  ///
  /// 在 garnet 中的相对路径:libs/server/Resp/Vector/VectorManager.Callbacks.cs:ReadSizeUnknown
  ///
  /// ID 映射读取的承接口（trait 已随 webc-diskann 0.59.0-webc.5 async 化，
  /// 冷区经 [`Self::read_outcome_async`] 异步臂收割，inline_wait 内联收割随
  /// webc-diskann 同步契约一并退役）
  fn read<F>(&self, context: u64, key: &[u8], f: F) -> impl Future<Output = bool> + Send
  where
    F: FnMut(&[u8]) + Send,
  {
    let key = key.to_vec();
    async move {
      let Some(session) =
        with_active_vector_session(|session: &StoreSession<D>| SessionRef(session))
      else {
        report_missing_session("read");
        return false;
      };
      let phys_key = session.vector_key(context, &key);
      let fut = self.read_outcome_async(session, &phys_key, f);
      matches!(fut.await, ReadOutcome::Hit)
    }
  }

  /// 写入（对标 C# WriteCallbackUnmanaged:365-383 的 Upsert + 挂起即收割）
  ///
  /// 在 garnet 中的相对路径:libs/server/Resp/Vector/VectorManager.Callbacks.cs:WriteCallbackUnmanaged
  ///
  /// 与 [`Self::rmw`] 同款同键条带写锁：direct write 与读改写窗口在同键上
  /// 完全互斥，杜绝读改写写回以旧基值覆盖已落盘的新写入。条带闸窗口内
  /// `.await`（冷写页翻转让位）——任务态串行闸等待方挂任务队列而非线程，
  /// 互斥窗口语义与旧同步条带锁一致（见 [`StripedSerialLock`] 注释）。
  async fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool {
    let Some(session) = with_active_vector_session(|s: &StoreSession<D>| SessionRef(s)) else {
      report_missing_session("write");
      return false;
    };
    let phys_key = session.vector_key(context, key);
    let _lock = self.rmw_locks.acquire(fast_hash(phys_key.as_slice())).await;
    self.write_locked(session, &phys_key, value).await
  }

  /// 删除（对标 C# DeleteCallbackUnmanaged:385-396；C# 断言删除不挂起，
  /// rust 冷区键的墓碑追加以 await 闭环）
  ///
  /// 在 garnet 中的相对路径:libs/server/Resp/Vector/VectorManager.Callbacks.cs:DeleteCallbackUnmanaged
  async fn delete(&self, context: u64, key: &[u8]) -> bool {
    let Some(session) = with_active_vector_session(|s: &StoreSession<D>| SessionRef(s)) else {
      report_missing_session("delete");
      return false;
    };
    let phys_key = session.vector_key(context, key);
    AssertSessionSend(session.delete_raw(&phys_key))
      .await
      .unwrap_or(false)
  }

  /// 读改写（对标 C# ReadModifyWriteCallbackUnmanaged:398-419）
  ///
  /// 在 garnet 中的相对路径:libs/server/Resp/Vector/VectorManager.Callbacks.cs:ReadModifyWriteCallbackUnmanaged
  ///
  /// C# 向量存 ISessionFunctions 更新三钩子的 rust 折叠落点（读旧 → 改 → 整值
  /// 写回一条臂；写底层 wkv upsert 自含原位优先，无独立原位钩子）：
  /// libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs:InitialUpdater
  /// （缺失键初始写入臂：read NotFound → 零基缓冲 f 后落写）；
  /// libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs:CopyUpdater
  /// （已存在键读旧改新整值重投：read Hit → buf 拷旧 → f 改 → write 重投）；
  /// libs/server/Storage/Functions/VectorStore/VectorSessionFunctions.cs:InPlaceUpdater
  /// （原位改写由写底层 wkv upsert 的原位优先内核承接，本臂不再单设）
  ///
  /// 同键原子窗口（对标 Tsavorite `InternalRMW` 的
  /// `FindOrCreateTagAndTryEphemeralXLock` 桶独占瞬时锁）：任务态条带闸把
  /// 读旧（read_outcome_async，含冷读 await）→ 闭包更新 → 写回（含冷写
  /// await）收进同一互斥区间，等待方挂任务队列零 CPU（见
  /// [`StripedSerialLock`] 注释）。`write_len == 0` 纯读探测短路分支不进写
  /// 路径，直接跳过独占加锁保持高效。
  async fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, mut f: F) -> bool
  where
    F: FnMut(&mut [u8]) + Send,
  {
    let Some(session) = with_active_vector_session(|s: &StoreSession<D>| SessionRef(s)) else {
      report_missing_session("rmw");
      return false;
    };
    let phys_key = session.vector_key(context, key);
    // 纯读短路（C# 谓词 WriteDesiredSize == 0 判假）：缺失键 NeedInitialUpdate
    // 判假 → NOTFOUND、已存在键 NeedCopyUpdate 判假 → SUCCESS，均不进 updater、
    // 绝不写记录；两种状态 IsCompletedSuccessfully 皆为真，故应答恒成功
    if write_len == 0 {
      return !matches!(
        self.read_outcome_async(session, &phys_key, |_| {}).await,
        ReadOutcome::Failed
      );
    }

    let _lock = self.rmw_locks.acquire(fast_hash(phys_key.as_slice())).await;

    let mut buf = vec![0u8; write_len];
    let outcome = self
      .read_outcome_async(session, &phys_key, |curr| {
        let n = buf.len().min(curr.len());
        buf[..n].copy_from_slice(&curr[..n]);
      })
      .await;
    // 存储 IO 失败严禁当作初始写入，对齐 C# 失败返回 0 且上游不落写
    if matches!(outcome, ReadOutcome::Failed) {
      return false;
    }
    f(&mut buf);
    self.write_locked(session, &phys_key, &buf).await
  }

  /// 内联过滤（对标 C# FilterCallbackUnmanaged 直委托 EvaluateCandidateFilter，
  /// libs/server/Resp/Vector/VectorManager.Callbacks.cs:422-424 →
  /// VectorManager.Filter.cs:266-322）
  ///
  /// 在 garnet 中的相对路径:libs/server/Resp/Vector/VectorManager.Callbacks.cs:FilterCallbackUnmanaged
  ///
  /// 当前线程存在检索入口装配的内联过滤上下文（`vector_manager_filter`
  /// 的 `InlineFilterGuard` 守卫）时，按 internal_id 单读属性记录并内联求值
  /// 编译后的过滤表达式：本仓属性记录以 internal_id 为键（对标 C# 需经
  /// ExtMap 反查外部 ID 再读属性的两步，见
  /// [`super::vector_manager_filter::evaluate_candidate_filter`] 文档），属性
  /// 缺失、读取失败或条件不满足一律排除，对齐 C# 缺失即排除口径。无过滤
  /// 上下文（未携 FILTER 的检索臂与后台臂）回落放行 true，与无过滤语义一致，
  /// 绝不 panic。
  async fn filter(&self, context: u64, internal_id: u32) -> bool {
    // 有无过滤上下文先判（无上下文回落放行 true，与无过滤语义一致，绝不
    // panic；状态引用不跨 await 持有——求值在属性读回后的同步段完成）
    let has_filter_state = with_inline_filter_state(|_| ()).is_some();
    if !has_filter_state {
      return true;
    }
    let Some(session) = with_active_vector_session(|s: &StoreSession<D>| SessionRef(s)) else {
      // 本执行域未绑会话：内联守卫仅装配于检索入口同步段（会话必已绑定），
      // 后台臂无守卫进不到此臂，理论不可达按失败口径收敛，不 panic
      report_missing_session("filter");
      return false;
    };
    let phys_key = session.vector_key(
      context | (Term::Attributes as u64),
      &internal_id.to_le_bytes(),
    );
    let mut attr = Vec::new();
    let hit = matches!(
      AssertSessionSend(self.read_outcome_async(session, &phys_key, |value: &[u8]| {
        attr.extend_from_slice(value)
      },))
      .await,
      ReadOutcome::Hit
    );
    if !hit {
      return false;
    }
    // 理论不可达的 None（has_filter_state 已判真）：读回与求值间无 await
    // 之外的让位点，守卫必仍在位；按放行收敛不 panic
    with_inline_filter_state(|state| evaluate_candidate_filter(state, &attr)).unwrap_or(true)
  }

  /// drop 清扫链存储端承接（对标 C# RunCleanupTaskAsync 的
  /// IterateLookupSnapshot 扫描删除段，PostDropCleanupFunctions.Reader）：
  /// 委托绑定会话的全日志扫描 + 物理墓碑内核（wkv purge_vector_context），
  /// 同键多版本去重后逐键墓碑。清扫以 await 闭环（调用方为清理协程，未持
  /// 任何页锁/纪元，且每次处理项自持专用会话，见
  /// [`vector_manager_cleanup`](super::vector_manager_cleanup)）。
  ///
  /// 扫描按 context 段匹配、删除按扫得的裸键，故与会话落位域无关（wkv
  /// `session/vector_cleanup.rs:65-96`）：任意执行域绑定的会话皆可完成同一清扫。
  async fn purge_context(&self, context: u64) -> bool {
    let Some(session) = with_active_vector_session(|s: &StoreSession<D>| SessionRef(s)) else {
      report_missing_session("purge_context");
      return false;
    };
    match AssertSessionSend(session.purge_vector_context(context)).await {
      Ok(purged) => {
        if purged > 0 {
          log::info!("向量域 drop 清扫完成: context={context} 物理墓碑键={purged}");
        }
        true
      }
      Err(err) => {
        log::error!(
          "Failure during background cleanup of deleted vector sets, implies storage leak: \
           context={context} err={err}"
        );
        false
      }
    }
  }

  fn log(&self, _context: u64, msg: &str) {
    log::info!("{msg}");
  }
}
