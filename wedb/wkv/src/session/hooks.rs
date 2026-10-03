//! 引擎实例级钩子族（对标 C# Tsavorite Allocator 单委托字段与 Garnet
//! 记录触发器挂点）：WATCH 版本推进 / 用户键删除缺席观测 / 在位枚举治理，
//! 以及写面 WATCH 版本收口内核。

use std::{future::Future, pin::Pin, sync::Arc};

use parking_lot::Mutex;
use wdev::Device;

use crate::{session::StoreSession, store::WedbStore};

/// set_context 冷检窗口测试留钩（一次性）：严格冷检通过后、虚 ID 解析前的
/// 间隙内回调，供「GC 空闲析构与 set_context 交错」定向用例放大竞态窗口。
/// 生产路径恒 None，仅一次无争锁读；业务代码禁止触碰
#[cfg(debug_assertions)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

  /// 用户键删除缺席观测钩子只读取口（重放端 store_delete String 臂的登记
  /// 缺席收口承接：物理删除未命中即宿主值域外登记态观测，与在线 DEL 共用
  /// 同一钩子单点，杜绝第二套登记摘除分派形态）
  pub fn delete_miss_hook_of(&self) -> Option<&DeleteMissHook> {
    self.delete_miss_hook.get()
  }

  /// 引擎实例级钩子三件的在位现态只读枚举（见 [`EngineHookSlots`] 治理契约；
  /// 数据面零消费，仅宿主换面断言与观测使用）
  ///
  /// 测试握手:仅 wedb tests 换面断言消费,生产零调用
  #[doc(hidden)]
  pub fn engine_hook_slots(&self) -> EngineHookSlots {
    EngineHookSlots {
      watch_hook: self.watch_hook.get().is_some(),
      event_sink: self.event_sink.get().is_some(),
      delete_miss_hook: self.delete_miss_hook.get().is_some(),
    }
  }
}

impl<D: Device> StoreSession<D> {
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
}
