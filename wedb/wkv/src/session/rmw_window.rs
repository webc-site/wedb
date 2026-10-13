//! 同键读改写（RMW）原子窗口（对标 Tsavorite 会话锁器 `ISessionLocker` 与
//! `InternalRMW` 的 ephemeral 桶闩跨读改写全程协议）
//!
//! C# 侧非事务会话经 `BasicSessionLocker.TryLockEphemeralExclusive` 在 RMW 的
//! 读—算—写全程持本键主桶排他闩（`libs/storage/Tsavorite/cs/src/core/Index/
//! Tsavorite/Implementation/InternalRMW.cs` 的
//! `FindOrCreateTagAndTryEphemeralXLock` + try/finally `UnlockEphemeralExclusive`），
//! 事务会话经 `TransactionalSessionLocker` 只断言不取闩——事务已在同一份锁内存上
//! 持该键桶的排他闩（`libs/server/Transaction/TxnKeyEntry.cs` 与
//! `Implementation/Locking/OverflowBucketLockTable.cs` 共用 `store.LockTable`，
//! 桶下标同为 `keyHash & size_mask`）。C# 的 `InternalUpsert.cs:67` 同一取闩口
//! 亦覆盖纯写回，故锁内读—算—写是该引擎的唯一形态，命令层从不裸读旧值再盲写。
//!
//! rust 侧锁源同为 windex `HashBucket` 内嵌闩，本键 `user_key` 的会合域主桶
//! 全仓单点同构：以会话**物理**前缀哈希为种子的 scoped 口径
//!（[`whasher::scoped_hash`]`(prefix, key) = fast_hash_with_seed(key,
//! fast_hash(prefix))`，前缀真值源 [`StoreSession::session_prefix`]）三处共取
//! ——本窗口、`wtxn` 事务键锁（`wtxn/src/txn_key_entry_comparison.rs::
//! scoped_key_hash` 同一构造口的 i64 位面）、`wkv` TTL 读改写窗口
//!（`wkv/src/ttl.rs` 经 `HashIndex::try_lock_key_hash_exclusive` 同源哈希
//! 持桶闩），三者同内存**同桶**，互斥即同键全序串行，全仓无第二把同址锁、
//! 亦无条带折算；跨租户/跨库同名键种子域分离落位正交，窗域假性互斥不存
//!（票 wtxn-wkv-keybucket-hash-scope-desync 口径统一，禁把任一面降回裸
//! `fast_hash` 重引跨租户假斥）。
//! 本窗口取闩即该入口的同两步组合（扩容协同 [`StoreSession::ensure_split_by_hash`]
//! 先行后，`bucket_index_for_hash` 定位主桶 +
//! `HashBucket::try_lock_exclusive` 单次尝试），只是闩的持有证明要跨 `&self`
//! 借用期交回命令层，故以窗口自身承载放闩，不自建第二张锁表、不另立锁语义。
//! 取闩/放闩形态与 `wtxn::TxnKeyEntries::acquire_plan`/`release_held` 同款：
//! 钉定 `Arc<HashIndex>` 版本 + 纯数据桶下标，跨 split 扩容不串锁，不新建守卫类型。
//!
//! 会话该走哪种锁器：C# 由 api 视图类型在编译期定（`libs/server/Resp/
//! RespServerSession.cs:ProcessMessages` 依 `txnManager.state == TxnState.Running`
//! 在 `basicApi` 与 `transactionalApi` 间派发），rust 不对命令层做双份单态化，
//! 改由同一判据的等强投影在命令分派单点写 [`StoreSession::push_session_locking`]
//! （Running 且本命令键窗全落本事务持锁桶域才置 `Transactional`，脚本重入段
//! 域外键落 `Basic` 自取闩，选型点见 wnode `garnet_api::exec`），窗口据此决定
//! 「自取闩」或「让闩于事务」。
//!
//! 与引擎侧回溯保护闩的关系：`session/raw/write/inplace.rs` 的 ephemeral 闩按
//! 物理记录键（带 `prefix|KeyTag` 前缀后再哈希）寻桶，与本窗口的 `user_key` 桶
//! 是两个不同基（garnet 单份哈希表故同键同桶，wedb 同键的 TTL/字符串/对象记录
//! 各自成键故各落一桶），两桶偶合时窗口持闩期内层取闩必失败并按 RETRY_LATER
//! 退避重试——该面在 HEAD 已随 `wtxn` 事务键锁与 `wkv/src/ttl.rs` 的 EXPIRE
//! 窗口同形存在（持 user_key 桶闩期内读写记录桶），属两基并存的既有结构，
//! 不在本窗口内做二次折算。取闩序恒为「user_key 桶 → 记录桶」单向（记录桶
//! 闩持有者从不反取 user_key 桶闩），无反序死锁面。带 TTL 复合写族
//!（SETEX/SET EX/SET KEEPTTL/GETEX/GETDEL 快慢路径各臂）、原值写回族
//!（SETRANGE/APPEND/INCR/BITFIELD）与裸写回族（SET/DEL/MSET/BITOP dest/
//! SETNX/ETag 写族/HCOLLECT/ZCOLLECT 快慢路径各臂）同纪律持窗——C#
//! InternalUpsert.cs:67 / InternalRMW.cs:70 / InternalDelete.cs:60 三写原语
//! 同取 FindOrCreateTagAndTryEphemeralXLock，upsert/RMW/delete 全写路径同闩
//! 域互斥，进窗即该锁形的 1:1 对位；盲写落在他者读算写间隙即非可串行化
//!（GETDEL 答旧删新、SET 被顶、SETNX 双成功），绝不容裸写游离闩域外。
//! 对象装载族信封写回在持窗之上再叠加落笔前终态复验（见 wnode
//! `resp/objects/object_store_utils.rs:obj_writeback_recheck_sync` 与同名
//! 异步档，票 r6-del-rmw-writeback-revalidate）——复验不过即弃写，按命令
//! 语义走既有降级 / 存储忙信号，绝不容装载期旧视图尾段盲写。

use std::{
  fmt, result,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  time::{Duration, Instant},
};

use smallvec::{SmallVec, smallvec};
use wbase::future::yield_now;
use wdev::Device;
use windex::{Error as WindexError, HashIndex};

use super::{BatchStoreSession, StoreSession};
use crate::error::{Error, Result};

/// 多键 RMW 计划构建计数器（测试观测用：验证争用轮次内 plan 仅首轮构建一次，杜绝每轮重建）
#[doc(hidden)]
pub static RMW_PLAN_BUILD_COUNT: AtomicUsize = AtomicUsize::new(0);

/// 最近一次计划钉定的索引版本地址（测试握手用：构建计数只证明「构建了」，不证明
/// 「钉在哪一版索引上」——版本推进失效重建的编排必须先确证首轮计划钉在旧表，
/// 否则扩容可能落在首轮构建之前，计划自始钉新表、争用与重建双双蒸发）
#[doc(hidden)]
pub static RMW_PLAN_PINNED_INDEX: AtomicUsize = AtomicUsize::new(0);

/// 计划取闩失手轮次计数（测试握手用：确证争用真的发生过一个整轮，
/// 而非首轮即取闩成功的空转通过）
#[doc(hidden)]
pub static RMW_PLAN_ACQUIRE_MISS: AtomicUsize = AtomicUsize::new(0);

/// 单键读改写窗口**失闩尝试**计数（测试观测口，仿 [`RMW_PLAN_ACQUIRE_MISS`]
/// 在库先例：失闩一次尝试计一次，取闩成功零计数——同步臂
/// [`BatchStoreSession::try_rmw_window`] 钉闩下恰 1 次、异步臂
/// [`BatchStoreSession::rmw_window`] 恰让核预算量级次，嵌自旋取闩核即放大三个
/// 数量级。macOS 线程让渡成本随竞争形态抖动三个数量级（1µs–10ms），失闩纯墙钟
/// 界不可确定性判定，单次尝试契约以本计数锁死；relaxed 自增仅失闩慢臂触达，
/// 取闩成功的热路径零额外原子）
#[doc(hidden)]
pub static RMW_KEY_LATCH_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

/// 键引用转换（支持 `&'k [u8]`、`&&'k [u8]` 等直接传入多键窗口，零堆分配）
pub trait KeyRef<'k> {
  fn to_key_ref(self) -> &'k [u8];
}

impl<'k> KeyRef<'k> for &'k [u8] {
  #[inline(always)]
  fn to_key_ref(self) -> &'k [u8] {
    self
  }
}

impl<'k> KeyRef<'k> for &&'k [u8] {
  #[inline(always)]
  fn to_key_ref(self) -> &'k [u8] {
    self
  }
}

/// 多键 RMW 计划槽位（记录待取排他闩的主桶下标与首个关联的用户键引用）
#[derive(Clone)]
struct RmwLockPlanSlot<'k> {
  bucket: usize,
  user_key: &'k [u8],
}

/// 待排定键条目
#[derive(Clone)]
struct RmwPlanEntry<'k> {
  bucket: usize,
  hash: u64,
  user_key: &'k [u8],
}

/// 多键 RMW 归并加锁计划（钉定索引快照版本 + 桶下标升序去重槽位）
struct RmwLockPlan<'k> {
  index: Arc<HashIndex>,
  slots: SmallVec<[RmwLockPlanSlot<'k>; 8]>,
}

/// 异步域让核重试预算（对标 C# 锁冲突转 pending 重试的外层循环）：预算耗尽即回
/// [`windex::Error::LockTimeout`]，杜绝同线程持闩者不再让核时的无界自旋。
/// 轮间让核非盲采：轮内失闩后查桶放闩通知位（[`windex::HashBucket::
/// consume_release_notify`]），见位即不让核立即重试——重试相位锚定放闩时刻，
/// 消除采样相位竞争（同键 collect 高频重入令闩空闲窗仅纳秒级占比，盲采必竞相
/// 偶败）；钉闩场景无放闩、无通知，环数与耗尽语义不变，fail-closed 保持。
/// 首失闩轮起在该桶登记让渡位（[`windex::HashBucket::set_handoff`]，RAII 守卫
/// [`HandoffReg`] 承载，成功/耗尽/panic 皆经 Drop 出册）：在册期间一切新取闩尝试
/// （含 collect 臂紧循环的重入）见位让位，闩的下一任归属由**序**保证而非竞速
/// 时机——放闩→重取纳秒窗内的相位竞争就此消除；本环自身重试走绕开门控的
/// [`windex::HashBucket::try_lock_exclusive_now`]，无「自见自让」面。等闩墙钟续航
/// 由放闩方活跃度续等环承接（[`RMW_LATCH_EXTEND_BUDGET`]，钉闩零放闩即零续等、
/// fail-closed 形制不变），不靠步进睡眠——实测钉闩满预算下 timer 睡眠被满核
/// 竞争者拉长至 16-29ms/轮（预算环墙钟 16-29s），击穿钉闩十秒上界案。
const RMW_LATCH_YIELD_BUDGET: usize = 1024;
/// 放闩方活跃度续等预算（方向 ③ 修正形）：主预算烧尽时，若本环在**入册之后**
/// 消费到过放闩通知（在闩者活跃度铁证——释放仍在持续发生，与钉闩的永久静默
/// 判别面严格互斥），允许续等至多本轮数、且总墙钟不超 [`RMW_LATCH_EXTEND_
/// DEADLINE`]（轮数/墙钟双界）。零见证即零续等：钉闩场景环数、终态、失闩计数
/// 与主预算形制逐轮一致，fail-closed 秒级界不回退。区别于已证伪的 ever_woken
/// 续等形（判据「曾被唤醒」在挂起-唤醒链路上可双向死锁；本判据是放闩置位事件
/// 本身，且全程保持让核轮询零挂起、让渡在册持权随 Drop 出册，无「等一个不来
/// 的唤醒」面）。
const RMW_LATCH_EXTEND_BUDGET: usize = 4096;
/// 活跃度续等总墙钟上界（自入环起算；钉闩形制零触达）
const RMW_LATCH_EXTEND_DEADLINE: Duration = Duration::from_secs(4);

/// 等闩让渡登记守卫（RAII）：构造即在指定桶置 [`windex::HashBucket::set_handoff`]
/// 凭据，Drop 必出册——预算环成功持闩返回、预算耗尽上抛、索引版本推进换桶、
/// panic unwind 四条退出路径同守卫收口，登记位绝不跨请求滞留。钉定的
/// `Arc<HashIndex>` 版本与桶下标同源持存（与 [`RmwWindow::held`] 同纪律，
/// split 扩容版本推进时先出旧册再入新册）
struct HandoffReg {
  index: Arc<HashIndex>,
  bucket: usize,
}

impl HandoffReg {
  /// 登记即 priming：置让渡位后先消费并丢弃**入册前滞留**的放闩通知位——
  /// 续等见证只计入册之后的放闩置位事件（钉闩且入册前有旧释放残留时，
  /// 若无此 priming 即产生伪见证，续等环在钉闩形制下被误开，击穿
  /// fail-closed 秒级界）。被丢弃的位仅损失一次「不让核立即重试」的相位
  /// 优化：下一轮轮首 `try_lock_exclusive_now` 照常单次尝试，语义不变
  fn new(index: Arc<HashIndex>, bucket: usize) -> Self {
    let slot = index.get_bucket(bucket);
    slot.set_handoff();
    slot.consume_release_notify();
    Self { index, bucket }
  }
}

impl Drop for HandoffReg {
  fn drop(&mut self) {
    self.index.get_bucket(self.bucket).clear_handoff();
  }
}

/// 内层记录键闩失败重试环的同步重试预算（对齐本模块双档先例的同一 1024 量级，
/// 消费面：`session/raw/write/inplace.rs` 三写内核环与 `session/raw/read.rs`
/// `drive_mem_read` 驱动环）：reviv 开启下外层 user_key 桶闩窗口（本模块
/// `RmwWindow`、`wkv/src/ttl.rs` 的 KeyLatch）与内层记录物理键
/// （`prefix|KeyTag|user_key`）桶闩偶合同桶时，窗口持有期内层取闩恒失败，而
/// 重试环的退出依赖闩被释放、持有者正是本调用自身未返回的外层窗口（窗口随
/// 本调用返回才 Drop）——自等自即永久自旋，窗口 Drop 不可达。预算耗尽即回
/// [`Error::Index`]([`windex::Error::LockTimeout`]) 上抛，不在持闩窗口内消化、
/// 严禁窗口内二次自旋，调用方按既有错误通道退窗应答（窗口随栈 Drop 放闩），
/// 客户端得可重试错误而非挂死；同核跨任务互阻（compio thread-per-core 下同核
/// 他任务的无界同步环不让出 reactor，持闩者的完成事件永不被 poll）一并收口。
/// 复活池关闭默认档内层环免锁零触达，行为零变化
pub(crate) const INNER_LATCH_RETRY_BUDGET: u32 = 1024;

/// 会话锁器模式（对标 C# 两套 `ISessionLocker` 实现的按调用面选型，
/// 见 `libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionLocker.cs`
/// 的 `BasicSessionLocker` 与 `TransactionalSessionLocker` 两实现；映射锚点
/// 已在仓内其他位点登记，此处不复挂以免重复定义）
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SessionLocking {
  /// 非事务会话：读改写窗口自取本键桶排他闩
  Basic,
  /// 事务会话：本键桶排他闩已由本会话事务持有，窗口让闩（绝不自旋等自己）
  Transactional,
}

impl SessionLocking {
  /// 是否事务锁模式
  #[inline(always)]
  pub const fn is_transactional(self) -> bool {
    matches!(self, Self::Transactional)
  }
}

/// 同键读改写原子窗口（键级 RAII：本键桶排他闩的持有证明）
///
/// 值写回入口（[`Self::try_rmw_sync`] / [`Self::upsert_rmw`]）挂在本类型上，
/// 无窗口即取不到写回面，杜绝「读—写两步式盲写」的第二条路径；
/// `held` 为 `None` 即事务锁模式（本会话事务已持该桶闩，窗口零操作）。
/// 本类型只由 [`BatchStoreSession::try_rmw_window`] /
/// [`BatchStoreSession::rmw_window`] 构造，故持窗口必然处于批处理纪元保护下
/// （其值写回内核走零 `enter()` 的同步段，纪约与
/// `StoreSession::try_upsert_raw_sync_unprotected` 同）。
pub struct RmwWindow<'a, 'k, D: Device> {
  /// 归属会话（值写回内核的宿主）
  pub session: &'a StoreSession<D>,
  /// 窗口钉定的用户键（写回目标键与取闩键同源，编译面即不可错配）
  pub user_key: &'k [u8],
  /// 钉定的索引版本与本键主桶下标（跨 split 扩容不串锁；Drop 按同一版本放闩）
  pub held: Option<(Arc<HashIndex>, usize)>,
}

impl<D: Device> Drop for RmwWindow<'_, '_, D> {
  #[inline]
  fn drop(&mut self) {
    if let Some((index, bucket)) = &self.held {
      index.get_bucket(*bucket).unlock_exclusive();
    }
  }
}

/// 会话锁器模式 RAII 还原守卫（对标 C# api 视图按调用段进出选型：事务重放遍与
/// 事务过程视图进入时置 `Transactional`，离开即还原，杜绝位残留；分派选型位点
/// 的映射锚点登记在 `wnode/src/resp/resp_server_session.rs`，此处不复挂）
pub struct SessionLockingGuard<'a, D: Device> {
  session: &'a StoreSession<D>,
  prev: SessionLocking,
}

impl<D: Device> fmt::Debug for SessionLockingGuard<'_, D> {
  #[inline]
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("SessionLockingGuard")
      .field("prev", &self.prev)
      .finish_non_exhaustive()
  }
}

impl<D: Device> Drop for SessionLockingGuard<'_, D> {
  #[inline]
  fn drop(&mut self) {
    self.session.set_session_locking(self.prev);
  }
}

/// 会话锁器模式位（`StoreSession::session_locking` 字段的读写单点，
/// 判据与取用面在 [`crate::session::rmw_window`] 模块头）
pub(crate) struct SessionLockingState(AtomicBool);

impl SessionLockingState {
  #[inline(always)]
  pub(crate) const fn new() -> Self {
    Self(AtomicBool::new(false))
  }

  #[inline(always)]
  fn get(&self) -> SessionLocking {
    if self.0.load(Ordering::Relaxed) {
      SessionLocking::Transactional
    } else {
      SessionLocking::Basic
    }
  }

  #[inline(always)]
  fn swap(&self, locking: SessionLocking) -> SessionLocking {
    let prev = self.0.swap(locking.is_transactional(), Ordering::Relaxed);
    if prev {
      SessionLocking::Transactional
    } else {
      SessionLocking::Basic
    }
  }
}

impl<D: Device> StoreSession<D> {
  /// 当前会话锁器模式
  #[inline(always)]
  pub fn session_locking(&self) -> SessionLocking {
    self.session_locking.get()
  }

  /// 置位会话锁器模式并回旧值（分派单点选型与 RAII 还原共用；C# 由 api 视图类型
  /// 承载，rust 为会话态一位，见模块头口径说明）
  #[inline(always)]
  pub fn set_session_locking(&self, locking: SessionLocking) -> SessionLocking {
    self.session_locking.swap(locking)
  }

  /// 置位会话锁器模式并在离开作用域时还原旧值（命令分派单点与事务过程视图用）
  #[inline(always)]
  pub fn push_session_locking(&self, locking: SessionLocking) -> SessionLockingGuard<'_, D> {
    let prev = self.set_session_locking(locking);
    SessionLockingGuard {
      session: self,
      prev,
    }
  }
}

impl<'a, D: Device> BatchStoreSession<'a, D> {
  /// 取本键读改写原子窗口——非阻塞臂（同步快路径专用）
  ///
  /// 取闩为**单次尝试**（1:1 对标 C# `BasicSessionLocker.TryLockEphemeralExclusive`
  /// 的单次 `LockTable.TryLockExclusive`，失闩即回 `None` 回 `RETRY_LATER` 同判据，
  /// 重试由调用方降级异步臂 [`Self::rmw_window`] 的让核预算环承接，同步段绝不
  /// 嵌自旋忙等——compio thread-per-core 下同步忙等段连本核全部任务 poll 一并
  /// 卡顿，返回 `None` = 单次尝试未取到本键桶排他闩（同键并发 RMW / 他事务持锁 /
  /// EXPIRE 窗口占闩），或扩容协同期分块迁移失败（半迁移态禁对未迁移桶加闩，
  /// 视同未取闩降级；错误由异步臂 [`Self::rmw_window`] 入口同判据 `?` 显式上抛）；
  /// 瞬态争用由 `HashBucket::try_lock_exclusive` 内嵌 kMaxLockSpins 次让渡在单次
  /// 尝试内部承接（亚微秒持有者即得）；事务锁模式下本键桶闩已在本会话
  /// 事务手上，直接回让闩窗口
  ///
  /// 取闩前先行分裂协同（对标 InternalRMW.cs:67-72 首行铁律：
  /// `phase == IN_PROGRESS_GROW → SplitBuckets(hei.hash)` 严格先于
  /// `FindOrCreateTagAndTryEphemeralXLock`），确保排他闩仅施加在已完成数据迁移
  /// （SPLIT_COMPLETED）的桶上，杜绝与后台迁移内核 `insert_to_bucket` /
  /// `set_overflow_index` 的闩位碰撞，以及窗口内查空误判键不存在的读改写盲写覆盖
  #[inline]
  pub fn try_rmw_window<'s, 'k>(&'s self, user_key: &'k [u8]) -> Option<RmwWindow<'s, 'k, D>> {
    let session: &'s StoreSession<D> = self;
    let held = if session.session_locking().is_transactional() {
      None
    } else {
      Some(self.try_lock_key_bucket(user_key)?)
    };
    Some(RmwWindow {
      session,
      user_key,
      held,
    })
  }

  /// 归并加锁计划：按主桶下标升序排序后在单次线性扫描中合并同桶键，钉定当前索引版本快照
  fn build_rmw_lock_plan<'k>(&self, order: &[(u64, &'k [u8])]) -> Result<RmwLockPlan<'k>> {
    RMW_PLAN_BUILD_COUNT.fetch_add(1, Ordering::Relaxed);

    for &(hash, _) in order {
      self.ensure_split_by_hash(hash)?;
    }
    let index = self.store.index.load_full();
    RMW_PLAN_PINNED_INDEX.store(Arc::as_ptr(&index) as usize, Ordering::Relaxed);

    let mut entries: SmallVec<[RmwPlanEntry<'k>; 8]> = SmallVec::with_capacity(order.len());
    for &(hash, user_key) in order {
      let bucket = index.bucket_index_for_hash(hash);
      entries.push(RmwPlanEntry {
        bucket,
        hash,
        user_key,
      });
    }

    // 按桶下标升序全序定序（与 wtxn TxnKeyEntryComparison::compare 同序收敛）
    entries.sort_unstable_by(|a, b| a.bucket.cmp(&b.bucket).then_with(|| a.hash.cmp(&b.hash)));

    // 单次线性扫描合并同桶键，取首键作为窗口持有锚点，杜绝 O(N²) 回看全组
    let mut slots: SmallVec<[RmwLockPlanSlot<'k>; 8]> = SmallVec::with_capacity(entries.len());
    for entry in entries {
      if let Some(last) = slots.last()
        && last.bucket == entry.bucket
      {
        continue;
      }
      slots.push(RmwLockPlanSlot {
        bucket: entry.bucket,
        user_key: entry.user_key,
      });
    }

    Ok(RmwLockPlan { index, slots })
  }

  /// 依缓存计划尝试依次获取排他闩（任一失闩整体 RAII 放闩）
  ///
  /// 槽内取闩为**单次尝试**（票 pair_bucket_order_latch 挂死案定因）：本函数
  /// 是让核预算环（[`Self::rmw_window`] / [`Self::rmw_window_sorted`]）的
  /// 轮内执行体，轮间让核（`wbase::future::yield_now`）即重试机制本体——轮内
  /// 若再嵌套每轮 1024 次的自旋取闩预算（每次自旋又内含
  /// `HashBucket::try_lock_exclusive` 的 10 次线程让渡），单轮成本即
  /// 1024×10 次线程让渡（实测 ~3.5s，macOS swtch_pri 按时间片退让），永久
  /// 持闩（外部件钉闩 / 卡死持有者）下 1024 轮预算环退化为小时级延迟炸弹，
  /// fail-closed 忙应答永不到达。瞬态争用由 HashBucket 内嵌的 kMaxLockSpins
  /// 次让渡承接（亚微秒持有者轮内即得），跨轮持闩交轮间让核，两层各司其职
  /// 不重排。
  /// 返回 `Err(fail_bucket)` 即失闩槽位的主桶下标（争用单点），交预算环查该桶
  /// 放闩通知位定「不让核立即重试」或「让核」（见 [`RMW_LATCH_YIELD_BUDGET`]）
  /// `bypass_handoff`：让核预算环内已自登记让渡位的调用方走绕开门控臂
  /// （[`windex::HashBucket::try_lock_exclusive_now`]，防自见自让）；单次尝试的
  /// 同步臂走门控臂（[`windex::HashBucket::try_lock_exclusive`]，见册即让位、
  /// 降级信号语义不变）
  fn try_acquire_rmw_plan<'s, 'k>(
    &'s self,
    plan: &RmwLockPlan<'k>,
    bypass_handoff: bool,
  ) -> result::Result<SmallVec<[RmwWindow<'s, 'k, D>; 8]>, usize> {
    let session: &'s StoreSession<D> = self;
    let mut windows: SmallVec<[RmwWindow<'s, 'k, D>; 8]> =
      SmallVec::with_capacity(plan.slots.len());
    for slot in &plan.slots {
      let bucket_latch = plan.index.get_bucket(slot.bucket);
      let taken = if bypass_handoff {
        bucket_latch.try_lock_exclusive_now()
      } else {
        bucket_latch.try_lock_exclusive()
      };
      if !taken {
        RMW_PLAN_ACQUIRE_MISS.fetch_add(1, Ordering::Relaxed);
        return Err(slot.bucket);
      }
      windows.push(RmwWindow {
        session,
        user_key: slot.user_key,
        held: Some((plan.index.clone(), slot.bucket)),
      });
    }
    Ok(windows)
  }

  /// 多键读改写原子窗口——同步非阻塞臂（RENAME 双键 / MSETNX 全键的键组
  /// 排他闩，票 zcode-r15-generic 发现一：对标 C# UnifiedStoreOps.RENAME 的
  /// `SaveKeyEntryToLock(oldKey/newKey, Exclusive)` 双键事务锁与
  /// MainStoreOps.MSET_Conditional 的全键排他锁——键组闩内完成「判定 →
  /// 读改写」全序列，杜绝多步键序列中「探旧值 → 写回」间隙的并发交错）
  ///
  /// 取闩序按主桶下标升序（与 wtxn 同款桶升序定序收敛：RENAME a b 与
  /// RENAME b a 交叉各按同序取闩，无循环等待面）；同一桶号（同一索引版本下）
  /// 线性合并只取一闩即覆盖组内同桶键——排他闩非重入，自取必空转至预算耗尽。
  /// 任一键失闩已取窗口整体 RAII 放闩回 `None`，调用方沿既有 `Ok(false)`
  /// 降级慢路径同序持窗重放；事务锁模式下组内全键桶闩已在本会话事务手上，
  /// 直接回空窗口组（与单键形态同让闩语义）
  pub fn try_rmw_window_sorted<'s, 'k, I>(
    &'s self,
    keys: I,
  ) -> Option<SmallVec<[RmwWindow<'s, 'k, D>; 8]>>
  where
    I: IntoIterator,
    I::Item: KeyRef<'k>,
  {
    let session: &'s StoreSession<D> = self;
    if session.session_locking().is_transactional() {
      return Some(SmallVec::new());
    }

    let mut iter = keys.into_iter();
    let Some(first) = iter.next() else {
      return Some(SmallVec::new());
    };
    let first_key = first.to_key_ref();
    let Some(second) = iter.next() else {
      return self.try_rmw_window(first_key).map(|w| smallvec![w]);
    };

    let mut order: SmallVec<[(u64, &'k [u8]); 8]> = SmallVec::new();
    // 循环前缀外提：会话物理前缀单次读取，组内全键共用同一 scoped 种子域
    //（与 wtxn 事务键锁同一构造口径，三面同键同桶）
    let prefix = session.session_prefix();
    order.push((whasher::scoped_hash(&prefix, first_key), first_key));
    let second_key = second.to_key_ref();
    order.push((whasher::scoped_hash(&prefix, second_key), second_key));
    for key in iter {
      let k = key.to_key_ref();
      order.push((whasher::scoped_hash(&prefix, k), k));
    }

    let plan = self.build_rmw_lock_plan(&order).ok()?;
    self.try_acquire_rmw_plan(&plan, false).ok()
  }

  /// 多键读改写原子窗口——让核等待臂（异步域专用，形态与单键
  /// [`Self::rmw_window`] 同一重试契约：整组按序重取，任一轮失闩整体放闩
  /// 让核，预算耗尽回 [`windex::Error::LockTimeout`]，绝不留无界等待）
  ///
  /// 首轮构建归并加锁计划（钉定索引版本与桶序），后续轮仅按缓存计划单次尝试
  /// 排他闩，循环内不重排、不重哈希、不重分配；索引版本推进时整体失效重建一次
  pub async fn rmw_window_sorted<'s, 'k, I>(
    &'s self,
    keys: I,
  ) -> Result<SmallVec<[RmwWindow<'s, 'k, D>; 8]>>
  where
    I: IntoIterator,
    I::Item: KeyRef<'k>,
  {
    let session: &'s StoreSession<D> = self;
    if session.session_locking().is_transactional() {
      return Ok(SmallVec::new());
    }

    let mut iter = keys.into_iter();
    let Some(first) = iter.next() else {
      return Ok(SmallVec::new());
    };
    let first_key = first.to_key_ref();
    let Some(second) = iter.next() else {
      return self.rmw_window(first_key).await.map(|w| smallvec![w]);
    };

    let mut order: SmallVec<[(u64, &'k [u8]); 8]> = SmallVec::new();
    // 循环前缀外提：会话物理前缀单次读取，组内全键共用同一 scoped 种子域
    //（与 wtxn 事务键锁同一构造口径，三面同键同桶）
    let prefix = session.session_prefix();
    order.push((whasher::scoped_hash(&prefix, first_key), first_key));
    let second_key = second.to_key_ref();
    order.push((whasher::scoped_hash(&prefix, second_key), second_key));
    for key in iter {
      let k = key.to_key_ref();
      order.push((whasher::scoped_hash(&prefix, k), k));
    }

    let mut plan = self.build_rmw_lock_plan(&order)?;

    // 让渡登记守卫与活跃度续等见证：与单键臂同一契约
    //（见 RMW_LATCH_YIELD_BUDGET / RMW_LATCH_EXTEND_BUDGET 头注）
    let mut handoff: Option<HandoffReg> = None;
    let mut release_witnessed = 0usize;
    let ring_start = Instant::now();
    let mut round = 0usize;
    while round < RMW_LATCH_YIELD_BUDGET
      || (release_witnessed > 0
        && round < RMW_LATCH_YIELD_BUDGET + RMW_LATCH_EXTEND_BUDGET
        && ring_start.elapsed() < RMW_LATCH_EXTEND_DEADLINE)
    {
      round += 1;
      let cur_index = self.store.index.load_full();
      if !Arc::ptr_eq(&plan.index, &cur_index) {
        plan = self.build_rmw_lock_plan(&order)?;
      }
      match self.try_acquire_rmw_plan(&plan, true) {
        Ok(windows) => return Ok(windows),
        Err(fail_bucket) => {
          // 首失闩轮在争用单点桶登记让渡位；计划重建（索引版本推进）即换桶重登
          let re_register = match handoff.as_ref() {
            Some(reg) => !Arc::ptr_eq(&reg.index, &plan.index) || reg.bucket != fail_bucket,
            None => true,
          };
          if re_register {
            // 先出旧册再入新册：同桶跨版本时后建先拆会令旧守卫 Drop 擦掉新置位
            drop(handoff.take());
            handoff = Some(HandoffReg::new(plan.index.clone(), fail_bucket));
            release_witnessed = 0;
          }
          // 见放闩通知即不让核立即重试且计入续等见证（判据与形制见环头注）
          if plan.index.get_bucket(fail_bucket).consume_release_notify() {
            release_witnessed += 1;
            continue;
          }
        }
      }
      yield_now().await;
    }

    // 尾试前出册（同单键臂尾试纪律）
    drop(handoff);
    let cur_index = self.store.index.load_full();
    if !Arc::ptr_eq(&plan.index, &cur_index) {
      plan = self.build_rmw_lock_plan(&order)?;
    }
    match self.try_acquire_rmw_plan(&plan, true) {
      Ok(windows) => Ok(windows),
      Err(_) => Err(Error::Index(WindexError::LockTimeout)),
    }
  }

  /// 依据键哈希分裂协同并定位主桶与索引快照
  #[inline]
  fn locate_bucket_by_hash(&self, hash: u64) -> Option<(Arc<HashIndex>, usize)> {
    self.ensure_split_by_hash(hash).ok()?;
    let index = self.store.index.load_full();
    let bucket = index.bucket_index_for_hash(hash);
    Some((index, bucket))
  }

  /// 单键桶排他闩取闩核心（扩容协同先行 + 主桶定位 + **单次尝试**取闩；
  /// [`Self::try_rmw_window`] 与 [`Self::try_rmw_window_sorted`] 单键退化臂共用
  /// 的唯一取闩口，无第二张锁表）。失闩即回 `None` 交调用方降级，绝不嵌自旋
  /// 预算（票 wkv-rmw-window-single-key-async-latch-budget-latency-bomb 定因：
  /// 嵌套 1024 次自旋取闩 × 每次内含 `HashBucket::try_lock_exclusive` 10 次
  /// 线程让渡，永久持闩下单次调用即 ~3.5s 同核忙等，compio thread-per-core 下
  /// 阻塞本核全部任务 poll）；瞬态争用由 `HashBucket::try_lock_exclusive` 内嵌
  /// kMaxLockSpins 次让渡在单次尝试内部承接。寻桶经 [`whasher::scoped_hash`]
  /// 全仓单点（会话物理前缀种子），与 wtxn 事务键锁、TTL 键闩三面同键同桶互斥
  fn try_lock_key_bucket(&self, user_key: &[u8]) -> Option<(Arc<HashIndex>, usize)> {
    let hash = whasher::scoped_hash(&self.session_prefix(), user_key);
    let (index, bucket) = self.locate_bucket_by_hash(hash)?;
    if !index.get_bucket(bucket).try_lock_exclusive() {
      RMW_KEY_LATCH_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
      return None;
    }
    Some((index, bucket))
  }

  /// 取本键读改写原子窗口——让核等待臂（异步域专用，对标 C# 锁冲突转 pending 重试）
  ///
  /// 轮内执行体为**单次尝试**（与多键臂 [`Self::try_acquire_rmw_plan`] 先例同一
  /// 契约）：扩容协同 + 主桶定位经 [`Self::locate_bucket_by_hash`]（半迁移协同失败
  /// 沿用其 `.ok()` 吞错口径视同失闩，环内不开新错误通道），定位所得主桶上
  /// `HashBucket::try_lock_exclusive` 单次，失闩即让核进下一轮——轮间
  /// `wbase::future::yield_now` 即重试机制本体，轮内绝不嵌套自旋预算（轮内若嵌
  /// 1024 自旋 × 每次 10 线程让渡即 ≈3.5s 单轮，永久持闩下 1024 轮退化为小时级
  /// 延迟炸弹）；每轮 `load_full` 钉定当前活跃索引版本并按哈希换算桶下标，索引版本
  /// 推进即重定位（与 [`Self::rmw_window_sorted`] 轮内 `Arc::ptr_eq` 失效重建同一
  /// 判据的无计划直取形态）。持闩者在同核让出后即放闩，异核自旋即成；预算耗尽
  /// 回 [`windex::Error::LockTimeout`] 交调用方按存储错误应答，绝不留无界等待。
  /// 入口先行分裂协同并显式上抛迁移错误（同步臂 [`Self::try_rmw_window`]
  /// 的降级信号在此收口为终态错误）
  pub async fn rmw_window<'s, 'k>(&'s self, user_key: &'k [u8]) -> Result<RmwWindow<'s, 'k, D>> {
    let session: &'s StoreSession<D> = self;
    // 事务锁模式：本键桶排他闩已在本会话事务手上，直接回让闩窗口（判位于入口
    // 单次读取，与 rmw_window_sorted 同口径；模式位仅由本会话任务在命令分派点
    // 写入，让核等待期无外部翻转面）
    if session.session_locking().is_transactional() {
      return Ok(RmwWindow {
        session,
        user_key,
        held: None,
      });
    }
    // 扩容协同与取闩同 hash 单源（scoped 口径，禁按 A 哈希协同、按 B 哈希寻桶）
    let hash = whasher::scoped_hash(&session.session_prefix(), user_key);
    session.ensure_split_by_hash(hash)?;
    // 等闩让渡登记守卫 + 活跃度续等见证（契约见 RMW_LATCH_YIELD_BUDGET /
    // RMW_LATCH_EXTEND_BUDGET 头注）：首失闩轮入册，索引版本推进/换桶先出
    // 旧册再入新册，任何退出路径经 Drop 出册
    let mut handoff: Option<HandoffReg> = None;
    let mut release_witnessed = 0usize;
    let ring_start = Instant::now();
    let mut round = 0usize;
    while round < RMW_LATCH_YIELD_BUDGET
      || (release_witnessed > 0
        && round < RMW_LATCH_YIELD_BUDGET + RMW_LATCH_EXTEND_BUDGET
        && ring_start.elapsed() < RMW_LATCH_EXTEND_DEADLINE)
    {
      round += 1;
      if let Some((index, bucket)) = self.locate_bucket_by_hash(hash) {
        let slot = index.get_bucket(bucket);
        if slot.try_lock_exclusive_now() {
          return Ok(RmwWindow {
            session,
            user_key,
            held: Some((index, bucket)),
          });
        }
        RMW_KEY_LATCH_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
        // 首失闩轮登记让渡位；索引版本推进/换桶先出旧册再入新册
        let re_register = match handoff.as_ref() {
          Some(reg) => !Arc::ptr_eq(&reg.index, &index) || reg.bucket != bucket,
          None => true,
        };
        if re_register {
          // 先出旧册再入新册：同桶跨版本时后建先拆会令旧守卫 Drop 擦掉新置位
          drop(handoff.take());
          handoff = Some(HandoffReg::new(index.clone(), bucket));
          release_witnessed = 0;
        }
        // 见放闩通知即不让核立即重试且计入续等见证（判据与形制见环头注）
        if slot.consume_release_notify() {
          release_witnessed += 1;
          continue;
        }
      }
      yield_now().await;
    }
    // 尾试前先出册：终试与在册让渡位不冲突的形态统一由绕开臂承担，显式出册
    // 令尾试对全系统重开（失败即随本函数返回释放全部登记，绝不滞留）
    drop(handoff);
    match self.locate_bucket_by_hash(hash) {
      Some((index, bucket)) => {
        if index.get_bucket(bucket).try_lock_exclusive_now() {
          return Ok(RmwWindow {
            session,
            user_key,
            held: Some((index, bucket)),
          });
        }
        RMW_KEY_LATCH_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
        Err(Error::Index(WindexError::LockTimeout))
      }
      None => Err(Error::Index(WindexError::LockTimeout)),
    }
  }
}
