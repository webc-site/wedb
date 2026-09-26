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
  fmt,
  hint::spin_loop,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
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
#[derive(Clone, Copy)]
struct RmwLockPlanSlot<'k> {
  bucket: usize,
  user_key: &'k [u8],
}

/// 待排定键条目
#[derive(Clone, Copy)]
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

/// 同步快路径取闩重试预算（索引层单键闩入口 `HashIndex::try_lock_key_exclusive`
/// 为一次尝试、无自旋驱动、无超时判定，取闩失败的重试由本调用方承接——对标 C#
/// ephemeral 取闩失败回 `RETRY_LATER`）：持闩期只有读—算—写三段纯内存操作，
/// 微秒级即放闩，故 1024 轮单次尝试足够；预算耗尽仍不得闩即交调用方降级，
/// 同步域绝不无限自旋
const RMW_LATCH_SPIN_ATTEMPTS: usize = 1024;

/// 异步域让核重试预算（对标 C# 锁冲突转 pending 重试的外层循环）：预算耗尽即回
/// [`windex::Error::LockTimeout`]，杜绝同线程持闩者不再让核时的无界自旋
const RMW_LATCH_YIELD_BUDGET: usize = 1024;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
      index.bucket(*bucket).unlock_exclusive();
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
#[derive(Debug)]
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
  /// 返回 `None` = 自旋预算内未取到本键桶排他闩（同键并发 RMW / 他事务持锁 /
  /// EXPIRE 窗口占闩），或扩容协同期分块迁移失败（半迁移态禁对未迁移桶加闩，
  /// 视同未取闩降级；错误由异步臂 [`Self::rmw_window`] 入口同判据 `?` 显式上抛），
  /// 调用方按既有降级通道转异步闭环（对标 C# ephemeral 取闩
  /// 失败回 `RETRY_LATER`），同步域绝不无限自旋；事务锁模式下本键桶闩已在本会话
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

  /// 依缓存计划尝试依次获取排他闩（任一失闩整体 RAII 放闩回 None）
  ///
  /// 槽内取闩为**单次尝试**（票 pair_bucket_order_latch 挂死案定因）：本函数
  /// 是让核预算环（[`Self::rmw_window`] / [`Self::rmw_window_sorted`]）的
  /// 轮内执行体，轮间让核（`wbase::future::yield_now`）即重试机制本体——轮内
  /// 若再嵌套 [`Self::try_lock_bucket_exclusive`] 的 1024 次自旋预算（每次又
  /// 内含 `HashBucket::try_lock_exclusive` 的 10 次线程让渡），单轮成本即
  /// 1024×10 次线程让渡（实测 ~3.5s，macOS swtch_pri 按时间片退让），永久
  /// 持闩（外部件钉闩 / 卡死持有者）下 1024 轮预算环退化为小时级延迟炸弹，
  /// fail-closed 忙应答永不到达。瞬态争用由 HashBucket 内嵌的 kMaxLockSpins
  /// 次让渡承接（亚微秒持有者轮内即得），跨轮持闩交轮间让核，两层各司其职
  /// 不重排
  fn try_acquire_rmw_plan<'s, 'k>(
    &'s self,
    plan: &RmwLockPlan<'k>,
  ) -> Option<SmallVec<[RmwWindow<'s, 'k, D>; 8]>> {
    let session: &'s StoreSession<D> = self;
    let mut windows: SmallVec<[RmwWindow<'s, 'k, D>; 8]> =
      SmallVec::with_capacity(plan.slots.len());
    for slot in &plan.slots {
      if !plan.index.bucket(slot.bucket).try_lock_exclusive() {
        RMW_PLAN_ACQUIRE_MISS.fetch_add(1, Ordering::Relaxed);
        return None;
      }
      windows.push(RmwWindow {
        session,
        user_key: slot.user_key,
        held: Some((plan.index.clone(), slot.bucket)),
      });
    }
    Some(windows)
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
    self.try_acquire_rmw_plan(&plan)
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

    for _ in 0..RMW_LATCH_YIELD_BUDGET {
      let cur_index = self.store.index.load_full();
      if !Arc::ptr_eq(&plan.index, &cur_index) {
        plan = self.build_rmw_lock_plan(&order)?;
      }
      if let Some(windows) = self.try_acquire_rmw_plan(&plan) {
        return Ok(windows);
      }
      yield_now().await;
    }

    let cur_index = self.store.index.load_full();
    if !Arc::ptr_eq(&plan.index, &cur_index) {
      plan = self.build_rmw_lock_plan(&order)?;
    }
    match self.try_acquire_rmw_plan(&plan) {
      Some(windows) => Ok(windows),
      None => Err(Error::Index(WindexError::LockTimeout)),
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

  /// 在自旋预算内尝试获取指定桶排他闩
  #[inline]
  fn try_lock_bucket_exclusive(index: &HashIndex, bucket: usize) -> bool {
    let bucket_ref = index.bucket(bucket);
    let mut taken = bucket_ref.try_lock_exclusive();
    for _ in 0..RMW_LATCH_SPIN_ATTEMPTS {
      if taken {
        break;
      }
      spin_loop();
      taken = bucket_ref.try_lock_exclusive();
    }
    taken
  }

  /// 单键桶排他闩取闩核心（扩容协同先行 + 主桶定位 + 自旋预算内单次尝试
  /// 循环；[`Self::try_rmw_window`] 与 [`Self::try_rmw_window_sorted`] 共用
  /// 的唯一取闩口，无第二张锁表）。寻桶经 [`whasher::scoped_hash`] 全仓单点
  ///（会话物理前缀种子），与 wtxn 事务键锁、TTL 键闩三面同键同桶互斥
  fn try_lock_key_bucket(&self, user_key: &[u8]) -> Option<(Arc<HashIndex>, usize)> {
    let hash = whasher::scoped_hash(&self.session_prefix(), user_key);
    let (index, bucket) = self.locate_bucket_by_hash(hash)?;
    if !Self::try_lock_bucket_exclusive(&index, bucket) {
      return None;
    }
    Some((index, bucket))
  }

  /// 取本键读改写原子窗口——让核等待臂（异步域专用，对标 C# 锁冲突转 pending 重试）
  ///
  /// 每轮失败让核一次（持闩者在同核让出后即放闩，异核自旋即成），预算耗尽回
  /// [`windex::Error::LockTimeout`] 交调用方按存储错误应答，绝不留无界等待。
  /// 入口先行分裂协同并显式上抛迁移错误（同步臂 [`Self::try_rmw_window`]
  /// 的降级信号在此收口为终态错误）
  pub async fn rmw_window<'s, 'k>(&'s self, user_key: &'k [u8]) -> Result<RmwWindow<'s, 'k, D>> {
    if !self.session_locking().is_transactional() {
      // 扩容协同与取闩同 hash 单源（scoped 口径，禁按 A 哈希协同、按 B 哈希寻桶）
      let hash = whasher::scoped_hash(&self.session_prefix(), user_key);
      self.ensure_split_by_hash(hash)?;
    }
    for _ in 0..RMW_LATCH_YIELD_BUDGET {
      if let Some(window) = self.try_rmw_window(user_key) {
        return Ok(window);
      }
      yield_now().await;
    }
    match self.try_rmw_window(user_key) {
      Some(window) => Ok(window),
      None => Err(Error::Index(WindexError::LockTimeout)),
    }
  }
}
