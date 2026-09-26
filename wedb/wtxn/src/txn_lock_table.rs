//! 事务键锁表（对标 Tsavorite `OverflowBucketLockTable` 的引擎实例侧承接）
//!
//! C# 归属口径：锁表随 store 构造并绑该 store 实例
//!（`Tsavorite.cs:105` 字段、`:228` `LockTable = new OverflowBucketLockTable<>(this)`），
//! 会话经 `SessionFunctionsWrapper.cs:30` `LockTable => _clientSession.store.LockTable`
//! 取用，事务侧 `TxnKeyEntry.cs:54` 持 store 的事务上下文而非全局表。
//!
//! 锁源口径（唯一，与 windex 存储引擎共用同一份内存）：C# 事务锁**没有独立锁表内存**，
//! 锁位就嵌在哈希桶本体的第一个溢出桶 entry word 高位
//!（`HashBucket.cs:15`「复用地址之后的全部位作闩」、`:40-116` TryAcquire* 直接 CAS 桶字），
//! 锁粒度即哈希表桶数并随 split 扩容自动细化
//!（`OverflowBucketLockTable.cs:15` `NumBuckets => store.state[version].size_mask + 1`、
//! `:26-34` `GetBucketIndex` 每次现取当前版本 `size_mask`）。本表据此**不再自造锁内存**：
//! 持一个 store 侧注入的索引装载闭包（对标 `OverflowBucketLockTable` 持 `store` 引用），
//! 每笔事务现取当前 `HashIndex` 版本（粒度随索引规模/扩容联动），键锁登记与释放
//! 一律走 windex `HashBucket` 的内嵌共享/独占闩——与 wkv TTL 读改写窗口
//!（`wkv/src/ttl.rs` 经 `try_lock_key_hash_exclusive` 按 scoped 同源哈希持桶闩）
//! 同一把锁、同一份内存、同一寻桶口径（票 wtxn-wkv-keybucket-hash-scope-desync）。
//!
//! 加锁协议与 C# 同为**无守卫闩**：键哈希 `hash & size_mask` 定位桶，取闩返回成败、
//! 放闩按桶下标显式调用，故持锁集合是一串纯数据桶下标
//!（[`crate::txn_key_entry::TxnKeyEntries`]），无 `'static` 伪生命周期、无手写
//! `Send`/`Sync`；阻塞与超时重试外层在事务侧（对标 C# `TransactionalContext.Lock`），
//! 桶下标升序取闩消除死锁。
//!
//! 全事务屏障：C# 事务起点的 PREPARE_GROW 拦截与活跃事务计数挂
//! `StateMachineDriver`（`TransactionManager.cs:Run/Reset` 经
//! `AcquireTransactionVersion`/`EndTransaction` 成对调用，且驱动器是随 store
//! 绑定的 `readonly` 字段——注销必回注册时那个实例），rust 侧由本表以 store
//! 注入的注册闭包承接：注册成功返回一张与产出它的驱动实例一一绑定的
//! [`TxnBarrier`] 票据，事务持票到桶闩尽释后 `end_txn` 注销。注册成功到注销
//! 期间，本笔事务钉定的 `HashIndex` 版本绝不会被在线扩容切换，杜绝跨版本锁脱节。

use std::{
  fmt,
  sync::{Arc, LazyLock},
  thread,
};

use windex::HashIndex;

/// [`TxnLockTable::new`] 独立/测试锁表的默认桶数（2 的幂，对齐 `HashIndex` 掩码寻址）；
/// 生产锁表由 store 注入真实索引，粒度随索引规模与 split 扩容联动，与此默认值无关。
const DEFAULT_TXN_BUCKETS: usize = 1024;

/// 活跃事务屏障驱动端口（对标 C# 事务随 store 绑定的 `StateMachineDriver`
/// 实例引用：`TransactionManager.cs:185` 构造期 `this.stateMachineDriver =
/// db.StateMachineDriver`，`Run` 前置 `AcquireTransactionVersion`、`Reset`
/// 收尾 `EndTransaction(txnVersion)` 皆打在**同一实例**上）
///
/// rust 侧一张实现本端口的票即「本笔事务已在该驱动上注册活跃事务」的凭据：
/// 注册闭包只在注册成功时产出，注销经 [`Self::end_txn`] 回到产出它的那台驱动
/// ——引擎在线置换亦不会把计数错减到他实例（错减令新引擎计数下溢翻转，
/// 此后每次扩容排空永不自收敛）。
pub trait TxnBarrier: Send + Sync {
  /// 注销本笔活跃事务（对标 C# StateMachineDriver.cs:EndTransaction）
  ///
  /// 调用方必须已释放全部桶闩，且在 [`TxnBarrier`] 票据生命周期内至多调用一次
  /// （本域唯一调用点是 [`crate::TransactionManager::reset`] 的 take 收口）。
  fn end_txn(&self);
}

/// 屏障注册票据（注册成功的凭据；`None` = 未注册，调用方让步重试）
pub type TxnBarrierTicket = Arc<dyn TxnBarrier>;

/// 无扩容语义的直通票据（独立锁表 / 无 store 场景：注册恒成功、注销空操作）
struct NoopBarrier;

impl TxnBarrier for NoopBarrier {
  #[inline]
  fn end_txn(&self) {}
}

/// 直通票据单例（免每笔事务一次 Arc 分配）
static NOOP_BARRIER: LazyLock<TxnBarrierTicket> = LazyLock::new(|| Arc::new(NoopBarrier));

/// 事务键锁表（引擎实例所有，克隆即共享同一锁源）
///
/// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:OverflowBucketLockTable
///
/// 锁源与扩容屏障同源一并注入：C# 事务与锁表共挂同一 store 的
/// `StateMachineDriver`（`TransactionManager.cs:Run` 前置
/// `AcquireTransactionVersion` 的 PREPARE_GROW 全事务屏障 +
/// `Reset` 收尾 `EndTransaction`），rust 侧据此把该协议做成 store 注入的
/// 注册闭包，独立锁表默认直通（无扩容语义）。
pub struct TxnLockTable {
  /// store 侧注入的索引装载闭包：每笔事务现取当前 `HashIndex` 版本
  ///（对标 C# `store.state[resizeInfo.version]` 逐次现取，锁粒度随扩容细化）
  loader: Arc<dyn Fn() -> Arc<HashIndex> + Send + Sync>,
  /// store 侧注入的屏障注册闭包（单次尝试，Some = 已注册活跃事务并持注销票据、
  /// None = PrepareGrow 占用；对标 C# StateMachineDriver.cs:AcquireTransactionVersion）
  acquire: Arc<dyn Fn() -> Option<TxnBarrierTicket> + Send + Sync>,
}

impl Clone for TxnLockTable {
  /// 克隆共享句柄（对标 C# 各会话取同一 `store.LockTable` 引用）
  #[inline]
  fn clone(&self) -> Self {
    Self {
      loader: Arc::clone(&self.loader),
      acquire: Arc::clone(&self.acquire),
    }
  }
}

impl Default for TxnLockTable {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl fmt::Debug for TxnLockTable {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("TxnLockTable")
      .field("bucket_count", &self.pin().size)
      .finish()
  }
}

impl TxnLockTable {
  /// 构造独立锁表（自带一张默认规模 `HashIndex`，供无 store 的单元/测试场景；
  /// 与生产同机制，仅桶数为默认值、屏障直通）
  #[inline]
  pub fn new() -> Self {
    let index = Arc::new(
      HashIndex::new(DEFAULT_TXN_BUCKETS)
        .expect("DEFAULT_TXN_BUCKETS 为合法 2 的幂，HashIndex 构造恒成功"),
    );
    Self::from_loader(move || Arc::clone(&index))
  }

  /// 由 store 侧索引装载闭包构造引擎实例锁表（对标 `Tsavorite.cs:228` 随 store 构造）
  ///
  /// `loader` 每次调用现取当前 `HashIndex` 版本，使锁粒度随索引 split 在线扩容细化
  ///（对标 C# `GetBucketIndex` 逐次现取 `store.state[version].size_mask`）。
  /// 屏障直通无扩容语义；生产装配用 [`Self::from_loader_gated`] 挂接扩容状态机。
  #[inline]
  pub fn from_loader(loader: impl Fn() -> Arc<HashIndex> + Send + Sync + 'static) -> Self {
    Self {
      loader: Arc::new(loader),
      acquire: Arc::new(|| Some(Arc::clone(&NOOP_BARRIER))),
    }
  }

  /// 生产装配：索引装载闭包 + PREPARE_GROW 全事务屏障注册闭包
  ///（对标 C# 锁表与 `StateMachineDriver` 同挂一个 store——
  /// `TransactionManager.cs:Run/Reset` 的 `AcquireTransactionVersion` /
  /// `EndTransaction` 协议由 wkv `IndexResizeState` 承接，经本闭包注入）
  ///
  /// `acquire` 单次尝试注册活跃事务：`None` = 扩容处于 PrepareGrow，交调用方
  /// 让步重试；`Some(票据)` = 已注册，票据即注销句柄，桶闩尽释后 `end_txn`
  /// 一次注销。注册与注销经同一票据收口，杜绝跨实例错配与漏减/重减。
  #[inline]
  pub fn from_loader_gated(
    loader: impl Fn() -> Arc<HashIndex> + Send + Sync + 'static,
    acquire: impl Fn() -> Option<TxnBarrierTicket> + Send + Sync + 'static,
  ) -> Self {
    Self {
      loader: Arc::new(loader),
      acquire: Arc::new(acquire),
    }
  }

  /// 现取当前索引版本并钉定为整笔事务的锁面（取锁与放锁共用同一版本，跨 resize 不串锁）
  #[inline]
  pub fn pin(&self) -> Arc<HashIndex> {
    (self.loader)()
  }

  /// 事务加锁前的全事务屏障阻塞注册（对标 C#
  /// StateMachineDriver.cs:AcquireTransactionVersion 的
  /// `while (Phase == PREPARE_GROW) { ProtectAndDrain(); Thread.Yield(); }`
  /// 线程臂）：仅真线程上下文可用；compio 态用 [`Self::try_acquire_txn`] 单次
  /// 尝试 + 慢臂重驱，绝不占死 reactor。
  pub fn acquire_txn(&self) -> TxnBarrierTicket {
    loop {
      if let Some(ticket) = self.try_acquire_txn() {
        return ticket;
      }
      thread::yield_now();
    }
  }

  /// 屏障单次非阻塞注册尝试；`None` = 扩容处于 PrepareGrow，本笔事务尚未启动，
  /// 交调用方走既有让步/重驱臂重试。注册成功后本事务钉定的索引版本在票据
  /// [`TxnBarrier::end_txn`] 前绝不会被切换。
  #[inline]
  pub fn try_acquire_txn(&self) -> Option<TxnBarrierTicket> {
    (self.acquire)()
  }

  /// 键哈希定位桶下标（`hash & size_mask`，与 windex 同址；对标 C# `GetBucketIndex`）
  #[inline]
  pub fn bucket_index_for_hash(&self, key_hash: i64) -> usize {
    self.pin().bucket_index_for_hash(key_hash as u64)
  }

  /// 尝试取桶共享闩；竞争即返回 false
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:TryLockShared
  #[inline]
  pub fn try_lock_shared(&self, bucket: usize) -> bool {
    self.pin().bucket(bucket).try_lock_shared()
  }

  /// 尝试取桶独占闩（读者未排空即回退并返回 false）
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:TryLockExclusive
  #[inline]
  pub fn try_lock_exclusive(&self, bucket: usize) -> bool {
    self.pin().bucket(bucket).try_lock_exclusive()
  }

  /// 放桶共享闩
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:UnlockShared
  #[inline]
  pub fn unlock_shared(&self, bucket: usize) {
    self.pin().bucket(bucket).unlock_shared();
  }

  /// 放桶独占闩
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:UnlockExclusive
  #[inline]
  pub fn unlock_exclusive(&self, bucket: usize) {
    self.pin().bucket(bucket).unlock_exclusive();
  }
}
