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
//! 每笔事务开始现取当前 `HashIndex` 版本（粒度随索引规模/扩容联动），键锁登记与释放
//! 一律走 windex `HashBucket` 的内嵌共享/独占闩——与 wkv TTL 读改写窗口
//!（`wkv/src/ttl.rs` 经 `lock_key_exclusive` 持桶闩）同一把锁、同一份内存。
//!
//! 加锁协议与 C# 同为**无守卫闩**：键哈希 `hash & size_mask` 定位桶，取闩返回成败、
//! 放闩按桶下标显式调用，故持锁集合是一串纯数据桶下标
//!（[`crate::txn_key_entry::TxnKeyEntries`]），无 `'static` 伪生命周期、无手写
//! `Send`/`Sync`；阻塞与超时重试外层在事务侧（对标 C# `TransactionalContext.Lock`），
//! 桶下标升序取闩消除死锁。

use std::{fmt, sync::Arc};

use windex::HashIndex;

/// [`TxnLockTable::new`] 独立/测试锁表的默认桶数（2 的幂，对齐 `HashIndex` 掩码寻址）；
/// 生产锁表由 store 注入真实索引，粒度随索引规模与 split 扩容联动，与此默认值无关。
const DEFAULT_TXN_BUCKETS: usize = 1024;

/// 事务键锁表（引擎实例所有，克隆即共享同一锁源）
///
/// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable.cs:OverflowBucketLockTable
pub struct TxnLockTable {
  /// store 侧注入的索引装载闭包：每笔事务现取当前 `HashIndex` 版本
  ///（对标 C# `store.state[resizeInfo.version]` 逐次现取，锁粒度随扩容细化）
  loader: Arc<dyn Fn() -> Arc<HashIndex> + Send + Sync>,
}

impl Clone for TxnLockTable {
  /// 克隆共享句柄（对标 C# 各会话取同一 `store.LockTable` 引用）
  #[inline]
  fn clone(&self) -> Self {
    Self {
      loader: Arc::clone(&self.loader),
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
  /// 与生产同机制，仅桶数为默认值）
  #[inline]
  pub fn new() -> Self {
    let index = Arc::new(
      HashIndex::new(DEFAULT_TXN_BUCKETS)
        .expect("DEFAULT_TXN_BUCKETS 为合法 2 的幂，HashIndex 构造恒成功"),
    );
    Self {
      loader: Arc::new(move || Arc::clone(&index)),
    }
  }

  /// 由 store 侧索引装载闭包构造引擎实例锁表（对标 `Tsavorite.cs:228` 随 store 构造）
  ///
  /// `loader` 每次调用现取当前 `HashIndex` 版本，使锁粒度随索引 split 在线扩容细化
  ///（对标 C# `GetBucketIndex` 逐次现取 `store.state[version].size_mask`）。
  #[inline]
  pub fn from_loader(loader: impl Fn() -> Arc<HashIndex> + Send + Sync + 'static) -> Self {
    Self {
      loader: Arc::new(loader),
    }
  }

  /// 现取当前索引版本并钉定为整笔事务的锁面（取锁与放锁共用同一版本，跨 resize 不串锁）
  #[inline]
  pub fn pin(&self) -> Arc<HashIndex> {
    (self.loader)()
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
