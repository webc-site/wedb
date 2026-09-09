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

mod collection;
mod keys;
mod raw;

use std::{
  ops::Deref,
  result::Result as StdResult,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
  },
};

pub use collection::RawCollectionRead;
use wdev::Device;
use wepoch::{EpochGuard, Participant};
use wval::SessionPrefixBuf;

use crate::{error::Result, store::WedbStore};

/// 小哈希紧凑内联最大字段数（对齐 Redis 规范 512 项门限）
pub const HASH_MAX_COMPACT_ENTRIES: usize = 512;
/// 小哈希紧凑内联单个值最大字节数
pub const HASH_MAX_COMPACT_VALUE: usize = 64;
/// 小集合紧凑内联最大元素数
pub const SET_MAX_COMPACT_ENTRIES: usize = 128;
/// 小集合紧凑内联单个元素最大字节数
pub const SET_MAX_COMPACT_VALUE: usize = 64;
/// 小有序集合紧凑内联最大元素数
pub const ZSET_MAX_COMPACT_ENTRIES: usize = 128;
/// 小有序集合紧凑内联单个元素最大字节数
pub const ZSET_MAX_COMPACT_MEMBER: usize = 64;
/// 紧凑内联编码总大小安全门限（4096 字节，防止单条记录过大导致内存和 I/O 碎片化）
pub const MAX_COMPACT_TOTAL_BYTES: usize = 4096;

/// 客户端并发会话句柄（绑定一个 LightEpoch 参与者）
pub struct StoreSession<D: Device> {
  pub store: Arc<WedbStore<D>>,
  pub participant: Participant,
  pub copy_reads_to_tail: AtomicBool,
  pub record_elision: AtomicBool,
  pub namespace: AtomicU64,
  pub active_db: AtomicU64,
}

impl<D: Device> StoreSession<D> {
  /// 创建新的客户端会话（默认 ns=0, db=0）
  pub fn new(store: Arc<WedbStore<D>>, participant: Participant) -> Self {
    Self {
      store,
      participant,
      copy_reads_to_tail: AtomicBool::new(false),
      record_elision: AtomicBool::new(false),
      namespace: AtomicU64::new(0),
      active_db: AtomicU64::new(0),
    }
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

  /// 获取当前会话的 19 字节前缀缓冲（方案 A）
  ///
  /// 前缀由 `namespace`/`active_db` 两个原子变量唯一决定，`SessionPrefixBuf::new` 为
  /// const fn 纯栈上构造（19 字节零堆分配），按需重算完全免去 RwLock 读锁的原子计数器开销。
  /// 会话内命令严格串行执行，两个原子变量不存在撕裂风险。
  #[inline(always)]
  pub fn session_prefix(&self) -> SessionPrefixBuf {
    SessionPrefixBuf::new(self.namespace.load(Relaxed), self.active_db.load(Relaxed))
  }

  /// 原子更新当前会话的命名空间与活跃数据库编号（前缀由 `session_prefix` 按需重算）
  pub fn set_context(&self, ns: u64, db: u64) {
    self.namespace.store(ns, Relaxed);
    self.active_db.store(db, Relaxed);
  }

  /// 设置当前会话的活跃数据库编号并更新会话前缀
  #[inline]
  pub fn set_active_db(&self, db: u64) {
    self.set_context(self.namespace(), db);
  }

  /// 设置当前会话的命名空间并更新会话前缀
  #[inline]
  pub fn set_namespace(&self, ns: u64) {
    self.set_context(ns, self.active_db());
  }

  /// 设置是否在冷区读取成功后将记录自动提升追加到 Tail (对标 C# Garnet CopyReadsToTail)
  #[inline]
  pub fn set_copy_reads_to_tail(&self, enable: bool) {
    self.copy_reads_to_tail.store(enable, Relaxed);
  }

  /// 获取当前是否开启冷读提升回 Tail
  #[inline]
  pub fn copy_reads_to_tail(&self) -> bool {
    self.copy_reads_to_tail.load(Relaxed)
  }

  /// 设置是否开启记录脱钩剔除回收 (对标 C# Garnet RevivificationSettings.EnableRecordElision)
  #[inline]
  pub fn set_record_elision(&self, enable: bool) {
    self.record_elision.store(enable, Relaxed);
  }

  /// 获取当前是否开启记录脱钩剔除回收
  #[inline]
  pub fn record_elision(&self) -> bool {
    self.record_elision.load(Relaxed)
  }

  /// 进入批处理纪元保护上下文（严格对标 C# Garnet IUnsafeContext.BeginUnsafe）
  #[inline]
  pub fn enter_batch(&self) -> BatchStoreSession<'_, D> {
    let guard = self.participant.enter();
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
  /// 同步内存直读快路径（在已保护纪元下执行，彻底绕过 enter() 原子开销）
  #[inline(always)]
  pub fn try_read_in_memory<R>(
    &self,
    key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<Option<R>>> {
    self.session.try_read_in_memory_unprotected(key, f)
  }

  /// 在批处理纪元保护下尝试原位读-改-写记录（完全绕过 enter() 原子开销）
  #[inline(always)]
  pub fn try_modify_in_place<R>(
    &self,
    key: &[u8],
    f: impl FnOnce(&mut [u8]) -> Option<R>,
  ) -> Result<Option<R>> {
    self.session.try_modify_in_place_unprotected(key, f)
  }

  /// 在批处理已有纪元保护下尝试利用动态松弛原位覆写记录的值（严格对标 C# Garnet LogRecord.TrySetPinnedValueSpan）
  #[inline(always)]
  pub fn try_modify_with_slack(&self, key: &[u8], new_val: &[u8]) -> Result<bool> {
    self.session.try_modify_with_slack_unprotected(key, new_val)
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

  /// 同步读当前会话普通字符串键快路径（TTL 快门控 + 内存直读，零 enter() 原子开销）
  ///
  /// 返回三态：
  /// - `Ok(Some(Some(r)))`：内存命中，闭包零拷贝消费；
  /// - `Ok(Some(None))`：内存中明确不存在（无候选 / 墓碑）；
  /// - `Ok(None)`：须降级全异步 `read_with().await`（存在磁盘候选，或 TTL 记录标签
  ///   命中需异步过期裁决——`check_expired` 含磁盘路径与物理清除，绝不跨纪元 await）。
  #[inline(always)]
  pub fn try_read_sync<R>(
    &self,
    key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<Option<Option<R>>> {
    if self.session.has_ttl_tag_unprotected(key)? {
      return Ok(None);
    }
    self.session.try_read_in_memory_unprotected(key, f)
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
}
