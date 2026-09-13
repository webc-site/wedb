//! 向量集合键锁与索引读取（对标 libs/server/Resp/Vector/VectorManager.Locking.cs 与 libs/common/Synchronization/ReadOptimizedLock.cs）
//!
//! C# 以 ReadOptimizedLock（键哈希 → 读优化锁）实现共享读 / 独占写的
//! 升降级协议：共享读命中"需重建"的索引时升级独占，重建完成降级共享，
//! 并把持有的锁以 VectorSetLock（IDisposable）交还调用方。
//! Rust 侧以 [`VectorSetLocks`] 静态条带锁（键哈希 → 128 字节对齐 RawRwLock 条带数组）承接；
//! 零 ConcurrentMap 查找，零每键堆分配，彻底消除伪共享（False Sharing）。
//! parking_lot 无原子升降级，升级以"释放共享 → 竞争独占 → 复核"的
//! 让步重试形态对齐 C# TryPromote 失败后的再入路径。
//! 索引记录的存储读取经 [`super::vector_manager::VectorManager`] 的
//! 键值登记表承接（wkv 集成前为域内表）。

use core::{fmt, mem, ops};
use std::sync::Arc;

use parking_lot::lock_api::RawRwLock as _;
use wvector::{IndexConfig, VectorDistanceMetricType, VectorQuantType, VectorSetFlags};

use super::{
  vector_manager::{INDEX_SIZE_BYTES, VectorManager, VectorManagerResult},
  vector_manager_index::Index,
  vector_manager_quantization::{QuantizationChannel, QuantizationState, QuantizationStep},
};

/// 固定条带数（2 的幂，对齐 Garnet 静态分片哈希条带架构）。
pub const STRIPE_COUNT: usize = 256;

/// 条带掩码（编译期位与计算）。
pub const STRIPE_MASK: usize = STRIPE_COUNT - 1;

const _: () = assert!(STRIPE_COUNT.is_power_of_two(), "STRIPE_COUNT 必须为 2 的幂");

/// 缓存行对齐读写锁条带（128 字节对齐，防止多核 CPU 伪共享）。
#[repr(align(128))]
pub struct CacheAlignedLock(pub parking_lot::RawRwLock);

const _: () = assert!(
  mem::align_of::<CacheAlignedLock>() >= 128,
  "CacheAlignedLock 必须至少 128 字节对齐"
);

const _: () = assert!(
  mem::size_of::<CacheAlignedLock>() >= 128,
  "CacheAlignedLock 尺寸必须至少 128 字节以独立占有缓存行"
);

impl CacheAlignedLock {
  /// 初始化单个条带锁。
  #[inline]
  pub const fn new() -> Self {
    Self(parking_lot::RawRwLock::INIT)
  }
}

impl Default for CacheAlignedLock {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl fmt::Debug for CacheAlignedLock {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("CacheAlignedLock")
      .field("is_locked", &self.0.is_locked())
      .field("is_locked_exclusive", &self.0.is_locked_exclusive())
      .finish()
  }
}

/// 共享读守卫：存续期间索引不可被丢弃或重建（对齐 C# VectorSetLock 共享形态）。
pub struct VectorSetSharedGuard<'a> {
  stripes: &'a [CacheAlignedLock; STRIPE_COUNT],
  stripe: usize,
}

const _: () = assert!(
  mem::size_of::<VectorSetSharedGuard<'static>>() == 16,
  "VectorSetSharedGuard 需保持 16 字节以利用 CPU 寄存器传递"
);

impl<'a> VectorSetSharedGuard<'a> {
  /// 获取当前守卫锁定的条带索引。
  #[inline]
  pub fn stripe(&self) -> usize {
    self.stripe
  }
}

impl<'a> Drop for VectorSetSharedGuard<'a> {
  #[inline]
  fn drop(&mut self) {
    // SAFETY: self.stripe 在加锁时由 stripe_for(key) 经 STRIPE_MASK 截断，必然 < STRIPE_COUNT
    unsafe {
      self.stripes.get_unchecked(self.stripe).0.unlock_shared();
    }
  }
}

impl<'a> fmt::Debug for VectorSetSharedGuard<'a> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("VectorSetSharedGuard")
      .field("stripe", &self.stripe)
      .finish()
  }
}

/// 独占写守卫（对齐 C# VectorSetLock 独占形态；持有条带借用引用）。
pub struct VectorSetLockGuard<'a> {
  stripes: &'a [CacheAlignedLock; STRIPE_COUNT],
  stripe: usize,
}

const _: () = assert!(
  mem::size_of::<VectorSetLockGuard<'static>>() == 16,
  "VectorSetLockGuard 需保持 16 字节以利用 CPU 寄存器传递"
);

impl<'a> VectorSetLockGuard<'a> {
  /// 获取当前守卫锁定的条带索引。
  #[inline]
  pub fn stripe(&self) -> usize {
    self.stripe
  }
}

impl<'a> Drop for VectorSetLockGuard<'a> {
  #[inline]
  fn drop(&mut self) {
    // SAFETY: self.stripe 在加锁时由 stripe_for(key) 经 STRIPE_MASK 截断，必然 < STRIPE_COUNT
    unsafe {
      self.stripes.get_unchecked(self.stripe).0.unlock_exclusive();
    }
  }
}

impl<'a> fmt::Debug for VectorSetLockGuard<'a> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("VectorSetLockGuard")
      .field("stripe", &self.stripe)
      .finish()
  }
}

/// 向量集合键的读优化静态条带锁表（对标 Garnet ReadOptimizedLock）。
///
/// 堆上单次平铺固定分配 STRIPE_COUNT 个条带，无 ConcurrentMap 并发查找开销，
/// 无每键动态堆分配，消除锁 Arc 频繁创建销毁与字典膨胀。
#[derive(Clone)]
pub struct VectorSetLocks {
  stripes: Arc<[CacheAlignedLock; STRIPE_COUNT]>,
}

const _: () = assert!(
  mem::size_of::<VectorSetLocks>() == 8,
  "VectorSetLocks 内部仅持有一个薄指针 Arc"
);

impl Default for VectorSetLocks {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl fmt::Debug for VectorSetLocks {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("VectorSetLocks")
      .field("stripe_count", &self.stripes.len())
      .finish()
  }
}

impl ops::Deref for VectorSetLocks {
  type Target = [CacheAlignedLock; STRIPE_COUNT];

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.stripes
  }
}

impl VectorSetLocks {
  /// 初始化静态条带锁表（堆上平铺单次分配，避免栈溢出）。
  pub fn new() -> Self {
    let mut stripes = Vec::with_capacity(STRIPE_COUNT);
    for _ in 0..STRIPE_COUNT {
      stripes.push(CacheAlignedLock::new());
    }
    let boxed: Box<[CacheAlignedLock; STRIPE_COUNT]> = match stripes.into_boxed_slice().try_into() {
      Ok(b) => b,
      Err(_) => unreachable!(),
    };
    Self {
      stripes: Arc::from(boxed),
    }
  }

  /// 获取条带固定数组引用。
  #[inline]
  pub fn stripes(&self) -> &[CacheAlignedLock; STRIPE_COUNT] {
    &self.stripes
  }

  /// 计算键所属的条带编号。
  #[inline]
  pub fn stripe_for(&self, key: &[u8]) -> usize {
    stripe_for(key)
  }

  /// 获取共享锁（读路径；guard 存续期间阻止索引被丢弃）。
  #[inline]
  pub fn acquire_shared<'a>(&'a self, key: &[u8]) -> VectorSetSharedGuard<'a> {
    let stripe = stripe_for(key);
    // SAFETY: stripe_for 由 STRIPE_MASK 截断，必然 < STRIPE_COUNT
    unsafe { self.stripes.get_unchecked(stripe) }
      .0
      .lock_shared();
    VectorSetSharedGuard {
      stripes: &self.stripes,
      stripe,
    }
  }

  /// 尝试非阻塞共享锁获取；竞争时返回 None。
  #[inline]
  pub fn try_acquire_shared<'a>(&'a self, key: &[u8]) -> Option<VectorSetSharedGuard<'a>> {
    let stripe = stripe_for(key);
    // SAFETY: stripe_for 由 STRIPE_MASK 截断，必然 < STRIPE_COUNT
    if unsafe { self.stripes.get_unchecked(stripe) }
      .0
      .try_lock_shared()
    {
      Some(VectorSetSharedGuard {
        stripes: &self.stripes,
        stripe,
      })
    } else {
      None
    }
  }

  /// 获取独占锁（VREM / VSETATTR / 重建等写路径）。
  #[inline]
  pub fn acquire_exclusive<'a>(&'a self, key: &[u8]) -> VectorSetLockGuard<'a> {
    let stripe = stripe_for(key);
    // SAFETY: stripe_for 由 STRIPE_MASK 截断，必然 < STRIPE_COUNT
    unsafe { self.stripes.get_unchecked(stripe) }
      .0
      .lock_exclusive();
    VectorSetLockGuard {
      stripes: &self.stripes,
      stripe,
    }
  }

  /// 尝试非阻塞独占锁获取；竞争时返回 None。
  #[inline]
  pub fn try_acquire_exclusive<'a>(&'a self, key: &[u8]) -> Option<VectorSetLockGuard<'a>> {
    let stripe = stripe_for(key);
    // SAFETY: stripe_for 由 STRIPE_MASK 截断，必然 < STRIPE_COUNT
    if unsafe { self.stripes.get_unchecked(stripe) }
      .0
      .try_lock_exclusive()
    {
      Some(VectorSetLockGuard {
        stripes: &self.stripes,
        stripe,
      })
    } else {
      None
    }
  }

  /// 是否被独占持有（测试/调试辅助；对标 C# 独占锁状态判定）。
  #[inline]
  pub fn is_locked_exclusive(&self, key: &[u8]) -> bool {
    let stripe = stripe_for(key);
    // SAFETY: stripe_for 由 STRIPE_MASK 截断，必然 < STRIPE_COUNT
    unsafe { self.stripes.get_unchecked(stripe) }
      .0
      .is_locked_exclusive()
  }
}

/// `read_vector_index_core` 的三态结果（对齐 C# 的 status + wouldBlock 出参）。
#[derive(Debug)]
pub enum ReadIndexOutcome<'a> {
  /// 命中：索引记录 + 持有共享锁的守卫（存续期间索引不可被丢弃）。
  Hit(Index, VectorSetSharedGuard<'a>),
  /// 键不存在（C# readRes != OK）。
  NotFound,
  /// 锁竞争：调用方应让出线程后异步重试（仅 non_blocking 形态）。
  WouldBlock,
}

/// 键哈希（gxhash 64 位，对齐 C# GetKeyHash 的条带定位职责）。
#[inline]
pub fn key_hash_of(key: &[u8]) -> u64 {
  gxhash::gxhash64(key, 0)
}

/// 键哈希条带定位（gxhash 64 位，位与 STRIPE_MASK，O(1) 零分配）。
#[inline]
pub fn stripe_for(key: &[u8]) -> usize {
  (key_hash_of(key) as usize) & STRIPE_MASK
}

/// 新建索引的几何参数（对齐 C# ReadOrCreateVectorIndex 自 parseState 提取的字段集）。
#[derive(Debug, Clone, Copy)]
pub struct CreateIndexParams {
  /// 键所属 hash slot（创建时关联，供后续迁移）。
  pub hash_slot: u16,
  /// 向量维度。
  pub dims: u32,
  /// 降维后维度（0 = 不降维）。
  pub reduce_dims: u32,
  /// 量化类型。
  pub quant: VectorQuantType,
  /// 构建期探索因子。
  pub build_exploration_factor: u32,
  /// 每层链接数（M）。
  pub num_links: u32,
  /// 距离度量。
  pub distance_metric: VectorDistanceMetricType,
}

impl CreateIndexParams {
  /// 转换为索引几何配置。
  pub fn index_config(&self) -> IndexConfig {
    IndexConfig {
      dims: self.dims,
      reduce_dims: self.reduce_dims,
      quant_type: self.quant,
      distance_metric: self.distance_metric,
      build_exploration_factor: self.build_exploration_factor,
      num_links: self.num_links,
    }
  }
}

/// 重建完成后的量化调度（对齐 C# RecreateIndex 的 out requestQuantization 通道注入）。
fn request_quantization_if_needed<S: StoreCallbacks>(
  channel: &QuantizationChannel,
  manager: &VectorManager<S>,
  key: &[u8],
  context: u64,
) {
  if manager.service.needs_quantization(context) {
    let _ = channel.try_publish(QuantizationState::new(
      key.to_vec(),
      QuantizationStep::BuildQuantizationTable,
      0,
    ));
  }
}

use wvector::store::StoreCallbacks;

impl<S: StoreCallbacks> VectorManager<S> {
  /// libs/server/Resp/Vector/VectorManager.Locking.cs:NeedsRecreate
  ///
  /// 索引记录尺寸非法或未初始化（指针为空）时需要重建
  /// （先前 VectorManager 实例创建的索引指针已失效）。
  #[inline]
  pub fn needs_recreate(&self, index_config: &[u8]) -> bool {
    Index::from_bytes(index_config).is_none_or(|index| index.index_ptr == 0)
  }

  /// libs/server/Resp/Vector/VectorManager.Locking.cs:ReadVectorIndex
  ///
  /// 只读取向量集合索引（不创建）；需重建时自动重建。
  /// 命中时以共享锁守卫返回（guard 存续期间索引不可被删除）。
  pub fn read_vector_index<'a>(
    &'a self,
    key: &[u8],
  ) -> (Option<Index>, Option<VectorSetSharedGuard<'a>>) {
    match self.read_vector_index_core(key, false) {
      ReadIndexOutcome::Hit(index, guard) => (Some(index), Some(guard)),
      ReadIndexOutcome::NotFound | ReadIndexOutcome::WouldBlock => (None, None),
    }
  }

  /// libs/server/Resp/Vector/VectorManager.Locking.cs:ReadVectorIndexCore
  ///
  /// 锁升降级读协议：共享读 →（需重建）→ 独占重建 → 降级共享重读。
  /// `non_blocking == true` 时任一锁竞争都以 [`ReadIndexOutcome::WouldBlock`]
  /// 让步返回（调用方让出线程后异步重试，对齐 C# 线程池协作语义）。
  pub fn read_vector_index_core<'a>(
    &'a self,
    key: &[u8],
    non_blocking: bool,
  ) -> ReadIndexOutcome<'a> {
    loop {
      // 阶段 1：共享读
      let shared = if non_blocking {
        let Some(guard) = self.vector_set_locks.try_acquire_shared(key) else {
          return ReadIndexOutcome::WouldBlock;
        };
        guard
      } else {
        self.vector_set_locks.acquire_shared(key)
      };

      let Some(bytes) = self.read_stored_index(key) else {
        // 读取未命中（C# readRes != OK）：无锁返回
        return ReadIndexOutcome::NotFound;
      };

      // 单次解析：若已初始化直接命中返回，避免二次反序列化开销
      if let Some(index) = Index::from_bytes(&bytes)
        && index.index_ptr != 0
      {
        return ReadIndexOutcome::Hit(index, shared);
      }
      drop(shared);

      // 需重建，但上一次丢弃请求尚未处理时先自旋等待
      // （同一逻辑集合存在两个活跃索引会严重破坏插入）
      if self.drop_requested(key) {
        if non_blocking {
          // 让出线程而非自旋
          return ReadIndexOutcome::WouldBlock;
        }
        self.wait_for_disk_ann_index_drop(key);
        continue;
      }

      // 阶段 2：竞争独占（C# 经 TryPromoteSharedLock 原子升级；
      // parking_lot 无升级原语，以释放后竞争 + 独占下复核对齐）。
      // 守卫存活至本轮迭代结束（重建写回全程持锁）。
      let _exclusive = if non_blocking {
        let Some(guard) = self.vector_set_locks.try_acquire_exclusive(key) else {
          return ReadIndexOutcome::WouldBlock;
        };
        guard
      } else {
        self.vector_set_locks.acquire_exclusive(key)
      };

      let Some(bytes) = self.read_stored_index(key) else {
        return ReadIndexOutcome::NotFound;
      };

      let index = match Index::from_bytes(&bytes) {
        Some(index) if index.index_ptr != 0 => {
          // 他人已完成重建：降级共享重读
          continue;
        }
        Some(index) => index,
        None => Index::default(),
      };

      // 阶段 3：独占下重建原生索引并写回记录
      // （对齐 C# arg1 = RecreateIndexArg 的 RMW 写回路径）
      self.recreate_index_locked(key, &index);

      // 降级共享重读，避免持独占锁执行搜索
      continue;
    }
  }

  /// libs/server/Resp/Vector/VectorManager.Locking.cs:ReadOrCreateVectorIndex
  ///
  /// 读取向量集合索引；缺失或需重建时（在 `create` 提供建造参数的前提下）
  /// 创建/重建。`create == None` 对齐 InitialUpdater: NO 的 arg 语义：
  /// 缺失即报错。命中/建成后以共享锁守卫返回（C# 重建后同样降级共享，
  /// 避免持独占锁执行插入）。
  pub fn read_or_create_vector_index<'a>(
    &'a self,
    key: &[u8],
    create: Option<&CreateIndexParams>,
  ) -> Result<(Index, VectorSetSharedGuard<'a>), VectorManagerResult> {
    let mut demand_exclusive = false;

    loop {
      // 上轮共享升级失败：本轮直接以独占进入（对齐 C# takeExclusiveLock）
      if demand_exclusive {
        let exclusive = self.vector_set_locks.acquire_exclusive(key);
        let index = self.create_or_recreate_under_exclusive(key, create)?;
        drop(exclusive);
        return Ok((index, self.vector_set_locks.acquire_shared(key)));
      }

      // 阶段 1：共享读
      let shared = self.vector_set_locks.acquire_shared(key);
      if let Some(bytes) = self.read_stored_index(key)
        && let Some(index) = Index::from_bytes(&bytes)
        && index.index_ptr != 0
      {
        return Ok((index, shared));
      }
      drop(shared);

      // 缺失或需重建 → 尝试非阻塞升级独占；失败则本轮以独占重入
      if let Some(exclusive) = self.vector_set_locks.try_acquire_exclusive(key) {
        let index = self.create_or_recreate_under_exclusive(key, create)?;
        drop(exclusive);
        return Ok((index, self.vector_set_locks.acquire_shared(key)));
      }
      demand_exclusive = true;
    }
  }

  /// 独占锁内：命中且无需重建 → 原样返回；需重建 → 重建；缺失 → 按参数创建或报错。
  fn create_or_recreate_under_exclusive(
    &self,
    key: &[u8],
    create: Option<&CreateIndexParams>,
  ) -> Result<Index, VectorManagerResult> {
    match self.read_stored_index(key) {
      Some(bytes) => match Index::from_bytes(&bytes) {
        Some(index) if index.index_ptr != 0 => Ok(index),
        Some(index) => Ok(self.recreate_index_locked(key, &index)),
        None => match create {
          None => Err(VectorManagerResult::Invalid),
          Some(params) => self.create_index_locked(key, params),
        },
      },
      None => match create {
        // 缺失且不允许建桩（InitialUpdater: NO）
        None => Err(VectorManagerResult::Invalid),
        Some(params) => self.create_index_locked(key, params),
      },
    }
  }

  /// 独占锁内执行原生索引重建并写回记录（对齐 RecreateIndexArg 路径）。
  fn recreate_index_locked(&self, key: &[u8], index: &Index) -> Index {
    let _ = self
      .service
      .create_index(index.context, index.index_config(), self.callbacks.clone());
    let mut rebuilt = *index;
    rebuilt.index_ptr = 1;
    self.write_stored_index(key, &rebuilt.to_bytes());
    request_quantization_if_needed(&self.quantization_channel, self, key, rebuilt.context);
    rebuilt
  }

  /// 独占锁内创建新索引（对齐 CreateIndexArg 路径：分配上下文 + 全几何落记录）。
  fn create_index_locked(
    &self,
    key: &[u8],
    params: &CreateIndexParams,
  ) -> Result<Index, VectorManagerResult> {
    // 上下文 0 非法（保留），分配失败须报错而非落 0 桩记录
    let context = self
      .next_vector_set_context(params.hash_slot)
      .ok_or(VectorManagerResult::Invalid)?;
    let _ = self
      .service
      .create_index(context, params.index_config(), self.callbacks.clone());
    let index = Index {
      context,
      index_ptr: 1,
      dimensions: params.dims,
      reduce_dims: params.reduce_dims,
      num_links: params.num_links,
      build_exploration_factor: params.build_exploration_factor,
      quant_type: params.quant,
      distance_metric: params.distance_metric,
      flags: VectorSetFlags::NONE,
    };
    self.write_stored_index(key, &index.to_bytes());
    request_quantization_if_needed(&self.quantization_channel, self, key, context);
    Ok(index)
  }

  /// libs/server/Resp/Vector/VectorManager.Locking.cs:AcquireExclusiveLocks
  ///
  /// 为指定键获取独占锁（VREM / VSETATTR 等写路径的前置）。
  #[inline]
  pub fn acquire_exclusive_locks<'a>(&'a self, key: &[u8]) -> VectorSetLockGuard<'a> {
    self.vector_set_locks.acquire_exclusive(key)
  }

  /// libs/server/Resp/Vector/VectorManager.Locking.cs:ReadForDeleteVectorIndex
  ///
  /// 删除路径：独占锁下读取索引（阻止读取期间的并发重建/使用）。
  pub fn read_for_delete_vector_index<'a>(
    &'a self,
    key: &[u8],
  ) -> Option<(Index, VectorSetLockGuard<'a>)> {
    let exclusive = self.acquire_exclusive_locks(key);
    let stored = self.read_stored_index(key)?;
    let index = Index::from_bytes(&stored)?;
    Some((index, exclusive))
  }

  /// 存储层读取承接：键 → 索引记录字节（wkv 集成前为域内登记表）。
  pub fn read_stored_index(&self, key: &[u8]) -> Option<[u8; INDEX_SIZE_BYTES]> {
    self.key_index_registry.lock().get(key).copied()
  }

  /// 存储层写入承接：键 → 索引记录字节。
  pub fn write_stored_index(&self, key: &[u8], bytes: &[u8; INDEX_SIZE_BYTES]) {
    self.key_index_registry.lock().insert(key.to_vec(), *bytes);
  }

  /// 存储层删除承接：记录删除后自登记表移除键。
  pub fn remove_stored_index(&self, key: &[u8]) {
    self.key_index_registry.lock().remove(key);
  }

  /// VADD 追加日志参数（replay 语义；对齐 VADDAppendLogArg）。
  pub fn vadd_append_log_arg() -> i64 {
    super::vector_manager::VADD_APPEND_LOG_ARG
  }

  /// SuppressCleanup 标志测试辅助。
  pub fn index_has_suppress_cleanup(index: &Index) -> bool {
    index.flags.contains(VectorSetFlags::SUPPRESS_CLEANUP)
  }
}
