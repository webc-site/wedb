//! 向量集合键锁与索引读取（对标 libs/server/Resp/Vector/VectorManager.Locking.cs）
//!
//! C# 以 ReadOptimizedLock（键哈希 → 读优化锁）实现共享读 / 独占写的
//! 升降级协议：共享读命中"需重建"的索引时升级独占，重建完成降级共享，
//! 并把持有的锁以 VectorSetLock（IDisposable）交还调用方。
//! Rust 侧以 [`VectorSetLocks`]（键哈希 → parking_lot RwLock 注册表）承接；
//! parking_lot 无原子升降级，升级以"释放共享 → 竞争独占 → 复核"的
//! 让步重试形态对齐 C# TryPromote 失败后的再入路径。
//! 索引记录的存储读取经 [`super::vector_manager::VectorManager`] 的
//! 键值登记表承接（wkv 集成前为域内表）。

use std::sync::Arc;

use papaya::HashMap as ConcurrentMap;
use parking_lot::{
  RwLock,
  lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard},
};

use super::{
  vector_manager::{INDEX_SIZE_BYTES, VectorManager, VectorManagerResult},
  vector_manager__index::Index,
  vector_manager__quantization::{QuantizationChannel, QuantizationState, QuantizationStep},
  vector_types::{VectorDistanceMetricType, VectorQuantType, VectorSetFlags},
};

/// 共享读守卫：存续期间索引不可被丢弃或重建（对齐 C# VectorSetLock 共享形态）。
pub type VectorSetSharedGuard = ArcRwLockReadGuard<parking_lot::RawRwLock, ()>;

/// 独占写守卫（对齐 C# VectorSetLock 独占形态；持有 Arc，可跨作用域移交）。
pub type VectorSetLockGuard = ArcRwLockWriteGuard<parking_lot::RawRwLock, ()>;

/// 向量集合键的读优化锁注册表（键哈希 → RwLock）。
#[derive(Default)]
pub struct VectorSetLocks {
  locks: ConcurrentMap<u64, Arc<RwLock<()>>>,
}

impl VectorSetLocks {
  /// 定位（或创建）键对应的锁。
  fn lock_for(&self, key: &[u8]) -> Arc<RwLock<()>> {
    let key_hash = key_hash_of(key);
    if let Some(lock) = self.locks.pin().get(&key_hash) {
      return Arc::clone(lock);
    }
    let lock = Arc::new(RwLock::new(()));
    match self.locks.pin().try_insert(key_hash, Arc::clone(&lock)) {
      Ok(_) => lock,
      Err(_) => {
        // 并发插入：以既有锁为准
        self
          .locks
          .pin()
          .get(&key_hash)
          .map(Arc::clone)
          .unwrap_or(lock)
      }
    }
  }

  /// 获取共享锁（读路径；guard 存续期间阻止索引被丢弃）。
  pub fn acquire_shared(&self, key: &[u8]) -> VectorSetSharedGuard {
    self.lock_for(key).read_arc()
  }

  /// 尝试非阻塞共享锁获取；竞争时返回 None。
  pub fn try_acquire_shared(&self, key: &[u8]) -> Option<VectorSetSharedGuard> {
    self.lock_for(key).try_read_arc()
  }

  /// 获取独占锁（VREM / VSETATTR / 重建等写路径）。
  pub fn acquire_exclusive(&self, key: &[u8]) -> VectorSetLockGuard {
    self.lock_for(key).write_arc()
  }

  /// 尝试非阻塞独占锁获取；竞争时返回 None。
  pub fn try_acquire_exclusive(&self, key: &[u8]) -> Option<VectorSetLockGuard> {
    self.lock_for(key).try_write_arc()
  }

  /// 是否被独占持有（测试/调试辅助；try_read 失败即写持有或写等待中）。
  pub fn is_locked_exclusive(&self, key: &[u8]) -> bool {
    self
      .locks
      .pin()
      .get(&key_hash_of(key))
      .is_some_and(|l| l.try_read().is_none())
  }
}

/// `read_vector_index_core` 的三态结果（对齐 C# 的 status + wouldBlock 出参）。
#[derive(Debug)]
pub enum ReadIndexOutcome {
  /// 命中：索引记录 + 持有共享锁的守卫（存续期间索引不可被丢弃）。
  Hit(Index, VectorSetSharedGuard),
  /// 键不存在（C# readRes != OK）。
  NotFound,
  /// 锁竞争：调用方应让出线程后异步重试（仅 non_blocking 形态）。
  WouldBlock,
}

/// 键哈希（gxhash 64 位，对齐 C# GetKeyHash 的条带定位职责）。
fn key_hash_of(key: &[u8]) -> u64 {
  gxhash::gxhash64(key, 0)
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

/// 重建完成后的量化调度（对齐 C# RecreateIndex 的 out requestQuantization 通道注入）。
fn request_quantization_if_needed(
  channel: &QuantizationChannel,
  manager: &VectorManager,
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

impl VectorManager {
  /// libs/server/Resp/Vector/VectorManager.Locking.cs:NeedsRecreate
  ///
  /// 索引记录尺寸非法或未初始化（指针为空）时需要重建
  /// （先前 VectorManager 实例创建的索引指针已失效）。
  pub fn needs_recreate(&self, index_config: &[u8]) -> bool {
    match Index::from_bytes(index_config) {
      Some(index) => index.index_ptr == 0,
      None => true,
    }
  }

  /// libs/server/Resp/Vector/VectorManager.Locking.cs:ReadVectorIndex
  ///
  /// 只读取向量集合索引（不创建）；需重建时自动重建。
  /// 命中时以共享锁守卫返回（guard 存续期间索引不可被删除）。
  pub fn read_vector_index(&self, key: &[u8]) -> (Option<Index>, Option<VectorSetSharedGuard>) {
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
  pub fn read_vector_index_core(&self, key: &[u8], non_blocking: bool) -> ReadIndexOutcome {
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
      if !self.needs_recreate(&bytes) {
        // needs_recreate 已校验尺寸，此处解析必然成功
        let index = Index::from_bytes(&bytes).unwrap_or_default();
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
      if !self.needs_recreate(&bytes) {
        // 他人已完成重建：降级共享重读
        continue;
      }

      // 阶段 3：独占下重建原生索引并写回记录
      // （对齐 C# arg1 = RecreateIndexArg 的 RMW 写回路径）
      let index = Index::from_bytes(&bytes).unwrap_or_default();
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
  pub fn read_or_create_vector_index(
    &self,
    key: &[u8],
    create: Option<&CreateIndexParams>,
  ) -> Result<(Index, VectorSetSharedGuard), VectorManagerResult> {
    let mut demand_exclusive = false;

    loop {
      // 上轮共享升级失败：本轮直接以独占进入（对齐 C# takeExclusiveLock）
      if demand_exclusive {
        let exclusive = self.vector_set_locks.acquire_exclusive(key);
        return match self.create_or_recreate_under_exclusive(key, create) {
          // 建成/重建后降级共享交付，避免持独占锁执行插入
          Ok(index) => {
            drop(exclusive);
            Ok((index, self.vector_set_locks.acquire_shared(key)))
          }
          Err(e) => Err(e),
        };
      }

      // 阶段 1：共享读
      let shared = self.vector_set_locks.acquire_shared(key);
      if let Some(bytes) = self.read_stored_index(key)
        && !self.needs_recreate(&bytes)
      {
        let index = Index::from_bytes(&bytes).unwrap_or_default();
        return Ok((index, shared));
      }
      drop(shared);

      // 缺失或需重建 → 尝试非阻塞升级独占；失败则本轮以独占重入
      if let Some(exclusive) = self.vector_set_locks.try_acquire_exclusive(key) {
        return match self.create_or_recreate_under_exclusive(key, create) {
          Ok(index) => {
            drop(exclusive);
            Ok((index, self.vector_set_locks.acquire_shared(key)))
          }
          Err(e) => Err(e),
        };
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
      Some(bytes) => {
        let index = Index::from_bytes(&bytes).unwrap_or_default();
        if !self.needs_recreate(&bytes) {
          Ok(index)
        } else {
          // CreateIndexArg 的 RecreateIndex 分支：以记录既有几何重建
          Ok(self.recreate_index_locked(key, &index))
        }
      }
      None => match create {
        // 缺失且不允许建桩（InitialUpdater: NO）
        None => Err(VectorManagerResult::Invalid),
        Some(params) => Ok(self.create_index_locked(key, params)),
      },
    }
  }

  /// 独占锁内执行原生索引重建并写回记录（对齐 RecreateIndexArg 路径）。
  fn recreate_index_locked(&self, key: &[u8], index: &Index) -> Index {
    self.service.create_index(
      index.context,
      index.dimensions,
      index.reduce_dims,
      index.quant_type,
      index.build_exploration_factor,
      index.num_links,
      index.distance_metric,
    );
    let mut rebuilt = *index;
    rebuilt.index_ptr = 1;
    self.write_stored_index(key, &rebuilt.to_bytes());
    request_quantization_if_needed(&self.quantization_channel, self, key, rebuilt.context);
    rebuilt
  }

  /// 独占锁内创建新索引（对齐 CreateIndexArg 路径：分配上下文 + 全几何落记录）。
  fn create_index_locked(&self, key: &[u8], params: &CreateIndexParams) -> Index {
    let context = self
      .next_vector_set_context(params.hash_slot)
      .unwrap_or_default();
    self.service.create_index(
      context,
      params.dims,
      params.reduce_dims,
      params.quant,
      params.build_exploration_factor,
      params.num_links,
      params.distance_metric,
    );
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
    index
  }

  /// libs/server/Resp/Vector/VectorManager.Locking.cs:AcquireExclusiveLocks
  ///
  /// 为指定键获取独占锁（VREM / VSETATTR 等写路径的前置）。
  pub fn acquire_exclusive_locks(&self, key: &[u8]) -> VectorSetLockGuard {
    self.vector_set_locks.acquire_exclusive(key)
  }

  /// libs/server/Resp/Vector/VectorManager.Locking.cs:ReadForDeleteVectorIndex
  ///
  /// 删除路径：独占锁下读取索引（阻止读取期间的并发重建/使用）。
  pub fn read_for_delete_vector_index(&self, key: &[u8]) -> Option<(Index, VectorSetLockGuard)> {
    let exclusive = self.acquire_exclusive_locks(key);
    let stored = self.read_stored_index(key)?;
    let index = Index::from_bytes(&stored)?;
    Some((index, exclusive))
  }

  /// 存储层读取承接：键 → 索引记录字节（wkv 集成前为域内登记表）。
  pub(crate) fn read_stored_index(&self, key: &[u8]) -> Option<[u8; INDEX_SIZE_BYTES]> {
    self.key_index_registry.lock().get(key).copied()
  }

  /// 存储层写入承接：键 → 索引记录字节。
  pub(crate) fn write_stored_index(&self, key: &[u8], bytes: &[u8; INDEX_SIZE_BYTES]) {
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

#[cfg(test)]
mod tests {
  use super::{super::vector_manager::VectorManagerOptions, *};

  fn manager() -> VectorManager {
    VectorManager::new(VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    })
  }

  fn q8_index(context: u64) -> Index {
    Index {
      context,
      index_ptr: 1,
      dimensions: 4,
      reduce_dims: 0,
      num_links: 8,
      build_exploration_factor: 64,
      quant_type: VectorQuantType::NoQuant,
      distance_metric: VectorDistanceMetricType::L2,
      flags: VectorSetFlags::NONE,
    }
  }

  fn create_params() -> CreateIndexParams {
    CreateIndexParams {
      hash_slot: 5,
      dims: 4,
      reduce_dims: 0,
      quant: VectorQuantType::NoQuant,
      build_exploration_factor: 64,
      num_links: 8,
      distance_metric: VectorDistanceMetricType::L2,
    }
  }

  #[test]
  fn needs_recreate_semantics() {
    let manager = manager();

    // 未初始化（ptr=0）→ 需要重建
    let stub = Index {
      context: 8,
      index_ptr: 0,
      ..Index::default()
    };
    assert!(manager.needs_recreate(&stub.to_bytes()));

    // 已初始化 → 无需重建
    assert!(!manager.needs_recreate(&q8_index(8).to_bytes()));

    // 尺寸非法 → 需要重建
    assert!(manager.needs_recreate(&[0u8; 10]));
  }

  #[test]
  fn read_create_and_recreate_flow() {
    let manager = manager();
    let key = b"flow".to_vec();

    // 缺失且不允许创建 → Invalid（InitialUpdater: NO）
    assert_eq!(
      manager.read_or_create_vector_index(&key, None).unwrap_err(),
      VectorManagerResult::Invalid
    );

    // 允许创建 → 全几何记录 + ptr 已置位 + 共享守卫
    let (created, guard) = manager
      .read_or_create_vector_index(&key, Some(&create_params()))
      .unwrap();
    assert_eq!(created.index_ptr, 1);
    assert_ne!(created.context, 0);
    assert_eq!(created.dimensions, 4);
    drop(guard);

    // 只读路径命中：无需重建，直接共享返回
    let (read, guard) = manager.read_vector_index(&key);
    assert!(read.as_ref().is_some_and(|i| i.index_ptr == 1));
    assert_eq!(read.as_ref().unwrap().context, created.context);
    drop(guard);

    // 手动落一个 ptr=0 的桩 → 只读路径触发重建（升级独占 → 重建 → 降级共享）
    let mut stub = created;
    stub.index_ptr = 0;
    manager.write_stored_index(&key, &stub.to_bytes());
    let (read, guard) = manager.read_vector_index(&key);
    assert!(read.as_ref().is_some_and(|i| i.index_ptr == 1));
    drop(guard);
    assert!(!manager.needs_recreate(&manager.read_stored_index(&key).unwrap()));

    // ReadOrCreate 遇 ptr=0 桩同样走重建
    let mut stub2 = created;
    stub2.index_ptr = 0;
    manager.write_stored_index(&key, &stub2.to_bytes());
    let (read, guard) = manager
      .read_or_create_vector_index(&key, Some(&create_params()))
      .unwrap();
    assert_eq!(read.index_ptr, 1);
    drop(guard);
  }

  #[test]
  fn recreate_preserves_geometry_from_record() {
    let manager = manager();
    let key = b"geom".to_vec();

    // 以自定义几何建桩（ptr=0），重建后几何应保持
    let mut stub = q8_index(512);
    stub.index_ptr = 0;
    manager.write_stored_index(&key, &stub.to_bytes());
    let (read, _guard) = manager.read_vector_index(&key);
    let read = read.unwrap();
    assert_eq!(
      (read.dimensions, read.num_links, read.quant_type),
      (4, 8, VectorQuantType::NoQuant)
    );
    assert_eq!(read.context, 512);
  }

  #[test]
  fn locks_serialize_writers() {
    let manager = manager();
    let key = b"locked".to_vec();

    let (_index, guard) = manager
      .read_or_create_vector_index(&key, Some(&create_params()))
      .unwrap();
    // 交付守卫为共享形态：他人可再共享读，但不可独占
    assert!(manager.vector_set_locks.try_acquire_shared(&key).is_some());
    assert!(
      manager
        .vector_set_locks
        .try_acquire_exclusive(&key)
        .is_none()
    );
    assert!(!manager.vector_set_locks.is_locked_exclusive(&key));
    drop(guard);
    assert!(
      manager
        .vector_set_locks
        .try_acquire_exclusive(&key)
        .is_some()
    );

    // 共享期间：非阻塞共享可加，独占不可
    let read_guard = manager.vector_set_locks.acquire_shared(&key);
    assert!(manager.vector_set_locks.try_acquire_shared(&key).is_some());
    assert!(
      manager
        .vector_set_locks
        .try_acquire_exclusive(&key)
        .is_none()
    );
    drop(read_guard);
    assert!(
      manager
        .vector_set_locks
        .try_acquire_exclusive(&key)
        .is_some()
    );
  }

  #[test]
  fn delete_path_reads_under_lock() {
    let manager = manager();
    let key = b"del".to_vec();
    assert!(manager.read_for_delete_vector_index(&key).is_none());

    let (idx, guard) = manager
      .read_or_create_vector_index(&key, Some(&create_params()))
      .unwrap();
    // 共享守卫存续时删除路径的独占获取必须等待（先释放再进入删除路径）
    assert!(
      manager
        .vector_set_locks
        .try_acquire_exclusive(&key)
        .is_none()
    );
    drop(guard);

    let (found, del_guard) = manager.read_for_delete_vector_index(&key).unwrap();
    assert_eq!(found.context, idx.context);
    // 删除守卫为独占形态
    assert!(manager.vector_set_locks.is_locked_exclusive(&key));
    drop(del_guard);
    assert!(!manager.vector_set_locks.is_locked_exclusive(&key));

    // VADD 追加日志参数为 long.MinValue
    assert_eq!(VectorManager::vadd_append_log_arg(), i64::MIN);
    // SuppressCleanup 判定
    let mut flagged = q8_index(8);
    flagged.flags = VectorSetFlags::SUPPRESS_CLEANUP;
    assert!(VectorManager::index_has_suppress_cleanup(&flagged));
  }
}
