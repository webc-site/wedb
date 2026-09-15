//! BfTree 高层服务包装器 (1:1 对标 Garnet BfTreeService.cs)
//!
//! 封装 Microsoft Research 的 bf-tree 核心实例，提供零堆分配切片 API、流式扫描回调与快照恢复。
//!
//! # 模块拆分
//! - [`ops`]：insert / delete / read / scan 点操作
//! - [`snapshot`]：CPR 快照与恢复 (cpr_snapshot / recover_from_cpr_snapshot)
//! - [`lifecycle`]：生命周期释放 (dispose)

mod lifecycle;
mod ops;
mod snapshot;

use std::{
  fs,
  io::Read as _,
  path::Path,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicUsize, Ordering},
  },
};

use bf_tree::{BfTree, ConfigError};
use parking_lot::RwLock;

use crate::{
  error::{Error, Result},
  manager::CPR_MAGIC,
  types::StorageBackendType,
};

/// 栈上读写缓冲大小 (8192 字节，1:1 对标 C# Garnet stackalloc byte[8192]；
/// 读/扫描缓冲选路统一阈值，见 ops::with_read_buffer)
pub(crate) const STACK_BUF_SIZE: usize = 8192;

/// 最大记录大小下限 (保证读/扫描缓冲 ≥ 单值上限，4KB 对标 C# Read 栈缓冲基线)
pub(crate) const MIN_MAX_RECORD_SIZE: usize = 4096;

/// 检查文件是否为 bf-tree CPR 快照 (首部魔数校验；缺失/过小/读取失败一律 false)
///
/// 调引擎恢复前先行校验，把「损坏快照」变成结构化 [`Error::Recovery`] 而非依赖
/// catch_unwind (release 构建 panic = "abort" 下 unwind 拦截无效)。
/// 供宿主 (wkv) 在打开持久工作文件前判定「CPR 快照镜像 vs 孤儿基文件」复用。
#[inline]
pub fn file_has_cpr_magic(path: &Path) -> bool {
  let Ok(mut file) = fs::File::open(path) else {
    return false;
  };
  let mut magic = [0u8; CPR_MAGIC.len()];
  file.read_exact(&mut magic).is_ok() && magic == *CPR_MAGIC
}

/// 将 bf_tree::ConfigError 映射为可读字符串 (替代 Debug 格式化；format! 单次分配)
#[inline]
fn config_error_to_string(e: ConfigError) -> String {
  match e {
    ConfigError::MinimumRecordSize(s) => format!("MinimumRecordSize: {s}"),
    ConfigError::MaximumRecordSize(s) => format!("MaximumRecordSize: {s}"),
    ConfigError::LeafPageSize(s) => format!("LeafPageSize: {s}"),
    ConfigError::MaxKeyLen(s) => format!("MaxKeyLen: {s}"),
    ConfigError::CircularBufferSize(s) => format!("CircularBufferSize: {s}"),
    ConfigError::SnapshotFileInvalid(s) => format!("SnapshotFileInvalid: {s}"),
    ConfigError::SnapshotDisabled => "SnapshotDisabled".to_string(),
  }
}

/// 高层 BfTree 服务实例 (1:1 对标 Garnet BfTreeService)
///
/// 实例的后端/路径/记录上限在构造时一次定型、终身不变 (恢复走
/// [`recover_from_cpr_snapshot`](Self::recover_from_cpr_snapshot) 新建实例，
/// 1:1 对标 C# RestoreTree 的「新建树 + 注册表登记」，不存在原地换树)：
/// 热路径字段走原子量，文件路径走冷路径读写锁。
///
/// 外部构造对标 C# BfTreeService 构造由 [`crate::RangeIndexManager`] 托管
/// (create_bftree / recover_from_cpr_snapshot)，本层不暴露独立构造面。
///
/// 稳态下零写入原子计数开销 (1:1 对标 Garnet，点写由外部条带锁或 Epoch 保护；
/// 写静稳屏障由 [`crate::RangeIndexManager`] 的 checkpoint 体系承担，本层无屏障)。
pub struct BfTreeService {
  pub(crate) raw_tree: AtomicPtr<BfTree>,
  pub(crate) arc_tree: RwLock<Option<Arc<BfTree>>>,
  pub(crate) storage_backend: AtomicU8,
  pub(crate) file_path: RwLock<Option<String>>,
  pub(crate) max_record_size: AtomicUsize,
  pub(crate) disposed: AtomicBool,
}

unsafe impl Send for BfTreeService {}
unsafe impl Sync for BfTreeService {}

impl BfTreeService {
  /// 内部构建辅助函数，直接传入已知后端和路径
  pub(crate) fn new_with_backend(
    config: impl Into<bf_tree::Config>,
    storage_backend: StorageBackendType,
    file_path: Option<String>,
  ) -> Result<Self> {
    if storage_backend == StorageBackendType::Disk && file_path.is_none() {
      return Err(Error::InvalidArgument(
        "磁盘后端必须指定数据文件路径 (file_path)".into(),
      ));
    }

    let inner_cfg: bf_tree::Config = config.into();
    let max_record_size = inner_cfg.get_cb_max_record_size().max(MIN_MAX_RECORD_SIZE);

    let tree = match BfTree::with_config(inner_cfg, None) {
      Ok(t) => Arc::new(t),
      Err(e) => return Err(Error::InvalidConfig(config_error_to_string(e))),
    };

    let raw_ptr = Arc::as_ptr(&tree) as *mut BfTree;

    Ok(Self {
      raw_tree: AtomicPtr::new(raw_ptr),
      arc_tree: RwLock::new(Some(tree)),
      storage_backend: AtomicU8::new(storage_backend as u8),
      file_path: RwLock::new(file_path),
      max_record_size: AtomicUsize::new(max_record_size),
      disposed: AtomicBool::new(false),
    })
  }

  /// 检查底层引擎指针是否非空
  #[inline]
  pub fn has_tree(&self) -> bool {
    !self.raw_tree.load(Ordering::Acquire).is_null()
  }

  /// 获取底层 BfTree 的 Arc 实例 (仅在扫描迭代等需长生命周期保护时调用)
  #[inline]
  pub(crate) fn tree_arc(&self) -> Result<Arc<BfTree>> {
    self.check_disposed()?;
    self
      .arc_tree
      .read()
      .as_ref()
      .cloned()
      .ok_or(Error::Disposed)
  }

  /// 在无锁保护下安全借用底层 BfTree (纯指针直调，零 RwLock，零 Arc 克隆，1:1 对标 Garnet 纯指针直读)
  #[inline(always)]
  pub(crate) fn tree_ref(&self) -> Result<&BfTree> {
    let ptr = self.raw_tree.load(Ordering::Acquire);
    if ptr.is_null() {
      return Err(Error::Disposed);
    }
    Ok(unsafe { &*ptr })
  }

  /// 获取裸指针标识 (用于 RangeIndexStub.tree_handle，1:1 对标 Garnet BfTreeService.NativePtr)
  #[inline]
  pub fn native_ptr(&self) -> u64 {
    self.raw_tree.load(Ordering::Relaxed) as usize as u64
  }

  /// 获取数据文件路径
  #[inline]
  pub fn file_path(&self) -> Option<String> {
    self.file_path.read().clone()
  }

  /// 获取存储后端类型
  #[inline]
  pub fn storage_backend(&self) -> StorageBackendType {
    StorageBackendType::from_u8(self.storage_backend.load(Ordering::Acquire))
  }

  /// 获取最大记录大小 (读/扫描缓冲区 sizing 依据)
  #[inline]
  pub(crate) fn max_record_size(&self) -> usize {
    self.max_record_size.load(Ordering::Relaxed)
  }

  /// 检查是否已释放 (1:1 对标 is_disposed)
  #[inline]
  pub fn is_disposed(&self) -> bool {
    self.disposed.load(Ordering::Acquire)
  }

  /// 检查是否已释放
  #[inline]
  pub(crate) fn check_disposed(&self) -> Result<()> {
    if self.is_disposed() {
      Err(Error::Disposed)
    } else {
      Ok(())
    }
  }
}

impl Drop for BfTreeService {
  fn drop(&mut self) {
    self.dispose();
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use bf_tree::Config;

  use super::*;
  use crate::types::{BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult};

  fn mem_service() -> BfTreeService {
    let mut config = Config::default();
    config.cache_only(true);
    BfTreeService::new_with_backend(config, StorageBackendType::Memory, None).unwrap()
  }

  /// 空值插入快速拒绝：绝不透传引擎 (底层叶子插入 debug_assert 非空值，
  /// dev/test 构建下空值会 panic)，返回 InvalidKV 结构化结果码
  #[test]
  fn test_insert_empty_value_rejected_without_engine_panic() {
    let service = mem_service();
    // 空 value 无论 key 长短一律 InvalidKV，且不触发引擎断言
    assert_eq!(
      service.insert(b"long_enough_key", b""),
      BfTreeInsertResult::InvalidKV
    );
    assert_eq!(service.insert(b"k", b""), BfTreeInsertResult::InvalidKV);
    // 空值插入不得产生任何残留条目
    let (res, v) = service.read(b"long_enough_key");
    assert_eq!(res, BfTreeReadResult::NotFound);
    assert_eq!(v, None);
  }

  /// 释放后资源完全清空，后续操作返回 InvalidArguments，重复释放幂等
  #[test]
  fn test_dispose_lifecycle() {
    let service = Arc::new(mem_service());
    assert_eq!(
      service.insert(b"key1", b"val1"),
      BfTreeInsertResult::Success
    );
    assert!(!service.is_disposed());
    assert!(service.has_tree());

    service.dispose();
    assert!(service.is_disposed());
    assert!(!service.has_tree());
    assert_eq!(
      service.insert(b"key2", b"val2"),
      BfTreeInsertResult::InvalidArguments
    );
    assert_eq!(
      service.delete(b"key1"),
      BfTreeDeleteResult::InvalidArguments
    );
    // 重复释放幂等
    service.dispose();
    assert!(service.is_disposed());
  }
}
