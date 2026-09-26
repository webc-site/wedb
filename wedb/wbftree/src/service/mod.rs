//! BfTree 高层服务包装器 (1:1 对标 Garnet BfTreeService.cs)
//!
//! 封装 Microsoft Research 的 bf-tree 核心实例，提供零堆分配切片 API、流式扫描回调与快照恢复。
//!
//! # 模块拆分
//! - [`bulk`]：排序批量装载内核 (树内写入唯一真值路径)
//! - [`ops`]：insert / delete / read / scan 点操作
//! - [`snapshot`]：CPR 快照与恢复 (cpr_snapshot / recover_from_cpr_snapshot)
//!
//! 自研依据: doc/zh/collection.md 升阶服务（页级分层持久化）

mod bulk;
mod ops;
mod snapshot;

use std::{fs, io::Read as _, path::Path, sync::Arc};

use arc_swap::ArcSwapOption;
use bf_tree::{BfTree, ConfigError};
#[cfg(debug_assertions)]
pub use ops::SCAN_FAIL_INJECT;

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
/// 1:1 对标 C# RestoreTree 的「新建树 + 注册表登记」，不存在原地换树)。
///
/// 底层引擎单句柄单视图 (对标 C# `private nint _tree` 单一字段)：
/// 热路径经 arc-swap 无锁借用，冷路径字段为构造定型的普通量。
///
/// 外部构造对标 C# BfTreeService 构造由 [`crate::RangeIndexManager`] 托管
/// (create_bftree / recover_from_cpr_snapshot)，本层不暴露独立构造面。
///
/// 稳态下零写入原子计数开销 (1:1 对标 Garnet，点写由外部条带锁或 Epoch 保护；
/// 写静稳屏障由 [`crate::RangeIndexManager`] 的 checkpoint 体系承担，本层无屏障)。
pub struct BfTreeService {
  /// 底层引擎单句柄：构造时一次定型，dispose 经 swap(None) 原子摘除；
  /// 引擎随最后一个 Arc 归零而析构 (对标 C# LightEpoch 延迟释放语义)
  pub(crate) tree: ArcSwapOption<BfTree>,
  pub(crate) storage_backend: StorageBackendType,
  pub(crate) file_path: Option<String>,
  pub(crate) max_record_size: usize,
  pub(crate) enable_snapshots: bool,
  /// 常驻页环容量（字节）：构造时一次定型（新建取配置值，恢复取快照配置头
  /// 回读值），供 RangeIndexManager 预算记账与 MEMORY USAGE 披露消费。
  /// bf-tree 0.5.6 的 CircularBuffer::new 一次性整块分配该容量且树存活期常驻，
  /// 故该值即树的真实常驻页内存口径（非「冷热换入换出」动态量）
  pub(crate) cache_bytes: usize,
}

impl BfTreeService {
  /// 内部构建辅助函数，直接传入已知后端和路径
  pub(crate) fn new_with_backend(
    config: impl Into<bf_tree::Config>,
    storage_backend: StorageBackendType,
    file_path: Option<String>,
    enable_snapshots: bool,
  ) -> Result<Self> {
    if storage_backend == StorageBackendType::Disk && file_path.is_none() {
      return Err(Error::InvalidArgument(
        "磁盘后端必须指定数据文件路径 (file_path)".into(),
      ));
    }

    let inner_cfg: bf_tree::Config = config.into();
    let max_record_size = inner_cfg.get_cb_max_record_size().max(MIN_MAX_RECORD_SIZE);
    let cache_bytes = inner_cfg.get_cb_size_byte();

    let tree = match BfTree::with_config(inner_cfg, None) {
      Ok(t) => Arc::new(t),
      Err(e) => return Err(Error::InvalidConfig(config_error_to_string(e))),
    };

    Ok(Self {
      tree: ArcSwapOption::new(Some(tree)),
      storage_backend,
      file_path,
      max_record_size,
      enable_snapshots,
      cache_bytes,
    })
  }

  /// 在无锁保护下安全借用底层 BfTree 执行 `f` (点读/写路径统一入口)
  ///
  /// 并发正确性：arc-swap `load` 为 Acquire 读，[`Self::dispose`] 的
  /// `swap(None)` 为 Release 发布；Guard 存续期间底层引用保证存活——
  /// 与 dispose 并发时借用侧绝无悬空窗口 (dispose 摘除后 Guard 仍延迟
  /// 保活至闭包返回，对标 C# LightEpoch 延迟释放)。零 RwLock、零引用
  /// 计数开销，1:1 对标 Garnet 纯指针直读。
  #[inline]
  pub(crate) fn with_tree<R>(&self, f: impl FnOnce(&BfTree) -> R) -> Option<R> {
    // Guard 须绑定命名局部量延长存活，借出引用不得早于闭包执行消亡
    let guard = self.tree.load();
    let tree = guard.as_deref()?;
    Some(f(tree))
  }

  /// 获取底层 BfTree 的 Arc 实例 (仅在扫描迭代等需长生命周期保护时调用)
  #[inline]
  pub(crate) fn tree_arc(&self) -> Result<Arc<BfTree>> {
    self.tree.load_full().ok_or(Error::Disposed)
  }

  /// 获取裸指针标识 (用于 RangeIndexStub.tree_handle，1:1 对标 Garnet BfTreeService.NativePtr)
  #[inline]
  pub fn native_ptr(&self) -> u64 {
    self
      .tree
      .load()
      .as_ref()
      .map_or(0, |t| Arc::as_ptr(t) as usize as u64)
  }

  /// 获取数据文件路径 (构造时一次定型，零锁零分配借用，对标 C# FilePath)
  #[inline]
  pub fn file_path(&self) -> Option<&str> {
    self.file_path.as_deref()
  }

  /// 获取存储后端类型
  #[inline]
  pub fn storage_backend(&self) -> StorageBackendType {
    self.storage_backend
  }

  /// 获取最大记录大小 (读/扫描缓冲区 sizing 依据)
  #[inline]
  pub(crate) fn max_record_size(&self) -> usize {
    self.max_record_size
  }

  /// 获取常驻页环容量（字节，构造时一次定型）
  ///
  /// 预算记账（[`crate::RangeIndexManager`] 的 try_reserve / release）与
  /// MEMORY USAGE 升阶臂披露的唯一容量事实源；C# 无对应披露
  /// （libs/server/RangeIndexManager.cs 的 CacheSize 仅在存根静态字段，
  /// 无运行态披露），为 rust 分层防 OOM 自定义面的自带义务
  #[inline]
  pub fn cache_bytes(&self) -> usize {
    self.cache_bytes
  }

  /// 检查是否已释放 (1:1 对标 is_disposed；引擎摘除即释放，唯一守卫)
  #[inline]
  pub fn is_disposed(&self) -> bool {
    self.tree.load().is_none()
  }

  /// 释放实例并回收资源 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:Dispose)
  ///
  /// 幂等：单句柄 `swap(None)` 原子摘除，返回 `Some` 即首次释放
  /// (对标 C# `Interlocked.Exchange(ref _disposed, 1)` + `_tree = 0`；
  /// 摘除后 `native_ptr` 读 0，`is_disposed` 唯一守卫判真)。
  ///
  /// 并发正确性：swap 为 Release 发布，已借出的点读 Guard / 扫描 `Arc`
  /// 保活底层引擎至用毕才随最后一个引用归零析构 (对标 C# LightEpoch
  /// 延迟释放语义)，无需显式排空；「排空后才删数据文件」的删除路径语义
  /// 由 [`crate::RangeIndexManager::dispose_tree_under_lock`] 承担。
  pub fn dispose(&self) {
    drop(self.tree.swap(None));
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
    BfTreeService::new_with_backend(config, StorageBackendType::Memory, None, false).unwrap()
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

    service.dispose();
    assert!(service.is_disposed());
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
