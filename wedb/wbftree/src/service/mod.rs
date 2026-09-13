//! BfTree 高层服务包装器 (1:1 对标 Garnet BfTreeService.cs)
//!
//! 封装 Microsoft Research 的 bf-tree 核心实例，提供零堆分配切片 API、流式扫描回调与快照恢复。
//!
//! # 模块拆分
//! - [`ops`]：insert / delete / read / scan 点操作
//! - [`snapshot`]：CPR 快照与恢复 (cpr_snapshot / recover_in_place / recover_from_cpr_snapshot)
//! - [`barrier`]：写入屏障、排空在途写者与生命周期释放 (dispose / dispose_quiesced)

mod barrier;
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
pub use ops::SCAN_ALL_START_KEY;
use parking_lot::RwLock;
use wbase::backoff::backoff;

use crate::{
  error::{Error, Result},
  manager::CPR_MAGIC,
  types::{BfTreeConfig, StorageBackendType},
};

/// 栈上单值读取缓冲区大小 (值 ≤ 4096 字节走零堆分配快路径)
const STACK_READ_BUF_SIZE: usize = 4096;

/// 栈上扫描缓冲区大小 (8192 字节，1:1 对标 C# Garnet stackalloc byte[8192])
pub(crate) const STACK_SCAN_BUF_SIZE: usize = 8192;

/// 便捷构造的默认调优参数 (1:1 对标 Garnet 默认树参数)
const PRESET_LEAF_PAGE_SIZE: usize = 16384;
const PRESET_MAX_RECORD_SIZE: usize = 4096;
const PRESET_MAX_KEY_LEN: usize = 512;
const PRESET_MIN_RECORD_SIZE: usize = 4;

/// 最大记录大小下限 (保证读/扫描缓冲区 ≥ 单值上限，同时作为栈缓冲路径的切换阈值)
pub(crate) const MIN_MAX_RECORD_SIZE: usize = STACK_READ_BUF_SIZE;

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
/// 后端/路径/记录上限支持原地恢复换树 (`recover_in_place`)：
/// 热路径字段走原子量，文件路径走冷路径读写锁。
///
/// 稳态下零写入原子计数开销 (1:1 对标 Garnet，点写由外部条带锁或 Epoch 保护)，
/// 仅保留 `barriers` 供显式写入屏障 (如 checkpoint / 测试) 在冷路径退避等待。
pub struct BfTreeService {
  pub(crate) raw_tree: AtomicPtr<BfTree>,
  pub(crate) arc_tree: RwLock<Option<Arc<BfTree>>>,
  pub(crate) retired_trees: RwLock<Vec<Arc<BfTree>>>,
  pub(crate) storage_backend: AtomicU8,
  pub(crate) file_path: RwLock<Option<String>>,
  pub(crate) max_record_size: AtomicUsize,
  pub(crate) disposed: AtomicBool,
  pub(crate) barriers: AtomicUsize,
}

unsafe impl Send for BfTreeService {}
unsafe impl Sync for BfTreeService {}

/// BfTree 写入屏障守卫 (兼容保留)
pub struct WriteBarrierGuard<'a> {
  pub(crate) service: &'a BfTreeService,
}

impl Drop for WriteBarrierGuard<'_> {
  fn drop(&mut self) {
    self.service.barriers.fetch_sub(1, Ordering::Release);
  }
}

impl BfTreeService {
  /// 当屏障生效时退避等待屏障解除 (冷路径，常态下无屏障 0 开销)
  #[cold]
  #[inline(never)]
  pub(crate) fn wait_for_barrier(&self) {
    let mut spins = 0u32;
    while self.barriers.load(Ordering::Acquire) != 0 {
      backoff(spins);
      spins = spins.wrapping_add(1);
    }
  }

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
      retired_trees: RwLock::new(Vec::new()),
      storage_backend: AtomicU8::new(storage_backend as u8),
      file_path: RwLock::new(file_path),
      max_record_size: AtomicUsize::new(max_record_size),
      disposed: AtomicBool::new(false),
      barriers: AtomicUsize::new(0),
    })
  }

  /// 根据配置创建全新的 BfTreeService (零 Debug 字符串解析)
  pub fn new(config: BfTreeConfig) -> Result<Self> {
    let storage_backend = config.storage_backend;
    let file_path = config.file_path;
    Self::new_with_backend(config.inner, storage_backend, file_path)
  }

  /// 便捷构造共享的默认调优配置 (1:1 对标 Garnet 默认树参数)
  pub fn preset_config(cb_min_record_size: usize) -> BfTreeConfig {
    let mut config = BfTreeConfig::default();
    config
      .use_snapshot(true)
      .leaf_page_size(PRESET_LEAF_PAGE_SIZE)
      .cb_max_record_size(PRESET_MAX_RECORD_SIZE)
      .cb_max_key_len(PRESET_MAX_KEY_LEN)
      .cb_min_record_size(if cb_min_record_size > 0 {
        cb_min_record_size
      } else {
        PRESET_MIN_RECORD_SIZE
      })
      .scan_promotion_rate(0);
    config
  }

  /// 便捷创建磁盘文件后端树实例 (1:1 对标 libs/cluster/Server/Gossip/Gossip.cs 中构造 BfTreeService(filePath: path, ...))
  pub fn open_disk(path: impl AsRef<Path>, cb_min_record_size: usize) -> Result<Self> {
    let p = path.as_ref();
    if p.as_os_str().is_empty() {
      return Err(Error::InvalidArgument(
        "磁盘后端必须指定有效的数据文件路径".into(),
      ));
    }
    if let Some(parent) = p.parent()
      && !parent.as_os_str().is_empty()
    {
      fs::create_dir_all(parent)?;
    }
    let mut config = Self::preset_config(cb_min_record_size);
    config.file_path(p);
    Self::new_with_backend(
      config,
      StorageBackendType::Disk,
      Some(p.to_string_lossy().into_owned()),
    )
  }

  /// 便捷创建纯内存后端树实例 (1:1 对标 libs/cluster/Server/Gossip/Gossip.cs 中构造 BfTreeService(storageBackend: Memory, ...))
  pub fn open_memory(cb_min_record_size: usize) -> Result<Self> {
    let mut config = Self::preset_config(cb_min_record_size);
    config.cache_only(true);
    Self::new_with_backend(config, StorageBackendType::Memory, None)
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
  use std::{env, fs, process, sync::Arc, thread::spawn};

  use super::*;
  use crate::types::{
    BfTreeConfig, BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, StorageBackendType,
  };

  fn mem_service() -> BfTreeService {
    BfTreeService::open_memory(0).unwrap()
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

  /// CPR 快照与并发写不互斥 (对标 C# 非阻塞并发 CPR)：快照进行中写入照常完成，
  /// 快照文件可恢复且点态自洽
  #[test]
  fn test_cpr_snapshot_concurrent_with_writers() {
    let service = Arc::new(mem_service());
    // 快照前基线数据：恢复后必须全量命中
    for i in 0..100u32 {
      let k = format!("base{i:04}");
      assert_eq!(
        service.insert(k.as_bytes(), b"base_value"),
        BfTreeInsertResult::Success
      );
    }

    let dir = env::temp_dir().join(format!(
      "wbftree_cpr_concurrent_{}_{}",
      process::id(),
      fastrand::u64(..)
    ));
    fs::create_dir_all(&dir).unwrap();
    let snap = dir.join("snap.bftree");

    // 写者与快照并发：写者全程不得等待快照完成
    let wsvc = Arc::clone(&service);
    let writer = spawn(move || {
      for i in 0..5000u32 {
        let k = format!("live{i:05}");
        assert_eq!(
          wsvc.insert(k.as_bytes(), b"payload"),
          BfTreeInsertResult::Success
        );
      }
    });
    // 主线程立即快照 (此刻写者大概率仍在途)：非屏障实现下快照不排空写者
    service.cpr_snapshot(&snap).unwrap();
    writer.join().unwrap();

    // 快照文件可恢复，快照前基线数据完整 (CPR 点态包含基线与部分在途写)
    let recovered =
      BfTreeService::recover_from_cpr_snapshot(&snap, true, StorageBackendType::Disk).unwrap();
    for i in 0..100u32 {
      let k = format!("base{i:04}");
      let (res, v) = recovered.read(k.as_bytes());
      assert_eq!(res, BfTreeReadResult::Found, "基线键 {k} 必须在快照中");
      assert_eq!(v.as_deref(), Some(&b"base_value"[..]));
    }

    fs::remove_dir_all(&dir).unwrap();
  }

  /// 损坏快照 (魔数不匹配/截断) 恢复必须返回结构化错误，绝不 panic
  #[test]
  fn test_recover_from_corrupt_snapshot_returns_err() {
    let dir = env::temp_dir().join(format!(
      "wbftree_corrupt_{}_{}",
      process::id(),
      fastrand::u64(..)
    ));
    fs::create_dir_all(&dir).unwrap();

    // 1. 无魔数文件
    let bad_magic = dir.join("bad_magic.bftree");
    fs::write(&bad_magic, b"garbage payload").unwrap();
    let err = BfTreeService::recover_from_cpr_snapshot(&bad_magic, true, StorageBackendType::Disk);
    assert!(matches!(err, Err(Error::Recovery(_))));

    // 2. 魔数被截断 (不足 16 字节)
    let truncated = dir.join("truncated.bftree");
    fs::write(&truncated, b"BF-TREE").unwrap();
    let err = BfTreeService::recover_from_cpr_snapshot(&truncated, true, StorageBackendType::Disk);
    assert!(matches!(err, Err(Error::Recovery(_))));

    // 3. 错误魔数 (长度合法但内容不符)
    let wrong_magic = dir.join("wrong_magic.bftree");
    fs::write(&wrong_magic, b"XX-TREE-V0-BEGIN_PAYLOAD").unwrap();
    let err =
      BfTreeService::recover_from_cpr_snapshot(&wrong_magic, true, StorageBackendType::Disk);
    assert!(matches!(err, Err(Error::Recovery(_))));

    fs::remove_dir_all(&dir).unwrap();
  }

  /// recover_in_place 换树后缓冲上限必须与恢复树严格同源 (r9 回归)：
  /// 小上限树 (preset 4096 栈路径) 换入大上限快照 (8192 暂存路径) 后，
  /// 读取旧上限之外的值不得越界 panic；读侧 sizing 载入点在读锁内与
  /// 写锁临界段内的上限发布配对 (见 read/recover_in_place 文档)
  #[test]
  fn test_recover_in_place_republishes_max_record_size() {
    let dir = env::temp_dir().join(format!(
      "wbftree_swap_max_{}_{}",
      process::id(),
      fastrand::u64(..)
    ));
    fs::create_dir_all(&dir).unwrap();

    // 大上限源树：值 6000B (键+值 ≤ 8192) 落树并 CPR 快照
    // (min=8 配 leaf=32768 满足引擎「每页记录数 ≤ 2^12」约束)
    let src_work = dir.join("src.data.bftree");
    let snap = dir.join("snap.bftree");
    {
      let mut config = BfTreeConfig::default();
      config
        .use_snapshot(true)
        .leaf_page_size(32768)
        .cb_max_record_size(8192)
        .cb_max_key_len(PRESET_MAX_KEY_LEN)
        .cb_min_record_size(8);
      config.file_path(&src_work);
      let src = BfTreeService::new(config).unwrap();
      let big = [b'x'; 6000];
      assert_eq!(src.insert(b"big_key", &big), BfTreeInsertResult::Success);
      src.cpr_snapshot(&snap).unwrap();
    }

    // 小上限目标树 (preset cb_max_record_size=4096)：换树前 6000B 值被拒
    let work = dir.join("work.data.bftree");
    let target = BfTreeService::open_disk(&work, 0).unwrap();
    assert_eq!(target.max_record_size(), PRESET_MAX_RECORD_SIZE);
    let big = [b'x'; 6000];
    assert_eq!(
      target.insert(b"big_key", &big),
      BfTreeInsertResult::InvalidKV
    );

    // 原地换入大上限快照树：上限随树同步翻新 (4096 → 8192，读路径切到暂存缓冲)
    target.recover_in_place(&snap, &work).unwrap();
    assert_eq!(target.max_record_size(), 8192);

    // 旧上限之外的值可读 (sizing 与新树同源，栈 4096 缓冲绝不对上 6000B 值)
    let (res, v) = target.read(b"big_key");
    assert_eq!(res, BfTreeReadResult::Found);
    assert_eq!(v.as_deref(), Some(&big[..]));

    // 新树按快照配置承载大值写入；小值照常
    assert_eq!(target.insert(b"fresh", &big), BfTreeInsertResult::Success);
    let (res, v) = target.read(b"fresh");
    assert_eq!(res, BfTreeReadResult::Found);
    assert_eq!(v.as_deref(), Some(&big[..]));

    // read_into 同口径：足量外部缓冲直读命中，容量不足安全拒绝 (InvalidArguments)
    // 而非直读越界
    let mut big_out = [0u8; 8192];
    let (res, len) = target.read_into(b"big_key", &mut big_out);
    assert_eq!(res, BfTreeReadResult::Found);
    assert_eq!(&big_out[..len], &big[..]);
    let mut small_out = [0u8; 64];
    let (res, _) = target.read_into(b"big_key", &mut small_out);
    assert_eq!(res, BfTreeReadResult::InvalidArguments);

    fs::remove_dir_all(&dir).unwrap();
  }
}
