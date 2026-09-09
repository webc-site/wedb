//! BfTree 高层服务包装器 (1:1 对标 Garnet BfTreeService.cs)
//!
//! 封装 Microsoft Research 的 bf-tree 核心实例，提供零堆分配切片 API、流式扫描回调与快照恢复。

use std::{
  cell::RefCell,
  fs,
  io::Read as _,
  panic::{self, AssertUnwindSafe},
  path::{Path, PathBuf},
  result::Result as StdResult,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
  },
};

use bf_tree::{BfTree, ConfigError, LeafInsertResult, LeafReadResult, ScanIter, ScanIterError};
use parking_lot::RwLock;
use wbase::time::{Duration, Instant};

use crate::{
  error::{Error, Result},
  manager::CPR_MAGIC,
  types::{
    BfTreeConfig, BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, ScanRecord,
    ScanReturnField, StorageBackendType,
  },
};

/// 栈上单值读取缓冲区大小 (值 ≤ 4096 字节走零堆分配快路径)
const STACK_READ_BUF_SIZE: usize = 4096;

/// 栈上扫描缓冲区大小 (键+值 ≤ cb_max_record_size ≤ 8192 时零堆分配快路径)
const STACK_SCAN_BUF_SIZE: usize = 8192;

/// 屏障排空在途写者的总超时上限 (对照 C# LightEpoch 排空无超时的差异：本实现
/// 同步等待无 epoch 兜底，持有者卡死时以错误显式上抛而非无限烧核)
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// 自旋退避一步 (统一复用 wbase 阶梯退避状态机)
pub(crate) use wbase::backoff::backoff;

/// 快照文件缺失错误
fn snapshot_missing(path: &Path) -> Error {
  let mut msg = String::from(SNAPSHOT_MISSING_PREFIX);
  msg.push_str(&path.display().to_string());
  Error::Recovery(msg)
}

/// 便捷构造的默认调优参数 (1:1 对标 Garnet 默认树参数)
const PRESET_LEAF_PAGE_SIZE: usize = 16384;
const PRESET_MAX_RECORD_SIZE: usize = 4096;
const PRESET_MAX_KEY_LEN: usize = 512;
const PRESET_MIN_RECORD_SIZE: usize = 4;

/// 快照文件缺失错误消息前缀
const SNAPSHOT_MISSING_PREFIX: &str = "快照文件不存在: ";

/// 最大记录大小下限 (保证读/扫描缓冲区 ≥ 单值上限，同时作为栈缓冲路径的切换阈值)
const MIN_MAX_RECORD_SIZE: usize = STACK_READ_BUF_SIZE;

// 线程本地点读暂存缓冲 (compio 线程每核：同线程串行复用，无竞争、无锁)。
// 容量按需增长到 cb_max_record_size 后终身复用，替代大值 GET 路径每次的
// 「分配 + 清零 + 收缩」三次堆操作；`read`/`read_into` 不重入、不跨 await，
// RefCell 独占借用恒安全。
thread_local! {
  static READ_SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// 检查文件是否为 bf-tree CPR 快照 (首部魔数校验；缺失/过小/读取失败一律 false)
///
/// 调引擎恢复前先行校验，把「损坏快照」变成结构化 [`Error::Recovery`] 而非依赖
/// catch_unwind (release 构建 panic = "abort" 下 unwind 拦截无效)。
#[inline]
pub(crate) fn file_has_cpr_magic(path: &Path) -> bool {
  let Ok(mut file) = fs::File::open(path) else {
    return false;
  };
  let mut magic = [0u8; CPR_MAGIC.len()];
  file.read_exact(&mut magic).is_ok() && magic == *CPR_MAGIC
}

/// 将 bf_tree::ConfigError 映射为可读字符串 (替代 Debug 格式化)
#[inline]
fn config_error_to_string(e: ConfigError) -> String {
  match e {
    ConfigError::MinimumRecordSize(s) => {
      let mut msg = String::from("MinimumRecordSize: ");
      msg.push_str(&s);
      msg
    }
    ConfigError::MaximumRecordSize(s) => {
      let mut msg = String::from("MaximumRecordSize: ");
      msg.push_str(&s);
      msg
    }
    ConfigError::LeafPageSize(s) => {
      let mut msg = String::from("LeafPageSize: ");
      msg.push_str(&s);
      msg
    }
    ConfigError::MaxKeyLen(s) => {
      let mut msg = String::from("MaxKeyLen: ");
      msg.push_str(&s);
      msg
    }
    ConfigError::CircularBufferSize(s) => {
      let mut msg = String::from("CircularBufferSize: ");
      msg.push_str(&s);
      msg
    }
    ConfigError::SnapshotFileInvalid(s) => {
      let mut msg = String::from("SnapshotFileInvalid: ");
      msg.push_str(&s);
      msg
    }
    ConfigError::SnapshotDisabled => String::from("SnapshotDisabled"),
  }
}

/// 将 bf_tree::ScanIterError 映射为可读字符串 (替代 Debug 格式化)
#[inline]
fn scan_iter_error_to_string(e: ScanIterError) -> &'static str {
  match e {
    ScanIterError::CacheOnlyMode => "CacheOnlyMode",
    ScanIterError::InvalidStartKey => "InvalidStartKey",
    ScanIterError::InvalidEndKey => "InvalidEndKey",
    ScanIterError::InvalidCount => "InvalidCount",
    ScanIterError::InvalidKeyRange => "InvalidKeyRange",
  }
}

/// 高层 BfTree 服务实例 (1:1 对标 Garnet BfTreeService)
///
/// 后端/路径/记录上限支持原地恢复换树 (`recover_in_place`)：
/// 热路径字段走原子量，文件路径走冷路径读写锁。
pub struct BfTreeService {
  tree: RwLock<Option<Arc<BfTree>>>,
  storage_backend: AtomicU8,
  file_path: RwLock<Option<String>>,
  max_record_size: AtomicUsize,
  disposed: AtomicBool,
  /// 活动写者计数 (insert/delete 微守卫持有时 > 0)
  writers: AtomicUsize,
  /// 活动屏障计数 (对标 Garnet checkpoint barrier)：> 0 时新写者短暂自旋等待。
  /// 计数而非布尔位，嵌套叠加（外层屏障内嵌换树/释放排空的内层屏障）时写者
  /// 阻塞至最外层守卫丢弃，杜绝内层先行释放溶解外层窗口。
  barriers: AtomicUsize,
}

/// BfTree 写者 RAII 守卫：持有时写入计数 > 0，供换树/释放窗口排空在途写者
struct WriteGuard<'a> {
  writers: &'a AtomicUsize,
}

impl<'a> WriteGuard<'a> {
  /// 获取写者守卫：换树/释放屏障进行中则退避等待屏障放行后再登记
  #[inline]
  fn acquire(service: &'a BfTreeService) -> Self {
    let mut spins = 0u32;
    loop {
      if service.barriers.load(Ordering::Acquire) == 0 {
        // SeqCst 登记 + SeqCst 复读与屏障侧「store(true) → load(writers)」构成
        // Dekker/store-buffering 配对：Release/Acquire 允许两侧同时读到旧值
        // （屏障漏算在途写者且写者漏看屏障），SeqCst 全序保证任一侧必然观察到对方。
        service.writers.fetch_add(1, Ordering::SeqCst);
        // 双检：登记瞬间屏障恰好开始则回退重试，保证与换树/释放互斥
        if service.barriers.load(Ordering::SeqCst) == 0 {
          return Self {
            writers: &service.writers,
          };
        }
        service.writers.fetch_sub(1, Ordering::Release);
      }
      backoff(spins);
      spins = spins.wrapping_add(1);
    }
  }
}

impl Drop for WriteGuard<'_> {
  #[inline]
  fn drop(&mut self) {
    self.writers.fetch_sub(1, Ordering::Release);
  }
}

/// BfTree 写入屏障 RAII 守卫 (对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SetCheckpointBarrier)
///
/// 持有期间屏障计数 > 0，全部 insert/delete 自旋等待；丢弃时递减计数，
/// 写者阻塞至最外层守卫丢弃。持有着必须保证屏障窗口内不做任何跨线程
/// 事件等待（await / driver I/O 依赖）——同步自旋的写者不会让出 executor，
/// 屏障持有着若依赖同线程事件将形成死锁。
pub struct WriteBarrierGuard<'a> {
  service: &'a BfTreeService,
}

impl Drop for WriteBarrierGuard<'_> {
  #[inline]
  fn drop(&mut self) {
    self.service.barriers.fetch_sub(1, Ordering::Release);
  }
}

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

    Ok(Self {
      tree: RwLock::new(Some(tree)),
      storage_backend: AtomicU8::new(storage_backend as u8),
      file_path: RwLock::new(file_path),
      max_record_size: AtomicUsize::new(max_record_size),
      disposed: AtomicBool::new(false),
      writers: AtomicUsize::new(0),
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
  fn preset_config(cb_min_record_size: usize) -> BfTreeConfig {
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
      });
    config
  }

  /// 便捷创建磁盘文件后端树实例 (1:1 对标 libs/cluster/Server/Gossip/Gossip.cs:new BfTreeService(filePath: path, ...))
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

  /// 便捷创建纯内存后端树实例 (1:1 对标 libs/cluster/Server/Gossip/Gossip.cs:new BfTreeService(storageBackend: Memory, ...))
  pub fn open_memory(cb_min_record_size: usize) -> Result<Self> {
    let mut config = Self::preset_config(cb_min_record_size);
    config.cache_only(true);
    Self::new_with_backend(config, StorageBackendType::Memory, None)
  }

  /// 获取底层 BfTree 的 Arc 实例
  #[inline]
  fn tree_arc(&self) -> Result<Arc<BfTree>> {
    self.check_disposed()?;
    self.tree.read().as_ref().cloned().ok_or(Error::Disposed)
  }

  /// 在读锁保护下安全借用底层 BfTree (零 Arc 克隆，零原子增减开销)
  #[inline]
  fn with_tree<R>(&self, f: impl FnOnce(&BfTree) -> R) -> Result<R> {
    self.check_disposed()?;
    let guard = self.tree.read();
    match guard.as_ref() {
      Some(tree) => Ok(f(tree)),
      None => Err(Error::Disposed),
    }
  }

  /// 获取裸指针标识 (用于 RangeIndexStub.tree_handle)
  #[inline]
  pub fn native_ptr(&self) -> u64 {
    if let Some(tree) = self.tree.read().as_ref() {
      Arc::as_ptr(tree) as usize as u64
    } else {
      0
    }
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
  fn max_record_size(&self) -> usize {
    self.max_record_size.load(Ordering::Relaxed)
  }

  /// 检查是否已释放 (1:1 对标 is_disposed)
  #[inline]
  pub fn is_disposed(&self) -> bool {
    self.disposed.load(Ordering::Acquire)
  }

  /// 检查是否已释放
  #[inline]
  fn check_disposed(&self) -> Result<()> {
    if self.is_disposed() {
      Err(Error::Disposed)
    } else {
      Ok(())
    }
  }

  /// 插入键值对 (零 Arc 克隆；空值快速拒绝，与底层 min_record_size 校验语义一致)
  ///
  /// 顶部登记写者微守卫：换树/释放窗口短暂自旋等待屏障放行 (对标 Garnet checkpoint barrier)。
  /// CPR 快照不经屏障 (引擎阶段协议与点写并发安全，对标 C# 非阻塞语义)。
  #[inline]
  pub fn insert(&self, key: &[u8], value: &[u8]) -> BfTreeInsertResult {
    if value.is_empty() {
      return BfTreeInsertResult::InvalidKV;
    }
    let _guard = WriteGuard::acquire(self);
    self
      .with_tree(|tree| match tree.insert(key, value) {
        LeafInsertResult::Success => BfTreeInsertResult::Success,
        LeafInsertResult::InvalidKV(_) => BfTreeInsertResult::InvalidKV,
      })
      .unwrap_or(BfTreeInsertResult::InvalidArguments)
  }

  /// 读取键对应的值 (≤4096 字节值走栈缓冲区零堆分配；更大值复用线程本地暂存缓冲)
  ///
  /// 底层 bf-tree 要求读取缓冲区不小于值长度（否则越界 panic），此处缓冲区恒 ≥ cb_max_record_size，绝无越界。
  ///
  /// 大值路径的时间/空间复杂度优化 (compio 线程每核：同线程串行复用，无竞争)：
  /// 旧实现每次 GET 按 cb_max_record_size 堆分配 + 整段清零 + shrink_to_fit 二次
  /// 收缩 (2 次分配 + O(max_record_size) memset)；现改为线程本地暂存一次分配终身
  /// 复用，命中后仅按值长精确拷出 (1 次分配、零清零)，稳态空间 O(1)/线程。
  pub fn read(&self, key: &[u8]) -> (BfTreeReadResult, Option<Vec<u8>>) {
    let max_record_size = self.max_record_size.load(Ordering::Relaxed);
    if max_record_size <= STACK_READ_BUF_SIZE {
      let mut stack_buf = [0u8; STACK_READ_BUF_SIZE];
      let (res, len) = self.read_direct(key, &mut stack_buf);
      (
        res,
        (res == BfTreeReadResult::Found).then(|| stack_buf[..len].to_vec()),
      )
    } else {
      READ_SCRATCH.with(|scratch| {
        let mut scratch = scratch.borrow_mut();
        if scratch.len() < max_record_size {
          scratch.resize(max_record_size, 0);
        }
        let (res, len) = self.read_direct(key, &mut scratch[..max_record_size]);
        (
          res,
          (res == BfTreeReadResult::Found).then(|| scratch[..len].to_vec()),
        )
      })
    }
  }

  /// 直读：要求 out_buf 容量 ≥ cb_max_record_size（恒能容纳任意合法值，零额外开销）
  #[inline]
  fn read_direct(&self, key: &[u8], out_buf: &mut [u8]) -> (BfTreeReadResult, usize) {
    self
      .with_tree(|tree| match tree.read(key, out_buf) {
        LeafReadResult::Found(n) => (BfTreeReadResult::Found, n as usize),
        LeafReadResult::NotFound => (BfTreeReadResult::NotFound, 0),
        LeafReadResult::Deleted => (BfTreeReadResult::Deleted, 0),
        LeafReadResult::InvalidKey => (BfTreeReadResult::InvalidKey, 0),
      })
      .unwrap_or((BfTreeReadResult::InvalidArguments, 0))
  }

  /// 经临时缓冲读取后按需拷贝至 out_buf (值超出 out_buf 容量时返回 InvalidArguments)
  #[inline]
  fn read_via_scratch(
    &self,
    key: &[u8],
    out_buf: &mut [u8],
    scratch: &mut [u8],
  ) -> (BfTreeReadResult, usize) {
    let (res, len) = self.read_direct(key, scratch);
    match res {
      BfTreeReadResult::Found if out_buf.len() >= len => {
        out_buf[..len].copy_from_slice(&scratch[..len]);
        (BfTreeReadResult::Found, len)
      }
      BfTreeReadResult::Found => (BfTreeReadResult::InvalidArguments, 0),
      r => (r, 0),
    }
  }

  /// 读取键对应的值到用户提供的输出切片中（零堆分配）
  ///
  /// 当 out_buf 容量 ≥ cb_max_record_size 时走直读快路径；否则改用内部安全缓冲读取后按需拷贝，
  /// 值超出 out_buf 容量时返回 InvalidArguments（底层 bf-tree 缓冲区过小会直接越界 panic，此处彻底拦截）。
  pub fn read_into(&self, key: &[u8], out_buf: &mut [u8]) -> (BfTreeReadResult, usize) {
    let max_record_size = self.max_record_size.load(Ordering::Relaxed);
    if out_buf.len() >= max_record_size {
      return self.read_direct(key, out_buf);
    }
    if max_record_size <= STACK_READ_BUF_SIZE {
      let mut stack_buf = [0u8; STACK_READ_BUF_SIZE];
      self.read_via_scratch(key, out_buf, &mut stack_buf)
    } else {
      READ_SCRATCH.with(|scratch| {
        let mut scratch = scratch.borrow_mut();
        if scratch.len() < max_record_size {
          scratch.resize(max_record_size, 0);
        }
        self.read_via_scratch(key, out_buf, &mut scratch[..max_record_size])
      })
    }
  }

  /// 删除指定键 (打入墓碑标记，与 insert 同受换树/释放屏障保护)
  #[inline]
  pub fn delete(&self, key: &[u8]) -> BfTreeDeleteResult {
    let _guard = WriteGuard::acquire(self);
    self
      .with_tree(|tree| {
        tree.delete(key);
        BfTreeDeleteResult::Success
      })
      .unwrap_or(BfTreeDeleteResult::InvalidArguments)
  }

  /// 基于数量的流式范围扫描 (内部栈缓冲区零分配回调)
  ///
  /// count == 0 直接返回 0 (1:1 对标 Garnet 原生层允许 count=0 的行为)
  pub fn scan_with_count_callback<F>(
    &self,
    start_key: &[u8],
    count: usize,
    return_field: ScanReturnField,
    on_record: F,
  ) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    if count == 0 {
      return Ok(0);
    }
    self.scan_callback(
      |tree| tree.scan_with_count(start_key, count, return_field),
      return_field,
      on_record,
    )
  }

  /// 基于数量的范围扫描并返回记录列表
  pub fn scan_with_count(
    &self,
    start_key: &[u8],
    count: usize,
    return_field: ScanReturnField,
  ) -> Result<Vec<ScanRecord>> {
    let mut records = Vec::with_capacity(count.min(1024));
    self.scan_with_count_callback(start_key, count, return_field, |k, v| {
      records.push(ScanRecord {
        key: k.to_vec(),
        value: v.to_vec(),
      });
      true
    })?;
    Ok(records)
  }

  /// 闭区间流式范围扫描 (内部栈缓冲区零分配回调)
  ///
  /// start_key > end_key 时视为空区间直接返回 0 (1:1 对标 Garnet 原生层行为)
  pub fn scan_with_end_key_callback<F>(
    &self,
    start_key: &[u8],
    end_key: &[u8],
    return_field: ScanReturnField,
    on_record: F,
  ) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    if start_key > end_key {
      return Ok(0);
    }
    self.scan_callback(
      |tree| tree.scan_with_end_key(start_key, end_key, return_field),
      return_field,
      on_record,
    )
  }

  /// 闭区间范围扫描并返回记录列表
  pub fn scan_with_end_key(
    &self,
    start_key: &[u8],
    end_key: &[u8],
    return_field: ScanReturnField,
  ) -> Result<Vec<ScanRecord>> {
    let mut records = Vec::with_capacity(32);
    self.scan_with_end_key_callback(start_key, end_key, return_field, |k, v| {
      records.push(ScanRecord {
        key: k.to_vec(),
        value: v.to_vec(),
      });
      true
    })?;
    Ok(records)
  }

  /// 全表顺序流式扫描 (内部栈缓冲区零分配回调)
  pub fn scan_all_callback<F>(&self, return_field: ScanReturnField, on_record: F) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    self.scan_with_count_callback(&[0], usize::MAX, return_field, on_record)
  }

  /// 全表顺序扫描
  pub fn scan_all(&self, return_field: ScanReturnField) -> Result<Vec<ScanRecord>> {
    let mut records = Vec::with_capacity(32);
    self.scan_all_callback(return_field, |k, v| {
      records.push(ScanRecord {
        key: k.to_vec(),
        value: v.to_vec(),
      });
      true
    })?;
    Ok(records)
  }

  /// 扫描统一驱动：经底层校验构造迭代器 (非法键/区间返回 Err，杜绝底层未定义行为)，逐条填充回调。
  ///
  /// 迭代前仅克隆一次底层 Arc 并随即释放包装读锁：整个用户回调期间不持有任何服务级锁，
  /// 回调因此可安全重入本服务的点读 (read/read_into) 乃至 dispose 而无包装层自死锁风险
  /// (1:1 对标 C# 扫描期间无托管锁的语义；parking_lot 写优先，持读锁跨回调时一旦有
  /// dispose 排队，回调内任何重入读取都将永久阻塞)。Arc 同时保证迭代期间底层引擎实例
  /// 存活，dispose 与扫描并发时扫描仍可在存活引擎上安全完成 (对标 C# LightEpoch 延迟释放)。
  ///
  /// 注意：回调不得对同一棵树重入写入 (insert/delete/scan)——底层引擎扫描持有叶子共享
  /// 闩锁，同线程重入写同叶子会在引擎闩锁层自死锁 (与 C# 原生层约束一致，非包装层问题)。
  fn scan_callback<F>(
    &self,
    make_iter: impl FnOnce(&BfTree) -> StdResult<ScanIter<'_, '_>, ScanIterError>,
    return_field: ScanReturnField,
    mut on_record: F,
  ) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    // 单次原子引用计数开销换取回调重入安全，绝不持锁跨用户回调
    let tree = self.tree_arc()?;
    let mut iter = make_iter(&tree)
      .map_err(|e| Error::InvalidArgument(scan_iter_error_to_string(e).to_string()))?;

    // 缓冲区恒 ≥ 最大记录长度 (键+值 ≤ cb_max_record_size)，底层填充绝不会越界
    let mut stack_buf = [0u8; STACK_SCAN_BUF_SIZE];
    let mut heap_buf;
    let max_record_size = self.max_record_size.load(Ordering::Relaxed);
    let buf: &mut [u8] = if max_record_size <= STACK_SCAN_BUF_SIZE {
      &mut stack_buf
    } else {
      heap_buf = vec![0u8; max_record_size];
      &mut heap_buf
    };

    let mut scanned = 0;
    while let Some((k_len, v_len)) = iter.next(buf) {
      let k = if return_field != ScanReturnField::Value {
        &buf[..k_len]
      } else {
        &[]
      };
      let v = if return_field != ScanReturnField::Key {
        &buf[k_len..k_len + v_len]
      } else {
        &[]
      };
      scanned += 1;
      if !on_record(k, v) {
        break;
      }
    }

    Ok(scanned)
  }

  /// 排空全部在途写者 (对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotUnderClaim 的 claim 等待)
  ///
  /// 必须在 [`write_barrier`](Self::write_barrier) 置位后调用：等待已越过双检的
  /// 在途写者全部退出，此后 writers == 0 即树对写静稳，可安全换树/释放。
  /// 退避阶梯等待，超过 [`DRAIN_TIMEOUT`] 返回 [`Error::Timeout`]（C# 由 LightEpoch
  /// 排空语义保证推进，此处为同步自旋无 epoch 兜底，必须以超时显式暴露持有者异常）。
  fn drain_writers(&self) -> Result<()> {
    self.drain_writers_within(DRAIN_TIMEOUT)
  }

  /// [`drain_writers`](Self::drain_writers) 的可注入超时版本 (测试确定性验证用)
  fn drain_writers_within(&self, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let mut spins = 0u32;
    while self.writers.load(Ordering::SeqCst) != 0 {
      if Instant::now() >= deadline {
        return Err(Error::Timeout);
      }
      backoff(spins);
      spins = spins.wrapping_add(1);
    }
    Ok(())
  }

  /// 开启写入屏障并返回 RAII 守卫 (对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SetCheckpointBarrier)
  ///
  /// 计数式屏障：嵌套叠加时写者阻塞至最外层守卫丢弃；守卫丢弃递减计数。
  /// ⚠️ 持有窗口内严禁跨 await / 依赖同线程 I/O 事件（见类型文档）。
  /// 仅用于换树/释放等生命周期窗口 ([`Self::recover_in_place`] /
  /// [`Self::dispose_quiesced`])；CPR 快照不经屏障——引擎 CPR 阶段协议与点写
  /// 并发安全 (对标 C# 非阻塞语义)。
  pub fn write_barrier(&self) -> WriteBarrierGuard<'_> {
    // SeqCst 置位：与写者侧 fetch_add(SeqCst) + 双检 load(SeqCst) 构成全序配对
    self.barriers.fetch_add(1, Ordering::SeqCst);
    WriteBarrierGuard { service: self }
  }

  /// 触发 CPR 快照 (1:1 对标 Garnet 原生层 bftree_cpr_snapshot 的非阻塞语义)
  ///
  /// 与 insert/delete 并发安全：引擎 CPR 采用阶段协议 (REST → PREPARE →
  /// IN_PROGRESS → SWEEP)，在途写者按当前快照版本把触碰的页自行拷入快照文件，
  /// 快照不阻塞写、写不阻塞快照 (较旧版「屏障排空再快照」的改动：屏障会让
  /// 大树 checkpoint 期间全部写入停摆且在 compio 核上自旋空转)。
  /// 同一棵树的并发快照互斥由 [`crate::RangeIndexManager`] 的 per-tree claim
  /// 承担——引擎对并发快照静默 no-op，宿主必须串行化。
  ///
  /// 底层 bf-tree 在未启用 use_snapshot 等异常场景下直接 panic 而非返回错误，
  /// 此处 catch_unwind 拦截转换为 Err (1:1 对标 Garnet 原生互操作层的处理方式)。
  /// 注意：release 构建全局 `panic = "abort"`，panic 路径实际以进程终止收场
  /// （检查点元数据未发布，重启一致性不受影响）；catch_unwind 仅在
  /// unwind 构建（dev/test）下生效，且 panic 点位于任何写入之前，无部分写入副作用。
  pub fn cpr_snapshot(&self, snapshot_path: impl AsRef<Path>) -> Result<()> {
    let tree = self.tree_arc()?;
    let p = snapshot_path.as_ref();
    if let Some(parent) = p.parent()
      && !parent.as_os_str().is_empty()
    {
      fs::create_dir_all(parent)?;
    }
    panic::catch_unwind(AssertUnwindSafe(|| tree.cpr_snapshot(p)))
      .map_err(|_| Error::Snapshot("底层引擎异常 (快照未启用或内部状态异常)".into()))
  }

  /// 从 CPR 快照原地换入恢复树 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:RestoreTree 的句柄重锚定)
  ///
  /// 恢复流程（保证任何时刻都不破坏仍存活的旧树，且任一步失败状态自洽）：
  /// 1. 快照拷贝至 `work_path.recovering` 临时文件（绝不覆盖旧树正在使用的 `work_path`）；
  /// 2. 从临时文件恢复出新树（其活动基文件即该临时 inode）；
  /// 3. 临时 inode 原子 rename 至 `work_path`——旧树 fd 指向原 inode 不受换名影响；
  ///    此后工作路径命名即新快照态，下次启动可凭魔数直接恢复，且 purge 回收
  ///    token 目录绝不伤及活动树；
  /// 4. 屏障内排空在途写者后写锁下换树——换树并发窗口内不存在「写入旧树成功
  ///    应答却被换树丢弃」的丢失写（屏障前已应答的写入随恢复回滚属既定语义）；
  ///    旧树摘除后移出写锁析构（其基文件为已换名的原 inode，经 fd 访问全程有效）。
  ///
  /// 所有持有 `Arc<BfTreeService>` 的使用方（如 ACL 存储）无需重绑即透明使用恢复后的树。
  pub fn recover_in_place(&self, snapshot_path: &Path, work_path: &Path) -> Result<()> {
    if !snapshot_path.exists() {
      return Err(snapshot_missing(snapshot_path));
    }
    // 预建工作路径父目录：保证同目录 rename 不因目录缺失而失败
    if let Some(parent) = work_path.parent()
      && !parent.as_os_str().is_empty()
    {
      fs::create_dir_all(parent)?;
    }
    let mut tmp_os = work_path.as_os_str().to_os_string();
    tmp_os.push(".recovering");
    let tmp_path = PathBuf::from(tmp_os);

    // 1. 预置临时文件（覆盖上一轮可能的残留）；恢复失败时清理残留后原样上抛
    fs::copy(snapshot_path, &tmp_path)?;
    let recovered = match Self::recover_from_cpr_snapshot(&tmp_path, true, StorageBackendType::Disk)
    {
      Ok(tree) => tree,
      Err(e) => {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
      }
    };
    if self.disposed.load(Ordering::Acquire) {
      let _ = fs::remove_file(&tmp_path);
      return Err(Error::Disposed);
    }

    // 2. 先换名再换树：rename 失败时树未换、盘上态未动，清理残留即可原样上抛；
    //    成功后 work_path 命名即新快照 inode（旧树经 fd 继续访问原 inode，不受影响）
    if let Err(e) = fs::rename(&tmp_path, work_path) {
      let _ = fs::remove_file(&tmp_path);
      return Err(e.into());
    }

    // 3. 屏障内排空在途写者再写锁换树：写锁内仅做指针替换，
    //    旧树析构（遍历基页刷盘，经自身 fd 访问已换名的原 inode）移至锁外
    let old_tree = {
      let _barrier = self.write_barrier();
      self.drain_writers()?;
      let mut guard = self.tree.write();
      // 写锁内复查：并发 dispose 抢先释放时放弃换树，保持已释放语义
      //（recovered 树随栈变量析构关闭，旧树槽位保持 None）
      if self.disposed.load(Ordering::Acquire) {
        return Err(Error::Disposed);
      }
      let old_tree = guard.take();
      *guard = recovered.tree.write().take();
      old_tree
    };
    drop(old_tree);
    self
      .storage_backend
      .store(recovered.storage_backend() as u8, Ordering::Release);
    *self.file_path.write() = Some(work_path.to_string_lossy().into_owned());
    self
      .max_record_size
      .store(recovered.max_record_size(), Ordering::Release);
    Ok(())
  }

  /// 从 CPR 快照文件恢复创建全新的 BfTreeService
  ///
  /// 调引擎前先做魔数预检：损坏/非快照文件走结构化 [`Error::Recovery`]，
  /// 不依赖 unwind 拦截 (release 构建 panic = "abort" 下 catch_unwind 无效)。
  /// catch_unwind 仅兜底引擎内部的断言异常 (1:1 对标 Garnet 原生互操作层
  /// bftree_new_from_cpr_snapshot 的处理方式)，dev/test 构建下生效。
  pub fn recover_from_cpr_snapshot(
    recovery_path: impl AsRef<Path>,
    enable_snapshots: bool,
    storage_backend: impl Into<StorageBackendType>,
  ) -> Result<Self> {
    let p = recovery_path.as_ref();
    if !p.exists() {
      return Err(snapshot_missing(p));
    }
    if !file_has_cpr_magic(p) {
      let mut msg = String::from("快照文件损坏或格式非法 (魔数不匹配): ");
      msg.push_str(&p.display().to_string());
      return Err(Error::Recovery(msg));
    }
    let backend = storage_backend.into();
    let use_snapshot = enable_snapshots;
    match panic::catch_unwind(AssertUnwindSafe(|| {
      BfTree::new_from_cpr_snapshot(p, use_snapshot, None, None, None)
    })) {
      Ok(Ok(tree)) => {
        let max_record_size = tree
          .config()
          .get_cb_max_record_size()
          .max(MIN_MAX_RECORD_SIZE);
        Ok(Self {
          tree: RwLock::new(Some(Arc::new(tree))),
          storage_backend: AtomicU8::new(backend as u8),
          file_path: RwLock::new(Some(p.to_string_lossy().into_owned())),
          max_record_size: AtomicUsize::new(max_record_size),
          disposed: AtomicBool::new(false),
          writers: AtomicUsize::new(0),
          barriers: AtomicUsize::new(0),
        })
      }
      Ok(Err(e)) => Err(Error::Recovery(config_error_to_string(e))),
      Err(_) => {
        let mut msg = String::from("快照文件损坏或格式非法: ");
        msg.push_str(&p.display().to_string());
        Err(Error::Recovery(msg))
      }
    }
  }

  /// 释放实例并回收资源
  ///
  /// 幂等：重复调用或与 [`dispose_quiesced`](Self::dispose_quiesced) 并发时仅一方生效。
  /// 仅适合确认无并发写者的场景 (进程退出 / 测试收尾)；注册表驱动的生命周期释放
  /// 一律走 [`dispose_quiesced`](Self::dispose_quiesced)。
  pub fn dispose(&self) {
    if !self.disposed.swap(true, Ordering::SeqCst) {
      self.tree.write().take();
    }
  }

  /// 屏障内排空在途写者后释放实例 (对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:DisposeTreeUnderLock 经 LightEpoch
  /// 延迟 dispose 的排空语义)
  ///
  /// C# 依赖 storeEpoch 把「摘树 + 删文件」推迟到所有在途 reader/writer 越过之后；
  /// 本实现改由调用方先从注册表摘除条目 (并发恢复立即不可见)，再以写入屏障挡住
  /// 新写者、排空已登记写者，保证树静稳后才摘除引擎实例。调用方随后删除数据文件
  /// 时，不存在「insert 已成功应答却落入正被 unlink 的 inode」的撕裂窗口——写入
  /// 与文件删除严格有序 (在途写完成于删除之前，屏障后新写直接以 Disposed 失败)。
  ///
  /// 排空超过 [`DRAIN_TIMEOUT`] 返回 [`Error::Timeout`]：此时 disposed 已置位
  /// (后续读写一律以 Disposed 拒绝)，仅引擎句柄延迟到 Arc 归零兜底释放，调用方
  /// 不得继续删文件 (在途写者尚未静稳)。
  ///
  /// 幂等：与 [`dispose`](Self::dispose) 并发时仅一方生效。
  pub fn dispose_quiesced(&self) -> Result<()> {
    if self.disposed.swap(true, Ordering::SeqCst) {
      return Ok(());
    }
    let _barrier = self.write_barrier();
    self.drain_writers_within(DRAIN_TIMEOUT)?;
    self.tree.write().take();
    Ok(())
  }
}

impl Drop for BfTreeService {
  fn drop(&mut self) {
    self.dispose();
  }
}

#[cfg(test)]
mod tests {
  use std::{
    env, process,
    thread::{sleep, spawn},
  };

  use super::*;

  fn mem_service() -> BfTreeService {
    BfTreeService::open_memory(0).unwrap()
  }

  /// 排空必须等在途写者退出后才摘树：对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:DisposeTreeUnderLock 经
  /// storeEpoch 排空后才 dispose + 删文件的顺序语义
  #[test]
  fn test_dispose_quiesced_waits_inflight_writer() {
    let service = Arc::new(mem_service());
    assert_eq!(
      service.insert(b"key1", b"val1"),
      BfTreeInsertResult::Success
    );

    // 模拟已越过双检的在途写者 (已登记、尚未退出)
    service.writers.store(1, Ordering::SeqCst);
    let done = Arc::new(AtomicBool::new(false));
    let (service_t, done_t) = (Arc::clone(&service), Arc::clone(&done));
    let handle = spawn(move || {
      service_t.dispose_quiesced().unwrap();
      done_t.store(true, Ordering::Release);
    });

    // 排空未完成：引擎实例尚未摘除 (屏障与 disposed 已置位)
    sleep(Duration::from_millis(100).into());
    assert!(!done.load(Ordering::Acquire));
    assert!(service.tree.read().is_some());

    // 写者退出 → 排空完成 → 树摘除
    service.writers.store(0, Ordering::SeqCst);
    handle.join().unwrap();
    assert!(done.load(Ordering::Acquire));
    assert!(service.tree.read().is_none());
    assert!(service.is_disposed());
    // 屏障放行后的新写者以 Disposed 拒绝，绝不落入已释放的树
    assert_eq!(
      service.insert(b"key2", b"val2"),
      BfTreeInsertResult::InvalidArguments
    );
  }

  /// 排空超时上抛且不摘树、不置 disposed (超时注入 50ms 确定性验证)
  #[test]
  fn test_drain_writers_timeout() {
    let service = mem_service();
    service.writers.store(1, Ordering::SeqCst);
    let err = service
      .drain_writers_within(Duration::from_millis(50))
      .unwrap_err();
    assert!(matches!(err, Error::Timeout));
    assert!(!service.is_disposed());
    assert!(service.tree.read().is_some());
    service.writers.store(0, Ordering::SeqCst);
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
}
