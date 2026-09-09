use std::{
  fs,
  path::Path,
  result::Result as StdResult,
  sync::{Arc, atomic::Ordering},
};

use thiserror::Error as ThisError;
use wbftree::{
  BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, BfTreeService, RangeIndexManager,
  RangeIndexStub, ScanRecord, ScanReturnField, StorageBackend, StorageBackendType, TreeTuning,
};
use wdev::Device;
use wval::{CollectionType, META_VALUE_SIZE, MetaValue};

use crate::{
  error::{Error, Result},
  session::StoreSession,
};

/// 范围索引操作错误类型 (1:1 对标 Garnet RangeIndexResult 与错误信息)
#[derive(ThisError, Debug, Clone, PartialEq, Eq)]
pub enum RangeIndexError {
  /// 索引已存在
  #[error("ERR index already exists")]
  AlreadyExists,
  /// 索引未找到
  #[error("ERR range index not found")]
  NotFound,
  /// 键类型不匹配
  #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
  WrongType,
  /// 键值长度超限
  #[error(
    "ERR key+value size must be between {min_record_size} and {max_record_size} bytes (got {total_len}), max key length {max_key_len} (got {key_len})"
  )]
  InvalidKV {
    min_record_size: u32,
    max_record_size: u32,
    max_key_len: u32,
    total_len: usize,
    key_len: usize,
  },
  /// 纯内存模式不支持扫描
  #[error("ERR RI.SCAN is not supported for MEMORY-mode indexes")]
  MemoryModeNotSupported,
  /// 内部存储或 I/O 错误
  #[error("ERR {0}")]
  Internal(String),
}

impl From<Error> for RangeIndexError {
  fn from(err: Error) -> Self {
    Self::Internal(err.to_string())
  }
}

impl From<wbftree::Error> for RangeIndexError {
  fn from(err: wbftree::Error) -> Self {
    match err {
      wbftree::Error::IndexExists => Self::AlreadyExists,
      other => Self::Internal(other.to_string()),
    }
  }
}

/// 默认叶子页面大小（自动推导失败时的回退值）
const DEFAULT_LEAF_PAGE_SIZE: usize = 4096;

/// 把 wbftree 同步重操作卸载到 compio 阻塞线程 (基于 compio 生态的核保护优化)
///
/// thread-per-core 下同步阻塞会停摆整核任务：快照恢复 (整文件解析 + 环形缓冲
/// 分配)、整树释放 (Drop 遍历基页刷盘)、文件创建/换入均可能达到百毫秒级，一律
/// 经 `spawn_blocking` 在独立线程执行，宿主核继续调度其他任务。热路径 (注册表
/// 命中后的内存点操作) 不经此通道，与 C# 会话线程直调开销对齐。
///
/// 要求调用方处于 compio 运行时上下文 (RI 会话操作本就依赖运行时异步 I/O)。
/// manager 的同步方法在闭包内自取自放条带锁，锁不跨线程边界；持锁跨 await 的
/// 原子性窗口 (publish/rename) 由调用方任务承担，compio 任务不迁移故安全。
pub(crate) async fn range_index_blocking<T: Send + 'static>(
  op: impl FnOnce() -> T + Send + 'static,
) -> T {
  compio::runtime::spawn_blocking(op)
    .await
    .expect("RangeIndex 阻塞任务异常退出")
}

impl<D: Device> StoreSession<D> {
  /// 创建新的 RangeIndex 索引 (1:1 对标 Garnet StorageSession.RangeIndexCreate)
  pub async fn range_index_create(
    &self,
    key: &[u8],
    storage_backend: StorageBackend,
    tuning: TreeTuning,
  ) -> StdResult<(), RangeIndexError> {
    // 1. 检查键是否已存在于存储中
    if self.read(key).await?.is_some() {
      return Err(RangeIndexError::AlreadyExists);
    }
    if let Some(meta) = self.load_meta(key).await?
      && meta.size > 0
    {
      return Err(RangeIndexError::AlreadyExists);
    }

    // 2. 动态计算叶子节点页面大小
    let actual_leaf_page_size = if tuning.leaf_page_size > 0 {
      tuning.leaf_page_size
    } else if tuning.max_record_size > 0 {
      RangeIndexManager::compute_leaf_page_size(tuning.max_record_size)
    } else {
      DEFAULT_LEAF_PAGE_SIZE
    };

    // 3. 在底层 RangeIndexManager 中创建并托管 BfTree 实例
    //    (数据文件创建 + 环形缓冲分配属重操作，卸载阻塞线程保护 compio 核)
    let mgr = Arc::clone(&self.store.range_index);
    let create_key = key.to_vec();
    let create_tuning = TreeTuning {
      leaf_page_size: actual_leaf_page_size,
      ..tuning
    };
    let create_backend = storage_backend.clone();
    let tree =
      range_index_blocking(move || mgr.create_bftree(&create_key, create_backend, create_tuning))
        .await
        .map_err(RangeIndexError::from)?;

    // 4. 构建定长 35 字节 RangeIndexStub 并持久化入主日志库
    let stub = RangeIndexStub::new(
      tree.native_ptr(),
      tuning.cache_size as u64,
      tuning.min_record_size as u32,
      tuning.max_record_size as u32,
      tuning.max_key_len as u32,
      actual_leaf_page_size as u32,
      storage_backend,
    );

    let meta_k = self.session_meta_key(key);
    let key_id = self.store.next_key_id.fetch_add(1, Ordering::Relaxed);
    let meta = MetaValue::new(key_id, CollectionType::RangeIndex, 1, 1);

    let val = encode_meta_stub_record(&meta, &stub);

    if let Err(e) = self.upsert_raw(&meta_k, &val).await {
      // 事务回滚：清理此前在内存中注册及磁盘生成的孤儿文件
      // (尽力而为：主错误已优先上抛，回滚自身的屏障超时属进程级故障，不再覆盖)
      let _ = self.store.range_index.delete_index(key);
      return Err(RangeIndexError::Internal(e.to_string()));
    }

    Ok(())
  }

  /// 读取 RangeIndex 存根及元数据（支持防重入与类型安全检查）
  ///
  /// 热路径时间复杂度优化：RI 点操作每次调用本函数，旧实现经 load_meta 读一次
  /// 元记录后再 read_raw 重复读同一记录（2 次主存 I/O）；现改为单次 read_raw
  /// 同帧解析 MetaValue + TTL 守卫 + 类型检查 + 存根解码（1 次主存 I/O），
  /// 语义与 load_meta 口径一致（过期视同不存在、key_id 元数据同步）。
  pub async fn load_range_index_stub(
    &self,
    key: &[u8],
  ) -> StdResult<Option<(MetaValue, RangeIndexStub)>, RangeIndexError> {
    let meta_k = self.session_meta_key(key);
    let Some(bytes) = self.read_raw(&meta_k).await? else {
      // 无 RI 元记录：检查是否存在同名普通字符串键 (WRONGTYPE 语义)
      if self.read(key).await?.is_some() {
        return Err(RangeIndexError::WrongType);
      }
      return Ok(None);
    };
    if bytes.len() < META_VALUE_SIZE {
      return Ok(None);
    }
    let meta = MetaValue::from_slice(&bytes[..META_VALUE_SIZE])
      .map_err(|e| RangeIndexError::Internal(e.to_string()))?;
    if meta.size == 0 {
      self
        .store
        .update_key_id_meta(meta.key_id, meta.version, false);
      return Ok(None);
    }
    // TTL 守卫 (与 load_meta 口径一致)：过期集合视同不存在
    if self.has_ttl_tag(key)? && self.check_expired(key).await? {
      return Ok(None);
    }
    self
      .store
      .update_key_id_meta(meta.key_id, meta.version, true);
    if meta.collection_type != CollectionType::RangeIndex {
      return Err(RangeIndexError::WrongType);
    }
    if bytes.len() < META_VALUE_SIZE + wbftree::RANGE_INDEX_STUB_SIZE {
      return Ok(None);
    }
    let stub = RangeIndexStub::decode(
      &bytes[META_VALUE_SIZE..META_VALUE_SIZE + wbftree::RANGE_INDEX_STUB_SIZE],
    )
    .map_err(|e| RangeIndexError::Internal(e.to_string()))?;
    Ok(Some((meta, stub)))
  }

  /// 获取在线 BfTree 实例及其条带共享读锁 (1:1 对标 Garnet ReadRangeIndex 与 ReadRangeIndexLock)
  ///
  /// 先在无锁/共享锁状态下快速命中（稳态 O(1)：一次 volatile 读 + 一次注册表
  /// 查找），若树未激活则释放读锁后调用 get_or_open_tree（文件 I/O + 快照解析
  /// 属重操作，卸载 compio 阻塞线程，避免慢恢复停摆整核），随后重新获取共享
  /// 读锁。整个数据读取/修改操作在其 RAII 读锁保护下安全执行。
  pub async fn acquire_tree_read(
    &self,
    key: &[u8],
    stub: &RangeIndexStub,
  ) -> StdResult<(Arc<BfTreeService>, parking_lot::RwLockReadGuard<'_, ()>), RangeIndexError> {
    let key_hash = RangeIndexManager::key_hash_of(key);
    loop {
      match self.store.range_index.wait_for_tree_checkpoint(key) {
        Ok(true) => continue,
        Ok(false) => {}
        Err(e) => return Err(RangeIndexError::Internal(e.to_string())),
      }
      let read_lock = self.store.range_index.locks().read(key_hash);
      if let Some(tree) = self.store.range_index.get_tree(key) {
        return Ok((tree, read_lock));
      }
      drop(read_lock);

      // 惰性恢复慢路径：卸载阻塞线程 (条带锁在 manager 内部自取自放，不跨线程边界)
      let mgr = Arc::clone(&self.store.range_index);
      let restore_key = key.to_vec();
      let restore_stub = *stub;
      range_index_blocking(move || mgr.get_or_open_tree(&restore_key, &restore_stub))
        .await
        .map_err(|e| RangeIndexError::Internal(e.to_string()))?;
    }
  }

  /// 设置字段值 (1:1 对标 Garnet StorageSession.RangeIndexSet)
  pub async fn range_index_set(
    &self,
    key: &[u8],
    field: &[u8],
    value: &[u8],
  ) -> StdResult<(), RangeIndexError> {
    let (_, stub) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;

    let total_len = field.len() + value.len();
    if field.len() > stub.max_key_len as usize
      || total_len < stub.min_record_size as usize
      || total_len > stub.max_record_size as usize
    {
      return Err(RangeIndexError::InvalidKV {
        min_record_size: stub.min_record_size,
        max_record_size: stub.max_record_size,
        max_key_len: stub.max_key_len,
        total_len,
        key_len: field.len(),
      });
    }

    let (tree, _read_lock) = self.acquire_tree_read(key, &stub).await?;

    match tree.insert(field, value) {
      BfTreeInsertResult::Success => {
        if let Some(listener) = self.store.range_listener() {
          listener(key, field, value, false);
        }
        Ok(())
      }
      BfTreeInsertResult::InvalidArguments => {
        Err(RangeIndexError::Internal("invalid arguments".to_string()))
      }
      BfTreeInsertResult::InvalidKV => Err(RangeIndexError::InvalidKV {
        min_record_size: stub.min_record_size,
        max_record_size: stub.max_record_size,
        max_key_len: stub.max_key_len,
        total_len,
        key_len: field.len(),
      }),
    }
  }

  /// 读取字段值 (1:1 对标 Garnet StorageSession.RangeIndexGet)
  pub async fn range_index_get(
    &self,
    key: &[u8],
    field: &[u8],
  ) -> StdResult<Option<Vec<u8>>, RangeIndexError> {
    let (_, stub) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;

    let (tree, _read_lock) = self.acquire_tree_read(key, &stub).await?;

    let (res, val) = tree.read(field);
    match res {
      BfTreeReadResult::Found => Ok(val),
      BfTreeReadResult::NotFound | BfTreeReadResult::Deleted => Ok(None),
      BfTreeReadResult::InvalidArguments | BfTreeReadResult::InvalidKey => {
        Err(RangeIndexError::Internal("invalid arguments".to_string()))
      }
    }
  }

  /// 删除字段 (1:1 对标 Garnet StorageSession.RangeIndexDel)
  pub async fn range_index_del(
    &self,
    key: &[u8],
    field: &[u8],
  ) -> StdResult<bool, RangeIndexError> {
    let (_, stub) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;

    let (tree, _read_lock) = self.acquire_tree_read(key, &stub).await?;

    match tree.delete(field) {
      BfTreeDeleteResult::Success => {
        if let Some(listener) = self.store.range_listener() {
          listener(key, field, &[], true);
        }
        Ok(true)
      }
      BfTreeDeleteResult::InvalidArguments => {
        Err(RangeIndexError::Internal("invalid arguments".to_string()))
      }
    }
  }

  /// 基于数量的流式范围扫描 (内部栈缓冲区零分配回调，透传切片引用，O(1) 空间复杂度)
  pub async fn range_index_scan_stream<F>(
    &self,
    key: &[u8],
    start: &[u8],
    count: usize,
    return_field: ScanReturnField,
    on_record: F,
  ) -> StdResult<usize, RangeIndexError>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    let (_, stub) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;

    if stub.storage_backend == StorageBackendType::Memory.to_u8() {
      return Err(RangeIndexError::MemoryModeNotSupported);
    }

    let (tree, _read_lock) = self.acquire_tree_read(key, &stub).await?;

    Ok(tree.scan_with_count_callback(start, count, return_field, on_record)?)
  }

  /// 扫描指定数量的记录 (1:1 对标 Garnet StorageSession.RangeIndexScan，基于零分配流式底层构建)
  pub async fn range_index_scan(
    &self,
    key: &[u8],
    start: &[u8],
    count: usize,
    return_field: ScanReturnField,
  ) -> StdResult<Vec<ScanRecord>, RangeIndexError> {
    let mut records = Vec::with_capacity(count.min(1024));
    self
      .range_index_scan_stream(key, start, count, return_field, |k, v| {
        records.push(ScanRecord {
          key: k.to_vec(),
          value: v.to_vec(),
        });
        true
      })
      .await?;
    Ok(records)
  }

  /// 闭区间流式范围扫描 (内部栈缓冲区零分配回调，透传切片引用，O(1) 空间复杂度)
  pub async fn range_index_range_stream<F>(
    &self,
    key: &[u8],
    start: &[u8],
    end: &[u8],
    return_field: ScanReturnField,
    on_record: F,
  ) -> StdResult<usize, RangeIndexError>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    let (_, stub) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;

    if stub.storage_backend == StorageBackendType::Memory.to_u8() {
      return Err(RangeIndexError::MemoryModeNotSupported);
    }

    let (tree, _read_lock) = self.acquire_tree_read(key, &stub).await?;

    Ok(tree.scan_with_end_key_callback(start, end, return_field, on_record)?)
  }

  /// 范围查询闭区间 [start, end] (1:1 对标 Garnet StorageSession.RangeIndexRange，基于零分配流式底层构建)
  pub async fn range_index_range(
    &self,
    key: &[u8],
    start: &[u8],
    end: &[u8],
    return_field: ScanReturnField,
  ) -> StdResult<Vec<ScanRecord>, RangeIndexError> {
    let mut records = Vec::with_capacity(32);
    self
      .range_index_range_stream(key, start, end, return_field, |k, v| {
        records.push(ScanRecord {
          key: k.to_vec(),
          value: v.to_vec(),
        });
        true
      })
      .await?;
    Ok(records)
  }

  /// 检查索引是否存在且为 RangeIndex (1:1 对标 Garnet StorageSession.RangeIndexExists)
  pub async fn range_index_exists(&self, key: &[u8]) -> Result<bool> {
    if let Some(meta) = self.load_meta(key).await?
      && meta.size > 0
      && meta.collection_type == CollectionType::RangeIndex
    {
      return Ok(true);
    }
    Ok(false)
  }

  /// 获取索引配置 (1:1 对标 Garnet StorageSession.RangeIndexConfig)
  pub async fn range_index_config(&self, key: &[u8]) -> StdResult<RangeIndexStub, RangeIndexError> {
    let (_, stub) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;
    Ok(stub)
  }

  /// 获取索引指标与运行状态 (1:1 对标 Garnet StorageSession.RangeIndexMetrics)
  pub async fn range_index_metrics(
    &self,
    key: &[u8],
  ) -> StdResult<(u64, bool, bool, bool), RangeIndexError> {
    let (_, stub) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;

    // is_live 只以注册表为准：stub.tree_handle 仅作标识且可能为陈旧值
    // (崩溃恢复改写前 / recover_in_place 换树后)，据此推断会误报
    let (tree_handle, is_live) = match self.store.range_index.get_tree(key) {
      Some(tree) => (tree.native_ptr(), true),
      None => (0, false),
    };

    Ok((tree_handle, is_live, stub.is_flushed(), stub.is_recovered()))
  }

  /// 发布迁移或分块重组后的 RangeIndex (1:1 对标 Garnet PublishMigratedIndex)
  ///
  /// 全程持该键条带互斥写锁 (对标 C# 调用方持 RangeIndex X 锁发布)：存在性判定 →
  /// 旧树排空释放 → 快照文件原子换入 → 恢复注册 → 存根落盘，构成对同键并发
  /// 发布/惰性恢复/删除原子的单一窗口。文件换入在 Unix 上经 `rename` 原子替换，
  /// 消除旧实现「先 remove 再 rename」的文件缺失间隙。
  #[allow(clippy::await_holding_lock)]
  pub async fn publish_migrated_range_index(
    &self,
    key: &[u8],
    stub_bytes: &[u8],
    temp_path: &Path,
    replace: bool,
  ) -> StdResult<(), RangeIndexError> {
    let key_hash = RangeIndexManager::key_hash_of(key);
    // compio 任务不迁移 (spawn 无 Send 约束)，跨 await 持条件带锁安全；
    // 锁内 await 仅依赖主存 I/O，不依赖同条带锁，无死锁
    let _xlock = self.store.range_index.locks().write(key_hash);

    if self.range_index_exists(key).await? && !replace {
      return Err(RangeIndexError::AlreadyExists);
    }

    // 文件换入 + 旧树释放 + 恢复 + 注册表发布（调用方持锁，manager 内部不再加锁）。
    // 快照解析属重操作，卸载阻塞线程；条带锁由本任务继续持有跨 await，
    // 发布原子性不受卸载影响
    let mgr = Arc::clone(&self.store.range_index);
    let pub_key = key.to_vec();
    let pub_src = temp_path.to_path_buf();
    let tree = range_index_blocking(move || {
      mgr.publish_tree_from_snapshot_locked(&pub_key, &pub_src, replace)
    })
    .await
    .map_err(|e| RangeIndexError::Internal(e.to_string()))?;

    let mut stub =
      RangeIndexStub::decode(stub_bytes).map_err(|e| RangeIndexError::Internal(e.to_string()))?;
    stub.tree_handle = tree.native_ptr();
    stub.reset_flags();

    // 存根落盘仍在同一锁窗口内：崩溃最坏结果为旧存根 + 新数据文件 (同键迁移
    // 语义下内容一致，惰性恢复可正常打开)，不存在「存根在而文件失」的不可恢复态
    let meta_k = self.session_meta_key(key);
    let key_id = self.store.next_key_id.fetch_add(1, Ordering::Relaxed);
    let meta = MetaValue::new(key_id, CollectionType::RangeIndex, 1, 1);

    let val = encode_meta_stub_record(&meta, &stub);

    self
      .upsert_raw(&meta_k, &val)
      .await
      .map_err(|e| RangeIndexError::Internal(e.to_string()))?;

    Ok(())
  }

  /// RENAME 迁移 RangeIndex（对标 C# RENAME 复制存根后索引持续可用语义）
  ///
  /// 本实现数据文件按"键名哈希前缀"命名，无法别名共享：在旧键条带写锁 +
  /// 防重入快照 claim 下将活动树 CPR 快照至新键数据文件路径（快照窗口内旧键
  /// 写入被条带锁阻塞，杜绝「快照后写入不进新副本」的丢失写；锁释放到调用方
  /// 删除旧键之间的残留窗口由调用方紧随的 delete 收口），再从新文件恢复独立
  /// 树实例并按新键注册到管理器，最后写入新键元数据记录。
  pub async fn rename_range_index(&self, old_key: &[u8], new_key: &[u8]) -> Result<()> {
    // 读取旧键存根（调用方已确认 RI 元记录存在且 size > 0；缺失或畸形则无索引可迁移，
    // 防御性直接返回，交由调用方常规清理旧键）
    let old_meta_k = self.session_meta_key(old_key);
    let Some(bytes) = self.read_raw(&old_meta_k).await? else {
      return Ok(());
    };
    if bytes.len() < META_VALUE_SIZE + wbftree::RANGE_INDEX_STUB_SIZE {
      return Ok(());
    }
    let mut stub = RangeIndexStub::decode(
      &bytes[META_VALUE_SIZE..META_VALUE_SIZE + wbftree::RANGE_INDEX_STUB_SIZE],
    )?;

    // 检查点屏障等待：避免与进行中的单树快照并发（与 acquire_tree_read 口径一致）
    while self.store.range_index.wait_for_tree_checkpoint(old_key)? {}

    // 获取在线树实例（未激活则按存根懒打开旧数据文件）
    let old_tree = match self.store.range_index.get_tree(old_key) {
      Some(t) => t,
      None => self.store.range_index.get_or_open_tree(old_key, &stub)?,
    };

    // 旧键条带写锁 + 防重入 claim 下整树快照写入新键数据文件路径
    // (整树 CPR 快照含 fsync 属重操作，锁由本任务持有跨 await，卸载阻塞线程)
    let new_path = self.store.range_index.data_file_path_for_key(new_key);
    if let Some(parent) = new_path.parent() {
      let _ = fs::create_dir_all(parent);
    }
    let _ = fs::remove_file(&new_path);
    {
      let old_hash = RangeIndexManager::key_hash_of(old_key);
      // 锁由本任务持有跨 await：span 锁不跨线程，compio 任务不迁移，安全
      // (整树 CPR 快照含 fsync 属重操作，已卸载阻塞线程)
      #[allow(clippy::await_holding_lock)]
      let _xlock = self.store.range_index.locks().write(old_hash);
      let mgr = Arc::clone(&self.store.range_index);
      let snap_key = old_key.to_vec();
      let snap_tree = Arc::clone(&old_tree);
      let snap_dest = new_path.clone();
      range_index_blocking(move || {
        mgr.snapshot_tree_to_path_locked(&snap_key, &snap_tree, &snap_dest)
      })
      .await?;
    }

    // 从新路径恢复独立树实例并按新键注册
    let backend = StorageBackendType::from_u8(stub.storage_backend);
    let new_tree = Arc::new(BfTreeService::recover_from_cpr_snapshot(
      &new_path, true, backend,
    )?);
    stub.tree_handle = new_tree.native_ptr();
    stub.reset_flags();
    self.store.range_index.register_tree(new_key, new_tree);

    // 写入新键元数据记录（Meta + 新存根，定长纯栈编码）
    let new_meta_k = self.session_meta_key(new_key);
    let key_id = self.store.next_key_id.fetch_add(1, Ordering::Relaxed);
    let meta = MetaValue::new(key_id, CollectionType::RangeIndex, 1, 1);
    let val = encode_meta_stub_record(&meta, &stub);
    self.upsert_raw(&new_meta_k, &val).await?;
    Ok(())
  }
}

/// 栈上编码 MetaValue 与 RangeIndexStub，消除堆内存分配 (零拷贝/零堆分配)
#[inline]
fn encode_meta_stub_record(
  meta: &MetaValue,
  stub: &RangeIndexStub,
) -> [u8; META_VALUE_SIZE + wbftree::RANGE_INDEX_STUB_SIZE] {
  let mut val = [0u8; META_VALUE_SIZE + wbftree::RANGE_INDEX_STUB_SIZE];
  val[..META_VALUE_SIZE].copy_from_slice(&meta.to_bytes());
  val[META_VALUE_SIZE..].copy_from_slice(&stub.encode());
  val
}
