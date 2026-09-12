use std::{
  fs,
  io::{Error as IoError, ErrorKind},
  ops::Deref,
  path::Path,
  result::Result as StdResult,
  sync::{Arc, atomic::Ordering},
};

use compio::runtime::spawn_blocking;
use thiserror::Error as ThisError;
use wbftree::{
  BfTreeService, RANGE_INDEX_STUB_SIZE, RangeIndexManager, RangeIndexStub, ScanRecord,
  ScanReturnField, StorageBackend, StorageBackendType, TreeTuning,
};
use wcol::RiTreeOps;
use wdev::Device;
use wrecord::{RecordHeader, fast_key_eq};
use wval::{CollectionType, META_VALUE_SIZE, MetaValue, StorageEncoding};

use crate::{
  error::{Error, Result},
  read_cache::is_read_cache_addr,
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

impl From<IoError> for RangeIndexError {
  fn from(err: IoError) -> Self {
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

impl From<wcol::CollectionError> for RangeIndexError {
  fn from(err: wcol::CollectionError) -> Self {
    Self::Internal(err.to_string())
  }
}

pub struct TreeReadGuard<'a> {
  tree: Arc<BfTreeService>,
  _guard: parking_lot::RwLockReadGuard<'a, ()>,
}

impl<'a> TreeReadGuard<'a> {
  #[inline]
  pub(crate) fn new(tree: Arc<BfTreeService>, guard: parking_lot::RwLockReadGuard<'a, ()>) -> Self {
    Self {
      tree,
      _guard: guard,
    }
  }

  #[inline]
  pub fn tree(&self) -> &Arc<BfTreeService> {
    &self.tree
  }
}

impl Deref for TreeReadGuard<'_> {
  type Target = BfTreeService;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.tree
  }
}

#[inline]
fn rebind_stub(stub: &mut RangeIndexStub, tree: &BfTreeService) {
  stub.tree_handle = tree.native_ptr();
  stub.reset_flags();
}

/// 创建时未指定的调优默认值 (1:1 对标 Garnet RespServerSessionRangeIndex RI.CREATE 默认：
/// 16MiB 缓存 / min 64 / max 1024 / max key 128，创建时固化进存根)
const DEFAULT_CACHE_SIZE: usize = 16 * 1024 * 1024;
const DEFAULT_MIN_RECORD_SIZE: usize = 64;
const DEFAULT_MAX_RECORD_SIZE: usize = 1024;
const DEFAULT_MAX_KEY_LEN: usize = 128;

/// 0 值取默认的微小解析器 (创建时把解析后的实际值固化进存根，绝不为 0)
#[inline]
const fn nz_or(v: usize, d: usize) -> usize {
  if v > 0 { v } else { d }
}

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
) -> StdResult<T, RangeIndexError> {
  spawn_blocking(op)
    .await
    .map_err(|e| RangeIndexError::Internal(format!("RangeIndex 阻塞任务异常退出: {e}")))
}

/// 范围索引运行状态与统计指标 (1:1 对标 Garnet RangeIndexMetrics 与 RI.METRICS RESP 响应字段)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeIndexMetrics {
  /// 对齐 C# RI.METRICS 协议字段；仅本进程内有句柄语义，跨进程仅为不透明标识
  pub tree_handle: u64,
  /// 索引是否处于活跃状态 (以注册表在线状态为准)
  pub is_live: bool,
  /// 存根是否已被刷盘标记
  pub is_flushed: bool,
  /// 存根是否由检查点快照恢复
  pub is_recovered: bool,
}

impl<D: Device> StoreSession<D> {
  /// 创建新的 RangeIndex 索引 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexCreate)
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

    // 2. 解析调优参数：0 值取 Garnet RI.CREATE 同款默认并在创建时固化进存根
    //    (对标 C# 把解析后的实际值写入存根——后续长度校验与惰性恢复重建都拿
    //    真实值，绝不为 0；否则全零存根会让 set 的长度校验把一切写入拒之门外)
    let mut create_tuning = TreeTuning {
      cache_size: nz_or(tuning.cache_size, DEFAULT_CACHE_SIZE),
      min_record_size: nz_or(tuning.min_record_size, DEFAULT_MIN_RECORD_SIZE),
      max_record_size: nz_or(tuning.max_record_size, DEFAULT_MAX_RECORD_SIZE),
      max_key_len: nz_or(tuning.max_key_len, DEFAULT_MAX_KEY_LEN),
      leaf_page_size: tuning.leaf_page_size,
    };
    RangeIndexManager::resolve_tuning(&mut create_tuning);

    // 3. 在底层 RangeIndexManager 中创建并托管 BfTree 实例
    //    (数据文件创建 + 环形缓冲分配属重操作，卸载阻塞线程保护 compio 核)
    let mgr = Arc::clone(&self.store.range_index);
    let create_key = key.to_vec();
    let create_backend = storage_backend.clone();
    let tree =
      range_index_blocking(move || mgr.create_bftree(&create_key, create_backend, create_tuning))
        .await?
        .map_err(RangeIndexError::from)?;

    // 4. 构建定长 35 字节 RangeIndexStub 并持久化入主日志库
    let stub =
      RangeIndexStub::from_tuning(tree.native_ptr(), &create_tuning, storage_backend.clone());

    let meta_k = self.session_meta_key(key);
    let key_id = self.store.next_key_id.fetch_add(1, Ordering::Relaxed);
    let mut meta = MetaValue::new(key_id, CollectionType::RangeIndex, 1, 0);
    meta.set_encoding(StorageEncoding::FlattenedTree);

    let val = encode_meta_stub_record(&meta, &stub);

    if let Err(e) = self.upsert_raw(&meta_k, &val).await {
      // 事务回滚：清理此前在内存中注册及磁盘生成的孤儿文件
      // (尽力而为：主错误已优先上抛，回滚自身的屏障超时属进程级故障，不再覆盖)
      let _ = self.store.range_index.delete_index(key);
      return Err(RangeIndexError::Internal(e.to_string()));
    }

    if !self.store.aof_listeners_paused.load(Ordering::Relaxed)
      && let Some(listener) = self.store.range_create_listener()
    {
      listener(key, &storage_backend, create_tuning);
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
    if bytes.len() < META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE {
      return Ok(None);
    }
    let stub =
      RangeIndexStub::decode(&bytes[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE])
        .map_err(|e| RangeIndexError::Internal(e.to_string()))?;
    Ok(Some((meta, stub)))
  }

  /// 获取在线 BfTree 实例及其条带共享读锁 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:ReadRangeIndex 与 ReadRangeIndexLock)
  ///
  /// 先在无锁/共享锁状态下快速命中（稳态 O(1)：一次 volatile 读 + 一次注册表
  /// 查找），若树未激活则释放读锁后调用 get_or_open_tree（文件 I/O + 快照解析
  /// 属重操作，卸载 compio 阻塞线程，避免慢恢复停摆整核），激活完成后执行
  /// RIRESTORE 存根回写（新句柄写回 + 清 Recovered 位，对标 C# RestoreTree 尾段
  /// 的 RIRESTORE RMW），随后重新获取共享读锁。整个数据读取/修改操作在其 RAII
  /// 读锁保护下安全执行。
  pub async fn acquire_tree_read(
    &self,
    key: &[u8],
    stub: &RangeIndexStub,
  ) -> StdResult<TreeReadGuard<'_>, RangeIndexError> {
    let key_hash = RangeIndexManager::key_hash_of(key);
    loop {
      match self.store.range_index.wait_for_tree_checkpoint(key) {
        Ok(true) => continue,
        Ok(false) => {}
        Err(e) => return Err(RangeIndexError::Internal(e.to_string())),
      }
      let read_lock = self.store.range_index.locks().read(key_hash);
      if let Some(tree) = self.store.range_index.get_tree(key) {
        return Ok(TreeReadGuard::new(tree, read_lock));
      }
      drop(read_lock);

      // 惰性恢复慢路径：卸载阻塞线程 (条带锁在 manager 内部自取自放，不跨线程边界)
      let mgr = Arc::clone(&self.store.range_index);
      let restore_key = key.to_vec();
      let restore_stub = *stub;
      let tree = range_index_blocking(move || mgr.get_or_open_tree(&restore_key, &restore_stub))
        .await?
        .map_err(|e| RangeIndexError::Internal(e.to_string()))?;

      // RIRESTORE 存根回写：不持任何条带锁 (对标 C# RestoreTree 释放 X 锁后再发
      // RIRESTORE RMW 的分裂设计——锁内发 RMW 会与延迟 OnFlush 自死锁)
      self.restore_range_index_stub(key, &tree).await?;
    }
  }

  /// 惰性激活后的存根回写 (1:1 对标 libs/server/Storage/Functions/MainStore/RMWMethods.cs:954-957
  /// 与 1480-1493 的 RIRESTORE RMW + libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:184-192
  /// RecreateIndex)
  ///
  /// C# 在 RestoreTree 恢复并注册树实例后，经 RIRESTORE RMW 把恢复树的新句柄写回
  /// 存根并清除 IsRecovered 位，本方法承接同一语义：
  /// - 快路径为可变区原位读-改-写 (InPlaceUpdater 等价，页写锁内读-验-改，零追加零 CAS)；
  /// - 只读/磁盘区降级候选链定位 + 补丁追加 + CAS 挂载 (CopyUpdater 等价)；
  /// - 墓碑 / 非 RangeIndex 元记录零写跳过 (RIRESTORE.NeedInitialUpdate=false：键已被
  ///   并发删除时 RMW 返回 NOTFOUND，绝不复活存根)；
  /// - 内部维护写旁路写监听 (对标 C# RIRESTORE 不入 AOF——瞬态句柄非用户写效果，
  ///   追加走 append_record_compacted，原位直调 hlog 无监听通知)；
  /// - 幂等：句柄已绑定当前树且 Recovered 位已清时零写。
  ///
  /// 清 Recovered 位的意义 (对标 C# RecreateIndex 注释)：使后续淘汰重开选择反映
  /// 激活后写入的刷盘快照而非过期的检查点快照——未清位时 get_or_open_tree 的恢复
  /// 源选择会永久绕过刷盘文件 (见 wbftree lifecycle 的 IsRecovered 分流)。
  ///
  /// 对标差异：C# 存根句柄是热路径直接调用的原生指针，重启后由 OnDiskRead 清零
  /// (InvalidateStub)；本实现路由一律走注册表、句柄仅作标识，故以「句柄 ≠ 当前树
  /// native_ptr」的补丁条件等价覆盖跨重启陈旧句柄的清理。C# 在 RIRESTORE 失败时把
  /// 整个 RestoreTree 视作失败 (客户端见 NOTFOUND)；本实现显式上抛 Internal——激活
  /// 已成功而回写失败属存储 I/O 故障，静默吞掉会掩盖持久化态与运行态的偏离。
  async fn restore_range_index_stub(
    &self,
    key: &[u8],
    tree: &BfTreeService,
  ) -> StdResult<(), RangeIndexError> {
    let meta_k = self.session_meta_key(key);
    let native = tree.native_ptr();

    // 1. InPlaceUpdater 等价：可变区页写锁内读-验-改。闭包恒 Some——None 仅表示
    //    记录不在可变区 (含 RC 链头 / 墓碑 / Tag 碰撞)，交由候选链慢路径定位；
    //    闭包内已治愈或非 RI 元记录时零写即闭环
    {
      let _guard = self.participant.enter();
      if let Some(addr) = self.store.index.find_tag(&meta_k)
        && !is_read_cache_addr(addr)
      {
        let closed = self
          .store
          .hlog
          .try_modify_record_in_place(addr, &meta_k, |val| {
            if let Some((healed, len)) = recreated_stub_record(val, native) {
              val[..len].copy_from_slice(&healed[..len]);
            }
            Some(())
          })
          .map_err(|e| RangeIndexError::Internal(e.to_string()))?;
        if closed.is_some() {
          return Ok(());
        }
      }
    }

    // 2. CopyUpdater 等价：候选链定位当前记录 (含磁盘冷区) → 补丁追加 + CAS 挂载。
    //    纪元纪律对齐 delete_raw_disk_slow：索引探测与挂载持短守卫，磁盘读免守卫
    let begin_addr = self.store.begin_address();
    let addrs = {
      let _guard = self.participant.enter();
      self.store.index.lookup_candidates(&meta_k)
    };
    for cand in addrs {
      let main_head = {
        let _guard = self.participant.enter();
        self.store.read_cache.skip_read_cache(cand)
      };
      if main_head == 0 {
        continue;
      }
      let mut cur = main_head;
      while cur >= begin_addr {
        let record = if self.store.hlog.is_on_disk(cur) {
          self.store.hlog.read_disk_record(cur).await
        } else {
          let _guard = self.participant.enter();
          self.store.hlog.read_record(cur).await
        };
        let Ok(record) = record else { break };
        if !record
          .key()
          .is_ok_and(|rec_key| fast_key_eq(rec_key, &meta_k))
        {
          // Tag 碰撞：解析记录头提取前驱地址，沿磁盘链回溯
          cur = RecordHeader::from_slice(record.as_slice())
            .map(|h| h.address())
            .unwrap_or(0);
          continue;
        }
        // 命中当前记录：墓碑 / 非 RI 元记录 / 已治愈均零写闭环，绝不追加复活
        let Some((healed, len)) = record
          .value()
          .ok()
          .and_then(|val| recreated_stub_record(val, native))
        else {
          return Ok(());
        };
        let new_addr = self
          .append_record_compacted(&meta_k, &healed[..len], main_head, false)
          .await
          .map_err(|e| RangeIndexError::Internal(e.to_string()))?;
        let _guard = self.participant.enter();
        // CAS 失败 (并发写移动链头)：追加帧沦为链上垃圾由 GC 回收；治愈幂等，
        // 下次激活重试 (对标 C# CopyUpdater CAS 败者不重试同帧)
        let _ = self.store.index.update_address(&meta_k, cand, new_addr);
        return Ok(());
      }
    }
    Ok(())
  }

  /// 设置字段值 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexSet)
  pub async fn range_index_set(
    &self,
    key: &[u8],
    field: &[u8],
    value: &[u8],
  ) -> StdResult<(), RangeIndexError> {
    let (mut meta, stub) = self
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

    let tree = self.acquire_tree_read(key, &stub).await?;

    match tree.ri_set(field, value) {
      Ok(is_new) => {
        if is_new {
          meta.inc_size(1);
          self
            .save_bftree_meta_stub(key, &meta, &stub)
            .await
            .map_err(|e| RangeIndexError::Internal(e.to_string()))?;
        }
        if !self.store.aof_listeners_paused.load(Ordering::Relaxed)
          && let Some(listener) = self.store.range_listener()
        {
          listener(key, field, value, false);
        }
        Ok(())
      }
      Err(wcol::CollectionError::KeyTooLong) => Err(RangeIndexError::InvalidKV {
        min_record_size: stub.min_record_size,
        max_record_size: stub.max_record_size,
        max_key_len: stub.max_key_len,
        total_len,
        key_len: field.len(),
      }),
      Err(e) => Err(RangeIndexError::Internal(e.to_string())),
    }
  }

  /// 读取字段值 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexGet)
  pub async fn range_index_get(
    &self,
    key: &[u8],
    field: &[u8],
  ) -> StdResult<Option<Vec<u8>>, RangeIndexError> {
    let (_, stub) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;

    let tree = self.acquire_tree_read(key, &stub).await?;

    tree
      .ri_get(field)
      .map_err(|e| RangeIndexError::Internal(e.to_string()))
  }

  /// 删除字段 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexDel)
  pub async fn range_index_del(
    &self,
    key: &[u8],
    field: &[u8],
  ) -> StdResult<bool, RangeIndexError> {
    let (mut meta, stub) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;

    let tree = self.acquire_tree_read(key, &stub).await?;

    let removed = tree.ri_del(field)?;
    if removed {
      meta.dec_size(1);
      self
        .save_bftree_meta_stub(key, &meta, &stub)
        .await
        .map_err(|e| RangeIndexError::Internal(e.to_string()))?;
    }
    if !self.store.aof_listeners_paused.load(Ordering::Relaxed)
      && let Some(listener) = self.store.range_listener()
    {
      listener(key, field, &[], true);
    }
    Ok(true)
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

    let tree = self.acquire_tree_read(key, &stub).await?;

    Ok(tree.ri_scan_with_field(start, count, return_field, on_record)?)
  }

  /// 扫描指定数量的记录 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexScan，基于零分配流式底层构建)
  pub async fn range_index_scan(
    &self,
    key: &[u8],
    start: &[u8],
    count: usize,
    return_field: ScanReturnField,
  ) -> StdResult<Vec<ScanRecord>, RangeIndexError> {
    let mut records = Vec::with_capacity(count.min(1024));
    self
      .range_index_scan_stream(
        key,
        start,
        count,
        return_field,
        ScanRecord::sink(&mut records),
      )
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

    let tree = self.acquire_tree_read(key, &stub).await?;

    Ok(tree.ri_range_with_field(start, end, return_field, on_record)?)
  }

  /// 范围查询闭区间 [start, end] (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexRange，基于零分配流式底层构建)
  pub async fn range_index_range(
    &self,
    key: &[u8],
    start: &[u8],
    end: &[u8],
    return_field: ScanReturnField,
  ) -> StdResult<Vec<ScanRecord>, RangeIndexError> {
    let mut records = Vec::with_capacity(32);
    self
      .range_index_range_stream(
        key,
        start,
        end,
        return_field,
        ScanRecord::sink(&mut records),
      )
      .await?;
    Ok(records)
  }

  /// 获取 RangeIndex 元素总数
  ///
  /// O(1) 元数据直读：与 HLEN / SCARD / ZCARD 一致，直接读取主存 MetaValue.size，零 IO、零扫树。
  pub async fn range_index_len(&self, key: &[u8]) -> StdResult<usize, RangeIndexError> {
    let (meta, _) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;

    Ok(meta.size as usize)
  }

  /// 检查索引是否存在且为 RangeIndex (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexExists)
  pub async fn range_index_exists(&self, key: &[u8]) -> Result<bool> {
    if let Some(meta) = self.load_meta(key).await?
      && meta.collection_type == CollectionType::RangeIndex
    {
      return Ok(true);
    }
    Ok(false)
  }

  /// 获取索引配置 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexConfig)
  pub async fn range_index_config(&self, key: &[u8]) -> StdResult<RangeIndexStub, RangeIndexError> {
    let (_, stub) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;
    Ok(stub)
  }

  /// 获取索引指标与运行状态 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexMetrics)
  pub async fn range_index_metrics(
    &self,
    key: &[u8],
  ) -> StdResult<RangeIndexMetrics, RangeIndexError> {
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

    Ok(RangeIndexMetrics {
      tree_handle,
      is_live,
      is_flushed: stub.is_flushed(),
      is_recovered: stub.is_recovered(),
    })
  }

  /// 发布迁移或分块重组后的 RangeIndex 存储底座（对应 PublishMigratedIndex 的文件换入与存根落盘存储逻辑）
  ///
  /// 锁纪律：条带写锁在阻塞任务内部获取与释放，严禁持同步锁跨 await（与 rename_range_index 一致）。
  /// 存在性判定 → 快照文件原子换入与树恢复注册（阻塞任务持条带写锁原子完成）→ 存根落盘。
  pub async fn publish_migrated_range_index(
    &self,
    key: &[u8],
    stub_bytes: &[u8],
    temp_path: &Path,
    replace: bool,
  ) -> StdResult<(), RangeIndexError> {
    if self.range_index_exists(key).await? && !replace {
      return Err(RangeIndexError::AlreadyExists);
    }

    // 文件换入 + 旧树释放 + 恢复 + 注册表发布。
    // 快照解析属重操作，卸载阻塞线程，并在阻塞任务内部获取条带写锁保证发布原子性。
    let mgr = Arc::clone(&self.store.range_index);
    let pub_key = key.to_vec();
    let pub_src = temp_path.to_path_buf();
    let tree = range_index_blocking(move || {
      let key_hash = RangeIndexManager::key_hash_of(&pub_key);
      let _xlock = mgr.locks().write(key_hash);
      mgr.publish_tree_from_snapshot_locked(&pub_key, &pub_src, replace)
    })
    .await?
    .map_err(|e| RangeIndexError::Internal(e.to_string()))?;

    let mut stub =
      RangeIndexStub::decode(stub_bytes).map_err(|e| RangeIndexError::Internal(e.to_string()))?;
    rebind_stub(&mut stub, &tree);

    // 存根落盘（条带锁窗口之外：锁已随阻塞任务收束，严禁持同步锁跨 await）。
    // 崩溃最坏结果为旧存根 + 新数据文件（同键迁移语义下内容一致，惰性恢复可正常
    // 打开），不存在「存根在而文件失」的不可恢复态
    let meta_k = self.session_meta_key(key);
    let key_id = self.store.next_key_id.fetch_add(1, Ordering::Relaxed);
    let count = tree.ri_len().unwrap_or(0) as u64;
    let mut meta = MetaValue::new(key_id, CollectionType::RangeIndex, 1, count);
    meta.set_encoding(StorageEncoding::FlattenedTree);

    let val = encode_meta_stub_record(&meta, &stub);

    self
      .upsert_raw(&meta_k, &val)
      .await
      .map_err(|e| RangeIndexError::Internal(e.to_string()))?;

    // 写后持锁复核（补偿存根落盘逃逸条带锁窗口与 C# PublishMigratedIndex 全程持
    // RangeIndex X 锁的原子发布契约差距）：换树注册 → 存根落盘的间隙内，同键
    // DELETE 可插入（锁内摘除注册表条目 + 删数据文件 + 元记录墓碑），本条后写的
    // 存根将复活指向已删文件的幻影键。复核发现条目已被并发摘除即回滚墓碑本条
    // 元记录；条目仍在但树已被并发再发布换替时不回滚——文件谱系已归新发布所有，
    // 其自身存根落盘收敛最终态（is_live 只以注册表为准，陈旧 stub.tree_handle
    // 运行时无害，见 range_index_metrics 注释）
    let mgr2 = Arc::clone(&self.store.range_index);
    let chk_key = key.to_vec();
    let entry_alive = range_index_blocking(move || {
      let _xlock = mgr2.locks().write(RangeIndexManager::key_hash_of(&chk_key));
      mgr2
        .live_indexes()
        .pin()
        .get(&RangeIndexManager::key_id_of(&chk_key))
        .is_some()
    })
    .await
    .map_err(|e| RangeIndexError::Internal(e.to_string()))?;
    if !entry_alive {
      // 盲回滚会误杀并发 re-publish（replace）刚落盘的新存根（不同 key_id，其自身
      // 存根落盘收敛最终态）：仅当 meta_k 最新记录仍是本条写入（key_id 相符）才写
      // 回滚墓碑——对标 C# PublishMigratedIndex 的 RICREATE RMW 以 status.Record.Created
      // 区分「新建成功 / 竞态被覆盖」并据此决定注册与否
      // (libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs:216-227)。
      // 最新记录已被并发墓碑覆盖或已被新存根换替时，本条写入已不可见，无需回滚。
      let rollable = match self.read_raw(&meta_k).await {
        Ok(Some(bytes)) => {
          bytes.len() >= META_VALUE_SIZE
            && MetaValue::from_slice(&bytes[..META_VALUE_SIZE]).is_ok_and(|m| m.key_id == key_id)
        }
        Ok(None) => false,
        Err(e) => return Err(RangeIndexError::Internal(e.to_string())),
      };
      if rollable {
        self
          .delete_raw(&meta_k)
          .await
          .map_err(|e| RangeIndexError::Internal(format!("发布回滚失败（并发删除竞争中）: {e}")))?;
        return Err(RangeIndexError::Internal(
          "发布树在存根落盘窗口内被并发删除，已回滚元记录".to_string(),
        ));
      }
      return Err(RangeIndexError::Internal(
        "发布树在存根落盘窗口内被并发删除，元记录已被并发写覆盖，无需回滚".to_string(),
      ));
    }

    Ok(())
  }

  /// RENAME 迁移 RangeIndex（对标 C# RENAME 复制存根后索引持续可用语义）
  ///
  /// 本实现数据文件按"键名哈希前缀"命名，无法别名共享：在旧键条带写锁 +
  /// 防重入快照 claim 下将活动树 CPR 快照至新键数据文件路径（快照窗口内旧键
  /// 写入被条带锁阻塞，杜绝「快照后写入不进新副本」的丢失写；锁释放到调用方
  /// 删除旧键之间的残留窗口由调用方紧随的 delete 收口），再从新文件恢复独立
  /// 树实例并按新键注册到管理器，最后写入新键元数据记录。
  ///
  /// 锁纪律：旧键条带写锁在阻塞任务内部获取与释放，严禁持同步锁跨 await
  pub async fn rename_range_index(&self, old_key: &[u8], new_key: &[u8]) -> Result<()> {
    // 读取旧键存根（调用方已确认 RI 元记录存在且 size > 0；缺失或畸形则无索引可迁移，
    // 防御性直接返回，交由调用方常规清理旧键）
    let old_meta_k = self.session_meta_key(old_key);
    let Some(bytes) = self.read_raw(&old_meta_k).await? else {
      return Ok(());
    };
    if bytes.len() < META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE {
      return Ok(());
    }
    let mut stub =
      RangeIndexStub::decode(&bytes[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE])?;
    let old_meta = MetaValue::from_slice(&bytes[..META_VALUE_SIZE])?;

    // 检查点屏障等待：避免与进行中的单树快照并发（与 acquire_tree_read 口径一致）
    while self.store.range_index.wait_for_tree_checkpoint(old_key)? {}

    // 旧树惰性恢复 → 新键数据文件预置 → 旧键条带写锁 + 防重入 claim 下整树快照
    // → 新树独立恢复，四步串行合并进单一阻塞任务：整树快照含 fsync、恢复含
    // 快照解析 + 环形缓冲分配，均属百毫秒级重操作，一律卸载阻塞线程保护 compio 核
    // (锁纪律：get_or_open_tree 的条带写锁自取自放后，快照段再自取旧键写锁——
    // 两段先后串行不嵌套；快照持锁窗口阻塞同条带旧键写入，杜绝「快照后写入
    // 不进新副本」的丢失写)
    let mgr = Arc::clone(&self.store.range_index);
    let old_key_owned = old_key.to_vec();
    let new_key_owned = new_key.to_vec();
    let restore_stub = stub;
    let new_tree =
      range_index_blocking(move || -> StdResult<Arc<BfTreeService>, RangeIndexError> {
        let old_tree = match mgr.get_tree(&old_key_owned) {
          Some(t) => t,
          None => mgr.get_or_open_tree(&old_key_owned, &restore_stub)?,
        };
        let new_path = mgr.data_file_path_for_key(&new_key_owned);
        if let Some(parent) = new_path.parent() {
          fs::create_dir_all(parent)?;
        }
        if let Err(e) = fs::remove_file(&new_path)
          && e.kind() != ErrorKind::NotFound
        {
          return Err(e.into());
        }
        let old_hash = RangeIndexManager::key_hash_of(&old_key_owned);
        let _xlock = mgr.locks().write(old_hash);
        mgr.snapshot_tree_to_path_locked(&old_key_owned, &old_tree, &new_path)?;

        let backend = StorageBackendType::from_u8(restore_stub.storage_backend);
        BfTreeService::recover_from_cpr_snapshot(&new_path, true, backend)
          .map(Arc::new)
          .map_err(RangeIndexError::from)
      })
      .await??;

    rebind_stub(&mut stub, &new_tree);
    self.store.range_index.register_tree(new_key, new_tree);

    // 写入新键元数据记录（Meta + 新存根，定长纯栈编码）
    let new_meta_k = self.session_meta_key(new_key);
    let key_id = self.store.next_key_id.fetch_add(1, Ordering::Relaxed);
    let mut meta = MetaValue::new(key_id, CollectionType::RangeIndex, 1, old_meta.size);
    meta.set_encoding(StorageEncoding::FlattenedTree);
    let val = encode_meta_stub_record(&meta, &stub);
    self.upsert_raw(&new_meta_k, &val).await?;
    Ok(())
  }
}

/// RIRESTORE 存根补丁内核：校验记录为存活 RangeIndex 元记录后，重绑句柄并清
/// 恢复位 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:184-192
/// RecreateIndex——仅改 TreeHandle 与 IsRecovered，其余字节原样保留)
///
/// 返回 None = 零写跳过：非目标记录 (定长不足 / 非 RangeIndex / size=0 墓碑语义)
/// 或已治愈 (句柄已绑定当前树且恢复位已清)
#[inline]
fn recreated_stub_record(val: &[u8], new_tree_handle: u64) -> Option<([u8; 128], usize)> {
  if val.len() < META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE || val.len() > 128 {
    return None;
  }
  let meta = MetaValue::from_slice(&val[..META_VALUE_SIZE]).ok()?;
  if meta.collection_type != CollectionType::RangeIndex
    && (meta.encoding() != StorageEncoding::FlattenedTree || meta.size == 0)
  {
    return None;
  }
  let mut stub =
    RangeIndexStub::decode(&val[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE]).ok()?;
  if stub.tree_handle == new_tree_handle && !stub.is_recovered() {
    return None;
  }
  stub.recreate_index(new_tree_handle);
  let mut out = [0u8; 128];
  let val_len = val.len();
  out[..val_len].copy_from_slice(val);
  out[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE].copy_from_slice(&stub.encode());
  Some((out, val_len))
}

/// 栈上编码 MetaValue 与 RangeIndexStub，消除堆内存分配 (零拷贝/零堆分配)
///
/// wkv 检查点恢复路径 (checkpoint.rs) 的存根自愈回写共用此单一编码实现
#[inline]
pub(crate) fn encode_meta_stub_record(
  meta: &MetaValue,
  stub: &RangeIndexStub,
) -> [u8; META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE] {
  let mut val = [0u8; META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE];
  val[..META_VALUE_SIZE].copy_from_slice(&meta.to_bytes());
  val[META_VALUE_SIZE..].copy_from_slice(&stub.encode());
  val
}
