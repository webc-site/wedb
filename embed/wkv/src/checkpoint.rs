//! RangeIndex 检查点快照与故障恢复模块 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotAllTreesForCheckpoint 与 RebuildFromSnapshotIfPending)

use std::{
  error::Error as StdError,
  path::{Path, PathBuf},
  sync::{Arc, atomic::Ordering},
};

use itoa::Buffer;
use wbftree::{
  RANGE_INDEX_STUB_SIZE, RangeIndexManager, RangeIndexStub, StorageBackendType, TreeEntry,
};
use wdev::Device;
use windex::{HashBucket, HashBucketEntry};
use wval::{CollectionType, META_VALUE_SIZE, MetaValue, NamespaceDbCodec};

use crate::{
  config::StoreConfig,
  error::{Error, Result},
  read_cache::is_read_cache_addr,
  store::{KEY_ID_ASSIGN_MARGIN, WedbStore},
};

/// 共享 BfTree 快照在 checkpoint token 目录下的子目录名
pub const BFTREE_SNAPSHOT_DIR: &str = "bftree";

/// 共享 BfTree 快照文件名
pub const BFTREE_SNAPSHOT_FILE: &str = "shared.bftree";

/// 拼接共享 BfTree 的 token 快照路径 `{checkpoint_dir}/{token}/bftree/shared.bftree`
fn bftree_snapshot_path(checkpoint_dir: &Path, token: u128) -> PathBuf {
  let mut buf = Buffer::new();
  checkpoint_dir
    .join(buf.format(token))
    .join(BFTREE_SNAPSHOT_DIR)
    .join(BFTREE_SNAPSHOT_FILE)
}

/// wkv::Error → wcpr::Error 宿主端口映射（零字符串化，全链路类型化）
///
/// wcpr 可表达的基础设施错误（Device/Index/Hlog/Epoch/Io/Cpr）逐变体透明转发；
/// wcpr 不依赖 wbftree 与本 crate（依赖无环），BfTree 快照、RangeIndex 恢复、
/// 配置校验等宿主专属错误装箱经 `wcpr::Error::Host` 透明透传，Display 与
/// `source()` 错误链完整保留
fn cpr_err(e: Error) -> wcpr::Error {
  match e {
    Error::Device(e) => e.into(),
    Error::Epoch(e) => e.into(),
    Error::HLog(e) => e.into(),
    Error::Index(e) => e.into(),
    Error::Io(e) => e.into(),
    Error::Cpr(e) => e,
    other => (Box::new(other) as Box<dyn StdError + Send + Sync>).into(),
  }
}

impl<D: Device> WedbStore<D> {
  /// 遍历并为所有在线与待激活的 RangeIndex 执行 CPR 检查点快照落盘
  /// (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotAllTreesForCheckpoint)
  pub fn take_range_index_checkpoints(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    token: u128,
  ) -> Result<usize> {
    let mut buf = Buffer::new();
    let token_str = buf.format(token);
    Ok(
      self
        .range_index
        .snapshot_all_trees_to_dir(checkpoint_dir.as_ref(), token_str)?,
    )
  }

  /// 从 Checkpoint 快照还原 RangeIndex 并恢复注册存根
  /// (1:1 对标 Garnet OnRecoverySnapshotRead / MarkRecoveredFromCheckpoint / RebuildFromSnapshotIfPending)
  ///
  /// 与 C# 一致，恢复期只做「存根自愈 + 文件预置 + pending 注册」，**绝不急切打开
  /// 引擎实例**——每棵树的环形缓冲区在首次访问时才由 get_or_open_tree 惰性分配，
  /// RI 键规模大时启动内存与耗时保持 O(1)/树。检查点目录内的树由
  /// recover_all_trees_from_dir 预置；检查点之后新建的树（存根在日志、数据文件
  /// 为上一进程残留工作文件）由日志扫描注册 pending，数据文件缺失时同样注册，
  /// 交由 WAL 重放 (ri_create) 重建。
  pub async fn recover_range_indexes(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    token: u128,
  ) -> Result<usize> {
    let mut buf = Buffer::new();
    let token_str = buf.format(token);
    let dir = checkpoint_dir.as_ref();

    // 1. 先从目标检查点目录预置所有 .bftree 快照物理文件并注册 pending 条目
    let mut count = self
      .range_index
      .recover_all_trees_from_dir(dir, token_str)?;

    // 2. 遍历 HashIndex 与 HybridLog 中所有的 RangeIndex 元数据存根
    // 识别其存根，标记 mark_recovered_from_checkpoint() 并在 RangeIndexManager 中恢复注册
    let begin_addr = self.begin_address();
    let participant = self.epoch.register()?;
    let _guard = participant.enter();

    for bucket in self.index.buckets.iter() {
      let mut curr_bucket = bucket;
      loop {
        for item in curr_bucket.entries.iter().take(HashBucket::DATA_ENTRIES) {
          let raw = item.load(Ordering::Acquire);
          if raw == 0 {
            continue;
          }
          let entry = HashBucketEntry::from_raw(raw);
          if entry.is_tentative() {
            continue;
          }
          let mut addr = entry.address();
          if is_read_cache_addr(addr) {
            addr = self.read_cache.skip_read_cache(addr);
          }
          if addr < begin_addr {
            continue;
          }

          if let Ok(record) = self.hlog.read_record(addr).await
            && let Ok(key) = record.key()
            && let Some(user_key) = NamespaceDbCodec::decode_meta_user_key(key)
            && let Ok(val) = record.value()
            && val.len() >= META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE
            && let Ok(meta) = MetaValue::from_slice(val)
            && meta.collection_type == CollectionType::RangeIndex
          {
            let stub_slice = &val[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE];
            if let Ok(mut stub) = RangeIndexStub::decode(stub_slice) {
              // 标记已从检查点恢复 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:MarkRecoveredFromCheckpoint)
              stub.mark_recovered_from_checkpoint();

              // 更新记录中的存根并落盘 (定长 51 字节，纯栈分配零堆开销)
              let mut new_val = [0u8; META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE];
              new_val[..META_VALUE_SIZE].copy_from_slice(&val[..META_VALUE_SIZE]);
              new_val[META_VALUE_SIZE..].copy_from_slice(&stub.encode());
              if !self.hlog.try_update_in_place(addr, key, &new_val)? {
                let new_addr = self.hlog.append(key, &new_val, addr, false)?;
                self.index.update_address(key, addr, new_addr);
              }

              // 在 RangeIndexManager 中注册 pending 条目 (tree=None，惰性恢复)。
              // 前缀用内联 Base32Buf128 构造，注册路径零堆分配
              let key_id = RangeIndexManager::key_id_of(user_key);
              let key_hash = RangeIndexManager::key_hash_of(user_key);
              let hash_prefix = RangeIndexManager::base32_prefix_of(user_key);

              let is_registered = self.range_index.live_indexes().pin().contains_key(&key_id);

              if !is_registered {
                let tree_entry = Arc::new(TreeEntry::new(None, key_hash, key_id, hash_prefix));
                let pin = self.range_index.live_indexes().pin();
                if pin.insert(key_id, tree_entry).is_none() {
                  count += 1;
                }
              }
            }
          }
        }

        let overflow_idx = curr_bucket.overflow_index();
        if overflow_idx == 0 {
          break;
        }
        match self.index.overflow_pool.get(overflow_idx) {
          Some(next) => curr_bucket = next,
          None => break,
        }
      }
    }

    Ok(count)
  }
  /// 为共享 BfTree 执行 CPR 检查点快照落盘 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotAllTreesForCheckpoint 共享树部分)
  ///
  /// 未配置 `bftree_path`（临时内存树，无持久化承诺）时跳过返回 0；成功返回 1。
  /// 配置了 `bftree_path` 但引擎退化为非磁盘后端属异常状态：静默跳过会让后续
  /// 所有 Checkpoint 均不含共享树快照，重启即永久丢失 ACL 与 Flattened ZSet，
  /// 故显式报错暴露。快照经引擎 CPR 阶段协议与 insert/delete 并发安全
  /// (对标 C# 非阻塞语义)，无撕裂。
  pub fn take_bftree_checkpoint(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    token: u128,
  ) -> Result<usize> {
    if self.config.bftree_path.is_none() {
      // 未配置持久工作文件：临时树无持久化承诺，按设计静默跳过
      return Ok(0);
    }
    if self.bftree.storage_backend() != StorageBackendType::Disk {
      let mut msg =
        String::from("共享 BfTree 引擎非磁盘后端，无法为配置的持久工作文件生成快照: token=");
      let mut buf = itoa::Buffer::new();
      msg.push_str(buf.format(token));
      return Err(wbftree::Error::Snapshot(msg).into());
    }
    self
      .bftree
      .cpr_snapshot(bftree_snapshot_path(checkpoint_dir.as_ref(), token))?;
    Ok(1)
  }

  /// 从 Checkpoint 快照恢复共享 BfTree (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:RebuildFromSnapshotIfPending + RestoreTree)
  ///
  /// 快照存在：经临时文件安全换树恢复（活动基文件最终重命名至持久工作路径，
  /// 绝不引用会被 purge 回收的 token 目录内文件）；快照不存在：保持 open 时
  /// 已从工作文件恢复的状态。工作路径父目录由 [`BfTreeService::recover_in_place`] 内建。
  pub fn recover_bftree_from_checkpoint(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    token: u128,
  ) -> Result<()> {
    let snapshot_path = bftree_snapshot_path(checkpoint_dir.as_ref(), token);
    let Some(work_path) = &self.config.bftree_path else {
      // meta 未记录 bftree_path（旧版本 checkpoint 或配置漂移）但 token 目录存在
      // 共享树快照：静默跳过 = 丢弃可恢复的 ACL / Flattened ZSet 数据，拒绝恢复
      if snapshot_path.exists() {
        let mut msg = String::from(
          "Checkpoint 含共享 BfTree 快照但 StoreMeta 未记录 bftree_path，拒绝静默丢弃可恢复数据，请配置 bftree_path 后重试: token=",
        );
        let mut buf = itoa::Buffer::new();
        msg.push_str(buf.format(token));
        return Err(wbftree::Error::Recovery(msg).into());
      }
      return Ok(());
    };
    if !snapshot_path.exists() {
      return Ok(());
    }
    self.bftree.recover_in_place(&snapshot_path, work_path)?;
    Ok(())
  }
}

/// 独立函数：遍历并执行所有 RangeIndex 的 CPR 快照 (对标 take_cpr_snapshots)
pub fn take_cpr_snapshots<D: Device>(
  store: &WedbStore<D>,
  checkpoint_dir: impl AsRef<Path>,
  token: u128,
) -> Result<usize> {
  store.take_range_index_checkpoints(checkpoint_dir, token)
}

/// 独立函数：从 Checkpoint 恢复所有 RangeIndex 并重建状态机 (对标 recover_cpr_snapshots)
pub async fn recover_cpr_snapshots<D: Device>(
  store: &WedbStore<D>,
  checkpoint_dir: impl AsRef<Path>,
  token: u128,
) -> Result<usize> {
  store.recover_range_indexes(checkpoint_dir, token).await
}

/// 独立函数：为共享 BfTree 执行 CPR 快照 (镜像 take_cpr_snapshots)
pub fn take_shared_bftree_snapshot<D: Device>(
  store: &WedbStore<D>,
  checkpoint_dir: impl AsRef<Path>,
  token: u128,
) -> Result<usize> {
  store.take_bftree_checkpoint(checkpoint_dir, token)
}

/// 独立函数：从 Checkpoint 恢复共享 BfTree (镜像 recover_cpr_snapshots)
pub fn recover_shared_bftree<D: Device>(
  store: &WedbStore<D>,
  checkpoint_dir: impl AsRef<Path>,
  token: u128,
) -> Result<()> {
  store.recover_bftree_from_checkpoint(checkpoint_dir, token)
}

impl<D: Device> wcpr::CprStore for WedbStore<D> {
  type Device = D;

  #[inline]
  fn hlog(&self) -> &whlog::HybridLog<D> {
    &self.hlog
  }

  #[inline]
  fn index(&self) -> &windex::HashIndex {
    &self.index
  }

  #[inline]
  fn epoch(&self) -> &wepoch::LightEpoch {
    &self.epoch
  }

  #[inline]
  fn tail_address(&self) -> u64 {
    self.tail_address()
  }

  #[inline]
  fn begin_address(&self) -> u64 {
    self.begin_address()
  }

  #[inline]
  fn head_address(&self) -> u64 {
    self.head_address()
  }

  #[inline]
  fn shift_read_only_address(&self, target: u64) {
    self.shift_read_only_address(target);
  }

  #[inline]
  async fn flush_all(&self) -> wcpr::Result<()> {
    self.flush_all().await.map_err(cpr_err)
  }

  #[inline]
  fn entry_count(&self) -> usize {
    self.entry_count()
  }

  #[inline]
  fn skip_read_cache(&self, addr: u64) -> u64 {
    if self.read_cache.is_enabled {
      self.read_cache.skip_read_cache(addr)
    } else {
      0
    }
  }

  #[inline]
  fn take_range_index_checkpoints(&self, dir: &Path, token: u128) -> wcpr::Result<usize> {
    self
      .take_range_index_checkpoints(dir, token)
      .map_err(cpr_err)
  }

  #[inline]
  fn take_bftree_checkpoint(&self, dir: &Path, token: u128) -> wcpr::Result<usize> {
    self.take_bftree_checkpoint(dir, token).map_err(cpr_err)
  }

  #[inline]
  fn checkpoint_store_meta(&self) -> wcpr::StoreMeta {
    wcpr::StoreMeta {
      index_size: self.config.index_size,
      page_size: self.config.page_size,
      num_pages: self.config.num_pages,
      mutable_fraction: self.config.mutable_fraction,
      max_sessions: self.config.max_sessions,
      enable_revivification: self.config.enable_revivification,
      enable_read_cache: self.config.enable_read_cache,
      read_cache_num_pages: self.config.read_cache_num_pages,
      range_index_dir: self
        .config
        .range_index_dir
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned()),
      bftree_path: self
        .config
        .bftree_path
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned()),
      next_key_id: self.next_key_id.load(Ordering::Relaxed),
    }
  }
}

impl<D: Device> wcpr::CprRecover for WedbStore<D> {
  /// 从恢复组件装配引擎（容量契约：恢复配置完全由检查点 StoreMeta 决定）
  ///
  /// `index_size`/`page_size`/`num_pages` 等核心容量一律取自持久化 StoreMeta
  /// 重建配置，索引按快照原样定容重建（wcpr 已做 meta 与快照严格相等校验）——
  /// 恢复入口没有"用户本次配置"通道，绝不按调用方意愿缩表；需改容请全新建库。
  /// [`Self::from_components`] 内部再做 config 与实际索引容量一致性预检兜底。
  async fn from_recovered(
    recovered: wcpr::RecoveredCheckpoint<D>,
    checkpoint_dir: &Path,
    device: Arc<D>,
  ) -> wcpr::Result<Self> {
    let mut config = StoreConfig::new(
      recovered.meta.store_meta.index_size,
      recovered.meta.store_meta.page_size,
      recovered.meta.store_meta.num_pages,
      recovered.meta.store_meta.mutable_fraction,
    )
    .map_err(cpr_err)?
    .with_max_sessions(recovered.meta.store_meta.max_sessions)
    .map_err(cpr_err)?;

    if recovered.meta.store_meta.enable_revivification {
      config = config.with_revivification(true);
    }
    if recovered.meta.store_meta.enable_read_cache {
      config = config
        .with_read_cache(true)
        .with_read_cache_pages(recovered.meta.store_meta.read_cache_num_pages)
        .map_err(cpr_err)?;
    }
    if let Some(p) = &recovered.meta.store_meta.range_index_dir {
      config = config.with_range_index_dir(p);
    }
    if let Some(p) = &recovered.meta.store_meta.bftree_path {
      config = config.with_bftree_path(p);
    }

    let store = Self::from_components(
      config,
      recovered.index,
      recovered.hlog,
      recovered.epoch,
      device,
    )
    .map_err(cpr_err)?;
    store.raise_key_id_floor(
      recovered
        .meta
        .store_meta
        .next_key_id
        .saturating_add(KEY_ID_ASSIGN_MARGIN),
    );
    store
      .recover_range_indexes(checkpoint_dir, recovered.meta.token)
      .await
      .map_err(cpr_err)?;
    store
      .recover_bftree_from_checkpoint(checkpoint_dir, recovered.meta.token)
      .map_err(cpr_err)?;

    Ok(store)
  }
}

impl<D: Device> WedbStore<D> {
  /// 创建持久化 Checkpoint 快照（对标 C# Tsavorite `TakeFullCheckpointAsync`）
  #[inline]
  pub async fn create_checkpoint(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    cp_type: wcpr::CheckpointType,
  ) -> Result<wcpr::CheckpointMeta> {
    let mgr = wcpr::CheckpointManager::<D>::new();
    mgr
      .create_checkpoint(self, checkpoint_dir, cp_type)
      .await
      .map_err(Error::from)
  }

  /// 使用指定 Token 创建持久化 Checkpoint 快照
  #[inline]
  pub async fn create_checkpoint_with_token(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    cp_type: wcpr::CheckpointType,
    token: u128,
  ) -> Result<wcpr::CheckpointMeta> {
    let mgr = wcpr::CheckpointManager::<D>::new();
    mgr
      .create_checkpoint_with_token(self, checkpoint_dir, cp_type, token)
      .await
      .map_err(Error::from)
  }

  /// 从指定 Checkpoint 进行崩溃恢复（对标 C# Tsavorite `RecoverAsync(Guid)`）
  ///
  /// 容量契约：恢复配置（含 `index_size`）完全由检查点 StoreMeta 决定，本入口
  /// 无用户配置参数；索引按快照原样定容重建，绝不静默缩表。需改容请全新建库
  /// （索引打开时定容且运行期无在线扩容）。
  #[inline]
  pub async fn recover(
    checkpoint_dir: impl AsRef<Path>,
    token: u128,
    device: Arc<D>,
  ) -> Result<Self> {
    wcpr::CheckpointManager::<D>::recover(checkpoint_dir, token, device)
      .await
      .map_err(Error::from)
  }

  /// 从目录中最新的有效 Checkpoint 执行崩溃恢复（对标 C# Tsavorite `RecoverAsync()`）
  ///
  /// 容量契约同 [`Self::recover`]：恢复配置完全由检查点 StoreMeta 决定。
  #[inline]
  pub async fn recover_latest(checkpoint_dir: impl AsRef<Path>, device: Arc<D>) -> Result<Self> {
    wcpr::CheckpointManager::<D>::recover_latest(checkpoint_dir, device)
      .await
      .map_err(Error::from)
  }
}

/// Wedb 存储引擎专用检查点管理器（对标 Garnet `GarnetCheckpointManager`）
#[derive(Debug, Default)]
pub struct CheckpointManager<D: Device = wdev::SegmentedDevice> {
  inner: wcpr::CheckpointManager<D>,
}

impl<D: Device> CheckpointManager<D> {
  /// 创建 CheckpointManager 实例
  #[inline]
  pub const fn new() -> Self {
    Self {
      inner: wcpr::CheckpointManager::new(),
    }
  }

  /// 创建指定设备类型的 CheckpointManager 实例
  #[inline]
  pub const fn with_device() -> Self {
    Self::new()
  }

  /// 创建持久化 Checkpoint 快照
  #[inline]
  pub async fn create_checkpoint<S: wcpr::CprStore<Device = D>>(
    &self,
    store: &S,
    checkpoint_dir: impl AsRef<Path>,
    cp_type: wcpr::CheckpointType,
  ) -> wcpr::Result<wcpr::CheckpointMeta> {
    self
      .inner
      .create_checkpoint(store, checkpoint_dir, cp_type)
      .await
  }

  /// 使用指定 Token 创建持久化 Checkpoint 快照
  #[inline]
  pub async fn create_checkpoint_with_token<S: wcpr::CprStore<Device = D>>(
    &self,
    store: &S,
    checkpoint_dir: impl AsRef<Path>,
    cp_type: wcpr::CheckpointType,
    token: u128,
  ) -> wcpr::Result<wcpr::CheckpointMeta> {
    self
      .inner
      .create_checkpoint_with_token(store, checkpoint_dir, cp_type, token)
      .await
  }

  /// 从指定 Checkpoint 进行崩溃恢复，重构并实例化全新的 WedbStore
  #[inline]
  pub async fn recover(
    checkpoint_dir: impl AsRef<Path>,
    token: u128,
    device: Arc<D>,
  ) -> wcpr::Result<WedbStore<D>> {
    wcpr::CheckpointManager::<D>::recover(checkpoint_dir, token, device).await
  }

  /// 实例恢复方法
  #[inline]
  pub async fn recover_store(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    token: u128,
    device: Arc<D>,
  ) -> wcpr::Result<WedbStore<D>> {
    Self::recover(checkpoint_dir, token, device).await
  }

  /// 从目录中最新的有效 Checkpoint 执行崩溃恢复
  #[inline]
  pub async fn recover_latest(
    checkpoint_dir: impl AsRef<Path>,
    device: Arc<D>,
  ) -> wcpr::Result<WedbStore<D>> {
    wcpr::CheckpointManager::<D>::recover_latest(checkpoint_dir, device).await
  }

  /// 实例恢复最新方法
  #[inline]
  pub async fn recover_latest_store(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    device: Arc<D>,
  ) -> wcpr::Result<WedbStore<D>> {
    Self::recover_latest(checkpoint_dir, device).await
  }

  /// 列出目标目录中所有可用的 Checkpoint Token
  #[inline]
  pub fn list_checkpoints(checkpoint_dir: impl AsRef<Path>) -> wcpr::Result<Vec<u128>> {
    wcpr::CheckpointManager::<D>::list_checkpoints(checkpoint_dir)
  }

  /// 检索目标目录中最新的有效 Checkpoint Token
  #[inline]
  pub fn find_latest_checkpoint(checkpoint_dir: impl AsRef<Path>) -> wcpr::Result<Option<u128>> {
    wcpr::CheckpointManager::<D>::find_latest_checkpoint(checkpoint_dir)
  }

  /// 清理指定 Token 的快照物理文件
  #[inline]
  pub fn purge_checkpoint(checkpoint_dir: impl AsRef<Path>, token: u128) -> wcpr::Result<()> {
    wcpr::CheckpointManager::<D>::purge_checkpoint(checkpoint_dir, token)
  }

  /// 清空目标目录下全部 Checkpoint 文件
  #[inline]
  pub fn purge_all(checkpoint_dir: impl AsRef<Path>) -> wcpr::Result<()> {
    wcpr::CheckpointManager::<D>::purge_all(checkpoint_dir)
  }

  /// 保留最新 keep 个检查点
  #[inline]
  pub fn purge_outdated(checkpoint_dir: impl AsRef<Path>, keep: usize) -> wcpr::Result<Vec<u128>> {
    wcpr::CheckpointManager::<D>::purge_outdated(checkpoint_dir, keep)
  }

  /// 实例清理方法
  #[inline]
  pub fn purge(&self, checkpoint_dir: impl AsRef<Path>, token: u128) -> wcpr::Result<()> {
    Self::purge_checkpoint(checkpoint_dir, token)
  }

  /// 实例全量清理方法
  #[inline]
  pub fn purge_all_checkpoints(&self, checkpoint_dir: impl AsRef<Path>) -> wcpr::Result<()> {
    Self::purge_all(checkpoint_dir)
  }

  /// 实例保留最新 N 个检查点方法
  #[inline]
  pub fn purge_outdated_checkpoints(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    keep: usize,
  ) -> wcpr::Result<Vec<u128>> {
    Self::purge_outdated(checkpoint_dir, keep)
  }

  /// 异步生成并原子落盘 HashIndex 快照
  #[inline]
  pub async fn take_index_checkpoint(
    &self,
    index: &windex::HashIndex,
    entry_count: usize,
    checkpoint_dir: impl AsRef<Path>,
    token: u128,
    rc_skip: impl Fn(u64) -> u64,
  ) -> wcpr::Result<wcpr::IndexMeta> {
    self
      .inner
      .take_index_checkpoint(index, entry_count, checkpoint_dir, token, rc_skip)
      .await
  }
}
