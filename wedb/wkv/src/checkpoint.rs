//! RangeIndex 检查点快照与故障恢复模块 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotAllTreesForCheckpoint 与 RebuildFromSnapshotIfPending)

use std::{
  path::Path,
  sync::{Arc, atomic::Ordering},
};

use wbftree::{RANGE_INDEX_STUB_SIZE, RangeIndexStub};
use wdev::Device;
use windex::{HashBucket, HashBucketEntry};
use wval::{CollectionType, META_VALUE_SIZE, MetaValue, NamespaceDbCodec, StorageEncoding};

use crate::{
  config::StoreConfig,
  error::{Error, Result},
  read_cache::is_read_cache_addr,
  store::{KEY_ID_ASSIGN_MARGIN, WedbStore},
};

/// wkv::Error → wcpr::Error 宿主端口映射（零字符串化，全链路类型化）
///
/// wcpr 可表达的基础设施错误（Device/Index/Hlog/Epoch/Io/Cpr）逐变体透明转发；
/// wcpr 不依赖 wbftree 与本 crate（依赖无环），BfTree 快照、RangeIndex 恢复、
/// 配置校验等宿主专属错误经 `wcpr::Error::Host` 透明透传
fn cpr_err(e: Error) -> wcpr::Error {
  match e {
    Error::Device(e) => e.into(),
    Error::Epoch(e) => e.into(),
    Error::HLog(e) => e.into(),
    Error::Index(e) => e.into(),
    Error::Io(e) => e.into(),
    Error::Cpr(e) => e,
    other => wcpr::Error::Host(other.to_string()),
  }
}

impl<D: Device> WedbStore<D> {
  /// 遍历并为所有在线与待激活的 RangeIndex 执行 CPR 检查点快照落盘（调用 snapshot_all_trees_for_checkpoint）
  pub fn take_range_index_checkpoints(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    token: u128,
  ) -> Result<usize> {
    Ok(
      self
        .range_index
        .snapshot_all_trees_to_dir(checkpoint_dir.as_ref(), token)?,
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
    let dir = checkpoint_dir.as_ref();

    // 1. 先从目标检查点目录预置所有 .bftree 快照物理文件并注册 pending 条目
    let mut count = self.range_index.recover_all_trees_from_dir(dir, token)?;

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
            && (meta.collection_type == CollectionType::RangeIndex
              || (meta.encoding() == StorageEncoding::FlattenedTree && meta.size > 0))
          {
            let stub_slice = &val[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE];
            if let Ok(mut stub) = RangeIndexStub::decode(stub_slice) {
              // 标记已从检查点恢复 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:MarkRecoveredFromCheckpoint)
              stub.mark_recovered_from_checkpoint();

              // 已自愈存根零写跳过：持久化字节已等于自愈编码 (句柄已清零 + 恢复位
              // 已置) 时，多轮恢复的重复回写纯属浪费——原位改写退化为同址重写，
              // 失败路径还会多出一次追加 + 索引地址更新；跳过仅省写副作用，注册
              // 副作用照常执行
              let healed = stub.encode();
              if stub_slice != healed.as_slice() {
                // 保留存根之后可能存在的扩展字段（例如 ListStub 的 head 与 tail，长达 16 字节）
                if val.len() <= 128 {
                  let mut new_val = [0u8; 128];
                  new_val[..val.len()].copy_from_slice(val);
                  new_val[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE]
                    .copy_from_slice(&healed);
                  let new_slice = &new_val[..val.len()];
                  if !self.hlog.try_update_in_place(addr, key, new_slice)? {
                    let new_addr = self.hlog.append(key, new_slice, addr, false)?;
                    self.index.update_address(key, addr, new_addr);
                  }
                } else {
                  let mut new_val = val.to_vec();
                  new_val[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE]
                    .copy_from_slice(&healed);
                  if !self.hlog.try_update_in_place(addr, key, &new_val)? {
                    let new_addr = self.hlog.append(key, &new_val, addr, false)?;
                    self.index.update_address(key, addr, new_addr);
                  }
                }
              }

              // 在 RangeIndexManager 中注册 pending 条目 (tree=None，惰性恢复)
              if self.range_index.register_pending(user_key) {
                count += 1;
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

/// 检查点版本切换回调（对标 libs/cluster/Server/Replication/GarnetClusterCheckpointManager.cs:checkpointVersionShiftStart/End 委托）
///
/// 参数为 (old_version, new_version)；wedb 以 Token 大小序承担 C# storeVersion
/// 语义（见 [`CheckpointManager::create_checkpoint`] 的 token 签发论证），
/// 故 old/new 即上次与本次的检查点 Token
pub struct VersionShiftFn {
  ptr: *mut (),
  call: unsafe fn(*mut (), i64, i64),
  drop: unsafe fn(*mut ()),
}

unsafe impl Send for VersionShiftFn {}
unsafe impl Sync for VersionShiftFn {}

impl VersionShiftFn {
  pub fn new<F: Fn(i64, i64) + Send + Sync + 'static>(f: F) -> Self {
    unsafe fn call_fn<F: Fn(i64, i64)>(ptr: *mut (), old: i64, new: i64) {
      let f = unsafe { &*(ptr as *const F) };
      f(old, new);
    }
    unsafe fn drop_fn<F>(ptr: *mut ()) {
      unsafe {
        drop(Box::from_raw(ptr as *mut F));
      }
    }
    Self {
      ptr: Box::into_raw(Box::new(f)) as *mut (),
      call: call_fn::<F>,
      drop: drop_fn::<F>,
    }
  }

  #[inline]
  pub fn call(&self, old: i64, new: i64) {
    unsafe { (self.call)(self.ptr, old, new) }
  }
}

impl Drop for VersionShiftFn {
  fn drop(&mut self) {
    if !self.ptr.is_null() {
      unsafe { (self.drop)(self.ptr) };
      self.ptr = std::ptr::null_mut();
    }
  }
}

impl<F: Fn(i64, i64) + Send + Sync + 'static> From<Box<F>> for VersionShiftFn {
  fn from(b: Box<F>) -> Self {
    Self::new(b)
  }
}

impl From<fn(i64, i64)> for VersionShiftFn {
  fn from(f: fn(i64, i64)) -> Self {
    Self::new(f)
  }
}

/// Wedb 存储引擎专用检查点管理器（对标 Garnet `GarnetCheckpointManager` /
/// `GarnetClusterCheckpointManager`——后者在前者之上追加版本切换委托与复制域钩子）
pub struct CheckpointManager<D: Device = wdev::SegmentedDevice> {
  inner: wcpr::CheckpointManager<D>,
  /// libs/cluster/Server/Replication/GarnetClusterCheckpointManager.cs:checkpointVersionShiftStart
  version_shift_start: parking_lot::RwLock<Option<VersionShiftFn>>,
  /// libs/cluster/Server/Replication/GarnetClusterCheckpointManager.cs:checkpointVersionShiftEnd
  version_shift_end: parking_lot::RwLock<Option<VersionShiftFn>>,
}

impl<D: Device> Default for CheckpointManager<D> {
  fn default() -> Self {
    Self::new()
  }
}

impl<D: Device> CheckpointManager<D> {
  /// 创建 CheckpointManager 实例
  #[inline]
  pub const fn new() -> Self {
    Self {
      inner: wcpr::CheckpointManager::new(),
      version_shift_start: parking_lot::RwLock::new(None),
      version_shift_end: parking_lot::RwLock::new(None),
    }
  }

  /// 注册版本切换回调（集群装配期注入；对标 C# ReplicationManager 构造内
  /// `ReplicationLogCheckpointManager.checkpointVersionShiftStart = ...` 的委托装配）
  pub fn set_version_shift_hooks(
    &self,
    start: Option<VersionShiftFn>,
    end: Option<VersionShiftFn>,
  ) {
    *self.version_shift_start.write() = start;
    *self.version_shift_end.write() = end;
  }

  /// 触发版本切换开始回调（无回调或无历史版本时静默）
  fn fire_version_shift_start(&self, old_version: i64, new_version: i64) {
    if let Some(cb) = self.version_shift_start.read().as_ref() {
      cb.call(old_version, new_version);
    }
  }

  /// 触发版本切换结束回调（无回调或无历史版本时静默）
  fn fire_version_shift_end(&self, old_version: i64, new_version: i64) {
    if let Some(cb) = self.version_shift_end.read().as_ref() {
      cb.call(old_version, new_version);
    }
  }

  /// 创建持久化 Checkpoint 快照
  ///
  /// 版本切换回调时序对标 C# Tsavorite 状态机：CheckpointVersionShiftStart 在
  /// 快照发起（版本号切换）时触发，CheckpointVersionShiftEnd 在快照完成
  /// （PERSISTENCE_CALLBACK 落盘）后触发。失败路径不触发 End（wedb 语义下失败
  /// Token 已被整体回收、版本未实际切换；C# 版本号在 PREPARE 即切换、失败亦
  /// 已生效，属两地版本承载机制的固有差异，语义上均保证「End 必然晚于同轮
  /// Start 且快照成功才发布 End」）
  #[inline]
  pub async fn create_checkpoint<S: wcpr::CprStore<Device = D>>(
    &self,
    store: &S,
    checkpoint_dir: impl AsRef<Path>,
    cp_type: wcpr::CheckpointType,
  ) -> wcpr::Result<wcpr::CheckpointMeta> {
    let dir = checkpoint_dir.as_ref();
    // Token 预知签发（进程守卫 + 目录下界，与 wcpr::create_checkpoint 内部同源），
    // 使版本切换回调可在快照发起前携带新版本号
    let floor = wcpr::CheckpointManager::<D>::find_latest_checkpoint(dir)?.unwrap_or(0);
    let token = wcpr::next_token_above(floor);
    self
      .create_checkpoint_with_token(store, dir, cp_type, token)
      .await
  }

  /// 使用指定 Token 创建持久化 Checkpoint 快照（含版本切换回调触发）
  #[inline]
  pub async fn create_checkpoint_with_token<S: wcpr::CprStore<Device = D>>(
    &self,
    store: &S,
    checkpoint_dir: impl AsRef<Path>,
    cp_type: wcpr::CheckpointType,
    token: u128,
  ) -> wcpr::Result<wcpr::CheckpointMeta> {
    let old_version =
      wcpr::CheckpointManager::<D>::find_latest_checkpoint(checkpoint_dir.as_ref())?
        .map(|t| t as i64)
        .unwrap_or(0);
    let new_version = token as i64;

    self.fire_version_shift_start(old_version, new_version);
    let meta = self
      .inner
      .create_checkpoint_with_token(store, checkpoint_dir, cp_type, token)
      .await;
    match &meta {
      Ok(_) => self.fire_version_shift_end(old_version, new_version),
      Err(_) => {
        log::warn!("Checkpoint 失败，跳过 VersionShiftEnd 回调（版本未切换）: token={token:#x}")
      }
    }
    meta
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
