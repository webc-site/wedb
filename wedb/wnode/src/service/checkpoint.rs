use super::*;

/// 检查点目录口径（C# GetStoreCheckpointDirectory：`{CheckpointBaseDirectory}/
/// Store/checkpoints`，对标 GarnetServerOptions.cs:692-693；基目录真源为
/// wconf `NodeArgs::checkpoint_base_dir`——显式 `--checkpoint-dir` 优先，
/// 缺省回落数据目录，经 RuntimeServerOptions.checkpoint_base_directory 投影，
/// CONFIG GET dir 同源回显。检查点与 WAL 可置独立卷，数据盘故障时快照基线
/// 可存活）。`base_dir` 空串（嵌入式宿主直传 `RuntimeServerOptions::default`
/// 形态，对位 C# 缺省 `""` 空回落根）回落数据目录默认布局
pub(super) fn checkpoint_dir_of(base_dir: &Path, data_path: &Path) -> PathBuf {
  if base_dir.as_os_str().is_empty() {
    return data_path
      .parent()
      .unwrap_or_else(|| Path::new("."))
      .join("Store")
      .join("checkpoints");
  }
  base_dir.join("Store").join("checkpoints")
}

/// 从最新检查点恢复存储句柄（database 恢复面宿主段）
///
/// libs/server/StoreWrapper.cs:RecoverCheckpointAsync
/// libs/server/Databases/IDatabaseManager.cs:RecoverCheckpointAsync
/// libs/server/Databases/DatabaseManagerBase.cs:RecoverCheckpointAsync
/// libs/server/Databases/SingleDatabaseManager.cs:RecoverCheckpointAsync
///
/// 以空库为恢复宿主（GarnetDatabase 契约：版本基线对齐 + 恢复设备源），
/// 经 DatabaseManagerBase 的 recover_database_checkpoint_async 执行恢复
///（C# DatabaseManagerBase.RecoverDatabaseCheckpointAsync 真身），
/// 恢复出的全新 [`WedbStore`] 优先采用并补启 GC（`open_shared` 仅覆盖
/// 宿主段）；目录无有效快照时返回宿主空库（冷启动语义）
pub(super) async fn recover_checkpoint_store(
  checkpoint_dir: &Path,
  device: Arc<SegmentedDevice>,
  config: StoreConfig,
) -> crate::Result<SharedStore<SegmentedDevice>> {
  let bootstrap = WedbStore::open_shared(config, Arc::clone(&device))?;
  let db = Arc::new(GarnetDatabase::<SegmentedDevice>::new(
    0,
    Arc::clone(&bootstrap),
    device,
    checkpoint_dir.to_path_buf(),
    None,
  ));
  let mgr = SingleDatabaseManager::new(checkpoint_dir.to_path_buf(), Arc::clone(&db));
  let Some(store) = mgr
    .base
    .recover_database_checkpoint_async(&db, None)
    .await?
  else {
    return Ok(bootstrap);
  };
  store.start_gc();
  log::info!("Recovered checkpoint: {}", checkpoint_dir.display());
  Ok(store)
}

impl<F> StorageSessionProvider<F>
where
  F: Fn(u64, StoreGarnetApi<SegmentedDevice>) -> Option<RespSessionConsumer>,
{
  /// 检查点目录换装（生产覆写臂单套机制，(false,true)/(false,false) 两臂共形）：
  /// 覆写目录真源并同步重建 GarnetDatabase/SingleDatabaseManager 双件，补挂
  /// primary_tasks 角色域——双件构造期捕获目录，仅覆写 [`Self::checkpoint_dir`]
  /// 字段不换装即令 SAVE 落点与声明目录分叉。`aof` 位随装配形态：AOF 臂传
  /// 点亮句柄，冷启默认臂 None
  pub(super) fn reattach_checkpoint_dir(
    &mut self,
    checkpoint_dir: PathBuf,
    aof: Option<Arc<GarnetAppendOnlyFile>>,
  ) {
    self.checkpoint_dir = checkpoint_dir;
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&self.store),
      Arc::clone(&self.store.device),
      self.checkpoint_dir.clone(),
      aof,
    ));
    self.database_manager = Arc::new(SingleDatabaseManager::new(self.checkpoint_dir.clone(), db));
    // 换装点同步注入角色域：AOF 超限臂在任意装配形态下读同一角色位
    self
      .database_manager
      .attach_primary_tasks(Arc::clone(&self.primary_tasks));
    // 换装点同步注入向量管理器（诞生点即配对点：(false,false)/(false,true)
    // 两臂检查点目录换装后的新双件与 FLUSH 族登记域回收保持联动，不因换装
    // 断链复现默认臂旁路）
    self
      .database_manager
      .attach_vector_manager(Arc::clone(&self.vector_manager));
  }
}
