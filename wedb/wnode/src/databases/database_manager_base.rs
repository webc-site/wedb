//! 数据库管理共享基座（对标 libs/server/Databases/DatabaseManagerBase.cs）
//!
//! C# 侧为抽象基类，承载检查点 / AOF / 恢复 / 清空的跨模式共享实现；Rust 侧
//! 为 [`DatabaseManagerBase`]，方法统一以 [`GarnetDatabase`] 为操作对象，
//! 由 Single / Multi 管理器组合复用。AOF 记录严格遵循 Garnet 标准 AofHeader + AofEntryType 布局。

use std::{
  path::PathBuf,
  sync::{
    Arc,
    atomic::Ordering::{Relaxed, Release},
  },
};

use waof::AofAddress;
use wdev::Device;
use wkv::{CheckpointManager, CheckpointType, WedbStore};
use wval::{GarnetObjectType, SessionPrefixBuf};

use super::{garnet_database::GarnetDatabase, i_database_manager::HybridLogStats};

/// 快照恢复产物：恢复出的存储句柄（多库共享场景由调用方接管重挂）
pub type RecoveredStore<D> = Option<Arc<WedbStore<D>>>;

/// 检查点版本号映射（优先高 64 位，回退低 64 位）
#[inline]
pub const fn checkpoint_version(token: u128) -> i64 {
  let ver = (token >> 64) as i64;
  if ver != 0 { ver } else { token as i64 }
}

/// 检查点后默认保留快照份数
pub const DEFAULT_POST_CHECKPOINT_RETAIN_COUNT: usize = 2;

/// 检查点执行策略
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointPolicy {
  /// 自动 full checkpoint 的日志增长阈值（字节）
  pub full_checkpoint_log_interval: u64,
  /// 是否使用折叠检查点（增量快照，对标 FoldOverCheckpoints）
  pub use_fold_over_checkpoints: bool,
}

impl Default for CheckpointPolicy {
  fn default() -> Self {
    Self {
      full_checkpoint_log_interval: 1 << 30, // 默认 1GB 触发 full
      use_fold_over_checkpoints: false,
    }
  }
}

/// 共享基座：检查点管理器 + 快照目录
pub struct DatabaseManagerBase<D: Device> {
  /// wkv 检查点管理器
  pub checkpoint_mgr: CheckpointManager<D>,
  /// 默认检查点目录（库未显式指定时使用）
  pub checkpoint_dir: PathBuf,
  /// 检查点策略（full 判定与类型选择）
  pub checkpoint_policy: CheckpointPolicy,
}

impl<D: Device> DatabaseManagerBase<D> {
  /// 以默认检查点目录与默认策略创建基座
  pub fn new(checkpoint_dir: PathBuf) -> Self {
    Self {
      checkpoint_mgr: CheckpointManager::new(),
      checkpoint_dir,
      checkpoint_policy: CheckpointPolicy::default(),
    }
  }

  /// 取库或新建（多库模式的映射由管理器覆写）
  ///
  /// 单库语义：恒返回 db0。
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:TryGetOrAddDatabase
  pub fn try_get_or_add_database(
    &self,
    db: &Arc<GarnetDatabase<D>>,
  ) -> wkv::Result<(Arc<GarnetDatabase<D>>, bool)> {
    Ok((Arc::clone(db), false))
  }

  /// 尝试暂停检查点（幂等：已暂停仍返回 true）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:TryPauseCheckpoints
  pub fn try_pause_checkpoints(&self, db: &GarnetDatabase<D>) -> bool {
    !db.checkpoint_paused.swap(true, Relaxed)
  }

  /// 恢复检查点调度
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:ResumeCheckpoints
  pub fn resume_checkpoints(&self, db: &GarnetDatabase<D>) {
    db.checkpoint_paused.store(false, Relaxed);
  }

  /// 恢复数据库检查点：从指定（或最新）令牌恢复出全新存储句柄
  ///
  /// `recover_from_token` 优先；`replica_recover` 语义与主恢复一致（副本
  /// 恢复同样以快照为基准，再交 AOF 追平）。wkv 恢复产出全新 [`WedbStore`]，
  /// 存储句柄替换由管理器初始化路径接管。恢复成功即将存储版本推进至恢复
  /// 令牌（对标 C# `RecoverAsync` 返回 storeVersion、`store.CurrentVersion`
  /// 成为 AOF 重放的版本基线——`ShouldSkipRecord` 跳过低版本条目）。
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:RecoverDatabaseCheckpointAsync
  pub async fn recover_database_checkpoint_async(
    &self,
    db: &GarnetDatabase<D>,
    recover_from_token: Option<u128>,
  ) -> wkv::Result<RecoveredStore<D>> {
    let token = match recover_from_token {
      Some(t) => Some(t),
      None => CheckpointManager::<D>::find_latest_checkpoint(&db.checkpoint_dir)
        .map_err(wkv::Error::from)?,
    };
    match token {
      Some(t) => {
        let store = Arc::new(
          CheckpointManager::recover(&db.checkpoint_dir, t, Arc::clone(&db.device))
            .await
            .map_err(wkv::Error::from)?,
        );
        // 版本基线推进：共享存储模型下当前在用 store 亦对齐至恢复版本，
        // 后续 AOF 重放跳过 checkpoint 已覆盖的旧代条目
        store.set_current_version(checkpoint_version(t));
        db.store.set_current_version(checkpoint_version(t));
        Ok(Some(store))
      }
      None => Ok(None),
    }
  }

  /// 恢复数据库 AOF：设备面日志恢复（磁盘段位点扫描）+ 全量重放
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:RecoverDatabaseAOFAsync
  ///（C# = `db.AppendOnlyFile.Log.RecoverAsync()` 仅恢复位点；重放由
  /// `AofProcessor.Recover` 从 BeginAddress 全量扫描、以 `ShouldSkipRecord`
  /// 版本过滤承接）
  pub async fn recover_database_aof_async(&self, db: &GarnetDatabase<D>) -> wkv::Result<u64> {
    let Some(aof) = &db.aof else {
      return Ok(0);
    };
    aof.recover_async().await;
    self.replay_database_aof(db, u64::MAX).await
  }

  /// 重放数据库 AOF（至 `until` 地址；u64::MAX = 尾部），返回重放条数
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:ReplayDatabaseAOF
  pub async fn replay_database_aof(&self, db: &GarnetDatabase<D>, until: u64) -> wkv::Result<u64> {
    let Some(aof) = &db.aof else {
      return Ok(0);
    };
    let _pause = db.store.pause_aof_listeners();
    Arc::clone(aof).replay_database_aof(db, until).await
  }

  /// 拍数据库检查点（TakeCheckpointAsync 的 full 判定 + InitiateCheckpointAsync 五步）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:TakeCheckpointAsync 与
  /// InitiateCheckpointAsync（498-545）的合流内核：
  /// 1. full 判定：LastSaveStoreTailAddress == 0 或日志增量达 FullCheckpointLogInterval；
  /// 2. OnCheckpointInitiated（集群）：由复制域给出检查点覆盖的 AOF 地址；
  ///    单机形态直取 AOF 尾地址（C# else 分支 TailAddress + SetCurrentSafeAofAddress，
  ///    安全地址由本方法末尾 update_last_save 承接）；
  /// 3. 执行快照（wkv CheckpointManager，版本切换回调经其 set_version_shift_hooks
  ///    由集群装配层注入，PRIMARY 时写入 CheckpointStart/EndCommit 标记）；
  /// 4. AddNewCheckpointEntry（集群 && AOF）：登记检查点条目并安全截断；
  ///    单机形态 TruncateUntil + Commit（物理截断 + 刷盘，与数据记录同一
  ///    物理 AOF 域——域统一后截断位点对单一日志成立）；
  /// 5. 记录保存点；版本推进至快照令牌（后续 AOF 条目携带新版本，重放端
  ///    依 ShouldSkipRecord 跳过旧代条目——对标 C# store.SetVersion）。
  pub async fn take_database_checkpoint_async(&self, db: &GarnetDatabase<D>) -> wkv::Result<bool> {
    if db.checkpoint_paused.load(Relaxed) {
      return Ok(false);
    }

    let cp_type = if self.checkpoint_policy.use_fold_over_checkpoints {
      CheckpointType::FoldOver
    } else {
      CheckpointType::Snapshot
    };

    let mut covered = AofAddress::create(1, 0);
    if let Some(aof) = &db.aof {
      covered = AofAddress::create(1, aof.tail_address());
      if (0..covered.length()).all(|i| covered.get(i as usize).is_some_and(|a| a <= 0)) {
        log::info!(
          "Will truncate AOF to {} after checkpoint (files deleted after next commit), db_id = {}",
          covered.to_aof_string(),
          db.id
        );
      }
    }

    let meta = self
      .checkpoint_mgr
      .create_checkpoint(&*db.store, &db.checkpoint_dir, cp_type)
      .await
      .map_err(wkv::Error::from)?;

    if let Some(aof) = &db.aof {
      aof.truncate_until_async(&covered).await;
      aof.commit_flush_async().await;
    }

    let token = meta.token;
    db.update_last_save(coarsetime::Clock::now_since_epoch().as_millis());
    db.store.set_current_version(checkpoint_version(token));

    Ok(true)
  }

  /// 检查点辅助：距上次保存早于 `entry_ms` 才拍
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:TakeCheckpointHelperAsync
  pub async fn take_checkpoint_helper_async(
    &self,
    db: &GarnetDatabase<D>,
    entry_ms: u64,
  ) -> wkv::Result<bool> {
    if db.last_save_ms() < entry_ms {
      self.take_database_checkpoint_async(db).await
    } else {
      Ok(false)
    }
  }

  /// 按需检查点（管理器入口）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:TakeOnDemandCheckpointAsync
  pub async fn take_on_demand_checkpoint_async(
    &self,
    db: &GarnetDatabase<D>,
    entry_ms: u64,
  ) -> wkv::Result<()> {
    self.take_checkpoint_helper_async(db, entry_ms).await?;
    Ok(())
  }

  /// 若 AOF 增长达到限额则触发检查点
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:TaskCheckpointBasedOnAofSizeLimitAsync（内核）
  pub async fn checkpoint_if_aof_exceeds(
    &self,
    db: &GarnetDatabase<D>,
    aof_size_limit: u64,
  ) -> wkv::Result<bool> {
    if db.aof_size() >= aof_size_limit {
      self.take_database_checkpoint_async(db).await
    } else {
      Ok(false)
    }
  }

  /// AOF 提交：物理刷盘推进保存点
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:CompactionCommitAofAsync
  pub async fn commit_aof(&self, db: &GarnetDatabase<D>) -> wkv::Result<()> {
    if let Some(aof) = &db.aof {
      aof.commit_flush_async().await;
      db.last_save_store_tail_address
        .store(db.store.tail_address(), Release);
    }
    Ok(())
  }

  /// 重置数据库内容（删除本库全部用户键 + 复位保存点）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:ResetDatabase
  pub async fn reset_database(&self, db: &GarnetDatabase<D>) -> wkv::Result<()> {
    let session = db.store.new_session()?;
    session.set_active_db(db.id.max(0) as u64);
    let prefix = SessionPrefixBuf::new(0, db.id.max(0) as u64);
    let prefix_slice = prefix.as_slice();
    let mut user_keys = Vec::new();
    let _ = db
      .store
      .hlog()
      .scan(
        db.store.begin_address(),
        db.store.tail_address(),
        |_addr, rec| {
          let key = rec.key();
          if let Some(rest) = key.strip_prefix(prefix_slice)
            && !rec.is_tombstone()
            && rest.len() > 1
          {
            user_keys.push(rest[1..].to_vec());
          }
          Ok(true)
        },
      )
      .await;
    for key in user_keys {
      let _ = session.try_delete_sync(&key);
    }
    if let Some(aof) = &db.aof {
      let until = AofAddress::create(1, aof.tail_address());
      aof.truncate_until_async(&until).await;
    }
    db.last_save_ms.store(0, Relaxed);
    db.last_save_store_tail_address.store(0, Release);
    Ok(())
  }

  /// 哈希对象收集扫描（按标签过滤的对象枚举），返回对象数
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:ExecuteHashCollect
  pub async fn execute_hash_collect(&self, db: &GarnetDatabase<D>) -> wkv::Result<usize> {
    let (_, n) = execute_collect(db, GarnetObjectType::Hash as u8).await?;
    Ok(n)
  }

  /// 有序集合对象收集扫描，返回对象数
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:ExecuteSortedSetCollect
  pub async fn execute_sorted_set_collect(&self, db: &GarnetDatabase<D>) -> wkv::Result<usize> {
    let (_, n) = execute_collect(db, GarnetObjectType::SortedSet as u8).await?;
    Ok(n)
  }

  /// 对象存收集扫描
  pub async fn execute_object_collection(&self, db: &GarnetDatabase<D>) -> wkv::Result<usize> {
    let prefix = SessionPrefixBuf::new(0, db.id.max(0) as u64);
    let prefix_slice = prefix.as_slice();
    let mut count = 0usize;
    let _ = db
      .store
      .hlog()
      .scan(
        db.store.begin_address(),
        db.store.tail_address(),
        |_addr, rec| {
          let key = rec.key();
          if let Some(rest) = key.strip_prefix(prefix_slice)
            && !rec.is_tombstone()
            && let Some(&tag) = rest.first()
            && (GarnetObjectType::SortedSet as u8..=GarnetObjectType::Set as u8).contains(&tag)
          {
            count += 1;
          }
          Ok(true)
        },
      )
      .await;
    Ok(count)
  }

  /// 存储索引按需增长
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:GrowIndexIfNeededAsync
  pub fn grow_index_if_needed_async(&self, db: &GarnetDatabase<D>) -> wkv::Result<bool> {
    Ok(!db.store_index_maxed_out.load(Relaxed))
  }

  /// 发起检查点（供后台任务调用的统一入口）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:InitiateCheckpointAsync
  pub async fn initiate_checkpoint_async(&self, db: &GarnetDatabase<D>) -> wkv::Result<bool> {
    self.take_database_checkpoint_async(db).await
  }

  /// 检查点后清理：保留最近 2 份快照，清除过期令牌
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:RunPostCheckpointCleanup
  pub fn run_post_checkpoint_cleanup(&self, db: &GarnetDatabase<D>) -> wkv::Result<usize> {
    let purged = CheckpointManager::<D>::purge_outdated(
      &db.checkpoint_dir,
      DEFAULT_POST_CHECKPOINT_RETAIN_COUNT,
    )
    .map_err(wkv::Error::from)?;
    Ok(purged.len())
  }

  /// 键空间过期键物理删除扫描（wkv 原生入口），返回 (删除数, 扫描记录数)
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:StoreExpiredKeyDeletionScan
  pub async fn store_expired_key_deletion_scan(
    &self,
    db: &GarnetDatabase<D>,
  ) -> wkv::Result<(u64, u64)> {
    db.store
      .expired_key_deletion_scan(if db.id == 0 { None } else { Some(db.id as u64) })
      .await
  }

  /// 数据库键空间统计，返回 (键数, 过期数)
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:GetDatabaseKeyspaceStats
  pub async fn get_database_keyspace_stats(
    &self,
    db: &GarnetDatabase<D>,
  ) -> wkv::Result<(u64, u64)> {
    db.store.keyspace_stats().await
  }

  /// 采集单库混合日志分布统计
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:CollectHybridLogStatsForDb
  pub async fn collect_hybrid_log_stats_for_db(
    &self,
    db: &GarnetDatabase<D>,
  ) -> wkv::Result<HybridLogStats> {
    let (key_count, expire_count) = self.get_database_keyspace_stats(db).await?;
    Ok(HybridLogStats {
      begin_address: db.store.begin_address(),
      read_only_address: db.store.read_only_address(),
      head_address: db.store.head_address(),
      tail_address: db.store.tail_address(),
      key_count,
      expire_count,
    })
  }

  /// 全库混合日志分布统计（基座视角：单库一份）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:CollectHybridLogStats
  pub async fn collect_hybrid_log_stats(
    &self,
    db: &GarnetDatabase<D>,
  ) -> wkv::Result<Vec<(i64, HybridLogStats)>> {
    Ok(vec![(
      db.id,
      self.collect_hybrid_log_stats_for_db(db).await?,
    )])
  }
}

/// 按对象标签做收集扫描（哈希/有序集合共用内核），返回 (标签, 对象数)
async fn execute_collect<D: Device>(db: &GarnetDatabase<D>, tag: u8) -> wkv::Result<(u8, usize)> {
  let prefix = SessionPrefixBuf::new(0, db.id.max(0) as u64);
  let prefix_slice = prefix.as_slice();
  let mut count = 0usize;
  let _ = db
    .store
    .hlog()
    .scan(
      db.store.begin_address(),
      db.store.tail_address(),
      |_addr, rec| {
        let key = rec.key();
        if let Some(rest) = key.strip_prefix(prefix_slice)
          && !rec.is_tombstone()
          && rest.first() == Some(&tag)
        {
          count += 1;
        }
        Ok(true)
      },
    )
    .await;
  Ok((tag, count))
}
