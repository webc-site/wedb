//! 数据库管理共享基座（对标 libs/server/Databases/DatabaseManagerBase.cs）
//!
//! C# 侧为抽象基类，承载检查点 / AOF / 恢复 / 清空的跨模式共享实现；Rust 侧
//! 为 [`DatabaseManagerBase`]，方法统一以 [`GarnetDatabase`] 为操作对象，
//! 由 Single / Multi 管理器组合复用。AOF 记录格式（本域约定）：
/// `[1B op][8B key_len BE][key][value]`，op：0=UPSERT、1=DELETE。
use std::fmt;
use std::{
  io,
  path::PathBuf,
  sync::{Arc, atomic::Ordering::Relaxed},
};

use waof::{AofEntryType, WalRecord};
use wdev::Device;
use wkv::{CheckpointManager, CheckpointType, WedbStore};

use super::{garnet_database::GarnetDatabase, i_database_manager::HybridLogStats};
use crate::storage::session::objectstore::common::{OBJ_TAG_HASH, OBJ_TAG_SORTED_SET};

/// AOF 操作码：UPSERT
pub const AOF_OP_UPSERT: u8 = 0;
/// AOF 操作码：DELETE
pub const AOF_OP_DELETE: u8 = 1;

/// 快照恢复产物：恢复出的存储句柄（多库共享场景由调用方接管重挂）
pub type RecoveredStore<D> = Option<Arc<WedbStore<D>>>;

/// 共享基座：检查点管理器 + 快照目录
pub struct DatabaseManagerBase<D: Device> {
  /// wkv 检查点管理器
  pub checkpoint_mgr: CheckpointManager<D>,
  /// 默认检查点目录（库未显式指定时使用）
  pub checkpoint_dir: PathBuf,
}

impl<D: Device> DatabaseManagerBase<D> {
  /// 以默认检查点目录创建基座
  pub fn new(checkpoint_dir: PathBuf) -> Self {
    Self {
      checkpoint_mgr: CheckpointManager::new(),
      checkpoint_dir,
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
  /// 存储句柄替换由管理器初始化路径接管。
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
      Some(t) => Ok(Some(Arc::new(
        CheckpointManager::recover(&db.checkpoint_dir, t, Arc::clone(&db.device))
          .await
          .map_err(wkv::Error::from)?,
      ))),
      None => Ok(None),
    }
  }

  /// 恢复数据库 AOF：从上次保存尾地址重放到当前尾
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:RecoverDatabaseAOFAsync
  pub async fn recover_database_aof_async(&self, db: &GarnetDatabase<D>) -> wkv::Result<u64> {
    let from = db.last_save_store_tail_address.load(Relaxed);
    self.replay_database_aof(db, from, u64::MAX).await
  }

  /// 重放数据库 AOF（`from..until` 区间），返回重放条数
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:ReplayDatabaseAOF
  pub async fn replay_database_aof(
    &self,
    db: &GarnetDatabase<D>,
    from: u64,
    until: u64,
  ) -> wkv::Result<u64> {
    let Some(aof) = &db.aof else {
      return Ok(0);
    };
    let session = db.store.new_session()?;
    let mut replayed = 0u64;
    let end = until.min(aof.tail_address());
    let mut it = aof.scan(from, end);
    while let Some(rec) = it.next().await.map_err(aof_err)? {
      if apply_aof_record(&session, &rec).await? {
        replayed += 1;
      }
    }
    Ok(replayed)
  }

  /// 拍数据库检查点（FoldOver 折叠模式），成功后记录保存点
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:TakeCheckpointAsync（单库实现内核）
  pub async fn take_database_checkpoint_async(&self, db: &GarnetDatabase<D>) -> wkv::Result<bool> {
    if db.checkpoint_paused.load(Relaxed) {
      return Ok(false);
    }
    let token = coarsetime::Clock::now_since_epoch().as_u64() as u128;
    let meta = self
      .checkpoint_mgr
      .create_checkpoint_with_token(
        &*db.store,
        &db.checkpoint_dir,
        CheckpointType::FoldOver,
        token,
      )
      .await
      .map_err(wkv::Error::from)?;
    db.update_last_save(meta.created_at);
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

  /// AOF 大小达限检查点
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

  /// AOF 提交：waof 环形缓冲刷盘语义由写路径闭环，此处推进保存点
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:CompactionCommitAofAsync
  pub fn commit_aof(&self, db: &GarnetDatabase<D>) -> wkv::Result<()> {
    if let Some(aof) = &db.aof {
      db.last_save_store_tail_address
        .store(aof.tail_address(), Relaxed);
    }
    Ok(())
  }

  /// 追加一条 UPSERT 到 AOF
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:EnqueueDatabaseCommit
  pub fn enqueue_upsert(
    &self,
    db: &GarnetDatabase<D>,
    key: &[u8],
    value: &[u8],
  ) -> wkv::Result<()> {
    enqueue_record(
      db,
      AofEntryType::MainStoreStoreCommand,
      AOF_OP_UPSERT,
      key,
      Some(value),
    )
  }

  /// 追加一条 DELETE 到 AOF
  pub fn enqueue_delete(&self, db: &GarnetDatabase<D>, key: &[u8]) -> wkv::Result<()> {
    enqueue_record(
      db,
      AofEntryType::MainStoreStoreCommand,
      AOF_OP_DELETE,
      key,
      None,
    )
  }

  /// 重置数据库内容（删除本库全部用户键 + 复位保存点）
  ///
  /// 缺口说明：C# 侧 FLUSHDB 走 UNREGISTER 面删除对象存与主存记录；
  /// wkv 共享存储模型下逐键删除（不经 truncate——物理截断会摧毁同库
  /// 共享该引擎的其他逻辑库数据）。
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:ResetDatabase
  pub async fn reset_database(&self, db: &GarnetDatabase<D>) -> wkv::Result<()> {
    let session = db.store.new_session()?;
    session.set_active_db(db.id.max(0) as u64);
    let batch = session.enter_batch();
    let ss = crate::storage::session::storage_session::StorageSession::new(batch);
    let (_, keys) = ss.string_snapshot().await?;
    for key in keys {
      ss.delete_string(&key).await?;
    }
    ss.clear_watches();
    db.last_save_ms.store(0, Relaxed);
    db.last_save_store_tail_address.store(0, Relaxed);
    Ok(())
  }

  /// 哈希对象收集扫描（按标签过滤的对象枚举），返回对象数
  ///
  /// 缺口说明：C# 侧逐哈希对象做引用计数回收；wkv 生命周期由引擎 GC
  /// 承担，此处退化为按标签枚举统计。
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:ExecuteHashCollect
  pub async fn execute_hash_collect(&self, db: &GarnetDatabase<D>) -> wkv::Result<usize> {
    let (_, n) = execute_collect(self, db, OBJ_TAG_HASH).await?;
    Ok(n)
  }

  /// 有序集合对象收集扫描，返回对象数
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:ExecuteSortedSetCollect
  pub async fn execute_sorted_set_collect(&self, db: &GarnetDatabase<D>) -> wkv::Result<usize> {
    let (_, n) = execute_collect(self, db, OBJ_TAG_SORTED_SET).await?;
    Ok(n)
  }

  /// 存储索引按需增长
  ///
  /// 缺口说明：wkv 哈希索引容量在打开时固定（INDEX_BUCKET_BYTES 桶扩位），
  /// 无运行时增长入口；返回 false 表示未执行增长。
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
    let purged =
      CheckpointManager::<D>::purge_outdated(&db.checkpoint_dir, 2).map_err(wkv::Error::from)?;
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
async fn execute_collect<D: Device>(
  _base: &DatabaseManagerBase<D>,
  db: &GarnetDatabase<D>,
  tag: u8,
) -> wkv::Result<(u8, usize)> {
  let session = db.store.new_session()?;
  session.set_active_db(db.id.max(0) as u64);
  let batch = session.enter_batch();
  let ss = crate::storage::session::storage_session::StorageSession::new(batch);
  let n = ss.object_collect(|t, _| t == tag).await?;
  Ok((tag, n))
}

/// waof 错误 → wkv 错误的统一包装（AOF 属 IO 面，走 Io 变体）
pub fn aof_err(e: impl fmt::Display) -> wkv::Error {
  wkv::Error::Io(io::Error::other(e.to_string()))
}

/// 编码并追加一条 AOF 记录
pub fn enqueue_record<D: Device>(
  db: &GarnetDatabase<D>,
  entry_type: AofEntryType,
  op: u8,
  key: &[u8],
  value: Option<&[u8]>,
) -> wkv::Result<()> {
  let Some(aof) = &db.aof else {
    return Ok(()); // 未启用 AOF：静默跳过（对标 enableAOF=false）
  };
  let value = value.unwrap_or_default();
  let mut payload = Vec::with_capacity(1 + 8 + key.len() + value.len());
  payload.push(op);
  payload.extend_from_slice(&(key.len() as u64).to_be_bytes());
  payload.extend_from_slice(key);
  payload.extend_from_slice(value);
  aof.enqueue(&payload).map_err(aof_err)?;
  let _ = entry_type; // 记录类型由 waof 记录头承担，保留参数对标 C# 入参
  Ok(())
}

/// 应用单条 AOF 记录到存储（UPSERT / DELETE），格式非法返回 false
pub async fn apply_aof_record<D: Device>(
  session: &wkv::StoreSession<D>,
  rec: &WalRecord,
) -> wkv::Result<bool> {
  let payload = rec.as_slice();
  if payload.len() < 9 {
    return Ok(false);
  }
  let op = payload[0];
  let key_len = u64::from_be_bytes(payload[1..9].try_into().map_err(aof_err)?) as usize;
  if payload.len() < 9 + key_len {
    return Ok(false);
  }
  let key = &payload[9..9 + key_len];
  match op {
    AOF_OP_UPSERT => {
      session.upsert(key, &payload[9 + key_len..]).await?;
      Ok(true)
    }
    AOF_OP_DELETE => {
      session.delete(key).await?;
      Ok(true)
    }
    _ => Ok(false),
  }
}
