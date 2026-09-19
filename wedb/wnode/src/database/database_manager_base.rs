//! 数据库管理共享基座（对标 libs/server/Databases/DatabaseManagerBase.cs）
//!
//! C# 侧为抽象基类，承载检查点 / AOF / 恢复 / 清空的跨模式共享实现；Rust 侧
//! 为 [`DatabaseManagerBase`]，方法统一以 [`GarnetDatabase`] 为操作对象，
//! 由 Single / Multi 管理器组合复用。AOF 记录严格遵循 Garnet 标准 AofHeader + AofEntryType 布局。

use std::{
  future::Future,
  marker::PhantomData,
  path::PathBuf,
  sync::{
    Arc, OnceLock,
    atomic::Ordering::{AcqRel, Acquire, Relaxed, Release},
  },
};

use waof::AofAddress;
use wbase::time::now_ms;
use wcpr::{self, CheckpointType};
use wdev::Device;
use wkv::{WedbStore, store::grow_index_blocking};

use super::garnet_database::GarnetDatabase;
use crate::cluster_provider::ClusterProviderHandle;

/// 快照恢复产物：恢复出的存储句柄（多库共享场景由调用方接管重挂）
pub type RecoveredStore<D> = Option<Arc<WedbStore<D>>>;

/// 检查点版本号映射（优先高 64 位，回退低 64 位）
#[inline]
pub const fn checkpoint_version(token: u128) -> i64 {
  let ver = (token >> 64) as i64;
  if ver != 0 { ver } else { token as i64 }
}

/// 检查点保留代数（对标 C# 检查点管理器 removeOutdated 环容量
/// `DeviceLogCommitCheckpointManager.cs:19` 的 `const byte indexTokenCount = 2`
/// ——环满两代即拍新删旧；C# 的 logTokenCount = 1 是「索引/日志异 Token」
/// 时代的配对余量，rust 统一检查点模型一个 Token 承载 index/hlog/RangeIndex
/// 一套文件，无错配形态，取两环上界 2 同形）。
///
/// C# 该值是编译期常数而非配置旋钮（garnet 全域 Options.cs 无 checkpoint-keep
/// 类项，只有决定**是否**自动清理的形态位 removeOutdated），rust 据而不另立
/// 配置槽位：保留数由本常量承载，形态位由基座的 `cluster` 句柄在位与否承担
/// （与检查点版本切换标记同一判据，对标 C# `GarnetServer.cs:396` 的
/// removeOutdated = !EnableCluster）
pub const CHECKPOINT_RETAIN_GENERATIONS: usize = 2;

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

/// 共享基座：快照目录与策略
pub struct DatabaseManagerBase<D: Device> {
  /// 默认检查点目录（库未显式指定时使用）
  pub checkpoint_dir: PathBuf,
  /// 检查点策略（full 判定与类型选择）
  pub checkpoint_policy: CheckpointPolicy,
  /// 集群提供者句柄（检查点版本切换标记的复制域出口；与 flush_gate 同点装配，
  /// None = 单机形态无标记面，对标 C# 委托挂 ReplicationLogCheckpointManager，
  /// standalone provider 无 replication manager 故不写标记）
  cluster: OnceLock<ClusterProviderHandle>,
  _marker: PhantomData<D>,
}

impl<D: Device> DatabaseManagerBase<D> {
  /// 以默认检查点目录与默认策略创建基座
  pub fn new(checkpoint_dir: PathBuf) -> Self {
    Self {
      checkpoint_dir,
      checkpoint_policy: CheckpointPolicy::default(),
      cluster: OnceLock::new(),
      _marker: PhantomData,
    }
  }

  /// 注入集群提供者句柄（集群装配期一次，与 flush_gate 同点；对标 C# 构造期把
  /// checkpointVersionShiftStart/End 委托挂到检查点管理器）。None（未注入）=
  /// 单机形态，检查点内核不写版本切换标记
  pub fn attach_cluster_provider(&self, cluster: ClusterProviderHandle) {
    let _ = self.cluster.set(cluster);
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

  /// 尝试暂停检查点（占用检查点锁，成功返回 true；已被占用返回 false）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:TryPauseCheckpoints
  pub fn try_pause_checkpoints(&self, db: &GarnetDatabase<D>) -> bool {
    !db.checkpoint_paused.swap(true, AcqRel)
  }

  /// 恢复检查点调度（释放检查点锁）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:ResumeCheckpoints
  pub fn resume_checkpoints(&self, db: &GarnetDatabase<D>) {
    db.checkpoint_paused.store(false, Release);
  }

  /// 恢复数据库检查点：从指定（或最新）令牌恢复出全新存储句柄
  ///
  /// `recover_from_token` 优先；`replica_recover` 语义与主恢复一致（副本
  /// 恢复同样以快照为基准，再交 AOF 追平）。wkv 恢复产出全新 [`WedbStore`]，
  /// 存储句柄替换由管理器初始化路径接管。恢复成功即将存储版本推进至恢复
  /// 令牌（对标 C# `RecoverAsync` 返回 storeVersion、`store.CurrentVersion`
  /// 成为 AOF 重放的版本基线——`ShouldSkipRecord` 跳过低版本条目）。
  ///
  /// 恢复成功后追加「清未用」尾段，见 [`Self::purge_unrecovered_checkpoints`]。
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:RecoverDatabaseCheckpointAsync
  pub async fn recover_database_checkpoint_async(
    &self,
    db: &GarnetDatabase<D>,
    recover_from_token: Option<u128>,
  ) -> wkv::Result<RecoveredStore<D>> {
    let token = match recover_from_token {
      Some(t) => Some(t),
      None => wcpr::find_latest_checkpoint(&db.checkpoint_dir)?,
    };
    match token {
      Some(t) => {
        let store =
          Arc::new(WedbStore::recover(&db.checkpoint_dir, t, Arc::clone(&db.device)).await?);
        // 版本基线推进：共享存储模型下当前在用 store 亦对齐至恢复版本，
        // 后续 AOF 重放跳过 checkpoint 已覆盖的旧代条目
        store.set_current_version(checkpoint_version(t));
        db.store.set_current_version(checkpoint_version(t));
        self.purge_unrecovered_checkpoints(db, t);
        Ok(Some(store))
      }
      None => Ok(None),
    }
  }

  /// 恢复后「清未用」：物理删除目录内未被本次恢复选中的全部快照
  ///
  /// 对标 C# 检查点管理器的 `OnRecovery`
  ///（`DeviceLogCommitCheckpointManager.cs:337-365`：首行 `if (!removeOutdated)
  /// return;`，随后把 `GetLogCheckpointTokens` / `GetIndexCheckpointTokens` 中
  /// 一切不等于本次恢复 Token 的快照逐个 Delete，原文注释 "Purge all log/index
  /// checkpoints that were not used for recovery"）——未被选中的快照对本次恢复
  /// 之后的写入链已无意义（版本基线已抬至恢复 Token，旧代条目重放即被
  /// `ShouldSkipRecord` 跳过），留盘即永久占用。
  ///
  /// 形态位与 [`Self::take_database_checkpoint_async`] 第 7 步同源（`cluster`
  /// 句柄缺位 = C# `GarnetServer.cs:396` 的 removeOutdated = !EnableCluster
  /// 为真）：集群形态让位复制域 CheckpointStore 单轨，本口按其回收。
  ///
  /// 与按代回收的分工：本口按**身份**筛——保留被选中那一版、删其余全部（显式
  /// 恢复历史 Token 时更新的代同样作废），故不复用 [`wcpr::purge_outdated`] 的
  /// 按条数口径：按条数留的是「最新 keep 个」，显式恢复旧 Token 时会误删在用版。
  ///
  /// 启动期形态位说明（与 C# 的唯一结构差）：rust 的启动恢复先于
  /// `wedb/src/server/boot.rs:105` 的 cluster 句柄注入（管理器由 `open_from_args`
  /// 自建），故集群宿主的启动恢复亦走本段；删集与集群轨自身的启动清理段重合——
  /// `replication_manager.rs:1130` `initialize_checkpoint_store` 的 seed 同取
  /// `find_latest_checkpoint` 那一 Token，经 `CheckpointStore::initialize` →
  /// `purge_all_checkpoints_except_entry` 留同一版删其余，两段不构成并行的第二套
  /// 回收（运行期恢复入口形态位已就位，集群形态按位让位）。
  ///
  /// best-effort：删除失败只 warn，不回滚已成功的恢复（C# 的 `deviceFactory.Delete`
  /// 同为不影响恢复结果的清理尾段），下一轮检查点按代回收再收。
  fn purge_unrecovered_checkpoints(&self, db: &GarnetDatabase<D>, recovered: u128) {
    if self.cluster.get().is_some() {
      return;
    }
    let Ok(tokens) = wcpr::list_checkpoints(&db.checkpoint_dir) else {
      return;
    };
    for stale in tokens {
      if stale == recovered {
        continue;
      }
      if let Err(e) = wcpr::purge_checkpoint(&db.checkpoint_dir, stale) {
        log::warn!(
          "Failed purging checkpoint {stale:#x} unrecovered by recovery in {}: {e}",
          db.checkpoint_dir.display()
        );
      }
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
  /// 2. 预知签发 Token 并推进版本号（对标 C# PREPARE 进入 IN_PROGRESS 时 Version 推进）；
  /// 3. OnCheckpointInitiated（集群）：由复制域给出检查点覆盖的 AOF 地址；
  ///    单机形态直取 AOF 尾地址（C# else 分支 TailAddress + SetCurrentSafeAofAddress，
  ///    安全地址由本方法末尾 update_last_save 承接）；
  /// 4. 执行快照（WedbStore.create_checkpoint_with_token，
  ///    快照期间前台写入携带新版本）；
  /// 5. AOF 边界持久化 + 截断：快照发布后经
  ///    [`wcpr::publish_checkpoint_aof_address`] 把 covered 补写进检查点元数据
  ///    （对标 C# GarnetCheckpointManager.GetCookie 将 CurrentSafeAofAddress
  ///    序列化进检查点 cookie），随后 AddNewCheckpointEntry（集群 && AOF）登记
  ///    检查点条目并安全截断——异步截断口经 SlowFuture 擦除壳承载、内核处
  ///    await 收口（见下方截断层形态二分）；单机形态 TruncateUntil + Commit
  ///    （物理截断 + 刷盘，
  ///    与数据记录同一物理 AOF 域——域统一后截断位点对单一日志成立）；
  /// 6. 记录保存点（update_last_save）；
  /// 7. 快照保留回收（单机形态）：拍新删旧至 [`CHECKPOINT_RETAIN_GENERATIONS`]
  ///    代（对标 C# 检查点状态机 REST 段的 CleanupIndexCheckpoint /
  ///    CleanupLogCheckpoint）；集群形态由复制域 CheckpointStore 读者闸门单轨
  ///    接管，本步不触发（形态判据同版本切换标记）。
  pub async fn take_database_checkpoint_async(&self, db: &GarnetDatabase<D>) -> wkv::Result<bool> {
    let cp_type = if self.checkpoint_policy.use_fold_over_checkpoints {
      CheckpointType::FoldOver
    } else {
      CheckpointType::Snapshot
    };

    // 预知签发 Token 并提前推进版本号：快照期间写入即携带新版本号，
    // 杜绝快照窗口内写入携带旧版本而在崩溃恢复时被 AOF 重放误跳过
    let floor = wcpr::find_latest_checkpoint(&db.checkpoint_dir)?.unwrap_or(0);
    let token = wcpr::next_token_above(floor);
    let new_version = checkpoint_version(token);
    // 版本切换开始（对标 C# GlobalBeforeEnteringState IN_PROGRESS →
    // CheckpointVersionShiftStart）：主库向 AOF 追加 CheckpointStartCommit 标记，
    // 副本据此进入模糊区缓冲新代条目。单机形态（未注入 cluster 句柄）不写标记
    if let Some(cluster) = self.cluster.get() {
      cluster.checkpoint_version_shift_start(new_version);
    }
    db.store.set_current_version(new_version);

    let mut covered = AofAddress::create(1, 0);
    if let Some(aof) = &db.aof {
      // covered 取源按形态二分（对标 C# :509 EnableCluster 二分）：集群句柄
      // 在位 → 复制域经 on_checkpoint_initiated 给出覆盖位点（PRIMARY 取当前
      // 复制位点并更新提交安全地址，REPLICA 取检查点开始标记位点）；否则直取
      // AOF 尾地址（C# else 分支 TailAddress + SetCurrentSafeAofAddress，
      // 安全地址由本方法末尾 update_last_save 承接）
      if let Some(cluster) = self.cluster.get() {
        cluster.on_checkpoint_initiated(&mut covered);
      } else {
        covered = AofAddress::create(1, aof.tail_address());
      }
      if (0..covered.length() as usize).any(|i| covered.get(i).is_some_and(|a| a > 0)) {
        // C# DatabaseManagerBase.cs:515 文案为 "files deleted after next commit"
        //（逻辑截断 + 提交面删段组合）；rust 提交面不删段，截断走唯一物理回收
        // 真身 truncate_until_async 即时删段，故文案据实反映"检查点落盘后立即回收"
        log::info!(
          "Will truncate AOF to {} right after checkpoint (segments deleted on truncate), db_id = {}",
          covered.to_aof_string(),
          db.id
        );
      }
    }

    db.store
      .create_checkpoint_with_token(&db.checkpoint_dir, cp_type, token)
      .await?;

    // 版本切换结束（对标 C# GlobalBeforeEnteringState WAIT_FLUSH →
    // CheckpointVersionShiftEnd）：快照落盘后、截断前追加 CheckpointEndCommit
    // 标记，副本据此退出模糊区并重放缓冲条目。失败路径不到此处（create_checkpoint_with_token
    // 以 ? 上抛），语义同 C# WAIT_FLUSH 仅在快照成功后进入
    if let Some(cluster) = self.cluster.get() {
      cluster.checkpoint_version_shift_end(new_version);
    }

    if db.aof.is_some() {
      // AOF 边界随检查点元数据持久化：取快照发起时的 covered（快照窗口内
      // 的并发写入使当前尾地址大于覆盖边界，C# CurrentSafeAofAddress 同为
      // 发起时 TailAddress）；快照已提交，补写失败即向上传播不静默
      wcpr::publish_checkpoint_aof_address(
        &db.checkpoint_dir,
        token,
        covered.get(0).unwrap_or_default() as u64,
      )
      .await?;
    }

    // 截断层按形态二分（对标 C# :536 的 EnableCluster && EnableAOF 与
    // else 分支层级：C# 该段与 AppendOnlyFile 判空正交，rust 集群形态恒启
    // AOF——复制域依赖 AOF 流，句柄在位即 C# 该合取式为真）：
    // 集群句柄在位 → 复制域登记 CheckpointEntry 历史并经 SafeTruncateAOF
    // 截断（full 恒 true 对齐 rust 统一检查点模型，与副本 attach 按需链
    // take_on_demand_checkpoint 同口径）；否则 TruncateUntil + Commit
    //（C# else 分支，AppendOnlyFile 判空等价）。物理回收真身仍只有
    // truncate_until 一处（safe_truncate_aof 内部亦走它），不造第二套
    if let Some(cluster) = self.cluster.get() {
      if let Some(slow) = cluster.add_new_checkpoint_entry(true, covered, token, token) {
        slow.await;
      }
    } else if let Some(aof) = &db.aof {
      aof.truncate_until_async(&covered).await;
      aof.commit_flush_async().await;
    }

    db.update_last_save(now_ms());

    // 快照保留回收（对标 C# 检查点状态机 REST 段的 CleanupIndexCheckpoint /
    // CleanupLogCheckpoint 环形 tokenHistory 拍新删旧）：仅单机形态执行——
    // `cluster` 句柄缺位即 C# 侧 removeOutdated = !EnableCluster 为真的同一
    // 形态位（与上方版本切换标记同源判据）。集群形态下快照淘汰由复制域
    // CheckpointStore 读者闸门单轨接管（它按在途读者停手，本处的按条数纯
    // unlink 无此感知力，两轨并行必误删在传快照），故此处让位不触发。
    //
    // SAVE / BGSAVE / AOF 体积超限三条生产链共走本内核，不回收即检查点目录
    // 随打点次数单调增长直至磁盘耗尽；失败只 warn 不回滚已发布快照（C# 的
    // 设备删除同为 best-effort），下一轮检查点再收。回收面只覆盖已发布
    // Token（meta 未落的在途快照不在 list 内），并发下一轮的半成品不受波及
    if self.cluster.get().is_none()
      && let Err(e) = wcpr::purge_outdated(&db.checkpoint_dir, CHECKPOINT_RETAIN_GENERATIONS)
    {
      log::warn!(
        "Failed purging outdated checkpoints in {}: {e}",
        db.checkpoint_dir.display()
      );
    }

    Ok(true)
  }

  /// 按需检查点（管理器入口）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:TakeOnDemandCheckpointAsync
  pub async fn take_on_demand_checkpoint_async(
    &self,
    db: &GarnetDatabase<D>,
    entry_ms: u64,
  ) -> wkv::Result<()> {
    if db.last_save_ms() < entry_ms {
      self.take_database_checkpoint_async(db).await?;
    }
    Ok(())
  }

  /// 索引溢出超阈值时执行单索引扩容判定与动作（单索引判定内核）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:GrowIndexIfNeededAsync
  ///
  /// 判定 `index_size < index_max_size 且 overflow_count > index_size * threshold / 100`。
  /// 若满足条件则触发翻倍扩容回调。返回索引是否已达上限（`index_size >= index_max_size`）。
  pub async fn grow_index_if_needed<F, G>(
    &self,
    index_max_size: usize,
    overflow_count: u64,
    resize_threshold: i64,
    index_size_retriever: F,
    grow_action: G,
  ) -> wkv::Result<bool>
  where
    F: Fn() -> usize,
    G: Future<Output = wkv::Result<bool>>,
  {
    let current_size = index_size_retriever();
    log::debug!(
      "IndexAutoGrowTask: checking index size {current_size} against max {index_max_size} with overflow {overflow_count}"
    );

    // u128 中间量：threshold 为 i64 CLI 直通（极端值 × usize 索引规模在
    // u64 域回绕出小阈值导致误扩容），u128 乘积域恒不溢出
    let threshold = resize_threshold.max(0) as u64;
    let thresholded_overflow = u128::from(current_size as u64) * u128::from(threshold) / 100;
    if current_size < index_max_size && u128::from(overflow_count) > thresholded_overflow {
      log::info!(
        "IndexAutoGrowTask: overflowCount {overflow_count} ratio more than threshold {threshold}%. Doubling index size..."
      );
      grow_action.await?;
    }

    let final_size = index_size_retriever();
    if final_size < index_max_size {
      return Ok(false);
    }

    // 扩容后复核：确认最终规模确已触及上限（与入场检查日志区分语义）
    log::debug!(
      "IndexAutoGrowTask: post-grow recheck: index size {final_size} reached max {index_max_size} with overflow {overflow_count}"
    );
    Ok(true)
  }

  /// 检查并按需扩容指定数据库的主存储索引
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:GrowIndexesIfNeededAsync
  pub async fn grow_indexes_if_needed(
    &self,
    db: &GarnetDatabase<D>,
    index_max_size: usize,
    resize_threshold: i64,
  ) -> wkv::Result<bool> {
    if db.store_index_maxed_out.load(Acquire) {
      return Ok(true);
    }

    let store = &db.store;
    let overflow_count = store.active_index().overflow_pool.allocated_count();
    let maxed_out = self
      .grow_index_if_needed(
        index_max_size,
        overflow_count,
        resize_threshold,
        || store.active_index().size,
        // 扩容含纪元排空忙等与全量分块迁移自旋（大索引秒级），经 compio
        // spawn_blocking 卸载至阻塞线程，reactor 宿主核继续调度其他任务
        grow_index_blocking(Arc::clone(store)),
      )
      .await?;

    if maxed_out {
      db.store_index_maxed_out.store(true, Release);
    }

    Ok(maxed_out)
  }

  /// AOF 提交：物理刷盘推进 committed_until 至 safe_tail
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:CompactionCommitAofAsync
  ///
  /// C# 该内核与 Single/MultiDatabaseManager.CommitToAofAsync 共同动作即
  /// `db.AppendOnlyFile.Log.CommitAsync()`（物理刷盘推进提交位点，不动
  /// LastSave 系——LastSaveStoreTailAddress 仅检查点域推进），rust 合一处
  /// 内核供检查点 compaction 与命令通道（COMMITAOF）复用
  pub async fn commit_aof(&self, db: &GarnetDatabase<D>) -> wkv::Result<()> {
    if let Some(aof) = &db.aof {
      aof.commit_flush_async().await;
    }
    Ok(())
  }

  /// 全库清空数据（O(1) 物理截断 + AOF 截断至尾）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:FlushAllDatabases
  ///
  /// 原按库 [`wkv::WedbStore::flush_database`] 截断形态（ns=0 硬编码 +
  /// truncate_aof 门控）已并入 [`super::single_database_manager::SingleDatabaseManager`]
  /// flush 三入口（换号清库 + SafeFlushAOF 广播；共享单 AOF 换号路径不物理
  /// 截断，旧记录经逻辑条目 + 延时 GC 承接，C# 集群 safeTruncateAof=false
  /// 分支同形）。SWAPDB 搬移走 wkv `StoreSession::swap_databases` 内核
  /// （经写端口镜像 AOF，绝不截断），不经此路。
  pub async fn flush_all_databases(&self, db: &GarnetDatabase<D>) -> wkv::Result<()> {
    db.store.flush_all_databases().await?;
    if let Some(aof) = &db.aof {
      let until = AofAddress::create(1, aof.tail_address());
      aof.truncate_until_async(&until).await;
    }
    Ok(())
  }

  /// 重置数据库（拆除重建族：数据清空 + AOF 位点归零 + 复位保存点）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:ResetDatabase
  ///
  /// C# 为 `db.Store.Reset()`（TailAddress > 64 时日志地址归零、分配器
  /// 拆除重建）、`Aof Log.Reset()`（位点归零）与 LastSave 归零；rust
  /// 共享存储模型下数据清空等价为 wkv O(1) 虚拟换号，语义差
  /// 由 AOF 段承载——[`GarnetAppendOnlyFile::reset_async`] 位点归零（对标
  /// `Log.Reset()`），区别于 [`Self::flush_all_databases`] 的截断至尾。
  pub async fn reset_database(&self, db: &GarnetDatabase<D>) -> wkv::Result<()> {
    db.store.flush_database(0, db.id.max(0) as u64).await?;
    if let Some(aof) = &db.aof {
      aof.reset_async().await;
    }
    db.last_save_ms.store(0, Relaxed);
    db.last_save_store_tail_address.store(0, Release);
    db.store_index_maxed_out.store(false, Release);
    Ok(())
  }

  /// 采集单库混合日志内存分布扫描
  ///
  /// 在 garnet 中的相对路径: libs/server/Databases/DatabaseManagerBase.cs:CollectHybridLogStatsForDb
  ///
  /// C# 对主/对象存储各扫一遍（`CollectHybridLogStats(db, db.Store, ...)`）；
  /// wedb 单物理日志 + wcol 信封统一值域，仅 main store 形态，扫描内核
  /// 收敛在 [`WedbStore::hlog_scan_metrics`]（区域 × 状态 × (条数, 字节)）。
  pub async fn collect_hybrid_log_stats_for_db(
    &self,
    db: &GarnetDatabase<D>,
  ) -> wkv::Result<wkv::HybridLogScanMetrics> {
    db.store.hlog_scan_metrics().await
  }

  /// 全库混合日志内存分布统计（基座视角：单库一份）
  ///
  /// 转发至 wkv 扫描内核 [`wkv::WedbStore::hlog_scan_metrics`]，C#
  /// CollectHybridLogStats 的严格映射单点随内核。
  pub async fn collect_hybrid_log_stats(
    &self,
    db: &GarnetDatabase<D>,
  ) -> wkv::Result<Vec<(i64, wkv::HybridLogScanMetrics)>> {
    Ok(vec![(
      db.id,
      self.collect_hybrid_log_stats_for_db(db).await?,
    )])
  }
}
