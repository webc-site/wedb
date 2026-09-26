//! 单库管理器（对标 libs/server/Databases/SingleDatabaseManager.cs）
//!
//! 固定 DB 0 的管理模式：检查点 / AOF / 恢复全部落在唯一数据库上。

use std::{
  io,
  path::PathBuf,
  sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, Ordering::Relaxed},
  },
  thread::yield_now,
};

use parking_lot::Mutex;
use waof::AofEntryType;
use wbase::hash_slot::slot_of;
use wdev::Device;
use wkv::{CollectionError, Error, WedbStore};

use super::{
  database_manager_base::DatabaseManagerBase, garnet_database::GarnetDatabase,
  i_database_manager::IDatabaseManager,
};
use crate::{
  aof::garnet_append_only_file::GarnetAppendOnlyFile,
  cluster_provider::ClusterProviderHandle,
  primary_tasks::PrimaryTasks,
  resp::{
    slow_path::SlowFuture,
    vector::{
      vector_manager::{RegistryReclaim, VectorManager},
      vector_registry_recovery::rebuild_registry_from_store,
    },
  },
};

/// 恢复收口的后台静默等待上限（C# RecoverVectorSets → WaitForQuiescence 为
/// 无限等待；本仓以超时上限防呆，到期返回 false 仅告警不阻塞启动）。
const RECOVER_VECTOR_QUIESCENCE_TIMEOUT_MS: u64 = 30_000;

/// 待补投的 FLUSH 族 AOF 广播条目（safe_flush_aof 入队失败补偿）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingFlushAof {
  pub op_type: AofEntryType,
  pub unsafe_truncate_log: bool,
  pub ns: u64,
  pub db: u64,
}

/// 单库管理器
pub struct SingleDatabaseManager<D: Device> {
  /// 共享基座（检查点管理器等）
  pub base: DatabaseManagerBase<D>,
  /// 唯一数据库（DB 0）
  pub db: Arc<GarnetDatabase<D>>,
  /// 清库广播门（集群提供者句柄，装配期注入；None = 单机形态恒视为主库，
  /// 对标 C# standalone provider IsPrimary == true）
  flush_gate: OnceLock<ClusterProviderHandle>,
  /// AOF 体积超限检查点的副本角色门（PrimaryTasks 角色域，装配期注入；
  /// None = 裸构造形态恒视为主角色，与 flush_gate 缺省语义一致）。
  /// 角色位取 PrimaryTasks.replica 而非 ClusterProvider::is_replica()：
  /// 前者挂起点更宽（全量同步 attach、REPLICAOF 指向主端等窗口内 config
  /// 角色可能仍是主），换成后者会放宽保护，副本截断本地 AOF 直接破坏
  /// 主从推流衔接
  primary_tasks: OnceLock<Arc<PrimaryTasks>>,
  /// 向量集合管理器（FLUSH 族登记表域回收联动用，装配期注入；None =
  /// 向量预览未点亮，回收静默跳过）
  vector_manager: OnceLock<Arc<VectorManager>>,
  /// 待补投 FLUSH 族广播条目（safe_flush_aof 失败补偿队列）
  pending_flush_aof: Mutex<Vec<PendingFlushAof>>,
  /// 仅测试用：注入下一次 safe_flush_aof 入队失败（单次有效）
  enqueue_fault_injected: AtomicBool,
}

impl<D: Device> SingleDatabaseManager<D> {
  /// 创建单库管理器
  pub fn new(checkpoint_dir: PathBuf, db: Arc<GarnetDatabase<D>>) -> Self {
    Self {
      base: DatabaseManagerBase::new(checkpoint_dir),
      db,
      flush_gate: OnceLock::new(),
      primary_tasks: OnceLock::new(),
      vector_manager: OnceLock::new(),
      pending_flush_aof: Mutex::new(Vec::new()),
      enqueue_fault_injected: AtomicBool::new(false),
    }
  }

  /// 注入清库广播门（集群装配期一次；boot.rs set_store 同点时序）
  ///
  /// 同一句柄双用：清库广播门（本管理器 flush_gate）与检查点版本切换标记出口
  /// （base.attach_cluster_provider，对标 C# provider 既是 flush 门控源又是
  /// checkpointVersionShift 委托宿主）
  pub fn attach_flush_gate(&self, gate: ClusterProviderHandle) {
    self.base.attach_cluster_provider(gate.clone());
    let _ = self.flush_gate.set(gate);
  }

  /// 注入副本角色门（服务装配期一次；from_parts 与 AOF 点亮、--recover
  /// 重放两处装点注入同一 Arc，保证任何装配形态下臂内读到同一角色位）
  pub fn attach_primary_tasks(&self, tasks: Arc<PrimaryTasks>) {
    let _ = self.primary_tasks.set(tasks);
  }

  /// 注入向量集合管理器（服务装配期一次；FLUSH 族域回收联动）
  pub fn attach_vector_manager(&self, vm: Arc<VectorManager>) {
    let _ = self.vector_manager.set(vm);
  }

  /// 获取关联的向量集合管理器（若已装配）
  pub fn try_vector_manager(&self) -> Option<Arc<VectorManager>> {
    self.vector_manager.get().cloned()
  }

  /// 登记表域回收联动（域值与广播条目载荷同源；未注入即静默跳过。
  /// 摘除臂为真异步：逐键条带独占锁 + 登记写透 `.await`，无内联收割）
  async fn reclaim_registry(&self, reclaim: RegistryReclaim) {
    if let Some(vm) = self.vector_manager.get() {
      vm.reclaim_registry_domain(reclaim).await;
    }
  }

  /// 当前是否副本角色（角色门缺省裸构造 = 主角色）
  fn is_replica(&self) -> bool {
    self
      .primary_tasks
      .get()
      .is_some_and(|tasks| tasks.is_replica())
  }

  /// 是否主库（广播门缺省单机 = true）
  fn is_primary(&self) -> bool {
    self.flush_gate.get().is_none_or(|gate| gate.is_primary())
  }

  /// 检查设备是否处于污染状态（副本检查点接收失败等导致的底层存储损坏）
  #[inline]
  pub fn is_device_contaminated(&self) -> bool {
    self
      .flush_gate
      .get()
      .is_some_and(|gate| gate.is_device_contaminated())
  }

  /// 仅测试用：注入下一次 safe_flush_aof 入队失败（单次有效）
  #[inline]
  pub fn set_enqueue_fault_injected(&self, inject: bool) {
    self.enqueue_fault_injected.store(inject, Relaxed);
  }

  /// 待补投条目数（safe_flush_aof 失败补偿账本长度）
  #[inline]
  pub fn pending_flush_aof_count(&self) -> usize {
    self.pending_flush_aof.lock().len()
  }

  /// 尝试排空先前入队失败的待补投 Flush 广播条目（对标 safe_flush_aof 失败补偿）
  pub fn drain_pending_flush_aof(&self) -> wkv::Result<()> {
    if let Some(aof) = &self.db.aof {
      self.drain_pending_flush_aof_inner(aof)?;
    }
    Ok(())
  }

  fn drain_pending_flush_aof_inner(&self, aof: &GarnetAppendOnlyFile) -> wkv::Result<()> {
    let mut pending = self.pending_flush_aof.lock();
    if pending.is_empty() {
      return Ok(());
    }
    let is_primary = self.is_primary();
    while let Some(&entry) = pending.first() {
      match aof.enqueue_safe_flush_aof_if_primary(
        is_primary,
        entry.op_type,
        entry.unsafe_truncate_log,
        entry.ns,
        entry.db,
      ) {
        Ok(()) => {
          pending.remove(0);
        }
        Err(e) => {
          return Err(Error::AofEnqueue(e.to_string()));
        }
      }
    }
    Ok(())
  }

  /// libs/server/Databases/SingleDatabaseManager.cs:SafeFlushAOF（Enqueue 段）
  ///
  /// 清库执行段后原子补写 FLUSH 族广播条目：仅主库入队（副本经回放条目
  /// 承接清库，绝不二次入队），载荷 (ns, db) 与数据条目物理键前缀同域。
  /// 入队失败时先同步重试，若仍失败则把条目记录至待补投账本（pending_flush_aof），
  /// 确保重试或后续管理命令时补投该广播条目，消灭死域无法在副本侧回收的漏洞；
  /// 最终以 AofEnqueue 上抛拒绝命令（调用方不得静默吞错）。
  ///
  /// C# SafeFlushAOF 的无条件 SafeTruncateAOF 截断段（主副均做）在本层不重复
  /// 出现：FLUSHALL 由 flush_all_databases 的「先截断后入队」承接；Enqueue 段
  /// 对偶即本函数（AOF 侧助手 enqueue_safe_flush_aof_if_primary 为其内联
  /// IsPrimary 门控臂，非独立函数级映射）。
  fn safe_flush_aof(
    &self,
    op_type: AofEntryType,
    unsafe_truncate_log: bool,
    ns: u64,
    db: u64,
  ) -> wkv::Result<()> {
    if let Some(aof) = &self.db.aof {
      let is_primary = self.is_primary();
      // 先尝试清偿先前残留的待补投条目
      let _ = self.drain_pending_flush_aof_inner(aof);

      // 测试故障注入点：单次模拟 AOF 入队失败
      if self.enqueue_fault_injected.swap(false, Relaxed) {
        self.pending_flush_aof.lock().push(PendingFlushAof {
          op_type,
          unsafe_truncate_log,
          ns,
          db,
        });
        return Err(Error::AofEnqueue(
          "injected safe_flush_aof failure".to_string(),
        ));
      }

      // 同步重试至上限
      const MAX_RETRIES: usize = 3;
      let mut last_err = None;
      for attempt in 0..MAX_RETRIES {
        match aof.enqueue_safe_flush_aof_if_primary(
          is_primary,
          op_type,
          unsafe_truncate_log,
          ns,
          db,
        ) {
          Ok(()) => {
            let _ = self.drain_pending_flush_aof_inner(aof);
            return Ok(());
          }
          Err(e) => {
            last_err = Some(e);
            if attempt + 1 < MAX_RETRIES {
              yield_now();
            }
          }
        }
      }

      // 入队失败：登记待补投条目，防止 (ns, db) 广播永久缺失
      let err = last_err.expect("重试循环必有 last_err");
      self.pending_flush_aof.lock().push(PendingFlushAof {
        op_type,
        unsafe_truncate_log,
        ns,
        db,
      });
      return Err(Error::AofEnqueue(err.to_string()));
    }
    Ok(())
  }

  /// 单库 TryGetOrAddDatabase（恒 db0，不新建）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:TryGetOrAddDatabase
  /// libs/server/StoreWrapper.cs:TryGetOrAddDatabase
  ///（StoreWrapper 转发折叠：多库兼容门随双轨折叠删除，单库恒 db0 即唯一落点）
  pub fn try_get_or_add_database(&self) -> wkv::Result<(Arc<GarnetDatabase<D>>, bool)> {
    self.base.try_get_or_add_database(&self.db)
  }

  /// 单库 TryGetDatabase（恒 db0）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:TryGetDatabase
  /// libs/server/Databases/DatabaseManagerBase.cs:TryGetDatabase
  /// libs/server/StoreWrapper.cs:TryGetDatabase
  ///（基类抽象声明与 StoreWrapper 转发折叠：StoreWrapper 层的
  /// CheckMultiDatabaseCompatibility 多库兼容门随双轨折叠删除，单库恒 db0
  /// 即唯一落点）
  pub fn try_get_database(&self) -> Option<Arc<GarnetDatabase<D>>> {
    Some(Arc::clone(&self.db))
  }

  /// 单库在线引擎换持：管理面引擎引用与数据面置换同步更新
  ///
  /// C# 原型无对位换持口——副本全量恢复经 `Store.RecoverAsync` 原位重构，
  /// `StoreWrapper.Store => databaseManager.Store`（StoreWrapper.cs:41）
  /// 计算属性天然转发同一实例；rust 恢复产出全新 [`WedbStore`] 实例
  ///（replica_diskbased_sync 换库段），管理面引用经本口单点换指，后续
  /// 检查点/清库/索引扩容/统计即作用于恢复后的最新在线引擎
  pub fn swap_store(&self, store: Arc<WedbStore<D>>) {
    self.db.swap_store(store);
  }

  /// 上次保存时间（毫秒 Unix 时间戳，0 表示从未保存）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:LastSaveTime
  #[inline]
  pub fn last_save_ms(&self) -> u64 {
    self.db.last_save_ms()
  }

  /// 单库暂停检查点
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:TryPauseCheckpoints
  /// libs/server/StoreWrapper.cs:TryPauseCheckpoints
  ///（StoreWrapper 转发折叠：多库兼容门随双轨折叠删除，单库闸即唯一落点）
  pub fn try_pause_checkpoints(&self) -> bool {
    self.base.try_pause_checkpoints(&self.db)
  }

  /// 单库恢复检查点调度
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:ResumeCheckpoints
  /// libs/server/StoreWrapper.cs:ResumeCheckpoints
  ///（StoreWrapper 转发折叠：多库兼容门随双轨折叠删除）
  pub fn resume_checkpoints(&self) {
    self.base.resume_checkpoints(&self.db);
  }

  /// 重置复活化统计
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:ResetRevivificationStats
  /// libs/server/Databases/DatabaseManagerBase.cs:ResetRevivificationStats
  ///（抽象基类声明，rust 双轨折叠：单库实现即唯一落点）
  ///
  /// 单库形态唯一存储句柄直下（C# `Store.ResetRevivificationStats()` 同构）：
  /// 复活账目在 wkv 侧由 `WedbStore::reviv_pool`（wreviv FreeRecordPool 四计数）
  /// 单一承载，本口即 INFO RESETSTAT 的 reviv 臂落点
  pub fn reset_revivification_stats(&self) {
    self.db.store().reset_revivification_stats();
  }

  /// 单库快照
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:GetDatabasesSnapshot
  /// libs/server/Databases/DatabaseManagerBase.cs:GetDatabasesSnapshot
  ///（抽象基类声明，rust 双轨折叠：单库实现即唯一落点）
  pub fn get_databases_snapshot(&self) -> Vec<Arc<GarnetDatabase<D>>> {
    vec![Arc::clone(&self.db)]
  }

  /// 单库恢复向量集合（C# RecoverVectorSets 全对位：扫描回建登记表与
  /// 上下文元数据 → ReconcileRecoveredState 收口 → WaitForQuiescence）  ///
  /// 全区间扫描恢复出的日志，把 [`KeyTag::VectorRegistry`] 旁路记录按强类型
  /// 子标签解出后直接喂对应恢复方法暂存（C# 恢复趟 OnRecoverySnapshotRead
  /// 逐记录喂 SanitizeAndTrackIngestedRecordIfApplicable 的 rust 对位形态：
  /// rust 无 Tsavorite 记录头，判别由 VectorRegistrySubTag 承接），再经
  /// [`VectorManager::reconcile_recovered_state`] 一次性还原元数据数组并
  /// 清理未恢复上下文。向量预览未点亮（管理器未注入）时返回 0。
  ///
  /// 返回恢复出的向量集登记条数（C# 返回 void，计数为本仓观测扩展）。
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:RecoverVectorSets
  /// libs/server/Databases/DatabaseManagerBase.cs:RecoverVectorSets
  ///（抽象基类声明，rust 双轨折叠：单库实现即唯一落点）
  pub async fn recover_vector_sets(&self) -> wkv::Result<u64> {
    let Some(vm) = self.vector_manager.get() else {
      return Ok(0);
    };
    let Some(recovered) = rebuild_registry_from_store(&self.db.store(), vm.as_ref()).await else {
      return Err(Error::Collection(CollectionError::Corrupted(
        "向量集合登记回建失败：记录损坏或自备会话资源不可得",
      )));
    };
    if !vm.reconcile_recovered_state(false).await {
      return Err(Error::Collection(CollectionError::Corrupted(
        "向量集合恢复收口失败：非空校验或恢复簿记不一致",
      )));
    }
    // 恢复未完成的清理与量化静默等待（C# RecoverVectorSets → WaitForQuiescence
    // 的无限等待对位；超时上限为本仓防呆扩展，到期仅告警不阻塞启动）
    vm.wait_for_quiescence(RECOVER_VECTOR_QUIESCENCE_TIMEOUT_MS);
    Ok(recovered as u64)
  }
}

impl<D: Device> SingleDatabaseManager<D> {
  /// 检查点收口入口：暂停闸内推进（SAVE/BGSAVE、副本重放钩子、集群按需
  /// 重拍、[`IDatabaseManager::take_checkpoint_async`] 全部经此走闸）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:TakeCheckpointAsync
  /// libs/server/StoreWrapper.cs:TakeCheckpointAsync
  ///（StoreWrapper 转发折叠：SAVE/BGSAVE 慢路径
  /// `StoreGarnetApi::checkpoint_command_slow` 直调本收口入口，多库兼容门
  /// 随双轨折叠删除）
  /// 保留 _background 形参以匹配 SingleDatabaseManager.TakeCheckpointAsync 签名规范；
  /// C# 尾段 RunPostCheckpointCleanup 仅清对象堆序列化区间（rust 统一检查点
  /// 模型无对位）。快照磁盘淘汰按形态二分且两轨互斥（对标 C# removeOutdated =
  /// !EnableCluster）：单机形态由检查点内核尾段按代回收（其第 7 步），集群
  /// 形态归复制面 CheckpointStore 读者闸门
  ///
  /// 走闸形态（C# 同名法 :122 同位）：占闸失败即 `Ok(false)`（C#
  /// `if (!TryPauseCheckpoints) return false`，StoreWrapper.cs:414 契约
  /// "False if another checkpointing process is already in progress"；
  /// RESP 层映射 already-in-progress 应答）→ 闸内推进 → 无条件还闸（C#
  /// helper finally ResumeCheckpoints 同位）。占闸失败绝不排队等待：闸
  /// 持有方可能是外部 TryPauseCheckpoints（运维暂停面），还闸时点不受命
  /// 令侧控制，让渡等待即无上界死等。互斥覆盖快照段之外的版本推进、移
  /// 位标记入账与 AOF 截断（wcpr 实例闸 `acquire` 只串行快照段，见
  /// [`DatabaseManagerBase::take_database_checkpoint_async`] 两层闸门
  /// 关系说明），语义判据：任何两检查点（不论入口）不得并发推进版本/
  /// 入账移位标记/截断。让渡重试（C# :152 `while (!TryPauseCheckpoints)
  /// await Task.Yield()`）只属按需入口
  /// [`IDatabaseManager::take_on_demand_checkpoint_async`]；AOF 限长任务
  /// 「占用即轮空」走显式对
  /// （[`Self::task_checkpoint_based_on_aof_size_limit_async`]）
  /// 设备污染即拒（检查点/清库族共用判据与文案单点，禁逐臂二写）
  fn ensure_device_clean(&self, what: &str) -> wkv::Result<()> {
    if self.is_device_contaminated() {
      return Err(Error::Io(io::Error::other(format!(
        "Device is contaminated by a failed checkpoint receive, refusing {what}"
      ))));
    }
    Ok(())
  }

  pub async fn take_checkpoint(&self, _background: bool) -> wkv::Result<bool> {
    self.ensure_device_clean("checkpoint")?;
    if !self.try_pause_checkpoints() {
      return Ok(false);
    }
    self.take_checkpoint_within_gate().await
  }

  /// 闸内推进段（调用方已占闸成功）：推进 + 无条件还闸
  ///
  /// C# TakeCheckpointAsync 的 helper 段（try { 检查点 } finally {
  /// ResumeCheckpoints }）同位：占闸与推进段拆开，为 BGSAVE 在占闸成功
  /// 的同步窗口内即回成功应答、推进段转 spawn 后台承接（resp 层 slow.rs
  /// 分派处；对标 C# background=true 不 await helper 即返 true）
  pub async fn take_checkpoint_within_gate(&self) -> wkv::Result<bool> {
    if let Err(e) = self.ensure_device_clean("checkpoint") {
      self.resume_checkpoints();
      return Err(e);
    }
    let _ = self.drain_pending_flush_aof();
    let ret = self.base.take_database_checkpoint_async(&self.db).await;
    self.resume_checkpoints();
    ret
  }

  /// 占闸：CAS 失败让渡重试至拿到（C# TakeOnDemandCheckpointAsync :152
  /// `while + Task.Yield` 让渡形态，唯一使用方即该按需入口）
  ///
  /// event_listener 挂起等待（与 wcpr 实例闸 `acquire` 同构）：注册监听后
  /// CAS 复查防漏通知，还闸方 [`Self::resume_checkpoints`] 精准唤醒一个
  /// 等待者；唤醒链闭环——每个占闸成功者必经 [`Self::resume_checkpoints`]
  /// 还闸并续唤醒下一个，等待上界即持有者一轮检查点，无自旋轮询
  async fn acquire_checkpoint_gate(&self) {
    loop {
      if self.try_pause_checkpoints() {
        return;
      }
      let listener = self.db.checkpoint_gate_resume.listen();
      if self.try_pause_checkpoints() {
        return;
      }
      listener.await;
    }
  }

  /// 暂停闸门内的 AOF 超限打点段（副本角色门 + 检查点内核）
  ///
  /// 承接 C# 臂内 try 块闸门段（映射挂在
  /// [`Self::task_checkpoint_based_on_aof_size_limit_async`]，此处不重复
  /// 登记）。独立函数即 C# try 块形态：早退（副本轮空）不跨还闸——还闸
  /// 由调用方无条件执行。LastSave 时间戳回填由内核
  /// update_last_save 单点承接，此处不重复
  async fn checkpoint_within_pause_gate(
    &self,
    aof_size: u64,
    aof_size_limit: u64,
  ) -> wkv::Result<bool> {
    self.ensure_device_clean("checkpoint")?;
    // 检查点由 AOF 重放侧触发的窗口（副本角色）绝不本地打点
    if self.is_replica() {
      log::info!("Replica skipping TaskCheckpointBasedOnAofSizeLimitAsync");
      return Ok(false);
    }
    log::info!(
      "Enforcing AOF size limit currentAofSize: {aof_size} > AofSizeLimit: {aof_size_limit}"
    );
    self.base.take_database_checkpoint_async(&self.db).await
  }

  /// 等待单库 AOF 提交完成（事件驱动无锁等待）。提交失败上浮（C#
  /// SingleDatabaseManager.cs:240-243 WaitForCommitToAofAsync 无捕获穿透）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:WaitForCommitToAofAsync
  pub async fn wait_for_commit_to_aof_async(&self) -> waof::Result<bool> {
    if let Some(aof) = &self.db.aof {
      aof.log().wait_for_commit_all_async(0).await?;
    }
    Ok(true)
  }

  /// 单库恢复 AOF
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:RecoverAOFAsync
  /// libs/server/StoreWrapper.cs:RecoverAOFAsync
  ///（StoreWrapper 转发折叠：恢复装配 `StorageSessionProvider::
  /// open_recovered_with_config_and_aof` 亦经此同链）
  pub async fn recover_aof(&self) -> wkv::Result<u64> {
    self.base.recover_database_aof_async(&self.db).await
  }

  /// 单库重放 AOF
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:ReplayAOF
  /// libs/server/StoreWrapper.cs:ReplayAOF
  ///（StoreWrapper 转发折叠：恢复装配全量重放臂同链）
  pub async fn replay_aof(&self, until: u64) -> wkv::Result<u64> {
    self.base.replay_database_aof(&self.db, until).await
  }

  /// 单库清空（换号 + 广播族）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:FlushDatabase
  /// （接口成员 libs/server/Databases/IDatabaseManager.cs:FlushDatabase 即本函数承接，rust 不在
  /// [`IDatabaseManager`](super::i_database_manager::IDatabaseManager) 上另开
  /// 零参投影臂）
  ///
  /// RESP FLUSHDB 主路径唯一清库入口（慢路径执行段
  /// `StoreGarnetApi::flush_command_slow`）：
  ///
  /// C# `FlushDatabase(unsafeTruncateLog, dbId)` + `SafeFlushAOF(FlushDb, ...)`：
  /// wkv O(1) 虚拟换号清库后原子补写 FlushDb 广播条目（仅主库入队），载荷
  /// (vns, 旧 vdb) 与数据条目物理键前缀同域。rust 共享单 AOF 不物理截断
  /// （换号模型旧数据由延时 GC 回收，截断将抹除其他库未检查点历史；C# 集群
  /// safeTruncateAof == false 分支同形）。换号映射批次（新 DbMap + 旧域墓碑 +
  /// 分配水位）经 commit_swap 落盘时镜像为 DbMeta 条目先行入队，副本回放按
  /// 主库号直设映射格，绝无本地二次取号（doc/zh/db.md 主从映射继承）
  pub async fn flush_database(
    &self,
    ns: u64,
    db_id: u64,
    unsafe_truncate_log: bool,
  ) -> wkv::Result<()> {
    self.ensure_device_clean("flush")?;
    let (vns, domain_db) = self.db.store().flush_database(ns, db_id).await?;
    let slot = slot_of(ns, db_id);
    let reclaim = RegistryReclaim::Database {
      vns,
      vdb: domain_db,
      slot: Some(slot),
    };
    // 换号回收：死亡域 (vns, 旧 vdb) 的登记条目与底层 context 一并联动清理（AOF 前首轮）
    self.reclaim_registry(reclaim).await;
    self.safe_flush_aof(AofEntryType::FlushDb, unsafe_truncate_log, vns, domain_db)?;
    // AOF 提交与广播后二次回收扫尾：捕获换号至落盘窗口期落表的迟到条目
    self.reclaim_registry(reclaim).await;
    Ok(())
  }

  /// 清空整命名空间（多租户隔离清库 + 广播）
  ///
  /// C# 无 ns 维度（每实例单租户）；rust 非 0 租户 FLUSHALL 执行段，
  /// FlushNs 广播条目携带 (旧 vns, 0) 域载荷
  pub async fn flush_namespace(&self, ns: u64, unsafe_truncate_log: bool) -> wkv::Result<()> {
    self.ensure_device_clean("flush")?;
    let (domain_vns, _) = self.db.store().flush_namespace(ns).await?;
    let reclaim = RegistryReclaim::Namespace { vns: domain_vns };
    self.reclaim_registry(reclaim).await;
    self.safe_flush_aof(AofEntryType::FlushNs, unsafe_truncate_log, domain_vns, 0)?;
    self.reclaim_registry(reclaim).await;
    Ok(())
  }

  /// 全租户换号广播门转发（doc/zh/db.md 4.5 集群总线广播）
  ///
  /// None = 单机形态（flush_gate 未装配，本地换号即完成）；Some 为协调者
  /// 应答闭环字节（收齐全部 Primary ack 才 +OK，失败回错误），入口以
  /// 其结果作为客户端最终应答，严禁先 +OK 再异步广播
  pub fn flushall_broadcast(&self, ns: u64) -> Option<SlowFuture> {
    self
      .flush_gate
      .get()
      .and_then(|gate| gate.flushall_broadcast(ns))
  }

  /// 全部清空（全域物理截断 + 广播）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:FlushAllDatabases
  /// libs/server/StoreWrapper.cs:FlushAllDatabases
  ///（StoreWrapper 转发折叠：FLUSHALL 慢路径执行段直调本口）
  ///
  /// C# `FlushAllDatabases(unsafeTruncateLog)` + `SafeFlushAOF(FlushAll, ...)`：
  /// wkv 物理截断 O(1) 全域清空 + AOF 截断至尾后补写 FlushAll 广播条目
  /// （先截断后入队，与 C# SafeFlushAOF 的 SafeTruncateAOF → Enqueue 同序；
  /// 全域清空无他库存活，共享 AOF 截断安全）
  pub async fn flush_all_databases(&self, unsafe_truncate_log: bool) -> wkv::Result<()> {
    self.ensure_device_clean("flush")?;
    self.base.flush_all_databases(&self.db).await?;
    self.reclaim_registry(RegistryReclaim::All).await;
    self.safe_flush_aof(AofEntryType::FlushAll, unsafe_truncate_log, 0, 0)?;
    self.reclaim_registry(RegistryReclaim::All).await;
    Ok(())
  }

  /// 单库重置（拆除重建族：数据清空 + AOF 位点归零 + 保存点复位）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:Reset
  /// libs/server/Databases/DatabaseManagerBase.cs:Reset
  ///（抽象基类声明，rust 双轨折叠：单库实现即唯一落点）
  pub async fn reset(&self) -> wkv::Result<()> {
    let ret = self.base.reset_database(&self.db).await;
    self.reclaim_registry(RegistryReclaim::All).await;
    ret
  }

  /// 单库混合日志内存分布统计
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:CollectHybridLogStats
  pub async fn collect_hybrid_log_stats(
    &self,
  ) -> wkv::Result<Vec<(i64, wkv::HybridLogScanMetrics)>> {
    self.base.collect_hybrid_log_stats(&self.db).await
  }

  /// 检查并按需自动扩容单库主存储索引
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:GrowIndexesIfNeededAsync
  pub async fn grow_indexes_if_needed(
    &self,
    index_max_size: usize,
    resize_threshold: i64,
  ) -> wkv::Result<bool> {
    self
      .base
      .grow_indexes_if_needed(&self.db, index_max_size, resize_threshold)
      .await
  }

  /// 按需检查点（管理器入口）：闸内按需判定与推进
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:TakeOnDemandCheckpointAsync
  /// libs/server/StoreWrapper.cs:TakeOnDemandCheckpointAsync
  ///（StoreWrapper 转发折叠：多库兼容门随双轨折叠删除）
  /// 同闸同形（:152 `while (!TryPauseCheckpoints) await Task.Yield()` →
  /// 闸内 LastSaveTime 判定与推进 → finally Resume）：last_save 判定移入
  /// 闸内，杜绝判定通过后到持闸前窗口被并发检查点抢先推进的语义回退
  pub async fn take_on_demand_checkpoint_async(&self, entry_ms: u64) -> wkv::Result<bool> {
    self.acquire_checkpoint_gate().await;
    let ret = self
      .base
      .take_on_demand_checkpoint_async(&self.db, entry_ms)
      .await;
    self.resume_checkpoints();
    ret
  }
}

impl<D: Device> IDatabaseManager<D> for SingleDatabaseManager<D> {
  async fn try_get_or_add_database(&self) -> wkv::Result<(Arc<GarnetDatabase<D>>, bool)> {
    SingleDatabaseManager::try_get_or_add_database(self)
  }

  fn try_get_database(&self) -> Option<Arc<GarnetDatabase<D>>> {
    SingleDatabaseManager::try_get_database(self)
  }

  fn last_save_ms(&self) -> u64 {
    SingleDatabaseManager::last_save_ms(self)
  }

  fn try_pause_checkpoints(&self) -> bool {
    SingleDatabaseManager::try_pause_checkpoints(self)
  }

  fn resume_checkpoints(&self) {
    SingleDatabaseManager::resume_checkpoints(self);
  }

  /// 恢复检查点（接口实现；基类抽象与单库具体实现双轨折叠为本落点）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:RecoverCheckpointAsync
  /// libs/server/Databases/SingleDatabaseManager.cs:RecoverCheckpointAsync
  async fn recover_checkpoint_async(
    &self,
    _replica_recover: bool,
    recover_from_token: Option<u128>,
  ) -> wkv::Result<()> {
    // 副本恢复与主恢复同以快照为基准（差异由 AOF 追平承担）
    self
      .base
      .recover_database_checkpoint_async(&self.db, recover_from_token)
      .await
      .map(|_checkpoint| ())
  }

  async fn take_checkpoint_async(&self, background: bool) -> wkv::Result<bool> {
    SingleDatabaseManager::take_checkpoint(self, background).await
  }

  async fn take_on_demand_checkpoint_async(&self, entry_ms: u64) -> wkv::Result<bool> {
    SingleDatabaseManager::take_on_demand_checkpoint_async(self, entry_ms).await
  }

  /// AOF 体积超限检查点唯一驱动入口（service 周期任务循环直调本臂）
  ///
  /// libs/server/Databases/SingleDatabaseManager.cs:TaskCheckpointBasedOnAofSizeLimitAsync
  /// libs/server/Databases/DatabaseManagerBase.cs:TaskCheckpointBasedOnAofSizeLimitAsync（抽象基类段，实现由本臂覆写）
  ///
  /// 四段次序对标 C#：尺寸预判（严格大于才打点，`aofSize <= aofSizeLimit`
  /// 早退，未超限轮不触碰暂停闸门）→ 取暂停闸门 → 闸门内段（副本角色门 +
  /// 打点）→ 无条件还闸。闸门用 try_pause_checkpoints/resume_checkpoints
  /// 显式对（同名 C# 两法）；与收口入口 [`Self::take_checkpoint`] 的分工：
  /// 周期任务「占用即轮空」不排队（闸占用中本轮直接跳过，续等由下一轮周期
  /// 重投承接，避免在 compio 单线程 reactor 上排队堆积），其余生产链一律经
  /// 收口入口让渡等待
  async fn task_checkpoint_based_on_aof_size_limit_async(
    &self,
    aof_size_limit: u64,
  ) -> wkv::Result<()> {
    let aof_size = self.db.aof_size();
    if aof_size <= aof_size_limit {
      return Ok(());
    }
    if !self.try_pause_checkpoints() {
      log::debug!("AofSizeLimitTask: checkpoint in progress, skip this round");
      return Ok(());
    }
    let ret = self
      .checkpoint_within_pause_gate(aof_size, aof_size_limit)
      .await;
    self.resume_checkpoints();
    ret?;
    Ok(())
  }

  /// AOF 提交（接口实现；基类抽象与单库具体实现双轨折叠为本落点）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:CommitToAofAsync
  /// libs/server/Databases/SingleDatabaseManager.cs:CommitToAofAsync
  async fn commit_to_aof_async(&self) -> wkv::Result<()> {
    self.base.commit_aof(&self.db).await
  }

  /// 等待 AOF 提交完成（接口实现；基类抽象声明折叠为本落点）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:WaitForCommitToAofAsync
  async fn wait_for_commit_to_aof_async(&self) -> waof::Result<bool> {
    SingleDatabaseManager::wait_for_commit_to_aof_async(self).await
  }

  /// 恢复 AOF（接口实现；基类抽象声明折叠为本落点）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:RecoverAOFAsync
  async fn recover_aof_async(&self) -> wkv::Result<u64> {
    SingleDatabaseManager::recover_aof(self).await
  }

  /// 重放 AOF（接口实现；基类抽象声明折叠为本落点）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:ReplayAOF
  async fn replay_aof(&self, until: u64) -> wkv::Result<u64> {
    SingleDatabaseManager::replay_aof(self, until).await
  }

  fn reset_revivification_stats(&self) {
    SingleDatabaseManager::reset_revivification_stats(self);
  }

  fn get_databases_snapshot(&self) -> Vec<Arc<GarnetDatabase<D>>> {
    SingleDatabaseManager::get_databases_snapshot(self)
  }

  async fn reset(&self) -> wkv::Result<()> {
    SingleDatabaseManager::reset(self).await
  }

  async fn flush_all_databases(&self) -> wkv::Result<()> {
    SingleDatabaseManager::flush_all_databases(self, false).await
  }

  async fn collect_hybrid_log_stats(&self) -> wkv::Result<Vec<(i64, wkv::HybridLogScanMetrics)>> {
    SingleDatabaseManager::collect_hybrid_log_stats(self).await
  }

  async fn recover_vector_sets(&self) -> wkv::Result<u64> {
    SingleDatabaseManager::recover_vector_sets(self).await
  }

  async fn grow_indexes_if_needed_async(
    &self,
    index_max_size: usize,
    resize_threshold: i64,
  ) -> wkv::Result<bool> {
    SingleDatabaseManager::grow_indexes_if_needed(self, index_max_size, resize_threshold).await
  }
}
