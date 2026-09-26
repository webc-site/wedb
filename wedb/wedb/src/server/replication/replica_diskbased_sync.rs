//! 副本磁盘基恢复闭环（全量同步第三段：接收完成 → 检查点导入 → 在线引擎
//! 置换 → 授予位点应答）
//!
//! 对标 libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs
//! （ReplicationManager partial：TryReplicaDiskbasedRecovery /
//! CreateCheckpointDevice / ShouldInitialize）。

use std::{fs::create_dir_all, sync::Arc};

use waof::AofAddress;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::resp::vector::vector_manager::VectorManager;

use crate::server::{
  cluster_provider::ClusterProvider,
  replication::{
    checkpoint_entry::CheckpointEntry, recovery_status::RecoveryStatus,
    replication_manager::ReplicationManager,
  },
};

/// libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:
/// CreateCheckpointDevice
///
/// 导入文件集物理落点（接收面布局的恢复侧镜像；对标 C# 按类型取设备的
/// 目录语义——STORE_HLOG 即引擎设备真身，index/meta 走检查点目录）：
/// - 引擎设备文件：在线引擎自带（`store.device`），接收面已直写；
/// - index/meta：检查点目录 wcpr 命名，[`wcpr::recover`] 零改名直读。
///
/// C# ShouldInitialize 的段容量初始化不转写：rust 单文件设备无段位图，
/// 容量由恢复元数据 StoreMeta 决定（wkv::from_recovered 契约）。
fn imported_device(store: &WedbStore<SegmentedDevice>) -> Arc<SegmentedDevice> {
  Arc::clone(&store.device)
}

/// 副本恢复请求束（C# TryReplicaDiskbasedRecovery 的 in/ref 参数面收敛）
#[derive(Debug, Clone)]
pub struct ReplicaRecoverRequest {
  /// 是否从接收 token 文件集恢复引擎（C# recoverStoreFromToken）
  pub recover_store_from_token: bool,
  /// AOF 回放掩码（C# replayAOFMap；rust 主端恒发 0）
  pub replay_aof_map: u64,
  /// 主复制 ID（C# primaryReplicaId）
  pub primary_repl_id: String,
  /// 远端检查点条目（C# remoteCheckpoint）
  pub remote_entry: CheckpointEntry,
  /// 快照覆盖区间下界（C# beginAddress）
  pub begin_address: AofAddress,
  /// 授予副本的复制位点（C# recoveredReplicationOffset）
  pub tail_address: AofAddress,
}

/// libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:
/// TryReplicaDiskbasedRecovery
///
/// 副本检查点导入闭环：
/// 1. 恢复门控：此刻 curr 已是 ClusterReplicate（attach 链全程持锁——命令 /
///    重连臂由 try_add_replica_async 握、启动臂由 start_replication_attach
///    前置握 InitializeRecover），本步无独立校验动作；
/// 2. recover_store_from_token：从接收文件集（wcpr 恢复 = 组件级重构 +
///    宿主装配）恢复出全新 [`WedbStore`]，经置换钩子接管在线引擎；
///    false 时跳过（C# 同款分支：同历史复用本地检查点恢复态；rust 副本引擎
///    不随 attach 重构——引擎只走到本分支时置换一次，其余时刻不换，该布尔的
///    来源与判据见 [`ReplicationManager::disk_resync_strategy`] 文档，本处
///    自述即为口径，不另挂在途文档）；
/// 3. replayAOFMap > 0 拒绝：rust 主端恒发 0——C# ComputeAofSyncReplayAddress
///    的「副本回放本地 AOF 衔接旧检查点」形态依赖副本运行期存储应用链
///    （未转写的架构边界），全量同步由「导入 + 授予位点直推」承接；
/// 4. 物理日志对齐检查点覆盖点（C# Log.Initialize(begin, offset)）；
/// 5. 复制位点 / 检查点历史 / 主复制 ID 收敛（C# replicationOffset 赋值 +
///    PurgeAllCheckpointsExceptEntry + InitializeCheckpointStore +
///    TryUpdateMyPrimaryReplId）；
/// 6. EndRecovery(CheckpointRecoveredAtReplica)（C# finally：此刻 curr 为
///    ClusterReplicate / InitializeRecover，矩阵合法；放行 AOF 流但锁仍持到
///    attach 收尾 finish_replica_sync 才释放）。
///
/// 返回授予副本的复制位点（主端据此挂 AOF 推流驱动；C# 应答
/// replicationOffset 的同载荷）。
pub async fn try_replica_diskbased_recovery(
  provider: &Arc<ClusterProvider>,
  rm: &Arc<ReplicationManager>,
  request: &ReplicaRecoverRequest,
) -> Result<AofAddress, String> {
  let ReplicaRecoverRequest {
    recover_store_from_token,
    replay_aof_map,
    primary_repl_id,
    remote_entry,
    begin_address,
    tail_address,
  } = request;
  rm.update_last_primary_sync_time();
  // 全量同步前挂起 Primary 类后台任务（C# ReplicaDiskbasedSync.cs:154
  // SuspendPrimaryOnlyTasksAsync 对译：检查点导入与 AOF 衔接期 GC/周期任务
  // 会干扰在线引擎置换与本地日志地址空间；与 try_add_replica_async 尾的
  // 挂起互为幂等双保险）
  provider.suspend_primary_tasks();
  let vm = provider.try_vector_manager();
  if let Some(vm) = &vm {
    vm.pause_cleanup_async();
  }
  struct CleanupPauseGuard(Option<Arc<VectorManager>>);
  impl Drop for CleanupPauseGuard {
    fn drop(&mut self) {
      if let Some(vm) = self.0.take() {
        vm.resume_cleanup();
      }
    }
  }
  let mut cleanup_guard = CleanupPauseGuard(vm);
  log::info!(
    "Replica Recover Store: {storeVersion}>[{sHlogToken}]",
    storeVersion = remote_entry.metadata.store_version,
    sHlogToken = remote_entry.metadata.store_hlog_token,
  );

  let mark_contaminated_if_dirty = || {
    if rm.is_hlog_dirty() {
      rm.mark_device_contaminated();
      provider.mark_device_contaminated();
    }
  };

  if *replay_aof_map != 0 {
    if *recover_store_from_token {
      mark_contaminated_if_dirty();
    }
    // rust 主端发送面恒发 0（模块文档条 3）；非零即协议违约
    return Err("replayAOFMap is not expected in AOF direct-push architecture".to_string());
  }

  let mut recovered_offset = *tail_address;

  if *recover_store_from_token {
    let token = remote_entry.metadata.store_hlog_token;
    if token == 0 {
      mark_contaminated_if_dirty();
      return Err("checkpoint token missing for store recovery".to_string());
    }
    let checkpoint_dir = match provider.try_checkpoint_dir() {
      Some(dir) => dir,
      None => {
        mark_contaminated_if_dirty();
        return Err("checkpoint dir not wired".to_string());
      }
    };
    if let Err(e) = create_dir_all(&checkpoint_dir) {
      mark_contaminated_if_dirty();
      return Err(format!("IOERR create checkpoint dir: {e}"));
    }

    // 元数据最后落盘 = 提交标记：meta 缺席即接收流未完成，拒绝导入半截
    // 文件集（对齐 wcpr「meta 即发布」崩溃一致性协议）
    if !checkpoint_dir.join(wcpr::meta_filename(token)).is_file() {
      mark_contaminated_if_dirty();
      return Err("checkpoint metadata not received (incomplete transfer)".to_string());
    }

    // lib/storage/Tsavorite/cs/src/core/Index/Recovery/Recovery.cs 组件级
    // 恢复（对标 C# RecoverCheckpointAsync(replicaRecover: true, ...)）：
    // 当前引擎设备即接收目标设备（同一 SegmentedDevice 实例，单句柄表）
    let old_store = match provider.try_store() {
      Some(store) => store,
      None => {
        mark_contaminated_if_dirty();
        return Err("store not wired".to_string());
      }
    };
    let device = imported_device(&old_store);
    let new_store = match WedbStore::recover(&checkpoint_dir, token, device).await {
      Ok(s) => s,
      Err(e) => {
        rm.mark_device_contaminated();
        provider.mark_device_contaminated();
        return Err(format!("checkpoint recovery failed: {e}"));
      }
    };
    let new_store = Arc::new(new_store);
    // 常驻回收驱动随换入实例挂载（幂等）：换入引擎不经 open_shared，不挂则
    // 从库本地待释放队列与死亡账本续扫无人消费。它不是 C# Primary 类任务、
    // 不受 gc.enabled 门禁——与下方「副本角色不拉起 GC」的扫描循环停跑语义
    // 正交（doc/zh/db.md 主从异步屏障：从库回放投递的批次由它落地）
    wkv::spawn_bftree_reclaimer(&new_store);
    // 副本角色不拉起 GC（C# 全量同步前已挂起 Primary 类任务，导入的新引擎
    // 保持 GC 停跑；升主恢复点 resume_primary_tasks 按当前配置重启）
    provider.swap_online_store(new_store);
  }

  // 物理日志对齐检查点覆盖区间（C# Log.Initialize(beginAddress, offset)；
  // 单物理日志取标量投影，begin/tail 由主端快照覆盖位点给出）
  if let Some(wal) = provider.try_wal() {
    let begin = begin_address.get(0).unwrap_or(0).max(0) as u64;
    let tail = recovered_offset.get(0).unwrap_or(0).max(0) as u64;
    wal.safe_initialize(begin, tail);
  }

  // 位点收敛（C# replicationOffset = recoveredReplicationOffset）
  for sublog in 0..rm.sublog_count() {
    let v = recovered_offset
      .get(sublog)
      .unwrap_or_else(|| recovered_offset.get(0).unwrap_or(0));
    recovered_offset.set(sublog, v);
    rm.set_sublog_replication_offset(sublog, v);
  }

  // 检查点历史收敛：远端条目登记为本地最新（供下次 PartialResync 判定），
  // 检查点目录仅保留导入文件集（C# PurgeAllCheckpointsExceptEntry +
  // InitializeCheckpointStore；C# GetLatestCheckpointEntryFromDisk 的磁盘
  // 扫描面由 wcpr 目录列举承接）
  if *recover_store_from_token {
    let dir = provider
      .try_checkpoint_dir()
      .ok_or_else(|| "checkpoint dir not wired".to_string())?;
    super::checkpoint_store::purge_checkpoint_files_except(
      &dir,
      remote_entry.metadata.store_hlog_token,
      remote_entry.metadata.store_hlog_token,
    );
  }
  rm.add_checkpoint_entry(remote_entry.clone(), true);

  // 更新复制 ID 标记后续检查点归属同一历史（C# TryUpdateMyPrimaryReplId）
  rm.try_update_my_primary_repl_id(primary_repl_id);

  // 接收状态置换（C# finally recvCheckpointHandler?.Dispose 同位）：恢复成功
  // 收尾——本轮接收闸门与脏标记净态，并解除 provider 管理面屏障（设备半写
  // 事实已被换入的新引擎视图取代，屏障再留即永久闭锁：下一轮重连的快照
  // 分块会被本轮拒收）
  rm.on_recovery_success();
  provider.clear_device_contaminated();
  // 恢复完成放行 AOF 流（C# finally EndRecovery(CheckpointRecoveredAtReplica)；
  // cannot_stream_aof 自此为假，但锁仍持到 attach 收尾）
  rm.end_recovery(RecoveryStatus::CheckpointRecoveredAtReplica, false);
  // 换引擎后向量登记回建（工单 zcode-r137c-snaplock2 宗二，与无盘臂
  // frame_import 内存面同步投喂双臂形制统一）：C# 原位恢复登记表随 store
  // 整表重置与引擎连续（ReplicaDiskbasedSync.cs 无回建臂系 N.A.），rust
  // 实例置换形态下主端向量集的旁路记录已随导入文件集入新引擎日志，内存
  // 镜像须在此经启动面同口 recover_vector_sets 回建收口（回建 + 残影清退
  // + reconcile + wait_for_quiescence 单机制零新设），置于 CleanupPauseGuard
  // 释放前执行——回建期清理闸门仍在位，未恢复上下文标记与投递不被并发清理
  // 竞扰；回收失败即本轮全量收口失败（回位点拒授予，下一轮重连重推，与
  // 启动面恢复失败拒启同口径）
  if *recover_store_from_token && let Some(dm) = provider.try_database_manager() {
    let recovered_vectors = dm
      .recover_vector_sets()
      .await
      .map_err(|e| format!("vector registry recovery after replica swap failed: {e}"))?;
    log::info!("ReplicaRecover: recovered vector sets: {recovered_vectors}");
  }
  if let Some(vm) = cleanup_guard.0.take() {
    vm.resume_cleanup();
    vm.queue_cleanups();
  }
  log::info!(
    "ReplicaRecover: ReplicaReplicationOffset = {recovered_offset}",
    recovered_offset = recovered_offset.to_aof_string()
  );
  Ok(recovered_offset)
}
