//! 副本磁盘基恢复闭环（全量同步第三段：接收完成 → 检查点导入 → 在线引擎
//! 置换 → 授予位点应答）
//!
//! 对标 libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs
//! （ReplicationManager partial：TryReplicaDiskbasedRecovery /
//! CreateCheckpointDevice / ShouldInitialize）。

use std::{
  fs::create_dir_all,
  path::{Path, PathBuf},
  sync::Arc,
};

use waof::AofAddress;
use wdev::SegmentedDevice;
use wkv::WedbStore;

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
/// 1. 恢复门控校验（调用方 CLUSTER REPLICATE 已 begin_recovery(ClusterReplicate)）；
/// 2. recover_store_from_token：从接收文件集（wcpr 恢复 = 组件级重构 +
///    宿主装配）恢复出全新 [`WedbStore`]，经置换钩子接管在线引擎；
///    false 时跳过（C# 同款分支：同历史复用本地检查点恢复态，rust 副本
///    引擎不随 attach 重构，见 task/ing/m4-checkpoint-import.md 条 6）；
/// 3. replayAOFMap > 0 拒绝：rust 主端恒发 0——C# ComputeAofSyncReplayAddress
///    的「副本回放本地 AOF 衔接旧检查点」形态依赖副本运行期存储应用链
///    （未转写的架构边界），全量同步由「导入 + 授予位点直推」承接；
/// 4. 物理日志对齐检查点覆盖点（C# Log.Initialize(begin, offset)）；
/// 5. 复制位点 / 检查点历史 / 主复制 ID 收敛（C# replicationOffset 赋值 +
///    PurgeAllCheckpointsExceptEntry + InitializeCheckpointStore +
///    TryUpdateMyPrimaryReplId）；
/// 6. EndRecovery(CheckpointRecoveredAtReplica)（C# finally）。
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
  log::info!(
    "Replica Recover Store: {storeVersion}>[{sHlogToken}]",
    storeVersion = remote_entry.metadata.store_version,
    sHlogToken = remote_entry.metadata.store_hlog_token,
  );

  if *replay_aof_map != 0 {
    // rust 主端发送面恒发 0（模块文档条 3）；非零即协议违约
    return Err("replayAOFMap is not expected in AOF direct-push architecture".to_string());
  }

  let mut recovered_offset = *tail_address;

  if *recover_store_from_token {
    let token = remote_entry.metadata.store_hlog_token;
    if token == 0 {
      return Err("checkpoint token missing for store recovery".to_string());
    }
    let checkpoint_dir = provider
      .try_checkpoint_dir()
      .ok_or_else(|| "checkpoint dir not wired".to_string())?;
    create_dir_all(&checkpoint_dir).map_err(|e| format!("IOERR create checkpoint dir: {e}"))?;

    // 元数据最后落盘 = 提交标记：meta 缺席即接收流未完成，拒绝导入半截
    // 文件集（对齐 wcpr「meta 即发布」崩溃一致性协议）
    if !checkpoint_dir.join(wcpr::meta_filename(token)).is_file() {
      return Err("checkpoint metadata not received (incomplete transfer)".to_string());
    }

    // lib/storage/Tsavorite/cs/src/core/Index/Recovery/Recovery.cs 组件级
    // 恢复（对标 C# RecoverCheckpointAsync(replicaRecover: true, ...)）：
    // 当前引擎设备即接收目标设备（同一 SegmentedDevice 实例，单句柄表）
    let old_store = provider
      .try_store()
      .ok_or_else(|| "store not wired".to_string())?;
    let device = imported_device(&old_store);
    let new_store = WedbStore::recover(&checkpoint_dir, token, device)
      .await
      .map_err(|e| format!("checkpoint recovery failed: {e}"))?;
    let new_store = Arc::new(new_store);
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
    purge_checkpoints_except(
      &checkpoint_dir_of(provider)?,
      remote_entry.metadata.store_hlog_token,
    );
  }
  rm.add_checkpoint_entry(remote_entry.clone(), true);

  // 更新复制 ID 标记后续检查点归属同一历史（C# TryUpdateMyPrimaryReplId）
  rm.try_update_my_primary_repl_id(primary_repl_id);

  // 接收状态置换（C# finally recvCheckpointHandler?.Dispose 同位）
  rm.reset_recv_checkpoint_handler();
  // 恢复完成放行 AOF 流（C# finally EndRecovery(CheckpointRecoveredAtReplica)）
  rm.end_recovery(RecoveryStatus::CheckpointRecoveredAtReplica, false);
  log::info!(
    "ReplicaRecover: ReplicaReplicationOffset = {recovered_offset}",
    recovered_offset = recovered_offset.to_aof_string()
  );
  Ok(recovered_offset)
}

/// 副本检查点目录（provider 注入面；调用方保证已注入）
fn checkpoint_dir_of(provider: &Arc<ClusterProvider>) -> Result<PathBuf, String> {
  provider
    .try_checkpoint_dir()
    .ok_or_else(|| "checkpoint dir not wired".to_string())
}

/// 保留导入 token 文件集，清理检查点目录内其余陈旧 token
/// （对标 C# CheckpointStore.PurgeAllCheckpointsExceptEntry 的物理清理面；
/// 转调 checkpoint_store 单点实现，容错跳过不可删除项，绝不阻断导入闭环）
fn purge_checkpoints_except(dir: &Path, keep_token: u128) {
  super::checkpoint_store::purge_checkpoint_files_except(dir, keep_token, keep_token);
}
