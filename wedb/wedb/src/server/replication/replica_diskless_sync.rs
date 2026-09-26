//! 集群无盘全量同步与快照流驱动 (ReplicaDisklessSync)
//!
//! 在 garnet 中的相对路径:
//! - libs/cluster/Server/Replication/ReplicaOps/ReplicaDisklessSync.cs
//! - libs/cluster/Server/Replication/PrimaryOps/PrimarySync.cs
//!
//! 架构设计：
//! 基于流式 Chunk 解耦传输与本地写入，零临时磁盘文件，保证并发无死锁。
//! 1. 副本侧发起: [`try_replicate_diskless_sync_async`] 登记副本后向主端发
//!    `CLUSTER ATTACH_SYNC` 上报同步元数据；主端 `network_cluster_attach_sync`
//!    （`cluster_session/replication.rs`）收帧后转入本文件第 2 步。本支由
//!    C# `ReplicaDisklessSync` 开关选路，选路只在唯一读点
//!    [`try_replicate_sync_async`](super::assembly::try_replicate_sync_async)
//!    发生，其驱动挂点包括：`cluster_provider.rs` `ensure_replication` 断链
//!    重连后台体、`cluster_session/replica_of.rs` REPLICAOF、
//!    `cluster_session/replication.rs` CLUSTER REPLICATE，以及
//!    C# 第四处 `ReplicationManager.Start` 启动臂（由 `start_replication_attach` 承接）。
//! 2. 主端协商与扇出: [`try_begin_diskless_sync_async`]（对标 C#
//!    `ReplicationManager.TryBeginDisklessSyncAsync`，PrimarySync.cs）经
//!    [`ReplicationSyncManager`] 入册会话并由 leader 攒批编排：
//!    REPL_DISKLESS_SYNC_DELAY 窗口内同批副本一次开窗，批内共享一枚快照覆盖
//!    锚，单遍存储活扫描逐记录锁步扇出全部全量会话（
//!    [`diskless_replication`] 子模块，对标 C# PrimaryOps/DisklessReplication
//!    目录拓扑）；逐副本各起一遍全库扫描的旧单副本路线已删除，N=1 走同一
//!    扇出路径，不留第二套快照架构。
//! 3. 流式传输: 主端通过 `CLUSTER SYNC` 将全量键值记录（单条或分块 Chunk）流式推送到本批全部副本。
//! 4. 副本恢复: 主端经推流连接向副本发 `CLUSTER ATTACH_SYNC`（primary 元数据），
//!    副本 `TryReplicaDisklessRecovery` 校验并对齐 AOF/WAL 位点与版本、收敛
//!    复制 ID，回传恢复位点后主端方从该位点建驱动转入增量推流 (`APPENDLOG`) 阶段。

use std::sync::Arc;

use waof::AofAddress;
use wnode::resp::vector::vector_manager::VectorManager;

use crate::{
  client::GarnetClient,
  server::{
    cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
    replication::{
      assembly::replicate_sync_async, diskless_replication::ReplicationSyncManager,
      recovery_status::RecoveryStatus, replicate_sync_options::ReplicateSyncOptions,
      replication_manager::ReplicationManager, sync_metadata::SyncMetadata,
    },
    wait_async,
  },
};

/// 副本发起无盘同步入口（diskless 支）
///
/// 开关选路在全仓唯一选路口
/// [`try_replicate_sync_async`](super::assembly::try_replicate_sync_async)；
/// 公共前后段（登记副本 → 纪元等待 → 后台 / 前台发起 → catch / finally 收尾）
/// 与 diskbased 支同源，复用 [`replicate_sync_async`] 骨架，本模块只供 attach
/// 体 [`replica_diskless_attach`]（即 C# 内联局部函数 TryBeginReplicaSyncAsync）。
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Replication/ReplicaOps/ReplicaDisklessSync.cs:TryReplicateDisklessSyncAsync
pub async fn try_replicate_diskless_sync_async(
  provider: &Arc<ClusterProvider>,
  opts: ReplicateSyncOptions,
) -> Result<(), String> {
  let attach_provider = Arc::clone(provider);
  replicate_sync_async(provider, opts, async move {
    let Some(rm) = attach_provider.replication_manager() else {
      return Err("replication manager not initialized".to_string());
    };
    replica_diskless_attach(&attach_provider, &rm).await
  })
  .await
}

/// 副本向主端发起 `CLUSTER ATTACH_SYNC` 同步握手（attach 体，收尾由骨架承接）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Replication/ReplicaOps/ReplicaDisklessSync.cs:TryBeginReplicaSyncAsync
///
/// C# TryBeginReplicaSyncAsync 体内 `disklessSync` 恒真（选路已在唯一选路口
/// 按开关完成），故其 `!disklessSync` 三支（storeWrapper.Reset 清库重建、
/// GetLatestCheckpointEntryFromDisk 盘检查点、ReceiveCheckpointHandler
/// 接收器）不在本函数内——那三支属 diskbased 支的
/// [`recover_replication`](super::assembly::recover_replication) 形态。
async fn replica_diskless_attach(
  provider: &Arc<ClusterProvider>,
  rm: &Arc<ReplicationManager>,
) -> Result<(), String> {
  // 1. 复位本节点既有复制驱动（C# ResetReplicaReplayDriverStore 清副本重放
  //    驱动 + aofSyncDriverStore.Reset 清主端推流驱动）
  rm.reset_replica_replay_driver_store();
  rm.aof_sync_driver_store.reset();

  // 2. 挂起 Primary 类后台任务（C# SuspendPrimaryOnlyTasksAsync：周期提交与
  //    GC 轮空，避免干扰衔接期的本地日志地址空间）
  provider.suspend_primary_tasks();

  // 3. 反查主端 endpoint 与本节点身份（C# CurrentConfig.GetLocalNodePrimaryAddress
  //    + LocalNodeRole / LocalNodeId）
  let Some(cm) = provider.cluster_manager() else {
    return Err("cluster manager not initialized".to_string());
  };
  let (primary_addr, primary_port, local_role, local_id) = {
    let config = cm.current_config();
    let (primary_addr_opt, primary_port) = config.get_local_node_primary_address();
    let primary_addr =
      primary_addr_opt.ok_or_else(|| "primary address not found in config".to_string())?;
    (
      primary_addr,
      primary_port,
      config.local_node_role(),
      config.local_node_id().unwrap_or_default(),
    )
  };

  // 4. 连接主节点（用后即弃，对标 C# gcs 构造 / finally Dispose；复制网络
  //    缓冲池同池注入，对标 C# ReplicaDisklessSync.cs:145 gcs 构造的
  //    replicationManager.GetNetworkPool 形参）
  let mut client = GarnetClient::with_auth(
    format!("{primary_addr}:{primary_port}"),
    provider.cluster_username(),
    provider.cluster_password(),
  );
  client.set_network_pool(Some(rm.network_pool()));
  if let Err(e) = client.connect_async().await {
    log::warn!("failed connecting to primary for diskless attach sync: {e}");
  }
  if !client.is_connected() {
    client.dispose();
    return Err("failed connecting to primary for diskless attach sync".to_string());
  }

  // 5. 组同步元数据并经 CLUSTER ATTACH_SYNC 上报（C# SyncMetadata +
  //    ExecuteClusterAttachSync；attach 级限时取 repl_attach_timeout（优先读 runtime_config），
  //    对标 ReplicaDisklessSync.cs:171-174 WaitAsync(REPL_ATTACH_TIMEOUT)）
  let wal = provider
    .try_wal()
    .ok_or_else(|| "local wal not wired for diskless attach sync".to_string())?;
  let sublog_count = rm.sublog_count() as i32;
  let sync_meta = SyncMetadata {
    full_sync: false,
    origin_node_role: local_role,
    origin_node_id: local_id,
    current_primary_repl_id: rm.primary_repl_id(),
    current_store_version: provider
      .try_store()
      .map(|store| store.current_version())
      .unwrap_or(0),
    current_aof_begin_address: AofAddress::create(sublog_count, wal.begin_address() as i64),
    current_aof_tail_address: AofAddress::create(sublog_count, wal.tail_address() as i64),
    current_replication_offset: rm.get_current_replication_offset(),
    checkpoint_entry: None,
  };
  drop(wal);

  let resp = wait_async(
    provider.repl_attach_timeout(),
    client.execute_cluster_attach_sync(&sync_meta.to_byte_array()),
  )
  .await;
  client.dispose();
  let resp = resp
    .ok_or_else(|| "diskless attach sync timeout".to_string())?
    .map_err(|e| format!("execute_cluster_attach_sync failed: {e}"))?;

  // C# 端不消费主端应答（后续恢复与增量衔接由主端经推流连接回发
  // ATTACH_SYNC 承接，见 try_replica_diskless_recovery），此处仅记日志
  log::info!("Diskless sync attach complete, primary granted offset: {resp}");
  Ok(())
}

/// 副本无盘全量恢复执行体（对标 C# TryReplicaDisklessRecovery）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Replication/ReplicaOps/ReplicaDisklessSync.cs:TryReplicaDisklessRecovery
pub fn try_replica_diskless_recovery(
  provider: &Arc<ClusterProvider>,
  rm: &Arc<ReplicationManager>,
  primary_sync_meta: &SyncMetadata,
) -> Result<AofAddress, String> {
  rm.update_last_primary_sync_time();

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

  // 全量同步前挂起 Primary 类后台任务（C# ReplicaDisklessSync.cs:124
  // SuspendPrimaryOnlyTasksAsync 对译：恢复导入与 AOF 衔接期 GC/周期任务
  // 会干扰本地日志地址空间；与 try_add_replica_async 尾的挂起互为幂等
  // 双保险）
  provider.suspend_primary_tasks();
  let aof_begin = primary_sync_meta.current_aof_begin_address;
  let aof_tail = if !primary_sync_meta.full_sync {
    rm.get_current_replication_offset()
  } else {
    aof_begin
  };

  // 初始化 WAL 日志
  if let Some(wal) = provider.try_wal() {
    let begin = aof_begin.get(0).unwrap_or(0).max(0) as u64;
    let tail = aof_tail.get(0).unwrap_or(0).max(0) as u64;
    wal.safe_initialize(begin, tail);
  }

  // 复制位点收敛
  let mut recovered = aof_tail;
  for sublog in 0..rm.sublog_count() {
    let v = recovered
      .get(sublog)
      .unwrap_or_else(|| recovered.get(0).unwrap_or(0));
    recovered.set(sublog, v);
    rm.set_sublog_replication_offset(sublog, v);
  }

  // store 版本收敛（对标 C# ReplicaDisklessSync.cs:228
  // storeWrapper.store.SetVersion(primarySyncMetadata.currentStoreVersion)）：
  // 快照链不重放锚前历史，副本版本必须显式收敛到主端当下版本，否则重连协商
  // 「副本上报版本 != 主端当下版本」判据恒真——凡主端发生过检查点换版，副本
  // 每次重连都被判 FullResync 整库重灌，部分重同步永久失效。无条件收敛：
  // fetch_max 单调无回退险（部分重同步臂版本本已相等，收敛即无操作；恢复帧
  // 版本不低于此后重放的任何记录携带版本，ShouldSkipRecord 版本闸绝不误跳）
  if let Some(store) = provider.try_store() {
    store.set_current_version(primary_sync_meta.current_store_version);
  }

  // 更新主复制 ID
  rm.try_update_my_primary_repl_id(&primary_sync_meta.current_primary_repl_id);
  // 恢复完成放行 AOF 流（C# finally EndRecovery(CheckpointRecoveredAtReplica)；
  // curr 为 ClusterReplicate / InitializeRecover（attach 链全程持锁），矩阵
  // 合法；cannot_stream_aof 自此为假，锁仍持到 attach 收尾）
  rm.end_recovery(RecoveryStatus::CheckpointRecoveredAtReplica, false);
  if let Some(vm) = cleanup_guard.0.take() {
    vm.resume_cleanup();
    vm.queue_cleanups();
  }

  log::info!(
    "Replica diskless recovery completed, replication offset: {}",
    recovered.to_aof_string()
  );
  Ok(recovered)
}

/// 主端处理无盘同步请求入口（对标 C# ReplicationManager.TryBeginDisklessSyncAsync）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/PrimarySync.cs:TryBeginDisklessSyncAsync
///
/// 会话入册 [`ReplicationSyncManager::add_replica_sync_session`] + 会话驱动
/// [`ReplicationSyncManager::replication_sync_driver`] 两步（C# 同形态：
/// AddReplicaSyncSession → ReplicationSyncDriverAsync）。多会话 leader 攒批、
/// 批内共享快照覆盖锚与单遍扫描锁步扇出、逐会话 AOF 增量衔接（BeginAofSync
/// 语义）全部收敛在 diskless_replication 子模块：本入口 N=1 与 N>1 同路，
/// 无单副本快路径。入册失败（同批同步进行中 / 重复节点）以错误上抛，由
/// attach 帧处理面回 RESP 错误（C# RESP_ERR_CREATE_SYNC_SESSION_ERROR 同位）。
pub async fn try_begin_diskless_sync_async(
  provider: &Arc<ClusterProvider>,
  assets: &PrimaryReplicationAssets,
  local_node_id: u128,
  replica_endpoint: &str,
  replica_meta: &SyncMetadata,
) -> Result<AofAddress, String> {
  let rm = provider
    .replication_manager()
    .ok_or_else(|| "replication manager not initialized".to_string())?;
  let manager: Arc<ReplicationSyncManager> = Arc::clone(&rm.replication_sync_manager);
  let session = manager.add_replica_sync_session(
    replica_endpoint.to_string(),
    replica_meta.clone(),
    rm.sublog_count(),
    Arc::clone(&rm.aof_sync_driver_store),
  )?;
  manager
    .replication_sync_driver(&session, provider, &rm, assets, local_node_id)
    .await
}
