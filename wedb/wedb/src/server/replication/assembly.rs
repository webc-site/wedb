//! 复制数据面生产装配（主端推流面 + 副本接收面 + 副本重连发起钩子）
//!
//! 对标 C# 装配形态：
//! - 主端推流面：C# ReplicationManager 构造期经 clusterProvider.storeWrapper
//!   反查 appendOnlyFile 建立 AofSyncDriverStore 与推流任务；Rust 依赖方向
//!   反转，由本模块 [`wire_replication_data_plane`] 在宿主装配期正向注入
//!   [`PrimaryReplicationAssets`]（wal + 推流泵 + 副本同步会话）。
//! - 副本接收面：C# 会话侧 replicaReplaySession 可达面；Rust 经
//!   `set_replica_replication_session` 注入 [`ClusterReplicationSession`]，
//!   CLUSTER APPENDLOG 记录帧经网络会话直达落盘重放。
//! - 副本同步发起：C# 四处驱动点各按 `ReplicaDisklessSync` 开关在
//!   TryReplicateDisklessSyncAsync / TryReplicateDiskbasedSyncAsync 之间三元
//!   选路；rust 收敛为唯一选路口 [`try_replicate_sync_async`]——diskbased 支
//!   即 C# ReplicationManager.RecoverReplication →
//!   TryReplicateDiskbasedSyncAsync（libs/cluster/Server/Replication/
//!   ReplicaOps/ReplicaDiskbasedSync.cs:ReplicaSyncAttachTaskAsync）：
//!   副本清空本地重放状态后向主端发 CLUSTER INITIATE_REPLICA_SYNC
//!   （5 参：节点 id、指派主 repl id、检查点条目、副本 AOF begin/tail），
//!   主端 +OK 后回连副本建立 APPENDLOG 推流；本模块
//!   [`recover_replication`] 承接同一 attach 体（ensure_replication 断链时
//!   后台任务直调驱动、REPLICAOF 与 CLUSTER REPLICATE 命令臂当场发起并按
//!   失败回 -ERR；开关点亮时同一选路改由副本主动 ATTACH_SYNC 发起端
//!   [`super::replica_diskless_sync::try_replicate_diskless_sync_async`] 承接）。

use std::{future::Future, sync::Arc};

use compio::runtime::spawn;
use waof::WalLog;
use wbase::hex::hex_str_u128;
use wdev::SegmentedDevice;

use super::{
  aof_replication_pump::AofReplicationPump, checkpoint_entry::CheckpointEntry,
  cluster_replication_session::ClusterReplicationSession, recovery_status::RecoveryStatus,
  replica_diskless_sync::try_replicate_diskless_sync_async, replica_replay_task,
  replica_sync_session::ReplicaSyncSession, replica_wire::REPL_ATTACH_TIMEOUT,
  replicate_sync_options::ReplicateSyncOptions,
};
use crate::{
  client::GarnetClient,
  server::{
    cluster_manager_worker_state::replicate_err_text,
    cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
    wait_async,
  },
};

/// 复制数据面装配（宿主启动路径与集成测试共用的唯一装配体；AOF 门控
/// 点亮时调用一次）
///
/// 副本接收会话的 replay 端口传 None：记录帧重放推进走重放驱动仓库
///（init 帧握手注册驱动 + 背景重放任务应用回推），与会话断链处置
///（dispose 释放驱动仓库）同源同寿
pub fn wire_replication_data_plane(
  cluster: &Arc<ClusterProvider>,
  wal: Arc<WalLog<SegmentedDevice>>,
) {
  let Some(rm) = cluster.replication_manager() else {
    return;
  };
  // 副本重放应用资产注入（对标 C# rm 构造期 aofProcessor + storeWrapper
  // 反查装配）：aof + store 双在场才建（对标 C# EnableAOF 门控）；缺席为
  // 退化装配，副本位点保持会话落盘面 enqueued 形态。runtime_config 随资产
  // 注入（对标 C# storeWrapper.runtimeConfig 可达面：重放空转周期每轮现取）
  match (cluster.try_aof(), cluster.try_store()) {
    (Some(aof), Some(store)) => rm.set_replay_assets(Some(Arc::new(
      replica_replay_task::ReplayAssets::new(aof, store, cluster.try_runtime_config()),
    ))),
    _ => rm.set_replay_assets(None),
  };
  // 副本接收面：CLUSTER APPENDLOG → 保真落盘 + 背景重放应用位点回推
  cluster.set_replica_replication_session(Some(Arc::new(ClusterReplicationSession::new(
    Arc::clone(cluster),
    Arc::clone(&wal),
    None,
  ))));
  // 主端推流面：策略协商 + 建连 + 补扫的发起资产。空闲防抖窗口周期在
  // 循环内每轮现取 replica-sync-delay 槽位（CONFIG SET 即时生效）
  let pump = Arc::new(AofReplicationPump::new(Arc::clone(
    &rm.aof_sync_driver_store,
  )));
  pump.start_throttle_loop(cluster.try_runtime_config());
  cluster.set_primary_replication(Some(Arc::new(PrimaryReplicationAssets {
    wal: Arc::clone(&wal),
    pump,
    sync_session: Arc::new(ReplicaSyncSession::new(Arc::clone(&rm))),
  })));
  // 本地日志句柄（副本重连发起的 begin/tail 位点源）
  cluster.set_wal(wal);
}

/// 单槽位点序列化为 span 字节（C# `AofAddress.Span` 形态：8B LE 裸字节，无长度头）
fn aof_span(address: i64) -> Vec<u8> {
  address.to_le_bytes().to_vec()
}

/// 副本重连发起动作（diskbased attach 体；对标 C#
/// ReplicationManager.RecoverReplication → TryReplicateDiskbasedSyncAsync 的
/// ReplicaSyncAttachTaskAsync 发起段）
///
/// 流程（对标 C# ReplicaSyncAttachTaskAsync 发起段）：
/// 1. 清空本地重放驱动仓库（重注册由主端 init 帧握手完成；此处预注册会令
///    IsReplicating 状态面误报「流活跃」而中断 ensure_replication 静默重试）；
/// 2. 构造 5 参（节点 id、指派主 repl id、检查点条目、副本 AOF begin/tail；
///    无检查点时上报空条目，对标 C# GetLatestCheckpointEntryFromDisk 空库
///    语义）；
/// 3. 专用客户端向主端发起 CLUSTER INITIATE_REPLICA_SYNC，cluster_timeout
///    级超时（failover 治理同款，杜绝无限挂起）；
/// 4. 应答面：成功仅记日志（数据面由主端回连异步建立）；失败 / 超时回 Err
///    交驱动点处置——ensure_replication 重连臂记告警后按轮询节流重试（对标
///    C# RecoverReplication 失败后按轮询节奏重试），命令臂据此回错误应答
///    （C# 同一发起体的失败即 REPLICAOF / CLUSTER REPLICATE 的 -ERR 文案）
pub async fn recover_replication(
  provider: &Arc<ClusterProvider>,
  primary: u128,
) -> Result<(), String> {
  let Some(rm) = provider.replication_manager() else {
    return Err("replication manager not initialized".to_string());
  };
  // 1. 清空本地重放驱动仓库 + 检查点接收状态（对标 C# 每次 attach
  //    new ReceiveCheckpointHandler 的置换语义）+ 清残留主端推流驱动
  //    （对标 C# 相邻序列 aofSyncDriverStore.Reset——"Remove aofSync tasks
  //    if this node was a primary"；断链重连时本节点可能刚被 gossip 翻回
  //    主角色残留驱动，shipped watermark 残留会钉死背压闸门）
  rm.reset_replica_replay_driver_store();
  rm.reset_recv_checkpoint_handler();
  rm.aof_sync_driver_store.reset();

  // 2. 主端 endpoint 与本端节点 id（集群配置反查）
  let Some(cm) = provider.cluster_manager() else {
    return Err("cluster manager not initialized".to_string());
  };
  let (address, port) = cm.current_config().get_local_node_primary_address();
  let Some(node_id) = cm.current_config().local_node_id().filter(|id| *id != 0) else {
    return Err(format!(
      "replication recovery to {primary} skipped: local node id unknown"
    ));
  };
  let Some(address) = address else {
    return Err(format!(
      "replication recovery to {primary} skipped: primary endpoint unknown"
    ));
  };

  // 3. 发起参数束
  let checkpoint_entry = rm
    .checkpoint_store
    .read()
    .latest_entry()
    .map(|entry| entry.to_byte_array())
    .unwrap_or_else(|| CheckpointEntry::with_sublogs(1).to_byte_array());
  let Some(wal) = provider.try_wal() else {
    return Err(format!(
      "replication recovery to {primary} skipped: local wal not wired"
    ));
  };
  let aof_begin = aof_span(wal.begin_address() as i64);
  let aof_tail = aof_span(wal.tail_address() as i64);
  drop(wal);

  // 4. 专用客户端发起（用后即弃，对标 C# gcs 构造 / finally Dispose）
  let client = GarnetClient::with_auth(
    format!("{address}:{port}"),
    provider.cluster_username(),
    provider.cluster_password(),
  );
  client.connect_async().await;
  let initiated = client.is_connected();
  let res = if initiated {
    // 应答限时取 attach 级 REPL_ATTACH_TIMEOUT（60s）——C#
    // ReplicaDiskbasedSync.cs:182-186 对 ExecuteClusterInitiateReplicaSync
    // 以 WaitAsync(GetTimeSpan(REPL_ATTACH_TIMEOUT)) 限时（回填源
    // ReplicaAttachTimeout，GarnetServerOptions.cs:425），与节点失联判定
    // 的 cluster_node_timeout 无涉
    wait_async(
      Some(REPL_ATTACH_TIMEOUT),
      // 协议帧参数：节点 id 仅在命令面渲染 hex
      client.initiate_replica_sync_async(
        &hex_str_u128(node_id),
        &rm.primary_repl_id(),
        &checkpoint_entry,
        &aof_begin,
        &aof_tail,
      ),
    )
    .await
    .ok_or_else(|| "timed out".to_string())
  } else {
    Err("not connected".to_string())
  };
  client.dispose();

  // 5. 应答面：成功仅记日志（数据面由主端回连异步建立）；失败 / 超时回 Err
  //    交驱动点处置——轮询臂记告警后按节流重试、命令臂据此回 -ERR 文案
  //    两失败臂的差别只在尾缀原因（对端 -ERR 的 Error / 超时未连接的串），
  //    发起动作同名 INITIATE_REPLICA_SYNC，故归一原因后共用一处措辞、不留
  //    第二套模板（C# 同一 catch 把 ex.Message 原样作应答，
  //    libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:188-195）
  let reason = match res {
    Ok(Ok(_)) => {
      log::info!("Replica sync initiated to {primary}");
      return Ok(());
    }
    Ok(Err(e)) => e.to_string(),
    Err(msg) => msg,
  };
  Err(format!(
    "Failed to initiate replica sync to {primary}: {reason}"
  ))
}

/// 副本同步发起骨架（C# `TryReplicateDisklessSyncAsync` 与
/// `TryReplicateDiskbasedSyncAsync` 两支同形的公共前后段：登记副本 → 纪元等待
/// → attach 发起（Background 臂即发即忘）→ catch / finally 收尾；两支仅 attach
/// 体不同，由驱动点作为未来对象传入。两支的 1:1 对标锚点在两个入口函数上，
/// 本函数不占锚位）
pub(crate) async fn replicate_sync_async<F>(
  provider: &Arc<ClusterProvider>,
  opts: ReplicateSyncOptions,
  attach: F,
) -> Result<(), String>
where
  // 仅 'static：本仓 compio 运行时为线程本地驱动器（spawn 无 Send 约束），
  // 而 attach 体内的 GarnetClient 建连未来非 Send，加 Send 界即断链
  F: Future<Output = Result<(), String>> + 'static,
{
  // 1. TryAddReplica 臂（对标 C# TryAddReplicaAsync(options.NodeId,
  //    options.Force, options.UpgradeLock)；失败即原样上抛其错误文案，
  //    C# `return (false, error)` 同口径；本臂不消费时不取句柄，避免
  //    TryAddReplica:false 的调用形态被无关前置挡住）
  if opts.try_add_replica {
    let cm = provider
      .cluster_manager()
      .ok_or_else(|| "cluster manager not initialized".to_string())?;
    cm.try_add_replica_async(opts.node_id, opts.force, opts.upgrade_lock)
      .await
      .map_err(replicate_err_text)?;
  }

  // 2. 纪元推进等待（C# session.UnsafeBumpAndWaitForEpochTransitionAsync；
  //    rust 无会话线程模型，统一经 provider 面推进）
  provider.bump_and_wait_for_epoch_transition_async().await;

  // 3. attach 发起：Background 臂即发即忘、错误在体内记日志（对标 C#
  //    `_ = TryBeginReplicaSyncAsync(forceAsync: true)` 后仍回 (true, default)）；
  //    前台臂回传结果供驱动点写 OK / -ERR 应答
  if opts.background {
    let provider = Arc::clone(provider);
    spawn(async move {
      if let Err(e) = finish_replica_sync(&provider, opts, attach.await) {
        log::warn!(
          "Background replica sync to {} failed: {e}",
          hex_str_u128(opts.node_id)
        );
      }
    })
    .detach();
    return Ok(());
  }
  finish_replica_sync(provider, opts, attach.await)
}

/// attach 收尾（C# attach 体内 catch / finally 两支的合并形态）
fn finish_replica_sync(
  provider: &Arc<ClusterProvider>,
  opts: ReplicateSyncOptions,
  result: Result<(), String>,
) -> Result<(), String> {
  // catch 臂（C# catch：AllowReplicaResetOnFailure 时把本节点复位为主）
  if result.is_err()
    && opts.allow_replica_reset_on_failure
    && let Some(cm) = provider.cluster_manager()
  {
    cm.try_reset_replica();
  }

  // finally 臂（C# ReplicaDiskbasedSync.cs:197-208 / ReplicaDisklessSync.cs:
  // 185-194 的 finally 对偶）：锁由 try_add_replica_async（或启动臂的
  // InitializeRecover 前置）握到本收尾，此处一次做全——upgrade_lock 臂降回
  // ReadRole（外层驱动点统一 AllowRoleChange 收尾），其余臂释放到 NoRecovery。
  // 不变式：走到本收尾的调用必持锁（三个驱动点——重连臂 / REPLICAOF /
  // CLUSTER REPLICATE 经 try_add_replica_async 握 ClusterReplicate、启动臂
  // 经 start_replication_attach 前置握 InitializeRecover），故释放无需再按
  // 入口分支；若新增驱动点，必须在进入 attach 前先握锁，否则此处释放会落
  // 在 NoRecovery 起点被状态矩阵判非法
  if let Some(rm) = provider.replication_manager() {
    if opts.upgrade_lock {
      rm.end_recovery(RecoveryStatus::ReadRole, true);
    } else {
      rm.end_recovery(RecoveryStatus::NoRecovery, false);
    }
  }
  result
}

/// 副本磁盘基同步发起（登记副本 + 纪元等待后向主端发
/// CLUSTER INITIATE_REPLICA_SYNC，attach 体即 [`recover_replication`]）
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:TryReplicateDiskbasedSyncAsync
pub async fn try_replicate_diskbased_sync_async(
  provider: &Arc<ClusterProvider>,
  opts: ReplicateSyncOptions,
) -> Result<(), String> {
  let primary = opts.node_id;
  let attach_provider = Arc::clone(provider);
  replicate_sync_async(provider, opts, async move {
    recover_replication(&attach_provider, primary).await
  })
  .await
}

/// 副本同步发起的唯一选路口（对标 C# 四处驱动点各自的
/// `ReplicaDisklessSync ? TryReplicateDisklessSyncAsync :
/// TryReplicateDiskbasedSyncAsync` 三元：rust 全仓只在本函数读一次开关
/// [`ClusterProvider::replica_diskless_sync`]，驱动点一律经此发起，
/// 杜绝散读配置与第二套发起路径）
pub async fn try_replicate_sync_async(
  provider: &Arc<ClusterProvider>,
  opts: ReplicateSyncOptions,
) -> Result<(), String> {
  if provider.replica_diskless_sync() {
    try_replicate_diskless_sync_async(provider, opts).await
  } else {
    try_replicate_diskbased_sync_async(provider, opts).await
  }
}

#[cfg(test)]
mod tests {
  use waof::AofAddress;

  use super::*;

  /// span 序列化与主端 AofAddress::from_span 的往返（C# beginAddress.Span /
  /// FromSpan 契约）
  #[test]
  fn aof_span_roundtrip() {
    assert_eq!(aof_span(0), 0i64.to_le_bytes().to_vec());
    assert_eq!(aof_span(4096), 4096i64.to_le_bytes().to_vec());
    let span = aof_span(-64);
    assert_eq!(AofAddress::from_span(&span).get(0), Some(-64));
    // 单槽位点 span 长度恒 8B（from_span length = 8 >> 3 = 1）
    assert_eq!(AofAddress::from_span(&span).length(), 1);
  }

  /// 空检查点条目序列化可被主端 FromByteArray 还原（C# 空库上报语义）
  #[test]
  fn empty_checkpoint_entry_roundtrip() {
    let bytes = CheckpointEntry::with_sublogs(1).to_byte_array();
    let decoded = CheckpointEntry::from_byte_array(&bytes).expect("空条目必须可解码");
    assert_eq!(decoded.metadata.store_version, -1);
    assert_eq!(decoded.metadata.store_hlog_token, 0);
    assert!(decoded.metadata.store_primary_repl_id.is_none());
  }
}
