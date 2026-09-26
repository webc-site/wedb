//! 无盘全量同步闭环收敛集成测试（恢复帧接线 + 回传位点锚定推流起点）
//!
//! 对标 C# diskless 闭环：
//! - 主端恢复握手：libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/
//!   ReplicaSyncSession.cs:BeginAofSyncAsync（构造 primary 元数据经推流连接发
//!   ATTACH_SYNC，以副本回传位点 TryAddReplicationDriver 建驱动）
//! - 副本恢复承接：libs/cluster/Server/Replication/ReplicaOps/ReplicaDisklessSync.cs:
//!   TryReplicaDisklessRecovery（WAL Initialize 对齐 + 复制位点 + ReplicationId 收敛）
//!
//! 链路：主端 try_begin_diskless_sync_async（FullResync 协商 → 快照流 → 推流
//! 连接上发 ATTACH_SYNC primary 元数据 → 副本恢复回传位点 → 以回传位点建驱动
//! 补扫积压）→ 真 socket GarnetServer 副本集群会话承接 → 断言副本恢复位点与
//! 主端对齐、APPENDLOG 记录帧衔接不再 divergent 断流、复制 ID 收敛、增量续推。

mod common;
#[path = "common/primary_assets.rs"]
mod primary_assets_core;
use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use common::{open_node, provider_with_role, replica_host};
use primary_assets_core::primary_assets;
use waof::AofAddress;
use wedb::server::{
  replication::{
    cluster_replication_session::ClusterReplicationSession, recovery_status::RecoveryStatus,
    replica_diskless_sync::try_begin_diskless_sync_async, sync_metadata::SyncMetadata,
  },
  worker::NodeRole,
};
use wtest_base::wait_for;

/// 测试节点身份（内部 u128；协议面渲染 32 字符小写 hex）
const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

/// 非空日志主端 diskless 闭环：恢复帧接线后副本位点与主端对齐、复制 ID
/// 收敛、积压与增量 APPENDLOG 记录帧衔接不再 divergent 断流
#[compio::test]
async fn diskless_sync_recovers_replica_offset_and_converges_repl_id() {
  // ===== 主端：先落非空日志（排除空日志零位点巧合对齐）
  let source = open_node("diskless_loop_source");
  let provider_p = provider_with_role(
    &source,
    PRIMARY_ID,
    7000,
    NodeRole::Primary,
    PRIMARY_ID,
    false,
    Some(0),
  );
  for i in 0..3 {
    source
      .wal
      .enqueue(format!("loop-backlog-{i}").as_bytes())
      .unwrap();
  }
  let primary_tail = source.wal.tail_address() as i64;
  assert!(primary_tail > 0, "主端日志必须非空");

  // ===== 副本：带旧本地日志（旧地址空间 tail > 0）+ 全接线 + 宿主服务器
  //（恢复帧 safe_initialize 重置对齐是衔接前提；无恢复帧时旧尾位与主端
  // 首帧必然 divergent 断流，测试据此区分新旧行为）
  let replica = open_node("diskless_loop_replica");
  for i in 0..2 {
    replica
      .wal
      .enqueue(format!("loop-stale-{i}").as_bytes())
      .unwrap();
  }
  assert!(replica.wal.tail_address() > 0, "副本旧地址空间必须非空");
  let provider_r = provider_with_role(
    &replica,
    REPLICA_ID,
    7001,
    NodeRole::Replica,
    PRIMARY_ID,
    false,
    Some(0),
  );
  provider_r.set_replica_replication_session(Some(Arc::new(ClusterReplicationSession::new(
    Arc::clone(&provider_r),
    Arc::clone(&replica.wal),
    None,
  ))));
  let (server, replica_addr) = replica_host(&provider_r, NonZeroUsize::new(1));

  let rm_p = provider_p.replication_manager().unwrap();
  let rm_r = provider_r.replication_manager().unwrap();
  let primary_repl_id = rm_p.primary_repl_id();
  assert_ne!(
    rm_r.primary_repl_id(),
    primary_repl_id,
    "独立节点复制 ID 初值必不同"
  );
  // 副本握手完成后的读角色门控（对标 C# TryBeginReplicaSyncAsync 尾态
  // EndRecovery(ReadRole, downgradeLock: true)；恢复帧 end_recovery 的
  // ReadRole → CheckpointRecoveredAtReplica 为合法迁移）
  assert!(
    rm_r.begin_recovery(RecoveryStatus::ReadRole, false),
    "副本恢复门控就位"
  );

  // ===== 主端发起无盘全量同步（无检查点历史 + 副本零位点 → FullResync）
  let assets = primary_assets(&source, &rm_p);
  let meta = SyncMetadata {
    full_sync: false,
    origin_node_role: NodeRole::Replica,
    origin_node_id: REPLICA_ID,
    current_primary_repl_id: rm_r.primary_repl_id(),
    current_store_version: 0,
    current_aof_begin_address: AofAddress::create(1, 0),
    current_aof_tail_address: AofAddress::create(1, 0),
    current_replication_offset: AofAddress::create(1, 0),
    checkpoint_entry: None,
  };
  let sync_from =
    try_begin_diskless_sync_async(&provider_p, &assets, PRIMARY_ID, &replica_addr, &meta)
      .await
      .unwrap();

  // 恢复帧回传位点：FullResync 副本位点收敛到主端快照覆盖锚（扫描前尾
  // = primary_tail，锚前积压记录已含快照不再重放，AOF 恰从锚续推）
  assert_eq!(
    sync_from.get(0),
    Some(primary_tail),
    "副本恢复位点必经 ATTACH_SYNC 回传并收敛到主端授予锚"
  );

  // 复制 ID 收敛（恢复帧 try_update_my_primary_repl_id 生效）
  assert_eq!(
    rm_r.primary_repl_id(),
    primary_repl_id,
    "副本主复制 ID 必须经恢复帧收敛为主端 ID"
  );

  // WAL 地址空间对齐：副本日志从锚重置后与主端尾严格衔接（恢复帧未接线
  // 时副本保留旧地址空间尾，首个记录帧即 divergent 断流）
  let converged = wait_for(
    || replica.wal.tail_address() as i64 == primary_tail,
    Duration::from_secs(5),
  )
  .await;
  assert!(converged, "副本 WAL 尾必须追平主端（积压记录帧全量落盘）");
  assert_eq!(
    rm_r.get_current_replication_offset().get(0),
    Some(primary_tail),
    "副本复制位点必须推进到主端尾"
  );
  let driver = rm_p
    .aof_sync_driver_store
    .drivers()
    .into_iter()
    .find(|d| d.remote_node_id() == REPLICA_ID)
    .expect("推流驱动在册");
  assert!(
    driver.is_connected(),
    "推流驱动必须保持连接（记录帧衔接通过，未触发 divergent 断流）"
  );

  // ===== 增量续推衔接：主端追加记录，副本位点继续推进（divergent 断流则冻结）
  for i in 0..2 {
    source
      .wal
      .enqueue(format!("loop-live-{i}").as_bytes())
      .unwrap();
  }
  let new_tail = source.wal.tail_address() as i64;
  let _ = assets.pump.sync_backlog(&source.wal).await;
  let caught_up = wait_for(
    || rm_r.get_current_replication_offset().get(0) == Some(new_tail),
    Duration::from_secs(5),
  )
  .await;
  assert!(caught_up, "增量记录帧必须衔接落位（副本位点持续推进）");
  assert_eq!(replica.wal.tail_address() as i64, new_tail);

  server.dispose();
}
