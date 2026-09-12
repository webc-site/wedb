//! 主端副本同步会话（INITIATE_REPLICA_SYNC 服务面与副本 attach 链）
//!
//! 对标 C# ReplicaSyncSession（libs/cluster/Server/Replication/PrimaryOps/
//! DiskbasedReplication/ReplicaSyncSession.cs）——副本发起同步请求后，
//! 主端协商同步策略、建立副本发送通道（AofSyncDriver + wire）、挂推流
//! 泵并补扫存量积压。C# SendCheckpointAsync 的检查点快照下发段（SNAPSHOT
//! 传输）不在本轮范围（网络快照面另行承接），本会话承接其 startAofSync
//! 尾段：TryAddReplicationDriver + TryConnectToReplica（AofSyncDriver.RunAsync
//! → connect + APPENDLOG_INIT + 迭代泵）。

use std::sync::Arc;

use waof::{AofAddress, WalLog};
use wdev::Device;

use crate::server::replication::{
  aof_replication_pump::AofReplicationPump,
  aof_sync_driver::AofSyncDriver,
  replica_wire::{AofSyncWire, TcpSessionWire},
  replication_manager::{ReplicationManager, ResyncStrategy},
  sync_metadata::SyncMetadata,
};

/// 主端副本同步会话
pub struct ReplicaSyncSession {
  rm: Arc<ReplicationManager>,
  local_node_id: String,
}

impl ReplicaSyncSession {
  /// libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:ReplicaSyncSession
  pub fn new(rm: Arc<ReplicationManager>, local_node_id: String) -> Self {
    Self { rm, local_node_id }
  }

  /// 副本驱动入库并接线发送通道（内存通道 attach 形态）
  ///
  /// 对标 C# SendCheckpointAsync 尾段 TryAddReplicationDriver +
  /// TryConnectToReplica 的组合（wire 已由装配方建立）；重复 attach 先移除
  /// 旧驱动（对标 C# AcquireCheckpointEntryAsync 内 AssertDoesNotExist +
  /// 断链重连的驱动置换语义）
  pub fn attach_replica_wire(
    &self,
    remote_node_id: &str,
    wire: Arc<dyn AofSyncWire>,
    start_address: &AofAddress,
  ) -> bool {
    self.rm.aof_sync_driver_store.try_remove(remote_node_id);
    let driver = Arc::new(AofSyncDriver::new(
      self.local_node_id.clone(),
      remote_node_id.to_string(),
      start_address,
    ));
    if !self
      .rm
      .aof_sync_driver_store
      .try_add_replication_driver(driver.clone(), false)
    {
      return false;
    }
    driver.attach_wire(wire);
    true
  }

  /// INITIATE_REPLICA_SYNC 主端处理：协商策略 → TCP 通道建连（含 init 帧
  /// 握手）→ 驱动入库接线 → 推流泵挂载 → 存量补扫
  ///
  /// 对标 C# SendCheckpointAsync 的 #region startAofSync 段；返回授予副本
  /// 的同步起始位点（syncFromAofAddress 应答载荷）。FullResync 场景下
  /// 检查点快照下发未接线，退化为从协商位点起的 AOF 全量直推（副本空库
  /// 重放模型，数据一致性由 AofProcessor 唯一重放保证；差异见模块文档）
  pub async fn initiate_replica_sync<D: Device>(
    &self,
    pump: &Arc<AofReplicationPump>,
    wal: &WalLog<D>,
    replica_endpoint: &str,
    replica_meta: &SyncMetadata,
    fast_aof_truncate: bool,
  ) -> Result<AofAddress, String> {
    // 1. 同步策略协商（committed = 主端已提交位；primary_begin = 主端 AOF 起点）
    let committed_until = AofAddress::create(1, wal.committed_until_address() as i64);
    let primary_aof_begin = AofAddress::create(1, wal.begin_address() as i64);
    let strategy = self.rm.determine_resync_strategy(
      replica_meta,
      &committed_until,
      &primary_aof_begin,
      fast_aof_truncate,
    );
    let sync_start = match &strategy {
      ResyncStrategy::PartialResync {
        sync_start_address, ..
      }
      | ResyncStrategy::FullResync {
        sync_start_address, ..
      } => *sync_start_address,
    };

    // 2. 建立副本 TCP 发送通道（connect 即建连 + APPENDLOG init 握手等 +OK；
    //    副本会话对 init 帧注册重放驱动，握手成功即副本接收面就绪）
    let wire = TcpSessionWire::connect(replica_endpoint, &self.local_node_id, 0, None, None)
      .await
      .map_err(|e| format!("Failed connecting to replica for aofSync: {e}"))?;

    // 3. 驱动入库 + 通道接线（TryAdd 失败即截断拒绝，对标 C# 拒绝语义）
    if !self.attach_replica_wire(&replica_meta.origin_node_id, wire, &sync_start) {
      return Err("Failed trying to try update replication task".to_string());
    }

    // 4. 推流泵挂载（此后主端新写入同栈分发）+ 存量补扫（attach 前的记录）
    pump.attach_sink(wal);
    let (forwarded, skipped) = pump
      .sync_backlog(wal)
      .await
      .map_err(|e| format!("AOF backlog sync failed: {e}"))?;
    log::info!(
      "Replica {} aof sync attached from {:?}: backlog forwarded {forwarded}, skipped {skipped}",
      replica_meta.origin_node_id,
      sync_start
    );

    Ok(sync_start)
  }
}
