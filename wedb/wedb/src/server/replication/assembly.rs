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
//! - 副本重连发起：C# ReplicationManager.RecoverReplication →
//!   TryReplicateDiskbasedSyncAsync（libs/cluster/Server/Replication/
//!   ReplicaOps/ReplicaDiskbasedSync.cs:ReplicaSyncAttachTaskAsync）——
//!   副本清空本地重放状态后向主端发 CLUSTER INITIATEREPLICASYNC
//!   （5 参：节点 id、指派主 repl id、检查点条目、副本 AOF begin/tail），
//!   主端 +OK 后回连副本建立 APPENDLOG 推流；本模块
//!   [`recover_replication`] 承接同一流程（ensure_replication 断链时
//!   后台任务直调驱动）。

use std::{sync::Arc, time::Duration};

use compio::time::timeout;
use waof::{AofAddress, WalLog};
use wdev::SegmentedDevice;

use super::{
  aof_replication_pump::AofReplicationPump, checkpoint_entry::CheckpointEntry,
  cluster_replication_session::ClusterReplicationSession, replica_sync_session::ReplicaSyncSession,
};
use crate::{
  client::GarnetClient,
  server::cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
};

/// 推流泵空闲节流周期（C# defaults.conf ReplicaSyncDelayMs = 5：
/// ServerConfigType.REPLICA_SYNC_DELAY 默认值）
const REPLICA_SYNC_DELAY: Duration = Duration::from_millis(5);

/// 复制数据面装配（宿主启动路径与集成测试共用的唯一装配体；AOF 门控
/// 点亮时调用一次）
///
/// 副本接收会话的 replay 端口传 None：记录帧重放推进走重放驱动仓库
///（init 帧握手注册驱动 + `consume_direct` 位点推进），与会话断链处置
///（dispose 释放驱动仓库）同源同寿
pub fn wire_replication_data_plane(
  cluster: &Arc<ClusterProvider>,
  wal: Arc<WalLog<SegmentedDevice>>,
) {
  let Some(rm) = cluster.replication_manager() else {
    return;
  };
  // 副本接收面：CLUSTER APPENDLOG → 保真落盘 + 重放驱动位点推进
  cluster.set_replica_replication_session(Some(Arc::new(ClusterReplicationSession::new(
    Arc::clone(cluster),
    Arc::clone(&wal),
    None,
  ))));
  // 主端推流面：策略协商 + 建连 + 补扫的发起资产
  let pump = Arc::new(AofReplicationPump::new(Arc::clone(
    &rm.aof_sync_driver_store,
  )));
  pump.start_throttle_loop(REPLICA_SYNC_DELAY);
  cluster.set_primary_replication(Some(Arc::new(PrimaryReplicationAssets {
    wal: Arc::clone(&wal),
    pump,
    sync_session: Arc::new(ReplicaSyncSession::new(Arc::clone(&rm))),
  })));
  // 本地日志句柄（副本重连发起的 begin/tail 位点源）
  cluster.set_wal(wal);
}

/// 单槽位点序列化为 span 字节（C# `AofAddress.Span` 形态：去长度头）
fn aof_span(address: i64) -> Vec<u8> {
  let mut bytes = AofAddress::create(1, address).serialize();
  bytes.remove(0);
  bytes
}

/// 副本重连发起动作（EnsureReplication 第 7 步后台任务体；对标 C#
/// ReplicationManager.RecoverReplication → TryReplicateDiskbasedSyncAsync）
///
/// 流程（对标 C# ReplicaSyncAttachTaskAsync 发起段）：
/// 1. 清空本地重放驱动仓库（重注册由主端 init 帧握手完成；此处预注册会令
///    IsReplicating 状态面误报「流活跃」而中断 ensure_replication 静默重试）；
/// 2. 构造 5 参（节点 id、指派主 repl id、检查点条目、副本 AOF begin/tail；
///    无检查点时上报空条目，对标 C# GetLatestCheckpointEntryFromDisk 空库
///    语义）；
/// 3. 专用客户端向主端发起 CLUSTER INITIATEREPLICASYNC，cluster_timeout
///    级超时（failover 治理同款，杜绝无限挂起）；
/// 4. 应答面：成功仅记日志（数据面由主端回连异步建立）；失败 / 超时静默
///    （ensure_replication 轮询节流驱动下一轮重试，对标 C# RecoverReplication
///    失败后按轮询节奏重试）
pub async fn recover_replication(provider: &Arc<ClusterProvider>, primary: &str) {
  let Some(rm) = provider.replication_manager() else {
    return;
  };
  // 1. 清空本地重放驱动仓库
  rm.reset_replica_replay_driver_store();

  // 2. 主端 endpoint 与本端节点 id（集群配置反查）
  let Some(cm) = provider.cluster_manager() else {
    return;
  };
  let (address, port) = cm.current_config().get_local_node_primary_address();
  let Some(node_id) = cm
    .current_config()
    .local_node_id()
    .map(String::from)
    .filter(|id| !id.is_empty())
  else {
    log::warn!("Replication recovery to {primary} skipped: local node id unknown");
    return;
  };
  let Some(address) = address else {
    log::warn!("Replication recovery to {primary} skipped: primary endpoint unknown");
    return;
  };

  // 3. 发起参数束
  let checkpoint_entry = rm
    .checkpoint_store
    .read()
    .latest_entry()
    .map(|entry| entry.to_byte_array())
    .unwrap_or_else(|| CheckpointEntry::with_sublogs(1).to_byte_array());
  let Some(wal) = provider.try_wal() else {
    log::warn!("Replication recovery to {primary} skipped: local wal not wired");
    return;
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
    timeout(
      Duration::from_millis(provider.cluster_node_timeout_ms()),
      client.initiate_replica_sync_async(
        &node_id,
        &rm.primary_repl_id(),
        &checkpoint_entry,
        &aof_begin,
        &aof_tail,
      ),
    )
    .await
    .map_err(|_| "timed out".to_string())
  } else {
    Err("not connected".to_string())
  };
  client.dispose();

  // 5. 应答面：失败静默，节流轮询驱动重试
  match res {
    Ok(Ok(_)) => log::info!("Replica sync initiated to {primary}"),
    Ok(Err(msg)) => {
      log::warn!("Failed to initiate replica sync to {primary}: {msg}");
    }
    Err(msg) => {
      log::warn!("INITIATEREPLICASYNC to {primary} failed: {msg}");
    }
  }
}

#[cfg(test)]
mod tests {
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
