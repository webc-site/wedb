//! ClusterProvider 检查点覆盖与副本截断集成测试（自 src/server/cluster_provider.rs
//! 内嵌测试迁出）
//!
//! 多组件装配（ClusterProvider + ClusterManager + ReplicationManager +
//! GarnetLog）：检查点覆盖位点按角色取源、副本侧安全截断物理推进日志 begin。
//! 对标 C# ClusterProvider.cs:OnCheckpointInitiated 与 SafeTruncateAOF。

use std::sync::Arc;

use waof::AofAddress;
use wedb::server::{
  cluster::{CheckpointCallbackFace, IClusterProvider},
  cluster_provider::ClusterProvider,
  replication::recovery_status::RecoveryStatus,
  worker::NodeRole,
};

/// 对标 C# OnCheckpointInitiated：角色判定只看配置（LocalNodeRole），
/// 主节点恢复期（is_recovering 为真）不得误入副本分支取
/// ReplicationCheckpointStartOffset（副本检查点截断位点）。
#[test]
fn primary_recovering_takes_current_replication_offset() {
  let provider = ClusterProvider::new();
  provider
    .cluster_manager()
    .unwrap()
    .try_set_local_node_role(NodeRole::Primary);

  let rm = provider.replication_manager().unwrap();
  let current = AofAddress::create(1, 256);
  let start = AofAddress::create(1, 64);
  rm.set_current_replication_offset(current);
  rm.set_replication_checkpoint_start_offset(start);

  // 置恢复态：旧实现 is_replica() 含 is_recovering 分支，此处会误取 start_offset
  assert!(rm.begin_recovery(RecoveryStatus::InitializeRecover, false));
  assert!(rm.is_recovering());

  let mut covered = AofAddress::create(1, 0);
  provider.on_checkpoint_initiated(&mut covered);
  assert_eq!(
    covered.get(0),
    current.get(0),
    "主节点恢复期仍应取当前复制位点"
  );
  assert_ne!(covered.get(0), start.get(0), "不得取副本检查点开始位点");
}

/// 配置角色为 REPLICA 时取 ReplicationCheckpointStartOffset
#[test]
fn replica_by_config_takes_checkpoint_start_offset() {
  let provider = ClusterProvider::new();
  provider
    .cluster_manager()
    .unwrap()
    .try_set_local_node_role(NodeRole::Replica);

  let rm = provider.replication_manager().unwrap();
  let current = AofAddress::create(1, 256);
  let start = AofAddress::create(1, 64);
  rm.set_current_replication_offset(current);
  rm.set_replication_checkpoint_start_offset(start);

  let mut covered = AofAddress::create(1, 0);
  provider.on_checkpoint_initiated(&mut covered);
  assert_eq!(
    covered.get(0),
    start.get(0),
    "REPLICA 应取检查点开始标记位点"
  );
}

/// 副本侧安全截断物理闭环（对标 C# ClusterProvider.SafeTruncateAOF
/// else 分支 `appendOnlyFile?.Log.TruncateUntil(truncateUntil)`——
/// 副本无 Commit，刷盘由复制流驱动）
#[test]
fn replica_safe_truncate_physically_shifts_log_begin() {
  use wbase::entry_type::AofEntryType;
  use wconf::RuntimeServerOptions;
  use wnode::{
    GarnetAppendOnlyFile, GarnetLog, InMemorySublog, Sublog, aof::garnet_log::RecordShape,
  };

  let provider = ClusterProvider::new();
  provider
    .cluster_manager()
    .unwrap()
    .try_set_local_node_role(NodeRole::Replica);

  // 内存单子日志 AOF 门面（装配期 set_aof 注入）
  let options = RuntimeServerOptions::default();
  let log = Arc::new(GarnetLog::new(
    &options,
    vec![Arc::new(Sublog::Mem(InMemorySublog::new()))],
    None,
  ));
  provider.set_aof(Some(Arc::new(GarnetAppendOnlyFile::new(
    Arc::clone(&log),
    &options,
    None,
  ))));

  let record = RecordShape {
    op_type: AofEntryType::StoreUpsert,
    version: 1,
    session_id: 1,
    key: b"k",
    value: b"v",
    input: &[],
    database_id: 0,
  };
  log.enqueue(&record);
  assert!(log.get_tail_address(0) > 1);
  assert_eq!(log.get_begin_address(0), 1);

  // 副本检查点覆盖地址（ReplicationCheckpointStartOffset 形态）→ 物理 begin 推进
  let covered = AofAddress::create(1, 64);
  // 预置复制位点超前于覆盖地址：截断不得回退位点（C# 副本分支不触碰
  // replicationOffset）
  let rm = provider.replication_manager().unwrap();
  rm.set_current_replication_offset(AofAddress::create(1, 128));
  // Arc 自动解引用至 trait 面（对标 C# provider.SafeTruncateAOF）
  provider.safe_truncate_aof(&covered);
  assert_eq!(
    log.get_begin_address(0),
    64,
    "副本物理 begin 应推进至截断位点"
  );
  assert_eq!(
    rm.get_current_replication_offset().get(0),
    Some(128),
    "截断不得回退副本复制位点"
  );
}
