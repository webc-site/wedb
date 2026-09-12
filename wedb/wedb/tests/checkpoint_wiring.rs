//! 集群检查点全链接线集成测试（PRIMARY 侧）
//!
//! 验证对标 C# DatabaseManagerBase.InitiateCheckpointAsync 的完整链：
//! OnCheckpointInitiated（复制域给出覆盖地址）→ 版本切换回调写
//! CheckpointStart/EndCommit 标记到 storeWrapper AOF 通道 → AddNewCheckpointEntry

use std::sync::Arc;

use parking_lot::Mutex;
use waof::{AofAddress, AofEntryType};
use wedb::server::{
  cluster_provider::ClusterProvider,
  replication::{CheckpointCallbackFace, StoreCommitChannel, recovery_status::RecoveryStatus},
  worker::NodeRole,
};

#[test]
fn primary_checkpoint_flow_wires_cluster_callbacks() {
  let commit_log = Arc::new(Mutex::new(Vec::new()));

  // 1. 集群装配：ClusterProvider（本地节点设为 PRIMARY）+ commit 通道 + 版本切换回调
  let provider = ClusterProvider::new();
  provider
    .cluster_manager()
    .unwrap()
    .try_set_local_node_role(NodeRole::Primary);
  let channel = StoreCommitChannel::Recorder(commit_log.clone());
  provider.set_commit_channel(Some(channel));
  let (shift_start, shift_end) = provider.checkpoint_version_shift_hooks();

  // 主侧复制位点预先推进
  let rm_offset = 256i64;
  let rm = provider.replication_manager().unwrap();
  rm.set_current_replication_offset(AofAddress::create(1, rm_offset));

  // 2. 检查点发起：获取覆盖地址
  let mut covered_addr = AofAddress::create(1, 0);
  provider.on_checkpoint_initiated(&mut covered_addr);
  assert_eq!(covered_addr.get(0), Some(rm_offset));

  // 3. 版本切换
  shift_start(0, 42);
  shift_end(0, 42);

  // 4. 断言：CheckpointStartCommit/EndCommit 标记按序落入 commit 通道
  {
    let guard = commit_log.lock();
    assert_eq!(
      *guard,
      vec![
        (AofEntryType::CheckpointStartCommit, 42),
        (AofEntryType::CheckpointEndCommit, 42),
      ],
      "版本切换回调应按序写入 Start/End 标记"
    );
  }

  // 5. 登记检查点
  provider.add_new_checkpoint_entry(true, covered_addr, 1001, 1001);

  // 6. 断言：检查点条目已登记
  let entry = rm
    .checkpoint_store
    .read()
    .latest_entry()
    .expect("entry registered");
  assert_eq!(
    entry.metadata.store_checkpoint_covered_aof_address.get(0),
    Some(rm_offset),
    "覆盖地址应取 OnCheckpointInitiated 给出的安全位点"
  );
  assert_eq!(entry.metadata.store_hlog_token, 1001);

  // 7. 断言：SafeTruncateAOF 已推进驱动仓库截断下界
  assert_eq!(
    rm.aof_sync_driver_store.get_truncated_until().get(0),
    Some(rm_offset),
    "PRIMARY 无副本时安全截断应直达覆盖地址"
  );
}

/// REPLICA 角色：版本切换回调不写标记
#[test]
fn replica_role_skips_commit_markers() {
  let commit_log = Arc::new(Mutex::new(Vec::new()));

  let provider = ClusterProvider::new();
  let channel = StoreCommitChannel::Recorder(commit_log.clone());
  provider.set_commit_channel(Some(channel));
  let (shift_start, shift_end) = provider.checkpoint_version_shift_hooks();

  shift_start(0, 100);
  shift_end(0, 100);
  assert_eq!(
    *commit_log.lock(),
    vec![
      (AofEntryType::CheckpointStartCommit, 100),
      (AofEntryType::CheckpointEndCommit, 100),
    ]
  );

  // REPLICA 恢复态：begin_recovery 后 is_replica() 为真，闭包短路不写
  let rm = provider.replication_manager().unwrap();
  assert!(rm.begin_recovery(RecoveryStatus::InitializeRecover, false));
  assert!(provider.is_replica());
  shift_start(0, 200);
  shift_end(0, 200);
  assert_eq!(
    commit_log.lock().len(),
    2,
    "REPLICA 角色下不应新增标记"
  );
}
