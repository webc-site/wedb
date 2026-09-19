//! 集群检查点全链接线集成测试（PRIMARY 侧）
//!
//! 验证对标 C# DatabaseManagerBase.InitiateCheckpointAsync 的完整链：
//! OnCheckpointInitiated（复制域给出覆盖地址）→ 版本切换回调写
//! CheckpointStart/EndCommit 标记到 storeWrapper AOF 通道 → AddNewCheckpointEntry

use std::{fs::create_dir_all, sync::Arc};

use compio::runtime::Runtime;
use parking_lot::Mutex;
use waof::{AofAddress, AofEntryType};
use wedb::server::{
  cluster_provider::ClusterProvider,
  replication::{CheckpointCallbackFace, StoreCommitFn, recovery_status::RecoveryStatus},
  worker::NodeRole,
};
use wnode::{
  ClusterProvider as _,
  database::{GarnetDatabase, SingleDatabaseManager},
};

/// 注入记录型提交回调（测试可观测注入点：断言版本切换标记按序送达写入面）
fn recorder(commit_log: Arc<Mutex<Vec<(AofEntryType, i64)>>>) -> StoreCommitFn {
  Arc::new(move |entry_type: AofEntryType, version: i64| {
    commit_log.lock().push((entry_type, version));
  })
}

#[test]
fn primary_checkpoint_flow_wires_cluster_callbacks() {
  let commit_log = Arc::new(Mutex::new(Vec::new()));

  // 1. 集群装配：ClusterProvider（本地节点设为 PRIMARY）+ commit 回调 + 版本切换回调
  let provider = ClusterProvider::new();
  provider
    .cluster_manager()
    .unwrap()
    .try_set_local_node_role(NodeRole::Primary);
  provider.set_commit_channel(Some(recorder(commit_log.clone())));
  let shift_start = |new: i64| provider.checkpoint_version_shift_start(new);
  let shift_end = |new: i64| provider.checkpoint_version_shift_end(new);

  // 主侧复制位点预先推进
  let rm_offset = 256i64;
  let rm = provider.replication_manager().unwrap();
  rm.set_current_replication_offset(AofAddress::create(1, rm_offset));

  // 2. 检查点发起：获取覆盖地址
  let mut covered_addr = AofAddress::create(1, 0);
  provider.on_checkpoint_initiated(&mut covered_addr);
  assert_eq!(covered_addr.get(0), Some(rm_offset));

  // 3. 版本切换
  shift_start(42);
  shift_end(42);

  // 4. 断言：CheckpointStartCommit/EndCommit 标记按序落入 commit 通道
  {
    let guard = commit_log.lock();
    assert_eq!(
      *guard,
      vec![
        (AofEntryType::CheckpointStartCommit, 42),
        (AofEntryType::CheckpointEndCommit, 42),
      ],
      "版本切换通知应按序写入 Start/End 标记"
    );
  }

  // 5. 登记检查点（AddNewCheckpointEntry → SafeTruncateAOF：现收敛为
  // async 单口径，须 await 方可推进 truncated_until 记账，见步骤 7 断言）
  Runtime::new()
    .unwrap()
    .block_on(provider.add_new_checkpoint_entry(true, covered_addr, 1001, 1001));

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

/// REPLICA 角色：版本切换通知不写标记
#[test]
fn replica_role_skips_commit_markers() {
  let commit_log = Arc::new(Mutex::new(Vec::new()));

  let provider = ClusterProvider::new();
  provider.set_commit_channel(Some(recorder(commit_log.clone())));
  let shift_start = |new: i64| provider.checkpoint_version_shift_start(new);
  let shift_end = |new: i64| provider.checkpoint_version_shift_end(new);

  shift_start(100);
  shift_end(100);
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
  shift_start(200);
  shift_end(200);
  assert_eq!(commit_log.lock().len(), 2, "REPLICA 角色下不应新增标记");
}

/// 按需检查点拍摄与条目自动登记测试
#[test]
fn on_demand_checkpoint_takes_and_registers_entry() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (store_dir, store) = wtest_base::open_test_store("odc_test")?;
    let cp_dir = store_dir.path().join("checkpoints");
    create_dir_all(&cp_dir).unwrap();
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      cp_dir.clone(),
      None,
    ));
    let dm = Arc::new(SingleDatabaseManager::new(cp_dir.clone(), db));

    let provider = ClusterProvider::new();
    provider.set_checkpoint_dir(cp_dir);
    provider.set_database_manager(dm);

    // 初始状态下检查点仓库为空
    let rm = provider.replication_manager().unwrap();
    assert!(rm.checkpoint_store.read().latest_entry().is_none());

    // 触发按需检查点拍摄
    let res = provider.take_on_demand_checkpoint().await;
    assert!(res.is_ok_and(|b| b));

    // 断言按需检查点条目已自动登记进 replication_manager 的 checkpoint_store
    let latest = rm.checkpoint_store.read().latest_entry();
    assert!(latest.is_some(), "按需检查点应自动登记进 checkpoint_store");
    let entry = latest.unwrap();
    assert_ne!(entry.metadata.store_hlog_token, 0);

    Ok(())
  })
}

/// 快照磁盘保留单轨收口：读者闸门接通物理 unlink。
/// 传输会话持读者期间，检查点登记触发的淘汰链 TrySuspendReaders 失败停手，
/// 在传快照文件不被 unlink；释放读者后下一轮登记回收全部陈旧 token
/// （对标 C# CheckpointStore.DeleteOutdatedCheckpoints 读者感知淘汰：
/// TrySuspendReaders -> CanDeleteToken -> DeleteLog/DeleteIndexCheckpoint）
#[test]
fn disk_retention_follows_reader_gate() -> aok::Void {
  use wcpr::list_checkpoints;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let (store_dir, store) = wtest_base::open_test_store("reader_gate_retention")?;
    let cp_dir = store_dir.path().join("checkpoints");
    create_dir_all(&cp_dir).unwrap();
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      cp_dir.clone(),
      None,
    ));
    let dm = Arc::new(SingleDatabaseManager::new(cp_dir.clone(), db));

    let provider = ClusterProvider::new();
    provider.set_checkpoint_dir(cp_dir.clone());
    // 对齐生产装配链（boot.rs:105 attach_flush_gate）：集群宿主下数据库管理器
    // 必持 cluster 句柄——该句柄即 C# removeOutdated = !EnableCluster 的形态位，
    // 缺位会被检查点内核判为单机形态、走引擎级按代回收，与本用例要证的复制域
    // 读者闸门轨并行（正是要杜绝的第二轨形态）
    dm.attach_flush_gate(provider.clone());
    provider.set_database_manager(dm);
    let rm = provider.replication_manager().unwrap();
    // 对齐生产装配链（boot.rs:118 rm.set_checkpoint_dir 传导 checkpoint_store）：
    // 缺此注入时淘汰链无物理删除能力，本用例读者闸门断言空转
    rm.set_checkpoint_dir(cp_dir.clone());

    // 第 1 次检查点：登记 e1（磁盘 t1）
    assert!(provider.take_on_demand_checkpoint().await.unwrap());
    let t1 = rm
      .checkpoint_store
      .read()
      .latest_entry()
      .expect("entry registered")
      .metadata
      .store_hlog_token;

    // 传输会话持读者（C# TryGetLatestCheckpointEntryFromMemory 形态）
    let reader = rm
      .checkpoint_store
      .read()
      .try_get_latest_checkpoint_entry_from_memory()
      .expect("reader");

    // 第 2、3 次检查点：淘汰链 TrySuspendReaders 失败停手，t1 必须保留
    //（按条数纯 unlink 的旧形态下，第 3 次检查点尾段即回收 t1）
    for _ in 0..2 {
      assert!(provider.take_on_demand_checkpoint().await.unwrap());
    }
    let tokens = list_checkpoints(&cp_dir).unwrap();
    assert!(
      tokens.contains(&t1),
      "读者持有期间快照 t1 不得被 unlink: {tokens:?}"
    );

    // 释放读者后下一次检查点：淘汰链推进，陈旧 token 全部回收
    reader.remove_reader();
    assert!(provider.take_on_demand_checkpoint().await.unwrap());
    let latest = rm
      .checkpoint_store
      .read()
      .latest_entry()
      .expect("entry registered")
      .metadata
      .store_hlog_token;
    assert_eq!(
      list_checkpoints(&cp_dir).unwrap(),
      vec![latest],
      "释放读者后下一轮登记回收全部陈旧 token"
    );

    Ok(())
  })
}
