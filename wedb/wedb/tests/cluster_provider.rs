//! ClusterProvider 检查点覆盖与副本截断集成测试（自 src/server/cluster_provider.rs
//! 内嵌测试迁出）
//!
//! 多组件装配（ClusterProvider + ClusterManager + ReplicationManager +
//! GarnetLog）：检查点覆盖位点按角色取源、副本侧安全截断物理推进日志 begin。
//! 对标 C# ClusterProvider.cs:OnCheckpointInitiated 与 SafeTruncateAOF。

use std::sync::Arc;

use compio::runtime::Runtime;
use waof::AofAddress;
use wedb::server::{
  cluster::{CheckpointCallbackFace, IClusterProvider},
  cluster_provider::ClusterProvider,
  replication::recovery_status::RecoveryStatus,
  worker::NodeRole,
};
use wnode::ClusterProvider as _;

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
  <ClusterProvider as CheckpointCallbackFace>::on_checkpoint_initiated(&provider, &mut covered);
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
  <ClusterProvider as CheckpointCallbackFace>::on_checkpoint_initiated(&provider, &mut covered);
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
  use waof::AofEntryType;
  use wconf::RuntimeServerOptions;
  use wnode::{GarnetAppendOnlyFile, GarnetLog, RecordShape};

  let provider = ClusterProvider::new();
  provider
    .cluster_manager()
    .unwrap()
    .try_set_local_node_role(NodeRole::Replica);

  // 轻量真实段设备单子日志 AOF 门面（装配期 set_aof 注入）
  let options = RuntimeServerOptions::default();
  let log = Arc::new(
    GarnetLog::new(
      &options,
      {
        let (_dirs, backends) = wnode_test::test_sublogs("cluster_provider", 1);
        backends
      },
      None,
    )
    .expect("构造 GarnetLog"),
  );
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
    // 大载荷：提交后 committed 越过截断位点 64，物理截断不被 min(committed) 钳回
    value: &[0u8; 128],
    input: &[],
    database_id: 0,
  };
  // 副本检查点覆盖地址（ReplicationCheckpointStartOffset 形态）→ 物理 begin 推进
  let covered = AofAddress::create(1, 64);
  let rm = provider.replication_manager().unwrap();
  Runtime::new().unwrap().block_on(async {
    let _ = log.enqueue(&record);
    assert!(log.get_tail_address(0) > 64, "尾地址应越过截断位点");
    assert_eq!(log.get_begin_address(0), 0);
    // 提交刷盘推进 committed（物理截断受 min(committed) 钳制，须先落盘）
    log.commit_async().await;
    assert!(log.committed_until_address().get(0).unwrap_or(0) > 64);
    // 预置复制位点超前于覆盖地址：截断不得回退位点（C# 副本分支不触碰
    // replicationOffset）
    rm.set_current_replication_offset(AofAddress::create(1, 128));
    // Arc 自动解引用至 trait 面（对标 C# provider.SafeTruncateAOF）
    provider.safe_truncate_aof(&covered).await;
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
  });
}

/// INFO 复制段副本侧指标组（对标 libs/cluster/Server/ClusterProvider.cs
/// GetReplicationInfo 副本分支 15 字段与基座 12 字段）
#[test]
fn replication_info_replica_lag_fields() {
  use std::sync::atomic::Ordering;

  use wbase::time::now_ms_i64;
  use wconf::RuntimeServerOptions;
  use wnode::{GarnetAppendOnlyFile, GarnetLog};
  use wresp::metrics::MetricsItem;

  let provider = ClusterProvider::new();
  provider
    .cluster_manager()
    .unwrap()
    .try_set_local_node_role(NodeRole::Replica);

  // 轻量真实段设备单子日志 AOF 门面（装配期 set_aof 注入）
  let options = RuntimeServerOptions::default();
  let log = Arc::new(
    GarnetLog::new(
      &options,
      {
        let (_dirs, backends) = wnode_test::test_sublogs("cluster_provider", 1);
        backends
      },
      None,
    )
    .expect("构造 GarnetLog"),
  );
  provider.set_aof(Some(Arc::new(GarnetAppendOnlyFile::new(
    Arc::clone(&log),
    &options,
    None,
  ))));
  provider.set_aof_replay_max_lag_bytes(1024);

  // 复制位点清零：日志尾与位点的差 = 滞后
  let rm = provider.replication_manager().unwrap();
  rm.set_current_replication_offset(AofAddress::create(1, 0));

  let info = provider.get_replication_info();
  let get = |items: &[MetricsItem], name: &str| {
    items
      .iter()
      .find(|i: &&MetricsItem| i.name == name)
      .map(|i| i.value.clone())
  };

  // 发现三：REPLICA 角色且主指 None 的配置快照，副本分支 15 字段与基座 12 字段齐备（共 27 字段）
  assert_eq!(
    info.len(),
    27,
    "副本分支 15 字段与基座 12 字段须齐备（共 27 字段）"
  );
  assert_eq!(
    get(&info, "master_host").unwrap(),
    "",
    "未配置 primary address 时 master_host 应输出空字符串"
  );
  assert_eq!(get(&info, "master_port").unwrap(), "-1");

  // 发现一 & 发现二：稳定态（!is_recovering）下 master_sync_in_progress 输出 "False"，master_sync_last_io_seconds_ago 恒为 0
  assert_eq!(get(&info, "master_sync_in_progress").unwrap(), "False");
  assert_eq!(get(&info, "master_sync_last_io_seconds_ago").unwrap(), "0");

  let tail = log.get_tail_address(0);
  assert_eq!(
    get(&info, "replication_offset_vector_lag").unwrap(),
    tail.to_string(),
    "向量滞后 = 日志尾 - 复制位点"
  );
  assert_eq!(
    get(&info, "replication_offset_acc_lag").unwrap(),
    tail.to_string(),
    "聚合滞后 = 逐槽差之和（单槽即同值）"
  );
  assert_eq!(get(&info, "aof_replay_max_lag_bytes").unwrap(), "1024");
  // 读一致性管理器未装配：对齐 C# rcm == null 分支输出 -1
  assert_eq!(
    get(&info, "physical_sublog_max_sequence_vector").unwrap(),
    "-1"
  );
  assert_eq!(
    get(&info, "physical_sublog_max_drift_sequence_vector").unwrap(),
    "-1"
  );

  // 发现一 & 发现二（恢复态分支）：置恢复态并回填单调时间戳
  assert!(rm.begin_recovery(RecoveryStatus::InitializeRecover, false));
  rm.primary_sync_last_timestamp
    .store(now_ms_i64() - 5_000, Ordering::Release);
  let info_recovering = provider.get_replication_info();
  assert_eq!(info_recovering.len(), 27);
  // 发现一：恢复中输出首字母大写 "True"
  assert_eq!(
    get(&info_recovering, "master_sync_in_progress").unwrap(),
    "True",
    "恢复中 master_sync_in_progress 须以首字母大写 True 渲染"
  );
  // 发现二：恢复中输出实际流逝秒数（约 5s）
  let io_secs: i64 = get(&info_recovering, "master_sync_last_io_seconds_ago")
    .unwrap()
    .parse()
    .unwrap();
  assert!(
    (4..=6).contains(&io_secs),
    "恢复态 master_sync_last_io_seconds_ago 须返回实际流逝秒数（约 5s），实得 {io_secs}"
  );

  // 恢复结束切回稳定态
  rm.end_recovery(RecoveryStatus::NoRecovery, false);
  let info_ended = provider.get_replication_info();
  assert_eq!(info_ended.len(), 27);
  assert_eq!(
    get(&info_ended, "master_sync_in_progress").unwrap(),
    "False",
    "恢复结束后 master_sync_in_progress 切回 False"
  );
  assert_eq!(
    get(&info_ended, "master_sync_last_io_seconds_ago").unwrap(),
    "0",
    "恢复结束后 master_sync_last_io_seconds_ago 恒回 0"
  );
}

/// 副本 attach 超时单点映射与哨兵语义（对标 C# ReplicaAttachTimeout / GetTimeSpan）：
/// 1. 缺省 60s 回归（未注入 runtime_config 回退 Some(60s) 保嵌入形态）
/// 2. 注入 runtime_config 缺省值 60s 回归
/// 3. 配 0 断言 None 且 attach 无 60s 上界（wait_async None 分支无限等待）
/// 4. CONFIG SET repl-attach-timeout 0 同效（槽位 min=0 合法接受）
/// 5. 正数透传
/// 6. 负数配置（CLI allow_negative_numbers）经 seconds_from_time_span 归 0 断言 None
#[test]
fn repl_attach_timeout_sentinel_and_mapping() {
  use std::time::Duration;

  use wconf::{RuntimeServerConfig, RuntimeServerOptions, ServerConfigType};

  // 1. 未注入 runtime_config：缺省 60s 回退（保嵌入形态）
  let provider = ClusterProvider::new();
  assert_eq!(
    provider.repl_attach_timeout(),
    Some(Duration::from_secs(60)),
    "未注入 runtime_config 时应回落 Some(60s)"
  );

  // 2. 注入默认 runtime_config：缺省 60s 回归
  let cfg = Arc::new(RuntimeServerConfig::with_defaults());
  provider.set_runtime_config(Arc::clone(&cfg));
  assert_eq!(
    provider.repl_attach_timeout(),
    Some(Duration::from_secs(60)),
    "注入默认配置时应为 Some(60s)"
  );

  // 3. 配 0：断言 None 且 attach 无 60s 上界
  let cfg_zero = Arc::new(RuntimeServerConfig::new(RuntimeServerOptions {
    replica_attach_timeout_secs: 0,
    ..Default::default()
  }));
  let provider_zero = ClusterProvider::new();
  provider_zero.set_runtime_config(cfg_zero);
  assert_eq!(
    provider_zero.repl_attach_timeout(),
    None,
    "配 0 时应返回 None（无限等待，无 60s 上界）"
  );

  // 4. CONFIG SET repl-attach-timeout 0 同效（槽位 min=0 合法接受）
  assert_eq!(
    cfg.try_set(ServerConfigType::ReplAttachTimeout, "0"),
    Ok(None),
    "CONFIG SET repl-attach-timeout 0 须合法接受"
  );
  assert_eq!(
    provider.repl_attach_timeout(),
    None,
    "CONFIG SET 0 后应返回 None（无限等待）"
  );

  // 5. 正数透传
  assert_eq!(
    cfg.try_set(ServerConfigType::ReplAttachTimeout, "120"),
    Ok(None)
  );
  assert_eq!(
    provider.repl_attach_timeout(),
    Some(Duration::from_secs(120)),
    "正数配置须透传为对应 Duration"
  );

  // 6. 负数配置（CLI allow_negative_numbers 放行）经 seconds_from_time_span 归 0 断言 None
  let cfg_neg = Arc::new(RuntimeServerConfig::new(RuntimeServerOptions {
    replica_attach_timeout_secs: -5,
    ..Default::default()
  }));
  let provider_neg = ClusterProvider::new();
  provider_neg.set_runtime_config(cfg_neg);
  assert_eq!(
    provider_neg.repl_attach_timeout(),
    None,
    "负数配置应经 seconds_from_time_span 归 0 表现为 None（无限等待）"
  );
}
