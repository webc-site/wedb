//! 复制管理器核心状态机与位点同步集成测试
use std::{
  fs::{read, write},
  sync::Arc,
  thread,
  time::{Duration, Instant},
};

use compio::{
  runtime::{Runtime, spawn},
  time::sleep,
};
use event_listener::Event;
use waof::AofAddress;
use wcpr::{
  CheckpointMeta, CheckpointType, FORMAT_VERSION, HlogMeta, IndexMeta, StoreMeta, meta_filename,
};
use wedb::server::{
  replication::{
    checkpoint_entry::{CheckpointEntry, CheckpointMetadata},
    recovery_status::RecoveryStatus,
    replication_history::ReplicationHistory,
    replication_manager::{ReplicationManager, ResyncStrategy},
    sync_metadata::SyncMetadata,
  },
  worker::NodeRole,
};

/// 构造领先当前位点的目标地址
fn ahead_offset(mgr: &ReplicationManager) -> AofAddress {
  let mut target = mgr.get_current_replication_offset();
  target.set(0, target.get(0).unwrap_or(0) + 100);
  target
}

/// 位点等待中断面：abort 事件触发即以未追平收口、在途等待项注销归零，
/// 不挂满剩余超时（failover abort 响应性的取消面对位 C# cts.Cancel）
#[test]
fn test_offset_wait_abort_interrupts() {
  let mgr = Arc::new(ReplicationManager::with_options(2, None, false));
  let target = ahead_offset(&mgr);
  let abort = Arc::new(Event::new());
  Runtime::new().unwrap().block_on(async {
    let trigger = Arc::clone(&abort);
    spawn(async move {
      sleep(Duration::from_millis(50)).await;
      // 仅触发中断，不推进位点：位点若追平则 select 以追平优先，无法验证中断面
      trigger.notify(usize::MAX);
    })
    .detach();

    let start = Instant::now();
    let mut abort_listener = abort.listen();
    let caught = mgr
      .wait_for_replication_offset_async_with_abort(
        &target,
        Some(Duration::from_secs(30)),
        &mut abort_listener,
      )
      .await;
    let elapsed = start.elapsed();
    assert!(!caught, "abort 触发后应以未追平收口");
    assert!(
      elapsed < Duration::from_secs(5),
      "abort 应立即打断位点等待而非挂满超时: {elapsed:?}"
    );
    assert!(!mgr.has_offset_waiters(), "在途等待项应已注销");
  });
}

/// 中断面在场时追平路径不受影响：abort 未触发，位点推进照常精准唤醒
#[test]
fn test_offset_wait_convergence_with_abort_surface() {
  let mgr = Arc::new(ReplicationManager::with_options(2, None, false));
  let target = ahead_offset(&mgr);
  let abort = Arc::new(Event::new());
  Runtime::new().unwrap().block_on(async {
    let mgr2 = Arc::clone(&mgr);
    let target2 = target;
    spawn(async move {
      sleep(Duration::from_millis(50)).await;
      let v = target2.get(0).unwrap_or(0);
      mgr2.set_sublog_replication_offset(0, v);
    })
    .detach();

    let start = Instant::now();
    let mut abort_listener = abort.listen();
    let caught = mgr
      .wait_for_replication_offset_async_with_abort(
        &target,
        Some(Duration::from_secs(30)),
        &mut abort_listener,
      )
      .await;
    let elapsed = start.elapsed();
    assert!(caught, "位点推进后应精准唤醒判追平");
    assert!(
      elapsed < Duration::from_secs(5),
      "不应挂满超时: {elapsed:?}"
    );
    assert!(!mgr.has_offset_waiters(), "追平后等待项应已摘除");
  });
}

/// 无超时位点等待收口面（对标 C# WaitForReplicationOffsetAsync 唯一提前
/// 退出面 ctsRepManager）：在途等待被 rm dispose 打断，按 C# :572 口径
/// 应答 -1 位点哨兵——不悬挂、也不静默答一个看似追平的位点
#[test]
fn test_offset_wait_unbounded_interrupted_by_dispose() {
  let mgr = Arc::new(ReplicationManager::with_options(2, None, false));
  let target = ahead_offset(&mgr);
  Runtime::new().unwrap().block_on(async {
    let killer = Arc::clone(&mgr);
    spawn(async move {
      sleep(Duration::from_millis(50)).await;
      killer.dispose();
    })
    .detach();

    let start = Instant::now();
    let offset = mgr.wait_for_replication_offset_async(&target).await;
    let elapsed = start.elapsed();
    assert_eq!(
      offset,
      AofAddress::create(2, -1),
      "停机应答应等于 C# Create(AofPhysicalSublogCount, -1) 哨兵"
    );
    assert!(
      elapsed < Duration::from_secs(5),
      "dispose 应立即打断无超时位点等待: {elapsed:?}"
    );
    assert!(!mgr.has_offset_waiters(), "在途等待项应已注销");
  });
}

/// 停机后再进入的位点等待：粘滞标志短路即刻返回 -1 位点，不登记等待项
/// （对标 C# 轮询环首轮即观察到 ctsRepManager 已取消）
#[test]
fn test_offset_wait_unbounded_after_dispose_returns_negative() {
  let mgr = ReplicationManager::with_options(2, None, false);
  let target = ahead_offset(&mgr);
  mgr.dispose();
  let offset = Runtime::new()
    .unwrap()
    .block_on(mgr.wait_for_replication_offset_async(&target));
  assert_eq!(offset, AofAddress::create(2, -1));
  assert!(!mgr.has_offset_waiters(), "短路返回不应登记等待项");
}

/// 无超时位点等待追平面：位点推进精准唤醒并应答当下位点；追平延时无论
/// 多长都不会被内层超时误判（旧 10 秒魔法常量在类型面已不存在）
#[test]
fn test_offset_wait_unbounded_convergence() {
  let mgr = Arc::new(ReplicationManager::with_options(2, None, false));
  let target = ahead_offset(&mgr);
  Runtime::new().unwrap().block_on(async {
    let mgr2 = Arc::clone(&mgr);
    let target2 = target;
    spawn(async move {
      sleep(Duration::from_millis(50)).await;
      let v = target2.get(0).unwrap_or(0);
      mgr2.set_sublog_replication_offset(0, v);
    })
    .detach();

    let start = Instant::now();
    let offset = mgr.wait_for_replication_offset_async(&target).await;
    let elapsed = start.elapsed();
    assert!(offset.equals_all(&target), "追平后应答当下位点: {offset:?}");
    assert!(
      elapsed < Duration::from_secs(5),
      "位点推进应精准唤醒: {elapsed:?}"
    );
    assert!(!mgr.has_offset_waiters(), "追平后等待项应已摘除");
  });
}

#[test]
fn test_replication_manager_full_flow() {
  let mgr = ReplicationManager::with_options(2, None, false);
  // 对标 C# 构造：初始位点为空日志起点（rust WalLog 无头区，判据 begin == tail）
  assert_eq!(mgr.get_replication_offset(0), 0);
  mgr.set_sublog_replication_offset(0, 100);
  assert_eq!(mgr.get_replication_offset(0), 100);

  // 对标 C# SetSublogReplicationOffset 直接赋值语义
  mgr.set_sublog_replication_offset(0, 50);
  assert_eq!(mgr.get_replication_offset(0), 50);
  mgr.set_sublog_replication_offset(0, 100);
  assert_eq!(mgr.get_replication_offset(0), 100);

  // 故障转移位点轮转测试
  let repl_id = mgr.primary_repl_id();
  mgr.try_update_for_failover();
  assert_eq!(mgr.primary_repl_id2(), repl_id);
  assert_ne!(mgr.primary_repl_id(), repl_id);
  assert_eq!(mgr.get_replication_offset2().get(0), Some(100));

  // 恢复状态机测试
  assert_eq!(mgr.recovery_status(), RecoveryStatus::NoRecovery);
  assert!(!mgr.is_recovering());

  assert!(mgr.begin_recovery(RecoveryStatus::InitializeRecover, false));
  assert!(mgr.is_recovering());
  assert!(mgr.cannot_stream_aof());

  mgr.end_recovery(RecoveryStatus::CheckpointRecoveredAtReplica, false);
  assert!(!mgr.cannot_stream_aof());

  mgr.end_recovery(RecoveryStatus::NoRecovery, false);
  assert!(!mgr.is_recovering());
}

#[test]
fn test_reset_replica_replay_driver_store_rebuilds() {
  let mgr = Arc::new(ReplicationManager::with_options(2, None, false));
  // 注册驱动后重置：容器应重建且可再次注册（对标 C# Dispose + new）
  assert!(mgr.initialize_replica_replay_driver(0));
  assert!(!mgr.initialize_replica_replay_driver(0));
  mgr.reset_replica_replay_driver_store();
  assert!(mgr.initialize_replica_replay_driver(0));
}

/// 复制域恢复：PRIMARY 走检查点内存索引初始化（rust 纯内存模型下
/// initialize(None) 即重置，磁盘 cookie 加载已剥离——见 checkpoint_store
/// 架构说明）；REPLICA 跳过（等待与 primary 重新同步）
#[test]
fn test_recover_async_replication_domain() {
  let mgr = ReplicationManager::with_options(2, None, false);
  let initial = mgr.get_recovered_safe_aof_address();

  let mut meta = CheckpointMetadata::new(2);
  meta.store_checkpoint_covered_aof_address = AofAddress::create(2, 777);
  mgr
    .checkpoint_store
    .write()
    .add_checkpoint_entry(CheckpointEntry::new(meta), true);

  let rt = Runtime::new().expect("compio runtime");
  // REPLICA：不动检查点内存索引（条目保留），恢复安全位点保持初始值
  rt.block_on(mgr.recover_async(false));
  assert!(mgr.checkpoint_store.read().latest_entry().is_some());
  assert_eq!(mgr.get_recovered_safe_aof_address(), initial);

  // PRIMARY：initialize(None) 重置内存索引（条目清空）——重启后内存
  // 本为空，此处验证的是初始化路径可达且不残留旧代条目
  rt.block_on(mgr.recover_async(true));
  assert!(mgr.checkpoint_store.read().latest_entry().is_none());
  assert_eq!(mgr.get_recovered_safe_aof_address(), initial);
}

/// C# 构造门控三分支（ReplicationManager.cs:159-177）：
/// recover=true 且 replication.conf 非空 → replid 历史跨重启保留（修复前
/// 生产装配无持久化目录，主端重启丢历史、副本被迫全量重同步）；
/// recover=false → 初始化新历史并覆盖旧文件（对标 InitializeReplicationHistory
/// 尾段 FlushConfig）；空目录 recover=true → 新历史落盘 replication.conf
#[test]
fn test_with_options_replication_history_gate() {
  let dir = tempfile::tempdir().expect("tempdir");
  let config_dir = dir.path().join("cluster");

  // 第一代：推进位点 + failover 轮转 replid（触发 flush 落盘）
  let first = ReplicationManager::with_options(2, Some(&config_dir), false);
  first.set_sublog_replication_offset(0, 900);
  first.try_update_for_failover();
  let (id, id2, offset2) = (
    first.primary_repl_id(),
    first.primary_repl_id2(),
    first.get_replication_offset2(),
  );
  assert!(!id2.is_empty());

  // 分支 1：recover=true + 文件非空 → replid 历史与轮转位点跨重启保留
  let recovered = ReplicationManager::with_options(2, Some(&config_dir), true);
  assert_eq!(recovered.primary_repl_id(), id);
  assert_eq!(recovered.primary_repl_id2(), id2);
  assert_eq!(recovered.get_replication_offset2(), offset2);

  // 分支 2：recover=false → 新历史覆盖旧文件（对标 C# Initialize + FlushConfig）
  let fresh = ReplicationManager::with_options(2, Some(&config_dir), false);
  assert_ne!(fresh.primary_repl_id(), id);
  let reloaded = ReplicationManager::with_options(2, Some(&config_dir), true);
  assert_eq!(
    reloaded.primary_repl_id(),
    fresh.primary_repl_id(),
    "旧历史须已被新历史覆盖"
  );

  // 分支 3：空目录 + recover=true → 初始化新历史并落盘 replication.conf
  let empty_dir = tempfile::tempdir().expect("tempdir").path().join("cluster");
  let boot = ReplicationManager::with_options(2, Some(&empty_dir), true);
  assert!(!boot.primary_repl_id().is_empty());
  assert!(
    empty_dir.join("replication.conf").exists(),
    "初始化须落盘 replication.conf"
  );
}

/// 落盘口并发互斥（C# ReplicationHistoryManager.cs:FlushConfig 的 `lock (this)`
/// 对位面）：两个更新入口——副本 attach 恢复面的 try_update_my_primary_repl_id
/// 与 failover 面的 try_update_for_failover——在不同线程交叠反复落盘，判据取
/// 终态而非时序：join 后文件必可解出、且等于终态内存历史（交错写会命中
/// recover_or_init 的损坏分支换 replid → 副本 replid 全失配退全量；
/// 版本倒退说明落盘未与内存版本序一致）
#[test]
fn test_concurrent_history_flush_keeps_single_complete_version() {
  let dir = tempfile::tempdir().expect("tempdir");
  let config_dir = dir.path().join("cluster");
  let mgr = Arc::new(ReplicationManager::with_options(
    2,
    Some(&config_dir),
    false,
  ));

  let mut handles = Vec::new();
  for is_failover in [false, true, false, true] {
    let mgr = Arc::clone(&mgr);
    handles.push(thread::spawn(move || {
      for i in 0..50 {
        if is_failover {
          mgr.try_update_for_failover();
        } else {
          mgr.try_update_my_primary_repl_id(&format!("repl-id-{i}"));
        }
      }
    }));
  }
  for handle in handles {
    handle.join().expect("flush thread panicked");
  }

  let path = config_dir.join("replication.conf");
  let on_disk = ReplicationHistory::from_byte_array(&read(&path).expect("read history"))
    .expect("并发落盘须恒为某单一完整版本");
  assert_eq!(
    on_disk,
    *mgr.current_replication_config.read(),
    "末次 flush 在锁内取当前值，文件须等于终态内存历史"
  );
}

/// 磁盘臂登记的主端检查点（store_hlog_token 非 0、同历史同版本）
fn disk_primary_entry(
  mgr: &ReplicationManager,
  store_version: i64,
  covered: i64,
) -> CheckpointEntry {
  let mut meta = CheckpointMetadata::new(2);
  meta.store_version = store_version;
  meta.store_hlog_token = 0x123;
  meta.store_primary_repl_id = Some(mgr.primary_repl_id());
  meta.store_checkpoint_covered_aof_address = AofAddress::create(2, covered);
  let entry = CheckpointEntry::new(meta);
  mgr
    .checkpoint_store
    .write()
    .add_checkpoint_entry(entry.clone(), true);
  entry
}

/// 磁盘链臂判据：只由 skipLocalMainStoreCheckpoint（两侧 CheckpointEntry 的
/// 历史与 storeVersion）与 AOF 接续位点决断，不读 SyncMetadata.current_store_version
#[test]
fn test_disk_resync_strategy_partial_and_full() {
  let mgr = ReplicationManager::with_options(2, None, false);
  let entry = disk_primary_entry(&mgr, 10, 500);

  let committed = AofAddress::create(2, 1000);
  let primary_begin = AofAddress::create(2, 200);
  let negotiate = |m: &SyncMetadata| mgr.disk_resync_strategy(m, &committed, &primary_begin, false);

  // 1. 同一历史且位点对齐 -> PartialResync（增量流自副本尾位点接续）
  let sync_meta = SyncMetadata {
    full_sync: false,
    origin_node_role: NodeRole::Replica,
    origin_node_id: 0x2E71,
    current_primary_repl_id: mgr.primary_repl_id(),
    current_store_version: 10,
    current_aof_begin_address: AofAddress::create(2, 200),
    current_aof_tail_address: AofAddress::create(2, 800),
    current_replication_offset: AofAddress::create(2, 800),
    checkpoint_entry: Some(entry.clone()),
  };
  match negotiate(&sync_meta) {
    ResyncStrategy::PartialResync {
      sync_start_address,
      replay_aof_mask,
    } => {
      assert_eq!(sync_start_address.get(0), Some(800));
      assert_eq!(replay_aof_mask, 0b11);
    }
    _ => panic!("Expected PartialResync"),
  }

  // 2. 副本 AOF 起始位点超过了检查点覆盖位点（中间缺失） -> 强制 FullResync
  let sync_meta_gap = SyncMetadata {
    current_aof_begin_address: AofAddress::create(2, 600), // > 500
    origin_node_id: 0x2E72,
    ..sync_meta.clone()
  };
  assert!(matches!(
    negotiate(&sync_meta_gap),
    ResyncStrategy::FullResync { .. }
  ));

  // 3. 同历史同版本、无 AOF 增量（replayAOFMap = 0）的重连：C# 磁盘链
  //    skipLocalMainStoreCheckpoint 为真即不下发快照，续推协商位点，不落
  //    FullResync（合并判据下此形态恒判 Full，重连白推一遍检查点）
  let sync_meta_idle = SyncMetadata {
    current_store_version: 0,
    current_aof_tail_address: AofAddress::create(2, 500),
    current_replication_offset: AofAddress::create(2, 500),
    origin_node_id: 0x2E73,
    ..sync_meta.clone()
  };
  let ResyncStrategy::PartialResync {
    sync_start_address,
    replay_aof_mask,
  } = negotiate(&sync_meta_idle)
  else {
    panic!("idle 重连须为 PartialResync（不下发检查点）");
  };
  assert_eq!(replay_aof_mask, 0, "无增量可回放");
  assert_eq!(sync_start_address.get(0), Some(500));

  // 4. store 版本维度不被磁盘臂消费：同一 idle 元数据把上报版本抬到任意值
  //    仍判 Partial
  for version in [7i64, 999, i64::MAX] {
    let meta = SyncMetadata {
      current_store_version: version,
      origin_node_id: 0x2E74,
      ..sync_meta_idle.clone()
    };
    assert!(
      matches!(negotiate(&meta), ResyncStrategy::PartialResync { .. }),
      "磁盘臂判据不得引用 current_store_version（上报版本 {version}）"
    );
  }

  // 5. 副本检查点版本与主端不等 -> skipLocalMainStoreCheckpoint 为假 ->
  //    FullResync（版本比对取自 CheckpointEntry.metadata.store_version）
  let mut stale_replica = entry.clone();
  stale_replica.metadata.store_version = 11;
  let sync_meta_stale = SyncMetadata {
    checkpoint_entry: Some(stale_replica),
    origin_node_id: 0x2E75,
    ..sync_meta.clone()
  };
  assert!(matches!(
    negotiate(&sync_meta_stale),
    ResyncStrategy::FullResync { .. }
  ));
}

/// 无盘臂判据：对标 DisklessReplication/ReplicaSyncSession.cs:NeedToFullSync
/// 的三条件（历史、store 版本 !=、副本 AOF 尾位点越界；第 4 条回放量门限在
/// rust 配置面缺席），方向是 != 而非存在性判据
#[test]
fn test_diskless_resync_strategy_need_full_sync_conditions() {
  let mgr = ReplicationManager::with_options(2, None, false);
  let entry = disk_primary_entry(&mgr, 10, 500);

  const PRIMARY_STORE_VERSION: i64 = 10;
  let committed = AofAddress::create(2, 1000);
  let primary_begin = AofAddress::create(2, 200);
  let primary_tail = AofAddress::create(2, 1000);
  let negotiate = |m: &SyncMetadata| {
    mgr.diskless_resync_strategy(
      m,
      PRIMARY_STORE_VERSION,
      &committed,
      &primary_begin,
      &primary_tail,
      false,
    )
  };

  // 同历史、同版本、尾位点在可服务区间内、无待回放量 -> 免快照放行
  let base = SyncMetadata {
    full_sync: false,
    origin_node_role: NodeRole::Replica,
    origin_node_id: 0x3E71,
    current_primary_repl_id: mgr.primary_repl_id(),
    current_store_version: PRIMARY_STORE_VERSION,
    current_aof_begin_address: AofAddress::create(2, 200),
    current_aof_tail_address: AofAddress::create(2, 500),
    current_replication_offset: AofAddress::create(2, 500),
    checkpoint_entry: Some(entry.clone()),
  };
  assert!(matches!(
    negotiate(&base),
    ResyncStrategy::PartialResync { .. }
  ));

  // 版本不等即全量（!= 方向）：副本版本高于主端
  let ahead = SyncMetadata {
    current_store_version: 11,
    origin_node_id: 0x3E72,
    ..base.clone()
  };
  assert!(matches!(
    negotiate(&ahead),
    ResyncStrategy::FullResync { .. }
  ));
  // 副本版本 0（尚无 store）而主端非 0：同为不等，仍全量
  let zero = SyncMetadata {
    current_store_version: 0,
    origin_node_id: 0x3E73,
    ..base.clone()
  };
  assert!(matches!(
    negotiate(&zero),
    ResyncStrategy::FullResync { .. }
  ));

  // 副本尾位点越出主端可服务区间上界 -> 全量（C# outOfRangeAof 臂）
  let beyond = SyncMetadata {
    current_aof_tail_address: AofAddress::create(2, 4000),
    current_replication_offset: AofAddress::create(2, 4000),
    origin_node_id: 0x3E74,
    ..base.clone()
  };
  assert!(matches!(
    negotiate(&beyond),
    ResyncStrategy::FullResync { .. }
  ));
  // 副本尾位点低于主端 AOF 起点（AOF 已被截断）-> 全量
  let truncated = SyncMetadata {
    current_aof_tail_address: AofAddress::create(2, 100),
    current_replication_offset: AofAddress::create(2, 100),
    origin_node_id: 0x3E75,
    ..base.clone()
  };
  assert!(matches!(
    negotiate(&truncated),
    ResyncStrategy::FullResync { .. }
  ));

  // 主从历史不一致 -> 全量（C# sendMainStore 的 !sameHistory 臂）
  let other_history = SyncMetadata {
    current_primary_repl_id: "0000000000000000000000000000000f".to_string(),
    origin_node_id: 0x3E76,
    ..base.clone()
  };
  assert!(matches!(
    negotiate(&other_history),
    ResyncStrategy::FullResync { .. }
  ));

  // 四条件皆不成立且有增量 -> 免快照，从协商位点续推 AOF
  let replay = SyncMetadata {
    current_aof_tail_address: AofAddress::create(2, 800),
    current_replication_offset: AofAddress::create(2, 800),
    origin_node_id: 0x3E77,
    ..base.clone()
  };
  let ResyncStrategy::PartialResync {
    sync_start_address,
    replay_aof_mask,
  } = negotiate(&replay)
  else {
    panic!("expected PartialResync");
  };
  assert_eq!(sync_start_address.get(0), Some(800));
  assert_eq!(replay_aof_mask, 0b11);
}

#[test]
fn test_data_loss_check() {
  let mgr = ReplicationManager::with_options(1, None, false);
  let begin = AofAddress::create(1, 1000);
  let ok_req = AofAddress::create(1, 1200);
  let bad_req = AofAddress::create(1, 800);

  assert!(mgr.data_loss_check(false, &ok_req, &begin).is_ok());
  assert!(mgr.data_loss_check(false, &bad_req, &begin).is_err());
  assert!(mgr.data_loss_check(true, &bad_req, &begin).is_ok());
}

#[test]
fn test_add_checkpoint_entry_registers_into_store() {
  let mgr = ReplicationManager::with_options(2, None, false);
  let mut meta = CheckpointMetadata::new(2);
  meta.store_version = 7;
  let latest = mgr.checkpoint_store.read().latest_entry();
  assert!(latest.is_none());

  mgr.add_checkpoint_entry(CheckpointEntry::new(meta), true);
  let latest = mgr.checkpoint_store.read().latest_entry();
  assert_eq!(latest.expect("registered").metadata.store_version, 7);
}

#[test]
fn test_replication_manager_dispose_and_offset_operations() {
  let mgr = ReplicationManager::with_options(2, None, false);
  mgr.initialize_replication_history(2);
  mgr.try_update_my_primary_repl_id("node-primary-1");
  assert_eq!(mgr.primary_repl_id(), "node-primary-1");

  mgr.set_replication_checkpoint_start_offset(AofAddress::create(2, 500));
  assert_eq!(
    mgr.get_replication_checkpoint_start_offset().get(0),
    Some(500)
  );
  mgr.set_sublog_checkpoint_start_offset(1, 800);
  assert_eq!(
    mgr.get_replication_checkpoint_start_offset().get(1),
    Some(800)
  );

  // 验证 dispose 释放所有驱动与等待读者生命周期打通
  mgr.dispose();
}

/// INFO CINFO disk_checkpoint_entry 磁盘探测面（对标 C#
/// ReplicationCheckpointManagement.cs:GetLatestCheckpointFromDiskInfo：
/// 扫盘读最新快照元数据输出，缺失/损坏回退 "(empty)"）
#[test]
fn test_get_latest_checkpoint_from_disk_info() {
  let mgr = ReplicationManager::new();

  // 未注入目录：缺席形态
  assert_eq!(mgr.get_latest_checkpoint_from_disk_info(), "(empty)");

  // 注入空目录：无快照同为缺席形态
  let dir = tempfile::tempdir().expect("tempdir");
  mgr.set_checkpoint_dir(dir.path().to_path_buf());
  assert_eq!(mgr.get_latest_checkpoint_from_disk_info(), "(empty)");

  // 写入封签完备的快照元数据：输出 token（hex）与 AOF 覆盖地址
  let mut meta = CheckpointMeta {
    token: 0x0123_4567_89ab_cdef_u128,
    cp_type: CheckpointType::Snapshot,
    index_meta: IndexMeta {
      size: 64,
      overflow_count: 2,
      entry_count: 100,
    },
    index_start_logical_address: 12288,
    hlog_meta: HlogMeta {
      begin_address: 64,
      head_address: 4096,
      flushed_until_address: 8192,
      tail_address: 16384,
    },
    store_meta: StoreMeta {
      index_size: 64,
      page_size: 16384,
      num_pages: 8,
      mutable_fraction: 0.5,
      max_sessions: 64,
      enable_revivification: false,
      enable_read_cache: true,
      read_cache_num_pages: 8,
      range_index_dir: None,
      next_key_id: 7,
    },
    created_at: 1_700_000_000_000,
    checkpoint_aof_address: Some(0x4000),
    format_version: FORMAT_VERSION,
    integrity_crc32: 0,
  };
  meta.seal();
  write(dir.path().join(meta_filename(meta.token)), meta.encode()).expect("write meta");

  let info = mgr.get_latest_checkpoint_from_disk_info();
  assert!(
    info.contains("storeHlogToken=123456789abcdef"),
    "got: {info}"
  );
  assert!(
    info.contains("storeIndexToken=123456789abcdef"),
    "got: {info}"
  );
  assert!(
    info.contains("storeCheckpointCoveredAofAddress=16384"),
    "got: {info}"
  );

  // 元数据损坏：解码失败回退缺席形态（对标 C# catch 分支）
  let corrupt = tempfile::tempdir().expect("tempdir");
  mgr.set_checkpoint_dir(corrupt.path().to_path_buf());
  write(corrupt.path().join(meta_filename(1)), b"junk").expect("write junk");
  assert_eq!(mgr.get_latest_checkpoint_from_disk_info(), "(empty)");
}
