//! Primary 类后台任务角色门控集成测试
//!
//! 对标 libs/server/StoreWrapper.cs 的 SuspendPrimaryOnlyTasksAsync /
//! StartPrimaryTasks 家族与 libs/cluster 侧四处角色切换调用点
//!（ClusterManagerWorkerState.cs:226 降副本、ReplicaOfCommand.cs:50
//! REPLICAOF、ReplicaFailoverSession.cs:346 接管、ReplicaDiskbasedSync.cs:154
//! 全量同步前）：
//! - 挂起后 Primary 类任务停（GC 扫描不再推进 + 任务域角色位置位）；
//! - 升主恢复后 GC 重启、周期任务复跑；
//! - 降副本（try_add_replica_async）接线后 GC 与任务域同步挂起。

use std::{sync::Arc, time::Duration};

use compio::{runtime::Runtime, time::sleep};
use waof::AofAddress;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  replication::{
    aof_sync_driver::AofSyncDriver, assembly::try_replicate_sync_async,
    replicate_sync_options::ReplicateSyncOptions,
  },
  worker::{LocalWorkerSpec, NodeRole, Worker},
};
use wkv::{StoreConfig, WedbStore};
use wnode::{PrimaryTasks, aof::aof_backpressure::AofBackpressure, open_node_with_config};
use wtest_base::test_store_config;

/// 挂起后 GC 扫描停推（统计计数不再前进），恢复后重新推进
#[test]
fn suspend_stops_gc_scan_and_resume_restarts() -> aok::Void {
  Runtime::new()?.block_on(async {
    let dir = tempfile::tempdir()?;
    let (store, _broker, _vm): (Arc<WedbStore<SegmentedDevice>>, _, _) =
      open_node_with_config(short_scan_config(), dir.path().join("gate.db"))?;
    assert!(store.gc_running(), "前置：内置 GC 扫描在跑");

    let provider = ClusterProvider::new();
    provider.set_store(Arc::clone(&store));
    let tasks: Arc<PrimaryTasks> = Arc::new(PrimaryTasks::default());
    provider.set_primary_tasks(tasks);
    // 挂起（降副本/全量同步前的统一入口）：先降副本，GC 停循环 + 角色位置位
    provider
      .cluster_manager()
      .unwrap()
      .try_set_local_node_role(NodeRole::Replica);
    provider.suspend_primary_tasks();
    assert!(provider.primary_tasks().unwrap().is_replica());
    assert!(!store.gc_running(), "降副本后 GC 扫描必须停止");

    // 恢复（REPLICAOF NO ONE / 接管的统一入口）：先升主，GC 重启 + 角色位复位
    provider
      .cluster_manager()
      .unwrap()
      .try_set_local_node_role(NodeRole::Primary);
    provider.resume_primary_tasks();
    assert!(!provider.primary_tasks().unwrap().is_replica());
    assert!(store.gc_running(), "升主后 GC 扫描必须恢复");
    Ok::<(), aok::Error>(())
  })?;
  Ok(())
}

/// 恢复态副本装配：set_primary_tasks 按当前角色同步挂起（对标
/// StoreWrapper.Start 按角色分派）
#[test]
fn replica_boot_suspends_primary_tasks() -> aok::Void {
  Runtime::new()?.block_on(async {
    let dir = tempfile::tempdir()?;
    let (store, _broker, _vm) =
      open_node_with_config(short_scan_config(), dir.path().join("replica.db"))?;
    store.start_gc();
    assert!(store.gc_running());

    let provider = ClusterProvider::new();
    provider
      .cluster_manager()
      .unwrap()
      .try_set_local_node_role(NodeRole::Replica);
    provider.set_store(Arc::clone(&store));
    provider.set_primary_tasks(Arc::new(PrimaryTasks::default()));

    assert!(provider.is_replica());
    assert!(
      provider.primary_tasks().unwrap().is_replica(),
      "恢复态副本装配必须挂起 Primary 类任务域"
    );
    assert!(!store.gc_running(), "恢复态副本装配必须停掉 GC 扫描");
    Ok::<(), aok::Error>(())
  })?;
  Ok(())
}

/// 降副本接线（ClusterManagerWorkerState.cs:226 对标）：try_add_replica_async
/// 完成后 Primary 类任务挂起
#[test]
fn try_add_replica_suspends_primary_tasks() -> aok::Void {
  Runtime::new()?.block_on(async {
    let cp = ClusterProvider::new();
    cp.set_store({
      let dir = tempfile::tempdir()?;
      let (store, ..) = open_node_with_config(short_scan_config(), dir.path().join("r.db"))?;
      store.start_gc();
      // 目录句柄生命周期覆盖测试体：tempdir 由闭包持有至返回
      Box::leak(Box::new(dir));
      store
    });
    let m = cp.cluster_manager().unwrap();
    {
      let mut config = m.current_config.write();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
        address: "127.0.0.1",
        port: 7000,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
      config.workers.push(Worker {
        nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_DE12),
        address: "127.0.0.1".into(),
        port: 7001,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        replication_offset: 0,
        hostname: None,
      });
    }

    let tasks: Arc<PrimaryTasks> = Arc::new(PrimaryTasks::default());
    cp.set_primary_tasks(tasks);
    assert!(!cp.primary_tasks().unwrap().is_replica());

    // 本地无槽位（force=true 跳过槽位校验）：降副本成功即挂起
    m.try_add_replica_async(0x0000_0000_0000_0000_0000_0000_0000_DE12, true, false)
      .await?;
    assert!(
      cp.primary_tasks().unwrap().is_replica(),
      "降副本后 Primary 类任务必须挂起"
    );
    assert!(
      !cp.try_store().unwrap().gc_running(),
      "降副本后 GC 扫描必须停止"
    );
    Ok::<(), aok::Error>(())
  })?;
  Ok(())
}

/// 降副本清残留主端推流驱动（对标 C# ReplicaSyncAttachTaskAsync 段
/// aofSyncDriverStore.Reset——"Remove aofSync tasks if this node was a
/// primary"）：try_add_replica_async 配置翻转成功后旧驱动出册、背压闸门
/// 写 MAX 水位释放（shipped watermark 残留不得钉死背压闸门）
#[test]
fn try_add_replica_resets_aof_sync_driver_store() -> aok::Void {
  Runtime::new()?.block_on(async {
    let cp = Arc::new(ClusterProvider::new());
    let m = ClusterManager::new(Arc::clone(&cp));
    {
      let mut config = m.current_config.write();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
        address: "127.0.0.1",
        port: 7000,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
      config.workers.push(Worker {
        nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_DE12),
        address: "127.0.0.1".into(),
        port: 7001,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        replication_offset: 0,
        hostname: None,
      });
    }

    // 预置主端推流驱动 + 背压闸门：attach 后闸门收紧到驱动起点
    let rm = cp.replication_manager().unwrap();
    let bp = Arc::new(AofBackpressure::new(2, 4096));
    rm.aof_sync_driver_store
      .attach_backpressure(Some(Arc::clone(&bp)));
    let d = Arc::new(AofSyncDriver::new(
      0x0000_0000_0000_0000_0000_0000_0000_DE11,
      0x0000_0000_0000_0000_0000_0000_0000_DE22,
      2,
      &AofAddress::create(2, 0),
      None,
    ));
    assert!(
      rm.aof_sync_driver_store
        .try_add_replication_driver(d, false)
    );
    assert_eq!(rm.aof_sync_driver_store.count(), 1, "前置：推流驱动在册");
    assert_eq!(
      bp.get_shipped_watermark(0),
      0,
      "前置：副本 attach 后闸门收紧"
    );

    // 降副本：配置翻转成功即清残留驱动、闸门释放
    m.try_add_replica_async(0x0000_0000_0000_0000_0000_0000_0000_DE12, true, false)
      .await?;
    assert_eq!(
      rm.aof_sync_driver_store.count(),
      0,
      "降副本后残留主端推流驱动必须清空"
    );
    assert_eq!(
      bp.get_shipped_watermark(0),
      i64::MAX,
      "降副本后背压闸门必须释放"
    );
    Ok::<(), aok::Error>(())
  })?;
  Ok(())
}

/// 短扫描间隔配置（GC 统计断言的时间余量）
fn short_scan_config() -> StoreConfig {
  let mut config = test_store_config();
  config.gc.enabled = true;
  config.gc.scan_interval_ms = 50;
  config
}

/// 角色守卫对称性（单一守卫结构，zcode-r55-toposwitch 执行方案 2）：
/// suspend/resume 内建配置角色判据——主态挂起与副本态恢复均为 no-op，
/// 杜绝第三处角色变更路径误挂起停摆 GC、或副本角色下错误运转后台任务。
/// 守卫只看配置角色（is_primary），不看恢复期——三处升主路径均在恢复锁内
/// 恢复（C# StartPrimaryTasks 锁内纪律），含恢复期的 is_replica 在此必误判
#[test]
fn role_guard_rejects_cross_role_calls() -> aok::Void {
  Runtime::new()?.block_on(async {
    let dir = tempfile::tempdir()?;
    let (store, _b, _v) = open_node_with_config(short_scan_config(), dir.path().join("g.db"))?;
    let provider = ClusterProvider::new();
    provider.set_store(Arc::clone(&store));
    provider.set_primary_tasks(Arc::new(PrimaryTasks::default()));
    assert!(store.gc_running(), "前置：内置 GC 扫描在跑");

    // 主态 suspend → 守卫 no-op（挂起不得扰主态后台任务）
    provider.suspend_primary_tasks();
    assert!(
      !provider.primary_tasks().unwrap().is_replica(),
      "主态挂起必须被角色守卫拒绝"
    );
    assert!(store.gc_running(), "主态挂起不得停 GC 扫描");

    // 降副本挂起 → 守卫放行（GC 停、任务域挂起）
    provider
      .cluster_manager()
      .unwrap()
      .try_set_local_node_role(NodeRole::Replica);
    provider.suspend_primary_tasks();
    assert!(provider.primary_tasks().unwrap().is_replica());
    assert!(!store.gc_running(), "副本态挂起必须停 GC 扫描");

    // 副本态 resume → 守卫 no-op（GC 与周期任务不得在副本角色下运转）
    provider.resume_primary_tasks();
    assert!(
      provider.primary_tasks().unwrap().is_replica(),
      "副本态恢复必须被角色守卫拒绝"
    );
    assert!(!store.gc_running(), "副本态恢复不得启动 GC 扫描");

    // 翻主 resume → 守卫放行（GC 重启、任务域复位）
    provider
      .cluster_manager()
      .unwrap()
      .try_set_local_node_role(NodeRole::Primary);
    provider.resume_primary_tasks();
    assert!(!provider.primary_tasks().unwrap().is_replica());
    assert!(store.gc_running(), "升主恢复必须重启 GC 扫描");
    Ok::<(), aok::Error>(())
  })?;
  Ok(())
}

/// attach 失败回滚自愈（zcode-r55-toposwitch 发现一）：CLUSTER REPLICATE /
/// REPLICAOF 前台发起至不可达主端，try_add_replica_async 的挂起轴（周期任务
/// 角色位 + store GC）必须在回滚复位主后恢复——否则回滚后的主节点以
/// PRIMARY 继续接写而 GC/周期任务全停，无日志无指标暴露且无自愈通路
///（C# 失败臂同病，rust 多 store.stop_gc 第二停摆轴危害更甚）
#[test]
fn attach_failure_rollback_resumes_primary_tasks() -> aok::Void {
  Runtime::new()?.block_on(async {
    let cp = ClusterProvider::new();
    cp.set_store({
      let dir = tempfile::tempdir()?;
      let (store, ..) = open_node_with_config(short_scan_config(), dir.path().join("rb.db"))?;
      // 目录句柄生命周期覆盖测试体：tempdir 由闭包持有至返回
      Box::leak(Box::new(dir));
      store
    });
    let m = cp.cluster_manager().unwrap();
    {
      let mut config = m.current_config.write();
      config.initialize_local_worker(LocalWorkerSpec {
        node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
        address: "127.0.0.1",
        port: 7000,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
      config.workers.push(Worker {
        nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_DE12),
        address: "127.0.0.1".into(),
        port: 7001,
        config_epoch: 1,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        replication_offset: 0,
        hostname: None,
      });
    }
    let tasks: Arc<PrimaryTasks> = Arc::new(PrimaryTasks::default());
    cp.set_primary_tasks(tasks);
    assert!(cp.try_store().unwrap().gc_running(), "前置：GC 扫描在跑");

    // 前台发起（C# ReplicaOfCommand.cs:79-85 同参：Force/TryAddReplica/
    // AllowReplicaResetOnFailure:true，UpgradeLock:false）：降副本挂起成功、
    // attach 失败（本装配未接线本地 wal），回滚臂复位主 + 恢复后台任务
    let opts = ReplicateSyncOptions::new(
      0x0000_0000_0000_0000_0000_0000_0000_DE12,
      false, // background:false 前台
      true,  // force:true 跳过槽位洁净校验
      true,  // try_add_replica:true（挂起轴触发点）
      true,  // allow_replica_reset_on_failure:true（回滚复位臂）
      false, // upgrade_lock:false
    );
    let result = try_replicate_sync_async(&cp, opts).await;
    assert!(result.is_err(), "不可达主端 attach 必须失败");

    assert!(m.current_config().is_primary(), "回滚后必须复位主角色");
    assert!(
      !cp.primary_tasks().unwrap().is_replica(),
      "回滚后任务域角色位必须复位（周期任务复跑）"
    );
    assert!(
      cp.try_store().unwrap().gc_running(),
      "回滚后内置 GC 扫描必须恢复运转"
    );
    Ok::<(), aok::Error>(())
  })?;
  Ok(())
}

/// 挂起期间 GC 统计不再推进的观测断言（补充 suspend_stops_gc_scan 的
/// 行为级验证：挂起后静置两个扫描周期，累计扫描数必须不变）
#[test]
fn suspended_gc_stats_stop_advancing() -> aok::Void {
  Runtime::new()?.block_on(async {
    let dir = tempfile::tempdir()?;
    let (store, _b, _v) = open_node_with_config(short_scan_config(), dir.path().join("s.db"))?;
    let before = store.gc_stats().map(|s| s.total_scanned).unwrap_or(0);

    let provider = ClusterProvider::new();
    provider.set_store(Arc::clone(&store));
    provider.set_primary_tasks(Arc::new(PrimaryTasks::default()));
    provider
      .cluster_manager()
      .unwrap()
      .try_set_local_node_role(NodeRole::Replica);
    provider.suspend_primary_tasks();
    assert!(!store.gc_running());

    sleep(Duration::from_millis(180)).await;
    let after = store.gc_stats().map(|s| s.total_scanned).unwrap_or(0);
    assert_eq!(before, after, "挂起后 GC 扫描统计不得推进（循环已停）");
    Ok::<(), aok::Error>(())
  })?;
  Ok(())
}
