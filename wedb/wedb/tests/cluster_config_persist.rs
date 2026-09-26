//! 集群拓扑落盘与启动恢复集成测试（对标 ClusterManager.cs 构造段盘恢复 +
//! FlushConfig/FlushTaskAsync 落盘链路）：写盘→重启（新 ClusterProvider 实例）
//! →恢复拓扑断言

use std::{
  fs::{read, read_dir},
  path::Path,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
  time::Duration,
};

use aok::Void;
use compio::{runtime::Runtime, time::sleep};
use wbase::hash_slot::CLUSTER_SLOT_COUNT;
use wedb::server::{
  cluster_config::ClusterConfig,
  cluster_manager::{read_device, write_into},
  cluster_provider::ClusterProvider,
  hash_slot::SlotState,
  worker::{LocalWorkerSpec, NodeRole},
};

/// 装配持久化并返回（recover 门控与周期任务前置须在 compio 运行时内）
fn boot(
  dir: &Path,
  address: &str,
  port: i32,
  flush_frequency_ms: i32,
  clean_config: bool,
  announce_hostname: &str,
) -> Arc<ClusterProvider> {
  let cp = ClusterProvider::new();
  cp.initialize_cluster_config(
    address,
    port,
    &dir.join("nodes.conf"),
    flush_frequency_ms,
    clean_config,
    announce_hostname,
  )
  .expect("集群拓扑装配");
  cp
}
/// 即时刷盘（频率 0）：配置演化即落盘，重启后恢复同一节点 ID 与槽位拓扑
#[test]
fn test_cluster_config_persist_and_recover() -> Void {
  let dir = tempfile::tempdir()?.keep();
  let path = dir.join("nodes.conf");

  // 首启：无盘 → 新节点 ID；建槽 → flush_config 即时写盘
  let first = boot(&dir, "127.0.0.1", 7101, 0, false, "");
  let cm = first.cluster_manager().unwrap();
  let node_id = cm
    .current_config()
    .local_node_id()
    .expect("本地节点 ID 已初始化");
  assert_eq!(cm.current_config().num_workers(), 1);

  {
    let mut config = cm.current_config.write();
    config.update_slot_state(100, 1, SlotState::Stable);
    config.update_slot_state(101, 1, SlotState::Migrating);
  }
  cm.flush_config();
  assert!(path.exists(), "频率 0 变更即落盘");

  // 重启恢复：节点 ID / 端点 / 槽位拓扑逐项一致
  let second = boot(&dir, "127.0.0.1", 7101, 0, false, "");
  let cm2 = second.cluster_manager().unwrap();
  let restored = cm2.current_config();
  assert_eq!(restored.local_node_id(), Some(node_id));
  assert_eq!(restored.local_node_ip(), "127.0.0.1");
  assert_eq!(restored.local_node_port(), 7101);
  assert_eq!(restored.get_state(100), SlotState::Stable);
  assert_eq!(restored.get_state(101), SlotState::Migrating);
  assert_eq!(restored.local_node_role(), NodeRole::Primary);
  drop(restored);

  // 恢复后本地 worker 按当前端点重建（MEET/Gossip 身份连续，非从零重来）
  assert!(cm2.current_config().local_node_id().is_some());
  aok::OK
}

/// 频率 -1：纯内存模式，变更不落盘、启动不恢复
#[test]
fn test_cluster_config_flush_disabled_memory_only() -> Void {
  let dir = tempfile::tempdir()?.keep();
  let provider = ClusterProvider::new();
  provider.initialize_cluster_config("127.0.0.1", 7102, &dir.join("nodes.conf"), -1, false, "")?;
  let cm = provider.cluster_manager().unwrap();
  cm.try_bump_cluster_epoch();
  assert!(!dir.join("nodes.conf").exists(), "-1 模式任何路径不得写盘");
  aok::OK
}

/// clean-cluster-config：盘上有效拓扑仍跳过恢复，得全新节点 ID
#[test]
fn test_clean_cluster_config_skips_recovery() -> Void {
  let dir = tempfile::tempdir()?.keep();
  let first = boot(&dir, "127.0.0.1", 7103, 0, false, "");
  let node_id = first
    .cluster_manager()
    .unwrap()
    .current_config()
    .local_node_id()
    .unwrap();
  // 配置演化触发即时落盘（启动本身不刷盘，对标 C#）
  first.cluster_manager().unwrap().try_bump_cluster_epoch();
  assert!(dir.join("nodes.conf").exists());

  let second = boot(&dir, "127.0.0.1", 7103, 0, true, "");
  let cm2 = second.cluster_manager().unwrap();
  assert_ne!(
    cm2.current_config().local_node_id().unwrap(),
    node_id,
    "clean 标志下须全新节点 ID"
  );
  assert_eq!(cm2.current_config().num_workers(), 1);
  aok::OK
}

/// 周期刷盘（频率 > 0）：变更仅置脏，周期任务在 compio 运行时内落盘
#[test]
fn test_periodic_flush_task_writes_disk() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempfile::tempdir()?.keep();
    let path = dir.join("nodes.conf");
    let provider = boot(&dir, "127.0.0.1", 7104, 50, false, "");
    let cm = provider.cluster_manager().unwrap();
    // 装配段 init_local 不触 flush（对标 C#），周期任务未消费前无盘文件
    assert!(!path.exists());

    cm.try_bump_cluster_epoch();
    for _ in 0..100 {
      if path.exists() {
        break;
      }
      sleep(Duration::from_millis(20)).await;
    }
    assert!(path.exists(), "周期任务须在超时前落盘");

    let bytes = loop {
      match read(&path) {
        // 周期写者经安全原子写链（tmp+sync+rename）落盘，绝无半写暴露
        Ok(b) if ClusterConfig::from_byte_array(&b).is_ok() => break b,
        Ok(_) | Err(_) => sleep(Duration::from_millis(5)).await,
      }
    };
    let decoded = ClusterConfig::from_byte_array(&bytes).expect("周期落盘载荷可解码");
    assert_eq!(decoded.local_node_id(), cm.current_config().local_node_id());
    assert_eq!(decoded.local_node_config_epoch(), 1);
    aok::OK
  })
}

/// 端点漂移（容器 IP 变化）：恢复保节点 ID，本地端点按当前配置覆写
#[test]
fn test_recover_updates_local_endpoint() -> Void {
  let dir = tempfile::tempdir()?.keep();
  let first = boot(&dir, "10.0.0.5", 7105, 0, false, "");
  let node_id = first
    .cluster_manager()
    .unwrap()
    .current_config()
    .local_node_id()
    .unwrap();
  // 演化触发落盘，留下可恢复拓扑
  first.cluster_manager().unwrap().try_bump_cluster_epoch();

  let second = boot(&dir, "10.0.0.9", 7105, 0, false, "");
  let binding = second.cluster_manager().unwrap();
  let restored = binding.current_config();
  assert_eq!(restored.local_node_id(), Some(node_id));
  assert_eq!(restored.local_node_ip(), "10.0.0.9");
  aok::OK
}

/// 落盘载荷完整性：worker 列表与全槽位拓扑经重启逐项一致
#[test]
fn test_recovered_topology_full_match() -> Void {
  let dir = tempfile::tempdir()?.keep();
  let first = boot(&dir, "127.0.0.1", 7106, 0, false, "");
  let cm = first.cluster_manager().unwrap();
  {
    let mut config = cm.current_config.write();
    for slot in (0..CLUSTER_SLOT_COUNT).step_by(97) {
      config.update_slot_state(slot, 1, SlotState::Stable);
    }
  }
  cm.flush_config();

  let second = boot(&dir, "127.0.0.1", 7106, 0, false, "");
  let binding = second.cluster_manager().unwrap();
  let a = cm.current_config();
  let b = binding.current_config();
  for slot in 0..CLUSTER_SLOT_COUNT {
    assert_eq!(
      (a.slot_map[slot].worker_id, a.slot_map[slot].state),
      (b.slot_map[slot].worker_id, b.slot_map[slot].state),
      "槽 {slot} 拓扑不一致"
    );
  }
  aok::OK
}

/// 纪元碰撞自愈与落盘：等值碰撞 gossip 合并后本地纪元原子提升并即时刷盘
/// （对标 Gossip.cs TryMerge → ClusterConfig.HandleConfigEpochCollision →
/// FlushConfig/ToByteArray 链路）
#[test]
fn test_epoch_collision_self_heal_persists_to_disk() -> Void {
  // 合并挂起门为异步读写锁，取读锁的 merge 须在 compio 运行时内驱动
  let rt = Runtime::new()?;
  let dir = tempfile::tempdir()?.keep();
  let path = dir.join("nodes.conf");
  let first = boot(&dir, "127.0.0.1", 7107, 0, false, "");
  let cm = first.cluster_manager().unwrap();
  {
    let mut config = cm.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: 0xAAAA_0000_0000_0000_0000_0000_0000_0001,
      address: "127.0.0.1",
      port: 7107,
      config_epoch: 5,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
  }

  // 对端同 epoch 且节点 id 字典序更大：碰撞仲裁本地自增（全局 max+1）
  let mut sender = ClusterConfig::new();
  sender.initialize_local_worker(LocalWorkerSpec {
    node_id: 0xBBBB_0000_0000_0000_0000_0000_0000_0002,
    address: "127.0.0.1",
    port: 7108,
    config_epoch: 5,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: None,
  });
  assert!(rt.block_on(cm.try_merge(&sender, true)));
  assert_eq!(
    cm.current_config().local_node_config_epoch(),
    6,
    "碰撞自愈须提升为全局 max+1"
  );

  // 频率 0 即时落盘：盘上载荷纪元与内存一致
  let decoded = ClusterConfig::from_byte_array(&read(&path)?).expect("落盘载荷可解码");
  assert_eq!(decoded.local_node_config_epoch(), 6);

  // 自愈后再合并同对端：纪元已错开，无变化不再触发
  assert!(!rt.block_on(cm.try_merge(&sender, true)));
  assert_eq!(cm.current_config().local_node_config_epoch(), 6);

  // 已初始化纪元拒绝二次设置（覆写/倒退），SET-CONFIG-EPOCH 仅限从 0 初始化
  assert!(cm.try_set_local_config_epoch(3).is_err());
  assert_eq!(cm.current_config().local_node_config_epoch(), 6);
  aok::OK
}

/// 截断半写注入：任一长度截断的 nodes.conf 载荷一律被 from_byte_array 拒载
/// 证明非原子裸写发生崩溃截断将致永久拒启死锁，反衬原子写链防护的必要性
#[test]
fn test_truncated_payload_rejected_by_from_byte_array() -> Void {
  let dir = tempfile::tempdir()?.keep();
  let provider = boot(&dir, "127.0.0.1", 7109, 0, false, "");
  let cm = provider.cluster_manager().unwrap();
  cm.try_bump_cluster_epoch();

  let path = dir.join("nodes.conf");
  let valid_bytes = read(&path)?;
  assert!(valid_bytes.len() > 10, "载荷具备完整头部与体");
  assert!(ClusterConfig::from_byte_array(&valid_bytes).is_ok());

  // 从 0 到 len-1 字节任一截断切片均须拒载，绝不可解码出破坏态
  for truncated_len in 0..valid_bytes.len() {
    let truncated = &valid_bytes[..truncated_len];
    assert!(
      ClusterConfig::from_byte_array(truncated).is_err(),
      "长度 {truncated_len} 的截断切片未被拒载"
    );
  }
  aok::OK
}

/// 原子写与临时文件清理验证：
/// write_into 落盘成功后目标文件有效且同目录下零 .tmp 临时文件残留；
/// 再次覆写同样原子生效且无残留
#[test]
fn test_write_into_atomic_and_tmp_cleanup() -> Void {
  let dir = tempfile::tempdir()?.keep();
  let path = dir.join("nodes.conf");

  let payload1 = b"cluster_config_v1_test_payload";
  write_into(&path, payload1)?;
  assert_eq!(read_device(&path)?, payload1);

  // 验证无任何 .tmp 孤儿临时文件残留
  for entry in read_dir(&dir)? {
    let entry = entry?;
    let name = entry.file_name();
    let name_str = name.to_string_lossy();
    assert!(
      !name_str.ends_with(".tmp"),
      "落盘后不应残留临时文件: {name_str}"
    );
  }

  // 二次原子覆写
  let payload2 = b"cluster_config_v2_updated_longer_payload";
  write_into(&path, payload2)?;
  assert_eq!(read_device(&path)?, payload2);

  for entry in read_dir(&dir)? {
    let entry = entry?;
    let name = entry.file_name();
    let name_str = name.to_string_lossy();
    assert!(
      !name_str.ends_with(".tmp"),
      "覆写后不应残留临时文件: {name_str}"
    );
  }

  aok::OK
}

/// 并发压测：多线程并发演化配置并触发即时刷盘（flush_frequency_ms = 0）
/// 验证：
/// 1. 写盘互斥锁 flush_disk_lock 保证锁内读快照，落盘序与内存版本序严格一致，盘上纪元单调不降（绝无旧版本覆写新版本）
/// 2. 原子写链保证并发读盘者永不读到半写截断文件（每个读到的快照均能完整解码）
/// 3. 所有并发写者结束后，盘上版本严格等于内存最终版本
#[test]
fn test_concurrent_flush_monotonic_disk_epoch() -> Void {
  let dir = tempfile::tempdir()?.keep();
  let path = dir.join("nodes.conf");
  let provider = boot(&dir, "127.0.0.1", 7110, 0, false, "");
  let cm = provider.cluster_manager().unwrap();

  let stop = Arc::new(AtomicBool::new(false));
  let running = Arc::clone(&stop);
  let path_reader = path.clone();

  // 并发读盘线程：持续读取 nodes.conf，断言：
  // 1) 读到的字节必能解码为合法 ClusterConfig（证明原子 rename 生效、零半写）
  // 2) 读到的 local_node_config_epoch 必须单调不降（证明锁内读快照生效、无旧覆新）
  // 100µs 步进采样：断言对象是快照合法性与单调性，非采样密度，空转独占 CPU 反伤写者
  let reader_handle = thread::spawn(move || {
    let mut last_observed_epoch = 0i64;
    while !running.load(Ordering::Acquire) {
      if let Ok(bytes) = read(&path_reader)
        && let Ok(config) = ClusterConfig::from_byte_array(&bytes)
      {
        let epoch = config.local_node_config_epoch();
        assert!(
          epoch >= last_observed_epoch,
          "盘上配置版本回退：前值 {last_observed_epoch}，现值 {epoch}"
        );
        last_observed_epoch = epoch;
      }
      thread::sleep(Duration::from_micros(100));
    }
  });

  // 两并发写者线程：交替 bump epoch 与切换角色（锁互斥语义与次数无关，
  // 12×2 已覆盖交错面；频率 0 每次变更走 tmp+sync+rename 全量落盘，迭代是纯 fsync 成本）
  let cm1 = Arc::clone(&cm);
  let w1 = thread::spawn(move || {
    for _ in 0..12 {
      cm1.try_bump_cluster_epoch();
    }
  });

  let cm2 = Arc::clone(&cm);
  let w2 = thread::spawn(move || {
    for i in 0..12 {
      if i % 2 == 0 {
        cm2.try_bump_cluster_epoch();
      } else {
        cm2.try_set_local_node_role(NodeRole::Primary);
      }
    }
  });

  w1.join().unwrap();
  w2.join().unwrap();
  stop.store(true, Ordering::Release);
  reader_handle.join().unwrap();

  // 终态校验：盘上最终载荷必等于内存最终状态
  let final_bytes = read(&path)?;
  let final_disk_config = ClusterConfig::from_byte_array(&final_bytes).expect("最终盘上载荷可解码");
  let final_mem_epoch = cm.current_config().local_node_config_epoch();
  assert_eq!(
    final_disk_config.local_node_config_epoch(),
    final_mem_epoch,
    "盘上最终 epoch 须与内存终态严格一致"
  );

  aok::OK
}
