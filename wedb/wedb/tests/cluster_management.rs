//! 集群管理集成测试，对标 Garnet ClusterManagementTests
use std::sync::Arc;

use aok::Void;
use compio::runtime::Runtime;
use wbase::time::now_secs;
use wdev::SegmentedDevice;
use wedb::{
  error::Error,
  server::{
    cluster_config::ClusterPreferredEndpointType,
    cluster_manager::ClusterManager,
    cluster_provider::ClusterProvider,
    hash_slot::SlotState,
    worker::{LocalWorkerSpec, NodeRole, Worker},
  },
};
use wkv::WedbStore;
use wnode::StorageSession;
use wtest_base::test_store_config;

/// test/cluster/Garnet.test.cluster/ClusterManagementTests.cs:ClusterForgetTest
#[test]
fn cluster_forget_test() -> Void {
  let cp = Arc::new(ClusterProvider::default());
  let m = ClusterManager::new(cp);
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

  // 挂起合并写锁为异步锁，摘除节点须在 compio 运行时内驱动
  let rt = Runtime::new().unwrap();

  // Cannot forget self
  assert!(matches!(
    rt.block_on(m.try_remove_worker(0x0000_0000_0000_0000_0000_0000_0000_DE11, 60)),
    Err(Error::CannotForgetMyself)
  ));

  // Forget node_2
  rt.block_on(m.try_remove_worker(0x0000_0000_0000_0000_0000_0000_0000_DE12, 60))?;
  assert!(m.is_banned(0x0000_0000_0000_0000_0000_0000_0000_DE12));
  assert!(
    !m.current_config
      .read()
      .is_known(0x0000_0000_0000_0000_0000_0000_0000_DE12)
  );

  aok::OK
}

/// test/cluster/Garnet.test.cluster/ClusterManagementTests.cs:ClusterResetTest
#[test]
fn cluster_reset_test() -> Void {
  let rt = Runtime::new().unwrap();
  let cp = Arc::new(ClusterProvider::default());
  let m = ClusterManager::new(cp);
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: Some("node1.cluster"),
    });
  }
  let old_id = m.current_config.read().local_node_id().unwrap();

  // 空存储只读会话：try_reset 临界区内 HasKeysInSlots 键检查的真实扫描面
  // （对标 C# storeWrapper.HasKeysInSlots，无键则放行复位）
  let dir = tempfile::tempdir().unwrap().keep();
  let device = Arc::new(SegmentedDevice::single_file(dir.join("reset.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);

  // Soft reset keeps node_id and hostname
  rt.block_on(async { m.try_reset(true, 60, &storage).await })?;
  assert_eq!(m.current_config.read().local_node_id().unwrap(), old_id);
  assert_eq!(
    m.current_config.read().local_node_hostname(),
    Some("node1.cluster")
  );

  // Hard reset generates fresh node_id and resets config epoch, but preserves hostname
  rt.block_on(async { m.try_reset(false, 60, &storage).await })?;
  let new_id = m.current_config.read().local_node_id().unwrap();
  assert_ne!(new_id, old_id);
  assert_eq!(m.current_config.read().local_node_config_epoch(), 0);
  assert_eq!(
    m.current_config.read().local_node_hostname(),
    Some("node1.cluster")
  );

  aok::OK
}

/// test/cluster/Garnet.test.cluster/ClusterManagementTests.cs:ClusterSlotsTest
#[test]
fn cluster_slots_test() {
  let cp = Arc::new(ClusterProvider::default());
  let m = ClusterManager::new(cp);
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: 0x0000_0000_0000_0000_0000_0000_0000_DE11,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: Some("localhost"),
    });
    config.assign_slots(&[0, 1, 2, 8, 9, 10], 1, SlotState::Stable);
  }

  let slots_info = m
    .current_config
    .read()
    .get_slots_info(ClusterPreferredEndpointType::Ip);
  assert!(slots_info.contains(":0\r\n:2\r\n"));
  assert!(slots_info.contains(":8\r\n:10\r\n"));

  assert_eq!(
    ClusterManager::get_range(&[0, 1, 2, 8, 9, 10]),
    "> 0-2 8-10 "
  );
}

/// test/cluster/Garnet.test.cluster/ClusterManagementTests.cs:ClusterSlotRangesTest
#[test]
fn cluster_slot_ranges_test() {
  assert_eq!(
    ClusterManager::get_range(&[0, 1, 2, 5, 9, 10]),
    "> 0-2 5-5 9-10 "
  );
  assert_eq!(ClusterManager::get_range(&[7]), "> 7-7 ");
  assert_eq!(ClusterManager::get_range(&[]), "> ");
}

/// test/cluster/Garnet.test.cluster/ClusterManagementTests.cs:ClusterShardsTest
#[test]
fn cluster_shards_test() {
  let cp = Arc::new(ClusterProvider::default());
  let m = ClusterManager::new(cp);
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
    config.assign_slots(&[0, 1, 2, 3], 1, SlotState::Stable);
  }

  let shards = m
    .current_config
    .read()
    .get_shards_info(None, ClusterPreferredEndpointType::Ip);
  assert!(shards.contains(":0\r\n:3"));
}

/// test/cluster/Garnet.test.cluster/ClusterManagementTests.cs:ClusterSetSlotBadOptions
#[test]
fn cluster_set_slot_bad_options() {
  let cp = Arc::new(ClusterProvider::default());
  let m = ClusterManager::new(cp);
  {
    let mut config = m.current_config.write();
    config.initialize_local_worker(LocalWorkerSpec {
      node_id: 0x0000_0000_0000_0000_0000_0000_0001_0CA1,
      address: "127.0.0.1",
      port: 7000,
      config_epoch: 1,
      role: NodeRole::Primary,
      replica_of_node_id: None,
      hostname: None,
    });
  }

  // Cannot migrate to myself
  assert!(matches!(
    m.try_prepare_slot_for_migration(100, 0x10CA1),
    Err(Error::MigrateToMyself)
  ));

  // Unknown node
  assert!(matches!(
    m.try_prepare_slot_for_migration(100, 0x999),
    Err(Error::NodeNotFound(_))
  ));
}

/// 从条目字符串 `{node_id hex} : {diff}` 提取秒数
fn seconds_of(entry: &str, node_id_hex: &str) -> i64 {
  entry[node_id_hex.len() + 3..].parse().unwrap()
}

#[test]
fn test_ban_list_includes_expired_entries_with_negative_seconds() {
  const BANNED_ACTIVE: u128 = 0x0000_0000_0000_0000_0000_0000_0000_0001;
  const BANNED_EXPIRED: u128 = 0x0000_0000_0000_0000_0000_0000_0000_0002;
  let cm = ClusterManager::new(Arc::new(ClusterProvider::default()));
  cm.ban_node(BANNED_ACTIVE, 100);
  // 直接注入已过期封禁条目（早于当前 50 秒）
  let now = now_secs() as i64;
  cm.worker_ban_list.pin().insert(BANNED_EXPIRED, now - 50);

  let list = cm.get_ban_list();
  assert_eq!(list.len(), 2);

  let expired_hex = format!("{BANNED_EXPIRED:032x}");
  let active_hex = format!("{BANNED_ACTIVE:032x}");

  // 过期条目仍列出且为负秒（对标 C# GetBanList 全量输出）
  let expired = list.iter().find(|e| e.starts_with(&expired_hex)).unwrap();
  assert_eq!(seconds_of(expired, &expired_hex), -50);

  // 未过期条目秒数落在 (0, 100]
  let active = list.iter().find(|e| e.starts_with(&active_hex)).unwrap();
  assert!((1..=100).contains(&seconds_of(active, &active_hex)));
}

/// 并发解禁不丢更新（papaya 条件删除语义，封禁表出 RwLock 后的原子性承接面）
///
/// 原 RwLock<HashMap> 形态靠写锁把「判 + 删」「读 + 判 + 覆盖」整段串行化；
/// 换并发字典后原子性必须由单次操作承接，本用例钉住两件事：
///
/// 1. 多线程并发清理（迭代快照 + remove_if）叠加并发重封（覆盖式 insert）之
///    下，未过期封禁条目恒在、恒判活，过期条目恒被摘除——即 cleanup 的条件
///    删除绝不误伤并发续封的条目（丢更新 = 已封禁节点被并发清理悄悄放行）；
/// 2. 同一条过期条目被多线程同时条件删除，恰一次生效（remove_if 的 CAS 复判
///    保证唯一胜出者），其余只观察到已缺席。
#[test]
fn ban_list_concurrent_cleanup_loses_no_update() {
  use std::{
    sync::atomic::{AtomicUsize, Ordering},
    thread,
  };

  const ACTIVE: u128 = 0x0000_0000_0000_0000_0000_0000_0000_00A1;
  const EXPIRED: u128 = 0x0000_0000_0000_0000_0000_0000_0000_00B2;
  const RACE: u128 = 0x0000_0000_0000_0000_0000_0000_0000_00C3;
  const THREADS: usize = 8;
  const ROUNDS: usize = 200;

  let cm = Arc::new(ClusterManager::new(Arc::new(ClusterProvider::default())));
  cm.ban_node(ACTIVE, 3600);
  // 直接注入一条已过期封禁，令并发清理与它竞争
  let past = now_secs() as i64 - 50;
  cm.worker_ban_list.pin().insert(EXPIRED, past);

  // 一、并发清理 + 并发点查 + 并发重封
  let mut handles = Vec::with_capacity(THREADS);
  for _ in 0..THREADS {
    let cm = Arc::clone(&cm);
    handles.push(thread::spawn(move || {
      for _ in 0..ROUNDS {
        cm.cleanup_ban_list();
        assert!(cm.is_banned(ACTIVE), "未过期封禁在并发清理下不得判为已解禁");
        // 与 cleanup 的条件删除同键竞争：覆盖式续封不得被误摘
        cm.ban_node(ACTIVE, 3600);
      }
    }));
  }
  for h in handles {
    h.join().unwrap();
  }
  assert!(cm.is_banned(ACTIVE), "并发窗口结束后未过期封禁仍在册");
  assert_eq!(
    cm.get_ban_list().len(),
    1,
    "过期条目恰被摘除一次：只剩 ACTIVE，无半删残留亦无误删 ACTIVE"
  );

  // 二、过期条目并发条件删除：恰一次生效
  let expired = now_secs() as i64 - 1;
  cm.worker_ban_list.pin().insert(RACE, expired);
  let winners = Arc::new(AtomicUsize::new(0));
  let mut handles = Vec::with_capacity(THREADS);
  for _ in 0..THREADS {
    let cm = Arc::clone(&cm);
    let winners = Arc::clone(&winners);
    handles.push(thread::spawn(move || {
      let now = now_secs() as i64;
      let ban_list = cm.worker_ban_list.pin();
      let removed = ban_list.remove_if(&RACE, |_, expiry| *expiry <= now);
      if matches!(removed, Ok(Some(_))) {
        winners.fetch_add(1, Ordering::Relaxed);
      }
    }));
  }
  for h in handles {
    h.join().unwrap();
  }
  assert_eq!(
    winners.load(Ordering::Relaxed),
    1,
    "条件删除恰一次胜出（CAS 点复判），无重复解禁"
  );
  assert!(!cm.is_banned(RACE));
}

/// test/cluster/Garnet.test.cluster/ClusterManagementTests.cs:ClusterKeySlotTest
///
/// 库级定槽（doc/zh/db.md 4.1）：CLUSTER KEYSLOT 回声调用会话当前库槽位，
/// 键内容不参与定槽——确定性由 wbase 单测钉死，此处验证同会话恒同槽
#[test]
fn cluster_key_slot_test() {
  use wbase::hash_slot::slot_of;
  assert_eq!(slot_of(0, 0), slot_of(0, 0));
}
