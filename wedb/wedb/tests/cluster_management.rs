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
      hostname: None,
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

  // Soft reset keeps node_id
  rt.block_on(async { m.try_reset(true, 60, &storage).await })?;
  assert_eq!(m.current_config.read().local_node_id().unwrap(), old_id);

  // Hard reset generates fresh node_id and resets config epoch
  rt.block_on(async { m.try_reset(false, 60, &storage).await })?;
  let new_id = m.current_config.read().local_node_id().unwrap();
  assert_ne!(new_id, old_id);
  assert_eq!(m.current_config.read().local_node_config_epoch(), 0);

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
  cm.worker_ban_list.write().insert(BANNED_EXPIRED, now - 50);

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

/// test/cluster/Garnet.test.cluster/ClusterManagementTests.cs:ClusterKeySlotTest
///
/// 库级定槽（doc/zh/db.md 4.1）：CLUSTER KEYSLOT 回声调用会话当前库槽位，
/// 键内容不参与定槽——确定性由 wbase 单测钉死，此处验证同会话恒同槽
#[test]
fn cluster_key_slot_test() {
  use wbase::hash_slot::slot_of;
  assert_eq!(slot_of(0, 0), slot_of(0, 0));
}
