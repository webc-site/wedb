//! 集群槽位验证与 RESP 错误格式化测试，对标 Garnet ClusterSlotVerify
use aok::Void;
use wbase::hash_slot::hash_slot as cluster_slot;
use wedb::server::{
  cluster_config::{ClusterConfig, ClusterPreferredEndpointType},
  hash_slot::{HashSlot, SlotState},
  slot_verify::{ClusterSlotVerificationState, multi_key_slot_verify, single_key_slot_verify},
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};

/// 验证 RESP 错误格式化符合 Redis Cluster 规范
#[test]
fn test_slot_verify_resp_error_formatting() -> Void {
  let mut buf = Vec::new();

  // 1. Ok 不写入任何错误
  let state_ok = ClusterSlotVerificationState::Ok;
  state_ok.write_resp_error(&mut buf);
  assert!(buf.is_empty());

  // 2. Moved 重定向
  let state_moved = ClusterSlotVerificationState::Moved {
    slot: 3999,
    endpoint: "127.0.0.1".into(),
    port: 7001,
  };
  buf.clear();
  state_moved.write_resp_error(&mut buf);
  assert_eq!(&buf, b"-MOVED 3999 127.0.0.1:7001\r\n");

  // 3. Ask 重定向
  let state_ask = ClusterSlotVerificationState::Ask {
    slot: 12000,
    endpoint: "192.168.1.100".into(),
    port: 6379,
  };
  buf.clear();
  state_ask.write_resp_error(&mut buf);
  assert_eq!(&buf, b"-ASK 12000 192.168.1.100:6379\r\n");

  // 4. ClusterDown
  let state_down = ClusterSlotVerificationState::ClusterDown;
  buf.clear();
  state_down.write_resp_error(&mut buf);
  assert_eq!(&buf, b"-CLUSTERDOWN Hash slot not served\r\n");

  // 5. CrossSlot
  let state_cross = ClusterSlotVerificationState::CrossSlot;
  buf.clear();
  state_cross.write_resp_error(&mut buf);
  assert_eq!(
    &buf,
    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n"
  );

  // 6. TryAgain
  let state_tryagain = ClusterSlotVerificationState::TryAgain;
  buf.clear();
  state_tryagain.write_resp_error(&mut buf);
  assert_eq!(
    &buf,
    b"-TRYAGAIN Multiple keys request during rehashing of slot\r\n"
  );

  Ok(())
}

/// 验证单键本地命中与非本地 MOVED 重定向
#[test]
fn test_single_key_slot_verify_stable() -> Void {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: "node_local_primary",
    address: "127.0.0.1",
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });

  let remote_worker_id = config.workers.len() as u16;
  config.workers.push(Worker {
    nodeid: Some("node_remote_primary".to_string()),
    address: "127.0.0.1".to_string(),
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: Some("".to_string()),
  });

  // 0..8192 归属本地 (LOCAL_WORKER_ID = 1)
  for slot in 0..8192 {
    config.slot_map[slot] = HashSlot {
      worker_id: LOCAL_WORKER_ID as u16,
      state: SlotState::Stable,
    };
  }
  // 8192..16384 归属远端
  for slot in 8192..16384 {
    config.slot_map[slot] = HashSlot {
      worker_id: remote_worker_id,
      state: SlotState::Stable,
    };
  }

  let slot_local = 100u16;
  let slot_remote = 10000u16;

  // 本地槽位读写 -> Ok
  let res_local = single_key_slot_verify(
    &config,
    slot_local,
    false,
    false,
    false,
    true,
    ClusterPreferredEndpointType::Ip,
  );
  assert!(matches!(res_local, ClusterSlotVerificationState::Ok));

  // 远端槽位读写 -> Moved
  let res_remote = single_key_slot_verify(
    &config,
    slot_remote,
    false,
    false,
    false,
    true,
    ClusterPreferredEndpointType::Ip,
  );
  match res_remote {
    ClusterSlotVerificationState::Moved {
      slot,
      endpoint,
      port,
    } => {
      assert_eq!(slot, slot_remote);
      assert_eq!(endpoint, "127.0.0.1");
      assert_eq!(port, 7001);
    }
    _ => panic!("Expected Moved state for remote slot"),
  }

  Ok(())
}

/// 验证槽位迁移态：源节点本地有键返回 Ok，本地无键返回 ASK
#[test]
fn test_single_key_slot_verify_migrating() -> Void {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: "node_src",
    address: "127.0.0.1",
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });

  let target_worker_id = config.workers.len() as u16;
  config.workers.push(Worker {
    nodeid: Some("node_tgt".to_string()),
    address: "127.0.0.1".to_string(),
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: Some("".to_string()),
  });

  let slot = 500u16;
  // Migrating 状态下 worker_id 指向目标节点 target_worker_id
  config.slot_map[slot as usize] = HashSlot {
    worker_id: target_worker_id,
    state: SlotState::Migrating,
  };

  // 1. 键在源节点本地存在 (can_access_key = true) -> Ok
  let res_exist = single_key_slot_verify(
    &config,
    slot,
    false,
    false,
    false,
    true,
    ClusterPreferredEndpointType::Ip,
  );
  assert!(matches!(res_exist, ClusterSlotVerificationState::Ok));

  // 2. 键在源节点本地不存在 (can_access_key = false) -> Ask 指向目标节点
  let res_not_exist = single_key_slot_verify(
    &config,
    slot,
    false,
    false,
    false,
    false,
    ClusterPreferredEndpointType::Ip,
  );
  match res_not_exist {
    ClusterSlotVerificationState::Ask {
      slot: ask_slot,
      endpoint,
      port,
    } => {
      assert_eq!(ask_slot, slot);
      assert_eq!(endpoint, "127.0.0.1");
      assert_eq!(port, 7001);
    }
    _ => panic!("Expected Ask state when key does not exist on migrating source"),
  }

  Ok(())
}

/// 验证槽位导入态：目标节点带有 ASKING 会话标志允许访问，无 ASKING 则 MOVED 重定向
#[test]
fn test_single_key_slot_verify_importing() -> Void {
  let mut config = ClusterConfig::new();
  // 本地为目标节点
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: "node_tgt",
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });

  // 远端为源节点
  let src_worker_id = config.workers.len() as u16;
  config.workers.push(Worker {
    nodeid: Some("node_src".to_string()),
    address: "127.0.0.1".to_string(),
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: Some("".to_string()),
  });

  let slot = 500u16;
  // Importing 状态下 worker_id 指向源节点
  config.slot_map[slot as usize] = HashSlot {
    worker_id: src_worker_id,
    state: SlotState::Importing,
  };

  // 1. 无 ASKING 会话标记且本地无该键 -> MOVED 重定向至原所有者 node_src
  let res_no_asking = single_key_slot_verify(
    &config,
    slot,
    false,
    false,
    false,
    false,
    ClusterPreferredEndpointType::Ip,
  );
  match res_no_asking {
    ClusterSlotVerificationState::Moved {
      slot: moved_slot,
      endpoint,
      port,
    } => {
      assert_eq!(moved_slot, slot);
      assert_eq!(endpoint, "127.0.0.1");
      assert_eq!(port, 7000);
    }
    _ => panic!("Expected Moved to source node without ASKING"),
  }

  // 2. 带 ASKING 会话标记 -> Ok
  let res_asking = single_key_slot_verify(
    &config,
    slot,
    false,
    true,
    false,
    false,
    ClusterPreferredEndpointType::Ip,
  );
  assert!(matches!(res_asking, ClusterSlotVerificationState::Ok));

  Ok(())
}

/// 验证节点 Recovering 状态下的行为
#[test]
fn test_slot_verify_recovering() -> Void {
  let mut config = ClusterConfig::new();

  // 主节点在恢复中 -> ClusterDown
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: "node_primary",
    address: "127.0.0.1",
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });
  config.slot_map[10] = HashSlot {
    worker_id: LOCAL_WORKER_ID as u16,
    state: SlotState::Stable,
  };

  let res_primary_rec = single_key_slot_verify(
    &config,
    10,
    false,
    false,
    true, // is_recovering = true
    true,
    ClusterPreferredEndpointType::Ip,
  );
  assert!(matches!(
    res_primary_rec,
    ClusterSlotVerificationState::ClusterDown
  ));

  // 从节点在恢复中 -> MOVED 到主节点
  let mut replica_config = ClusterConfig::new();
  replica_config.initialize_local_worker(LocalWorkerSpec {
    node_id: "node_replica",
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Replica,
    replica_of_node_id: Some("node_primary"),
    hostname: Some(""),
  });
  let primary_worker_id = replica_config.workers.len() as u16;
  replica_config.workers.push(Worker {
    nodeid: Some("node_primary".to_string()),
    address: "127.0.0.1".to_string(),
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: Some("".to_string()),
  });
  replica_config.slot_map[10] = HashSlot {
    worker_id: primary_worker_id,
    state: SlotState::Stable,
  };
  let res_replica_rec = single_key_slot_verify(
    &replica_config,
    10,
    false,
    false,
    true, // is_recovering = true
    true,
    ClusterPreferredEndpointType::Ip,
  );
  match res_replica_rec {
    ClusterSlotVerificationState::Moved {
      slot,
      endpoint,
      port,
    } => {
      assert_eq!(slot, 10);
      assert_eq!(endpoint, "127.0.0.1");
      assert_eq!(port, 7000);
    }
    _ => panic!("Expected Moved to primary when replica is recovering"),
  }

  Ok(())
}

/// 验证多键跨槽位检查 (CROSSSLOT 与 TRYAGAIN)
#[test]
fn test_multi_key_slot_verify() -> Void {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: "node_local",
    address: "127.0.0.1",
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });

  // 1. 相同哈希标签的键 (Hash Tag) -> 同槽位
  let key_a = b"{user100}:profile";
  let key_b = b"{user100}:orders";
  assert_eq!(cluster_slot(key_a), cluster_slot(key_b));
  let slot = cluster_slot(key_a);
  config.slot_map[slot as usize] = HashSlot {
    worker_id: LOCAL_WORKER_ID as u16,
    state: SlotState::Stable,
  };

  let res_same = multi_key_slot_verify(
    &config,
    &[slot, slot],
    false,
    false,
    false,
    ClusterPreferredEndpointType::Ip,
    |_key_idx| true,
  );
  assert!(matches!(res_same, ClusterSlotVerificationState::Ok));

  // 2. 不同槽位的多键 -> CROSSSLOT
  let slot_x = 100u16;
  let slot_y = 200u16;
  config.slot_map[slot_x as usize] = HashSlot {
    worker_id: LOCAL_WORKER_ID as u16,
    state: SlotState::Stable,
  };
  config.slot_map[slot_y as usize] = HashSlot {
    worker_id: LOCAL_WORKER_ID as u16,
    state: SlotState::Stable,
  };
  let res_cross = multi_key_slot_verify(
    &config,
    &[slot_x, slot_y],
    false,
    false,
    false,
    ClusterPreferredEndpointType::Ip,
    |_key_idx| true,
  );
  assert!(matches!(res_cross, ClusterSlotVerificationState::CrossSlot));

  // 3. 槽位处于迁移态下的多键请求：部分键在本地 (Ok)，部分键已迁移 (Ask) -> TRYAGAIN
  let target_worker_id = config.workers.len() as u16;
  config.workers.push(Worker {
    nodeid: Some("node_remote_mig".to_string()),
    address: "127.0.0.1".to_string(),
    port: 7002,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: Some("".to_string()),
  });
  config.slot_map[slot as usize] = HashSlot {
    worker_id: target_worker_id,
    state: SlotState::Migrating,
  };
  let res_migrating = multi_key_slot_verify(
    &config,
    &[slot, slot],
    false,
    false,
    false,
    ClusterPreferredEndpointType::Ip,
    |idx| idx == 0,
  );
  assert!(matches!(
    res_migrating,
    ClusterSlotVerificationState::TryAgain
  ));

  Ok(())
}
