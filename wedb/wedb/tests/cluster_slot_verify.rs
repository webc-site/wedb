//! 集群槽位验证与 RESP 错误格式化测试，对标 Garnet ClusterSlotVerify
use aok::Void;
use wbase::hash_slot::CLUSTER_SLOT_COUNT;
use wedb::server::{
  cluster_config::{ClusterConfig, ClusterPreferredEndpointType},
  hash_slot::{HashSlot, SlotState},
  slot_verify::{
    ClusterSlotVerificationState, SlotVerifiedState, SlotVerifySessionState,
    single_key_slot_verify, write_slot_verification_message,
  },
  worker::{LOCAL_WORKER_ID, LocalWorkerSpec, NodeRole, Worker},
};

/// 验证 RESP 错误格式化符合 Redis Cluster 规范（经渲染单点
/// write_slot_verification_message：MOVED/ASK 按 state 查 config 取端点）
#[test]
fn test_slot_verify_resp_error_formatting() -> Void {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x10CA1,
    address: "127.0.0.1",
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });
  let remote_a = config.workers.len() as u16;
  config.workers.push(Worker {
    nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_2E70),
    address: "127.0.0.1".into(),
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: Some("".into()),
  });
  let remote_b = config.workers.len() as u16;
  config.workers.push(Worker {
    nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_2E72),
    address: "192.168.1.100".into(),
    port: 6379,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: Some("".into()),
  });
  config.slot_map[3999] = HashSlot {
    worker_id: remote_a,
    state: SlotState::Stable,
  };
  config.slot_map[12000] = HashSlot {
    worker_id: remote_b,
    state: SlotState::Stable,
  };
  let pref = ClusterPreferredEndpointType::Ip;
  let mut buf = Vec::new();

  // 1. Ok 不写入任何错误
  write_slot_verification_message(
    &config,
    ClusterSlotVerificationState::new(SlotVerifiedState::Ok, 1),
    pref,
    &mut buf,
  );
  assert!(buf.is_empty());

  // 2. Moved 重定向（端点按 state 查 config）
  write_slot_verification_message(
    &config,
    ClusterSlotVerificationState::new(SlotVerifiedState::Moved, 3999),
    pref,
    &mut buf,
  );
  assert_eq!(&buf, b"-MOVED 3999 127.0.0.1:7001\r\n");

  // 3. Ask 重定向
  buf.clear();
  write_slot_verification_message(
    &config,
    ClusterSlotVerificationState::new(SlotVerifiedState::Ask, 12000),
    pref,
    &mut buf,
  );
  assert_eq!(&buf, b"-ASK 12000 192.168.1.100:6379\r\n");

  // 4. ClusterDown
  buf.clear();
  write_slot_verification_message(
    &config,
    ClusterSlotVerificationState::new(SlotVerifiedState::ClusterDown, 10),
    pref,
    &mut buf,
  );
  assert_eq!(&buf, b"-CLUSTERDOWN Hash slot not served\r\n");

  // 6. TryAgain
  buf.clear();
  write_slot_verification_message(
    &config,
    ClusterSlotVerificationState::new(SlotVerifiedState::TryAgain, 10),
    pref,
    &mut buf,
  );
  assert_eq!(
    &buf,
    b"-TRYAGAIN Multiple keys request during rehashing of slot\r\n"
  );

  Ok(())
}

/// MOVED/ASK 重定向帧非 ASCII 端点折叠字节锁（zcode-r139c-cluenc）：C# 组串处
/// `Encoding.ASCII.GetBytes` 逐字符折 '?'（RespClusterSlotVerify.cs:27/:54/:61），
/// rust 唯一成帧点 write_redirect_error 端点实参经 wbase::ascii_sanitize 单机制
/// 逐字节折 '?'——Hostname 偏好下 "café"（UTF-8 五字节）端点出帧 "caf??"，
/// 帧形 -{kind} {slot} {endpoint}:{port}\r\n 不变；C# UTF-16 逐字符折形 "caf?"
/// 非逐字节等形（值域收拢，登记见 deviations.md）
#[test]
fn test_redirect_frames_fold_non_ascii_hostname_endpoint() {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x10CA1,
    address: "127.0.0.1",
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: None,
  });
  let remote = config.workers.len() as u16;
  config.workers.push(Worker {
    nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_2E70),
    address: "127.0.0.1".into(),
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: Some("café".into()),
  });
  config.slot_map[3999] = HashSlot {
    worker_id: remote,
    state: SlotState::Stable,
  };
  config.slot_map[12000] = HashSlot {
    worker_id: remote,
    state: SlotState::Importing,
  };
  let pref = ClusterPreferredEndpointType::Hostname;

  let mut buf = Vec::new();
  write_slot_verification_message(
    &config,
    ClusterSlotVerificationState::new(SlotVerifiedState::Moved, 3999),
    pref,
    &mut buf,
  );
  assert_eq!(&buf, b"-MOVED 3999 caf??:7001\r\n");

  buf.clear();
  write_slot_verification_message(
    &config,
    ClusterSlotVerificationState::new(SlotVerifiedState::Ask, 12000),
    pref,
    &mut buf,
  );
  assert_eq!(&buf, b"-ASK 12000 caf??:7001\r\n");
  assert!(buf.is_ascii(), "重定向帧对外值域应恒 ASCII");
}

/// 验证单键本地命中与非本地 MOVED 重定向
#[test]
fn test_single_key_slot_verify_stable() -> Void {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x10CA1,
    address: "127.0.0.1",
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });

  let remote_worker_id = config.workers.len() as u16;
  config.workers.push(Worker {
    nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_2E73),
    address: "127.0.0.1".into(),
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: Some("".into()),
  });

  // 0..8192 归属本地 (LOCAL_WORKER_ID = 1)
  for slot in 0..8192 {
    config.slot_map[slot] = HashSlot {
      worker_id: LOCAL_WORKER_ID as u16,
      state: SlotState::Stable,
    };
  }
  // 8192..CLUSTER_SLOT_COUNT 归属远端
  for slot in 8192..CLUSTER_SLOT_COUNT {
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
    SlotVerifySessionState::default(),
    false,
    true,
  );
  assert!(res_local.is_ok());

  // 远端槽位读写 -> Moved
  let res_remote = single_key_slot_verify(
    &config,
    slot_remote,
    false,
    SlotVerifySessionState::default(),
    false,
    true,
  );
  assert_eq!(res_remote.state, SlotVerifiedState::Moved);
  assert_eq!(res_remote.slot, slot_remote);
  let (endpoint, port) =
    config.get_endpoint_from_slot(slot_remote, ClusterPreferredEndpointType::Ip);
  assert_eq!(endpoint, "127.0.0.1");
  assert_eq!(port, 7001);

  Ok(())
}

/// 验证槽位迁移态：源节点本地有键返回 Ok，本地无键返回 ASK
#[test]
fn test_single_key_slot_verify_migrating() -> Void {
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x0DE1_0000_0000_0000_0000_0000_0000_0001,
    address: "127.0.0.1",
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: Some(""),
  });

  let target_worker_id = config.workers.len() as u16;
  config.workers.push(Worker {
    nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_2E72),
    address: "127.0.0.1".into(),
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: Some("".into()),
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
    SlotVerifySessionState::default(),
    false,
    true,
  );
  assert!(res_exist.is_ok());

  // 2. 键在源节点本地不存在 (can_access_key = false) -> Ask 指向目标节点
  let res_not_exist = single_key_slot_verify(
    &config,
    slot,
    false,
    SlotVerifySessionState::default(),
    false,
    false,
  );
  assert_eq!(res_not_exist.state, SlotVerifiedState::Ask);
  assert_eq!(res_not_exist.slot, slot);
  let (endpoint, port) = config.ask_endpoint_from_slot(slot, ClusterPreferredEndpointType::Ip);
  assert_eq!(endpoint, "127.0.0.1");
  assert_eq!(port, 7001);

  Ok(())
}

/// 验证槽位导入态：目标节点带有 ASKING 会话标志允许访问，无 ASKING 则 MOVED 重定向
#[test]
fn test_single_key_slot_verify_importing() -> Void {
  let mut config = ClusterConfig::new();
  // 本地为目标节点
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x0000_0000_0000_0000_0000_0000_0000_2E72,
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
    nodeid: Some(0x0DE1_0000_0000_0000_0000_0000_0000_0001),
    address: "127.0.0.1".into(),
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: Some("".into()),
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
    SlotVerifySessionState::default(),
    false,
    false,
  );
  assert_eq!(res_no_asking.state, SlotVerifiedState::Moved);
  assert_eq!(res_no_asking.slot, slot);
  let (endpoint, port) = config.get_endpoint_from_slot(slot, ClusterPreferredEndpointType::Ip);
  assert_eq!(endpoint, "127.0.0.1");
  assert_eq!(port, 7000);

  // 2. 带 ASKING 会话标记 -> Ok
  let res_asking = single_key_slot_verify(
    &config,
    slot,
    false,
    SlotVerifySessionState {
      session_asking: true,
      ..SlotVerifySessionState::default()
    },
    false,
    false,
  );
  assert!(res_asking.is_ok());

  Ok(())
}

/// 验证副本节点 READONLY 会话读（C# IsLocal(enableReplicaReads) 语义）
#[test]
fn test_single_key_slot_verify_replica_reads() -> Void {
  let mut config = ClusterConfig::new();
  // 本地为副本，其主节点持有全部槽位
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x0000_0000_0000_0000_0000_0000_0000_2E72,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Replica,
    replica_of_node_id: Some(0x0000_0000_0000_0000_0000_0000_0000_2E73),
    hostname: Some(""),
  });
  let primary_worker_id = config.workers.len() as u16;
  config.workers.push(Worker {
    nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_2E73),
    address: "127.0.0.1".into(),
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: Some("".into()),
  });
  let slot = 100u16;
  config.slot_map[slot as usize] = HashSlot {
    worker_id: primary_worker_id,
    state: SlotState::Stable,
  };

  // 1. 读命令 + READONLY 会话 -> 副本可本地服务
  let res_readonly = single_key_slot_verify(
    &config,
    slot,
    true,
    SlotVerifySessionState {
      read_only_session: true,
      ..SlotVerifySessionState::default()
    },
    false,
    true,
  );
  assert!(res_readonly.is_ok());

  // 2. 读命令 + 默认会话（未 READONLY）-> 重定向至主节点
  let res_default = single_key_slot_verify(
    &config,
    slot,
    true,
    SlotVerifySessionState::default(),
    false,
    true,
  );
  assert_eq!(res_default.state, SlotVerifiedState::Moved);
  let (_, port) = config.get_endpoint_from_slot(slot, ClusterPreferredEndpointType::Ip);
  assert_eq!(port, 7000);

  // 3. 写命令 -> 副本一律重定向（READONLY 会话态不影响写路径）
  let res_write = single_key_slot_verify(
    &config,
    slot,
    false,
    SlotVerifySessionState {
      read_only_session: true,
      ..SlotVerifySessionState::default()
    },
    false,
    true,
  );
  assert_eq!(res_write.state, SlotVerifiedState::Moved);
  let (_, port) = config.get_endpoint_from_slot(slot, ClusterPreferredEndpointType::Ip);
  assert_eq!(port, 7000);

  Ok(())
}

/// 验证节点 Recovering 状态下的行为
#[test]
fn test_slot_verify_recovering() -> Void {
  let mut config = ClusterConfig::new();

  // 主节点在恢复中 -> ClusterDown
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x0000_0000_0000_0000_0000_0000_0000_2E73,
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
    SlotVerifySessionState::default(),
    true, // is_recovering = true
    true,
  );
  assert_eq!(res_primary_rec.state, SlotVerifiedState::ClusterDown);

  // 从节点在恢复中 -> MOVED 到主节点
  let mut replica_config = ClusterConfig::new();
  replica_config.initialize_local_worker(LocalWorkerSpec {
    node_id: 0x0000_0000_0000_0000_0000_0000_0000_2E72,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Replica,
    replica_of_node_id: Some(0x0000_0000_0000_0000_0000_0000_0000_2E73),
    hostname: Some(""),
  });
  let primary_worker_id = replica_config.workers.len() as u16;
  replica_config.workers.push(Worker {
    nodeid: Some(0x0000_0000_0000_0000_0000_0000_0000_2E73),
    address: "127.0.0.1".into(),
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: Some("".into()),
  });
  replica_config.slot_map[10] = HashSlot {
    worker_id: primary_worker_id,
    state: SlotState::Stable,
  };
  let res_replica_rec = single_key_slot_verify(
    &replica_config,
    10,
    false,
    SlotVerifySessionState::default(),
    true, // is_recovering = true
    true,
  );
  assert_eq!(res_replica_rec.state, SlotVerifiedState::Moved);
  assert_eq!(res_replica_rec.slot, 10);
  let (endpoint, port) =
    replica_config.get_endpoint_from_slot(10, ClusterPreferredEndpointType::Ip);
  assert_eq!(endpoint, "127.0.0.1");
  assert_eq!(port, 7000);

  Ok(())
}
