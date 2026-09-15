//! 集群副本接收面集成测试（CLUSTER APPENDLOG 处理链路）
//!
//! 对标 Garnet NetworkClusterAppendLog + ReplicaReplaySession.ProcessPrimaryStream

use std::{io::ErrorKind, sync::Arc};

use waof::{RecordHeader, WalConfig, WalLog};
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  replication::cluster_replication_session::{AppendLogOutcome, ClusterReplicationSession},
  worker::{LocalWorkerSpec, NodeRole, Worker},
};
use wnode::MessageConsumerFace;

/// 辅助构造副本角色的 ClusterProvider 与配套 WalLog
fn setup_replica_environment(
  dir: &tempfile::TempDir,
  local_id: &str,
  primary_id: &str,
) -> (Arc<ClusterProvider>, Arc<WalLog<SegmentedDevice>>) {
  let provider = Arc::new(ClusterProvider::default());
  provider.initialize_replication_manager(1, None, false);

  let cm = Arc::new(ClusterManager::new(provider.clone()));
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: local_id,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Replica,
    replica_of_node_id: Some(primary_id),
    hostname: None,
  });
  config.workers.push(Worker {
    nodeid: Some(primary_id.to_string()),
    address: "127.0.0.1".to_string(),
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  });
  *cm.current_config.write() = config;
  *provider.cluster_manager.write() = Some(cm);

  let wal_path = dir.path().join("replica_aof.wal");
  let device = Arc::new(SegmentedDevice::single_file(wal_path).unwrap());
  let wal = Arc::new(WalLog::new(device, WalConfig::default()).unwrap());

  (provider, wal)
}

#[test]
fn cluster_replication_session_process_append_log_flow() {
  let dir = tempfile::tempdir().unwrap();
  let (provider, wal) = setup_replica_environment(&dir, "replica_1", "primary_1");
  let session = ClusterReplicationSession::new(provider.clone(), wal.clone(), None);

  // 1. 角色与主节点校验：错误 primary_id 拒绝
  let err = session
    .process_append_log("wrong_primary", 0, -1, -1, -1, &[])
    .unwrap_err();
  assert_eq!(err.kind(), ErrorKind::PermissionDenied);

  // 2. 初始化帧（-1/-1/-1）：注册重放驱动成功
  let outcome = session
    .process_append_log("primary_1", 0, -1, -1, -1, &[])
    .expect("init success");
  assert_eq!(outcome, AppendLogOutcome::Initialized);

  // 重复初始化同一 sublog 失败（对标 C# 抛异常）
  let dup_err = session
    .process_append_log("primary_1", 0, -1, -1, -1, &[])
    .unwrap_err();
  assert_eq!(dup_err.kind(), ErrorKind::AlreadyExists);

  // 3. 正常记录帧：保真落盘进 WalLog 且更新 replication offset（须携带合法 8B 记录头）
  let payload = b"test_payload_record_bytes_001";
  let header = RecordHeader::for_payload(payload);
  let mut frame = Vec::new();
  frame.extend_from_slice(&header.to_bytes());
  frame.extend_from_slice(payload);
  let frame_len = frame.len() as i64;
  let next_addr = 100;
  let outcome = session
    .process_append_log("primary_1", 0, 0, 0, next_addr, &frame)
    .expect("record append success");
  assert_eq!(outcome, AppendLogOutcome::Record);
  assert_eq!(wal.tail_address() as i64, frame_len);

  let rm = provider.replication_manager().unwrap();
  assert_eq!(rm.get_replication_offset(0), next_addr);

  // 4. Divergent 校验：传入 current_address 不等于当前 tail_address 报错
  let div_err = session
    .process_append_log("primary_1", 0, 0, 9999, 10000, &frame)
    .unwrap_err();
  assert_eq!(div_err.kind(), ErrorKind::InvalidData);
}

/// 构造带 8B 记录头的完整记录帧
fn record_frame(payload: &[u8]) -> Vec<u8> {
  let mut frame = Vec::new();
  frame.extend_from_slice(&RecordHeader::for_payload(payload).to_bytes());
  frame.extend_from_slice(payload);
  frame
}

/// FastAofTruncate 跳跃重对齐（对标 C# ReplicaReplaySession.cs:54-74
/// SafeInitialize 分支）：主端 checkpoint 后截断 AOF 推流产生跳跃帧
///（currentAddress > previousAddress），副本本地地址空间重对齐后断点续流；
/// 开关关闭时同帧保持 Divergent 断流（对标 C# 无重对齐分支即 divergent 异常）
#[test]
fn cluster_replication_session_fast_aof_truncate_realignment() {
  let dir = tempfile::tempdir().unwrap();
  let (provider, wal) = setup_replica_environment(&dir, "replica_1", "primary_1");
  // 开启 FastAofTruncate（对标 C# serverOptions.FastAofTruncate = true）
  provider.set_fast_aof_truncate(true);
  let session = ClusterReplicationSession::new(provider.clone(), wal.clone(), None);

  // 初始化帧：注册重放驱动
  let outcome = session
    .process_append_log("primary_1", 0, -1, -1, -1, &[])
    .expect("init success");
  assert_eq!(outcome, AppendLogOutcome::Initialized);

  // 稳态帧：记录区间 [0, frame_len)，与主端地址严格衔接
  let steady = record_frame(b"steady_state_record_payload");
  session
    .process_append_log("primary_1", 0, 0, 0, steady.len() as i64, &steady)
    .expect("steady record success");
  assert_eq!(wal.tail_address() as i64, steady.len() as i64);

  // 跳跃帧：主端截断后从跳跃点 4096 重推（previousAddress = 上一帧 next）
  let skip_frame = record_frame(b"post_truncate_record");
  let skip_current = 4096i64;
  let skip_next = skip_current + skip_frame.len() as i64;
  let outcome = session
    .process_append_log(
      "primary_1",
      0,
      steady.len() as i64,
      skip_current,
      skip_next,
      &skip_frame,
    )
    .expect("skip frame realigned and appended");
  assert_eq!(outcome, AppendLogOutcome::Record);
  // 断点续流：本地地址空间前跳对齐跳跃点，记录落盘衔接跳跃点
  assert_eq!(wal.begin_address() as i64, skip_current);
  assert_eq!(wal.tail_address() as i64, skip_next);
  // 位点随重对齐推进至跳跃点、随落盘推进至 next
  let rm = provider.replication_manager().unwrap();
  assert_eq!(rm.get_replication_offset(0), skip_next);

  // 关闭开关：同一跳跃场景不再重对齐，Divergent 断流
  provider.set_fast_aof_truncate(false);
  let tail = wal.tail_address() as i64;
  let err = session
    .process_append_log("primary_1", 0, tail, 8192, 8192 + 10, &skip_frame)
    .unwrap_err();
  assert_eq!(err.kind(), ErrorKind::InvalidData);
}

#[test]
fn cluster_replication_session_message_consumer_try_consume() {
  let dir = tempfile::tempdir().unwrap();
  let (provider, wal) = setup_replica_environment(&dir, "replica_1", "primary_1");
  let mut session = ClusterReplicationSession::new(provider, wal, None);

  // 构造 7 元素初始化帧 RESP: *7\r\n$7\r\nCLUSTER\r\n$9\r\nAPPENDLOG\r\n$9\r\nprimary_1\r\n$1\r\n0\r\n$2\r\n-1\r\n$2\r\n-1\r\n$2\r\n-1\r\n
  let init_frame = b"*7\r\n$7\r\nCLUSTER\r\n$9\r\nAPPENDLOG\r\n$9\r\nprimary_1\r\n$1\r\n0\r\n$2\r\n-1\r\n$2\r\n-1\r\n$2\r\n-1\r\n";
  let mut resp = Vec::new();
  session.recv_buffer.extend_from_slice(init_frame);
  let remaining = session.try_consume_messages_into(&mut resp);
  assert_eq!(remaining, Some(0));
  assert_eq!(resp, b"+OK\r\n");

  // 构造 8 元素记录帧 RESP（合法帧含 8B 头）
  let payload = b"test_stream_data";
  let header = RecordHeader::for_payload(payload);
  let mut record_frame_bytes = Vec::new();
  record_frame_bytes.extend_from_slice(&header.to_bytes());
  record_frame_bytes.extend_from_slice(payload);
  let next_addr = record_frame_bytes.len();

  let mut rec_frame = Vec::new();
  rec_frame.extend_from_slice(
    format!(
      "*8\r\n$7\r\nCLUSTER\r\n$9\r\nAPPENDLOG\r\n$9\r\nprimary_1\r\n$1\r\n0\r\n$1\r\n0\r\n$1\r\n0\r\n${}\r\n{}\r\n${}\r\n",
      next_addr.to_string().len(),
      next_addr,
      record_frame_bytes.len(),
    )
    .as_bytes(),
  );
  rec_frame.extend_from_slice(&record_frame_bytes);
  rec_frame.extend_from_slice(b"\r\n");

  session.recv_buffer.extend_from_slice(&rec_frame);
  let remaining = session.try_consume_messages_into(&mut resp);
  assert_eq!(remaining, Some(0));
  assert!(resp.is_empty(), "记录帧不回写应答（发出即忘）");

  // 半包测试（数据未到齐）：残余驻留缓冲，游标不产出应答
  let partial_frame = &rec_frame[..15];
  session.recv_buffer.extend_from_slice(partial_frame);
  let remaining = session.try_consume_messages_into(&mut resp);
  assert_eq!(remaining, Some(partial_frame.len()));
  assert!(resp.is_empty());

  // 非法命令测试：返回 -ERR
  let invalid_cmd = b"*2\r\n$4\r\nPING\r\n$0\r\n\r\n";
  session.recv_buffer.extend_from_slice(invalid_cmd);
  let remaining = session.try_consume_messages_into(&mut resp);
  assert_eq!(remaining, Some(0));
  assert!(resp.starts_with(b"-ERR"));
}
