//! 副本背景重放与主端节流集成测试（异步重放模式应用链闭环）
//!
//! 对标 Garnet C#：
//! - ReplicaReplayDriver.cs:InitializeBackgroundReplayTask + ThrottlePrimary
//! - ReplicaReplaySession.cs:ProcessPrimaryStream 异步路径
//!   （enqueue → 背景重放应用进存储 → applied 位点回推）
//! - ReplicaReplayDriver.cs:SignalTimeAdvance（ADVANCE_TIME 脉冲读一致
//!   时间推进）
//!
//! 真实应用链装配：single_log_aof(副本 wal) + open_test_store 构造
//! ReplayAssets 注入 rm；记录帧携带真实 AOF 条目（RecordShape 编码），
//! 背景重放线程经 AofProcessor 应用进存储。

use std::{sync::Arc, time::Duration};

use aok::OK;
use compio::runtime::Runtime;
use waof::{AofEntryType, WalConfig, WalFrameHeader, WalLog};
use wbase::hex::hex_str_u128;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  replication::{
    cluster_replication_session::{AppendLogOutcome, ClusterReplicationSession},
    replica_replay_task::ReplayAssets,
    replication_manager::ReplicationManager,
  },
  worker::{LocalWorkerSpec, NodeRole, Worker},
};
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, ReplayInput,
  aof::{
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::{GarnetLog, RecordShape},
    waof_sublog::single_log_aof,
  },
  storage::session::storage_session::StorageSession,
};
use wresp::command::RespCommand;
use wtest_base::{open_test_store, wait_for};
use wval::{KeyTag, NamespaceDbCodec};

const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

/// 物理键编码（条目 key 统一为 wkv 物理键：[NsVarint][DbVarint][KeyTag][用户键]）
fn physical(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// SET 条目编码入队（对标 C# ProcessAofRecordInternal 消费的 StoreUpsert 记录）
fn enqueue_upsert(log: &GarnetLog, key: &[u8], value: &[u8]) {
  let mut input = Vec::new();
  ReplayInput {
    cmd: RespCommand::Set,
    flags: 0,
    sub_id: 0,
    obj_type: 0,
    arg1: 0,
    arg2: 0,
    arg3: 0,
    args: vec![key.to_vec(), value.to_vec()],
  }
  .serialize(&mut input);
  let _ = log.enqueue(&RecordShape {
    op_type: AofEntryType::StoreUpsert,
    version: 0,
    session_id: 1,
    key: &physical(key),
    value,
    input: &input,
    database_id: 0,
  });
}

/// 内存日志构造（真实 AOF 条目的产出源；扫描取出条目字节作推流帧负载）
fn source_entries(key: &[u8], value: &[u8]) -> Vec<u8> {
  let options = RuntimeServerOptions::default();
  let (_dirs, backends) = wnode_test::test_sublogs("src_entries", 1);
  let log = Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog"));
  enqueue_upsert(&log, key, value);
  let begin = log.get_begin_address(0);
  let mut records = Vec::new();
  log.scan_single_with(0, begin, log.get_tail_address(0), |r| {
    records.push(r.clone());
    true
  });
  records
    .into_iter()
    .next()
    .map(|r| r.payload)
    .expect("条目产出")
}

/// 完整记录帧（8B wal 记录头 + AOF 条目，主端推流帧口径）
fn record_frame(entry: &[u8]) -> Vec<u8> {
  let mut frame = WalFrameHeader::for_payload(entry).to_bytes().to_vec();
  frame.extend_from_slice(entry);
  frame
}

/// 副本复制域装配：副本角色 provider + rm 挂重放资产（aof 覆盖同一 wal）
/// + 接收会话
struct ReplicaFixture {
  _dir: tempfile::TempDir,
  _store_dir: tempfile::TempDir,
  rm: Arc<ReplicationManager>,
  wal: Arc<WalLog<SegmentedDevice>>,
  aof: Arc<GarnetAppendOnlyFile>,
  session: ClusterReplicationSession<SegmentedDevice>,
  store: Arc<WedbStore<SegmentedDevice>>,
}

fn setup_replica(max_lag_bytes: i32) -> ReplicaFixture {
  setup_replica_with(max_lag_bytes, 1)
}

/// 带 AofReplayTaskCount 形参的装配变体（读一致时间面仅在 multi-log
/// 模式在场，对标 C# MultiLogEnabled 门控：physical>1 || replay>1）
fn setup_replica_with(max_lag_bytes: i32, replay_task_count: i32) -> ReplicaFixture {
  let provider = Arc::new(ClusterProvider::default());
  provider.initialize_replication_manager(1, None, false);
  let rm = provider.replication_manager().expect("rm ready");

  let cm = Arc::new(ClusterManager::new(provider.clone()));
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: REPLICA_ID,
    address: "127.0.0.1",
    port: 7001,
    config_epoch: 1,
    role: NodeRole::Replica,
    replica_of_node_id: Some(PRIMARY_ID),
    hostname: None,
  });
  config.workers.push(Worker {
    nodeid: Some(PRIMARY_ID),
    address: "127.0.0.1".into(),
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    replication_offset: 0,
    hostname: None,
  });
  *cm.current_config.write() = config;
  *provider.cluster_manager.write() = Some(cm);

  let dir = tempfile::tempdir().expect("tempdir");
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join("replica.wal")).expect("device"));
  let wal = Arc::new(WalLog::new(device, WalConfig::default()).expect("wal"));
  // 单物理日志 aof 覆盖同一 wal（生产装配形态，扫描/tail/读一致时间同源）
  let aof_options = RuntimeServerOptions {
    aof_replay_task_count: replay_task_count,
    ..RuntimeServerOptions::default()
  };
  let aof: Arc<GarnetAppendOnlyFile> =
    single_log_aof(wal.clone(), &aof_options).expect("装配 single_log_aof");
  let (store_dir, store) = open_test_store("replica-replay").expect("store");
  // 重放资产注入（对标 wire_replication_data_plane 装配面）
  rm.set_replay_assets(Some(Arc::new(ReplayAssets::new(
    Arc::clone(&aof),
    Arc::clone(&store),
    None,
  ))));
  provider.set_aof_replay_max_lag_bytes(max_lag_bytes);

  let session = ClusterReplicationSession::new(provider.clone(), wal.clone(), None);
  ReplicaFixture {
    _dir: dir,
    _store_dir: store_dir,
    rm,
    wal,
    aof,
    session,
    store,
  }
}

/// RESP 批串元素（$len 前导 + 字节 + CRLF）
fn bulk(bytes: &[u8]) -> Vec<u8> {
  let mut b = format!("${}\r\n", bytes.len()).into_bytes();
  b.extend_from_slice(bytes);
  b.extend_from_slice(b"\r\n");
  b
}

/// CLUSTER APPENDLOG 记录帧 RESP 编码（8 元素，消费泵协议推流入会话缓冲）
fn resp_appendlog_record(
  primary: u128,
  sublog_idx: usize,
  previous: i64,
  current: i64,
  next: i64,
  payload: &[u8],
) -> Vec<u8> {
  let mut f = b"*8\r\n".to_vec();
  f.extend_from_slice(&bulk(b"CLUSTER"));
  f.extend_from_slice(&bulk(b"APPENDLOG"));
  f.extend_from_slice(&bulk(hex_str_u128(primary).as_bytes()));
  f.extend_from_slice(&bulk(sublog_idx.to_string().as_bytes()));
  f.extend_from_slice(&bulk(previous.to_string().as_bytes()));
  f.extend_from_slice(&bulk(current.to_string().as_bytes()));
  f.extend_from_slice(&bulk(next.to_string().as_bytes()));
  f.extend_from_slice(&bulk(payload));
  f
}

/// 初始化帧握手（注册重放驱动）+ 推流一条记录帧，返回帧尾地址
fn attach_and_push(fixture: &ReplicaFixture, entry: &[u8]) -> i64 {
  fixture
    .session
    .process_append_log(PRIMARY_ID, 0, -1, -1, -1, &[])
    .expect("init handshake");
  push_record(fixture, entry)
}

/// 推流一条记录帧（稳态衔接：current = 当前尾位），返回帧尾地址
fn push_record(fixture: &ReplicaFixture, entry: &[u8]) -> i64 {
  let frame = record_frame(entry);
  let current = fixture.wal.tail_address() as i64;
  let next = current + frame.len() as i64;
  let outcome = fixture
    .session
    .process_append_log(PRIMARY_ID, 0, current, current, next, &frame)
    .expect("record push");
  assert_eq!(outcome, AppendLogOutcome::Record);
  next
}

/// 存储读取（重放应用闭环断言面）
async fn read_string(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let session = store.new_session().expect("read session");
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  storage.read_string(key).await.expect("read string")
}

/// 滞后触发背景重放 + 水位追平 + 存储应用闭环：记录帧落盘后背景任务启动，
/// applied 位点经驱动权威面回推至日志尾，记录真实应用进存储
#[test]
fn background_replay_applies_records_and_catches_up() -> aok::Void {
  Runtime::new()?.block_on(async {
    let mut fixture = setup_replica(-1);
    let entry1 = source_entries(b"k1", b"v1");
    let entry2 = source_entries(b"k2", b"v2");

    // 首帧：初始化 + 首条记录（背景重放应被滞后触发启动）
    attach_and_push(&fixture, &entry1);
    let driver = fixture
      .rm
      .replica_replay_driver_store
      .get_replay_driver(0)
      .expect("驱动已注册");
    assert!(
      driver.background_replay_started(),
      "背景重放任务被首帧滞后触发"
    );

    // 次帧：幂等（背景任务不重复启动），位点照常收敛
    let next = push_record(&fixture, &entry2);
    assert!(
      wait_for(
        || fixture.rm.get_replication_offset(0) >= next,
        Duration::from_secs(5),
      )
      .await,
      "applied 位点经权威面回推追平日志尾"
    );
    assert!(
      fixture.wal.tail_address() as i64 >= next,
      "记录帧已保真落盘"
    );

    // 存储应用闭环：键值已由背景重放写入存储引擎
    assert_eq!(
      read_string(&fixture.store, b"k1").await.as_deref(),
      Some(b"v1".as_slice()),
      "记录 1 已应用进存储"
    );
    assert_eq!(
      read_string(&fixture.store, b"k2").await.as_deref(),
      Some(b"v2".as_slice()),
      "记录 2 已应用进存储"
    );

    // 断链处置：驱动仓库重置（清驱动停背景重放，容器保持开放），重连
    // init 帧可重新注册（对标 C# 会话自有驱动仓库随会话消亡的语义）
    fixture.session.dispose();
    assert!(
      !driver.background_replay_started(),
      "背景重放任务随断链处置终止"
    );
    let outcome = fixture
      .session
      .process_append_log(PRIMARY_ID, 0, -1, -1, -1, &[])
      .expect("处置后重连初始化成功");
    assert_eq!(outcome, AppendLogOutcome::Initialized);
    OK
  })
}

/// 主端节流生效（同步形态折叠：maxLag=0 每帧锁步）：消费泵协议推帧后泵
/// 取走节流挂起体 await——resolve 返回时位点必须已追平帧尾（协程挂起
/// 等待重放应用完成，无线程阻塞），收敛态次帧装配即直通
#[test]
fn throttle_primary_blocks_until_caught_up() -> aok::Void {
  Runtime::new()?.block_on(async {
    let mut fixture = setup_replica(0);
    let entry = source_entries(b"tk", b"tv");

    // 初始化帧握手经消费泵协议（注册重放驱动）
    let init_frame = concat!(
      "*7\r\n$7\r\nCLUSTER\r\n$9\r\nAPPENDLOG\r\n$32\r\n",
      "0de10000000000000000000000000001",
      "\r\n$1\r\n0\r\n$2\r\n-1\r\n$2\r\n-1\r\n$2\r\n-1\r\n"
    )
    .as_bytes();
    fixture.session.recv_buffer.extend_from_slice(init_frame);
    let mut resp = Vec::new();
    assert_eq!(
      fixture.session.try_consume_messages_into(&mut resp),
      Some(0)
    );
    assert_eq!(resp, b"+OK\r\n", "初始化帧回 +OK");

    // 记录帧推流：落盘后 lag 越限，消费面暂存节流挂起体
    let frame = record_frame(&entry);
    let next = frame.len() as i64;
    fixture
      .session
      .recv_buffer
      .extend_from_slice(&resp_appendlog_record(PRIMARY_ID, 0, 0, 0, next, &frame));
    resp.clear();
    assert_eq!(
      fixture.session.try_consume_messages_into(&mut resp),
      Some(0)
    );
    assert!(resp.is_empty(), "记录帧不回写应答");

    // 泵取走节流挂起体，await 至位点收敛（挂起协程不占线程；装配前重放
    // 竞速即时收敛的直通形态同样放行至追平）
    if let Some(wait) = fixture.session.take_slow_wait() {
      assert!(wait.resolve().await.is_empty(), "节流放行无应答字节");
    }
    assert_eq!(
      fixture.rm.get_replication_offset(0),
      next,
      "maxLag=0 锁步：放行时 applied 位点追平帧尾"
    );
    assert_eq!(
      read_string(&fixture.store, b"tk").await.as_deref(),
      Some(b"tv".as_slice()),
      "锁步放行前记录已应用进存储"
    );

    // 追平后推次帧：锁步语义保持（重放应用至帧尾收敛）
    let entry2 = source_entries(b"tk2", b"tv2");
    let frame2 = record_frame(&entry2);
    let current2 = next;
    let next2 = current2 + frame2.len() as i64;
    fixture
      .session
      .recv_buffer
      .extend_from_slice(&resp_appendlog_record(
        PRIMARY_ID, 0, current2, current2, next2, &frame2,
      ));
    resp.clear();
    assert_eq!(
      fixture.session.try_consume_messages_into(&mut resp),
      Some(0)
    );
    assert!(
      wait_for(
        || fixture.rm.get_replication_offset(0) >= next2,
        Duration::from_secs(5),
      )
      .await,
      "第二帧锁步追平"
    );
    OK
  })
}

/// ADVANCE_TIME 脉冲消费闭环：位点追平后 signal_time_advance 推进读一致
/// 时间（advance_virtual_sublog_time 下游生效）；过期脉冲单调不回退
#[test]
fn advance_time_pulse_applies_when_caught_up() -> aok::Void {
  Runtime::new()?.block_on(async {
    let fixture = setup_replica_with(-1, 2);
    let entry = source_entries(b"pk", b"pv");

    attach_and_push(&fixture, &entry);
    let next = fixture.wal.tail_address() as i64;
    assert!(
      wait_for(
        || fixture.rm.get_replication_offset(0) >= next,
        Duration::from_secs(5),
      )
      .await,
      "位点追平前置条件"
    );

    let driver = fixture
      .rm
      .replica_replay_driver_store
      .get_replay_driver(0)
      .expect("驱动已注册");
    // 脉冲应用面与背景重放同源（同一 aof 实例的读一致管理器）
    let rcm = fixture
      .aof
      .read_consistency_manager()
      .expect("读一致管理器在场");

    // 追平态：脉冲被应用（背景重放循环 throttle 消化，读一致时间推进）
    driver.signal_time_advance(77);
    assert!(
      wait_for(
        || rcm.get_physical_sublog_max(0) == 77,
        Duration::from_secs(5),
      )
      .await,
      "追平后脉冲推进读一致时间"
    );

    // 过期脉冲直退：单调面不回退
    driver.signal_time_advance(50);
    assert_eq!(
      rcm.get_physical_sublog_max(0),
      77,
      "过期脉冲不推进（单调守卫）"
    );
    OK
  })
}
