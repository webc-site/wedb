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
//!
//! 副本一致读端到端三支（同 fixture 复用）：主库写入经会话推流 + 背景重放
//! 落进存储后，读侧在 `wait_for_sequence_number` 上真挂起、被**真实回放链**
//! 放行（无手工水位回灌：单物理日志折叠重放须覆盖该物理子日志全部虚拟子日
//! 志）、重放链未运行时上抛 ConsistentReadTimeout 拒读脏值、角色翻主即刻
//! 零等待直通（防主库读路径白付一致读协议开销回潮）。
//!
//! 回放补维两臂（裁决见 task/done/replica-replay-sublog-dimension-verdict
//! .md）：replay_chain_releases_non_zero_virtual_sublog_waiter（记录/批终臂
//! 的 waiter 级放行）与 advance_time_pulse_advances_every_virtual_sublog
//!（ADVANCE_TIME 脉冲臂须覆盖全部虚拟子日志）。

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
  time::Duration,
};

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
use wkv::{Error, WedbStore};
use wnode::{
  MessageConsumerFace, ReplayInput,
  aof::{
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::{GarnetLog, RecordShape},
    readconsistency::{
      read_consistency_manager::ReadConsistencyManager,
      replica_read_session_context::ReadSessionState,
      virtual_sublog_replay_state::ReadSessionWaiter,
    },
    waof_sublog::single_log_aof,
  },
  primary_tasks::PrimaryTasks,
  storage::session::storage_session::StorageSession,
};
use wresp::command::RespCommand;
use wtest_base::{open_test_store, wait_for};
use wval::{KeyTag, NamespaceDbCodec};

const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

/// 读侧新鲜度等待默认超时（对标 C# GarnetServerOptions.ReplicaSyncTimeout
/// 默认 5s；放行臂留百倍以上余量，不参与时序判据）
const DEFAULT_SYNC_TIMEOUT_SECS: u64 = 5;

/// 读侧新鲜度等待超时下限（同一口径为秒级，超时臂取最小值以缩短挂起）
const MIN_SYNC_TIMEOUT_SECS: u64 = 1;

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
/// + 接收会话 + 服务级角色门
struct ReplicaFixture {
  _dir: tempfile::TempDir,
  _store_dir: tempfile::TempDir,
  rm: Arc<ReplicationManager>,
  wal: Arc<WalLog<SegmentedDevice>>,
  aof: Arc<GarnetAppendOnlyFile>,
  session: ClusterReplicationSession<SegmentedDevice>,
  store: Arc<WedbStore<SegmentedDevice>>,
  /// 角色门（对标生产装配单点：provider.set_primary_tasks 注入、服务按连接
  /// 挂进 ReadSessionState 的 role_gate 分量；set_primary_tasks 按当前角色
  /// 初始化挂起态，本 fixture 集群配置角色为 Replica 故置位即副本态）
  gate: Arc<PrimaryTasks>,
}

fn setup_replica(max_lag_bytes: i32) -> ReplicaFixture {
  setup_replica_with(max_lag_bytes, 1, DEFAULT_SYNC_TIMEOUT_SECS)
}

/// 带 AofReplayTaskCount 与读侧新鲜度超时形参的装配变体（读一致时间面仅在
/// multi-log 模式在场，对标 C# MultiLogEnabled 门控：physical>1 || replay>1；
/// 超时对标 RuntimeServerOptions.replica_sync_timeout_secs，秒级口径）
fn setup_replica_with(
  max_lag_bytes: i32,
  replay_task_count: i32,
  replica_sync_timeout_secs: u64,
) -> ReplicaFixture {
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
    replica_sync_timeout_secs,
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
    None,
  ))));
  provider.set_aof_replay_max_lag_bytes(max_lag_bytes);
  // 角色门注入（对标 StoreWrapper 建服务时的 primary_tasks 装配；
  // set_primary_tasks 按 provider.is_replica 初始化副本挂起位）
  let gate = Arc::new(PrimaryTasks::default());
  provider.set_primary_tasks(Arc::clone(&gate));

  let session = ClusterReplicationSession::new(provider.clone(), wal.clone(), None);
  ReplicaFixture {
    _dir: dir,
    _store_dir: store_dir,
    rm,
    wal,
    aof,
    session,
    store,
    gate,
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

/// 真实复制面推流一条记录 + 背景重放应用确证：帧落盘后 applied 位点经驱动
/// 权威面回推追平帧尾（值已在存储，读侧是否放行另由读一致时间裁决）
async fn push_and_await_replay(fixture: &ReplicaFixture, entry: &[u8]) {
  let next = attach_and_push(fixture, entry);
  assert!(
    wait_for(
      || fixture.rm.get_replication_offset(0) >= next,
      Duration::from_secs(5),
    )
    .await,
    "applied 位点追平帧尾，记录已重放应用进存储"
  );
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
    let fixture = setup_replica_with(-1, 2, DEFAULT_SYNC_TIMEOUT_SECS);
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

    // 追平态：脉冲被应用（背景重放循环 throttle 消化，读一致时间推进）。
    // 脉冲值取在 applied 水位之上：重放链自身已把读一致时间推到 applied 位点
    //（回放补维臂，见 replay_chain_releases_non_zero_virtual_sublog_waiter），
    // 只有高于该水位的脉冲才可被观测到推进
    let pulse = fixture.rm.get_replication_offset(0) + 77;
    driver.signal_time_advance(pulse);
    assert!(
      wait_for(
        || rcm.get_physical_sublog_max(0) == pulse,
        Duration::from_secs(5),
      )
      .await,
      "追平后脉冲推进读一致时间"
    );

    // 过期脉冲直退：单调面不回退
    driver.signal_time_advance(pulse - 27);
    assert_eq!(
      rcm.get_physical_sublog_max(0),
      pulse,
      "过期脉冲不推进（单调守卫）"
    );
    OK
  })
}

/// ADVANCE_TIME 脉冲须覆盖本物理子日志的**全部**虚拟子日志（回放补维脉冲臂）
///
/// C# 侧多回放任务形态下，一颗脉冲经页闸栏广播后由各任务推自有虚拟子日志
///（ReplicaReplayTask.cs:75-78）；单任务形态下直推 (physical, 0) 已完备——
/// 因为 GetReplayTaskIdx 恒为 0，(physical, 0) 即该物理子日志的全部槽位。
/// 本仓把并行重放折叠为单消费者，但读侧路由仍按 replay_task_count 分槽，
/// 故折叠道之外的 (0, 1) 槽同样只能由这条脉冲链放行。
#[test]
fn advance_time_pulse_advances_every_virtual_sublog() -> aok::Void {
  Runtime::new()?.block_on(async {
    let fixture = setup_replica_with(-1, 2, DEFAULT_SYNC_TIMEOUT_SECS);
    let rcm = fixture
      .aof
      .read_consistency_manager()
      .expect("多日志拓扑读一致管理器在场");
    let folded_vsr = fixture.aof.get_virtual_sublog_idx(0, 0);
    let other_vsr = fixture.aof.get_virtual_sublog_idx(0, 1);
    assert_eq!(rcm.virtual_sublog_count(), 2, "拓扑 = 单物理 × 双回放");
    assert_ne!(folded_vsr, other_vsr, "折叠道之外另成一槽");

    push_and_await_replay(&fixture, &source_entries(b"ap", b"aq")).await;
    let driver = fixture
      .rm
      .replica_replay_driver_store
      .get_replay_driver(0)
      .expect("驱动已注册");

    // 追平态发一颗高于 applied 水位的脉冲：两槽前沿须同时落到脉冲值
    let pulse = fixture.rm.get_replication_offset(0) + 177;
    driver.signal_time_advance(pulse);
    assert!(
      wait_for(
        || rcm.vsr(folded_vsr).max() == pulse && rcm.vsr(other_vsr).max() == pulse,
        Duration::from_secs(5),
      )
      .await,
      "脉冲推进该物理子日志全部虚拟子日志（折叠道外槽不得滞留）"
    );

    // 过期脉冲不回退任何一槽
    driver.signal_time_advance(pulse - 1);
    assert_eq!(
      rcm.vsr(other_vsr).max(),
      pulse,
      "过期脉冲不推进（单调守卫）"
    );
    OK
  })
}

/// 取一对读侧路由到不同虚拟子日志的用户键（跨子日志新鲜度校验的触发前提）
///
/// 读侧哈希与换算一律走管理器公开口，并落在 **String 域记录物理键** 上：
/// 一致读触发哈希域即读侧所读记录的物理键（wkv `StoreSession::
/// consistent_read_hash`），与回放侧草图入账的 AOF 条目键同字节同哈希，故
/// `key_hash(物理键)` + `virtual_sublog_idx_of_hash` 正是 `verify_key_freshness`
/// 所用同一路由，所选键对必然落在「上一读子日志 ≠ 本读子日志」分支上。
fn pick_cross_sublog_keys(rcm: &ReadConsistencyManager) -> (Vec<u8>, Vec<u8>) {
  assert_eq!(
    rcm.virtual_sublog_count(),
    2,
    "一致读臂拓扑 = 单物理 × 双回放"
  );
  let route = |user_key: &[u8]| rcm.virtual_sublog_idx_of_hash(rcm.key_hash(&physical(user_key)));
  let find = |want: usize| -> Vec<u8> {
    (0..256u32)
      .map(|i| format!("crk-{i}").into_bytes())
      .find(|key| route(key) == want)
      .expect("目标虚拟子日志必有路由键")
  };
  (find(0), find(1))
}

/// 一致读挂起 → 真实回放链放行端到端（读侧真阻塞链，无手工水位回灌）
///
/// 编排（读侧新鲜度等待是线程级阻塞，见 VirtualSublogReplayState::
/// wait_for_sequence_number 对位 C# WaitForSequenceNumber 的
/// “arm the waiter and block”，故读者独占线程、编排留在主线程 runtime）：
/// 1. 读者以生产装配口建会话（aof 读一致管理器 + 角色门 →
///    ReadSessionState::attach → with_read_session_state），先读 kp 完成协议
///    热身（置 last_virtual_sublog_idx 与会话序列号），再读路由到另一虚拟
///    子日志的 kq；
/// 2. 主线程只走真实复制面：kq 的 AOF 条目经会话推流落盘、由背景重放经
///    aof_processor 应用进存储；
/// 3. 判据：读被放行并读到重放值，且放行水位就是重放链推出来的 applied 位点
///    ——全程不碰管理器的水位回灌口。
///
/// 补维前的红相（本臂镜像 N5 手工回灌臂，登记见
/// task/done/replica-replay-sublog-dimension-verdict.md）：折叠重放面只推进
/// 虚拟子日志 `get_virtual_sublog_idx(physical, 0)`，kq 所辖子日志前沿恒 0，
/// 读者停在 wait_for_sequence_number 至上抛 ConsistentReadTimeout。
#[test]
fn replica_consistent_read_blocks_until_replay_releases() -> aok::Void {
  Runtime::new()?.block_on(async {
    let fixture = setup_replica_with(-1, 2, DEFAULT_SYNC_TIMEOUT_SECS);
    let rcm = fixture
      .aof
      .read_consistency_manager()
      .expect("多日志拓扑读一致管理器在场");
    assert!(fixture.gate.is_replica(), "fixture 处于副本角色");
    let (kp, kq) = pick_cross_sublog_keys(&rcm);
    let kq_vsr = rcm.virtual_sublog_idx_of_hash(rcm.key_hash(&physical(&kq)));
    assert_ne!(
      kq_vsr,
      fixture.aof.get_virtual_sublog_idx(0, 0),
      "kq 落在折叠道之外的虚拟子日志"
    );

    // 读者线程：专属 runtime 上跑一致读会话（阻塞式新鲜度等待只挂该线程）
    let store = Arc::clone(&fixture.store);
    let manager = Arc::clone(&rcm);
    let gate = Arc::clone(&fixture.gate);
    let crossing = Arc::new(AtomicBool::new(false));
    let crossing_w = Arc::clone(&crossing);
    let (kp_w, kq_w) = (kp.clone(), kq.clone());
    let reader = thread::spawn(move || {
      Runtime::new().expect("读者 runtime").block_on(async move {
        let session = store
          .new_session()
          .expect("一致读会话")
          .with_read_session_state(Some(Arc::new(ReadSessionState::attach(
            manager,
            Some(gate),
          ))));
        let ss = StorageSession::new(session.enter_batch());
        assert!(ss.is_consistent_read_session(), "附着态派生一致读会话");
        // 热身读：与 kq 不同虚拟子日志，走完 pre/post 协议建立会话序列号
        assert_eq!(
          ss.read_string(&kp_w).await.expect("热身读放行"),
          None,
          "热身键未写入"
        );
        crossing_w.store(true, Ordering::Release);
        ss.read_string(&kq_w).await.expect("放行后一致读须成功")
      })
    });

    assert!(
      wait_for(|| crossing.load(Ordering::Acquire), Duration::from_secs(5)).await,
      "读者已进入跨子日志读窗口"
    );

    // 真实复制面推流 + 背景重放应用：kq 的值经 aof_processor 落进存储
    push_and_await_replay(&fixture, &source_entries(&kq, b"vq")).await;
    assert_eq!(
      read_string(&fixture.store, &kq).await.as_deref(),
      Some(b"vq".as_slice()),
      "重放已把 kq 应用进存储（普通会话可读到）"
    );
    assert_eq!(
      rcm.vsr(kq_vsr).max(),
      fixture.rm.get_replication_offset(0),
      "kq 所辖虚拟子日志的读一致时间由重放链推至 applied 位点"
    );

    // 无任何手工水位回灌：读者只能由真实回放链放行
    let value = reader.join().expect("读者收尾");
    assert_eq!(value.as_deref(), Some(b"vq".as_slice()), "放行后读到重放值");
    OK
  })
}

/// 非零虚拟子日志的等待者可被真实回放链放行（waiter 级镜像臂）
///
/// 与上一臂同一命题，但把等待者直接挂在折叠道之外的虚拟子日志槽上——用的
/// 就是 `verify_key_freshness` 停等的同一原语，不涉及读侧哈希路由域（该域
/// 归 wkv 一致读侧，见 task/done/consistent-read-sketch-hash-domain-mismatch
/// .md）。编排：先在 (physical 0, replay 1) 槽上停等（目标 1，前沿须严格越
/// 过才放行）→ 只走真实复制面推流 + 背景重放 → 等待者返回放行，且放行前沿
/// 等重于重放链的 applied 位点（本臂全程无管理器水位回灌口调用）。
///
/// 补维前的红相：回放链只推进折叠道 (physical, 0) 槽，非零槽前沿恒 0，等待
/// 者停至超时返回 false。
#[test]
fn replay_chain_releases_non_zero_virtual_sublog_waiter() -> aok::Void {
  Runtime::new()?.block_on(async {
    let fixture = setup_replica_with(-1, 2, DEFAULT_SYNC_TIMEOUT_SECS);
    let rcm = fixture
      .aof
      .read_consistency_manager()
      .expect("多日志拓扑读一致管理器在场");
    let folded_vsr = fixture.aof.get_virtual_sublog_idx(0, 0);
    let other_vsr = fixture.aof.get_virtual_sublog_idx(0, 1);
    assert_eq!(rcm.virtual_sublog_count(), 2, "拓扑 = 单物理 × 双回放");
    assert_ne!(folded_vsr, other_vsr, "折叠道之外另成一槽");
    assert_eq!(rcm.vsr(other_vsr).max(), 0, "重放前非零槽前沿为 0");

    // 等待者线程：停等在非零虚拟子日志槽上（同步阻塞原语，独占线程）
    let manager = Arc::clone(&rcm);
    let waiter = Arc::new(ReadSessionWaiter::new());
    let released = thread::spawn(move || {
      manager.vsr(other_vsr).wait_for_sequence_number(
        1,
        &waiter,
        Duration::from_secs(DEFAULT_SYNC_TIMEOUT_SECS),
      )
    });

    push_and_await_replay(&fixture, &source_entries(b"rcrk", b"rcrv")).await;
    assert_eq!(
      read_string(&fixture.store, b"rcrk").await.as_deref(),
      Some(b"rcrv".as_slice()),
      "记录已由真实回放链应用进存储"
    );

    assert!(
      released.join().expect("等待者收尾"),
      "非零虚拟子日志等待者须由真实回放链放行"
    );
    assert_eq!(
      rcm.vsr(other_vsr).max(),
      fixture.rm.get_replication_offset(0),
      "放行前沿等重于回放 applied 位点"
    );
    OK
  })
}

/// 存储直写（与重放链无关的本地写入；滞后臂的「脏值已在存储而读一致时间未
/// 推进」前提由此构造——补维后回放链一旦应用记录即推全部虚拟子日志前沿，
/// 唯有不经回放链的写入才留得住持久性滞后）
async fn write_string(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8], val: &[u8]) {
  let session = store.new_session().expect("write session");
  let storage = StorageSession::new(session.enter_batch());
  storage.upsert_string(key, val).await.expect("write string");
}

/// 滞后臂的脏值装载：本地直写存储、不启重放链（前沿滞留 0）
async fn stage_dirty_value(fixture: &ReplicaFixture, key: &[u8], val: &[u8]) {
  write_string(&fixture.store, key, val).await;
  assert_eq!(
    read_string(&fixture.store, key).await.as_deref(),
    Some(val),
    "脏值在场：读侧须拒绝而非静默续读"
  );
}

/// 滞后水位不推进时一致读超时上抛（不静默续读、不返脏值）
///
/// kq 的值已在存储（本地直写，读一致时间面不因它推进），而副本背景重放链
/// 从未运行：其虚拟子日志前沿恒 0。副本角色的新鲜度校验必然挂起至超时，上抛
/// [`wkv::Error::ConsistentReadTimeout`]（对标 C# WaitForSequenceNumber 抛
/// TimeoutException），脏值不外泄。超时取口径下限 1s（replica_sync_timeout
/// _secs 秒级），断言只依赖错误类型不依赖余量。
///
/// 口径变更（补维后）：旧形态以「回放链只推进折叠道 (physical,0)、kq 所辖
/// 子日志永不推进」为滞后源，那正是本票修掉的缺陷；持久性滞后自此只能来自
/// 重放链未运行，故脏值改由本地直写装载。回放链自身的放行能力由
/// replica_consistent_read_blocks_until_replay_releases 与
/// replay_chain_releases_non_zero_virtual_sublog_waiter 两臂承接。
#[test]
fn replica_consistent_read_times_out_when_watermark_lags() -> aok::Void {
  Runtime::new()?.block_on(async {
    let fixture = setup_replica_with(-1, 2, MIN_SYNC_TIMEOUT_SECS);
    let rcm = fixture
      .aof
      .read_consistency_manager()
      .expect("多日志拓扑读一致管理器在场");
    let (kp, kq) = pick_cross_sublog_keys(&rcm);

    stage_dirty_value(&fixture, &kq, b"dirty").await;

    let session = fixture
      .store
      .new_session()
      .expect("一致读会话")
      .with_read_session_state(Some(Arc::new(ReadSessionState::attach(
        Arc::clone(&rcm),
        Some(Arc::clone(&fixture.gate)),
      ))));
    let ss = StorageSession::new(session.enter_batch());
    assert_eq!(
      ss.read_string(&kp).await.expect("热身读放行"),
      None,
      "首读无跨子日志约束直通协议热身"
    );
    let err = ss
      .read_string(&kq)
      .await
      .expect_err("回放未跟上时一致读必须上抛");
    assert!(
      matches!(err, Error::ConsistentReadTimeout),
      "一致读超时语义：{err:?}"
    );
    OK
  })
}

/// 角色翻主后同一读请求零等待直通（防主库读路径白付一致读协议开销回潮）
///
/// 与超时臂同一 fixture、同一读请求：挂起条件（上一读子日志 ≠ 本读子日志、
/// 会话序列号不低于本读子日志水位）原样在场，差别只在角色门。非副本角色下
/// 由 replica_read_session_context 的 role_gate 短路，读即刻返存储真值；回翻
/// 副本又即刻挂起至上抛——角色动态性双向生效。脏值同超时臂由本地直写装载
///（补维后回放链在场即推前沿，挂起前提须由未运行的重放链留住）。
#[test]
fn primary_role_consistent_read_passes_without_wait() -> aok::Void {
  Runtime::new()?.block_on(async {
    let fixture = setup_replica_with(-1, 2, MIN_SYNC_TIMEOUT_SECS);
    let rcm = fixture
      .aof
      .read_consistency_manager()
      .expect("多日志拓扑读一致管理器在场");
    let (kp, kq) = pick_cross_sublog_keys(&rcm);

    stage_dirty_value(&fixture, &kq, b"vq").await;

    let session = fixture
      .store
      .new_session()
      .expect("一致读会话")
      .with_read_session_state(Some(Arc::new(ReadSessionState::attach(
        Arc::clone(&rcm),
        Some(Arc::clone(&fixture.gate)),
      ))));
    let ss = StorageSession::new(session.enter_batch());
    assert_eq!(
      ss.read_string(&kp).await.expect("热身读放行"),
      None,
      "副本角色热身读建立挂起前提"
    );

    // 升主：角色门翻转（对标角色切换点 PrimaryTasks 的副本位翻负）
    fixture.gate.set_replica(false);
    assert!(!fixture.gate.is_replica());
    assert_eq!(
      ss.read_string(&kq).await.expect("主库读零等待直通"),
      Some(b"vq".to_vec()),
      "非副本角色短路：同一读请求不付一致读协议等待"
    );

    // 回翻副本：同一读请求即刻重回挂起至上抛
    fixture.gate.set_replica(true);
    let err = ss
      .read_string(&kq)
      .await
      .expect_err("副本角色下挂起条件复在，须超时上抛");
    assert!(
      matches!(err, Error::ConsistentReadTimeout),
      "角色动态性回翻生效：{err:?}"
    );
    OK
  })
}
