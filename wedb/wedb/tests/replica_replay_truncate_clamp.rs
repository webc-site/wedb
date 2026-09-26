//! 副本背景重放：截断跳跃后扫描起点钳位回归（FastAofTruncate >1MB 死锁）
//!
//! 对标 Garnet C#：
//! - libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLogScanIterator.cs:
//!   GetNextInternal（`currentAddress < BeginAddress` 时 MonotonicUpdate 把游标
//!   单调跃迁至 BeginAddress，绝不滞留被截断区）
//! - libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:
//!   BackgroundReplayTaskAsync（回放驱动以跃迁后游标续扫，不因地界前跳空转）
//!
//! 危害回归：run_replay_loop 若漏掉把 applied 钳至日志 begin，FastAofTruncate
//! 跳跃重对齐（safe_initialize 把 begin/tail 前跳）后 begin 可远超 applied，
//! 一旦跨度大于 REPLAY_CHUNK_BYTES（1MB）便令传入底层 scan_single_iter 的
//! end（旧 applied + 1MB）小于 begin——底层 scan 虽对 start 钳 begin
//! （waof_sublog.rs:399 `scan(start.max(begin), end)`），但 end 不随之修正，
//! 区间 start > end 反转，迭代器首轮即空返回、applied 永不前移 → 回放死锁
//! （replayed_offset 停摆、主端 ThrottlePrimary 永久挂起）。本测构造该 1MB+
//! 跳跃，确证回放链自动跃迁至 begin 并正常重放其后续记录。

use std::{sync::Arc, time::Duration};

use aok::OK;
use waof::{AofEntryType, WalConfig, WalFrameHeader, WalLog};
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
  aof::{
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::{GarnetLog, RecordShape},
    waof_sublog::single_log_aof,
  },
  primary_tasks::PrimaryTasks,
  storage::session::storage_session::StorageSession,
};
use wnode_test::replay_input_bytes;
use wresp::command::RespCommand;
use wtest_base::{open_test_store, wait_for};
use wval::{KeyTag, NamespaceDbCodec};

const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

/// 跳跃落点：单物理子日志 0 上，begin/tail 一次性前跳至此。取 REPLAY_CHUNK_BYTES
/// （1MB）的整 2 倍，确保 begin > applied(0) + 1MB 的区间反转死锁条件成立，
/// 且远小于内存窗口上限（单记录正常尺寸可落）
const TRUNCATE_JUMP_TO: i64 = 2 * (1 << 20);

/// 物理键编码（条目 key 统一为 wkv 物理键：[NsVarint][DbVarint][KeyTag][用户键]）
fn physical(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// 内存日志构造一条真实 AOF SET 条目（StoreUpsert 记录，重放应用进存储的负载源）
fn source_entry(key: &[u8], value: &[u8]) -> Vec<u8> {
  let options = RuntimeServerOptions::default();
  let (_dirs, backends) = wnode_test::test_sublogs("src_entries", 1);
  let log = Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog"));
  let input = replay_input_bytes(RespCommand::Set, vec![key.to_vec(), value.to_vec()]);
  let _ = log.enqueue(&RecordShape {
    op_type: AofEntryType::StoreUpsert,
    version: 0,
    session_id: 1,
    key: &physical(key),
    value,
    input: &input,
    database_id: 0,
  });
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

/// 副本复制域装配：副本角色 provider + rm 挂重放资产（aof 覆盖同一 wal）+ 接收会话
struct ReplicaFixture {
  _dir: tempfile::TempDir,
  _store_dir: tempfile::TempDir,
  rm: Arc<ReplicationManager>,
  aof: Arc<GarnetAppendOnlyFile>,
  provider: Arc<ClusterProvider>,
  session: ClusterReplicationSession<SegmentedDevice>,
  store: Arc<WedbStore<SegmentedDevice>>,
  _gate: Arc<PrimaryTasks>,
}

fn setup_replica() -> ReplicaFixture {
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
  let aof_options = RuntimeServerOptions::default();
  let aof: Arc<GarnetAppendOnlyFile> =
    single_log_aof(wal.clone(), &aof_options).expect("装配 single_log_aof");
  let (store_dir, store) = open_test_store("replica-truncate-clamp").expect("store");
  rm.set_replay_assets(Some(Arc::new(ReplayAssets::new(
    Arc::clone(&aof),
    Arc::clone(&store),
    None,
    None,
  ))));
  // 异步重放形态：maxLag=-1 关闭主端节流挂起，背景重放线程独立推进 applied
  provider.set_aof_replay_max_lag_bytes(-1);
  let gate = Arc::new(PrimaryTasks::default());
  provider.set_primary_tasks(Arc::clone(&gate));

  let session = ClusterReplicationSession::new(provider.clone(), wal.clone(), None);
  ReplicaFixture {
    _dir: dir,
    _store_dir: store_dir,
    rm,
    aof,
    provider,
    session,
    store,
    _gate: gate,
  }
}

/// 存储读取（重放应用闭环断言面）
async fn read_string(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let session = store.new_session().expect("read session");
  let batch = session.enter_batch();
  StorageSession::new(batch)
    .read_string(key)
    .await
    .expect("read string")
}

/// 截断跳跃后回放起点钳位回归：begin 前跳至 2×REPLAY_CHUNK_BYTES（>applied+1MB），
/// 断言回放链自动跃迁至 begin、其后续记录正常重放进存储、applied 位点越跳跃点
/// 前移至新帧尾且不陷死锁
#[compio::test]
async fn replay_clamps_applied_to_begin_after_truncate_jump() -> aok::Void {
  let fixture = setup_replica();

  // 初始化帧握手：注册重放驱动（带重放资产），此时日志空、begin=tail=0、驱动未起背景重放
  fixture
    .session
    .process_append_log(PRIMARY_ID, 0, -1, -1, -1, &[])
    .expect("init handshake");
  let driver = fixture
    .rm
    .current_replica_replay_driver_store()
    .get_replay_driver(0)
    .expect("驱动已注册");

  // FastAofTruncate 推流跳跃重对齐开关：开启后 current > previous 的帧经会话
  // safe_initialize 把本地地址空间前跳至 current（生产截断跳跃同源挂点）
  fixture.provider.set_fast_aof_truncate(true);

  // 跳跃帧：previous=0（落后游标，背景重放自此起 applied=0）、current=2MB（begin/tail
  // 前跳落点）、next=2MB+帧长。会话据此安全重对齐并在 2MB 处落一条正常尺寸记录
  let entry = source_entry(b"trunk", b"after-jump");
  let frame = record_frame(&entry);
  let next = TRUNCATE_JUMP_TO + frame.len() as i64;
  let outcome = fixture
    .session
    .process_append_log(PRIMARY_ID, 0, 0, TRUNCATE_JUMP_TO, next, &frame)
    .expect("跳跃帧推流");
  assert_eq!(outcome, AppendLogOutcome::Record);

  // 死锁前提确证：日志 begin 已远超驱动 applied 起点（0）一个 1MB 窗口以上
  let begin = fixture.aof.log().get_begin_address(0);
  assert!(
    begin >= TRUNCATE_JUMP_TO,
    "begin 前跳确证：{begin} > applied(0) + 1MB"
  );
  assert!(
    driver.background_replay_started(),
    "背景重放已由跳跃帧 previous=0 启动（applied 起点落后 begin）"
  );

  // 判据：回放链自动跃迁至 begin 并把跳跃点后的记录应用进存储——applied 位点
  // 越 2MB 推进至新帧尾（钳位缺失时 applied 恒 0、end 恒 1MB < begin，区间反转
  // 令迭代器首轮空返回、永不前移，此处必超时）
  assert!(
    wait_for(
      || fixture.rm.get_replication_offset(0) >= next,
      Duration::from_secs(5),
    )
    .await,
    "钳位后 applied 跃迁至 begin 并重放至新帧尾，无死锁"
  );
  assert_eq!(
    read_string(&fixture.store, b"trunk").await.as_deref(),
    Some(b"after-jump".as_slice()),
    "跳跃点后的记录已由回放链应用进存储"
  );
  OK
}
