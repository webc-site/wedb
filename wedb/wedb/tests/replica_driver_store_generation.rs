//! 副本重放驱动仓代际隔离集成测试（数据丢失级缺陷判别面：旧复制连接
//! 迟到 dispose 误杀新代重放驱动仓 + 驱动缺席静默旁路直推位点）
//!
//! 对标 Garnet C#：
//! - ReplicaReplayManager.cs:34-38 `ResetReplicaReplayDriverStore` =
//!   旧实例 Dispose + new 新实例（容器非共享单例，代际换代）
//! - RespClusterReplicationCommands.cs:221-224 APPENDLOG 初始化帧注册成功
//!   时会话捕获当时代际引用为私有字段（replicaReplayDriverStore）
//! - ClusterSession.cs:212-219 Dispose 只 dispose 会话自持代际实例
//!   （ReplicaReplayDriverStore.cs:73-95 dispose 经内部标志幂等）——换代后
//!   旧连接迟到 dispose 落在已处置旧实例上幂等空转，绝不误杀新主连接
//!   已注册的新一代驱动
//! - ReplicaReplaySession.cs:106-125 ProcessPrimaryStream 驱动缺席即异常
//!   上抛（GarnetException clientResponse:false 致命断流）——不存在「落盘
//!   不应用、位点照推」的静默旁路；仅无重放资产的退化装配保留直推

use std::{io, io::ErrorKind, sync::Arc, time::Duration};

use waof::{AofEntryType, WalConfig, WalFrameHeader, WalLog};
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  replication::{
    cluster_replication_session::{AppendLogOutcome, ClusterReplicationSession},
    driver_registry::DriverLifecycle,
    replica_replay_task::ReplayAssets,
    replication_manager::ReplicationManager,
  },
  worker::{LocalWorkerSpec, NodeRole, Worker},
};
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace,
  aof::{
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::{GarnetLog, RecordShape},
    waof_sublog::single_log_aof,
  },
  storage::session::storage_session::StorageSession,
};
use wnode_test::replay_input_bytes;
use wresp::command::RespCommand;
use wtest_base::{open_test_store, wait_for};
use wval::{KeyTag, NamespaceDbCodec};

const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

/// 物理键编码（与 replica_background_replay.rs 同口径）
fn physical(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// SET 条目入队源日志（真实 AOF 条目作推流帧负载，严禁假 mock）
fn enqueue_upsert(log: &GarnetLog, key: &[u8], value: &[u8]) {
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
}

/// 源日志取首条记录条目字节（真实 AOF 编码）
fn source_entries(key: &[u8], value: &[u8]) -> Vec<u8> {
  let options = RuntimeServerOptions::default();
  let (_dirs, backends) = wnode_test::test_sublogs("gen_entries", 1);
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

/// 代际判别 fixture：副本角色 provider + rm（可选挂真实重放资产）+ 副本 wal
struct Fixture {
  _dir: tempfile::TempDir,
  _store_dir: tempfile::TempDir,
  provider: Arc<ClusterProvider>,
  rm: Arc<ReplicationManager>,
  wal: Arc<WalLog<SegmentedDevice>>,
  /// 重放资产 aof 存活锚（drop 即断开应用链）
  _aof: Option<Arc<GarnetAppendOnlyFile>>,
  /// 重放资产 store 存活锚 + 判别读取面
  store: Option<Arc<WedbStore<SegmentedDevice>>>,
}

/// 装配（with_assets = 真实 aof/store 重放资产在场；false = 纯落盘退化形态）
fn setup(with_assets: bool) -> Fixture {
  let provider = Arc::new(ClusterProvider::default());
  provider.initialize_replication_manager(1, None, false);
  let rm = provider.replication_manager().expect("rm ready");

  let cm = Arc::new(ClusterManager::new(Arc::clone(&provider)));
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
  provider.set_aof_replay_max_lag_bytes(-1);

  let (store_dir, aof, store) = if with_assets {
    let aof_options = RuntimeServerOptions {
      aof_replay_task_count: 1,
      ..RuntimeServerOptions::default()
    };
    let aof: Arc<GarnetAppendOnlyFile> =
      single_log_aof(Arc::clone(&wal), &aof_options).expect("装配 single_log_aof");
    let (store_dir, store) = open_test_store("gen-replay").expect("store");
    // 重放资产注入（对标 wire_replication_data_plane 装配面）
    rm.set_replay_assets(Some(Arc::new(ReplayAssets::new(
      Arc::clone(&aof),
      Arc::clone(&store),
      None,
      None,
    ))));
    (store_dir, Some(aof), Some(store))
  } else {
    (tempfile::tempdir().expect("tempdir"), None, None)
  };

  Fixture {
    _dir: dir,
    _store_dir: store_dir,
    provider,
    rm,
    wal,
    _aof: aof,
    store,
  }
}

impl Fixture {
  /// 独立连接会话（每连接一个消费面实例，对标 per-connection session）
  fn session(&self) -> ClusterReplicationSession<SegmentedDevice> {
    ClusterReplicationSession::new(Arc::clone(&self.provider), Arc::clone(&self.wal), None)
  }

  /// 初始化帧握手（注册重放驱动并捕获当时代驱动仓）
  fn init(&self, session: &ClusterReplicationSession<SegmentedDevice>) {
    assert_eq!(
      session
        .process_append_log(PRIMARY_ID, 0, -1, -1, -1, &[])
        .expect("初始化帧握手"),
      AppendLogOutcome::Initialized,
      "初始化帧握手应答 Initialized"
    );
  }

  /// 稳态记录帧推流（current = 当前尾位），返回 (应答, 帧尾地址)
  fn push_record(
    &self,
    session: &ClusterReplicationSession<SegmentedDevice>,
    payload: &[u8],
  ) -> io::Result<(AppendLogOutcome, i64)> {
    let frame = record_frame(payload);
    let current = self.wal.tail_address() as i64;
    let next = current + frame.len() as i64;
    session
      .process_append_log(PRIMARY_ID, 0, current, current, next, &frame)
      .map(|outcome| (outcome, next))
  }
}

/// 核心判别：旧连接注册代 A → 换主换代注册代 B → 旧连接迟到 dispose，
/// 代 B 驱动与位点应用链完全不受影响（旧缺陷中共享单例 reset 会误杀
/// 代 B 驱动，此后记录帧落盘不应用、位点静默假推进直至重连永久丢数据）
#[compio::test]
async fn late_dispose_of_old_generation_spares_current_generation() -> aok::Void {
  let fixture = setup(true);
  let key = b"gen-key";
  let value = b"gen-value";

  // ---- 旧连接 C1 在代 A 完成初始化注册
  let mut s1 = fixture.session();
  fixture.init(&s1);
  let gen_a = fixture.rm.current_replica_replay_driver_store();
  assert!(
    gen_a.get_replay_driver(0).is_some(),
    "代 A 驱动经初始化帧注册在册"
  );
  assert!(
    fixture.rm.has_active_replication_stream(),
    "代 A 在册即活跃复制流"
  );

  // ---- 换主恢复：recovery 面换代（dispose A + new B），新连接 C2 在代 B 注册
  fixture.rm.reset_replica_replay_driver_store();
  let gen_b = fixture.rm.current_replica_replay_driver_store();
  assert!(!Arc::ptr_eq(&gen_a, &gen_b), "换代即全新实例（非共享单例）");
  let s2 = fixture.session();
  fixture.init(&s2);
  assert!(
    gen_b.get_replay_driver(0).is_some(),
    "代 B 驱动经新连接初始化帧注册在册"
  );

  // ---- 代 B 正常推流一条记录（真实 AOF 条目，背景重放启动）
  let entry = source_entries(key, value);
  let (outcome, next) = fixture.push_record(&s2, &entry).expect("C2 记录帧推流成功");
  assert_eq!(outcome, AppendLogOutcome::Record);
  let driver_b = gen_b.get_replay_driver(0).expect("代 B 驱动在册");
  assert!(
    driver_b.background_replay_started(),
    "代 B 背景重放任务已启动（max_lag=-1 滞后触发）"
  );

  // ---- 旧连接 C1 迟到 dispose：只 dispose 自持代 A 实例（已被换代处置 →
  // CAS 幂等空转）；重复 dispose 同样空转不炸
  MessageConsumerFace::dispose(&mut s1);
  MessageConsumerFace::dispose(&mut s1);
  assert!(
    gen_b.get_replay_driver(0).is_some(),
    "旧连接迟到 dispose 绝不误杀当前代 B 驱动"
  );
  assert!(
    gen_a.get_replay_driver(0).is_none(),
    "自持代 A 驱动已随处置排空"
  );
  assert!(driver_b.is_active(), "代 B 驱动保持活动，应用链未被波及");

  // ---- 位点应用链不受影响：applied 经代 B 驱动权威回推追平帧尾，值进存储
  assert!(
    wait_for(
      || fixture.rm.get_replication_offset(0) >= next,
      Duration::from_secs(5),
    )
    .await,
    "代 B 应用链位点追平，未被旧连接 dispose 打断"
  );
  let store = fixture.store.as_ref().expect("assets store");
  let session = store.new_session().expect("read session");
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  assert_eq!(
    storage.read_string(key).await.expect("读取").as_deref(),
    Some(value.as_slice()),
    "记录已真实应用进存储"
  );
  Ok(())
}

/// 当前代持有连接自身断连：dispose 自持当代容器 → 当代驱动排空、活跃复制流
/// 判定即刻可复位；recovery 换代后重注册恢复可用（对标 C# 断连即断流语义，
/// 重连初始化前必须换代）
#[compio::test]
async fn current_generation_dispose_resets_active_stream_until_reset() -> aok::Void {
  let fixture = setup(true);
  let mut s = fixture.session();
  fixture.init(&s);
  let gen_cur = fixture.rm.current_replica_replay_driver_store();
  assert!(fixture.rm.has_active_replication_stream());

  MessageConsumerFace::dispose(&mut s);
  assert!(
    gen_cur.get_replay_driver(0).is_none(),
    "当代驱动随自持仓处置排空"
  );
  assert!(
    !fixture.rm.has_active_replication_stream(),
    "活跃复制流判定即刻复位，ensure_replication 可判恢复"
  );
  assert!(
    !fixture.rm.initialize_replica_replay_driver(0),
    "已处置代容器拒新注册（对标 C# dispose 关闭语义，杜绝孤儿驱动）"
  );

  // recovery 换代后新连接初始化帧重注册成功
  fixture.rm.reset_replica_replay_driver_store();
  let s2 = fixture.session();
  fixture.init(&s2);
  assert!(
    fixture
      .rm
      .current_replica_replay_driver_store()
      .get_replay_driver(0)
      .is_some(),
    "换代后新连接初始化帧重注册成功"
  );
  Ok(())
}

/// 驱动缺席 + 重放资产在场：记录帧必须致命断流上抛（对标 C#
/// ReplicaReplaySession.cs:106-125 驱动缺席异常 → clientResponse:false），
/// 不得静默直推位点——位点假推进即重连续传起点跳过未应用数据
#[compio::test]
async fn driver_absent_with_assets_is_fatal_not_silent_push() -> aok::Void {
  let fixture = setup(true);
  let entry = source_entries(b"lost-key", b"lost-value");

  // 形态一：会话从未注册（自持代仓 None）→ NotFound 致命，位点不动
  let s = fixture.session();
  let err = fixture
    .push_record(&s, &entry)
    .expect_err("驱动缺席 + 资产在场必须致命上抛，不得 Ok");
  assert_eq!(
    err.kind(),
    ErrorKind::NotFound,
    "驱动缺席帧上抛 NotFound（消费泵转 fatal_disconnect 断流）"
  );
  assert_eq!(
    fixture.rm.get_replication_offset(0),
    0,
    "致命帧绝不同步推进位点（旧旁路在此假推进埋数据丢失）"
  );

  // 形态二：会话注册代 A 后 recovery 换代（代 A 已处置），旧连接迟到记录帧
  // 自持代仓取不到驱动 → 同样致命，且绝不在当代旁路自造注册
  let s1 = fixture.session();
  fixture.init(&s1);
  fixture.rm.reset_replica_replay_driver_store();
  let err = fixture
    .push_record(&s1, &entry)
    .expect_err("自持代仓已处置即驱动缺席，致命上抛");
  assert_eq!(err.kind(), ErrorKind::NotFound);
  assert!(
    fixture
      .rm
      .current_replica_replay_driver_store()
      .get_replay_driver(0)
      .is_none(),
    "缺席帧不得旁路自造注册"
  );
  Ok(())
}

/// 退化装配（重放资产缺席）双臂保留：驱动在册 → 位点 enqueued 直推；驱动
/// 缺席 → 静默 no-op（对标 C# 纯落盘退化形态，不升级致命），防清理过头
#[compio::test]
async fn degenerate_flush_only_assembly_keeps_direct_push_and_noop() -> aok::Void {
  let fixture = setup(false);
  assert!(fixture.rm.replay_assets().is_none());
  let entry = source_entries(b"disk-key", b"disk-value");

  // 驱动在册臂：落盘面直推位点（enqueued 语义保留）
  let s = fixture.session();
  fixture.init(&s);
  let (outcome, next) = fixture
    .push_record(&s, &entry)
    .expect("退化装配驱动在册直推");
  assert_eq!(outcome, AppendLogOutcome::Record);
  assert_eq!(
    fixture.rm.get_replication_offset(0),
    next,
    "退化装配位点保持落盘面直推"
  );

  // 驱动缺席臂：静默 no-op 不致命（帧照常落盘，位点由落盘面直推）
  let s2 = fixture.session();
  let (outcome, next2) = fixture
    .push_record(&s2, &entry)
    .expect("退化装配驱动缺席仅记账 no-op，不升级致命");
  assert_eq!(outcome, AppendLogOutcome::Record);
  assert_eq!(fixture.rm.get_replication_offset(0), next2);
  Ok(())
}
