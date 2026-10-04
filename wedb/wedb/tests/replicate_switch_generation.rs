//! CLUSTER REPLICATE 改挂换代集成测试（对标 libs/cluster/Server/Replication/
//! ReplicaOps/ReplicaDiskbasedSync.cs:50 与 ReplicaDisklessSync.cs:44 的挂接
//! 入口行 `storeWrapper.appendOnlyFile.CreateOrUpdateKeySequenceManager()`，
//! 位于 TryAddReplica 与纪元等待之间）
//!
//! 缺陷场景：多日志拓扑温挂轮换——副本先挂主 A 回放积累 A 代际高序列号
//!（fetch_max 单调永驻，virtual_sublog_replay_state.rs:205-217），脱离后
//! CLUSTER REPLICATE 改挂生成器起点更低的新主 B；挂接入口缺换代则 A 代际
//! 残留污染副本读闸（read_consistency_manager.rs:332-369）：残留高草图把
//! 会话序列号顶到 B 流追不上的高度 → 持续假 ConsistentReadTimeout；残留高
//! 前沿使 mssn < cached 假新鲜跳等待 → 陈旧读。C# 同输入在挂接点整体换代
//! 无残留。
//!
//! 测试形态：真副本复制域装配（provider + rm + 真 wal/aof/store + 接收会话
//! 与背景重放链），换代经生产发起入口
//! [`try_replicate_sync_async`] 全骨架构跑（TryAddReplica 登记改挂 → 换代 →
//! 纪元等待 → attach；attach 臂因新主端点无人监听确定性失败，不影响换代副
//! 作用面的裁决——换代在纪元等待前已落）。改挂前后的草图/前沿残留以回放侧
//! 同一入账口形态装入旧代际管理器（对标 garnet 测试 ReplicationTests 系列
//! 里跨主切换前置态的装载口径）。

use std::{sync::Arc, time::Duration};

#[path = "common/replica_entry_frames.rs"]
mod replica_entry_frames;
use aok::OK;
use replica_entry_frames::{physical, record_frame, source_entries};
use waof::{WalConfig, WalLog};
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  replication::{
    assembly::try_replicate_sync_async,
    cluster_replication_session::{AppendLogOutcome, ClusterReplicationSession},
    replica_replay_task::ReplayAssets,
    replicate_sync_options::ReplicateSyncOptions,
    replication_manager::ReplicationManager,
  },
  worker::{LocalWorkerSpec, NodeRole, Worker},
};
use wkv::Error;
use wnode::{
  aof::{
    garnet_append_only_file::GarnetAppendOnlyFile,
    readconsistency::{
      read_consistency_manager::ReadConsistencyManager,
      replica_read_session_context::ReadSessionState,
    },
    waof_sublog::single_log_aof,
  },
  storage::session::storage_session::StorageSession,
};
use wtest_base::{open_test_store, wait_for};

const PRIMARY_A_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_000A;
const PRIMARY_B_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_000B;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

/// 旧代际残留高值：单条记录帧长量级 ~10²，10⁵ 保证严格高于改挂后 B 流
/// 追平位点，红/绿两臂的闸面判定确定性分离
const STALE_RESIDUE: i64 = 100_000;

/// 读者会话新鲜度等待超时（红臂挂起时长上限，绿臂不取用）
const READ_WAIT_TIMEOUT: Duration = Duration::from_millis(200);

/// 双主改挂 fixture：本节点主角色（try_add_replica 前置校验形），
/// 配置在册新主 B（端点 127.0.0.1:1 恒拒），多日志拓扑 = 单物理 × 双回放
struct SwitchFixture {
  _dir: tempfile::TempDir,
  _store_dir: tempfile::TempDir,
  provider: Arc<ClusterProvider>,
  rm: Arc<ReplicationManager>,
  wal: Arc<WalLog<SegmentedDevice>>,
  aof: Arc<GarnetAppendOnlyFile>,
  session: ClusterReplicationSession<SegmentedDevice>,
  store: Arc<wkv::WedbStore<SegmentedDevice>>,
  cm: Arc<ClusterManager>,
}

fn setup_switch(tag: &str) -> SwitchFixture {
  let provider = Arc::new(ClusterProvider::default());
  provider.initialize_replication_manager(1, None, false);
  let rm = provider.replication_manager().expect("rm ready");

  let cm = Arc::new(ClusterManager::new(Arc::clone(&provider)));
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: REPLICA_ID,
    address: "127.0.0.1",
    port: 7301,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: None,
  });
  config.workers.push(Worker {
    nodeid: Some(PRIMARY_B_ID),
    address: "127.0.0.1".into(),
    port: 1,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: Some(PRIMARY_A_ID),
    replication_offset: 0,
    hostname: None,
  });
  *cm.current_config.write() = config;
  *provider.cluster_manager.write() = Some(Arc::clone(&cm));

  let dir = tempfile::tempdir().expect("tempdir");
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join(format!("{tag}.wal"))).expect("wal设备"));
  let wal = Arc::new(WalLog::new(Arc::clone(&device), WalConfig::default()).expect("wal"));
  let aof_options = RuntimeServerOptions {
    aof_replay_task_count: 2,
    ..RuntimeServerOptions::default()
  };
  let aof: Arc<GarnetAppendOnlyFile> =
    single_log_aof(Arc::clone(&wal), &aof_options).expect("装配 single_log_aof");
  let (store_dir, store) = open_test_store(tag).expect("store");
  rm.set_replay_assets(Some(Arc::new(ReplayAssets::new(
    Arc::clone(&aof),
    Arc::clone(&store),
    None,
    None,
  ))));
  provider.set_aof_replay_max_lag_bytes(-1);
  provider.set_aof(Some(Arc::clone(&aof)));
  provider.set_store(Arc::clone(&store));
  provider.set_wal(Arc::clone(&wal));
  let session = ClusterReplicationSession::new(Arc::clone(&provider), Arc::clone(&wal), None);
  SwitchFixture {
    _dir: dir,
    _store_dir: store_dir,
    provider,
    rm,
    wal,
    aof,
    session,
    store,
    cm,
  }
}

/// 经生产发起入口改挂 B（TryAddReplica → 换代 → 纪元等待 → attach 全骨架；
/// B 端点无人监听，attach 臂确定性失败即本票换代的观测点：换代发生在
/// 纪元等待之前，发起失败不回滚代际——与 C# 同序同义）
async fn replicate_to_b(fixture: &SwitchFixture) {
  let opts = ReplicateSyncOptions::new(
    PRIMARY_B_ID,
    false, // background：前台取回错误，杜绝残留在途任务
    false, // force：本节点主角色 + 无槽位指派，走完整校验臂
    true,  // try_add_replica：真实登记改挂
    false, // allow_replica_reset_on_failure
    false, // upgrade_lock
  );
  let err = try_replicate_sync_async(&fixture.provider, opts)
    .await
    .expect_err("B 端点拒连，attach 臂确定性失败");
  assert!(
    err.to_string().contains("replica sync"),
    "发起失败文案口径：{err}"
  );
  assert_eq!(
    fixture.cm.current_config.read().local_node_primary_id(),
    Some(PRIMARY_B_ID),
    "TryAddReplica 登记已翻转：本节点改挂 B"
  );
}

/// B 流推一条记录并经真实背景重放链应用（换代后喂入面落新代际的确证路径）
async fn push_b_and_await_replay(fixture: &SwitchFixture, key: &[u8], value: &[u8]) {
  let entry = source_entries("src_entries", key, value);
  fixture
    .session
    .process_append_log(PRIMARY_B_ID, 0, -1, -1, -1, &[])
    .expect("B 流 init 握手（配置已改挂 B，主 id 校验放行）");
  let frame = record_frame(&entry);
  let current = fixture.wal.tail_address() as i64;
  let next = current + frame.len() as i64;
  let outcome = fixture
    .session
    .process_append_log(PRIMARY_B_ID, 0, current, current, next, &frame)
    .expect("B 流记录推流");
  assert_eq!(outcome, AppendLogOutcome::Record);
  assert!(
    wait_for(
      || fixture.rm.get_replication_offset(0) >= next,
      Duration::from_secs(5),
    )
    .await,
    "B 流记录经背景重放链应用追平"
  );
}

/// 存储直写（与重放链无关的本地写入：假新鲜臂「脏值在场而读一致时间未推进」
/// 前提装载，沿 replica_background_replay 的 stage_dirty_value 同式）
async fn write_string(fixture: &SwitchFixture, key: &[u8], val: &[u8]) {
  let session = fixture.store.new_session().expect("write session");
  let storage = StorageSession::new(session.enter_batch());
  storage.upsert_string(key, val).await.expect("write string");
}

/// 取一对读侧路由到不同虚拟子日志的用户键（单物理 × 双回放拓扑）
fn pick_cross_sublog_keys(rcm: &ReadConsistencyManager) -> (Vec<u8>, Vec<u8>) {
  assert_eq!(
    rcm.virtual_sublog_count(),
    2,
    "fixture 拓扑 = 单物理 × 双回放"
  );
  let route = |user_key: &[u8]| rcm.virtual_sublog_idx_of_hash(rcm.key_hash(&physical(user_key)));
  let find = |want: usize| -> Vec<u8> {
    (0..256u32)
      .map(|i| format!("gen-{i}").into_bytes())
      .find(|key| route(key) == want)
      .expect("目标虚拟子日志必有路由键")
  };
  (find(0), find(1))
}

/// 无角色门一致读会话由两行为臂各自内联构造（对位服务侧建会话逐连接
/// 现取 aof.read_consistency_manager，沿 consistent_read_session.rs
/// ReadSessionState::new 同式）。
/// 挂接入口整体换代：版本递增、旧代际残留不跨代驻留、旧管理器值原样驻留
///（fetch_max 根因实证）
#[compio::test]
async fn replicate_switch_regenerates_sequence_manager() -> aok::Void {
  let fixture = setup_switch("replicate-switch-regen");
  let stale = fixture
    .aof
    .read_consistency_manager()
    .expect("多日志拓扑构造即建管理器");
  assert_eq!(stale.current_version(), 1, "构造期首代版本 1");
  let (ka, kb) = pick_cross_sublog_keys(&stale);
  let hash_a = stale.key_hash(&physical(&ka));
  let vsr_a = stale.virtual_sublog_idx_of_hash(hash_a);
  let vsr_b = stale.virtual_sublog_idx_of_hash(stale.key_hash(&physical(&kb)));
  // 旧代际残留装载：A 挂接期积累的高草图 + 高前沿（回放侧同一入账口形态）
  stale
    .vsr(vsr_a)
    .update_key_sequence_number(hash_a, STALE_RESIDUE);
  stale.update_virtual_sublog_max_sequence_number(vsr_b, STALE_RESIDUE);

  replicate_to_b(&fixture).await;

  let fresh = fixture
    .aof
    .read_consistency_manager()
    .expect("多日志拓扑管理器在场");
  assert!(!Arc::ptr_eq(&stale, &fresh), "改挂即整体换代");
  assert_eq!(
    fresh.current_version(),
    stale.current_version() + 1,
    "新代际版本 = 前代 + 1（C# CreateOrUpdateKeySequenceManager 语义）"
  );
  assert_eq!(
    fresh.vsr(vsr_a).get_key_sequence_number(hash_a),
    0,
    "旧代际残留草图不得跨代驻留（撤除换代调用即翻红）"
  );
  assert_eq!(fresh.vsr(vsr_b).max(), 0, "旧代际残留前沿不得跨代驻留");
  assert_eq!(
    stale.vsr(vsr_a).get_key_sequence_number(hash_a),
    STALE_RESIDUE,
    "旧代际 fetch_max 单调永驻——换代缺失时读闸污染的来源"
  );
  assert_eq!(stale.vsr(vsr_b).max(), STALE_RESIDUE);
  OK
}

/// 假超时臂：改挂后新读者会话不被旧代际残留草图闸死——B 流重放追平即放行
///
/// 红/绿分界：撤除挂接换代则同一管理器残留高草图把热身读的会话序列号顶至
/// 100_000，B 流前沿（帧长量级）永远追不上，对 kb 的一致读持续假
/// ConsistentReadTimeout；换代后草图归零、热身读 mssn 落 B 流真实位点，
/// 追平即放行读到重放真值。
#[compio::test]
async fn replica_read_after_switch_not_falsely_timed_out() -> aok::Void {
  let fixture = setup_switch("replicate-switch-timeout");
  let stale = fixture
    .aof
    .read_consistency_manager()
    .expect("多日志拓扑构造即建管理器");
  let (ka, kb) = pick_cross_sublog_keys(&stale);
  let hash_a = stale.key_hash(&physical(&ka));
  let vsr_a = stale.virtual_sublog_idx_of_hash(hash_a);
  stale
    .vsr(vsr_a)
    .update_key_sequence_number(hash_a, STALE_RESIDUE);

  replicate_to_b(&fixture).await;
  // B 流真实回放链：kb 记录落盘 + 背景重放应用进存储并推新代际水位
  push_b_and_await_replay(&fixture, &kb, b"vb").await;

  let reader = fixture
    .store
    .new_session()
    .expect("一致读会话")
    .with_read_session_state(Some(Arc::new(ReadSessionState::new(
      fixture
        .aof
        .read_consistency_manager()
        .expect("换代后新管理器在场"),
      2,
      Some(READ_WAIT_TIMEOUT),
    ))));
  let ss = StorageSession::new(reader.enter_batch());
  // 热身读 ka（缺席）：post 以草图推进会话序列号——换代后为 0
  assert_eq!(
    ss.read_string(&ka).await.expect("热身读放行"),
    None,
    "ka 未由 B 流重放"
  );
  assert_eq!(
    ss.read_string(&kb)
      .await
      .expect("B 流已追平，一致读不得假超时"),
    Some(b"vb".to_vec()),
    "放行后读到 B 流重放真值"
  );
  OK
}

/// 假新鲜臂：旧代际残留高前沿不得跨代让未追平的键被陈旧读放行
///
/// 红/绿分界：撤除换代则 kb 槽前沿残留 100_000，热身读后 mssn(0) <
/// cached 高值假新鲜跳等待，本地直写的脏值被一致读放行返回；换代后新代际
/// 前沿归零，B 流未重放 kb 时一致读如实挂起至超时上抛
/// [`wkv::Error::ConsistentReadTimeout`]（拒脏读，与 C# 新挂接等待回放追平
/// 同语义）。
#[compio::test]
async fn replica_read_after_switch_not_released_stale() -> aok::Void {
  let fixture = setup_switch("replicate-switch-stale");
  let stale = fixture
    .aof
    .read_consistency_manager()
    .expect("多日志拓扑构造即建管理器");
  let (ka, kb) = pick_cross_sublog_keys(&stale);
  let vsr_b = stale.virtual_sublog_idx_of_hash(stale.key_hash(&physical(&kb)));
  stale.update_virtual_sublog_max_sequence_number(vsr_b, STALE_RESIDUE);
  // 脏值直写存储（不经重放链——B 流未追平的在场前提）
  write_string(&fixture, &kb, b"dirty").await;

  replicate_to_b(&fixture).await;

  let reader = fixture
    .store
    .new_session()
    .expect("一致读会话")
    .with_read_session_state(Some(Arc::new(ReadSessionState::new(
      fixture
        .aof
        .read_consistency_manager()
        .expect("换代后新管理器在场"),
      2,
      Some(READ_WAIT_TIMEOUT),
    ))));
  let ss = StorageSession::new(reader.enter_batch());
  assert_eq!(
    ss.read_string(&ka).await.expect("热身读放行"),
    None,
    "首读建立前驱子日志"
  );
  let err = ss
    .read_string(&kb)
    .await
    .expect_err("B 流未重放 kb，换代后一致读须等待而非放行脏值");
  assert!(
    matches!(err, Error::ConsistentReadTimeout),
    "一致读等待语义：{err:?}（撤除换代即假新鲜返回脏值翻红）"
  );
  OK
}
