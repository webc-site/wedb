#![recursion_limit = "256"]
//! 磁盘臂部分重同步 recover 钳制集成测试（票
//! wedb-repl-diskbased-partial-resync-skips-replica-recover-clamp）
//!
//! 对标 C# 真源：
//! - garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/
//!   ReplicaSyncSession.cs:178 ExecuteClusterBeginReplicaRecover 无条件往返
//!   （部分重同步形态不发快照帧、recoverStoreFromToken=false + replayAOFMap）
//! - garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:
//!   TryReplicaDiskbasedRecovery 的 `if (replayAOFMap > 0)` ReplayAOF 臂 +
//!   Log.Initialize 钳位
//!
//! 缺陷回归面：磁盘臂 PartialResync 缺 recover 钳制往返时，断连窗口的
//! 「已落盘未应用」残留段 [applied, tail) 永久漏应用——本套件以真实装配
//! （真 wal + 真存储引擎 + 真重放应用链 + 真恢复闭环，无假 mock）锁死：
//! 残留段补应用终态对账、钳位单轮收敛（无 divergent 断流）、授予位点落后
//! 应用位点的发散卫窗、全量导入臂拒非零掩码契约。
//!
//! 装配形态对标 replica_background_replay.rs 同款生产形（single_log_aof +
//! ReplayAssets 注入 rm + 接收会话真推流）。

#[path = "common/replica_wal_fixture.rs"]
mod replica_wal_fixture;
use replica_wal_fixture::replica_wal_fixture;

#[path = "common/replica_entry_frames.rs"]
mod replica_entry_frames;
use replica_entry_frames::{record_frame, source_entries};

#[path = "common/replica_topology.rs"]
mod replica_topology_core;
use std::{sync::Arc, time::Duration};

use aok::{OK, Void};
use compio::time::sleep;
use waof::{AofAddress, WalLog};
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wedb::server::{
  cluster_provider::ClusterProvider,
  replication::{
    checkpoint_entry::CheckpointEntry,
    cluster_replication_session::{AppendLogOutcome, ClusterReplicationSession},
    error::ReplicationError,
    recovery_status::RecoveryStatus,
    replica_diskbased_sync::{ReplicaRecoverRequest, try_replica_diskbased_recovery},
    replica_replay_task::ReplayAssets,
    replication_manager::ReplicationManager,
  },
};
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, aof::waof_sublog::single_log_aof, primary_tasks::PrimaryTasks,
  storage::session::storage_session::StorageSession,
};
use wtest_base::{open_test_store, wait_for};

const PRIMARY_ID: u128 = 0x0C1A_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0C1A_0000_0000_0000_0000_0000_0000_0002;

/// 背景重放线程退场余量（dispose 后线程至多一个空转周期 5ms 收敛；200ms
/// 为 40 倍余量。残留段入盘后的位点前提断言兜底：线程若未退场而是应用了
/// 残留段，前提断言即响红，绝不静默误绿）
const REPLAY_THREAD_DRAIN: Duration = Duration::from_millis(200);

/// 副本复制域装配：副本角色 provider + rm 挂重放资产 + 接收会话
///（生产装配形，对标 wire_replication_data_plane 注入面）
struct ClampFixture {
  _dir: tempfile::TempDir,
  _store_dir: tempfile::TempDir,
  provider: Arc<ClusterProvider>,
  rm: Arc<ReplicationManager>,
  wal: Arc<WalLog<SegmentedDevice>>,
  store: Arc<WedbStore<SegmentedDevice>>,
  session: ClusterReplicationSession<SegmentedDevice>,
}

fn setup_replica() -> ClampFixture {
  let (provider, rm, dir, wal) = replica_wal_fixture(REPLICA_ID, PRIMARY_ID);
  let aof =
    single_log_aof(wal.clone(), &RuntimeServerOptions::default()).expect("装配 single_log_aof");
  let (store_dir, store) = open_test_store("recover-clamp").expect("store");
  rm.set_replay_assets(Some(Arc::new(ReplayAssets::new(
    Arc::clone(&aof),
    Arc::clone(&store),
    None,
    None,
  ))));
  provider.set_aof_replay_max_lag_bytes(-1);
  provider.set_primary_tasks(Arc::new(PrimaryTasks::default()));
  provider.set_wal(wal.clone());

  let session = ClusterReplicationSession::new(provider.clone(), wal.clone(), None);
  ClampFixture {
    _dir: dir,
    _store_dir: store_dir,
    provider,
    rm,
    wal,
    store,
    session,
  }
}

/// 初始化帧握手 + 推流一条记录帧（真会话真落盘真重放），返回帧尾地址
fn attach_and_push(fixture: &ClampFixture, entry: &[u8]) -> i64 {
  fixture
    .session
    .process_append_log(PRIMARY_ID, 0, -1, -1, -1, &[])
    .expect("init handshake");
  push_record(fixture, entry)
}

/// 推流一条记录帧（稳态衔接：current = 当前尾位），返回帧尾地址
fn push_record(fixture: &ClampFixture, entry: &[u8]) -> i64 {
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

/// 存储读取（补应用终态对账断言面）
async fn read_string(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let session = store.new_session().expect("read session");
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  storage.read_string(key).await.expect("read string")
}

/// 断链预态装载（默认生产形态：磁盘复制 + maxLag=-1 + 背景重放）：
/// 1. 前段 k1/k2 经真会话推流 + 背景重放应用进存储，applied 位点追平 M；
/// 2. dispose 断链终止背景重放线程（生产断连处置同点）；
/// 3. 残留段帧经会话同一落盘原语 enqueue_raw 保真入盘——「已落盘未应用」
///    段 [M, N)：lock(v-applied) → k3 → k4 → lock(v-final)，位点前提断言
///    钉死无任何人应用。
///
/// 返回 (applied=M, k3 帧尾 a3, 残留段尾 N)。
async fn stage_applied_then_residual(fixture: &mut ClampFixture) -> (i64, i64, i64) {
  let next1 = attach_and_push(fixture, &source_entries("recover_clamp_src", b"k1", b"v1"));
  assert!(
    wait_for(
      || fixture.rm.get_replication_offset(0) >= next1,
      Duration::from_secs(5),
    )
    .await,
    "前段应用位点追平帧尾"
  );
  let applied = push_record(fixture, &source_entries("recover_clamp_src", b"k2", b"v2"));
  assert!(
    wait_for(
      || fixture.rm.get_replication_offset(0) >= applied,
      Duration::from_secs(5),
    )
    .await,
    "applied 位点追平（M 定格于前段帧尾）"
  );

  // 断链：dispose 终止会话自持代驱动仓与背景重放线程（生产断连处置同点）
  fixture.session.dispose();
  sleep(REPLAY_THREAD_DRAIN).await;

  // 残留段逐帧保真入盘（与 process_primary_stream 同一落盘原语）
  let mut a3 = 0i64;
  for (key, value) in [
    (&b"lock"[..], &b"v-applied"[..]),
    (&b"k3"[..], &b"v3"[..]),
    (&b"k4"[..], &b"v4"[..]),
    (&b"lock"[..], &b"v-final"[..]),
  ] {
    let frame = record_frame(&source_entries("recover_clamp_src", key, value));
    let landed = fixture.wal.enqueue_raw(&frame).expect("残留帧保真入盘");
    if key == b"k3" {
      a3 = landed as i64 + frame.len() as i64;
    }
  }
  let tail = fixture.wal.tail_address() as i64;

  // 前提断言：残留段已落盘、应用位点钉死在 M（断链后无任何应用链推进）
  assert_eq!(fixture.wal.tail_address() as i64, tail);
  assert!(
    tail > a3 && a3 > applied,
    "残留段区间 [M, N) 非空前提: applied={applied}, a3={a3}, tail={tail}"
  );
  assert_eq!(
    fixture.rm.get_replication_offset(0),
    applied,
    "残留段「已落盘未应用」前提：applied 位点未推进"
  );
  (applied, a3, tail)
}

/// 部分重同步恢复请求（生产载荷形：recover_store_from_token=false + 掩码 +
/// 主端覆盖 begin + 协商授予位点）
fn partial_recover_request(begin: i64, granted: i64, replay_aof_map: u64) -> ReplicaRecoverRequest {
  ReplicaRecoverRequest {
    recover_store_from_token: false,
    replay_aof_map,
    primary_repl_id: "clamp-primary-repl-id".to_string(),
    remote_entry: CheckpointEntry::with_sublogs(1),
    begin_address: AofAddress::create(1, begin),
    tail_address: AofAddress::create(1, granted),
  }
}

/// 核心回归（P1）：断连残留段 [M, N) 重挂后由 recover 钳制臂补应用进存储，
/// 终态双侧对账（存储含全部条目 + 位点连续推进至授予位点无跳变）
///
/// 修复前红相：PartialResync 臂无任何恢复往返，残留段不进存储、位点批末
/// 直接 M 跳 N（假消费推进），k3/k4/锁终写永久漏应用。
#[compio::test]
async fn residual_segment_replayed_and_offset_converges_on_partial_recover() -> Void {
  let mut fixture = setup_replica();
  let (_applied, _a3, tail) = stage_applied_then_residual(&mut fixture).await;

  // 生产 attach 前置位（attach 链全程持 ClusterReplicate，恢复状态矩阵合法）
  *fixture.rm.current_recovery_status.write() = RecoveryStatus::ClusterReplicate;

  let offset = try_replica_diskbased_recovery(
    &fixture.provider,
    &fixture.rm,
    &partial_recover_request(
      fixture.wal.begin_address() as i64,
      tail,
      1, // replayAOFMap 位 0 置位（单物理日志）
    ),
  )
  .await
  .expect("钳制往返应成功");
  assert_eq!(offset, AofAddress::create(1, tail), "回传位点 = 授予位点");

  // 终态对账（存储侧）：残留段全部条目已应用，前段保持
  assert_eq!(
    read_string(&fixture.store, b"k1").await.as_deref(),
    Some(b"v1".as_slice()),
    "前段条目保持"
  );
  assert_eq!(
    read_string(&fixture.store, b"k3").await.as_deref(),
    Some(b"v3".as_slice()),
    "残留段条目 k3 已补应用（修复前永久缺失）"
  );
  assert_eq!(
    read_string(&fixture.store, b"k4").await.as_deref(),
    Some(b"v4".as_slice()),
    "残留段条目 k4 已补应用（修复前永久缺失）"
  );
  // 非幂等写序锁：同键 v-applied → v-final 两代写，残留段漏应用即停留旧值
  assert_eq!(
    read_string(&fixture.store, b"lock").await.as_deref(),
    Some(b"v-final".as_slice()),
    "写序锁终值收敛：残留段漏应用即停留 v-applied"
  );

  // 终态对账（位点侧）：applied 连续推进至授予位点（修复前 M 跳 N 假消费）
  assert_eq!(
    fixture.rm.get_replication_offset(0),
    tail,
    "应用位点 = 授予位点（连续推进，无跳变）"
  );
  assert_eq!(
    fixture.wal.tail_address() as i64,
    tail,
    "本地日志尾对齐授予位点（C# Log.Initialize 钳位）"
  );
  assert_eq!(
    fixture.wal.committed_until_address() as i64,
    tail,
    "提交边界随钳位对齐"
  );
  // 部分臂维持本地检查点仓现状（C# cEntry 保持副本自有条目；本预态无条目）
  assert_eq!(
    fixture.rm.checkpoint_store.read().entry_count(),
    0,
    "部分臂不采纳远端检查点条目"
  );
  OK
}

/// 钳位场景单轮收敛：授予位点低于副本日志尾（主端 committed/repl_offset2
/// 钳制形态）时，残留段只补应用到授予位点、日志尾钳位对齐，重挂流在钳位
/// 点严格衔接不触发 divergent 断流
#[compio::test]
async fn clamped_grant_converges_single_round_without_divergent_loop() -> Void {
  let mut fixture = setup_replica();
  let (applied, a3, tail) = stage_applied_then_residual(&mut fixture).await;
  // 授予位点钳在残留段中部（k3 帧尾）：其后段被钳位截弃，主端此后自钳位点重推
  assert!(a3 > applied && a3 < tail, "钳位点落在残留段中部");

  *fixture.rm.current_recovery_status.write() = RecoveryStatus::ClusterReplicate;
  let offset = try_replica_diskbased_recovery(
    &fixture.provider,
    &fixture.rm,
    &partial_recover_request(fixture.wal.begin_address() as i64, a3, 1),
  )
  .await
  .expect("钳位往返应单轮成功");
  assert_eq!(offset, AofAddress::create(1, a3), "回传钳位后位点");

  assert_eq!(
    read_string(&fixture.store, b"k3").await.as_deref(),
    Some(b"v3".as_slice()),
    "钳位点前残留段已补应用"
  );
  assert_eq!(
    read_string(&fixture.store, b"k4").await,
    None,
    "钳位点后残段被截弃（主端自钳位点重推，不重复应用）"
  );
  assert_eq!(
    fixture.wal.tail_address() as i64,
    a3,
    "日志尾钳位对齐（后续重推帧与本地尾严格衔接的前提）"
  );
  assert_eq!(
    fixture.rm.get_replication_offset(0),
    a3,
    "应用位点收敛至钳位点"
  );

  // 重挂衔接：init 帧重注册重放驱动后，主端自钳位点的记录帧严格衔接
  //（修复前主端流自协商位点推流、本地尾仍是旧尾，首帧必命中 divergent
  // 致命断流 → 重连风暴）；此处自钳位点续推帧成功即单轮收敛实证
  fixture.rm.reset_replica_replay_driver_store();
  let outcome = fixture
    .session
    .process_append_log(PRIMARY_ID, 0, -1, -1, -1, &[])
    .expect("钳位后重挂初始化成功");
  assert_eq!(outcome, AppendLogOutcome::Initialized);
  let frame = record_frame(&source_entries("recover_clamp_src", b"k5", b"v5"));
  let current = fixture.wal.tail_address() as i64;
  let next = current + frame.len() as i64;
  let outcome = fixture
    .session
    .process_append_log(PRIMARY_ID, 0, current, current, next, &frame)
    .expect("钳位点续推帧严格衔接（无 divergent 断流）");
  assert_eq!(outcome, AppendLogOutcome::Record);
  assert!(
    wait_for(
      || fixture.rm.get_replication_offset(0) >= next,
      Duration::from_secs(5),
    )
    .await,
    "重放链自钳位点续推应用"
  );
  assert_eq!(
    read_string(&fixture.store, b"k5").await.as_deref(),
    Some(b"v5".as_slice()),
    "重推条目照常应用"
  );
  OK
}

/// 发散卫窗：授予位点落后应用位点（副本把主端未提交段应用过了头）时部分
/// 接续不可为——显式失败交重同步收敛，绝不钳位谎报（静默钳位会把已应用
/// 段重放两遍，非幂等写即发散）
#[compio::test]
async fn granted_behind_applied_fails_loud_for_full_resync() -> Void {
  let mut fixture = setup_replica();
  let (applied, _a3, _tail) = stage_applied_then_residual(&mut fixture).await;
  *fixture.rm.current_recovery_status.write() = RecoveryStatus::ClusterReplicate;

  let err = try_replica_diskbased_recovery(
    &fixture.provider,
    &fixture.rm,
    &partial_recover_request(fixture.wal.begin_address() as i64, applied - 1, 1),
  )
  .await
  .expect_err("授予位点落后应用位点必须显式失败");
  assert!(
    matches!(&err, ReplicationError::HistoryGap(m) if m.contains("divergent history")),
    "失败文案应指明历史发散需全量: {err}"
  );
  OK
}

/// 全量导入臂拒非零掩码契约保持：rust 主端全量链恒发 0，
/// recover_store_from_token=true 携非零 replayAOFMap 即协议违约
#[compio::test]
async fn full_import_arm_rejects_nonzero_mask() -> Void {
  let fixture = setup_replica();
  *fixture.rm.current_recovery_status.write() = RecoveryStatus::ClusterReplicate;

  let request = ReplicaRecoverRequest {
    recover_store_from_token: true,
    replay_aof_map: 1,
    primary_repl_id: "clamp-primary-repl-id".to_string(),
    remote_entry: CheckpointEntry::with_sublogs(1),
    begin_address: AofAddress::create(1, 0),
    tail_address: AofAddress::create(1, 0),
  };
  let err = try_replica_diskbased_recovery(&fixture.provider, &fixture.rm, &request)
    .await
    .expect_err("全量导入臂非零掩码必须拒绝");
  assert!(
    matches!(&err, ReplicationError::Protocol(m) if m.contains("replayAOFMap is not expected")),
    "协议违约文案保持: {err}"
  );
  OK
}
