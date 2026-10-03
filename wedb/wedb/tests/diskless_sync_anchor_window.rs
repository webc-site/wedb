//! 无盘全量同步快照锚定集成测试（扫描前 tail 作锚，授予位点即锚、锚后记录恰应用一次）
//!
//! 对标 C# diskless 位点锚定链：
//! - libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/
//!   ReplicationSnapshotIterator.cs:SnapshotIteratorManager（构造期
//!   CheckpointCoveredAddress = Log.TailAddress，先于流式快照）
//! - libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/
//!   ReplicaSyncSession.cs:BeginAofSyncAsync（fullSync 时授予
//!   checkpointCoveredAofAddress，AOF 恰从锚续推）
//!
//! 缺口反证基线：无锚时授予位点为协商 sync_start（= 日志起点 0），锚前窗口
//! 记录在快照终值之上被重放一遍。锚定后快照已含锚前效果、AOF 恰从锚续推，
//! 副本状态与主端严格一致。
//!
//! 记录形态口径（字符串写侧漏斗删净增量重放臂后的现状）：主存字符串的写侧
//! 条目恒为终值 StoreUpsert / 墓碑 StoreDelete 加 TTL 旁路 StoreRMW(PEXPIREAT
//! 绝对毫秒 | PERSIST)，INCR/APPEND 一类增量语义 StoreRMW 臂已整族删除（遇该
//! 形态条目回放显式失败，见 aof_processor.rs:store_rmw），故终值条目对锚前
//! 重放天然收敛。本测试因此把「锚」的不变量落在授予位点的直断言上
//!（sync_from == 扫描前锚，非 0），锚后续推记录再以「值 + TTL 与主端一致」
//! 把关恰应用一次；仍非幂等的集合增量（ObjectStoreRMW 的 ReplayInput 载荷）
//! 由 wnode 对象存重放域测试覆盖。

#[path = "common/replica_host.rs"]
mod replica_host;

use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use wedb_test::{
  diskless_provider::diskless_provider,
  node_storage::{NodeStorage, open_node},
};

#[path = "common/diskless_sync_kick.rs"]
mod diskless_sync_kick;
use diskless_sync_kick::try_full_sync;

#[path = "common/replay_rig.rs"]
mod replay_rig;
use replay_rig::ReplayRig;
use waof::AofEntryType;
use wbase::{
  convert::{TICKS_PER_SECOND, expire_at_milliseconds_to_ticks},
  time::now_ms,
};
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wedb::server::worker::NodeRole;
use wkv::WedbStore;
use wnode::{
  ReplayInput,
  aof::{
    garnet_log::{GarnetLog, RecordShape},
    replay_input::EMPTY_REPLAY_INPUT_BYTES,
  },
  storage::session::storage_session::StorageSession,
};
use wresp::command::RespCommand;
use wtest_base::wait_for;
use wval::{KeyTag, NamespaceDbCodec};

const CTR_KEY: &[u8] = b"anchor:ctr";
const APP_KEY: &[u8] = b"anchor:app";

/// 物理键编码（AOF 条目与存储统一 [NsVarint][DbVarint][KeyTag][用户键]）
fn physical(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// 终值 StoreUpsert 条目（写侧字符串值写的唯一形态：整值随条目搬运、input 空）
fn upsert_entry(key: &[u8], value: &[u8]) -> Vec<u8> {
  entry_bytes(
    AofEntryType::StoreUpsert,
    key,
    value,
    &EMPTY_REPLAY_INPUT_BYTES,
  )
}

/// TTL 旁路 StoreRMW 条目（写侧唯一 TTL 事件源恒 Pexpireat + 绝对 .NET Ticks）
fn pexpireat_entry(key: &[u8], expire_at_ticks: i64) -> Vec<u8> {
  entry_bytes(
    AofEntryType::StoreRMW,
    key,
    &[],
    &serialize_input(ReplayInput {
      cmd: RespCommand::Pexpireat,
      flags: 0,
      sub_id: 0,
      obj_type: 0,
      arg1: expire_at_ticks,
      arg2: 0,
      arg3: 0,
      args: Vec::new(),
    }),
  )
}

/// ReplayInput 序列化
fn serialize_input(input: ReplayInput) -> Vec<u8> {
  let mut bytes = Vec::new();
  input.serialize(&mut bytes);
  bytes
}

/// 条目编码（RecordShape 统一形态，经临时 GarnetLog 产出条目字节）
fn entry_bytes(op_type: AofEntryType, key: &[u8], value: &[u8], input: &[u8]) -> Vec<u8> {
  let options = RuntimeServerOptions::default();
  let (_dirs, backends) = wnode_test::test_sublogs("anchor_entry", 1);
  let log = GarnetLog::new(&options, backends, None).expect("构造 GarnetLog");
  log
    .enqueue(&RecordShape::new(
      op_type,
      0,
      1,
      &physical(key),
      value,
      input,
    ))
    .expect("条目入队");
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

const PRIMARY_ID: u128 = 1;
const REPLICA_ID: u128 = 2;

/// 主端锚前窗口预置：store 置终值 + WAL 落该终值的写侧条目
/// （CTR 五次累加的逐次终值 StoreUpsert + APP 两段追加的终值 StoreUpsert；
/// 快照扫到末条终值，无锚时该窗口被 AOF 全量重放）
async fn seed_primary_window(node: &NodeStorage) {
  {
    let session = node.store.new_session().unwrap();
    let batch = session.enter_batch();
    let storage = StorageSession::new(batch);
    storage.upsert_string(CTR_KEY, b"5").await.unwrap();
    storage.upsert_string(APP_KEY, b"abcd").await.unwrap();
  }
  // 主端 enqueue 直推 AOF 条目字节（WalLog 记录层自加帧头，推流泵
  // next_frame 重建完整帧，副本 enqueue_raw 拆头校验落盘）
  for value in [b"1", b"2", b"3", b"4", b"5"] {
    node.wal.enqueue(&upsert_entry(CTR_KEY, value)).unwrap();
  }
  node.wal.enqueue(&upsert_entry(APP_KEY, b"ab")).unwrap();
  node.wal.enqueue(&upsert_entry(APP_KEY, b"abcd")).unwrap();
}

/// 存储读取（重放应用闭环断言面）
async fn read_string(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let session = store.new_session().expect("read session");
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  storage.read_string(key).await.expect("read string")
}

/// TTL 记录读取（绝对 .NET Ticks，None = 无 TTL；TTL 旁路条目应用断言面）
async fn read_ttl(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<i64> {
  let session = store.new_session().expect("ttl session");
  session.ttl_of(key).await.expect("read ttl")
}

/// 锚定闭环：授予位点恰为扫描前锚（锚前窗口不再重放），锚后续推的写侧真实
/// 形态记录（终值 StoreUpsert + 绝对毫秒 Pexpireat）在副本恰应用一次，
/// 副本值与 TTL 均与主端严格一致
#[compio::test]
async fn diskless_sync_anchor_prevents_double_apply() {
  // ===== 主端：锚前窗口预置（store 终值 + WAL 终值条目），锚 = 预写后尾
  let source = open_node("anchor_source");
  let provider_p = diskless_provider(
    &source,
    PRIMARY_ID,
    7000,
    NodeRole::Primary,
    PRIMARY_ID,
    false,
  );
  seed_primary_window(&source).await;
  let anchor = source.wal.tail_address() as i64;
  assert!(anchor > 0, "锚前窗口写入后主端日志必须非空");

  // ===== 副本：真实回放装配（ReplayAssets + single_log_aof 覆盖同一 wal；
  // 装配束单源见 common/replay_rig.rs——恢复门控随装配束先行就位，宿主
  // 监听在同步发起前无连接到达，门控时序无观测差）
  let ReplayRig {
    node: replica,
    provider: _provider_r,
    rm: rm_r,
    server,
    addr: replica_addr,
  } = replay_rig::replay_rig(
    "anchor_replica",
    REPLICA_ID,
    PRIMARY_ID,
    7001,
    false,
    NonZeroUsize::new(1),
  );

  let rm_p = provider_p.replication_manager().unwrap();
  assert_ne!(
    rm_r.primary_repl_id(),
    rm_p.primary_repl_id(),
    "独立节点复制 ID 初值必不同"
  );

  // ===== 发起全量同步（无检查点历史 + 副本零位点 → FullResync）
  let (sync_from, assets) = try_full_sync(
    &provider_p,
    &source,
    &replica_addr,
    PRIMARY_ID,
    REPLICA_ID,
    &rm_r,
    None,
  )
  .await;

  // 授予位点 = 快照覆盖锚（扫描前尾），非协商 sync_start（= 0）
  assert_eq!(
    sync_from.get(0),
    Some(anchor),
    "FullResync 授予位点必须是扫描前锚（快照已含锚前效果，AOF 恰从锚续推）"
  );

  // ===== 续推衔接：主端锚后追加写侧真实形态记录，副本恰应用一次
  {
    let session = source.store.new_session().unwrap();
    let batch = session.enter_batch();
    let storage = StorageSession::new(batch);
    storage.upsert_string(CTR_KEY, b"7").await.unwrap();
  }
  // 两条终值条目按序推入（漏应用或半应用即停在 "6"）
  for value in [b"6", b"7"] {
    source.wal.enqueue(&upsert_entry(CTR_KEY, value)).unwrap();
  }
  // TTL 旁路条目：主端 TTL 在快照之后落同一绝对 ticks，副本的 TTL 记录
  // 只可能来自该条目（快照未含），据此断言锚后条目确已应用
  let expire_at_ticks = expire_at_milliseconds_to_ticks(now_ms() as i64 + 300_000);
  source
    .wal
    .enqueue(&pexpireat_entry(APP_KEY, expire_at_ticks))
    .unwrap();
  {
    let session = source.store.new_session().unwrap();
    let batch = session.enter_batch();
    let storage = StorageSession::new_readonly(batch);
    storage
      .expire_at_ticks(APP_KEY, expire_at_ticks)
      .await
      .unwrap();
  }
  let new_tail = source.wal.tail_address() as i64;
  let _ = assets.pump.sync_backlog(&source.wal).await;

  // 副本位点追平主端尾（记录帧衔接 + 背景重放 applied 回推）
  let caught_up = wait_for(
    || rm_r.get_current_replication_offset().get(0) == Some(new_tail),
    Duration::from_secs(5),
  )
  .await;
  assert!(caught_up, "副本复制位点必须追平主端尾");

  // 锚后记录恰应用一次：副本计数器 = 主端 7（漏应用即停在 6），
  // APPEND 串 = 主端 abcd，APP 键 TTL 与主端同绝对 ticks
  let replica_ctr = read_string(&replica.store, CTR_KEY).await.unwrap();
  let replica_app = read_string(&replica.store, APP_KEY).await.unwrap();
  let primary_ctr = read_string(&source.store, CTR_KEY).await.unwrap();
  let primary_app = read_string(&source.store, APP_KEY).await.unwrap();
  let replica_ttl = read_ttl(&replica.store, APP_KEY).await;
  let primary_ttl = read_ttl(&source.store, APP_KEY).await;
  assert_eq!(
    replica_ctr, primary_ctr,
    "计数器必须与主端一致（终值条目按序应用）"
  );
  assert_eq!(replica_ctr, b"7", "计数器终值必须恰为锚后两条终值条目");
  assert_eq!(
    replica_app, primary_app,
    "APPEND 串必须与主端一致（无重复拼接）"
  );
  assert_eq!(replica_app, b"abcd");
  assert!(
    primary_ttl.is_some_and(|p| replica_ttl.is_some_and(|r| (r - p).abs() < TICKS_PER_SECOND)),
    "锚后 Pexpireat 条目须在副本恰应用一次（绝对 ticks 与主端一致），\
     实际 主 {primary_ttl:?} 副本 {replica_ttl:?}"
  );

  server.dispose();
}
