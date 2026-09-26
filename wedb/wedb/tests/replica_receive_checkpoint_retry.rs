//! 从库快照接收「网络中断 → 新一轮全量从头重推」判别用例
//!
//! 对标 C# 原型（garnet/libs/cluster/Server/Replication/ReplicaOps/
//! DiskbasedReplication/ReceiveCheckpointHandler.cs:64-65 与 :161-162 注释
//! "On retry, this may reopen an existing file from a previous failed
//! attempt. This is safe because chunks are streamed from the start,
//! overwriting any partial data."；ReplicaDiskbasedSync.cs:161 每轮 attach
//! `recvCheckpointHandler = new(...)`）：C# 从库从不因上一轮失败把设备闭锁成
//! 拒绝后续一切快照的损坏态——rust 的 device_contaminated 若不随新会话净态，
//! 一次网络抖动即令从库节点永久报废。
//!
//! 三面判别：
//! 1. 中断重连后新一轮全量分块必须从头收下并覆盖旧半写，导入闭环换入新引擎、
//!    读到主端键（修复前必红：重连后首帧即被自家入口检查拒收）；
//! 2. 未成功恢复的半写仍须留在本层管理面屏障上（本地 flush / take_checkpoint
//!    拒服务），屏障的唯一解除点是一次成功的全量恢复收口；
//! 3. 本轮 StoreHlog 段写失败仍须关掉本轮接收（半截覆盖不得继续收帧喂导入面，
//!    防护不得随闭锁一并删没）。
//!
//! 全用例走真帧接收臂（CLUSTER SNAPSHOT_DATA / BEGIN_REPLICA_RECOVER 直投
//! 会话）与真设备落盘，无替身。

#[path = "common/cluster_consumer.rs"]
mod cluster_cc;

use std::{fs, path::PathBuf, sync::Arc};

use cluster_cc::cluster_consumer;
use compio::runtime::Runtime;
use waof::{AofAddress, WalConfig, WalLog};
use wcpr::{CheckpointMeta, CheckpointType};
use wdev::{Device, SegmentedDevice};
use wedb::server::{
  cluster_config::ClusterConfig,
  cluster_manager::ClusterManager,
  cluster_provider::ClusterProvider,
  replication::{
    checkpoint_entry::{CheckpointEntry, CheckpointFileType, CheckpointMetadata},
    recovery_status::RecoveryStatus,
  },
  worker::{LocalWorkerSpec, NodeRole},
};
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer, database::checkpoint_version,
  storage::session::storage_session::StorageSession,
};
use wtest_base::{resp_frame, test_store_config};

/// 测试节点身份
const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00A1;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_00B2;

/// 快照分块尺寸（扇区整数倍，与主端发送面切分同构）
const CHUNK: usize = 1 << 17;

/// 单槽位点 span（C# AofAddress.Span：8B LE）
fn aof_span(address: i64) -> Vec<u8> {
  address.to_le_bytes().to_vec()
}

/// 独立存储节点（db 文件 + 检查点目录 + wal）
struct NodeStorage {
  _dir: tempfile::TempDir,
  store: Arc<WedbStore<SegmentedDevice>>,
  checkpoint_dir: PathBuf,
  wal: Arc<WalLog<SegmentedDevice>>,
}

fn open_node(tag: &str) -> NodeStorage {
  let dir = tempfile::tempdir().unwrap();
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join(format!("{tag}.db"))).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let wal_device =
    Arc::new(SegmentedDevice::single_file(dir.path().join(format!("{tag}.wal"))).unwrap());
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default()).unwrap());
  let checkpoint_dir = dir.path().join("checkpoints");
  fs::create_dir_all(&checkpoint_dir).unwrap();
  NodeStorage {
    _dir: dir,
    store,
    checkpoint_dir,
    wal,
  }
}

/// 写 string 键
async fn put_str(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8], value: &[u8]) {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  storage.upsert_string(key, value).await.unwrap();
}

/// 读 string 键
async fn read_str(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  storage.read_string(key).await.unwrap()
}

/// 角色 provider（对齐宿主装配：rm/store/wal/checkpoint_dir 全接线）
fn wired_provider(
  node: &NodeStorage,
  node_id: u128,
  port: i32,
  role: NodeRole,
) -> Arc<ClusterProvider> {
  let provider = ClusterProvider::new();
  provider.initialize_replication_manager(1, None, false);
  let cm = Arc::new(ClusterManager::new(Arc::clone(&provider)));
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id,
    address: "127.0.0.1",
    port,
    config_epoch: 1,
    role,
    replica_of_node_id: if role == NodeRole::Replica {
      Some(PRIMARY_ID)
    } else {
      None
    },
    hostname: None,
  });
  *cm.current_config.write() = config;
  *provider.cluster_manager.write() = Some(cm);
  provider.set_store(Arc::clone(&node.store));
  provider.set_checkpoint_dir(node.checkpoint_dir.clone());
  provider.set_wal(Arc::clone(&node.wal));
  provider
}

/// 慢命令往返：同步段消费，挂起慢路径时 block_on 驱动应答
fn drive(rt: &Runtime, c: &mut RespSessionConsumer, frame_bytes: &[u8]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(frame_bytes);
  c.return_recv_scratch(scratch);
  let mut out = Vec::new();
  let remaining = c.try_consume_messages_into(&mut out);
  assert_eq!(remaining, Some(0), "帧应被完整消费");
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async {
      out.extend_from_slice(&slow.resolve().await);
    });
  }
  out
}

/// SNAPSHOT_DATA 单帧发射（不做应答判定，供失败臂取应答原文）
fn snapshot_data(
  token: u128,
  file_type: CheckpointFileType,
  start_address: i64,
  data: &[u8],
) -> Vec<u8> {
  let type_str = (file_type as i64).to_string();
  let start_str = start_address.to_string();
  resp_frame(&[
    b"CLUSTER",
    b"SNAPSHOT_DATA",
    &token.to_le_bytes(),
    type_str.as_bytes(),
    start_str.as_bytes(),
    data,
  ])
}

/// 段流帧发射：按段发 SNAPSHOT_DATA + 空载荷收尾，断言逐帧 +OK
fn emit_file_stream(
  rt: &Runtime,
  consumer: &mut RespSessionConsumer,
  token: u128,
  file_type: CheckpointFileType,
  source: &[u8],
  start_address: u64,
) {
  let mut offset = start_address;
  let end = start_address + source.len() as u64;
  while offset < end {
    let len = CHUNK.min((end - offset) as usize);
    let chunk = &source[(offset - start_address) as usize..][..len];
    let resp = drive(
      rt,
      consumer,
      &snapshot_data(token, file_type, offset as i64, chunk),
    );
    assert_eq!(resp, b"+OK\r\n", "文件段应答");
    offset += len as u64;
  }
  let resp = drive(
    rt,
    consumer,
    &snapshot_data(token, file_type, offset as i64, &[]),
  );
  assert_eq!(resp, b"+OK\r\n", "EOF 哨兵应答");
}

/// 主端拍检查点并登记复制域条目（发送面同源）
async fn take_primary_checkpoint(
  node: &NodeStorage,
  provider: &Arc<ClusterProvider>,
) -> (u128, CheckpointEntry, i64) {
  let meta = wcpr::create_checkpoint(
    node.store.as_ref(),
    &node.store.ckpt_gate,
    &node.checkpoint_dir,
    CheckpointType::FoldOver,
  )
  .await
  .unwrap();
  let token = meta.token;
  let covered = 0;
  let mut metadata = CheckpointMetadata::new(1);
  metadata.store_version = checkpoint_version(token);
  metadata.store_hlog_token = token;
  metadata.store_index_token = token;
  metadata.store_checkpoint_covered_aof_address = AofAddress::create(1, covered);
  metadata.store_primary_repl_id = Some(provider.replication_manager().unwrap().primary_repl_id());
  let entry = CheckpointEntry::new(metadata);
  provider
    .replication_manager()
    .unwrap()
    .add_checkpoint_entry(entry.clone(), true);
  (token, entry, covered)
}

/// 主端检查点文件集快照（发送面同源读取口径）
struct PrimaryCheckpointFiles {
  hlog: Vec<u8>,
  hlog_start: u64,
  index: Vec<u8>,
  meta: Vec<u8>,
}

async fn read_primary_checkpoint_files(node: &NodeStorage, token: u128) -> PrimaryCheckpointFiles {
  let meta_bytes = fs::read(node.checkpoint_dir.join(wcpr::meta_filename(token))).unwrap();
  let meta = CheckpointMeta::decode(&meta_bytes).unwrap();
  let sector = node.store.device.sector_size() as u64;
  let start = meta.hlog_meta.begin_address / sector * sector;
  let file_len = node.store.device.get_file_size(0).unwrap();
  let raw_end = file_len
    .max(meta.hlog_meta.flushed_until_address)
    .max(meta.hlog_meta.tail_address);
  let end = raw_end / sector * sector;
  let hlog = node
    .store
    .device
    .read_range(start, (end - start) as usize)
    .await
    .unwrap();
  let index = fs::read(node.checkpoint_dir.join(wcpr::index_filename(token))).unwrap();
  PrimaryCheckpointFiles {
    hlog: hlog[..].to_vec(),
    hlog_start: start,
    index,
    meta: meta_bytes,
  }
}

/// 判别主用例：hlog 半写 → 断链重连（会话初始化单点）→ 新一轮全量从头覆盖
/// 重推必须被收下，导入闭环换入新引擎读到主端键，两面屏障随收口解除
#[test]
fn interrupted_full_sync_retry_reships_from_start_and_imports() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // ===== 主端建库 + 检查点 + 文件集快照
    let primary = open_node("primary");
    let primary_provider = wired_provider(&primary, PRIMARY_ID, 7000, NodeRole::Primary);
    put_str(&primary.store, b"retry_key_a", b"value_a").await;
    put_str(&primary.store, b"retry_key_b", b"value_b").await;
    let (token, entry, covered) = take_primary_checkpoint(&primary, &primary_provider).await;
    let files = read_primary_checkpoint_files(&primary, token).await;
    assert!(!files.hlog.is_empty(), "主端 hlog 段源非空");

    // ===== 副本空库
    let replica = open_node("replica");
    let provider = wired_provider(&replica, REPLICA_ID, 7001, NodeRole::Replica);
    let rm = provider.replication_manager().unwrap();
    let mut consumer = cluster_consumer(&provider);
    let old_store = Arc::clone(&replica.store);

    // ===== 同步轮 1：hlog 首块收下后网络中断（不发 EOF、不发 index/meta）
    let cut = CHUNK.min(files.hlog.len());
    let resp = drive(
      &rt,
      &mut consumer,
      &snapshot_data(
        token,
        CheckpointFileType::StoreHlog,
        files.hlog_start as i64,
        &files.hlog[..cut],
      ),
    );
    assert_eq!(resp, b"+OK\r\n", "轮 1 首块必须收下");
    assert!(rm.is_hlog_dirty(), "hlog 段写必须置脏标记");

    // ===== 断链重连：recover_replication 的会话初始化单点
    provider.reset_recv_checkpoint_handler();
    assert!(
      !rm.is_device_contaminated(),
      "新一轮全量前接收闸门必须净态，否则重连的首块即被自家拒收"
    );
    assert!(
      provider.is_device_contaminated(),
      "未恢复的 hlog 半写须升级到管理面屏障（防护不得删没）"
    );

    // ===== 同步轮 2：主端自起始地址从头重推全部文件集（顺序覆盖轮 1 半写）
    emit_file_stream(
      &rt,
      &mut consumer,
      token,
      CheckpointFileType::StoreHlog,
      &files.hlog,
      files.hlog_start,
    );
    emit_file_stream(
      &rt,
      &mut consumer,
      token,
      CheckpointFileType::StoreIndex,
      &files.index,
      0,
    );
    let resp = drive(
      &rt,
      &mut consumer,
      &snapshot_data(token, CheckpointFileType::StoreSnapshot, -1, &files.meta),
    );
    assert_eq!(resp, b"+OK\r\n", "元数据整包必须收下");

    // ===== 导入：BEGIN_REPLICA_RECOVER 成功收口
    assert!(rm.begin_recovery(RecoveryStatus::ClusterReplicate, false));
    let recover_frame = resp_frame(&[
      b"CLUSTER",
      b"BEGIN_REPLICA_RECOVER",
      b"1",
      b"0",
      b"primary-repl-id-1",
      &entry.to_byte_array(),
      &aof_span(covered),
      &aof_span(covered),
    ]);
    let resp = drive(&rt, &mut consumer, &recover_frame);
    let expected = format!("${}\r\n{covered}\r\n", covered.to_string().len());
    assert_eq!(resp, expected.as_bytes(), "恢复应答为授予位点 bulk string");

    // ===== 覆盖写入闭环断言：引擎置换 + 主端键全量可读
    let new_store = provider.try_store().unwrap();
    assert!(!Arc::ptr_eq(&new_store, &old_store), "导入必须置换在线引擎");
    assert_eq!(
      read_str(&new_store, b"retry_key_a").await.unwrap(),
      b"value_a"
    );
    assert_eq!(
      read_str(&new_store, b"retry_key_b").await.unwrap(),
      b"value_b"
    );

    // ===== 收口即双屏障解除（一次成功的全量恢复是唯一清除点）
    assert!(
      !provider.is_device_contaminated(),
      "成功恢复收口必须解除管理面屏障，否则下一轮抖动重连即永久闭锁"
    );
    assert!(!rm.is_device_contaminated());
    assert!(!rm.is_hlog_dirty());
    assert!(
      provider
        .take_on_demand_checkpoint(provider.last_save_ms())
        .await
        .is_ok_and(|taken| !taken),
      "屏障解除后按需快照面恢复放行"
    );
  });
}

/// 对照面：半写未经成功恢复时，本地 flush / take_checkpoint 管理面仍须被拒；
/// 屏障绝不外溢成对快照接收入口的闭锁
#[test]
fn abandoned_half_write_keeps_management_barrier_but_reopens_receive() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let replica = open_node("replica");
    let provider = wired_provider(&replica, REPLICA_ID, 7001, NodeRole::Replica);
    let rm = provider.replication_manager().unwrap();
    let mut consumer = cluster_consumer(&provider);
    let token = 0xfeed_face_1234_5678_9abc_def0_1122_3344u128;

    // 轮 1：hlog 收下首块即中断
    let resp = drive(
      &rt,
      &mut consumer,
      &snapshot_data(token, CheckpointFileType::StoreHlog, 0, &[0xaa_u8; 4096]),
    );
    assert_eq!(resp, b"+OK\r\n");

    // 重连复位 + 再复位（多轮失败重试）：管理面屏障持续在册
    provider.reset_recv_checkpoint_handler();
    provider.reset_recv_checkpoint_handler();
    assert!(
      provider.is_device_contaminated(),
      "半写未成功恢复时管理面屏障不得因重连而失效"
    );
    let taken = provider
      .take_on_demand_checkpoint(provider.last_save_ms())
      .await;
    assert!(
      taken
        .as_ref()
        .is_err_and(|e| e.contains("contaminated") || e.contains("refusing checkpoint")),
      "屏障在册时按需快照必须被拒: {taken:?}"
    );

    // 屏障不外溢：接收入口照常收下新一轮从头重推的分块
    let resp = drive(
      &rt,
      &mut consumer,
      &snapshot_data(token, CheckpointFileType::StoreHlog, 0, &[0x55u8; 4096]),
    );
    assert_eq!(resp, b"+OK\r\n", "管理面屏障不得闭锁快照接收面");
    let resp = drive(
      &rt,
      &mut consumer,
      &snapshot_data(token, CheckpointFileType::StoreHlog, 0, &[]),
    );
    assert_eq!(resp, b"+OK\r\n", "收尾哨兵同样放行");

    // 半截文件集仍拒绝导入（wcpr「meta 即发布」协议在册）
    assert!(rm.begin_recovery(RecoveryStatus::ClusterReplicate, false));
    let ghost_entry = CheckpointEntry::new(CheckpointMetadata::new(1));
    let resp = drive(
      &rt,
      &mut consumer,
      &resp_frame(&[
        b"CLUSTER",
        b"BEGIN_REPLICA_RECOVER",
        b"1",
        b"0",
        b"primary-repl-id-1",
        &ghost_entry.to_byte_array(),
        &aof_span(0),
        &aof_span(0),
      ]),
    );
    assert!(resp.starts_with(b"-ERR"), "缺 meta 必须拒绝导入: {resp:?}");
    assert!(
      provider.is_device_contaminated(),
      "导入失败的半写须留在管理面屏障上"
    );
  });
}

/// 对照面：本轮 StoreHlog 段写失败即关闭本轮接收（三张入口全拒），
/// 会话复位后新一轮从头重推恢复放行——本轮闸门与跨会话闭锁的分界
#[test]
fn in_session_hlog_write_failure_gates_only_current_attempt() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let replica = open_node("replica");
    let provider = wired_provider(&replica, REPLICA_ID, 7001, NodeRole::Replica);
    let rm = provider.replication_manager().unwrap();
    let mut consumer = cluster_consumer(&provider);
    let token = 0x0123_4567_89ab_cdef_0123_4567_89ab_cdefu128;

    // 正常首块
    let resp = drive(
      &rt,
      &mut consumer,
      &snapshot_data(token, CheckpointFileType::StoreHlog, 0, &[0x11u8; 4096]),
    );
    assert_eq!(resp, b"+OK\r\n");
    assert!(!rm.is_device_contaminated());

    // 非扇区对齐段地址：设备写开口失败 → 本轮 hlog 半写不可信
    let resp = drive(
      &rt,
      &mut consumer,
      &snapshot_data(token, CheckpointFileType::StoreHlog, 1, &[0x22u8; 4096]),
    );
    assert!(resp.starts_with(b"-ERR"), "非对齐 hlog 段必须失败");
    assert!(
      rm.is_device_contaminated(),
      "hlog 段写失败必须关闭本轮接收闸门"
    );

    // 本轮余流三张入口全拒
    for frame in [
      snapshot_data(token, CheckpointFileType::StoreHlog, 4096, &[0x33u8; 4096]),
      snapshot_data(token, CheckpointFileType::StoreIndex, 0, &[0x44u8; 16]),
      resp_frame(&[
        b"CLUSTER",
        b"SEND_CKPT_METADATA",
        &token.to_le_bytes(),
        (CheckpointFileType::StoreSnapshot as i64)
          .to_string()
          .as_bytes(),
        b"meta",
      ]),
      resp_frame(&[
        b"CLUSTER",
        b"SEND_CKPT_FILE_SEGMENT",
        &token.to_le_bytes(),
        (CheckpointFileType::StoreIndex as i64)
          .to_string()
          .as_bytes(),
        b"0",
        b"segment",
        b"0",
      ]),
    ] {
      let resp = drive(&rt, &mut consumer, &frame);
      assert!(
        resp.starts_with(b"-ERR")
          && resp
            .windows(30)
            .any(|w| w.starts_with(b"refusing remaining")),
        "本轮闸门关闭后接收入口必须拒收: {resp:?}"
      );
    }

    // 会话复位：本轮闸门净态，新一轮从头重推放行（旧半写被覆盖）
    provider.reset_recv_checkpoint_handler();
    let resp = drive(
      &rt,
      &mut consumer,
      &snapshot_data(token, CheckpointFileType::StoreHlog, 0, &[0x55u8; 4096]),
    );
    assert_eq!(resp, b"+OK\r\n", "新一轮全量首块必须被收下（从头覆盖）");
    let written = replica.store.device.read_range(0, 4096).await.unwrap();
    assert!(
      written.iter().all(|b| *b == 0x55),
      "新一轮必须自起始地址覆盖上轮半写数据"
    );
  });
}
