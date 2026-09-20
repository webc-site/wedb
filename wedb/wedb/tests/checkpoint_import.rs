//! M4 检查点网络导入面集成测试（副本非空库一致性真实链路）
//!
//! 对标 test/cluster/Garnet.test.cluster.replication 的磁盘基复制检查点流：
//! - 帧级：脚本化假端点模式（cluster_migration.rs 同款 RESP 帧直投）驱动
//!   SNAPSHOT_DATA / SEND_CKPT_METADATA / SEND_CKPT_FILE_SEGMENT /
//!   BEGIN_REPLICA_RECOVER 四 arm，断言到「置换后引擎可读主端键」；
//! - 套接字：真 GarnetServer 双节点形态，主端 initiate_replica_sync 全链
//!   （快照下发 + BEGIN_REPLICA_RECOVER 往返 + attach 推流）。

use std::{fs, path::PathBuf, sync::Arc, time::Duration};

use compio::runtime::Runtime;
use waof::{AofAddress, WalConfig, WalLog};
use wbftree::{RangeIndexManager, StorageBackendType, TreeTuning};
use wcpr::{CheckpointMeta, CheckpointType};
use wdev::{Device, SegmentedDevice};
use wedb::{
  client::GarnetClient,
  server::{
    cluster::IClusterProvider,
    cluster_config::ClusterConfig,
    cluster_manager::ClusterManager,
    cluster_provider::{ClusterProvider, PrimaryReplicationAssets},
    cluster_session::ClusterSession,
    replication::{
      aof_replication_pump::AofReplicationPump,
      checkpoint_entry::{CheckpointEntry, CheckpointFileType, CheckpointMetadata},
      cluster_replication_session::ClusterReplicationSession,
      recovery_status::RecoveryStatus,
      replica_sync_session::ReplicaSyncSession,
      sync_metadata::SyncMetadata,
    },
    worker::{LocalWorkerSpec, NodeRole},
  },
};
use wkv::WedbStore;
use wnode::{
  GarnetServer, MessageConsumerFace, RespSessionConsumer, WireFormat,
  database::checkpoint_version,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  storage::session::storage_session::StorageSession,
};
use wtest_base::{resp_frame, test_store_config, wait_for};
use wtxn::{TxnLockTable, WatchVersionMap};

/// 测试节点身份（内部 u128；协议面渲染 32 字符小写 hex）
const PRIMARY_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0001;
const REPLICA_ID: u128 = 0x0DE1_0000_0000_0000_0000_0000_0000_0002;

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
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
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

/// 副本角色 provider（对齐宿主装配：rm/store/wal/checkpoint_dir 全接线）
fn replica_provider(node: &NodeStorage) -> Arc<ClusterProvider> {
  let provider = ClusterProvider::new();
  provider.initialize_replication_manager(1, None, false);
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
  *cm.current_config.write() = config;
  *provider.cluster_manager.write() = Some(cm);
  provider.set_store(Arc::clone(&node.store));
  provider.set_checkpoint_dir(node.checkpoint_dir.clone());
  provider.set_wal(Arc::clone(&node.wal));
  provider
}

/// 主端角色 provider（全槽自持）
fn primary_provider(node: &NodeStorage) -> Arc<ClusterProvider> {
  let provider = ClusterProvider::new();
  provider.initialize_replication_manager(1, None, false);
  let cm = Arc::new(ClusterManager::new(Arc::clone(&provider)));
  let mut config = ClusterConfig::new();
  config.initialize_local_worker(LocalWorkerSpec {
    node_id: PRIMARY_ID,
    address: "127.0.0.1",
    port: 7000,
    config_epoch: 1,
    role: NodeRole::Primary,
    replica_of_node_id: None,
    hostname: None,
  });
  *cm.current_config.write() = config;
  *provider.cluster_manager.write() = Some(cm);
  provider.set_store(Arc::clone(&node.store));
  provider.set_checkpoint_dir(node.checkpoint_dir.clone());
  provider.set_wal(Arc::clone(&node.wal));
  provider
}

/// 集群会话消费者（provider.set_store 与执行域同源）
fn cluster_consumer(provider: &Arc<ClusterProvider>) -> RespSessionConsumer {
  let cluster_session: Arc<ClusterSession> = provider.create_cluster_session();
  let store = provider.try_store().unwrap();
  let mut consumer = RespSessionConsumer::with_cluster(
    1,
    RespServerSessionOptions::default(),
    cluster_session,
    provider.provider_handle(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  );
  consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
  consumer
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

/// 主端拍检查点并登记复制域条目（对标 on_checkpoint_initiated +
/// add_new_checkpoint_entry 的登记组合；covered 为快照后 AOF 授予位点）
async fn take_primary_checkpoint(
  node: &NodeStorage,
  provider: &Arc<ClusterProvider>,
) -> (u128, CheckpointEntry, i64) {
  let meta = wcpr::create_checkpoint(
    node.store.as_ref(),
    &node.checkpoint_dir,
    CheckpointType::FoldOver,
  )
  .await
  .unwrap();
  let token = meta.token;
  // 空日志起点（rust WalLog 无头区，判据 begin == tail）
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

/// 主端检查点文件集快照（hlog 段源字节 / index 文件字节 / meta 文件字节）
struct PrimaryCheckpointFiles {
  hlog: Vec<u8>,
  hlog_start: u64,
  index: Vec<u8>,
  meta: Vec<u8>,
}

/// 读主端检查点文件集（发送面同源读取口径：设备池化读 + 检查点目录文件）
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

/// 段流帧发射：按段发 SNAPSHOT_DATA + 空载荷收尾，断言逐帧 +OK
fn emit_file_stream(
  rt: &Runtime,
  consumer: &mut RespSessionConsumer,
  token: u128,
  file_type: i64,
  source: &[u8],
  start_address: u64,
) {
  const CHUNK: usize = 1 << 17;
  let token_bytes = token.to_le_bytes();
  let mut offset = start_address;
  let end = start_address + source.len() as u64;
  while offset < end {
    let len = CHUNK.min((end - offset) as usize);
    let chunk = &source[(offset - start_address) as usize..][..len];
    let frame = resp_frame(&[
      b"CLUSTER",
      b"SNAPSHOT_DATA",
      &token_bytes,
      file_type.to_string().as_bytes(),
      offset.to_string().as_bytes(),
      chunk,
    ]);
    let resp = drive(rt, consumer, &frame);
    assert_eq!(resp, b"+OK\r\n", "文件段应答");
    offset += len as u64;
  }
  let eof = resp_frame(&[
    b"CLUSTER",
    b"SNAPSHOT_DATA",
    &token_bytes,
    file_type.to_string().as_bytes(),
    offset.to_string().as_bytes(),
    &[],
  ]);
  let resp = drive(rt, consumer, &eof);
  assert_eq!(resp, b"+OK\r\n", "EOF 哨兵应答");
}

/// 全链：主端检查点 → 三形态帧接收落盘 → BEGIN_REPLICA_RECOVER 导入 →
/// 在线引擎置换 → 副本读到主端键（非空库一致性）
#[test]
fn checkpoint_stream_import_swaps_online_engine() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // ===== 主端：建库 + 快照 + 复制域登记
    let primary = open_node("primary");
    let primary_provider = primary_provider(&primary);
    put_str(&primary.store, b"ckpt_key_a", b"value_a").await;
    put_str(&primary.store, b"ckpt_key_b", b"value_b").await;
    let (token, entry, covered) = take_primary_checkpoint(&primary, &primary_provider).await;
    let files = read_primary_checkpoint_files(&primary, token).await;
    assert!(!files.hlog.is_empty(), "主端 hlog 段源非空");

    // ===== 副本：空库 + 全接线 provider
    let replica = open_node("replica");
    let provider = replica_provider(&replica);
    let rm = provider.replication_manager().unwrap();
    let mut consumer = cluster_consumer(&provider);
    let old_store = Arc::clone(&replica.store);

    // ===== 段流接收：hlog → index → meta（元数据最后落盘）
    emit_file_stream(
      &rt,
      &mut consumer,
      token,
      CheckpointFileType::StoreHlog as i64,
      &files.hlog,
      files.hlog_start,
    );
    emit_file_stream(
      &rt,
      &mut consumer,
      token,
      CheckpointFileType::StoreIndex as i64,
      &files.index,
      0,
    );
    let meta_frame = resp_frame(&[
      b"CLUSTER",
      b"SNAPSHOT_DATA",
      &token.to_le_bytes(),
      (CheckpointFileType::StoreSnapshot as i64)
        .to_string()
        .as_bytes(),
      b"-1",
      &files.meta,
    ]);
    assert_eq!(drive(&rt, &mut consumer, &meta_frame), b"+OK\r\n");

    // ===== 接收面布局断言：meta/index 落在检查点目录（wcpr 命名零改名）
    assert!(
      replica
        .checkpoint_dir
        .join(wcpr::meta_filename(token))
        .is_file()
    );
    assert!(
      replica
        .checkpoint_dir
        .join(wcpr::index_filename(token))
        .is_file()
    );

    // ===== 接收探针：设备文件幅面与记录字节落位（接收面文件承载闭环）
    assert!(
      replica.store.device.get_file_size(0).unwrap() >= files.hlog.len() as u64,
      "设备文件幅面不足"
    );

    // ===== BEGIN_REPLICA_RECOVER（恢复门控对标 TryAddReplicaAsync 的
    // BeginRecovery(ClusterReplicate) 前置）
    assert!(rm.begin_recovery(RecoveryStatus::ClusterReplicate, false));
    let recover_frame = resp_frame(&[
      b"CLUSTER",
      b"BEGIN_REPLICA_RECOVER",
      b"T",
      b"0",
      b"primary-repl-id-1",
      &entry.to_byte_array(),
      &aof_span(covered),
      &aof_span(covered),
    ]);
    let resp = drive(&rt, &mut consumer, &recover_frame);
    let expected = format!("${}\r\n{covered}\r\n", covered.to_string().len());
    assert_eq!(resp, expected.as_bytes(), "恢复应答为授予位点 bulk string");

    // ===== 在线引擎置换断言：provider 视图换新 + 新引擎读到主端键
    let new_store = provider.try_store().unwrap();
    assert!(!Arc::ptr_eq(&new_store, &old_store), "导入必须置换在线引擎");
    assert_eq!(
      read_str(&new_store, b"ckpt_key_a").await.unwrap(),
      b"value_a"
    );
    assert_eq!(
      read_str(&new_store, b"ckpt_key_b").await.unwrap(),
      b"value_b"
    );

    // ===== 复制域收敛断言：位点 / replid / 恢复态 / 检查点历史
    assert_eq!(rm.get_replication_offset(0), covered);
    assert_eq!(rm.primary_repl_id(), "primary-repl-id-1");
    assert_eq!(
      rm.recovery_status(),
      RecoveryStatus::CheckpointRecoveredAtReplica
    );
    assert_eq!(
      rm.checkpoint_store
        .read()
        .latest_entry()
        .unwrap()
        .metadata
        .store_hlog_token,
      token
    );

    // ===== wal 对齐断言：物理日志起点对齐快照覆盖点
    assert_eq!(replica.wal.begin_address(), covered as u64);
    assert_eq!(replica.wal.tail_address(), covered as u64);
  });
}

/// 全链 RangeIndex 往返：主端建 RI → 检查点（token 目录落 rangeindex 快照
/// 树文件）→ 副本帧接收（头帧 key_id 元数据 + 段流 + 空收尾）→
/// BEGIN_REPLICA_RECOVER 导入 → 置换后引擎 RI 可读——检查点时刻已存在的
/// RI 树随快照文件在副本重建（对标 libs/cluster/Server/Replication/
/// PrimaryOps/DiskbasedReplication/RangeIndexSnapshotReader.cs 全量同步
/// 下发链；缺此链副本检查点前建树永不重建、ri_set 回放静默跳过）
#[test]
fn checkpoint_stream_import_restores_range_index_trees() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // ===== 主端：建库 + RI.CREATE / RI.SET + 快照
    let primary = open_node("primary");
    let primary_provider = primary_provider(&primary);
    // RI 键值长度契约：field+value 总长须落在 [min_record_size, max_record_size]
    let field_a = [b'f'; 32];
    let field_b = [b'g'; 32];
    let value_a = [b'a'; 40];
    let value_b = [b'b'; 40];
    {
      let session = primary.store.new_session().unwrap();
      session
        .range_index_create(b"ri_key", StorageBackendType::Disk, TreeTuning::default())
        .await
        .unwrap();
      session
        .range_index_set(b"ri_key", &field_a, &value_a)
        .await
        .unwrap();
      session
        .range_index_set(b"ri_key", &field_b, &value_b)
        .await
        .unwrap();
    }
    let (token, entry, covered) = take_primary_checkpoint(&primary, &primary_provider).await;

    // 主端发送源枚举（与 send_store_checkpoint 同源）：token 目录内已
    // 快照树文件必须存在，文件名 stem 解码即 key_id
    let ri_files =
      RangeIndexManager::enumerate_checkpoint_snapshots(&primary.checkpoint_dir, token).unwrap();
    assert_eq!(ri_files.len(), 1, "检查点必须落盘 RI 快照树文件");
    let (ri_key_id, ri_path) = ri_files[0].clone();

    // ===== 副本：空库 + 全接线 provider
    let replica = open_node("replica");
    let provider = replica_provider(&replica);
    let rm = provider.replication_manager().unwrap();
    let mut consumer = cluster_consumer(&provider);
    let old_store = Arc::clone(&replica.store);

    // ===== 段流接收：hlog → index → RI 快照树文件（头帧 → 段帧 → 空收尾）
    let files = read_primary_checkpoint_files(&primary, token).await;
    emit_file_stream(
      &rt,
      &mut consumer,
      token,
      CheckpointFileType::StoreHlog as i64,
      &files.hlog,
      files.hlog_start,
    );
    emit_file_stream(
      &rt,
      &mut consumer,
      token,
      CheckpointFileType::StoreIndex as i64,
      &files.index,
      0,
    );
    let ri_header_frame = resp_frame(&[
      b"CLUSTER",
      b"SNAPSHOT_DATA",
      &token.to_le_bytes(),
      (CheckpointFileType::StoreRangeindexSnapshot as i64)
        .to_string()
        .as_bytes(),
      b"-1",
      &ri_key_id.to_le_bytes(),
    ]);
    assert_eq!(drive(&rt, &mut consumer, &ri_header_frame), b"+OK\r\n");
    let ri_content = fs::read(&ri_path).unwrap();
    emit_file_stream(
      &rt,
      &mut consumer,
      token,
      CheckpointFileType::StoreRangeindexSnapshot as i64,
      &ri_content,
      0,
    );
    let meta_frame = resp_frame(&[
      b"CLUSTER",
      b"SNAPSHOT_DATA",
      &token.to_le_bytes(),
      (CheckpointFileType::StoreSnapshot as i64)
        .to_string()
        .as_bytes(),
      b"-1",
      &files.meta,
    ]);
    assert_eq!(drive(&rt, &mut consumer, &meta_frame), b"+OK\r\n");

    // ===== 接收落盘布局：token 子目录 rangeindex/（恢复端
    // recover_all_trees_from_dir 候选目录同构，落盘即收敛）
    let ri_snapshot_path =
      RangeIndexManager::checkpoint_snapshot_path_in(&replica.checkpoint_dir, token, ri_key_id);
    assert_eq!(
      fs::read(&ri_snapshot_path).unwrap(),
      ri_content,
      "RI 快照树文件必须逐字节落盘"
    );

    // ===== BEGIN_REPLICA_RECOVER（导入 = 从接收文件集恢复全新引擎）
    assert!(rm.begin_recovery(RecoveryStatus::ClusterReplicate, false));
    let recover_frame = resp_frame(&[
      b"CLUSTER",
      b"BEGIN_REPLICA_RECOVER",
      b"T",
      b"0",
      b"primary-repl-id-1",
      &entry.to_byte_array(),
      &aof_span(covered),
      &aof_span(covered),
    ]);
    let resp = drive(&rt, &mut consumer, &recover_frame);
    let expected = format!("${}\r\n{covered}\r\n", covered.to_string().len());
    assert_eq!(resp, expected.as_bytes(), "恢复应答为授予位点 bulk string");

    // ===== 置换后引擎 RI 可读：pending 注册 + 惰性激活走检查点快照预置
    let new_store = provider.try_store().unwrap();
    assert!(!Arc::ptr_eq(&new_store, &old_store), "导入必须置换在线引擎");
    let session = new_store.new_session().unwrap();
    assert!(
      session.range_index_exists(b"ri_key").await.unwrap(),
      "检查点时刻已存在的 RI 树必须在副本重建"
    );
    assert_eq!(
      session.range_index_get(b"ri_key", &field_a).await.unwrap(),
      Some(value_a.to_vec())
    );
    assert_eq!(
      session.range_index_get(b"ri_key", &field_b).await.unwrap(),
      Some(value_b.to_vec())
    );
  });
}

/// 接收臂拒收与旧形态命令：RI flush/OBJ 类型拒绝、RI 快照缺头帧拒绝、
/// token 串流拒绝、无 meta 导入拒绝；SEND_CKPT_METADATA /
/// SEND_CKPT_FILE_SEGMENT 旧形态臂闭环
#[test]
fn checkpoint_stream_rejections_and_legacy_arms() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let replica = open_node("replica");
    let provider = replica_provider(&replica);
    let rm = provider.replication_manager().unwrap();
    let mut consumer = cluster_consumer(&provider);
    let token = 0x1234_5678_9abc_def0_1122_3344_5566_7788u128;
    let token_bytes = token.to_le_bytes();

    // RI flush 类型（rust 统一检查点模型无恢复面消费，主端不发送）→ -ERR
    let ri_frame = resp_frame(&[
      b"CLUSTER",
      b"SNAPSHOT_DATA",
      &token_bytes,
      b"7",
      b"0",
      b"payload",
    ]);
    let resp = drive(&rt, &mut consumer, &ri_frame);
    assert!(resp.starts_with(b"-ERR"), "RI flush 类型必须拒绝: {resp:?}");

    // RI 快照段帧（type=8）缺头帧开槽 → -ERR（头帧元数据派生落盘路径，
    // 段帧惰性开槽无元数据可用）
    let ri_orphan_seg = resp_frame(&[
      b"CLUSTER",
      b"SNAPSHOT_DATA",
      &token_bytes,
      b"8",
      b"0",
      b"payload",
    ]);
    let resp = drive(&rt, &mut consumer, &ri_orphan_seg);
    assert!(resp.starts_with(b"-ERR"), "RI 快照缺头帧必须拒绝: {resp:?}");

    // RI 快照头帧载荷过短（非 16B key_id）→ -ERR
    let ri_short_header = resp_frame(&[
      b"CLUSTER",
      b"SNAPSHOT_DATA",
      &token_bytes,
      b"8",
      b"-1",
      b"short",
    ]);
    let resp = drive(&rt, &mut consumer, &ri_short_header);
    assert!(
      resp.starts_with(b"-ERR"),
      "RI 头帧载荷过短必须拒绝: {resp:?}"
    );

    // OBJ 类型文件段（统一检查点模型无对象存文件集）→ -ERR
    let obj_frame = resp_frame(&[
      b"CLUSTER",
      b"SNAPSHOT_DATA",
      &token_bytes,
      b"2",
      b"0",
      b"payload",
    ]);
    let resp = drive(&rt, &mut consumer, &obj_frame);
    assert!(resp.starts_with(b"-ERR"), "OBJ 类型必须拒绝: {resp:?}");

    // 域外 type 值 → -ERR
    let bad_type = resp_frame(&[
      b"CLUSTER",
      b"SNAPSHOT_DATA",
      &token_bytes,
      b"3",
      b"0",
      b"payload",
    ]);
    let resp = drive(&rt, &mut consumer, &bad_type);
    assert!(resp.starts_with(b"-ERR"), "域外 type 必须拒绝: {resp:?}");

    // token 串流（未收尾换 token）→ -ERR
    let open_a = resp_frame(&[
      b"CLUSTER",
      b"SNAPSHOT_DATA",
      &token_bytes,
      b"4",
      b"0",
      b"partial",
    ]);
    assert_eq!(drive(&rt, &mut consumer, &open_a), b"+OK\r\n");
    let token_b = 0xdead_beefu128;
    let open_b = resp_frame(&[
      b"CLUSTER",
      b"SNAPSHOT_DATA",
      &token_b.to_le_bytes(),
      b"4",
      b"8",
      b"partial",
    ]);
    let resp = drive(&rt, &mut consumer, &open_b);
    assert!(resp.starts_with(b"-ERR"), "换 token 必须拒绝: {resp:?}");
    // 收尾清理活跃槽，后续用例不受污染
    let close = resp_frame(&[b"CLUSTER", b"SNAPSHOT_DATA", &token_bytes, b"4", b"8", &[]]);
    assert_eq!(drive(&rt, &mut consumer, &close), b"+OK\r\n");

    // 无 meta 的 BEGIN_REPLICA_RECOVER → -ERR（半截文件集拒绝导入）
    assert!(rm.begin_recovery(RecoveryStatus::ClusterReplicate, false));
    let ghost_entry = CheckpointEntry::new(CheckpointMetadata::new(1));
    let ghost_frame = resp_frame(&[
      b"CLUSTER",
      b"BEGIN_REPLICA_RECOVER",
      b"T",
      b"0",
      b"primary-repl-id-1",
      &ghost_entry.to_byte_array(),
      &aof_span(0),
      &aof_span(0),
    ]);
    let resp = drive(&rt, &mut consumer, &ghost_frame);
    assert!(resp.starts_with(b"-ERR"), "缺 meta 必须拒绝导入: {resp:?}");
    rm.end_recovery(RecoveryStatus::NoRecovery, false);

    // ===== 旧形态命令闭环：SEND_CKPT_METADATA 写 meta + 文件段 + 收尾
    let meta_frame = resp_frame(&[
      b"CLUSTER",
      b"SEND_CKPT_METADATA",
      &token_bytes,
      b"5",
      b"meta-bytes",
    ]);
    assert_eq!(drive(&rt, &mut consumer, &meta_frame), b"+OK\r\n");
    assert_eq!(
      fs::read(replica.checkpoint_dir.join(wcpr::meta_filename(token))).unwrap(),
      b"meta-bytes"
    );

    let seg_frame = resp_frame(&[
      b"CLUSTER",
      b"SEND_CKPT_FILE_SEGMENT",
      &token_bytes,
      b"4",
      b"0",
      b"index-bytes",
      b"0",
    ]);
    assert_eq!(drive(&rt, &mut consumer, &seg_frame), b"+OK\r\n");
    let seg_eof = resp_frame(&[
      b"CLUSTER",
      b"SEND_CKPT_FILE_SEGMENT",
      &token_bytes,
      b"4",
      b"11",
      &[],
      b"1",
    ]);
    assert_eq!(drive(&rt, &mut consumer, &seg_eof), b"+OK\r\n");
    assert_eq!(
      fs::read(replica.checkpoint_dir.join(wcpr::index_filename(token))).unwrap(),
      b"index-bytes"
    );
  });
}

/// 副本宿主会话装配面（每连接读置换槽最新引擎——对齐宿主 get_session）
struct ReplicaSessionProvider {
  provider: Arc<ClusterProvider>,
}

impl wnode::SessionProviderFace for ReplicaSessionProvider {
  type Consumer = RespSessionConsumer;

  fn get_session(
    &self,
    _wire_format: WireFormat,
    _network_sender_id: u64,
  ) -> Option<RespSessionConsumer> {
    let cluster_session = self.provider.create_cluster_session();
    let store = self.provider.try_store()?;
    let mut consumer = RespSessionConsumer::with_cluster(
      1,
      RespServerSessionOptions::default(),
      cluster_session,
      self.provider.provider_handle(),
      Arc::new(StoreGarnetApi::new(store.new_session().ok()?)),
    );
    consumer.attach_transaction_components(Arc::new(WatchVersionMap::new(64)), TxnLockTable::new());
    Some(consumer)
  }
}

/// 套接字端到端：主端 initiate_replica_sync 全链（快照下发 + 往返 + attach
/// 推流）→ 副本引擎置换可读主端键 + AOF 记录续推落盘
#[test]
fn checkpoint_import_end_to_end_over_socket() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // ===== 主端：建库 + 快照 + 复制域登记 + 推流资产
    let primary = open_node("primary");
    let provider_p = primary_provider(&primary);
    let rm_p = provider_p.replication_manager().unwrap();
    put_str(&primary.store, b"e2e_key_a", b"value_a").await;
    put_str(&primary.store, b"e2e_key_b", b"value_b").await;
    // wal 头区占位（复制域 [0,64) 头区基线）+ 业务记录（covered 起续推源）
    primary.wal.enqueue(&[0u8; 56]).unwrap();
    primary.wal.commit().await.unwrap();
    let (token, _entry, covered) = take_primary_checkpoint(&primary, &provider_p).await;
    let record_payload = b"e2e_appendlog_record_payload".to_vec();
    primary.wal.enqueue(&record_payload).unwrap();
    primary.wal.commit().await.unwrap();

    // ===== 副本：完整宿主形态（RESP 服务器 + 每连接读置换槽）
    let replica = open_node("replica");
    let provider_r = replica_provider(&replica);
    let rm_r = provider_r.replication_manager().unwrap();
    // 副本接收面（CLUSTER APPENDLOG 落盘重放，对标 wire_replication_data_plane）
    provider_r.set_replica_replication_session(Some(Arc::new(ClusterReplicationSession::new(
      Arc::clone(&provider_r),
      Arc::clone(&replica.wal),
      None,
    ))));
    let server = GarnetServer::new(
      &["127.0.0.1:0".to_string()],
      65536,
      100,
      Arc::new(ReplicaSessionProvider {
        provider: Arc::clone(&provider_r),
      }),
    )
    .unwrap();
    server.start(None).unwrap();
    let replica_addr = server.local_addr().unwrap().to_string();

    // ===== 主端发起全量同步（策略协商 → 快照下发 → 往返 → attach 推流）
    let assets = Arc::new(PrimaryReplicationAssets {
      wal: Arc::clone(&primary.wal),
      pump: Arc::new(AofReplicationPump::new(Arc::clone(
        &rm_p.aof_sync_driver_store,
      ))),
      sync_session: Arc::new(ReplicaSyncSession::new(Arc::clone(&rm_p))),
    });
    let meta = SyncMetadata {
      full_sync: false,
      origin_node_role: NodeRole::Replica,
      origin_node_id: REPLICA_ID,
      current_primary_repl_id: rm_p.primary_repl_id(),
      current_store_version: 0,
      current_aof_begin_address: AofAddress::create(1, 0),
      current_aof_tail_address: AofAddress::create(1, 0),
      current_replication_offset: AofAddress::create(1, 0),
      checkpoint_entry: Some(CheckpointEntry::new(CheckpointMetadata::new(1))),
    };
    let granted = assets
      .sync_session
      .initiate_replica_sync(&provider_p, &assets, PRIMARY_ID, &replica_addr, &meta)
      .await
      .unwrap();
    assert_eq!(granted.get(0), Some(covered), "授予位点 = 快照覆盖点");

    // ===== 轮询副本引擎置换完成（快照流 + 导入 + 换引擎全链异步收口）
    let old_store_r = Arc::clone(&replica.store);
    let imported = wait_for(
      || {
        provider_r
          .try_store()
          .is_some_and(|s| !Arc::ptr_eq(&s, &old_store_r))
      },
      Duration::from_secs(10),
    )
    .await;
    assert!(imported, "副本必须在导入后完成引擎置换");

    // ===== 非空库一致性：两个键全量可见 + 检查点历史/位点/replid 收敛
    let new_store = provider_r.try_store().unwrap();
    assert_eq!(
      read_str(&new_store, b"e2e_key_a").await.unwrap(),
      b"value_a"
    );
    assert_eq!(
      read_str(&new_store, b"e2e_key_b").await.unwrap(),
      b"value_b"
    );
    assert_eq!(
      rm_r
        .checkpoint_store
        .read()
        .latest_entry()
        .unwrap()
        .metadata
        .store_hlog_token,
      token
    );
    assert!(rm_r.get_replication_offset(0) >= covered);
    assert_eq!(rm_r.primary_repl_id(), rm_p.primary_repl_id());

    // ===== AOF 续推：业务记录经 APPENDLOG 落副本 wal（covered 起续推）
    let wal_r = Arc::clone(&replica.wal);
    let streamed = wait_for(
      || wal_r.tail_address() > covered as u64,
      Duration::from_secs(10),
    )
    .await;
    assert!(streamed, "快照覆盖点之后的 AOF 记录必须续推落盘");
    assert!(rm_r.has_active_replication_stream(), "attach 后复制流活跃");
  });
}

/// GarnetClient 未连接面：快照帧发送失败返回错误（协议面守卫）
#[test]
fn snapshot_data_client_requires_connection() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let client = GarnetClient::with_auth("127.0.0.1:1".to_string(), None, None);
    let res = client
      .snapshot_data_async(&[0u8; 16], CheckpointFileType::StoreHlog as i64, 0, b"data")
      .await;
    assert!(res.is_err(), "未连接必须报错");
  });
}
