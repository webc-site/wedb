//! 检查点文件面单源（存储读写辅助 + 主端检查点文件集快照 + 段流帧发射）
//!
//! 收口 checkpoint_import / receive_checkpoint_dir_fsync /
//! replica_receive_checkpoint_retry / replica_diskbased_vector_rebuild 四册
//! 逐字同形的文件级装配：aof_span、put_str/read_str、take_primary_checkpoint、
//! PrimaryCheckpointFiles + read_primary_checkpoint_files、snapshot_data、
//! emit_file_stream。差异面（存储与目录句柄）以参数暴露，宿主册直挂（沿用
//! primary_assets 先例）：
//!
//! ```text
//! #[path = "common/ckpt_files.rs"]
//! mod ckpt_files;
//! ```

use std::{fs, path::Path, sync::Arc};

use compio::runtime::Runtime;
use waof::AofAddress;
use wcpr::{CheckpointMeta, CheckpointType};
use wdev::{Device, SegmentedDevice};
use wedb::server::{
  cluster_provider::ClusterProvider,
  replication::{
    checkpoint_entry::{CheckpointEntry, CheckpointFileType, CheckpointMetadata},
    recovery_status::RecoveryStatus,
    replication_manager::ReplicationManager,
  },
};
use wedb_test::resp_drive_scratch::drive;
use wkv::WedbStore;
use wnode::{
  RespSessionConsumer, database::checkpoint_version,
  storage::session::storage_session::StorageSession,
};
use wtest_base::resp_frame;

/// 单槽位点 span（C# AofAddress.Span：8B LE）
pub fn aof_span(address: i64) -> Vec<u8> {
  address.to_le_bytes().to_vec()
}

/// 写 string 键
pub async fn put_str(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8], value: &[u8]) {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new(batch);
  storage.upsert_string(key, value).await.unwrap();
}

/// 读 string 键
pub async fn read_str(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> Option<Vec<u8>> {
  let session = store.new_session().unwrap();
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  storage.read_string(key).await.unwrap()
}

/// 主端拍检查点并登记复制域条目（对标 on_checkpoint_initiated +
/// add_new_checkpoint_entry 的登记组合；covered 为快照后 AOF 授予位点）
pub async fn take_primary_checkpoint(
  store: &Arc<WedbStore<SegmentedDevice>>,
  checkpoint_dir: &Path,
  provider: &Arc<ClusterProvider>,
) -> (u128, CheckpointEntry, i64) {
  let meta = wcpr::create_checkpoint(
    store.as_ref(),
    &store.ckpt_gate,
    checkpoint_dir,
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
pub struct PrimaryCheckpointFiles {
  /// hlog 段源字节（设备池化读，扇区对齐起止）
  pub hlog: Vec<u8>,
  /// hlog 段源起始地址（扇区对齐）
  pub hlog_start: u64,
  /// index ckpt 文件字节
  pub index: Vec<u8>,
  /// meta 文件字节（元数据最后落盘 = 提交标记）
  pub meta: Vec<u8>,
}

/// 读主端检查点文件集（发送面同源读取口径：设备池化读 + 检查点目录文件）
pub async fn read_primary_checkpoint_files(
  store: &Arc<WedbStore<SegmentedDevice>>,
  checkpoint_dir: &Path,
  token: u128,
) -> PrimaryCheckpointFiles {
  let meta_bytes = fs::read(checkpoint_dir.join(wcpr::meta_filename(token))).unwrap();
  let meta = CheckpointMeta::decode(&meta_bytes).unwrap();
  let sector = store.device.sector_size() as u64;
  let start = meta.hlog_meta.begin_address / sector * sector;
  let file_len = store.device.get_file_size(0).unwrap();
  let raw_end = file_len
    .max(meta.hlog_meta.flushed_until_address)
    .max(meta.hlog_meta.tail_address);
  let end = raw_end / sector * sector;
  let hlog = store
    .device
    .read_range(start, (end - start) as usize)
    .await
    .unwrap();
  let index = fs::read(checkpoint_dir.join(wcpr::index_filename(token))).unwrap();
  PrimaryCheckpointFiles {
    hlog: hlog[..].to_vec(),
    hlog_start: start,
    index,
    meta: meta_bytes,
  }
}

/// SNAPSHOT_DATA 单帧发射
pub fn snapshot_data(
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
pub fn emit_file_stream(
  rt: &Runtime,
  consumer: &mut RespSessionConsumer,
  token: u128,
  file_type: CheckpointFileType,
  source: &[u8],
  start_address: u64,
) {
  /// 快照分块尺寸（扇区整数倍，与主端发送面切分同构）
  const CHUNK: usize = 1 << 17;
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

/// BEGIN_REPLICA_RECOVER 导入往返：恢复门控（ClusterReplicate）→ 恢复帧
/// 直投 → 授予位点 bulk string 应答断言。恢复门控注释语境各册各异，留在
/// 调用侧；缺 meta 拒收臂不走本往返（ghost 用例自持 -ERR 断言）。
pub fn begin_recover_exchange(
  rm: &ReplicationManager,
  rt: &Runtime,
  consumer: &mut RespSessionConsumer,
  entry: &CheckpointEntry,
  covered: i64,
) {
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
  let resp = drive(rt, consumer, &recover_frame);
  let expected = format!("${}\r\n{covered}\r\n", covered.to_string().len());
  assert_eq!(resp, expected.as_bytes(), "恢复应答为授予位点 bulk string");
}

/// 段流接收头两臂（hlog → index）；meta / RI 快照臂由用例按序续接
pub fn emit_hlog_index(
  rt: &Runtime,
  consumer: &mut RespSessionConsumer,
  token: u128,
  files: &PrimaryCheckpointFiles,
) {
  emit_file_stream(
    rt,
    consumer,
    token,
    CheckpointFileType::StoreHlog,
    &files.hlog,
    files.hlog_start,
  );
  emit_file_stream(
    rt,
    consumer,
    token,
    CheckpointFileType::StoreIndex,
    &files.index,
    0,
  );
}
