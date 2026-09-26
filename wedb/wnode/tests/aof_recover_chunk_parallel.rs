//! 页级并行恢复分块大记录重组回归（对标 garnet/libs/server/AOF/Recover/
//! RecoverReplayTask.cs:ReplayPage × AofChunkedRecordReader.cs:ReadChunk）
//!
//! 回归点（task: wnode-parallel-recover-chunk-continuation-frame-parse）：
//! 写端 EnqueueSpanChunked 每帧携带完整帧头（C# TsavoriteLog.Chunked.cs:
//! WriteOneRecord 线协议），并行恢复 Worker 对页内每条记录盲调
//! `record_gate::can_replay` 头解析并按 chunk_header.key_hash 一致性路由时，
//! 续块帧不再因缺头崩溃（InvalidRecordHeader / UnsupportedReplayHeaderType /
//! UnknownEntryType），也不因缺 key_hash 误路由产生撕裂消费；同一逻辑大记录
//! 全部分块帧路由至同一 Worker 后按 object_id 重组为完整记录。
//!
//! 覆盖两拓扑：单物理日志多回放任务（ReplayPage 并行双闸栏路径）与多物理
//! 分片日志（multi_log_recover 逐分片驱动，分片内同按回放任务数并行）。

use std::{str::from_utf8, sync::Arc};

use aok::{OK, Void};
use waof::{AofAddress, AofEntryType, SequenceNumberGenerator, WalConfig};
use wconf::RuntimeServerOptions;
use wnode::{
  aof::{
    aof_processor::{AofProcessor, ReplayTarget},
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::{GarnetLog, MIN_PARTIAL_ALLOC_SIZE, RecordShape},
    recover::aof_recover::AofRecover,
  },
  storage::session::storage_session::StorageSession,
};
use wnode_test::replay_input_bytes;
use wresp::command::RespCommand;
use wtest_base::open_test_store_with_budget;
use wval::{KeyTag, NamespaceDbCodec};

/// 触发自动分块的大值（key+value+input 超过最小分配尺寸即由统一入队口分块）
const BIG_VALUE_LEN: usize = MIN_PARTIAL_ALLOC_SIZE as usize + 4096;

/// 物理键编码（条目 key 统一为 wkv 物理键：[NsVarint][DbVarint][KeyTag][用户键]）
fn physical(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// upsert 条目入队（带 input 组件，对标生产 SET 写日志形状）
fn enqueue_set(log: &GarnetLog, version: i64, session_id: i32, key: &[u8], value: &[u8]) -> Void {
  let input = replay_input_bytes(RespCommand::Set, vec![key.to_vec(), value.to_vec()]);
  log.enqueue(&RecordShape {
    op_type: AofEntryType::StoreUpsert,
    version,
    session_id,
    key: &physical(key),
    value,
    input: &input,
    database_id: 0,
  })?;
  OK
}

/// 大记录批次写入并登记（用户键 → 期望值）
fn write_big_records(
  log: &GarnetLog,
  names: &[&[u8]],
  fill: u8,
  version: i64,
  session_id: i32,
) -> Void {
  for (i, name) in names.iter().enumerate() {
    let mut value = vec![fill; BIG_VALUE_LEN];
    value[i] = fill.wrapping_add(1);
    enqueue_set(log, version, session_id, name, &value)?;
  }
  OK
}

/// 大记录重组验证核：全部大记录逐字节一致、非分块对照记录落地
async fn assert_reassembled<S>(
  storage: &StorageSession<'_, S>,
  expected: &[(&[u8], Vec<u8>)],
) -> Void
where
  S: wdev::Device,
{
  for (name, value) in expected {
    let actual = storage.read_string(name).await?;
    assert!(
      actual.as_deref() == Some(value.as_slice()),
      "大记录 {:?} 重组值须与写入逐字节一致 (actual_len={:?}, expected_len={})",
      from_utf8(name),
      actual.as_ref().map(|v| v.len()),
      value.len()
    );
  }
  OK
}

/// 分块多帧配置：小页大窗（值超 64KB 页载荷即拆多帧——并行恢复的续块帧
/// 路由与重组正须多帧形态驱动；帧组总预留仍落入 8MB 窗口）
fn chunk_frame_config() -> WalConfig {
  WalConfig {
    page_size: 64 * 1024,
    buffer_size: 8 * 1024 * 1024,
    ..WalConfig::default()
  }
}

/// 单物理日志多回放任务：页级并行恢复（ReplayPage 双闸栏路径）重组分块大记录。
///
/// 修复前：续块帧为裸数据，Worker 的 can_replay 对其盲调 AofHeader::parse，
/// 非法前导字节即 InvalidRecordHeader / UnsupportedReplayHeaderType /
/// UnknownEntryType 上抛，整页恢复崩溃；即便碰巧解析出合法字段也因缺乏真实
/// key_hash 误路由，产生撕裂消费。
#[compio::test]
async fn single_physical_log_parallel_recover_reassembles_chunked_records() -> Void {
  let (_dirs, backends) =
    wnode_test::test_sublogs_with_config("aof_chunk_para", 1, chunk_frame_config());
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: 1,
    aof_replay_task_count: 4,
    ..RuntimeServerOptions::default()
  };
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  ));
  let log = aof.log();

  // 三条分块大记录（不同 key → 不同回放任务归属）+ 一条非分块对照
  let names: [&[u8]; 3] = [b"big-a", b"big-b", b"big-c"];
  write_big_records(log, &names, b'a', 5, 7)?;
  enqueue_set(log, 5, 7, b"small", b"v-small")?;

  // 物理刷盘（恢复以落盘为准）
  log.commit_async().await;

  // 页级并行恢复（replay_task_count=4 走 RecoverLogDriver 并行双闸栏路径）
  // 目标库页容量随预算自适应（256MB → 4MB 页），容下 1MB 分块阈值大值整包内联
  let (_dir, store) = open_test_store_with_budget("aof-chunk-para-dst.db", 256 << 20)?;
  let session = store.new_session()?;
  let storage = StorageSession::new(session.enter_batch());
  store.set_current_version(5);
  let target = ReplayTarget {
    session: &storage,
    store: Arc::clone(&store),
    aof_floor: vec![],
  };
  let processor = AofProcessor::new(Arc::clone(&aof));
  let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
  assert!(replayed > 0, "恢复须重放条目");

  let mut expected = Vec::new();
  for (i, name) in names.into_iter().enumerate() {
    let mut value = vec![b'a'; BIG_VALUE_LEN];
    value[i] = b'a' + 1;
    expected.push((name, value));
  }
  assert_reassembled(&storage, &expected).await?;
  assert_eq!(
    storage.read_string(b"small").await?,
    Some(b"v-small".to_vec()),
    "非分块对照记录落地"
  );
  OK
}

/// 多物理分片日志：multi_log_recover 逐分片驱动重组分块大记录（大记录全帧
/// 携带 key_hash，确定性路由至所属分片，续块帧绝不误投他片）。
#[compio::test]
async fn sharded_log_recover_reassembles_chunked_records() -> Void {
  let (_dirs, backends) =
    wnode_test::test_sublogs_with_config("aof_chunk_shard", 2, chunk_frame_config());
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: 2,
    aof_replay_task_count: 2,
    ..RuntimeServerOptions::default()
  };
  let seq_gen = Some(Arc::new(SequenceNumberGenerator::new(0)));
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, seq_gen.clone()).expect("构造 GarnetLog")),
    &options,
    seq_gen,
  ));
  let log = aof.log();

  let names: [&[u8]; 4] = [b"shard-a", b"shard-b", b"shard-c", b"shard-d"];
  write_big_records(log, &names, b'A', 5, 9)?;
  enqueue_set(log, 5, 9, b"small", b"v-small")?;

  // 提交全物理子日志并收敛各子日志 cookie（multi_log_recover 上界来源）
  log.commit_async().await;
  log.recover_async().await.expect("设备面恢复");

  let (_dir, store) = open_test_store_with_budget("aof-chunk-shard-dst.db", 256 << 20)?;
  let session = store.new_session()?;
  let storage = StorageSession::new(session.enter_batch());
  store.set_current_version(5);
  let target = ReplayTarget {
    session: &storage,
    store: Arc::clone(&store),
    aof_floor: vec![],
  };
  let processor = AofProcessor::new(Arc::clone(&aof));
  let until = AofAddress::create(2, -1);
  let replayed = AofRecover::multi_log_recover(&processor, &aof, 0, &until, &target).await?;
  assert!(replayed > 0, "恢复须重放条目");

  let mut expected = Vec::new();
  for (i, name) in names.into_iter().enumerate() {
    let mut value = vec![b'A'; BIG_VALUE_LEN];
    value[i] = b'A' + 1;
    expected.push((name, value));
  }
  assert_reassembled(&storage, &expected).await?;
  assert_eq!(
    storage.read_string(b"small").await?,
    Some(b"v-small".to_vec()),
    "非分块对照记录落地"
  );
  OK
}
