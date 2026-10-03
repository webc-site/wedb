//! 分块 AOF 记录重组与状态机集成测试
//! （对应 libs/server/AOF/AofChunkedRecordReader.cs: ChunkedAccumulator + AofChunkedRecordReader）

use std::sync::Arc;

use aok::OK;
use waof::{AofChunkHeader, AofEntryType, AofHeader, SequenceNumberGenerator, WalConfig, WalLog};
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wnode::{
  aof::{
    AofSublog, WaofSublog,
    aof_chunked_record_reader::{
      AofChunkReadError, AofChunkedRecordReader, ChunkedAccumulator, Component,
    },
    aof_processor::{AofProcessor, AofReplayError, ReplayTarget},
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::{GarnetLog, MIN_PARTIAL_ALLOC_SIZE, RecordShape},
  },
  storage::session::storage_session::StorageSession,
};

/// 触发自动分块的最小大记录（key+value 超过最小分配尺寸即由统一入队口分块）
const BIG_VALUE_LEN: usize = MIN_PARTIAL_ALLOC_SIZE as usize + 64;

/// 分块多帧配置：小页大窗（值超 64KB 页载荷即拆多帧，帧组总预留仍落入 8MB 窗口）
fn small_page_config() -> WalConfig {
  WalConfig {
    page_size: 64 * 1024,
    buffer_size: 8 * 1024 * 1024,
    ..WalConfig::default()
  }
}

fn test_backends_with_config(
  tag: &str,
  count: usize,
  config: WalConfig,
) -> (Vec<tempfile::TempDir>, Vec<Arc<AofSublog>>) {
  let mut dirs = Vec::new();
  let mut backends = Vec::new();
  for i in 0..count {
    let dir = tempfile::tempdir().expect("tempdir");
    let dev = Arc::new(
      SegmentedDevice::single_file(dir.path().join(format!("{tag}_{i}.wal"))).expect("dev"),
    );
    let wal = WalLog::new(dev, config).expect("wal");
    backends.push(Arc::new(WaofSublog::new(Arc::new(wal))));
    dirs.push(dir);
  }
  (dirs, backends)
}

fn chunked_upsert_entry() -> (Vec<tempfile::TempDir>, Vec<Vec<u8>>) {
  let options = RuntimeServerOptions::default();
  let (dirs, backends) = test_backends_with_config("chunk_reader", 1, small_page_config());
  let log = GarnetLog::new(&options, backends, None).expect("构造 GarnetLog");
  let value = vec![b'x'; BIG_VALUE_LEN];
  log
    .enqueue(&RecordShape {
      op_type: AofEntryType::ObjectStoreUpsert,
      version: 3,
      session_id: 9,
      key: b"big",
      value: &value,
      input: &[],
      database_id: 0,
    })
    .unwrap();
  let mut payloads = Vec::new();
  log.scan_single_with(0, 0, i64::MAX, |r| {
    if !waof::is_commit_frame(&r.payload) {
      payloads.push(r.payload.clone());
    }
    true
  });
  (dirs, payloads)
}

#[test]
fn first_and_next_component_progression() {
  let chunk_header = AofChunkHeader {
    overflow_key_length: 3,
    overflow_value_length: 5,
    input_length: 2,
    object_id: 1,
    key_hash: 7,
  };
  let acc = ChunkedAccumulator::new(AofEntryType::StoreUpsert, &chunk_header);
  assert_eq!(acc.first_component(), Component::Key);
  let mut acc = acc;
  assert!(acc.next_component());
  assert_eq!(acc.current_component, Component::Value);
  assert!(acc.next_component());
  assert_eq!(acc.current_component, Component::Input);
  assert!(!acc.next_component());
  assert!(acc.is_complete);

  // delete 形状：无 value/input
  let del = AofChunkHeader {
    overflow_key_length: 3,
    overflow_value_length: 0,
    input_length: 0,
    object_id: 2,
    key_hash: 8,
  };
  let mut acc = ChunkedAccumulator::new(AofEntryType::StoreDelete, &del);
  assert!(!acc.next_component());
  assert!(acc.is_complete);
}

#[test]
fn feed_splits_across_component_boundaries() {
  let chunk_header = AofChunkHeader {
    overflow_key_length: 2,
    overflow_value_length: 3,
    input_length: 2,
    object_id: 3,
    key_hash: 9,
  };
  let mut acc = ChunkedAccumulator::new(AofEntryType::StoreUpsert, &chunk_header);
  // 单块数据连拼 key+value+input
  assert!(acc.feed(b"ab"));
  assert!(acc.feed(b"cde"));
  assert!(acc.feed(b"fg"));
  assert!(acc.all_components_filled());
  assert_eq!(acc.key_span(), b"ab");
  assert_eq!(acc.value_span(), b"cde");
  assert_eq!(acc.input_span(), b"fg");
  assert!(acc.verify().is_ok());
}

#[test]
fn verify_detects_length_mismatch() {
  let chunk_header = AofChunkHeader {
    overflow_key_length: 4,
    overflow_value_length: 0,
    input_length: 0,
    object_id: 4,
    key_hash: 1,
  };
  let mut acc = ChunkedAccumulator::new(AofEntryType::StoreDelete, &chunk_header);
  assert!(acc.feed(b"ab"));
  assert!(acc.verify().is_err(), "key 未积满应判不一致");
  assert!(acc.feed(b"cd"));
  assert!(acc.verify().is_ok());
  // 越界数据为损坏
  assert!(!acc.feed(b"x"));
}

#[test]
fn read_chunk_reassembles_from_cycle1_writer() {
  let (_dirs, records) = chunked_upsert_entry();
  assert!(records.len() >= 2, "大值拆首块 + 数据块");
  let mut reader = AofChunkedRecordReader::new();
  let mut completed = None;
  for record in &records {
    if let Ok(Some(acc)) = reader.read_chunk(record) {
      completed = Some(acc);
    }
  }
  let acc = completed.expect("全部块后应完成重组");
  assert_eq!(acc.op_type, AofEntryType::ObjectStoreUpsert);
  assert_eq!(acc.key_span(), b"big");
  let value: Vec<u8> = acc.object_value_bytes().into_owned();
  assert_eq!(value.len(), BIG_VALUE_LEN);
  assert_eq!(acc.session_id, 9);
  assert_eq!(acc.store_version, 3);
  assert_eq!(reader.in_progress.len(), 0);
}

#[test]
fn read_chunk_accumulates_until_complete() {
  let (_dirs, records) = chunked_upsert_entry();
  let mut reader = AofChunkedRecordReader::new();
  // 首块尚不完整
  assert!(reader.read_chunk(&records[0]).unwrap().is_none());
  assert_eq!(reader.in_progress.len(), 1);
  // 余块闭合
  for record in &records[1..] {
    if let Ok(Some(acc)) = reader.read_chunk(record) {
      assert_eq!(acc.key_span(), b"big");
      assert_eq!(reader.in_progress.len(), 0);
      return;
    }
  }
  panic!("数据块未能闭合重组");
}

#[test]
fn duplicate_chunk_after_completion_is_corrupt() {
  let (_dirs, records) = chunked_upsert_entry();
  let mut reader = AofChunkedRecordReader::new();
  let mut last = None;
  for record in &records {
    last = reader.read_chunk(record).unwrap();
  }
  assert!(last.is_some());
  // 已完成记录重复块为损坏（对标 C# ReadChunk:211 throw GarnetException，翻转既有静默续扫）
  assert!(matches!(
    reader.read_chunk(&records[0]),
    Err(AofChunkReadError::DuplicateChunk(_))
  ));
}

#[test]
fn accumulator_views_per_op_type() {
  let chunk_header = AofChunkHeader {
    overflow_key_length: 1,
    overflow_value_length: 2,
    input_length: 0,
    object_id: 1,
    key_hash: 1,
  };
  let mut acc = ChunkedAccumulator::new(AofEntryType::StoreUpsert, &chunk_header);
  assert!(acc.feed(b"k"));
  assert!(acc.feed(b"vv"));
  assert_eq!(acc.key_span(), b"k");
  assert_eq!(acc.value_span(), b"vv");
  assert!(acc.input_span().is_empty());

  let del_header = AofChunkHeader {
    overflow_key_length: 1,
    overflow_value_length: 0,
    input_length: 0,
    object_id: 2,
    key_hash: 1,
  };
  let mut del = ChunkedAccumulator::new(AofEntryType::StoreDelete, &del_header);
  assert!(del.feed(b"k"));
  assert_eq!(del.key_span(), b"k");
  assert!(del.value_span().is_empty());
}

/// 锁测 4(a)：残簿 + 同键后继组隔离验证
/// 喂大键 A 的部分分块帧后喂大键 B 的全帧（A 与 B 同键），
/// 断言 bigkey 终值等于 B 的值且 B 未被并入 A 的残簿（revert-proof）。
#[compio::test]
async fn residual_accumulator_does_not_poison_same_key_successor() -> aok::Void {
  let (_dir, store) = wtest_base::open_test_store_with_budget("aof-chunk-poison.db", 256 << 20)?;
  let options = RuntimeServerOptions::default();
  let (_dirs, backends) = test_backends_with_config("chunk_poison", 1, small_page_config());
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  ));
  let log = aof.log();

  let key = b"same-bigkey";
  let physical_key = wnode_test::physical_at(0, 0, key);
  let val_a = vec![b'A'; BIG_VALUE_LEN];
  let val_b = vec![b'B'; BIG_VALUE_LEN];

  // 1. 写入记录组 A
  log
    .enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 1,
      key: &physical_key,
      value: &val_a,
      input: &[],
      database_id: 0,
    })
    .unwrap();

  let mut frames_a = Vec::new();
  log.scan_single_with(0, 0, i64::MAX, |r| {
    if !waof::is_commit_frame(&r.payload) {
      frames_a.push((r.address, r.payload.clone()));
    }
    true
  });
  assert!(frames_a.len() >= 2, "大值 A 须分出至少 2 帧");

  // 2. 写入记录组 B (同键，新值)
  let addr_b_start = log.get_tail_address(0);
  log
    .enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 2,
      key: &physical_key,
      value: &val_b,
      input: &[],
      database_id: 0,
    })
    .unwrap();

  let mut frames_b = Vec::new();
  log.scan_single_with(0, addr_b_start, i64::MAX, |r| {
    if !waof::is_commit_frame(&r.payload) {
      frames_b.push((r.address, r.payload.clone()));
    }
    true
  });
  assert!(frames_b.len() >= 2, "大值 B 须分出至少 2 帧");

  // 验证写端为 A 和 B 分配了不同的组级唯一 object_id
  let (_, ch_a) = AofHeader::get_chunked_header_ref(&frames_a[0].1).expect("解析 A 头");
  let (_, ch_b) = AofHeader::get_chunked_header_ref(&frames_b[0].1).expect("解析 B 头");
  assert_eq!(ch_a.key_hash, ch_b.key_hash, "同键 key_hash 保持一致");
  assert_ne!(
    ch_a.object_id, ch_b.object_id,
    "记录组级唯一 object_id 必须互异"
  );

  // 3. 装配重放处理器与存储会话
  wnode_test::replay_target!(store, 1, vec![], storage, target);
  let processor = AofProcessor::new(Arc::clone(&aof));

  // 喂大键 A 的部分分块帧（首帧，模拟回放起点落在组中或网络断流残留的残簿）
  let is_ckpt = processor
    .process_aof_record_internal(0, &frames_a[0].1, true, frames_a[0].0 as i64, &target)
    .await?;
  assert!(!is_ckpt);

  // 验证 A 的残簿已在 reader 的 in_progress 中滞留
  assert_eq!(
    processor
      .coordinator()
      .context(0)
      .chunked_reader
      .in_progress
      .len(),
    1,
    "A 残簿滞留中"
  );

  // 4. 喂大键 B 的全部帧（A 与 B 同键）
  for (addr, payload) in &frames_b {
    processor
      .process_aof_record_internal(0, payload, true, *addr as i64, &target)
      .await?;
  }

  // 5. 断言 bigkey 终值等于 B 的值（val_b），且 B 未被并入 A 的残簿
  let got = storage.read_string(key).await?.expect("bigkey 须落库");
  assert_eq!(got, val_b, "bigkey 终值必须等于后继组 B 的值");

  // A 的残簿仍在 in_progress 中独立滞留，未被 B 并入或合流
  assert_eq!(
    processor
      .coordinator()
      .context(0)
      .chunked_reader
      .in_progress
      .len(),
    1,
    "A 的残簿依然独立滞留，未被 B 污染"
  );

  OK
}

/// 锁测 4(b) 臂一：重复块响亮上抛断言（翻转既有静默续扫断言）
#[compio::test]
async fn four_corrupt_states_duplicate_chunk_loud_error() -> aok::Void {
  let (_dir, store) = wtest_base::open_test_store_with_budget("aof-chunk-dup.db", 256 << 20)?;
  let options = RuntimeServerOptions::default();
  let (_dirs, backends) = test_backends_with_config("chunk_dup", 1, small_page_config());
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  ));
  let log = aof.log();
  let value = vec![b'd'; BIG_VALUE_LEN];
  let key = b"k_dup";
  let physical_key = wnode_test::physical_at(0, 0, key);
  log
    .enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 1,
      key: &physical_key,
      value: &value,
      input: &[],
      database_id: 0,
    })
    .unwrap();

  let mut frames = Vec::new();
  log.scan_single_with(0, 0, i64::MAX, |r| {
    if !waof::is_commit_frame(&r.payload) {
      frames.push((r.address, r.payload.clone()));
    }
    true
  });

  wnode_test::replay_target!(store, 1, vec![], storage, target);
  let processor = AofProcessor::new(Arc::clone(&aof));

  // 完整重放所有帧以完成该组记录
  for (addr, payload) in &frames {
    processor
      .process_aof_record_internal(0, payload, true, *addr as i64, &target)
      .await?;
  }

  // 重放已完成记录的块：必须响亮上抛 DuplicateChunk（翻转既有静默续扫）
  let res = processor
    .process_aof_record_internal(0, &frames[0].1, true, frames[0].0 as i64, &target)
    .await;
  assert!(
    matches!(
      res,
      Err(AofReplayError::ChunkRead(
        AofChunkReadError::DuplicateChunk(_)
      ))
    ),
    "已完成记录的重复块必须上抛 DuplicateChunk 错误: got {res:?}"
  );
  OK
}

/// 锁测 4(b) 臂二：段长越界响亮上抛断言
#[compio::test]
async fn four_corrupt_states_segment_out_of_bounds_loud_error() -> aok::Void {
  let (_dir, store) =
    wtest_base::open_test_store_with_budget("aof-chunk-oob.db", 128 * 1024 * 1024)?;
  let options = RuntimeServerOptions::default();
  let (_dirs, backends) = test_backends_with_config("chunk_oob", 1, small_page_config());
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  ));
  let log = aof.log();
  let value = vec![b'o'; BIG_VALUE_LEN];
  let key = b"k_oob";
  let physical_key = wnode_test::physical_at(0, 0, key);
  log
    .enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 1,
      key: &physical_key,
      value: &value,
      input: &[],
      database_id: 0,
    })
    .unwrap();

  let mut frames = Vec::new();
  log.scan_single_with(0, 0, i64::MAX, |r| {
    if !waof::is_commit_frame(&r.payload) {
      frames.push((r.address, r.payload.clone()));
    }
    true
  });

  let session = store.new_session()?;
  let storage = StorageSession::new(session.enter_batch());
  let target = ReplayTarget {
    session: &storage,
    store: Arc::clone(&store),
    aof_floor: vec![],
  };
  let processor = AofProcessor::new(Arc::clone(&aof));

  // 截断帧负载使其长度不足以容纳分块帧头（段长越界）
  let truncated_payload = &frames[0].1[..AofHeader::TOTAL_SIZE + 5];
  let res = processor
    .process_aof_record_internal(0, truncated_payload, true, frames[0].0 as i64, &target)
    .await;
  assert!(
    matches!(
      res,
      Err(AofReplayError::ChunkRead(
        AofChunkReadError::SegmentOutOfBounds { .. }
      ))
    ),
    "段长越界必须上抛 SegmentOutOfBounds 错误: got {res:?}"
  );
  OK
}

/// 锁测 4(b) 臂三：组件溢出响亮上抛断言
#[compio::test]
async fn four_corrupt_states_component_overflow_loud_error() -> aok::Void {
  let (_dir, store) =
    wtest_base::open_test_store_with_budget("aof-chunk-ovf.db", 128 * 1024 * 1024)?;
  let options = RuntimeServerOptions::default();
  let (_dirs, backends) = test_backends_with_config("chunk_ovf", 1, small_page_config());
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  ));
  let log = aof.log();
  let value = vec![b'v'; BIG_VALUE_LEN];
  let key = b"k_ovf";
  let physical_key = wnode_test::physical_at(0, 0, key);
  log
    .enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 1,
      key: &physical_key,
      value: &value,
      input: &[],
      database_id: 0,
    })
    .unwrap();

  let mut frames = Vec::new();
  log.scan_single_with(0, 0, i64::MAX, |r| {
    if !waof::is_commit_frame(&r.payload) {
      frames.push((r.address, r.payload.clone()));
    }
    true
  });

  let session = store.new_session()?;
  let storage = StorageSession::new(session.enter_batch());
  let target = ReplayTarget {
    session: &storage,
    store: Arc::clone(&store),
    aof_floor: vec![],
  };
  let processor = AofProcessor::new(Arc::clone(&aof));

  // 先喂前面的帧
  for (addr, frame_bytes) in &frames[..frames.len() - 1] {
    processor
      .process_aof_record_internal(0, frame_bytes, true, *addr as i64, &target)
      .await?;
  }

  // 在最后一帧末尾附加额外字节（超出声明长度造成组件溢出）
  let mut overflow_frame = frames.last().unwrap().1.clone();
  overflow_frame.extend_from_slice(b"overflow_extra_bytes_123");

  let res = processor
    .process_aof_record_internal(
      0,
      &overflow_frame,
      true,
      frames.last().unwrap().0 as i64,
      &target,
    )
    .await;
  assert!(
    matches!(
      res,
      Err(AofReplayError::ChunkRead(
        AofChunkReadError::ComponentOverflow { .. }
      ))
    ),
    "组件溢出必须上抛 ComponentOverflow 错误: got {res:?}"
  );
  OK
}

/// 锁测 4(b) 臂四：verify 长度不符响亮上抛断言
#[compio::test]
async fn four_corrupt_states_verify_mismatch_loud_error() -> aok::Void {
  let (_dir, store) =
    wtest_base::open_test_store_with_budget("aof-chunk-vfy.db", 128 * 1024 * 1024)?;
  let options = RuntimeServerOptions::default();
  let (_dirs, backends) = test_backends_with_config("chunk_vfy", 1, small_page_config());
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  ));
  let log = aof.log();
  let value = vec![b'm'; BIG_VALUE_LEN];
  let key = b"k_vfy";
  let physical_key = wnode_test::physical_at(0, 0, key);
  log
    .enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 1,
      key: &physical_key,
      value: &value,
      input: &[],
      database_id: 0,
    })
    .unwrap();

  let mut frames = Vec::new();
  log.scan_single_with(0, 0, i64::MAX, |r| {
    if !waof::is_commit_frame(&r.payload) {
      frames.push((r.address, r.payload.clone()));
    }
    true
  });
  assert!(frames.len() >= 2);

  let session = store.new_session()?;
  let storage = StorageSession::new(session.enter_batch());
  let target = ReplayTarget {
    session: &storage,
    store: Arc::clone(&store),
    aof_floor: vec![],
  };
  let processor = AofProcessor::new(Arc::clone(&aof));

  // 喂首帧
  processor
    .process_aof_record_internal(0, &frames[0].1, true, frames[0].0 as i64, &target)
    .await?;

  // 构造第二帧：篡改其分块头中的 declared overflow_value_length，使其与首帧不一致
  let mut corrupted_second = frames[1].1.clone();
  // 分块头在 AofHeader::TOTAL_SIZE 之后，overflow_value_length 在偏移 4..8
  let chunk_offset = AofHeader::TOTAL_SIZE;
  let corrupted_val_len = (BIG_VALUE_LEN as u32 + 9999).to_le_bytes();
  corrupted_second[chunk_offset + 4..chunk_offset + 8].copy_from_slice(&corrupted_val_len);

  let res = processor
    .process_aof_record_internal(0, &corrupted_second, true, frames[1].0 as i64, &target)
    .await;
  assert!(
    matches!(
      res,
      Err(AofReplayError::ChunkRead(
        AofChunkReadError::VerifyMismatch { .. }
      ))
    ),
    "校验长度不符必须上抛 VerifyMismatch 错误: got {res:?}"
  );
  OK
}

/// 分片拓扑两枚物理子日志：一热一冷
const SEED_SUBLOG_COUNT: usize = 2;
const HOT_SUBLOG: usize = 1;
const COLD_SUBLOG: usize = 0;
/// 预热记录尺寸与条数（抬热子日志尾，夹具前提在测试内断言钉死）
const FILLER_VALUE_LEN: usize = 64 * 1024;
const FILLER_COUNT: usize = 80;

/// 搜索路由到指定物理子日志的键（分片路由 = 键哈希对子日志数取模）
fn keys_routed_to(sublog: usize, prefix: &str, want: usize) -> Vec<Vec<u8>> {
  let mut keys = Vec::new();
  for i in 0..100_000usize {
    let key = format!("{prefix}_{i}").into_bytes();
    if (GarnetLog::hash(&key) as u64) % (SEED_SUBLOG_COUNT as u64) == sublog as u64 {
      keys.push(key);
      if keys.len() == want {
        return keys;
      }
    }
  }
  panic!("路由键搜索失败: 未凑足 {want} 枚落到子日志 {sublog} 的键");
}

/// 冷子日志上已派发过的分块组身份（按出现顺序去重）
fn chunked_ids_of(log: &GarnetLog, sublog: usize) -> Vec<u64> {
  let mut ids = Vec::new();
  log.scan_single_with(sublog, 0, i64::MAX, |r| {
    if !waof::is_commit_frame(&r.payload)
      && let Some((_, header)) = AofHeader::get_chunked_header_ref(&r.payload)
      && !ids.contains(&header.object_id)
    {
      ids.push(header.object_id);
    }
    true
  });
  ids
}

fn enqueue_chunked(log: &GarnetLog, key: &[u8], fill: u8) {
  let value = vec![fill; BIG_VALUE_LEN];
  log
    .enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 1,
      key,
      value: &value,
      input: &[],
      database_id: 0,
    })
    .expect("大值分块入队");
}

fn enqueue_filler(log: &GarnetLog, key: &[u8], value: &[u8]) {
  log
    .enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 1,
      key,
      value,
      input: &[],
      database_id: 0,
    })
    .expect("预热记录入队");
}

/// 锁测：分块组身份序号逐物理子日志分桶起发（全局最大尾起发形态必红）
///
/// 夹具：预热实例只写热子日志抬高其尾地址；实例 a 在冷子日志派两组大值身份后
/// 退役；实例 b 以同一批后端再派一组。全局形态两次构造看到的最大尾同为热尾
/// （a 未触碰热子日志，其尾在 a 期间不变），故 b 的起发点回落到 a 已派发的区间
/// 并重发 a 的首枚 id——长驻读取器中前实例的未完成残簿即与后继组交叉污染；
/// 分桶形态下 b 从冷子日志自身末态尾起发，与前实例在同子日志的派发区间不相交。
#[test]
fn chunk_group_ids_seeded_per_sublog_across_restart() {
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: SEED_SUBLOG_COUNT as _,
    aof_replay_task_count: 2,
    ..RuntimeServerOptions::default()
  };
  let (_dirs, backends) =
    test_backends_with_config("chunk_seed", SEED_SUBLOG_COUNT, small_page_config());
  let seq_gen = || Some(Arc::new(SequenceNumberGenerator::new(0)));

  let hot_keys = keys_routed_to(HOT_SUBLOG, "seed_hot", FILLER_COUNT);
  let cold_keys = keys_routed_to(COLD_SUBLOG, "seed_cold", 3);

  // 预热：仅抬热子日志尾
  {
    let log = GarnetLog::new(&options, backends.clone(), seq_gen()).expect("构造预热 GarnetLog");
    let value = vec![b'f'; FILLER_VALUE_LEN];
    for key in &hot_keys {
      enqueue_filler(&log, key, &value);
    }
  }

  // 实例 a：冷子日志派两组大值
  let a_ids = {
    let log = GarnetLog::new(&options, backends.clone(), seq_gen()).expect("构造 GarnetLog a");
    enqueue_chunked(&log, &cold_keys[0], b'a');
    enqueue_chunked(&log, &cold_keys[1], b'b');
    let ids = chunked_ids_of(&log, COLD_SUBLOG);
    assert_eq!(ids.len(), 2, "夹具：实例 a 应在冷子日志派发两枚互异组身份");
    ids
  };

  // 实例 b：同批后端再派一组冷大值
  let b_ids = {
    let log = GarnetLog::new(&options, backends.clone(), seq_gen()).expect("构造 GarnetLog b");
    assert!(
      log.get_tail_address(HOT_SUBLOG) > log.get_tail_address(COLD_SUBLOG),
      "夹具前提：热子日志尾须高于冷子日志末态尾，否则全局最大尾形态不会重发旧身份，本测失鉴别力"
    );
    enqueue_chunked(&log, &cold_keys[2], b'c');
    chunked_ids_of(&log, COLD_SUBLOG)
  };

  let b_id = *b_ids.last().expect("实例 b 应派发一枚组身份");
  assert!(
    !a_ids.contains(&b_id),
    "跨实例冷子日志组身份不得重发: a={a_ids:?} b={b_id}"
  );
  assert_eq!(b_ids.len(), 3, "冷子日志三组身份须互异");
}
