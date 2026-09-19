//! 范围索引 AOF 复制面集成测试（自 src 内嵌测试迁出）
//!
//! 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs：
//! RI.SET / RI.DEL 的 StoreRMW 入队形状、迁移流分块灌入 AOF（首尾标志）、
//! 分块边界校验与空 AOF / 缺文件失败路径、流块标志位面与哨兵/后端映射。

use std::{fs, sync::Arc};

use tempfile::TempDir;
use waof::{AofEntryType, AofHeader, WalRecord};
use wbftree::{MIN_CHUNK_SIZE, RangeIndexManager, StorageBackendType};
use wcol::RespInputFlags;
use wconf::RuntimeServerOptions;
use wnode::{
  AofWriteContext, GarnetAppendOnlyFile, GarnetLog, ReplayInput,
  rangeindex::range_index_manager_replication::{
    RangeIndexManagerReplication, RangeIndexStreamArgs, STREAMED_PUBLISH_LOG_ARG,
  },
};
use wresp::command::RespCommand;

const HEADER_SIZE: usize = AofHeader::TOTAL_SIZE;

/// C# StreamedPublishLogArg 位面：IsLast = 1，IsFirst = 2（对齐 src 常量）
const STREAM_CHUNK_IS_FIRST_FLAG: i64 = 2;
const STREAM_CHUNK_IS_LAST_FLAG: i64 = 1;

/// C# RespInputFlags.Deterministic（src deterministic_flags 的同构表达）
fn deterministic_flags() -> u8 {
  RespInputFlags::DETERMINISTIC.bits()
}

/// 流块 arg1 首尾标志打包（src pack_stream_chunk_flags 的同构表达）
fn pack_stream_chunk_flags(is_first: bool, is_last: bool) -> i64 {
  ((is_last as i64) * STREAM_CHUNK_IS_LAST_FLAG) | ((is_first as i64) * STREAM_CHUNK_IS_FIRST_FLAG)
}

/// 流块 arg1 首尾标志解包（src unpack_stream_chunk_flags 的同构表达）
fn unpack_stream_chunk_flags(arg1: i64) -> (bool, bool) {
  (
    arg1 & STREAM_CHUNK_IS_FIRST_FLAG != 0,
    arg1 & STREAM_CHUNK_IS_LAST_FLAG != 0,
  )
}

/// 内存 AOF（无盘拓扑；测试断言入队形状与地址推进）
fn memory_aof() -> Arc<GarnetAppendOnlyFile> {
  let options = RuntimeServerOptions::default();
  Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(
      GarnetLog::new(
        &options,
        {
          let (_dirs, backends) = wnode_test::test_sublogs("ri_repl", 1);
          backends
        },
        None,
      )
      .expect("构造 GarnetLog"),
    ),
    &options,
    None,
  ))
}

fn replication_in(dir: &TempDir) -> RangeIndexManagerReplication {
  RangeIndexManagerReplication::new(Arc::new(
    RangeIndexManager::new(dir.path().join("ri"), dir.path().join("cpr")).unwrap(),
  ))
}

/// 解析记录的 RI 输入段（payload = 头 + SpanByte key + input；
/// StoreRMW 形状无 value 组件）
fn parse_input(record: &WalRecord, key_len: usize) -> ReplayInput {
  let input_start = HEADER_SIZE + 4 + key_len;
  ReplayInput::deserialize(&record.payload[input_start..]).expect("ReplayInput roundtrip")
}

/// 解析记录的 SpanByte key 段
fn parse_key(record: &WalRecord) -> &[u8] {
  let len = u32::from_le_bytes(
    record.payload[HEADER_SIZE..HEADER_SIZE + 4]
      .try_into()
      .unwrap(),
  ) as usize;
  &record.payload[HEADER_SIZE + 4..HEADER_SIZE + 4 + len]
}

fn parse_header(record: &WalRecord) -> AofHeader {
  AofHeader::parse(&record.payload).expect("header decodable")
}

/// 分块边界：< 47 拒绝，47 恰好接受（C# MinChunkSize）
#[test]
fn chunk_size_boundary_rejects_below_trailer_size() {
  let dir = TempDir::new().unwrap();
  let replication = replication_in(&dir);
  assert!(
    replication
      .set_aof_stream_chunk_size(MIN_CHUNK_SIZE - 1)
      .is_err()
  );
  assert!(
    replication
      .set_aof_stream_chunk_size(MIN_CHUNK_SIZE)
      .is_ok()
  );
  assert_eq!(replication.aof_stream_chunk_size(), MIN_CHUNK_SIZE);
  assert!(replication.set_aof_stream_chunk_size(0).is_err());
  assert!(replication.set_aof_stream_chunk_size(256 * 1024).is_ok());
  assert_eq!(replication.aof_stream_chunk_size(), 256 * 1024);
}

/// RI.SET / RI.DEL 入队：StoreRMW 头 + key 领衔负载 + 确定性标志；NULL AOF /
/// 存储过程模式跳过；地址单调推进
#[test]
fn replicate_set_del_enqueue_shapes() {
  let aof = memory_aof();
  let dir = TempDir::new().unwrap();
  let replication = replication_in(&dir);

  // NULL AOF / 存储过程模式：跳过入队（返回 0）
  let ctx = AofWriteContext {
    version: 7,
    session_id: 3,
  };
  let none_ctx = AofWriteContext {
    version: 1,
    session_id: 1,
  };
  assert_eq!(
    replication
      .replicate_range_index_set(b"k", b"f", b"v", None, none_ctx, false)
      .unwrap(),
    0
  );
  assert_eq!(
    replication
      .replicate_range_index_set(b"k", b"f", b"v", Some(&aof), none_ctx, true)
      .unwrap(),
    0
  );
  assert_eq!(
    replication
      .replicate_range_index_del(b"k", b"f", None, none_ctx, false)
      .unwrap(),
    0
  );

  let a1 = replication
    .replicate_range_index_set(b"k", b"field-1", b"value-1", Some(&aof), ctx, false)
    .unwrap();
  let a2 = replication
    .replicate_range_index_del(b"k", b"field-2", Some(&aof), ctx, false)
    .unwrap();
  assert!(a1 >= 0 && a2 > a1, "addresses must advance monotonically");

  // 回读：两条目均 StoreRMW 头 + key 领衔负载 + 确定性标志
  let records = scan_records(aof.log());
  assert_eq!(records.len(), 2);
  for record in &records {
    let header = parse_header(record);
    assert_eq!(header.op_type, AofEntryType::StoreRMW as u8);
    assert_eq!(header.store_version, 7);
    assert_eq!(header.session_id, 3);
    assert_eq!(parse_key(record), b"k");
    let input = parse_input(record, 1);
    assert_eq!(input.flags, deterministic_flags());
    assert!(input.cmd == RespCommand::Riset || input.cmd == RespCommand::Ridel);
  }
  let first = parse_input(&records[0], 1);
  assert_eq!(first.cmd, RespCommand::Riset);
  assert_eq!(first.args, vec![b"field-1".to_vec(), b"value-1".to_vec()]);
  let second = parse_input(&records[1], 1);
  assert_eq!(second.cmd, RespCommand::Ridel);
  assert_eq!(second.args, vec![b"field-2".to_vec()]);
}

/// 单块流：首块与末块标志同时置位，块载荷为完整框
#[test]
fn single_chunk_stream_enqueues_with_first_and_last_flags() {
  let aof = memory_aof();
  let dir = TempDir::new().unwrap();
  let file_path = dir.path().join("snapshot.bftree");
  fs::write(&file_path, b"tiny-snapshot").unwrap();

  let replication = replication_in(&dir);
  let ctx = AofWriteContext {
    version: 5,
    session_id: 2,
  };
  replication
    .replicate_range_index_stream(
      RangeIndexStreamArgs {
        key: b"key",
        obj_type: 0,
        stub: &[9u8; 35],
        file_path: &file_path,
        ctx,
        chunk_size: 4096,
      },
      Some(&aof),
    )
    .unwrap();

  let records = scan_records(aof.log());
  assert_eq!(records.len(), 1, "small snapshot fits one chunk");
  let header = parse_header(&records[0]);
  assert_eq!(header.op_type, AofEntryType::RangeIndexStreamChunk as u8);
  let parsed = parse_input(&records[0], 3);
  assert_eq!(parsed.cmd, RespCommand::None);
  // 单块流：首块与末块标志同时置位
  assert_eq!(
    parsed.arg1,
    STREAM_CHUNK_IS_FIRST_FLAG | STREAM_CHUNK_IS_LAST_FLAG
  );
  // 块载荷 = keyLen(4)+key(3)+fileLen(8)+file(13)+hash(8)+stubLen(4)+stub(35)
  assert_eq!(
    parsed.args.first().map(Vec::len),
    Some(4 + 3 + 8 + 13 + 8 + 4 + 35)
  );
}

/// 多块流：仅首块带首标志、仅末块带末标志
#[test]
fn multi_chunk_stream_marks_only_outer_flags() {
  let aof = memory_aof();
  let dir = TempDir::new().unwrap();
  let file_path = dir.path().join("big.bftree");
  // 40KB 快照、128B 分块 → 严格多块
  fs::write(&file_path, vec![0xABu8; 40 * 1024]).unwrap();

  let replication = replication_in(&dir);
  let ctx = AofWriteContext {
    version: 2,
    session_id: 1,
  };
  replication
    .replicate_range_index_stream(
      RangeIndexStreamArgs {
        key: b"k",
        obj_type: 0,
        stub: &[7u8; 35],
        file_path: &file_path,
        ctx,
        chunk_size: 128,
      },
      Some(&aof),
    )
    .unwrap();

  let all_records = scan_records(aof.log());
  let records: Vec<_> = all_records
    .iter()
    .filter(|r| !waof::is_commit_frame(&r.payload))
    .collect();
  assert!(records.len() > 10, "expected strictly multi-chunk stream");
  for (i, record) in records.iter().enumerate() {
    let arg1 = parse_input(record, 1).arg1;
    let is_first = arg1 & STREAM_CHUNK_IS_FIRST_FLAG != 0;
    let is_last = arg1 & STREAM_CHUNK_IS_LAST_FLAG != 0;
    assert_eq!(is_first, i == 0, "only first chunk carries first flag");
    assert_eq!(
      is_last,
      i == records.len() - 1,
      "only last chunk carries last flag"
    );
  }
}

/// NULL AOF：告警后安全 no-op
#[test]
fn null_aof_stream_is_warning_noop() {
  let dir = TempDir::new().unwrap();
  let file_path = dir.path().join("s.bftree");
  fs::write(&file_path, b"payload").unwrap();
  let replication = replication_in(&dir);
  assert!(
    replication
      .replicate_range_index_stream(
        RangeIndexStreamArgs {
          key: b"k",
          obj_type: 0,
          stub: &[0u8; 35],
          file_path: &file_path,
          ctx: AofWriteContext {
            version: 1,
            session_id: 1,
          },
          chunk_size: 4096,
        },
        None,
      )
      .is_ok()
  );
}

/// 快照文件缺失：流式灌入失败上抛
#[test]
fn missing_file_fails_stream() {
  let aof = memory_aof();
  let dir = TempDir::new().unwrap();
  let replication = replication_in(&dir);
  let result = replication.replicate_range_index_stream(
    RangeIndexStreamArgs {
      key: b"k",
      obj_type: 0,
      stub: &[0u8; 35],
      file_path: &dir.path().join("absent.bftree"),
      ctx: AofWriteContext {
        version: 1,
        session_id: 1,
      },
      chunk_size: 4096,
    },
    Some(&aof),
  );
  assert!(result.is_err());
}

/// 流块 arg1 位面 round-trip：IsLast = 1，IsFirst = 2，可叠加（C# 位面）
#[test]
fn stream_chunk_flags_pack_roundtrip() {
  assert_eq!(pack_stream_chunk_flags(false, false), 0);
  assert_eq!(
    pack_stream_chunk_flags(true, false),
    STREAM_CHUNK_IS_FIRST_FLAG
  );
  assert_eq!(
    pack_stream_chunk_flags(false, true),
    STREAM_CHUNK_IS_LAST_FLAG
  );
  assert_eq!(pack_stream_chunk_flags(true, true), 3);
  for (first, last) in [(false, false), (true, false), (false, true), (true, true)] {
    let (f, l) = unpack_stream_chunk_flags(pack_stream_chunk_flags(first, last));
    assert_eq!((f, l), (first, last));
  }
}

/// 迁移发布哨兵与后端字节映射对齐 C#
///（long.MinValue 哨兵；0=Disk，1=Memory）
#[test]
fn sentinel_and_backend_mapping_match_csharp() {
  assert_eq!(STREAMED_PUBLISH_LOG_ARG, i64::MIN);
  assert_eq!(StorageBackendType::from_u8(0), StorageBackendType::Disk);
  assert_eq!(StorageBackendType::from_u8(1), StorageBackendType::Memory);
  assert_eq!(StorageBackendType::Disk.to_u8(), 0);
  assert_eq!(StorageBackendType::Memory.to_u8(), 1);
}

/// 闭包收集扫描（等价旧 scan_single Vec 面，测试专用）
fn scan_records(log: &GarnetLog) -> Vec<WalRecord> {
  let mut records = Vec::new();
  log.scan_single_with(0, 0, i64::MAX, |rec| {
    if !waof::is_commit_frame(&rec.payload) {
      records.push(rec.clone());
    }
    true
  });
  records
}
