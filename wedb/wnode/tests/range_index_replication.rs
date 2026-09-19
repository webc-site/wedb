//! 范围索引 AOF 复制面集成测试（自 src 内嵌测试迁出）
//!
//! 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs：
//! 迁移流分块灌入 AOF（首尾标志）、空 AOF / 缺文件失败路径、
//! 流块标志位面与后端映射。

use std::{fs, sync::Arc};

use tempfile::TempDir;
use waof::{AofEntryType, AofHeader, WalRecord};
use wbftree::{RangeIndexManager, StorageBackendType};
use wconf::RuntimeServerOptions;
use wnode::{
  AofWriteContext, GarnetAppendOnlyFile, GarnetLog, ReplayInput,
  rangeindex::range_index_manager_replication::{
    RangeIndexManagerReplication, RangeIndexStreamArgs,
  },
};
use wresp::command::RespCommand;

const HEADER_SIZE: usize = AofHeader::TOTAL_SIZE;

/// C# StreamedPublishLogArg 位面：IsLast = 1，IsFirst = 2（对齐 src 常量；
/// Replace = 4 为本仓 AOF 流通道扩展，重灌流换入语义随块携载）
const STREAM_CHUNK_IS_FIRST_FLAG: i64 = 2;
const STREAM_CHUNK_IS_LAST_FLAG: i64 = 1;
const STREAM_CHUNK_REPLACE_FLAG: i64 = 4;

/// 流块 arg1 首尾与 replace 标志打包（src pack_stream_chunk_flags 的同构表达）
fn pack_stream_chunk_flags(is_first: bool, is_last: bool, replace: bool) -> i64 {
  ((is_last as i64) * STREAM_CHUNK_IS_LAST_FLAG)
    | ((is_first as i64) * STREAM_CHUNK_IS_FIRST_FLAG)
    | ((replace as i64) * STREAM_CHUNK_REPLACE_FLAG)
}

/// 流块 arg1 首尾与 replace 标志解包（src unpack_stream_chunk_flags 的同构表达）
fn unpack_stream_chunk_flags(arg1: i64) -> (bool, bool, bool) {
  (
    arg1 & STREAM_CHUNK_IS_FIRST_FLAG != 0,
    arg1 & STREAM_CHUNK_IS_LAST_FLAG != 0,
    arg1 & STREAM_CHUNK_REPLACE_FLAG != 0,
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

fn parse_header(record: &WalRecord) -> AofHeader {
  AofHeader::parse(&record.payload).expect("header decodable")
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
        replace: false,
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
        replace: false,
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
          replace: false,
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
      replace: false,
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

/// 流块 arg1 位面 round-trip：IsLast = 1，IsFirst = 2，Replace = 4，可叠加
#[test]
fn stream_chunk_flags_pack_roundtrip() {
  assert_eq!(pack_stream_chunk_flags(false, false, false), 0);
  assert_eq!(
    pack_stream_chunk_flags(true, false, false),
    STREAM_CHUNK_IS_FIRST_FLAG
  );
  assert_eq!(
    pack_stream_chunk_flags(false, true, false),
    STREAM_CHUNK_IS_LAST_FLAG
  );
  assert_eq!(pack_stream_chunk_flags(true, true, false), 3);
  assert_eq!(
    pack_stream_chunk_flags(true, true, true),
    3 | STREAM_CHUNK_REPLACE_FLAG
  );
  for (first, last, replace) in [
    (false, false, false),
    (true, false, false),
    (false, true, false),
    (true, true, false),
    (true, true, true),
    (false, false, true),
  ] {
    let (f, l, r) = unpack_stream_chunk_flags(pack_stream_chunk_flags(first, last, replace));
    assert_eq!((f, l, r), (first, last, replace));
  }
}

/// 后端字节映射对齐 C#（0=Disk，1=Memory）
#[test]
fn backend_mapping_match_csharp() {
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
