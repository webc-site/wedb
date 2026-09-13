//! 范围索引分块流（序列化 / 反序列化 / 迁移读取器）集成测试
//!
//! 自 src 内嵌测试整体迁出：多块流重组、文件字节落盘、校验失败检测、
//! 终态拒绝与迁移读取器推进协议（对标 libs/server/Resp/RangeIndex/
//! RangeIndexChunked{Serializer,Deserializer}.cs 与 RangeIndexMigrationReader.cs）。

use std::{fs, io};

use tempfile::tempdir;
use wnode::resp::rangeindex::{
  range_index_chunked_deserializer::RangeIndexChunkedDeserializer,
  range_index_chunked_serializer::{MIN_CHUNK_SIZE, RangeIndexChunkedSerializer},
  range_index_migration_reader::{DEFAULT_FILE_READ_BUFFER_SIZE, RangeIndexMigrationReader},
};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

// ======================== 序列化器（RangeIndexChunkedSerializer） ========================

#[test]
fn move_next_frames_key_header_first() {
  let mut s = RangeIndexChunkedSerializer::new(b"key", &[0u8; 35], 0);
  assert!(!s.is_complete());
  let mut dest = [0u8; 64];
  let n = s.move_next(&mut dest).unwrap();
  // 首块至少含 4B keyLen + key 字节；小端编码长度
  assert_eq!(u32::from_le_bytes([dest[0], dest[1], dest[2], dest[3]]), 3);
  assert!(n >= 4 + 3);
}

#[test]
fn move_next_zero_progress_on_undersized_destination() {
  let mut s = RangeIndexChunkedSerializer::new(b"key", &[0u8; 35], 16);
  let mut dest = [0u8; 2];
  // 2 字节装不下 4B keyLen 头：零写入（不推进阶段）
  assert_eq!(s.move_next(&mut dest).unwrap(), 0);
}

#[test]
fn move_next_after_complete_is_contract_violation() {
  let mut s = RangeIndexChunkedSerializer::new(b"", &[0u8; 35], 0);
  let mut dest = [0u8; MIN_CHUNK_SIZE];
  // 供给完全部 0 字节文件数据后一路推进至 Done
  loop {
    let n = s.move_next(&mut dest).unwrap();
    if s.is_complete() {
      break;
    }
    assert!(n > 0);
  }
  let err = s.move_next(&mut dest).unwrap_err();
  assert!(err.to_string().contains("already completed"));
}

#[test]
fn supply_file_data_feeds_file_phase() {
  let mut s = RangeIndexChunkedSerializer::new(b"k", &[0u8; 35], 10);
  assert!(!s.needs_file_data()); // 尚在 KeyHeader 阶段
  let mut dest = vec![0u8; MIN_CHUNK_SIZE];
  // 推进到 FileData 阶段（键 1 字节 + 头可同块放下）
  s.move_next(&mut dest).unwrap();
  if !s.is_complete() && s.needs_file_data() {
    s.supply_file_data(&[7u8; 10]);
    assert_eq!(s.file_data_remaining(), 10);
  }
  assert_eq!(s.total_file_bytes(), 10);
}

#[test]
fn write_trailer_validates_capacity_without_touching_state() {
  let s = RangeIndexChunkedSerializer::new(b"k", &[0u8; 35], 0);
  let mut small = [0u8; 8 + 4 + 34];
  assert!(s.write_trailer(&mut small, &[0u8; 35]).is_err());
  let mut ok = [0u8; MIN_CHUNK_SIZE];
  assert_eq!(
    s.write_trailer(&mut ok, &[0u8; 35]).unwrap(),
    MIN_CHUNK_SIZE
  );
  // 校验不旁路推进状态机
  assert!(!s.is_complete());
}

// ======================== 反序列化器（RangeIndexChunkedDeserializer） ========================

/// 引擎序列化器（构造合法流，勿再手写帧格式）
fn frame_stream(key: &[u8], stub: &[u8], file: &[u8], chunk_size: usize) -> Vec<Vec<u8>> {
  let mut serializer = wbftree::RangeIndexChunkedSerializer::new(key, stub, file.len() as u64);
  let mut chunks = Vec::new();
  let mut supplied = 0usize;
  let mut dest = vec![0u8; chunk_size];
  while !serializer.is_complete() {
    if serializer.needs_file_data() && !file.is_empty() {
      let n = (file.len() - supplied).min(5);
      serializer.supply_file_data(&file[supplied..supplied + n]);
      supplied += n;
    }
    dest.fill(0);
    let written = serializer.move_next(&mut dest).unwrap();
    assert!(written > 0);
    chunks.push(dest[..written].to_vec());
  }
  chunks
}

#[test]
fn reassembles_multi_chunk_stream() {
  let dir = tempdir().unwrap();
  let temp = dir.path().join("reassembly.bftree");
  let key = b"idx-key";
  let stub = [0xABu8; 35];
  let file: Vec<u8> = (0..1000u32).map(|i| i as u8).collect();

  let chunks = frame_stream(key, &stub, &file, 64);
  assert!(chunks.len() > 3, "expected genuinely multi-chunk stream");

  let mut d = RangeIndexChunkedDeserializer::new(&temp).unwrap();
  for chunk in &chunks {
    assert!(d.process_chunk(chunk).unwrap(), "chunk rejected");
  }
  assert!(d.is_complete());
  assert!(!d.has_error());
  assert_eq!(d.key(), &key[..]);
  assert_eq!(d.stub(), &stub);
  assert_eq!(d.parse_trailer().unwrap(), 35);
  // 临时文件已落盘且长度一致（CloseStream 已内联执行）
  assert!(d.close_stream());
  assert_eq!(fs::read(&temp).unwrap(), file);
  d.dispose();
  assert!(!temp.exists(), "dispose removes temp file");
}

#[test]
fn file_bytes_written_tracks_progress() {
  let dir = tempdir().unwrap();
  let temp = dir.path().join("progress.bftree");
  let stub = [1u8; 35];
  let file = vec![9u8; 300];

  let chunks = frame_stream(b"k", &stub, &file, 47);
  let mut d = RangeIndexChunkedDeserializer::new(&temp).unwrap();
  // 文件段开启前无临时文件
  assert!(!d.close_stream());
  assert_eq!(d.write_file_bytes(), 0);
  // 逐块喂入：已落盘字节数单调递增至文件总长
  let mut last = 0u64;
  for chunk in chunks.iter().take(chunks.len() - 1) {
    d.process_chunk(chunk).unwrap();
    let written = d.write_file_bytes();
    assert!(written >= last);
    last = written;
  }
  d.process_chunk(chunks.last().unwrap()).unwrap();
  assert!(d.is_complete());
  assert_eq!(d.write_file_bytes(), 300);
  assert!(d.close_stream());
  d.dispose();
}

#[test]
fn empty_chunks_are_noops() {
  let dir = tempdir().unwrap();
  let mut d = RangeIndexChunkedDeserializer::new(dir.path().join("noop.bftree")).unwrap();
  // 空块：合法且无进展（C# 语义：非错误）
  assert!(d.process_chunk(&[]).unwrap());
  assert!(!d.is_complete());
  assert!(!d.has_error());
  d.dispose();
}

#[test]
fn corrupt_checksum_is_detected() {
  let dir = tempdir().unwrap();
  let mut chunks = frame_stream(b"k", &[2u8; 35], &[3u8; 64], 47);
  // 篡改末块首字节：末块必含尾部框，首字节或为哈希字节（直接失配）或为
  // 文件尾字节（重算哈希失配）——两种布局下校验必败
  let last = chunks.last_mut().unwrap();
  last[0] ^= 0xFF;

  let mut d = RangeIndexChunkedDeserializer::new(dir.path().join("bad.bftree")).unwrap();
  for chunk in chunks.iter().take(chunks.len() - 1) {
    assert!(d.process_chunk(chunk).unwrap());
  }
  assert!(!d.process_chunk(&chunks[chunks.len() - 1]).unwrap());
  assert!(d.has_error());
  assert!(d.take_error().is_some());
  assert!(!d.is_complete());
  d.dispose();
}

#[test]
fn feeding_after_terminal_state_is_rejected() {
  let dir = tempdir().unwrap();
  let chunks = frame_stream(b"k", &[5u8; 35], &[6u8; 16], 47);
  let mut d = RangeIndexChunkedDeserializer::new(dir.path().join("done.bftree")).unwrap();
  for chunk in &chunks {
    d.process_chunk(chunk).unwrap();
  }
  assert!(d.is_complete());
  // 完成后再喂块：拒绝但状态保持完成
  assert!(!d.process_chunk(b"trailing").unwrap());
  assert!(d.is_complete());
  d.dispose();
  // 已释放后再喂块：同样拒绝
  assert!(!d.process_chunk(&chunks[0]).unwrap());
}

#[test]
fn std_file_write_replay_guard() {
  // WriteFileBytes 语义对照：文件字节按序落盘（引擎经 File 写入）
  let dir = tempdir().unwrap();
  let p = dir.path().join("w.bftree");
  {
    let mut f = fs::File::create(&p).unwrap();
    std::io::Write::write_all(&mut f, &[1, 2, 3]).unwrap();
  }
  assert_eq!(fs::read(&p).unwrap(), vec![1, 2, 3]);
}

// ======================== 迁移读取器（RangeIndexMigrationReader） ========================

/// 从内存字节源构造读取器
fn reader_for<'a>(
  file: &'a [u8],
  key: &[u8],
  stub: &[u8],
  temp: Option<std::path::PathBuf>,
) -> RangeIndexMigrationReader<&'a [u8]> {
  let serializer = RangeIndexChunkedSerializer::new(key, stub, file.len() as u64);
  RangeIndexMigrationReader::new(serializer, file, temp, DEFAULT_FILE_READ_BUFFER_SIZE).unwrap()
}

#[test]
fn drives_stream_to_completion_in_destination_sized_chunks() {
  let stub = [0x11u8; 35];
  let file: Vec<u8> = (0..2000u32).map(|i| i as u8).collect();
  let mut r = reader_for(&file, b"idx", &stub, None);

  assert_eq!(r.total_file_bytes(), 2000);
  let mut out = Vec::new();
  let mut buf = vec![0u8; MIN_CHUNK_SIZE + 3];
  while !r.is_complete() {
    let written = r.read_next_chunk(&mut buf).unwrap();
    assert!(written > 0, "incomplete stream must make progress");
    out.extend_from_slice(&buf[..written]);
  }
  // 流完成的标志：键头 + 键 + 文件长头 + 文件字节 + 尾部框 全部框出
  let key_len = u32::from_le_bytes([out[0], out[1], out[2], out[3]]) as usize;
  assert_eq!(&out[4..4 + key_len], b"idx");
  // 尾部框为流的最后 47 字节，其中末 35 字节为存根
  assert_eq!(&out[out.len() - 35..], &stub);
  r.dispose();
}

#[test]
fn validate_destination_rejects_undersized_buffer() {
  let r = reader_for(b"data", b"k", &[0u8; 35], None);
  assert!(r.validate_destination(MIN_CHUNK_SIZE - 1).is_err());
  assert!(r.validate_destination(MIN_CHUNK_SIZE).is_ok());
  // 经读取入口同样拒绝
  let mut r2 = reader_for(b"data", b"k", &[0u8; 35], None);
  let mut small = vec![0u8; MIN_CHUNK_SIZE - 1];
  assert!(r2.read_next_chunk(&mut small).is_err());
}

#[test]
fn truncated_file_fails_supply() {
  // 声明 100 字节文件数据，源只有 10 字节 → 文件段中途读尽
  let src: &[u8] = &[7u8; 10];
  let serializer = RangeIndexChunkedSerializer::new(b"k", &[0u8; 35], 100);
  let mut r =
    RangeIndexMigrationReader::new(serializer, src, None, DEFAULT_FILE_READ_BUFFER_SIZE).unwrap();
  let mut buf = vec![0u8; 4096];
  let err = loop {
    match r.read_next_chunk(&mut buf) {
      Ok(_) => continue,
      Err(e) => break e,
    }
  };
  assert!(err.to_string().contains("truncated"));
  // SupplyFileDataOrThrow 校验形态：0 字节 + 剩余 > 0 → 截断
  assert!(r.supply_file_data_or_throw(0, 90).is_err());
  // 有字节供给时不报错
  assert!(r.supply_file_data_or_throw(16, 90).is_ok());
  // 剩余为 0 时读尽亦不算截断（文件段恰收满）
  assert!(r.supply_file_data_or_throw(0, 0).is_ok());
}

#[test]
fn dispose_deletes_owned_temp_snapshot() {
  let dir = tempdir().unwrap();
  let temp = dir.path().join("snapshot.bftree");
  fs::write(&temp, b"payload").unwrap();

  let mut r = reader_for(b"abc", b"k", &[0u8; 35], Some(temp.clone()));
  assert!(!r.is_complete());
  r.dispose();
  assert!(!temp.exists(), "owned temp snapshot must be deleted");
  // 幂等
  r.dispose();
}

#[test]
fn zero_read_buffer_rejected() {
  let serializer = RangeIndexChunkedSerializer::new(b"k", &[0u8; 35], 0);
  let err = RangeIndexMigrationReader::new(serializer, io::empty(), None, 0)
    .err()
    .expect("zero buffer must be rejected");
  assert!(err.to_string().contains("positive"));
}
