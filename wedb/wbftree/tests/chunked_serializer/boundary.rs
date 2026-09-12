use std::{
  fs::{self, File},
  io::{Seek, SeekFrom},
};

use aok::{OK, Result};
use wbftree::{
  DEFAULT_MIGRATION_CHUNK_SIZE, INDEX_SIZE_BYTES, RangeIndexChunkedDeserializer,
  RangeIndexChunkedSerializer, RangeIndexManager, compute_checksum,
};

use super::common::{
  TestDirGuard, build_payload, create_buffer, create_stub, random_bytes, serializer_move_next,
};

/// 缓冲区不足以容纳键头部时推迟至下一个数据块
#[test]
fn buffer_too_small_for_key_header_defers_to_next_chunk() -> Result<()> {
  let td = TestDirGuard::new("buf_small_key");
  let file_path = td.path().join("keyheader.bftree");
  fs::write(&file_path, vec![0u8; 32])?;

  let key = b"mykey";
  let stub = create_stub();

  let mut fs = File::open(&file_path)?;
  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub, 32);

  let mut tiny_buf = [0u8; 3];
  let written = serializer_move_next(&mut serializer, &mut tiny_buf, &mut fs)?;
  assert_eq!(0, written);
  assert!(!serializer.is_complete());

  let mut buffer = create_buffer();
  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  while !serializer.is_complete() {
    let len = serializer_move_next(&mut serializer, &mut buffer, &mut fs)?;
    assert!(deserializer.process_chunk(&buffer[..len])?);
  }

  assert!(deserializer.is_complete());
  assert!(!deserializer.has_error());
  assert_eq!(deserializer.key(), key);

  OK
}

/// 缓冲区不足以容纳文件头部时推迟至下一个数据块
#[test]
fn buffer_too_small_for_file_header_defers_to_next_chunk() -> Result<()> {
  let td = TestDirGuard::new("buf_small_file_hdr");
  let file_path = td.path().join("fileheader.bftree");
  let file_data = random_bytes(64, 42);
  fs::write(&file_path, &file_data)?;

  let key = b"k";
  let stub = create_stub();

  let mut fs = File::open(&file_path)?;
  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub, file_data.len() as u64);

  // Buffer 容纳 key header (4) + key (1)，但留给 file header 空间不足 8 字节
  let mut small_buf = vec![0u8; 4 + key.len() + 6];

  let written = serializer_move_next(&mut serializer, &mut small_buf, &mut fs)?;

  assert_eq!(4 + key.len(), written);
  assert!(!serializer.is_complete());

  let key_len = u32::from_le_bytes(small_buf[..4].try_into()?);
  assert_eq!(key.len() as u32, key_len);

  let mut buffer = create_buffer();
  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  assert!(deserializer.process_chunk(&small_buf[..written])?);

  while !serializer.is_complete() {
    let len = serializer_move_next(&mut serializer, &mut buffer, &mut fs)?;
    assert!(deserializer.process_chunk(&buffer[..len])?);
  }

  assert!(deserializer.is_complete());
  assert!(!deserializer.has_error());
  assert_eq!(deserializer.key(), key);

  OK
}

/// 缓冲区不足以容纳尾部数据时推迟至下一个数据块
#[test]
fn buffer_too_small_for_trailer_defers_to_next_chunk() -> Result<()> {
  let td = TestDirGuard::new("buf_small_trailer");
  let file_data = random_bytes(64, 42);
  let file_path = td.path().join("trailer.bftree");
  fs::write(&file_path, &file_data)?;

  let key = b"k";
  let stub = create_stub();
  let trailer_size = 8 + 4 + stub.len();

  let mut fs = File::open(&file_path)?;
  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub, file_data.len() as u64);

  let buf_size = 4 + key.len() + 8 + file_data.len() + trailer_size - 1;
  let mut buf = vec![0u8; buf_size];
  let written = serializer_move_next(&mut serializer, &mut buf, &mut fs)?;

  assert_eq!(4 + key.len() + 8 + file_data.len(), written);
  assert!(!serializer.is_complete());

  let mut buffer = create_buffer();
  let trailer_len = serializer_move_next(&mut serializer, &mut buffer, &mut fs)?;
  assert_eq!(trailer_size, trailer_len);
  assert!(serializer.is_complete());

  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  assert!(deserializer.process_chunk(&buf[..written])?);
  assert!(!deserializer.is_complete());
  assert!(deserializer.process_chunk(&buffer[..trailer_len])?);
  assert!(deserializer.is_complete());
  assert!(!deserializer.has_error());

  OK
}

/// 测试精确跨越分块阶段边界时的状态平滑转换
#[test]
fn exact_phase_boundary_transitions() -> Result<()> {
  let td = TestDirGuard::new("exact_phase");
  let key = b"k";
  let stub = create_stub();
  const CHUNK_SIZE: usize = 64;
  let header_overhead = 4 + key.len() + 8;

  let file_size = CHUNK_SIZE - header_overhead;
  let file_data = random_bytes(file_size, 77);
  let file_path = td.path().join("boundary.bftree");
  fs::write(&file_path, &file_data)?;

  let mut fs = File::open(&file_path)?;
  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub, file_size as u64);
  let mut buffer = vec![0u8; CHUNK_SIZE];

  let len1 = serializer_move_next(&mut serializer, &mut buffer, &mut fs)?;
  assert_eq!(CHUNK_SIZE, len1);
  assert!(!serializer.is_complete());

  let len2 = serializer_move_next(&mut serializer, &mut buffer, &mut fs)?;
  let trailer_size = 8 + 4 + stub.len();
  assert_eq!(trailer_size, len2);
  assert!(serializer.is_complete());

  // 往返校验
  fs.seek(SeekFrom::Start(0))?;

  let mut serializer2 = RangeIndexChunkedSerializer::new(key, &stub, file_size as u64);
  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer2 = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  while !serializer2.is_complete() {
    let len = serializer_move_next(&mut serializer2, &mut buffer, &mut fs)?;
    assert!(deserializer2.process_chunk(&buffer[..len])?);
  }

  assert!(deserializer2.is_complete());
  assert!(!deserializer2.has_error());

  OK
}

/// 验证尾部校验和与存根内容在分块流中的完整性
#[test]
fn trailer_checksum_and_stub_content_verification() -> Result<()> {
  let td = TestDirGuard::new("trailer_cs");
  let file_data = random_bytes(256, 44);
  let file_path = td.path().join("checksum.bftree");
  fs::write(&file_path, &file_data)?;

  let key = b"hashkey";
  let stub = create_stub();
  let mut buffer = create_buffer();

  let mut fs = File::open(&file_path)?;
  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub, file_data.len() as u64);

  let len = serializer_move_next(&mut serializer, &mut buffer, &mut fs)?;
  assert!(serializer.is_complete());

  let payload = &buffer[..len];
  let trailer_size = 8 + 4 + stub.len();
  let trailer_start = len - trailer_size;

  let hash_from_payload = u64::from_le_bytes(payload[trailer_start..trailer_start + 8].try_into()?);
  let stub_len_from_payload =
    u32::from_le_bytes(payload[trailer_start + 8..trailer_start + 12].try_into()?);
  let stub_from_payload = &payload[trailer_start + 12..trailer_start + 12 + stub.len()];

  assert_eq!(INDEX_SIZE_BYTES as u32, stub_len_from_payload);
  assert_eq!(&stub[..], stub_from_payload);

  let manual_hash = compute_checksum(&file_data);
  assert_eq!(manual_hash, hash_from_payload);

  OK
}

/// 测试分块传输过程中 is_complete 状态的正确流转
#[test]
fn is_complete_transitions_correctly() -> Result<()> {
  let td = TestDirGuard::new("is_complete");
  let file_data = random_bytes(DEFAULT_MIGRATION_CHUNK_SIZE + 100, 33);
  let file_path = td.path().join("complete.bftree");
  fs::write(&file_path, &file_data)?;

  let key = b"progresskey";
  let stub = create_stub();
  let mut buffer = create_buffer();

  let mut fs = File::open(&file_path)?;
  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub, file_data.len() as u64);

  assert!(!serializer.is_complete());

  let mut chunk_count = 0;
  while !serializer.is_complete() {
    if chunk_count > 0 {
      assert!(!serializer.is_complete());
    }
    serializer_move_next(&mut serializer, &mut buffer, &mut fs)?;
    chunk_count += 1;
  }

  assert!(serializer.is_complete());
  assert!(chunk_count > 1);

  OK
}

/// 文件数据阶段缓冲区耗尽时安全切分而不抛出异常
#[test]
fn buffer_exhausted_at_file_data_phase_does_not_throw() -> Result<()> {
  let td = TestDirGuard::new("buf_exhaust");
  let file_data = random_bytes(100, 42);
  let file_path = td.path().join("exhausted.bftree");
  fs::write(&file_path, &file_data)?;

  let key = b"k";
  let stub = create_stub();

  let mut fs = File::open(&file_path)?;
  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub, file_data.len() as u64);

  // Buffer 刚好容纳 key header (4) + key (1) + file header (8) = 13
  let mut exact_buf = vec![0u8; 4 + key.len() + 8];

  let written = serializer_move_next(&mut serializer, &mut exact_buf, &mut fs)?;

  assert_eq!(exact_buf.len(), written);
  assert!(!serializer.is_complete());

  let mut buffer = create_buffer();
  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  assert!(deserializer.process_chunk(&exact_buf[..written])?);

  while !serializer.is_complete() {
    let len = serializer_move_next(&mut serializer, &mut buffer, &mut fs)?;
    assert!(deserializer.process_chunk(&buffer[..len])?);
  }

  assert!(deserializer.is_complete());
  assert!(!deserializer.has_error());

  OK
}

/// 等待尾部阶段收到空切片保持非完成状态
#[test]
fn empty_chunk_during_waiting_for_trailer() -> Result<()> {
  let td = TestDirGuard::new("empty_trailer");
  let key = b"k";
  let file_data = [0x01, 0x02];
  let stub = create_stub();
  let payload = build_payload(key, &file_data, &stub);

  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  let file_data_end = 4 + key.len() + 8 + file_data.len();

  assert!(deserializer.process_chunk(&payload[..file_data_end])?);
  assert!(!deserializer.is_complete());

  // 空块
  assert!(deserializer.process_chunk(&[])?);
  assert!(!deserializer.is_complete());

  // Trailer
  assert!(deserializer.process_chunk(&payload[file_data_end..])?);
  assert!(deserializer.is_complete());

  OK
}

/// 接收文件数据阶段收到空切片保持非完成状态
#[test]
fn empty_chunk_during_receiving_file_data() -> Result<()> {
  let td = TestDirGuard::new("empty_receiving");
  let key = b"mykey";
  let file_data = [0x01, 0x02, 0x03, 0x04];
  let stub = create_stub();
  let payload = build_payload(key, &file_data, &stub);

  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  let partial_end = 4 + key.len() + 8 + 2;
  assert!(deserializer.process_chunk(&payload[..partial_end])?);
  assert!(!deserializer.is_complete());

  assert!(deserializer.process_chunk(&[])?);
  assert!(!deserializer.is_complete());
  assert!(!deserializer.has_error());

  assert!(deserializer.process_chunk(&payload[partial_end..])?);
  assert!(deserializer.is_complete());
  assert_eq!(deserializer.key(), key);

  OK
}

/// 等待键长度头部阶段收到空切片保持非完成状态
#[test]
fn empty_chunk_at_waiting_for_key_header() -> Result<()> {
  let td = TestDirGuard::new("empty_key_hdr");
  let key = b"mykey";
  let file_data = [0x01, 0x02];
  let stub = create_stub();
  let payload = build_payload(key, &file_data, &stub);

  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  assert!(deserializer.process_chunk(&[])?);
  assert!(!deserializer.is_complete());
  assert!(!deserializer.has_error());

  assert!(deserializer.process_chunk(&payload)?);
  assert!(deserializer.is_complete());
  assert_eq!(deserializer.key(), key);

  OK
}

/// 等待文件长度头部阶段收到空切片保持非完成状态
#[test]
fn empty_chunk_during_waiting_for_file_header() -> Result<()> {
  let td = TestDirGuard::new("empty_file_hdr");
  let key = b"mykey";
  let file_data = [0x01, 0x02];
  let stub = create_stub();
  let payload = build_payload(key, &file_data, &stub);

  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  let key_end = 4 + key.len();
  assert!(deserializer.process_chunk(&payload[..key_end])?);
  assert!(!deserializer.is_complete());

  assert!(deserializer.process_chunk(&[])?);
  assert!(!deserializer.is_complete());
  assert!(!deserializer.has_error());

  assert!(deserializer.process_chunk(&payload[key_end..])?);
  assert!(deserializer.is_complete());
  assert_eq!(deserializer.key(), key);

  OK
}

/// 测试键数据跨越两个独立数据块的正确拼接与还原
#[test]
fn key_split_across_two_chunks() -> Result<()> {
  let td = TestDirGuard::new("key_split");
  let key = b"longkeyname";
  let file_data = [0xFFu8];
  let stub = create_stub();
  let payload = build_payload(key, &file_data, &stub);

  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  // Chunk 1: 4 字节头 + 3 字节 key
  assert!(deserializer.process_chunk(&payload[..4 + 3])?);
  assert!(!deserializer.is_complete());

  // Chunk 2: 剩余 key + 文件头 + 数据 + trailer
  assert!(deserializer.process_chunk(&payload[4 + 3..])?);

  assert!(deserializer.is_complete());
  assert_eq!(deserializer.key(), key);

  OK
}

/// 测试键长度头独占单个数据块的极端边界解析
#[test]
fn key_header_alone_in_chunk() -> Result<()> {
  let td = TestDirGuard::new("key_hdr_alone");
  let key = b"mykey";
  let file_data = [0x01u8];
  let stub = create_stub();
  let payload = build_payload(key, &file_data, &stub);

  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  // Chunk 1: 仅 4 字节 key 长度头
  assert!(deserializer.process_chunk(&payload[..4])?);
  assert!(!deserializer.is_complete());

  // Chunk 2: 空块
  assert!(deserializer.process_chunk(&[])?);
  assert!(!deserializer.is_complete());

  // Chunk 3: 剩余全部数据
  assert!(deserializer.process_chunk(&payload[4..])?);
  assert!(deserializer.is_complete());
  assert_eq!(deserializer.key(), key);

  OK
}
