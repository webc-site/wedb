use std::fs::{self, File};

use aok::{OK, Result};
use wbftree::{
  DEFAULT_MIGRATION_CHUNK_SIZE, RangeIndexChunkedDeserializer, RangeIndexChunkedSerializer,
  RangeIndexManager,
};

use super::common::{
  TestDirGuard, build_payload, create_buffer, create_stub, random_bytes, serializer_move_next,
};

/// 测试单块完整序列化与反序列化往返及协议编码验证
#[test]
fn single_chunk_round_trip() -> Result<()> {
  let td = TestDirGuard::new("single_chunk");
  let file_data = random_bytes(1024, 42);
  let file_path = td.path().join("small.bftree");
  fs::write(&file_path, &file_data)?;

  let key = b"mykey";
  let stub = create_stub();
  let mut buffer = create_buffer();

  let mut fs = File::open(&file_path)?;
  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub, file_data.len() as u64);

  let len = serializer_move_next(&mut serializer, &mut buffer, &mut fs)?;
  assert!(len > 0);
  let payload = &buffer[..len];
  assert!(serializer.is_complete());

  // 校验 wire 格式: [4-byte keyLen][key][8-byte fileCount]...
  let mut offset = 0;

  let key_len_from_payload = u32::from_le_bytes(payload[offset..offset + 4].try_into()?);
  assert_eq!(key.len() as u32, key_len_from_payload);
  offset += 4;
  assert_eq!(key, &payload[offset..offset + key.len()]);
  offset += key.len();
  let file_size_from_payload = u64::from_le_bytes(payload[offset..offset + 8].try_into()?);
  assert_eq!(file_data.len() as u64, file_size_from_payload);

  // 反序列化校验
  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  assert!(deserializer.process_chunk(payload)?);
  assert!(deserializer.is_complete());
  assert!(!deserializer.has_error());
  assert_eq!(key, deserializer.key());

  OK
}

/// 测试多数据块分割下的完整序列化与反序列化往返
#[test]
fn multi_chunk_round_trip() -> Result<()> {
  let td = TestDirGuard::new("multi_chunk");
  let file_size = DEFAULT_MIGRATION_CHUNK_SIZE * 3 + 1000;
  let file_data = random_bytes(file_size, 123);
  let file_path = td.path().join("large.bftree");
  fs::write(&file_path, &file_data)?;

  let key = b"largekey";
  let stub = create_stub();
  let mut buffer = create_buffer();

  let mut fs = File::open(&file_path)?;
  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub, file_size as u64);

  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  let mut chunk_count = 0;
  while !serializer.is_complete() {
    let len = serializer_move_next(&mut serializer, &mut buffer, &mut fs)?;
    assert!(deserializer.process_chunk(&buffer[..len])?);
    chunk_count += 1;
  }

  assert!(chunk_count > 1);
  assert!(deserializer.is_complete());
  assert!(!deserializer.has_error());
  assert_eq!(key, deserializer.key());

  OK
}

/// 测试多数据块往返后文件内容完全无损还原
#[test]
fn file_content_preserved_in_round_trip() -> Result<()> {
  let td = TestDirGuard::new("content_pres");
  let file_data = random_bytes(DEFAULT_MIGRATION_CHUNK_SIZE * 2 + 500, 77);
  let file_path = td.path().join("content.bftree");
  fs::write(&file_path, &file_data)?;

  let key = b"contentkey";
  let stub = create_stub();
  let mut buffer = create_buffer();

  let mut fs = File::open(&file_path)?;
  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub, file_data.len() as u64);

  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  while !serializer.is_complete() {
    let len = serializer_move_next(&mut serializer, &mut buffer, &mut fs)?;
    assert!(deserializer.process_chunk(&buffer[..len])?);
  }

  assert!(deserializer.is_complete());

  let restored = fs::read(deserializer.temp_path())?;
  assert_eq!(restored.len(), file_data.len());
  assert_eq!(restored, file_data);

  OK
}

/// 测试往返后 RangeIndexStub 存根元数据完全保持不变
#[test]
fn stub_preserved_in_round_trip() -> Result<()> {
  let td = TestDirGuard::new("stub_pres");
  let file_path = td.path().join("stubtest.bftree");
  fs::write(&file_path, vec![0u8; 100])?;

  let key = b"stubkey";
  let stub = create_stub();
  let mut buffer = create_buffer();

  let mut fs = File::open(&file_path)?;
  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub, 100);

  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  while !serializer.is_complete() {
    let len = serializer_move_next(&mut serializer, &mut buffer, &mut fs)?;
    assert!(deserializer.process_chunk(&buffer[..len])?);
  }

  assert!(deserializer.is_complete());
  assert_eq!(deserializer.key(), key);
  assert_eq!(deserializer.stub(), &stub[..]);

  OK
}

/// 测试超大键跨越多个微型数据块的往返一致性
#[test]
fn key_spanning_multiple_chunks_round_trip() -> Result<()> {
  let td = TestDirGuard::new("key_multichunk");
  let file_path = td.path().join("tinyChunk.bftree");
  let file_data = random_bytes(100, 55);
  fs::write(&file_path, &file_data)?;

  let key = random_bytes(200, 66);
  let stub = create_stub();
  const TINY_CHUNK_SIZE: usize = 50;
  let mut buffer = vec![0u8; TINY_CHUNK_SIZE];

  let mut fs = File::open(&file_path)?;
  let mut serializer = RangeIndexChunkedSerializer::new(&key, &stub, file_data.len() as u64);

  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  let mut chunk_count = 0;
  while !serializer.is_complete() {
    let len = serializer_move_next(&mut serializer, &mut buffer, &mut fs)?;
    assert!(deserializer.process_chunk(&buffer[..len])?);
    chunk_count += 1;
  }

  assert!(chunk_count > 4);
  assert!(deserializer.is_complete());
  assert!(!deserializer.has_error());
  assert_eq!(deserializer.key(), key.as_slice());

  OK
}

/// 测试小缓冲区多次步进序列化的往返一致性
#[test]
fn small_buffer_round_trip() -> Result<()> {
  let td = TestDirGuard::new("small_buf_rt");
  let file_data = random_bytes(50, 99);
  let file_path = td.path().join("smallbuf.bftree");
  fs::write(&file_path, &file_data)?;

  let key = b"abcdef";
  let stub = create_stub();

  let mut fs = File::open(&file_path)?;
  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub, file_data.len() as u64);

  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  let trailer_size = 8 + 4 + stub.len();
  let mut buffer = vec![0u8; trailer_size];

  let mut all_chunks = Vec::new();
  let mut chunk_count = 0;

  while !serializer.is_complete() {
    let len = serializer_move_next(&mut serializer, &mut buffer, &mut fs)?;
    if len > 0 {
      all_chunks.extend_from_slice(&buffer[..len]);
    }
    chunk_count += 1;
    assert!(chunk_count < 1000, "序列化器在预期轮次内未完成");
  }

  assert!(chunk_count > 1);
  assert!(deserializer.process_chunk(&all_chunks)?);
  assert!(deserializer.is_complete());
  assert!(!deserializer.has_error());
  assert_eq!(deserializer.key(), key);

  OK
}

/// 测试文件数据刚好填满数据块时尾部顺延至下一块
#[test]
fn file_data_exactly_fills_chunk_trailer_in_next_chunk() -> Result<()> {
  let td = TestDirGuard::new("fill_chunk");
  let key = b"testkey";
  let file_data = random_bytes(100, 42);
  let stub = create_stub();
  let payload = build_payload(key, &file_data, &stub);

  let file_data_end = 4 + key.len() + 8 + file_data.len();

  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  // Chunk 1: 数据正好到文件结尾
  assert!(deserializer.process_chunk(&payload[..file_data_end])?);
  assert!(!deserializer.is_complete());

  // 验证临时文件写入
  let tmp_path = deserializer.temp_path();
  let written_data = fs::read(tmp_path)?;
  assert_eq!(file_data.len(), written_data.len());
  assert_eq!(file_data, written_data);

  // Chunk 2: Trailer
  assert!(deserializer.process_chunk(&payload[file_data_end..])?);

  assert!(deserializer.is_complete());
  assert!(!deserializer.has_error());
  assert_eq!(deserializer.key(), key);
  assert_eq!(deserializer.stub(), &stub[..]);

  OK
}

/// 测试极端逐字节（每块 1 字节）喂入文件数据的反序列化还原
#[test]
fn file_data_one_byte_per_chunk() -> Result<()> {
  let td = TestDirGuard::new("1b_per_chunk");
  let key = b"k";
  let file_data = [0xAA, 0xBB, 0xCC, 0xDD];
  let stub = create_stub();
  let payload = build_payload(key, &file_data, &stub);

  let manager = RangeIndexManager::from_root(td.path()).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  let header_end = 4 + key.len() + 8;

  // Chunk 1: header
  assert!(deserializer.process_chunk(&payload[..header_end])?);

  // Chunks 2-5: 每次 1 字节
  for i in 0..file_data.len() {
    assert!(deserializer.process_chunk(&payload[header_end + i..header_end + i + 1])?);
    assert!(!deserializer.is_complete());
  }

  // 最终块: Trailer
  let trailer_start = header_end + file_data.len();

  assert!(deserializer.process_chunk(&payload[trailer_start..])?);
  assert!(deserializer.is_complete());
  assert_eq!(deserializer.key(), key);

  OK
}
