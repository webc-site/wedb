use std::{env, fs, slice};

use aok::{OK, Result};
use wbftree::{
  RangeIndexChunkedDeserializer, RangeIndexChunkedSerializer, RangeIndexStub, StorageBackend,
  compute_checksum, compute_checksum_with_seed,
};
use whasher::{StreamHasher, compute_checksum_with_seed as hasher_checksum_with_seed};

/// 测试 RangeIndexChunkedSerializer 分块流式序列化与反序列化全流程
#[test]
fn test_range_index_chunked_streaming() -> Result<()> {
  let key = b"garnet:ri:cluster_migrate_key";
  let stub = RangeIndexStub::new(
    0xdead_beef_0000_1111,
    64 * 1024 * 1024,
    4,
    1024,
    32,
    4096,
    StorageBackend::Std,
  );
  let stub_bytes = stub.encode();

  // 构造模拟文件数据并验证流式计算与一次性校验和一致性
  let file_content = b"Mock BfTree file payload data spanning multiple network chunks";
  let total_file_bytes = file_content.len() as u64;
  let mut hasher = StreamHasher::default();
  hasher.write(file_content);
  let checksum = hasher.finish();

  assert_eq!(checksum, compute_checksum(file_content));

  // 验证种子校验和一致性
  let seed = 0x1234_5678_u64;
  assert_eq!(
    compute_checksum_with_seed(file_content, seed),
    hasher_checksum_with_seed(file_content, seed)
  );

  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub_bytes, total_file_bytes);

  let temp_dest = env::temp_dir().join(format!("chunk_dest_{}.bftree", fastrand::u64(..)));
  let mut deserializer = RangeIndexChunkedDeserializer::new(&temp_dest)?;

  // 以超小 chunk (64B) 模拟切片流式传输
  let mut chunk_buf = [0u8; 64];
  while !serializer.is_complete() {
    if serializer.needs_file_data() {
      serializer.supply_file_data(file_content);
    }
    let written = serializer.move_next(&mut chunk_buf)?;
    if written > 0 {
      let _ = deserializer.process_chunk(&chunk_buf[..written])?;
    }
  }

  assert!(deserializer.is_complete());
  assert_eq!(deserializer.key(), key);
  assert_eq!(deserializer.stub(), stub_bytes);

  let read_back_file = fs::read(&temp_dest)?;
  assert_eq!(read_back_file, file_content);

  let _ = fs::remove_file(&temp_dest);
  OK
}

/// 测试极端 1 字节切片喂入下的流式分块鲁棒性
#[test]
fn test_range_index_extreme_1byte_chunk_streaming() -> Result<()> {
  let key = b"frag_key";
  let stub = RangeIndexStub::new(
    0xfeed_face_1234_5678,
    32 * 1024 * 1024,
    4,
    1024,
    32,
    4096,
    StorageBackend::Std,
  );
  let stub_bytes = stub.encode();
  let file_content = b"Stream fragmentation resilience test payload";
  let total_file_bytes = file_content.len() as u64;

  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub_bytes, total_file_bytes);
  let temp_dest = env::temp_dir().join(format!("chunk_1b_{}.bftree", fastrand::u64(..)));
  let mut deserializer = RangeIndexChunkedDeserializer::new(&temp_dest)?;

  // 1. 序列化成一个完整的数据流缓存
  let mut full_stream = Vec::new();
  let mut tmp_chunk = [0u8; 128];
  while !serializer.is_complete() {
    if serializer.needs_file_data() {
      serializer.supply_file_data(file_content);
    }
    let n = serializer.move_next(&mut tmp_chunk)?;
    if n > 0 {
      full_stream.extend_from_slice(&tmp_chunk[..n]);
    }
  }

  // 2. 键字节和文件正文字节以极端 1 字节切片喂入，头部和 Trailer 保持原子单块 (严格协议)
  let key_start = 4;
  let key_end = key_start + key.len();
  let file_header_end = key_end + 8;
  let file_data_end = file_header_end + file_content.len();

  // 2.1 键长度头 (4 字节单块)
  assert!(deserializer.process_chunk(&full_stream[..key_start])?);
  // 2.2 键数据 (逐 1 字节喂入)
  for b in &full_stream[key_start..key_end] {
    assert!(deserializer.process_chunk(slice::from_ref(b))?);
  }
  // 2.3 文件长度头 (8 字节单块)
  assert!(deserializer.process_chunk(&full_stream[key_end..file_header_end])?);
  // 2.4 文件正文数据 (逐 1 字节喂入)
  for b in &full_stream[file_header_end..file_data_end] {
    assert!(deserializer.process_chunk(slice::from_ref(b))?);
  }
  // 2.5 尾部 Trailer (47 字节单块)
  assert!(deserializer.process_chunk(&full_stream[file_data_end..])?);

  assert!(deserializer.is_complete());
  assert_eq!(deserializer.key(), key);
  assert_eq!(deserializer.stub(), stub_bytes);

  let read_back_file = fs::read(&temp_dest)?;
  assert_eq!(read_back_file, file_content);

  let _ = fs::remove_file(&temp_dest);
  OK
}

/// 测试流式实时计算 gxhash 校验和并成功通过校验
#[test]
fn test_range_index_streaming_with_zero_checksum() -> Result<()> {
  let key = b"zero_checksum_key";
  let stub = RangeIndexStub::new(
    0x1122_3344_5566_7788,
    16 * 1024 * 1024,
    4,
    1024,
    32,
    4096,
    StorageBackend::Std,
  );
  let stub_bytes = stub.encode();
  let file_content = b"Automatic streaming checksum calculation test payload";
  let total_file_bytes = file_content.len() as u64;

  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub_bytes, total_file_bytes);
  let temp_dest = env::temp_dir().join(format!("chunk_zero_ckpt_{}.bftree", fastrand::u64(..)));
  let mut deserializer = RangeIndexChunkedDeserializer::new(&temp_dest)?;

  let mut chunk_buf = [0u8; 64];
  while !serializer.is_complete() {
    if serializer.needs_file_data() {
      serializer.supply_file_data(file_content);
    }
    let written = serializer.move_next(&mut chunk_buf)?;
    if written > 0 {
      let _ = deserializer.process_chunk(&chunk_buf[..written])?;
    }
  }

  assert!(deserializer.is_complete());
  assert_eq!(deserializer.key(), key);
  assert_eq!(deserializer.stub(), stub_bytes);

  let read_back_file = fs::read(&temp_dest)?;
  assert_eq!(read_back_file, file_content);

  let _ = fs::remove_file(&temp_dest);
  OK
}
