use std::fs::{self, File};

use aok::{OK, Result};
use wbftree::{
  INDEX_SIZE_BYTES, RangeIndexChunkedDeserializer, RangeIndexChunkedSerializer, RangeIndexManager,
  RangeIndexMigrationReader, compute_checksum,
};

use super::common::{
  TestDirGuard, build_payload, create_buffer, create_stub, random_bytes, serializer_move_next,
};

/// 空文件反序列化时安全拦截并进入错误状态
#[test]
fn empty_file_rejected_by_deserializer() -> Result<()> {
  let td = TestDirGuard::new("empty_file");
  let key = b"emptykey";
  let file_data = b"";
  let stub = create_stub();
  let payload = build_payload(key, file_data, &stub);

  let manager = RangeIndexManager::from_root(td.path());
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  assert!(!deserializer.process_chunk(&payload)?);
  assert!(deserializer.has_error());

  OK
}

/// 文件数据遭篡改时反序列化器检测到校验和不匹配
#[test]
fn corrupted_checksum_detected() -> Result<()> {
  let td = TestDirGuard::new("corrupt_cs");
  let file_data = random_bytes(512, 99);
  let file_path = td.path().join("corrupt.bftree");
  fs::write(&file_path, &file_data)?;

  let key = b"corruptkey";
  let stub = create_stub();
  let mut buffer = create_buffer();

  let mut fs = File::open(&file_path)?;
  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub, file_data.len() as u64);

  let len = serializer_move_next(&mut serializer, &mut buffer, &mut fs)?;
  assert!(len > 0);
  let mut payload = buffer[..len].to_vec();

  // 篡改一个文件数据字节
  let file_data_offset = 4 + key.len() + 8;
  payload[file_data_offset + 10] ^= 0xFF;

  let manager = RangeIndexManager::from_root(td.path());
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  assert!(!deserializer.process_chunk(&payload)?);
  assert!(deserializer.has_error());
  assert!(!deserializer.is_complete());

  OK
}

/// 负数文件大小头部被反序列化器拒绝并进入错误状态
#[test]
fn negative_file_size_is_error() -> Result<()> {
  let td = TestDirGuard::new("neg_file_size");
  // [4-byte keyLen=0][8-byte negative fileCount]
  let mut payload = vec![0u8; 4 + 8];
  payload[..4].copy_from_slice(&0u32.to_le_bytes());
  payload[4..12].copy_from_slice(&(-1i64).to_le_bytes());

  let manager = RangeIndexManager::from_root(td.path());
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  assert!(!deserializer.process_chunk(&payload)?);
  assert!(deserializer.has_error());

  OK
}

/// 文件长度头部被非法截断进入错误状态
#[test]
fn split_file_header_is_error() -> Result<()> {
  let td = TestDirGuard::new("split_hdr");
  let key = b"k";

  let manager = RangeIndexManager::from_root(td.path());
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  // Chunk 1: 完整的 key header + key -> 进入 WaitingForFileHeader
  let mut key_chunk = vec![0u8; 4 + key.len()];
  key_chunk[..4].copy_from_slice(&(key.len() as u32).to_le_bytes());
  key_chunk[4..].copy_from_slice(key);
  assert!(deserializer.process_chunk(&key_chunk)?);
  assert!(!deserializer.is_complete());

  // Chunk 2: 只有 4 字节，少于 8 字节的文件头 -> Error
  assert!(!deserializer.process_chunk(&[0u8; 4])?);

  assert!(deserializer.has_error());

  OK
}

/// 尾部数据过短不足以容纳校验和与存根长度时报错
#[test]
fn trailer_too_small_is_error() -> Result<()> {
  let td = TestDirGuard::new("trailer_small");
  let key = b"k";
  let file_data = [0xABu8];

  let manager = RangeIndexManager::from_root(td.path());
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  // Chunk 1: key header + key + file header + 完整文件 -> 进入 WaitingForTrailer
  let mut pre_trailer = vec![0u8; 4 + key.len() + 8 + file_data.len()];

  let mut o = 0;
  pre_trailer[o..o + 4].copy_from_slice(&(key.len() as u32).to_le_bytes());
  o += 4;
  pre_trailer[o..o + key.len()].copy_from_slice(key);
  o += key.len();
  pre_trailer[o..o + 8].copy_from_slice(&(file_data.len() as u64).to_le_bytes());
  o += 8;
  pre_trailer[o..o + file_data.len()].copy_from_slice(&file_data);
  assert!(deserializer.process_chunk(&pre_trailer)?);
  assert!(!deserializer.is_complete());

  // Chunk 2: 只有 11 字节，少于 [8-byte hash][4-byte stubLen] (12 字节) -> Error
  assert!(!deserializer.process_chunk(&[0u8; 11])?);

  assert!(deserializer.has_error());

  OK
}

/// 目标快照文件路径无法创建时反序列化器安全转入错误状态
#[test]
fn file_open_failure_goes_to_error_state() -> Result<()> {
  let td = TestDirGuard::new("bad_open");
  // 无法创建的非法目录路径
  let bad_path = td
    .path()
    .join("non_existing_nested_dir/uncreatable/snapshot.bftree");

  let mut deserializer = RangeIndexChunkedDeserializer::new(&bad_path)?;

  let key = b"k";
  let mut payload = vec![0u8; 4 + key.len() + 8];
  payload[..4].copy_from_slice(&(key.len() as u32).to_le_bytes());
  payload[4..4 + key.len()].copy_from_slice(key);
  payload[4 + key.len()..].copy_from_slice(&10u64.to_le_bytes());

  assert!(!deserializer.process_chunk(&payload)?);
  assert!(deserializer.has_error());

  OK
}

/// 头部数据小于 4 字节直接判定为格式错误
#[test]
fn too_small_header_is_error() -> Result<()> {
  let td = TestDirGuard::new("hdr_small");
  let payload = [0u8; 2]; // 少于 4 字节

  let manager = RangeIndexManager::from_root(td.path());
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  assert!(!deserializer.process_chunk(&payload)?);
  assert!(deserializer.has_error());

  OK
}

/// 反序列化器一旦进入错误状态后续输入均维持终态错误
#[test]
fn error_state_is_terminal() -> Result<()> {
  let td = TestDirGuard::new("terminal_err");
  let mut payload = vec![0u8; 4 + 8];
  payload[..4].copy_from_slice(&0u32.to_le_bytes());
  payload[4..].copy_from_slice(&(-1i64).to_le_bytes());

  let manager = RangeIndexManager::from_root(td.path());
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  assert!(!deserializer.process_chunk(&payload)?);
  assert!(deserializer.has_error());

  assert!(!deserializer.process_chunk(&[0u8; 100])?);
  assert!(deserializer.has_error());

  OK
}

/// 非法的存根长度字段被反序列化器拦截报错
#[test]
fn invalid_stub_size_is_error() -> Result<()> {
  let td = TestDirGuard::new("bad_stub_size");
  let bad_stub_size = 10usize;
  let key = b"badstub";
  let bad_stub = vec![0u8; bad_stub_size];
  let file_data = [0xABu8];

  let hash = compute_checksum(&file_data);

  let trailer_size = 8 + 4 + bad_stub_size;
  let mut payload = vec![0u8; 4 + key.len() + 8 + file_data.len() + trailer_size];
  let mut offset = 0;
  payload[offset..offset + 4].copy_from_slice(&(key.len() as u32).to_le_bytes());
  offset += 4;
  payload[offset..offset + key.len()].copy_from_slice(key);
  offset += key.len();
  payload[offset..offset + 8].copy_from_slice(&(file_data.len() as u64).to_le_bytes());
  offset += 8;
  payload[offset..offset + file_data.len()].copy_from_slice(&file_data);
  offset += file_data.len();
  payload[offset..offset + 8].copy_from_slice(&hash.to_le_bytes());
  offset += 8;
  payload[offset..offset + 4].copy_from_slice(&(bad_stub_size as u32).to_le_bytes());
  offset += 4;
  payload[offset..offset + bad_stub_size].copy_from_slice(&bad_stub);

  let manager = RangeIndexManager::from_root(td.path());
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  assert!(!deserializer.process_chunk(&payload)?);
  assert!(deserializer.has_error());

  OK
}

/// 存根字节流不完整时反序列化器报错
#[test]
fn truncated_stub_is_error() -> Result<()> {
  let td = TestDirGuard::new("trunc_stub");
  let key = b"truncstub";
  let file_data = [0xABu8];
  let hash = compute_checksum(&file_data);

  let actual_stub_bytes = INDEX_SIZE_BYTES - 1;
  let mut payload = vec![0u8; 4 + key.len() + 8 + file_data.len() + 8 + 4 + actual_stub_bytes];
  let mut offset = 0;
  payload[offset..offset + 4].copy_from_slice(&(key.len() as u32).to_le_bytes());
  offset += 4;
  payload[offset..offset + key.len()].copy_from_slice(key);
  offset += key.len();
  payload[offset..offset + 8].copy_from_slice(&(file_data.len() as u64).to_le_bytes());
  offset += 8;
  payload[offset..offset + file_data.len()].copy_from_slice(&file_data);
  offset += file_data.len();
  payload[offset..offset + 8].copy_from_slice(&hash.to_le_bytes());
  offset += 8;
  payload[offset..offset + 4].copy_from_slice(&(INDEX_SIZE_BYTES as u32).to_le_bytes());

  let manager = RangeIndexManager::from_root(td.path());
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  assert!(!deserializer.process_chunk(&payload)?);
  assert!(deserializer.has_error());

  OK
}

/// 尾部包含多余的额外字节时反序列化器报错
#[test]
fn over_long_trailer_is_error() -> Result<()> {
  let td = TestDirGuard::new("overlong_trailer");
  let key = b"overlong";
  let file_data = [0xABu8];
  let hash = compute_checksum(&file_data);

  const EXTRA_BYTES: usize = 3;
  let mut payload =
    vec![0u8; 4 + key.len() + 8 + file_data.len() + 8 + 4 + INDEX_SIZE_BYTES + EXTRA_BYTES];
  let mut offset = 0;
  payload[offset..offset + 4].copy_from_slice(&(key.len() as u32).to_le_bytes());
  offset += 4;
  payload[offset..offset + key.len()].copy_from_slice(key);
  offset += key.len();
  payload[offset..offset + 8].copy_from_slice(&(file_data.len() as u64).to_le_bytes());
  offset += 8;
  payload[offset..offset + file_data.len()].copy_from_slice(&file_data);
  offset += file_data.len();
  payload[offset..offset + 8].copy_from_slice(&hash.to_le_bytes());
  offset += 8;
  payload[offset..offset + 4].copy_from_slice(&(INDEX_SIZE_BYTES as u32).to_le_bytes());

  let manager = RangeIndexManager::from_root(td.path());
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  assert!(!deserializer.process_chunk(&payload)?);
  assert!(deserializer.has_error());

  OK
}

/// 序列化完成后再次调用 move_next 安全返回错误
#[test]
fn move_next_after_done_throws() -> Result<()> {
  let td = TestDirGuard::new("done_throws");
  let file_path = td.path().join("done.bftree");
  fs::write(&file_path, vec![0u8; 64])?;

  let key = b"k";
  let stub = create_stub();
  let mut buffer = create_buffer();

  let mut fs = File::open(&file_path)?;
  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub, 64);

  while !serializer.is_complete() {
    serializer_move_next(&mut serializer, &mut buffer, &mut fs)?;
  }

  assert!(serializer.move_next(&mut buffer).is_err());

  OK
}

/// 源文件在流式读取过程中提前截断时抛出异常
#[test]
fn truncated_file_throws_exception() -> Result<()> {
  let td = TestDirGuard::new("trunc_file");
  let file_path = td.path().join("truncated.bftree");
  fs::write(&file_path, vec![0u8; 50])?;

  let key = b"k";
  let stub = create_stub();
  let mut buffer = create_buffer();

  let mut fs = File::open(&file_path)?;
  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub, 1000);

  let mut hit_err = false;
  while !serializer.is_complete() {
    if serializer_move_next(&mut serializer, &mut buffer, &mut fs).is_err() {
      hit_err = true;
      break;
    }
  }
  assert!(hit_err);

  OK
}

/// 多次释放 MigrationReader 保持幂等安全
#[test]
fn double_dispose_is_idempotent() -> Result<()> {
  let td = TestDirGuard::new("double_disp");
  let file_path = td.path().join("dispose.bftree");
  fs::write(&file_path, vec![0u8; 16])?;

  let fs = File::open(&file_path)?;
  let serializer = RangeIndexChunkedSerializer::new(b"k", &create_stub(), 16);
  let mut reader = RangeIndexMigrationReader::new(serializer, fs, Some(file_path.clone()), 256)?;

  reader.dispose();
  assert!(!file_path.exists(), "释放后临时快照文件必须被清理");
  reader.dispose(); // 重复释放不应 panic

  OK
}

/// 释放反序列化器自动清理未完成的临时文件
#[test]
fn dispose_cleans_temp_file() -> Result<()> {
  let td = TestDirGuard::new("dispose_clean");
  let manager = RangeIndexManager::from_root(td.path());
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  let key = b"tmp";
  let mut payload = vec![0u8; 4 + key.len() + 8 + 10];
  let mut offset = 0;
  payload[offset..offset + 4].copy_from_slice(&(key.len() as u32).to_le_bytes());
  offset += 4;
  payload[offset..offset + key.len()].copy_from_slice(key);
  offset += key.len();
  payload[offset..offset + 8].copy_from_slice(&100u64.to_le_bytes());

  deserializer.process_chunk(&payload)?;

  let tmp_path = deserializer.temp_path().to_path_buf();
  assert!(tmp_path.exists());

  deserializer.dispose();
  assert!(!tmp_path.exists());

  OK
}

/// 启动初始化自动清理迁移临时目录中的残留文件
#[test]
fn startup_cleans_up_migration_tmp_dir() -> Result<()> {
  let td = TestDirGuard::new("startup_clean");
  let tmp_dir = td.path().join("migration-tmp");
  fs::create_dir_all(&tmp_dir)?;
  fs::write(tmp_dir.join("orphan.bftree"), b"leftover")?;

  let manager = RangeIndexManager::from_root(td.path());
  assert!(tmp_dir.exists());
  let count = fs::read_dir(&tmp_dir)?.count();
  assert_eq!(0, count);
  drop(manager);

  OK
}

/// 零长度键被反序列化器拒绝并进入错误状态
#[test]
fn zero_length_key_rejected_by_deserializer() -> Result<()> {
  let td = TestDirGuard::new("zero_key");
  let key = b"";
  let file_data = [0x01u8];
  let stub = create_stub();
  let payload = build_payload(key, &file_data, &stub);

  let manager = RangeIndexManager::from_root(td.path());
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  assert!(!deserializer.process_chunk(&payload)?);
  assert!(deserializer.has_error());

  OK
}

/// 零字节文件大小声明被反序列化器拒绝
#[test]
fn zero_file_data_rejected_by_deserializer() -> Result<()> {
  let td = TestDirGuard::new("zero_file_data");
  let key = b"k";
  let file_data = b"";
  let stub = create_stub();
  let payload = build_payload(key, file_data, &stub);

  let manager = RangeIndexManager::from_root(td.path());
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  assert!(!deserializer.process_chunk(&payload)?);
  assert!(deserializer.has_error());

  OK
}

/// 尾部校验和与篡改的文件正文不匹配时反序列化失败
#[test]
fn corrupted_file_data_fails_checksum_in_trailer() -> Result<()> {
  let td = TestDirGuard::new("corrupt_trailer_cs");
  let key = b"k";
  let file_data = [0x01, 0x02, 0x03];
  let stub = create_stub();
  let mut payload = build_payload(key, &file_data, &stub);

  let file_data_offset = 4 + key.len() + 8;
  payload[file_data_offset] ^= 0xFF;

  let manager = RangeIndexManager::from_root(td.path());
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  assert!(!deserializer.process_chunk(&payload)?);
  assert!(deserializer.has_error());

  OK
}

/// 声明大小小于实际文件时仅序列化声明的前缀长度
#[test]
fn declared_size_smaller_than_actual_file_emits_truncated_prefix() -> Result<()> {
  let td = TestDirGuard::new("decl_smaller");
  let full_file_data = random_bytes(500, 55);
  let file_path = td.path().join("shorter.bftree");
  fs::write(&file_path, &full_file_data)?;

  let declared_size = 200u64;
  let key = b"shortkey";
  let stub = create_stub();
  let mut buffer = create_buffer();

  let mut fs = File::open(&file_path)?;
  let mut serializer = RangeIndexChunkedSerializer::new(key, &stub, declared_size);

  let len = serializer_move_next(&mut serializer, &mut buffer, &mut fs)?;
  assert!(serializer.is_complete());

  let file_size_offset = 4 + key.len();
  let file_size_from_payload =
    u64::from_le_bytes(buffer[file_size_offset..file_size_offset + 8].try_into()?);
  assert_eq!(declared_size, file_size_from_payload);

  let manager = RangeIndexManager::from_root(td.path());
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;

  assert!(deserializer.process_chunk(&buffer[..len])?);
  assert!(deserializer.is_complete());
  assert!(!deserializer.has_error());

  let restored = fs::read(deserializer.temp_path())?;
  assert_eq!(declared_size as usize, restored.len());
  assert_eq!(
    &full_file_data[..declared_size as usize],
    restored.as_slice()
  );

  OK
}
