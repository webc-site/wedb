use std::{
  array,
  env::temp_dir,
  fs::{self, File},
  io::{self, Read},
  ops::Deref,
  path::{Path, PathBuf},
};

use aok::Result;
use wbftree::{
  DEFAULT_MIGRATION_CHUNK_SIZE, INDEX_SIZE_BYTES, RangeIndexChunkedDeserializer,
  RangeIndexChunkedSerializer, RangeIndexManager, RangeIndexMigrationReader, compute_checksum,
};

/// 测试目录 RAII 自动清理守卫
pub struct TestDirGuard {
  path: PathBuf,
}

impl TestDirGuard {
  pub fn new(prefix: &str) -> Self {
    let path = temp_dir().join(format!("ri_test_{}_{}", prefix, fastrand::u64(..)));
    if path.exists() {
      let _ = fs::remove_dir_all(&path);
    }
    fs::create_dir_all(&path).expect("创建测试临时目录失败");
    Self { path }
  }

  pub fn path(&self) -> &Path {
    &self.path
  }
}

impl Deref for TestDirGuard {
  type Target = Path;
  fn deref(&self) -> &Self::Target {
    &self.path
  }
}

impl AsRef<Path> for TestDirGuard {
  fn as_ref(&self) -> &Path {
    &self.path
  }
}

impl Drop for TestDirGuard {
  fn drop(&mut self) {
    if self.path.exists() {
      let _ = fs::remove_dir_all(&self.path);
    }
  }
}

pub fn create_stub() -> [u8; INDEX_SIZE_BYTES] {
  array::from_fn(|i| (0xA0 + i) as u8)
}

pub fn create_buffer() -> Vec<u8> {
  vec![0u8; DEFAULT_MIGRATION_CHUNK_SIZE]
}

pub fn random_bytes(len: usize, seed: u64) -> Vec<u8> {
  let mut rng = fastrand::Rng::with_seed(seed);
  let mut bytes = vec![0u8; len];
  for b in &mut bytes {
    *b = rng.u8(..);
  }
  bytes
}

/// 测试辅助函数：使用 File 读取文件数据并驱动序列化器 (SerializerMoveNext)
pub fn serializer_move_next<R: Read>(
  serializer: &mut RangeIndexChunkedSerializer,
  mut dest: &mut [u8],
  reader: &mut R,
) -> Result<usize> {
  let mut total_written = 0;
  let mut file_buf = vec![0u8; dest.len()];
  while !serializer.is_complete() && !dest.is_empty() {
    if serializer.needs_file_data() {
      let max_read = (file_buf.len() as u64).min(serializer.file_data_remaining()) as usize;
      let bytes_read = reader.read(&mut file_buf[..max_read])?;
      if bytes_read == 0 && serializer.file_data_remaining() > 0 {
        return Err(
          io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!(
              "RangeIndex 文件截断：剩余 {} 字节未读",
              serializer.file_data_remaining()
            ),
          )
          .into(),
        );
      }
      serializer.supply_file_data(&file_buf[..bytes_read]);
    }

    let written = serializer.move_next(dest)?;
    if written == 0 {
      break;
    }
    dest = &mut dest[written..];
    total_written += written;
  }
  Ok(total_written)
}

/// 辅助函数：构造测试用的分块流字节序列 (BuildPayload)
pub fn build_payload(key: &[u8], file_data: &[u8], stub: &[u8]) -> Vec<u8> {
  let hash = compute_checksum(file_data);
  let total_len = 4 + key.len() + 8 + file_data.len() + 8 + 4 + stub.len();
  let mut payload = Vec::with_capacity(total_len);
  payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
  payload.extend_from_slice(key);
  payload.extend_from_slice(&(file_data.len() as u64).to_le_bytes());
  payload.extend_from_slice(file_data);
  payload.extend_from_slice(&hash.to_le_bytes());
  payload.extend_from_slice(&(stub.len() as u32).to_le_bytes());
  payload.extend_from_slice(stub);
  payload
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ChunkDriver {
  SerializerHelper,
  MigrationReader,
}

/// 统一测试辅助函数：对比 SerializerHelper 和 MigrationReader 在全生命周期下的分块往返一致性
pub fn assert_round_trip(
  driver: ChunkDriver,
  test_dir: &Path,
  key: &[u8],
  file_data: &[u8],
  chunk_size: usize,
) -> Result<()> {
  let stub = create_stub();
  let src_path = test_dir.join(format!("rt-{}.bftree", fastrand::u64(..)));
  fs::write(&src_path, file_data)?;

  let manager = RangeIndexManager::from_root(test_dir).unwrap();
  let mut deserializer = RangeIndexChunkedDeserializer::new(manager.derive_temp_migration_path())?;
  let mut buffer = vec![0u8; chunk_size];

  match driver {
    ChunkDriver::SerializerHelper => {
      let mut fs = File::open(&src_path)?;
      let mut serializer = RangeIndexChunkedSerializer::new(key, &stub, file_data.len() as u64);
      while !serializer.is_complete() {
        let len = serializer_move_next(&mut serializer, &mut buffer, &mut fs)?;
        assert!(len > 0, "序列化器推进失败");
        assert!(deserializer.process_chunk(&buffer[..len])?);
      }
    }
    ChunkDriver::MigrationReader => {
      let fs = File::open(&src_path)?;
      let serializer = RangeIndexChunkedSerializer::new(key, &stub, file_data.len() as u64);
      let mut reader =
        RangeIndexMigrationReader::new(serializer, fs, Some(src_path.clone()), chunk_size)?;
      while !reader.is_complete() {
        let len = reader.read_next_chunk(&mut buffer)?;
        assert!(len > 0, "读取器推进失败");
        assert!(deserializer.process_chunk(&buffer[..len])?);
      }
    }
  }

  assert!(deserializer.is_complete());
  assert!(!deserializer.has_error());
  assert_eq!(deserializer.key(), key);
  assert_eq!(deserializer.stub(), &stub[..]);
  let restored = fs::read(deserializer.temp_path())?;
  assert_eq!(restored, file_data);

  Ok(())
}
