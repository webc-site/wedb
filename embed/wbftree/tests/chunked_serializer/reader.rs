use std::fs::{self, File};

use aok::{OK, Result};
use wbftree::{
  DEFAULT_MIGRATION_CHUNK_SIZE, MIN_CHUNK_SIZE, RangeIndexChunkedSerializer,
  RangeIndexMigrationReader,
};

use super::common::{ChunkDriver, TestDirGuard, assert_round_trip, create_stub, random_bytes};

/// 测试单块往返一致性（SerializerHelper 驱动）
#[test]
fn round_trip_single_chunk_serializer() -> Result<()> {
  let td = TestDirGuard::new("rt_sc_ser");
  assert_round_trip(
    ChunkDriver::SerializerHelper,
    td.path(),
    b"mykey",
    &random_bytes(1024, 42),
    DEFAULT_MIGRATION_CHUNK_SIZE,
  )
}

/// 测试单块往返一致性（MigrationReader 驱动）
#[test]
fn round_trip_single_chunk_reader() -> Result<()> {
  let td = TestDirGuard::new("rt_sc_rdr");
  assert_round_trip(
    ChunkDriver::MigrationReader,
    td.path(),
    b"mykey",
    &random_bytes(1024, 42),
    DEFAULT_MIGRATION_CHUNK_SIZE,
  )
}

/// 测试多块流式往返一致性（SerializerHelper 驱动）
#[test]
fn round_trip_multi_chunk_serializer() -> Result<()> {
  let td = TestDirGuard::new("rt_mc_ser");
  assert_round_trip(
    ChunkDriver::SerializerHelper,
    td.path(),
    b"largekey",
    &random_bytes(DEFAULT_MIGRATION_CHUNK_SIZE * 3 + 1000, 123),
    DEFAULT_MIGRATION_CHUNK_SIZE,
  )
}

/// 测试多块流式往返一致性（MigrationReader 驱动）
#[test]
fn round_trip_multi_chunk_reader() -> Result<()> {
  let td = TestDirGuard::new("rt_mc_rdr");
  assert_round_trip(
    ChunkDriver::MigrationReader,
    td.path(),
    b"largekey",
    &random_bytes(DEFAULT_MIGRATION_CHUNK_SIZE * 3 + 1000, 123),
    DEFAULT_MIGRATION_CHUNK_SIZE,
  )
}

/// 测试键长度大于单块大小的往返一致性（SerializerHelper 驱动）
#[test]
fn round_trip_key_larger_than_chunk_serializer() -> Result<()> {
  let td = TestDirGuard::new("rt_klarge_ser");
  assert_round_trip(
    ChunkDriver::SerializerHelper,
    td.path(),
    &random_bytes(200, 66),
    &random_bytes(100, 55),
    64,
  )
}

/// 测试键长度大于单块大小的往返一致性（MigrationReader 驱动）
#[test]
fn round_trip_key_larger_than_chunk_reader() -> Result<()> {
  let td = TestDirGuard::new("rt_klarge_rdr");
  assert_round_trip(
    ChunkDriver::MigrationReader,
    td.path(),
    &random_bytes(200, 66),
    &random_bytes(100, 55),
    64,
  )
}

/// 测试超小数据块分片的往返一致性（SerializerHelper 驱动）
#[test]
fn round_trip_small_chunk_serializer() -> Result<()> {
  let td = TestDirGuard::new("rt_small_ser");
  assert_round_trip(
    ChunkDriver::SerializerHelper,
    td.path(),
    b"k",
    &random_bytes(500, 77),
    64,
  )
}

/// 测试超小数据块分片的往返一致性（MigrationReader 驱动）
#[test]
fn round_trip_small_chunk_reader() -> Result<()> {
  let td = TestDirGuard::new("rt_small_rdr");
  assert_round_trip(
    ChunkDriver::MigrationReader,
    td.path(),
    b"k",
    &random_bytes(500, 77),
    64,
  )
}

/// 测试文件数据刚好填满首块的往返一致性（SerializerHelper 驱动）
#[test]
fn round_trip_file_exactly_fills_first_chunk_serializer() -> Result<()> {
  let td = TestDirGuard::new("rt_fill1_ser");
  assert_round_trip(
    ChunkDriver::SerializerHelper,
    td.path(),
    b"k",
    &random_bytes(51, 88),
    64,
  )
}

/// 测试文件数据刚好填满首块的往返一致性（MigrationReader 驱动）
#[test]
fn round_trip_file_exactly_fills_first_chunk_reader() -> Result<()> {
  let td = TestDirGuard::new("rt_fill1_rdr");
  assert_round_trip(
    ChunkDriver::MigrationReader,
    td.path(),
    b"k",
    &random_bytes(51, 88),
    64,
  )
}

/// 读取截断文件时 MigrationReader 返回错误
#[test]
fn reader_truncated_file_throws() -> Result<()> {
  let td = TestDirGuard::new("rdr_trunc");
  let src_path = td.path().join("reader-trunc.bftree");
  fs::write(&src_path, vec![0u8; 10])?;

  let serializer = RangeIndexChunkedSerializer::new(b"k", &create_stub(), 1000);
  let fs = File::open(&src_path)?;
  let mut reader = RangeIndexMigrationReader::new(serializer, fs, Some(src_path), 256)?;

  let mut buffer = [0u8; 256];
  let mut hit_err = false;
  while !reader.is_complete() {
    if reader.read_next_chunk(&mut buffer).is_err() {
      hit_err = true;
      break;
    }
  }
  assert!(hit_err);

  OK
}

/// 传入非正数分块大小时 MigrationReader 构造报错
#[test]
fn reader_non_positive_chunk_size_throws() -> Result<()> {
  let td = TestDirGuard::new("rdr_chunk_zero");
  let src_path = td.path().join("reader-chunksize.bftree");
  fs::write(&src_path, vec![0u8; 8])?;

  let fs = File::open(&src_path)?;
  let serializer = RangeIndexChunkedSerializer::new(b"k", &create_stub(), 8);

  assert!(
    RangeIndexMigrationReader::new(serializer, fs, Some(src_path), 0).is_err(),
    "read_buffer_size 0 必须返回错误"
  );

  OK
}

/// 目标缓冲区小于最小分块阈值时读取报错
#[test]
fn reader_destination_below_minimum_throws() -> Result<()> {
  let td = TestDirGuard::new("rdr_dest_min");
  let src_path = td.path().join("reader-destmin.bftree");
  fs::write(&src_path, random_bytes(64, 7))?;

  let serializer = RangeIndexChunkedSerializer::new(b"k", &create_stub(), 64);
  let fs = File::open(&src_path)?;
  let mut reader = RangeIndexMigrationReader::new(serializer, fs, Some(src_path), 8)?;

  const { assert!(MIN_CHUNK_SIZE > 0) };
  let mut too_small = vec![0u8; MIN_CHUNK_SIZE - 1];
  assert!(reader.read_next_chunk(&mut too_small).is_err());

  OK
}

/// 测试精确最小分块阈值下的往返一致性（SerializerHelper 驱动）
#[test]
fn round_trip_exact_min_chunk_size_serializer() -> Result<()> {
  let td = TestDirGuard::new("rt_min_ser");
  assert_round_trip(
    ChunkDriver::SerializerHelper,
    td.path(),
    b"k",
    &random_bytes(300, 31),
    MIN_CHUNK_SIZE,
  )
}

/// 测试精确最小分块阈值下的往返一致性（MigrationReader 驱动）
#[test]
fn round_trip_exact_min_chunk_size_reader() -> Result<()> {
  let td = TestDirGuard::new("rt_min_rdr");
  assert_round_trip(
    ChunkDriver::MigrationReader,
    td.path(),
    b"k",
    &random_bytes(300, 31),
    MIN_CHUNK_SIZE,
  )
}
