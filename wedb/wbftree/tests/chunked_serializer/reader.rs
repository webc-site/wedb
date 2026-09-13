use std::fs::{self, File};

use aok::{OK, Result};
use wbftree::{
  DEFAULT_MIGRATION_CHUNK_SIZE, MIN_CHUNK_SIZE, RangeIndexChunkedSerializer,
  RangeIndexMigrationReader,
};

use super::common::{ChunkDriver, TestDirGuard, assert_round_trip, create_stub, random_bytes};

/// 两种分块驱动全量参与往返：序列化直驱与 MigrationReader 必须逐字节等价
const ALL_DRIVERS: [ChunkDriver; 2] = [ChunkDriver::SerializerHelper, ChunkDriver::MigrationReader];

/// 测试单块往返一致性（双驱动）
#[test]
fn round_trip_single_chunk() -> Result<()> {
  for driver in ALL_DRIVERS {
    let td = TestDirGuard::new("rt_single_chunk");
    assert_round_trip(
      driver,
      td.path(),
      b"mykey",
      &random_bytes(1024, 42),
      DEFAULT_MIGRATION_CHUNK_SIZE,
    )?;
  }
  OK
}

/// 测试多块流式往返一致性（双驱动）
#[test]
fn round_trip_multi_chunk() -> Result<()> {
  for driver in ALL_DRIVERS {
    let td = TestDirGuard::new("rt_multi_chunk");
    assert_round_trip(
      driver,
      td.path(),
      b"largekey",
      &random_bytes(DEFAULT_MIGRATION_CHUNK_SIZE * 3 + 1000, 123),
      DEFAULT_MIGRATION_CHUNK_SIZE,
    )?;
  }
  OK
}

/// 测试键长度大于单块大小的往返一致性（双驱动）
#[test]
fn round_trip_key_larger_than_chunk() -> Result<()> {
  for driver in ALL_DRIVERS {
    let td = TestDirGuard::new("rt_key_larger");
    assert_round_trip(
      driver,
      td.path(),
      &random_bytes(200, 66),
      &random_bytes(100, 55),
      64,
    )?;
  }
  OK
}

/// 测试超小数据块分片的往返一致性（双驱动）
#[test]
fn round_trip_small_chunk() -> Result<()> {
  for driver in ALL_DRIVERS {
    let td = TestDirGuard::new("rt_small_chunk");
    assert_round_trip(driver, td.path(), b"k", &random_bytes(500, 77), 64)?;
  }
  OK
}

/// 测试文件数据刚好填满首块的往返一致性（双驱动）
#[test]
fn round_trip_file_exactly_fills_first_chunk() -> Result<()> {
  for driver in ALL_DRIVERS {
    let td = TestDirGuard::new("rt_fill_first");
    assert_round_trip(driver, td.path(), b"k", &random_bytes(51, 88), 64)?;
  }
  OK
}

/// 测试精确最小分块阈值下的往返一致性（双驱动）
#[test]
fn round_trip_exact_min_chunk_size() -> Result<()> {
  for driver in ALL_DRIVERS {
    let td = TestDirGuard::new("rt_min_chunk");
    assert_round_trip(driver, td.path(), b"k", &random_bytes(300, 31), MIN_CHUNK_SIZE)?;
  }
  OK
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
