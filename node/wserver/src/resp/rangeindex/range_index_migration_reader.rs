//! 范围索引迁移读取器（对标 libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs）
//!
//! 持有文件数据源的序列化器异步包装（Rust 侧为 `io::Read` 同步源；C# 的
//! ReadAsync 在引擎读缓冲口径下等价为同步读，wserver 调用方均在阻塞
//! 线程上下文驱动）。循环推进序列化器处理阶段迁移（头部 → 文件数据 →
//! 尾部框），并在 Dispose / Drop 时关流 + 删除源侧临时快照，防止迁移
//! 快照残留。状态机本体由引擎承接（embed/wbftree/src/chunk.rs，1:1 对标
//! 同一 C# 类）；本结构是会话域包装：统一错误面与目标缓冲下界校验。

use std::{io::Read, path::PathBuf};

use wkv::RangeIndexMigrationReader as Engine;

use super::range_index_chunked_serializer::{ChunkStreamError, MIN_CHUNK_SIZE};

/// 迁移读取器默认文件读缓冲（1MiB）
///
/// libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs:DefaultFileReadBufferSize
/// （与序列化分块大小相互独立：读缓冲只影响文件系统读次数）
pub const DEFAULT_FILE_READ_BUFFER_SIZE: usize = 1 << 20;

/// 迁移读取器（会话域包装）
pub struct RangeIndexMigrationReader<R: Read>(Engine<R>);

impl<R: Read> RangeIndexMigrationReader<R> {
  /// 构造：包装序列化器与数据源
  ///
  /// `temp_file_path` 为本读取器拥有的快照文件路径（Dispose 时删除）；
  /// 数据源不背靠自有临时文件时传 None。`read_buffer_size` 为文件读缓冲
  /// 大小（须为正）
  ///
  /// libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs:#ctor
  pub fn new(
    serializer: super::range_index_chunked_serializer::RangeIndexChunkedSerializer,
    reader: R,
    temp_file_path: Option<PathBuf>,
    read_buffer_size: usize,
  ) -> Result<Self, ChunkStreamError> {
    if read_buffer_size == 0 {
      return Err(ChunkStreamError::InvalidState(
        "readBufferSize must be positive".to_string(),
      ));
    }
    Ok(Self(
      Engine::new(serializer.0, reader, temp_file_path, read_buffer_size)
        .map_err(|e| ChunkStreamError::InvalidState(e.to_string()))?,
    ))
  }

  /// libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs:ReadNextChunkAsync
  ///
  /// 读取下一分块：需要时从数据源补文件数据，再交序列化器框入目标缓冲；
  /// 循环以处理单次调用内的阶段迁移。完成协议：调用方以
  /// `while !reader.is_complete()` 驱动，发出最后字节的调用返回后
  /// `is_complete` 翻真；此后不得再调用。目标缓冲不足
  /// [`MIN_CHUNK_SIZE`] 时拒绝（见 [`Self::validate_destination`]）
  pub fn read_next_chunk_async(
    &mut self,
    destination: &mut [u8],
  ) -> Result<usize, ChunkStreamError> {
    self.validate_destination(destination.len())?;
    self
      .0
      .read_next_chunk(destination)
      .map_err(|e| ChunkStreamError::InvalidState(e.to_string()))
  }

  /// libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs:ReadNextChunk
  ///
  /// 非异步路径的同步同形体（Rust 单一实现，与 async 形态同体）
  pub fn read_next_chunk(&mut self, destination: &mut [u8]) -> Result<usize, ChunkStreamError> {
    self.read_next_chunk_async(destination)
  }

  /// libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs:ValidateDestination
  ///
  /// 目标缓冲须 ≥ [`MIN_CHUNK_SIZE`]（尾部框尺寸）：目标缓冲即序列化器的
  /// 框出缓冲，装不下最大单框元素（尾部框）则流永远无法完成
  pub fn validate_destination(&self, length: usize) -> Result<(), ChunkStreamError> {
    if length < MIN_CHUNK_SIZE {
      return Err(ChunkStreamError::InvalidState(format!(
        "destination must be at least {MIN_CHUNK_SIZE} bytes (the trailer size) so the stream can complete, got {length}"
      )));
    }
    Ok(())
  }

  /// libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs:SupplyFileDataOrThrow
  ///
  /// 数据源读尽而流未完成（读到 0 字节）即失败：文件被截断，剩余
  /// `file_data_remaining` 字节永远无法补齐。C# 以私有方法从序列化器直读
  /// 剩余量；Rust 引擎在读循环内联判定，此处以校验形态承接供边界测试直查
  pub fn supply_file_data_or_throw(
    &self,
    bytes_read: usize,
    file_data_remaining: u64,
  ) -> Result<(), ChunkStreamError> {
    if bytes_read == 0 && file_data_remaining > 0 {
      return Err(ChunkStreamError::InvalidState(format!(
        "RangeIndex file truncated: {file_data_remaining} bytes remaining"
      )));
    }
    Ok(())
  }

  /// 序列化器是否已输出全部数据
  #[inline]
  pub fn is_complete(&self) -> bool {
    self.0.is_complete()
  }

  /// 快照文件总大小
  #[inline]
  pub fn total_file_bytes(&self) -> u64 {
    self.0.total_file_bytes()
  }

  /// libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs:Dispose
  ///
  /// 关闭数据源并删除自有临时快照文件（尽力而为）；幂等（Drop 兜底）
  pub fn dispose(&mut self) {
    self.0.dispose();
  }
}

#[cfg(test)]
mod tests {
  use std::{fs, io};

  use tempfile::tempdir;

  use super::*;
  use crate::resp::rangeindex::range_index_chunked_serializer::RangeIndexChunkedSerializer;

  /// 从内存字节源构造读取器
  fn reader_for<'a>(
    file: &'a [u8],
    key: &[u8],
    stub: &[u8],
    temp: Option<PathBuf>,
  ) -> RangeIndexMigrationReader<&'a [u8]> {
    let serializer = RangeIndexChunkedSerializer::new(key, stub, file.len() as u64);
    RangeIndexMigrationReader::new(serializer, file, temp, DEFAULT_FILE_READ_BUFFER_SIZE).unwrap()
  }

  #[test]
  fn default_read_buffer_is_one_mib() {
    assert_eq!(DEFAULT_FILE_READ_BUFFER_SIZE, 1 << 20);
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
}
