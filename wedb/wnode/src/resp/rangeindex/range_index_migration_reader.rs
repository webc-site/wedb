//! 范围索引迁移读取器（对标 libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs）
//!
//! 持有文件数据源的序列化器异步包装（Rust 侧为 `io::Read` 同步源；C# 的
//! ReadAsync 在引擎读缓冲口径下等价为同步读，wserver 调用方均在阻塞
//! 线程上下文驱动）。循环推进序列化器处理阶段迁移（头部 → 文件数据 →
//! 尾部框），并在 Dispose / Drop 时关流 + 删除源侧临时快照，防止迁移
//! 快照残留。状态机本体由引擎承接（wedb/wbftree/src/chunk.rs，1:1 对标
//! 同一 C# 类）；本结构是会话域包装：统一错误面与目标缓冲下界校验。

use std::{io::Read, path::PathBuf};

use wbftree::{MIN_CHUNK_SIZE, RangeIndexMigrationReader as Engine};

use super::range_index_chunked_serializer::ChunkStreamError;

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
