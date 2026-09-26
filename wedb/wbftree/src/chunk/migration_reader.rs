use std::{fs, io::Read, path::PathBuf};

use crate::{
  chunk::{MIN_CHUNK_SIZE, RangeIndexChunkedSerializer},
  error::{Error, Result},
};

/// 迁移读取器默认文件读缓冲（1MiB）
///
/// libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs:DefaultFileReadBufferSize
/// （与序列化分块大小相互独立：读缓冲只影响文件系统读次数）
pub const DEFAULT_FILE_READ_BUFFER_SIZE: usize = 1 << 20;

/// RangeIndex 迁移读取器 (1:1 对标 Garnet RangeIndexMigrationReader)
///
/// 封装了 `RangeIndexChunkedSerializer` 和底层的读取流（如 `File`），并在 Dispose / Drop 时负责关闭流并删除源侧的临时快照文件。
pub struct RangeIndexMigrationReader<R: Read> {
  serializer: RangeIndexChunkedSerializer,
  reader: Option<R>,
  temp_file_path: Option<PathBuf>,
  read_buffer: Vec<u8>,
  disposed: bool,
}

impl<R: Read> RangeIndexMigrationReader<R> {
  /// 创建新的迁移读取器
  pub fn new(
    serializer: RangeIndexChunkedSerializer,
    reader: R,
    temp_file_path: Option<PathBuf>,
    read_buffer_size: usize,
  ) -> Result<Self> {
    if read_buffer_size == 0 {
      return Err(Error::InvalidArgument(
        "read_buffer_size must be positive".into(),
      ));
    }
    Ok(Self {
      serializer,
      reader: Some(reader),
      temp_file_path,
      read_buffer: vec![0u8; read_buffer_size],
      disposed: false,
    })
  }

  /// 序列化是否已经全部完成
  #[inline]
  pub fn is_complete(&self) -> bool {
    self.serializer.is_complete()
  }

  /// 快照文件总大小
  #[inline]
  pub fn total_file_bytes(&self) -> u64 {
    self.serializer.total_file_bytes()
  }

  /// 读取下一个分块并写入 `destination` (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs:ReadNextChunk 与 libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs:ReadNextChunkAsync)
  ///
  /// 包含 libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs:ValidateDestination 校验与 libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs:SupplyFileDataOrThrow 数据源推进判定
  pub fn read_next_chunk(&mut self, mut destination: &mut [u8]) -> Result<usize> {
    if destination.len() < MIN_CHUNK_SIZE {
      return Err(Error::InvalidArgument(format!(
        "destination must be at least {MIN_CHUNK_SIZE} bytes (the trailer size) so the stream can complete"
      )));
    }

    let initial_len = destination.len();
    let reader = match self.reader.as_mut() {
      Some(r) => r,
      None => return Err(Error::InvalidArgument("Reader already disposed".into())),
    };

    while !self.serializer.is_complete() && !destination.is_empty() {
      if self.serializer.needs_file_data() {
        let max_read =
          (self.read_buffer.len() as u64).min(self.serializer.file_data_remaining()) as usize;
        let bytes_read = reader.read(&mut self.read_buffer[..max_read])?;
        if bytes_read == 0 && self.serializer.file_data_remaining() > 0 {
          return Err(Error::Corrupted(format!(
            "RangeIndex file truncated: {} bytes remaining",
            self.serializer.file_data_remaining()
          )));
        }
        self
          .serializer
          .supply_file_data(&self.read_buffer[..bytes_read]);
      }

      let written = self.serializer.move_next(destination)?;
      if written == 0 {
        break;
      }
      destination = &mut destination[written..];
    }

    Ok(initial_len - destination.len())
  }

  /// 释放读取器并清理临时文件
  ///
  /// libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs:Dispose
  pub fn dispose(&mut self) {
    if self.disposed {
      return;
    }
    self.disposed = true;
    self.reader.take();
    if let Some(ref path) = self.temp_file_path
      && path.exists()
    {
      let _ = fs::remove_file(path);
    }
  }
}

impl<R: Read> Drop for RangeIndexMigrationReader<R> {
  fn drop(&mut self) {
    self.dispose();
  }
}
