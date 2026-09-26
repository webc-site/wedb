use std::{
  fs::{self, File, OpenOptions},
  io::Write,
  path::{Path, PathBuf},
};

use whasher::StreamHasher;

use crate::{
  chunk::{CHECKSUM_BYTES, FILE_LEN_BYTES, KEY_LEN_BYTES, STUB_LEN_BYTES},
  error::{Error, Result},
  stub::RANGE_INDEX_STUB_SIZE,
};

/// 允许的最大键长度安全上限 (64MB)
pub const MAX_KEY_LEN_BYTES: usize = 64 * 1024 * 1024;
/// 允许的最大快照文件大小安全上限 (64GB)
pub const MAX_FILE_LEN_BYTES: u64 = 64 * 1024 * 1024 * 1024;

/// 反序列化状态机阶段 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs:State)
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum DeserializerState {
  WaitingForKeyHeader,
  ReceivingKeyData,
  WaitingForFileHeader,
  ReceivingFileData,
  WaitingForTrailer,
  Complete,
  Error,
  Disposed,
}

/// 纯状态机分块反序列化器 (1:1 对标 Garnet RangeIndexChunkedDeserializer)
///
/// 接收流式数据块，就地重组键、落盘中间文件并校验校验和，最终恢复定长存根。
/// 协议损坏时状态转入 Error 态并记录具体原因，经
/// [`take_error`](Self::take_error) 获取 (对标 C# 的 logger.LogError 逐条上报)。
pub struct RangeIndexChunkedDeserializer {
  temp_path: PathBuf,
  file: Option<File>,
  state: DeserializerState,
  key: Vec<u8>,
  key_bytes_received: usize,
  total_file_bytes: u64,
  file_bytes_remaining: u64,
  hasher: StreamHasher,
  finalizer_stub: Vec<u8>,
  error: Option<Error>,
}

impl RangeIndexChunkedDeserializer {
  /// 创建新的反序列化器，指定文件写入的临时目标路径
  ///
  /// 签名保留 `Result` 以兼容消费方 `?` 链式用法（如迁移接收路径），当前构造本身不会失败
  pub fn new(temp_path: impl Into<PathBuf>) -> Result<Self> {
    let temp_path = temp_path.into();
    Ok(Self {
      temp_path,
      file: None,
      state: DeserializerState::WaitingForKeyHeader,
      key: Vec::new(),
      key_bytes_received: 0,
      total_file_bytes: 0,
      file_bytes_remaining: 0,
      hasher: StreamHasher::default(),
      finalizer_stub: Vec::new(),
      error: None,
    })
  }

  /// 是否已经完整完成反序列化并通过校验 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs:IsComplete)
  #[inline]
  pub fn is_complete(&self) -> bool {
    self.state == DeserializerState::Complete
  }

  /// 是否遇到无法恢复的协议损坏错误 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs:HasError)
  #[inline]
  pub fn has_error(&self) -> bool {
    self.state == DeserializerState::Error
  }

  /// 取走最近一次协议损坏的具体原因 (仅 [`has_error`](Self::has_error) 后有值)
  #[inline]
  pub fn take_error(&mut self) -> Option<Error> {
    self.error.take()
  }

  /// 置入不可恢复错误态并记录原因 (状态机 Error 态唯一入口)
  fn fail(&mut self, reason: Error) {
    self.error = Some(reason);
    self.state = DeserializerState::Error;
  }

  /// 提取反序列化得到的键 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs:Key)
  #[inline]
  pub fn key(&self) -> &[u8] {
    &self.key
  }

  /// 提取反序列化得到的存根二进制 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs:Stub)
  #[inline]
  pub fn stub(&self) -> &[u8] {
    &self.finalizer_stub
  }

  /// 临时数据文件路径 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs:TempPath)
  #[inline]
  pub fn temp_path(&self) -> &Path {
    &self.temp_path
  }

  /// 处理传入的数据块切片 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs:ProcessChunk)
  ///
  /// 内联承接 libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs:WriteFileBytes 与 libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs:ParseTrailer
  pub fn process_chunk(&mut self, mut data: &[u8]) -> Result<bool> {
    loop {
      match self.state {
        DeserializerState::Error | DeserializerState::Complete | DeserializerState::Disposed => {
          return Ok(false);
        }

        DeserializerState::WaitingForKeyHeader => {
          // 空数据块安全跳过
          if data.is_empty() {
            return Ok(true);
          }

          // 协议要求键长度头必须完整放入单个数据块
          let Some((key_len_bytes, rest)) = data.split_first_chunk::<KEY_LEN_BYTES>() else {
            self.fail(Error::Corrupted(format!(
              "键长度头被重新分块拆散 (仅 {} 字节，4 字节头部必须完整位于单个数据块内)",
              data.len()
            )));
            return Ok(false);
          };
          let key_len_i32 = i32::from_le_bytes(*key_len_bytes);
          data = rest;

          if key_len_i32 <= 0 || (key_len_i32 as usize) > MAX_KEY_LEN_BYTES {
            self.fail(Error::Corrupted(format!("键长度非法: {key_len_i32}")));
            return Ok(false);
          }

          let key_len = key_len_i32 as usize;
          self.key = vec![0u8; key_len];
          self.key_bytes_received = 0;
          self.state = DeserializerState::ReceivingKeyData;
        }

        DeserializerState::ReceivingKeyData => {
          let needed = self.key.len() - self.key_bytes_received;
          let n = needed.min(data.len());
          self.key[self.key_bytes_received..self.key_bytes_received + n]
            .copy_from_slice(&data[..n]);
          self.key_bytes_received += n;
          data = &data[n..];

          if self.key_bytes_received < self.key.len() {
            return Ok(true);
          }

          self.state = DeserializerState::WaitingForFileHeader;
        }

        DeserializerState::WaitingForFileHeader => {
          // 空数据块安全跳过
          if data.is_empty() {
            return Ok(true);
          }

          // 协议要求文件长度头必须完整放入单个数据块
          let Some((file_len_bytes, rest)) = data.split_first_chunk::<FILE_LEN_BYTES>() else {
            self.fail(Error::Corrupted(format!(
              "文件长度头被重新分块拆散 (仅 {} 字节，8 字节头部必须完整位于单个数据块内)",
              data.len()
            )));
            return Ok(false);
          };
          let file_len_i64 = i64::from_le_bytes(*file_len_bytes);
          data = rest;

          // 严格校验文件大小：若 <= 0 则转入 Error 并返回 false
          if file_len_i64 <= 0 || (file_len_i64 as u64) > MAX_FILE_LEN_BYTES {
            self.fail(Error::Corrupted(format!("文件长度非法: {file_len_i64}")));
            return Ok(false);
          }

          let file_len = file_len_i64 as u64;
          self.total_file_bytes = file_len;
          self.file_bytes_remaining = file_len;
          self.state = DeserializerState::ReceivingFileData;

          match OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.temp_path)
          {
            Ok(f) => self.file = Some(f),
            Err(e) => {
              self.fail(Error::Io(e));
              return Ok(false);
            }
          }
        }

        DeserializerState::ReceivingFileData => {
          if data.is_empty() {
            return Ok(true);
          }

          if self.file_bytes_remaining > 0 {
            let count = (data.len() as u64).min(self.file_bytes_remaining) as usize;
            let file_part = &data[..count];
            if let Some(ref mut f) = self.file {
              if let Err(e) = f.write_all(file_part) {
                self.fail(Error::Io(e));
                return Ok(false);
              }
            } else {
              self.fail(Error::Corrupted(
                "文件句柄缺失 (状态机未按 WaitingForFileHeader → ReceivingFileData 流转)".into(),
              ));
              return Ok(false);
            }
            self.hasher.write(file_part);
            self.file_bytes_remaining -= count as u64;
            data = &data[count..];
          }

          if self.file_bytes_remaining == 0 {
            // 数据屏障 (双屏障口径见 wdev::sync_dir 文档)：迁移 temp 文件内容
            // 在此掉电持久；其目录项屏障由发布路径 rename 换入后的
            // wdev::sync_dir 收口 (见 manager::lifecycle)
            if let Some(mut f) = self.file.take()
              && let Err(e) = f.flush().and_then(|_| f.sync_all())
            {
              self.fail(Error::Io(e));
              return Ok(false);
            }
            self.state = DeserializerState::WaitingForTrailer;
          } else {
            return Ok(true);
          }
        }

        DeserializerState::WaitingForTrailer => {
          if data.is_empty() {
            return Ok(true);
          }

          let min_trailer_len = CHECKSUM_BYTES + STUB_LEN_BYTES;
          let Some((checksum_bytes, after_checksum)) = data.split_first_chunk::<CHECKSUM_BYTES>()
          else {
            self.fail(Error::Corrupted(format!(
              "trailer 被重新分块拆散 (仅 {} 字节，checksum+stubLen 共 {min_trailer_len} 字节必须完整位于单个数据块内)",
              data.len()
            )));
            return Ok(false);
          };
          let received_hash = u64::from_le_bytes(*checksum_bytes);

          let Some((stub_len_bytes, after_stub_len)) =
            after_checksum.split_first_chunk::<STUB_LEN_BYTES>()
          else {
            self.fail(Error::Corrupted(format!(
              "trailer 被重新分块拆散 (仅 {} 字节，checksum+stubLen 共 {min_trailer_len} 字节必须完整位于单个数据块内)",
              data.len()
            )));
            return Ok(false);
          };
          let stub_len = i32::from_le_bytes(*stub_len_bytes);
          data = after_stub_len;

          // 严格校验存根长度：stub_len 必须精确等于 RANGE_INDEX_STUB_SIZE (35 字节)
          if stub_len != RANGE_INDEX_STUB_SIZE as i32 {
            self.fail(Error::Corrupted(format!(
              "存根长度非法: {stub_len} (流格式规定必须恰好 {RANGE_INDEX_STUB_SIZE} 字节)"
            )));
            return Ok(false);
          }

          // 严格校验 Trailer 尾部字节：完整流的 trailer (checksum+stubLen+stub 共 47
          // 字节) 必须单块到达且在 stub 末尾恰好结束；传输层重新分块拆散 trailer
          // 与截断/尾随字节的畸形流在此不可区分，一律判为协议损坏 (见模块文档分块规则)
          if data.len() != RANGE_INDEX_STUB_SIZE {
            self.fail(Error::Corrupted(format!(
              "trailer 尾部存根未与校验和同块完整到达: 剩余 {} 字节，应为 {RANGE_INDEX_STUB_SIZE} 字节；传输层不得把 47 字节 trailer 重新分块拆散",
              data.len()
            )));
            return Ok(false);
          }

          let calculated_checksum = self.hasher.finish();
          if received_hash != calculated_checksum {
            self.fail(Error::Corrupted(format!(
              "校验和不匹配: 接收 {received_hash:#x}, 计算 {calculated_checksum:#x}"
            )));
            return Ok(false);
          }

          self.finalizer_stub = data.to_vec();
          self.state = DeserializerState::Complete;
          return Ok(true);
        }
      }
    }
  }

  /// 释放反序列化器并清理临时文件 (1:1 对标 Garnet IDisposable.Dispose 与 libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs:CloseStream)
  ///
  /// libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs:Dispose
  pub fn dispose(&mut self) {
    if self.state == DeserializerState::Disposed {
      return;
    }
    self.state = DeserializerState::Disposed;
    self.file.take();
    if self.temp_path.exists() {
      let _ = fs::remove_file(&self.temp_path);
    }
  }
}

impl Drop for RangeIndexChunkedDeserializer {
  fn drop(&mut self) {
    self.dispose();
  }
}
