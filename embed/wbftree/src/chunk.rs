//! RangeIndex 分块流式序列化与反序列化状态机 (1:1 对标 Garnet RangeIndexChunkedSerializer 与 RangeIndexChunkedDeserializer)
//!
//! # 流格式
//!
//! 单条 RangeIndex 迁移记录的完整字节流：
//!
//! ```text
//! [4-byte keyLen (LE)][key bytes][8-byte fileBytes (LE)][file payload][8-byte checksum (LE)][4-byte stubLen (LE)][stub bytes]
//! ```
//!
//! # 分块规则 (传输层契约)
//!
//! 序列化侧由调用方按任意块长切 `move_next` 输出；键数据与文件正文可跨任意多块，
//! 但以下三处具有**单块原子性**（与 C# 逐条对应，[`MIN_CHUNK_SIZE`] = 47 字节即
//! 由最长的单块结构 trailer 决定）：
//!
//! 1. `keyLen` 头部必须完整位于一个块内；
//! 2. `fileBytes` 头部必须完整位于一个块内；
//! 3. **trailer（checksum + stubLen + stub 共 47 字节）必须整体在一个块内到达，
//!    且流在该块末尾恰好结束**——[`RangeIndexChunkedDeserializer`] 在 trailer 阶段
//!    严格校验「剩余 35 字节即 stub」，传输层若把 trailer 重新分块拆散（哪怕拆成
//!    连续的合法块）会被判为协议损坏而非等待续块：状态机无法区分「拆散的 trailer」
//!    与「截断/带尾随字节的畸形流」，两者都表现为同一不可恢复错误。
//!    违反原因经 [`RangeIndexChunkedDeserializer::take_error`](RangeIndexChunkedDeserializer::take_error)
//!    获取。[`RangeIndexMigrationReader`] 按 `dest.len() >= MIN_CHUNK_SIZE` 输出
//!    天然满足该契约；接收端按 47 字节以上缓冲循环调 `read_next_chunk` 亦然。
//!
//! # 与源的刻意差异
//!
//! 校验和用 [`StreamHasher`]（whasher 的 gxhash 硬件加速实现），不与 C# 的
//! XxHash64 逐位兼容（本项目统一 gxhash 后端，见 sync.md）。

use std::{
  fs::{self, File, OpenOptions},
  io::{Read, Write},
  mem,
  path::{Path, PathBuf},
};

pub use whasher::{StreamHasher, compute_checksum, compute_checksum_with_seed};

use crate::{
  error::{Error, Result},
  stub::RANGE_INDEX_STUB_SIZE,
};

/// 键长度字段长度 (4 字节小端无符号整数)
pub const KEY_LEN_BYTES: usize = 4;
/// 文件总长度字段长度 (8 字节小端无符号整数)
pub const FILE_LEN_BYTES: usize = 8;
/// 校验和字段长度 (8 字节小端无符号整数)
pub const CHECKSUM_BYTES: usize = 8;
/// 存根长度字段长度 (4 字节小端无符号整数)
pub const STUB_LEN_BYTES: usize = 4;

/// 允许的最大键长度安全上限 (64MB)
pub const MAX_KEY_LEN_BYTES: usize = 64 * 1024 * 1024;
/// 允许的最大快照文件大小安全上限 (64GB)
pub const MAX_FILE_LEN_BYTES: u64 = 64 * 1024 * 1024 * 1024;

/// 保证分块序列化器能够向前推进的最小分块长度
///
/// 必须至少容纳最大的单块尾部结构：`[8-byte checksum][4-byte stubLen][35-byte stub]` = 47 字节
pub const MIN_CHUNK_SIZE: usize = CHECKSUM_BYTES + STUB_LEN_BYTES + RANGE_INDEX_STUB_SIZE;

/// 序列化状态阶段
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum SerializerPhase {
  KeyHeader,
  KeyData,
  FileHeader,
  FileData,
  Trailer,
  Done,
}

/// 纯状态机分块序列化器 (1:1 对标 Garnet RangeIndexChunkedSerializer)
///
/// 不直接进行磁盘 I/O，由调用方在需要文件数据时调用 [`supply_file_data`](Self::supply_file_data) 供给数据。
pub struct RangeIndexChunkedSerializer {
  key_bytes: Vec<u8>,
  stub_bytes: Vec<u8>,
  total_file_bytes: u64,
  file_bytes_emitted: u64,
  key_bytes_emitted: usize,
  hasher: StreamHasher,
  phase: SerializerPhase,
  unprocessed_file_data: Vec<u8>,
  unprocessed_offset: usize,
}

impl RangeIndexChunkedSerializer {
  /// 创建新的分块序列化器 (1:1 对标 libs/cluster/Server/Gossip/Gossip.cs:new RangeIndexChunkedSerializer(key, stub, totalFileBytes))
  pub fn new(key: &[u8], stub: &[u8], total_file_bytes: u64) -> Self {
    Self {
      key_bytes: key.to_vec(),
      stub_bytes: stub.to_vec(),
      total_file_bytes,
      file_bytes_emitted: 0,
      key_bytes_emitted: 0,
      hasher: StreamHasher::default(),
      phase: SerializerPhase::KeyHeader,
      unprocessed_file_data: Vec::new(),
      unprocessed_offset: 0,
    }
  }

  /// 快照文件声明的总字节大小
  #[inline]
  pub fn total_file_bytes(&self) -> u64 {
    self.total_file_bytes
  }

  /// 序列化是否已经全部完成
  #[inline]
  pub fn is_complete(&self) -> bool {
    self.phase == SerializerPhase::Done
  }

  /// 当前是否正处于 FileData 阶段并急需调用方喂入文件数据切片
  #[inline]
  pub fn needs_file_data(&self) -> bool {
    self.phase == SerializerPhase::FileData
      && self.file_bytes_emitted < self.total_file_bytes
      && self.unprocessed_offset >= self.unprocessed_file_data.len()
  }

  /// 剩余待发送的文件字节数
  #[inline]
  pub fn file_data_remaining(&self) -> u64 {
    self
      .total_file_bytes
      .saturating_sub(self.file_bytes_emitted)
  }

  /// 供给一段文件数据切片
  pub fn supply_file_data(&mut self, data: &[u8]) {
    self.unprocessed_file_data.clear();
    self.unprocessed_file_data.extend_from_slice(data);
    self.unprocessed_offset = 0;
  }

  /// 生成下一个数据块，向 `dest` 写入尽可能多的帧数据
  ///
  /// 返回实际写入 `dest` 的字节数
  pub fn move_next(&mut self, dest: &mut [u8]) -> Result<usize> {
    if self.phase == SerializerPhase::Done {
      return Err(Error::InvalidArgument(
        "Serializer has already completed".into(),
      ));
    }

    let dest_capacity = dest.len();
    let mut written = 0;

    // 1. 键长度头部 [4-byte keyLen] (必须完整容纳在单块中)
    if self.phase == SerializerPhase::KeyHeader {
      if dest_capacity - written < KEY_LEN_BYTES {
        return Ok(written);
      }
      let key_len = (self.key_bytes.len() as u32).to_le_bytes();
      dest[written..written + KEY_LEN_BYTES].copy_from_slice(&key_len);
      written += KEY_LEN_BYTES;
      self.phase = SerializerPhase::KeyData;
    }

    // 2. 键字节数据（支持跨块切片）
    if self.phase == SerializerPhase::KeyData {
      let remain_key = self.key_bytes.len() - self.key_bytes_emitted;
      let avail_dest = dest_capacity - written;
      let to_copy = remain_key.min(avail_dest);
      dest[written..written + to_copy]
        .copy_from_slice(&self.key_bytes[self.key_bytes_emitted..self.key_bytes_emitted + to_copy]);
      written += to_copy;
      self.key_bytes_emitted += to_copy;

      if self.key_bytes_emitted < self.key_bytes.len() {
        return Ok(written);
      }
      self.phase = SerializerPhase::FileHeader;
    }

    // 3. 文件总长度头部 [8-byte fileBytes] (必须完整容纳在单块中)
    if self.phase == SerializerPhase::FileHeader {
      if dest_capacity - written < FILE_LEN_BYTES {
        return Ok(written);
      }
      dest[written..written + FILE_LEN_BYTES].copy_from_slice(&self.total_file_bytes.to_le_bytes());
      written += FILE_LEN_BYTES;
      self.phase = SerializerPhase::FileData;
    }

    // 4. 文件正文数据（流式搬运已供给的数据）
    if self.phase == SerializerPhase::FileData {
      if self.file_bytes_emitted < self.total_file_bytes {
        let avail_dest = dest_capacity - written;
        if avail_dest == 0 {
          return Ok(written);
        }
        let max_copy = ((self.total_file_bytes - self.file_bytes_emitted) as usize).min(avail_dest);
        let unproc_avail = self.unprocessed_file_data.len() - self.unprocessed_offset;
        let to_copy = max_copy.min(unproc_avail);
        if to_copy == 0 {
          return Ok(written);
        }
        let src_slice =
          &self.unprocessed_file_data[self.unprocessed_offset..self.unprocessed_offset + to_copy];
        dest[written..written + to_copy].copy_from_slice(src_slice);
        self.hasher.write(src_slice);
        written += to_copy;
        self.unprocessed_offset += to_copy;
        self.file_bytes_emitted += to_copy as u64;
      }

      if self.file_bytes_emitted >= self.total_file_bytes {
        self.phase = SerializerPhase::Trailer;
      }
    }

    // 5. 尾部信息 [8-byte checksum][4-byte stubLen][stub] (必须完整容纳在单块中)
    if self.phase == SerializerPhase::Trailer {
      let trailer_len = CHECKSUM_BYTES + STUB_LEN_BYTES + self.stub_bytes.len();
      if dest_capacity - written < trailer_len {
        return Ok(written);
      }

      // Checksum (流式哈希收敛)
      let actual_checksum = self.hasher.finish();
      dest[written..written + CHECKSUM_BYTES].copy_from_slice(&actual_checksum.to_le_bytes());
      written += CHECKSUM_BYTES;

      // Stub Len
      let stub_len = (self.stub_bytes.len() as u32).to_le_bytes();
      dest[written..written + STUB_LEN_BYTES].copy_from_slice(&stub_len);
      written += STUB_LEN_BYTES;

      // Stub Payload
      dest[written..written + self.stub_bytes.len()].copy_from_slice(&self.stub_bytes);
      written += self.stub_bytes.len();

      self.phase = SerializerPhase::Done;
    }

    Ok(written)
  }
}

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

  /// 是否已经完整完成反序列化并通过校验
  #[inline]
  pub fn is_complete(&self) -> bool {
    self.state == DeserializerState::Complete
  }

  /// 是否遇到无法恢复的协议损坏错误
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

  /// 提取反序列化得到的键
  #[inline]
  pub fn key(&self) -> &[u8] {
    &self.key
  }

  /// 提取反序列化得到的存根二进制
  #[inline]
  pub fn stub(&self) -> &[u8] {
    &self.finalizer_stub
  }

  /// 临时数据文件路径
  #[inline]
  pub fn temp_path(&self) -> &Path {
    &self.temp_path
  }

  /// 处理传入的数据块切片 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs:ProcessChunk)
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

  /// 释放反序列化器并清理临时文件 (1:1 对标 Garnet IDisposable.Dispose)
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

  /// 取走临时文件所有权，避免 Drop 时被删除
  pub fn take_temp_path(mut self) -> PathBuf {
    self.state = DeserializerState::Disposed;
    self.file.take();
    mem::take(&mut self.temp_path)
  }
}

impl Drop for RangeIndexChunkedDeserializer {
  fn drop(&mut self) {
    self.dispose();
  }
}

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

  /// 读取下一个分块并写入 `destination`
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
