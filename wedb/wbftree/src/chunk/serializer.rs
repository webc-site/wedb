//!
//! # 与源的刻意差异
//!
//! 校验和用 [`StreamHasher`]（whasher 的 gxhash 硬件加速实现），不与 C# 的
//! XxHash64 逐位兼容（本项目统一 gxhash 后端，见 sync.md）。

use whasher::StreamHasher;

use crate::{
  chunk::{CHECKSUM_BYTES, FILE_LEN_BYTES, KEY_LEN_BYTES, STUB_LEN_BYTES},
  error::{Error, Result},
  stub::RANGE_INDEX_STUB_SIZE,
};

/// 保证分块序列化器能够向前推进的最小分块长度
///
/// libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs:MinChunkSize
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
  /// 创建新的分块序列化器 (1:1 对标 libs/cluster/Server/Gossip/Gossip.cs 中构造 RangeIndexChunkedSerializer(key, stub, totalFileBytes))
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

  /// 快照文件声明的总字节大小 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs:TotalFileBytes)
  #[inline]
  pub fn total_file_bytes(&self) -> u64 {
    self.total_file_bytes
  }

  /// 序列化是否已经全部完成 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs:IsComplete)
  #[inline]
  pub fn is_complete(&self) -> bool {
    self.phase == SerializerPhase::Done
  }

  /// 当前是否正处于 FileData 阶段并急需调用方喂入文件数据切片 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs:NeedsFileData)
  #[inline]
  pub fn needs_file_data(&self) -> bool {
    self.phase == SerializerPhase::FileData
      && self.file_bytes_emitted < self.total_file_bytes
      && self.unprocessed_offset >= self.unprocessed_file_data.len()
  }

  /// 剩余待发送的文件字节数 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs:FileDataRemaining)
  #[inline]
  pub fn file_data_remaining(&self) -> u64 {
    self
      .total_file_bytes
      .saturating_sub(self.file_bytes_emitted)
  }

  /// 供给一段文件数据切片 (libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs:SupplyFileData)
  pub fn supply_file_data(&mut self, data: &[u8]) {
    self.unprocessed_file_data.clear();
    self.unprocessed_file_data.extend_from_slice(data);
    self.unprocessed_offset = 0;
  }

  /// 生成下一个数据块，向 `dest` 写入尽可能多的帧数据 (libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs:MoveNext)
  ///
  /// 内联承接 libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs:WriteTrailer 尾部信息写入
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
        let remaining = self.total_file_bytes - self.file_bytes_emitted;
        let max_copy = remaining.min(avail_dest as u64) as usize;
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
