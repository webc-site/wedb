//! AOF 头层级：按日志拓扑选择线上格式
//! （对标 libs/server/AOF/AofHeader.cs:AofHeader / AofHeaderType /
//! AofShardedHeader / 事务与分块变体）。
//!
//! 头类型决定条目的线上格式：
//! - BasicHeader（16B）：单物理日志
//! - ShardedHeader（24B）：多物理日志的逐键条目（+ sequenceNumber）
//! - SingleLogTransactionHeader（50B）：单物理日志多回放的协调操作
//!   （+ participantCount + replayTaskAccessVector，用日志地址排序）
//! - ShardedLogTransactionHeader（58B）：多物理日志的协调操作
//!
//! 非事务类型另有分块变体（大对象值跨多条目），低两位与基础类型一致，
//! 且 ChunkedRecordFlag（0b0100）置位。

use std::mem::size_of;

/// 头类型判别值（对齐 C# AofHeaderType）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AofHeaderType {
  /// 单物理日志基础头。
  BasicHeader = 0,
  /// 多物理日志头（+ sequenceNumber）。
  ShardedHeader = 1,
  /// 单物理日志事务头。
  SingleLogTransactionHeader = 2,
  /// 多物理日志事务头。
  ShardedLogTransactionHeader = 3,
  /// BasicHeader 的分块变体。
  BasicChunkHeader = 4,
  /// ShardedHeader 的分块变体。
  ShardedChunkHeader = 5,
}

impl AofHeaderType {
  /// 全部成员（含分块变体），按判别值升序。
  pub const ALL: [AofHeaderType; 6] = [
    Self::BasicHeader,
    Self::ShardedHeader,
    Self::SingleLogTransactionHeader,
    Self::ShardedLogTransactionHeader,
    Self::BasicChunkHeader,
    Self::ShardedChunkHeader,
  ];

  /// 该类型的完整头尺寸（字节）。
  pub const fn total_size(self) -> usize {
    match self {
      Self::BasicHeader => AofHeader::TOTAL_SIZE,
      Self::ShardedHeader => AofShardedHeader::TOTAL_SIZE,
      Self::SingleLogTransactionHeader => AofSingleLogTransactionHeader::TOTAL_SIZE,
      Self::ShardedLogTransactionHeader => AofShardedLogTransactionHeader::TOTAL_SIZE,
      Self::BasicChunkHeader => AofHeader::TOTAL_SIZE + AofChunkHeader::TOTAL_SIZE,
      Self::ShardedChunkHeader => AofShardedHeader::TOTAL_SIZE + AofChunkHeader::TOTAL_SIZE,
    }
  }
}

/// 基础 AOF 头（16B）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AofHeader {
  /// AOF 版本。
  pub aof_header_version: u8,
  /// 头类型 + 标志位。
  pub flags: u8,
  /// 操作类型（AofEntryType 判别值）。
  pub op_type: u8,
  /// 存储过程 id（与 databaseId 联合）。
  pub procedure_id: u8,
  /// 数据库 id（FLUSH 命令用；与 procedureId 联合）。
  pub database_id: u8,
  /// 存储版本。
  pub store_version: i64,
  /// 会话 id。
  pub session_id: i32,
}

impl AofHeader {
  /// 头尺寸。
  pub const TOTAL_SIZE: usize = 16;
  /// 当前 AOF 头版本。
  pub const AOF_HEADER_VERSION: u8 = 5;
  /// 本构建可读的最高版本（更高版本由更新构建写入，不可安全解释）。
  pub const MAX_SUPPORTED_AOF_HEADER_VERSION: u8 = Self::AOF_HEADER_VERSION;
  /// flags 中标识头类型的位段（3 位）。
  pub const AOF_HEADER_TYPE_MASK: u8 = 0b0111;
  /// 分块记录标志（类型位段最高位）。
  pub const CHUNKED_RECORD_FLAG: u8 = 0b0100;
  /// Unsafe 截断标志（FLUSH 命令用）。
  pub const UNSAFE_TRUNCATE_LOG_FLAG: u8 = 0b1000;

  /// C# 默认构造：flags 清零、版本置当前。
  pub const fn new() -> Self {
    Self {
      aof_header_version: Self::AOF_HEADER_VERSION,
      flags: 0,
      op_type: 0,
      procedure_id: 0,
      database_id: 0,
      store_version: 0,
      session_id: 0,
    }
  }

  /// libs/server/AOF/AofHeader.cs:UnsafeTruncateLog（getter）
  ///
  /// 是否 Unsafe 截断日志（FLUSH 命令）。
  pub const fn unsafe_truncate_log(&self) -> bool {
    (self.flags & Self::UNSAFE_TRUNCATE_LOG_FLAG) != 0
  }

  /// Setter for unsafe_truncate_log (AofHeader.cs UnsafeTruncateLog.set)
  pub fn set_unsafe_truncate_log(&mut self, value: bool) {
    if value {
      self.flags |= Self::UNSAFE_TRUNCATE_LOG_FLAG;
    } else {
      self.flags &= !Self::UNSAFE_TRUNCATE_LOG_FLAG;
    }
  }

  /// libs/server/AOF/AofHeader.cs:HeaderType
  pub const fn header_type(&self) -> Option<AofHeaderType> {
    match self.flags & Self::AOF_HEADER_TYPE_MASK {
      0 => Some(AofHeaderType::BasicHeader),
      1 => Some(AofHeaderType::ShardedHeader),
      2 => Some(AofHeaderType::SingleLogTransactionHeader),
      3 => Some(AofHeaderType::ShardedLogTransactionHeader),
      4 => Some(AofHeaderType::BasicChunkHeader),
      5 => Some(AofHeaderType::ShardedChunkHeader),
      _ => None,
    }
  }

  /// Setter for header_type (AofHeader.cs HeaderType.set)
  pub fn set_header_type(&mut self, value: AofHeaderType) {
    debug_assert!((value as u8) <= Self::AOF_HEADER_TYPE_MASK);
    self.flags = (self.flags & !Self::AOF_HEADER_TYPE_MASK) | value as u8;
  }

  /// libs/server/AOF/AofHeader.cs:IsChunked
  ///
  /// 本记录是否为更大分块逻辑记录的一片。
  pub const fn is_chunked(&self) -> bool {
    (self.flags & Self::CHUNKED_RECORD_FLAG) != 0
  }

  /// 从条目起始字节解析头（16B LE 布局，字段偏移与 C# 逐字节一致）。
  pub const fn parse(entry: &[u8]) -> Option<Self> {
    let Some(chunk) = entry.first_chunk::<{ Self::TOTAL_SIZE }>() else {
      return None;
    };
    Some(Self {
      aof_header_version: chunk[0],
      flags: chunk[1],
      op_type: chunk[2],
      procedure_id: chunk[3],
      database_id: chunk[3],
      store_version: i64::from_le_bytes([
        chunk[4], chunk[5], chunk[6], chunk[7], chunk[8], chunk[9], chunk[10], chunk[11],
      ]),
      session_id: i32::from_le_bytes([chunk[12], chunk[13], chunk[14], chunk[15]]),
    })
  }

  /// 序列化为 16B（LE 布局）。
  pub fn to_bytes(&self) -> [u8; Self::TOTAL_SIZE] {
    let mut out = [0u8; Self::TOTAL_SIZE];
    out[0] = self.aof_header_version;
    out[1] = self.flags;
    out[2] = self.op_type;
    out[3] = if self.procedure_id != 0 {
      self.procedure_id
    } else {
      self.database_id
    };
    out[4..12].copy_from_slice(&self.store_version.to_le_bytes());
    out[12..16].copy_from_slice(&self.session_id.to_le_bytes());
    out
  }

  /// libs/server/AOF/AofHeader.cs:SkipHeader
  ///
  /// 返回条目载荷的起始偏移（按头类型跳过完整头）；未知类型返回 None
  ///（对齐 C# GarnetException 路径）。
  pub const fn skip_header(entry: &[u8]) -> Option<usize> {
    let Some(header) = AofHeader::parse(entry) else {
      return None;
    };
    match header.header_type() {
      Some(t) => Some(t.total_size()),
      None => None,
    }
  }

  /// libs/server/AOF/AofHeader.cs:GetChunkedHeaderRef
  ///
  /// 返回分块记录的内嵌 [`AofChunkHeader`] 在条目内的偏移；
  /// 非分块类型返回 None（对齐 C# GarnetException 路径）。
  pub fn get_chunked_header_ref(entry: &[u8]) -> Option<(usize, AofChunkHeader)> {
    let header = AofHeader::parse(entry)?;
    let offset = match header.header_type()? {
      AofHeaderType::BasicChunkHeader => AofHeader::TOTAL_SIZE,
      AofHeaderType::ShardedChunkHeader => AofShardedHeader::TOTAL_SIZE,
      _ => return None,
    };
    Some((offset, AofChunkHeader::parse(&entry[offset..])?))
  }
}

impl Default for AofHeader {
  fn default() -> Self {
    Self::new()
  }
}

/// 多物理日志头：BasicHeader + sequenceNumber。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AofShardedHeader {
  /// 基础头。
  pub basic: AofHeader,
  /// 读一致性协议用的跨子日志排序号。
  pub sequence_number: i64,
}

impl AofShardedHeader {
  /// 头尺寸。
  pub const TOTAL_SIZE: usize = AofHeader::TOTAL_SIZE + 8;

  /// 序列化为 24B（LE 布局）。
  pub fn to_bytes(&self) -> [u8; Self::TOTAL_SIZE] {
    let mut out = [0u8; Self::TOTAL_SIZE];
    out[..AofHeader::TOTAL_SIZE].copy_from_slice(&self.basic.to_bytes());
    out[AofHeader::TOTAL_SIZE..].copy_from_slice(&self.sequence_number.to_le_bytes());
    out
  }

  /// 解析。
  pub fn parse(entry: &[u8]) -> Option<Self> {
    let chunk = entry.first_chunk::<{ Self::TOTAL_SIZE }>()?;
    let seq_bytes: [u8; 8] = chunk[AofHeader::TOTAL_SIZE..Self::TOTAL_SIZE]
      .try_into()
      .ok()?;
    Some(Self {
      basic: AofHeader::parse(chunk)?,
      sequence_number: i64::from_le_bytes(seq_bytes),
    })
  }
}

/// 协调操作的重放任务位图字节数（每物理子日志最多 256 回放任务）。
pub const REPLAY_TASK_ACCESS_VECTOR_BYTES: usize = 32;

/// 单物理日志事务头：BasicHeader + participantCount + 位图。
#[derive(Debug, Clone, Copy)]
pub struct AofSingleLogTransactionHeader {
  /// 基础头。
  pub basic: AofHeader,
  /// 参与事务的回放任务总数（虚拟子日志回放同步用）。
  pub participant_count: i16,
  /// 参与回放任务位图。
  pub replay_task_access_vector: [u8; REPLAY_TASK_ACCESS_VECTOR_BYTES],
}

impl AofSingleLogTransactionHeader {
  /// 头尺寸。
  pub const TOTAL_SIZE: usize = AofHeader::TOTAL_SIZE + 2 + REPLAY_TASK_ACCESS_VECTOR_BYTES;

  /// 序列化为 50B（LE 布局）。
  pub fn to_bytes(&self) -> [u8; Self::TOTAL_SIZE] {
    let mut out = [0u8; Self::TOTAL_SIZE];
    out[..AofHeader::TOTAL_SIZE].copy_from_slice(&self.basic.to_bytes());
    out[AofHeader::TOTAL_SIZE..AofHeader::TOTAL_SIZE + 2]
      .copy_from_slice(&self.participant_count.to_le_bytes());
    out[AofHeader::TOTAL_SIZE + 2..].copy_from_slice(&self.replay_task_access_vector);
    out
  }

  /// 解析。
  pub fn parse(entry: &[u8]) -> Option<Self> {
    if entry.len() < Self::TOTAL_SIZE {
      return None;
    }
    let mut vector = [0u8; REPLAY_TASK_ACCESS_VECTOR_BYTES];
    vector.copy_from_slice(&entry[AofHeader::TOTAL_SIZE + 2..Self::TOTAL_SIZE]);
    Some(Self {
      basic: AofHeader::parse(entry)?,
      participant_count: i16::from_le_bytes(
        entry[AofHeader::TOTAL_SIZE..AofHeader::TOTAL_SIZE + 2]
          .try_into()
          .expect("长度恰为 2"),
      ),
      replay_task_access_vector: vector,
    })
  }
}

/// 多物理日志事务头：ShardedHeader + participantCount + 位图。
#[derive(Debug, Clone, Copy)]
pub struct AofShardedLogTransactionHeader {
  /// 分片头。
  pub sharded: AofShardedHeader,
  /// 参与事务的回放任务总数。
  pub participant_count: i16,
  /// 参与回放任务位图。
  pub replay_task_access_vector: [u8; REPLAY_TASK_ACCESS_VECTOR_BYTES],
}

impl AofShardedLogTransactionHeader {
  /// 头尺寸。
  pub const TOTAL_SIZE: usize = AofShardedHeader::TOTAL_SIZE + 2 + REPLAY_TASK_ACCESS_VECTOR_BYTES;

  /// 序列化为 58B（LE 布局）。
  pub fn to_bytes(&self) -> [u8; Self::TOTAL_SIZE] {
    let mut out = [0u8; Self::TOTAL_SIZE];
    out[..AofShardedHeader::TOTAL_SIZE].copy_from_slice(&self.sharded.to_bytes());
    out[AofShardedHeader::TOTAL_SIZE..AofShardedHeader::TOTAL_SIZE + 2]
      .copy_from_slice(&self.participant_count.to_le_bytes());
    out[AofShardedHeader::TOTAL_SIZE + 2..].copy_from_slice(&self.replay_task_access_vector);
    out
  }

  /// 解析（与 AofSingleLogTransactionHeader::parse 对称）。
  pub fn parse_sharded(entry: &[u8]) -> Option<Self> {
    if entry.len() < Self::TOTAL_SIZE {
      return None;
    }
    let sharded = AofShardedHeader::parse(entry)?;
    let mut vector = [0u8; REPLAY_TASK_ACCESS_VECTOR_BYTES];
    vector.copy_from_slice(&entry[AofShardedHeader::TOTAL_SIZE + 2..Self::TOTAL_SIZE]);
    Some(Self {
      sharded,
      participant_count: i16::from_le_bytes(
        entry[AofShardedHeader::TOTAL_SIZE..AofShardedHeader::TOTAL_SIZE + 2]
          .try_into()
          .expect("长度恰为 2"),
      ),
      replay_task_access_vector: vector,
    })
  }
}

/// 分块帧头（28B = 3×u32 + u64 + i64）：长度三元组 + objectId + keyHash。
///（对齐 C# AofChunkHeader.cs:AofChunkHeader.TotalSize）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AofChunkHeader {
  /// 溢出 key 长度。
  pub overflow_key_length: u32,
  /// 溢出 value 长度。
  pub overflow_value_length: u32,
  /// input 长度。
  pub input_length: u32,
  /// 分块对象 id。
  pub object_id: u64,
  /// key 哈希。
  pub key_hash: i64,
}

impl AofChunkHeader {
  /// 头尺寸。
  pub const TOTAL_SIZE: usize = 3 * size_of::<u32>() + size_of::<u64>() + size_of::<i64>();
  /// objectId 字段偏移。
  pub const OBJECT_ID_OFFSET: usize = 3 * size_of::<u32>();

  /// 序列化为 28B（LE 布局）。
  pub fn to_bytes(&self) -> [u8; Self::TOTAL_SIZE] {
    let mut out = [0u8; Self::TOTAL_SIZE];
    out[..4].copy_from_slice(&self.overflow_key_length.to_le_bytes());
    out[4..8].copy_from_slice(&self.overflow_value_length.to_le_bytes());
    out[8..12].copy_from_slice(&self.input_length.to_le_bytes());
    out[Self::OBJECT_ID_OFFSET..Self::OBJECT_ID_OFFSET + 8]
      .copy_from_slice(&self.object_id.to_le_bytes());
    out[Self::OBJECT_ID_OFFSET + 8..].copy_from_slice(&self.key_hash.to_le_bytes());
    out
  }

  /// 解析。
  pub fn parse(entry: &[u8]) -> Option<Self> {
    let chunk = entry.first_chunk::<{ Self::TOTAL_SIZE }>()?;
    let klen_bytes: [u8; 4] = chunk[0..4].try_into().ok()?;
    let vlen_bytes: [u8; 4] = chunk[4..8].try_into().ok()?;
    let inlen_bytes: [u8; 4] = chunk[8..12].try_into().ok()?;
    let oid_bytes: [u8; 8] = chunk[Self::OBJECT_ID_OFFSET..Self::OBJECT_ID_OFFSET + 8]
      .try_into()
      .ok()?;
    let khash_bytes: [u8; 8] = chunk[Self::OBJECT_ID_OFFSET + 8..Self::TOTAL_SIZE]
      .try_into()
      .ok()?;
    Some(Self {
      overflow_key_length: u32::from_le_bytes(klen_bytes),
      overflow_value_length: u32::from_le_bytes(vlen_bytes),
      input_length: u32::from_le_bytes(inlen_bytes),
      object_id: u64::from_le_bytes(oid_bytes),
      key_hash: i64::from_le_bytes(khash_bytes),
    })
  }
}

#[cfg(test)]
mod tests {
  use super::{AofChunkHeader, AofHeader, AofHeaderType};

  #[test]
  fn header_roundtrip_and_flags() {
    let mut h = AofHeader::new();
    h.set_header_type(AofHeaderType::BasicHeader);
    h.op_type = 0x00;
    h.store_version = 42;
    h.session_id = -7;
    let bytes = h.to_bytes();
    let parsed = AofHeader::parse(&bytes).unwrap();
    assert_eq!(parsed, h);
    assert_eq!(parsed.header_type(), Some(AofHeaderType::BasicHeader));
    assert!(!parsed.is_chunked());
    assert!(!parsed.unsafe_truncate_log());

    h.set_unsafe_truncate_log(true);
    h.set_header_type(AofHeaderType::ShardedChunkHeader);
    assert!(h.unsafe_truncate_log());
    assert!(h.is_chunked());
    assert_eq!(h.header_type(), Some(AofHeaderType::ShardedChunkHeader));
  }

  #[test]
  fn skip_header_offsets() {
    for (t, size) in [
      (AofHeaderType::BasicHeader, 16),
      (AofHeaderType::ShardedHeader, 24),
      (AofHeaderType::SingleLogTransactionHeader, 50),
      (AofHeaderType::ShardedLogTransactionHeader, 58),
    ] {
      let mut h = AofHeader::new();
      h.set_header_type(t);
      assert_eq!(AofHeader::skip_header(&h.to_bytes()), Some(size));
    }
  }

  #[test]
  fn chunk_header_ref() {
    let mut h = AofHeader::new();
    h.set_header_type(AofHeaderType::BasicChunkHeader);
    let mut entry = h.to_bytes().to_vec();
    let chunk = AofChunkHeader {
      overflow_key_length: 8,
      overflow_value_length: 0,
      input_length: 4,
      object_id: 7,
      key_hash: -1,
    };
    let mut chunk_bytes = Vec::new();
    chunk_bytes.extend_from_slice(&chunk.overflow_key_length.to_le_bytes());
    chunk_bytes.extend_from_slice(&chunk.overflow_value_length.to_le_bytes());
    chunk_bytes.extend_from_slice(&chunk.input_length.to_le_bytes());
    chunk_bytes.extend_from_slice(&chunk.object_id.to_le_bytes());
    chunk_bytes.extend_from_slice(&chunk.key_hash.to_le_bytes());
    entry.extend_from_slice(&chunk_bytes);

    let (offset, parsed) = AofHeader::get_chunked_header_ref(&entry).unwrap();
    assert_eq!(offset, 16);
    assert_eq!(parsed, chunk);

    // 非分块类型返回 None。
    let mut plain = AofHeader::new();
    plain.set_header_type(AofHeaderType::BasicHeader);
    assert!(AofHeader::get_chunked_header_ref(&plain.to_bytes()).is_none());
  }
}
