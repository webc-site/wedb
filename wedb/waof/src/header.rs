use std::mem::size_of;

use strum::FromRepr;
use wbase::{
  crc::{Crc32Hasher, crc32},
  entry_type::REPLAY_TASK_ACCESS_VECTOR_BYTES,
};

use super::error::{Error, Result};

/// WAL 记录头定长字节大小（8 字节：4 字节 entry_len + 4 字节 crc32）
pub const RECORD_HEADER_LEN: usize = 8;

/// 空负载记录的 CRC32 哨兵值（刻意取非 0）
///
/// 使全零 8 字节头唯一对应扇区填充（padding）/崩溃残缺尾部，而已提交的空记录
/// 携带非零哨兵可在崩溃恢复中被识别，兑现 commit 的持久性承诺
/// （对照 C# TsavoriteLog：其记录头含非零 AllocatedSize 字段天然可区分，无此歧义）
const EMPTY_PAYLOAD_CRC: u32 = 0xFFFF_FFFF;

/// 计算记录负载的 CRC32 校验码（空负载返回哨兵值，保证头不全零）
#[inline]
fn payload_crc(payload: &[u8]) -> u32 {
  if payload.is_empty() {
    EMPTY_PAYLOAD_CRC
  } else {
    crc32(payload)
  }
}

/// const 上下文定长写入原语：把 src 拷入 out[off..off+N]
///
/// 序列化布局 = 各字段 LE 编码按 C# StructLayout 显式 FieldOffset 落位；
/// const fn 无法调用 copy_from_slice，各头序列化统一经此原语按偏移写入，
/// 消除散落的手写字节循环
#[inline]
const fn write_at<const N: usize>(out: &mut [u8], off: usize, src: [u8; N]) {
  let mut i = 0;
  while i < N {
    out[off + i] = src[i];
    i += 1;
  }
}

/// WAL 记录头元数据
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct RecordHeader {
  /// 记录负载的字节长度
  pub entry_len: u32,
  /// 记录负载的 CRC32 校验码
  pub crc32: u32,
}

impl RecordHeader {
  /// 创建新的记录头
  #[inline]
  pub const fn new(entry_len: u32, crc32: u32) -> Self {
    Self { entry_len, crc32 }
  }

  /// 检查记录头是否为全零（仅扇区末尾 padding 或残缺尾部；合法记录头绝不全零）
  #[inline]
  pub const fn is_zero(&self) -> bool {
    self.entry_len == 0 && self.crc32 == 0
  }

  /// 获取负载长度（usize 格式）
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:GetLength
  ///
  /// C# GetLength 按日志 checksum 类型取偏移（None=0 / PerEntry=8）后读 4B 记录长度，
  /// 供扫描迭代器推进；waof 记录头固定 8B（4B entry_len + 4B 内嵌 CRC32），无 checksum
  /// 变体偏移，entry_len 恒在偏移 0，与 C# 两种取偏移情形固定等价（帧总长 =
  /// payload_len() + RECORD_HEADER_LEN）。迭代器推进见 waof/src/iterator.rs
  #[inline]
  pub const fn payload_len(&self) -> usize {
    self.entry_len as usize
  }

  /// 为给定的负载数据计算 CRC32 并创建记录头
  #[inline]
  pub fn for_payload(payload: &[u8]) -> Self {
    Self {
      entry_len: payload.len() as u32,
      crc32: payload_crc(payload),
    }
  }

  /// 为分部件负载增量计算 CRC32 并创建记录头（scatter-write 入口）
  ///
  /// CRC32 线性可分段：分段累加与整包单遍结果逐位一致，调用方无须预拼
  /// 整包即可得到与 [`Self::for_payload`] 完全相同的记录头
  #[inline]
  pub fn for_payload_parts(parts: &[&[u8]]) -> Self {
    let total_len: usize = parts.iter().map(|part| part.len()).sum();
    if total_len == 0 {
      return Self::new(0, EMPTY_PAYLOAD_CRC);
    }
    let mut hasher = Crc32Hasher::new();
    for part in parts {
      hasher.update(part);
    }
    Self {
      entry_len: total_len as u32,
      crc32: hasher.finalize(),
    }
  }

  /// 校验负载数据长度与 CRC32 校验码
  #[inline]
  pub fn verify(&self, payload: &[u8]) -> Result<()> {
    if payload.len() != self.entry_len as usize {
      return Err(Error::InvalidRecordHeader);
    }
    let actual = payload_crc(payload);
    if actual != self.crc32 {
      return Err(Error::ChecksumMismatch {
        expected: self.crc32,
        actual,
      });
    }
    Ok(())
  }

  /// 将记录头转为 8 字节定长数组（单次 64 位位移与编码）
  #[inline]
  pub const fn to_bytes(&self) -> [u8; RECORD_HEADER_LEN] {
    let packed = (self.entry_len as u64) | ((self.crc32 as u64) << 32);
    packed.to_le_bytes()
  }

  /// 从 8 字节定长数组无失败解码记录头（内存路径专用，单次 64 位无分支解码）
  #[inline]
  pub const fn from_bytes(src: &[u8; RECORD_HEADER_LEN]) -> Self {
    let packed = u64::from_le_bytes(*src);
    Self {
      entry_len: packed as u32,
      crc32: (packed >> 32) as u32,
    }
  }

  /// 尝试从字节切片中快速解码记录头（const fn）
  #[inline(always)]
  pub const fn decode_opt(src: &[u8]) -> Option<Self> {
    if let Some((chunk, _)) = src.split_first_chunk::<RECORD_HEADER_LEN>() {
      Some(Self::from_bytes(chunk))
    } else {
      None
    }
  }

  /// 从切片中解码记录头（磁盘路径专用，切片长度不足 8 字节时报错）
  #[inline]
  pub fn decode(src: &[u8]) -> Result<Self> {
    Self::decode_opt(src).ok_or(Error::InvalidRecordHeader)
  }
}

/// libs/server/AOF/AofHeader.cs:AofHeaderType
///
/// 头类型判别值（对齐 C# AofHeaderType）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromRepr)]
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
  #[inline]
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

/// libs/server/AOF/AofHeader.cs:AofHeader
///
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
  #[inline]
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
  #[inline]
  pub const fn unsafe_truncate_log(&self) -> bool {
    (self.flags & Self::UNSAFE_TRUNCATE_LOG_FLAG) != 0
  }

  /// Setter for unsafe_truncate_log (AofHeader.cs UnsafeTruncateLog.set)
  #[inline]
  pub fn set_unsafe_truncate_log(&mut self, value: bool) {
    if value {
      self.flags |= Self::UNSAFE_TRUNCATE_LOG_FLAG;
    } else {
      self.flags &= !Self::UNSAFE_TRUNCATE_LOG_FLAG;
    }
  }

  /// libs/server/AOF/AofHeader.cs:HeaderType
  #[inline]
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
  #[inline]
  pub fn set_header_type(&mut self, value: AofHeaderType) {
    debug_assert!((value as u8) <= Self::AOF_HEADER_TYPE_MASK);
    self.flags = (self.flags & !Self::AOF_HEADER_TYPE_MASK) | value as u8;
  }

  /// libs/server/AOF/AofHeader.cs:IsChunked
  ///
  /// 本记录是否为更大分块逻辑记录的一片。
  #[inline]
  pub const fn is_chunked(&self) -> bool {
    (self.flags & Self::CHUNKED_RECORD_FLAG) != 0
  }

  /// 从条目起始字节解析头（16B LE 布局，字段偏移与 C# 逐字节一致）。
  #[inline]
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

  /// 序列化为 16B（LE 布局，字段偏移对标 C# AofHeader FieldOffset 0/1/2/3/4/12）。
  #[inline]
  pub const fn to_bytes(&self) -> [u8; Self::TOTAL_SIZE] {
    // procedureId/databaseId 在 C# 为 FieldOffset(3) union（最后写入者胜）；
    // 序列化侧以 procedure_id 非零优先、否则写 database_id 近似。两者回放等价：
    // 回放端（AofProcessor.cs:379/394）按 opType 判读该字节——存储过程条目
    // 只读 procedureId、FLUSH 条目只读 databaseId，两类条目 opType 互斥且
    // 两字段至多一个非零，故字节取值与 union 写入无差别
    let prefix = [
      self.aof_header_version,
      self.flags,
      self.op_type,
      if self.procedure_id != 0 {
        self.procedure_id
      } else {
        self.database_id
      },
    ];
    let mut out = [0u8; Self::TOTAL_SIZE];
    write_at(&mut out, 0, prefix);
    write_at(&mut out, 4, self.store_version.to_le_bytes());
    write_at(&mut out, 12, self.session_id.to_le_bytes());
    out
  }

  /// libs/server/AOF/AofHeader.cs:SkipHeader
  ///
  /// 返回条目载荷的起始偏移（按头类型跳过完整头）；未知类型返回 None
  ///（对齐 C# GarnetException 路径）。
  #[inline]
  pub const fn skip_header(entry: &[u8]) -> Option<usize> {
    let Some(header) = Self::parse(entry) else {
      return None;
    };
    match header.header_type() {
      Some(t) => Some(t.total_size()),
      None => None,
    }
  }

  /// 条目序列号提取单点（重放链各入口共用，对标 C#
  /// AofProcessor.cs:GetSynchronizedOperationParams / CanReplay / SkipReplay 与
  /// AofReplayCoordinator.cs:UpdateMaxSequenceNumberFromHeader 的统一取数语义：
  /// 分片形态（ShardedHeader / ShardedChunkHeader / ShardedLogTransactionHeader）
  /// 取内嵌 sequenceNumber，其余形态无内嵌序号、由调用方以条目地址兜底）。
  /// 头缺失 / 未知类型 / 分片段截断返回 None（对齐 C# GarnetException 路径）。
  #[inline]
  pub fn sequence_number_of(entry: &[u8], fallback: i64) -> Option<i64> {
    let header = Self::parse(entry)?;
    match header.header_type()? {
      AofHeaderType::ShardedHeader
      | AofHeaderType::ShardedChunkHeader
      | AofHeaderType::ShardedLogTransactionHeader => {
        // ShardedLogTransactionHeader 的 sequenceNumber 位于 sharded 段
        //（FieldOffset 16），解析前 24B 即可取得
        AofShardedHeader::parse(entry).map(|sh| sh.sequence_number)
      }
      _ => Some(fallback),
    }
  }

  /// libs/server/AOF/AofHeader.cs:GetChunkedHeaderRef
  ///
  /// 返回分块记录的内嵌 [`AofChunkHeader`] 在条目内的偏移；
  /// 非分块类型返回 None（对齐 C# GarnetException 路径）。
  #[inline]
  pub const fn get_chunked_header_ref(entry: &[u8]) -> Option<(usize, AofChunkHeader)> {
    let Some(header) = Self::parse(entry) else {
      return None;
    };
    let Some(ht) = header.header_type() else {
      return None;
    };
    let offset = match ht {
      AofHeaderType::BasicChunkHeader => Self::TOTAL_SIZE,
      AofHeaderType::ShardedChunkHeader => AofShardedHeader::TOTAL_SIZE,
      _ => return None,
    };
    if entry.len() < offset + AofChunkHeader::TOTAL_SIZE {
      return None;
    }
    let chunk_slice = entry.split_at(offset).1;
    let Some(chunk) = AofChunkHeader::parse(chunk_slice) else {
      return None;
    };
    Some((offset, chunk))
  }
}

impl Default for AofHeader {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

/// libs/server/AOF/AofHeader.cs:AofShardedHeader
///
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

  /// 序列化为 24B（LE 布局：basic 在前，sequenceNumber 对标 C# FieldOffset(16)）。
  #[inline]
  pub const fn to_bytes(&self) -> [u8; Self::TOTAL_SIZE] {
    let mut out = [0u8; Self::TOTAL_SIZE];
    write_at(&mut out, 0, self.basic.to_bytes());
    write_at(
      &mut out,
      AofHeader::TOTAL_SIZE,
      self.sequence_number.to_le_bytes(),
    );
    out
  }

  /// 解析。
  #[inline]
  pub const fn parse(entry: &[u8]) -> Option<Self> {
    let Some(chunk) = entry.first_chunk::<{ Self::TOTAL_SIZE }>() else {
      return None;
    };
    let Some(basic) = AofHeader::parse(chunk) else {
      return None;
    };
    let seq = i64::from_le_bytes([
      chunk[16], chunk[17], chunk[18], chunk[19], chunk[20], chunk[21], chunk[22], chunk[23],
    ]);
    Some(Self {
      basic,
      sequence_number: seq,
    })
  }
}

/// libs/server/AOF/AofHeader.cs:AofSingleLogTransactionHeader
///
/// 单物理日志事务头：BasicHeader + participantCount + 位图。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

  /// 序列化为 50B（LE 布局：basic 在前，participantCount 对标 C# FieldOffset(16)、
  /// 位图对标 FieldOffset(18)）。
  #[inline]
  pub const fn to_bytes(&self) -> [u8; Self::TOTAL_SIZE] {
    let mut out = [0u8; Self::TOTAL_SIZE];
    write_at(&mut out, 0, self.basic.to_bytes());
    write_at(
      &mut out,
      AofHeader::TOTAL_SIZE,
      self.participant_count.to_le_bytes(),
    );
    write_at(
      &mut out,
      AofHeader::TOTAL_SIZE + 2,
      self.replay_task_access_vector,
    );
    out
  }

  /// 解析。
  #[inline]
  pub const fn parse(entry: &[u8]) -> Option<Self> {
    let Some(chunk) = entry.first_chunk::<{ Self::TOTAL_SIZE }>() else {
      return None;
    };
    let Some(basic) = AofHeader::parse(chunk) else {
      return None;
    };
    let (_, tail) = chunk.split_at(AofHeader::TOTAL_SIZE);
    let Some((p_bytes, rest)) = tail.split_first_chunk::<2>() else {
      return None;
    };
    let Some((vector, _)) = rest.split_first_chunk::<REPLAY_TASK_ACCESS_VECTOR_BYTES>() else {
      return None;
    };
    Some(Self {
      basic,
      participant_count: i16::from_le_bytes(*p_bytes),
      replay_task_access_vector: *vector,
    })
  }
}

/// libs/server/AOF/AofHeader.cs:AofShardedLogTransactionHeader
///
/// 多物理日志事务头：ShardedHeader + participantCount + 位图。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

  /// 序列化为 58B（LE 布局：sharded 在前，participantCount 对标 C# FieldOffset(24)、
  /// 位图对标 FieldOffset(26)）。
  #[inline]
  pub const fn to_bytes(&self) -> [u8; Self::TOTAL_SIZE] {
    let mut out = [0u8; Self::TOTAL_SIZE];
    write_at(&mut out, 0, self.sharded.to_bytes());
    write_at(
      &mut out,
      AofShardedHeader::TOTAL_SIZE,
      self.participant_count.to_le_bytes(),
    );
    write_at(
      &mut out,
      AofShardedHeader::TOTAL_SIZE + 2,
      self.replay_task_access_vector,
    );
    out
  }

  /// 解析。
  #[inline]
  pub const fn parse(entry: &[u8]) -> Option<Self> {
    let Some(chunk) = entry.first_chunk::<{ Self::TOTAL_SIZE }>() else {
      return None;
    };
    let Some(sharded) = AofShardedHeader::parse(chunk) else {
      return None;
    };
    let (_, tail) = chunk.split_at(AofShardedHeader::TOTAL_SIZE);
    let Some((p_bytes, rest)) = tail.split_first_chunk::<2>() else {
      return None;
    };
    let Some((vector, _)) = rest.split_first_chunk::<REPLAY_TASK_ACCESS_VECTOR_BYTES>() else {
      return None;
    };
    Some(Self {
      sharded,
      participant_count: i16::from_le_bytes(*p_bytes),
      replay_task_access_vector: *vector,
    })
  }
}

/// libs/server/AOF/AofChunkHeader.cs:AofChunkHeader
///
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

  /// 序列化为 28B（LE 布局，字段偏移对标 C# AofChunkHeader FieldOffset 0/4/8/12/20）。
  #[inline]
  pub const fn to_bytes(&self) -> [u8; Self::TOTAL_SIZE] {
    let mut out = [0u8; Self::TOTAL_SIZE];
    write_at(&mut out, 0, self.overflow_key_length.to_le_bytes());
    write_at(&mut out, 4, self.overflow_value_length.to_le_bytes());
    write_at(&mut out, 8, self.input_length.to_le_bytes());
    write_at(&mut out, 12, self.object_id.to_le_bytes());
    write_at(&mut out, 20, self.key_hash.to_le_bytes());
    out
  }

  /// 解析。
  #[inline]
  pub const fn parse(entry: &[u8]) -> Option<Self> {
    let Some(chunk) = entry.first_chunk::<{ Self::TOTAL_SIZE }>() else {
      return None;
    };
    Some(Self {
      overflow_key_length: u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]),
      overflow_value_length: u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]),
      input_length: u32::from_le_bytes([chunk[8], chunk[9], chunk[10], chunk[11]]),
      object_id: u64::from_le_bytes([
        chunk[12], chunk[13], chunk[14], chunk[15], chunk[16], chunk[17], chunk[18], chunk[19],
      ]),
      key_hash: i64::from_le_bytes([
        chunk[20], chunk[21], chunk[22], chunk[23], chunk[24], chunk[25], chunk[26], chunk[27],
      ]),
    })
  }
}

#[cfg(test)]
mod tests {
  use super::{
    AofChunkHeader, AofHeader, AofHeaderType, AofShardedHeader, AofShardedLogTransactionHeader,
    AofSingleLogTransactionHeader, REPLAY_TASK_ACCESS_VECTOR_BYTES, RecordHeader,
  };

  #[test]
  fn test_record_header_roundtrip() {
    let header = RecordHeader::new(128, 0x1234_5678);
    let bytes = header.to_bytes();
    let decoded = RecordHeader::decode(&bytes).unwrap();
    assert_eq!(decoded, header);
    assert_eq!(decoded.payload_len(), 128);
    assert!(!decoded.is_zero());

    let zero = RecordHeader::new(0, 0);
    assert!(zero.is_zero());

    // 空负载头必须带非零 CRC 哨兵，决不能与全零 padding 混淆
    let empty_payload_header = RecordHeader::for_payload(&[]);
    assert_eq!(empty_payload_header.payload_len(), 0);
    assert!(
      !empty_payload_header.is_zero(),
      "空有效记录头必须携带非零哨兵 CRC"
    );
    assert_eq!(empty_payload_header.crc32, super::EMPTY_PAYLOAD_CRC);

    // 校验全零定长数组为 padding / 损坏
    let all_zeros = [0u8; super::RECORD_HEADER_LEN];
    let zero_decoded = RecordHeader::decode(&all_zeros).unwrap();
    assert!(zero_decoded.is_zero(), "全零头唯一标识 padding 或残缺尾部");
  }

  #[test]
  fn test_record_header_for_payload_and_verify() {
    let payload = b"hello aof payload";
    let header = RecordHeader::for_payload(payload);
    assert_eq!(header.payload_len(), payload.len());
    assert!(header.verify(payload).is_ok());

    let corrupted = b"hello aof payloae";
    assert!(header.verify(corrupted).is_err());
    assert!(header.verify(&payload[..payload.len() - 1]).is_err());
  }

  #[test]
  fn test_record_header_decode_boundary() {
    let short = [0u8; 7];
    assert!(RecordHeader::decode_opt(&short).is_none());
    assert!(RecordHeader::decode(&short).is_err());
  }

  #[test]
  fn test_aof_header_roundtrip_and_flags() {
    let mut h = AofHeader::new();
    h.set_header_type(AofHeaderType::BasicHeader);
    h.op_type = 0x01;
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

    let bytes2 = h.to_bytes();
    let parsed2 = AofHeader::parse(&bytes2).unwrap();
    assert_eq!(parsed2, h);
    assert!(parsed2.unsafe_truncate_log());
    assert!(parsed2.is_chunked());
  }

  #[test]
  fn test_aof_sharded_header_roundtrip() {
    let mut basic = AofHeader::new();
    basic.set_header_type(AofHeaderType::ShardedHeader);
    basic.store_version = 100;
    basic.session_id = 42;
    let sharded = AofShardedHeader {
      basic,
      sequence_number: 999_888_777,
    };
    let bytes = sharded.to_bytes();
    assert_eq!(bytes.len(), AofShardedHeader::TOTAL_SIZE);
    let parsed = AofShardedHeader::parse(&bytes).unwrap();
    assert_eq!(parsed, sharded);
    assert_eq!(parsed.sequence_number, 999_888_777);
  }

  #[test]
  fn test_aof_transaction_headers_roundtrip() {
    let mut basic = AofHeader::new();
    basic.set_header_type(AofHeaderType::SingleLogTransactionHeader);
    let mut vector = [0u8; REPLAY_TASK_ACCESS_VECTOR_BYTES];
    vector[0] = 0xAA;
    vector[31] = 0x55;

    let single_txn = AofSingleLogTransactionHeader {
      basic,
      participant_count: 8,
      replay_task_access_vector: vector,
    };
    let bytes_single = single_txn.to_bytes();
    assert_eq!(
      bytes_single.len(),
      AofSingleLogTransactionHeader::TOTAL_SIZE
    );
    let parsed_single = AofSingleLogTransactionHeader::parse(&bytes_single).unwrap();
    assert_eq!(parsed_single, single_txn);
    assert_eq!(parsed_single.participant_count, 8);
    assert_eq!(parsed_single.replay_task_access_vector[0], 0xAA);
    assert_eq!(parsed_single.replay_task_access_vector[31], 0x55);

    let mut sharded_basic = AofHeader::new();
    sharded_basic.set_header_type(AofHeaderType::ShardedLogTransactionHeader);
    let sharded = AofShardedHeader {
      basic: sharded_basic,
      sequence_number: 123456,
    };
    let sharded_txn = AofShardedLogTransactionHeader {
      sharded,
      participant_count: 16,
      replay_task_access_vector: vector,
    };
    let bytes_sharded = sharded_txn.to_bytes();
    assert_eq!(
      bytes_sharded.len(),
      AofShardedLogTransactionHeader::TOTAL_SIZE
    );
    let parsed_sharded = AofShardedLogTransactionHeader::parse(&bytes_sharded).unwrap();
    assert_eq!(parsed_sharded, sharded_txn);
  }

  #[test]
  fn test_aof_chunk_header_roundtrip() {
    let chunk = AofChunkHeader {
      overflow_key_length: 12,
      overflow_value_length: 4096,
      input_length: 64,
      object_id: 12345678901234,
      key_hash: -987654321,
    };
    let bytes = chunk.to_bytes();
    assert_eq!(bytes.len(), AofChunkHeader::TOTAL_SIZE);
    let parsed = AofChunkHeader::parse(&bytes).unwrap();
    assert_eq!(parsed, chunk);
  }

  /// 磁盘字节布局锚点：roundtrip 只能发现 parse/to_bytes 对称性错位，
  /// 此处按 C# FieldOffset 逐字段断言绝对偏移，锁死序列化格式
  #[test]
  fn test_header_disk_layout_anchors() {
    // AofHeader：偏移 0=version、1=flags、2=opType、3=procedureId/databaseId union、
    // 4=storeVersion、12=sessionID
    let mut h = AofHeader::new();
    h.op_type = 0x07;
    h.procedure_id = 0x09;
    h.store_version = 0x0102_0304_0506_0708;
    h.session_id = 0x0a0b_0c0d;
    let b = h.to_bytes();
    assert_eq!(b[0], AofHeader::AOF_HEADER_VERSION);
    assert_eq!(b[1], 0);
    assert_eq!(b[2], 0x07);
    assert_eq!(b[3], 0x09);
    assert_eq!(b[4..12], 0x0102_0304_0506_0708u64.to_le_bytes());
    assert_eq!(b[12..16], 0x0a0b_0c0du32.to_le_bytes());

    // procedure_id 为 0 时 union 字节写 database_id
    h.procedure_id = 0;
    h.database_id = 0x0e;
    assert_eq!(h.to_bytes()[3], 0x0e);

    // AofShardedHeader：sequenceNumber @16
    let sh = AofShardedHeader {
      basic: h,
      sequence_number: -2,
    };
    let sb = sh.to_bytes();
    assert_eq!(&sb[..16], &h.to_bytes()[..]);
    assert_eq!(sb[16..24], (-2i64).to_le_bytes());

    // AofSingleLogTransactionHeader：participantCount @16、位图 @18
    let mut vector = [0u8; REPLAY_TASK_ACCESS_VECTOR_BYTES];
    vector[0] = 0xAA;
    vector[31] = 0x55;
    let st = AofSingleLogTransactionHeader {
      basic: h,
      participant_count: -3,
      replay_task_access_vector: vector,
    };
    let stb = st.to_bytes();
    assert_eq!(&stb[..16], &h.to_bytes()[..]);
    assert_eq!(stb[16..18], (-3i16).to_le_bytes());
    assert_eq!(stb[18..50], vector);

    // AofShardedLogTransactionHeader：participantCount @24、位图 @26
    let sht = AofShardedLogTransactionHeader {
      sharded: sh,
      participant_count: 7,
      replay_task_access_vector: vector,
    };
    let shtb = sht.to_bytes();
    assert_eq!(&shtb[..24], &sh.to_bytes()[..]);
    assert_eq!(shtb[24..26], 7i16.to_le_bytes());
    assert_eq!(shtb[26..58], vector);

    // AofChunkHeader：长度三元组 @0/4/8、objectId @12、keyHash @20
    let ch = AofChunkHeader {
      overflow_key_length: 1,
      overflow_value_length: 2,
      input_length: 3,
      object_id: 4,
      key_hash: -5,
    };
    let cb = ch.to_bytes();
    assert_eq!(cb[..4], 1u32.to_le_bytes());
    assert_eq!(cb[4..8], 2u32.to_le_bytes());
    assert_eq!(cb[8..12], 3u32.to_le_bytes());
    assert_eq!(cb[12..20], 4u64.to_le_bytes());
    assert_eq!(cb[20..28], (-5i64).to_le_bytes());
  }

  #[test]
  fn test_skip_header_offsets() {
    for (t, size) in [
      (AofHeaderType::BasicHeader, 16),
      (AofHeaderType::ShardedHeader, 24),
      (AofHeaderType::SingleLogTransactionHeader, 50),
      (AofHeaderType::ShardedLogTransactionHeader, 58),
      (AofHeaderType::BasicChunkHeader, 44),
      (AofHeaderType::ShardedChunkHeader, 52),
    ] {
      assert_eq!(t.total_size(), size);
      let mut h = AofHeader::new();
      h.set_header_type(t);
      assert_eq!(AofHeader::skip_header(&h.to_bytes()), Some(size));
    }
  }

  #[test]
  fn test_chunk_header_ref() {
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
    entry.extend_from_slice(&chunk.to_bytes());

    let (offset, parsed) = AofHeader::get_chunked_header_ref(&entry).unwrap();
    assert_eq!(offset, 16);
    assert_eq!(parsed, chunk);

    // 非分块类型返回 None。
    let mut plain = AofHeader::new();
    plain.set_header_type(AofHeaderType::BasicHeader);
    assert!(AofHeader::get_chunked_header_ref(&plain.to_bytes()).is_none());
  }

  #[test]
  fn test_header_parse_truncated_boundaries() {
    let short_bytes = [0u8; 15];
    assert!(AofHeader::parse(&short_bytes).is_none());
    assert!(AofShardedHeader::parse(&[0u8; 23]).is_none());
    assert!(AofSingleLogTransactionHeader::parse(&[0u8; 49]).is_none());
    assert!(AofShardedLogTransactionHeader::parse(&[0u8; 57]).is_none());
    assert!(AofChunkHeader::parse(&[0u8; 27]).is_none());
    assert!(AofHeader::skip_header(&short_bytes).is_none());
  }
}
