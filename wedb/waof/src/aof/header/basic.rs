//! 基础 AOF 头与头类型判别值（对标 libs/server/AOF/AofHeader.cs 的
//! AofHeaderType / AofHeader）
//!
//! 自研依据: AOF 基础头

use strum::FromRepr;

use super::{
  AofChunkHeader, AofShardedHeader, AofShardedLogTransactionHeader, AofSingleLogTransactionHeader,
  write_at,
};

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
  /// AOF 版本（本仓自持域，取值见 [`AofHeader::AOF_FORMAT_VERSION`]）。
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
  /// 本仓自持的 AOF 格式版本（对应 C# `AofHeader.AofHeaderVersion` 的字节槽位）。
  ///
  /// 与 C# 的 1..=5 版本域刻意不同号：高位置 1 永不重叠，因为本仓 AOF 载荷与
  /// C# garnet 实质异构（物理键 `[NsVarint][DbVarint][KeyTag][用户键]` 前缀、
  /// 32B 显式分列重放输入头），同号会造成跨仓文件通过版本门后被静默误读。
  /// 读侧版本门按等值判定（见 wnode `process_aof_record_internal`），跨仓文件
  /// 与本仓旧代际文件一律显式拒绝，不做向下兼容。
  pub const AOF_FORMAT_VERSION: u8 = 0x80;
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
      aof_header_version: Self::AOF_FORMAT_VERSION,
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

#[cfg(test)]
mod tests {
  use super::{AofHeader, AofHeaderType};

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
}
