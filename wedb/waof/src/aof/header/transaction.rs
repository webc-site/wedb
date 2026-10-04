//! 分片头与事务头族（对标 libs/server/AOF/AofHeader.cs 的 AofShardedHeader /
//! AofSingleLogTransactionHeader / AofShardedLogTransactionHeader）
//!
//! 自研依据: AOF 事务头（C# 对应 AofHeader Txn 面）

use wbase::store_type::REPLAY_TASK_ACCESS_VECTOR_BYTES;

use super::{AofHeader, write_at};

/// 事务头公共尾部写入单点：`participantCount(i16 LE)` 落在前缀头之后偏移
/// `prefix_size`、位图紧随其后偏移 `prefix_size + 2`（对标 C#
/// FieldOffset(prefix_size)/FieldOffset(prefix_size+2)）。单/分片两类事务头
/// 仅此尾部逐字段一致，序列化经此收敛为单点。
#[inline]
const fn write_txn_tail(
  out: &mut [u8],
  prefix_size: usize,
  participant_count: i16,
  vector: [u8; REPLAY_TASK_ACCESS_VECTOR_BYTES],
) {
  write_at(out, prefix_size, participant_count.to_le_bytes());
  write_at(out, prefix_size + 2, vector);
}

/// 事务头公共尾部解析单点：从定长 `chunk` 的前缀头之后切出
/// `(participantCount, 位图)`；长度不足即 `None`。与 [`write_txn_tail`] 同布局。
/// const 面用 let-else（const fn 内 `?` 已不可用，本文件 [`AofShardedHeader::parse`]
/// 同款）；语义同 C# AofHeader.cs Parse 系长度不足返回 null。
#[inline]
const fn parse_txn_tail(
  chunk: &[u8],
  prefix_size: usize,
) -> Option<(i16, [u8; REPLAY_TASK_ACCESS_VECTOR_BYTES])> {
  let tail = chunk.split_at(prefix_size).1;
  let Some((p_bytes, rest)) = tail.split_first_chunk::<2>() else {
    return None;
  };
  let Some((vector, _)) = rest.split_first_chunk::<REPLAY_TASK_ACCESS_VECTOR_BYTES>() else {
    return None;
  };
  Some((i16::from_le_bytes(*p_bytes), *vector))
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
    let b = self.basic.to_bytes();
    let s = self.sequence_number.to_le_bytes();
    [
      b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13],
      b[14], b[15], s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
    ]
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
    write_txn_tail(
      &mut out,
      AofHeader::TOTAL_SIZE,
      self.participant_count,
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
    let Some((participant_count, replay_task_access_vector)) =
      parse_txn_tail(chunk, AofHeader::TOTAL_SIZE)
    else {
      return None;
    };
    Some(Self {
      basic,
      participant_count,
      replay_task_access_vector,
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
    write_txn_tail(
      &mut out,
      AofShardedHeader::TOTAL_SIZE,
      self.participant_count,
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
    let Some((participant_count, replay_task_access_vector)) =
      parse_txn_tail(chunk, AofShardedHeader::TOTAL_SIZE)
    else {
      return None;
    };
    Some(Self {
      sharded,
      participant_count,
      replay_task_access_vector,
    })
  }
}
