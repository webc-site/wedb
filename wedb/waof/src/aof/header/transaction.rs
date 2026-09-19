//! 分片头与事务头族（对标 libs/server/AOF/AofHeader.cs 的 AofShardedHeader /
//! AofSingleLogTransactionHeader / AofShardedLogTransactionHeader）

use wbase::store_type::REPLAY_TASK_ACCESS_VECTOR_BYTES;

use super::{AofHeader, write_at};

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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::aof::header::AofHeaderType;

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
}
