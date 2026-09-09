//! AOF 条目类型与负载形状判定
//! （对标 libs/server/AOF/AofEntryType.cs:AofEntryType / AofStoreType /
//! AofEntryTypeExtensions）。

/// AOF 条目类型（判别值与 C# 逐项一致）。
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, num_enum::TryFromPrimitive, num_enum::IntoPrimitive,
)]
#[repr(u8)]
pub enum AofEntryType {
  /// 存储型 upsert。
  StoreUpsert = 0x00,
  /// 存储型 RMW。
  StoreRMW = 0x01,
  /// 存储型 delete。
  StoreDelete = 0x02,
  /// 对象存储 upsert。
  ObjectStoreUpsert = 0x10,
  /// 对象存储 RMW。
  ObjectStoreRMW = 0x11,
  /// 对象存储 delete。
  ObjectStoreDelete = 0x12,
  /// 事务开始。
  TxnStart = 0x20,
  /// 事务提交。
  TxnCommit = 0x21,
  /// 事务中止。
  TxnAbort = 0x22,
  /// 统一 checkpoint 起始标记。
  CheckpointStartCommit = 0x30,
  /// 统一 checkpoint 结束标记。
  CheckpointEndCommit = 0x32,
  /// 主存储流式 checkpoint 起始标记。
  MainStoreStreamingCheckpointStartCommit = 0x40,
  /// 对象存储流式 checkpoint 起始标记。
  ObjectStoreStreamingCheckpointStartCommit = 0x41,
  /// 主存储流式 checkpoint 结束标记。
  MainStoreStreamingCheckpointEndCommit = 0x42,
  /// 对象存储流式 checkpoint 结束标记。
  ObjectStoreStreamingCheckpointEndCommit = 0x43,
  /// 存储过程。
  StoredProcedure = 0x50,
  /// FLUSH ALL。
  FlushAll = 0x60,
  /// FLUSH DB。
  FlushDb = 0x61,
  /// 统一存储 upsert 字符串。
  UnifiedStoreStringUpsert = 0x70,
  /// 统一存储 upsert 对象。
  UnifiedStoreObjectUpsert = 0x71,
  /// 统一存储 RMW。
  UnifiedStoreRMW = 0x72,
  /// 统一存储 delete。
  UnifiedStoreDelete = 0x73,
  /// 迁移 Range Index 序列化文件的单个分块——AOF 内部传输条目，
  /// 经 RangeIndexManager.HandleRangeIndexStreamReplay 回放。
  RangeIndexStreamChunk = 0x80,
}

/// 条目归属的存储类别（对齐 C# AofStoreType）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AofStoreType {
  /// 主存储。
  MainStoreType = 0x0,
  /// 对象存储。
  ObjectStoreType = 0x1,
  /// 事务。
  TxnType = 0x2,
  /// 复制。
  ReplicationType = 0x3,
  /// Checkpoint。
  CheckpointType = 0x4,
  /// FlushDb。
  FlushDbType = 0x5,
}

impl AofEntryType {
  /// libs/server/AOF/AofEntryType.cs:HasKey
  ///
  /// 条目在头之后是否携带 key 负载；无键条目（事务、checkpoint、flush、
  /// 存储过程）无 key。
  pub fn has_key(self) -> bool {
    matches!(
      self,
      Self::StoreUpsert
        | Self::StoreRMW
        | Self::StoreDelete
        | Self::ObjectStoreUpsert
        | Self::ObjectStoreRMW
        | Self::ObjectStoreDelete
        | Self::UnifiedStoreStringUpsert
        | Self::UnifiedStoreObjectUpsert
        | Self::UnifiedStoreRMW
        | Self::UnifiedStoreDelete
        | Self::RangeIndexStreamChunk
    )
  }

  /// libs/server/AOF/AofEntryType.cs:HasChunkValue
  ///
  /// 回放记录在 key 之后是否携带长度前缀的 value 分块（Upsert 形状）。
  /// 分块写入器与读取器依赖此判定保持一致。
  pub fn has_chunk_value(self) -> bool {
    matches!(
      self,
      Self::StoreUpsert
        | Self::ObjectStoreUpsert
        | Self::UnifiedStoreStringUpsert
        | Self::UnifiedStoreObjectUpsert
    )
  }

  /// libs/server/AOF/AofEntryType.cs:HasChunkInput
  ///
  /// 回放记录在 key/value 之后是否携带原始（非长度前缀）input 尾部
  /// （Upsert-with-input 与 RMW 形状）；对象 upsert 与 delete 无 input。
  pub fn has_chunk_input(self) -> bool {
    matches!(
      self,
      Self::StoreUpsert
        | Self::StoreRMW
        | Self::ObjectStoreRMW
        | Self::UnifiedStoreStringUpsert
        | Self::UnifiedStoreRMW
    )
  }

  /// libs/server/AOF/AofEntryType.cs:HasChunkObjectValue
  ///
  /// 分块 value 是否为流式对象值（长度未知，读取器需累积而非按头的
  /// overflowValueLength 预分配；字符串 upsert 的值是预定长的）。
  pub fn has_chunk_object_value(self) -> bool {
    matches!(
      self,
      Self::ObjectStoreUpsert | Self::UnifiedStoreObjectUpsert
    )
  }

  /// 条目归属的存储类别（C# 内部约定：低 4 位 0x0x = 主存储，0x1x = 对象存储）。
  pub fn store_type(self) -> AofStoreType {
    match self {
      Self::TxnStart | Self::TxnCommit | Self::TxnAbort => AofStoreType::TxnType,
      Self::CheckpointStartCommit
      | Self::CheckpointEndCommit
      | Self::MainStoreStreamingCheckpointStartCommit
      | Self::ObjectStoreStreamingCheckpointStartCommit
      | Self::MainStoreStreamingCheckpointEndCommit
      | Self::ObjectStoreStreamingCheckpointEndCommit => AofStoreType::CheckpointType,
      Self::FlushAll | Self::FlushDb => AofStoreType::FlushDbType,
      Self::ObjectStoreUpsert | Self::ObjectStoreRMW | Self::ObjectStoreDelete => {
        AofStoreType::ObjectStoreType
      }
      _ => {
        if matches!(self, Self::UnifiedStoreObjectUpsert) {
          AofStoreType::ObjectStoreType
        } else {
          AofStoreType::MainStoreType
        }
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::{AofEntryType, AofStoreType};

  #[test]
  fn payload_shapes() {
    assert!(AofEntryType::StoreUpsert.has_key());
    assert!(AofEntryType::StoreUpsert.has_chunk_value());
    assert!(AofEntryType::StoreUpsert.has_chunk_input());
    assert!(!AofEntryType::StoreUpsert.has_chunk_object_value());

    assert!(AofEntryType::ObjectStoreUpsert.has_chunk_object_value());
    assert!(!AofEntryType::ObjectStoreUpsert.has_chunk_input());

    assert!(AofEntryType::StoreRMW.has_chunk_input());
    assert!(!AofEntryType::StoreRMW.has_chunk_value());

    assert!(!AofEntryType::TxnStart.has_key());
    assert!(!AofEntryType::FlushAll.has_key());
    assert!(!AofEntryType::RangeIndexStreamChunk.has_chunk_value());
    assert!(AofEntryType::RangeIndexStreamChunk.has_key());
  }

  #[test]
  fn discriminants_roundtrip() {
    assert_eq!(
      AofEntryType::try_from(0x00u8),
      Ok(AofEntryType::StoreUpsert)
    );
    assert_eq!(
      AofEntryType::try_from(0x80u8),
      Ok(AofEntryType::RangeIndexStreamChunk)
    );
    assert_eq!(
      AofEntryType::StoreUpsert.store_type(),
      AofStoreType::MainStoreType
    );
    assert_eq!(
      AofEntryType::ObjectStoreDelete.store_type(),
      AofStoreType::ObjectStoreType
    );
    assert_eq!(AofEntryType::TxnCommit.store_type(), AofStoreType::TxnType);
  }
}
