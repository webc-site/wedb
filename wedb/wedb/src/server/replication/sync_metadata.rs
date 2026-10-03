use std::io;

use waof::AofAddress;

use crate::server::{
  replication::checkpoint_entry::{CheckpointEntry, CheckpointMetadata},
  worker::NodeRole,
};

#[derive(Debug, bitcode::Encode, bitcode::Decode)]
struct SyncMetadataWire {
  full_sync: bool,
  origin_node_role: NodeRole,
  origin_node_id: u128,
  current_primary_repl_id: String,
  current_store_version: i64,
  current_aof_begin_address: AofAddress,
  current_aof_tail_address: AofAddress,
  checkpoint_metadata: Option<CheckpointMetadata>,
}

/// libs/cluster/Server/Replication/SyncMetadata.cs:SyncMetadata（字段公开直构，
/// 对标 C# 逐字段构造；线格式刻意 bitcode 化且裁去 C# 线上的
/// currentReplicationOffset 字段——rust-internal 复制协议无 C# 双端互操作面，
/// 登记 deviations.md §203）
#[derive(Debug, Clone)]
pub struct SyncMetadata {
  pub full_sync: bool,
  pub origin_node_role: NodeRole,
  pub origin_node_id: u128,
  pub current_primary_repl_id: String,
  pub current_store_version: i64,
  pub current_aof_begin_address: AofAddress,
  pub current_aof_tail_address: AofAddress,
  pub checkpoint_entry: Option<CheckpointEntry>,
}

impl SyncMetadata {
  /// libs/cluster/Server/Replication/SyncMetadata.cs:ToByteArray
  ///
  /// 序列化为二进制（基于 bitcode 紧凑编码）
  pub fn to_byte_array(&self) -> Vec<u8> {
    let wire = SyncMetadataWire {
      full_sync: self.full_sync,
      origin_node_role: self.origin_node_role,
      origin_node_id: self.origin_node_id,
      current_primary_repl_id: self.current_primary_repl_id.clone(),
      current_store_version: self.current_store_version,
      current_aof_begin_address: self.current_aof_begin_address,
      current_aof_tail_address: self.current_aof_tail_address,
      checkpoint_metadata: self.checkpoint_entry.as_ref().map(|e| e.metadata.clone()),
    };
    bitcode::encode(&wire)
  }

  /// libs/cluster/Server/Replication/SyncMetadata.cs:FromByteArray
  ///
  /// 从二进制字节反序列化
  pub fn from_byte_array(serialized: &[u8]) -> io::Result<Self> {
    let wire: SyncMetadataWire =
      bitcode::decode(serialized).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(Self {
      full_sync: wire.full_sync,
      origin_node_role: wire.origin_node_role,
      origin_node_id: wire.origin_node_id,
      current_primary_repl_id: wire.current_primary_repl_id,
      current_store_version: wire.current_store_version,
      current_aof_begin_address: wire.current_aof_begin_address,
      current_aof_tail_address: wire.current_aof_tail_address,
      checkpoint_entry: wire.checkpoint_metadata.map(CheckpointEntry::new),
    })
  }
}
