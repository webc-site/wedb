use std::io;

use waof::AofAddress;

use crate::server::{
  replication::checkpoint_entry::{CheckpointEntry, CheckpointMetadata},
  worker::NodeRole,
};

#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
struct SyncMetadataWire {
  full_sync: bool,
  origin_node_role: NodeRole,
  origin_node_id: String,
  current_primary_repl_id: String,
  current_store_version: i64,
  current_aof_begin_address: AofAddress,
  current_aof_tail_address: AofAddress,
  current_replication_offset: AofAddress,
  checkpoint_metadata: Option<CheckpointMetadata>,
}

/// libs/cluster/Server/Replication/SyncMetadata.cs:SyncMetadata
///
/// 节点间主备同步协商元数据（字段公开直构，对标 C# 逐字段构造）
#[derive(Debug, Clone)]
pub struct SyncMetadata {
  pub full_sync: bool,
  pub origin_node_role: NodeRole,
  pub origin_node_id: String,
  pub current_primary_repl_id: String,
  pub current_store_version: i64,
  pub current_aof_begin_address: AofAddress,
  pub current_aof_tail_address: AofAddress,
  pub current_replication_offset: AofAddress,
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
      origin_node_id: self.origin_node_id.clone(),
      current_primary_repl_id: self.current_primary_repl_id.clone(),
      current_store_version: self.current_store_version,
      current_aof_begin_address: self.current_aof_begin_address,
      current_aof_tail_address: self.current_aof_tail_address,
      current_replication_offset: self.current_replication_offset,
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
      current_replication_offset: wire.current_replication_offset,
      checkpoint_entry: wire.checkpoint_metadata.map(CheckpointEntry::new),
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::server::replication::checkpoint_entry::CheckpointMetadata;

  #[test]
  fn test_sync_metadata_roundtrip() {
    let mut meta = CheckpointMetadata::new(1);
    meta.store_version = 10;
    meta.store_hlog_token = 0xabcdef;
    meta.store_index_token = 0x123456;
    meta.store_checkpoint_covered_aof_address = AofAddress::create(1, 50);
    let entry = CheckpointEntry::new(meta);

    let sync = SyncMetadata {
      full_sync: true,
      origin_node_role: NodeRole::Primary,
      origin_node_id: "node-origin-1".to_string(),
      current_primary_repl_id: "primary-repl-id-1".to_string(),
      current_store_version: 10,
      current_aof_begin_address: AofAddress::create(1, 0),
      current_aof_tail_address: AofAddress::create(1, 2048),
      current_replication_offset: AofAddress::create(1, 1024),
      checkpoint_entry: Some(entry),
    };

    let bytes = sync.to_byte_array();
    let decoded = SyncMetadata::from_byte_array(&bytes).expect("decode failed");

    assert_eq!(sync.full_sync, decoded.full_sync);
    assert_eq!(sync.origin_node_role, decoded.origin_node_role);
    assert_eq!(sync.origin_node_id, decoded.origin_node_id);
    assert_eq!(
      sync.current_primary_repl_id,
      decoded.current_primary_repl_id
    );
    assert_eq!(sync.current_store_version, decoded.current_store_version);
    assert_eq!(
      sync.current_aof_begin_address,
      decoded.current_aof_begin_address
    );
    assert_eq!(
      sync.current_aof_tail_address,
      decoded.current_aof_tail_address
    );
    assert_eq!(
      sync.current_replication_offset,
      decoded.current_replication_offset
    );
    assert_eq!(
      sync.checkpoint_entry.as_ref().unwrap().metadata,
      decoded.checkpoint_entry.as_ref().unwrap().metadata
    );
  }
}
