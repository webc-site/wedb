//! r320 迁出：sync_metadata 内联 tests 块（src/server/replication/sync_metadata.rs:79-133）
//! 主备同步协商元数据逐字段 bitcode 往返锁（deviations 锁测，原样保留）

use waof::AofAddress;
use wedb::server::{
  replication::{
    checkpoint_entry::{CheckpointEntry, CheckpointMetadata},
    sync_metadata::SyncMetadata,
  },
  worker::NodeRole,
};

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
    origin_node_id: 0x0000_0000_0000_0000_0000_0000_0000_0E12,
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
