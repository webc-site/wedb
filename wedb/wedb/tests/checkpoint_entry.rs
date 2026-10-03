use waof::AofAddress;
use wedb::server::replication::{
  assembly::{aof_span, aof_span_array},
  checkpoint_entry::{CheckpointEntry, CheckpointFileType, CheckpointMetadata},
};

#[test]
fn test_readers_suspend_flow() {
  let entry = CheckpointEntry::with_sublogs(1);
  assert!(entry.try_add_reader());
  assert_eq!(entry.reader_count(), 1);
  assert!(!entry.try_suspend_readers());

  entry.remove_reader();
  assert_eq!(entry.reader_count(), 0);
  assert!(entry.try_suspend_readers());
  assert!(entry.is_suspended());

  // 挂起后禁止新增读者
  assert!(!entry.try_add_reader());
}

#[test]
fn test_checkpoint_entry_serialization() {
  let mut meta = CheckpointMetadata::new(2);
  meta.store_version = 42;
  meta.store_hlog_token = 0x1234_5678_9abc_def0_1122_3344_5566_7788;
  meta.store_index_token = 0x8877_6655_4433_2211_0fed_cba9_8765_4321;
  meta.store_primary_repl_id = Some("abc123replid".to_string());
  meta.store_checkpoint_covered_aof_address = AofAddress::create(2, 1024);

  let entry = CheckpointEntry::new(meta);
  let bytes = entry.to_byte_array();
  let decoded = CheckpointEntry::from_byte_array(&bytes).expect("entry must decode");
  assert_eq!(entry.metadata, decoded.metadata);
}

/// span 序列化与主端 AofAddress::from_span 的往返（C# beginAddress.Span / FromSpan 契约）
#[test]
fn test_aof_span_roundtrip() {
  assert_eq!(aof_span(0), 0i64.to_le_bytes().to_vec());
  assert_eq!(aof_span_array(0), 0i64.to_le_bytes());
  assert_eq!(aof_span(4096), 4096i64.to_le_bytes().to_vec());
  assert_eq!(aof_span_array(4096), 4096i64.to_le_bytes());
  let span = aof_span(-64);
  assert_eq!(AofAddress::from_span(&span).get(0), Some(-64));
  // 单槽位点 span 长度恒 8B（from_span length = 8 >> 3 = 1）
  assert_eq!(AofAddress::from_span(&span).length(), 1);
}

/// 空检查点条目序列化可被主端 FromByteArray 还原（C# 空库上报语义）
#[test]
fn test_empty_checkpoint_entry_roundtrip() {
  let bytes = CheckpointEntry::with_sublogs(1).to_byte_array();
  let decoded = CheckpointEntry::from_byte_array(&bytes).expect("空条目必须可解码");
  assert_eq!(decoded.metadata.store_version, -1);
  assert_eq!(decoded.metadata.store_hlog_token, 0);
  assert!(decoded.metadata.store_primary_repl_id.is_none());
}

#[test]
fn test_checkpoint_file_type_protocol_mapping() {
  assert_eq!(
    CheckpointFileType::from_protocol(0),
    Some(CheckpointFileType::None)
  );
  assert_eq!(
    CheckpointFileType::from_protocol(1),
    Some(CheckpointFileType::StoreHlog)
  );
  assert_eq!(
    CheckpointFileType::from_protocol(2),
    Some(CheckpointFileType::StoreHlogObj)
  );
  assert_eq!(CheckpointFileType::from_protocol(3), None);
  assert_eq!(
    CheckpointFileType::from_protocol(4),
    Some(CheckpointFileType::StoreIndex)
  );
  assert_eq!(
    CheckpointFileType::from_protocol(5),
    Some(CheckpointFileType::StoreSnapshot)
  );
  assert_eq!(
    CheckpointFileType::from_protocol(6),
    Some(CheckpointFileType::StoreSnapshotObj)
  );
  assert_eq!(
    CheckpointFileType::from_protocol(7),
    Some(CheckpointFileType::StoreRangeindexFlush)
  );
  assert_eq!(
    CheckpointFileType::from_protocol(8),
    Some(CheckpointFileType::StoreRangeindexSnapshot)
  );
  assert_eq!(CheckpointFileType::from_protocol(9), None);
  assert_eq!(CheckpointFileType::from_protocol(-1), None);

  assert_eq!(CheckpointFileType::None.as_str(), "NONE");
  assert_eq!(CheckpointFileType::StoreHlog.as_str(), "STORE_HLOG");
  assert_eq!(CheckpointFileType::StoreIndex.as_str(), "STORE_INDEX");
}

#[test]
fn test_contains_shared_token() {
  let mut meta1 = CheckpointMetadata::new(1);
  meta1.store_hlog_token = 100;
  meta1.store_index_token = 200;
  let entry1 = CheckpointEntry::new(meta1);

  let mut meta2 = CheckpointMetadata::new(1);
  meta2.store_hlog_token = 100;
  meta2.store_index_token = 999;
  let entry2 = CheckpointEntry::new(meta2);

  assert!(entry1.contains_shared_token(&entry2, CheckpointFileType::StoreHlog));
  assert!(!entry1.contains_shared_token(&entry2, CheckpointFileType::StoreIndex));
  assert!(entry1.contains_shared_token(&entry2, CheckpointFileType::StoreSnapshot));
}
