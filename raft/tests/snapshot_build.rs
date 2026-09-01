use std::fs::read_to_string;
use std::sync::Arc;

use tempfile::tempdir;
use wedb_raft::engine::FjallEngine;
use wedb_raft::store::key::SM_DATA_FAMILY;
use wedb_raft::store::snapshot::build_snapshot;
use wedb_raft::store::snapshot::util::{snapshot_data_file, snapshot_id_dir};
use wedb_raft::types::{LeaderId, LogId, StoredMembership};
use zenoh_raft::Membership;

fn create_test_engine_with_data() -> (Arc<FjallEngine>, tempfile::TempDir) {
  let temp_dir = tempdir().unwrap();
  let path = temp_dir.path().join("data");
  let engine = Arc::new(FjallEngine::new(path, vec![SM_DATA_FAMILY.to_string()]).unwrap());
  let cf = engine.keyspace(SM_DATA_FAMILY).unwrap();
  cf.insert(b"key1", b"value1").unwrap();
  cf.insert(b"key2", b"value2").unwrap();
  cf.insert(b"key3", b"value3").unwrap();
  (engine, temp_dir)
}

fn create_test_engine_empty() -> (Arc<FjallEngine>, tempfile::TempDir) {
  let temp_dir = tempdir().unwrap();
  let path = temp_dir.path().join("data");
  let engine = Arc::new(FjallEngine::new(path, vec![SM_DATA_FAMILY.to_string()]).unwrap());
  (engine, temp_dir)
}

#[compio::test]
async fn test_build_snapshot_with_data() {
  let (engine, _temp_db) = create_test_engine_with_data();
  let temp_dir = tempdir().unwrap();
  let snapshot_dir = temp_dir.path().join("snapshots");

  let last_applied_log_id = Some(LogId {
    leader_id: LeaderId {
      term: 2,
      node_id: 1,
    },
    index: 10,
  });

  let last_membership = StoredMembership::new(
    Some(LogId {
      leader_id: LeaderId {
        term: 2,
        node_id: 1,
      },
      index: 5,
    }),
    Membership::default(),
  );

  let result = build_snapshot(&engine, &snapshot_dir, last_applied_log_id, last_membership).await;

  assert!(result.is_ok());

  let snapshot = result.unwrap();
  assert_eq!(snapshot.meta.last_log_id, last_applied_log_id);

  let last_id = read_to_string(snapshot_dir.join("last_snapshot_id")).unwrap();
  assert!(last_id.starts_with("T2-N1-10-"));

  let snapshot_id_dir = snapshot_id_dir(&snapshot_dir, &last_id);
  assert!(snapshot_id_dir.exists());

  let snapshot_data = snapshot_data_file(&snapshot_id_dir);
  assert!(snapshot_data.exists());

  let meta_file = snapshot_id_dir.join("meta");
  assert!(meta_file.exists());
}

#[compio::test]
async fn test_build_snapshot_empty_db() {
  let (engine, _temp_db) = create_test_engine_empty();
  let snapshot_dir = tempdir().unwrap().path().join("snapshots");

  let last_applied_log_id = None;
  let last_membership = StoredMembership::new(None, Membership::default());

  let result = build_snapshot(&engine, &snapshot_dir, last_applied_log_id, last_membership).await;

  assert!(result.is_ok());

  let snapshot = result.unwrap();
  assert!(snapshot.meta.last_log_id.is_none());

  let last_id = read_to_string(snapshot_dir.join("last_snapshot_id")).unwrap();
  assert!(last_id.starts_with("0-0-"));
}

#[test]
fn test_snapshot_id_format_with_log_id() {
  let last_applied_log_id = Some(LogId {
    leader_id: LeaderId {
      term: 3,
      node_id: 5,
    },
    index: 100,
  });
  let snapshot_idx = 1234567890;

  let snapshot_id = if let Some(last) = last_applied_log_id {
    let leader_id = last.committed_leader_id();
    let idx = last.index();
    format!("{leader_id}-{idx}-{snapshot_idx}")
  } else {
    format!("0-0-{snapshot_idx}")
  };

  assert_eq!(snapshot_id, "T3-N5-100-1234567890");
}
