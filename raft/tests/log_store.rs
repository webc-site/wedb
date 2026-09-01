mod util;
use util::create_log_id;

use std::io;
use std::sync::Arc;
use tempfile::tempdir;
use wedb_raft::engine::FjallEngine;
use wedb_raft::store::FjallLogStore;
use wedb_raft::types::{Cmd, Entry, LogEntry, TypeConfig, UpsertKV};
use zenoh_raft::Vote;
use zenoh_raft::entry::EntryPayload;
use zenoh_raft::storage::{IOFlushed, RaftLogReader, RaftLogStorage};

fn create_entry(term: u64, node_id: u64, index: u64) -> Entry {
  Entry {
    log_id: create_log_id(term, node_id, index),
    payload: EntryPayload::Normal(LogEntry::new(Cmd::UpsertKV(UpsertKV::insert(
      format!("key_{index}"),
      format!("value_{index}").into_bytes(),
    )))),
  }
}

fn create_test_log_store() -> FjallLogStore<TypeConfig> {
  let dir = tempdir().unwrap();
  let engine = Arc::new(
    FjallEngine::new(
      dir.path(),
      vec!["_raft_log".to_string(), "_raft_meta".to_string()],
    )
    .unwrap(),
  );
  FjallLogStore::<TypeConfig>::create(engine).unwrap()
}

async fn append_entries(
  log_store: &mut FjallLogStore<TypeConfig>,
  entries: Vec<Entry>,
) -> Result<(), io::Error> {
  log_store
    .append(entries, IOFlushed::noop())
    .await
    .map_err(|e| io::Error::other(format!("{e}")))?;
  Ok(())
}

#[compio::test]
async fn test_raft_log_operations() -> Result<(), io::Error> {
  let mut log_store = create_test_log_store();

  let entries: Vec<_> = (1..=10).map(|i| create_entry(1, 1, i)).collect();
  append_entries(&mut log_store, entries).await?;

  let all = log_store.try_get_log_entries(1..=10).await?;
  assert_eq!(all.len(), 10);
  assert_eq!(all[all.len() - 1].log_id.index, 10);

  let range = log_store.try_get_log_entries(3..=7).await?;
  assert_eq!(range.len(), 5);
  assert_eq!(range[0].log_id.index, 3);

  let more: Vec<_> = (11..=15).map(|i| create_entry(1, 1, i)).collect();
  append_entries(&mut log_store, more).await?;
  assert_eq!(log_store.try_get_log_entries(1..=15).await?.len(), 15);

  log_store
    .truncate_after(Some(create_log_id(1, 1, 11)))
    .await?;
  assert_eq!(log_store.try_get_log_entries(1..=15).await?.len(), 11);
  assert_eq!(log_store.try_get_log_entries(12..=15).await?.len(), 0);

  log_store.purge(create_log_id(1, 1, 5)).await?;
  assert_eq!(log_store.get_last_purged_log_id()?.unwrap().index, 5);
  let after_purge = log_store.try_get_log_entries(1..=10).await?;
  assert_eq!(after_purge.len(), 5);
  assert_eq!(after_purge[0].log_id.index, 6);

  let new: Vec<_> = (11..=13).map(|i| create_entry(2, 1, i)).collect();
  append_entries(&mut log_store, new).await?;
  let final_logs = log_store.try_get_log_entries(6..=13).await?;
  assert_eq!(final_logs.len(), 8);
  assert_eq!(final_logs[0].log_id.leader_id.term, 1);
  assert_eq!(final_logs[5].log_id.leader_id.term, 2);

  Ok(())
}

#[test]
fn test_set_and_get_last_purged() -> Result<(), io::Error> {
  let log_store = create_test_log_store();

  assert!(log_store.get_last_purged_log_id()?.is_none());

  let log_id = create_log_id(1, 1, 100);
  log_store.set_last_purged_log_id(&log_id)?;

  let retrieved = log_store.get_last_purged_log_id()?.unwrap();
  assert_eq!(retrieved.leader_id.term, log_id.leader_id.term);
  assert_eq!(retrieved.leader_id.node_id, log_id.leader_id.node_id);
  assert_eq!(retrieved.index, log_id.index);

  let new_log_id = create_log_id(2, 2, 200);
  log_store.set_last_purged_log_id(&new_log_id).unwrap();

  let updated = log_store.get_last_purged_log_id()?.unwrap();
  assert_eq!(updated.index, new_log_id.index);

  Ok(())
}

#[test]
fn test_set_and_get_committed() -> Result<(), io::Error> {
  let log_store = create_test_log_store();

  assert!(log_store.get_committed()?.is_none());

  let log_id = create_log_id(1, 1, 100);
  log_store.set_committed(&Some(log_id))?;

  let retrieved = log_store.get_committed()?.unwrap();
  assert_eq!(retrieved.leader_id.term, log_id.leader_id.term);
  assert_eq!(retrieved.leader_id.node_id, log_id.leader_id.node_id);
  assert_eq!(retrieved.index, log_id.index);

  let new_log_id = create_log_id(2, 2, 200);
  log_store.set_committed(&Some(new_log_id))?;

  let updated = log_store.get_committed()?.unwrap();
  assert_eq!(updated.index, new_log_id.index);

  log_store.set_committed(&None)?;
  assert!(log_store.get_committed()?.is_none());

  Ok(())
}

#[test]
fn test_set_and_get_vote() -> Result<(), io::Error> {
  let mut log_store = create_test_log_store();

  assert!(log_store.get_vote()?.is_none());

  let vote = Vote::new(1, 1);
  log_store.set_vote(&vote)?;

  let retrieved = log_store.get_vote()?.unwrap();
  assert_eq!(retrieved.leader_id().term, vote.leader_id().term);
  assert_eq!(retrieved.leader_id().node_id, vote.leader_id().node_id);

  let new_vote = Vote::new(2, 2);
  log_store.set_vote(&new_vote)?;

  let updated = log_store.get_vote()?.unwrap();
  assert_eq!(updated.leader_id().term, 2);
  assert_eq!(updated.leader_id().node_id, 2);

  Ok(())
}

#[compio::test]
async fn test_log_store_large_batch_append() -> Result<(), io::Error> {
  let mut log_store = create_test_log_store();

  // 一次性批量追加 500 条日志条目
  let entries: Vec<_> = (1..=500).map(|i| create_entry(1, 1, i)).collect();
  append_entries(&mut log_store, entries).await?;

  let all = log_store.try_get_log_entries(1..=500).await?;
  assert_eq!(all.len(), 500);
  assert_eq!(all[0].log_id.index, 1);
  assert_eq!(all[499].log_id.index, 500);

  // 验证范围检索
  let sub = log_store.try_get_log_entries(200..=300).await?;
  assert_eq!(sub.len(), 101);
  assert_eq!(sub[0].log_id.index, 200);
  assert_eq!(sub[100].log_id.index, 300);

  Ok(())
}
