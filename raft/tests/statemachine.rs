use futures::stream::iter;
use wedb_raft::endpoint::Endpoint;
use wedb_raft::engine::FjallEngine;
use wedb_raft::store::key::{SM_DATA_FAMILY, SM_META_FAMILY};
use wedb_raft::store::statemachine::FjallStateMachine;
use wedb_raft::types::Node;

mod util;
use util::create_log_id;

use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;
use tempfile::tempdir;

async fn create_test_state_machine() -> FjallStateMachine {
  let dir = tempdir().unwrap();
  let engine = Arc::new(
    FjallEngine::new(
      dir.path(),
      vec!["_sm_meta".to_string(), "_sm_data".to_string()],
    )
    .unwrap(),
  );
  let snap_dir = dir.path().join("snapshots");
  FjallStateMachine::new(engine, snap_dir).await.unwrap()
}

#[compio::test]
async fn test_set_and_get_last_applied() -> Result<(), io::Error> {
  let sm = create_test_state_machine().await;

  assert!(sm.get_last_applied_log_id()?.is_none());

  let log_id = create_log_id(1, 1, 100);
  sm.set_last_applied_log_id(Some(log_id))?;

  let retrieved = sm.get_last_applied_log_id()?.unwrap();
  assert_eq!(retrieved.leader_id.term, log_id.leader_id.term);
  assert_eq!(retrieved.leader_id.node_id, log_id.leader_id.node_id);
  assert_eq!(retrieved.index, log_id.index);

  let new_log_id = create_log_id(2, 2, 200);
  sm.set_last_applied_log_id(Some(new_log_id))?;

  let updated = sm.get_last_applied_log_id()?.unwrap();
  assert_eq!(updated.leader_id.term, 2);
  assert_eq!(updated.leader_id.node_id, 2);
  assert_eq!(updated.index, 200);

  sm.set_last_applied_log_id(None)?;
  assert!(sm.get_last_applied_log_id()?.is_none());

  Ok(())
}

#[compio::test]
async fn test_recover_sys_data_last_applied() -> Result<(), io::Error> {
  let temp_dir = tempfile::tempdir().unwrap();
  let path = temp_dir.path().to_path_buf();
  let keyspace_li = vec![SM_META_FAMILY.to_string(), SM_DATA_FAMILY.to_string()];
  let engine = Arc::new(FjallEngine::new(path.join("data"), keyspace_li).unwrap());

  let sm1 = FjallStateMachine::new(engine.clone(), path.clone())
    .await
    .unwrap();

  let log_id = create_log_id(3, 5, 300);
  sm1.set_last_applied_log_id(Some(log_id))?;

  let sm2 = FjallStateMachine::new(engine.clone(), path.clone())
    .await
    .unwrap();

  let recovered = sm2.get_last_applied_log_id()?;
  assert!(recovered.is_some());

  let recovered_log_id = recovered.unwrap();
  assert_eq!(recovered_log_id.leader_id.term, log_id.leader_id.term);
  assert_eq!(recovered_log_id.leader_id.node_id, log_id.leader_id.node_id);
  assert_eq!(recovered_log_id.index, log_id.index);

  let new_log_id = create_log_id(4, 6, 400);
  sm2.set_last_applied_log_id(Some(new_log_id))?;

  let sm3 = FjallStateMachine::new(engine.clone(), path).await.unwrap();

  let updated = sm3.get_last_applied_log_id()?.unwrap();
  assert_eq!(updated.leader_id.term, 4);
  assert_eq!(updated.leader_id.node_id, 6);
  assert_eq!(updated.index, 400);

  Ok(())
}

#[compio::test]
async fn test_recover_sys_data_nodes() -> Result<(), io::Error> {
  let temp_dir = tempfile::tempdir().unwrap();
  let path = temp_dir.path().to_path_buf();
  let keyspace_li = vec![SM_META_FAMILY.to_string(), SM_DATA_FAMILY.to_string()];
  let engine = Arc::new(FjallEngine::new(path.join("data"), keyspace_li).unwrap());

  let sm1 = FjallStateMachine::new(engine.clone(), path.clone())
    .await
    .unwrap();

  let node1 = Node {
    node_id: 1,
    endpoint: Endpoint::new("127.0.0.1", 8081),
  };
  sm1.add_node(node1)?;

  let node2 = Node {
    node_id: 2,
    endpoint: Endpoint::new("127.0.0.1", 8082),
  };
  sm1.add_node(node2)?;

  let sm2 = FjallStateMachine::new(engine.clone(), path.clone())
    .await
    .unwrap();

  let nodes = sm2.get_nodes()?;
  assert_eq!(nodes.len(), 2);
  assert!(nodes.contains_key(&1));
  assert!(nodes.contains_key(&2));

  let recovered_node1 = nodes.get(&1).unwrap();
  assert_eq!(recovered_node1.node_id, 1);
  assert_eq!(recovered_node1.endpoint.host(), "127.0.0.1");
  assert_eq!(recovered_node1.endpoint.port(), 8081);

  sm2.remove_node(1)?;

  let sm3 = FjallStateMachine::new(engine.clone(), path).await.unwrap();

  let nodes3 = sm3.get_nodes()?;
  assert_eq!(nodes3.len(), 1);
  assert!(!nodes3.contains_key(&1));
  assert!(nodes3.contains_key(&2));

  Ok(())
}

#[compio::test]
async fn test_recover_sys_data_all_fields() -> Result<(), io::Error> {
  let temp_dir = tempfile::tempdir().unwrap();
  let path = temp_dir.path().to_path_buf();
  let keyspace_li = vec![SM_META_FAMILY.to_string(), SM_DATA_FAMILY.to_string()];
  let engine = Arc::new(FjallEngine::new(path.join("data"), keyspace_li).unwrap());

  let sm1 = FjallStateMachine::new(engine.clone(), path.clone())
    .await
    .unwrap();

  let log_id = create_log_id(7, 9, 700);
  sm1.set_last_applied_log_id(Some(log_id))?;

  for i in 1..=3 {
    sm1.add_node(Node {
      node_id: i,
      endpoint: Endpoint::new("127.0.0.1", 9090 + i as u16),
    })?;
  }

  let sm2 = FjallStateMachine::new(engine.clone(), path).await.unwrap();

  let recovered_log_id = sm2.get_last_applied_log_id()?.unwrap();
  assert_eq!(recovered_log_id.leader_id.term, 7);
  assert_eq!(recovered_log_id.leader_id.node_id, 9);
  assert_eq!(recovered_log_id.index, 700);

  let membership = sm2.get_last_membership()?;
  let voter_ids: BTreeSet<_> = membership.membership().voter_ids().collect();
  assert_eq!(voter_ids.len(), 3);
  assert!(voter_ids.contains(&1));
  assert!(voter_ids.contains(&2));
  assert!(voter_ids.contains(&3));

  Ok(())
}

#[compio::test]
async fn test_scan_prefix_basic() -> Result<(), io::Error> {
  let sm = create_test_state_machine().await;
  let cf = sm.keyspace_data();

  cf.insert(b"user:1:name", b"Alice").unwrap();
  cf.insert(b"user:1:age", b"25").unwrap();
  cf.insert(b"user:2:name", b"Bob").unwrap();
  cf.insert(b"user:2:age", b"30").unwrap();
  cf.insert(b"product:1:name", b"Laptop").unwrap();
  cf.insert(b"product:1:price", b"999").unwrap();

  let user_results = sm.scan_prefix(b"user:")?;
  assert_eq!(user_results.len(), 4);

  let keys: Vec<_> = user_results
    .iter()
    .map(|(k, _)| String::from_utf8_lossy(k).to_string())
    .collect();
  assert!(keys.contains(&"user:1:name".to_string()));
  assert!(keys.contains(&"user:1:age".to_string()));
  assert!(keys.contains(&"user:2:name".to_string()));
  assert!(keys.contains(&"user:2:age".to_string()));

  let user1_results = sm.scan_prefix(b"user:1:")?;
  assert_eq!(user1_results.len(), 2);

  let product_results = sm.scan_prefix(b"product:")?;
  assert_eq!(product_results.len(), 2);

  Ok(())
}

#[compio::test]
async fn test_scan_prefix_empty_result() -> Result<(), io::Error> {
  let sm = create_test_state_machine().await;
  let cf = sm.keyspace_data();

  cf.insert(b"key1", b"value1").unwrap();
  cf.insert(b"key2", b"value2").unwrap();

  let results = sm.scan_prefix(b"nonexistent:")?;
  assert!(results.is_empty());

  Ok(())
}

#[compio::test]
async fn test_scan_prefix_binary_keys() -> Result<(), io::Error> {
  let sm = create_test_state_machine().await;
  let cf = sm.keyspace_data();

  cf.insert([0x01, 0x00, 0x01], b"data1").unwrap();
  cf.insert([0x01, 0x00, 0x02], b"data2").unwrap();
  cf.insert([0x01, 0x01, 0x01], b"data3").unwrap();
  cf.insert([0x02, 0x00, 0x01], b"data4").unwrap();

  let results = sm.scan_prefix(&[0x01, 0x00])?;
  assert_eq!(results.len(), 2);

  let values: Vec<_> = results.iter().map(|(_, v)| v.clone()).collect();
  assert!(values.contains(&b"data1".to_vec()));
  assert!(values.contains(&b"data2".to_vec()));

  Ok(())
}

#[compio::test]
async fn test_sweep_expired_keys() -> Result<(), io::Error> {
  let sm = create_test_state_machine().await;
  let cf = sm.keyspace_data();

  // 写入 3 个键：1 个不过期，1 个已过期，1 个未到期
  cf.insert(b"permanent", b"val_perm").unwrap();

  cf.insert(b"expired_key", b"val_exp").unwrap();
  sm.set_ttl("expired_key", 1000).unwrap(); // 1970 年，必然过期

  cf.insert(b"future_key", b"val_future").unwrap();
  sm.set_ttl("future_key", 4_000_000_000_000).unwrap(); // 远未来

  // 主动扫描清理
  let swept = sm.sweep_expired_keys(100)?;
  assert_eq!(swept, 1, "Should sweep exactly 1 expired key");

  // 验证永久键与未来键仍在
  assert!(cf.get(b"permanent").unwrap().is_some());
  assert!(cf.get(b"future_key").unwrap().is_some());

  // 验证已过期键已被物理清除
  assert!(cf.get(b"expired_key").unwrap().is_none());
  assert!(sm.get_ttl_expire_at("expired_key")?.is_none());

  Ok(())
}

#[compio::test]
async fn test_state_machine_batch_upsert_atomic() -> Result<(), io::Error> {
  use wedb_raft::types::{Cmd, Entry, LogEntry, UpsertKV};
  use zenoh_raft::entry::EntryPayload;
  use zenoh_raft::storage::RaftStateMachine;

  let mut sm = create_test_state_machine().await;

  // 构造包含 200 条插入与 50 条删除/TTL 的原子批量操作
  let mut upserts = Vec::with_capacity(250);
  for i in 0..200 {
    upserts.push(UpsertKV::insert(
      format!("batch_key_{i}"),
      format!("batch_val_{i}").into_bytes(),
    ));
  }
  for i in 0..50 {
    upserts.push(UpsertKV::insert_with_ttl(
      format!("ttl_key_{i}"),
      format!("ttl_val_{i}").into_bytes(),
      Some(4_000_000_000_000), // 远未来
    ));
  }

  let entry = Entry {
    log_id: create_log_id(1, 1, 1),
    payload: EntryPayload::Normal(LogEntry::new(Cmd::BatchUpsertKV { entries: upserts })),
  };

  let stream = iter(vec![Ok((entry, None))]);
  sm.apply(stream).await?;

  // 验证最后应用的日志索引已原子持久化
  let last_applied = sm.get_last_applied_log_id()?.unwrap();
  assert_eq!(last_applied.index, 1);

  // 验证批量写入的数据均正确落盘并可读取
  for i in 0..200 {
    let val = sm.get_kv(&format!("batch_key_{i}"))?;
    assert_eq!(val, Some(format!("batch_val_{i}").into_bytes()));
  }

  // 验证 TTL 键及其元数据正确存储
  for i in 0..50 {
    let val = sm.get_kv(&format!("ttl_key_{i}"))?;
    assert_eq!(val, Some(format!("ttl_val_{i}").into_bytes()));
    assert_eq!(
      sm.get_ttl_expire_at(&format!("ttl_key_{i}"))?,
      Some(4_000_000_000_000)
    );
  }

  // 执行第二轮批量操作：批量删除与覆盖
  let mut batch2 = Vec::new();
  for i in 0..100 {
    batch2.push(UpsertKV::delete(format!("batch_key_{i}")));
  }
  for i in 100..200 {
    batch2.push(UpsertKV::insert(
      format!("batch_key_{i}"),
      format!("updated_val_{i}").into_bytes(),
    ));
  }

  let entry2 = Entry {
    log_id: create_log_id(1, 1, 2),
    payload: EntryPayload::Normal(LogEntry::new(Cmd::BatchUpsertKV { entries: batch2 })),
  };

  let stream2 = iter(vec![Ok((entry2, None))]);
  sm.apply(stream2).await?;

  // 验证已删除的键返回 None
  for i in 0..100 {
    let val = sm.get_kv(&format!("batch_key_{i}"))?;
    assert!(val.is_none(), "Key batch_key_{i} should be deleted");
  }

  // 验证更新的键返回新值
  for i in 100..200 {
    let val = sm.get_kv(&format!("batch_key_{i}"))?;
    assert_eq!(val, Some(format!("updated_val_{i}").into_bytes()));
  }

  Ok(())
}

#[compio::test]
async fn test_state_machine_with_ttl_key_zero_alloc() -> Result<(), io::Error> {
  let sm = create_test_state_machine().await;

  // 测试短键（<= 120 字节栈上构造）
  let short_key = "user:session:12345";
  sm.set_ttl(short_key, 2_000_000_000_000)?;
  assert_eq!(sm.get_ttl_expire_at(short_key)?, Some(2_000_000_000_000));
  assert!(!sm.is_expired(short_key));
  let removed = sm.remove_ttl(short_key)?;
  assert!(removed);
  assert!(sm.get_ttl_expire_at(short_key)?.is_none());

  // 测试长键（> 120 字节堆上构造）
  let long_key = "a".repeat(200);
  sm.set_ttl(&long_key, 3_000_000_000_000)?;
  assert_eq!(sm.get_ttl_expire_at(&long_key)?, Some(3_000_000_000_000));
  assert!(!sm.is_expired(&long_key));
  let removed_long = sm.remove_ttl(&long_key)?;
  assert!(removed_long);
  assert!(sm.get_ttl_expire_at(&long_key)?.is_none());

  Ok(())
}

#[compio::test]
async fn test_state_machine_ttl_idx_time_ordered_pruning() -> Result<(), io::Error> {
  let sm = create_test_state_machine().await;
  let cf = sm.keyspace_data();

  // 写入 5 个键：2 个已过期，3 个未过期
  for i in 1..=2 {
    let k = format!("exp_{i}");
    cf.insert(k.as_bytes(), b"val").unwrap();
    sm.set_ttl(&k, 1000 + i)?; // 早期过期
  }

  for i in 1..=3 {
    let k = format!("future_{i}");
    cf.insert(k.as_bytes(), b"val").unwrap();
    sm.set_ttl(&k, 4_000_000_000_000 + i)?; // 远未来
  }

  // 执行清理，应该只清理 2 个过期键并在遇到第 3 个（未过期）时立即终止
  let swept = sm.sweep_expired_keys(100)?;
  assert_eq!(swept, 2);

  for i in 1..=2 {
    let k = format!("exp_{i}");
    assert!(cf.get(k.as_bytes()).unwrap().is_none());
    assert!(sm.get_ttl_expire_at(&k)?.is_none());
  }

  for i in 1..=3 {
    let k = format!("future_{i}");
    assert!(cf.get(k.as_bytes()).unwrap().is_some());
    assert!(sm.get_ttl_expire_at(&k)?.is_some());
  }

  Ok(())
}
