//! AOF 重放归组口坏流显式失败回归（task/ing/waof-replay-nested-txnstart-sp-silent-swallow）
//!
//! 对标 C# AofReplayCoordinator.cs:146-147/:169-170 活动组在位两臂
//! throw GarnetException「恢复即中止」契约：
//! 1. 同会话连发两 TxnStart（孤儿 TxnStart 后接新 TxnStart 的坏流撞点）：
//!    重放必须显式错误上抛，绝不静默吞并保留残组带病续放；
//! 2. 活动组在位撞组内 StoredProcedure 条目（session 与活动组同键）：同样
//!    显式失败（组外 SP 消费基线 replay_stored_proc 恒 Err，组内臂拒绝
//!    语义对齐，不留吞并分叉）；
//! 3. 正常标记配对路径（TxnStart..TxnCommit）不受拒绝语义影响，整组重放
//!    语义保持。

use std::sync::Arc;

use aok::OK;
use waof::AofEntryType;
use wconf::RuntimeServerOptions;
use wnode::{
  aof::{
    aof_processor::{AofProcessor, ReplayTarget},
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::{GarnetLog, RecordShape},
    recover::aof_recover::AofRecover,
  },
  storage::session::storage_session::StorageSession,
};
use wnode_test::replay_input_bytes;
use wresp::command::RespCommand;
use wtest_base::open_test_store;
use wval::{KeyTag, NamespaceDbCodec};

/// 单物理日志拓扑的 AOF 装配（对标 aof_replay.rs aof_fixture 同一形态）
fn aof_fixture() -> aok::Result<Arc<GarnetAppendOnlyFile>> {
  let options = RuntimeServerOptions::default();
  let backends = {
    let (_dirs, backends) = wnode_test::test_sublogs("aof_nested_txn_reject", 1);
    backends
  };
  Ok(Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  )))
}

/// 物理键编码（条目 key 统一为 wkv 物理键：[NsVarint][DbVarint][KeyTag][用户键]）
fn physical(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// 无键标记条目入队（TxnStart/TxnCommit/StoredProcedure 等，session 指定）
fn enqueue_marker(log: &GarnetLog, op_type: AofEntryType, session_id: i32) -> aok::Result<i64> {
  Ok(log.enqueue(&RecordShape {
    op_type,
    version: 5,
    session_id,
    key: &[],
    value: &[],
    input: &[],
    database_id: 0,
  })?)
}

/// upsert 条目编码入队（指定 session_id，事务组内数据条目形态）
fn enqueue_upsert_session(
  log: &GarnetLog,
  session_id: i32,
  key: &[u8],
  value: &[u8],
) -> aok::Result<i64> {
  let input = replay_input_bytes(RespCommand::Set, vec![key.to_vec(), value.to_vec()]);
  Ok(log.enqueue(&RecordShape {
    op_type: AofEntryType::StoreUpsert,
    version: 5,
    session_id,
    key: &physical(key),
    value,
    input: &input,
    database_id: 0,
  })?)
}

/// 场景 1：同会话连发两 TxnStart → 重放显式错误上抛（文案对位 C#
/// "No nested transactions expected"），绝非静默吞并收敛
#[compio::test]
async fn nested_txn_start_same_session_rejects_loudly() -> aok::Void {
  let (_dir, _store) = open_test_store("aof-nested-txn.db")?;
  let aof = aof_fixture()?;
  let log = aof.log();

  enqueue_marker(log, AofEntryType::TxnStart, 1)?;
  enqueue_marker(log, AofEntryType::TxnStart, 1)?;
  log.commit();

  // 重放段：全新库恢复，坏流必须在归组口撞出显式错误
  let (_dir2, store2) = open_test_store("aof-nested-txn-dst.db")?;
  let session2 = store2.new_session()?;
  let storage2 = StorageSession::new(session2.enter_batch());
  store2.set_current_version(5);
  let target = ReplayTarget {
    session: &storage2,
    store: Arc::clone(&store2),
    aof_floor: vec![],
  };
  let processor = AofProcessor::new(Arc::clone(&aof));
  let err = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target)
    .await
    .expect_err("嵌套 TxnStart 必须显式失败上抛而非静默收敛");
  assert!(
    err.to_string().contains("No nested transactions expected"),
    "错误文案须对位 C# 契约: {err}"
  );
  OK
}

/// 场景 2：活动组在位撞组内 StoredProcedure（session 与活动组同键）→
/// 重放显式错误上抛（文案对位组外 SP 消费基线 + within transaction 语境）
#[compio::test]
async fn stored_proc_within_active_txn_group_rejects_loudly() -> aok::Void {
  let (_dir, _store) = open_test_store("aof-sp-in-txn.db")?;
  let aof = aof_fixture()?;
  let log = aof.log();

  enqueue_marker(log, AofEntryType::TxnStart, 7)?;
  enqueue_marker(log, AofEntryType::StoredProcedure, 7)?;
  log.commit();

  let (_dir2, store2) = open_test_store("aof-sp-in-txn-dst.db")?;
  let session2 = store2.new_session()?;
  let storage2 = StorageSession::new(session2.enter_batch());
  store2.set_current_version(5);
  let target = ReplayTarget {
    session: &storage2,
    store: Arc::clone(&store2),
    aof_floor: vec![],
  };
  let processor = AofProcessor::new(Arc::clone(&aof));
  let err = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target)
    .await
    .expect_err("组内存储过程条目必须显式失败上抛而非静默吞并");
  assert!(
    err.to_string().contains("已移除自定义事务过程支持"),
    "错误文案须对位组外 SP 消费基线: {err}"
  );
  OK
}

/// 场景 3：正常标记配对路径（TxnStart..组内写..TxnCommit）不受拒绝语义
/// 影响，整组重放语义保持（组内键恢复后可见）
#[compio::test]
async fn paired_txn_markers_replay_path_unchanged() -> aok::Void {
  let (_dir, _store) = open_test_store("aof-txn-paired.db")?;
  let aof = aof_fixture()?;
  let log = aof.log();

  enqueue_marker(log, AofEntryType::TxnStart, 3)?;
  enqueue_upsert_session(log, 3, b"txn:paired", b"ok")?;
  enqueue_marker(log, AofEntryType::TxnCommit, 3)?;
  log.commit();

  let (_dir2, store2) = open_test_store("aof-txn-paired-dst.db")?;
  let session2 = store2.new_session()?;
  let storage2 = StorageSession::new(session2.enter_batch());
  store2.set_current_version(5);
  let target = ReplayTarget {
    session: &storage2,
    store: Arc::clone(&store2),
    aof_floor: vec![],
  };
  let processor = AofProcessor::new(Arc::clone(&aof));
  let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
  assert_eq!(replayed, 3, "TxnStart/组内写/TxnCommit 三条目均计入重放");
  assert_eq!(
    storage2.read_string(b"txn:paired").await?,
    Some(b"ok".to_vec()),
    "正常配对组整组重放语义保持"
  );
  OK
}
