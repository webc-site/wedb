//! 未决快照崩溃恢复回归（对标 C# AofRecover.cs:103 SingleLogRecover 以
//! asReplica: false 调用 ProcessAofRecordInternal 的单机恢复契约）
//!
//! 缺陷场景：检查点快照窗口内崩溃——AOF 已写 CheckpointStartCommit 与后续
//! 新代增量，未及写 CheckpointEndCommit。恢复驱动若误持副本身份
//! （as_replica = true），增量在 record_gate::should_skip_record 命中模糊区
//! 拦截分支被暂存 fuzzy_region_ops 并跳过重放，且清算臂永不触发，全部增量
//! 随 AofProcessor 析构物理丢弃（数据丢失）。
//!
//! 断言面：as_replica = false 时版本闸严格退化为 is_old_version_record
//! 直写判定——版本 ≥ 恢复基线的未决快照增量在单任务与页级并行两条恢复臂
//! 均即时完整落库。

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

/// 单物理日志拓扑的 AOF 装配（可指定并行回放任务数）
fn aof_with_replay_tasks(replay_task_count: i32) -> Arc<GarnetAppendOnlyFile> {
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: 1,
    aof_replay_task_count: replay_task_count,
    ..RuntimeServerOptions::default()
  };
  let backends = {
    let (_dirs, backends) = wnode_test::test_sublogs("aof_recover_pending_snapshot", 1);
    backends
  };
  Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  ))
}

/// 物理键编码（与主写入面同构：[NsVarint][DbVarint][KeyTag][用户键]）
fn physical(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// upsert 条目编码入队
fn enqueue_upsert(log: &GarnetLog, version: i64, key: &[u8], value: &[u8]) -> aok::Result<i64> {
  let input = replay_input_bytes(RespCommand::Set, vec![key.to_vec(), value.to_vec()]);
  Ok(log.enqueue(&RecordShape {
    op_type: AofEntryType::StoreUpsert,
    version,
    session_id: 1,
    key: &physical(key),
    value,
    input: &input,
    database_id: 0,
  })?)
}

/// 构造崩溃现场：基线 (v5) 写入 → CheckpointStartCommit (v6) → 新代增量
/// (v6) → 缺失 CheckpointEndCommit（模拟快照窗口断电）→ 物理刷盘
async fn crash_with_pending_snapshot(log: &GarnetLog, inc_count: usize) -> aok::Result<()> {
  // 基线：上一个已完成检查点覆盖的写入（版本 5）
  let _ = enqueue_upsert(log, 5, b"base", b"v-base")?;
  // 快照窗口开启标记（新代 6）
  let _ = log.enqueue_database_commit(AofEntryType::CheckpointStartCommit, 6)?;
  // 窗口内增量写（版本 6 ≥ 基线，恢复须即时重放）
  for i in 0..inc_count {
    let k = format!("inc_{i}");
    let v = format!("val_{i}");
    let _ = enqueue_upsert(log, 6, k.as_bytes(), v.as_bytes())?;
  }
  // 崩溃：CheckpointEndCommit 未写出
  log.commit_async().await;
  OK
}

/// 单任务恢复臂：未决快照增量完整落库
#[compio::test]
async fn recover_pending_snapshot_single_task_replays_increments() -> aok::Void {
  let (_dir, _store) = open_test_store("aof-recover-pending-src.db")?;
  let aof = aof_with_replay_tasks(1);
  crash_with_pending_snapshot(aof.log(), 8).await?;

  // 恢复落点：全新库，版本基线 5（上一个已完成检查点）
  let (_dir2, store2) = open_test_store("aof-recover-pending-dst.db")?;
  let session2 = store2.new_session()?;
  let storage2 = StorageSession::new(session2.enter_batch());
  store2.set_current_version(5);
  let target = ReplayTarget {
    session: &storage2,
    store: Arc::clone(&store2),
    aof_floor: vec![],
  };
  let processor = AofProcessor::new(Arc::clone(&aof));
  AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;

  assert_eq!(
    storage2.read_string(b"base").await?,
    Some(b"v-base".to_vec()),
    "基线写入恢复"
  );
  for i in 0..8 {
    let k = format!("inc_{i}");
    let v = format!("val_{i}");
    assert_eq!(
      storage2.read_string(k.as_bytes()).await?,
      Some(v.into_bytes()),
      "未决快照增量 {k} 须即时重放落库（as_replica=false 版本闸退化直写）"
    );
  }
  OK
}

/// 页级并行恢复臂（4 Worker 双闸栏）：未决快照增量完整落库
#[compio::test]
async fn recover_pending_snapshot_parallel_replays_increments() -> aok::Void {
  let (_dir, _store) = open_test_store("aof-recover-pending-par-src.db")?;
  let aof = aof_with_replay_tasks(4);
  // 64 键跨哈希分片铺满各回放任务
  crash_with_pending_snapshot(aof.log(), 64).await?;

  let (_dir2, store2) = open_test_store("aof-recover-pending-par-dst.db")?;
  let session2 = store2.new_session()?;
  let storage2 = StorageSession::new(session2.enter_batch());
  store2.set_current_version(5);
  let target = ReplayTarget {
    session: &storage2,
    store: Arc::clone(&store2),
    aof_floor: vec![],
  };
  let processor = AofProcessor::new(Arc::clone(&aof));
  AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;

  assert_eq!(
    storage2.read_string(b"base").await?,
    Some(b"v-base".to_vec()),
    "基线写入恢复"
  );
  for i in 0..64 {
    let k = format!("inc_{i}");
    let v = format!("val_{i}");
    assert_eq!(
      storage2.read_string(k.as_bytes()).await?,
      Some(v.into_bytes()),
      "并行臂未决快照增量 {k} 须完整恢复，不得入模糊区缓冲丢弃"
    );
  }
  OK
}
