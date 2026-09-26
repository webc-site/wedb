//! 多日志崩溃恢复跨分片事务组回归（对标 garnet/testGNV 相关事务恢复场景与
//! libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:ProcessTransactionGroup
//! 的 !asReplica 豁免分支）
//!
//! 回归点：多物理日志拓扑下 `AofRecover::multi_log_recover` 串行驱动各物理
//! 子日志，崩溃恢复臂（as_replica=false）重放跨子日志事务组
//! （participant_count > 1）时必须豁免 Acquire/Release 同步栅栏——其余参与者
//! 所在子日志的回放驱动被当前 await 永久阻塞，等栅栏即确定性死锁（C# 恢复
//! 期读不会暴露局部中间态事务、写入顺序已在入队时确定，故直接顺序重放）。

use std::sync::Arc;

use aok::OK;
use waof::{AofAddress, AofEntryType, SequenceNumberGenerator};
use wbase::store_type::REPLAY_TASK_ACCESS_VECTOR_BYTES;
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
use wtxn::SublogAccess;
use wval::{KeyTag, NamespaceDbCodec};

/// 物理键编码（条目 key 统一为 wkv 物理键：[NsVarint][DbVarint][KeyTag][用户键]）
fn physical(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// upsert 条目编码入队（指定 session_id）
fn enqueue_set(
  log: &GarnetLog,
  version: i64,
  session_id: i32,
  key: &[u8],
  value: &[u8],
) -> aok::Result<i64> {
  let input = replay_input_bytes(RespCommand::Set, vec![key.to_vec(), value.to_vec()]);
  Ok(log.enqueue(&RecordShape {
    op_type: AofEntryType::StoreUpsert,
    version,
    session_id,
    key: &physical(key),
    value,
    input: &input,
    database_id: 0,
  })?)
}

/// 多物理日志崩溃恢复跨分片事务组：恢复流程顺畅收敛，全部事务数据落地，
/// 无栅栏死锁挂起。
#[compio::test]
async fn multi_log_recover_cross_sublog_txn_group_converges() -> aok::Void {
  // 两物理子日志分片拓扑（multi_log_enabled = physical_sublog_count > 1）
  let (_dirs, backends) = wnode_test::test_sublogs("aof_mlog_txn_dl", 2);
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: 2,
    aof_replay_task_count: 1,
    ..RuntimeServerOptions::default()
  };
  let seq_gen = Some(Arc::new(SequenceNumberGenerator::new(0)));
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, seq_gen.clone()).expect("构造 GarnetLog")),
    &options,
    seq_gen,
  ));
  let log = aof.log();

  // 跨子日志事务组：TxnStart/TxnCommit 标记按位图 0b11 广播两物理子日志
  //（分片事务头逐参与子日志落位），participant_count = 2；组内 SET 按键
  // 哈希分片路由，与标记同 session_id 入同一事务组
  let vectors = [[0u8; REPLAY_TASK_ACCESS_VECTOR_BYTES]; 2];
  let access = SublogAccess {
    physical_vector: 0b11,
    virtual_vectors: &vectors,
    participant_count: 2,
  };
  log.enqueue_txn(AofEntryType::TxnStart, 5, 7, &access)?;
  enqueue_set(log, 5, 7, b"txn_a", b"v1")?;
  enqueue_set(log, 5, 7, b"txn_b", b"v2")?;
  log.enqueue_txn(AofEntryType::TxnCommit, 5, 7, &access)?;
  // 非事务对照键（另一会话）
  enqueue_set(log, 5, 9, b"plain", b"v3")?;

  // 提交全物理子日志（同一 cookie 随各子日志落 commit 帧），随后对标生产
  // 重启第一步行：设备面恢复收敛各子日志 recovered_cookie（恢复上界来源）
  log.commit_async().await;
  log.recover_async().await.expect("设备面恢复");

  // 重放到全新库
  let (_dir, store) = open_test_store("aof-mlog-txn-dl-dst.db")?;
  let session = store.new_session()?;
  let storage = StorageSession::new(session.enter_batch());
  store.set_current_version(5);
  let target = ReplayTarget {
    session: &storage,
    store: Arc::clone(&store),
    aof_floor: vec![],
  };
  let processor = AofProcessor::new(Arc::clone(&aof));
  // until 向量全 -1 = 各物理子日志恢复至尾（生产 until=u64::MAX 同形）
  let until = AofAddress::create(2, -1);

  // 修复前：串行恢复首个子日志回放到该跨分片组即在 LeaderBarrier 永久
  // 挂起（其余参与者所在子日志的驱动无法启动），节点卡死在恢复流程；
  // 修复后：恢复臂 !as_replica 短路栅栏直接顺序重放，顺畅收敛
  let replayed = AofRecover::multi_log_recover(&processor, &aof, 0, &until, &target).await?;
  // 计数口径：两子日志 TxnStart/TxnCommit 各 2 条（事务标记消化亦计数）+
  // 组内 SET 2 + 对照 SET 1 = 7；commit 帧不计数
  assert_eq!(replayed, 7, "两子日志全量条目重放收敛");

  // 全部事务数据落地（组内键可能分属不同子日志，跨分片一致性不受影响）
  assert_eq!(
    storage.read_string(b"txn_a").await?,
    Some(b"v1".to_vec()),
    "事务组内键 txn_a 落地"
  );
  assert_eq!(
    storage.read_string(b"txn_b").await?,
    Some(b"v2".to_vec()),
    "事务组内键 txn_b 落地"
  );
  assert_eq!(
    storage.read_string(b"plain").await?,
    Some(b"v3".to_vec()),
    "非事务对照键落地"
  );

  OK
}
