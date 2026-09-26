//! 并行恢复无键同步操作 LeaderBarrier 有界会合回归（task/ing/zcode-r43-parrecover
//! 发现一）
//!
//! 对标 C# AofReplayCoordinator.cs:ProcessSynchronizedOperation 的
//! `leaderBarrier.TrySignalOrWait(out _, serverOptions.ReplicaSyncTimeout)`
//! 有限会合（默认 5 秒）与 libs/common/Synchronization/LeaderBarrier.cs:30-65
//! 超时不抛异常、首到场者仍以 Leader 身份独占执行并 Release 放行的语义。
//!
//! 回归点：同批「归属坏条目在前、FLUSH 在后」——坏条目（向量族 RMW 条目在
//! 向量域未接线时按归属哈希仅在归属 worker 上报错，非归属 worker `continue`
//! 跳过）使归属 worker 永不抵达 FLUSH 栅栏；其余 worker 依约进 LeaderBarrier。
//! 修复前 join_barrier 恒传 None 无限等待且等待不观察任何取消位，等待 worker
//! 被钉死页内、错误 worker 与 leader 冻结于 completed 闸（r27 修复的取消链路
//! 对栅栏内部等待不可达，冻结点自闸栏平移至栅栏自身）；修复后会合以
//! replica_sync_timeout 为界，超时首到场者仍以 Leader 独占执行 FLUSH 并放行，
//! 恢复有界收敛，错误经 error_slot 于 join 后上抛。

use std::{sync::Arc, time::Duration};

use compio::time::timeout;
use waof::{AofEntryType, WalConfig};
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
use wtest_base::open_test_store_with_budget;
use wval::{KeyTag, NamespaceDbCodec};

/// 有界时限：栅栏会合超时（ReplicaSyncTimeout 默认 5 秒）单次在界内收敛，
/// 修复前（恒 None 无限等待）在此确定性判失败而非无限死等
const BOUNDED_TIMEOUT: Duration = Duration::from_secs(30);

/// 错误前注入记录数：坏条目与 FLUSH 同批（扫描批次 256 条，300 > 256 使
/// 坏条目与紧随的 FLUSH 落于第二批），复刻票面「坏记录在前 FLUSHDB 在后
/// 同批」冻结形态
const PRE_POISON_RECORDS: usize = 300;

/// FLUSH 后续写记录数：页内 FLUSH 之后仍有自有条目（超时续跑臂的真实形态）
const POST_FLUSH_RECORDS: usize = 60;

/// 物理键编码（条目 key 统一为 wkv 物理键：[NsVarint][DbVarint][KeyTag][用户键]）
fn physical(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// upsert 条目入队（带 input 组件，对标生产 SET 写日志形状）
fn enqueue_set(
  log: &GarnetLog,
  version: i64,
  session_id: i32,
  key: &[u8],
  value: &[u8],
) -> waof::Result<()> {
  let input = replay_input_bytes(RespCommand::Set, vec![key.to_vec(), value.to_vec()]);
  log.enqueue(&RecordShape {
    op_type: AofEntryType::StoreUpsert,
    version,
    session_id,
    key: &physical(key),
    value,
    input: &input,
    database_id: 0,
  })?;
  Ok(())
}

/// 向量族毒条目入队：StoreRMW + Vadd 形态，重放臂在向量域未接线时显式上抛
///（Vector Set (preview) commands are not enabled）。条目有键 → 按归属哈希
/// 仅归属 worker 应用时报错，非归属 worker 归属判定失败 `continue` 跳过——
/// 恰好构造「恰一 worker 于 FLUSH 前缺席」的栅栏缺员形态
fn enqueue_vadd_poison(log: &GarnetLog, key: &[u8]) -> waof::Result<()> {
  let input = replay_input_bytes(RespCommand::Vadd, vec![]);
  log.enqueue(&RecordShape {
    op_type: AofEntryType::StoreRMW,
    version: 5,
    session_id: 7,
    key: &physical(key),
    value: b"",
    input: &input,
    database_id: 0,
  })?;
  Ok(())
}

/// 装配含「坏条目在前 FLUSH 在后同批」的 AOF（单物理日志；
/// `replay_task_count` 决定恢复走并行/单任务路径）
async fn build_aof(
  case: &str,
  replay_task_count: i32,
  poison: bool,
  flush: bool,
) -> waof::Result<Arc<GarnetAppendOnlyFile>> {
  let (_dirs, backends) = wnode_test::test_sublogs_with_config(case, 1, WalConfig::default());
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: 1,
    aof_replay_task_count: replay_task_count,
    ..RuntimeServerOptions::default()
  };
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  ));
  let log = aof.log();

  for i in 0..PRE_POISON_RECORDS {
    enqueue_set(log, 5, 7, format!("pre-{i}").as_bytes(), b"v")?;
  }
  if poison {
    enqueue_vadd_poison(log, b"poison-vadd")?;
  }
  if flush {
    // 无键 FLUSH 族条目：全员自有（can_replay 无键 Basic 全任务处理），
    // 经 LeaderBarrier 跨任务会合——毒条目归属 worker 恰在此栅栏缺席
    log.enqueue_safe_flush_aof(AofEntryType::FlushAll, false, 0, 0)?;
  }
  for i in 0..POST_FLUSH_RECORDS {
    enqueue_set(log, 5, 7, format!("post-{i}").as_bytes(), b"v")?;
  }

  log.commit_async().await;
  Ok(aof)
}

/// 同批「毒条目在前 FLUSH 在后」并行恢复（replay_task_count=2）：恢复须在
/// 有界时间返回 Err 并携带毒条目身份，而非冻结于 LeaderBarrier（修复前等待
/// worker 恒 None 无限等待、页内 completed 闸永无全员会合）
#[compio::test]
async fn poison_before_flush_same_batch_parallel_recover_bounded_error() -> aok::Result<()> {
  let aof = build_aof("para_barrier_poison", 2, true, true).await?;

  let (_dir, store) = open_test_store_with_budget("para-barrier-poison.db", 64 << 20)?;
  let session = store.new_session()?;
  let storage = StorageSession::new(session.enter_batch());
  store.set_current_version(5);
  let target = ReplayTarget {
    session: &storage,
    store: Arc::clone(&store),
    aof_floor: vec![],
  };
  let processor = AofProcessor::new(Arc::clone(&aof));

  let recover = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target);
  let inner = timeout(BOUNDED_TIMEOUT, recover)
    .await
    .expect("恢复不得冻结于 LeaderBarrier（有界时限外仍无返回）");
  let err = inner.expect_err("毒条目必须使恢复以 Err 收敛");
  let msg = err.to_string();
  assert!(
    msg.contains("Vector Set"),
    "错误须携带毒条目（向量域未接线）身份透出，实得: {msg}"
  );
  Ok(())
}

/// 同构健康批（无毒条目，FLUSH 与全员对齐）并行恢复对照臂：栅栏正常会合
/// 一次收敛，FLUSH 前条目被清、FLUSH 后条目在场——超时续跑语义不误伤健康
/// 恢复（有界且 Ok）
#[compio::test]
async fn flush_same_batch_healthy_parallel_recover_bounded_ok() -> aok::Result<()> {
  let aof = build_aof("para_barrier_healthy", 2, false, true).await?;

  let (_dir, store) = open_test_store_with_budget("para-barrier-healthy.db", 64 << 20)?;
  let session = store.new_session()?;
  let storage = StorageSession::new(session.enter_batch());
  let target = ReplayTarget {
    session: &storage,
    store: Arc::clone(&store),
    aof_floor: vec![],
  };
  let processor = AofProcessor::new(Arc::clone(&aof));

  let recover = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target);
  let replayed = timeout(BOUNDED_TIMEOUT, recover)
    .await
    .expect("健康恢复不得冻结于 LeaderBarrier")
    .expect("健康批恢复必须 Ok");

  // FLUSH 前条目已被清空、FLUSH 后条目在场（FLUSH 独占执行恰一次）
  assert_eq!(
    session.read(b"pre-0").await?,
    None,
    "FLUSH 前条目必须被 FLUSH 清空"
  );
  assert_eq!(
    session.read(b"post-0").await?,
    Some(b"v".to_vec()),
    "FLUSH 后条目必须完整在场"
  );
  // 统计口径：键控条目仅归属 worker 计一次；无键 FLUSH 条目全员自有、
  // 按参与 worker 数计入（r42-replayarm 已裁的纯统计口径）
  let expect = (PRE_POISON_RECORDS + POST_FLUSH_RECORDS + 2) as u64;
  assert_eq!(replayed, expect, "健康批全量条目必须悉数重放");
  Ok(())
}
