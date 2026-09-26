//! 并行恢复 worker 装配失败有界收敛回归（task/ing/zcode-r43-parrecover 发现三
//! 收场面对应：worker 线程任何静默死亡形态——panic / 装配失败——都不得使
//! leader 冻结于双闸栏，对标 C# RecoverReplayTask.cs RecoverReplayTaskAsync
//! try/catch(Exception) 全捕后 CancelAsync 的有界收敛）
//!
//! 回归点：worker 私有会话装配失败（wkv 纪元参与者表满，`new_session` 显式
//! 上抛）。修复前该面为 `.expect` panic——线程静默死亡，error_slot 不落、
//! is_cancelled 不置、闸到达计数缺员且 closed 无人置位，leader 冻结于 ready
//! 闸、join 吞 Err，启动装配段永挂且运维不可诊断；修复后装配失败走统一收场
//! 面：落错误槽 + 置取消位 + 关闸广播（closed 使全员闸等待即刻逃逸），leader
//! 依取消位截停发布，恢复有界返回 Err 且错误身份透出。

use std::{sync::Arc, time::Duration};

use compio::time::timeout;
use tempfile::tempdir;
use waof::{AofEntryType, WalConfig};
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wkv::WedbStore;
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
use wtest_base::test_store_config;
use wval::{KeyTag, NamespaceDbCodec};

/// 有界时限：装配失败即时收场（无栅栏等待），修复前（线程静默死亡、ready 闸
/// 缺员）在此确定性判失败而非无限死等
const BOUNDED_TIMEOUT: Duration = Duration::from_secs(30);

/// 记录数：跨批（扫描批次 256 条），保证错误面在批次发布窗口内被点亮
const RECORDS: usize = 600;

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

/// 小参与者容量库装配：`max_sessions` 收窄使 worker 私有会话装配必然失败
///（纪元参与者表满，wkv 显式上抛而非 panic）
fn open_tight_session_store(
  tag: &str,
) -> aok::Result<(tempfile::TempDir, Arc<WedbStore<SegmentedDevice>>)> {
  let dir = tempdir()?;
  let mut config = test_store_config();
  config.gc.enabled = false;
  // 目标会话 1 + 预占 2：恢复目标装配后全表占满，并行 worker new_session
  // 必然触及 ExceededMaxThreads 显式失败面
  config.max_sessions = 3;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{tag}.db")),
  )?);
  let store = Arc::new(WedbStore::open(config, device)?);
  Ok((dir, store))
}

/// worker 会话装配失败（纪元表满）并行恢复：恢复须在有界时间返回 Err 并携带
/// 装配失败身份，而非冻结于双闸栏（修复前 worker 线程 expect panic 静默死亡，
/// ready 闸缺员、closed 无人置位，leader 永挂且 join 吞 Err）
#[compio::test]
async fn worker_session_assembly_failure_parallel_recover_bounded_error() -> aok::Result<()> {
  // AOF：单物理日志 + 并行拓扑
  let (_dirs, backends) =
    wnode_test::test_sublogs_with_config("para_worker_assembly", 1, WalConfig::default());
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: 1,
    aof_replay_task_count: 2,
    ..RuntimeServerOptions::default()
  };
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  ));
  {
    let log = aof.log();
    for i in 0..RECORDS {
      enqueue_set(log, 5, 7, format!("k-{i}").as_bytes(), b"v")?;
    }
    log.commit_async().await;
  }

  // 紧参与者容量库：目标会话先占一位，预占会话耗尽余量 → 并行 worker
  // 私有会话装配必然显式失败
  let (_dir, store) = open_tight_session_store("para-worker-assembly")?;
  let target_session = store.new_session()?;
  let mut held = Vec::new();
  while let Ok(session) = store.new_session() {
    held.push(session);
  }
  assert!(!held.is_empty(), "预置：余量参与者槽位必须已被占满");
  let storage = StorageSession::new(target_session.enter_batch());
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
    .expect("恢复不得冻结于双闸栏（有界时限外仍无返回）");
  let err = inner.expect_err("worker 装配失败必须使恢复以 Err 收敛");
  let msg = err.to_string();
  assert!(
    msg.contains("最大参与者数量上限"),
    "错误须携带会话装配失败身份透出，实得: {msg}"
  );
  drop(held);
  Ok(())
}
