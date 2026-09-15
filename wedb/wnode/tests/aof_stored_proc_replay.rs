//! AOF 集成回放测试（自 src/aof/aof_processor.rs 内嵌测试迁出）
//!
//! compio Runtime + 临时目录真存储全链回放：入队 → recover 扫描 → 处理器
//! 分发 → 存储过程工厂重建 / 目标库清理。
//! 对标 C# libs/server/AOF/AofProcessor.cs：ReplayStoredProc →
//! StoredProcRunnerBase → RunCustomTxnProcAtReplica，与 ReplayAOF case
//! FlushDb → storeWrapper.FlushDatabase(unsafeTruncateLog, dbId)。

use std::{
  env::temp_dir,
  fs::{create_dir_all, remove_dir_all},
  path::Path,
  process::id,
  sync::Arc,
};

use aok::{OK, Void};
use parking_lot::RwLock;
use wbase::entry_type::AofEntryType;
use wconf::RuntimeServerOptions;
use wcustom::{CustomCommandManager, CustomTxnProc, LAST_SET_KV, SetTxnProc};
use wdev::SegmentedDevice;
use wedb_test::test_store_config;
use wkv::WedbStore;
use wnode::{
  aof::{
    aof_processor::{AofProcessor, ReplayTarget},
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::{GarnetLog, InMemorySublog},
    recover::aof_recover::AofRecover,
    replaycoordinator::stored_proc_replay::{StoredProcRegistryReplayer, stored_proc_args},
    sublog::Sublog,
  },
  storage::StorageSession,
};
use wtxn::{SublogAccess, WatchVersionMap};

/// 内存 AOF（单物理日志拓扑；回放形状断言用）
fn memory_aof() -> Arc<GarnetAppendOnlyFile> {
  let options = RuntimeServerOptions::default();
  let backends: Vec<Arc<Sublog>> = vec![Arc::new(Sublog::Mem(InMemorySublog::new()))];
  Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None)),
    &options,
    None,
  ))
}

/// 临时目录小容量真存储（收尾自清；小预算配置对标 C# 16MB 基线）
fn open_store(dir: &Path) -> aok::Result<Arc<WedbStore<SegmentedDevice>>> {
  let device = Arc::new(SegmentedDevice::single_file(dir.join("data.db"))?);
  Ok(Arc::new(WedbStore::open(test_store_config(), device)?))
}

/// AOF 存储过程条目端到端回放：入队 → recover 扫描 → 处理器分发 →
/// 注册表工厂重建过程执行全链闭环
///（对标 C# ReplayStoredProc → StoredProcRunnerBase → RunCustomTxnProcAtReplica）。
#[test]
fn stored_proc_entry_replays_via_recover() -> Void {
  use compio::runtime::Runtime;

  let rt = Runtime::new()?;
  rt.block_on(async {
    *LAST_SET_KV.lock() = None;
    fn set_proc_factory() -> CustomTxnProc {
      CustomTxnProc::Set(SetTxnProc::default())
    }

    let mut manager = CustomCommandManager::new();
    let proc_id = manager
      .register_transaction("SETX", Some(set_proc_factory), None, None)
      .unwrap();

    let aof = memory_aof();
    let log = aof.log();

    // 入队存储过程条目（载荷 = 过程参数序列）
    let mut body = Vec::new();
    stored_proc_args::encode(&[b"pk".to_vec(), b"pv".to_vec()], &mut body);
    log.enqueue_stored_proc(
      AofEntryType::StoredProcedure,
      5,
      7,
      proc_id,
      &body,
      &SublogAccess::default(),
    );
    log.commit();

    // 处理器装配注册表执行面并恢复回放
    let mut processor = AofProcessor::new(Arc::clone(&aof));
    processor.set_stored_proc_replayer(Arc::new(StoredProcRegistryReplayer::new(Arc::new(
      RwLock::new(manager),
    ))));

    let dir = temp_dir().join(format!("wnode-proc-replay-{}", id()));
    create_dir_all(&dir)?;
    let store = open_store(&dir)?;
    let session = store.new_session()?;
    let storage = StorageSession::new(session.enter_batch(), Arc::new(WatchVersionMap::new(16)));
    let target = ReplayTarget::new(&storage, &store);

    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(replayed, 1, "存储过程条目应被重放");

    // 过程执行效果落库（is_recovering 路径的最终状态）
    let recorded = LAST_SET_KV.lock().clone().unwrap();
    assert_eq!(recorded.0, b"pk");
    assert_eq!(recorded.1, b"pv");
    let _ = remove_dir_all(&dir);
    OK
  })
}

/// FLUSHDB 条目回放单库边界：多库写入 → 回放 databaseId=0 的 FLUSHDB 条目 →
/// 仅库 0 被清空，库 1 数据完好
/// （对标 C# libs/server/AOF/AofProcessor.cs:ReplayAOF case FlushDb →
/// storeWrapper.FlushDatabase(unsafeTruncateLog, dbId: header.databaseId)）
#[test]
fn flush_db_entry_replays_targeted_database() -> Void {
  use compio::runtime::Runtime;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let aof = memory_aof();
    let log = aof.log();

    // 多库写入：库 0 与库 1 各一键（直写存储面，不经 AOF）
    let dir = temp_dir().join(format!("wnode-flushdb-replay-{}", id()));
    create_dir_all(&dir)?;
    let store = open_store(&dir)?;
    let db0 = store.new_session()?;
    db0.set_context(0, 0);
    db0.upsert(b"db0key", b"v0").await?;
    let db1 = store.new_session()?;
    db1.set_context(0, 1);
    db1.upsert(b"db1key", b"v1").await?;

    // 入队 FLUSHDB 条目（databaseId = 0）
    log.enqueue_safe_flush_aof(AofEntryType::FlushDb, false, 0);
    log.commit();

    // 处理器装配与恢复回放
    let processor = AofProcessor::new(Arc::clone(&aof));
    let session = store.new_session()?;
    let storage = StorageSession::new(session.enter_batch(), Arc::new(WatchVersionMap::new(16)));
    let target = ReplayTarget::new(&storage, &store);

    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(replayed, 1, "FLUSHDB 条目应被重放");

    // 仅 databaseId 指定库被清
    db0.set_context(0, 0);
    assert_eq!(db0.read(b"db0key").await?, None, "库 0 应被清空");
    db1.set_context(0, 1);
    assert_eq!(
      db1.read(b"db1key").await?,
      Some(b"v1".to_vec()),
      "库 1 数据必须完好"
    );

    let _ = remove_dir_all(&dir);
    OK
  })
}
