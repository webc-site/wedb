//! AOF StoreRMW 重放面未知命令失败语义测试
//!
//! 对标 C# AofProcessor.cs:StoreRMW（input 直通 Tsavorite RMW 面）与
//! MainStore/RMWMethods.cs InPlaceUpdaterWorker default 尾部
//! `throw new GarnetException("Unsupported operation on input")`：
//! 未知 RMW 命令恢复显式失败（Err 沿 Recover 链上抛），杜绝静默吞没
//! 恢复数据。事务组路径对齐 C# ProcessTransactionGroupOperations 无
//! catch 语义——组内条目失败即传播。

use std::sync::{
  Arc,
  atomic::{AtomicI64, Ordering},
};

use compio::runtime::Runtime;
use wbase::entry_type::AofEntryType;
use wcol::RespInputFlags;
use wconf::RuntimeServerOptions;
use wdatabase::DEFAULT_VERSION_MAP_SIZE;
use wdev::SegmentedDevice;
use wedb_test::test_store_config;
use wkv::WedbStore;
use wnode::{
  AofProcessor, GarnetAppendOnlyFile, GarnetLog, InMemorySublog, ReplayInput, Sublog,
  aof::{
    aof_processor::{ReplayInputSlice, ReplayTarget},
    garnet_log::RecordShape,
    recover::aof_recover::AofRecover,
  },
  storage::session::storage_session::StorageSession,
};
use wresp::RespCommand;
use wtxn::WatchVersionMap;
use wval::{KeyTag, NamespaceDbCodec};

/// 内存 AOF（无盘拓扑；重放面与磁盘拓扑同路径）
fn memory_aof() -> Arc<GarnetAppendOnlyFile> {
  let options = RuntimeServerOptions::default();
  Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(
      &options,
      vec![Arc::new(Sublog::Mem(InMemorySublog::new()))],
      None,
    )),
    &options,
    None,
  ))
}

/// 直写入队一条 StoreRMW 条目（cmd 由调用方指定）
fn enqueue_store_rmw(aof: &GarnetAppendOnlyFile, version: i64, cmd: RespCommand) {
  let no_args: [&[u8]; 0] = [];
  let input = ReplayInputSlice::new(cmd, &no_args).with_flags(RespInputFlags::DETERMINISTIC.bits());
  ReplayInput::with_encoded_slices(&input, |serialized| {
    aof.log().enqueue(&RecordShape {
      op_type: AofEntryType::StoreRMW,
      version,
      session_id: 0,
      key: NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, b"k").as_slice(),
      value: &[],
      input: serialized,
      database_id: 0,
    });
  });
}

/// 入队事务边界条目（TxnStart / TxnCommit）
fn enqueue_txn_marker(aof: &GarnetAppendOnlyFile, version: i64, op_type: AofEntryType) {
  aof.log().enqueue(&RecordShape {
    op_type,
    version,
    session_id: 7,
    key: &[],
    value: &[],
    input: &[],
    database_id: 0,
  });
}

/// 未知 RMW 命令（合法命令但不在可入 AOF 的 RMW 命令集）单条恢复显式失败
#[test]
fn store_rmw_replay_unknown_cmd_fails_recover() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempfile::tempdir().unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("rmw.db")).unwrap());
    let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
    let aof = memory_aof();

    // GET 非写入端可入 AOF 的 RMW 命令集，出现即损坏/演化失配
    enqueue_store_rmw(&aof, 0, RespCommand::Get);
    aof.log().commit();

    let replay_session = store.new_session().unwrap();
    let batch = replay_session.enter_batch();
    let storage =
      StorageSession::new(batch, Arc::new(WatchVersionMap::new(DEFAULT_VERSION_MAP_SIZE)));
    let processor = AofProcessor::new(Arc::clone(&aof));
    let target = ReplayTarget::new(&storage, &store);
    let result = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await;
    let err = result.expect_err("未知 RMW 命令应使恢复显式失败");
    assert!(
      err.to_string().contains("unsupported cmd"),
      "错误文案应指明未支持命令: {err}"
    );
  });
}

/// 事务组内未知 RMW 命令：组重放失败即传播（C# 无 catch 语义）
#[test]
fn store_rmw_replay_unknown_cmd_in_txn_group_propagates() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempfile::tempdir().unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("txn.db")).unwrap());
    let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
    let aof = memory_aof();

    // 同 session_id 的 TxnStart → 未知 RMW 数据条目 → TxnCommit（非模糊区
    // 立即提交，组交 process_transaction_group_operations 重放）
    enqueue_txn_marker(&aof, 0, AofEntryType::TxnStart);
    enqueue_store_rmw(&aof, 0, RespCommand::Get);
    enqueue_txn_marker(&aof, 0, AofEntryType::TxnCommit);
    aof.log().commit();

    let replay_session = store.new_session().unwrap();
    let batch = replay_session.enter_batch();
    let storage =
      StorageSession::new(batch, Arc::new(WatchVersionMap::new(DEFAULT_VERSION_MAP_SIZE)));
    let processor = AofProcessor::new(Arc::clone(&aof));
    let target = ReplayTarget::new(&storage, &store);
    let result = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await;
    let err = result.expect_err("事务组内未知 RMW 命令应使恢复显式失败");
    assert!(
      err.to_string().contains("unsupported cmd"),
      "错误文案应指明未支持命令: {err}"
    );
  });
}
