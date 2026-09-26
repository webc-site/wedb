//! 向量/范围索引 AOF 重放错误链单源收敛测试（票 zcode-r135c 案二）
//!
//! 旧形态：重放族向 `AofReplayError::Replay(String)` 收敛时分派臂
//! （`aof_processor_store_ops.rs`）再套第二段 `format!("... failed: {e}")`
//! 双段 stringify，且 `vector_manager_replication.rs` 多处 `map_err(|_|)`
//! 丢弃因由。修复后：重放族内单源自足成文（含因由），分派臂直传 /
//! 经 `From<ReplicationError>` 归位既有 Store/Log 透明通道。
//! 全部走 AOF 恢复真实重放链（内存日志 + StoreRMW 条目入队 +
//! `AofRecover::single_log_recover`），无 mock。

use std::{io::Error, sync::Arc};

use waof::{AofEntryType, Error as WaofError};
use wbase::group_commit::Broken;
use wbftree::{Error as WbftreeError, RangeIndexManager};
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wkv::{CollectionError, Error as WkvError, RangeIndexError, WedbStore};
use wnode::{
  AofProcessor, GarnetAppendOnlyFile, GarnetLog, ReplayInput,
  aof::{
    aof_processor::{AofReplayError, ReplayTarget},
    garnet_log::RecordShape,
    recover::aof_recover::AofRecover,
    replay_input::ReplayInputSlice,
  },
  range_index::range_index_manager_replication::RangeIndexManagerReplication,
  resp::vector::{
    vector_manager::{
      VADD_APPEND_LOG_ARG, VREM_APPEND_LOG_ARG, VectorManager, VectorManagerOptions,
    },
    vector_store_callbacks::{
      ActiveDedicatedVectorSession, OwnedActiveVectorSession, WedbVectorStoreCallbacks,
    },
  },
  storage::session::storage_session::StorageSession,
};
use wresp::command::RespCommand;
use wtest_base::test_store_config;
use wval::{KeyTag, NamespaceDbCodec};
use wvector::Callbacks;

/// 独立测试库
fn open_store(tag: &str) -> (tempfile::TempDir, Arc<WedbStore<SegmentedDevice>>) {
  let dir = tempfile::tempdir().unwrap();
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join(format!("{tag}.db"))).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), Arc::clone(&device)).unwrap());
  (dir, store)
}

/// 内存 AOF（与 aof_store_rmw_replay.rs 同形态；重放面与磁盘拓扑同路径）
fn memory_aof(tag: &str) -> Arc<GarnetAppendOnlyFile> {
  let options = RuntimeServerOptions::default();
  Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(
      GarnetLog::new(
        &options,
        {
          let (_dirs, backends) = wnode_test::test_sublogs(tag, 1);
          backends
        },
        None,
      )
      .expect("构造 GarnetLog"),
    ),
    &options,
    None,
  ))
}

/// 向量管理器（生产装配同形态，见 vector_set_interrupt_delete_recovery.rs；
/// 本文件用例全走缺失/损坏早退臂，不触专用会话）
fn vector_manager(store: &Arc<WedbStore<SegmentedDevice>>) -> Arc<VectorManager> {
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new())),
  ));
  let s = Arc::clone(store);
  vm.attach_dedicated_session_factory(Arc::new(move || {
    s.new_session()
      .ok()
      .map(OwnedActiveVectorSession::new)
      .map(ActiveDedicatedVectorSession::from_bound)
  }));
  vm
}

/// 直写入队一条根域 StoreRMW 条目（cmd/arg1/args 由调用方指定）
fn enqueue_rmw(aof: &GarnetAppendOnlyFile, cmd: RespCommand, arg1: i64, args: &[&[u8]]) {
  let input = ReplayInputSlice::new_deterministic(cmd, args).with_args_num(arg1, 0, 0);
  ReplayInput::with_encoded_slices(&input, |serialized| {
    let _ = aof.log().enqueue(&RecordShape {
      op_type: AofEntryType::StoreRMW,
      version: 0,
      session_id: 0,
      key: NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, b"vk").as_slice(),
      value: &[],
      input: serialized,
      database_id: 0,
    });
  });
}

/// 单日志恢复收割首错（可选注入 RI 重放面；向量面经 aof 注入）
async fn recover_err(
  aof: &Arc<GarnetAppendOnlyFile>,
  store: &Arc<WedbStore<SegmentedDevice>>,
  ri: Option<Arc<RangeIndexManagerReplication>>,
) -> AofReplayError {
  let replay_session = store.new_session().unwrap();
  let batch = replay_session.enter_batch();
  let storage = StorageSession::new(batch);
  let mut processor = AofProcessor::new(Arc::clone(aof));
  if let Some(ri) = ri {
    processor.set_range_index_manager(ri);
  }
  let target = ReplayTarget::new(&storage, store);
  AofRecover::single_log_recover(&processor, aof, 0, 0, -1, &target)
    .await
    .expect_err("损坏/不可满足的重放条目必须使恢复显式失败")
}

/// VREM 重放遇缺失索引：单源成文直传——错误恰为族内文案，
/// 无分派臂第二段 stringify（旧形态 "Vector Set replay failed: ..." 包装绝迹）
#[compio::test]
async fn vrem_missing_index_error_is_single_source_text() {
  let (_dir, store) = open_store("vrem_single");
  let aof = memory_aof("vrem_single");
  let vm = vector_manager(&store);
  aof.set_vector_manager(vm);
  enqueue_rmw(&aof, RespCommand::Vrem, VREM_APPEND_LOG_ARG, &[b"el"]);
  aof.log().commit();

  let err = recover_err(&aof, &store, None).await;
  let AofReplayError::Replay(message) = &err else {
    panic!("缺失索引属重放语义失败，应落 Replay 变体: {err:?}");
  };
  assert_eq!(
    message, "Failed to read Vector Set index during AOF replay",
    "文案必须为族内单源原文，零包装漂移"
  );
  assert!(
    !err.to_string().contains("Vector Set replay failed"),
    "分派臂不得再套第二段 stringify: {err}"
  );
}

/// VADD 重放遇参数截断：corrupt 单源成文直传，同样无外裹文案
#[compio::test]
async fn vadd_truncated_args_error_is_single_source_text() {
  let (_dir, store) = open_store("vadd_corrupt");
  let aof = memory_aof("vadd_corrupt");
  let vm = vector_manager(&store);
  aof.set_vector_manager(vm);
  // 仅携 dims 一个 4B 参：reduce_dims 腿必缺 → corrupt("VADD")
  let dims = 2u32.to_le_bytes();
  enqueue_rmw(&aof, RespCommand::Vadd, VADD_APPEND_LOG_ARG, &[&dims]);
  aof.log().commit();

  let err = recover_err(&aof, &store, None).await;
  let AofReplayError::Replay(message) = &err else {
    panic!("参数截断属重放语义失败，应落 Replay 变体: {err:?}");
  };
  assert_eq!(
    message, "vector VADD replay input corrupted",
    "corrupt 文案必须为族内单源原文"
  );
}

/// RICREATE 重放遇损坏存根：`ReplicationError::Msg` 经
/// `From<ReplicationError>` 归位 Replay 单源直传（旧形态
/// "RangeIndex replay failed: ..." 包装绝迹）
#[compio::test]
async fn ricreate_corrupt_stub_error_is_single_source_text() {
  let dir = tempfile::tempdir().unwrap();
  let (_sdir, store) = open_store("ri_corrupt");
  let aof = memory_aof("ri_corrupt");
  let engine = Arc::new(
    RangeIndexManager::new(dir.path().join("ri"), dir.path().join("cpr"))
      .expect("构造 RangeIndexManager"),
  );
  let ri = Arc::new(RangeIndexManagerReplication::new(Arc::clone(&engine)));
  // 存根长度非 RANGE_INDEX_STUB_SIZE → "Corrupt RI.CREATE AOF entry: stub size ..."
  enqueue_rmw(&aof, RespCommand::Ricreate, 0, &[b"short"]);
  aof.log().commit();

  let err = recover_err(&aof, &store, Some(ri)).await;
  let AofReplayError::Replay(message) = &err else {
    panic!("损坏存根无类型源，应落 Replay 变体: {err:?}");
  };
  assert!(
    message.contains("Corrupt RI.CREATE AOF entry"),
    "Msg 腿文案应原样归位: {message}"
  );
  assert!(
    !err.to_string().contains("RangeIndex replay failed"),
    "分派臂不得再套第二段 stringify: {err}"
  );
}

/// `From<ReplicationError>` 逐变体归位锁：类型化腿进既有 Store/Log
/// 透明通道（因由链完整），Msg 腿留 Replay
#[test]
fn replication_error_maps_onto_replay_transparent_channels() {
  use wnode::range_index::range_index_manager_replication::ReplicationError as R;

  let e: AofReplayError = R::Msg("boom-msg".into()).into();
  assert!(
    matches!(&e, AofReplayError::Replay(m) if m == "boom-msg"),
    "Msg 腿应直落 Replay: {e:?}"
  );

  let e: AofReplayError = R::Io(Error::other("boom-io")).into();
  assert!(
    matches!(&e, AofReplayError::Store(WkvError::Io(_))),
    "Io 腿应归位 Store(Io) 透明通道: {e:?}"
  );
  assert!(
    e.to_string().contains("boom-io"),
    "transparent 链必须保全因由: {e}"
  );

  let e: AofReplayError = R::BfTree(WbftreeError::IndexExists).into();
  assert!(
    matches!(
      &e,
      AofReplayError::Store(WkvError::Collection(CollectionError::Tree(
        WbftreeError::IndexExists
      )))
    ),
    "BfTree 腿应归位 Store(Collection(Tree)) 通道: {e:?}"
  );

  let e: AofReplayError = R::RangeIndex(RangeIndexError::AlreadyExists).into();
  assert!(
    matches!(
      &e,
      AofReplayError::Store(WkvError::RangeIndex(RangeIndexError::AlreadyExists))
    ),
    "RangeIndex 腿应归位 Store(RangeIndex) 通道: {e:?}"
  );

  let e: AofReplayError = R::Aof(WaofError::PipelineBroken(Broken)).into();
  assert!(
    matches!(&e, AofReplayError::Log(WaofError::PipelineBroken(_))),
    "Aof 腿应归位 Log 透明通道: {e:?}"
  );
}
