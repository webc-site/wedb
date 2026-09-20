//! AOF StoreRMW 重放面未知命令失败语义测试
//!
//! 对标 C# AofProcessor.cs:StoreRMW（input 直通 Tsavorite RMW 面）与
//! MainStore/RMWMethods.cs InPlaceUpdaterWorker default 尾部
//! `throw new GarnetException("Unsupported operation on input")`：
//! 未知 RMW 命令恢复显式失败（Err 沿 Recover 链上抛），杜绝静默吞没
//! 恢复数据。事务组路径对齐 C# ProcessTransactionGroupOperations 无
//! catch 语义——组内条目失败即传播。

use std::sync::Arc;

use compio::runtime::Runtime;
use waof::AofEntryType;
use wcol::RespInputFlags;
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  AofProcessor, GarnetAppendOnlyFile, GarnetLog, ReplayInput,
  aof::{
    aof_processor::ReplayTarget, garnet_log::RecordShape, recover::aof_recover::AofRecover,
    replay_input::ReplayInputSlice,
  },
  storage::session::storage_session::StorageSession,
};
use wresp::command::RespCommand;
use wtest_base::test_store_config;
use wval::{KeyTag, NamespaceDbCodec};

/// 内存 AOF（无盘拓扑；重放面与磁盘拓扑同路径）
fn memory_aof() -> Arc<GarnetAppendOnlyFile> {
  let options = RuntimeServerOptions::default();
  Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(
      GarnetLog::new(
        &options,
        {
          let (_dirs, backends) = wnode_test::test_sublogs("aof_rmw", 1);
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

/// 直写入队一条 StoreRMW 条目（cmd 由调用方指定）
fn enqueue_store_rmw(aof: &GarnetAppendOnlyFile, version: i64, cmd: RespCommand) {
  let no_args: [&[u8]; 0] = [];
  let input = ReplayInputSlice::new(cmd, &no_args).with_flags(RespInputFlags::DETERMINISTIC.bits());
  ReplayInput::with_encoded_slices(&input, |serialized| {
    let _ = aof.log().enqueue(&RecordShape {
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
  let _ = aof.log().enqueue(&RecordShape {
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
    let storage = StorageSession::new(batch);
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
    let storage = StorageSession::new(batch);
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

/// Expire/Pexpire 相对时长重臂已删：rust 写入端唯一 TTL 事件源 TtlWrite 恒以
/// Pexpireat + 主端线性化绝对毫秒（或 Persist）入 AOF（service.rs
/// on_aof_store_event），全仓无 Expire/Pexpire 条目写入方；C# AOF 亦不存在
/// 相对时长条目形态（主端 KeyAdminCommands.cs:423-432 统一线性化为绝对
/// ticks 打包 ExpirationWithOption.Word）。重放遇该形态即损坏/演化失配，
/// 必须显式失败——若按重放端 now_ticks 重算相对时长，将叠加复制延迟使
/// 从库 TTL 系统性偏短（死臂删除语义锚定，防复发）
#[test]
fn store_rmw_replay_expire_relative_form_fails_recover() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempfile::tempdir().unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("exp.db")).unwrap());
    let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
    let aof = memory_aof();

    // 相对形态条目（arg1=相对秒/毫秒）不在可入 AOF 的 RMW 命令集
    enqueue_store_rmw(&aof, 0, RespCommand::Expire);
    enqueue_store_rmw(&aof, 0, RespCommand::Pexpire);
    aof.log().commit();

    let replay_session = store.new_session().unwrap();
    let batch = replay_session.enter_batch();
    let storage = StorageSession::new(batch);
    let processor = AofProcessor::new(Arc::clone(&aof));
    let target = ReplayTarget::new(&storage, &store);
    let result = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await;
    let err = result.expect_err("Expire/Pexpire 相对形态条目应使恢复显式失败");
    assert!(
      err.to_string().contains("unsupported cmd"),
      "错误文案应指明未支持命令: {err}"
    );
  });
}

/// 写侧不可产的 StoreRMW 命令形态（零生产者死臂）逐条落 default 臂显式失败：
/// rust 侧 StoreRMW 条目生产者全集只有 service.rs:on_aof_store_event 的七个镜像臂
///（Pexpireat/Persist/Setwithetag/Riset/Ridel/Ricreate/Delifexpim）加 RI/Vector 族
/// 直写入队点；主存字符串 RMW 在命令端就地折成终值经 StoreUpsert 镜像
///（INCR 族/APPEND/SETRANGE/SETEX/PSETEX/GETDEL 皆然，EXPIRE 族恒绝对毫秒），
/// 故下面 11 形态条目一旦出现即损坏或写/放两端演化失配，必须显式失败：
/// - Expireat/Setex/Psetex 携带与写侧相反或不可产的 arg1 口径（详见
///   aof_processor.rs:store_rmw 的删除声明），静默执行会让从库 TTL 系统性偏短
/// - Getdel/Incr 族/Append/Setrange 的净效果由 StoreUpsert/StoreDelete 承载，
///   重放端不设第二套手写 RMW 臂（原 rmw_main_store 收口已删）
///
/// （死臂删除语义锚定，防复发）
#[test]
fn store_rmw_replay_zero_producer_forms_fail_recover() {
  for cmd in [
    RespCommand::Expireat,
    RespCommand::Getdel,
    RespCommand::Setex,
    RespCommand::Psetex,
    RespCommand::Incr,
    RespCommand::Incrby,
    RespCommand::Decr,
    RespCommand::Decrby,
    RespCommand::Incrbyfloat,
    RespCommand::Append,
    RespCommand::Setrange,
  ] {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let dir = tempfile::tempdir().unwrap();
      let device = Arc::new(SegmentedDevice::single_file(dir.path().join("zero.db")).unwrap());
      let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
      let aof = memory_aof();

      enqueue_store_rmw(&aof, 0, cmd);
      aof.log().commit();

      let replay_session = store.new_session().unwrap();
      let batch = replay_session.enter_batch();
      let storage = StorageSession::new(batch);
      let processor = AofProcessor::new(Arc::clone(&aof));
      let target = ReplayTarget::new(&storage, &store);
      let err = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target)
        .await
        .expect_err("零生产者 StoreRMW 形态应使恢复显式失败");
      assert!(
        err.to_string().contains("unsupported cmd"),
        "{cmd:?} 形态条目的错误文案应指明未支持命令: {err}"
      );
    });
  }
}
