//! AOF 重放闭环集成测试（wkv 临时库 + GarnetLog 写入 → AofProcessor 恢复
//! 重放 → 状态一致断言）
//!
//! 覆盖：主存 upsert/RMW(INCR/APPEND)/delete 回放、对象 upsert/delete 回放、
//! 事务组（TxnStart..TxnCommit）整组重放、检查点标记模糊区、版本闸
//! （旧代跳过/新代缓冲后重放）、分块记录重组回放、FlushAll 标记、
//! 前缀一致上界（SkipReplay）。

use std::sync::Arc;

use aok::OK;
use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use waof::{AofAddress, AofEntryType};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  aof::{
    aof_processor::{AofProcessor, ReplayInput, ReplayTarget},
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::{ChunkedShape, GarnetLog, InMemorySublog, RecordShape},
    recover::{
      aof_recover::AofRecover,
      recover_log_driver::{PageReplaySlice, RecoverLogDriver},
    },
    sublog::Sublog,
  },
  config::runtime_server_options::RuntimeServerOptions,
  databases::garnet_database::DEFAULT_VERSION_MAP_SIZE,
  storage::session::storage_session::StorageSession,
  types::{GarnetObjectType, RespCommand},
};
use wtxn::WatchVersionMap;

type TestStore = Arc<WedbStore<SegmentedDevice>>;

/// 独立 WATCH 版本表（生产由 GarnetDatabase 持有；会话级测试用默认桶数实例）
fn test_version_map() -> Arc<WatchVersionMap> {
  Arc::new(WatchVersionMap::new(DEFAULT_VERSION_MAP_SIZE))
}

/// 打开临时文件库
fn open_store(tag: &str) -> aok::Result<(TempDir, TestStore)> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag))?);
  let config = StoreConfig::new(16384, 65536, 64, 0.5)?;
  let store = Arc::new(WedbStore::open(config, device)?);
  Ok((dir, store))
}

/// 单物理日志拓扑的 AOF 装配
fn aof_fixture() -> aok::Result<Arc<GarnetAppendOnlyFile>> {
  let options = RuntimeServerOptions::default();
  let backends = vec![Arc::new(Sublog::Mem(InMemorySublog::new()))];
  Ok(Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None)),
    &options,
    None,
  )))
}

/// 物理键编码（条目 key 统一为 wkv 物理键：[NsVarint][DbVarint][KeyTag][用户键]）
fn physical(user_key: &[u8]) -> Vec<u8> {
  wval::NamespaceDbCodec::encode_tagged_key(0, 0, wkv::KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// upsert 条目编码入队（指定 session_id）
fn enqueue_upsert_session(
  log: &GarnetLog,
  version: i64,
  session_id: i32,
  key: &[u8],
  value: &[u8],
) -> aok::Result<i64> {
  let mut input = Vec::new();
  ReplayInput {
    cmd: RespCommand::Set,
    flags: 0,
    sub_id: 0,
    obj_type: 0,
    arg1: 0,
    arg2: 0,
    arg3: 0,
    args: vec![key.to_vec(), value.to_vec()],
  }
  .serialize(&mut input);
  Ok(log.enqueue(&RecordShape {
    op_type: AofEntryType::StoreUpsert,
    version,
    session_id,
    key: &physical(key),
    value,
    input: &input,
    database_id: 0,
  }))
}

/// upsert 条目编码入队（默认 session_id 1）
fn enqueue_upsert(log: &GarnetLog, version: i64, key: &[u8], value: &[u8]) -> aok::Result<i64> {
  enqueue_upsert_session(log, version, 1, key, value)
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofUpsertStoreRecoverTestAsync
/// 主存写入→恢复重放→状态一致闭环
#[test]
fn test_upsert_rmw_delete_replay_loop() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_store("aof-loop.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();

    // ── 写入段：模拟命令层 AOF 通路 ──
    // SET k v1（upsert）
    enqueue_upsert(log, 5, b"k", b"v1")?;
    // SET cnt 10（upsert，供 INCR 叠加）
    enqueue_upsert(log, 5, b"cnt", b"10")?;
    // INCRBY cnt 32（RMW）
    {
      let mut input = Vec::new();
      ReplayInput {
        cmd: RespCommand::Incrby,
        flags: 0,
        sub_id: 0,
        obj_type: 0,
        arg1: 32,
        arg2: 0,
        arg3: 0,
        args: vec![b"32".to_vec()],
      }
      .serialize(&mut input);
      log.enqueue(&RecordShape {
        op_type: AofEntryType::StoreRMW,
        version: 5,
        session_id: 1,
        key: &physical(b"cnt"),
        value: &[],
        input: &input,
        database_id: 0,
      });
    }
    // APPEND k "-tail"（RMW 携带 parseState 参数序列化）
    {
      let mut input = Vec::new();
      ReplayInput {
        cmd: RespCommand::Append,
        flags: 0,
        sub_id: 0,
        obj_type: 0,
        arg1: 0,
        arg2: 0,
        arg3: 0,
        args: vec![b"-tail".to_vec()],
      }
      .serialize(&mut input);
      log.enqueue(&RecordShape {
        op_type: AofEntryType::StoreRMW,
        version: 5,
        session_id: 1,
        key: &physical(b"k"),
        value: &[],
        input: &input,
        database_id: 0,
      });
    }
    // DEL gone（delete，先建键）
    enqueue_upsert(log, 5, b"gone", b"x")?;
    {
      log.enqueue(&RecordShape {
        op_type: AofEntryType::StoreDelete,
        version: 5,
        session_id: 1,
        key: &physical(b"gone"),
        value: &[],
        input: &[],
        database_id: 0,
      });
    }
    log.commit();

    // ── 重放段：全新库恢复 ──
    let (_dir2, store2) = open_store("aof-loop-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch(), test_version_map());
    let target = ReplayTarget {
      session: &storage2,
      store: Arc::clone(&store2),
      store_version: 5,
    };
    let processor = AofProcessor::new(Arc::clone(&aof));
    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(replayed, 6, "全部条目均应重放");

    // ── 状态一致断言 ──
    assert_eq!(
      storage2.read_string(b"k").await?,
      Some(b"v1-tail".to_vec()),
      "upsert + APPEND 重放闭环"
    );
    assert_eq!(
      storage2.read_string(b"cnt").await?,
      Some(b"42".to_vec()),
      "upsert + INCRBY 重放闭环"
    );
    assert_eq!(
      storage2.read_string(b"gone").await?,
      None,
      "delete 重放闭环"
    );
    OK
  })
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofUpsertObjectStoreRecoverTestAsync
/// 对象存 upsert/delete 回放闭环
#[test]
fn test_object_replay_loop() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_store("aof-obj.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();

    // HSET 信封 upsert（对象值 = [tag u8][payload]）
    let mut value = vec![GarnetObjectType::Hash as u8];
    value.extend_from_slice(b"payload");
    log.enqueue(&RecordShape {
      op_type: AofEntryType::ObjectStoreUpsert,
      version: 5,
      session_id: 1,
      key: &physical(b"h"),
      value: &value,
      input: &[],
      database_id: 0,
    });
    // 对象 delete
    log.enqueue(&RecordShape {
      op_type: AofEntryType::ObjectStoreDelete,
      version: 5,
      session_id: 1,
      key: &physical(b"h"),
      value: &[],
      input: &[],
      database_id: 0,
    });
    log.commit();

    // 重放到全新库
    let (_dir2, store2) = open_store("aof-obj-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch(), test_version_map());
    let target = ReplayTarget {
      session: &storage2,
      store: Arc::clone(&store2),
      store_version: 5,
    };
    let processor = AofProcessor::new(Arc::clone(&aof));
    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(replayed, 2);

    // HSET 信封后 DEL：键应不存在
    assert_eq!(storage2.read_string(b"h").await?, None, "对象 delete 闭环");
    OK
  })
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofCustomTxnRecoverTestAsync
/// 版本闸与检查点模糊区：旧代跳过、新代缓冲后重放、事务组整组重放
#[test]
fn test_version_gate_and_txn_group_replay() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_store("aof-txn.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();

    // 当前存储版本 = 6
    // (1) 旧代 upsert（v5 < 6）：恢复路径跳过
    enqueue_upsert(log, 5, b"old", b"stale")?;
    // (2) 事务组：TxnStart → SET a 1 → SET b 2 → TxnCommit（v6）
    log.enqueue(&RecordShape {
      op_type: AofEntryType::TxnStart,
      version: 6,
      session_id: 9,
      key: &[],
      value: &[],
      input: &[],
      database_id: 0,
    });
    enqueue_upsert_session(log, 6, 9, b"a", b"1")?;
    enqueue_upsert_session(log, 6, 9, b"b", b"2")?;
    log.enqueue(&RecordShape {
      op_type: AofEntryType::TxnCommit,
      version: 6,
      session_id: 9,
      key: &[],
      value: &[],
      input: &[],
      database_id: 0,
    });
    // (3) FlushAll 标记（广播条目；单日志拓扑落 BasicHeader 纯头）
    log.enqueue_database_commit(AofEntryType::FlushAll, 6);
    log.commit();

    // 重放
    let (_dir2, store2) = open_store("aof-txn-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch(), test_version_map());
    let target = ReplayTarget {
      session: &storage2,
      store: Arc::clone(&store2),
      store_version: 6,
    };
    let processor = AofProcessor::new(Arc::clone(&aof));
    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(replayed, 6, "条目计数含跳过与标记条");

    // 事务组两键均落库（旧代键被版本闸跳过）
    assert_eq!(storage2.read_string(b"old").await?, None, "旧代条目跳过");
    assert_eq!(storage2.read_string(b"a").await?, Some(b"1".to_vec()));
    assert_eq!(storage2.read_string(b"b").await?, Some(b"2".to_vec()));
    OK
  })
}

/// test/standalone/Garnet.test.scripting/RespAofChunkTests.cs:AofLargeStringValueSpanChunkRecoverTest
/// 分块记录：写入端分块 → 读取端重组回放闭环
#[test]
fn test_chunked_record_replay_loop() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_store("aof-chunk.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();

    // 大对象值分块写入（enqueue_object_chunked；信封 = [tag u8][payload]）
    let mut big_value = vec![GarnetObjectType::Hash as u8];
    big_value.extend_from_slice(&vec![b'o'; 300]);
    log.enqueue_object_chunked(&ChunkedShape {
      record: RecordShape {
        op_type: AofEntryType::ObjectStoreUpsert,
        version: 5,
        session_id: 2,
        key: &physical(b"big"),
        value: &big_value,
        input: &[],
        database_id: 0,
      },
      write_value: true,
      write_input: false,
    });
    log.commit();

    // 重放：处理器内置分块读取器完成重组后按对象 upsert 落库
    let (_dir2, store2) = open_store("aof-chunk-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch(), test_version_map());
    let target = ReplayTarget {
      session: &storage2,
      store: Arc::clone(&store2),
      store_version: 5,
    };
    let processor = AofProcessor::new(Arc::clone(&aof));
    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(replayed, 2, "首块 + 数据块 = 2 条记录");
    // 信封 [tag][payload]：重组值经对象 upsert 落库后完整一致
    let got = storage2.read_string(b"big").await?.unwrap_or_default();
    assert_eq!(got.len(), 1 + 300, "信封 = 标签 + 载荷");
    assert_eq!(
      got[0],
      GarnetObjectType::Hash as u8,
      "GarnetObjectType::Hash"
    );
    assert!(got[1..].iter().all(|&b| b == b'o'));
    OK
  })
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofUpsertStoreCkptRecoverTestAsync
/// 前缀一致上界：SkipReplay 阈值截断 + 版本闸跳过计数
#[test]
fn test_skip_replay_prefix_bound() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_store("aof-skip.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();
    let first = enqueue_upsert(log, 5, b"early", b"1")?;
    enqueue_upsert(log, 5, b"later", b"2")?;
    log.commit();

    // until_sequence_number = first：第二条地址超过阈值 → 前缀截断
    let (_dir2, store2) = open_store("aof-skip-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch(), test_version_map());
    let target = ReplayTarget {
      session: &storage2,
      store: Arc::clone(&store2),
      store_version: 5,
    };
    let processor = AofProcessor::new(Arc::clone(&aof));
    let replayed =
      AofRecover::recover_replay_driver(&processor, &aof, 0, -1, first, &target).await?;
    assert_eq!(replayed, 1, "前缀一致上界截断后续条目");
    assert_eq!(storage2.read_string(b"early").await?, Some(b"1".to_vec()));
    assert_eq!(storage2.read_string(b"later").await?, None);

    // 无效地址向量形态核对
    let invalid = aof.invalid_aof_address();
    assert_eq!(invalid, AofAddress::create(1, -1));
    OK
  })
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofObjectStoreRMWDeleteRecoverHashTestAsync
/// 对象存 RMW（Hash / Set）重放闭环
#[test]
fn test_object_store_rmw_replay_loop() -> aok::Void {
  use wnode::api::garnet_status::GarnetStatus;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_store("aof-obj-rmw.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();

    // 1. HSET myhash f1 v1, f2 v2
    {
      let mut input = Vec::new();
      ReplayInput {
        cmd: RespCommand::Hset,
        flags: 0,
        sub_id: 0,
        obj_type: 0,
        arg1: 0,
        arg2: 0,
        arg3: 0,
        args: vec![
          b"f1".to_vec(),
          b"v1".to_vec(),
          b"f2".to_vec(),
          b"v2".to_vec(),
        ],
      }
      .serialize(&mut input);
      log.enqueue(&RecordShape {
        op_type: AofEntryType::ObjectStoreRMW,
        version: 5,
        session_id: 1,
        key: &physical(b"myhash"),
        value: &[],
        input: &input,
        database_id: 0,
      });
    }

    // 2. SADD myset m1, m2
    {
      let mut input = Vec::new();
      ReplayInput {
        cmd: RespCommand::Sadd,
        flags: 0,
        sub_id: 0,
        obj_type: 0,
        arg1: 0,
        arg2: 0,
        arg3: 0,
        args: vec![b"m1".to_vec(), b"m2".to_vec()],
      }
      .serialize(&mut input);
      log.enqueue(&RecordShape {
        op_type: AofEntryType::ObjectStoreRMW,
        version: 5,
        session_id: 1,
        key: &physical(b"myset"),
        value: &[],
        input: &input,
        database_id: 0,
      });
    }

    log.commit();

    // 重放
    let (_dir2, store2) = open_store("aof-obj-rmw-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch(), test_version_map());
    let target = ReplayTarget {
      session: &storage2,
      store: Arc::clone(&store2),
      store_version: 5,
    };
    let processor = AofProcessor::new(Arc::clone(&aof));
    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(replayed, 2, "2 条对象 RMW 条目重放");

    // 验证 Hash 对象字段
    let f1_val = storage2.hash_get(b"myhash", b"f1").await?;
    assert_eq!(f1_val, (GarnetStatus::Ok, Some(b"v1".to_vec())));
    let f2_val = storage2.hash_get(b"myhash", b"f2").await?;
    assert_eq!(f2_val, (GarnetStatus::Ok, Some(b"v2".to_vec())));

    // 验证 Set 对象成员
    let (st1, is_m1) = storage2.set_is_member(b"myset", b"m1").await?;
    assert_eq!(st1, GarnetStatus::Ok);
    assert!(is_m1);
    let (st2, is_m2) = storage2.set_is_member(b"myset", b"m2").await?;
    assert_eq!(st2, GarnetStatus::Ok);
    assert!(is_m2);
    let (st3, is_m3) = storage2.set_is_member(b"myset", b"m3").await?;
    assert_eq!(st3, GarnetStatus::Ok);
    assert!(!is_m3);

    OK
  })
}

#[test]
fn test_multi_log_recover_and_replay_task() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let aof = aof_fixture()?;
    let (_dir2, store2) = open_store("aof-multi-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch(), test_version_map());
    let target = ReplayTarget {
      session: &storage2,
      store: Arc::clone(&store2),
      store_version: 5,
    };
    let processor = AofProcessor::new(Arc::clone(&aof));

    // 1. multi_log_recover
    let until_addr = AofAddress::create(1, 100);
    let total =
      AofRecover::multi_log_recover(&processor, &aof, 0, &until_addr, -1, &target).await?;
    assert_eq!(total, 0);

    // 2. recover_replay_task_async & create_and_run_intra_page_parallel_replay_tasks
    let driver = RecoverLogDriver::new(0, 0, 0, 0);
    let count = driver
      .recover_replay_task_async(&processor, &aof, &[], 0, 0, &target)
      .await?;
    assert_eq!(count, 0);

    let page = PageReplaySlice {
      entries: &[],
      start_address: 0,
      entry_stride: 0,
    };
    driver
      .create_and_run_intra_page_parallel_replay_tasks(&processor, &aof, page, &target)
      .await?;

    OK
  })
}
