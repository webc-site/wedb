//! AOF 重放闭环集成测试（wkv 临时库 + GarnetLog 写入 → AofProcessor 恢复
//! 重放 → 状态一致断言）
//!
//! 覆盖：主存 upsert/RMW(INCR/APPEND)/delete 回放、对象 upsert/delete 回放、
//! 事务组（TxnStart..TxnCommit）整组重放、事务头按 replay_task_access_vector
//! 位图的并行准入、检查点标记模糊区、版本闸
//! （旧代跳过/新代缓冲后重放）、分块记录重组回放、FlushAll 标记、
//! 前缀一致上界（SkipReplay）。

use std::sync::Arc;

use aok::OK;
use compio::runtime::Runtime;
use parking_lot::Mutex;
use wacl::{AccessControlList, AclParser, GarnetAclAuthenticator};
use waof::{AofAddress, AofEntryType, AofHeader, Error};
use wbase::store_type::REPLAY_TASK_ACCESS_VECTOR_BYTES;
use wcol::{
  hash::hash_object::{HashObject, HashOperation},
  object_payload::{GarnetObjectPayload, obj_decode},
  set::set_object::{SetObject, SetOperation},
  zset::sorted_set_object::{SortedSetObject, SortedSetOperation},
};
use wconf::RuntimeServerOptions;
use wnode::{
  aof::{
    aof_processor::{AofProcessor, ReplayTarget},
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::{GarnetLog, MIN_PARTIAL_ALLOC_SIZE, RecordShape},
    record_gate,
    recover::aof_recover::AofRecover,
    replay_input::ReplayInput,
  },
  resp::{
    acl_store::AclStore,
    garnet_api::StoreGarnetApi,
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  storage::session::storage_session::StorageSession,
};
use wnode_test::drain_output;
use wresp::command::RespCommand;
use wtest_base::open_test_store;
use wtxn::SublogAccess;
use wval::{GarnetObjectType, KeyTag, NamespaceDbCodec};

/// 单物理日志拓扑的 AOF 装配
fn aof_fixture() -> aok::Result<Arc<GarnetAppendOnlyFile>> {
  let options = RuntimeServerOptions::default();
  let backends = {
    let (_dirs, backends) = wnode_test::test_sublogs("aof_replay", 1);
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

/// ACL 用户记录编码（与主写入面 acl_commands.rs::network_acl_setuser 同一单点
/// 编码器 `AclParser::parse_acl_rule(..)?.to_bytes()`）：KeyTag::Acl 值域载荷
/// 一律为 bitcode 紧凑用户记录（存储唯一格式，无文本旁轨），AOF 条目镜像的
/// 即这份记录字节，回放臂与 AUTH 读面共用同一解码器 `User::from_rule_bytes`
fn acl_record(rule: &str) -> aok::Result<Vec<u8>> {
  Ok(AclParser::parse_acl_rule(rule)?.to_bytes())
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
  })?)
}

/// upsert 条目编码入队（默认 session_id 1）
fn enqueue_upsert(log: &GarnetLog, version: i64, key: &[u8], value: &[u8]) -> aok::Result<i64> {
  enqueue_upsert_session(log, version, 1, key, value)
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofUpsertStoreRecoverTestAsync
/// 主存写入→恢复重放→状态一致闭环（终值 StoreUpsert / StoreDelete 两形态）
#[test]
fn test_upsert_rmw_delete_replay_loop() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_test_store("aof-loop.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();

    // ── 写入段：命令层 AOF 通路的真实条目形态 ──
    // 主存字符串写全部落 StoreUpsert 终值 / StoreDelete 墓碑：APPEND / INCRBY 在
    // 命令端 read_user_sync + try_rmw_sync 就地算出终值，AOF 只镜像物理写效果
    //（写侧 StoreRMW 条目仅由 TTL / ETag / RI / Vector 旁路产出，见
    // service.rs:on_aof_store_event），条目序与主端 apply 序一致即收敛
    // SET k v1（upsert）
    enqueue_upsert(log, 5, b"k", b"v1")?;
    // APPEND k "-tail"（净效果 = 终值 StoreUpsert）
    enqueue_upsert(log, 5, b"k", b"v1-tail")?;
    // SET cnt 10（upsert）
    enqueue_upsert(log, 5, b"cnt", b"10")?;
    // INCRBY cnt 32（净效果 = 终值 StoreUpsert）
    enqueue_upsert(log, 5, b"cnt", b"42")?;
    // DEL gone（delete，先建键）
    enqueue_upsert(log, 5, b"gone", b"x")?;
    {
      let _ = log.enqueue(&RecordShape {
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
    let (_dir2, store2) = open_test_store("aof-loop-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch());
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
      "APPEND 终值条目重放闭环"
    );
    assert_eq!(
      storage2.read_string(b"cnt").await?,
      Some(b"42".to_vec()),
      "INCRBY 终值条目重放闭环"
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
    let (_dir, _store) = open_test_store("aof-obj.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();

    // HSET 信封 upsert（对象值 = [tag u8][payload]）
    let mut value = vec![GarnetObjectType::Hash as u8];
    value.extend_from_slice(b"payload");
    let _ = log.enqueue(&RecordShape {
      op_type: AofEntryType::ObjectStoreUpsert,
      version: 5,
      session_id: 1,
      key: &physical(b"h"),
      value: &value,
      input: &[],
      database_id: 0,
    });
    // 对象 delete
    let _ = log.enqueue(&RecordShape {
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
    let (_dir2, store2) = open_test_store("aof-obj-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch());
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
    let (_dir, _store) = open_test_store("aof-txn.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();

    // 当前存储版本 = 6
    // (0) FlushAll 条目（广播条目；单日志拓扑落 BasicHeader 纯头）：
    // 回放即全库用户域清空（C# FlushAllDatabases），置于事务组之前
    // 使后续事务写入不被清掉
    let _ = log.enqueue_database_commit(AofEntryType::FlushAll, 6);
    // (1) 旧代 upsert（v5 < 6）：恢复路径跳过
    enqueue_upsert(log, 5, b"old", b"stale")?;
    // (2) 事务组：TxnStart → SET a 1 → SET b 2 → TxnCommit（v6）
    let _ = log.enqueue(&RecordShape {
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
    let _ = log.enqueue(&RecordShape {
      op_type: AofEntryType::TxnCommit,
      version: 6,
      session_id: 9,
      key: &[],
      value: &[],
      input: &[],
      database_id: 0,
    });
    log.commit();

    // 重放
    let (_dir2, store2) = open_test_store("aof-txn-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch());
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
    let (_dir, _store) = open_test_store("aof-chunk.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();

    // 大对象值分块写入（统一入队口自动分块；信封 = [tag u8][payload]）
    let mut big_value = vec![GarnetObjectType::Hash as u8];
    big_value.extend_from_slice(&vec![b'o'; MIN_PARTIAL_ALLOC_SIZE as usize + 64]);
    let _ = log.enqueue(&RecordShape {
      op_type: AofEntryType::ObjectStoreUpsert,
      version: 5,
      session_id: 2,
      key: &physical(b"big"),
      value: &big_value,
      input: &[],
      database_id: 0,
    });
    log.commit();

    // 重放：处理器内置分块读取器完成重组后按对象 upsert 落库
    // （落库侧信封整包内联，whlog 单记录不跨页：128MB 预算推导 2MB 页，
    // 容纳 1MB 分块阈值值；对标 C# 测试 pageSize = targetBytes/128 的大页意图）
    let (_dir2, store2) =
      wtest_base::open_test_store_with_budget("aof-chunk-replay.db", 128 * 1024 * 1024)?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch());
    let target = ReplayTarget {
      session: &storage2,
      store: Arc::clone(&store2),
      store_version: 5,
    };
    let processor = AofProcessor::new(Arc::clone(&aof));
    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(replayed, 2, "首块 + 数据块 = 2 条记录");
    // 信封 [tag][payload]：重组值经对象 upsert 落 ObjectEnvelope 域后完整一致
    let got = storage2
      .read_tag_with(b"big", KeyTag::ObjectEnvelope, |v| v.to_vec())
      .await?
      .unwrap_or_default();
    assert_eq!(got.len(), big_value.len(), "信封 = 标签 + 载荷");
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
    let (_dir, _store) = open_test_store("aof-skip.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();
    let first = enqueue_upsert(log, 5, b"early", b"1")?;
    enqueue_upsert(log, 5, b"later", b"2")?;
    log.commit();

    // until_sequence_number = first：第二条地址超过阈值 → 前缀截断
    let (_dir2, store2) = open_test_store("aof-skip-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch());
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
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_test_store("aof-obj-rmw.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();

    // 1. HSET myhash f1 v1, f2 v2
    {
      let mut input = Vec::new();
      ReplayInput {
        cmd: RespCommand::Hset,
        flags: 0,
        sub_id: HashOperation::Hset as u8,
        obj_type: HashObject::OBJECT_TAG.as_u8(),
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
      let _ = log.enqueue(&RecordShape {
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
        sub_id: SetOperation::Sadd as u8,
        obj_type: SetObject::OBJECT_TAG.as_u8(),
        arg1: 0,
        arg2: 0,
        arg3: 0,
        args: vec![b"m1".to_vec(), b"m2".to_vec()],
      }
      .serialize(&mut input);
      let _ = log.enqueue(&RecordShape {
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
    let (_dir2, store2) = open_test_store("aof-obj-rmw-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch());
    let target = ReplayTarget {
      session: &storage2,
      store: Arc::clone(&store2),
      store_version: 5,
    };
    let processor = AofProcessor::new(Arc::clone(&aof));
    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(replayed, 2, "2 条对象 RMW 条目重放");

    // 验证 Hash 对象字段
    let hash_obj = storage2
      .read_tag_with(b"myhash", KeyTag::ObjectEnvelope, |raw| {
        obj_decode(raw, HashObject::OBJECT_TAG).and_then(HashObject::from_blob)
      })
      .await?
      .flatten()
      .expect("hash obj exists");
    assert_eq!(
      hash_obj.hash.get(b"f1".as_slice()).map(|v| v.as_slice()),
      Some(b"v1".as_slice())
    );
    assert_eq!(
      hash_obj.hash.get(b"f2".as_slice()).map(|v| v.as_slice()),
      Some(b"v2".as_slice())
    );

    // 验证 Set 对象成员
    let set_obj = storage2
      .read_tag_with(b"myset", KeyTag::ObjectEnvelope, |raw| {
        obj_decode(raw, SetObject::OBJECT_TAG).and_then(SetObject::from_blob)
      })
      .await?
      .flatten()
      .expect("set obj exists");
    assert!(set_obj.set.contains(b"m1".as_slice()));
    assert!(set_obj.set.contains(b"m2".as_slice()));
    assert!(!set_obj.set.contains(b"m3".as_slice()));

    OK
  })
}

/// 并行 AOF 页级双闸栏恢复集成测试（对标 C# CreateAndRunIntraPageParallelReplayTasks）
#[test]
fn test_parallel_aof_recover_loop() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_test_store("aof-parallel-src.db")?;

    let options = RuntimeServerOptions {
      aof_physical_sublog_count: 1,
      aof_replay_task_count: 4,
      ..RuntimeServerOptions::default()
    };
    let backends = {
      let (_dirs, backends) = wnode_test::test_sublogs("aof_replay", 1);
      backends
    };
    let aof = Arc::new(GarnetAppendOnlyFile::new(
      Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
      &options,
      None,
    ));
    let log = aof.log();

    // 写入 64 个不同 key
    for i in 0..64 {
      let k = format!("key_{i}");
      let v = format!("val_{i}");
      enqueue_upsert(log, 5, k.as_bytes(), v.as_bytes())?;
    }
    log.commit();

    // 重放到全新库（4 个 Worker 协程并行回放）
    let (_dir2, store2) = open_test_store("aof-parallel-dst.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch());
    let target = ReplayTarget {
      session: &storage2,
      store: Arc::clone(&store2),
      store_version: 5,
    };
    let processor = AofProcessor::new(Arc::clone(&aof));
    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(replayed, 64, "全部 64 条记录并行重放成功");

    // 状态断言
    for i in 0..64 {
      let k = format!("key_{i}");
      let v = format!("val_{i}");
      assert_eq!(
        storage2.read_string(k.as_bytes()).await?,
        Some(v.into_bytes()),
        "键 {k} 重放状态一致"
      );
    }

    OK
  })
}

/// 并行回放事务头准入回归（对标 C# AofProcessor.cs:CanReplay 的
/// SingleLogTransactionHeader / ShardedLogTransactionHeader 两支位图判定）：
/// 单物理日志 + `replay_task_count = 2` 形态下，事务标记与 FLUSH 广播条目携带
/// `replay_task_access_vector`，按位图归属分派；缺支时这两族条目在两个任务上
/// 均被静默丢弃（重放计数 0）。
#[test]
fn parallel_replay_admits_txn_header_entries_by_access_vector() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_test_store("aof-txn-header-src.db")?;
    let options = RuntimeServerOptions {
      aof_physical_sublog_count: 1,
      aof_replay_task_count: 2,
      ..RuntimeServerOptions::default()
    };
    let backends = {
      let (_dirs, backends) = wnode_test::test_sublogs("aof_txn_header", 1);
      backends
    };
    let aof = Arc::new(GarnetAppendOnlyFile::new(
      Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
      &options,
      None,
    ));
    let log = aof.log();

    // 事务标记仅任务 0 触达（位图 0b01、参与者 1，与 TransactionManager
    // :ComputeSublogAccessVector 的写侧口径一致）
    let mut vectors = [[0u8; REPLAY_TASK_ACCESS_VECTOR_BYTES]; 1];
    vectors[0][0] = 0b01;
    let access = SublogAccess {
      physical_vector: 0b1,
      virtual_vectors: &vectors,
      participant_count: 1,
    };
    log.enqueue_txn(AofEntryType::TxnStart, 5, 1, &access)?;
    log.enqueue_txn(AofEntryType::TxnCommit, 5, 1, &access)?;
    // FLUSHALL 广播条目写侧位图全置位 → 两任务各自重放（C# 广播语义）
    log.enqueue_database_commit(AofEntryType::FlushAll, 5)?;
    log.commit();

    let (_dir2, store2) = open_test_store("aof-txn-header-dst.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch());
    let target = ReplayTarget {
      session: &storage2,
      store: Arc::clone(&store2),
      store_version: 5,
    };
    let processor = AofProcessor::new(Arc::clone(&aof));
    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(
      replayed, 4,
      "TxnStart/TxnCommit 各恰一次 + FlushAll 两任务各一次"
    );

    // 未知头型不再等同「不归本任务」的静默跳过，而是上抛中止恢复
    let mut bogus = AofHeader::new();
    bogus.flags = 0b0111;
    assert!(matches!(
      record_gate::can_replay(&aof, &bogus.to_bytes(), 0, 8),
      Err(Error::UnsupportedReplayHeaderType(7))
    ));

    OK
  })
}

/// ObjectStoreRMW 对象增量（ZSet）重放闭环
#[test]
fn test_object_store_rmw_zset_replay_loop() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_test_store("aof-obj-rmw-zset.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();

    // 1. ZADD myzset 10.5 item1, 20.5 item2 via ObjectStoreRMW
    {
      let mut input = Vec::new();
      ReplayInput {
        cmd: RespCommand::Zadd,
        flags: 0,
        sub_id: SortedSetOperation::Zadd as u8,
        obj_type: SortedSetObject::OBJECT_TAG.as_u8(),
        arg1: 0,
        arg2: 0,
        arg3: 0,
        args: vec![
          b"10.5".to_vec(),
          b"item1".to_vec(),
          b"20.5".to_vec(),
          b"item2".to_vec(),
        ],
      }
      .serialize(&mut input);
      let _ = log.enqueue(&RecordShape {
        op_type: AofEntryType::ObjectStoreRMW,
        version: 5,
        session_id: 1,
        key: &physical(b"myzset"),
        value: &[],
        input: &input,
        database_id: 0,
      });
    }

    log.commit();

    // 重放
    let (_dir2, store2) = open_test_store("aof-obj-rmw-zset-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch());
    let target = ReplayTarget {
      session: &storage2,
      store: Arc::clone(&store2),
      store_version: 5,
    };
    let processor = AofProcessor::new(Arc::clone(&aof));
    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(replayed, 1, "1 条 ZSet ObjectStoreRMW 条目重放");

    // 验证 ZSet 成员
    let zset_obj = storage2
      .read_tag_with(b"myzset", KeyTag::ObjectEnvelope, |raw| {
        obj_decode(raw, SortedSetObject::OBJECT_TAG).and_then(SortedSetObject::from_blob)
      })
      .await?
      .flatten()
      .expect("zset obj exists");
    assert_eq!(
      zset_obj.sorted_set_dict.get(b"item1".as_slice()),
      Some(&10.5)
    );
    assert_eq!(
      zset_obj.sorted_set_dict.get(b"item2".as_slice()),
      Some(&20.5)
    );

    OK
  })
}

/// ACL 用户规则条目 AOF 回放闭环（KeyTag::Acl 旁路标签，主从复制链路）
///
/// 主库 `ACL SETUSER` / `ACL DELUSER` 产生的 StoreUpsert / StoreDelete 条目
/// 物理键标签为 0x0D，从库回放须落回 db 0 的 Acl 域而非字符串域（后者 SET
/// 语义会清 TTL/信封误伤旁路记录）。回放后按 `(ns, 0, Acl, user)` 点查可见。
#[test]
fn test_acl_replay_loop() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, _store) = open_test_store("aof-acl.db")?;
    let aof = aof_fixture()?;
    let log = aof.log();

    let alice_rule = acl_record(concat!(
      "user alice on ",
      "#8f0e2f76e22b43e2855189877e7dc1e1e7d98c226c95db247cd1d547928334a9",
      " +@all"
    ))?;
    let alice_key = NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::Acl, b"alice")
      .as_slice()
      .to_vec();
    let bob_key = NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::Acl, b"bob")
      .as_slice()
      .to_vec();
    let bob_rule = acl_record("user bob on +@all")?;

    // SETUSER alice（落盘）+ SETUSER bob 后 DELUSER bob（墓碑删除）
    let _ = log.enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 5,
      session_id: 1,
      key: &alice_key,
      value: &alice_rule,
      input: &[],
      database_id: 0,
    });
    let _ = log.enqueue(&RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 5,
      session_id: 1,
      key: &bob_key,
      value: &bob_rule,
      input: &[],
      database_id: 0,
    });
    let _ = log.enqueue(&RecordShape {
      op_type: AofEntryType::StoreDelete,
      version: 5,
      session_id: 1,
      key: &bob_key,
      value: &[],
      input: &[],
      database_id: 0,
    });
    log.commit();

    // 从库空库全量回放
    let (_dir2, store2) = open_test_store("aof-acl-replay.db")?;
    let session2 = store2.new_session()?;
    let storage2 = StorageSession::new(session2.enter_batch());
    let target = ReplayTarget {
      session: &storage2,
      store: Arc::clone(&store2),
      store_version: 5,
    };
    let processor = AofProcessor::new(Arc::clone(&aof));
    let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
    assert_eq!(replayed, 3, "3 条 ACL 条目均应重放");

    // 点查：alice 规则原样落 db 0 Acl 域；bob 已被墓碑删除
    let probe = store2.new_session()?;
    let acl_store = AclStore::new(&probe);
    assert_eq!(
      acl_store.read(0, b"alice")?,
      Some(alice_rule.to_vec()),
      "ACL upsert 回放后点查命中且值原样"
    );
    assert_eq!(
      acl_store.read(0, b"bob")?,
      None,
      "ACL delete 回放后点查落空"
    );

    // 端到端 AUTH：回放恢复的用户经会话存储点查认证成功
    let mut session = RespServerSession::new(
      1,
      RespServerSessionOptions {
        default_user: "default".into(),
        ..RespServerSessionOptions::default()
      },
    );
    session.attach_acl(Some(Arc::new(Mutex::new(GarnetAclAuthenticator::new(
      Arc::new(AccessControlList::new("")?),
    )))));
    session.set_garnet_api(Arc::new(StoreGarnetApi::new(store2.new_session()?)));
    session
      .recv_buffer
      .extend_from_slice(b"*3\r\n$4\r\nAUTH\r\n$5\r\nalice\r\n$8\r\npassw0rd\r\n");
    assert!(session.try_consume_messages().is_some());
    assert_eq!(
      drain_output(&mut session),
      b"+OK\r\n",
      "回放恢复的 alice 认证成功"
    );
    OK
  })
}
