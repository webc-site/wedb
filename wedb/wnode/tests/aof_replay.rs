//! AOF 重放闭环集成测试（wkv 临时库 + GarnetLog 写入 → AofProcessor 恢复
//! 重放 → 状态一致断言）
//!
//! 覆盖：主存 upsert/RMW(INCR/APPEND)/delete 回放、对象 upsert/delete 回放、
//! 事务组（TxnStart..TxnCommit）整组重放、事务头按 replay_task_access_vector
//! 位图的并行准入、检查点标记模糊区（含模糊区内提交事务组的清算整组重放）、
//! 版本闸
//! （旧代跳过/新代缓冲后重放）、副本重放检查点结束臂的本地打点与截断闭环、
//! 分块记录重组回放、FlushAll 标记、前缀一致上界（SkipReplay）。

use std::{fs::create_dir_all, sync::Arc};

use aok::OK;
use wacl::{AccessControlList, AclParser, GarnetAclAuthenticator};
use waof::{
  AofAddress, AofEntryType, AofHeader, AofHeaderType, Error, SequenceNumberGenerator, WalConfig,
};
use wbase::store_type::REPLAY_TASK_ACCESS_VECTOR_BYTES;
use wcol::{
  hash::hash_object::{HashObject, HashOperation},
  list::list_object::{ListObject, ListOperation},
  object_payload::{GarnetObjectPayload, obj_decode},
  set::set_object::{SetObject, SetOperation},
  zset::sorted_set_object::{SortedSetObject, SortedSetOperation},
};
use wconf::RuntimeServerOptions;
use wnode::{
  aof::{
    aof_processor::{AofProcessor, ReplayTarget, ReplicaCheckpointHook},
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::{GarnetLog, MIN_PARTIAL_ALLOC_SIZE, RecordShape},
    record_gate,
    recover::aof_recover::AofRecover,
    replay_input::ReplayInput,
  },
  database::{GarnetDatabase, SingleDatabaseManager},
  resp::{
    acl_store::AclStore,
    garnet_api::StoreGarnetApi,
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  storage::session::storage_session::StorageSession,
};
use wnode_test::{drain_output, replay_input_bytes};
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

/// 信封 RMW 条目物理键（ObjectStoreRMW 条目两类两域：信封 RMW 条目恒
/// ObjectEnvelope 域、与 service.rs ObjectRmw 镜像单点同构；重放端
/// object_store_rmw 先验物理键 tag 路由，String 域 ObjectStoreRMW 条目零
/// 生产者——标签路由收紧后此类形态留痕跳过，禁再用 String 域造该类条目）
fn physical_env(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::ObjectEnvelope, user_key)
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

/// upsert 条目编码入队（默认 session_id 1）
fn enqueue_upsert(log: &GarnetLog, version: i64, key: &[u8], value: &[u8]) -> aok::Result<i64> {
  enqueue_upsert_session(log, version, 1, key, value)
}

/// 无 input 条目编码入队（键值直传；错误随既有 `let _ =` 写法吞弃，语义不变）
fn enqueue_entry(
  log: &GarnetLog,
  op_type: AofEntryType,
  version: i64,
  session_id: i32,
  key: &[u8],
  value: &[u8],
) {
  let _ = log.enqueue(&RecordShape {
    op_type,
    version,
    session_id,
    key,
    value,
    input: &[],
    database_id: 0,
  });
}

/// 对象存 RMW 条目编码入队（信封域物理键，ReplayInput 序列化即 input）
fn enqueue_obj_rmw(
  log: &GarnetLog,
  version: i64,
  session_id: i32,
  user_key: &[u8],
  input: ReplayInput,
) {
  let mut bytes = Vec::new();
  input.serialize(&mut bytes);
  let _ = log.enqueue(&RecordShape {
    op_type: AofEntryType::ObjectStoreRMW,
    version,
    session_id,
    key: &physical_env(user_key),
    value: &[],
    input: &bytes,
    database_id: 0,
  });
}

/// 重放落点装配宏：既有 store 上 new_session → StorageSession →（可选设版本）→ ReplayTarget
///
/// target 借 storage、storage 借会话，三段借用链无法经函数整体返回（自引用
/// 元组），故以宏原样搬运装配；展开序即历史手写序，断言语义与执行序不变。
/// 双臂：带版本臂 = 会话 → 存储会话 → set_current_version → target；
/// 无版本臂供版本已由用例先行设定的场合（不补设，维持原执行序）。
macro_rules! replay_target {
  ($store:ident, $version:expr, $storage:ident, $target:ident) => {
    let __session = $store.new_session()?;
    let $storage = StorageSession::new(__session.enter_batch());
    $store.set_current_version($version);
    let $target = ReplayTarget {
      session: &$storage,
      store: Arc::clone(&$store),
      aof_floor: vec![],
    };
  };
  ($store:ident, $storage:ident, $target:ident) => {
    let __session = $store.new_session()?;
    let $storage = StorageSession::new(__session.enter_batch());
    let $target = ReplayTarget {
      session: &$storage,
      store: Arc::clone(&$store),
      aof_floor: vec![],
    };
  };
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofUpsertStoreRecoverTestAsync
/// 主存写入→恢复重放→状态一致闭环（终值 StoreUpsert / StoreDelete 两形态）
#[compio::test]
async fn test_upsert_rmw_delete_replay_loop() -> aok::Void {
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
  enqueue_entry(
    log,
    AofEntryType::StoreDelete,
    5,
    1,
    &physical(b"gone"),
    &[],
  );
  log.commit();

  // ── 重放段：全新库恢复 ──
  let (_dir2, store2) = open_test_store("aof-loop-replay.db")?;
  replay_target!(store2, 5, storage2, target);
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
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofUpsertObjectStoreRecoverTestAsync
/// 对象存 upsert/delete 回放闭环
#[compio::test]
async fn test_object_replay_loop() -> aok::Void {
  let (_dir, _store) = open_test_store("aof-obj.db")?;
  let aof = aof_fixture()?;
  let log = aof.log();

  // HSET 信封 upsert（对象值 = [tag u8][payload]）
  let mut value = vec![GarnetObjectType::Hash as u8];
  value.extend_from_slice(b"payload");
  enqueue_entry(
    log,
    AofEntryType::ObjectStoreUpsert,
    5,
    1,
    &physical(b"h"),
    &value,
  );
  // 对象 delete
  enqueue_entry(
    log,
    AofEntryType::ObjectStoreDelete,
    5,
    1,
    &physical(b"h"),
    &[],
  );
  log.commit();

  // 重放到全新库
  let (_dir2, store2) = open_test_store("aof-obj-replay.db")?;
  replay_target!(store2, 5, storage2, target);
  let processor = AofProcessor::new(Arc::clone(&aof));
  let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
  assert_eq!(replayed, 2);

  // HSET 信封后 DEL：键应不存在
  assert_eq!(storage2.read_string(b"h").await?, None, "对象 delete 闭环");
  OK
}

/// 版本闸与检查点模糊区：旧代跳过、新代缓冲后重放、事务组整组重放
///
/// rust 侧重放协调器行为锁测，无 C# 单例对标（原挂 RespAofTests.cs:
/// AofCustomTxnRecoverTestAsync 系错挂——该例走 READWRITETX 动态注册面，
/// 属整族不转写的弃案族，见 wcustom/src/lib.rs 登记）
#[compio::test]
async fn test_version_gate_and_txn_group_replay() -> aok::Void {
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
  enqueue_entry(log, AofEntryType::TxnStart, 6, 9, &[], &[]);
  enqueue_upsert_session(log, 6, 9, b"a", b"1")?;
  enqueue_upsert_session(log, 6, 9, b"b", b"2")?;
  enqueue_entry(log, AofEntryType::TxnCommit, 6, 9, &[], &[]);
  log.commit();

  // 重放
  let (_dir2, store2) = open_test_store("aof-txn-replay.db")?;
  replay_target!(store2, 6, storage2, target);
  let processor = AofProcessor::new(Arc::clone(&aof));
  let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
  assert_eq!(replayed, 6, "条目计数含跳过与标记条");

  // 事务组两键均落库（旧代键被版本闸跳过）
  assert_eq!(storage2.read_string(b"old").await?, None, "旧代条目跳过");
  assert_eq!(storage2.read_string(b"a").await?, Some(b"1".to_vec()));
  assert_eq!(storage2.read_string(b"b").await?, Some(b"2".to_vec()));
  OK
}

/// test/standalone/Garnet.test.scripting/RespAofChunkTests.cs:AofLargeStringValueSpanChunkRecoverTest
/// 分块记录：写入端分块 → 读取端重组回放闭环
#[compio::test]
async fn test_chunked_record_replay_loop() -> aok::Void {
  let (_dir, _store) = open_test_store("aof-chunk.db")?;
  let options = RuntimeServerOptions::default();
  let (_dirs, backends) = wnode_test::test_sublogs_with_config(
    "aof_chunk",
    1,
    waof::WalConfig {
      page_size: 1024 * 1024,
      buffer_size: 8 * 1024 * 1024,
      ..WalConfig::default()
    },
  );
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
    &options,
    None,
  ));
  let log = aof.log();

  // 大对象值分块写入（统一入队口自动分块；信封 = [tag u8][payload]）
  let mut big_value = vec![GarnetObjectType::Hash as u8];
  big_value.extend_from_slice(&vec![b'o'; MIN_PARTIAL_ALLOC_SIZE as usize + 64]);
  enqueue_entry(
    log,
    AofEntryType::ObjectStoreUpsert,
    5,
    2,
    &physical(b"big"),
    &big_value,
  );
  log.commit();

  // 重放：处理器内置分块读取器完成重组后按对象 upsert 落库
  // （落库侧信封整包内联，whlog 单记录不跨页：128MB 预算推导 2MB 页，
  // 容纳 1MB 分块阈值值；对标 C# 测试 pageSize = targetBytes/128 的大页意图）
  let (_dir2, store2) =
    wtest_base::open_test_store_with_budget("aof-chunk-replay.db", 128 * 1024 * 1024)?;
  replay_target!(store2, 5, storage2, target);
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
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofUpsertStoreCkptRecoverTestAsync
/// 前缀一致上界：SkipReplay 阈值截断 + 版本闸跳过计数
#[compio::test]
async fn test_skip_replay_prefix_bound() -> aok::Void {
  let (_dir, _store) = open_test_store("aof-skip.db")?;
  let aof = aof_fixture()?;
  let log = aof.log();
  let first = enqueue_upsert(log, 5, b"early", b"1")?;
  enqueue_upsert(log, 5, b"later", b"2")?;
  log.commit();

  // until_sequence_number = first：第二条地址超过阈值 → 前缀截断
  let (_dir2, store2) = open_test_store("aof-skip-replay.db")?;
  replay_target!(store2, 5, storage2, target);
  let processor = AofProcessor::new(Arc::clone(&aof));
  let replayed = AofRecover::recover_replay_driver(&processor, &aof, 0, -1, first, &target).await?;
  assert_eq!(replayed, 1, "前缀一致上界截断后续条目");
  assert_eq!(storage2.read_string(b"early").await?, Some(b"1".to_vec()));
  assert_eq!(storage2.read_string(b"later").await?, None);

  // 无效地址向量形态核对
  let invalid = aof.invalid_aof_address();
  assert_eq!(invalid, AofAddress::create(1, -1));
  OK
}

/// 对象存 RMW 增量形（Hash / Set）重放闭环（对标 RespAofTests.cs 对象存 RMW
/// 恢复族；原挂 AofObjectStoreRMWDeleteRecoverHashTestAsync 锚系错挂——本例
/// 不触删臂，r324 订正，删臂正册见本文件 test_obj_rmw_hash_delete_arm_replay_loop）
#[compio::test]
async fn test_object_store_rmw_replay_loop() -> aok::Void {
  let (_dir, _store) = open_test_store("aof-obj-rmw.db")?;
  let aof = aof_fixture()?;
  let log = aof.log();

  // 1. HSET myhash f1 v1, f2 v2
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"myhash",
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
    },
  );

  // 2. SADD myset m1, m2
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"myset",
    ReplayInput {
      cmd: RespCommand::Sadd,
      flags: 0,
      sub_id: SetOperation::Sadd as u8,
      obj_type: SetObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"m1".to_vec(), b"m2".to_vec()],
    },
  );

  log.commit();

  // 重放
  let (_dir2, store2) = open_test_store("aof-obj-rmw-replay.db")?;
  replay_target!(store2, 5, storage2, target);
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
}

/// 并行 AOF 页级双闸栏恢复集成测试（对标 C# CreateAndRunIntraPageParallelReplayTasks）
#[compio::test]
async fn test_parallel_aof_recover_loop() -> aok::Void {
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
  replay_target!(store2, 5, storage2, target);
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
}

/// 并行回放事务头准入回归（对标 C# AofProcessor.cs:CanReplay 的
/// SingleLogTransactionHeader / ShardedLogTransactionHeader 两支位图判定）：
/// 单物理日志 + `replay_task_count = 2` 形态下，事务标记与 FLUSH 广播条目携带
/// `replay_task_access_vector`，按位图归属分派；缺支时这两族条目在两个任务上
/// 均被静默丢弃（重放计数 0）。
#[compio::test]
async fn parallel_replay_admits_txn_header_entries_by_access_vector() -> aok::Void {
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
  replay_target!(store2, 5, storage2, target);
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
}

/// ObjectStoreRMW 对象增量（ZSet）重放闭环
#[compio::test]
async fn test_object_store_rmw_zset_replay_loop() -> aok::Void {
  let (_dir, _store) = open_test_store("aof-obj-rmw-zset.db")?;
  let aof = aof_fixture()?;
  let log = aof.log();

  // 1. ZADD myzset 10.5 item1, 20.5 item2 via ObjectStoreRMW
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"myzset",
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
    },
  );

  log.commit();

  // 重放
  let (_dir2, store2) = open_test_store("aof-obj-rmw-zset-replay.db")?;
  replay_target!(store2, 5, storage2, target);
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
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofObjectStoreRMWDeleteRecoverHashTestAsync
/// ＋ AofObjectStoreRMWPartialDeleteRecoverHashTestAsync（:1042）
/// Hash 删臂重放闭环：删空落键 / 删部续存 / 删后同键续改
#[compio::test]
async fn test_obj_rmw_hash_delete_arm_replay_loop() -> aok::Void {
  let (_dir, _store) = open_test_store("aof-hash-del.db")?;
  let aof = aof_fixture()?;
  let log = aof.log();

  // 删空落键（C#:987 HDEL 全部字段 → 恢复后键不存在）
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"h_del",
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
    },
  );
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"h_del",
    ReplayInput {
      cmd: RespCommand::Hdel,
      flags: 0,
      sub_id: HashOperation::Hdel as u8,
      obj_type: HashObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"f1".to_vec(), b"f2".to_vec()],
    },
  );

  // 删部续存（C#:1042 同键两次 HDEL，剩余字段存活）
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"h_part",
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
        b"f3".to_vec(),
        b"v3".to_vec(),
      ],
    },
  );
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"h_part",
    ReplayInput {
      cmd: RespCommand::Hdel,
      flags: 0,
      sub_id: HashOperation::Hdel as u8,
      obj_type: HashObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"f1".to_vec()],
    },
  );
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"h_part",
    ReplayInput {
      cmd: RespCommand::Hdel,
      flags: 0,
      sub_id: HashOperation::Hdel as u8,
      obj_type: HashObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"f2".to_vec()],
    },
  );

  // 删后同键续改：HSET → HDEL 删空落键 → 同键再 HSET 重建信封
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"h_re",
    ReplayInput {
      cmd: RespCommand::Hset,
      flags: 0,
      sub_id: HashOperation::Hset as u8,
      obj_type: HashObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"h3".to_vec(), b"v3".to_vec()],
    },
  );
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"h_re",
    ReplayInput {
      cmd: RespCommand::Hdel,
      flags: 0,
      sub_id: HashOperation::Hdel as u8,
      obj_type: HashObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"h3".to_vec()],
    },
  );
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"h_re",
    ReplayInput {
      cmd: RespCommand::Hset,
      flags: 0,
      sub_id: HashOperation::Hset as u8,
      obj_type: HashObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"h4".to_vec(), b"v4".to_vec()],
    },
  );
  log.commit();

  let (_dir2, store2) = open_test_store("aof-hash-del-replay.db")?;
  replay_target!(store2, 5, storage2, target);
  let processor = AofProcessor::new(Arc::clone(&aof));
  let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
  assert_eq!(replayed, 8, "Hash 删臂族 8 条 RMW 条目全量重放");

  // 删空落键：信封域经 is_empty→delete_string 双域自愈后缺席
  assert_eq!(
    storage2
      .read_tag_with(b"h_del", KeyTag::ObjectEnvelope, |v| v.len())
      .await?,
    None,
    "HDEL 删空落键（变异复核坐点：翻 is_empty 门必红）"
  );
  assert_eq!(
    storage2.read_string(b"h_del").await?,
    None,
    "落键后字符串域无残留"
  );

  // 删部续存：键在、只剩 f3
  let part = storage2
    .read_tag_with(b"h_part", KeyTag::ObjectEnvelope, |raw| {
      obj_decode(raw, HashObject::OBJECT_TAG).and_then(HashObject::from_blob)
    })
    .await?
    .flatten()
    .expect("h_part 信封续存");
  assert_eq!(part.hash.len(), 1, "两次 HDEL 后仅剩 f3");
  assert_eq!(
    part.hash.get(b"f3".as_slice()).map(|v| v.as_slice()),
    Some(b"v3".as_slice())
  );
  assert!(part.hash.get(b"f1".as_slice()).is_none());
  assert!(part.hash.get(b"f2".as_slice()).is_none());

  // 删后同键续改：重建信封只含 h4
  let re = storage2
    .read_tag_with(b"h_re", KeyTag::ObjectEnvelope, |raw| {
      obj_decode(raw, HashObject::OBJECT_TAG).and_then(HashObject::from_blob)
    })
    .await?
    .flatten()
    .expect("h_re 删空后重建信封");
  assert_eq!(re.hash.len(), 1, "重建后只含续改字段");
  assert_eq!(
    re.hash.get(b"h4".as_slice()).map(|v| v.as_slice()),
    Some(b"v4".as_slice())
  );
  assert!(re.hash.get(b"h3".as_slice()).is_none());
  OK
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofObjectStoreRMWDeleteRecoverSetTestAsync
/// Set 删臂重放闭环：删空落键 / 删部续存（C# 无 set 删部单例，按 :1042 hash
/// 删部同形延伸）/ 删后同键续改
#[compio::test]
async fn test_obj_rmw_set_delete_arm_replay_loop() -> aok::Void {
  let (_dir, _store) = open_test_store("aof-set-del.db")?;
  let aof = aof_fixture()?;
  let log = aof.log();

  // 删空落键（C#:1015 SREM 全部成员 → 键不存在）
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"s_del",
    ReplayInput {
      cmd: RespCommand::Sadd,
      flags: 0,
      sub_id: SetOperation::Sadd as u8,
      obj_type: SetObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"m1".to_vec(), b"m2".to_vec()],
    },
  );
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"s_del",
    ReplayInput {
      cmd: RespCommand::Srem,
      flags: 0,
      sub_id: SetOperation::Srem as u8,
      obj_type: SetObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"m1".to_vec(), b"m2".to_vec()],
    },
  );

  // 删部续存：三成员删一余二
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"s_part",
    ReplayInput {
      cmd: RespCommand::Sadd,
      flags: 0,
      sub_id: SetOperation::Sadd as u8,
      obj_type: SetObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"m1".to_vec(), b"m2".to_vec(), b"m3".to_vec()],
    },
  );
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"s_part",
    ReplayInput {
      cmd: RespCommand::Srem,
      flags: 0,
      sub_id: SetOperation::Srem as u8,
      obj_type: SetObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"m1".to_vec()],
    },
  );

  // 删后同键续改：SADD → SREM 删空落键 → 同键再 SADD 重建信封
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"s_re",
    ReplayInput {
      cmd: RespCommand::Sadd,
      flags: 0,
      sub_id: SetOperation::Sadd as u8,
      obj_type: SetObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"x1".to_vec()],
    },
  );
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"s_re",
    ReplayInput {
      cmd: RespCommand::Srem,
      flags: 0,
      sub_id: SetOperation::Srem as u8,
      obj_type: SetObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"x1".to_vec()],
    },
  );
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"s_re",
    ReplayInput {
      cmd: RespCommand::Sadd,
      flags: 0,
      sub_id: SetOperation::Sadd as u8,
      obj_type: SetObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"x2".to_vec()],
    },
  );
  log.commit();

  let (_dir2, store2) = open_test_store("aof-set-del-replay.db")?;
  replay_target!(store2, 5, storage2, target);
  let processor = AofProcessor::new(Arc::clone(&aof));
  let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
  assert_eq!(replayed, 7, "Set 删臂族 7 条 RMW 条目全量重放");

  assert_eq!(
    storage2
      .read_tag_with(b"s_del", KeyTag::ObjectEnvelope, |v| v.len())
      .await?,
    None,
    "SREM 删空落键"
  );
  assert_eq!(
    storage2.read_string(b"s_del").await?,
    None,
    "落键后字符串域无残留"
  );

  let part = storage2
    .read_tag_with(b"s_part", KeyTag::ObjectEnvelope, |raw| {
      obj_decode(raw, SetObject::OBJECT_TAG).and_then(SetObject::from_blob)
    })
    .await?
    .flatten()
    .expect("s_part 信封续存");
  assert_eq!(part.set.len(), 2, "SREM 后余二成员");
  assert!(!part.set.contains(b"m1".as_slice()));
  assert!(part.set.contains(b"m2".as_slice()));
  assert!(part.set.contains(b"m3".as_slice()));

  let re = storage2
    .read_tag_with(b"s_re", KeyTag::ObjectEnvelope, |raw| {
      obj_decode(raw, SetObject::OBJECT_TAG).and_then(SetObject::from_blob)
    })
    .await?
    .flatten()
    .expect("s_re 删空后重建信封");
  assert_eq!(re.set.len(), 1, "重建后只含续改成员");
  assert!(re.set.contains(b"x2".as_slice()));
  assert!(!re.set.contains(b"x1".as_slice()));
  OK
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofObjectStoreRMWDeleteRecoverSortedSetTestAsync（:919）
/// ＋ AofObjectStoreRMWDeleteRecoverSortedSetEmptyTestAsync（:959）
/// ＋ AofRMWObjectStoreCopyUpdateRecoverTestAsync（:637 同键续改）
/// ZSet 删臂重放闭环：删空落键 / 删部续存 / 删后同键续改
#[compio::test]
async fn test_obj_rmw_zset_delete_arm_replay_loop() -> aok::Void {
  let (_dir, _store) = open_test_store("aof-zset-del.db")?;
  let aof = aof_fixture()?;
  let log = aof.log();

  // 删部续存（C#:919 ZREM top1 余 top2=60）
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"z_part",
    ReplayInput {
      cmd: RespCommand::Zadd,
      flags: 0,
      sub_id: SortedSetOperation::Zadd as u8,
      obj_type: SortedSetObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![
        b"50".to_vec(),
        b"top1".to_vec(),
        b"60".to_vec(),
        b"top2".to_vec(),
      ],
    },
  );
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"z_part",
    ReplayInput {
      cmd: RespCommand::Zrem,
      flags: 0,
      sub_id: SortedSetOperation::Zrem as u8,
      obj_type: SortedSetObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"top1".to_vec()],
    },
  );

  // 删空落键（C#:959 ZREM 唯一成员 → 键不存在）
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"z_del",
    ReplayInput {
      cmd: RespCommand::Zadd,
      flags: 0,
      sub_id: SortedSetOperation::Zadd as u8,
      obj_type: SortedSetObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"50".to_vec(), b"top1".to_vec()],
    },
  );
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"z_del",
    ReplayInput {
      cmd: RespCommand::Zrem,
      flags: 0,
      sub_id: SortedSetOperation::Zrem as u8,
      obj_type: SortedSetObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"top1".to_vec()],
    },
  );

  // 删后同键续改（C#:637 同键二次 ZADD 改形：删空落键后重建续改，bbbb=4 存活）
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"z_re",
    ReplayInput {
      cmd: RespCommand::Zadd,
      flags: 0,
      sub_id: SortedSetOperation::Zadd as u8,
      obj_type: SortedSetObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"1".to_vec(), b"a".to_vec()],
    },
  );
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"z_re",
    ReplayInput {
      cmd: RespCommand::Zrem,
      flags: 0,
      sub_id: SortedSetOperation::Zrem as u8,
      obj_type: SortedSetObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"a".to_vec()],
    },
  );
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"z_re",
    ReplayInput {
      cmd: RespCommand::Zadd,
      flags: 0,
      sub_id: SortedSetOperation::Zadd as u8,
      obj_type: SortedSetObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"4".to_vec(), b"bbbb".to_vec()],
    },
  );
  log.commit();

  let (_dir2, store2) = open_test_store("aof-zset-del-replay.db")?;
  replay_target!(store2, 5, storage2, target);
  let processor = AofProcessor::new(Arc::clone(&aof));
  let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
  assert_eq!(replayed, 7, "ZSet 删臂族 7 条 RMW 条目全量重放");

  let part = storage2
    .read_tag_with(b"z_part", KeyTag::ObjectEnvelope, |raw| {
      obj_decode(raw, SortedSetObject::OBJECT_TAG).and_then(SortedSetObject::from_blob)
    })
    .await?
    .flatten()
    .expect("z_part 信封续存");
  assert_eq!(part.sorted_set_dict.len(), 1, "ZREM 后余一成员");
  assert_eq!(part.sorted_set_dict.get(b"top1".as_slice()), None);
  assert_eq!(part.sorted_set_dict.get(b"top2".as_slice()), Some(&60.0));

  assert_eq!(
    storage2
      .read_tag_with(b"z_del", KeyTag::ObjectEnvelope, |v| v.len())
      .await?,
    None,
    "ZREM 删空落键"
  );
  assert_eq!(
    storage2.read_string(b"z_del").await?,
    None,
    "落键后字符串域无残留"
  );

  let re = storage2
    .read_tag_with(b"z_re", KeyTag::ObjectEnvelope, |raw| {
      obj_decode(raw, SortedSetObject::OBJECT_TAG).and_then(SortedSetObject::from_blob)
    })
    .await?
    .flatten()
    .expect("z_re 删空后重建信封");
  assert_eq!(re.sorted_set_dict.len(), 1, "重建后只含续改成员");
  assert_eq!(re.sorted_set_dict.get(b"bbbb".as_slice()), Some(&4.0));
  assert_eq!(re.sorted_set_dict.get(b"a".as_slice()), None);
  OK
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofListObjectStoreRecoverTestAsync（:857 list 族整族重放首册）
/// ＋ AofObjectStoreRMWDeleteRecoverListTestAsync（:889 LPOP 删空落键）
/// ＋ AofExpiryRMWObjectStoreRecoverTestAsync（:530 key2 臂空后重推续改，TTL 形不并入）
/// List 删臂重放闭环：删空落键 / 删部续存 / 删后同键续改
#[compio::test]
async fn test_obj_rmw_list_delete_arm_replay_loop() -> aok::Void {
  let (_dir, _store) = open_test_store("aof-list-del.db")?;
  let aof = aof_fixture()?;
  let log = aof.log();

  // 删部续存：RPUSH a b c → LPOP count=1 摘头，余 [b, c]
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"l_part",
    ReplayInput {
      cmd: RespCommand::Rpush,
      flags: 0,
      sub_id: ListOperation::Rpush as u8,
      obj_type: ListObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
    },
  );
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"l_part",
    ReplayInput {
      cmd: RespCommand::Lpop,
      flags: 0,
      sub_id: ListOperation::Lpop as u8,
      obj_type: ListObject::OBJECT_TAG.as_u8(),
      arg1: 1,
      arg2: 0,
      arg3: 0,
      args: vec![],
    },
  );

  // 删空落键（C#:889 唯一元素 LPOP → 键不存在）
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"l_del",
    ReplayInput {
      cmd: RespCommand::Rpush,
      flags: 0,
      sub_id: ListOperation::Rpush as u8,
      obj_type: ListObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"v1".to_vec()],
    },
  );
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"l_del",
    ReplayInput {
      cmd: RespCommand::Lpop,
      flags: 0,
      sub_id: ListOperation::Lpop as u8,
      obj_type: ListObject::OBJECT_TAG.as_u8(),
      arg1: 1,
      arg2: 0,
      arg3: 0,
      args: vec![],
    },
  );

  // 删后同键续改（C#:530 key2 臂：列表空后重推续改，不落 TTL 形）
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"l_re",
    ReplayInput {
      cmd: RespCommand::Rpush,
      flags: 0,
      sub_id: ListOperation::Rpush as u8,
      obj_type: ListObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"q1".to_vec()],
    },
  );
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"l_re",
    ReplayInput {
      cmd: RespCommand::Lpop,
      flags: 0,
      sub_id: ListOperation::Lpop as u8,
      obj_type: ListObject::OBJECT_TAG.as_u8(),
      arg1: 1,
      arg2: 0,
      arg3: 0,
      args: vec![],
    },
  );
  enqueue_obj_rmw(
    log,
    5,
    1,
    b"l_re",
    ReplayInput {
      cmd: RespCommand::Rpush,
      flags: 0,
      sub_id: ListOperation::Rpush as u8,
      obj_type: ListObject::OBJECT_TAG.as_u8(),
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args: vec![b"r1".to_vec(), b"r2".to_vec()],
    },
  );
  log.commit();

  let (_dir2, store2) = open_test_store("aof-list-del-replay.db")?;
  replay_target!(store2, 5, storage2, target);
  let processor = AofProcessor::new(Arc::clone(&aof));
  let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
  assert_eq!(replayed, 7, "List 删臂族 7 条 RMW 条目全量重放");

  // 删部续存：信封在、头元素已摘、次序保持
  let part = storage2
    .read_tag_with(b"l_part", KeyTag::ObjectEnvelope, |raw| {
      obj_decode(raw, ListObject::OBJECT_TAG).and_then(ListObject::from_blob)
    })
    .await?
    .flatten()
    .expect("l_part 信封续存");
  assert_eq!(
    part.list.iter().map(Vec::as_slice).collect::<Vec<_>>(),
    vec![b"b".as_slice(), b"c".as_slice()],
    "LPOP count=1 摘头余尾序"
  );

  // 删空落键：双域皆缺
  assert_eq!(
    storage2
      .read_tag_with(b"l_del", KeyTag::ObjectEnvelope, |v| v.len())
      .await?,
    None,
    "LPOP 删空落键"
  );
  assert_eq!(
    storage2.read_string(b"l_del").await?,
    None,
    "落键后字符串域无残留"
  );

  // 删后同键续改：落键后重建，只含重推元素
  let re = storage2
    .read_tag_with(b"l_re", KeyTag::ObjectEnvelope, |raw| {
      obj_decode(raw, ListObject::OBJECT_TAG).and_then(ListObject::from_blob)
    })
    .await?
    .flatten()
    .expect("l_re 删空后重建信封");
  assert_eq!(
    re.list.iter().map(Vec::as_slice).collect::<Vec<_>>(),
    vec![b"r1".as_slice(), b"r2".as_slice()],
    "重建信封只含续改元素"
  );
  OK
}

/// ACL 用户规则条目 AOF 回放闭环（KeyTag::Acl 旁路标签，主从复制链路）
///
/// 主库 `ACL SETUSER` / `ACL DELUSER` 产生的 StoreUpsert / StoreDelete 条目
/// 物理键标签为 0x0D，从库回放须落回 db 0 的 Acl 域而非字符串域（后者 SET
/// 语义会清 TTL/信封误伤旁路记录）。回放后按 `(ns, 0, Acl, user)` 点查可见。
#[compio::test]
async fn test_acl_replay_loop() -> aok::Void {
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
  enqueue_entry(
    log,
    AofEntryType::StoreUpsert,
    5,
    1,
    &alice_key,
    &alice_rule,
  );
  enqueue_entry(log, AofEntryType::StoreUpsert, 5, 1, &bob_key, &bob_rule);
  enqueue_entry(log, AofEntryType::StoreDelete, 5, 1, &bob_key, &[]);
  log.commit();

  // 从库空库全量回放
  let (_dir2, store2) = open_test_store("aof-acl-replay.db")?;
  replay_target!(store2, 5, storage2, target);
  let processor = AofProcessor::new(Arc::clone(&aof));
  let replayed = AofRecover::single_log_recover(&processor, &aof, 0, 0, -1, &target).await?;
  assert_eq!(replayed, 3, "3 条 ACL 条目均应重放");

  // 点查：alice 规则原样落 db 0 Acl 域；bob 已被墓碑删除
  let probe = store2.new_session()?;
  let acl_store = AclStore::new(&probe);
  assert_eq!(
    acl_store.read(0, b"alice").await?,
    Some(alice_rule.to_vec()),
    "ACL upsert 回放后点查命中且值原样"
  );
  assert_eq!(
    acl_store.read(0, b"bob").await?,
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
  session.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(Arc::new(
    AccessControlList::new("")?,
  )))));
  session.set_garnet_api(Arc::new(StoreGarnetApi::new(store2.new_session()?)));
  session
    .recv_buffer
    .extend_from_slice(b"*3\r\n$4\r\nAUTH\r\n$5\r\nalice\r\n$8\r\npassw0rd\r\n");
  assert!(session.try_consume_messages().is_some());
  let mut resp_buf = Vec::new();
  wnode_test::drive_pending_parks(&mut session, &mut resp_buf, true).await;
  session.output.extend_from_slice(&resp_buf);
  assert_eq!(
    drain_output(&mut session),
    b"+OK\r\n",
    "回放恢复的 alice 认证成功"
  );
  OK
}

/// 取出日志当前可读的逻辑 AOF 条目（地址 + 负载），供重放臂逐条直喂
///
/// 计数口径 = 逻辑条目而非物理帧：commit() 时常驻提交协程随批尾写入日志的
/// 24B commit 元数据帧（MAGIC+begin+cookie，见 waof/src/wal/commit.rs，对标
/// C# TsavoriteLog.cs 的 TryEnqueueCommitRecord）经单点判据 `waof::is_commit_frame`
/// 滤除——该帧是恢复收敛提交边界的元数据，不是 AOF 数据条目；重放消费面
/// （aof_processor / recover_replay_task / aof_chunked_record_reader）同判据
/// 过滤，本 helper 随动即与 `AofRecover::single_log_recover` 的 replayed 计数
/// 同口径（本文件 ACL 等用例 replayed==3 断言即在证该口径不含帧）。
fn drain_log_entries(log: &GarnetLog) -> Vec<(i64, Vec<u8>)> {
  let mut records = Vec::new();
  log.scan_single_with(0, log.get_begin_address(0), log.get_tail_address(0), |r| {
    if !waof::is_commit_frame(&r.payload) {
      records.push((r.address as i64, r.payload.clone()));
    }
    true
  });
  records
}

/// 副本重放 CheckpointEndCommit 臂的本地拍检查点与截断闭环
///
/// 对标 C# 链路：AofProcessor 的 ProcessAofRecordInternal（CheckpointEndCommit
/// 支 asReplica && header.storeVersion > store.CurrentVersion →
/// StoreWrapper 的 TakeCheckpointAsync → DatabaseManagerBase 的
/// InitiateCheckpointAsync 登记 + 截断面）。断言面：
/// 1. 起始标记回报 is_checkpoint_start（副本据此记起始位点）；
/// 2. 新代条目先入模糊区缓冲、结束标记臂拍完检查点后才落库（次序与 C# 一致）；
/// 3. 宿主面确实产出本地检查点基线（版本推进 + 目录内新快照）；
/// 4. 本地 wal 随检查点覆盖位点物理截断（副本 wal 不再只增不减）；
/// 5. 同代再次遇到结束标记不再打点（版本闸）。
///
/// 覆盖位点取源：本用例在 wnode 层装配库管理器，不注入集群句柄，故内核走
/// 单机形态分支取 AOF 尾地址为 covered（副本形态下由 on_checkpoint_initiated
/// 取检查点起始位点，该回调面属复制域、其接线由 wedb 侧用例承接）；本用例锁
/// 的是重放臂自身的触发面：钩子在场即拍、次序在前、版本闸拦截重复打点。
#[compio::test]
async fn replica_checkpoint_end_marker_takes_local_checkpoint() -> aok::Void {
  let (_dir, _store) = open_test_store("aof-ckpt-src.db")?;
  let aof = aof_fixture()?;
  let log = aof.log();

  // 主端标记链：起始标记 → 新代 (v7) 写入 → 结束标记
  let _ = log.enqueue_database_commit(AofEntryType::CheckpointStartCommit, 7)?;
  let _ = enqueue_upsert(log, 7, b"fuzzy", b"v-new")?;
  let _ = log.enqueue_database_commit(AofEntryType::CheckpointEndCommit, 7)?;
  log.commit();

  // 副本重放落点：库版本 6（低于标记代 7），常驻库管理器持同一 aof
  let (dir2, store2) = open_test_store("aof-ckpt-replay.db")?;
  store2.set_current_version(6);
  let cp_dir = dir2.path().join("0");
  create_dir_all(&cp_dir).expect("检查点目录就绪");
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&store2),
    Arc::clone(&store2.device),
    cp_dir.clone(),
    Some(Arc::clone(&aof)),
  ));
  let manager = Arc::new(SingleDatabaseManager::new(cp_dir.clone(), Arc::clone(&db)));
  replay_target!(store2, 6, storage2, target);
  // 检查点钩子注入（对标 assembly.rs 装配面：动作经类型擦除钩子下达
  // SingleDatabaseManager::take_checkpoint，AofProcessor 不持库管理器句柄）
  let hooked_manager = Arc::clone(&manager);
  let hook: Arc<ReplicaCheckpointHook> = Arc::new(move || {
    let manager = Arc::clone(&hooked_manager);
    Box::pin(async move { manager.take_checkpoint(false).await.map(|_| ()) })
  });
  let processor = AofProcessor::new(Arc::clone(&aof));
  processor.set_checkpoint_hook(hook);

  let begin_before = log.get_begin_address(0);
  let records = drain_log_entries(log);
  // 逐条构成（逻辑条目恰 3，实测物理帧 4）：①CheckpointStartCommit(v7)
  // ②StoreUpsert(v7,"fuzzy"→"v-new") ③CheckpointEndCommit(v7)；第 4 条
  // 物理帧取证实测为 commit() 随批尾的 24B commit 元数据帧（payload=
  // MAGIC(FF,'M','O','C','T','I','M','M')||begin||cookie，addr 紧随标记对、
  // 链连续），按消费面同一判据在 helper 滤除。判别力反向注入已验：临时撤掉
  // 首轮结束标记写入臂，本计数断言红（left:2 right:3）；产品臂若重复发出
  // 数据条目，本计数与下游直喂断言同样必红。
  assert_eq!(records.len(), 3, "标记对与新代条目均在可读段内");

  // 起始标记：回报供副本记起始位点，并进入模糊区
  assert!(
    processor
      .process_aof_record_internal(0, &records[0].1, true, records[0].0, &target)
      .await?,
    "起始标记回报 is_checkpoint_start"
  );
  processor
    .process_aof_record_internal(0, &records[1].1, true, records[1].0, &target)
    .await?;
  assert_eq!(
    storage2.read_string(b"fuzzy").await?,
    None,
    "模糊区内新代条目先缓冲、未即时应用"
  );

  // 结束标记：先本地拍检查点，再重放缓冲条目
  let version_before = store2.current_version();
  processor
    .process_aof_record_internal(0, &records[2].1, true, records[2].0, &target)
    .await?;
  assert!(
    store2.current_version() > version_before,
    "臂内经钩子推进到检查点新版本"
  );
  assert_eq!(
    wcpr::list_checkpoints(&cp_dir)?.len(),
    1,
    "副本本地检查点基线已落盘"
  );
  assert_eq!(
    storage2.read_string(b"fuzzy").await?,
    Some(b"v-new".to_vec()),
    "缓冲条目在拍完之后落库"
  );
  assert!(
    log.get_begin_address(0) > begin_before,
    "本地 wal 随检查点覆盖位点物理截断"
  );

  // 版本闸：同代标记对不再触发第二次打点（现值已高于标记代 7，钩子不再被
  // 驱动）。第二轮可读条数随日志对齐而变，非本用例断言面，故只逐条直喂
  let _ = log.enqueue_database_commit(AofEntryType::CheckpointStartCommit, 7)?;
  let _ = log.enqueue_database_commit(AofEntryType::CheckpointEndCommit, 7)?;
  log.commit();
  let version_at_gate = store2.current_version();
  let second = drain_log_entries(log);
  assert!(!second.is_empty(), "截断后仍有在途标记对可读");
  for record in &second {
    processor
      .process_aof_record_internal(0, &record.1, true, record.0, &target)
      .await?;
  }
  assert_eq!(
    store2.current_version(),
    version_at_gate,
    "同代结束标记不再推进版本"
  );
  assert_eq!(
    wcpr::list_checkpoints(&cp_dir)?.len(),
    1,
    "同代结束标记不再打点"
  );
  OK
}

/// 检查点结束臂推进版本后，后续旧代记录按推进后的当前版本过滤（动态版本闸）
///
/// 对标 C# 链路：AofProcessor.ShouldSkipRecord 的 IsOldVersionRecord /
/// IsNewVersionRecord 每条动态读 storeWrapper.store.CurrentVersion
/// （AofProcessor.cs:793-798），绝不缓存构造期快照。断言面：
/// 1. 结束标记臂拍完本地检查点，存储当前版本推进越过标记代；
/// 2. 检查点后到达的旧代条目（header.store_version == 检查点前旧值）按推进后
///    的版本判定为旧代并跳过——已被检查点持久化的记录不重复重放
///    （Exactly-Once）；静态快照基线（旧值 < 旧值恒假）则漏放复活。
#[compio::test]
async fn replica_post_checkpoint_old_version_record_skipped() -> aok::Void {
  let (_dir, _store) = open_test_store("aof-ckpt-stale-src.db")?;
  let aof = aof_fixture()?;
  let log = aof.log();

  // 主端标记链：起始标记(v7) → 新代写入(v7) → 结束标记(v7) → 旧代写入(v6，
  // 模拟检查点前已持久化、日志序滞后的残留条目)
  let _ = log.enqueue_database_commit(AofEntryType::CheckpointStartCommit, 7)?;
  let _ = enqueue_upsert(log, 7, b"fuzzy", b"v-new")?;
  let _ = log.enqueue_database_commit(AofEntryType::CheckpointEndCommit, 7)?;
  let _ = enqueue_upsert(log, 6, b"stale", b"v-old")?;
  log.commit();

  // 副本重放落点：库版本 6（旧代），常驻库管理器持同一 aof，钩子注入
  let (dir2, store2) = open_test_store("aof-ckpt-stale-replay.db")?;
  store2.set_current_version(6);
  let cp_dir = dir2.path().join("0");
  create_dir_all(&cp_dir).expect("检查点目录就绪");
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&store2),
    Arc::clone(&store2.device),
    cp_dir.clone(),
    Some(Arc::clone(&aof)),
  ));
  let manager = Arc::new(SingleDatabaseManager::new(cp_dir.clone(), Arc::clone(&db)));
  replay_target!(store2, storage2, target);
  let hooked_manager = Arc::clone(&manager);
  let hook: Arc<ReplicaCheckpointHook> = Arc::new(move || {
    let manager = Arc::clone(&hooked_manager);
    Box::pin(async move { manager.take_checkpoint(false).await.map(|_| ()) })
  });
  let processor = AofProcessor::new(Arc::clone(&aof));
  processor.set_checkpoint_hook(hook);

  let records = drain_log_entries(log);
  assert_eq!(records.len(), 4, "标记对 2 + 新代写入 + 旧代写入均可读");

  // 起始标记进入模糊区；新代条目入缓冲
  assert!(
    processor
      .process_aof_record_internal(0, &records[0].1, true, records[0].0, &target)
      .await?,
    "起始标记回报 is_checkpoint_start"
  );
  processor
    .process_aof_record_internal(0, &records[1].1, true, records[1].0, &target)
    .await?;
  assert_eq!(
    storage2.read_string(b"fuzzy").await?,
    None,
    "模糊区内新代条目先缓冲"
  );

  // 结束标记：先拍检查点（版本推进），再清算缓冲条目
  processor
    .process_aof_record_internal(0, &records[2].1, true, records[2].0, &target)
    .await?;
  assert!(
    store2.current_version() > 7,
    "结束标记臂经钩子推进版本越过标记代"
  );
  assert_eq!(
    storage2.read_string(b"fuzzy").await?,
    Some(b"v-new".to_vec()),
    "缓冲条目清算落库"
  );

  // 检查点后到达的旧代条目：动态版本闸按推进后的当前版本判定为旧代跳过
  // （静态构造期快照 6 判 6 < 6 恒假，该条目会复活重放）
  processor
    .process_aof_record_internal(0, &records[3].1, true, records[3].0, &target)
    .await?;
  assert_eq!(
    storage2.read_string(b"stale").await?,
    None,
    "检查点推进后旧代条目须被 ShouldSkipRecord 跳过（Exactly-Once）"
  );
  OK
}

/// 模糊区内提交的事务组在 CheckpointEndCommit 清算时整组重放落地
///
/// 对标 C# 链路：AofReplayCoordinator.AddOrReplayTransactionOperation 模糊区
/// TxnCommit 支（AddToFuzzyRegionBuffer 压入提交标记 + txnGroupBuffer 入队）
/// → CheckpointEndCommit 清算 ProcessFuzzyRegionOperations 顺序识别标记触发
/// ProcessFuzzyRegionTransactionGroup FIFO 出组整组重放（绝不当常规数据条目
/// 派发——标记无键载荷，prepare_key 无键即判「AOF 条目负载损坏」崩溃）。
/// 断言面：
/// 1. 模糊区窗口内两笔事务（各自 TxnStart + 数据写入 + TxnCommit）与非事务
///    新代写入全部缓冲、未即时落库；
/// 2. 结束标记清算无崩溃报错，两事务组按 FIFO 标记配对整组重放落地；
/// 3. 数据一致性正确（各键终值原样），清算后模糊区缓冲与事务组缓冲清空；
/// 4. as_replica 经清算臂透传（副本免钩子形态：结束标记无本地打点仍完成清算）。
#[compio::test]
async fn fuzzy_region_txn_commit_replays_group_on_settlement() -> aok::Void {
  let (_dir, _store) = open_test_store("aof-fuzzy-txn-src.db")?;
  let aof = aof_fixture()?;
  let log = aof.log();

  // 主端标记链（新代 v7）：起始标记 → 事务组1(s9) → 事务组2(s10) →
  // 非事务写入 → 结束标记
  let _ = log.enqueue_database_commit(AofEntryType::CheckpointStartCommit, 7)?;
  // 事务组1(s9)：TxnStart → SET ta 1 → SET tb 2 → TxnCommit
  enqueue_entry(log, AofEntryType::TxnStart, 7, 9, &[], &[]);
  enqueue_upsert_session(log, 7, 9, b"ta", b"1")?;
  enqueue_upsert_session(log, 7, 9, b"tb", b"2")?;
  enqueue_entry(log, AofEntryType::TxnCommit, 7, 9, &[], &[]);
  // 事务组2(s10)：TxnStart → SET tc 3 → TxnCommit
  enqueue_entry(log, AofEntryType::TxnStart, 7, 10, &[], &[]);
  enqueue_upsert_session(log, 7, 10, b"tc", b"3")?;
  enqueue_entry(log, AofEntryType::TxnCommit, 7, 10, &[], &[]);
  let _ = enqueue_upsert(log, 7, b"plain", b"v7")?;
  let _ = log.enqueue_database_commit(AofEntryType::CheckpointEndCommit, 7)?;
  log.commit();

  // 副本重放落点：库版本 6（低于标记代 7），钩子不注入（清算臂纯缓冲重放，
  // 无本地打点面）
  let (_dir2, store2) = open_test_store("aof-fuzzy-txn-replay.db")?;
  store2.set_current_version(6);
  replay_target!(store2, storage2, target);
  let processor = AofProcessor::new(Arc::clone(&aof));

  let records = drain_log_entries(log);
  assert_eq!(records.len(), 10, "标记对 2 + 事务标记 4 + 数据写入 4");

  // 起始标记进入模糊区
  assert!(
    processor
      .process_aof_record_internal(0, &records[0].1, true, records[0].0, &target)
      .await?,
    "起始标记回报 is_checkpoint_start"
  );
  // 窗口内全部条目：事务组入组 + 提交标记与组缓冲成对登记，均不落库
  for record in &records[1..9] {
    processor
      .process_aof_record_internal(0, &record.1, true, record.0, &target)
      .await?;
  }
  assert_eq!(
    processor.coordinator().fuzzy_region_buffer_count(0),
    3,
    "两枚提交标记 + 非事务写入入模糊区缓冲"
  );
  assert_eq!(
    processor.coordinator().context(0).txn_group_buffer.len(),
    2,
    "两事务组成对入队待清算"
  );
  for key in [b"ta".as_slice(), b"tb", b"tc", b"plain"] {
    assert_eq!(
      storage2.read_string(key).await?,
      None,
      "模糊区内缓冲未即时应用"
    );
  }

  // 结束标记清算：TxnCommit 标记识别出组按 FIFO 整组重放，无崩溃报错
  processor
    .process_aof_record_internal(0, &records[9].1, true, records[9].0, &target)
    .await?;
  assert_eq!(storage2.read_string(b"ta").await?, Some(b"1".to_vec()));
  assert_eq!(storage2.read_string(b"tb").await?, Some(b"2".to_vec()));
  assert_eq!(storage2.read_string(b"tc").await?, Some(b"3".to_vec()));
  assert_eq!(
    storage2.read_string(b"plain").await?,
    Some(b"v7".to_vec()),
    "非事务新代条目清算臂照常落库"
  );
  assert_eq!(
    processor.coordinator().fuzzy_region_buffer_count(0),
    0,
    "清算后模糊区缓冲清空"
  );
  assert_eq!(
    processor.coordinator().context(0).txn_group_buffer.len(),
    0,
    "事务组缓冲消费后清空（标记与组一一对齐出队）"
  );
  assert!(
    !processor.coordinator().context(0).in_fuzzy_region(),
    "清算完成退出模糊区"
  );
  OK
}

/// ShardedChunkHeader 并行回放路由回归（对标 libs/server/AOF/AofProcessor.cs 的 CanReplay
/// 的 ShardedChunkHeader 支）：分块条目键分散于各片，多物理日志写侧首帧布局为
/// `Sharded 头(24B) + 分块头(28B) + 裸键`（裸键无 4B 小端长度前缀）。修复前
/// `ShardedHeader | ShardedChunkHeader` 两支统一走 [`record_gate::peek_entry_key`]
/// 强行把裸键首 4 字节读作长度前缀——越界即 `InvalidRecordHeader` 中断恢复，
/// 侥幸合法则路由哈希偏离 `chunk.key_hash`、把同记录各片派往不同回放任务无法
/// 聚合。修复后与 Basic 分块支对称：以 `is_chunked()` 判定取内嵌 `chunk.key_hash`
/// 路由。全链路经真实写侧（`GarnetLog::enqueue` 触发 `enqueue_span_chunked`）落盘
/// 再回扫取帧，杜绝手搓帧与写侧布局漂移。
#[compio::test]
async fn sharded_chunk_header_can_replay_routes_by_key_hash() -> aok::Void {
  // 两物理子日志 × 四回放任务：多物理日志拓扑令分块记录落 ShardedChunkHeader，
  // 回放任务数 > 1 使路由分派可辨
  let (_dirs, backends) = wnode_test::test_sublogs("aof_sharded_chunk_route", 2);
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: 2,
    aof_replay_task_count: 4,
    ..RuntimeServerOptions::default()
  };
  let seq_gen = Some(Arc::new(SequenceNumberGenerator::new(0)));
  let aof = Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(GarnetLog::new(&options, backends, seq_gen.clone()).expect("构造 GarnetLog")),
    &options,
    seq_gen,
  ));
  let log = aof.log();

  // 超 `MIN_PARTIAL_ALLOC_SIZE` 的大 value 触发写侧自动分块
  let user_key = b"chunky-route-target-key";
  let big = vec![0u8; MIN_PARTIAL_ALLOC_SIZE as usize + 4096];
  enqueue_upsert(log, 5, user_key.as_slice(), &big)?;
  log.commit_async().await;

  // 该键按哈希路由至确定物理子日志，回扫取回其分块首帧（版本字节 + 头型双判据
  // 排除 commit 帧与 value/input 续帧）
  let key = physical(user_key.as_slice());
  let hash = GarnetLog::hash(&key);
  let sublog_idx = log.get_physical_sublog_idx(hash);
  let mut chunk_frame: Option<Vec<u8>> = None;
  log.scan_single_with(
    sublog_idx,
    log.get_begin_address(sublog_idx),
    log.get_tail_address(sublog_idx),
    |r| {
      if waof::is_commit_frame(&r.payload) {
        return true;
      }
      let is_chunk_head = AofHeader::parse(&r.payload).is_some_and(|h| {
        h.aof_header_version == AofHeader::AOF_FORMAT_VERSION
          && h.header_type() == Some(AofHeaderType::ShardedChunkHeader)
      });
      if is_chunk_head {
        chunk_frame.get_or_insert_with(|| r.payload.clone());
      }
      true
    },
  );
  let frame = chunk_frame.expect("写侧分块首帧应为 ShardedChunkHeader");

  // 1. 写侧盖入的分块头内嵌 key_hash = 物理键哈希（路由真源）
  let (_, ch) = AofHeader::get_chunked_header_ref(&frame).expect("分块头内嵌可解析");
  assert_eq!(ch.key_hash, hash, "chunk.key_hash 即路由源");

  // 2. 缺陷根因留证：裸键无长度前缀，旧解析路径必失败（修复前 can_replay 据此
  //    判定即在此处抛 InvalidRecordHeader 或算出错误路由）
  assert!(
    record_gate::peek_entry_key(&frame).is_none(),
    "分块首帧裸键不可经 peek_entry_key 解析"
  );

  // 3. can_replay 不再报错，且按 chunk.key_hash 唯一准入 owner 任务、其余三任务拒绝
  let owner = log.get_replay_task_idx(hash);
  assert!(owner < options.aof_replay_task_count as usize);
  for idx in 0..options.aof_replay_task_count as usize {
    let (admit, _) =
      record_gate::can_replay(&aof, &frame, idx, 0).expect("分块头判定不应抛 InvalidRecordHeader");
    assert_eq!(
      admit,
      idx == owner,
      "仅 owner 任务 {owner} 准入，任务 {idx} 判定错误"
    );
  }

  OK
}
