//! WAL Journaling 与 Replay 闭环端到端集成测试
//!
//! 验证（单一机制：GarnetLog 唯一编码 + AofProcessor 唯一重放）：
//! 1. 统一端口制 WAL Journaling：KV 写入/删除、RangeIndex 创建/设置/删除自动进入 WAL
//! 2. WAL 提交与物理持久化
//! 3. Replay 闭环：replay_into_session（AofProcessor）把 AOF 流重放到全新实例
//! 4. 断言主从/新旧实例数据完全一致

use std::sync::Arc;

use aok::{OK, Void};
use tempfile::{TempDir, tempdir};
use waof::{AofEntryType, AofHeader, WalConfig, WalLog, is_commit_frame};
use wbftree::{StorageBackendType, TreeTuning};
use wcol::RespInputFlags;
use wdev::SegmentedDevice;
use wkv::{RangeIndexError, StoreConfig, WedbStore};
use wnode::{
  aof::{GarnetAppendOnlyFile, ReplayInput, garnet_log::RecordShape},
  resp,
  service::NodeService,
};
use wresp::command::RespCommand;
use wval::{GarnetObjectType, KeyTag, NamespaceDbCodec};

const TUNE: TreeTuning = TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

type TestEnv = (
  TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  Arc<WalLog<SegmentedDevice>>,
);

fn open_test_node(tag: &str) -> aok::Result<TestEnv> {
  let dir = tempdir()?;
  let store_device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{tag}.db")),
  )?);
  let wal_device = Arc::new(SegmentedDevice::single_file(
    dir.path().join(format!("{tag}.wal")),
  )?);
  let mut config = StoreConfig::new(2048, 64 * 1024, 16, 0.5)?;
  config.range_index_dir = Some(dir.path().to_path_buf());
  let store = Arc::new(WedbStore::open(config, store_device)?);
  let wal = Arc::new(WalLog::new(wal_device, WalConfig::default())?);
  Ok((dir, store, wal))
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofUpsertStoreAutoCommitRecoverTestAsync
#[compio::test]
async fn test_wal_replay_e2e_closure() -> Void {
  // 1. 初始化 Primary NodeService (挂载存储与 WAL)
  let (_dir1, store1, wal1) = open_test_node("primary")?;
  let primary = NodeService::with_wal(Arc::clone(&store1), Arc::clone(&wal1))?;

  // 2. 执行数据写入操作:
  // (a) KV 写入 (SET / upsert)
  primary.session().upsert(b"user:1001", b"Alice").await?;
  primary.session().upsert(b"user:1002", b"Bob").await?;
  primary
    .session()
    .upsert(b"user:temp", b"To-Be-Deleted")
    .await?;

  // (b) KV 删除 (DEL / delete)
  let deleted_kv = primary.session().delete(b"user:temp").await?;
  assert!(deleted_kv);

  // (c) RangeIndex 创建 (RI.CREATE)
  primary
    .ri_create(b"idx:scores", StorageBackendType::Disk, TUNE)
    .await?;

  // (d) RangeIndex 字段设置 (RI.SET)
  primary
    .ri_set(b"idx:scores", b"player_1", b"score:980")
    .await?;
  primary
    .ri_set(b"idx:scores", b"player_2", b"score:850")
    .await?;
  primary
    .ri_set(b"idx:scores", b"player_3", b"score:720")
    .await?;

  // (e) RangeIndex 字段更新 (覆盖写入)
  primary
    .ri_set(b"idx:scores", b"player_1", b"score:999")
    .await?;

  // (f) RangeIndex 字段删除 (RI.DEL)
  let deleted_ri = primary.ri_del(b"idx:scores", b"player_3").await?;
  assert!(deleted_ri);

  // 提交 WAL 到持久层
  wal1.commit().await?;

  // 3. 验证 WAL 中已有日志条目
  let mut scan = wal1.scan_committed();
  let mut scanned_count = 0u64;
  while let Some(record) = scan.next().await? {
    // commit 元数据帧随批写出于日志尾部，不计入数据条目数
    if is_commit_frame(&record.payload) {
      continue;
    }
    scanned_count += 1;
  }
  assert!(
    scanned_count >= 8,
    "WAL 必须包含全部已提交条目，实际={scanned_count}"
  );

  // 4. 创建第二个全新存储节点（Replica / Recover target）
  let (_dir2, store2, wal2) = open_test_node("replica")?;
  let replica = NodeService::with_wal(Arc::clone(&store2), Arc::clone(&wal2))?;
  let replica_session = replica.session();

  // 5. 将 Primary 的 AOF 流重放到第二个节点的会话中
  let replayed = primary.replay_into_session(replica_session).await?;
  assert_eq!(
    replayed, scanned_count,
    "重放条目数应与已提交日志数严格一致"
  );

  // 6. 断言两端状态一致：
  // (a) KV 状态核对
  assert_eq!(
    replica_session.read(b"user:1001").await?,
    Some(b"Alice".to_vec()),
    "user:1001 必须正确重放"
  );
  assert_eq!(
    replica_session.read(b"user:1002").await?,
    Some(b"Bob".to_vec()),
    "user:1002 必须正确重放"
  );
  assert_eq!(
    replica_session.read(b"user:temp").await?,
    None,
    "已删除的 user:temp 在重放后必须不存在"
  );

  // 与 Primary 本端逐项断言严格相等
  assert_eq!(
    replica_session.read(b"user:1001").await?,
    primary.session().read(b"user:1001").await?
  );
  assert_eq!(
    replica_session.read(b"user:1002").await?,
    primary.session().read(b"user:1002").await?
  );
  assert_eq!(
    replica_session.read(b"user:temp").await?,
    primary.session().read(b"user:temp").await?
  );

  // (b) RangeIndex 字段状态核对
  assert_eq!(
    replica_session
      .range_index_get(b"idx:scores", b"player_1")
      .await?,
    Some(b"score:999".to_vec()),
    "player_1 更新后的分数必须正确重放"
  );
  assert_eq!(
    replica_session
      .range_index_get(b"idx:scores", b"player_2")
      .await?,
    Some(b"score:850".to_vec()),
    "player_2 的分数必须正确重放"
  );
  assert_eq!(
    replica_session
      .range_index_get(b"idx:scores", b"player_3")
      .await?,
    None,
    "已删除的 player_3 在重放后必须不存在"
  );

  // 与 Primary 本端逐字段断言严格相等
  for field in [b"player_1".as_slice(), b"player_2", b"player_3"] {
    assert_eq!(
      replica_session
        .range_index_get(b"idx:scores", field)
        .await?,
      primary
        .session()
        .range_index_get(b"idx:scores", field)
        .await?,
      "RangeIndex 字段 {field:?} 在主从两端必须一致"
    );
  }

  OK
}

/// 重放臂（C# AofUpsertStoreCommitTaskRecoverTestAsync 的重启恢复断言；映射注释在 commitaof.rs 权威处）
#[compio::test]
async fn test_replay_wrapper_replay_from() -> Void {
  let (_dir1, store1, wal1) = open_test_node("replayer_primary")?;
  let primary = NodeService::with_wal(Arc::clone(&store1), Arc::clone(&wal1))?;

  primary.session().upsert(b"foo", b"bar").await?;
  wal1.commit().await?;

  let (_dir2, store2, wal2) = open_test_node("replayer_replica")?;
  let replica = NodeService::with_wal(Arc::clone(&store2), Arc::clone(&wal2))?;

  let count = primary.replay_into_session(replica.session()).await?;
  assert_eq!(count, 1);

  assert_eq!(replica.session().read(b"foo").await?, Some(b"bar".to_vec()));
  OK
}

/// 自述锚：direct_ricreate 写入日志并在副本端重放（rust 自定义命令，无 C# 对位）
#[compio::test]
async fn test_session_direct_ricreate_journals_and_replays() -> Void {
  let (_dir1, store1, wal1) = open_test_node("direct_primary")?;
  let primary = NodeService::with_wal(Arc::clone(&store1), Arc::clone(&wal1))?;

  // 通过 session() 直调 range_index_create（对标 RESP network_ricreate 路径）
  primary
    .session()
    .range_index_create(b"idx:direct", StorageBackendType::Disk, TUNE)
    .await?;
  primary
    .session()
    .range_index_set(b"idx:direct", b"field_1", b"value_0001")
    .await?;
  wal1.commit().await?;

  let (_dir2, store2, wal2) = open_test_node("direct_replica")?;
  let replica = NodeService::with_wal(Arc::clone(&store2), Arc::clone(&wal2))?;

  let replayed = primary.replay_into_session(replica.session()).await?;
  assert_eq!(replayed, 2);

  assert_eq!(
    replica
      .session()
      .range_index_get(b"idx:direct", b"field_1")
      .await?,
    Some(b"value_0001".to_vec())
  );
  OK
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofDeleteObjectStoreRecoverTestAsync
#[compio::test]
async fn test_replay_tolerance_on_missing_index() -> Void {
  let (_dir1, store1, wal1) = open_test_node("tolerance_primary")?;
  let primary = NodeService::with_wal(Arc::clone(&store1), Arc::clone(&wal1))?;

  // 手工写入两条针对不存在索引的 StoreRMW (RI.SET / RI.DEL) 条目
  // （GarnetLog::enqueue 唯一编码：物理键 + ReplayInput 载荷）
  let physical = |user: &[u8]| -> Vec<u8> {
    NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user)
      .as_slice()
      .to_vec()
  };
  for (cmd, args) in [
    (RespCommand::Riset, vec![b"f".to_vec(), b"v".to_vec()]),
    (RespCommand::Ridel, vec![b"f".to_vec()]),
  ] {
    let input = ReplayInput {
      cmd,
      flags: RespInputFlags::DETERMINISTIC.bits(),
      sub_id: 0,
      obj_type: 0,
      arg1: 0,
      arg2: 0,
      arg3: 0,
      args,
    };
    let mut serialized = Vec::new();
    input.serialize(&mut serialized);
    let _ = primary.aof().log().enqueue(&RecordShape {
      op_type: AofEntryType::StoreRMW,
      version: 0,
      session_id: 0,
      key: &physical(b"nonexistent"),
      value: &[],
      input: &serialized,
      database_id: 0,
    });
  }
  wal1.commit().await?;

  let (_dir2, store2, wal2) = open_test_node("tolerance_replica")?;
  let replica = NodeService::with_wal(Arc::clone(&store2), Arc::clone(&wal2))?;

  // 重放对标 Garnet `if (status != GarnetStatus.OK) return;` 幂等忽略 NotFound，不报错中止
  let replayed = primary.replay_into_session(replica.session()).await?;
  assert_eq!(replayed, 2);
  OK
}

/// 自述锚：range_index_drop 写入日志并在副本端重放（rust 自定义命令，无 C# 对位）
#[compio::test]
async fn test_range_index_drop_journals_and_replays() -> Void {
  let (_dir1, store1, wal1) = open_test_node("drop_primary")?;
  let primary = NodeService::with_wal(Arc::clone(&store1), Arc::clone(&wal1))?;

  // 创建索引并写入字段
  primary
    .session()
    .range_index_create(b"idx:to_drop", StorageBackendType::Disk, TUNE)
    .await?;
  primary
    .session()
    .range_index_set(b"idx:to_drop", b"field_1", b"val_1")
    .await?;
  assert!(primary.session().range_index_exists(b"idx:to_drop").await?);

  // 删除该 RangeIndex 键 (通过通用 session.delete 触发 drop listener 入流 StoreDelete)
  let dropped = primary.session().delete(b"idx:to_drop").await?;
  assert!(dropped, "删除已存在的 RangeIndex 必须返回 true");
  assert!(!primary.session().range_index_exists(b"idx:to_drop").await?);

  wal1.commit().await?;

  // 创建 Replica 节点并重放全部 AOF 流
  let (_dir2, store2, wal2) = open_test_node("drop_replica")?;
  let replica = NodeService::with_wal(Arc::clone(&store2), Arc::clone(&wal2))?;

  let replayed = primary.replay_into_session(replica.session()).await?;
  assert_eq!(replayed, 3, "必须包含 RiCreate + RiSet + Drop 三条条目");

  // 断言 Replica 端该 RangeIndex 亦已被清理
  assert!(
    !replica.session().range_index_exists(b"idx:to_drop").await?,
    "重放 StoreDelete 后副本端索引必须不存在"
  );
  OK
}

/// RI.DEL 删空自愈端到端主从一致（.agents/skills/transpile/SKILL.md 严格删空
/// 生命周期条 / doc/zh/collection.md 3.3）：主端逐字段删至计数归零即走树态排空
/// 回收单点，AOF 先后顺序固定为「整键墓碑 StoreDelete（RangeIndexDrop）→ 字段
/// 条目 RangeIndexWrite(delete=true)」，全新副本重放零失败且无幽灵空元记录、无
/// 残留字段与在线树（墓碑后到的字段条目命中 NotFound 由回放臂静默跳过）
#[compio::test]
async fn test_range_index_del_to_empty_replays_to_empty_replica() -> Void {
  let (_dir1, store1, wal1) = open_test_node("ri_empty_primary")?;
  let primary = NodeService::with_wal(Arc::clone(&store1), Arc::clone(&wal1))?;

  primary
    .session()
    .range_index_create(b"idx:empty", StorageBackendType::Disk, TUNE)
    .await?;
  primary
    .session()
    .range_index_set(b"idx:empty", b"field_1", b"val_1")
    .await?;
  primary
    .session()
    .range_index_set(b"idx:empty", b"field_2", b"val_2")
    .await?;

  // 非删空删除：主端索引存活、计数递减（回放臂同内核，零行为回归）
  assert!(
    primary
      .session()
      .range_index_del(b"idx:empty", b"field_1")
      .await?
  );
  assert_eq!(primary.session().range_index_count(b"idx:empty").await?, 1);

  // 删空：主端计数归零即自愈销毁整键，元记录与随键域一并消失
  assert!(
    primary
      .session()
      .range_index_del(b"idx:empty", b"field_2")
      .await?
  );
  assert!(!primary.session().range_index_exists(b"idx:empty").await?);
  assert!(primary.session().load_meta(b"idx:empty").await?.is_none());
  wal1.commit().await?;

  let (_dir2, store2, wal2) = open_test_node("ri_empty_replica")?;
  let replica = NodeService::with_wal(Arc::clone(&store2), Arc::clone(&wal2))?;
  let replayed = primary.replay_into_session(replica.session()).await?;
  assert_eq!(
    replayed, 6,
    "create + 2 set + 非删空 del + 删空整键墓碑 + 删空字段条目共六条"
  );

  // 副本与主端同态：整键消亡，绝无「EXISTS 报空而 KEYS 仍列出」的幽灵索引
  assert!(!replica.session().range_index_exists(b"idx:empty").await?);
  assert!(!replica.session().contains_key(b"idx:empty").await?);
  assert!(replica.session().load_meta(b"idx:empty").await?.is_none());
  assert!(matches!(
    replica.session().range_index_count(b"idx:empty").await,
    Err(RangeIndexError::NotFound)
  ));
  assert!(
    replica
      .session()
      .store
      .range_index
      .get_tree(&NamespaceDbCodec::encode_tagged_key(
        0,
        0,
        KeyTag::Meta,
        b"idx:empty",
      ))
      .is_none(),
    "删空墓碑重放后副本端树实例必须注销"
  );
  OK
}

/// 非零 (ns, db) 会话的 RI 创建/写入/删除端到端回放：主端在 (ns=5, db=3) 建 RI 并写/删字段，
/// 重放到全新副本后：副本默认 (0,0) 域必须无该索引（杜绝幽灵/跨租界穿透），
/// 切至 (5,3) 域方可见且字段完整——证明 AOF 记录携真实 ns/db 前缀寻址。
#[compio::test]
async fn test_range_index_replay_preserves_ns_db_domain() -> Void {
  let (_dir1, store1, wal1) = open_test_node("ri_domain_primary")?;
  let primary = NodeService::with_wal(Arc::clone(&store1), Arc::clone(&wal1))?;

  // 主端切至 (ns=5, db=3) 后建 RI、写字段、删字段（覆盖 Create/Write/Drop 三臂）
  assert!(primary.session().set_context(5, 3));
  primary
    .session()
    .range_index_create(b"ri:domain", StorageBackendType::Disk, TUNE)
    .await?;
  primary
    .session()
    .range_index_set(b"ri:domain", b"f_keep", b"v1")
    .await?;
  primary
    .session()
    .range_index_set(b"ri:domain", b"f_drop", b"v2")
    .await?;
  primary
    .session()
    .range_index_del(b"ri:domain", b"f_drop")
    .await?;
  assert!(primary.session().range_index_exists(b"ri:domain").await?);

  wal1.commit().await?;

  let (_dir2, store2, wal2) = open_test_node("ri_domain_replica")?;
  let replica = NodeService::with_wal(Arc::clone(&store2), Arc::clone(&wal2))?;
  let replayed = primary.replay_into_session(replica.session()).await?;
  assert!(replayed >= 4, "须含 Create + 两次 Write + Del 四条 RI 条目");

  // 副本默认域 (0,0)：绝不出现同名幽灵索引
  assert_eq!(replica.session().namespace(), 0);
  assert_eq!(replica.session().active_db(), 0);
  assert!(
    !replica.session().range_index_exists(b"ri:domain").await?,
    "非零 (ns,db) 的 RI 不得在副本 (0,0) 域凭空出现"
  );

  // 切至源域 (5,3)：索引完整恢复，被删字段消失、保留字段在位
  assert!(replica.session().set_context(5, 3));
  assert!(
    replica.session().range_index_exists(b"ri:domain").await?,
    "RI 应落在原 (ns=5, db=3) 域"
  );
  assert_eq!(
    replica.session().range_index_count(b"ri:domain").await?,
    1,
    "删空字段后计数应为 1（f_drop 已回收）"
  );
  assert_eq!(
    replica
      .session()
      .range_index_get(b"ri:domain", b"f_keep")
      .await?,
    Some(b"v1".to_vec()),
    "保留字段应随域恢复到副本"
  );
  assert_eq!(
    replica
      .session()
      .range_index_get(b"ri:domain", b"f_drop")
      .await?,
    None,
    "被删字段不得在副本复活"
  );
  OK
}

/// 解析 AofHeader 的操作类型（GarnetLog 写入端布局）
fn entry_op(payload: &[u8]) -> AofEntryType {
  // commit 元数据帧非 AOF 条目（重放/检视面跳过，与 AofProcessor 过滤一致）；
  // 此处直接报错暴露调用方漏滤
  assert!(!is_commit_frame(payload), "commit 帧不入条目检视面");
  let header = AofHeader::parse(payload).expect("header decodable");
  AofEntryType::try_from(header.op_type).expect("op known")
}

/// 解析条目 key（SpanByte 长度前缀）与 input 段
fn entry_key_input(payload: &[u8]) -> (Vec<u8>, ReplayInput) {
  let body = &payload[AofHeader::TOTAL_SIZE..];
  let key_len = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
  let key = body[4..4 + key_len].to_vec();
  let input = ReplayInput::deserialize(&body[4 + key_len..]).expect("input decodable");
  (key, input)
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofListObjectStoreRecoverTestAsync
#[compio::test]
async fn test_object_collection_wal_replay_e2e() -> Void {
  let (_dir1, store1, wal1) = open_test_node("obj_primary")?;
  let primary = NodeService::with_wal(Arc::clone(&store1), Arc::clone(&wal1))?;

  let mut sess = resp::resp_server_session::RespServerSession::default();
  let mut out = Vec::new();

  // 1. Hash operations: HSET f1, f2 then HDEL f1
  {
    let batch = primary.session().enter_batch();
    sess
      .hash_set(&[b"hash1", b"f1", b"v1", b"f2", b"v2"], &batch, &mut out)
      .unwrap();
    out.clear();
    sess
      .hash_delete(&[b"hash1", b"f1"], &batch, &mut out)
      .unwrap();
    out.clear();
  }

  // 2. Set operations: SADD m1, m2, m3 then SREM m2
  {
    let batch = primary.session().enter_batch();
    sess
      .set_add(&[b"set1", b"m1", b"m2", b"m3"], &batch, &mut out)
      .unwrap();
    out.clear();
    sess
      .set_remove(&[b"set1", b"m2"], &batch, &mut out)
      .unwrap();
    out.clear();
  }

  // 3. List operations: LPUSH e1, e2 then RPOP
  {
    let batch = primary.session().enter_batch();
    sess
      .list_push(&[b"list1", b"e1", b"e2"], &batch, &mut out, true)
      .unwrap();
    out.clear();
    sess.list_pop(&[b"list1"], &batch, &mut out, false).unwrap();
    out.clear();
  }

  // 4. SortedSet operations: ZADD z1, z2, z3 then ZREM z2
  {
    let batch = primary.session().enter_batch();
    sess
      .sorted_set_add(
        &[b"zset1", b"10.0", b"z1", b"20.0", b"z2", b"30.0", b"z3"],
        &batch,
        &mut out,
      )
      .unwrap();
    out.clear();
    sess
      .sorted_set_remove(&[b"zset1", b"z2"], &batch, &mut out)
      .unwrap();
    out.clear();
  }

  // Commit WAL
  wal1.commit().await?;

  // Verify WAL does not contain O(N) KvUpsert for object keys, only ObjectRmw
  let records: Vec<_> = scan_records(primary.aof())
    .into_iter()
    .filter(|r| !is_commit_frame(&r.payload))
    .collect();
  let mut obj_rmw_count = 0;
  for record in &records {
    let op = entry_op(&record.payload);
    assert_ne!(
      op,
      AofEntryType::StoreUpsert,
      "对象操作绝不能以 KvUpsert 写入 WAL"
    );
    if op == AofEntryType::ObjectStoreRMW {
      obj_rmw_count += 1;
    }
  }
  assert_eq!(
    obj_rmw_count, 8,
    "4 组操作（每组 2 次 RMW）应产生恰好 8 条 ObjectRmw"
  );

  // Create Replica node
  let (_dir2, store2, wal2) = open_test_node("obj_replica")?;
  let replica = NodeService::with_wal(Arc::clone(&store2), Arc::clone(&wal2))?;

  // Replay primary AOF into replica
  let replayed = primary.replay_into_session(replica.session()).await?;
  assert_eq!(replayed, 8, "必须重放全部 8 个对象变更条目");

  // Verify on Replica
  let rep_batch = replica.session().enter_batch();

  // Hash check: f1 was deleted, f2 is v2
  out.clear();
  sess
    .hash_get(&[b"hash1", b"f2"], &rep_batch, &mut out)
    .unwrap();
  assert_eq!(out, b"$2\r\nv2\r\n");
  out.clear();
  sess
    .hash_get(&[b"hash1", b"f1"], &rep_batch, &mut out)
    .unwrap();
  assert_eq!(out, b"$-1\r\n");

  // Set check: m1 and m3 exist, m2 removed
  out.clear();
  sess
    .set_is_member(&[b"set1", b"m1"], &rep_batch, &mut out)
    .unwrap();
  assert_eq!(out, b":1\r\n");
  out.clear();
  sess
    .set_is_member(&[b"set1", b"m2"], &rep_batch, &mut out)
    .unwrap();
  assert_eq!(out, b":0\r\n");
  out.clear();
  sess
    .set_is_member(&[b"set1", b"m3"], &rep_batch, &mut out)
    .unwrap();
  assert_eq!(out, b":1\r\n");

  // List check: length is 1, remaining element is e2
  out.clear();
  sess.list_length(&[b"list1"], &rep_batch, &mut out).unwrap();
  assert_eq!(out, b":1\r\n");

  // SortedSet check: z1 and z3 exist with correct scores, z2 removed
  out.clear();
  sess
    .sorted_set_score(&[b"zset1", b"z1"], &rep_batch, &mut out)
    .unwrap();
  assert_eq!(out, b"$2\r\n10\r\n");
  out.clear();
  sess
    .sorted_set_score(&[b"zset1", b"z2"], &rep_batch, &mut out)
    .unwrap();
  assert_eq!(out, b"$-1\r\n");
  out.clear();
  sess
    .sorted_set_score(&[b"zset1", b"z3"], &rep_batch, &mut out)
    .unwrap();
  assert_eq!(out, b"$2\r\n30\r\n");

  OK
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofRMWObjectStoreRecoverTestAsync
#[compio::test]
async fn test_object_wal_write_amplification() -> Void {
  let (_dir, store, wal) = open_test_node("amp_primary")?;
  let primary = NodeService::with_wal(Arc::clone(&store), Arc::clone(&wal))?;

  let mut sess = resp::resp_server_session::RespServerSession::default();
  let mut out = Vec::new();

  // 1. 建立一个含 100 个字段的大 Hash
  let mut args: Vec<&[u8]> = vec![b"bighash"];
  let fields: Vec<(Vec<u8>, Vec<u8>)> = (0..100)
    .map(|i| {
      (
        format!("field_{i:04}").into_bytes(),
        format!("val_{i:04}").into_bytes(),
      )
    })
    .collect();
  for (f, v) in &fields {
    args.push(f.as_slice());
    args.push(v.as_slice());
  }

  {
    let batch = primary.session().enter_batch();
    sess.hash_set(&args, &batch, &mut out).unwrap();
  }
  wal.commit().await?;

  // 记录当前已写条目数（commit 元数据帧自滤）
  let records: Vec<_> = scan_records(primary.aof())
    .into_iter()
    .filter(|r| !is_commit_frame(&r.payload))
    .collect();
  assert_eq!(records.len(), 1, "初次创建大 Hash 仅一条 ObjectRmw");

  // 2. 单独增量追加一个字段
  out.clear();
  {
    let batch = primary.session().enter_batch();
    sess
      .hash_set(&[b"bighash", b"new_field", b"new_val"], &batch, &mut out)
      .unwrap();
  }
  wal.commit().await?;

  // 3. 扫描第二条条目，验证其不是 O(N) 的全量对象 dump，而是增量 ObjectRmw 帧
  let records: Vec<_> = scan_records(primary.aof())
    .into_iter()
    .filter(|r| !is_commit_frame(&r.payload))
    .collect();
  let second = records.get(1).expect("second record must exist");
  assert_eq!(
    entry_op(&second.payload),
    AofEntryType::ObjectStoreRMW,
    "条目必须为增量 ObjectRmw 而非全量 KvUpsert"
  );
  let (key, frame) = entry_key_input(&second.payload);
  assert!(key.ends_with(b"bighash"));
  assert_eq!(frame.obj_type, GarnetObjectType::Hash as u8);

  // 验证载荷大小：单字段 RMW 帧应小于 100 字节，绝不能是上千字节的完整对象 blob
  assert!(
    second.payload.len() < 128,
    "单字段变更 WAL 条目应极小（O(1)），实际大小: {}",
    second.payload.len()
  );
  assert_eq!(frame.args, vec![b"new_field".to_vec(), b"new_val".to_vec()]);

  OK
}

/// 闭包收集扫描（等价旧 scan_single Vec 面，测试专用）
fn scan_records(aof: &GarnetAppendOnlyFile) -> Vec<waof::WalRecord> {
  let mut records = Vec::new();
  aof.log().scan_single_with(0, 0, i64::MAX, |rec| {
    records.push(rec.clone());
    true
  });
  records
}
