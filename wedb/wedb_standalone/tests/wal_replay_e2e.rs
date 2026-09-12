//! WAL Journaling 与 Replay 闭环端到端集成测试
//!
//! 验证（单一机制：GarnetLog 唯一编码 + AofProcessor 唯一重放）：
//! 1. 统一端口制 WAL Journaling：KV 写入/删除、RangeIndex 创建/设置/删除自动进入 WAL
//! 2. WAL 提交与物理持久化
//! 3. Replay 闭环：replay_into_session（AofProcessor）把 AOF 流重放到全新实例
//! 4. 断言主从/新旧实例数据完全一致

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use waof::{AofEntryType, WalConfig, WalLog};
use wdev::SegmentedDevice;
use wkv::{StorageBackend, StoreConfig, TreeTuning, WedbStore};
use wnode::{
  aof::{AofHeader, ReplayInput, garnet_log::RecordShape},
  resp,
  service::NodeService,
  types::{GarnetObjectType, RespCommand, RespInputFlags},
};

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
#[test]
fn test_wal_replay_e2e_closure() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
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
      .ri_create(b"idx:scores", StorageBackend::Std, TUNE)
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
    while let Some(_record) = scan.next().await? {
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
  })
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofUpsertStoreCommitTaskRecoverTestAsync
#[test]
fn test_replay_wrapper_replay_from() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
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
  })
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIIsOpenBasicTest
#[test]
fn test_session_direct_ricreate_journals_and_replays() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir1, store1, wal1) = open_test_node("direct_primary")?;
    let primary = NodeService::with_wal(Arc::clone(&store1), Arc::clone(&wal1))?;

    // 通过 session() 直调 range_index_create（对标 RESP network_ricreate 路径）
    primary
      .session()
      .range_index_create(b"idx:direct", StorageBackend::Std, TUNE)
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
  })
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofDeleteObjectStoreRecoverTestAsync
#[test]
fn test_replay_tolerance_on_missing_index() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir1, store1, wal1) = open_test_node("tolerance_primary")?;
    let primary = NodeService::with_wal(Arc::clone(&store1), Arc::clone(&wal1))?;

    // 手工写入两条针对不存在索引的 StoreRMW (RI.SET / RI.DEL) 条目
    // （GarnetLog::enqueue 唯一编码：物理键 + ReplayInput 载荷）
    let physical = |user: &[u8]| -> Vec<u8> {
      wval::NamespaceDbCodec::encode_tagged_key(0, 0, wkv::KeyTag::String, user)
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
      primary.aof().log().enqueue(&RecordShape {
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
  })
}

/// test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIDropBasicTest
#[test]
fn test_range_index_drop_journals_and_replays() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir1, store1, wal1) = open_test_node("drop_primary")?;
    let primary = NodeService::with_wal(Arc::clone(&store1), Arc::clone(&wal1))?;

    // 创建索引并写入字段
    primary
      .session()
      .range_index_create(b"idx:to_drop", StorageBackend::Std, TUNE)
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
  })
}

/// 解析 AofHeader 的操作类型（GarnetLog 写入端布局）
fn entry_op(payload: &[u8]) -> AofEntryType {
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
#[test]
fn test_object_collection_wal_replay_e2e() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
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
    let records = primary.aof().log().scan_single(0, 0, i64::MAX);
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
  })
}

/// test/standalone/Garnet.test.scripting/RespAofTests.cs:AofRMWObjectStoreRecoverTestAsync
#[test]
fn test_object_wal_write_amplification() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
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

    // 记录当前已写条目数
    let records = primary.aof().log().scan_single(0, 0, i64::MAX);
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
    let records = primary.aof().log().scan_single(0, 0, i64::MAX);
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
  })
}
