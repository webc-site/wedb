//! 存储 API 域集成测试（wkv 临时文件库驱动 StorageSession / 数据库管理器全链路）
//!
//! 覆盖：字符串族、条件写、位图、HLL、LCS、对象四类型（含 WRONGTYPE 与空回收）、
//! TTL / SCAN / 槽位删除、WATCH 登记、单库与多库管理器（检查点 / AOF 重放 / 清空）。

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wobject::{hash::hash_object::HashObject, list::list_object::OperationDirection};
use wserver::{
  api::garnet_status::GarnetStatus,
  databases::{
    database_manager_base::{AOF_OP_UPSERT, enqueue_record},
    database_manager_factory::{DatabaseManager, DatabaseManagerFactory},
    garnet_database::GarnetDatabase,
    i_database_manager::IDatabaseManager,
  },
  storage::session::{
    common::array_key_iteration_functions::cluster_slot,
    mainstore::{advanced_ops::StringRMWOp, bitmap_ops::BitFieldOp},
    objectstore::{sorted_set_geo_ops::GeoCenter, sorted_set_ops::ZSetAggregate},
    storage_session::StorageSession,
  },
};

type TestStore = Arc<WedbStore<SegmentedDevice>>;

/// 打开临时文件库
fn open_store(tag: &str) -> aok::Result<(TempDir, TestStore)> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag))?);
  let config = StoreConfig::new(16384, 65536, 64, 0.5)?;
  let store = Arc::new(WedbStore::open(config, device)?);
  Ok((dir, store))
}

/// 创建会话（批处理纪元内）
fn storage_session<'s, D: wdev::Device>(
  session: &'s wkv::StoreSession<D>,
) -> StorageSession<'s, D> {
  StorageSession::new(session.enter_batch())
}

/// 字符串族 + 位图 + HLL + LCS 全链路
#[test]
fn test_string_and_bitmap_and_hll() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_store("str.db")?;
    let session = store.new_session()?;
    let ss = storage_session(&session);

    // SET / GET / APPEND / SETRANGE / GETRANGE
    ss.upsert_string(b"k", b"hello").await?;
    assert_eq!(ss.read_string(b"k").await?, Some(b"hello".to_vec()));
    let (s, len) = ss.append(b"k", b" world").await?;
    assert_eq!(s, GarnetStatus::Ok);
    assert_eq!(len, 11);
    let (s, len) = ss.setrange(b"k", 6, b"WORLD").await?;
    assert_eq!((s, len), (GarnetStatus::Ok, 11));
    assert_eq!(ss.getrange(b"k", 0, -1).await?, b"hello WORLD".to_vec());

    // 条件写 NX / XX
    let (s, _) = ss.set_conditional(b"nx", b"v", true, false, false).await?;
    assert_eq!(s, GarnetStatus::Ok);
    let (s, _) = ss.set_conditional(b"nx", b"v2", true, false, false).await?;
    assert_eq!(s, GarnetStatus::NotFound);
    let (s, _) = ss.set_conditional(b"nx", b"v3", false, true, false).await?;
    assert_eq!(s, GarnetStatus::Ok);

    // INCR 族（RMW 分发）
    let (s, v) = ss
      .rmw_main_store(b"cnt", StringRMWOp::Incr { delta: 5 })
      .await?;
    assert_eq!((s, v), (GarnetStatus::Ok, Some(5)));
    let (s, v) = ss
      .rmw_main_store(b"cnt", StringRMWOp::Incr { delta: -2 })
      .await?;
    assert_eq!((s, v), (GarnetStatus::Ok, Some(3)));

    // SETBIT / GETBIT / BITCOUNT
    ss.string_set_bit(b"bm", 0, 1).await?;
    let (s, bit) = ss.string_get_bit(b"bm", 0).await?;
    assert_eq!((s, bit), (GarnetStatus::Ok, 1));
    let (_, n) = ss.string_bit_count(b"bm", 0, -1, false).await?;
    assert_eq!(n, 1);

    // BITFIELD：u8 位宽溢出 WRAP
    let (_, out) = ss
      .string_bit_field(
        b"bf",
        &[BitFieldOp::Set {
          is_signed: false,
          bits: 8,
          offset: 0,
          value: 300,
          wrap: true,
          sat: false,
        }],
      )
      .await?;
    assert_eq!(out, vec![Some(44)]); // 300 & 0xFF

    // PFADD / PFCOUNT / PFMERGE（基数估计的量级正确性）
    for i in 0..1000u32 {
      ss.hyper_log_log_add(b"hll", &[i.to_le_bytes().as_slice()])
        .await?;
    }
    let (_, count) = ss.hyper_log_log_length(b"hll").await?;
    assert!((900.0..=1100.0).contains(&count), "HLL 估计 {count}");

    // LCS
    ss.upsert_string(b"a", b"abcde").await?;
    ss.upsert_string(b"b", b"ace").await?;
    let (s, r) = ss.lcs(b"a", b"b").await?;
    assert_eq!(s, GarnetStatus::Ok);
    let (total, _) = r.unwrap();
    assert_eq!(total, 3); // "ace"

    Ok(())
  })
}

/// 对象四类型：哈希 / 集合 / 列表 / 有序集合（含 WRONGTYPE 与 GEO 检索）
#[test]
fn test_object_stores() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_store("obj.db")?;
    let session = store.new_session()?;
    let ss = storage_session(&session);

    // 哈希：HSET / HGET / HDEL / 空回收
    let (_, added) = ss
      .hash_set(
        b"h",
        &[
          (b"f1".as_slice(), b"v1".as_slice()),
          (b"f2".as_slice(), b"v2".as_slice()),
        ],
        false,
      )
      .await?;
    assert_eq!(added, 2);
    let (s, v) = ss.hash_get(b"h", b"f1").await?;
    assert_eq!(
      (s, v.as_deref()),
      (GarnetStatus::Ok, Some(b"v1".as_slice()))
    );
    let (_, removed) = ss.hash_delete(b"h", &[b"f1", b"f2"]).await?;
    assert_eq!(removed, 2);
    assert_eq!(ss.hash_length(b"h").await?, (GarnetStatus::Ok, 0)); // 键已回收

    // WRONGTYPE：字符串值上执行哈希读
    ss.upsert_string(b"s", b"plain").await?;
    assert_eq!(
      ss.hash_get(b"s", b"f").await?,
      (GarnetStatus::WrongType, None)
    );

    // 集合：SADD / SINTER / SDIFF
    // SADD 返回新增成员数：已有成员不计入（对齐 Redis）
    assert_eq!(
      ss.set_add(b"sa", &[b"1", b"2", b"3"]).await?,
      (GarnetStatus::Ok, 3)
    );
    assert_eq!(
      ss.set_add(b"sa", &[b"2", b"4"]).await?,
      (GarnetStatus::Ok, 1)
    );
    ss.set_add(b"sb", &[b"2", b"3", b"4"]).await?;
    let (_, inter) = ss.set_intersect(&[b"sa", b"sb"]).await?;
    assert_eq!(inter.len(), 3);
    let (_, diff) = ss.set_diff(&[b"sa", b"sb"]).await?;
    assert_eq!(diff, vec![b"1".to_vec()]);
    // SUNIONSTORE / SDIFFSTORE 返回结果集基数
    assert_eq!(
      ss.set_union_store(b"su", &[b"sa", b"sb"]).await?,
      (GarnetStatus::Ok, 4)
    );
    assert_eq!(
      ss.set_diff_store(b"sd", &[b"sa", b"sb"]).await?,
      (GarnetStatus::Ok, 1)
    );

    // 列表：LPUSH / LRANGE / LPOP / 弹空回收
    ss.list_push(b"l", &[b"b", b"a"], OperationDirection::Left, false)
      .await?;
    assert_eq!(ss.list_length(b"l").await?, (GarnetStatus::Ok, 2));
    let (_, range) = ss.list_range(b"l", 0, -1).await?;
    assert_eq!(range, vec![b"a".to_vec(), b"b".to_vec()]);
    ss.list_pop_multiple(b"l", 10, OperationDirection::Left)
      .await?;
    assert_eq!(ss.list_length(b"l").await?, (GarnetStatus::Ok, 0));

    // 有序集合：ZADD / ZRANGE / ZRANK / ZPOPMIN / 交集
    let members = [
      (b"m1".as_slice(), 1.0),
      (b"m2".as_slice(), 2.0),
      (b"m3".as_slice(), 3.0),
    ];
    ss.sorted_set_add(b"z", &members, false, false, false, false)
      .await?;
    let (_, top) = ss.sorted_set_range(b"z", 0, 0, false, true).await?;
    assert_eq!(top, vec![(b"m1".to_vec(), Some(1.0))]);
    assert_eq!(
      ss.sorted_set_rank(b"z", b"m3", false).await?,
      (GarnetStatus::Ok, Some(2))
    );
    ss.sorted_set_add(
      b"z2",
      &[(b"m2".as_slice(), 2.5)],
      false,
      false,
      false,
      false,
    )
    .await?;
    let (_, inter) = ss
      .sorted_set_intersect(&[b"z", b"z2"], &[1.0, 1.0], ZSetAggregate::Sum)
      .await?;
    assert_eq!(inter, vec![(b"m2".to_vec(), 4.5)]);
    let (_, popped) = ss.sorted_set_pop(b"z", 1, true).await?;
    assert_eq!(popped, vec![(b"m1".to_vec(), 1.0)]);

    // GEO：半径检索（巴黎 ↔ 伦敦 ~344km，柏林 ~878km）
    let cities = [
      (2.35, 48.85, &b"paris"[..]),
      (-0.12, 51.5, &b"london"[..]),
      (13.4, 52.52, &b"berlin"[..]),
    ];
    ss.geo_add(b"geo", &cities, false, false).await?;
    let (_, hits) = ss
      .geo_search_read_only(b"geo", GeoCenter::Member(b"paris"), 500_000.0)
      .await?;
    assert_eq!(hits.len(), 2, "巴黎 500km 内应含伦敦");
    assert_eq!(hits[0].0, b"paris".to_vec());

    Ok(())
  })
}

/// TTL / SCAN / KEYS / DBSIZE / 槽位删除 / WATCH 登记
#[test]
fn test_ttl_scan_watch() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_store("scan.db")?;
    let session = store.new_session()?;
    let ss = storage_session(&session);

    for k in ["user:1", "user:2", "order:1"] {
      ss.upsert_string(k.as_bytes(), b"v").await?;
    }
    // 带过期键：100ms 后到期
    ss.upsert_string(b"user:tmp", b"v").await?;
    ss.expire_in_ms(b"user:tmp", 50).await?;
    assert!(ss.pttl_ms(b"user:tmp").await? > 0);

    assert_eq!(ss.db_size().await?, 4);
    let (_, keys) = ss.db_scan(b"user:*", false, b"", 10).await?;
    assert_eq!(keys.len(), 3);

    // PERSIST 与到期
    ss.persist_key(b"user:tmp").await?;
    assert_eq!(ss.pttl_ms(b"user:tmp").await?, -1);

    // 槽位删除：先算出 user:1 的槽位再删除
    let slot = cluster_slot(b"user:1");
    let deleted = ss.delete_slot_keys(&[slot]).await?;
    assert_eq!(deleted, 1);

    // WATCH 登记
    ss.watch_key(b"user:2");
    assert!(ss.watched_version(b"user:2").is_some());
    assert!(ss.watched_version(b"absent").is_none());
    ss.clear_watches();
    assert!(ss.watched_version(b"user:2").is_none());

    // UNIFIED：EXISTS / RENAMENX
    assert_eq!(ss.exists(b"user:2").await?, GarnetStatus::Ok);
    let (s, n) = ss.renamenx(b"user:2", b"user:renamed").await?;
    assert_eq!((s, n), (GarnetStatus::Ok, 1));
    assert_eq!(ss.exists(b"user:2").await?, GarnetStatus::NotFound);
    Ok(())
  })
}

/// 单库管理器：检查点落盘 + AOF 重放 + 清空
#[test]
fn test_single_database_manager() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store) = open_store("single.db")?;
    let aof_device = Arc::new(SegmentedDevice::single_file(dir.path().join("aof.db"))?);
    let aof = Arc::new(waof::WalLog::new(aof_device, waof::WalConfig::default())?);
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      dir.path().to_path_buf(),
      Some(aof),
    ));
    let manager = DatabaseManagerFactory::create_database_manager(
      false,
      Arc::clone(&store),
      dir.path().to_path_buf(),
      Arc::clone(&db),
    );
    assert!(!DatabaseManagerFactory::should_create_multiple_database_manager(1));
    assert!(DatabaseManagerFactory::should_create_multiple_database_manager(2));

    let DatabaseManager::Single(single) = &manager else {
      panic!("单库模式应构造 Single"); // 测试断言
    };

    // 写入 + 入队 AOF
    let session = store.new_session()?;
    session.upsert(b"rk", b"rv").await?;
    enqueue_record(
      &db,
      waof::AofEntryType::MainStoreStoreCommand,
      AOF_OP_UPSERT,
      b"rk",
      Some(b"rv"),
    )?;

    // 检查点落盘
    assert!(single.take_checkpoint(true).await?);
    assert!(
      wkv::CheckpointManager::<SegmentedDevice>::find_latest_checkpoint(dir.path())?.is_some()
    );

    // 清空后 AOF 重放恢复
    single.flush_database().await?;
    assert_eq!(session.read(b"rk").await?, None);
    let replayed = single.recover_aof_async().await?;
    assert_eq!(replayed, 1);
    assert_eq!(session.read(b"rk").await?, Some(b"rv".to_vec()));

    // 多库兼容门槛
    assert!(!single.try_swap_databases(0, 1).await);
    assert_eq!(manager.database_count(), 1);
    assert!(manager.try_default_database().is_some());
    Ok(())
  })
}

/// 多库管理器：按需建库 / SWAPDB / 快照
#[test]
fn test_multi_database_manager() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store) = open_store("multi.db")?;
    let manager = DatabaseManagerFactory::create_database_manager(
      true,
      Arc::clone(&store),
      dir.path().to_path_buf(),
      Arc::new(GarnetDatabase::new(
        0,
        Arc::clone(&store),
        Arc::clone(&store.device),
        dir.path().join("0"),
        None,
      )),
    );
    let DatabaseManager::Multi(multi) = &manager else {
      panic!("多库模式应构造 Multi"); // 测试断言
    };

    // 前缀隔离：db0 与 db1 同键不同值
    let (db0, added0) = manager
      .try_default_database()
      .map_or((None, false), |d| (Some(d), false));
    let _ = added0;
    let db0 = db0.unwrap();
    let session = store.new_session()?;
    session.set_active_db(0);
    session.upsert(b"k", b"zero").await?;
    let (db1, added1) = multi.try_get_or_add_database(1).await?;
    assert!(added1);
    session.set_active_db(1);
    assert_eq!(session.read(b"k").await?, None); // db1 看不见 db0 的键
    session.upsert(b"k", b"one").await?;
    session.set_active_db(0);
    assert_eq!(session.read(b"k").await?, Some(b"zero".to_vec()));
    let _ = (db0, db1);

    // SWAPDB
    assert!(multi.try_swap_databases(0, 1).await);
    session.set_active_db(0);
    assert_eq!(session.read(b"k").await?, Some(b"one".to_vec()));

    // 全库清空
    use wserver::databases::i_database_manager::IDatabaseManager;
    multi.flush_all_databases().await?;
    assert_eq!(session.read(b"k").await?, None);
    Ok(())
  })
}

/// 已持久化库编号枚举的错误语义（对标上游 d20d63993 对
/// libs/server/Databases/MultiDatabaseManager.cs:TryGetSavedDatabaseIds 的恢复可见性修复）：
/// 根目录不存在为良性全新启动态（空集，恢复静默跳过，上游 `Directory.Exists` 守卫）；
/// 真实枚举失败必须显式报错，绝不静默空集恢复
#[test]
fn test_multi_saved_database_ids_error_semantics() -> aok::Void {
  use std::fs;

  use wserver::databases::multi_database_manager::MultiDatabaseManager;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store) = open_store("ids.db")?;

    // 1. 根目录不存在：良性空集，recover_checkpoint_async 静默成功且不注册任何库
    let manager = MultiDatabaseManager::new(Arc::clone(&store), dir.path().join("checkpoints"));
    assert_eq!(manager.try_get_saved_database_ids()?, Vec::<i64>::new());
    manager.recover_checkpoint_async(false, None).await?;
    assert!(manager.get_databases_snapshot().is_empty());

    // 2. 根目录路径被普通文件占用：枚举真实失败，须显式报错而非静默空集
    let blocked = dir.path().join("blocked");
    fs::write(&blocked, b"not a directory")?;
    let manager = MultiDatabaseManager::new(Arc::clone(&store), blocked);
    assert!(manager.try_get_saved_database_ids().is_err());
    assert!(manager.recover_checkpoint_async(false, None).await.is_err());
    Ok(())
  })
}

/// 哈希对象载荷兼容（wobject 序列化往返）
#[test]
fn test_wobject_roundtrip() -> aok::Void {
  use std::io::Cursor;
  let obj = HashObject::new();
  obj.hash.pin().insert(b"f".to_vec(), b"v".to_vec());
  let mut bytes = Vec::new();
  obj.serialize(&mut bytes)?;
  let back = HashObject::deserialize(&mut Cursor::new(bytes))?;
  assert_eq!(
    back.hash.pin().get(b"f".as_slice()).map(|v| v.as_slice()),
    Some(b"v".as_slice())
  );
  Ok(())
}

/// review r1 回归：SCAN 游标推进 / 弹空与删空保留结果 / 排他端点 /
/// WRONGTYPE 传播 / 空结果不物化 / BITCOUNT 空值 / WATCH 登记语义
#[test]
fn test_review_r1_regressions() -> aok::Void {
  use wserver::{api::i_garnet_api::IGarnetApi, storage::session::storage_session::StoreType};

  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_store("r1.db")?;
    let session = store.new_session()?;
    let ss = storage_session(&session);

    // ---- SCAN 游标推进：分页必须前进且终止（修复游标死循环） ----
    ss.set_add(b"set", &[b"a", b"b", b"c", b"d"]).await?;
    let mut cursor = Vec::new();
    let mut got: Vec<Vec<u8>> = Vec::new();
    for _ in 0..10 {
      let (s, c, page) = IGarnetApi::set_scan(&ss, b"set", &cursor, b"", 2).await?;
      assert_eq!(s, GarnetStatus::Ok);
      got.extend(page);
      if c.is_empty() {
        break;
      }
      cursor = c;
    }
    let mut want = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()];
    want.sort();
    assert_eq!(got, want, "SSCAN 三页应收全 4 成员");

    // ZSCAN 同链路分页
    let zm = [
      (b"z1".as_slice(), 1.0),
      (b"z2".as_slice(), 2.0),
      (b"z3".as_slice(), 3.0),
    ];
    ss.sorted_set_add(b"zscan", &zm, false, false, false, false)
      .await?;
    let (s, c1, p1) = IGarnetApi::sorted_set_scan(&ss, b"zscan", b"", b"", 2).await?;
    assert_eq!((s, p1.len()), (GarnetStatus::Ok, 2));
    let (s, c2, p2) = IGarnetApi::sorted_set_scan(&ss, b"zscan", &c1, b"", 2).await?;
    assert_eq!((s, p2.len(), c2.is_empty()), (GarnetStatus::Ok, 1, true));

    // db_scan：游标键在两页之间被删仍可续扫
    for k in ["k1", "k2", "k3"] {
      ss.upsert_string(k.as_bytes(), b"v").await?;
    }
    let (c1, p1) = ss.db_scan(b"k*", false, b"", 1).await?;
    assert_eq!(p1, vec![b"k1".to_vec()]);
    ss.delete_string(b"k1").await?;
    let (c2, p2) = ss.db_scan(b"k*", false, &c1, 1).await?;
    assert_eq!((p2, c2), (vec![b"k2".to_vec()], b"k2".to_vec()));

    // ---- 弹空 / 删空：返回值不丢失 ----
    ss.list_push(b"l1", &[b"x"], OperationDirection::Left, false)
      .await?;
    let (s, v) = ss.list_pop(b"l1", OperationDirection::Left).await?;
    assert_eq!((s, v), (GarnetStatus::Ok, Some(b"x".to_vec())));
    assert_eq!(ss.list_length(b"l1").await?, (GarnetStatus::Ok, 0));

    ss.sorted_set_add(b"zp", &[(b"m".as_slice(), 1.0)], false, false, false, false)
      .await?;
    let (s, popped) = ss.sorted_set_pop(b"zp", 1, true).await?;
    assert_eq!(
      (s, popped),
      (GarnetStatus::Ok, vec![(b"m".to_vec(), 1.0)]),
      "ZPOPMIN 弹空须回吐被弹成员"
    );
    assert_eq!(ss.exists(b"zp").await?, GarnetStatus::NotFound);

    // ZINCRBY：增量语义（修复 wobject Zincrby no-op）
    ss.sorted_set_add(b"zi", &[(b"m".as_slice(), 1.0)], false, false, false, false)
      .await?;
    let (s, v) = ss.sorted_set_increment(b"zi", b"m", 4.0).await?;
    assert_eq!((s, v), (GarnetStatus::Ok, Some(5.0)));
    assert_eq!(ss.sorted_set_score(b"zi", b"m").await?.1, Some(5.0));

    ss.sorted_set_add(b"zr", &[(b"m".as_slice(), 1.0)], false, false, false, false)
      .await?;
    let (_, n) = ss.sorted_set_remove(b"zr", &[b"m"]).await?;
    assert_eq!(n, 1, "ZREM 删空须回吐真实计数");

    let (_, n) = ss.list_remove(b"lr", b"a", 0).await?;
    // lr 键不存在：0
    assert_eq!(n, 0);

    // ---- ZREMRANGEBYSCORE 排他端点 ----
    ss.sorted_set_add(
      b"zex",
      &[(b"a".as_slice(), 5.0), (b"b".as_slice(), 6.0)],
      false,
      false,
      false,
      false,
    )
    .await?;
    let (s, n) = ss
      .sorted_set_remove_range_by_score(b"zex", b"(5", b"+inf")
      .await?;
    assert_eq!((s, n), (GarnetStatus::Ok, 1), "(5 排他：5.0 成员保留");
    assert_eq!(
      ss.sorted_set_score(b"zex", b"a").await?,
      (GarnetStatus::Ok, Some(5.0))
    );

    // ---- BITCOUNT 空值不 panic ----
    ss.upsert_string(b"empty", b"").await?;
    assert_eq!(
      ss.string_bit_count(b"empty", 0, -1, false).await?,
      (GarnetStatus::Ok, 0)
    );

    // ---- WRONGTYPE 传播 ----
    ss.upsert_string(b"str", b"plain").await?;
    let (s, _) = ss
      .hash_set(b"str", &[(b"f".as_slice(), b"v".as_slice())], false)
      .await?;
    assert_eq!(s, GarnetStatus::WrongType, "HSET 错误类型键须 WRONGTYPE");

    ss.hash_set(b"h2", &[(b"num".as_slice(), b"abc".as_slice())], false)
      .await?;
    let (s, _) = ss.hash_increment(b"h2", b"num", b"1", false).await?;
    assert_eq!(s, GarnetStatus::WrongType, "非数值字段增减须 WRONGTYPE");
    assert_eq!(
      ss.hash_get(b"h2", b"num").await?.1,
      Some(b"abc".to_vec()),
      "非数值字段不得被覆盖"
    );

    ss.set_add(b"st", &[b"x"]).await?;
    let (s, _) = ss.set_intersect(&[b"st", b"str"]).await?;
    assert_eq!(s, GarnetStatus::WrongType, "SINTER 错误类型键须 WRONGTYPE");

    // ---- *STORE 空结果不物化空对象键 ----
    ss.upsert_string(b"dest", b"old").await?;
    let (s, n) = ss.set_union_store(b"dest", &[b"no_such_set"]).await?;
    assert_eq!((s, n), (GarnetStatus::Ok, 0));
    assert_eq!(
      ss.exists(b"dest").await?,
      GarnetStatus::NotFound,
      "空并集须回收目标键"
    );

    // ---- HRANDFIELD 负计数：|count| 个、可重复、WITHVALUES 带值 ----
    // （对齐 C# HashObjectImpl.HashRandomField / Redis：WITHVALUES 对负计数同样生效）
    ss.hash_set(
      b"h3",
      &[
        (b"f1".as_slice(), b"1".as_slice()),
        (b"f2".as_slice(), b"2".as_slice()),
      ],
      false,
    )
    .await?;
    let (_, out) = ss.hash_random_field(b"h3", -5, true).await?;
    assert_eq!(out.len(), 5);
    assert!(
      out
        .iter()
        .all(|(k, v)| v.is_some() && (k.as_slice() == b"f1" || k.as_slice() == b"f2"))
    );

    // ---- LTRIM 裁剪至空：Ok + 整键回收 ----
    ss.list_push(b"l2", &[b"a"], OperationDirection::Left, false)
      .await?;
    assert_eq!(ss.list_trim(b"l2", 5, 10).await?, GarnetStatus::Ok);
    assert_eq!(ss.list_length(b"l2").await?, (GarnetStatus::Ok, 0));

    // ---- ZUNION 结果按 (score, member) 排名序 ----
    ss.sorted_set_add(
      "zu".as_ref(),
      &[(b"low".as_slice(), 1.0), (b"high".as_slice(), 9.0)],
      false,
      false,
      false,
      false,
    )
    .await?;
    let (_, u) = ss
      .sorted_set_union(&[b"zu"], &[1.0], ZSetAggregate::Sum)
      .await?;
    assert_eq!(u.first().map(|(m, _)| m.clone()), Some(b"low".to_vec()));
    assert_eq!(u.last().map(|(m, _)| m.clone()), Some(b"high".to_vec()));

    // ---- WATCH：C# void 语义，任意 StoreType 均登记 ----
    IGarnetApi::watch(&ss, b"k1", StoreType::None);
    assert!(ss.watched_version(b"k1").is_some());
    ss.clear_watches();

    Ok(())
  })
}

/// review r10 回归：WATCH 校验 / BITFIELD 跨字节全域 / ZMPOP WRONGTYPE 传播 /
/// 写路径去双读后的 WRONGTYPE 与缺键不物化
#[test]
fn test_review_r10_regressions() -> aok::Void {
  use wserver::api::i_garnet_api::IGarnetApi;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_store("r10.db")?;
    let session = store.new_session()?;
    let ss = storage_session(&session);

    // ---- WATCH：validate_watch_version 版本代理语义 ----
    ss.watch_key(b"wk");
    assert!(ss.validate_watch_version(), "登记后无写入应无冲突");
    ss.upsert_string(b"other", b"v").await?;
    assert!(!ss.validate_watch_version(), "任意写入推进尾地址即判冲突");
    ss.clear_watches();
    assert!(ss.validate_watch_version(), "空监视表恒无冲突");

    // ---- BITFIELD：SET 跨越缓冲末端不截断 ----
    let (_, out) = ss
      .string_bit_field(
        b"bf16",
        &[BitFieldOp::Set {
          is_signed: false,
          bits: 16,
          offset: 0,
          value: 300,
          wrap: false,
          sat: false,
        }],
      )
      .await?;
    assert_eq!(out, vec![Some(300)]);
    // 全域读回
    let (_, out) = ss
      .string_bit_field(
        b"bf16",
        &[BitFieldOp::Get {
          is_signed: false,
          bits: 16,
          offset: 0,
        }],
      )
      .await?;
    assert_eq!(out, vec![Some(300)]);
    // 部分跨越：缺失位按 0 参与（0x01 0x2C 位 4..12 = 0b0001_0010）
    let (_, out) = ss
      .string_bit_field(
        b"bf16",
        &[BitFieldOp::Get {
          is_signed: false,
          bits: 8,
          offset: 4,
        }],
      )
      .await?;
    assert_eq!(out, vec![Some(18)]);

    // ---- ZMPOP：WRONGTYPE 立即传播（对齐 C# SortedSetMPop）----
    ss.upsert_string(b"zstr", b"plain").await?;
    assert_eq!(
      ss.sorted_set_m_pop(&[b"zstr"], 1, true).await?,
      (GarnetStatus::WrongType, None)
    );

    // ---- HDEL：缺键不物化空哈希信封，返回 (Ok, 0) ----
    assert_eq!(
      ss.hash_delete(b"h_absent", &[b"f"]).await?,
      (GarnetStatus::Ok, 0)
    );
    assert_eq!(ss.exists(b"h_absent").await?, GarnetStatus::NotFound);

    // ---- HSET/HINCRBY 写路径（去双读后）WRONGTYPE 仍传播且不覆盖 ----
    ss.upsert_string(b"h_str", b"plain").await?;
    assert_eq!(
      ss.hash_set(b"h_str", &[(b"f".as_slice(), b"v".as_slice())], false)
        .await?,
      (GarnetStatus::WrongType, 0)
    );
    assert_eq!(
      ss.hash_increment(b"h_str", b"f", b"1", false).await?,
      (GarnetStatus::WrongType, None)
    );
    assert_eq!(
      ss.read_string(b"h_str").await?,
      Some(b"plain".to_vec()),
      "字符串键不得被对象写覆盖"
    );

    // ---- SPOP：缺键不物化空集合信封 ----
    assert_eq!(
      ss.set_pop(b"s_absent", 10).await?,
      (GarnetStatus::Ok, Vec::new())
    );
    assert_eq!(ss.exists(b"s_absent").await?, GarnetStatus::NotFound);

    // ---- LTRIM：缺键 NotFound（Aborted 路径）----
    assert_eq!(
      ss.list_trim(b"l_absent", 0, -1).await?,
      GarnetStatus::NotFound
    );

    // ---- SMOVE：成员缺失 (Ok,false)；同键对齐 C# 恒返 0 且不误删 ----
    ss.set_add(b"sm", &[b"x"]).await?;
    assert_eq!(
      ss.set_move(b"sm", b"sm2", b"missing").await?,
      (GarnetStatus::Ok, false)
    );
    assert_eq!(
      ss.set_move(b"sm", b"sm", b"x").await?,
      (GarnetStatus::Ok, false)
    );
    assert_eq!(ss.set_length(b"sm").await?, (GarnetStatus::Ok, 1));

    // ---- INCRBYFLOAT：极小值精度不丢失（最短往返表示落盘）----
    let (_, v) = IGarnetApi::increment_by_float(&ss, b"ftiny", 1e-20).await?;
    assert_eq!(v, Some(1e-20));
    let stored = ss.read_string(b"ftiny").await?.unwrap_or_default();
    assert_ne!(stored, b"0".to_vec(), "1e-20 不得被定点 17 位截断为 0");
    assert_eq!(parse_stored_f64(&stored), Some(1e-20), "落盘文本须往返还原");
    Ok(())
  })
}

/// 解析落盘的浮点文本（容忍空白，供回归断言）
fn parse_stored_f64(bytes: &[u8]) -> Option<f64> {
  use std::str;
  str::from_utf8(bytes).ok()?.trim().parse::<f64>().ok()
}

/// review rr1 语义对齐回归：SMOVE 判定序 / *STORE 空键列表 / ZINTER NaN→0 /
/// HINCRBYFLOAT 缺失字段原样存与最短往返 / LCS 缺键 OK+空输出
#[test]
fn test_review_rr1_csharp_semantics() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (_dir, store) = open_store("rr1.db")?;
    let session = store.new_session()?;
    let ss = storage_session(&session);

    // ---- SMOVE：对齐 C# SetMove 三态判定序 ----
    // 源键缺失 → NOTFOUND
    assert_eq!(
      ss.set_move(b"no_src", b"no_dst", b"m").await?,
      (GarnetStatus::NotFound, false)
    );
    ss.set_add(b"src", &[b"m"]).await?;
    // 源==目标：恒返 0，不查成员（成员在场也不搬移）
    assert_eq!(
      ss.set_move(b"src", b"src", b"m").await?,
      (GarnetStatus::Ok, false)
    );
    assert_eq!(ss.set_members(b"src").await?.1, vec![b"m".to_vec()]);
    // 目标类型错 → WRONGTYPE 且源不被改动
    ss.upsert_string(b"str", b"plain").await?;
    assert_eq!(
      ss.set_move(b"src", b"str", b"m").await?,
      (GarnetStatus::WrongType, false)
    );
    assert_eq!(ss.set_members(b"src").await?.1, vec![b"m".to_vec()]);
    // 成员不在源集合 → OK/0
    assert_eq!(
      ss.set_move(b"src", b"dst", b"absent").await?,
      (GarnetStatus::Ok, false)
    );
    // 正常搬移 → OK/1，源删空回收
    assert_eq!(
      ss.set_move(b"src", b"dst", b"m").await?,
      (GarnetStatus::Ok, true)
    );
    assert_eq!(ss.set_members(b"dst").await?.1, vec![b"m".to_vec()]);
    assert_eq!(ss.exists(b"src").await?, GarnetStatus::NotFound);

    // ---- *STORE 空键列表：提前返回 OK，不动目标键 ----
    ss.upsert_string(b"keep", b"old").await?;
    assert_eq!(
      ss.set_union_store(b"keep", &[]).await?,
      (GarnetStatus::Ok, 0)
    );
    assert_eq!(
      ss.set_intersect_store(b"keep", &[]).await?,
      (GarnetStatus::Ok, 0)
    );
    assert_eq!(
      ss.set_diff_store(b"keep", &[]).await?,
      (GarnetStatus::Ok, 0)
    );
    assert_eq!(
      ss.sorted_set_union_store(b"keep", &[], &[], ZSetAggregate::Sum)
        .await?,
      (GarnetStatus::Ok, 0)
    );
    assert_eq!(
      ss.sorted_set_intersect_store(b"keep", &[], &[], ZSetAggregate::Sum)
        .await?,
      (GarnetStatus::Ok, 0)
    );
    assert_eq!(
      ss.sorted_set_difference_store(b"keep", &[]).await?,
      (GarnetStatus::Ok, 0)
    );
    assert_eq!(
      ss.read_string(b"keep").await?,
      Some(b"old".to_vec()),
      "空键列表不得删除目标键"
    );

    // ---- ZINTER NaN→0：+inf 与 -inf 聚合产生 NaN 时归零（C# 缺陷兼容）----
    ss.sorted_set_add(
      b"zi1",
      &[(b"x".as_slice(), f64::INFINITY), (b"y".as_slice(), 1.0)],
      false,
      false,
      false,
      false,
    )
    .await?;
    ss.sorted_set_add(
      b"zi2",
      &[(b"x".as_slice(), f64::NEG_INFINITY), (b"y".as_slice(), 2.0)],
      false,
      false,
      false,
      false,
    )
    .await?;
    let (_, inter) = ss
      .sorted_set_intersect(&[b"zi1", b"zi2"], &[1.0, 1.0], ZSetAggregate::Sum)
      .await?;
    assert_eq!(
      inter,
      vec![(b"x".to_vec(), 0.0), (b"y".to_vec(), 3.0)],
      "NaN 须归零（ZINTER 专属，ZUNION 无此逻辑）"
    );
    let (_, union) = ss
      .sorted_set_union(&[b"zi1", b"zi2"], &[1.0, 1.0], ZSetAggregate::Sum)
      .await?;
    assert!(
      union
        .iter()
        .any(|(m, s)| m.as_slice() == b"x" && s.is_nan()),
      "ZUNION 聚合 NaN 原样保留"
    );

    // ---- HINCRBYFLOAT：缺失字段原样存增量文本；结果最短往返 ----
    let (s, v) = ss.hash_increment(b"hf", b"miss", b"0.1", true).await?;
    assert_eq!(
      (s, v.as_deref()),
      (GarnetStatus::Ok, Some(b"0.1".as_slice()))
    );
    // 0.1 + 0.2 → 0.30000000000000004（非 {:.17} 定点）
    let (s, v) = ss.hash_increment(b"hf", b"miss", b"0.2", true).await?;
    assert_eq!(
      (s, v.as_deref()),
      (GarnetStatus::Ok, Some(b"0.30000000000000004".as_slice()))
    );
    // 增量 ±INF → 拒绝（C# IsInfinity(incr) 检查）
    assert_eq!(
      ss.hash_increment(b"hf", b"inf", b"inf", true).await?,
      (GarnetStatus::WrongType, None)
    );
    // 现值溢出为 ∞ 后（结果照 C# 落存 ∞ 文本），下一次增减拒绝
    ss.hash_increment(b"hf", b"big", b"1e308", true).await?;
    let (s, v) = ss.hash_increment(b"hf", b"big", b"1e308", true).await?;
    assert_eq!(
      (s, v.as_deref()),
      (GarnetStatus::Ok, Some(b"inf".as_slice()))
    );
    assert_eq!(
      ss.hash_increment(b"hf", b"big", b"1", true).await?,
      (GarnetStatus::WrongType, None),
      "现值已为 ∞：拒绝增减"
    );

    // ---- HINCRBY：缺失字段原样存增量文本（含前导零）；溢出回绕（C# unchecked +=）----
    let (s, v) = ss.hash_increment(b"hi", b"c", b"007", false).await?;
    assert_eq!(
      (s, v.as_deref()),
      (GarnetStatus::Ok, Some(b"007".as_slice()))
    );
    let (s, v) = ss.hash_increment(b"hi", b"c", b"5", false).await?;
    assert_eq!(
      (s, v.as_deref()),
      (GarnetStatus::Ok, Some(b"12".as_slice()))
    );
    let (s, v) = ss
      .hash_increment(b"hi", b"w", b"9223372036854775807", false)
      .await?;
    assert_eq!(
      (s, v.as_deref()),
      (GarnetStatus::Ok, Some(b"9223372036854775807".as_slice()))
    );
    let (s, v) = ss.hash_increment(b"hi", b"w", b"1", false).await?;
    assert_eq!(
      (s, v.as_deref()),
      (GarnetStatus::Ok, Some(b"-9223372036854775808".as_slice())),
      "i64 溢出按 C# unchecked 回绕"
    );

    // ---- LCS：任一键缺失 → OK + 空输出（C# 不返回 NOTFOUND）----
    assert_eq!(
      ss.lcs(b"no_a", b"no_b").await?,
      (GarnetStatus::Ok, Some((0, Vec::new())))
    );

    // ---- BITFIELD：跨字节位域写入一次补齐缓冲（16 位域不得被截断）----
    let (_, out) = ss
      .string_bit_field(
        b"bf16",
        &[BitFieldOp::Set {
          is_signed: false,
          bits: 16,
          offset: 0,
          value: 0xABCD,
          wrap: false,
          sat: false,
        }],
      )
      .await?;
    assert_eq!(out, vec![Some(0xABCD)]);
    assert_eq!(
      ss.read_string(b"bf16").await?,
      Some(vec![0xAB, 0xCD]),
      "u16 位域须完整落盘两个字节"
    );

    Ok(())
  })
}
