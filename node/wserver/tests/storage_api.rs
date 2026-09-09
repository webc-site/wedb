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
    ss.set_add(b"sa", &[b"1", b"2", b"3"]).await?;
    ss.set_add(b"sb", &[b"2", b"3", b"4"]).await?;
    let (_, inter) = ss.set_intersect(&[b"sa", b"sb"]).await?;
    assert_eq!(inter.len(), 2);
    let (_, diff) = ss.set_diff(&[b"sa", b"sb"]).await?;
    assert_eq!(diff, vec![b"1".to_vec()]);

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
