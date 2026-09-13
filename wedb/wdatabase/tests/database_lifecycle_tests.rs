use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use wdatabase::{
  DatabaseManager, DatabaseManagerFactory, GarnetDatabase, IDatabaseManager, MultiDatabaseManager,
  SingleDatabaseManager,
};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};

type TestStore = Arc<WedbStore<SegmentedDevice>>;

fn open_store(tag: &str) -> aok::Result<(TempDir, TestStore)> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag))?);
  let config = StoreConfig::new(16384, 65536, 64, 0.5)?;
  let store = Arc::new(WedbStore::open(config, device)?);
  Ok((dir, store))
}

#[test]
fn test_single_database_manager_lifecycle() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store) = open_store("single.db")?;
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      dir.path().join("0"),
      None::<Arc<()>>,
    ));
    let single = SingleDatabaseManager::new(dir.path().to_path_buf(), Arc::clone(&db));

    // 1. TryGetOrAddDatabase 恒返回 db 0
    let (got_db, added) = single.try_get_or_add_database().unwrap();
    assert!(!added);
    assert_eq!(got_db.id, 0);

    // 2. 检查点暂停与恢复
    assert!(single.try_pause_checkpoints());
    assert!(!single.try_pause_checkpoints()); // 已处于暂停态
    single.resume_checkpoints();
    assert!(single.try_pause_checkpoints()); // 恢复后可再次暂停

    // 3. 快照包含 db0
    let snapshot = single.get_databases_snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].id, 0);

    // 4. 单库不支持 SWAPDB
    assert!(!single.try_swap_databases(0, 1).await);

    // 5. 写入键并清空
    let session = store.new_session()?;
    session.set_active_db(0);
    session.upsert(b"foo", b"bar").await?;
    assert_eq!(session.read(b"foo").await?, Some(b"bar".to_vec()));

    single.flush_all_databases().await?;
    assert_eq!(session.read(b"foo").await?, None);

    Ok(())
  })
}

#[test]
fn test_multi_database_manager_lifecycle_and_swap() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store) = open_store("multi.db")?;
    let manager = MultiDatabaseManager::<_, ()>::new(Arc::clone(&store), dir.path().to_path_buf());

    // 1. 注册 db0 与 db1
    let db0 = manager.try_add_database(0, Arc::clone(&store))?;
    manager.handle_database_added(&db0);
    let (db1, added1) = manager.try_get_or_add_database(1).await?;
    assert!(added1);
    assert_eq!(db1.id, 1);

    // 二次获取不应重复添加
    let (_db1_again, added1_again) = manager.try_get_or_add_database(1).await?;
    assert!(!added1_again);

    // 2. 快照应有两个数据库
    let snapshot = manager.get_databases_snapshot();
    assert_eq!(snapshot.len(), 2);

    // 3. 库键前缀物理隔离测试
    let session = store.new_session()?;
    session.set_active_db(0);
    session.upsert(b"key1", b"val_db0").await?;

    session.set_active_db(1);
    assert_eq!(session.read(b"key1").await?, None);
    session.upsert(b"key1", b"val_db1").await?;

    session.set_active_db(0);
    assert_eq!(session.read(b"key1").await?, Some(b"val_db0".to_vec()));

    // 4. SWAPDB 测试
    assert!(manager.try_swap_databases(0, 1).await);

    session.set_active_db(0);
    assert_eq!(session.read(b"key1").await?, Some(b"val_db1".to_vec()));
    session.set_active_db(1);
    assert_eq!(session.read(b"key1").await?, Some(b"val_db0".to_vec()));

    // 5. 单库清空测试
    manager.flush_database(0).await?;
    session.set_active_db(0);
    assert_eq!(session.read(b"key1").await?, None);
    session.set_active_db(1);
    assert_eq!(session.read(b"key1").await?, Some(b"val_db0".to_vec()));

    // 6. 全库清空测试
    manager.flush_all_databases().await?;
    session.set_active_db(1);
    assert_eq!(session.read(b"key1").await?, None);

    Ok(())
  })
}

#[test]
fn test_database_manager_factory() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store) = open_store("factory.db")?;
    let db0 = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      dir.path().join("0"),
      None::<Arc<()>>,
    ));

    // 判定函数
    assert!(!DatabaseManagerFactory::should_create_multiple_database_manager(1));
    assert!(DatabaseManagerFactory::should_create_multiple_database_manager(2));

    // 单库工厂生产
    let single_mgr = DatabaseManagerFactory::create_database_manager(
      false,
      Arc::clone(&store),
      dir.path().to_path_buf(),
      Arc::clone(&db0),
    );
    assert!(matches!(single_mgr, DatabaseManager::Single(_)));
    assert_eq!(single_mgr.database_count(), 1);
    assert_eq!(single_mgr.try_default_database().unwrap().id, 0);

    // 多库工厂生产
    let multi_mgr = DatabaseManagerFactory::create_database_manager(
      true,
      Arc::clone(&store),
      dir.path().to_path_buf(),
      Arc::clone(&db0),
    );
    assert!(matches!(multi_mgr, DatabaseManager::Multi(_)));
    assert_eq!(multi_mgr.database_count(), 1);
    assert_eq!(multi_mgr.try_default_database().unwrap().id, 0);

    Ok(())
  })
}
