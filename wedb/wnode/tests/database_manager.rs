//! 数据库管理面单测迁移（自 wnode/src/database/single_database_manager.rs
//! 内联测试迁出：内联形态会把 wtest_base 链入 wnode lib 测试二进制，其
//! ctor 抢装全局日志器，破坏 logging::tests 的进程级安装前提）

use std::sync::Arc;

use compio::runtime::Runtime;
use wbftree::{StorageBackendType, TreeTuning};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::database::{
  CHECKPOINT_RETAIN_GENERATIONS, DatabaseManagerBase, GarnetDatabase, IDatabaseManager,
  SingleDatabaseManager,
};
use wtest_base::open_test_store;
use wval::{KeyTag, NamespaceDbCodec};

/// 与 flush_database 域测试一致的默认树调优
const RI_TUNE: TreeTuning = TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

/// 树身份键（wkv tests/support::tree_id_key 同形，测试域 `(vns, vdb)` 的物理
/// Meta 键形态；数据文件名按它派生）
fn tree_id_key(vns: u64, vdb: u64, user_key: &[u8]) -> wval::TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(vns, vdb, KeyTag::Meta, user_key)
}

/// 恢复实例补挂常驻回收驱动（挂载收敛「实例产生点必带」回归固化）：
/// FLUSHDB 换号投递死亡域 RI 树 → 检查点提交（DbMeta 墓碑与死亡域存根落盘）→
/// 停机（未到期残件滞留磁盘，Drop 不强行释放）→ 重启恢复走生产恢复收口
/// `recover_database_checkpoint_async`（恢复产出全新引擎，不经 open_shared）。
///
/// 修复前恢复臂只调 start_gc——spawn_bftree_reclaimer 唯一生产挂点在
/// open_shared，恢复实例的死亡账本到期项永无人消费；修复后恢复收口随实例
/// 挂载，挂载态经 reclaimer_mounted() 观测断言。恢复期死亡域树文件的销毁
/// 由恢复扫描死亡域守卫同步承担（原子批已持久化、回滚窗已关闭，即时销毁
/// 即设计内语义）；恢复后新投递批次的「短纪元 → 到期被回收」端到端行为面
/// 由 wkv tests/store/flush_database.rs::test_open_shared_reclaimer_drives_release
/// 以同一常驻循环体锁定（恢复实例的纪元期限由检查点 StoreMeta 决定，不随
/// 调用方配置，无法在恢复臂定制短纪元，故不在本测试硬造）
#[test]
fn test_recovered_store_carries_bftree_reclaimer() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempfile::tempdir()?;
    let cp_dir = dir.path().join("cp");
    let mut config = StoreConfig::new(2048, 64 * 1024, 16, 0.5)?;
    // 短纪元：恢复期重投批次的删除须收敛进测试轮询窗（生产默认 86400 秒）
    config.gc.db_gc_reclaim_delay_secs = 1;
    let config = config.with_range_index_dir(dir.path().join("ri"));
    let db_file = dir.path().join("recovered_reclaimer.db");
    let old_vdb;
    let ri_data_file;
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_file)?);
      let store = Arc::new(WedbStore::open(config.clone(), device)?);
      let db = Arc::new(GarnetDatabase::new(
        0,
        Arc::clone(&store),
        Arc::clone(&store.device),
        cp_dir.clone(),
        None,
      ));
      let single = SingleDatabaseManager::new(cp_dir.clone(), Arc::clone(&db));

      // 死亡域 RI 树：换号同步臂只摘注册与投递，绝不物理删除
      let s0 = store.new_session()?;
      s0.set_context(0, 0);
      s0.range_index_create(b"idx", StorageBackendType::Disk, RI_TUNE)
        .await?;
      s0.range_index_set(b"idx", b"field", b"value").await?;
      ri_data_file = store
        .range_index()
        .data_file_path_for_key(&tree_id_key(0, 0, b"idx"));
      assert!(ri_data_file.exists(), "RI 数据文件须在盘");
      let (_, retired) = store.flush_database(0, 0).await?;
      old_vdb = retired;
      assert!(single.take_checkpoint(true).await.unwrap(), "快照须发布");
      drop(s0);
      drop(single);
      drop(db);
      drop(store);
    }
    assert!(
      ri_data_file.exists(),
      "安全纪元期限内停机不强行释放，残件滞留磁盘"
    );

    // 重启恢复：全新设备句柄与引导宿主（生产 recover_checkpoint_store 同形），
    // 恢复收口内随实例挂载常驻回收驱动
    let device = Arc::new(SegmentedDevice::single_file(&db_file)?);
    let bootstrap = Arc::new(WedbStore::open(config, Arc::clone(&device))?);
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&bootstrap),
      Arc::clone(&device),
      cp_dir.clone(),
      None,
    ));
    let single = SingleDatabaseManager::new(cp_dir, Arc::clone(&db));
    let recovered = single
      .base
      .recover_database_checkpoint_async(&db, None)
      .await
      .unwrap()
      .expect("最新快照须可恢复");

    // 本票回归点：恢复收口随实例挂载常驻回收驱动（幂等挂载位）
    assert!(
      recovered.reclaimer_mounted(),
      "恢复臂须随实例挂载常驻回收驱动"
    );

    // 死亡账本按墓碑恢复；死亡域树文件由恢复扫描死亡域守卫同步销毁
    assert!(
      recovered.vdb.is_dead_domain(0, old_vdb),
      "死亡账本须按 DbMeta 墓碑恢复"
    );
    assert!(
      !ri_data_file.exists(),
      "死亡域守卫须在恢复扫描期即时销毁已判死域树文件"
    );
    aok::OK
  })
}

#[test]
fn test_single_database_manager_grow_indexes_if_needed() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store) = open_test_store("single_grow_indexes").unwrap();
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      dir.path().join("0"),
      None,
    ));
    let single = SingleDatabaseManager::new(dir.path().to_path_buf(), Arc::clone(&db));

    let initial_size = store.active_index().size;
    // 阈值设为 50，当无溢出时，不触发扩容，未达 max_size 返回 false
    let maxed = single
      .grow_indexes_if_needed(initial_size * 4, 50)
      .await
      .unwrap();
    assert!(!maxed);
    assert_eq!(store.active_index().size, initial_size);

    // 当设置 index_max_size <= initial_size 时，返回 true 表示已达上限
    let maxed_limit = single
      .grow_indexes_if_needed(initial_size, 50)
      .await
      .unwrap();
    assert!(maxed_limit);

    // IDatabaseManager trait 调用
    let trait_maxed = IDatabaseManager::grow_indexes_if_needed_async(&single, initial_size, 50)
      .await
      .unwrap();
    assert!(trait_maxed);
  });
  aok::OK
}

/// 阈值乘法溢出安全（P1 回归）：threshold 为 i64 CLI 直通，极端值与
/// 大索引规模在 u64 域相乘溢出；u128 中间量下判定恒不触发、不 panic、
/// 永不自动扩容
#[test]
fn test_grow_index_threshold_overflow_never_grows() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, _store) = open_test_store("grow_threshold_overflow").unwrap();
    let base = DatabaseManagerBase::<wdev::SegmentedDevice>::new(dir.path().to_path_buf());
    let current_size = 1usize << 40;
    let grew = base
      .grow_index_if_needed(usize::MAX, 0, i64::MAX, || current_size, async {
        Ok::<bool, wkv::Error>(true)
      })
      .await
      .unwrap();
    assert!(!grew, "溢出域下永不自动扩容");
  });
  aok::OK
}

/// 单机形态快照保留回收：连续拍检查点，目录内快照代数须被钳制在
/// [`CHECKPOINT_RETAIN_GENERATIONS`]，最旧一代物理回收、最新一代仍可恢复
///
/// 对标 C# 检查点状态机 REST 段的 CleanupIndexCheckpoint /
/// CleanupLogCheckpoint（`IndexCheckpointSMTask.cs:52`、
/// `HybridLogCheckpointSMTask.cs:67` → removeOutdated 环拍新删旧）。
/// 形态判据取「不注入 cluster 句柄」——生产对位 wedb_standalone 宿主
/// （无 ClusterProvider / 复制域仓库），集群形态由复制域读者闸门单轨接管，
/// 见 `wedb/tests/checkpoint_wiring.rs::disk_retention_follows_reader_gate`
#[test]
fn test_standalone_checkpoint_retention_bounds_directory() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store) = open_test_store("standalone_retention").unwrap();
    let cp_dir = dir.path().join("0");
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      cp_dir.clone(),
      None,
    ));
    // 刻意不调 attach_flush_gate：cluster 句柄缺位即单机形态位
    let single = SingleDatabaseManager::new(cp_dir.clone(), Arc::clone(&db));

    let session = store.new_session().unwrap();
    let rounds = CHECKPOINT_RETAIN_GENERATIONS + 1;
    // 每轮签发并发布的 Token（Token 由时间戳派生、严格递增，值序即时序）
    let mut issued: Vec<u128> = Vec::new();
    for round in 0..rounds {
      session
        .upsert(format!("k{round}").as_bytes(), b"v".as_slice())
        .await
        .unwrap();
      assert!(single.take_checkpoint(true).await.unwrap());
      let tokens = wcpr::list_checkpoints(&cp_dir).unwrap();
      assert_eq!(
        tokens.len(),
        (round + 1).min(CHECKPOINT_RETAIN_GENERATIONS),
        "第 {round} 轮后目录内快照代数须被钳制"
      );
      let newest = *tokens.last().expect("快照已发布");
      assert!(
        issued.last().is_none_or(|prev| newest > *prev),
        "保留集最新项须为本轮新发 Token"
      );
      issued.push(newest);
    }

    let retained = wcpr::list_checkpoints(&cp_dir).unwrap();
    assert_eq!(
      retained,
      issued[issued.len() - CHECKPOINT_RETAIN_GENERATIONS..],
      "保留集须恰为最新 {CHECKPOINT_RETAIN_GENERATIONS} 代"
    );
    assert!(!retained.contains(&issued[0]), "最旧一代快照须已被物理回收");
    // 物理落点：meta 清单与 base32 快照子目录一并 unlink（不得只移出清单）
    let oldest = issued[0];
    assert!(
      !cp_dir
        .join(wcpr::meta_filename(oldest))
        .try_exists()
        .unwrap()
        && !cp_dir
          .join(wcpr::index_filename(oldest))
          .try_exists()
          .unwrap()
        && !cp_dir
          .join(wcpr::token_to_base32(oldest).as_str())
          .try_exists()
          .unwrap(),
      "最旧一代的 meta / 索引 / 快照子目录须已被物理回收"
    );

    // 幸存快照可恢复且内容完整（对标 C# 重启 RecoverAsync 取最新 Token）
    let recovered = single
      .base
      .recover_database_checkpoint_async(&db, None)
      .await
      .unwrap()
      .expect("最新快照须可恢复");
    let recovered_session = recovered.new_session().unwrap();
    for round in 0..rounds {
      assert_eq!(
        recovered_session
          .read(format!("k{round}").as_bytes())
          .await
          .unwrap(),
        Some(b"v".to_vec()),
        "第 {round} 轮写入须可在恢复视图中读回"
      );
    }
  });
  aok::OK
}

/// 恢复后「清未用」：显式恢复历史 Token 后，目录内未被选中的快照（含更新的
/// 一代）须被物理回收，恢复视图恰为所选那一版
///
/// 对标 C# 检查点管理器 `OnRecovery`
///（`DeviceLogCommitCheckpointManager.cs:337-365`：`if (!removeOutdated) return;`
/// 后逐个 Delete 非本次恢复 Token 的快照，原文 "Purge all log/index checkpoints
/// that were not used for recovery"）。形态判据同
/// [`test_standalone_checkpoint_retention_bounds_directory`]（不注入 cluster
/// 句柄 = 单机形态；集群形态让位复制域 CheckpointStore 单轨）
#[test]
fn test_recovery_purges_unrecovered_checkpoints() -> aok::Result<()> {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let (dir, store) = open_test_store("recovery_purge_unrecovered").unwrap();
    let cp_dir = dir.path().join("0");
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      cp_dir.clone(),
      None,
    ));
    let single = SingleDatabaseManager::new(cp_dir.clone(), Arc::clone(&db));
    let session = store.new_session().unwrap();

    session.upsert(b"k_old", b"v_old").await.unwrap();
    assert!(single.take_checkpoint(true).await.unwrap());
    let older = wcpr::find_latest_checkpoint(&cp_dir)
      .unwrap()
      .expect("第一代快照");

    session.upsert(b"k_new", b"v_new").await.unwrap();
    assert!(single.take_checkpoint(true).await.unwrap());
    let newest = wcpr::find_latest_checkpoint(&cp_dir)
      .unwrap()
      .expect("第二代快照");
    assert!(newest > older, "第二代 Token 须严格更新");
    assert_eq!(
      wcpr::list_checkpoints(&cp_dir).unwrap(),
      vec![older, newest],
      "保留数内两代快照须并存（清理前基线）"
    );

    // 显式恢复较旧一代：更新的代同属「未被本次恢复选中」，须一并回收
    //（按条数口径的 purge_outdated 在此会反向误删在用版，本断言即两口径的分界）
    let recovered = single
      .base
      .recover_database_checkpoint_async(&db, Some(older))
      .await
      .unwrap()
      .expect("历史快照须可恢复");
    assert_eq!(
      wcpr::list_checkpoints(&cp_dir).unwrap(),
      vec![older],
      "恢复后目录内须只余被选中的那一版"
    );
    assert!(
      !cp_dir
        .join(wcpr::meta_filename(newest))
        .try_exists()
        .unwrap()
        && !cp_dir
          .join(wcpr::index_filename(newest))
          .try_exists()
          .unwrap()
        && !cp_dir
          .join(wcpr::token_to_base32(newest).as_str())
          .try_exists()
          .unwrap(),
      "未选中一代的 meta / 索引 / 快照子目录须已被物理回收"
    );

    let recovered_session = recovered.new_session().unwrap();
    assert_eq!(
      recovered_session.read(b"k_old").await.unwrap(),
      Some(b"v_old".to_vec()),
      "所选一代的写入须可读回"
    );
    assert_eq!(
      recovered_session.read(b"k_new").await.unwrap(),
      None,
      "恢复视图须恰为所选那一版，不得含其后代写入"
    );
  });
  aok::OK
}

/// INFO RESETSTAT 的 reviv 臂真下发：单库管理器经唯一存储句柄把复位打到
/// 复活池账目上（票内原状是空函数体加「wkv 无复活化统计面」失真注释；
/// 本用例即反证——账目由 `wkv::WedbStore::reviv_pool` 单一承载，复位可达）
/// 验证 SingleDatabaseManager::reset_revivification_stats 复位池账目语义
#[test]
fn reset_revivification_stats_zeroes_pool_counters() -> aok::Result<()> {
  let (dir, store) = open_test_store("reset_reviv_stats")?;
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&store),
    Arc::clone(&store.device),
    dir.path().join("0"),
    None,
  ));
  let single = SingleDatabaseManager::new(dir.path().to_path_buf(), Arc::clone(&db));

  // 夹具先记账：一次成功投入 + 一次尺寸非法丢弃（drop_count 也属账目）
  assert!(store.reviv_pool.put(0x1000, 64, 0x1000));
  assert!(
    !store.reviv_pool.put(0x2000, 0, 0x1000),
    "尺寸 0 应被池丢弃并计入 drop"
  );
  let before = store.reviv_pool.stats();
  assert_eq!(
    (before.put_count, before.drop_count),
    (2, 1),
    "夹具应先有可复位的账目"
  );

  // inherent 口：即监视器 reviv 臂经 SessionProviderFace 的下发路径
  SingleDatabaseManager::reset_revivification_stats(&single);
  let after = store.reviv_pool.stats();
  assert_eq!(
    (
      after.put_count,
      after.take_count,
      after.hit_count,
      after.drop_count
    ),
    (0, 0, 0, 0),
    "复位须直落复活池四计数"
  );
  assert!(
    !store.reviv_pool.is_empty(),
    "复位只清账，不得清掉可复活槽位"
  );

  // IDatabaseManager trait 转发口同效（复位后新流量从零点继续记账）
  assert!(store.reviv_pool.put(0x3000, 64, 0x1000));
  IDatabaseManager::reset_revivification_stats(&single);
  assert_eq!(
    store.reviv_pool.stats().put_count,
    0,
    "trait 面应与 inherent 面同样下达"
  );
  aok::OK
}
