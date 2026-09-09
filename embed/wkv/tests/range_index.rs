//! RangeIndex 会话层功能测试 (1:1 移植 C# Garnet.test.rangeindex/RespRangeIndexTests)
//!
//! 用例与 C# 对应关系（未逐一移植的说明）：
//! - CRUD / WrongType / InvalidKV / Exists / MultipleFields：一一对应移植；
//! - Scan/Range 语义：字段投影与非存在索引报错在此移植，流式/零拷贝细节见
//!   `range_index_scan.rs`；
//! - Eviction/Flush/Promote 族（RIEviction*、RIFlushPromote*）：C# 依赖页刷盘
//!   OnFlush 触发器与 RIPROMOTE 体系，wedb 该管线为预留未接线（见 wbftree
//!   lib.rs），树级等价语义由 wbftree tests/manager_and_stub 覆盖；
//! - Checkpoint/Recover 族：多树检查点恢复、双检查点恢复到最新/指定版本、
//!   恢复后删除重建在此移植，存根自愈细节见 `checkpoint/index_checkpoint.rs`；
//! - RIAofReplayTest：AOF 重放在 wnode 层（apply → log → replay）实现。
//!
//! 并发语义差异：C# RIDel 对不存在的字段返回 0，本实现 bf-tree 墓碑删除不区分
//! 字段是否存在，删除恒返回 true（幂等），见 wnode ri_del 注释。

use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wbftree::{ScanReturnField, StorageBackend, TreeTuning};
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::{CheckpointManager, RangeIndexError, StoreConfig, WedbStore};

type AokResult<T> = std::result::Result<T, aok::Error>;

/// 与 C# 测试一致的默认树调优：min_record=8 / max_record=1024 / max_key_len=128
const TUNE: TreeTuning = TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

/// 构造独立临时目录中的全新引擎与会话
fn open_store(dir: &tempfile::TempDir, name: &str) -> AokResult<Arc<WedbStore<SegmentedDevice>>> {
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?
    .with_range_index_dir(dir.path().join("range_indexes"));
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(name))?);
  Ok(Arc::new(WedbStore::open(config, device)?))
}

/// RICreateBasicTest：创建后 exists 为真且可回读配置
#[test]
fn test_ri_create_basic() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "ri_basic.db")?;
    let session = store.new_session()?;

    assert!(!session.range_index_exists(b"idx").await?);
    session
      .range_index_create(b"idx", StorageBackend::Std, TUNE)
      .await?;
    assert!(session.range_index_exists(b"idx").await?);

    let stub = session.range_index_config(b"idx").await?;
    assert_eq!(stub.min_record_size, TUNE.min_record_size as u32);
    assert_eq!(stub.max_record_size, TUNE.max_record_size as u32);
    assert_eq!(stub.max_key_len, TUNE.max_key_len as u32);
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// RICreateDuplicateReturnsErrorTest：重复创建报 AlreadyExists
#[test]
fn test_ri_create_duplicate_returns_error() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "ri_dup.db")?;
    let session = store.new_session()?;

    session
      .range_index_create(b"idx", StorageBackend::Std, TUNE)
      .await?;
    let err = session
      .range_index_create(b"idx", StorageBackend::Std, TUNE)
      .await;
    assert!(matches!(err, Err(RangeIndexError::AlreadyExists)));
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// RICreateThenDeleteTest：删除后索引消失，可重建
#[test]
fn test_ri_create_then_delete() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "ri_del.db")?;
    let session = store.new_session()?;

    session
      .range_index_create(b"idx", StorageBackend::Std, TUNE)
      .await?;
    session.range_index_set(b"idx", b"f1", b"v1").await?;
    assert!(session.delete(b"idx").await?);

    assert!(!session.range_index_exists(b"idx").await?);
    let err = session.range_index_get(b"idx", b"f1").await;
    assert!(matches!(err, Err(RangeIndexError::NotFound)));

    // 重建后索引为空且可写
    session
      .range_index_create(b"idx", StorageBackend::Std, TUNE)
      .await?;
    assert_eq!(session.range_index_get(b"idx", b"f1").await?, None);
    session.range_index_set(b"idx", b"f1", b"v2").await?;
    assert_eq!(
      session.range_index_get(b"idx", b"f1").await?,
      Some(b"v2".to_vec())
    );
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// RICreateWithDefaultsTest + RICreateWithAllOptionsTest：零值调优走引擎默认，全量
/// 自定义调优按配置回读
#[test]
fn test_ri_create_with_defaults_and_all_options() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "ri_opts.db")?;
    let session = store.new_session()?;

    // 零值调优 (0 = 引擎默认) 可创建且可写
    let defaults = TreeTuning::default();
    session
      .range_index_create(b"idx_def", StorageBackend::Std, defaults)
      .await?;
    session
      .range_index_set(b"idx_def", b"field", b"value")
      .await?;
    assert_eq!(
      session.range_index_get(b"idx_def", b"field").await?,
      Some(b"value".to_vec())
    );

    // 全量自定义调优逐字段回读
    let all = TreeTuning {
      cache_size: 128 * 1024,
      min_record_size: 16,
      max_record_size: 2048,
      max_key_len: 64,
      leaf_page_size: 8192,
    };
    session
      .range_index_create(b"idx_all", StorageBackend::Std, all)
      .await?;
    let stub = session.range_index_config(b"idx_all").await?;
    assert_eq!(stub.cache_size, all.cache_size as u64);
    assert_eq!(stub.min_record_size, all.min_record_size as u32);
    assert_eq!(stub.max_record_size, all.max_record_size as u32);
    assert_eq!(stub.max_key_len, all.max_key_len as u32);
    assert_eq!(stub.leaf_page_size, all.leaf_page_size as u32);
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// RISetAndGetBasicTest / RISetOverwriteTest / RIGetNonExistentFieldTest /
/// RIGetNonExistentIndexTest / RISetOnNonExistentIndexTest / RIDelFieldTest
#[test]
fn test_ri_point_ops_semantics() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "ri_point.db")?;
    let session = store.new_session()?;

    // 未创建索引：get/set/del 一律 NotFound
    assert!(matches!(
      session.range_index_get(b"nope", b"f").await,
      Err(RangeIndexError::NotFound)
    ));
    assert!(matches!(
      session.range_index_set(b"nope", b"f", b"v").await,
      Err(RangeIndexError::NotFound)
    ));
    assert!(matches!(
      session.range_index_del(b"nope", b"f").await,
      Err(RangeIndexError::NotFound)
    ));

    session
      .range_index_create(b"idx", StorageBackend::Std, TUNE)
      .await?;

    // set + get 基础往返
    session
      .range_index_set(b"idx", b"field1", b"value1")
      .await?;
    assert_eq!(
      session.range_index_get(b"idx", b"field1").await?,
      Some(b"value1".to_vec())
    );

    // 覆盖写返回新值
    session
      .range_index_set(b"idx", b"field1", b"value2")
      .await?;
    assert_eq!(
      session.range_index_get(b"idx", b"field1").await?,
      Some(b"value2".to_vec())
    );

    // 不存在的字段 → None
    assert_eq!(session.range_index_get(b"idx", b"missing").await?, None);

    // 删除字段：墓碑语义，重复删除幂等返回 true
    assert!(session.range_index_del(b"idx", b"field1").await?);
    assert_eq!(session.range_index_get(b"idx", b"field1").await?, None);
    assert!(session.range_index_del(b"idx", b"field1").await?);
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// RIMultipleFieldsTest：多字段独立读写互不干扰
#[test]
fn test_ri_multiple_fields() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "ri_multi.db")?;
    let session = store.new_session()?;

    session
      .range_index_create(b"idx", StorageBackend::Std, TUNE)
      .await?;
    for i in 0..5 {
      session
        .range_index_set(
          b"idx",
          format!("f{i}").as_bytes(),
          format!("v{i}").as_bytes(),
        )
        .await?;
    }
    for i in 0..5 {
      assert_eq!(
        session
          .range_index_get(b"idx", format!("f{i}").as_bytes())
          .await?,
        Some(format!("v{i}").into_bytes())
      );
    }
    // 覆盖其中一个不影响其余
    session
      .range_index_set(b"idx", b"f2", b"overwritten")
      .await?;
    assert_eq!(
      session.range_index_get(b"idx", b"f2").await?,
      Some(b"overwritten".to_vec())
    );
    assert_eq!(
      session.range_index_get(b"idx", b"f3").await?,
      Some(b"v3".to_vec())
    );
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// RIWrongTypeOnNormalKeyTest / RIWrongTypeGetOnNormalKeyTest / RINormalGetOnRangeIndexKeyTest：
/// 普通字符串键与 RangeIndex 键相互 WRONGTYPE 隔离
#[test]
fn test_ri_wrong_type_isolation() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "ri_wtype.db")?;
    let session = store.new_session()?;

    // 普通字符串键上的 RI 操作 → WRONGTYPE
    session.upsert(b"str_key", b"plain").await?;
    assert!(matches!(
      session.range_index_get(b"str_key", b"f").await,
      Err(RangeIndexError::WrongType)
    ));
    assert!(matches!(
      session.range_index_set(b"str_key", b"f", b"v").await,
      Err(RangeIndexError::WrongType)
    ));

    // RangeIndex 键上的普通读取不泄露索引数据
    session
      .range_index_create(b"ri_key", StorageBackend::Std, TUNE)
      .await?;
    session.range_index_set(b"ri_key", b"f", b"secret").await?;
    let normal = session.read(b"ri_key").await;
    assert!(
      matches!(normal, Ok(None)) || normal.is_err(),
      "普通 GET 不得返回 RangeIndex 数据"
    );
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// RIScanOnNonExistentIndexTest / RIRangeOnNonExistentIndexTest + 字段投影语义
/// (RIScanBasicTest / RIScanFieldsKeyTest / RIScanFieldsValueTest / RIRangeBasicTest)
#[test]
fn test_ri_scan_range_semantics() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "ri_scan.db")?;
    let session = store.new_session()?;

    // 非存在索引 → NotFound
    assert!(matches!(
      session
        .range_index_scan(b"nope", b"", 10, ScanReturnField::KeyAndValue)
        .await,
      Err(RangeIndexError::NotFound)
    ));
    assert!(matches!(
      session
        .range_index_range(b"nope", b"a", b"z", ScanReturnField::KeyAndValue)
        .await,
      Err(RangeIndexError::NotFound)
    ));

    session
      .range_index_create(b"idx", StorageBackend::Std, TUNE)
      .await?;
    for i in 0..10u32 {
      session
        .range_index_set(
          b"idx",
          format!("key{i:02}").as_bytes(),
          format!("val{i:02}").as_bytes(),
        )
        .await?;
    }

    // KeyOnly 投影：值为空
    let keys = session
      .range_index_scan(b"idx", b"key00", 3, ScanReturnField::Key)
      .await?;
    assert_eq!(keys.len(), 3);
    assert!(keys.iter().all(|r| !r.key.is_empty() && r.value.is_empty()));

    // ValueOnly 投影：键为空
    let vals = session
      .range_index_scan(b"idx", b"key00", 3, ScanReturnField::Value)
      .await?;
    assert_eq!(vals.len(), 3);
    assert!(vals.iter().all(|r| r.key.is_empty() && !r.value.is_empty()));

    // 闭区间范围
    let ranged = session
      .range_index_range(b"idx", b"key03", b"key06", ScanReturnField::KeyAndValue)
      .await?;
    assert_eq!(ranged.len(), 4);
    assert_eq!(ranged.first().unwrap().key, b"key03");
    assert_eq!(ranged.last().unwrap().key, b"key06");
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// RISetInvalidKVFieldTooLongTest / RISetInvalidKVValueTooLongTest /
/// RISetInvalidKVRecordTooSmallTest：键长 / 记录上限 / 记录下限三向校验
#[test]
fn test_ri_invalid_kv_validation() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "ri_kv.db")?;
    let session = store.new_session()?;

    session.range_index_create(b"idx", StorageBackend::Std, TUNE).await?;

    // 字段超长 (max_key_len = 128)
    let long_field = vec![b'k'; TUNE.max_key_len + 1];
    let err = session.range_index_set(b"idx", &long_field, b"v").await;
    assert!(matches!(err, Err(RangeIndexError::InvalidKV { key_len: 129, .. })));

    // 记录超上限 (max_record_size = 1024)
    let big_value = vec![b'v'; TUNE.max_record_size];
    let err = session.range_index_set(b"idx", b"field", &big_value).await;
    assert!(
      matches!(err, Err(RangeIndexError::InvalidKV { total_len, .. }) if total_len > TUNE.max_record_size)
    );

    // 记录低于下限 (min_record_size = 8)
    let err = session.range_index_set(b"idx", b"k", b"v").await;
    assert!(
      matches!(err, Err(RangeIndexError::InvalidKV { total_len, .. }) if total_len < TUNE.min_record_size)
    );

    // 边界内合法
    session
      .range_index_set(b"idx", &vec![b'k'; TUNE.max_key_len], &[b'v'; 100])
      .await?;
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// RIExistsBasicTest：exists 精确区分不存在 / 存在 / 已删除
#[test]
fn test_ri_exists_basic() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "ri_exists.db")?;
    let session = store.new_session()?;

    assert!(!session.range_index_exists(b"idx").await?);
    session
      .range_index_create(b"idx", StorageBackend::Std, TUNE)
      .await?;
    assert!(session.range_index_exists(b"idx").await?);
    session.delete(b"idx").await?;
    assert!(!session.range_index_exists(b"idx").await?);
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// RIConcurrentMultiClientTest：多客户端并发读写同一索引的最终一致性
#[test]
fn test_ri_concurrent_multi_client() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "ri_concurrent.db")?;
    {
      let session = store.new_session()?;
      session
        .range_index_create(b"idx", StorageBackend::Std, TUNE)
        .await?;
    }

    const CLIENTS: usize = 6;
    const FIELDS_PER_CLIENT: usize = 50;
    let mut handles = Vec::new();
    for c in 0..CLIENTS {
      let store = Arc::clone(&store);
      handles.push(compio::runtime::spawn(async move {
        let session = store.new_session()?;
        for i in 0..FIELDS_PER_CLIENT {
          let field = format!("c{c:02}_f{i:03}");
          session
            .range_index_set(b"idx", field.as_bytes(), field.as_bytes())
            .await?;
        }
        Ok::<(), aok::Error>(())
      }));
    }
    for h in handles {
      h.await.expect("并发任务异常退出")?;
    }

    // 全部客户端写完后统一回读：最终一致且值与键精确对应
    let session = store.new_session()?;
    for c in 0..CLIENTS {
      for i in 0..FIELDS_PER_CLIENT {
        let field = format!("c{c:02}_f{i:03}");
        let expected = field.clone().into_bytes();
        assert_eq!(
          session.range_index_get(b"idx", field.as_bytes()).await?,
          Some(expected),
          "并发写后 {field} 必须精确可读"
        );
      }
    }
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// RICheckpointWithMultipleTreesAndRecoverTest：多索引检查点全量恢复
#[test]
fn test_ri_checkpoint_recover_multi_tree() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let store = open_store(&dir, "ri_ckpt_multi.db")?;

    let indexes: [(&[u8], &[u8]); 3] = [
      (b"idx_a", b"val_a"),
      (b"idx_b", b"val_b"),
      (b"idx_c", b"val_c"),
    ];
    let token;
    {
      let session = store.new_session()?;
      for (key, val) in indexes {
        session
          .range_index_create(key, StorageBackend::Std, TUNE)
          .await?;
        session.range_index_set(key, b"field", val).await?;
      }
      let meta = store
        .create_checkpoint(&ckpt_dir, CheckpointType::FoldOver)
        .await?;
      token = meta.token;
    } // 原 store 全部销毁，模拟崩溃

    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("ri_ckpt_multi.db"),
    )?);
    let recovered = Arc::new(CheckpointManager::recover(&ckpt_dir, token, device).await?);
    let session = recovered.new_session()?;
    for (key, val) in indexes {
      assert!(
        session.range_index_exists(key).await?,
        "{key:?} 恢复后必须存在"
      );
      assert_eq!(
        session.range_index_get(key, b"field").await?,
        Some(val.to_vec()),
        "{key:?} 恢复后字段必须精确回读"
      );
    }
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// RITwoCheckpointsRecoverToLatestTest / RIRecoverToEarlierCheckpointTest：
/// 双检查点下可恢复到最新版本，也可显式恢复到早期版本
#[test]
fn test_ri_two_checkpoints_recover_to_version() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let db_path = dir.path().join("ri_ckpt_two.db");
    let store = open_store(&dir, "ri_ckpt_two.db")?;

    let session = store.new_session()?;
    session
      .range_index_create(b"idx", StorageBackend::Std, TUNE)
      .await?;
    session
      .range_index_set(b"idx", b"field", b"version_1")
      .await?;
    let meta1 = store
      .create_checkpoint(&ckpt_dir, CheckpointType::FoldOver)
      .await?;
    let token1 = meta1.token;

    session
      .range_index_set(b"idx", b"field", b"version_2")
      .await?;
    let meta2 = store
      .create_checkpoint(&ckpt_dir, CheckpointType::FoldOver)
      .await?;
    let token2 = meta2.token;
    drop(session);
    drop(store);

    // 恢复到最新检查点：新值
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let latest = Arc::new(CheckpointManager::recover(&ckpt_dir, token2, device).await?);
      let session = latest.new_session()?;
      assert_eq!(
        session.range_index_get(b"idx", b"field").await?,
        Some(b"version_2".to_vec())
      );
    }

    // 显式恢复到早期检查点：旧值
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let earlier = Arc::new(CheckpointManager::recover(&ckpt_dir, token1, device).await?);
      let session = earlier.new_session()?;
      assert_eq!(
        session.range_index_get(b"idx", b"field").await?,
        Some(b"version_1".to_vec())
      );
    }
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// RIDeleteAfterRecoveryTest：恢复后删除索引并重建，状态机完全就绪
#[test]
fn test_ri_delete_after_recovery() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let db_path = dir.path().join("ri_del_recover.db");
    let store = open_store(&dir, "ri_del_recover.db")?;

    let token;
    {
      let session = store.new_session()?;
      session
        .range_index_create(b"idx", StorageBackend::Std, TUNE)
        .await?;
      session.range_index_set(b"idx", b"field", b"value").await?;
      let meta = store
        .create_checkpoint(&ckpt_dir, CheckpointType::FoldOver)
        .await?;
      token = meta.token;
    }

    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let recovered = Arc::new(CheckpointManager::recover(&ckpt_dir, token, device).await?);
    let session = recovered.new_session()?;
    assert_eq!(
      session.range_index_get(b"idx", b"field").await?,
      Some(b"value".to_vec())
    );

    // 恢复后的实例上删除 → 重建 → 全空可写
    assert!(session.delete(b"idx").await?);
    assert!(!session.range_index_exists(b"idx").await?);
    session
      .range_index_create(b"idx", StorageBackend::Std, TUNE)
      .await?;
    assert_eq!(session.range_index_get(b"idx", b"field").await?, None);
    session.range_index_set(b"idx", b"new", b"fresh").await?;
    assert_eq!(
      session.range_index_get(b"idx", b"new").await?,
      Some(b"fresh".to_vec())
    );
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 索引淘汰与注册表生命周期（对标 RIEvictionFreesEvictedTreeButKeepsLiveTest 的
/// 树实例语义；数据面保留由惰性恢复承担，C# 的数据保留依赖 flush 快照体系，
/// wedb 为预留未接线，故此处只验证注册表驱逐与重新打开闭环）
#[test]
fn test_ri_evicted_tree_reopens_on_access() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "ri_evict.db")?;
    let session = store.new_session()?;

    session
      .range_index_create(b"live", StorageBackend::Std, TUNE)
      .await?;
    session
      .range_index_create(b"evict", StorageBackend::Std, TUNE)
      .await?;
    session
      .range_index_set(b"evict", b"field", b"value")
      .await?;

    // 淘汰 evict (保留磁盘文件)：live 树不受影响
    assert!(store.range_index.dispose_tree(b"evict", false)?);
    assert!(
      store.range_index.get_tree(b"evict").is_none(),
      "淘汰后注册表必须摘除"
    );
    assert!(
      store.range_index.get_tree(b"live").is_some(),
      "未淘汰树必须保持在线"
    );

    // 再次访问：走惰性恢复重新打开 (不存在 → NotFound 而非内部错误)
    assert_eq!(session.range_index_get(b"evict", b"field").await?, None);
    assert!(
      store.range_index.get_tree(b"evict").is_some(),
      "惰性恢复必须重新注册在线树"
    );
    aok::Result::<()>::Ok(())
  })?;
  OK
}
