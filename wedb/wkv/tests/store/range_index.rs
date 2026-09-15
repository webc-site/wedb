//! RangeIndex 会话层独有语义测试 (1:1 移植 C# Garnet.test.rangeindex/RespRangeIndexTests)
//!
//! CRUD / WrongType / InvalidKV / Exists / MultipleFields / Scan/Range 的网络层
//! 行为测试由 wedb_standalone/tests/range_index_tests.rs 承接；本文件仅保留
//! store 层独有语义：
//! - InvalidKV 三向校验（键长 / 记录上限 / 下限）；
//! - 淘汰与惰性恢复、Flush 触发器数据保留、LEN O(1) 生命周期；
//! - Checkpoint/Recover 族与 RIRESTORE 存根回写；
//! - 多客户端并发最终一致性；
//! - 流式/零拷贝扫描细节见 `range_index_scan.rs`。
//!
//! 并发语义差异：C# RIDel 对不存在的字段返回 0，本实现 bf-tree 墓碑删除不区分
//! 字段是否存在，删除恒返回 true（幂等），见 wedb_standalone ri_del 注释。

use std::sync::Arc;

use aok::{OK, Result, Void};
use compio::runtime::{Runtime, spawn};
use tempfile::{TempDir, tempdir};
use wbftree::{StorageBackendType, TreeTuning};
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::{CheckpointManager, RangeIndexError, StoreConfig, WedbStore};

use crate::support::open_store_in;

/// 与 C# 测试一致的默认树调优：min_record=8 / max_record=1024 / max_key_len=128
const TUNE: TreeTuning = TreeTuning {
  cache_size: 65536,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 0,
};

/// 构造挂载 RangeIndex 目录的会话层引擎（目录组装为变体，存储开在 support 一处）
fn open_store(dir: &TempDir, name: &str) -> Result<Arc<WedbStore<SegmentedDevice>>> {
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?
    .with_range_index_dir(dir.path().join("range_indexes"));
  open_store_in(dir, name, config)
}

/// RISetInvalidKVFieldTooLongTest / RISetInvalidKVValueTooLongTest /
/// RISetInvalidKVRecordTooSmallTest：键长 / 记录上限 / 记录下限三向校验
#[test]
fn test_ri_invalid_kv_validation() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "ri_kv.db")?;
    let session = store.new_session()?;

    session.range_index_create(b"idx", StorageBackendType::Disk, TUNE).await?;

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
      .range_index_set(b"idx", &[b'k'; TUNE.max_key_len], &[b'v'; 100])
      .await?;
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
        .range_index_create(b"idx", StorageBackendType::Disk, TUNE)
        .await?;
    }

    const CLIENTS: usize = 6;
    const FIELDS_PER_CLIENT: usize = 50;
    let mut handles = Vec::new();
    for c in 0..CLIENTS {
      let store = Arc::clone(&store);
      handles.push(spawn(async move {
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
          .range_index_create(key, StorageBackendType::Disk, TUNE)
          .await?;
        session.range_index_set(key, b"field", val).await?;
      }
      let meta = CheckpointManager::new()
        .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
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
      .range_index_create(b"idx", StorageBackendType::Disk, TUNE)
      .await?;
    session
      .range_index_set(b"idx", b"field", b"version_1")
      .await?;
    let meta1 = CheckpointManager::new()
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
      .await?;
    let token1 = meta1.token;

    session
      .range_index_set(b"idx", b"field", b"version_2")
      .await?;
    let meta2 = CheckpointManager::new()
      .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
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
        .range_index_create(b"idx", StorageBackendType::Disk, TUNE)
        .await?;
      session.range_index_set(b"idx", b"field", b"value").await?;
      let meta = CheckpointManager::new()
        .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
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
      .range_index_create(b"idx", StorageBackendType::Disk, TUNE)
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

/// RIRESTORE 存根回写（对标 RangeIndexManager.Index.cs:184-192 RecreateIndex +
/// RMWMethods.cs:954-957 RIRESTORE）：检查点恢复后首次访问激活树并回写新句柄、
/// 清除 Recovered 位；访问前存根保持检查点恢复态（句柄 0 + Recovered 置位），
/// 二次访问幂等不再漂移
#[test]
fn test_ri_restore_stub_rebind_after_recovery() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let db_path = dir.path().join("ri_recreate.db");
    let token;
    {
      let store = open_store(&dir, "ri_recreate.db")?;
      let session = store.new_session()?;
      session
        .range_index_create(b"idx", StorageBackendType::Disk, TUNE)
        .await?;
      session.range_index_set(b"idx", b"field", b"value").await?;
      token = CheckpointManager::new()
        .create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver)
        .await?
        .token;
    } // 原 store 全部销毁，模拟崩溃

    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let recovered = Arc::new(CheckpointManager::recover(&ckpt_dir, token, device).await?);
    let session = recovered.new_session()?;

    // 恢复后未访问：存根保持 MarkRecoveredFromCheckpoint 态 (句柄 0 + 恢复位)
    let pending = session.range_index_config(b"idx").await?;
    assert!(pending.is_recovered());
    assert_eq!(pending.tree_handle, 0);

    // 首次访问：激活树 + RIRESTORE 回写，数据经预置检查点快照精确回读
    assert_eq!(
      session.range_index_get(b"idx", b"field").await?,
      Some(b"value".to_vec())
    );
    let healed = session.range_index_config(b"idx").await?;
    assert!(!healed.is_recovered(), "激活后必须清除 Recovered 位");
    let metrics = session.range_index_metrics(b"idx").await?;
    assert!(metrics.is_live);
    assert_eq!(
      healed.tree_handle, metrics.tree_handle,
      "存根句柄必须重绑到当前在线树 (对标 RecreateIndex)"
    );

    // 二次访问幂等：治愈后的存根稳定，不再发生写漂移
    assert_eq!(
      session.range_index_get(b"idx", b"field").await?,
      Some(b"value".to_vec())
    );
    assert_eq!(session.range_index_config(b"idx").await?, healed);
    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 索引淘汰与注册表生命周期（对标 test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIEvictionFreesEvictedTreeButKeepsLiveTest 的
/// 树实例语义；数据面保留由惰性恢复承担，C# 的数据保留依赖 flush 快照体系，
/// wedb 为预留未接线，故此处只验证注册表驱逐与重新打开闭环）
#[test]
fn test_ri_evicted_tree_reopens_on_access() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "ri_evict.db")?;
    let session = store.new_session()?;

    session
      .range_index_create(b"live", StorageBackendType::Disk, TUNE)
      .await?;
    session
      .range_index_create(b"evict", StorageBackendType::Disk, TUNE)
      .await?;
    session
      .range_index_set(b"evict", b"field", b"value")
      .await?;

    // 淘汰 evict (保留磁盘文件)：live 树不受影响
    assert!(store.range_index.dispose_tree_under_lock(b"evict", false)?);
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

/// 验证页面刷盘自动触发 OnFlush 存根置位与快照生成，驱逐后惰性恢复无缝保留历史数据
/// (1:1 对标 C# FlushRecordsInRange -> GarnetRecordTriggers.OnFlush -> RestoreTree 数据保留全闭环)
#[test]
fn test_ri_flush_trigger_and_lazy_recovery_preserves_data() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "ri_flush_trigger.db")?;
    let session = store.new_session()?;

    let key = b"flushed_ri";
    session
      .range_index_create(key, StorageBackendType::Disk, TUNE)
      .await?;
    session.range_index_set(key, b"alpha", b"val_alpha").await?;
    session.range_index_set(key, b"beta", b"val_beta").await?;

    // 1. 刷盘前存根为未刷盘状态
    let (_, stub_before) = session.load_range_index_stub(key).await?.unwrap();
    assert!(!stub_before.is_flushed(), "刷盘前存根不得为 Flushed");

    // 2. 执行全库刷盘 (触发 on_flush_pages -> on_flush_address)
    store.flush_all().await?;

    // 3. 验证内存记录与存根已被原位置位为 is_flushed = true
    let (_, stub_after) = session.load_range_index_stub(key).await?.unwrap();
    assert!(
      stub_after.is_flushed(),
      "flush_all 后存根必须被自动置位为 Flushed"
    );

    // 4. 模拟内存树被驱逐卸载 (仅保留磁盘文件)
    assert!(store.range_index.dispose_tree_under_lock(key, false)?);
    assert!(store.range_index.get_tree(key).is_none(), "内存树已卸载");

    // 5. 再次访问：惰性恢复重新打开并从 flush 快照完整回读历史数据
    let val_a = session.range_index_get(key, b"alpha").await?;
    assert_eq!(val_a.as_deref(), Some(b"val_alpha".as_slice()));
    let val_b = session.range_index_get(key, b"beta").await?;
    assert_eq!(val_b.as_deref(), Some(b"val_beta".as_slice()));

    // 6. 验证树已重新激活在线
    assert!(store.range_index.get_tree(key).is_some());

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// RIRENAME 独有覆盖：删空后重命名继承旧索引元数据（len=0），改名后可继续写入，
/// 整键删除物理清理。纯 LEN 计数语义由
/// wedb_standalone/tests/range_index_tests.rs:ri_len_basic_test 权威覆盖。
#[test]
fn test_ri_rename_range_index_lifecycle() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "ri_rename.db")?;
    let session = store.new_session()?;

    let key = b"my_ri";
    session
      .range_index_create(key, StorageBackendType::Disk, TUNE)
      .await?;
    session.range_index_set(key, b"field1", b"value1").await?;

    // 覆盖写不增长 len
    session
      .range_index_set(key, b"field1", b"value1_updated")
      .await?;
    assert_eq!(session.range_index_len(key).await?, 1);

    // 删空后重命名：继承旧索引元数据长度 0
    assert!(session.range_index_del(key, b"field1").await?);
    let new_key = b"my_ri_renamed";
    session.rename_range_index(key, new_key).await?;
    assert!(session.range_index_exists(new_key).await?);
    assert_eq!(session.range_index_len(new_key).await?, 0);

    // 改名后的索引可继续写入并验证计数
    session
      .range_index_set(new_key, b"field_x", b"value_x")
      .await?;
    assert_eq!(session.range_index_len(new_key).await?, 1);

    // 删除整键：物理清理索引文件，len 报 NotFound
    assert!(session.delete(new_key).await?);
    assert!(!session.range_index_exists(new_key).await?);
    assert!(matches!(
      session.range_index_len(new_key).await,
      Err(RangeIndexError::NotFound)
    ));

    aok::Result::<()>::Ok(())
  })?;
  OK
}
