//! RangeIndex 会话层独有语义测试 (1:1 移植 C# Garnet.test.rangeindex/RespRangeIndexTests)
//!
//! CRUD / WrongType / InvalidKV / Exists / MultipleFields / Scan/Range 的网络层
//! 行为测试由 wnode/tests/range_index_tests.rs 承接（含 RI.COUNT 计数面）；
//! 本文件仅保留 store 层独有语义：
//! - InvalidKV 三向校验（键长 / 记录上限 / 下限）；
//! - 淘汰与惰性恢复、Flush 触发器数据保留、计数 O(1) 生命周期；
//! - Checkpoint/Recover 族与 RIRESTORE 存根回写；
//! - 多客户端并发最终一致性；
//! - 删空自愈：RI.DEL 使 MetaValue.size 归零即接树态键排空回收单点销毁整键
//!   （元记录墓碑 + 随键 TTL + 树实例与换号旁表注销），杜绝幽灵空索引；
//! - 流式/零拷贝扫描细节见 `range_index_scan.rs`。
//!
//! 并发语义差异：C# RIDel 对不存在的字段返回 0，本实现 bf-tree 墓碑删除不区分
//! 字段是否存在，删除恒返回 true（幂等），见 `range_index/ops.rs:range_index_del`。

use std::sync::Arc;

use aok::{OK, Result, Void};
use compio::runtime::{Runtime, spawn};
use parking_lot::Mutex;
use tempfile::{TempDir, tempdir};
use wbftree::{ScanReturnField, StorageBackendType, TreeTuning};
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::{RangeIndexError, StoreConfig, StoreEvent, StoreEventSink, WedbStore};
use wval::GarnetObjectType;

use super::flush_database::wait_file_gone;
use crate::support::{open_store_in, tree_id_key};

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
#[compio::test]
async fn test_ri_invalid_kv_validation() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "ri_kv.db")?;
  let session = store.new_session()?;

  session
    .range_index_create(b"idx", StorageBackendType::Disk, TUNE)
    .await?;

  // 字段超长 (max_key_len = 128)
  let long_field = vec![b'k'; TUNE.max_key_len + 1];
  let err = session.range_index_set(b"idx", &long_field, b"v").await;
  assert!(matches!(
    err,
    Err(RangeIndexError::InvalidKV { key_len: 129, .. })
  ));

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
  OK
}

/// RIConcurrentMultiClientTest：多客户端并发读写同一索引的最终一致性
#[compio::test]
async fn test_ri_concurrent_multi_client() -> Void {
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
  OK
}

/// RICheckpointWithMultipleTreesAndRecoverTest：多索引检查点全量恢复
#[compio::test]
async fn test_ri_checkpoint_recover_multi_tree() -> Void {
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
    let meta = store
      .create_checkpoint(&ckpt_dir, CheckpointType::FoldOver)
      .await?;
    token = meta.token;
  } // 原 store 全部销毁，模拟崩溃

  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("ri_ckpt_multi.db"),
  )?);
  let recovered = Arc::new(WedbStore::recover(&ckpt_dir, token, device).await?);
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
  OK
}

/// RITwoCheckpointsRecoverToLatestTest / RIRecoverToEarlierCheckpointTest：
/// 双检查点下可恢复到最新版本，也可显式恢复到早期版本
#[compio::test]
async fn test_ri_two_checkpoints_recover_to_version() -> Void {
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
    let latest = Arc::new(WedbStore::recover(&ckpt_dir, token2, device).await?);
    let session = latest.new_session()?;
    assert_eq!(
      session.range_index_get(b"idx", b"field").await?,
      Some(b"version_2".to_vec())
    );
  }

  // 显式恢复到早期检查点：旧值
  {
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let earlier = Arc::new(WedbStore::recover(&ckpt_dir, token1, device).await?);
    let session = earlier.new_session()?;
    assert_eq!(
      session.range_index_get(b"idx", b"field").await?,
      Some(b"version_1".to_vec())
    );
  }
  OK
}

/// RIDeleteAfterRecoveryTest：恢复后删除索引并重建，状态机完全就绪
#[compio::test]
async fn test_ri_delete_after_recovery() -> Void {
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
    let meta = store
      .create_checkpoint(&ckpt_dir, CheckpointType::FoldOver)
      .await?;
    token = meta.token;
  }

  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let recovered = Arc::new(WedbStore::recover(&ckpt_dir, token, device).await?);
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
  OK
}

/// RIRESTORE 存根回写（对标 RangeIndexManager.Index.cs:184-192 RecreateIndex +
/// RMWMethods.cs:954-957 RIRESTORE）：检查点恢复后首次访问激活树并回写新句柄、
/// 清除 Recovered 位；访问前存根保持检查点恢复态（句柄 0 + Recovered 置位），
/// 二次访问幂等不再漂移
#[compio::test]
async fn test_ri_restore_stub_rebind_after_recovery() -> Void {
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
    token = store
      .create_checkpoint(&ckpt_dir, CheckpointType::FoldOver)
      .await?
      .token;
  } // 原 store 全部销毁，模拟崩溃

  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let recovered = Arc::new(WedbStore::recover(&ckpt_dir, token, device).await?);
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
  OK
}

/// 索引淘汰与注册表生命周期（对标 test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs:RIEvictionFreesEvictedTreeButKeepsLiveTest 的
/// 树实例语义；摘树 (delete_file=false) 的离线收口快照已接线 (wbftree
/// lifecycle.rs:detach_tree)，淘汰前已确认写入随收口快照持久化，重新打开
/// 必须无损回读——与升阶树摘除恢复「存量数据点查无损」承诺
/// (wnode tiered_stub_heal_write) 同判据）
#[compio::test]
async fn test_ri_evicted_tree_reopens_on_access() -> Void {
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
  assert!(
    store
      .range_index
      .dispose_tree_under_lock(&tree_id_key(0, 0, b"evict"), false)?
  );
  assert!(
    store
      .range_index
      .get_tree(&tree_id_key(0, 0, b"evict"))
      .is_none(),
    "淘汰后注册表必须摘除"
  );
  assert!(
    store
      .range_index
      .get_tree(&tree_id_key(0, 0, b"live"))
      .is_some(),
    "未淘汰树必须保持在线"
  );

  // 再次访问：走惰性恢复重新打开，收口快照承接淘汰前已确认写入，数据无损
  assert_eq!(
    session.range_index_get(b"evict", b"field").await?,
    Some(b"value".to_vec())
  );
  assert!(
    store
      .range_index
      .get_tree(&tree_id_key(0, 0, b"evict"))
      .is_some(),
    "惰性恢复必须重新注册在线树"
  );
  OK
}

/// 验证页面刷盘自动触发 OnFlush 存根置位与快照生成，驱逐后惰性恢复无缝保留历史数据
/// (1:1 对标 C# FlushRecordsInRange -> GarnetRecordTriggers.OnFlush -> RestoreTree 数据保留全闭环)
#[compio::test]
async fn test_ri_flush_trigger_and_lazy_recovery_preserves_data() -> Void {
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
  assert!(
    store
      .range_index
      .dispose_tree_under_lock(&tree_id_key(0, 0, key), false)?
  );
  assert!(
    store
      .range_index
      .get_tree(&tree_id_key(0, 0, key))
      .is_none(),
    "内存树已卸载"
  );

  // 5. 再次访问：惰性恢复重新打开并从 flush 快照完整回读历史数据
  let val_a = session.range_index_get(key, b"alpha").await?;
  assert_eq!(val_a.as_deref(), Some(b"val_alpha".as_slice()));
  let val_b = session.range_index_get(key, b"beta").await?;
  assert_eq!(val_b.as_deref(), Some(b"val_beta".as_slice()));

  // 6. 验证树已重新激活在线
  assert!(
    store
      .range_index
      .get_tree(&tree_id_key(0, 0, key))
      .is_some()
  );
  OK
}

/// RIRENAME 独有覆盖：删空即自愈销毁整键（不再有「继承长度 0」的空索引），
/// 自愈后同名重建可继续读写并改名，整键删除物理清理。计数语义由
/// wnode/tests/range_index_tests.rs:ri_count_basic_test 与
/// 本文件 ri_count_metadata_stable_across_promote_and_rename_test 权威覆盖。
#[compio::test]
async fn test_ri_rename_range_index_lifecycle() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "ri_rename.db")?;
  let session = store.new_session()?;

  let key = b"my_ri";
  session
    .range_index_create(key, StorageBackendType::Disk, TUNE)
    .await?;
  session.range_index_set(key, b"field1", b"value1").await?;

  // 覆盖写不增长计数
  session
    .range_index_set(key, b"field1", b"value1_updated")
    .await?;
  assert_eq!(session.range_index_count(key).await?, 1);

  // 删空最后一字段即自愈：整键消亡，其后的改名无索引可迁移（内核防御性
  // 空操作），绝不把长度 0 的存活索引继承给新键
  assert!(session.range_index_del(key, b"field1").await?);
  assert!(!session.range_index_exists(key).await?);
  assert!(matches!(
    session.range_index_count(key).await,
    Err(RangeIndexError::NotFound)
  ));
  let new_key = b"my_ri_renamed";
  session.rename_range_index(key, new_key).await?;
  assert!(!session.range_index_exists(new_key).await?);

  // 自愈后同名重建：drain 已注销树实例与换号旁表登记，重建不被 IndexExists
  // 拦截；改名后的索引可继续写入并验证计数
  session
    .range_index_create(key, StorageBackendType::Disk, TUNE)
    .await?;
  session.range_index_set(key, b"field_x", b"value_x").await?;
  session.rename_range_index(key, new_key).await?;
  assert!(session.range_index_exists(new_key).await?);
  assert_eq!(session.range_index_count(new_key).await?, 1);

  // 删除整键：物理清理索引文件，计数报 NotFound
  assert!(session.delete(new_key).await?);
  assert!(!session.range_index_exists(new_key).await?);
  assert!(matches!(
    session.range_index_count(new_key).await,
    Err(RangeIndexError::NotFound)
  ));
  OK
}

/// range_index_set_batch 批量折叠语义（rust 侧工程优化，C# 无对应批量接口）：
/// 新增计数 / 同批重复键后者胜 / 覆盖更新不增长 len / 乱序输入落盘有序 /
/// 空批次零副作用 / 预校验整体失败零副作用 / 事件逐条发射
#[compio::test]
async fn test_ri_set_batch_semantics() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "ri_batch.db")?;
  let session = store.new_session()?;

  let key = b"my_ri";
  session
    .range_index_create(key, StorageBackendType::Disk, TUNE)
    .await?;

  // 1. 乱序批量插入 3 个新键：返回真实新增数 3，len 同步增长
  let batch = [
    (b"charlie".as_slice(), b"v_c".as_slice()),
    (b"alpha".as_slice(), b"v_a".as_slice()),
    (b"bravo".as_slice(), b"v_b".as_slice()),
  ];
  let inserted = session.range_index_set_batch(key, &batch).await?;
  assert_eq!(inserted, 3);
  assert_eq!(session.range_index_count(key).await?, 3);

  // 2. 同批重复键仅末值生效，去重计数
  let dup_batch = [
    (b"dup".as_slice(), b"first".as_slice()),
    (b"dup".as_slice(), b"second".as_slice()),
    (b"delta".as_slice(), b"v_d".as_slice()),
  ];
  let inserted = session.range_index_set_batch(key, &dup_batch).await?;
  assert_eq!(inserted, 2);
  assert_eq!(session.range_index_count(key).await?, 5);
  assert_eq!(
    session.range_index_get(key, b"dup").await?,
    Some(b"second".to_vec())
  );

  // 3. 覆盖已有键不增长 len，新键计入新增
  let mixed_batch = [
    (b"alpha".as_slice(), b"v_a2".as_slice()),
    (b"echo".as_slice(), b"v_e2".as_slice()),
  ];
  let inserted = session.range_index_set_batch(key, &mixed_batch).await?;
  assert_eq!(inserted, 1);
  assert_eq!(session.range_index_count(key).await?, 6);
  assert_eq!(
    session.range_index_get(key, b"alpha").await?,
    Some(b"v_a2".to_vec())
  );

  // 4. 乱序输入落盘有序：scan 输出按键字节序
  let mut scanned = Vec::new();
  session
    .range_index_scan_stream(key, b"alpha", 100, ScanReturnField::Key, |k, _| {
      scanned.push(k.to_vec());
      true
    })
    .await?;
  let mut sorted = scanned.clone();
  sorted.sort();
  assert_eq!(scanned, sorted);

  // 5. 空批次：零新增零副作用
  assert_eq!(session.range_index_set_batch(key, &[]).await?, 0);
  assert_eq!(session.range_index_count(key).await?, 6);

  // 6. 预校验整体失败零副作用：一条低于 min_record_size (8) 即整体拒绝
  let bad_batch = [
    (b"foxtrot".as_slice(), b"v_f_long_enough".as_slice()),
    (b"golf".as_slice(), b"x".as_slice()),
  ];
  let err = session.range_index_set_batch(key, &bad_batch).await;
  assert!(matches!(
    err,
    Err(RangeIndexError::InvalidKV { total_len: 5, .. })
  ));
  assert_eq!(session.range_index_count(key).await?, 6);
  assert_eq!(session.range_index_get(key, b"foxtrot").await?, None);
  OK
}

/// 批量装载内核与逐条写入的终态等价 (存储层回归门)
///
/// 同一乱序条目集两条路径建树：排序批量内核 (ri_set_batch / 升阶灌入) vs
/// 逐字段 range_index_set。断言三层等价：
/// - 去重条数与落盘 meta.size 同值 (内核前置校验后按唯一键计数，旧逐条路径
///   把同批重复键重复计入)；
/// - 全区间有序扫描字节流逐字节相同 (引擎侧页结构与页内序同源)；
/// - 全键点读逐字节相同 (含长值页外链)。
#[compio::test]
async fn test_ri_bulk_load_matches_per_field_insert() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "ri_bulk_equiv.db")?;
  let session = store.new_session()?;

  // 乱序输入 + 同批重复键 + 每 16 条一条长值 (触页内搬迁与页外链)
  let mut entries: Vec<(Vec<u8>, Vec<u8>)> = (0..2048usize)
    .map(|i| {
      let idx = (i * 7919) % 2048;
      let val = if idx % 16 == 0 {
        format!("big{idx:06}").repeat(24)
      } else {
        format!("val{idx:06}")
      };
      (format!("f{idx:06}").into_bytes(), val.into_bytes())
    })
    .collect();
  entries.push((b"f000001".to_vec(), b"first".to_vec()));
  entries.push((b"f000001".to_vec(), b"last-write-wins".to_vec()));
  let view: Vec<(&[u8], &[u8])> = entries
    .iter()
    .map(|(k, v)| (k.as_slice(), v.as_slice()))
    .collect();

  session
    .range_index_create(b"ri_batch", StorageBackendType::Disk, TUNE)
    .await?;
  let bulk_count = session.range_index_set_batch(b"ri_batch", &view).await?;

  session
    .range_index_create(b"ri_seq", StorageBackendType::Disk, TUNE)
    .await?;
  for (field, value) in &entries {
    session.range_index_set(b"ri_seq", field, value).await?;
  }

  assert_eq!(bulk_count, 2048, "同批重复键只计一次新增");
  assert_eq!(
    session.range_index_count(b"ri_batch").await?,
    session.range_index_count(b"ri_seq").await?,
    "批量与逐条的落盘 meta.size 必须同值"
  );

  let mut dumps: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::new();
  for name in [b"ri_batch".as_slice(), b"ri_seq".as_slice()] {
    let mut out = Vec::new();
    session
      .range_index_scan_stream(
        name,
        b"\x00",
        entries.len(),
        ScanReturnField::KeyAndValue,
        |k, v| {
          out.push((k.to_vec(), v.to_vec()));
          true
        },
      )
      .await?;
    dumps.push(out);
  }
  let (dump_bulk, dump_seq) = (&dumps[0], &dumps[1]);
  assert_eq!(dump_bulk.len(), 2048);
  assert_eq!(dump_bulk, dump_seq, "全区间有序扫描字节流须与逐条写入一致");
  assert!(
    dump_bulk.windows(2).all(|w| w[0].0 < w[1].0),
    "扫描输出须严格按键升序"
  );

  // 集合升阶灌入路径与逐字段建树终态等价。升阶元记录的容器类型是 Hash，须经
  // [`load_collection_stub`](wkv::StoreSession::load_collection_stub) 读，RI 类型
  // 门禁的 range_index_count / range_index_get 不适用 (否则 WRONGTYPE)；容器回读的端到端
  // 口径由 wnode/tests/tiered_cmds_align.rs 的 HGETALL/ZCARD 族承接。
  session
    .promote_collection_to_bftree(
      b"ri_promoted",
      GarnetObjectType::Hash,
      entries.clone(),
      i64::MAX,
      false,
    )
    .await?;
  let (promoted_meta, mut promoted_stub) = session
    .load_collection_stub(b"ri_promoted")
    .await?
    .ok_or(RangeIndexError::NotFound)?;
  assert_eq!(
    promoted_meta.size as usize,
    dump_seq.len(),
    "升阶灌入的 meta.size 须等于内核去重条数"
  );
  let promoted_tree = session
    .acquire_tree_read(b"ri_promoted", &mut promoted_stub)
    .await?;
  let mut dump_promoted = Vec::new();
  promoted_tree.scan_with_count_callback(
    b"\x00",
    entries.len(),
    ScanReturnField::KeyAndValue,
    |k, v| {
      dump_promoted.push((k.to_vec(), v.to_vec()));
      true
    },
  )?;
  assert_eq!(
    dump_promoted, *dump_seq,
    "升阶灌入的全区间有序扫描须与逐字段写入一致 (含长值页外链，逐键点读等价见 \
     wbftree/tests/bulk_load.rs 内核层)"
  );

  for (field, _) in &entries {
    let seq_val = session.range_index_get(b"ri_seq", field).await?;
    assert_eq!(
      session.range_index_get(b"ri_batch", field).await?,
      seq_val,
      "逐键点读 (含长值页外链) 须同字节"
    );
  }
  assert_eq!(
    session.range_index_get(b"ri_batch", b"f000001").await?,
    Some(b"last-write-wins".to_vec()),
    "同批重复键取输入序末值"
  );
  OK
}

/// range_index_set_batch 事件逐条发射：每条输入各产生一条 RangeIndexWrite
/// （AOF 复制端逐条重放收敛同态，对标单点 range_index_set 语义）
#[test]
fn test_ri_set_batch_events() -> Void {
  type RiLog = Arc<Mutex<Vec<(Vec<u8>, Vec<u8>)>>>;

  fn ri_sink(log: RiLog) -> StoreEventSink {
    StoreEventSink::new(log, |log, _ver, _aof_session_id, event| {
      if let StoreEvent::RangeIndexWrite { field, val, .. } = event {
        log.lock().push((field.to_vec(), val.to_vec()));
      }
      Ok(())
    })
  }

  Runtime::new()?.block_on(async {
    let dir = tempdir()?;
    let store = open_store(&dir, "ri_batch_events.db")?;
    let log: RiLog = Arc::new(Mutex::new(Vec::new()));
    assert!(store.set_event_sink(ri_sink(Arc::clone(&log))));
    let session = store.new_session()?;

    let key = b"my_ri";
    session
      .range_index_create(key, StorageBackendType::Disk, TUNE)
      .await?;

    let batch = [
      (b"f1".as_slice(), b"value_1".as_slice()),
      (b"f2".as_slice(), b"value_2".as_slice()),
    ];
    session.range_index_set_batch(key, &batch).await?;

    let events = log.lock().clone();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0], (b"f1".to_vec(), b"value_1".to_vec()));
    assert_eq!(events[1], (b"f2".to_vec(), b"value_2".to_vec()));

    aok::Result::<()>::Ok(())
  })?;
  OK
}

/// 验证冷节点与 Flushed 存根在访问 (点查、范围扫描、写入) 时通过原子 RIPROMOTE 重新提升回可变区尾部
/// (1:1 对标 C# RMWMethods.cs:RIPROMOTE / NeedInitialUpdate / VarLenInputMethods.cs:GetRMWInitialFieldInfo / GetRMWModifiedFieldInfo)
#[compio::test]
async fn test_ri_cold_promote_on_point_and_scan() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "ri_cold_promote.db")?;
  let session = store.new_session()?;

  let key = b"cold_promote_idx";
  session
    .range_index_create(key, StorageBackendType::Disk, TUNE)
    .await?;
  session.range_index_set(key, b"alpha", b"val_alpha").await?;
  session.range_index_set(key, b"beta", b"val_beta").await?;
  session.range_index_set(key, b"gamma", b"val_gamma").await?;

  // 1. 刷盘前存根为未刷盘状态
  let (_, stub0) = session.load_range_index_stub(key).await?.unwrap();
  assert!(!stub0.is_flushed());

  // 2. 执行全库刷盘 -> 存根置位 is_flushed = true
  store.flush_all().await?;
  let (_, stub_flushed) = session.load_range_index_stub(key).await?.unwrap();
  assert!(
    stub_flushed.is_flushed(),
    "flush_all 后存根必须被自动置位为 Flushed"
  );

  // 3. 点查访问：触发 RIPROMOTE 重新提升至可变区尾部，清除 Flushed 标记
  let val_alpha = session.range_index_get(key, b"alpha").await?;
  assert_eq!(val_alpha.as_deref(), Some(b"val_alpha".as_slice()));

  let (_, stub_promoted) = session.load_range_index_stub(key).await?.unwrap();
  assert!(
    !stub_promoted.is_flushed(),
    "点查后存根必须通过 RIPROMOTE 自动提升至尾部并清除 Flushed 标记"
  );

  // 4. 再次触发页面刷盘使存根重新处于 Flushed 状态
  store.on_flush_pages(0, 0)?;
  let (_, stub_flushed2) = session.load_range_index_stub(key).await?.unwrap();
  assert!(stub_flushed2.is_flushed());

  // 5. 范围扫描访问：同样触发 RIPROMOTE 提升至尾部
  let mut scanned = Vec::new();
  session
    .range_index_scan_stream(key, b"a", 10, ScanReturnField::Key, |k, _| {
      scanned.push(k.to_vec());
      true
    })
    .await?;
  assert_eq!(scanned.len(), 3);

  let (_, stub_promoted2) = session.load_range_index_stub(key).await?.unwrap();
  assert!(
    !stub_promoted2.is_flushed(),
    "范围扫描后存根必须通过 RIPROMOTE 自动提升至尾部并清除 Flushed 标记"
  );

  // 6. 淘汰下线 + 刷盘：内存树卸载且存根处于 Flushed 状态
  store.on_flush_pages(0, 0)?;
  assert!(
    store
      .range_index
      .dispose_tree_under_lock(&tree_id_key(0, 0, key), false)?
  );
  assert!(
    store
      .range_index
      .get_tree(&tree_id_key(0, 0, key))
      .is_none()
  );

  // 6.1 O(1) 计数探针：树已下线，计数仍直读 MetaValue.size 返回 3，且不唤醒树
  // （计数若退化为 acquire_tree_read / 扫树，get_or_open_tree 会重建注册表
  // 条目，下面这条 is_none 断言即翻红）
  assert_eq!(session.range_index_count(key).await?, 3, "树下线后计数不变");
  assert!(
    store
      .range_index
      .get_tree(&tree_id_key(0, 0, key))
      .is_none(),
    "计数严禁唤醒已下线的树（O(1) 直读元数据探针）"
  );

  // 7. 闭区间范围扫描访问：同时触发惰性恢复 RIRESTORE + 提升 RIPROMOTE
  let mut range_scanned = Vec::new();
  session
    .range_index_range_stream(
      key,
      b"alpha",
      b"gamma",
      ScanReturnField::KeyAndValue,
      |k, v| {
        range_scanned.push((k.to_vec(), v.to_vec()));
        true
      },
    )
    .await?;
  assert_eq!(range_scanned.len(), 3);
  assert_eq!(range_scanned[0], (b"alpha".to_vec(), b"val_alpha".to_vec()));
  assert_eq!(range_scanned[1], (b"beta".to_vec(), b"val_beta".to_vec()));
  assert_eq!(range_scanned[2], (b"gamma".to_vec(), b"val_gamma".to_vec()));

  let (_, stub_promoted3) = session.load_range_index_stub(key).await?.unwrap();
  assert!(
    !stub_promoted3.is_flushed(),
    "恢复扫描后存根必须恢复在线且已解除 Flushed 状态"
  );
  assert!(
    store
      .range_index
      .get_tree(&tree_id_key(0, 0, key))
      .is_some()
  );

  // 8. 下线 → 惰性恢复（RIRESTORE）→ 提升（RIPROMOTE）全周期后计数不变
  assert_eq!(session.range_index_count(key).await?, 3, "恢复后计数不变");
  OK
}

/// 计数元数据不变量（rust 侧自定义扩展 RI.COUNT 的存储面，C# 无对应计数命令）：
/// 空索引计数 0、逐字段增删同步增减、删空即自愈销毁整键报 NotFound、
/// 刷盘 RIPROMOTE / 树下线 RIRESTORE 前后不变、RENAME 迁移后计数承接至新键
/// 且源键随段五排空持久墓碑读侧消失、整键删除后报 NotFound
#[compio::test]
async fn ri_count_metadata_stable_across_promote_and_rename_test() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "ri_count_meta.db")?;
  let session = store.new_session()?;
  let key = b"count_idx";

  // 1. 空索引计数 0（MetaValue.size 初值，未写入任何树记录）
  session
    .range_index_create(key, StorageBackendType::Disk, TUNE)
    .await?;
  assert_eq!(session.range_index_count(key).await?, 0);

  // 2. 逐字段写入：计数逐条增长，覆盖写不增长
  for (i, f) in [b"aa".as_slice(), b"bb".as_slice(), b"cc".as_slice()]
    .into_iter()
    .enumerate()
  {
    session.range_index_set(key, f, b"payload").await?;
    assert_eq!(session.range_index_count(key).await?, i + 1);
  }
  session.range_index_set(key, b"aa", b"payload2").await?;
  assert_eq!(
    session.range_index_count(key).await?,
    3,
    "覆盖写不得增长计数"
  );

  // 3. 刷盘（存根置 Flushed）后计数不变且不唤醒提升
  store.flush_all().await?;
  assert_eq!(session.range_index_count(key).await?, 3);
  let (_, stub_flushed) = session.load_range_index_stub(key).await?.unwrap();
  assert!(stub_flushed.is_flushed(), "前置条件：刷盘后存根为 Flushed");
  assert_eq!(
    session.range_index_count(key).await?,
    3,
    "计数不得因刷盘而失真"
  );
  let (_, stub_after_count) = session.load_range_index_stub(key).await?.unwrap();
  assert!(
    stub_after_count.is_flushed(),
    "计数不得触发 RIPROMOTE（刷盘位必须原样保留）"
  );

  // 4. 逐字段删空：非删空臂索引存活计数递减，最后一笔删除即触发自愈，
  // 整键销毁不留 :0 幽灵元记录（RI 类型 MetaValue::is_live 恒活，删空判据
  // 只能取 size，见 wkv range_index/ops.rs:range_index_del 删空臂）
  for f in [b"aa".as_slice(), b"bb".as_slice()] {
    assert!(session.range_index_del(key, f).await?);
    assert!(
      session.range_index_exists(key).await?,
      "非删空删除后索引必须存活（零行为回归）"
    );
  }
  assert_eq!(session.range_index_count(key).await?, 1);
  assert!(session.range_index_del(key, b"cc").await?);
  assert!(!session.range_index_exists(key).await?);
  assert!(matches!(
    session.range_index_count(key).await,
    Err(RangeIndexError::NotFound)
  ));

  // 5. 自愈后同名重建再写非空索引，RENAME 迁移：新键计数承接迁移前实况，
  // 源键随段五排空持久墓碑读侧消失（迁移即移动，claim 方案下元记录墓碑由排空单点落笔）
  session
    .range_index_create(key, StorageBackendType::Disk, TUNE)
    .await?;
  session.range_index_set(key, b"dd", b"payload").await?;
  session.range_index_set(key, b"ee", b"payload").await?;
  assert_eq!(session.range_index_count(key).await?, 2);
  let new_key = b"count_idx_renamed";
  session.rename_range_index(key, new_key).await?;
  assert_eq!(
    session.range_index_count(new_key).await?,
    2,
    "迁移后计数不变"
  );
  assert!(
    matches!(
      session.range_index_count(key).await,
      Err(RangeIndexError::NotFound)
    ),
    "迁移后源键随元记录墓碑读侧消失，不留双活副本"
  );
  // 迁移目的端可继续读写且计数收敛
  session.range_index_set(new_key, b"ff", b"payload").await?;
  assert_eq!(session.range_index_count(new_key).await?, 3);

  // 6. 整键删除：计数报 NotFound（不留幽灵计数）
  assert!(session.delete(new_key).await?);
  assert!(matches!(
    session.range_index_count(new_key).await,
    Err(RangeIndexError::NotFound)
  ));
  OK
}

/// 删空自愈与在途惰性恢复交错（对标
/// libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:RestoreTree 独占锁内
/// `session.Read_RangeIndex` 重读——readRes != OK 即并发删除/墓碑化，立即拒绝，
/// 绝不触盘恢复）：点查采样存根后键被删空（元记录墓碑 + 树注销 + 数据文件挂
/// 纪元延迟 unlink），此刻文件尚在盘——在途点查/写臂持旧存根进惰性恢复慢路径
/// 必须按元记录复核终态拒绝（NotFound），不重开数据文件、不注册幽灵树、不补登
/// 换号旁表（修复前盲恢复重开文件即泄漏：settle 锁内复查被幽灵注册误判为新
/// 世代复用而跳过 unlink），且纪元异步 unlink 不被阻塞、文件最终彻底清理
#[compio::test]
async fn test_ri_lazy_restore_rejects_tombstoned_key() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "ri_restore_tombstone.db")?;
  let session = store.new_session()?;

  let key = b"tomb_idx";
  session
    .range_index_create(key, StorageBackendType::Disk, TUNE)
    .await?;
  session.range_index_set(key, b"alpha", b"val").await?;

  // 在途点查持有纪元守卫采样存根（此刻树在线、数据文件在盘，纪元屏障生效）
  let in_flight_session = store.new_session()?;
  let in_flight_guard = in_flight_session.enter_batch();
  let (_, mut stale) = session.load_collection_stub(key).await?.unwrap();
  let data_path = store
    .range_index
    .data_file_path_for_key(&tree_id_key(0, 0, key));
  assert!(data_path.exists(), "前置：RI 数据文件在盘");

  // 删空自愈：墓碑 + 树注销 + 延迟 unlink 挂入纪元（同步臂不删文件）
  session.handle_bftree_drain_and_delete(key, false).await?;
  assert!(
    data_path.exists(),
    "删除臂不得同步 unlink（纪元异步屏障，doc/zh/collection.md 删空生命周期）"
  );

  // 在途读臂/写臂持旧存根进慢路径：元记录复核拒绝，NotFound 终态
  assert_eq!(
    session.acquire_tree_read(key, &mut stale).await.err(),
    Some(RangeIndexError::NotFound)
  );
  assert_eq!(
    session.acquire_tree_write(key, &mut stale).await.err(),
    Some(RangeIndexError::NotFound)
  );

  // 不幽灵复活：注册表无条目、换号旁表无登记、元记录读侧消失
  assert!(
    store
      .range_index
      .get_tree(&tree_id_key(0, 0, key))
      .is_none(),
    "被拒恢复不得注册幽灵树条目"
  );
  assert!(
    store.snapshot_bftree_domains().is_empty(),
    "被拒恢复不得补登换号回收旁表"
  );
  assert!(session.load_collection_stub(key).await?.is_none());

  // 释放旧在途读臂纪元保护，使延迟 unlink 具备收敛条件
  drop(in_flight_guard);

  // 纪元异步 unlink 不被阻塞：数据文件彻底清理无残留
  wait_file_gone(&data_path, &store).await?;
  OK
}

/// 延迟 unlink 先于在途恢复落地的降级（对标 C# RestoreTree 的
/// `File.Exists(workingPath)` 假臂：Release 模式 LogWarning + return false，
/// 上层答 NOTFOUND，绝不致命崩溃）：数据文件已被纪元异步删除后，在途点查/
/// 写臂持旧存根进慢路径必须收敛 NotFound（修复前撞 get_or_open_tree 的
/// Error::Recovery 致命恢复错误上抛，破坏可用性）
#[compio::test]
async fn test_ri_lazy_restore_missing_file_degrades_to_not_found() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "ri_restore_missing_file.db")?;
  let session = store.new_session()?;

  let key = b"gone_idx";
  session
    .range_index_create(key, StorageBackendType::Disk, TUNE)
    .await?;
  session.range_index_set(key, b"alpha", b"val").await?;
  let (_, mut stale) = session.load_collection_stub(key).await?.unwrap();
  let data_path = store
    .range_index
    .data_file_path_for_key(&tree_id_key(0, 0, key));

  // 删空并驱动纪元延迟 unlink 收敛完毕（文件已物理消失）
  session.handle_bftree_drain_and_delete(key, false).await?;
  wait_file_gone(&data_path, &store).await?;

  // 在途读臂/写臂：NotFound 安全收敛，绝不上抛致命恢复错误
  assert_eq!(
    session.acquire_tree_read(key, &mut stale).await.err(),
    Some(RangeIndexError::NotFound)
  );
  assert_eq!(
    session.acquire_tree_write(key, &mut stale).await.err(),
    Some(RangeIndexError::NotFound)
  );
  OK
}

/// 并发 RI.SET 计数无丢失（写臂共享读锁丢更新回归：旧实现多写者在条带共享
/// 读锁下读相同旧 meta.size、各自加 1 后回写覆写，计数更新丢失，彻底破坏
/// O(1) RI.COUNT 契约；升格独占写锁 + 锁内 refresh_tiered_meta 刷新后，
/// 「装载 → 树写 → 计数 → 回写」全程互斥）：
/// 多会话并发向同一索引写入互不重复的字段，最终 range_index_count 严格
/// 等于写入字段总数
#[compio::test]
async fn test_ri_concurrent_set_no_lost_count() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "ri_conc_count.db")?;
  {
    let session = store.new_session()?;
    session
      .range_index_create(b"idx", StorageBackendType::Disk, TUNE)
      .await?;
  }

  const CLIENTS: usize = 8;
  const FIELDS_PER_CLIENT: usize = 40;
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
      Ok::<(), RangeIndexError>(())
    }));
  }
  for h in handles {
    h.await.expect("并发写任务异常退出").unwrap();
  }

  let total = CLIENTS * FIELDS_PER_CLIENT;
  let session = store.new_session()?;
  assert_eq!(
    session.range_index_count(b"idx").await?,
    total,
    "并发 RI.SET 后计数必须严格等于写入字段总数，不得有丢失更新"
  );

  // 批量臂同款回归：并发 SETBATCH 新增计数同样无丢失
  let mut handles = Vec::new();
  for c in 0..CLIENTS {
    let store = Arc::clone(&store);
    handles.push(spawn(async move {
      let session = store.new_session()?;
      let fields: Vec<String> = (0..FIELDS_PER_CLIENT)
        .map(|i| format!("b{c:02}_f{i:03}"))
        .collect();
      let batch: Vec<(&[u8], &[u8])> = fields
        .iter()
        .map(|f| (f.as_bytes(), f.as_bytes()))
        .collect();
      session.range_index_set_batch(b"idx", &batch).await?;
      Ok::<(), RangeIndexError>(())
    }));
  }
  for h in handles {
    h.await.expect("并发批量写任务异常退出").unwrap();
  }
  assert_eq!(
    session.range_index_count(b"idx").await?,
    total * 2,
    "并发 RI.SETBATCH 后计数同样不得有丢失更新"
  );
  OK
}

/// 并发删除最后两字段恰一方观测删空自愈（回归：旧实现两写者同持共享读锁
/// 同读旧 size=2、各自减为 1，双双错过 size == 0，删空自愈彻底漏过——
/// 空树实例与数据文件永久残留泄漏；升格独占写锁 + 锁内刷新后写臂串行，
/// 后发写者刷新到 size=1，实删递减归零必然恰好触发一次 drain）：
/// 断言两任务无死锁完成、整键消亡、树注册表清空、数据文件纪元异步
/// unlink 彻底清理无孤儿
#[compio::test]
async fn test_ri_concurrent_del_last_two_fields_drains_once() -> Void {
  let dir = tempdir()?;
  let store = open_store(&dir, "ri_conc_drain.db")?;
  let session = store.new_session()?;
  let key = b"drain_idx";
  session
    .range_index_create(key, StorageBackendType::Disk, TUNE)
    .await?;
  session
    .range_index_set(key, b"field_01", b"value_01")
    .await?;
  session
    .range_index_set(key, b"field_02", b"value_02")
    .await?;
  assert_eq!(session.range_index_count(key).await?, 2);
  let data_path = store
    .range_index
    .data_file_path_for_key(&tree_id_key(0, 0, key));
  assert!(data_path.exists(), "前置：RI 数据文件在盘");

  // 两并发写者各删最后两个字段之一：独占锁 + 锁内刷新使恰好一方观测
  // size == 0 并触发 handle_bftree_drain_and_delete（drop 守卫后 drain，
  // 无自死锁）
  let mut handles = Vec::new();
  for field in [b"field_01".to_vec(), b"field_02".to_vec()] {
    let store = Arc::clone(&store);
    handles.push(spawn(async move {
      let session = store.new_session()?;
      session.range_index_del(b"drain_idx", &field).await?;
      Ok::<(), RangeIndexError>(())
    }));
  }
  for h in handles {
    h.await
      .expect("并发删除任务异常退出（疑死锁或 drain 自死锁）")
      .unwrap();
  }

  // 恰一方观测删空并自愈：整键消亡、树实例注销、数据文件彻底清理无孤儿
  assert!(!session.range_index_exists(b"drain_idx").await?);
  assert!(
    matches!(
      session.range_index_count(b"drain_idx").await,
      Err(RangeIndexError::NotFound)
    ),
    "删空自愈后不得残留幽灵空元记录"
  );
  assert!(
    store
      .range_index
      .get_tree(&tree_id_key(0, 0, b"drain_idx"))
      .is_none(),
    "删空自愈必须注销树实例，杜绝空树永驻泄漏"
  );
  wait_file_gone(&data_path, &store).await?;
  OK
}
