//! 存储会话读一致性数据链路端到端集成测试
//!
//! 验证 StorageSession 与 wkv::ConsistentReadContext 的 1:1 对标与闭环：
//! - 单键读取：read_string_with / read_string 经过 consistent_read_context 触发 pre/post 协议
//! - 批量预取读取：read_with_prefetch 经过 consistent_read_context 触发 pre_batch/post_batch 协议与重试
//! - 键空间扫描与遍历：db_scan / db_keys / iterate_store 逐键触发一致读协议
//! - 会话生命周期与切换：绑定与解绑 read_session_state 行为一致

use std::{sync::Arc, time::Duration};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  aof::readconsistency::{
    read_consistency_manager::ReadConsistencyManager,
    replica_read_session_context::ReadSessionState,
  },
  databases::garnet_database::DEFAULT_VERSION_MAP_SIZE,
  storage::session::storage_session::StorageSession,
};
use wtxn::WatchVersionMap;

#[test]
fn test_storage_session_consistent_read_pipeline() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let dev = Arc::new(SegmentedDevice::single_file(dir.path().join("test.db"))?);
    let config = StoreConfig::new(16384, 65536, 64, 0.5)?;
    let store = Arc::new(WedbStore::open(config, dev)?);
    let session = store.new_session()?;

    let version_map = Arc::new(WatchVersionMap::new(DEFAULT_VERSION_MAP_SIZE));
    let mut ss: StorageSession<'_, _, ReadSessionState> =
      StorageSession::new_with_read_session(session.enter_batch(), version_map, None);

    // 默认非一致读会话
    assert!(!ss.is_consistent_read_session());
    assert!(ss.consistent_read_context().is_none());

    // 写入测试数据
    ss.upsert_string(b"key1", b"val1").await?;
    ss.upsert_string(b"key2", b"val2").await?;
    ss.upsert_string(b"key3", b"val3").await?;

    // 构造一致读状态机并绑定到存储会话
    let manager = Arc::new(ReadConsistencyManager::new(1, 4, 4, -1, 0));
    let rss = Arc::new(ReadSessionState::new(
      manager,
      8,
      Duration::from_millis(100),
    ));
    ss.set_read_session_state(Some(rss.clone()));

    assert!(ss.is_consistent_read_session());
    assert!(ss.consistent_read_context().is_some());

    // 1. 测试单键读取链路 (read_string_with / read_string)
    let val1 = ss.read_string(b"key1").await?;
    assert_eq!(val1, Some(b"val1".to_vec()));
    let expected_hash1 = (whasher::fast_hash(b"key1") as i64) & i64::MAX;
    assert_eq!(rss.replica_context_snapshot().last_hash(), expected_hash1);

    // 零拷贝借用视图读取验证
    let val1_len = ss.read_string_with(b"key1", |v| v.len()).await?;
    assert_eq!(val1_len, Some(4));

    let val_none = ss.read_string(b"not_exist").await?;
    assert_eq!(val_none, None);
    let expected_hash_not_exist = (whasher::fast_hash(b"not_exist") as i64) & i64::MAX;
    assert_eq!(
      rss.replica_context_snapshot().last_hash(),
      expected_hash_not_exist
    );

    // 2. 测试批量读取链路 (read_with_prefetch)
    let keys = vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()];
    let mut collected = Vec::new();
    ss.read_with_prefetch(&keys, |idx, opt| {
      collected.push((idx, opt.map(|v| v.to_vec())));
    })
    .await?;

    assert_eq!(collected.len(), 3);
    assert_eq!(collected[0], (0, Some(b"val1".to_vec())));
    assert_eq!(collected[1], (1, Some(b"val2".to_vec())));
    assert_eq!(collected[2], (2, Some(b"val3".to_vec())));

    // 3. 测试 db_size 与 db_keys 迭代链路
    assert_eq!(ss.db_size().await?, 3);
    let all_keys = ss.db_keys(b"key*").await?;
    assert_eq!(all_keys.len(), 3);

    // 4. 测试 db_scan 迭代链路
    let (next_cursor, scanned_keys) = ss.db_scan(b"key*", false, b"", 10).await?;
    assert!(next_cursor.is_empty());
    assert_eq!(scanned_keys.len(), 3);

    // 5. 测试 iterate_store
    let mut count = 0;
    let total = ss
      .iterate_store(|_k, _v| {
        count += 1;
        true
      })
      .await?;
    assert_eq!(total, 3);
    assert_eq!(count, 3);

    // 6. 测试 ConsistentReadContext 禁写语义（对标 Tsavorite 严格防御）
    let ctx = ss.consistent_read_context().unwrap();
    assert!(ctx.upsert_forbidden().is_err());
    assert!(ctx.delete_forbidden().is_err());
    assert!(ctx.rmw_forbidden().is_err());

    // 7. 解绑读一致性状态机，回退到非一致读普通模式
    ss.set_read_session_state(None);
    assert!(!ss.is_consistent_read_session());
    assert!(ss.consistent_read_context().is_none());
    let val_after = ss.read_string(b"key1").await?;
    assert_eq!(val_after, Some(b"val1".to_vec()));

    Ok(())
  })
}
