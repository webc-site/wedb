//! 存储会话读一致性数据链路端到端集成测试
//!
//! 验证 StorageSession 与 wkv::ConsistentReadContext 的 1:1 对标与闭环：
//! - 单键读取：read_string_with / read_string 经连接级 wkv 会话附着态触发 pre/post 协议
//! - 批量读取：read_batch_with 经过 consistent_read_context 触发 pre_batch/post_batch 协议与重试
//! - 键空间扫描与遍历：db_scan / db_keys / scan_cursor 逐键触发一致读协议
//! - 附着态派生：is_consistent_read_session / consistent_read_context 自会话附着派生

use std::{sync::Arc, time::Duration};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  aof::readconsistency::{
    read_consistency_manager::ReadConsistencyManager,
    replica_read_session_context::ReadSessionState,
  },
  storage::session::storage_session::StorageSession,
};
use wtest_base::test_store_config;

#[test]
fn test_storage_session_consistent_read_pipeline() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let dev = Arc::new(SegmentedDevice::single_file(dir.path().join("test.db"))?);
    // 小预算测试配置（对标 C# 16MB 基线）
    let store = Arc::new(WedbStore::open(test_store_config(), dev)?);
    let session = store.new_session()?;

    // 构造一致读状态机并附着到连接级 wkv 会话（对标 C# 建会话时挂
    // ReadSessionState；快慢路径 StorageSession 经附着态自动派生）
    let manager = Arc::new(ReadConsistencyManager::new(
      1,
      4,
      4,
      -1,
      0,
      Duration::from_millis(100),
    ));
    // 回放事件源：先行推进各物理子日志所辖虚拟子日志前沿（对标副本回放侧
    // UpdatePhysicalSublogMaxSequenceNumber，ReadConsistencyManager.cs:188-194），
    // 使跨子日志新鲜度校验真实满足；回放未跟上时 pre 族超时上抛（C# 同构抛
    // TimeoutException），本用例走一致读全链路须先有回放推进
    for physical_sublog_idx in 0..4 {
      manager.update_physical_sublog_max_sequence_number(physical_sublog_idx, 1);
    }
    let rss = Arc::new(ReadSessionState::new(
      Arc::clone(&manager),
      8,
      Duration::from_millis(100),
    ));
    let session = session.with_read_session_state(Some(
      Arc::clone(&rss) as Arc<dyn wkv::ConsistentReadFunctions>
    ));

    let ss = StorageSession::new(session.enter_batch());

    // 附着态派生一致读会话判定
    assert!(ss.is_consistent_read_session());
    assert!(ss.consistent_read_context().is_some());

    // 写入测试数据
    ss.upsert_string(b"key1", b"val1").await?;
    ss.upsert_string(b"key2", b"val2").await?;
    ss.upsert_string(b"key3", b"val3").await?;

    // 1. 测试单键读取链路 (read_string_with / read_string)：经附着态触发 pre/post
    //（读后 hash 累积语义由 pre/post 协议内部闭环校验承接，白盒快照断言已随
    // replica_context_snapshot 死口移除）
    let val1 = ss.read_string(b"key1").await?;
    assert_eq!(val1, Some(b"val1".to_vec()));

    // 零拷贝借用视图读取验证
    let val1_len = ss.read_string_with(b"key1", |v| v.len()).await?;
    assert_eq!(val1_len, Some(4));

    let val_none = ss.read_string(b"not_exist").await?;
    assert_eq!(val_none, None);

    // 2. 测试批量读取链路（一致读上下文 read_batch_with：pre_batch/post_batch
    // 协议与重试在 wkv 批读内部闭环）
    let keys = vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()];
    let mut collected = Vec::new();
    ss.consistent_read_context()
      .unwrap()
      .read_batch_with(&keys, &mut |idx: usize, opt: Option<&[u8]>| {
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

    // 4. 测试 scan_cursor 迭代链路（地址游标）
    let (next_cursor, scanned_keys) = ss.scan_cursor(b"key*", false, 0, 10, None).await?;
    assert_eq!(next_cursor, 0);
    assert_eq!(scanned_keys.len(), 3);

    Ok(())
  })
}

#[test]
fn test_storage_session_without_attachment_is_plain() -> aok::Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let dev = Arc::new(SegmentedDevice::single_file(dir.path().join("test.db"))?);
    let store = Arc::new(WedbStore::open(test_store_config(), dev)?);
    let session = store.new_session()?;

    let ss = StorageSession::new(session.enter_batch());

    // 未附着：普通会话形态，读路径直通
    assert!(!ss.is_consistent_read_session());
    assert!(ss.consistent_read_context().is_none());
    ss.upsert_string(b"plain", b"v").await?;
    assert_eq!(ss.read_string(b"plain").await?, Some(b"v".to_vec()));
    Ok(())
  })
}
