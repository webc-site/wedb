//! 存储会话读一致性数据链路端到端集成测试
//!
//! 验证 StorageSession 与 wkv::ConsistentReadContext 的 1:1 对标与闭环：
//! - 单键读取：read_string_with / read_string 经连接级 wkv 会话附着态触发 pre/post 协议
//! - 批量读取：read_batch_with 经过 consistent_read_context 触发 pre_batch/post_batch 协议与重试
//! - 键空间扫描与遍历：db_scan / db_keys / scan_cursor 逐键触发一致读协议
//! - 附着态派生：is_consistent_read_session / consistent_read_context 自会话附着派生
//! - 哈希域往返：回放侧写 key 序列号草图（条目键 = 记录物理键域）必须被读侧
//!   命中，进而驱动跨虚拟子日志新鲜度约束（拒脏读 / 追平放行双臂）

use std::{sync::Arc, time::Duration};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{Error, WedbStore};
use wnode::{
  aof::readconsistency::{
    read_consistency_manager::ReadConsistencyManager,
    replica_read_session_context::ReadSessionState,
  },
  storage::session::storage_session::StorageSession,
};
use wtest_base::test_store_config;
use wval::{KeyTag, NamespaceDbCodec};

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

/// String 域记录物理键编码（与 `wnode/src/service.rs` 的 `physical_key` 单编码器
/// 同式：AOF 条目键即回放侧草图入账键；会话默认上下文 (vns=0, vdb=0)）
fn record_key(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// 键 → 虚拟子日志下标（读侧触发域口径：条目键哈希 + 管理器同一换算，
/// 即 `verify_key_freshness` 所用路由）
fn route_of(manager: &ReadConsistencyManager, user_key: &[u8]) -> usize {
  manager.virtual_sublog_idx_of_hash(manager.key_hash(&record_key(user_key)))
}

/// 取一对按记录物理键路由到不同虚拟子日志的用户键
fn pick_cross_sublog_keys(manager: &ReadConsistencyManager) -> (Vec<u8>, Vec<u8>) {
  assert_eq!(
    manager.virtual_sublog_count(),
    2,
    "本用例拓扑 = 单物理 × 双回放"
  );
  let find = |want: usize| -> Vec<u8> {
    (0..4096u32)
      .map(|i| format!("rtk-{i}").into_bytes())
      .find(|k| route_of(manager, k) == want)
      .expect("目标虚拟子日志必有路由键")
  };
  (find(0), find(1))
}

/// 回放侧「写草图」→ 读侧「命中」往返闭环（一致读哈希域判据）
///
/// 编排：`ka` / `kb` 按 **String 记录物理键** 路由到两个不同虚拟子日志；
/// 回放侧以 `aof_processor.rs::prepare_key` 的同一管理器口形态
/// （`update_virtual_sublog_key_sequence_number(virtual_sublog_idx_of_hash(hash),
/// hash, seq)`，hash 为条目键即记录物理键哈希）为 `ka` 落 key 序列号草图 100，
/// `kb` 所辖虚拟子日志前沿停在 `kb_frontier`：
/// - 读 `ka`：post 阶段必须命中 `ka` 的草图，会话序列号推进至 100；
/// - 读 `kb`：`kb_frontier = 50 < 100` 时新鲜度约束成立，上抛
///   [`wkv::Error::ConsistentReadTimeout`]（脏读拒绝）；`kb_frontier = 150 > 100`
///   时两读全放行并读到真值（证明上抛来自新鲜度协议而非其它）。
///
/// 红/绿分界：读侧若按用户键取哈希（修前缺陷域），草图槽永远取不到回放侧
/// 入账值，会话序列号停在 0，第二次读不构成等待条件而直接放行返回旧值——
/// 前者即本用例未修前的失败形态。
fn sketch_round_trip(kb_frontier: i64) -> wkv::Result<Option<Vec<u8>>> {
  let rt = Runtime::new().expect("runtime");
  rt.block_on(async {
    let dir = tempdir().expect("tempdir");
    let dev = Arc::new(SegmentedDevice::single_file(dir.path().join("rt.db")).expect("device"));
    let store = Arc::new(WedbStore::open(test_store_config(), dev).expect("store"));
    // 漂移关闭（阈值 -1）：本用例只裁决新鲜度等待，不牵动对齐栅栏
    let manager = Arc::new(ReadConsistencyManager::new(
      1,
      1,
      2,
      -1,
      0,
      Duration::from_millis(100),
    ));
    let (ka, kb) = pick_cross_sublog_keys(&manager);
    let hash_a = manager.key_hash(&record_key(&ka));

    // 回放事件源：ka 的 key 序列号草图入账 100（回放侧调用形态），
    // kb 所辖虚拟子日志前沿停在 kb_frontier
    manager.update_virtual_sublog_key_sequence_number(route_of(&manager, &ka), hash_a, 100);
    manager.update_virtual_sublog_max_sequence_number(route_of(&manager, &kb), kb_frontier);

    // 无角色门形态：一致读协议恒开（副本语义）
    let state = Arc::new(ReadSessionState::new(
      Arc::clone(&manager),
      manager.virtual_sublog_count(),
      Duration::from_millis(100),
    ));
    let session = store
      .new_session()
      .expect("会话")
      .with_read_session_state(Some(state as Arc<dyn wkv::ConsistentReadFunctions>));
    let ss = StorageSession::new(session.enter_batch());
    ss.upsert_string(&ka, b"va").await.expect("写 ka");
    ss.upsert_string(&kb, b"vb").await.expect("写 kb");

    // 首读：建立会话序列号（草图命中则推进至 100）
    assert_eq!(
      ss.read_string(&ka).await.expect("首读放行"),
      Some(b"va".to_vec()),
      "首读建立前驱子日志"
    );
    // 次读：跨子日志新鲜度裁决（结果上抛给调用方断言）
    ss.read_string(&kb).await
  })
}

#[test]
fn test_replay_sketch_hit_gates_cross_sublog_read() -> aok::Void {
  // 草图命中臂：会话序列号 100 未被滞后子日志（前沿 50）覆盖 → 拒读
  let err = sketch_round_trip(50).expect_err("回放草图命中后跨子日志读须被新鲜度约束拒绝");
  assert!(
    matches!(err, Error::ConsistentReadTimeout),
    "一致读超时语义：{err:?}"
  );
  Ok(())
}

#[test]
fn test_replay_sketch_hit_passes_when_sublog_caught_up() -> aok::Void {
  // 对照臂：同一读序，滞后子日志前沿推过会话序列号（150 > 100）→ 全放行
  let val = sketch_round_trip(150).expect("子日志已越过会话序列号，一致读须放行");
  assert_eq!(val.as_deref(), Some(b"vb".as_slice()));
  Ok(())
}
