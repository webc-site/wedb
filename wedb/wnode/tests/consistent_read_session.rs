//! 存储会话读一致性数据链路端到端集成测试
//!
//! 验证 StorageSession 与 wkv::ConsistentReadContext 的 1:1 对标与闭环：
//! - 单键读取：read_string_with / read_string 经连接级 wkv 会话附着态触发 pre/post 协议
//! - 批量读取：read_batch_with 经过 consistent_read_context 触发 pre_batch/post_batch 协议与重试
//! - 键空间扫描与遍历：db_scan / db_keys / scan_cursor 逐键触发一致读协议
//! - 附着态派生：read_session_state / consistent_read_context 自会话附着派生
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

#[compio::test]
async fn test_storage_session_consistent_read_pipeline() -> aok::Void {
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
    Some(Duration::from_millis(100)),
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
    Some(Duration::from_millis(100)),
  ));
  let session = session.with_read_session_state(Some(
    Arc::clone(&rss) as Arc<dyn wkv::ConsistentReadFunctions>
  ));

  let ss = StorageSession::new(session.enter_batch());

  // 附着态派生一致读会话判定
  assert!(ss.batch.read_session_state().is_some());
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
}

#[compio::test]
async fn test_storage_session_without_attachment_is_plain() -> aok::Void {
  let dir = tempdir()?;
  let dev = Arc::new(SegmentedDevice::single_file(dir.path().join("test.db"))?);
  let store = Arc::new(WedbStore::open(test_store_config(), dev)?);
  let session = store.new_session()?;

  let ss = StorageSession::new(session.enter_batch());

  // 未附着：普通会话形态，读路径直通
  assert!(!ss.batch.read_session_state().is_some());
  assert!(ss.consistent_read_context().is_none());
  ss.upsert_string(b"plain", b"v").await?;
  assert_eq!(ss.read_string(b"plain").await?, Some(b"v".to_vec()));
  Ok(())
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
/// 第二次读的探测形：点读 kb（点读族在案先例锚）与全库 SCAN（出帧键
/// pre/post 接线面）共用同一跨子日志夹具，红/绿双臂对照
enum Probe {
  Point,
  Scan,
}

fn sketch_round_trip(kb_frontier: i64, probe: Probe) -> wkv::Result<Vec<Vec<u8>>> {
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
      Some(Duration::from_millis(100)),
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
      Some(Duration::from_millis(100)),
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
    // 次读：跨子日志新鲜度裁决（结果上抛给调用方断言）。点读臂直读 kb；
    // SCAN 臂全库出帧，ka 先扫（append 序）post 抬会话序列号至 100 后，
    // kb 出帧前 pre 受新鲜度约束——红臂须在 SCAN 内上抛超时，绿臂双臂
    // 放行且 ka/kb 依次出帧
    match probe {
      Probe::Point => ss
        .read_string(&kb)
        .await
        .map(|v| v.map_or_else(Vec::new, |b| vec![b])),
      Probe::Scan => {
        let (_, keys) = ss.scan_cursor(b"", true, 0, 10, None).await?;
        assert_eq!(keys, vec![ka, kb], "SCAN 放行臂出帧键序");
        Ok(keys)
      }
    }
  })
}

#[test]
fn test_replay_sketch_hit_gates_cross_sublog_read() -> aok::Void {
  // 草图命中臂：会话序列号 100 未被滞后子日志（前沿 50）覆盖 → 拒读
  let err =
    sketch_round_trip(50, Probe::Point).expect_err("回放草图命中后跨子日志读须被新鲜度约束拒绝");
  assert!(
    matches!(err, Error::ConsistentReadTimeout),
    "一致读超时语义：{err:?}"
  );
  Ok(())
}

#[test]
fn test_replay_sketch_hit_passes_when_sublog_caught_up() -> aok::Void {
  // 对照臂：同一读序，滞后子日志前沿推过会话序列号（150 > 100）→ 全放行
  let val = sketch_round_trip(150, Probe::Point).expect("子日志已越过会话序列号，一致读须放行");
  assert_eq!(val, vec![b"vb".to_vec()]);
  Ok(())
}

#[test]
fn test_replay_sketch_hit_gates_cross_sublog_scan() -> aok::Void {
  // SCAN 接线面红臂：点读抬水位后全库 SCAN 触及滞后子日志键，pre 新鲜度
  // 约束成立，超时经 wkv Err 通道上抛（修前 SCAN 无 pre 直扫陈旧库放行）
  let err = sketch_round_trip(50, Probe::Scan).expect_err("SCAN 出帧键须被跨子日志新鲜度约束拒绝");
  assert!(
    matches!(err, Error::ConsistentReadTimeout),
    "SCAN 一致读超时语义：{err:?}"
  );
  Ok(())
}

#[test]
fn test_replay_sketch_hit_passes_scan_when_sublog_caught_up() -> aok::Void {
  // SCAN 绿臂：滞后子日志前沿推过会话序列号 → SCAN 全出帧（夹具内已按
  // ka/kb 序断言），SCAN 面零误伤
  sketch_round_trip(150, Probe::Scan).expect("子日志已越过会话序列号，SCAN 须放行");
  Ok(())
}

// ── 自 src/aof/readconsistency/{read_consistency_manager,replay_align_barrier}
//    内联测模块迁入（纯 pub 面；participant_event 直构臂因 ParticipantEvent
//    私有留守 src，先例 tests/consumer_registry_counters.rs）──

use std::{
  sync::mpsc,
  thread::{sleep, spawn},
  time::Instant,
};

use wnode::aof::{
  garnet_log::GarnetLog,
  readconsistency::{
    replay_align_barrier::ReplayAlignBarrier,
    replica_read_session_context::ReplicaReadSessionContext,
  },
};

fn manager() -> ReadConsistencyManager {
  // 2 物理 × 2 回放 = 4 虚拟子日志；漂移关闭（单机测试确定性）
  ReadConsistencyManager::new(1, 2, 2, -1, 0, Some(Duration::from_millis(10)))
}

#[test]
fn version_check_resets_context_once() {
  let m = manager();
  let ctx = ReplicaReadSessionContext::default();
  m.check_consistency_manager_version(&ctx);
  assert_eq!(ctx.session_version(), 1);
  ctx.set_maximum_session_sequence_number(50);
  m.check_consistency_manager_version(&ctx);
  assert_eq!(ctx.maximum_session_sequence_number(), 50, "同版本不重置");
}

#[test]
fn update_and_read_key_sequence_number() {
  let m = manager();
  let key = b"rk";
  assert_eq!(m.get_key_sequence_number(key, false), 0);
  m.update_virtual_sublog_key_sequence_number(
    m.virtual_sublog_idx_of_hash(GarnetLog::hash(key)),
    GarnetLog::hash(key),
    11,
  );
  assert_eq!(m.get_key_sequence_number(key, false), 11);
  assert!(m.get_key_sequence_number(key, true) >= 11);
}

#[test]
fn physical_sublog_max_vector_and_drift() {
  let m = manager();
  m.update_physical_sublog_max_sequence_number(0, 30);
  m.update_physical_sublog_max_sequence_number(1, 10);
  assert_eq!(m.get_physical_sublog_max(0), 30);
  assert_eq!(m.get_physical_sublog_max(1), 10);
  assert_eq!(m.get_physical_sublog_max_sequence_vector(), "30,10");
  assert_eq!(m.get_physical_sublog_max_drift_sequence_vector(), "0,20");
  assert_eq!(
    m.get_physical_sublog_max_replayed_sequence_number(),
    vec![30, 10]
  );
}

#[test]
fn virtual_sublog_routing_matches_formula() {
  let m = manager();
  for hash in [0i64, 1, 12345, i64::MAX, i64::MIN] {
    let expected = ((hash as u64) % 2) as usize * 2 + ((hash as u64) / 2 % 2) as usize;
    assert_eq!(m.virtual_sublog_idx_of_hash(hash), expected);
    assert!(m.virtual_sublog_idx_of_hash(hash) < 4);
  }
  // 域共享内核换算公式的直呼锁（virtual_sublog_idx pub(crate)）留守 src；
  // 此处经 pub 面 virtual_sublog_idx_of_hash 逐点同验（测试拓扑
  // replay_task_count = 2，1 * 2 + 2 = 4 已由上方 expected 覆盖）
}

#[test]
fn consistent_read_protocol_single_key() {
  let m = manager();
  let key = b"proto";
  let hash = GarnetLog::hash(key);
  m.update_virtual_sublog_key_sequence_number(m.virtual_sublog_idx_of_hash(hash), hash, 5);

  let ctx = ReplicaReadSessionContext::default();
  m.pre_single_key_consistent_read(hash, &ctx, Some(Duration::from_millis(10)))
    .unwrap();
  // 同子日志首读：无需等待即可推进
  m.post_single_key_consistent_read(&ctx);
  assert!(ctx.maximum_session_sequence_number() >= 5);
}

#[test]
fn batch_protocol_cross_sublog_wait_and_validate() {
  let m = manager();
  let k1 = b"b1";
  let k2 = b"b2";
  // 两键路由到不同子日志（穷举到一对）
  let (h1, h2) = (GarnetLog::hash(k1), GarnetLog::hash(k2));
  if m.virtual_sublog_idx_of_hash(h1) == m.virtual_sublog_idx_of_hash(h2) {
    return;
  }
  m.update_virtual_sublog_key_sequence_number(m.virtual_sublog_idx_of_hash(h1), h1, 3);

  let ctx = ReplicaReadSessionContext::default();
  let got1 = m
    .pre_batch_key_consistent_read(k1, &ctx, Some(Duration::from_millis(10)))
    .unwrap();
  // 第二键所在子日志已回放超越会话序列号：等待后通过
  m.update_virtual_sublog_key_sequence_number(m.virtual_sublog_idx_of_hash(h2), h2, 8);
  let got2 = m
    .pre_batch_key_consistent_read(k2, &ctx, Some(Duration::from_millis(500)))
    .unwrap();
  assert!(m.post_batch_key_consistent_read_validate(got1, &ctx));
  assert!(m.post_batch_key_consistent_read_validate(got2, &ctx));
  assert!(ctx.maximum_session_sequence_number() >= 3);
}

#[test]
fn advance_virtual_sublog_time_signals_barrier() {
  let m = ReadConsistencyManager::new(1, 1, 2, 5, 0, Some(Duration::from_millis(10)));
  m.replay_barrier.try_open_round(10);
  // 推进至目标：到场计数（另一虚拟子日志亦到场后放行）
  m.advance_virtual_sublog_time(0, 12);
  m.advance_virtual_sublog_time(1, 12);
  assert!(!m.replay_barrier.in_progress());
}

#[test]
fn infinite_timeout_sentinel_none_waits_for_replay() {
  let m = Arc::new(ReadConsistencyManager::new(1, 1, 2, -1, 0, None));
  assert_eq!(m.read_timeout(), None);
  let key = b"inf_key";
  let hash = GarnetLog::hash(key);
  let my_vsr = m.virtual_sublog_idx_of_hash(hash);
  let other_vsr = (my_vsr + 1) % 2;

  let ctx = ReplicaReadSessionContext::default();
  ctx.set_last_virtual_sublog_idx(other_vsr as i32);
  ctx.set_maximum_session_sequence_number(50);

  let m_cloned = Arc::clone(&m);
  let handle = spawn(move || {
    sleep(Duration::from_millis(20));
    m_cloned.update_virtual_sublog_max_sequence_number(my_vsr, 60);
  });
  // None 永等臂：回放推进后成功放行
  m.verify_key_freshness(hash, &ctx, None).unwrap();
  handle.join().unwrap();
}

#[test]
fn barrier_open_round_and_release_on_all_arrivals() {
  let barrier = ReplayAlignBarrier::new(2, Some(Duration::from_secs(1)));
  assert!(!barrier.in_progress());
  barrier.try_open_round(100);
  assert!(barrier.in_progress());
  // 已有轮次：开轮空操作
  barrier.try_open_round(200);
  assert!(barrier.in_progress());

  // 未达标不计数
  barrier.signal_arrival(0, 50);
  // 达标但重复到达去重：只计一次
  barrier.signal_arrival(0, 150);
  assert!(barrier.in_progress());
  // 第二参与者达标到场 → 全员到齐 → 轮次摘除
  barrier.signal_arrival(1, 150);
  assert!(!barrier.in_progress());
}

#[test]
fn barrier_blocking_arrival_released_by_peer() {
  let barrier = Arc::new(ReplayAlignBarrier::new(2, Some(Duration::from_secs(5))));
  barrier.try_open_round(10);

  // 参与者 1 阻塞等待
  let b2 = Arc::clone(&barrier);
  let handle = spawn(move || b2.signal_arrival_and_wait(1, 12));
  sleep(Duration::from_millis(20));
  // 参与者 0 到场触发全员放行
  barrier.signal_arrival_and_wait(0, 12);
  handle.join().unwrap();
  assert!(!barrier.in_progress());
}

#[test]
fn barrier_timeout_proceeds_unaligned() {
  let barrier = ReplayAlignBarrier::new(2, Some(Duration::from_millis(30)));
  barrier.try_open_round(10);
  // 仅一方到场：等待超时后直接返回（不卡死）
  let started = Instant::now();
  barrier.signal_arrival_and_wait(0, 12);
  assert!(started.elapsed() >= Duration::from_millis(30));
  assert!(barrier.in_progress(), "轮次未被放行者摘除（仅超时退出）");
}

#[test]
fn barrier_disable_rejects_and_enable_restores() {
  let barrier = ReplayAlignBarrier::new(2, None);
  barrier.try_open_round(10);
  barrier.disable();
  assert!(barrier.in_progress());
  // 禁用态开轮被占位轮拒绝
  barrier.try_open_round(20);
  barrier.enable();
  assert!(!barrier.in_progress());
  // 恢复后可正常开轮与放行
  barrier.try_open_round(30);
  barrier.signal_arrival(0, 40);
  barrier.signal_arrival(1, 40);
  assert!(!barrier.in_progress());
}

#[test]
fn barrier_three_participants_mixed_arrival_release() {
  let barrier = Arc::new(ReplayAlignBarrier::new(3, Some(Duration::from_secs(5))));
  barrier.try_open_round(100);

  // 参与者 0、1 阻塞到场，挂起等待全员
  let handles: Vec<_> = (0..2)
    .map(|i| {
      let b = Arc::clone(&barrier);
      spawn(move || b.signal_arrival_and_wait(i, 120))
    })
    .collect();
  sleep(Duration::from_millis(50));

  // 参与者 2（空闲子日志）非阻塞到场 → 全员到齐 → 集体放行
  barrier.signal_arrival(2, 120);
  for h in handles {
    h.join().unwrap();
  }
  assert!(!barrier.in_progress());
}

#[test]
fn barrier_three_participants_partial_timeout_then_release() {
  let timeout = Duration::from_millis(60);
  let barrier = Arc::new(ReplayAlignBarrier::new(3, Some(timeout)));
  barrier.try_open_round(10);

  // 参与者 0 到场阻塞，期间无人放行，超时弃权继续执行
  let b0 = Arc::clone(&barrier);
  let h0 = spawn(move || {
    let started = Instant::now();
    b0.signal_arrival_and_wait(0, 12);
    started.elapsed()
  });
  sleep(timeout + Duration::from_millis(40));

  // 弃权后参与者 1 非阻塞到场计数
  barrier.signal_arrival(1, 12);

  // 参与者 2 最后到场 → 减至 0 → 放行并摘除轮次
  barrier.signal_arrival_and_wait(2, 12);
  let elapsed0 = h0.join().unwrap();
  assert!(elapsed0 >= timeout, "参与者 0 应在超时后才弃权返回");
  assert!(!barrier.in_progress());
}

#[test]
fn barrier_five_participants_concurrent_rounds_rollover() {
  let barrier = Arc::new(ReplayAlignBarrier::new(5, Some(Duration::from_millis(500))));
  // 连续多轮并发：上一轮到达记录（旧轮 ID）不得令新轮去重误判，
  // 每轮 5 名参与者交错到场，全员到齐后摘除，紧接开下一轮
  for round in 1..=4 {
    let target = 100 * i64::from(round);
    barrier.try_open_round(target);
    assert!(barrier.in_progress());
    let handles: Vec<_> = (0..5)
      .map(|i| {
        let b = Arc::clone(&barrier);
        spawn(move || b.signal_arrival_and_wait(i, target + i as i64))
      })
      .collect();
    for h in handles {
      h.join().unwrap();
    }
    assert!(!barrier.in_progress(), "第 {round} 轮未摘除");
  }
}

#[test]
fn barrier_three_participants_repeat_arrival_dedupe_per_round() {
  let barrier = ReplayAlignBarrier::new(3, Some(Duration::from_secs(1)));
  // 连续多轮：同轮重复到场只计一次，跨轮（rollover）同参与者可再次到场
  for round in 1..=3 {
    let target = 10 * i64::from(round);
    barrier.try_open_round(target);
    for i in 0..3 {
      barrier.signal_arrival(i, target);
      barrier.signal_arrival(i, target);
    }
    assert!(!barrier.in_progress(), "第 {round} 轮未摘除");
  }
}

#[test]
fn barrier_three_participants_disable_releases_blocked() {
  let barrier = Arc::new(ReplayAlignBarrier::new(3, None));
  barrier.try_open_round(10);

  // 参与者 0、1 永等阻塞
  let (tx, rx) = mpsc::channel();
  for i in 0..2 {
    let b = Arc::clone(&barrier);
    let tx = tx.clone();
    spawn(move || {
      b.signal_arrival_and_wait(i, 12);
      let _ = tx.send(i);
    });
  }
  sleep(Duration::from_millis(50));

  // disable 放行既有阻塞等待者
  barrier.disable();
  for _ in 0..2 {
    rx.recv_timeout(Duration::from_secs(2))
      .expect("disable 应放行阻塞参与者");
  }
  // 禁用态占位轮仍表现为进行中
  assert!(barrier.in_progress());

  // enable 恢复后可正常开轮与放行
  barrier.enable();
  assert!(!barrier.in_progress());
  barrier.try_open_round(20);
  for i in 0..3 {
    barrier.signal_arrival(i, 30);
  }
  assert!(!barrier.in_progress());
}

// ── 自 src/aof/readconsistency/{virtual_sublog_replay_state,
//    replica_read_session_context} 内联测模块迁入(pub 面;sketch 布局/
//    waiter 队列内部态直读臂留守 src)──

use wnode::aof::readconsistency::virtual_sublog_replay_state::{
  ReadSessionWaiter, VirtualSublogReplayState,
};

#[test]
fn replay_state_monotonic_updates_and_frontier() {
  let state = VirtualSublogReplayState::new(i64::MAX);
  state.update_max_sequence_number(10);
  state.update_max_sequence_number(5);
  assert_eq!(state.max(), 10);

  state.update_key_sequence_number(0x1234, 7);
  assert_eq!(state.get_key_sequence_number(0x1234), 7);
  // 前沿 = max(草图谱值, 子日志前沿)
  assert_eq!(state.get_frontier_sequence_number(0x1234), 10);
  assert_eq!(state.get_frontier_sequence_number(0x9999), 10);
}

#[test]
fn replay_state_waiter_signaled_when_frontier_passes_target() {
  let state = VirtualSublogReplayState::new(i64::MAX);
  let waiter = Arc::new(ReadSessionWaiter::new());
  state.update_max_sequence_number(5);

  // 前沿已过目标：自旋快路径立即通过
  assert!(state.wait_for_sequence_number(4, &waiter, Some(Duration::from_millis(1))));

  // 目标高于前沿：入队等待，由推进方唤醒
  let state2 = Arc::new(VirtualSublogReplayState::new(i64::MAX));
  let waiter2 = Arc::new(ReadSessionWaiter::new());
  let state3 = Arc::clone(&state2);
  let waiter3 = Arc::clone(&waiter2);
  let handle =
    spawn(move || state3.wait_for_sequence_number(20, &waiter3, Some(Duration::from_secs(5))));
  sleep(Duration::from_millis(10));
  state2.update_max_sequence_number(21);
  assert!(handle.join().unwrap());
}

#[test]
fn replay_state_waiter_none_infinite_wait_via_listener_wait() {
  let state = Arc::new(VirtualSublogReplayState::new(i64::MAX));
  let waiter = Arc::new(ReadSessionWaiter::new());
  let s = Arc::clone(&state);
  let w = Arc::clone(&waiter);
  let handle = spawn(move || {
    // None 臂走 listener.wait() 永等，直到被推进方唤醒
    s.wait_for_sequence_number(50, &w, None)
  });
  sleep(Duration::from_millis(20));
  state.update_max_sequence_number(51);
  assert!(handle.join().unwrap());

  // ReadSessionWaiter::wait(None) 亦走 listener.wait()
  let waiter2 = Arc::new(ReadSessionWaiter::new());
  let w2 = Arc::clone(&waiter2);
  let handle2 = spawn(move || w2.wait(None));
  sleep(Duration::from_millis(20));
  waiter2.signal();
  assert!(handle2.join().unwrap());
}

#[test]
fn session_state_monotonic_sequence_and_reset() {
  let ctx = ReplicaReadSessionContext::new(4);
  assert_eq!(ctx.last_virtual_sublog_idx(), -1);
  ctx.set_maximum_session_sequence_number(10);
  ctx.advance_maximum_session_sequence_number(7);
  assert_eq!(
    ctx.maximum_session_sequence_number(),
    10,
    "单调推进：较小序列号不得倒退"
  );
  ctx.advance_maximum_session_sequence_number(15);
  assert_eq!(
    ctx.maximum_session_sequence_number(),
    15,
    "单调推进：较大序列号成功前移"
  );

  ctx.set_cached_sublog_max(1, 42);
  assert_eq!(ctx.cached_sublog_max(1), 42);
  ctx.reset_cached_sublog_max();
  assert_eq!(ctx.cached_sublog_max(1), 0, "reset 后缓存子日志序列号归零");
}
