#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 崩溃恢复完成后序列号生成器收口回归（对标 libs/server/AOF/Recover/
//! AofRecover.cs:24-48 `AofProcessor.Recover` 的 finally 块无条件
//! `ResetSequenceNumberGenerator`，与 libs/server/AOF/GarnetAppendOnlyFile.cs:
//! 136-145 的「NOTE: We need to update starting offset when recovering or
//! failing over to ensure time moves forward」契约）
//!
//! 缺陷场景：跨进程重启的第二代进程以 `SequenceNumberGenerator::new(0)` 重新
//! 起算，若 `replay_database_aof` 收尾不抬升起点，新写入条目携带的序列号将
//! 大幅回拨到崩溃前历史序列号之下；`record_gate::{can_replay, skip_replay}`
//! 以 `sequence_number > until_sequence_number` 判截断上界，回拨令新有效记录
//! 被当越界帧丢弃，主从发散。
//!
//! 测试形态：真盘两段式重启（第一代进程写高位序列号条目 → 句柄整体释放 →
//! 同一路径全新 `SegmentedDevice`/`WalLog`/`WaofSublog` + 零偏移新生成器 =
//! 第二代进程），经生产恢复链 `SingleDatabaseManager::replay_aof` →
//! `DatabaseManagerBase::replay_database_aof` → `GarnetAppendOnlyFile::
//! replay_database_aof` 全量重放后断言取号抬升。累积点为回放期
//! `prepare_key` / `recover_replay_task` 推进的 `ReadConsistencyManager` 各虚拟
//! 子日志最大已回放序列号，故一并断言其非空转（否则补调用等于没修）。

use std::{path::Path, sync::Arc};

use aok::OK;
use tempfile::TempDir;
use waof::{SequenceNumberGenerator, WalConfig, WalLog};
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wnode::{
  aof::{
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::GarnetLog,
    readconsistency::virtual_sublog_replay_state::VirtualSublogReplayState,
    waof_sublog::{AofSublog, WaofSublog},
  },
  database::{GarnetDatabase, SingleDatabaseManager},
  storage::session::storage_session::StorageSession,
};
use wnode_test::{enqueue_set_at, physical_at};
use wtest_base::open_test_store;

/// 分片拓扑物理子日志数（> 1 方持有生成器，C# MultiLogEnabled 同条件）
const PHYSICAL_SUBLOGS: usize = 2;

/// 崩溃前进程的取号起点（1e15 ns ≈ 11.6 天在线）：与长时在线主库的高精度
/// 时钟偏移等价；重启进程零偏移起算即回拨为纳秒级小值，二者相差 6~9 个数量级
const PRE_CRASH_BASE: i64 = 1_000_000_000_000_000;

/// 每个物理子日志预置的条目数（回放条数断言基准）
const KEYS_PER_SUBLOG: usize = 3;

/// 真实段设备物理子日志（生产 `service.rs:open_wal` 同款：同一路径重复
/// 打开即跨重启视图，段文件为唯一持久状态源）
fn open_sublog(dir: &Path, idx: usize) -> aok::Result<Arc<AofSublog>> {
  let device = Arc::new(SegmentedDevice::single_file(
    dir.join(format!("aof_seqgen_{idx}.wal")),
  )?);
  Ok(Arc::new(WaofSublog::new(Arc::new(WalLog::new(
    device,
    WalConfig::default(),
  )?))))
}

/// 分片拓扑 AOF 门面（`seq_num_gen` 与 GarnetLog 共享，C# 构造同款条件）
fn make_aof(
  options: &RuntimeServerOptions,
  backends: Vec<Arc<AofSublog>>,
  seq_num_gen: Arc<SequenceNumberGenerator>,
) -> aok::Result<Arc<GarnetAppendOnlyFile>> {
  let log = GarnetLog::new(options, backends, Some(Arc::clone(&seq_num_gen)))?;
  Ok(Arc::new(GarnetAppendOnlyFile::new(
    Arc::new(log),
    options,
    Some(seq_num_gen),
  )))
}

/// 崩溃恢复回放完成后须收口序列号生成器：新取号严格大于历史最大已回放序列号
#[compio::test]
async fn recovered_aof_replay_lifts_sequence_number_generator() -> aok::Void {
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: PHYSICAL_SUBLOGS as i32,
    aof_replay_task_count: 1,
    ..RuntimeServerOptions::default()
  };
  let wal_dir: TempDir = tempfile::tempdir()?;

  // ── 第一代进程：高位起点取号写条目，全子日志提交落盘后整体释放 ──
  let pre_keys: Vec<Vec<Vec<u8>>> = {
    let pre_seq = Arc::new(SequenceNumberGenerator::new(PRE_CRASH_BASE));
    let pre_aof = make_aof(
      &options,
      (0..PHYSICAL_SUBLOGS)
        .map(|i| open_sublog(wal_dir.path(), i))
        .collect::<aok::Result<Vec<_>>>()?,
      Arc::clone(&pre_seq),
    )?;
    let log = pre_aof.log();

    // 按键路由哈希（GarnetLog::hash + get_physical_sublog_idx，与写侧入队
    // 分片、回放准入 can_replay 同一单点口径）分桶，确保每个物理子日志均有
    // 落位记录——逐子日志已回放最大序列号才非空
    let mut buckets: Vec<Vec<Vec<u8>>> = vec![Vec::new(); PHYSICAL_SUBLOGS];
    for i in 0..(PHYSICAL_SUBLOGS * KEYS_PER_SUBLOG * 4) {
      let key = format!("rec_{i}").into_bytes();
      let idx = log.get_physical_sublog_idx(GarnetLog::hash(&physical_at(0, 0, &key)));
      if buckets[idx].len() < KEYS_PER_SUBLOG {
        buckets[idx].push(key);
      }
    }
    assert!(
      buckets.iter().all(|bucket| bucket.len() == KEYS_PER_SUBLOG),
      "预置键须铺满全部物理子日志，实际分桶 {buckets:?}"
    );
    for (idx, bucket) in buckets.iter().enumerate() {
      for key in bucket {
        enqueue_set_at(log, 0, 1, 0, key, &format!("v{idx}").into_bytes())?;
      }
    }
    // 崩溃前的最后一次提交：cookie（= 当次取号）随 commit 元数据帧落盘，
    // 即第二代 recover_latest_sequence_number 收敛出的恢复上界来源
    log.commit_async().await;
    buckets
  };

  // ── 第二代进程：同路径重开设备 + 零偏移新生成器（缺陷现场） ──
  let post_seq = Arc::new(SequenceNumberGenerator::new(0));
  let post_aof = make_aof(
    &options,
    (0..PHYSICAL_SUBLOGS)
      .map(|i| open_sublog(wal_dir.path(), i))
      .collect::<aok::Result<Vec<_>>>()?,
    Arc::clone(&post_seq),
  )?;
  // 设备面恢复（生产 open_recovered_with_config_and_aof 同序：先recover
  // 回填各子日志 cookie / 位点，再全量重放）
  post_aof.log().recover_async().await?;

  let (store_dir, store) = open_test_store("aof-recover-seqgen-dst.db")?;
  assert_eq!(
    store.current_version(),
    0,
    "无检查点的新恢复实例版本基线须为 0，条目版本 0 才不被 ShouldSkipRecord 跳过"
  );
  let checkpoint_dir = store_dir.path().join("ckpt");
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&store),
    Arc::clone(&store.device),
    checkpoint_dir.clone(),
    Some(Arc::clone(&post_aof)),
  ));
  let mgr = SingleDatabaseManager::new(checkpoint_dir, Arc::clone(&db));

  let replayed = mgr.replay_aof(u64::MAX).await?;
  assert_eq!(
    replayed as usize,
    PHYSICAL_SUBLOGS * KEYS_PER_SUBLOG,
    "崩溃前全部条目须经两分支全量重放（重放为空则累积点无从填充，收口断言失义）"
  );

  // 数据面复验：重放条目确已落库（读回各子日志预置键）
  let session = store.new_session()?;
  let storage = StorageSession::new(session.enter_batch());
  for (idx, bucket) in pre_keys.iter().enumerate() {
    for key in bucket {
      assert_eq!(
        storage.read_string(key).await?,
        Some(format!("v{idx}").into_bytes()),
        "键 {} 须经恢复重放落地",
        String::from_utf8_lossy(key)
      );
    }
  }

  // 累积点体检：回放期由 prepare_key / recover_replay_task 推进的一致性
  // 管理器各物理子日志最大已回放序列号，须确为崩溃前高位号段
  let maxes = post_aof
    .read_consistency_manager()
    .expect("多物理日志态一致性管理器必在位")
    .get_physical_sublog_max_replayed_sequence_number();
  assert_eq!(maxes.len(), PHYSICAL_SUBLOGS);
  assert!(
    maxes.iter().all(|m| *m >= PRE_CRASH_BASE),
    "各物理子日志最大已回放序列号须落在崩溃前高位段，实际 {maxes:?} ≥ {PRE_CRASH_BASE}"
  );
  let historical_max = *maxes.iter().max().expect("分片拓扑必有子日志");

  // 收口断言：回放收尾抬升起点后，首个取号下界即历史最大已回放序列号
  //（撤除收口则本行确定性翻红——重启进程取号仅为其运行时长纳秒数）
  let first = post_aof.get_sequence_number();
  assert!(
    first >= historical_max,
    "恢复后取号须抬升至历史最大已回放序列号 {historical_max} 之上，实际 {first}"
  );
  // C# 契约「time moves forward」：新序列号严格大于崩溃前已持久化的全部
  // 历史序列号（生成器 CAS 单调推进保证次一号 > 首号，故严格不等式成立）
  let next = post_aof.get_sequence_number();
  assert!(
    next > historical_max,
    "恢复后新生成序列号须严格大于历史最大序列号 {historical_max}，实际 {next}"
  );
  OK
}

/// 旧代际残留高值（远超崩溃前段 PRE_CRASH_BASE 的时间基准，模拟本进程
/// 此前挂接更高位点主库后遗留的草图/前沿水位）
const STALE_RESIDUE: i64 = 9_000_000_000_000_000;

/// 恢复起点换代：`replay_database_aof` 分派前须
/// `create_or_update_key_sequence_manager`（对位 C# AofRecover.cs:31 的
/// Recover try 块首行，与 finally 位 :37 `ResetSequenceNumberGenerator`
/// 成对；rust 缺前半即本票缺陷）
///
/// 危害场景：温启/二次挂接时构造期管理器已积旧代际高序列号
/// （fetch_max 单调永驻），换代缺失令本趟回放续写同一管理器——读闸经
/// 残留高前沿假新鲜放行（`verify_key_freshness` 的 mssn < cached 高值
/// 分支跳等待），快照未覆盖键被陈旧读。
///
/// 断言编排：第二代进程构造 v1 → 直喂 v1 残留高草图/高前沿（与回放侧
/// `VirtualSublogReplayState::UpdateKeySequenceNumber` 同一入账口形态，
/// 模拟旧挂接代际积累）→ 真盘全量重放 → 断言版本递增换代、残留值在新
/// 代际归零、本趟回放喂入的是新管理器（非空转）、旧代际残留原样驻留
/// （fetch_max 根因实证）。
///
/// 红/绿分界：撤除 `replay_database_aof` 起点的换代调用即确定性翻红——
/// 版本滞留 1、残留草图/前沿经 fetch_max 顶驻新读闸面。
#[compio::test]
async fn recovered_aof_replay_regenerates_key_sequence_manager() -> aok::Void {
  let options = RuntimeServerOptions {
    aof_physical_sublog_count: PHYSICAL_SUBLOGS as i32,
    aof_replay_task_count: 1,
    ..RuntimeServerOptions::default()
  };
  let wal_dir: TempDir = tempfile::tempdir()?;

  // ── 第一代进程：高位起点写条目（与上一用例同形，供真回放喂入） ──
  let pre_keys: Vec<Vec<Vec<u8>>> = {
    let pre_seq = Arc::new(SequenceNumberGenerator::new(PRE_CRASH_BASE));
    let pre_aof = make_aof(
      &options,
      (0..PHYSICAL_SUBLOGS)
        .map(|i| open_sublog(wal_dir.path(), i))
        .collect::<aok::Result<Vec<_>>>()?,
      Arc::clone(&pre_seq),
    )?;
    let log = pre_aof.log();
    let mut buckets: Vec<Vec<Vec<u8>>> = vec![Vec::new(); PHYSICAL_SUBLOGS];
    for i in 0..(PHYSICAL_SUBLOGS * KEYS_PER_SUBLOG * 4) {
      let key = format!("regen_{i}").into_bytes();
      let idx = log.get_physical_sublog_idx(GarnetLog::hash(&physical_at(0, 0, &key)));
      if buckets[idx].len() < KEYS_PER_SUBLOG {
        buckets[idx].push(key);
      }
    }
    assert!(buckets.iter().all(|b| b.len() == KEYS_PER_SUBLOG));
    for (idx, bucket) in buckets.iter().enumerate() {
      for key in bucket {
        enqueue_set_at(log, 0, 1, 0, key, &format!("r{idx}").into_bytes())?;
      }
    }
    log.commit_async().await;
    buckets
  };

  // ── 第二代进程：构造 + 设备面恢复，重放前旧代际先积残留高值 ──
  let post_seq = Arc::new(SequenceNumberGenerator::new(0));
  let post_aof = make_aof(
    &options,
    (0..PHYSICAL_SUBLOGS)
      .map(|i| open_sublog(wal_dir.path(), i))
      .collect::<aok::Result<Vec<_>>>()?,
    Arc::clone(&post_seq),
  )?;
  post_aof.log().recover_async().await?;

  let stale = post_aof
    .read_consistency_manager()
    .expect("多物理日志态构造即建管理器");
  assert_eq!(stale.current_version(), 1, "构造期首代版本 1");

  // 残留草图键：草图槽须与本趟全部重放键的槽位互斥（fetch_max 按槽入账，
  // 槽冲突会让「新代际归零」断言被合法回放值污染，失去换代判据）
  let replayed_slots: Vec<usize> = pre_keys
    .iter()
    .flatten()
    .map(|k| VirtualSublogReplayState::get_sketch_slot(GarnetLog::hash(&physical_at(0, 0, k))))
    .collect();
  let residue_key = (0..4096u32)
    .map(|i| format!("residue_{i}").into_bytes())
    .find(|k| {
      !replayed_slots.contains(&VirtualSublogReplayState::get_sketch_slot(GarnetLog::hash(
        &physical_at(0, 0, k),
      )))
    })
    .expect("冲突槽位至多占 6 格，必有干净候选");
  let residue_hash = GarnetLog::hash(&physical_at(0, 0, &residue_key));
  let residue_vsr = stale.virtual_sublog_idx_of_hash(residue_hash);
  // 直喂旧代际：残留高草图 + 所辖虚拟子日志高前沿（回放入账口同一形态）
  stale
    .vsr(residue_vsr)
    .update_key_sequence_number(residue_hash, STALE_RESIDUE);
  stale
    .vsr(residue_vsr)
    .update_max_sequence_number(STALE_RESIDUE);

  let (store_dir, store) = open_test_store("aof-recover-regen-dst.db")?;
  let checkpoint_dir = store_dir.path().join("ckpt");
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&store),
    Arc::clone(&store.device),
    checkpoint_dir.clone(),
    Some(Arc::clone(&post_aof)),
  ));
  let mgr = SingleDatabaseManager::new(checkpoint_dir, Arc::clone(&db));

  let replayed = mgr.replay_aof(u64::MAX).await?;
  assert_eq!(replayed as usize, PHYSICAL_SUBLOGS * KEYS_PER_SUBLOG);

  // 换代断言：恢复起点版本递增、与旧代际整体置换（撤除起点调用即翻红）
  let fresh = post_aof
    .read_consistency_manager()
    .expect("多物理日志态管理器在场");
  assert!(
    !Arc::ptr_eq(&stale, &fresh),
    "恢复起点须整体换代，不得原地续用旧管理器"
  );
  assert_eq!(
    fresh.current_version(),
    stale.current_version() + 1,
    "新代际版本 = 前代 + 1（C# CreateOrUpdateKeySequenceManager 语义）"
  );
  assert_eq!(fresh.current_version(), 2, "构造 v1 + 恢复起点换代 v2");

  // 残留归零：新代际草图/前沿不含旧挂接值
  assert_eq!(
    fresh.vsr(residue_vsr).get_key_sequence_number(residue_hash),
    0,
    "旧代际残留草图不得跨代驻留读闸"
  );
  // 残留前沿不跨代顶驻：新代际该槽前沿只应由本趟回放合法值推进
  // （PRE_CRASH 高位段 ≪ STALE_RESIDUE），残留 9e15 若跨代驻留即顶高
  assert!(
    fresh.vsr(residue_vsr).max() < STALE_RESIDUE,
    "旧代际残留前沿 {} 不得跨代顶驻新读闸，实际 {}",
    STALE_RESIDUE,
    fresh.vsr(residue_vsr).max()
  );
  // 根因实证：旧代际 fetch_max 单调永驻——这正是挂接/恢复必须整体换代、
  // 而非抬号清零的原因
  assert_eq!(
    stale.vsr(residue_vsr).get_key_sequence_number(residue_hash),
    STALE_RESIDUE,
    "旧管理器残留值单调驻留（换代前读闸污染的来源）"
  );

  // 喂入面落新代际：本趟回放累积的各物理子日志最大已回放序列号由新管理
  // 器承接（换代在分派前、AofProcessor::new 前，喂入不得落回旧代际）
  let maxes = fresh.get_physical_sublog_max_replayed_sequence_number();
  assert!(
    maxes.iter().all(|m| *m >= PRE_CRASH_BASE),
    "本趟回放须喂入新代际管理器至崩溃前高位段，实际 {maxes:?}"
  );
  OK
}
