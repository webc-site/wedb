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
use waof::{AofEntryType, SequenceNumberGenerator, WalConfig, WalLog};
use wconf::RuntimeServerOptions;
use wdev::SegmentedDevice;
use wnode::{
  aof::{
    garnet_append_only_file::GarnetAppendOnlyFile,
    garnet_log::{GarnetLog, RecordShape},
    waof_sublog::{AofSublog, WaofSublog},
  },
  database::{GarnetDatabase, SingleDatabaseManager},
  storage::session::storage_session::StorageSession,
};
use wnode_test::replay_input_bytes;
use wresp::command::RespCommand;
use wtest_base::open_test_store;
use wval::{KeyTag, NamespaceDbCodec};

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

/// 物理键编码（与主写入面同构：[NsVarint][DbVarint][KeyTag][用户键]）
fn physical(user_key: &[u8]) -> Vec<u8> {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::String, user_key)
    .as_slice()
    .to_vec()
}

/// SET 条目入队（版本 0 = 无检查点基线的恢复全量重放代际）
fn enqueue_set(log: &GarnetLog, key: &[u8], value: &[u8]) -> aok::Result<()> {
  let input = replay_input_bytes(RespCommand::Set, vec![key.to_vec(), value.to_vec()]);
  let key = physical(key);
  log.enqueue(&RecordShape {
    op_type: AofEntryType::StoreUpsert,
    version: 0,
    session_id: 1,
    key: &key,
    value,
    input: &input,
    database_id: 0,
  })?;
  Ok(())
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
      let idx = log.get_physical_sublog_idx(GarnetLog::hash(&physical(&key)));
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
        enqueue_set(log, key, format!("v{idx}").as_bytes())?;
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
