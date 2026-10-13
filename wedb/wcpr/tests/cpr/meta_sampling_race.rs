//! 检查点元数据 late-sampling 竞态回归（票 wcpr-hlog-meta-late-sampling-race）
//!
//! 缺陷：create_checkpoint_inner 第 3 步 WAIT_FLUSH 入口只捕获一致性截断点 tail，
//! HlogMeta 的 begin/head 推迟到第 9 步元数据落盘前现场采样，中间横跨第 5 步纪元排空、
//! 第 6 步 flush_all、第 8 步目录树 fsync 三个 await 的刷盘窗口（大库可达秒级）。窗口内
//! 前台写入回绕驱逐经 `ensure_page_ready` 自动推 head、紧缩链 `shift_begin_address` 推逻辑
//! begin（物理删段受 delete_floor 钳制），二者均可越过捕获 tail，产出 head>tail /
//! begin>tail 的撕裂元数据：integrity_crc32 只封签采样结果本身、拦不住时序撕裂；崩溃恢复
//! 时被 recover.rs 地址不变式校验具名拒启、recover_latest 逐代回退叠加本代发布后已截断的
//! AOF，旧检查点之后的写入静默永久丢失。
//!
//! 修复：采样点上移到第 3 步同步段，与 tail 同一无 await 区段一次性捕获 cp_begin/cp_head，
//! 第 9 步改用快照；flushed 维持后采样（次序自洽 + 恢复侧 flushed.min(tail) 钳制在位）。
//! 恢复侧零改动。
//!
//! C# 契约锚：HybridLogCheckpointSMTask.cs:GlobalBeforeEnteringState —— PREPARE 段捕获
//! beginAddress，WAIT_FLUSH 入口同段捕获 finalLogicalAddress = GetTailAddress() 紧接
//! headAddress = HeadAddress（相邻同步语句，捕获时刻运行时不变式 head <= tail 恒自洽）；
//! Checkpoint.cs:WriteHybridLogMetaInfo 只序列化已捕获快照，BeginAddress 物理推进推迟到
//! REST 段 CleanupLogCheckpoint。
//!
//! 注入用真实 HybridLog 原语（fixture `arm_sampling_race`）：非假 mock——head/begin 由真实
//! `shift_head_address` / `shift_begin_address` 推进，元数据由真实采样读回。两形态同判据：
//! 落盘 meta 满足 begin <= head <= min(flushed, tail) <= tail 且 recover_latest 恢复成功
//! 不再拒启，同时断言注入生效（窗口内真实 head/begin 已越过捕获 tail、旧现采必读到越界值）。

use std::sync::Arc;

use aok::{OK, Void};
use tempfile::tempdir;
use wcpr::{CheckpointType, CkptGateState, create_checkpoint, recover_latest};
use wdev::SegmentedDevice;

use super::support::{MiniStore, RACE_BEGIN_PAST_TAIL, RACE_HEAD_PAST_TAIL};

const K_BASE: &[u8] = b"meta-race:base";
const V_BASE: &[u8] = b"keep";

/// 驱逐形态：刷盘窗口内真实驱逐把 head 推过捕获 tail，第 9 步现采必产出 head>tail
#[compio::test]
async fn eviction_advances_head_past_captured_tail_without_meta_tearing() -> Void {
  let dir = tempdir()?;
  let ckpt_dir = dir.path().join("checkpoints");
  let db_path = dir.path().join("meta_race_head.db");
  let store = MiniStore::open(&db_path)?;
  let gate = CkptGateState::default();
  let p = store.session()?;

  store.put(&p, K_BASE, V_BASE).await?;
  drop(p);

  store.arm_sampling_race(RACE_HEAD_PAST_TAIL);
  let meta = create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver).await?;

  // 注入生效：窗口内真实驱逐把 head 推过了第 3 步捕获的 tail（旧第 9 步现采必读到越界值）
  assert!(
    store.hlog.head_address() > meta.hlog_meta.tail_address,
    "竞态窗口必须真实把 head 推过捕获 tail: head={:#x} tail={:#x}",
    store.hlog.head_address(),
    meta.hlog_meta.tail_address,
  );
  // 修复判据：落盘 meta 携第 3 步同点快照，取值冻结、绝不采窗口内被驱逐推进的现值
  assert!(
    meta.hlog_meta.head_address < store.hlog.head_address(),
    "修复须冻结第 3 步捕获快照，拒采窗口内陈旧现值"
  );
  assert!(
    meta.hlog_meta.begin_address <= meta.hlog_meta.head_address,
    "落盘 begin<=head（捕获时刻运行时不变式）"
  );
  assert!(
    meta.hlog_meta.head_address <= meta.hlog_meta.tail_address,
    "落盘 head<=tail（旧现采窗口内驱逐推 head 越界即此断言红、恢复具名拒启）"
  );
  assert!(
    meta
      .hlog_meta
      .flushed_until_address
      .min(meta.hlog_meta.tail_address)
      >= meta.hlog_meta.head_address,
    "落盘 min(flushed,tail)>=head（恢复校验族 head<=flushed）"
  );

  // 恢复侧具名校验通过（旧代码此处 recover_latest 抛 HeadAddress 超出 TailAddress 逐代回退）
  drop(store);
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let restored = recover_latest::<_, MiniStore>(&ckpt_dir, device).await?;
  let p2 = restored.session()?;
  assert_eq!(
    restored.get(&p2, K_BASE).await?.as_deref(),
    Some(V_BASE),
    "基线键随一致性快照恢复可见"
  );
  OK
}

/// 紧缩形态：刷盘窗口内真实紧缩把逻辑 begin 推过捕获 tail，第 9 步现采必产出 begin>tail
#[compio::test]
async fn compaction_advances_begin_past_captured_tail_without_meta_tearing() -> Void {
  let dir = tempdir()?;
  let ckpt_dir = dir.path().join("checkpoints");
  let db_path = dir.path().join("meta_race_begin.db");
  let store = MiniStore::open(&db_path)?;
  let gate = CkptGateState::default();
  let p = store.session()?;

  store.put(&p, K_BASE, V_BASE).await?;
  drop(p);

  store.arm_sampling_race(RACE_BEGIN_PAST_TAIL);
  let meta = create_checkpoint(&store, &gate, &ckpt_dir, CheckpointType::FoldOver).await?;

  // 注入生效：窗口内真实紧缩把逻辑 begin 推过了捕获 tail（旧现采必读到越界值）
  assert!(
    store.hlog.begin_address() > meta.hlog_meta.tail_address,
    "竞态窗口必须真实把 begin 推过捕获 tail: begin={:#x} tail={:#x}",
    store.hlog.begin_address(),
    meta.hlog_meta.tail_address,
  );
  assert!(
    meta.hlog_meta.begin_address < store.hlog.begin_address(),
    "修复须冻结第 3 步捕获快照，拒采窗口内被紧缩推进的现值"
  );
  assert!(
    meta.hlog_meta.begin_address <= meta.hlog_meta.tail_address,
    "落盘 begin<=tail（旧现采窗口内紧缩推 begin 越界即此断言红、恢复 begin>tail 具名拒启）"
  );
  assert!(
    meta.hlog_meta.begin_address <= meta.hlog_meta.head_address,
    "落盘 begin<=head（捕获时刻运行时不变式）"
  );
  assert!(
    meta
      .hlog_meta
      .flushed_until_address
      .min(meta.hlog_meta.tail_address)
      >= meta.hlog_meta.begin_address,
    "落盘 min(flushed,tail)>=begin（恢复校验族 flushed>=begin）"
  );

  drop(store);
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let restored = recover_latest::<_, MiniStore>(&ckpt_dir, device).await?;
  let p2 = restored.session()?;
  assert_eq!(
    restored.get(&p2, K_BASE).await?.as_deref(),
    Some(V_BASE),
    "基线键随一致性快照恢复可见"
  );
  OK
}
