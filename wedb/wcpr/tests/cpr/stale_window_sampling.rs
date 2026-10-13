//! 跨轮陈旧窗口采样：已提交写恰一次承接回归
//! （票 wcpr-stale-window-sampling-cross-round-committed-write-lost）
//!
//! 缺陷形态：写入口在轮 W 开窗期单次合字采样得 (bit=1, V_W)，经慢路径跨过
//! W 轮完整关窗与 W+1 轮开窗后才物理落笔——记录带陈旧位、AOF 条目盖陈旧戳
//! V_W。W+1 检查点发布后恢复：快照收录面被 undo 臂按位回滚剔除、AOF 承接面
//! 被版本闸按 V_W < V_W+1 跳旧，两侧皆不承接，已提交写静默丢失。
//!
//! 修复（读点下移）：位/戳取自分配成功后（tail CAS 与 revivify take 之后、
//! 物理编码前）的 version_shift_word 单读，反映落笔时刻窗口态——落笔于
//! W+1 开窗期即得 (bit=1, V_W+1)，undo 剔除由 AOF 重放恰一次承接。
//!
//! C# 对位：InternalUpsert.cs:318-321 取样紧贴 TryAllocateRecord 成功点，
//! InNewVersion 为会话相位副本（ExecutionContext.cs:162），CPR 状态机关窗链
//! 推进到 REST 需会话排空兜底采样-落笔不可跨完整关窗；rust 无排空机制，
//! 以「读点下移至分配成功点」对齐。
//!
//! AOF 侧经测试内镜像仿真 wnode 版本闸（record_gate.rs:is_old_version_record：
//! `stamp < 恢复基线` 即跳旧；镜像条目均在检查点 AOF 覆盖边界之上，位点闸不参与）。

use std::sync::{Arc, atomic::Ordering};

use aok::{OK, Void};
use tempfile::tempdir;
use wcpr::{CheckpointType, CkptGateState, next_token_above};
use wdev::SegmentedDevice;

use super::support::{HashIndexTestOps, MiniStore};

const K_BASE: &[u8] = b"stalewin:base";
const K_X: &[u8] = b"stalewin:x";
const V_X: &[u8] = b"committed-write";
/// 轮 W 版本（写者陈旧采样值的版本域）
const V_W: i64 = 1;
/// 轮 W+1 版本 = W+1 检查点发布后的恢复基线
const V_W1: i64 = 2;

/// 落笔于轮 W+1 模糊区（addr ∈ [index_start_W+1, tail)）的陈旧写：
/// 修复前 undo 剔除 + 跳旧两侧皆不承接（红）；修复后 undo 剔除由 AOF 重放
/// 恰一次承接（绿）
#[compio::test]
async fn stale_write_landing_in_fuzzy_region_carried_exactly_once() -> Void {
  let dir = tempdir()?;
  let ckpt_dir = dir.path().join("checkpoints");
  let db_path = dir.path().join("stalewin_fuzzy.db");
  let store = MiniStore::open(&db_path)?;
  let gate = CkptGateState::default();
  let p = store.session()?;

  // 基线键：轮 W 之前的稳定面，恢复读回归哨兵
  store.put(&p, K_BASE, b"keep").await?;

  // 轮 W 完整检查点：开窗（版本推进至 V_W）→ 快照 → 关窗
  let token_w = next_token_above(0);
  {
    let floor_w = store.hlog.begin_version_shift(V_W as u64);
    wcpr::create_checkpoint_with_token(
      &store,
      &gate,
      &ckpt_dir,
      CheckpointType::FoldOver,
      token_w,
      floor_w,
    )
    .await?;
    store.hlog.end_version_shift();
  }

  // 轮 W+1 开窗：地板 = 开窗瞬间 tail；写者自此之后（跨完整关窗再开窗）才物理落笔
  let floor_w1 = store.hlog.begin_version_shift(V_W1 as u64);

  // 读点下移后形态：位/戳取自分配成功点（CAS 后、编码前）单读，反映落笔
  // 时刻窗口态——落笔时窗口开启即得 (bit=1, V_W+1)，AOF 镜像随 append
  // 返回值传导同源戳。修复前形态为入口采样滞留 (bit=1, V_W) 直传 + AOF
  // 镜像盖陈旧戳 V_W，恢复期两侧皆不承接（红）
  let aof_mirror: Vec<(Vec<u8>, Vec<u8>, i64)> = {
    let _guard = p.enter();
    let (addr_x, ver_x) = store.hlog.append(K_X, V_X, 0, false)?;
    assert!(
      addr_x >= floor_w1,
      "结构前提: 写必须落在轮 W+1 模糊区: addr={addr_x:#x} floor={floor_w1:#x}"
    );
    assert_eq!(
      ver_x, V_W1,
      "落笔时刻读点的 AOF 戳必须为当前开窗版本（陈旧 V_W 即丢写红形态）"
    );
    store.index.insert(K_X, addr_x)?;
    vec![(K_X.to_vec(), V_X.to_vec(), ver_x)]
  };

  // 轮 W+1 完整检查点：发布后恢复基线即 V_W1
  let token_w1 = next_token_above(token_w);
  wcpr::create_checkpoint_with_token(
    &store,
    &gate,
    &ckpt_dir,
    CheckpointType::FoldOver,
    token_w1,
    floor_w1,
  )
  .await?;
  store.hlog.end_version_shift();

  // 崩溃恢复：从轮 W+1 检查点（最新 token）恢复
  drop(p);
  drop(store);
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let restored = wcpr::recover_latest::<_, MiniStore>(&ckpt_dir, device).await?;
  let p2 = restored.session()?;

  // AOF 版本闸重放仿真（wnode record_gate 同判据：stamp < 基线即跳旧）
  let mut applied = 0usize;
  for (k, v, stamp) in &aof_mirror {
    if *stamp < V_W1 {
      continue;
    }
    restored.put(&p2, k, v).await?;
    applied += 1;
  }

  assert_eq!(
    applied, 1,
    "该写必须恰一次承接: 修复前 undo 剔除 + 跳旧两侧皆不承接"
  );
  assert_eq!(
    restored.get(&p2, K_X).await?.as_deref(),
    Some(V_X),
    "跨完整关窗再开窗落笔的已提交写不得丢失"
  );
  assert_eq!(
    restored.get(&p2, K_BASE).await?.as_deref(),
    Some("keep".as_bytes()),
    "基线键不受陈旧窗口采样面影响"
  );
  assert_eq!(
    restored.recovery_undone.load(Ordering::Acquire),
    1,
    "落笔携带纪元位的记录恰被 undo 臂回滚剔除（快照收录面不承接）"
  );
  OK
}

/// 落笔于轮 W+1 检查点 tail 之后的陈旧写：不在扫描区间、不在快照，
// 承接面仅剩 AOF——修复前陈旧戳被跳旧即丢（红）；修复后落笔时刻戳等于
// 基线不跳旧，恰一次重放承接（绿）
#[compio::test]
async fn stale_write_landing_above_snapshot_tail_carried_exactly_once() -> Void {
  let dir = tempdir()?;
  let ckpt_dir = dir.path().join("checkpoints");
  let db_path = dir.path().join("stalewin_above_tail.db");
  let store = MiniStore::open(&db_path)?;
  let gate = CkptGateState::default();
  let p = store.session()?;

  store.put(&p, K_BASE, b"keep").await?;

  let token_w = next_token_above(0);
  {
    let floor_w = store.hlog.begin_version_shift(V_W as u64);
    wcpr::create_checkpoint_with_token(
      &store,
      &gate,
      &ckpt_dir,
      CheckpointType::FoldOver,
      token_w,
      floor_w,
    )
    .await?;
    store.hlog.end_version_shift();
  }

  // 轮 W+1 完整检查点：陈旧写者在检查点发布并关窗之后才落笔
  let floor_w1 = store.hlog.begin_version_shift(V_W1 as u64);
  let token_w1 = next_token_above(token_w);
  wcpr::create_checkpoint_with_token(
    &store,
    &gate,
    &ckpt_dir,
    CheckpointType::FoldOver,
    token_w1,
    floor_w1,
  )
  .await?;
  store.hlog.end_version_shift();

  // 读点下移后形态：落笔于 W+1 关窗之后、快照 tail 之上——位/戳取自分配
  // 成功点单读，得 (bit=0, V_W+1)，戳等于恢复基线不被跳旧。修复前形态为
  // 采样滞留 (bit=1, V_W)：三不沾（扫描区间外/快照外）+ 跳旧即丢（红）
  let aof_mirror: Vec<(Vec<u8>, Vec<u8>, i64)> = {
    let _guard = p.enter();
    let (addr_x, ver_x) = store.hlog.append(K_X, V_X, 0, false)?;
    assert_eq!(
      ver_x, V_W1,
      "关窗后落笔的 AOF 戳为关窗保留版本（等于基线，不被跳旧）"
    );
    store.index.insert(K_X, addr_x)?;
    vec![(K_X.to_vec(), V_X.to_vec(), ver_x)]
  };

  // 崩溃恢复（记录未随任何检查点落盘，纯 AOF 承接面）
  drop(p);
  drop(store);
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let restored = wcpr::recover_latest::<_, MiniStore>(&ckpt_dir, device).await?;
  let p2 = restored.session()?;

  let mut applied = 0usize;
  for (k, v, stamp) in &aof_mirror {
    if *stamp < V_W1 {
      continue;
    }
    restored.put(&p2, k, v).await?;
    applied += 1;
  }

  assert_eq!(applied, 1, "快照外写必须由 AOF 恰一次重放承接");
  assert_eq!(
    restored.get(&p2, K_X).await?.as_deref(),
    Some(V_X),
    "快照外的已提交写不得因陈旧戳被跳旧而丢失"
  );
  assert_eq!(
    restored.get(&p2, K_BASE).await?.as_deref(),
    Some("keep".as_bytes()),
    "基线键不受陈旧窗口采样面影响"
  );
  assert_eq!(
    restored.recovery_undone.load(Ordering::Acquire),
    0,
    "快照 tail 之上的落笔不在 undo 扫描区间"
  );
  OK
}
