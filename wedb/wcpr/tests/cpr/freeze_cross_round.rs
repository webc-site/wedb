//! 跨轮陈旧纪元记录冻结判据回归（票 zcode-r42-whlogfix 发现四）
//!
//! C# 对位：libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Helpers.cs:IsFrozen
//! 双析取项 `Ctx.IsInV1 && (logicalAddress <= startLogicalAddress || !srcRecordInfo.IsInNewVersion)`。
//!
//! 轮 N 版本推进窗内追加的 K1 携带 N 代 IN_NEW_VERSION 位；轮 N+1 开窗后 K1 地址
//! 已落到本轮地板（= index_start）之下而位仍在。若冻结判据丢失 `addr <= floor`
//! 析取项，等长原位 INCR 将原位命中该「老地址新内容」形态：字节进快照物理收录面
//! （addr < index_start 不在 undoNextVersion 回滚窗），AOF 条目版本戳 > covered 又
//! 必重放——同一效果恰双算。本用例经公开口驱动两轮窗口与轮间原位写，断言：
//! 1. 窗口期原位写必须冻结降级（判据缺 addr 项时此断言反中）；
//! 2. 崩溃恢复后 K1 恰为轮 N 值（降级追加面被回滚，无 AOF 形态下窗内写零承接）；
//! 3. 恢复统计 undone/replayed 精确计数，防静默路径漂移。

use std::sync::{Arc, atomic::Ordering};

use aok::{OK, Void};
use tempfile::tempdir;
use wcpr::{CheckpointType, CkptGateState, next_token_above};
use wdev::SegmentedDevice;

use super::support::{HashIndexTestOps, MiniStore};

const K1: &[u8] = b"freeze:k1";
/// 轮 N 窗内值（等长 INCR 的旧值形态）
const V_N: &[u8] = b"100";
/// 等长原位 INCR 的新值形态
const V_INCR: &[u8] = b"101";

#[compio::test]
async fn cross_round_stale_epoch_record_frozen_during_next_window() -> Void {
  let dir = tempdir()?;
  let ckpt_dir = dir.path().join("checkpoints");
  let db_path = dir.path().join("freeze_cross_round.db");
  let store = MiniStore::open(&db_path)?;
  let gate = CkptGateState::default();
  let p = store.session()?;

  // 基线键：占据快照稳定面，恢复读回归哨兵
  store.put(&p, b"freeze:base", b"keep").await?;

  // 轮 N 版本推进窗：窗内追加 K1，携带 N 代纪元位（宿主 begin → 写 → end 时序）
  let addr_n = {
    store.hlog.begin_version_shift(1);
    let addr = {
      let _guard = p.enter();
      let (addr, _) = store.hlog.append(K1, V_N, 0, false)?;
      store.index.insert(K1, addr)?;
      addr
    };
    store.hlog.end_version_shift();
    addr
  };

  // 轮 N+1 开窗：地板越过 K1，K1 成「地址在地板下、位属前代」的陈旧纪元记录
  let floor = store.hlog.begin_version_shift(2);
  assert!(
    floor > addr_n,
    "结构前提: 地板必须越过轮 N 记录: floor={floor:#x} addr={addr_n:#x}"
  );

  // 等长原位 INCR 尝试：冻结判据必须拒绝（判据缺 addr 析取项时此处反中，
  // 记录字节被原位改写即进快照收录面，AOF 形态下恰双算）
  let in_place = {
    let _guard = p.enter();
    store.hlog.try_update_in_place(addr_n, K1, V_INCR)?
  };
  assert!(!in_place, "跨轮陈旧纪元记录在窗口期必须冻结降级为尾部追加");

  // 写内核降级臂对位：RCU 尾部追加携带本轮纪元位并 CAS 接管索引槽位
  {
    let _guard = p.enter();
    let slot = store.index.find_tag(K1).expect("轮 N 已插入 K1 槽位");
    let (addr_fallback, _) = store.hlog.append(K1, V_INCR, addr_n, false)?;
    assert!(
      addr_fallback >= floor,
      "降级追加面必须落在本轮模糊区: addr={addr_fallback:#x} floor={floor:#x}"
    );
    assert!(
      store.index.update_address(K1, slot, addr_fallback),
      "测试串行场景索引 CAS 必成功"
    );
  }

  // 宿主时序对位：窗口保持开启进快照段，index_start 精准取本轮地板，返回后收口
  let token = next_token_above(0);
  let meta = wcpr::create_checkpoint_with_token(
    &store,
    &gate,
    &ckpt_dir,
    CheckpointType::FoldOver,
    token,
    floor,
  )
  .await?;
  store.hlog.end_version_shift();
  assert_eq!(
    meta.index_start_logical_address, floor,
    "快照模糊区地板必须与开窗地板同源"
  );

  // 崩溃恢复：降级追加面（带本轮位、addr >= index_start）必须被 undoNextVersion
  // 回滚，K1 生效面仅剩快照中的轮 N 值（无 AOF 形态下窗内写零承接即恰一次面）
  drop(p);
  drop(store);
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let restored = wcpr::recover_latest::<_, MiniStore>(&ckpt_dir, device).await?;
  let p2 = restored.session()?;

  assert_eq!(
    restored.get(&p2, K1).await?.as_deref(),
    Some(V_N),
    "K1 必须恰为轮 N 值: 原位冻结已拒绝、降级追加面已回滚"
  );
  assert_eq!(
    restored.get(&p2, b"freeze:base").await?.as_deref(),
    Some("keep".as_bytes()),
    "基线键不受跨轮冻结面影响"
  );
  assert_eq!(
    restored.recovery_undone.load(Ordering::Acquire),
    1,
    "恰一条窗内带位记录（降级追加面）被回滚"
  );
  assert_eq!(
    restored.recovery_replayed.load(Ordering::Acquire),
    0,
    "窗口内无旧版记录需模糊重插"
  );
  OK
}
