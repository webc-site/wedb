//! 换号物理回收常驻驱动挂载域集成测试（自 src/gc/reclaim.rs 内联测迁入：
//! tempdir 真 store 真设备形态，断言与覆盖原样保留）
//!
//! 依赖面：`spawn_bftree_reclaimer`/`GcManager`/`StoreConfig`/`WedbStore`/
//! `reclaimer_mounted()` 访问器均为既有 pub 面；`reclaim_physical` 与
//! `reclaim_inflight` 经 #[doc(hidden)] 测试专用口访问（见各自定义处注）。

use std::{
  sync::{Arc, atomic::Ordering::Relaxed},
  time::Duration as TimeDuration,
};

use compio::{runtime::Runtime, time as compio_time};
use wbase::supervise::snapshots;
use wdev::SegmentedDevice;
use wkv::{GcConfig, GcManager, StoreConfig, WedbStore, spawn_bftree_reclaimer};

/// 挂载位生命周期与防重复挂载幂等：open 冷启动不挂载（位假）→ 首 spawn 置位
/// → 同实例二次 spawn 直接返回（不 panic、位不重复置位）。open_shared 的
/// 「首挂即置位」形态由挂载点测试（wnode tests/database_manager.rs 恢复臂
/// 回归）与生产路径覆盖
#[test]
fn test_reclaimer_mount_is_idempotent() -> wkv::Result<()> {
  Runtime::new()?.block_on(async {
    let dir = tempfile::tempdir()?;
    let cfg = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("reclaimer_idem.db"),
    )?);
    let store = Arc::new(WedbStore::open(cfg, device)?);
    assert!(
      !store.reclaimer_mounted(),
      "open 冷启动不经挂载口，挂载位须为假"
    );
    spawn_bftree_reclaimer(&store);
    assert!(store.reclaimer_mounted(), "首次挂载即置位");
    spawn_bftree_reclaimer(&store);
    assert!(
      store.reclaimer_mounted(),
      "同实例二次挂载只认首次，幂等返回"
    );
    Ok(())
  })
}

#[test]
fn test_reclaim_physical_inflight_exclusion() -> wkv::Result<()> {
  Runtime::new()?.block_on(async {
    let dir = tempfile::tempdir()?;
    let cfg = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("reclaim_inflight.db"),
    )?);
    let store = Arc::new(WedbStore::open(cfg, device)?);
    let gc_mgr = GcManager::new(&store);

    let gc_cfg = GcConfig::default();

    // 早退臂：单飞闸已置位 → 原样让位返回 Ok(())。语义铁证在闸位：
    // 真跑过一轮必经 RunGuard、收场必落闸；闸仍置位即「未执行任何回收」
    // 的返回语义（让位 Ok ≠ 回收成功 Ok）
    store.reclaim_inflight.store(true, Relaxed);
    let res = gc_mgr.reclaim_physical(&store, &gc_cfg).await;
    assert!(res.is_ok(), "单飞闸置位下的让位返回必须 Ok: {res:?}");
    assert!(
      store.reclaim_inflight.load(Relaxed),
      "早退臂不得动闸：闸须保持置位（真跑过 RunGuard 必落闸）"
    );

    // 实跑臂：闸空闲 → 取闸执行，收场经 RunGuard 落闸、不残留自钉
    store.reclaim_inflight.store(false, Relaxed);
    let res = gc_mgr.reclaim_physical(&store, &gc_cfg).await;
    assert!(res.is_ok(), "闸空闲时实跑轮必须 Ok: {res:?}");
    assert!(
      !store.reclaim_inflight.load(Relaxed),
      "实跑轮收场必须经 RunGuard 落闸，不得残留自钉"
    );

    Ok(())
  })
}

/// 有界轮询：监督快照出现目标常驻任务（注册发生在 supervise_task 首次 poll），
/// 10ms 步进、5s 预算——固定 50ms 单发等待在慢机上 poll 未达即假红
async fn wait_until_supervised(name: &str) -> bool {
  for _ in 0..500 {
    if snapshots().iter().any(|s| s.name == name) {
      return true;
    }
    compio_time::sleep(TimeDuration::from_millis(10)).await;
  }
  snapshots().iter().any(|s| s.name == name)
}

/// 监督快照注册可见（r30-bgthread 发现五：INFO bg_task_health 真源——
/// 常驻回收任务挂载后进入 wbase::supervise 名单，panic 死亡留计数可观测，
/// 不再与空闲不可区分）
#[test]
fn test_reclaimer_registers_in_supervise_snapshot() -> wkv::Result<()> {
  Runtime::new()?.block_on(async {
    let dir = tempfile::tempdir()?;
    let cfg = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("reclaimer_supervise.db"),
    )?);
    let store = Arc::new(WedbStore::open(cfg, device)?);
    spawn_bftree_reclaimer(&store);
    // 注册发生在 supervise_task 首次 poll：轮询等齐再断言
    assert!(
      wait_until_supervised("bftree_reclaimer").await,
      "常驻回收任务须注册进监督快照"
    );
    Ok(())
  })
}
