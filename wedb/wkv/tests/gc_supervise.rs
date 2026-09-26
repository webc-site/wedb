//! 内置 GC 过期扫描循环监督接线集成测试（工单 bg-periodic-task-supervise-gap-matrix
//! 项 a）：[`GcManager::spawn`] 内置弱引用循环体经 wbase [`supervise_task`] 单点
//! 顶层监督——拉起后监督快照（INFO bg_task_health 真源）含本名且 alive 真；禁用
//! 终局后 alive 复位假且 [`GcHandle::is_active`] 变假（既有
//! [`WedbStore::reconcile_gc_scan`] 的 is_active 判定即可经 CONFIG SET 调停 /
//! 角色切换重拉）；同名 panic 注入验证 panics 计数递增与 alive 复位假（死亡不再
//! 与空闲不可区分）。对标 C# StoreWrapper.cs:ExpiredKeyDeletionScanTaskAsync
//! catch LogCritical「The task won't be resumed」的死亡留痕契约。
//!
//! 本二进制仅此一个测试：监督注册表为进程级全局名单，panic 注入与真实拉起须
//! 串行执行（仿 wkv/src/gc/reclaim.rs 监督快照测试形态）。
use std::{sync::Arc, time::Duration};

use aok::{OK, Void};
use compio::{runtime::Runtime, time::sleep};
use wbase::supervise::{BgTaskSnapshot, snapshots, supervise_task};
use wdev::SegmentedDevice;
use wkv::{GcConfig, GcManager, StoreConfig, WedbStore};

/// 取指定任务名的监督快照条目
fn snapshot(name: &str) -> Option<BgTaskSnapshot> {
  snapshots().into_iter().find(|s| s.name == name)
}

#[test]
fn test_gc_scan_loop_supervise_registration() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempfile::tempdir()?;
    let device = Arc::new(SegmentedDevice::new(
      dir.path().join("gc_supervise.db"),
      4096,
      4096,
    )?);
    let mut config = StoreConfig::new(1024, 4096, 16, 0.5)?;
    config.gc = GcConfig {
      enabled: true,
      scan_interval_ms: 20,
      ..GcConfig::default()
    };
    let store = Arc::new(WedbStore::open(config, device)?);
    store.update_gc_config(|c| {
      c.enabled = true;
      c.scan_interval_ms = 20;
    });
    let handle = GcManager::spawn(Arc::clone(&store));
    // 让执行器 poll 到监督包装（注册与 alive 置真发生在 supervise_task 调用即行）
    sleep(Duration::from_millis(80)).await;
    let e = snapshot("gc_scan").expect("GC 扫描循环须注册进监督快照（bg_task_health）");
    assert!(e.alive, "在跑态存活位须为真");
    assert_eq!(e.panics, 0, "拉起即行无 panic");

    // 禁用终局（Ok 臂）：循环在一个间隔内退出 → 外层 spawn 未来体结束 →
    // alive 复位假、is_finished 变真 → is_active 变假，reconcile_gc_scan 的
    // is_active 判定即可经 CONFIG SET 调停 / 角色切换重拉
    store.update_gc_config(|c| c.enabled = false);
    for _ in 0..200 {
      if handle.is_finished() {
        break;
      }
      sleep(Duration::from_millis(10)).await;
    }
    assert!(handle.is_finished(), "禁用即停：内置循环必须退出");
    assert!(
      !handle.is_active(),
      "终局即 is_active 假（reconcile_gc_scan 重拉判定可用）"
    );
    let e = snapshot("gc_scan").expect("终局后名单条目常驻");
    assert!(!e.alive, "正常终局后存活位复位假");

    // 同名 panic 注入（wbase 单点真实驱动、生产归组名）：panics 计数递增、
    // alive 保持复位假——死亡留痕经 bg_task_health 可观测
    let out: Result<(), _> = supervise_task("gc_scan", async {
      panic!("测试注入毒丸探针（GC 扫描循环）");
    })
    .await;
    assert!(out.is_err(), "panic 臂须以 Err 产出");
    assert_eq!(
      out.unwrap_err().text(),
      "测试注入毒丸探针（GC 扫描循环）",
      "panic 载荷文本留痕"
    );
    let e = snapshot("gc_scan").expect("同名条目归组复用");
    assert_eq!(e.panics, 1, "panic 计数监督快照可观测");
    assert!(!e.alive, "panic 终局后存活位复位假");

    drop(handle);
    drop(store);
    OK
  })
}
