//! 内置 GC 强取消路径的存活位收口锁测（独立测试目标：TASKS 注册表进程内
//! 共享，独立二进制规避与 gc.rs 其它测试的 gc_scan 环并行互扰）。
//!
//! 生产触发链（工单 wbase-supervise-cancel-path-alive-stuck-false-alive）：
//! GcHandle::drop 丢弃 JoinHandle 即执行器 cancel(true) 强取消，任务停泊于
//! gc_scan_loop 的 sleep(interval) 未经终局 poll 即整图丢弃，GC_SCAN_TASK
//! 存活位必须经 Supervised 的 Drop 臂确定性翻假（对标 C# TaskManager.cs
//! :CancelAsync 的 registry.TryRemove 即收口、:IsRunning 立即翻假），
//! 绝无「禁用即停 / 换入收口后快照仍报活」。
//!
//! 在 garnet 中的相对路径: libs/server/TaskManager/TaskManager.cs:CancelAsync

use std::{sync::Arc, time::Duration};

use aok::{OK, Void};
use compio::{runtime::Runtime, time::sleep};
use wbase::supervise::snapshots;
use wdev::SegmentedDevice;
use wkv::{GcConfig, GcManager, StoreConfig, WedbStore};

/// GC_SCAN_TASK 任务名（wkv/src/gc/mod.rs 私有常量的对位字面量）
const GC_SCAN_TASK: &str = "gc_scan";

/// GC_SCAN_TASK 条目存活位
fn gc_scan_alive() -> bool {
  snapshots()
    .into_iter()
    .any(|s| s.name == GC_SCAN_TASK && s.alive)
}

/// 轮询等待条件成立（协作式退出 + 异步取消收割均留足裕量；2s 上界）
async fn wait_until(pred: impl Fn() -> bool) -> bool {
  for _ in 0..200 {
    if pred() {
      return true;
    }
    sleep(Duration::from_millis(10)).await;
  }
  pred()
}

/// 构造独立临时库（GC 由测试手动驱动，间隔外置注入；TempDir 与 store 同寿）
async fn open_store(
  tag: &str,
) -> aok::Result<(tempfile::TempDir, Arc<WedbStore<SegmentedDevice>>)> {
  let dir = tempfile::tempdir()?;
  let device = Arc::new(SegmentedDevice::new(
    dir.path().join(format!("gc_alive_{tag}.db")),
    4096,
    4096,
  )?);
  let mut config = StoreConfig::new(1024, 4096, 16, 0.5)?;
  config.gc = GcConfig::default();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device)?);
  Ok((dir, store))
}

/// 强取消与换入重拉全链的存活位收口（单函数串行：同名归组下多环并行会互扰
/// gc_scan 条目观测，严禁拆分为并行测试）：
/// 1) GcManager::spawn 拉起令任务停泊长间隔 sleep 后丢弃句柄（生产触发链
///    的最小复刻：JoinHandle Drop → cancel(true)，未来体不经终局 poll 整图
///    丢弃）——存活位必须翻假，不得卡真；
/// 2) 公开链 start_gc 停泊 → stop_gc + start_gc 换入（reconcile_gc_scan 替换
///    重拉臂：先摘旧句柄 Drop 强取消清位、再重拉新实例）——新环回真；
/// 3) 换入后的新环仍可正常判死（禁用即停退出后存活位翻假），观测面不失真
#[test]
fn test_gc_scan_supervise_alive_cancel_and_reconcile() -> Void {
  Runtime::new()?.block_on(async {
    let (_dir, store) = open_store("stuck").await?;
    // 长间隔：任务停泊 sleep，stop 置协作位后至多不再 poll（强取消前置形态）
    store.update_gc_config(|c| {
      c.enabled = true;
      c.scan_interval_ms = 30_000;
    });

    // 1) 句柄丢弃即强取消：GC_SCAN_TASK 行不卡真（对标 CancelAsync 即收口）
    let handle = GcManager::spawn(Arc::clone(&store));
    sleep(Duration::from_millis(100)).await;
    assert!(gc_scan_alive(), "前置：停泊中的扫描环存活位为真");
    drop(handle);
    assert!(
      wait_until(|| !gc_scan_alive()).await,
      "强取消丢弃后 GC_SCAN_TASK 存活位必须翻假，不得卡真"
    );

    // 2) 公开链换入：新环 poll 后回真
    assert!(store.start_gc(), "停泊形态下 start_gc 必须重拉新环");
    assert!(
      wait_until(gc_scan_alive).await,
      "前置：新环存活位回真（新实例 poll 置真）"
    );
    sleep(Duration::from_millis(100)).await;
    store.stop_gc();
    assert!(
      store.start_gc(),
      "stop 后任务停泊未再 poll，start_gc 必须走替换重拉臂"
    );
    assert!(store.gc_running(), "换入后新环在跑");
    assert!(
      wait_until(gc_scan_alive).await,
      "换入后 GC_SCAN_TASK 行必须回真（残余瞬态误清由新环 poll 自愈）"
    );

    // 3) 判死面：短间隔再换入一轮，禁用即停退出后存活位翻假
    store.update_gc_config(|c| c.scan_interval_ms = 30);
    store.stop_gc();
    assert!(store.start_gc(), "短间隔配置下再走一轮替换重拉");
    assert!(wait_until(gc_scan_alive).await, "短间隔新环存活位回真");
    store.stop_gc();
    assert!(
      wait_until(|| !store.gc_running()).await,
      "禁用即停：新环必须退出"
    );
    assert!(
      wait_until(|| !gc_scan_alive()).await,
      "新环退出后存活位必须翻假，换入不损观测面判死力"
    );

    aok::Result::<()>::Ok(())
  })?;
  OK
}
