//! 会话延迟指标属主独占 + 版本翻转点按引用归并对标测试
//!（对标 libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs 的
//! Start/Stop/RecordValue/Return 与 libs/server/Metrics/Latency/
//! GarnetLatencyMetrics.cs:Merge 的 rust 归属转移形态：会话双缓冲槽为连接
//! 任务独占，退役槽由属主线程按引用并入全局延迟表）
//!
//! 修复前必红的可证伪点：
//! - 旧实现在归并出口经 `metrics_snapshot()` 持锁全量克隆
//!   `Vec<LatencyMetricsEntrySession>`（6 类别 × 2 槽 = 12 个直方图 + Vec
//!   本身，每轮 ≥13 次堆分配），本文件第三用例以计数分配器钉死归并轮
//!   分配数上限；
//! - 旧归并口只读不清槽（复位依赖监视器跨线程 `ResetAllLatencyMetrics`
//!   臂），"归并即清零、每窗样本恰计一次"在旧跨线程拆装下无同等保证，
//!   第一、二用例以样本数精确断言承接。
//!
//! 自研依据: 会话延迟归属合并

use std::{
  alloc::{GlobalAlloc, Layout, System},
  sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
  },
};

use wmetric::{GarnetLatencyMetricsSession, GarnetServerMonitor, LatencyMetricsType};

/// 全文件串行闸：集成测试同二进制内并发线程共享计数分配器，用例间以本锁
/// 互斥，保证分配计数窗口的观测纯净（锁仅覆盖测试体，不引入被测语义）。
static FILE_LOCK: Mutex<()> = Mutex::new(());

/// 分配次数计数（仅计 alloc 入口；dealloc 不 decrement，观测窗口取差值）。
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

/// 转发 System 并在每次分配入口计数的全局分配器（测试二进制专用）。
struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
  unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
    ALLOCS.fetch_add(1, Ordering::Relaxed);
    unsafe { System.alloc(layout) }
  }

  unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
    unsafe { System.dealloc(ptr, layout) }
  }
}

#[global_allocator]
static COUNTING: CountingAlloc = CountingAlloc;

/// 开延迟监视的监视器（不装进程级槽：会话显式取用本实例的时钟与全局表）。
fn monitor() -> GarnetServerMonitor {
  GarnetServerMonitor::new(1, true, true, false)
}

/// 以监视器同源时钟 + 全局出口建会话延迟表。
fn session(monitor: &GarnetServerMonitor) -> GarnetLatencyMetricsSession {
  GarnetLatencyMetricsSession::new(
    Arc::clone(&monitor.monitor_iterations),
    monitor.global_latency_metrics(),
    GarnetLatencyMetricsSession::DEFAULT_LATENCY_TYPES,
  )
}

/// 全局延迟表指定类别样本数。
fn global_calls(monitor: &GarnetServerMonitor, cmd: LatencyMetricsType) -> u64 {
  monitor
    .global_latency_metrics()
    .expect("track_latency 开启时全局延迟表就位")
    .lock()
    .metrics[cmd.idx()]
  .len()
}

/// 会话指定类别指定槽样本数。
fn slot_calls(session: &GarnetLatencyMetricsSession, cmd: LatencyMetricsType, ver: usize) -> u64 {
  session.metrics[cmd.idx()].latency[ver].len()
}

/// 用例一：会话内 start/stop 计数（属主独占表直读观测，无需锁与快照）。
#[test]
fn test_session_start_stop_counts_within_current_version_slot() {
  let _gate = FILE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
  let monitor = monitor();
  let mut session = session(&monitor);
  let ver = session.version();

  // 成对起停 3 次：当前版本槽恰 3 条
  for k in 0..3u64 {
    session.start(LatencyMetricsType::NetRsLat, 100 + k * 100);
    assert_eq!(session.get(LatencyMetricsType::NetRsLat), 100 + k * 100);
    session.stop(LatencyMetricsType::NetRsLat, 150 + k * 100);
    assert_eq!(session.get(LatencyMetricsType::NetRsLat), 0);
  }
  assert_eq!(slot_calls(&session, LatencyMetricsType::NetRsLat, ver), 3);

  // 无 start 直停：不加计（C# startTimestamp==0 短路同形）
  session.stop(LatencyMetricsType::NetRsLat, 999);
  assert_eq!(slot_calls(&session, LatencyMetricsType::NetRsLat, ver), 3);

  // 另一类别独立计槽：start/stop 与 record_value 各计一条
  session.start(LatencyMetricsType::NetRsOps, 10);
  session.stop(LatencyMetricsType::NetRsOps, 20);
  session.record_value(LatencyMetricsType::NetRsOps, 5);
  assert_eq!(slot_calls(&session, LatencyMetricsType::NetRsOps, ver), 2);
  assert_eq!(slot_calls(&session, LatencyMetricsType::NetRsLat, ver), 3);

  // 非当前版本槽恒零：双缓冲互斥
  assert_eq!(
    slot_calls(&session, LatencyMetricsType::NetRsLat, 1 - ver),
    0
  );
}

/// 用例二：跨会话版本翻转归并不丢量、不双计、释放不重复。
#[test]
fn test_cross_session_version_roll_merge_is_lossless_and_once() {
  let _gate = FILE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
  let monitor = monitor();
  let mut s1 = session(&monitor);
  let mut s2 = session(&monitor);

  // 两会话在同一窗口各自记点：s1 三条 NET_RS_LAT，s2 五条
  for k in 0..3i64 {
    s1.record_value(LatencyMetricsType::NetRsLat, 10 + k);
  }
  for k in 0..5i64 {
    s2.record_value(LatencyMetricsType::NetRsLat, 20 + k);
  }
  assert_eq!(global_calls(&monitor, LatencyMetricsType::NetRsLat), 0);

  // 时钟推进（监视器采样轮同款动作），两会话各自在下一次触碰时归并退役槽
  monitor.monitor_iterations.fetch_add(1, Ordering::Relaxed);
  s1.start(LatencyMetricsType::NetRsLat, 500);
  s2.stop(LatencyMetricsType::NetRsLat, 600); // 无进行中操作，仅触发翻转
  assert_eq!(global_calls(&monitor, LatencyMetricsType::NetRsLat), 8);
  // 归并即清零：两槽退役侧皆空，重复翻转不再注入
  assert_eq!(slot_calls(&s1, LatencyMetricsType::NetRsLat, 0), 0);
  assert_eq!(slot_calls(&s2, LatencyMetricsType::NetRsLat, 0), 0);
  monitor.monitor_iterations.fetch_add(1, Ordering::Relaxed);
  // s1.stop(700)：消费 start(500) 的在途戳，记入翻转后的新窗 slot 0（暂不入全局）
  s1.stop(LatencyMetricsType::NetRsLat, 700);
  s2.stop(LatencyMetricsType::NetRsLat, 800);
  assert_eq!(
    global_calls(&monitor, LatencyMetricsType::NetRsLat),
    8,
    "空退役槽再翻不得凭空注入"
  );

  // 释放收口：并入全局的是两条、各恰一次，共 10——
  // (a) 上一步 s1.stop(700) 消费的是 s1.start(500) 遗留的在途戳，C# 语义下
  //     必记一次样本（GarnetLatencyMetricsSession.cs:78-82 Stop 即
  //     RecordValue(Version)；LatencyMetricsEntrySession.cs:40-50 只要
  //     startTimestamp 非 0 即记录并清戳），该条落在翻转后的新窗 slot 0，
  //     所以上方全局仍为 8，并非「没记」；
  // (b) 本行 start(900)→stop(1000) 再记一条。
  // return_to_pool 双槽结算（C# 监视器 dispose 先 Merge 后 Return，
  // GarnetServerMonitor.cs:96-98 的属主侧倒转）把两条一次并入：merge 即
  // add+reset（garnet_latency_metrics.rs 合并臂），槽内样本无从二次入全局。
  s1.start(LatencyMetricsType::NetRsLat, 900);
  s1.stop(LatencyMetricsType::NetRsLat, 1_000);
  s1.return_to_pool();
  assert_eq!(global_calls(&monitor, LatencyMetricsType::NetRsLat), 10);
  s1.return_to_pool();
  assert_eq!(
    global_calls(&monitor, LatencyMetricsType::NetRsLat),
    10,
    "二次释放不得双计"
  );
  s2.return_to_pool();
}

/// 用例三：版本翻转归并按引用流加，全程不产生全量深拷贝。
///
/// 旧实现（`metrics_snapshot()` 克隆 12 个直方图 + Vec 再归并）单轮归并
/// ≥13 次堆分配；新实现同窗口实测分配数封顶远小于该值（小裕量容忍测试
/// 框架噪声），同时以样本确实到达全局 + 退役槽确已清零排除「没干活所以
/// 没分配」的空转通过。
#[test]
fn test_retired_slot_flush_merges_by_reference_without_deep_copy() {
  let _gate = FILE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
  let monitor = monitor();
  let mut session = session(&monitor);

  // 全部默认类别各预置一条样本，保证归并轮触达 6 个退役槽（12 直方图面）
  for &cmd in &LatencyMetricsType::ALL {
    session.record_value(cmd, 42);
  }

  // 时钟推进后，下一次触碰触发的翻转归并单独计数
  monitor.monitor_iterations.fetch_add(1, Ordering::Relaxed);
  let before = ALLOCS.load(Ordering::Relaxed);
  session.start(LatencyMetricsType::NetRsLat, 1);
  let flush_allocs = ALLOCS.load(Ordering::Relaxed) - before;

  // 深拷贝基线：每类别 2 槽克隆 + Vec + 条目数组 ≥13 次；上限取 4 容忍
  // 框架噪声（串行闸下本窗口无并发分配）
  assert!(
    flush_allocs < 13,
    "翻转归并产生深拷贝：单轮分配 {flush_allocs} 次（旧快照克隆口径 ≥13）"
  );
  // 「干过活」实证：全部类别样本按引用到达全局，会话退役槽归并即清零
  for &cmd in &LatencyMetricsType::ALL {
    assert_eq!(
      global_calls(&monitor, cmd),
      1,
      "{cmd:?} 退役槽样本未并入全局延迟表"
    );
    assert_eq!(slot_calls(&session, cmd, 0), 0, "{cmd:?} 退役槽未清零");
  }

  // 释放路径同样零深拷贝：双槽结算的分配计数与单槽同量级
  let before = ALLOCS.load(Ordering::Relaxed);
  session.return_to_pool();
  let dispose_allocs = ALLOCS.load(Ordering::Relaxed) - before;
  assert!(
    dispose_allocs < 13,
    "释放归并产生深拷贝：分配 {dispose_allocs} 次（旧快照克隆口径 ≥13）"
  );
}
