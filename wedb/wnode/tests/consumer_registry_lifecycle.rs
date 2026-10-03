//! 活跃消费者注册表 kill 语义 / 容量门 / 停机排空生命周期集成测
//!
//! 自 `wnode/src/servers/consumer_registry.rs` 内联测模块迁入（断言与覆盖
//! 原样保留）：dispose 排水与 kill 生命周期为 compio 多任务真调度形态，
//! 在途计数观测经 #[doc(hidden)] 测试专用口 `active_handler_count()`，
//! 终止态观测与等待经同口 `is_terminating`/`wait_terminate`（见各自定义处注）。

use std::{sync::Arc, thread};

use compio::runtime::spawn;
use wnode::servers::ConsumerRegistry;

/// 首杀即真、重复杀假（C# TryKill 语义），注销广播终止态
#[test]
fn kill_is_first_shot_only() {
  let registry = ConsumerRegistry::new();
  let entry = registry.register(7, "127.0.0.1:7001".into(), String::new());
  assert!(!entry.is_terminating());
  assert!(entry.kill_session());
  assert!(!entry.kill_session());
  assert!(entry.is_terminating());

  registry.unregister(7);
  assert!(entry.is_terminating());
}

/// 终止等待防竞态臂：已终止条目直接返回（监听注册晚于广播也不挂死）
#[test]
fn wait_terminate_returns_when_already_terminated() {
  use compio::runtime::Runtime;

  let registry = ConsumerRegistry::new();
  let entry = registry.register(12, "127.0.0.1:7012".into(), String::new());
  let rt = Runtime::new().unwrap();
  entry.kill_session();
  rt.block_on(entry.wait_terminate());
  registry.unregister(12);
  rt.block_on(entry.wait_terminate());
}

/// 在途守卫容量门（C# GarnetServerTcp.cs:236-241/302-307 语义）：
/// limit=2 时第三条拒绝，释放后可再进；Drop 配对归零
#[test]
fn connection_guard_enforces_limit() {
  let registry = Arc::new(ConsumerRegistry::new());
  let g1 = registry.try_acquire_connection(2).unwrap();
  let g2 = registry.try_acquire_connection(2).unwrap();
  assert_eq!(registry.active_handler_count(), 2);
  // 超限拒绝：计数即刻回退，不留在途泄漏
  assert!(registry.try_acquire_connection(2).is_none());
  assert_eq!(registry.active_handler_count(), 2);

  drop(g1);
  assert_eq!(registry.active_handler_count(), 1);
  // 断言产生的临时守卫语句结束即释放，计数回到 1
  assert!(registry.try_acquire_connection(2).is_some());
  drop(g2);
  // 全部释放归零（无漂移），额度可复用
  assert_eq!(registry.active_handler_count(), 0);
}

/// limit=-1 不限（与现状逐字节一致：恒放行）
#[test]
fn connection_guard_unlimited_when_minus_one() {
  let registry = Arc::new(ConsumerRegistry::new());
  let guards: Vec<_> = (0..64)
    .map(|_| registry.try_acquire_connection(-1).unwrap())
    .collect();
  assert_eq!(registry.active_handler_count(), 64);
  drop(guards);
  assert_eq!(registry.active_handler_count(), 0);
}

/// 并发 acquire/release 下计数不漂（fetch_add 与 Guard Drop 配对原子）
#[test]
fn connection_guard_concurrent_no_drift() {
  let registry = Arc::new(ConsumerRegistry::new());
  let handles: Vec<_> = (0..8)
    .map(|_| {
      let reg = Arc::clone(&registry);
      thread::spawn(move || {
        let held: Vec<_> = (0..50).map(|_| reg.try_acquire_connection(-1)).collect();
        drop(held);
      })
    })
    .collect();
  for t in handles {
    t.join().unwrap();
  }
  assert_eq!(registry.active_handler_count(), 0);
}

/// 停机排空：全量下杀令后等待注销归零（C# DisposeActiveHandlers 语义），
/// 不依赖 5 秒超时护栏——模拟泵在收到 kill 广播后注销即快速返回
#[test]
fn dispose_active_handlers_drains_after_kill() {
  use compio::runtime::Runtime;

  let registry = Arc::new(ConsumerRegistry::new());
  registry.note_connection_received();
  let entry = registry.register(11, "127.0.0.1:7011".into(), String::new());
  let rt = Runtime::new().unwrap();
  let reg = Arc::clone(&registry);
  rt.block_on(async move {
    // 模拟连接泵（kill.rs 哨兵路径）：终止广播命中后走 dispose 注销
    let pump_entry = Arc::clone(&entry);
    let pump_reg = Arc::clone(&reg);
    spawn(async move {
      pump_entry.wait_terminate().await;
      pump_reg.unregister(pump_entry.id);
    })
    .detach();
    reg.dispose_active_handlers().await;
  });
  assert_eq!(registry.connection_totals(), (1, 1, 0));
  assert!(registry.get(11).is_none());
}

/// 排空判据双条件：entries 空但 active_handler_count 未归零（容量门计量
/// 中/注册间隙的在途连接）不得提前返回，count 归零方收敛（C#
/// DisposeActiveHandlers 以 activeHandlerCount 轮询为判据的对偶）
#[test]
fn dispose_waits_for_in_flight_count_drain() {
  use std::time::Duration;

  use compio::{runtime::Runtime, time::sleep};

  let registry = Arc::new(ConsumerRegistry::new());
  let rt = Runtime::new().unwrap();
  let reg = Arc::clone(&registry);
  rt.block_on(async move {
    // 先取在途守卫（未注册条目）使 count==1 确定性在册，再启动 dispose——
    // 旧形延后 acquire 靠竞速覆盖：dispose 先行收敛则守卫从未与排水窗重叠
    let guard = Arc::clone(&reg).try_acquire_connection(-1);
    assert!(guard.is_some(), "在途守卫必须可取（装配前提）");
    assert_eq!(reg.active_handler_count(), 1, "守卫在册即 count==1");
    // 模拟 accept 与 register 间隙的在途连接收场：短延后释放守卫
    spawn(async move {
      sleep(Duration::from_millis(80)).await;
      drop(guard);
    })
    .detach();
    reg.dispose_active_handlers().await;
    // 返回即 count 已归零（若误以 entries 空为唯一判据则提前返回，
    // 此刻 count 仍为 1）
    assert_eq!(reg.active_handler_count(), 0);
  });
}
