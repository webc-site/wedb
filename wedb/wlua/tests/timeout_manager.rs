#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! Lua 超时看门狗管理器与登记项集成测试
//! 对标 libs/server/Lua/LuaTimeoutManager.cs

use std::{sync::Arc, thread};

use wlua::{LuaTimeoutManager, TIMEOUT_TRIGGERED};

#[test]
fn register_arm_tick_disarm() {
  let mgr = LuaTimeoutManager::new(500);
  assert_eq!(mgr.tick_millis(), 50);
  assert_eq!(mgr.timeout_millis(), 500);
  assert_eq!(mgr.active_count(), 0);

  let reg = mgr.register();
  assert_eq!(mgr.active_count(), 1);
  assert_eq!(reg.deadline(), 0);

  // 空闲（0）与未到期不触发。
  reg.arm(1_000, 500);
  mgr.tick_at(1_400);
  assert_eq!(reg.deadline(), 1_500);

  // 到期：CAS 压成哨兵激活中断。
  mgr.tick_at(1_500);
  assert_eq!(reg.deadline(), TIMEOUT_TRIGGERED);

  // 已激活（哨兵在槽）不重复激活，等 VM safepoint 抛错后由 disarm 清。
  mgr.tick_at(1_600);
  assert_eq!(reg.deadline(), TIMEOUT_TRIGGERED);

  // 撤销后不再触发。
  reg.disarm();
  mgr.tick_at(9_999);
  assert_eq!(reg.deadline(), 0);

  mgr.remove(&reg);
  assert_eq!(mgr.active_count(), 0);
}

#[test]
fn tick_divisions_floor() {
  // 超时小于 10ms 时 tick 频率保底 1ms（C# frequency < 1ms 钳制）。
  let mgr = LuaTimeoutManager::new(5);
  assert_eq!(mgr.tick_millis(), 1);
}

#[test]
fn cross_thread_activation_race() {
  // tick 线程与 VM 线程的换代竞态：旧到期值被新 run 覆盖后 CAS 失配放弃。
  let mgr = LuaTimeoutManager::new(100);
  let reg = mgr.register();

  // run A：截止 1_000（已过期）。
  reg.arm(500, 500);
  // run A 已结束、run B 已覆盖新截止。
  reg.arm(5_000, 500);

  // tick 持 A 的陈旧到期值判定，CAS 失配（值已是 B 的 5_500）→ B 不受误伤。
  mgr.tick_at(1_500);
  assert_eq!(reg.deadline(), 5_500);
  assert!(reg.deadline() > 1_500);
}

#[test]
fn stale_tick_after_disarm_spares_next_run() {
  // disarm 后旧 tick 的到期判定落空（0 非正不激活），后续 run 全新截止
  // 不被残留哨兵/旧判定误伤。
  let mgr = LuaTimeoutManager::new(100);
  let reg = mgr.register();

  // run A 截止已过期 → 正常结束 disarm 清 0。
  reg.arm(500, 100);
  reg.disarm();
  mgr.tick_at(10_000);
  assert_eq!(reg.deadline(), 0);

  // 后续 run B：全新截止，未到期放行、到期精准激活。
  reg.arm(10_000, 500);
  mgr.tick_at(10_400);
  assert_eq!(reg.deadline(), 10_500);
  mgr.tick_at(10_500);
  assert_eq!(reg.deadline(), TIMEOUT_TRIGGERED);
}

#[test]
fn concurrent_tick_while_arm() {
  // tick 线程与 arm/disarm 并发不崩不死锁（值语义最终一致）。
  let mgr = Arc::new(LuaTimeoutManager::new(10));
  let reg = mgr.register();
  let ticker = thread::spawn({
    let mgr = Arc::clone(&mgr);
    move || {
      for _ in 0..2_000 {
        mgr.tick();
      }
    }
  });
  for i in 0..2_000 {
    reg.arm(i, 10);
    reg.disarm();
  }
  ticker.join().unwrap();
  reg.disarm();
  assert_eq!(reg.deadline(), 0);
}
