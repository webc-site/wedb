//! Lua 脚本超时管理：集中登记 + 周期 tick 触发中断
//! （对标 libs/server/Lua/LuaTimeoutManager.cs:LuaTimeoutManager）。
//!
//! C# 三段式：专属定时线程（`Start` 循环）→ `AdvanceTimeout` 计数到限 →
//! `RequestTimeout(cookie)` 校验 cookie 后 sethook（每指令 debug hook 抛
//! 超时错误）。Rust 等价映射（复用 state.rs 的 VM safepoint 中断回调）：
//! - 计数到限 + cookie 校验合并为对截止槽的单次 CAS：槽值即 cookie，
//!   run 结束清 0 / 新 run 覆盖后，旧到期判定 CAS 失配自然放弃；
//! - sethook 由 luau interrupt 回调承接（挂上后每个 safepoint 轮询截止，
//!   槽值 0 时空转返回，未配超时的 runner 零开销）；
//! - `Start` 定时循环由宿主 tick 任务驱动（wnode 服务装配期 spawn，
//!   节拍取 [`LuaTimeoutManager::tick_millis`]）。

use std::{
  fmt,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
};

use wbase::{map::ConcurrentMap, time::now_ms};

use crate::state::Deadline;

/// C# TimeoutDivisions = 10：tick 频率 = 超时 / 10（±10% 精度，best effort）。
const TIMEOUT_DIVISIONS: i64 = 10;
/// 最小 tick 频率毫秒（低于 1ms 抖动过大，无意义）。
const MIN_TICK_MILLIS: i64 = 1;

/// 会话超时登记项（C# LuaTimeoutManager.Registration）。
///
/// 每个启用脚本的会话缓存一份；run 开始 arm / 结束 disarm（VM 线程），
/// tick 到期 CAS 激活（tick 线程）。
pub struct TimeoutRegistration {
  /// 登记键（remove 溯源）。
  id: u64,
  /// 当前 run 的截止槽（0 = 空闲；VM safepoint 与 tick 共读）。
  deadline: Arc<Deadline>,
}

impl TimeoutRegistration {
  /// C# Registration.SetCookie（run 开始形态）：登记截止 = now + timeout。
  pub fn arm(&self, now_monotonic_millis: i64, timeout_millis: i64) {
    self.deadline.store(
      now_monotonic_millis + timeout_millis.max(1),
      Ordering::Release,
    );
  }

  /// C# Registration.SetCookie(0)（run 结束形态）：撤销截止。
  pub fn disarm(&self) {
    self.deadline.store(0, Ordering::Release);
  }

  /// 当前截止值（0 = 空闲；诊断/测试面）。
  pub fn deadline(&self) -> i64 {
    self.deadline.load(Ordering::Acquire)
  }

  /// 共享截止槽（runner 换挂 VM 中断用，state.rs hook_shared_deadline）。
  pub fn shared_deadline(&self) -> Arc<Deadline> {
    Arc::clone(&self.deadline)
  }
}

impl fmt::Debug for TimeoutRegistration {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("TimeoutRegistration")
      .field("id", &self.id)
      .field("deadline", &self.deadline.load(Ordering::Relaxed))
      .finish()
  }
}

/// 会话超时管理器（C# LuaTimeoutManager）。
///
/// 进程级共享：服务装配期创建（超时配置存在时），tick 任务周期驱动
/// [`Self::tick`]；会话脚本缓存首装载脚本时登记（C# RegisterForTimeout）。
pub struct LuaTimeoutManager {
  /// 服务级超时（毫秒）。
  timeout_millis: i64,
  /// tick 频率（毫秒）= 超时 / TIMEOUT_DIVISIONS，下限 1ms。
  tick_millis: i64,
  /// 活跃登记（C# Registration[] 槽位数组的并发 map 等价）。
  registrations: ConcurrentMap<u64, Arc<TimeoutRegistration>>,
  /// 登记键分配器（C# cookie 单调递增语义）。
  next_id: AtomicU64,
}

impl fmt::Debug for LuaTimeoutManager {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("LuaTimeoutManager")
      .field("timeout_millis", &self.timeout_millis)
      .field("tick_millis", &self.tick_millis)
      .field("active_count", &self.active_count())
      .finish()
  }
}

impl LuaTimeoutManager {
  /// C# LuaTimeoutManager（构造）。
  ///
  /// `timeout_millis <= 0` 视为未启用（C# Timeout == InfiniteTimeSpan 时
  /// 服务装配期不建管理器），内部以 `max(1)` 防御。
  #[must_use]
  pub fn new(timeout_millis: i64) -> Self {
    let timeout_millis = timeout_millis.max(1);
    Self {
      timeout_millis,
      tick_millis: (timeout_millis / TIMEOUT_DIVISIONS).max(MIN_TICK_MILLIS),
      registrations: ConcurrentMap::default(),
      next_id: AtomicU64::new(0),
    }
  }

  /// 服务级超时（毫秒）。
  #[inline]
  #[must_use]
  pub const fn timeout_millis(&self) -> i64 {
    self.timeout_millis
  }

  /// tick 周期毫秒（宿主定时任务节拍源）。
  #[inline]
  #[must_use]
  pub const fn tick_millis(&self) -> i64 {
    self.tick_millis
  }

  /// C# RegisterForTimeout：登记返回句柄。
  ///
  /// 会话缓存首个脚本装载成功时调用（C# TryLoad 尾部登记——非每会话，
  /// 只为会跑脚本的会话付出登记成本）。
  #[must_use]
  pub fn register(&self) -> Arc<TimeoutRegistration> {
    let id = self.next_id.fetch_add(1, Ordering::Relaxed);
    let registration = Arc::new(TimeoutRegistration {
      id,
      deadline: Arc::new(Deadline::new(0)),
    });
    self
      .registrations
      .pin()
      .insert(id, Arc::clone(&registration));
    registration
  }

  /// C# RemoveRegistration（Registration.Dispose 落点）：注销登记。
  ///
  /// 会话缓存销毁时调用；未登记时为空操作。
  pub fn remove(&self, registration: &TimeoutRegistration) {
    self.registrations.pin().remove(&registration.id);
  }

  /// 当前活跃登记数（诊断/测试面）。
  #[must_use]
  pub fn active_count(&self) -> usize {
    self.registrations.pin().len()
  }

  /// 测试面：活跃登记的截止值列表（0 = 空闲）。
  #[cfg(test)]
  pub(crate) fn active_deadlines(&self) -> Vec<i64> {
    self
      .registrations
      .pin()
      .values()
      .map(|r| r.deadline())
      .collect()
  }

  /// C# TickTimeouts + AdvanceTimeout + RequestTimeout(cookie) 三合一。
  ///
  /// 单次遍历活跃登记：到期 run 以 CAS 把截止压到 now 激活立即中断
  /// （VM 下个 safepoint 抛超时错误）。CAS 失配即 run 已换代（结束清 0
  /// 或新 run 覆盖），放弃——C# cookie 校验的原子化等价。
  pub fn tick(&self) {
    let now = now_ms() as i64;
    for registration in self.registrations.pin().values() {
      let deadline = registration.deadline.load(Ordering::Acquire);
      if deadline == 0 || now < deadline {
        continue;
      }
      // 到期激活：截止压到 now，VM safepoint 即刻中断。
      _ =
        registration
          .deadline
          .compare_exchange(deadline, now, Ordering::AcqRel, Ordering::Acquire);
    }
  }
}

#[cfg(test)]
mod tests {
  use std::{
    sync::{Arc, atomic::Ordering},
    thread,
  };

  use super::{LuaTimeoutManager, TimeoutRegistration};

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

    // 到期：CAS 压到 now 激活中断。
    mgr.tick_at(1_500);
    assert_eq!(reg.deadline(), 1_500);
    mgr.tick_at(1_600);
    assert_eq!(reg.deadline(), 1_600);

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

    // tick 判定 A 过期但 CAS 失配（值已是 B 的 5_500）→ B 不受误伤。
    mgr.tick_at(1_500);
    assert_eq!(reg.deadline(), 5_500);
    assert!(reg.deadline() > 1_500);
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

  /// 受控时钟版 tick（测试注入；生产面走真实 now_ms 的 [`Self::tick`]）。
  impl LuaTimeoutManager {
    fn tick_at(&self, now_monotonic_millis: i64) {
      for registration in self.registrations.pin().values() {
        let deadline = registration.deadline.load(Ordering::Acquire);
        if deadline == 0 || now_monotonic_millis < deadline {
          continue;
        }
        _ = registration.deadline.compare_exchange(
          deadline,
          now_monotonic_millis,
          Ordering::AcqRel,
          Ordering::Acquire,
        );
      }
    }
  }

  // 装箱确认登记项可跨线程共享（tick 任务与 VM 线程分持）。
  #[test]
  fn registration_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TimeoutRegistration>();
    assert_send_sync::<LuaTimeoutManager>();
  }
}
