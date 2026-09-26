//! Lua 脚本超时管理：集中登记 + 周期 tick 触发中断
//! （对标 libs/server/Lua/LuaTimeoutManager.cs:LuaTimeoutManager）。
//!
//! C# 三段式：专属定时线程（`Start` 循环）→ `AdvanceTimeout` 计数到限 →
//! `RequestTimeout(cookie)` 校验 cookie 后 sethook（每指令 debug hook 抛
//! 超时错误）。Rust 等价映射（复用 state.rs 的 VM safepoint 中断回调）：
//! - 计数到限 + cookie 校验合并为对截止槽的单次 CAS：到期即把截止值原子
//!   压成 [`TIMEOUT_TRIGGERED`] 哨兵，槽值即 cookie，run 结束清 0 /
//!   新 run 覆盖后，旧到期判定 CAS 失配自然放弃；
//! - sethook 由 luau interrupt 回调承接：safepoint 只做一次原子哨兵读
//!   （读数 == [`TIMEOUT_TRIGGERED`] 才抛错），零时钟系统调用，与 C#
//!   「未激活期 VM 原生执行零钩子」的成本形态对齐；
//! - 计时判决权全收口于本模块 tick（单调毫秒，`wbase::time::now_ms_i64`，
//!   对标 C# Environment.TickCount64/Stopwatch 域，免 NTP 阶跃干扰）；
//! - C# `Start` 的专属定时线程内聚于本模块：[`LuaTimeoutManager::start`] 拉
//!   专用 OS 线程（`lua-timeout-watchdog`）按 [`LuaTimeoutManager::tick_millis`]
//!   节拍自驱 [`Self::tick`]，与任何 reactor 协作调度无关——死循环脚本霸占
//!   全部 worker 线程也不饿死看门狗；[`Self::dispose`] 置停机位唤醒线程并
//!   join 收口（C# `Dispose` 的 `cts.Cancel()` + `timerThread.Join()`），
//!   `Drop` 兜底调 `dispose`。

use std::{
  fmt,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  thread::{self, JoinHandle},
  time::Duration,
};

use parking_lot::{Condvar, Mutex};
use wbase::{map::ConcurrentMap, time::now_ms_i64};

use crate::state::Deadline;

/// C# TimeoutDivisions = 10：tick 频率 = 超时 / 10（±10% 精度，best effort）。
const TIMEOUT_DIVISIONS: i64 = 10;
/// 最小 tick 频率毫秒（低于 1ms 抖动过大，无意义）。
const MIN_TICK_MILLIS: i64 = 1;

/// 中断激活哨兵：tick 到期 CAS 成功后写入截止槽的唯一值。
///
/// 截止槽值域三态——`0` = 空闲（未设截止/已 disarm），`>0` = 已设截止
/// （单调毫秒），本值 = 到期已激活（只等 VM 下个 safepoint 抛超时错误，
/// 见 state.rs interrupt_trampoline）。对标 C# AdvanceTimeout 计数到限 +
/// SessionScriptCache.RequestTimeout cookie 比对命中后 sethook 的激活形态：
/// 激活语义单点收敛于此值，VM safepoint 零时钟轮询。
pub const TIMEOUT_TRIGGERED: i64 = -1;

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
  /// libs/server/Lua/LuaTimeoutManager.cs:SetCookie（run 开始形态）：
  /// 登记截止 = now + timeout。
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
/// 进程级共享：服务装配期创建并 [`Self::start`]（超时配置存在时），专用
/// 看门狗线程周期驱动 [`Self::tick`]；会话脚本缓存首装载脚本时登记
/// （C# RegisterForTimeout）。
pub struct LuaTimeoutManager {
  /// 服务级超时（毫秒）。
  timeout_millis: i64,
  /// tick 频率（毫秒）= 超时 / TIMEOUT_DIVISIONS，下限 1ms。
  tick_millis: i64,
  /// 活跃登记（C# Registration[] 槽位数组的并发 map 等价）。
  /// Arc 装箱：看门狗线程与管理器共持同一登记视图（papaya 的 Clone 是深拷贝
  /// 重建，不能直接 clone 进线程闭包）。
  registrations: Arc<ConcurrentMap<u64, Arc<TimeoutRegistration>>>,
  /// 登记键分配器（C# cookie 单调递增语义）。
  next_id: AtomicU64,
  /// 停机协调（C# timerThreadCts）：线程等待谓词 + dispose 唤醒退出。
  /// 单向棘轮——dispose 后 start 不复活（每装配期一管理器，无重启形态）。
  shutdown: Arc<(Mutex<bool>, Condvar)>,
  /// 专属看门狗线程句柄（C# timerThread）：None = 未启动 / 已收口。
  watchdog: Mutex<Option<JoinHandle<()>>>,
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
      registrations: Arc::new(ConcurrentMap::default()),
      next_id: AtomicU64::new(0),
      shutdown: Arc::new((Mutex::new(false), Condvar::new())),
      watchdog: Mutex::new(None),
    }
  }

  /// 服务级超时（毫秒）。
  #[inline]
  #[must_use]
  pub const fn timeout_millis(&self) -> i64 {
    self.timeout_millis
  }

  /// tick 周期毫秒（专属看门狗线程节拍源）。
  #[inline]
  #[must_use]
  pub const fn tick_millis(&self) -> i64 {
    self.tick_millis
  }

  /// libs/server/Lua/LuaTimeoutManager.cs:RegisterForTimeout：登记返回句柄。
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

  /// libs/server/Lua/LuaTimeoutManager.cs:RemoveRegistration：注销登记。
  ///
  /// libs/server/Lua/LuaTimeoutManager.cs 的 Dispose 合并承接：
  /// Registration.Dispose 在 C# 即纯转发 `owner.RemoveRegistration(this)`；
  /// 专属看门狗线程的停机收口由 [`Self::dispose`]（C# timerThreadCts.Cancel
  /// + timerThread.Join）承接，会话级注销单点即本方法。
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

  /// libs/server/Lua/LuaTimeoutManager.cs:Start
  ///
  /// 拉起专属看门狗线程（C# `new Thread{ Name = "LuaTimeoutManager",
  /// IsBackground = true }` 的等价：内核抢占式调度的独立 OS 线程，绝不经
  /// 任何 reactor/线程池——死循环脚本霸占全部 worker 也不饿死 tick）。
  /// 线程循环即 C# `while(!token.IsCancellationRequested){ if
  /// (WaitHandle.WaitOne(frequency)) return; TickTimeouts(); }`：Condvar 定时
  /// 等待 tick_millis 唤醒一轮 [`Self::tick`] 形态的到期推进，dispose 置位
  /// notify 即醒退出。幂等：已启动即空操作。
  pub fn start(&self) {
    let mut slot = self.watchdog.lock();
    if slot.is_some() {
      return;
    }
    // 线程闭包零自引用：只捕获登记视图 Arc 与停机协调（管理器销毁须先
    // join 收口，杜绝悬垂借用）。
    let registrations = Arc::clone(&self.registrations);
    let shutdown = Arc::clone(&self.shutdown);
    let period = Duration::from_millis(self.tick_millis as u64);
    *slot = Some(
      thread::Builder::new()
        .name("lua-timeout-watchdog".to_string())
        .spawn(move || {
          let (lock, cvar) = &*shutdown;
          let mut stopped = lock.lock();
          loop {
            // 谓词等待 = C# WaitOne(frequency)：超时到 = tick 节拍；
            // 被 notify 唤醒且停机位置位 = 收口退出。
            if cvar
              .wait_while_for(&mut stopped, |s| !*s, period)
              .timed_out()
            {
              advance_registrations(&registrations);
            } else {
              return;
            }
          }
        })
        .expect("Lua 超时专属看门狗线程拉起失败"),
    );
  }

  /// libs/server/Lua/LuaTimeoutManager.cs:Dispose
  ///
  /// 停机收口：置停机位 + notify 唤醒线程立即退出，join 等待收口返回。
  /// 幂等——句柄 take 后二次调用为空操作；[`Self::start`] 未启动时亦空操作。
  pub fn dispose(&self) {
    {
      let (lock, cvar) = &*self.shutdown;
      *lock.lock() = true;
      cvar.notify_all();
    }
    let handle = self.watchdog.lock().take();
    if let Some(handle) = handle {
      // 看门狗线程体无 panic 面（tick 纯原子推进），join 结果无信息量
      let _ = handle.join();
    }
  }

  /// libs/server/Lua/LuaTimeoutManager.cs:TickTimeouts
  /// libs/server/Lua/LuaTimeoutManager.cs:AdvanceTimeout
  /// libs/server/Lua/SessionScriptCache.cs:RequestTimeout
  ///
  /// 单次遍历活跃登记，单调时钟（[`now_ms_i64`]）即超时判决的唯一时钟源：
  /// 到期 run 以 CAS 把截止压成 [`TIMEOUT_TRIGGERED`] 哨兵激活中断（VM 下个
  /// safepoint 抛超时错误，见 state.rs interrupt_trampoline）。CAS 失配即
  /// run 已换代（结束清 0 或新 run 覆盖），放弃——C# AdvanceTimeout 计数
  /// 到限 + SessionScriptCache.RequestTimeout 的 cookie 比对合并进单次 CAS
  /// （槽值即 cookie）。
  pub fn tick(&self) {
    advance_registrations(&self.registrations);
  }
}

/// [`LuaTimeoutManager::tick`] 的登记视图臂：看门狗线程闭包与管理器共用的
/// 唯一推进实现（一处定义，线程侧不经管理器借用）。
fn advance_registrations(registrations: &ConcurrentMap<u64, Arc<TimeoutRegistration>>) {
  let now = now_ms_i64();
  for registration in registrations.pin().values() {
    let deadline = registration.deadline.load(Ordering::Acquire);
    if deadline <= 0 || now < deadline {
      continue;
    }
    // 到期激活：截止压成哨兵，VM safepoint 即刻中断。
    _ = registration.deadline.compare_exchange(
      deadline,
      TIMEOUT_TRIGGERED,
      Ordering::AcqRel,
      Ordering::Acquire,
    );
  }
}

/// C# using/最终izer 兜底：管理器随最后一个句柄释放时收口专属线程
/// （dispose 幂等，停机链显式调用与 Drop 并存安全）。
impl Drop for LuaTimeoutManager {
  fn drop(&mut self) {
    self.dispose();
  }
}

#[cfg(test)]
mod tests {
  use std::{
    sync::{Arc, atomic::Ordering},
    thread,
  };

  use super::{LuaTimeoutManager, TIMEOUT_TRIGGERED, TimeoutRegistration};

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

  /// 受控时钟版 tick（测试注入；生产面走真实单调时钟的 [`Self::tick`]）。
  impl LuaTimeoutManager {
    fn tick_at(&self, now_monotonic_millis: i64) {
      for registration in self.registrations.pin().values() {
        let deadline = registration.deadline.load(Ordering::Acquire);
        if deadline <= 0 || now_monotonic_millis < deadline {
          continue;
        }
        _ = registration.deadline.compare_exchange(
          deadline,
          TIMEOUT_TRIGGERED,
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
