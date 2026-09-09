//! Lua 超时管理：会话脚本执行时限的登记与检查
//! （对标 libs/server/Lua/LuaTimeoutManager.cs:LuaTimeoutManager）。
//!
//! C# 以毫秒 Cookie + 有序 Tick 队列驱动；Rust 以单调毫秒时钟
//! （coarsetime）+ 最小截止期限扫描承接同等语义。

use std::collections::BTreeMap;

/// 会话超时登记句柄（cookie 形态，C# `SetCookie` 的键）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimeoutCookie(pub u64);

/// 会话超时管理器。
#[derive(Default)]
pub struct LuaTimeoutManager {
  /// 活跃登记：cookie → 截止时刻（单调毫秒）。
  registrations: BTreeMap<TimeoutCookie, i64>,
  /// cookie 分配器。
  next_cookie: u64,
}

impl LuaTimeoutManager {
  /// libs/server/Lua/LuaTimeoutManager.cs:SetCookie
  ///
  /// 为登记项更新截止时刻（now_monotonic_millis + timeout_millis）。
  pub fn set_cookie(
    &mut self,
    cookie: TimeoutCookie,
    now_monotonic_millis: i64,
    timeout_millis: i64,
  ) {
    self
      .registrations
      .insert(cookie, now_monotonic_millis + timeout_millis.max(0));
  }

  /// libs/server/Lua/LuaTimeoutManager.cs:AdvanceTimeout
  ///
  /// 将全局时限前移 `millis`（时限 = 当前 + millis）。
  pub fn advance_timeout(&mut self, now_monotonic_millis: i64, millis: i64) {
    let deadline = now_monotonic_millis + millis.max(0);
    for deadline_slot in self.registrations.values_mut() {
      *deadline_slot = deadline;
    }
  }

  /// libs/server/Lua/LuaTimeoutManager.cs:RegisterForTimeout
  ///
  /// 登记会话超时，返回其 cookie。
  pub fn register_for_timeout(
    &mut self,
    now_monotonic_millis: i64,
    timeout_millis: i64,
  ) -> TimeoutCookie {
    self.next_cookie += 1;
    let cookie = TimeoutCookie(self.next_cookie);
    self.set_cookie(cookie, now_monotonic_millis, timeout_millis);
    cookie
  }

  /// libs/server/Lua/LuaTimeoutManager.cs:RemoveRegistration
  pub fn remove_registration(&mut self, cookie: TimeoutCookie) {
    self.registrations.remove(&cookie);
  }

  /// libs/server/Lua/LuaTimeoutManager.cs:TickTimeouts
  ///
  /// 单调时钟推进回调：移除已过期（截止 <= now）登记并返回其 cookie 序列。
  pub fn tick_timeouts(&mut self, now_monotonic_millis: i64) -> Vec<TimeoutCookie> {
    let expired: Vec<TimeoutCookie> = self
      .registrations
      .iter()
      .filter(|(_, deadline)| **deadline <= now_monotonic_millis)
      .map(|(cookie, _)| *cookie)
      .collect();
    for cookie in &expired {
      self.registrations.remove(cookie);
    }
    expired
  }

  /// 当前活跃登记数。
  pub fn active_count(&self) -> usize {
    self.registrations.len()
  }
}

#[cfg(test)]
mod tests {
  use super::{LuaTimeoutManager, TimeoutCookie};

  #[test]
  fn register_tick_expire() {
    let mut mgr = LuaTimeoutManager::default();
    let c1 = mgr.register_for_timeout(1_000, 500);
    let c2 = mgr.register_for_timeout(1_000, 2_000);
    assert_eq!(mgr.active_count(), 2);

    assert!(mgr.tick_timeouts(1_400).is_empty());
    assert_eq!(mgr.tick_timeouts(1_500), vec![c1]);
    assert_eq!(mgr.active_count(), 1);
    assert_eq!(mgr.tick_timeouts(3_000), vec![c2]);
  }

  #[test]
  fn set_cookie_and_advance() {
    let mut mgr = LuaTimeoutManager::default();
    let cookie = TimeoutCookie(7);
    mgr.set_cookie(cookie, 1_000, 1_000);
    // 前移时限到 now+100：1_100 时即到期。
    mgr.advance_timeout(1_000, 100);
    assert_eq!(mgr.tick_timeouts(1_100), vec![cookie]);
    mgr.remove_registration(cookie);
    assert_eq!(mgr.active_count(), 0);
  }
}
