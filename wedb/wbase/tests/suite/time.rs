//! 时钟源测试（实时域 / 单调计时域分离，自 tests/main.rs 迁入）

#[test]
fn test_time_primitives() {
  use wbase::time::*;

  let ms = now_ms();
  let nanos = now_nanos();

  assert!(ms > 0);
  assert!(nanos > 0);
  assert!(nanos >= ms * 1_000_000);
}

/// 计时源域纪律回归（对标 C# `System.Diagnostics.Stopwatch.GetTimestamp` 的
/// QueryPerformanceCounter 单调读数）：`now_stopwatch_ticks` 必须落在单调计时域，
/// 不得与 `now_nanos` 的实时域同源。旧实现取 `now_nanos() / NANOS_PER_TICK`，
/// 两域换算后差恒 < 100ns，本断言必红；换单调基后实时域读数领先计时域数十年
/// （与 NTP/手动回拨无关），断言恒绿且无需 sleep 等待
#[test]
fn test_stopwatch_ticks_anchored_on_monotonic_clock() {
  use wbase::time::{NANOS_PER_TICK, now_nanos, now_stopwatch_ticks};

  let ticks = now_stopwatch_ticks();
  let wall_ticks = now_nanos() / NANOS_PER_TICK;
  assert!(
    wall_ticks > ticks + 10_000_000,
    "计时源仍接在实时墙上钟域: wall_ticks={wall_ticks} mono_ticks={ticks}"
  );
  // 同域连续取时单调不减 → 区间作差恒非负，慢日志/直方图无回绕面
  assert!(now_stopwatch_ticks() >= ticks);
}
