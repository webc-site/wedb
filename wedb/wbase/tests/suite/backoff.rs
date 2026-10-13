//! 指数退避阶段机测试（自 tests/main.rs 迁入）

#[test]
fn test_backoff_stages() {
  use std::time::Duration;

  use wbase::backoff::*;

  let mut b = Backoff::new();
  assert_eq!(b.stage(), BackoffStage::Spin);
  assert!(b.stage().is_spin());

  for _ in 0..SPIN_LIMIT {
    b.advance();
  }
  assert_eq!(b.stage(), BackoffStage::Yield);

  for _ in SPIN_LIMIT..YIELD_LIMIT {
    b.advance();
  }
  assert_eq!(b.stage(), BackoffStage::Sleep);
  assert_eq!(SLEEP_DURATION, Duration::from_micros(50));

  b.reset();
  assert_eq!(b.stage(), BackoffStage::Spin);
  assert_eq!(b.step_count(), 0);
}

/// 阶段动作真源的派发契约：仅 Sleep 阶段触碰注入的定时器，忙等面全程不阻塞线程
#[test]
fn test_backoff_stage_wait_dispatch() {
  use std::{cell::Cell, future::ready, time::Duration};

  use wbase::{backoff::*, future::block_on};

  let timer_hits = Cell::new(0u32);
  let sleeper = |_: Duration| {
    timer_hits.set(timer_hits.get() + 1);
    ready(())
  };

  for stage in [BackoffStage::Spin, BackoffStage::Yield, BackoffStage::Sleep] {
    // 三面各跑一轮：wait 在 Sleep 走 50μs 线程微睡，wait_busy 与异步面皆不阻塞线程
    stage.wait();
    stage.wait_busy();
    block_on(stage.wait_async(&sleeper));
  }
  // Spin / Yield 让核，唯 Sleep 让渡到注入定时器
  assert_eq!(timer_hits.get(), 1);
}
