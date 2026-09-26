//! 自适应三阶退避状态机（自旋 → yield 让核 → 微秒级微睡）
//!
//! 专为多核并发与 compio thread-per-core 反应器模型设计：
//! - 第一阶段（< 32 轮）：`spin_loop()` 极短等待，兼顾硬件超线程友好；
//! - 第二阶段（32..1024 轮）：`yield_now()` 主动让出 CPU 时间片，避免单核无谓空转；
//! - 第三阶段（>= 1024 轮）：微秒级休眠（或让渡反应器），彻底避免烧核。
//!
//! 本模块是退避阶段机的单一真源：阈值（`stage_of`）与阶段动作
//! （[`BackoffStage::wait`] 同步 / [`BackoffStage::wait_async`] 异步 /
//! [`BackoffStage::wait_busy`] 忙等）均只在此处定义，调用方仅推进计数器。
//! 对标 C# AllocatorBase.cs:WaitToRetryNow——等待内核一处定义，调用点只递增 spins。

use core::{future::Future, hint::spin_loop};
use std::{
  thread::{sleep, yield_now as thread_yield_now},
  time::Duration,
};

use crate::future::yield_now;

/// 第一阶段：自旋上限轮数（32 轮）
pub const SPIN_LIMIT: u32 = 32;

/// 第二阶段：让核上限轮数（1024 轮）
pub const YIELD_LIMIT: u32 = 1024;

/// 第三阶段：单次休眠微秒数（50 微秒）
const SLEEP_MICROS: u64 = 50;

/// 第三阶段：休眠时长常量（编译期折叠，避免运行时重复构建）
pub const SLEEP_DURATION: Duration = Duration::from_micros(SLEEP_MICROS);

/// 退避阶段枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BackoffStage {
  /// CPU 指令级纯自旋
  Spin,
  /// 操作系统级让渡时间片
  Yield,
  /// 深度等待睡眠（同步线程 sleep 或异步 reactor 让渡）
  Sleep,
}

/// 由累计轮数计算所处退避阶段（阶段阈值的单一真源，动作见 [`BackoffStage::wait`] 族）
#[inline(always)]
const fn stage_of(step: u32) -> BackoffStage {
  if step < SPIN_LIMIT {
    BackoffStage::Spin
  } else if step < YIELD_LIMIT {
    BackoffStage::Yield
  } else {
    BackoffStage::Sleep
  }
}

impl BackoffStage {
  /// 处于 CPU 自旋阶段
  #[inline(always)]
  pub const fn is_spin(&self) -> bool {
    matches!(self, Self::Spin)
  }

  /// 同步阶段动作唯一真源：自旋 → 让核 → 微秒级线程微睡
  #[inline]
  pub fn wait(&self) {
    match self {
      Self::Spin => spin_loop(),
      Self::Yield => thread_yield_now(),
      Self::Sleep => sleep(SLEEP_DURATION),
    }
  }

  /// 忙等阶段动作：Sleep 深睡钳制为让核，全程绝不阻塞线程
  ///
  /// 供等待纯内存事件（CAS/槽位争用、在途头编码）的阶段机使用——此类事件的
  /// 就绪与线程睡眠无关，睡下去只会白白拖长延迟（compio thread-per-core 下
  /// 更会饿死同线程的就绪任务）。
  #[inline]
  pub fn wait_busy(&self) {
    match self {
      Self::Sleep => thread_yield_now(),
      stage => stage.wait(),
    }
  }

  /// 异步阶段动作唯一真源：Spin 保持 CPU 自旋，Yield 协作让出协程，Sleep 阶段改为非阻塞睡眠
  ///
  /// 反应器线程上禁用 [`wait`] 的线程微睡（会挂起同线程全部任务），故第三阶段
  /// await 注入的定时器；`sleeper` 由调用方按运行时提供（如 `compio::time::sleep`），
  /// wbase 自身与具体运行时保持解耦。
  #[inline]
  pub async fn wait_async<S: Future<Output = ()>>(&self, sleeper: impl FnOnce(Duration) -> S) {
    match self {
      Self::Sleep => sleeper(SLEEP_DURATION).await,
      Self::Yield => yield_now().await,
      stage => stage.wait(),
    }
  }
}

/// 自适应退避步进控制器
#[derive(Debug, Default, Clone, Copy)]
pub struct Backoff {
  step: u32,
}

impl Backoff {
  #[inline(always)]
  pub const fn new() -> Self {
    Self { step: 0 }
  }

  /// 获取当前已累计退避轮数
  #[inline(always)]
  pub const fn step_count(&self) -> u32 {
    self.step
  }

  /// 计算当前所处的退避阶段
  #[inline(always)]
  pub const fn stage(&self) -> BackoffStage {
    stage_of(self.step)
  }

  /// 执行一次同步退避并步进轮数
  #[inline]
  pub fn snooze(&mut self) {
    self.stage().wait();
    self.advance();
  }

  /// 仅步进计数器（适用于异步自定义等待场景）
  #[inline(always)]
  pub fn advance(&mut self) {
    self.step = self.step.saturating_add(1);
  }

  /// 重置退避状态机至初始自旋态
  #[inline(always)]
  pub fn reset(&mut self) {
    self.step = 0;
  }
}

/// 无状态阶梯退避辅助函数（同步模式，按轮数执行对应阶段动作）
#[inline]
pub fn backoff(round: u32) {
  stage_of(round).wait();
}
