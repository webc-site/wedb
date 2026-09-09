//! 自适应三阶退避状态机（自旋 → yield 让核 → 微秒级微睡）
//!
//! 专为多核并发与 compio thread-per-core 反应器模型设计：
//! - 第一阶段（< 32 轮）：`spin_loop()` 极短等待，兼顾硬件超线程友好；
//! - 第二阶段（32..1024 轮）：`yield_now()` 主动让出 CPU 时间片，避免单核无谓空转；
//! - 第三阶段（>= 1024 轮）：微秒级休眠（或让渡反应器），彻底避免烧核。

use core::hint::spin_loop;
use std::{
  thread::{sleep, yield_now},
  time::Duration,
};

/// 第一阶段：自旋上限轮数（32 轮）
pub const SPIN_LIMIT: u32 = 32;

/// 第二阶段：让核上限轮数（1024 轮）
pub const YIELD_LIMIT: u32 = 1024;

/// 第三阶段：单次休眠微秒数（50 微秒）
pub const SLEEP_MICROS: u64 = 50;

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

/// 由累计轮数计算所处退避阶段（`snooze` 与自由函数 `backoff` 的单一真源）
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

/// 执行一轮对应阶段的同步等待动作
#[inline]
fn wait_stage(stage: BackoffStage) {
  match stage {
    BackoffStage::Spin => spin_loop(),
    BackoffStage::Yield => yield_now(),
    BackoffStage::Sleep => sleep(SLEEP_DURATION),
  }
}

impl BackoffStage {
  /// 处于 CPU 自旋阶段
  #[inline(always)]
  pub const fn is_spin(&self) -> bool {
    matches!(self, Self::Spin)
  }

  /// 处于时间片让渡阶段
  #[inline(always)]
  pub const fn is_yield(&self) -> bool {
    matches!(self, Self::Yield)
  }

  /// 处于深度等待休眠阶段
  #[inline(always)]
  pub const fn is_sleep(&self) -> bool {
    matches!(self, Self::Sleep)
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

  /// 是否已进入深度休眠阶段
  #[inline(always)]
  pub const fn is_sleep(&self) -> bool {
    self.step >= YIELD_LIMIT
  }

  /// 执行一次同步退避并步进轮数
  #[inline]
  pub fn snooze(&mut self) {
    wait_stage(stage_of(self.step));
    self.step = self.step.saturating_add(1);
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
  wait_stage(stage_of(round));
}
