//! 高吞吐粗粒度时间戳工具（基于 VDSO 零系统调用）
//!
//! 专为存储引擎的 TTL 超时检查、GC 调度轮转与耗时统计设计，
//! 消除 `std::time::Instant` 或常规系统调用的上下文切换开销。

use coarsetime::Clock;
pub use coarsetime::{Duration, Instant};

/// 获取自 UNIX 纪元以来的当前毫秒时间戳（u64）
#[inline(always)]
pub fn now_ms() -> u64 {
  Clock::now_since_epoch().as_millis()
}

/// 获取自 UNIX 纪元以来的当前秒级时间戳（u64）
#[inline(always)]
pub fn now_secs() -> u64 {
  Clock::now_since_epoch().as_secs()
}

/// 获取自 UNIX 纪元以来的当前微秒时间戳（u64）
#[inline(always)]
pub fn now_micros() -> u64 {
  Clock::now_since_epoch().as_micros()
}

/// 获取自 UNIX 纪元以来的当前纳秒时间戳（u64）
#[inline(always)]
pub fn now_nanos() -> u64 {
  Clock::now_since_epoch().as_nanos()
}
