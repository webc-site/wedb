//! 高吞吐粗粒度时间戳工具（基于 VDSO 零系统调用）
//!
//! 专为存储引擎的 TTL 超时检查、GC 调度轮转与耗时统计设计，
//! 消除 `std::time::Instant` 或常规系统调用的上下文切换开销。

use coarsetime::Clock;

pub const UNIX_EPOCH_TICKS: i64 = 621_355_968_000_000_000;

/// 获取自 UNIX 纪元以来的当前秒时间戳（u64）
#[inline(always)]
pub fn now_secs() -> u64 {
  Clock::now_since_epoch().as_secs()
}

/// 获取自 UNIX 纪元以来的当前毫秒时间戳（u64）
#[inline(always)]
pub fn now_ms() -> u64 {
  Clock::now_since_epoch().as_millis()
}

/// 获取自 UNIX 纪元以来的当前纳秒时间戳（u64）
#[inline(always)]
pub fn now_nanos() -> u64 {
  Clock::now_since_epoch().as_nanos()
}

/// Stopwatch tick 换算率（100ns/tick；.NET Core `Stopwatch.Frequency` 恒 10 MHz）
pub const NANOS_PER_TICK: u64 = 100;

/// 获取当前 Stopwatch 刻度（i64，100ns 单调域，对标 C# `System.Diagnostics.Stopwatch.GetTimestamp`）
///
/// 延迟度量/慢日志域的统一计时源：起始与截止刻度同取 [`now_stopwatch_ticks`]
#[inline(always)]
pub fn now_stopwatch_ticks() -> i64 {
  (now_nanos() / NANOS_PER_TICK).min(i64::MAX as u64) as i64
}

/// 获取当前 .NET Ticks（i64，100ns 单位，0001-01-01 纪元，对标 C# `DateTimeOffset.UtcNow.UtcTicks`）
///
/// TTL/过期域的统一时钟：过期时间戳一律以 ticks 存储（对标 Garnet RecordDataHeader
/// 的 expiration ticks 语义），比较基准 `now_ticks()` 与存储值同域
#[inline(always)]
pub fn now_ticks() -> i64 {
  (Clock::now_since_epoch().as_nanos() / 100) as i64 + UNIX_EPOCH_TICKS
}

/// 当前单调时刻（coarsetime VDSO 零系统调用；C# 侧差值计时域
/// `DateTime.UtcNow` 差值的 coarsetime 单调对标，超时判定/剩余毫秒
/// 换算统一经此取点，不直连 coarsetime）
#[inline(always)]
pub fn now_instant() -> Instant {
  Instant::now()
}

pub use coarsetime::{Duration as InstantDuration, Instant};
