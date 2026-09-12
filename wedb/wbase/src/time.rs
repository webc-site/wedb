//! 高吞吐粗粒度时间戳工具（基于 VDSO 零系统调用）
//!
//! 专为存储引擎的 TTL 超时检查、GC 调度轮转与耗时统计设计，
//! 消除 `std::time::Instant` 或常规系统调用的上下文切换开销。

use coarsetime::Clock;
pub use coarsetime::{Duration, Instant};

pub const UNIX_EPOCH_TICKS: i64 = 621_355_968_000_000_000;

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

/// 获取当前 .NET Ticks（i64，100ns 单位，0001-01-01 纪元，对标 C# `DateTimeOffset.UtcNow.UtcTicks`）
///
/// TTL/过期域的统一时钟：过期时间戳一律以 ticks 存储（对标 Garnet RecordDataHeader
/// 的 expiration ticks 语义），比较基准 `now_ticks()` 与存储值同域
#[inline(always)]
pub fn now_ticks() -> i64 {
  (Clock::now_since_epoch().as_nanos() / 100) as i64 + UNIX_EPOCH_TICKS
}
