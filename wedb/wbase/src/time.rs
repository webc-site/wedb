//! 时间戳工具（基于 coarsetime / VDSO）
//!
//! 时钟域口径（对齐 coarsetime 0.1.37 实际行为）：
//! - `Clock::now_since_epoch()`（本模块 `now_secs`/`now_ms`/`now_nanos` 所用）非 wasm
//!   路径即 `time::SystemTime::now()`，走 VDSO 实时域高精度取时，零上下文切换；
//!   TTL 超时判定等语义正确性敏感路径一律用此实时域高精度时。
//! - 粗缓存时钟是 `Clock::recent_since_epoch()` 与 `coarsetime::Instant::now()`
//!   （Linux 走 `CLOCK_MONOTONIC_COARSE`，CONFIG_HZ=100 时粒度约 10ms），
//!   仅用于 wconn 延迟度量与后台 deadline 轮转等容忍粗粒度的域。
//! - 区间计时（延迟直方图/慢日志）取 [`now_stopwatch_ticks`]，源为 `std::time::Instant`
//!   （Linux `CLOCK_MONOTONIC`，VDSO 高精度免 syscall），实时钟被 NTP/手动回拨不影响
//!   其差值；粗粒度档（约 10ms）与微秒级慢日志阈值不同量级，不入本域。
//!
//! 时间戳域与计时域分工：`now_secs`/`now_ms`/`now_nanos`/[`now_ticks`] 是「某一刻」
//! 的实时域读数（可落盘、可跨进程比较）；[`now_stopwatch_ticks`] 是「过了多久」的
//! 单调域读数（只在同进程内作差，绝不当时间戳用，也不与实时域互推）。
//!
//! 粗粒度时钟禁入 TTL 判定路径（粒度误差会直接转化为过期提前/滞后）。

use std::{sync::LazyLock, time::Instant};

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

/// 单调计时域进程锚点（首次取时定标，之后读数即该时刻起的 100ns 刻度数）
///
/// C# `System.Diagnostics.Stopwatch` 的静态构造同样锚在单调计数器原点
/// （QueryPerformanceCounter 自开机起算），故 rust 锚在进程首次取时点与其
/// 只差一个常数偏移；本域全部读数为同进程区间差，偏移不影响语义
static STOPWATCH_ANCHOR: LazyLock<Instant> = LazyLock::new(Instant::now);

/// 获取当前 Stopwatch 刻度（u64，100ns 单调计时域，
/// 对标 C# `System.Diagnostics.Stopwatch.GetTimestamp`）
///
/// 延迟度量/慢日志域的统一计时源：起始与截止刻度同取 [`now_stopwatch_ticks`]，
/// 作差一律 [`u64::saturating_sub`]。读数自进程锚点起单调不减且恒非负，故直方图
/// 的 u64 计量域（`wbase::convert::stopwatch` 刻度因子即该域）无需换算即同域；
/// 实时钟回拨不会使其倒流。时间戳请改用 [`now_ticks`]/[`now_secs`]，两域禁互推
#[inline(always)]
pub fn now_stopwatch_ticks() -> u64 {
  // 基底偏移 +1 保证读数恒 > 0（0 为延迟直方图/会话 entry.start_timestamp 的未起测哨兵值）
  (STOPWATCH_ANCHOR.elapsed().as_nanos() / u128::from(NANOS_PER_TICK)) as u64 + 1
}

/// 获取当前 .NET Ticks（i64，100ns 单位，0001-01-01 纪元，对标 C# `DateTimeOffset.UtcNow.UtcTicks`）
///
/// TTL/过期域的统一时钟：过期时间戳一律以 ticks 存储（对标 Garnet RecordDataHeader
/// 的 expiration ticks 语义），比较基准 `now_ticks()` 与存储值同域
#[inline(always)]
pub fn now_ticks() -> i64 {
  (Clock::now_since_epoch().as_nanos() / 100) as i64 + UNIX_EPOCH_TICKS
}
