//! 数据原语换算 (对标 garnet/libs/common/ConvertUtils.cs)
//!
//! UtcNow 系列（[`utc_now_ticks`] / [`seconds_from_diff_utc_now_ticks`] /
//! [`milliseconds_from_diff_utc_now_ticks`]）需启用 `time` 特性：时间源统一
//! coarsetime 委托 [`crate::time::now_ticks`] 单一实现，不提供 `std::time` 回退分支；
//! 纯函数（*_from_ticks / unix_timestamp_in_* / unix_time_in_*）无时钟依赖，恒可用

pub use crate::time::UNIX_EPOCH_TICKS;
#[cfg(feature = "time")]
use crate::time::now_ticks;
pub const TICKS_PER_SECOND: i64 = 10_000_000;
pub const TICKS_PER_MILLISECOND: i64 = 10_000;

/// 获取当前 UTC 时间戳对应 .NET Ticks（对标 C# `DateTimeOffset.UtcNow.Ticks`，
/// coarsetime 零系统调用，委托 [`crate::time::now_ticks`] 单一实现）
#[cfg(feature = "time")]
#[inline]
pub fn utc_now_ticks() -> i64 {
  now_ticks()
}

/// garnet/libs/common/ConvertUtils.cs:SecondsFromDiffUtcNowTicks
#[cfg(feature = "time")]
#[inline]
pub fn seconds_from_diff_utc_now_ticks(ticks: i64) -> i64 {
  seconds_from_diff_ticks(ticks, utc_now_ticks())
}

/// 计算 ticks 与指定基准 ticks 之差并转换为秒（四舍五入进位；非正差值返回 -1）
#[inline]
pub fn seconds_from_diff_ticks(ticks: i64, now_ticks: i64) -> i64 {
  if ticks > 0 {
    let diff = ticks - now_ticks;
    if diff > 0 {
      (diff + TICKS_PER_SECOND / 2) / TICKS_PER_SECOND
    } else {
      -1
    }
  } else {
    -1
  }
}

/// garnet/libs/common/ConvertUtils.cs:MillisecondsFromDiffUtcNowTicks
#[cfg(feature = "time")]
#[inline]
pub fn milliseconds_from_diff_utc_now_ticks(ticks: i64) -> i64 {
  milliseconds_from_diff_ticks(ticks, utc_now_ticks())
}

/// 计算 ticks 与指定基准 ticks 之差并转换为毫秒（非正差值返回 -1）
#[inline]
pub fn milliseconds_from_diff_ticks(ticks: i64, now_ticks: i64) -> i64 {
  if ticks > 0 {
    let diff = ticks - now_ticks;
    if diff > 0 {
      diff / TICKS_PER_MILLISECOND
    } else {
      -1
    }
  } else {
    -1
  }
}

/// garnet/libs/common/ConvertUtils.cs:UnixTimestampInSecondsToTicks
#[inline]
pub fn unix_timestamp_in_seconds_to_ticks(unix_timestamp: i64) -> i64 {
  unix_timestamp * TICKS_PER_SECOND + UNIX_EPOCH_TICKS
}

/// garnet/libs/common/ConvertUtils.cs:UnixTimestampInMillisecondsToTicks
#[inline]
pub fn unix_timestamp_in_milliseconds_to_ticks(unix_timestamp: i64) -> i64 {
  unix_timestamp * TICKS_PER_MILLISECOND + UNIX_EPOCH_TICKS
}

/// garnet/libs/common/ConvertUtils.cs:UnixTimeInSecondsFromTicks
#[inline]
pub fn unix_time_in_seconds_from_ticks(ticks: i64) -> i64 {
  if ticks > 0 {
    (ticks - UNIX_EPOCH_TICKS) / TICKS_PER_SECOND
  } else {
    -1
  }
}

/// garnet/libs/common/ConvertUtils.cs:UnixTimeInMillisecondsFromTicks
#[inline]
pub fn unix_time_in_milliseconds_from_ticks(ticks: i64) -> i64 {
  if ticks > 0 {
    (ticks - UNIX_EPOCH_TICKS) / TICKS_PER_MILLISECOND
  } else {
    -1
  }
}
