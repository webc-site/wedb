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

/// 可表示绝对过期 .NET Ticks 的最大 Unix 秒
/// （(i64::MAX - UNIX_EPOCH_TICKS) / TICKS_PER_SECOND，编译期常量）
pub const MAX_UNIX_TIMESTAMP_SECONDS: i64 = (i64::MAX - UNIX_EPOCH_TICKS) / TICKS_PER_SECOND;

/// 可表示绝对过期 .NET Ticks 的最大 Unix 毫秒
/// （(i64::MAX - UNIX_EPOCH_TICKS) / TICKS_PER_MILLISECOND，编译期常量）
pub const MAX_UNIX_TIMESTAMP_MILLISECONDS: i64 =
  (i64::MAX - UNIX_EPOCH_TICKS) / TICKS_PER_MILLISECOND;

/// 相对秒 → 时长 ticks（饱和乘法：乘法溢出饱和至 i64::MAX，杜绝 debug 构建
/// 溢出 panic，C# unchecked 环绕对应的确定性降级）
///
/// EXPIRE/SET EX/SETEX 重放域（相对时长 ticks）的统一换算单点：
/// libs/server/Resp/KeyAdminCommands.cs:423 `AddSeconds` 的乘法前半段
#[inline]
pub fn duration_seconds_to_ticks(seconds: i64) -> i64 {
  seconds.saturating_mul(TICKS_PER_SECOND)
}

/// 相对毫秒 → 时长 ticks（饱和乘法，同 [`duration_seconds_to_ticks`]）
///
/// PEXPIRE/PSETEX 重放域的统一换算单点：
/// libs/server/Resp/KeyAdminCommands.cs:424 `AddMilliseconds` 的乘法前半段
#[inline]
pub fn duration_milliseconds_to_ticks(milliseconds: i64) -> i64 {
  milliseconds.saturating_mul(TICKS_PER_MILLISECOND)
}

/// 相对秒 → 绝对过期 .NET Ticks：now_ticks + 饱和(秒 * TicksPerSecond)
///
/// EXPIRE 命令端与 RESTORE(EX) 的换算单点：
/// libs/server/Resp/KeyAdminCommands.cs:423 `DateTimeOffset.UtcNow.AddSeconds(expiration).UtcTicks`
#[inline]
pub fn expire_after_to_ticks(now_ticks: i64, seconds: i64) -> i64 {
  now_ticks.saturating_add(duration_seconds_to_ticks(seconds))
}

/// 相对毫秒 → 绝对过期 .NET Ticks：now_ticks + 饱和(毫秒 * TicksPerMillisecond)
///
/// PEXPIRE 命令端的换算单点：
/// libs/server/Resp/KeyAdminCommands.cs:424 `AddMilliseconds(expiration).UtcTicks`
#[inline]
pub fn expire_after_ms_to_ticks(now_ticks: i64, milliseconds: i64) -> i64 {
  now_ticks.saturating_add(duration_milliseconds_to_ticks(milliseconds))
}

/// 绝对 Unix 秒 → 绝对过期 .NET Ticks（负值夹 0 = Unix 纪元，超
/// [`MAX_UNIX_TIMESTAMP_SECONDS`] 钳到最大可表示 ticks）
///
/// EXPIREAT 命令端与重放端的换算单点：
/// libs/server/Resp/KeyAdminCommands.cs:425（rust 在 C# 之上补确定性钳制）
#[inline]
pub fn expire_at_seconds_to_ticks(unix_seconds: i64) -> i64 {
  unix_timestamp_in_seconds_to_ticks(unix_seconds.clamp(0, MAX_UNIX_TIMESTAMP_SECONDS))
}

/// 绝对 Unix 毫秒 → 绝对过期 .NET Ticks（负值夹 0，超
/// [`MAX_UNIX_TIMESTAMP_MILLISECONDS`] 钳到最大可表示 ticks）
///
/// PEXPIREAT 命令端与重放端的换算单点：
/// libs/server/Resp/KeyAdminCommands.cs:426（rust 在 C# 之上补确定性钳制）
#[inline]
pub fn expire_at_milliseconds_to_ticks(unix_milliseconds: i64) -> i64 {
  unix_timestamp_in_milliseconds_to_ticks(unix_milliseconds.clamp(0, MAX_UNIX_TIMESTAMP_MILLISECONDS))
}

#[cfg(test)]
mod tests {
  use super::*;

  /// 相对时长饱和：乘法上/下界均饱和，无 panic
  #[test]
  fn duration_saturates() {
    assert_eq!(duration_seconds_to_ticks(i64::MAX), i64::MAX);
    assert_eq!(duration_seconds_to_ticks(i64::MIN), i64::MIN);
    assert_eq!(duration_milliseconds_to_ticks(i64::MAX), i64::MAX);
    assert_eq!(duration_milliseconds_to_ticks(i64::MIN), i64::MIN);
    assert_eq!(duration_seconds_to_ticks(1), TICKS_PER_SECOND);
    assert_eq!(duration_milliseconds_to_ticks(1), TICKS_PER_MILLISECOND);
  }

  /// 相对 → 绝对：now + 饱和时长，饱和加法不 panic
  #[test]
  fn expire_after_saturates() {
    let now = 70_000_000_000_000_000;
    assert_eq!(expire_after_to_ticks(now, 10), now + 10 * TICKS_PER_SECOND);
    assert_eq!(expire_after_ms_to_ticks(now, 10), now + 10 * TICKS_PER_MILLISECOND);
    // 时长饱和后加法继续饱和：结果钉在 i64::MAX
    assert_eq!(expire_after_to_ticks(now, i64::MAX), i64::MAX);
    assert_eq!(expire_after_ms_to_ticks(now, i64::MAX), i64::MAX);
    // 与时长单点的逐位等价（命令端 EXPIRE 与重放端 Setex 同公式源）；
    // 非饱和路径下 绝对 - now == 饱和时长
    for seconds in [0, 1, 60, 3_600, 86_400] {
      assert_eq!(
        expire_after_to_ticks(now, seconds) - now,
        duration_seconds_to_ticks(seconds)
      );
      assert_eq!(
        expire_after_ms_to_ticks(now, seconds) - now,
        duration_milliseconds_to_ticks(seconds)
      );
    }
  }

  /// 绝对 Unix 秒/毫秒钳制：负值夹 0（= Unix 纪元），超界钳到最大可表示 ticks
  #[test]
  fn expire_at_clamps() {
    assert_eq!(expire_at_seconds_to_ticks(-5), UNIX_EPOCH_TICKS);
    assert_eq!(expire_at_seconds_to_ticks(0), UNIX_EPOCH_TICKS);
    assert_eq!(
      expire_at_seconds_to_ticks(100),
      unix_timestamp_in_seconds_to_ticks(100)
    );
    // cap 换算整除截断：钳制结果为 i64::MAX 去掉截断余数，不溢出
    assert_eq!(
      expire_at_seconds_to_ticks(MAX_UNIX_TIMESTAMP_SECONDS),
      i64::MAX - (i64::MAX - UNIX_EPOCH_TICKS) % TICKS_PER_SECOND
    );
    assert_eq!(expire_at_seconds_to_ticks(i64::MAX), i64::MAX - (i64::MAX - UNIX_EPOCH_TICKS) % TICKS_PER_SECOND);
    assert_eq!(expire_at_milliseconds_to_ticks(-1), UNIX_EPOCH_TICKS);
    assert_eq!(
      expire_at_milliseconds_to_ticks(MAX_UNIX_TIMESTAMP_MILLISECONDS),
      i64::MAX - (i64::MAX - UNIX_EPOCH_TICKS) % TICKS_PER_MILLISECOND
    );
    assert_eq!(
      expire_at_milliseconds_to_ticks(i64::MAX),
      i64::MAX - (i64::MAX - UNIX_EPOCH_TICKS) % TICKS_PER_MILLISECOND
    );
  }

  /// 上界常量与钳制公式一致（重放端与命令端共用的 cap 单点）
  #[test]
  fn cap_constants_match_clamp() {
    assert_eq!(
      MAX_UNIX_TIMESTAMP_SECONDS,
      (i64::MAX - UNIX_EPOCH_TICKS) / TICKS_PER_SECOND
    );
    assert_eq!(
      MAX_UNIX_TIMESTAMP_MILLISECONDS,
      (i64::MAX - UNIX_EPOCH_TICKS) / TICKS_PER_MILLISECOND
    );
    // cap 之上再钳不改变结果（恒等：cap 即不动点）
    for over in [MAX_UNIX_TIMESTAMP_SECONDS + 1, i64::MAX] {
      assert_eq!(
        expire_at_seconds_to_ticks(over),
        expire_at_seconds_to_ticks(MAX_UNIX_TIMESTAMP_SECONDS)
      );
    }
    for over in [MAX_UNIX_TIMESTAMP_MILLISECONDS + 1, i64::MAX] {
      assert_eq!(
        expire_at_milliseconds_to_ticks(over),
        expire_at_milliseconds_to_ticks(MAX_UNIX_TIMESTAMP_MILLISECONDS)
      );
    }
  }
}
