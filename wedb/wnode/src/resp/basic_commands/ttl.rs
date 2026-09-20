//! TTL 与时间戳换算公共域（对标 libs/server/Resp/BasicCommands.cs 与 KeyAdminCommands.cs）

use wbase::{
  convert::{
    TICKS_PER_MILLISECOND, TICKS_PER_SECOND, UNIX_EPOCH_TICKS, expire_after_ms_to_ticks,
    expire_after_to_ticks,
  },
  time::now_ticks,
};
use wresp::cmd_strings as cs;

/// TimeSpan.MaxValue.TotalSeconds 对标上限（libs/server/Resp/BasicCommands.cs:NetworkGETEX）
pub const MAX_TIMESPAN_SECONDS: i64 = i64::MAX / TICKS_PER_SECOND;
/// TimeSpan.MaxValue.TotalMilliseconds 对标上限（libs/server/Resp/BasicCommands.cs:NetworkGETEX）
pub const MAX_TIMESPAN_MILLISECONDS: i64 = i64::MAX / TICKS_PER_MILLISECOND;
/// DateTimeOffset.MaxValue.ToUnixTimeSeconds() 对标上限（libs/server/Resp/BasicCommands.cs:NetworkGETEX）
pub const MAX_UNIX_TIME_SECONDS: i64 = 253_402_300_799;
/// DateTimeOffset.MaxValue.ToUnixTimeMilliseconds() 对标上限（libs/server/Resp/BasicCommands.cs:NetworkGETEX）
pub const MAX_UNIX_TIME_MILLISECONDS: i64 = 253_402_300_799_999;

/// GETEX 过期形态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GetexExpiry {
  /// 仅取值，既有 TTL 不动（无选项，或 EXAT/PXAT 折算结果不在未来：
  /// C# expiry=0，存储层 GETEX 分支 arg1==0 且非 PERSIST 时 NotUpdated）
  None,
  /// 清除过期（仅显式 PERSIST，见 RMWMethods.cs GETEX 分支 RemoveExpiration）
  Persist,
  /// 绝对过期 .NET Ticks（EX/PX/EXAT/PXAT 归一）
  At(i64),
}

/// 计算相对过期时间（EX/PX 换算 .NET Ticks）
#[inline]
pub(crate) const fn compute_relative_expiry(
  now: i64,
  expire_time: i64,
  max_val: i64,
  scale: i64,
) -> Result<i64, &'static str> {
  if expire_time > max_val {
    return Err(cs::RESP_ERR_GENERIC_INVALIDEXP_IN_GETEX);
  }
  let Some(ts_ticks) = expire_time.checked_mul(scale) else {
    return Err(cs::RESP_ERR_OVERFLOWEXP_IN_GETEX);
  };
  let Some(target) = now.checked_add(ts_ticks) else {
    return Err(cs::RESP_ERR_OVERFLOWEXP_IN_GETEX);
  };
  if target < 0 {
    return Err(cs::RESP_ERR_OVERFLOWEXP_IN_GETEX);
  }
  Ok(target)
}

/// 计算绝对过期时间（EXAT/PXAT 换算 .NET Ticks）
#[inline]
pub(crate) const fn compute_absolute_expiry(
  expire_time: i64,
  max_val: i64,
  scale: i64,
) -> Result<i64, &'static str> {
  if expire_time > max_val {
    return Err(cs::RESP_ERR_GENERIC_INVALIDEXP_IN_GETEX);
  }
  let Some(scaled) = expire_time.checked_mul(scale) else {
    return Err(cs::RESP_ERR_OVERFLOWEXP_IN_GETEX);
  };
  let Some(exp) = scaled.checked_add(UNIX_EPOCH_TICKS) else {
    return Err(cs::RESP_ERR_OVERFLOWEXP_IN_GETEX);
  };
  if exp < 0 {
    return Err(cs::RESP_ERR_OVERFLOWEXP_IN_GETEX);
  }
  Ok(exp)
}

/// 校验并计算相对时长到绝对过期 .NET Ticks（防止 i64 溢出）。
/// 对标 C# BasicCommands.cs:NetworkSET / NetworkSET_EX / NetworkSET_Conditional
#[inline]
pub(crate) fn try_get_absolute_expiry_ticks(expiry: i64, high_precision: bool) -> Option<i64> {
  let ticks_per_unit = if high_precision {
    TICKS_PER_MILLISECOND
  } else {
    TICKS_PER_SECOND
  };
  let current_ticks = now_ticks();
  if expiry <= 0 || expiry > (i64::MAX - current_ticks) / ticks_per_unit {
    return None;
  }
  Some(if high_precision {
    expire_after_ms_to_ticks(current_ticks, expiry)
  } else {
    expire_after_to_ticks(current_ticks, expiry)
  })
}

#[cfg(test)]
mod tests {
  use wbase::{
    convert::{unix_timestamp_in_milliseconds_to_ticks, unix_timestamp_in_seconds_to_ticks},
    time::now_ticks,
  };

  use super::*;

  #[test]
  fn test_try_get_absolute_expiry_ticks() {
    assert_eq!(try_get_absolute_expiry_ticks(0, false), None);
    assert_eq!(try_get_absolute_expiry_ticks(-1, false), None);
    assert_eq!(try_get_absolute_expiry_ticks(0, true), None);
    assert_eq!(try_get_absolute_expiry_ticks(-1, true), None);

    let now = now_ticks();
    let res_sec = try_get_absolute_expiry_ticks(10, false).unwrap();
    assert!(res_sec >= now + 10 * TICKS_PER_SECOND);
    assert!(res_sec <= now + 10 * TICKS_PER_SECOND + TICKS_PER_SECOND);

    let res_ms = try_get_absolute_expiry_ticks(100, true).unwrap();
    assert!(res_ms >= now + 100 * TICKS_PER_MILLISECOND);
    assert!(res_ms <= now + 100 * TICKS_PER_MILLISECOND + TICKS_PER_SECOND);

    // 溢出防护
    assert_eq!(try_get_absolute_expiry_ticks(i64::MAX, false), None);
    assert_eq!(try_get_absolute_expiry_ticks(i64::MAX, true), None);
  }

  #[test]
  fn test_compute_relative_expiry() {
    let now = 100_000_000_000;
    let res = compute_relative_expiry(now, 10, MAX_TIMESPAN_SECONDS, TICKS_PER_SECOND).unwrap();
    assert_eq!(res, expire_after_to_ticks(now, 10));

    let res_ms =
      compute_relative_expiry(now, 500, MAX_TIMESPAN_MILLISECONDS, TICKS_PER_MILLISECOND).unwrap();
    assert_eq!(res_ms, expire_after_ms_to_ticks(now, 500));

    // 超过 max_val
    assert_eq!(
      compute_relative_expiry(
        now,
        MAX_TIMESPAN_SECONDS + 1,
        MAX_TIMESPAN_SECONDS,
        TICKS_PER_SECOND
      ),
      Err(cs::RESP_ERR_GENERIC_INVALIDEXP_IN_GETEX)
    );

    // 日期溢出
    let overflow_sec = (i64::MAX - now) / TICKS_PER_SECOND + 1;
    assert_eq!(
      compute_relative_expiry(now, overflow_sec, i64::MAX, TICKS_PER_SECOND),
      Err(cs::RESP_ERR_OVERFLOWEXP_IN_GETEX)
    );
  }

  #[test]
  fn test_compute_absolute_expiry() {
    let res = compute_absolute_expiry(1000, MAX_UNIX_TIME_SECONDS, TICKS_PER_SECOND).unwrap();
    assert_eq!(res, unix_timestamp_in_seconds_to_ticks(1000));

    let res_ms =
      compute_absolute_expiry(1_000_000, MAX_UNIX_TIME_MILLISECONDS, TICKS_PER_MILLISECOND)
        .unwrap();
    assert_eq!(res_ms, unix_timestamp_in_milliseconds_to_ticks(1_000_000));

    // 超过 max_val
    assert_eq!(
      compute_absolute_expiry(
        MAX_UNIX_TIME_SECONDS + 1,
        MAX_UNIX_TIME_SECONDS,
        TICKS_PER_SECOND
      ),
      Err(cs::RESP_ERR_GENERIC_INVALIDEXP_IN_GETEX)
    );

    // 负值越界
    assert_eq!(
      compute_absolute_expiry(-1_000_000_000_000, MAX_UNIX_TIME_SECONDS, TICKS_PER_SECOND),
      Err(cs::RESP_ERR_OVERFLOWEXP_IN_GETEX)
    );
  }
}
