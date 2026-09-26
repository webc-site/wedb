//! TTL 与时间戳换算公共域（对标 libs/server/Resp/BasicCommands.cs 与 KeyAdminCommands.cs）

use wbase::{
  convert::{
    TICKS_PER_MILLISECOND, TICKS_PER_SECOND, UNIX_EPOCH_TICKS, expire_after_ms_to_ticks,
    expire_after_to_ticks, try_expire_after_to_ticks, try_expire_at_to_ticks,
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

/// GETEX 绝对域 gate 内乘加必不溢出（案二归并后 [`compute_absolute_expiry`]
/// 的 `None` 臂对命令可达值域为纯兜底的编译期证明，i128 域算乘加和上界）：
/// - 秒域：253,402,300,799 × 10,000,000 + 纪元 ticks
///   = 3,155,378,975,990,000,000
/// - 毫秒域：253,402,300,799,999 × 10,000 + 纪元 ticks
///   = 3,155,378,975,999,990,000
///
/// 两者均 < i64::MAX = 9,223,372,036,854,775,807，故 EXAT/PXAT 过 gate 后
/// checked 乘加恒成功；负入参在命令层 `parse_getex_args` 即拒（<=0），
/// 仅直接调用可达且由 `None`/负积臂兜底
const _: () = assert!(
  (MAX_UNIX_TIME_SECONDS as i128) * (TICKS_PER_SECOND as i128) + (UNIX_EPOCH_TICKS as i128)
    <= i64::MAX as i128
);
const _: () = assert!(
  (MAX_UNIX_TIME_MILLISECONDS as i128) * (TICKS_PER_MILLISECOND as i128)
    + (UNIX_EPOCH_TICKS as i128)
    <= i64::MAX as i128
);

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
///
/// 算术本体归 [`wbase::convert::try_expire_after_to_ticks`] 单源（案二收口，
/// deviations.md §143）：本函数只保留命令层值域 gate 与错误帧分流——
/// `expire_time > max_val` 回 INVALIDEXP（C# `TimeSpan.TotalSeconds/
/// TotalMilliseconds` 越界即拒的形态），gate 后乘加溢出（如 PX 取
/// [`MAX_TIMESPAN_MILLISECONDS`]，正滴答 now 下加法必越 i64::MAX）回
/// OVERFLOWEXP。旧实现自带的第二份乘加与 `target < 0` 死臂一并摘除：
/// gate 内非负入参乘加成功即非负，负入参在 `parse_getex_args` 即拒
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
  match try_expire_after_to_ticks(now, expire_time, scale) {
    Some(target) => Ok(target),
    None => Err(cs::RESP_ERR_OVERFLOWEXP_IN_GETEX),
  }
}

/// 计算绝对过期时间（EXAT/PXAT 换算 .NET Ticks）
///
/// 算术本体归 [`wbase::convert::try_expire_at_to_ticks`] 单源（案二收口，
/// deviations.md §143）：本函数只保留命令层值域 gate 与错误帧分流。gate 内
/// 乘加必不溢出（见上方编译期常量断言），`None` 臂对命令可达值域为纯兜底，
/// 仅直接调用负值（如 -1e12 秒，乘法先失败）可触发；旧实现自带的第三份
/// epoch 乘加与 `exp < 0` 死臂一并摘除
#[inline]
pub(crate) const fn compute_absolute_expiry(
  expire_time: i64,
  max_val: i64,
  scale: i64,
) -> Result<i64, &'static str> {
  if expire_time > max_val {
    return Err(cs::RESP_ERR_GENERIC_INVALIDEXP_IN_GETEX);
  }
  match try_expire_at_to_ticks(expire_time, scale) {
    Some(exp) => Ok(exp),
    None => Err(cs::RESP_ERR_OVERFLOWEXP_IN_GETEX),
  }
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

    // 命令层可达溢出：PX 取 gate 上界自身（不越 max_val 判帧）+ 正滴答
    // now（当前 epoch 量级）加法必越 i64::MAX → OVERFLOWEXP，锁死 gate
    // 与换算的分工不被改坏
    assert_eq!(
      compute_relative_expiry(
        638_000_000_000_000_000,
        MAX_TIMESPAN_MILLISECONDS,
        MAX_TIMESPAN_MILLISECONDS,
        TICKS_PER_MILLISECOND
      ),
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
