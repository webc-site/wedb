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

/// 相对时长换算为绝对过期 .NET Ticks（EX/PX 共用；high_precision 即 PX）。
/// 对标 C# `UtcNow + TimeSpan.FromSeconds/FromMilliseconds` 的 ticks 加算；
/// 乘加公式统一委托 [`wbase::convert`] 单点
pub(crate) fn expiry_ticks_from_now(expiry: i64, high_precision: bool) -> i64 {
  if high_precision {
    expire_after_ms_to_ticks(now_ticks(), expiry)
  } else {
    expire_after_to_ticks(now_ticks(), expiry)
  }
}
