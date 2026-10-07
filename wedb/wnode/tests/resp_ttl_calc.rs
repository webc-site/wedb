#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! TTL 与过期时间计算契约测试（basic_commands/ttl.rs 内联测试迁出）

use wbase::{
  convert::{
    TICKS_PER_MILLISECOND, TICKS_PER_SECOND, expire_after_ms_to_ticks, expire_after_to_ticks,
    unix_timestamp_in_milliseconds_to_ticks, unix_timestamp_in_seconds_to_ticks,
  },
  time::now_ticks,
};
use wnode::resp::basic_commands::{
  MAX_TIMESPAN_MILLISECONDS, MAX_TIMESPAN_SECONDS, MAX_UNIX_TIME_MILLISECONDS,
  MAX_UNIX_TIME_SECONDS, compute_absolute_expiry, compute_relative_expiry,
  try_get_absolute_expiry_ticks,
};
use wresp::cmd_strings as cs;

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
    compute_absolute_expiry(1_000_000, MAX_UNIX_TIME_MILLISECONDS, TICKS_PER_MILLISECOND).unwrap();
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
