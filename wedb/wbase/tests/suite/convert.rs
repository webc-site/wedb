use wbase::{
  convert::*,
  time::{self, NANOS_PER_TICK},
};

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

/// 双域刻度常量收口单一真源：TTL i64 域恒等派生自 time u64 真源（物理基准
/// NANOS_PER_TICK），与原字面值逐位全等且除法精确无截断
#[test]
fn ticks_constants_single_source_derivation() {
  assert_eq!(TICKS_PER_SECOND, time::TICKS_PER_SECOND as i64);
  assert_eq!(TICKS_PER_SECOND, 10_000_000);
  assert_eq!(TICKS_PER_MILLISECOND, TICKS_PER_SECOND / 1_000);
  assert_eq!(TICKS_PER_MILLISECOND, 10_000);
  // 除法精确：秒域刻度整除毫秒/微秒无余数，派生无截断损失
  assert_eq!(TICKS_PER_SECOND % 1_000, 0);
  assert_eq!(
    stopwatch::TICKS_PER_MICROSECOND * 1_000_000,
    time::TICKS_PER_SECOND
  );
  assert_eq!(NANOS_PER_TICK * TICKS_PER_SECOND as u64, 1_000_000_000);
}

/// 直方图/Stopwatch 计量域因子全部由 TICKS_PER_SECOND 单点派生，
/// 与 TTL 域（i64）同单位不同整数域
#[test]
fn stopwatch_scale_derives() {
  assert_eq!(stopwatch::TICKS_PER_MICROSECOND, 10);
  assert_eq!(stopwatch::seconds(1), TICKS_PER_SECOND as u64);
  assert_eq!(stopwatch::seconds(100), 100 * TICKS_PER_SECOND as u64);
  // 微秒 → tick → 微秒 往返恒等（因子同源于一个刻度）
  let micros = 1234u64;
  assert_eq!(
    micros * stopwatch::TICKS_PER_MICROSECOND / stopwatch::TICKS_PER_MICROSECOND,
    micros
  );
}

/// 相对 → 绝对：now + 饱和时长，饱和加法不 panic
#[test]
fn expire_after_saturates() {
  let now = 70_000_000_000_000_000;
  assert_eq!(expire_after_to_ticks(now, 10), now + 10 * TICKS_PER_SECOND);
  assert_eq!(
    expire_after_ms_to_ticks(now, 10),
    now + 10 * TICKS_PER_MILLISECOND
  );
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
  assert_eq!(
    expire_at_seconds_to_ticks(i64::MAX),
    i64::MAX - (i64::MAX - UNIX_EPOCH_TICKS) % TICKS_PER_SECOND
  );
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

#[test]
fn test_compute_expiration_ticks() {
  let now = 638_000_000_000_000_000i64;
  // 相对秒
  assert_eq!(
    compute_expiration_ticks(now, 10, false, false),
    expire_after_to_ticks(now, 10)
  );
  // 相对毫秒
  assert_eq!(
    compute_expiration_ticks(now, 1000, true, false),
    expire_after_ms_to_ticks(now, 1000)
  );
  // 绝对秒
  assert_eq!(
    compute_expiration_ticks(now, 1_700_000_000, false, true),
    expire_at_seconds_to_ticks(1_700_000_000)
  );
  // 绝对毫秒
  assert_eq!(
    compute_expiration_ticks(now, 1_700_000_000_000, true, true),
    expire_at_milliseconds_to_ticks(1_700_000_000_000)
  );
}

/// try_ 通用形与饱和/裸算形在可表示区间内逐位相等（GETEX 归单源后的
/// 行为不变对拍锁；scale 双域各锁一组）
#[test]
fn try_convert_parities() {
  let now = 638_000_000_000_000_000i64;
  for scale in [TICKS_PER_SECOND, TICKS_PER_MILLISECOND] {
    // 相对域：可表示区间内 try_ == 饱和形（等价于 now + duration*scale 不
    // 溢出即相等），且对 max 值域（命令层放行上界）逐点锁
    for duration in [0i64, 1, 60, 3_600, 86_400, now / scale] {
      let saturating = if scale == TICKS_PER_SECOND {
        expire_after_to_ticks(now, duration)
      } else {
        expire_after_ms_to_ticks(now, duration)
      };
      assert_eq!(
        try_expire_after_to_ticks(now, duration, scale),
        Some(saturating)
      );
    }
    // 绝对域：cap（MAX_UNIX_TIMESTAMP_*，恰为可表示上界）内 try_ == 裸算形
    let (ts_cap, bare): (i64, fn(i64) -> i64) = if scale == TICKS_PER_SECOND {
      (
        MAX_UNIX_TIMESTAMP_SECONDS,
        unix_timestamp_in_seconds_to_ticks,
      )
    } else {
      (
        MAX_UNIX_TIMESTAMP_MILLISECONDS,
        unix_timestamp_in_milliseconds_to_ticks,
      )
    };
    for ts in [0i64, 1, 1_700_000_000, ts_cap] {
      assert_eq!(try_expire_at_to_ticks(ts, scale), Some(bare(ts)));
    }
  }
  // 溢出即 None：相对域取命令层可达上界（now≈当前 epoch ticks + 1μs
  // 量级时长即越 i64::MAX）；绝对域取 cap+1 与 i64::MAX
  assert_eq!(
    try_expire_after_to_ticks(i64::MAX - 1, 1, TICKS_PER_MILLISECOND),
    None
  );
  assert_eq!(
    try_expire_at_to_ticks(MAX_UNIX_TIMESTAMP_SECONDS + 1, TICKS_PER_SECOND),
    None
  );
  assert_eq!(
    try_expire_at_to_ticks(i64::MAX, TICKS_PER_MILLISECOND),
    None
  );
}

// 自 tests/main.rs 迁入（原 test_convert_primitives）

#[test]
fn test_convert_primitives() {
  use wbase::convert::*;

  let ticks = unix_timestamp_in_seconds_to_ticks(1600000000);
  assert_eq!(unix_time_in_seconds_from_ticks(ticks), 1600000000);
  assert_eq!(unix_time_in_seconds_from_ticks(-1), -1);
  assert_eq!(unix_time_in_seconds_from_ticks(0), -1);

  let ms_ticks = unix_timestamp_in_milliseconds_to_ticks(1600000000123);
  assert_eq!(
    unix_time_in_milliseconds_from_ticks(ms_ticks),
    1600000000123
  );
  assert_eq!(unix_time_in_milliseconds_from_ticks(-1), -1);
  assert_eq!(unix_time_in_milliseconds_from_ticks(0), -1);

  let now_ticks = 100_000_000;
  assert_eq!(
    seconds_from_diff_ticks(now_ticks + 15_000_000, now_ticks),
    2
  );
  assert_eq!(
    seconds_from_diff_ticks(now_ticks + 14_999_999, now_ticks),
    1
  );
  assert_eq!(seconds_from_diff_ticks(now_ticks, now_ticks), -1);
  assert_eq!(seconds_from_diff_ticks(-1, now_ticks), -1);

  assert_eq!(
    milliseconds_from_diff_ticks(now_ticks + 50_000, now_ticks),
    5
  );
  assert_eq!(milliseconds_from_diff_ticks(now_ticks, now_ticks), -1);
}
