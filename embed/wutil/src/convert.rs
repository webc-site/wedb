use std::time::{SystemTime, UNIX_EPOCH};

const UNIX_EPOCH_TICKS: i64 = 621_355_968_000_000_000;
const TICKS_PER_SECOND: i64 = 10_000_000;
const TICKS_PER_MILLISECOND: i64 = 10_000;

fn utc_now_ticks() -> i64 {
  let duration_since_epoch = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default();
  (duration_since_epoch.as_secs() as i64 * TICKS_PER_SECOND)
    + (duration_since_epoch.subsec_nanos() as i64 / 100)
    + UNIX_EPOCH_TICKS
}

/// garnet/libs/common/ConvertUtils.cs:SecondsFromDiffUtcNowTicks
pub fn seconds_from_diff_utc_now_ticks(mut ticks: i64) -> i64 {
  let mut seconds = -1;
  if ticks > 0 {
    ticks -= utc_now_ticks();
    seconds = if ticks > 0 {
      (ticks + TICKS_PER_SECOND / 2) / TICKS_PER_SECOND
    } else {
      -1
    };
  }
  seconds
}

/// garnet/libs/common/ConvertUtils.cs:MillisecondsFromDiffUtcNowTicks
pub fn milliseconds_from_diff_utc_now_ticks(mut ticks: i64) -> i64 {
  let mut milliseconds = -1;
  if ticks > 0 {
    ticks -= utc_now_ticks();
    milliseconds = if ticks > 0 {
      ticks / TICKS_PER_MILLISECOND
    } else {
      -1
    };
  }
  milliseconds
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
pub fn unix_time_in_seconds_from_ticks(ticks: i64) -> i64 {
  if ticks > 0 {
    (ticks - UNIX_EPOCH_TICKS) / TICKS_PER_SECOND
  } else {
    -1
  }
}

/// garnet/libs/common/ConvertUtils.cs:UnixTimeInMillisecondsFromTicks
pub fn unix_time_in_milliseconds_from_ticks(ticks: i64) -> i64 {
  if ticks > 0 {
    (ticks - UNIX_EPOCH_TICKS) / TICKS_PER_MILLISECOND
  } else {
    -1
  }
}
