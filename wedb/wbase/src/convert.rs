//! 数据原语换算 (对标 garnet/libs/common/ConvertUtils.cs)
//!
//! UtcNow 系列（[`utc_now_ticks`] / [`seconds_from_diff_utc_now_ticks`] /
//! [`milliseconds_from_diff_utc_now_ticks`]）需启用 `time` 特性：时间源统一
//! coarsetime 委托 [`crate::time::now_ticks`] 单一实现，不提供 `std::time` 回退分支；
//! 纯函数（*_from_ticks / unix_timestamp_in_* / unix_time_in_*）无时钟依赖，恒可用
//!
//! 在 garnet 中的相对路径: libs/common/CoarseTimeProvider.cs + test/standalone/Garnet.test/CoarseTimeProviderTests.cs（Tick 换算与饱和）

use crate::time;
pub use crate::time::UNIX_EPOCH_TICKS;

/// TTL/实时域每秒刻度数（法定 i64 导出口，供全仓 TTL 与相对时间换算面消费）：
/// 唯一真源为 [`crate::time::TICKS_PER_SECOND`]（u64，物理基准 NANOS_PER_TICK），
/// 此处仅做恒等整数域派生——值恒 10,000,000，远小于 i64::MAX，as 转换永不失真，
/// 禁再写同值字面量（.NET DateTimeOffset.Ticks 与 Stopwatch.Frequency 同为 10 MHz）
pub const TICKS_PER_SECOND: i64 = time::TICKS_PER_SECOND as i64;

/// TTL/实时域每毫秒刻度数（编译期除法派生，100ns tick 域每毫秒恒千枚）
pub const TICKS_PER_MILLISECOND: i64 = TICKS_PER_SECOND / 1_000;

/// Stopwatch/直方图计量域（u64）的刻度因子单点
///
/// 对标 C# HdrHistogram 库的两个常量面 `OutputScalingFactor`（TimeStampToSeconds /
/// TimeStampToMicroseconds）与 `TimeStamp.Seconds`：C# 服务端指标（libs/server/Metrics/**）
/// 与客户端（libs/client/GarnetClient.cs）一律引用库常量、绝不自建换算；rust 侧本模块
/// 即该库常量位，wmetric（服务端指标）/ wconn（客户端指标）/ wnode（命令面阈值折算）
/// 三个消费方共用，禁在任何消费方本地派生同义常量或写裸字面量。
///
/// 与 [`TICKS_PER_SECOND`]（i64）的分工只在整数域、不在单位：两侧同为 100ns tick，
/// 且共用 [`crate::time::TICKS_PER_SECOND`]（u64）这一枚编译期真源——本模块直接取
/// 该 u64 常量、不经 i64 中转；TTL/过期记录取上方 i64 派生出口
/// （[`duration_seconds_to_ticks`]），直方图为 `Histogram<u64>` 故其边界与因子取 u64
pub mod stopwatch {
  use crate::time::TICKS_PER_SECOND;

  /// tick/微秒（100ns tick 域恒 10；对标 C# `OutputScalingFactor.TimeStampToMicroseconds`
  /// = `Stopwatch.Frequency / 1_000_000`，等价 .NET `TimeSpan.TicksPerMicrosecond`）
  ///
  /// 双向用途同源于一个因子：微秒 → tick 乘本值（客户端 RTT 记账、慢日志阈值折算），
  /// tick → 微秒输出除本值（服务端/客户端百分位输出）
  pub const TICKS_PER_MICROSECOND: u64 = TICKS_PER_SECOND / 1_000_000;

  /// 秒 → tick（对标 C# `TimeStamp.Seconds` = `Stopwatch.Frequency * seconds`；
  /// C# 三处直方图上界 `LongHistogram(1, TimeStamp.Seconds(100), 2)` 的同形换算，
  /// const fn 故可入编译期常量）
  #[inline]
  pub const fn seconds(seconds: u64) -> u64 {
    seconds * TICKS_PER_SECOND
  }
}

/// 计算 ticks 与指定基准 ticks 之差并转换为秒（四舍五入进位；非正差值返回 -1）
#[inline]
pub const fn seconds_from_diff_ticks(ticks: i64, now_ticks: i64) -> i64 {
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

/// 计算 ticks 与指定基准 ticks 之差并转换为毫秒（非正差值返回 -1）
#[inline]
pub const fn milliseconds_from_diff_ticks(ticks: i64, now_ticks: i64) -> i64 {
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
pub const fn unix_timestamp_in_seconds_to_ticks(unix_timestamp: i64) -> i64 {
  unix_timestamp * TICKS_PER_SECOND + UNIX_EPOCH_TICKS
}

/// garnet/libs/common/ConvertUtils.cs:UnixTimestampInMillisecondsToTicks
#[inline]
pub const fn unix_timestamp_in_milliseconds_to_ticks(unix_timestamp: i64) -> i64 {
  unix_timestamp * TICKS_PER_MILLISECOND + UNIX_EPOCH_TICKS
}

/// garnet/libs/common/ConvertUtils.cs:UnixTimeInSecondsFromTicks
#[inline]
pub const fn unix_time_in_seconds_from_ticks(ticks: i64) -> i64 {
  if ticks > 0 {
    (ticks - UNIX_EPOCH_TICKS) / TICKS_PER_SECOND
  } else {
    -1
  }
}

/// garnet/libs/common/ConvertUtils.cs:UnixTimeInMillisecondsFromTicks
#[inline]
pub const fn unix_time_in_milliseconds_from_ticks(ticks: i64) -> i64 {
  if ticks > 0 {
    (ticks - UNIX_EPOCH_TICKS) / TICKS_PER_MILLISECOND
  } else {
    -1
  }
}

/// 可表示绝对过期 .NET Ticks 的最大 Unix 秒
/// （(i64::MAX - UNIX_EPOCH_TICKS) / TICKS_PER_SECOND，编译期常量）
const MAX_UNIX_TIMESTAMP_SECONDS: i64 = (i64::MAX - UNIX_EPOCH_TICKS) / TICKS_PER_SECOND;

/// 可表示绝对过期 .NET Ticks 的最大 Unix 毫秒
/// （(i64::MAX - UNIX_EPOCH_TICKS) / TICKS_PER_MILLISECOND，编译期常量）
const MAX_UNIX_TIMESTAMP_MILLISECONDS: i64 = (i64::MAX - UNIX_EPOCH_TICKS) / TICKS_PER_MILLISECOND;

/// 相对秒 → 时长 ticks（饱和乘法：乘法溢出饱和至 i64::MAX，杜绝 debug 构建
/// 溢出 panic，C# unchecked 环绕对应的确定性降级）
///
/// EXPIRE/SET EX/SETEX 重放域（相对时长 ticks）的统一换算单点：
/// libs/server/Resp/KeyAdminCommands.cs:423 `AddSeconds` 的乘法前半段
#[inline]
pub const fn duration_seconds_to_ticks(seconds: i64) -> i64 {
  seconds.saturating_mul(TICKS_PER_SECOND)
}

/// 相对毫秒 → 时长 ticks（饱和乘法，同 [`duration_seconds_to_ticks`]）
///
/// PEXPIRE/PSETEX 重放域的统一换算单点：
/// libs/server/Resp/KeyAdminCommands.cs:424 `AddMilliseconds` 的乘法前半段
#[inline]
pub const fn duration_milliseconds_to_ticks(milliseconds: i64) -> i64 {
  milliseconds.saturating_mul(TICKS_PER_MILLISECOND)
}

/// 相对秒 → 绝对截止 .NET Ticks：now_ticks + 饱和(秒 * TicksPerSecond)
///
/// 「当前 ticks + 相对秒 = 绝对 ticks 截止」的唯一换算（调用方一律走本单点，
/// 禁各自裸乘刻度或在无符号域内乘后 as i64 收窄）：
/// - EXPIRE 命令端与 RESTORE(EX) 重放域：
///   libs/server/Resp/KeyAdminCommands.cs:423 `DateTimeOffset.UtcNow.AddSeconds(expiration).UtcTicks`
/// - 库/空间级延时 GC 回收截止（wkv `flush_database` / `flush_namespace` 写
///   DbMeta 死亡账本的 `expired_at`）：C# 无虚拟库换号与延时回收对位物，
///   rust 侧同一「now + 延时秒」形态复用本单点，不再第二套换算
///
/// 【有意偏差登记】此处将大值秒数钳制（saturating_add）到 `i64::MAX` ticks，正常回 :1。
/// 而 C# `DateTimeOffset.UtcNow.AddSeconds` 越界时（阈值约 2.5e11 秒）会抛
/// `ArgumentOutOfRangeException`，走会话异常通道掐连接。
/// （与 EXPIREAT/PEXPIREAT 绝对面钳制同源，登记见 doc/zh/deviations.md §4 b)；远端大值不再掐连接）
#[inline]
pub const fn expire_after_to_ticks(now_ticks: i64, seconds: i64) -> i64 {
  now_ticks.saturating_add(duration_seconds_to_ticks(seconds))
}

/// 相对毫秒 → 绝对过期 .NET Ticks：now_ticks + 饱和(毫秒 * TicksPerMillisecond)
///
/// PEXPIRE 命令端的换算单点：
/// libs/server/Resp/KeyAdminCommands.cs:424 `AddMilliseconds(expiration).UtcTicks`
#[inline]
pub const fn expire_after_ms_to_ticks(now_ticks: i64, milliseconds: i64) -> i64 {
  now_ticks.saturating_add(duration_milliseconds_to_ticks(milliseconds))
}

/// 绝对 Unix 秒 → 绝对过期 .NET Ticks（负值夹 0 = Unix 纪元，超
/// [`MAX_UNIX_TIMESTAMP_SECONDS`] 钳到最大可表示 ticks）
///
#[inline]
const fn clamp_i64(val: i64, min: i64, max: i64) -> i64 {
  if val < min {
    min
  } else if val > max {
    max
  } else {
    val
  }
}

/// 绝对 Unix 秒 → 绝对过期 .NET Ticks（负值夹 0 = Unix 纪元，超
/// [`MAX_UNIX_TIMESTAMP_SECONDS`] 钳到最大可表示 ticks）
///
/// EXPIREAT 命令端与重放端的换算单点：
/// libs/server/Resp/KeyAdminCommands.cs:425（rust 在 C# 之上补确定性钳制）
#[inline]
pub const fn expire_at_seconds_to_ticks(unix_seconds: i64) -> i64 {
  unix_timestamp_in_seconds_to_ticks(clamp_i64(unix_seconds, 0, MAX_UNIX_TIMESTAMP_SECONDS))
}

/// 绝对 Unix 毫秒 → 绝对过期 .NET Ticks（负值夹 0，超
/// [`MAX_UNIX_TIMESTAMP_MILLISECONDS`] 钳到最大可表示 ticks）
///
/// PEXPIREAT 命令端与重放端的换算单点：
/// libs/server/Resp/KeyAdminCommands.cs:426（rust 在 C# 之上补确定性钳制）
#[inline]
pub const fn expire_at_milliseconds_to_ticks(unix_milliseconds: i64) -> i64 {
  unix_timestamp_in_milliseconds_to_ticks(clamp_i64(
    unix_milliseconds,
    0,
    MAX_UNIX_TIMESTAMP_MILLISECONDS,
  ))
}

/// 过期时长/时间戳换算为绝对 .NET Ticks 的全域统一入口
///
/// 覆盖 EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT 及 HEXPIRE/ZEXPIRE 字段级换算：
/// - `is_timestamp`: true 时视为绝对 Unix 时间戳（负值夹 0、超界钳至最大可表示 ticks），
///   false 时视为相对时长并与 `now_ticks` 饱和累加
/// - `is_milliseconds`: true 为毫秒域，false 为秒域
///
/// 相对域大值分叉（C# `AddSeconds`/`AddMilliseconds` 超阈抛异常掐连对 rust
/// 饱和钳正常应答 `*N :1`，含 HEXPIRE/HPEXPIRE 与 ZEXPIRE/ZPEXPIRE 字段族）
/// 系有意偏差，登记回指 doc/zh/deviations.md §4a，严禁回改饱和算式。
#[inline]
pub const fn compute_expiration_ticks(
  now_ticks: i64,
  expiration: i64,
  is_milliseconds: bool,
  is_timestamp: bool,
) -> i64 {
  if is_timestamp {
    if is_milliseconds {
      expire_at_milliseconds_to_ticks(expiration)
    } else {
      expire_at_seconds_to_ticks(expiration)
    }
  } else if is_milliseconds {
    expire_after_ms_to_ticks(now_ticks, expiration)
  } else {
    expire_after_to_ticks(now_ticks, expiration)
  }
}

/// 相对时长 → 绝对截止 .NET Ticks 的 checked 通用形（`scale` 取
/// [`TICKS_PER_SECOND`] 或 [`TICKS_PER_MILLISECOND`]，秒/毫秒域一份代码）
///
/// GETEX EX/PX 换算的唯一算术单点（对标 C# GetexCommands.cs
/// `DateTimeOffset.UtcNow.Add{Seconds,Milliseconds}(expire).UtcTicks`）：
/// 溢出即 `None`，由命令层判帧回错误；sat/裸算形 [`expire_after_to_ticks`]
/// / [`expire_after_ms_to_ticks`] 保留给 EXPIRE 族与重放域的钳制语义，两者
/// 在可表示区间内逐位相等（对拍锁见本模块 tests）。命令层禁再手写第三份
/// 乘加（deviations.md §143 案二收口）。
#[inline]
pub const fn try_expire_after_to_ticks(now_ticks: i64, duration: i64, scale: i64) -> Option<i64> {
  // const fn 内 `?` 未定（const_try 不稳），显式 match 同形
  match duration.checked_mul(scale) {
    Some(ticks) => ticks.checked_add(now_ticks),
    None => None,
  }
}

/// 绝对 Unix 时间戳 → 绝对过期 .NET Ticks 的 checked 通用形（`scale` 取
/// [`TICKS_PER_SECOND`] 或 [`TICKS_PER_MILLISECOND`]，秒/毫秒域一份代码）
///
/// GETEX EXAT/PXAT 换算的唯一算术单点（对标 C#
/// `new DateTimeOffset(expire, DateTimeOffset.UnixEpoch).UtcTicks` 的
/// epoch 乘加半段，见 ConvertUtils.cs UnixTimestampIn*ToTicks）：溢出即
/// `None`，由命令层判帧回错误；与裸算形 [`unix_timestamp_in_seconds_to_ticks`]
/// / [`unix_timestamp_in_milliseconds_to_ticks`] 在可表示区间内逐位相等
/// （对拍锁见本模块 tests）。命令层禁再手写第三份乘加（deviations.md §143
/// 案二收口）。
#[inline]
pub const fn try_expire_at_to_ticks(unix_timestamp: i64, scale: i64) -> Option<i64> {
  match unix_timestamp.checked_mul(scale) {
    Some(ticks) => ticks.checked_add(UNIX_EPOCH_TICKS),
    None => None,
  }
}

/// 键级过期 ticks 的 4-bit coarse 粗化单点（1600ns 分辨率清零低 4 位；
/// 对位 C# ExpirationWithOption.cs 构造器 `(ticks >> 4) << 4` 的粗化半段——
/// C# 键级 EXPIRE 与字段级 HEXPIRE/ZEXPIRE 共用同一构造器，rust 键级不落
/// option 位故单列本粗化点；带 option 打包臂的符号锚点 1:1 挂在
/// `wresp::options::ExpirationWithOption::new`，此处不复挂）
///
/// C# 的粗化不是 TTL 通用口径，而是 EXPIRE 族把 ExpireOption 借进同一 long
/// 低 4 位打包的产物：键级只覆盖 EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT 与字段级
/// HEXPIRE/ZEXPIRE（字段级粗化随打包在 `wresp::ExpirationWithOption` 单点，
/// option 位不落键级 TTL 记录故不适用）。SET/GETEX/RENAME 在 C# 走裸 ticks
/// （garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs
/// TrySetExpiration / EvaluateExpire* 无移位），禁止一并粗化；rust 侧
/// GETEX 的 epoch 乘加换算亦归本模块 [`try_expire_after_to_ticks`] /
/// [`try_expire_at_to_ticks`] 单源，命令层禁再手写第三份。
///
/// 键级 EXPIRE 族值域裁决唯 wnode `RespServerSession::network_expire` 命令
/// 边界一处施加（对标 C# NetworkEXPIRE 打包粗化，同步快路径与异步慢路径
/// 共用同一参数解析单点）；wkv `StoreSession::expire_at` 会话入口与
/// `put_ttl`/`put_ttl_sync` 内核恒等裸写（对标 C# 存储侧 word 形恒等装载
/// UnifiedStore/RMWMethods.cs:216、:228 与 `ExpirationWithOption.cs:30-33`），
/// 保证 SET/GETEX/RENAME 族裸 ticks 经 AOF 重放/迁移导入/复制应用与主端
/// 存值逐位一致（历史双入口形态的收敛登记见 doc/zh/deviations.md §143）。
///
/// 粗化幂等：`(x >> 4) << 4` 二次施加不变。
#[inline]
pub const fn coarse_expire_ticks(expiration_time_in_ticks: i64) -> i64 {
  (expiration_time_in_ticks >> 4) << 4
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{time, time::NANOS_PER_TICK};

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
}
