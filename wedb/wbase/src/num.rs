//! 在 garnet 中的相对路径: libs/common/ConvertUtils.cs + test/standalone/Garnet.test/NumUtils.cs + Resp/RespReadUtilsTests.cs（严格数字语法）
use core::str::{FromStr, from_utf8};

/// garnet/libs/common/NumUtils.cs:TryParse
///
/// 整数与通用标量 `TryParse` 泛型实现：UTF-8 解码 + 类型解析，成功写入 `value` 返回 true
#[inline]
pub fn try_parse<T: FromStr>(source: &[u8], value: &mut T) -> bool {
  if let Ok(s) = from_utf8(source)
    && let Ok(v) = s.parse::<T>()
  {
    *value = v;
    return true;
  }
  false
}

/// 严格浮点解析所需的标量能力（f32/f64 标准库签名完全吻合，仅做静态分派约束）
trait StrictFloat: FromStr + Copy {
  const INFINITY: Self;
  const NEG_INFINITY: Self;
  fn is_nan(self) -> bool;
  fn is_infinite(self) -> bool;
}

impl StrictFloat for f32 {
  const INFINITY: Self = f32::INFINITY;
  const NEG_INFINITY: Self = f32::NEG_INFINITY;
  fn is_nan(self) -> bool {
    f32::is_nan(self)
  }
  fn is_infinite(self) -> bool {
    f32::is_infinite(self)
  }
}

impl StrictFloat for f64 {
  const INFINITY: Self = f64::INFINITY;
  const NEG_INFINITY: Self = f64::NEG_INFINITY;
  fn is_nan(self) -> bool {
    f64::is_nan(self)
  }
  fn is_infinite(self) -> bool {
    f64::is_infinite(self)
  }
}

/// 严格浮点解析泛型实现（garnet/libs/server/Resp/Parser/ParseUtils.cs:
/// TryReadDouble / TryReadFloat，含 can_be_infinite 门控；can_be_infinite=true
/// 即 libs/common/NumUtils.cs:TryParseWithInfinity——先严格解析、失败回落
/// TryReadInfinity 词形白名单，can_be_infinite=true 的 [`Self::strict_f64`]
/// 调用点与 C# TryParseWithInfinity 调用点一一对应）：Rust parse 整体
/// 消费 + Utf8Parser 词形拒绝（NaN 恒拒绝；inf/infinity 词形无数字，视作
/// 解析失败；纯数值溢出的 ±inf 保留，如 "1e999"），失败且 can_be_infinite
/// 时回落 RespReadUtils.TryReadInfinity 白名单（inf/+inf/-inf，3-4 字节，
/// 大小写不敏感；infinity 全拼非法）
///
/// 【有意偏差登记】C# dotnet Utf8Parser 大小写不敏感接受 "nan" 与 "infinity" 全拼，
/// Rust 侧恒拒 nan，且 infinity 侧仅认 inf/+inf/-inf。后果：C# 会将 `ZADD k nan m`
/// 视为有效从而把 NaN 混入集合，Rust 直接报错 not a valid float 从而拒收数据污染。
/// 注意：对于计算得出 NaN 的情况（如 ZADD INCR 时 0 加上 +inf 与 -inf），两侧一致报 SCORE_NAN 错，
/// 文案逐字节相同（`RESP_ERR_GENERIC_SCORE_NAN` 同串，见 doc/zh/deviations.md §2）。
#[inline]
fn strict_parse_float<T: StrictFloat>(raw: &[u8], can_be_infinite: bool) -> Option<T> {
  if let Ok(s) = from_utf8(raw)
    && let Ok(v) = s.parse::<T>()
    && !v.is_nan()
    && !(v.is_infinite() && !raw.iter().any(u8::is_ascii_digit))
  {
    return Some(v);
  }
  if can_be_infinite {
    return match infinity_sign(raw) {
      Some(true) => Some(T::INFINITY),
      Some(false) => Some(T::NEG_INFINITY),
      None => None,
    };
  }
  None
}

/// garnet/libs/server/Resp/Parser/ParseUtils.cs:TryReadDouble
#[inline]
pub fn strict_f64(raw: &[u8], can_be_infinite: bool) -> Option<f64> {
  strict_parse_float(raw, can_be_infinite)
}

/// garnet/libs/server/Resp/Parser/ParseUtils.cs:TryReadFloat
#[inline]
pub fn strict_f32(raw: &[u8], can_be_infinite: bool) -> Option<f32> {
  strict_parse_float(raw, can_be_infinite)
}

/// 严格整数文法单点（对标 C# RespReadUtils.TryReadInt64Safe allowLeadingZeros:false
/// 与整段消费；C# TryReadInt32Safe 虽声明 allowLeadingZeros 但未消费系死参，见
/// doc/zh/deviations.md §32）：可选 `+`/`-` 号，首数字 '0' 且仍有后续数字即拒绝
/// （"0"/"-0" 合法，"007" 非法），非数字字节与超 u64 幅值拒绝；返回（十进制幅值,
/// 是否负号），值域门另由调用方按目标宽度判定
#[inline]
fn scan_digits(raw: &[u8]) -> Option<(u64, bool)> {
  let (digits, negative) = match raw {
    [b'+', rest @ ..] => (rest, false),
    [b'-', rest @ ..] => (rest, true),
    rest => (rest, false),
  };
  if digits.is_empty() || (digits.len() > 1 && digits[0] == b'0') {
    return None;
  }
  let mut number: u64 = 0;
  for &d in digits {
    if !d.is_ascii_digit() {
      return None;
    }
    number = number.checked_mul(10)?.checked_add(u64::from(d - b'0'))?;
  }
  Some((number, negative))
}

/// C# 严格整数解析（对照 RespReadUtils.TryReadInt64Safe allowLeadingZeros: false 语义）：
/// 可选 +/- 号；首数字 '0' 且后续仍有数字即拒绝（"0"/"-0" 合法，"007" 非法）；
/// 须为纯数字且整体消费；负值域至 i64::MIN；溢出返回 None
#[inline]
pub fn strict_i64(raw: &[u8]) -> Option<i64> {
  let (number, negative) = scan_digits(raw)?;
  if negative {
    if number == i64::MIN.unsigned_abs() {
      Some(i64::MIN)
    } else {
      let positive = i64::try_from(number).ok()?;
      Some(-positive)
    }
  } else {
    i64::try_from(number).ok()
  }
}

/// 同 [`strict_i64`] 的 i32 值域版（C# int.MaxValue 上限语义）
#[inline]
pub fn strict_i32(raw: &[u8]) -> Option<i32> {
  i32::try_from(strict_i64(raw)?).ok()
}

/// 同 [`strict_i64`] 的 u64 值域版（无 C# 对应：本端口多租户命名空间等
/// u64 线面字段专用，库号不在此列，见 [`parse_db_index`] 的 int32 档位）：
/// 文法走同一 [`scan_digits`] 单点，值域 0..=u64::MAX，负号仅 `-0` 归零
#[inline]
pub fn strict_u64(raw: &[u8]) -> Option<u64> {
  let (number, negative) = scan_digits(raw)?;
  (!negative || number == 0).then_some(number)
}

/// 数据库索引解析错误（档位对照 C# `TryGetInt`，见 [`parse_db_index`]）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbIndexError {
  /// 非纯数字、空串、符号后无数字、尾随垃圾或超出 int32 值域（C# TryGetInt 失败档）；
  /// 前导零拒绝系 rust 严格收口（C# TryGetInt 因 TryReadInt32Safe 死参实际放行 007，见 doc/zh/deviations.md §32）
  NotInteger,
  /// int32 域内的负整数（C# 解析成功后 `index < 0` 范围门档）
  OutOfRange,
}

/// 解析 RESP 数据库编号，线面值域收口为 C# `int`（i32）档
///
/// 对标 libs/server/Resp/Parser/SessionParseState.cs:TryGetInt →
/// libs/server/Resp/Parser/ParseUtils.cs:TryReadInt（转调
/// libs/common/RespReadUtils.cs:TryReadInt32Safe，allowLeadingZeros:false +
/// 整段消费）：C# 侧库号只有 int32 一档，超范围字面量（如 `3000000000`）在
/// C# 属「不是整数」而非「库号越界」，故值域必须先过 [`strict_i32`] 再判负，
/// 不得放宽到 u64：
/// - [`strict_i32`] 失败（非纯数字、空串、符号后无数字、尾随垃圾、超 int32 值域；
///   前导零拒绝系 rust 收口，C# TryGetInt 因 TryReadInt32Safe 死参实际放行 007，
///   见 doc/zh/deviations.md §32；`-2147483648` 是 C# 合法 int，`-2147483649` 起非法）
///   → `DbIndexError::NotInteger`
/// - int32 域内负数（`-0` 归零除外）→ `DbIndexError::OutOfRange`
/// - 0..=i32::MAX → `Ok(i32)`
///
/// 内部库 ID 仍为 u64（`active_db_id`/`set_active_db`/`slot_of`），由调用方
/// 对 `Ok` 值无损升位，本函数不参与内部表示。
#[inline]
pub fn parse_db_index(raw: &[u8]) -> Result<i32, DbIndexError> {
  match strict_i32(raw) {
    Some(v) if v >= 0 => Ok(v),
    Some(_) => Err(DbIndexError::OutOfRange),
    None => Err(DbIndexError::NotInteger),
  }
}

/// inf 词形符号判定（C# libs/common/RespReadUtils.cs:TryReadInfinity 白名单：inf/+inf/-inf，
/// 3-4 字节，大小写不敏感；infinity 全拼非法）：Some(true) → +∞，
/// Some(false) → -∞
#[inline]
fn infinity_sign(raw: &[u8]) -> Option<bool> {
  let (positive, body) = match raw {
    [s @ (b'+' | b'-'), rest @ ..] => (*s == b'+', rest),
    _ => (true, raw),
  };
  body.eq_ignore_ascii_case(b"inf").then_some(positive)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn strict_i64_strict() {
    assert_eq!(strict_i64(b"42"), Some(42));
    assert_eq!(strict_i64(b"-7"), Some(-7));
    assert_eq!(strict_i64(b"+3"), Some(3));
    assert_eq!(strict_i64(b"0"), Some(0));
    assert_eq!(strict_i64(b"-0"), Some(0));
    assert_eq!(strict_i64(b"+0"), Some(0));
    assert_eq!(strict_i64(b"9223372036854775807"), Some(i64::MAX));
    assert_eq!(strict_i64(b"-9223372036854775808"), Some(i64::MIN));
    assert_eq!(strict_i64(b"007"), None);
    assert_eq!(strict_i64(b"-007"), None);
    assert_eq!(strict_i64(b"abc"), None);
    assert_eq!(strict_i64(b""), None);
    assert_eq!(strict_i64(b"1 2"), None);
    assert_eq!(strict_i64(b" 1"), None);
    assert_eq!(strict_i64(b"5 "), None);
    assert_eq!(strict_i64(b"1x"), None);
    assert_eq!(strict_i64(b"9223372036854775808"), None);
    assert_eq!(strict_i64(b"-9223372036854775809"), None);
  }

  #[test]
  fn strict_i32_strict() {
    assert_eq!(strict_i32(b"42"), Some(42));
    assert_eq!(strict_i32(b"-7"), Some(-7));
    assert_eq!(strict_i32(b"2147483647"), Some(i32::MAX));
    assert_eq!(strict_i32(b"-2147483648"), Some(i32::MIN));
    assert_eq!(strict_i32(b"2147483648"), None);
    assert_eq!(strict_i32(b"-2147483649"), None);
    assert_eq!(strict_i32(b"007"), None);
    assert_eq!(strict_i32(b"01"), None);
  }

  #[test]
  fn strict_f64_infinity_forms() {
    // 数值溢出得的 ±inf 保留（Utf8Parser 口径）
    assert_eq!(strict_f64(b"1e999", false), Some(f64::INFINITY));
    // NaN 恒拒绝
    assert_eq!(strict_f64(b"nan", true), None);
    // inf 词形仅 can_be_infinite 放行（TryReadInfinity 白名单：3-4 字节）
    assert_eq!(strict_f64(b"inf", true), Some(f64::INFINITY));
    assert_eq!(strict_f64(b"INF", true), Some(f64::INFINITY));
    assert_eq!(strict_f64(b"+inf", true), Some(f64::INFINITY));
    assert_eq!(strict_f64(b"+Inf", true), Some(f64::INFINITY));
    assert_eq!(strict_f64(b"-inf", true), Some(f64::NEG_INFINITY));
    assert_eq!(strict_f64(b"-INF", true), Some(f64::NEG_INFINITY));
    // infinity 全拼不在白名单（C# RespReadUtils.TryReadInfinity 仅认 3/4 字节）
    assert_eq!(strict_f64(b"infinity", true), None);
    assert_eq!(strict_f64(b"+infinity", true), None);
    assert_eq!(strict_f64(b"-INFINITY", true), None);
    assert_eq!(strict_f64(b"inf", false), None);
    assert_eq!(strict_f64(b"infinity", false), None);
    assert_eq!(strict_f64(b"info", true), None);
    assert_eq!(strict_f64(b"", true), None);
  }

  #[test]
  fn strict_f32_infinity_forms() {
    assert_eq!(strict_f32(b"1e999", true), Some(f32::INFINITY));
    assert_eq!(strict_f32(b"inf", true), Some(f32::INFINITY));
    assert_eq!(strict_f32(b"-inf", true), Some(f32::NEG_INFINITY));
    assert_eq!(strict_f32(b"-infinity", true), None);
    assert_eq!(strict_f32(b"nan", true), None);
    assert_eq!(strict_f32(b"inf", false), None);
  }

  #[test]
  fn test_try_parse_integers() {
    let mut v = 0i32;
    assert!(try_parse(b"123", &mut v));
    assert_eq!(v, 123);
    assert!(!try_parse(b"1x", &mut v));
  }

  #[test]
  fn test_strict_integers() {
    // 可选符号
    assert_eq!(strict_i64(b"42"), Some(42));
    assert_eq!(strict_i64(b"-7"), Some(-7));
    assert_eq!(strict_i64(b"+3"), Some(3));
    // 单独 0 / -0 / +0 合法
    assert_eq!(strict_i64(b"0"), Some(0));
    assert_eq!(strict_i64(b"-0"), Some(0));
    assert_eq!(strict_i64(b"+0"), Some(0));
    // 值域边界（C# u64 中转语义）
    assert_eq!(strict_i64(b"9223372036854775807"), Some(i64::MAX));
    assert_eq!(strict_i64(b"-9223372036854775808"), Some(i64::MIN));
    assert_eq!(strict_i64(b"9223372036854775808"), None);
    assert_eq!(strict_i64(b"-9223372036854775809"), None);
    // 前导零拒绝
    assert_eq!(strict_i64(b"007"), None);
    assert_eq!(strict_i64(b"-007"), None);
    // 尾随垃圾 / 空白 / 非数字
    assert_eq!(strict_i64(b"abc"), None);
    assert_eq!(strict_i64(b""), None);
    assert_eq!(strict_i64(b"1 2"), None);
    assert_eq!(strict_i64(b" 1"), None);
    assert_eq!(strict_i64(b"5 "), None);
    assert_eq!(strict_i64(b"1x"), None);
    // i32 值域
    assert_eq!(strict_i32(b"2147483647"), Some(i32::MAX));
    assert_eq!(strict_i32(b"-2147483648"), Some(i32::MIN));
    assert_eq!(strict_i32(b"2147483648"), None);
    assert_eq!(strict_i32(b"-2147483649"), None);
    assert_eq!(strict_i32(b"007"), None);
    assert_eq!(strict_i32(b"01"), None);
  }

  #[test]
  fn test_strict_floats() {
    // 有限数值
    assert_eq!(strict_f64(b"3.5", false), Some(3.5));
    // 纯数值溢出的 ±inf 保留（C# Utf8Parser 首分支接受，不受白名单约束）
    assert_eq!(strict_f64(b"1e999", false), Some(f64::INFINITY));
    assert_eq!(strict_f64(b"1e999", true), Some(f64::INFINITY));
    // INF 白名单（大小写不敏感、+INF 同号），受 can_be_infinite 门控
    assert_eq!(strict_f64(b"inf", true), Some(f64::INFINITY));
    assert_eq!(strict_f64(b"INF", true), Some(f64::INFINITY));
    assert_eq!(strict_f64(b"+Inf", true), Some(f64::INFINITY));
    assert_eq!(strict_f64(b"-inf", true), Some(f64::NEG_INFINITY));
    assert_eq!(strict_f64(b"-INF", true), Some(f64::NEG_INFINITY));
    assert_eq!(strict_f64(b"inf", false), None);
    assert_eq!(strict_f64(b"+INF", false), None);
    // "Infinity" 非白名单（C# 仅 3/4 字节 INF 形式）
    assert_eq!(strict_f64(b"Infinity", true), None);
    // NaN 恒拒绝
    assert_eq!(strict_f64(b"nan", true), None);
    assert_eq!(strict_f64(b"NaN", false), None);
    // 非数字 / 空串
    assert_eq!(strict_f64(b"abc", true), None);
    assert_eq!(strict_f64(b"", true), None);
    // f32 同语义（TryReadInfinity float 重载同白名单）
    assert_eq!(strict_f32(b"1e999", false), Some(f32::INFINITY));
    assert_eq!(strict_f32(b"inf", true), Some(f32::INFINITY));
    assert_eq!(strict_f32(b"inf", false), None);
    assert_eq!(strict_f32(b"nan", true), None);
    assert_eq!(strict_f32(b"1e3", false), Some(1000.0));
  }

  #[test]
  fn test_parse_db_index() {
    assert_eq!(parse_db_index(b"0"), Ok(0));
    assert_eq!(parse_db_index(b"+0"), Ok(0));
    assert_eq!(parse_db_index(b"-0"), Ok(0));
    assert_eq!(parse_db_index(b"1"), Ok(1));
    assert_eq!(parse_db_index(b"+1"), Ok(1));
    assert_eq!(parse_db_index(b"15"), Ok(15));
    // C# int 上界即档位上界
    assert_eq!(parse_db_index(b"2147483647"), Ok(i32::MAX));

    // int32 域内负数：C# 解析成功、落 `index < 0` 范围门
    assert_eq!(parse_db_index(b"-1"), Err(DbIndexError::OutOfRange));
    assert_eq!(parse_db_index(b"-99"), Err(DbIndexError::OutOfRange));
    assert_eq!(
      parse_db_index(b"-2147483648"),
      Err(DbIndexError::OutOfRange)
    );

    // 非整数档：非法文法与超 int32 值域同档（前导零拒收系 rust 严格收口，C# TryGetInt 因死参放行 007，见 doc/zh/deviations.md §32）
    assert_eq!(parse_db_index(b""), Err(DbIndexError::NotInteger));
    assert_eq!(parse_db_index(b"+"), Err(DbIndexError::NotInteger));
    assert_eq!(parse_db_index(b"-"), Err(DbIndexError::NotInteger));
    assert_eq!(parse_db_index(b"abc"), Err(DbIndexError::NotInteger));
    assert_eq!(parse_db_index(b"007"), Err(DbIndexError::NotInteger));
    assert_eq!(parse_db_index(b"3000000000"), Err(DbIndexError::NotInteger));
    assert_eq!(parse_db_index(b"2147483648"), Err(DbIndexError::NotInteger));
    assert_eq!(
      parse_db_index(b"-2147483649"),
      Err(DbIndexError::NotInteger)
    );
    assert_eq!(
      parse_db_index(b"18446744073709551616"),
      Err(DbIndexError::NotInteger)
    );
  }

  #[test]
  fn strict_u64_matches_strict_i64_grammar() {
    assert_eq!(strict_u64(b"0"), Some(0));
    assert_eq!(strict_u64(b"+0"), Some(0));
    assert_eq!(strict_u64(b"-0"), Some(0));
    assert_eq!(strict_u64(b"42"), Some(42));
    assert_eq!(strict_u64(b"+42"), Some(42));
    assert_eq!(strict_u64(b"18446744073709551615"), Some(u64::MAX));
    // 负数与超 u64 值域、前导零、尾随垃圾一律拒
    assert_eq!(strict_u64(b"-1"), None);
    assert_eq!(strict_u64(b"007"), None);
    assert_eq!(strict_u64(b"1x"), None);
    assert_eq!(strict_u64(b"5 "), None);
    assert_eq!(strict_u64(b""), None);
    assert_eq!(strict_u64(b"18446744073709551616"), None);
  }
}
