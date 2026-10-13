use wbase::num::*;

#[test]
fn test_try_parse_integers() {
  let mut v = 0i32;
  assert!(try_parse(b"123", &mut v));
  assert_eq!(v, 123);
  assert!(!try_parse(b"1x", &mut v));
}

#[test]
fn test_strict_integers() {
  // 可选符号与正负整数
  assert_eq!(strict_i64(b"42"), Some(42));
  assert_eq!(strict_i64(b"-7"), Some(-7));
  assert_eq!(strict_i64(b"+3"), Some(3));
  // 单独 0 / -0 / +0 合法
  assert_eq!(strict_i64(b"0"), Some(0));
  assert_eq!(strict_i64(b"-0"), Some(0));
  assert_eq!(strict_i64(b"+0"), Some(0));
  // 值域边界
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
  assert_eq!(strict_i32(b"42"), Some(42));
  assert_eq!(strict_i32(b"-7"), Some(-7));
  assert_eq!(strict_i32(b"2147483647"), Some(i32::MAX));
  assert_eq!(strict_i32(b"-2147483648"), Some(i32::MIN));
  assert_eq!(strict_i32(b"2147483648"), None);
  assert_eq!(strict_i32(b"-2147483649"), None);
  assert_eq!(strict_i32(b"007"), None);
  assert_eq!(strict_i32(b"01"), None);

  // u64 语法与边界
  assert_eq!(strict_u64(b"0"), Some(0));
  assert_eq!(strict_u64(b"+0"), Some(0));
  assert_eq!(strict_u64(b"-0"), Some(0));
  assert_eq!(strict_u64(b"42"), Some(42));
  assert_eq!(strict_u64(b"+42"), Some(42));
  assert_eq!(strict_u64(b"18446744073709551615"), Some(u64::MAX));
  assert_eq!(strict_u64(b"-1"), None);
  assert_eq!(strict_u64(b"007"), None);
  assert_eq!(strict_u64(b"1x"), None);
  assert_eq!(strict_u64(b"5 "), None);
  assert_eq!(strict_u64(b""), None);
  assert_eq!(strict_u64(b"18446744073709551616"), None);
}

#[test]
fn test_strict_floats() {
  // 有限数值
  assert_eq!(strict_f64(b"3.5", false), Some(3.5));
  // 纯数值溢出的 ±inf 保留
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
  // "Infinity" 全拼非法
  assert_eq!(strict_f64(b"Infinity", true), None);
  // NaN 恒拒绝
  assert_eq!(strict_f64(b"nan", true), None);
  assert_eq!(strict_f64(b"NaN", false), None);
  // 非数字 / 空串
  assert_eq!(strict_f64(b"abc", true), None);
  assert_eq!(strict_f64(b"", true), None);

  // f32 同语义
  assert_eq!(strict_f32(b"1e999", false), Some(f32::INFINITY));
  assert_eq!(strict_f32(b"inf", true), Some(f32::INFINITY));
  assert_eq!(strict_f32(b"-inf", true), Some(f32::NEG_INFINITY));
  assert_eq!(strict_f32(b"-infinity", true), None);
  assert_eq!(strict_f32(b"nan", true), None);
  assert_eq!(strict_f32(b"inf", false), None);
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
  assert_eq!(parse_db_index(b"2147483647"), Ok(i32::MAX));

  // int32 域内负数：OutOfRange
  assert_eq!(parse_db_index(b"-1"), Err(DbIndexError::OutOfRange));
  assert_eq!(parse_db_index(b"-99"), Err(DbIndexError::OutOfRange));
  assert_eq!(
    parse_db_index(b"-2147483648"),
    Err(DbIndexError::OutOfRange)
  );

  // 非整数档：NotInteger
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
