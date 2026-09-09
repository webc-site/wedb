#![allow(clippy::module_inception)]
#[cfg(test)]
mod tests {
  use crate::*;

  #[test]
  fn test_ascii() {
    assert!(ascii::is_between(b'B', b'A', b'Z'));
    assert!(!ascii::is_between(b'a', b'A', b'Z'));
    assert_eq!(ascii::to_lower(b'A'), b'a');
    assert_eq!(ascii::to_upper(b'b'), b'B');

    let mut cmd = b"hELLo".to_vec();
    ascii::to_upper_in_place(&mut cmd);
    assert_eq!(cmd, b"HELLO");

    let mut cmd = b"hELLo".to_vec();
    ascii::to_lower_in_place(&mut cmd);
    assert_eq!(cmd, b"hello");
  }

  #[test]
  fn test_num() {
    let mut is_neg = false;
    assert_eq!(num::count_digits(12345, &mut is_neg), 5);
    assert!(!is_neg);

    assert_eq!(num::count_digits(-987, &mut is_neg), 3);
    assert!(is_neg);

    let mut i32_val = 0;
    assert!(num::try_parse_i32(b"123", &mut i32_val));
    assert_eq!(i32_val, 123);

    let mut f64_val: f64 = 0.0;
    assert!(num::try_parse_with_infinity(b"+inf", &mut f64_val));
    assert!(f64_val.is_infinite() && f64_val.is_sign_positive());

    // 常规位提取：最低置位偏移并原位清除
    let mut bits = 0b0110u64;
    assert_eq!(num::get_next_offset(&mut bits), 1);
    assert_eq!(bits, 0b0100);

    // value == 0 边界：trailing_zeros 为 64，不得触发移位溢出 panic（对齐 C# 1UL<<64 取模语义）
    let mut zero = 0u64;
    assert_eq!(num::get_next_offset(&mut zero), 64);
    assert_eq!(zero, 0);
  }

  #[test]
  fn test_convert() {
    let ticks = convert::unix_timestamp_in_seconds_to_ticks(1600000000);
    assert_eq!(convert::unix_time_in_seconds_from_ticks(ticks), 1600000000);
  }

  /// CountCharsInDouble 与 C# 语义对拍：银行家舍入 + 2*Double.Epsilon 容差
  /// （任意非零差值即继续细化，fractionalDigits 上限 15）
  #[test]
  fn test_count_chars_in_double() {
    let count = |v: f64| {
      let mut int_digits = 0;
      let mut sign = 0u8;
      let mut frac = 0;
      num::count_chars_in_double(v, &mut int_digits, &mut sign, &mut frac);
      (int_digits, sign, frac)
    };

    // 零值快速路径
    assert_eq!(count(0.0), (1, 0, 0));
    // 符号位
    assert_eq!(count(-3.25), (1, 1, 2));
    // 123.456 精确 3 位小数收敛
    assert_eq!(count(123.456), (3, 0, 3));
    // 整数值无小数位
    assert_eq!(count(42.0), (2, 0, 0));
    // 0.30000000000000004 与 0.3 的差值非零：C# 容差下持续细化至 15 位上限
    assert_eq!(count(0.30000000000000004).2, 15);
    // 0.5 在第 1 位小数即精确收敛
    assert_eq!(count(0.5), (1, 0, 1));
    // 负零按零值处理（value == 0.0 对 -0.0 成立）
    assert_eq!(count(-0.0), (1, 0, 0));
  }

  #[test]
  fn test_hash_slot() {
    assert_eq!(
      hash_slot::hash_slot(b"123456789"),
      0x31C3 & hash_slot::HASH_SLOT_MAX
    );
    assert_eq!(
      hash_slot::hash_slot(b"key{user1}data"),
      hash_slot::hash_slot(b"user1")
    );
  }

  #[test]
  fn test_hash() {
    let h = hash::murmur_hash3_x64_a(b"test", 0);
    assert_ne!(h, 0);

    let (h1, h2) = hash::murmur_hash3_x128(b"test123456789012", 0);
    assert_ne!(h1, 0);
    assert_ne!(h2, 0);

    let h2_64 = hash::murmur_hash2_x64_a(b"test", 0);
    assert_ne!(h2_64, 0);
  }

  #[test]
  fn test_crc64() {
    let h = crc64::hash(b"123456789");
    assert_eq!(h.len(), 8);
  }
}
