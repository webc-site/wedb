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
  assert_eq!(convert::unix_time_in_seconds_from_ticks(-1), -1);
  assert_eq!(convert::unix_time_in_seconds_from_ticks(0), -1);

  let ms_ticks = convert::unix_timestamp_in_milliseconds_to_ticks(1600000000123);
  assert_eq!(
    convert::unix_time_in_milliseconds_from_ticks(ms_ticks),
    1600000000123
  );
  assert_eq!(convert::unix_time_in_milliseconds_from_ticks(-1), -1);
  assert_eq!(convert::unix_time_in_milliseconds_from_ticks(0), -1);

  let now_ticks = 100_000_000;
  assert_eq!(
    convert::seconds_from_diff_ticks(now_ticks + 15_000_000, now_ticks),
    2
  );
  assert_eq!(
    convert::seconds_from_diff_ticks(now_ticks + 14_999_999, now_ticks),
    1
  );
  assert_eq!(convert::seconds_from_diff_ticks(now_ticks, now_ticks), -1);
  assert_eq!(convert::seconds_from_diff_ticks(-1, now_ticks), -1);

  assert_eq!(
    convert::milliseconds_from_diff_ticks(now_ticks + 50_000, now_ticks),
    5
  );
  assert_eq!(
    convert::milliseconds_from_diff_ticks(now_ticks, now_ticks),
    -1
  );
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
