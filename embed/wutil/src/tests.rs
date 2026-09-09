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
  }

  #[test]
  fn test_convert() {
    let ticks = convert::unix_timestamp_in_seconds_to_ticks(1600000000);
    assert_eq!(convert::unix_time_in_seconds_from_ticks(ticks), 1600000000);
  }

  #[test]
  fn test_hash_slot() {
    assert_eq!(hash_slot::hash_slot(b"123456789"), 0x31C3 & 16383);
    assert_eq!(
      hash_slot::hash_slot(b"key{user1}data"),
      hash_slot::hash_slot(b"user1")
    );
  }
}
