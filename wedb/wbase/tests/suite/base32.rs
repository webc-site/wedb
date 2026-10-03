//! Base32hex 编解码测试（u64 / u128 编码、保序性、防御性解码，自 tests/main.rs 迁入）

#[test]
fn test_base32_primitives() {
  use std::{ffi::OsStr, path::Path};

  use wbase::base32::*;

  // 0. 常量验证
  assert_eq!(BASE32_LEN_U64, 13);
  assert_eq!(BASE32_LEN_U128, 26);
  assert_eq!(BASE32_LOWER_TABLE.len(), 32);

  // 1. u64 编码与解码
  let val64 = 0x0123_4567_89ab_cdef_u64;
  let b32_64 = encode_u64(val64);
  assert_eq!(b32_64.len(), BASE32_LEN_U64);
  assert_eq!(decode_u64(&b32_64), Some(val64));
  // 零与极值
  assert_eq!(decode_u64(&encode_u64(0)), Some(0));
  assert_eq!(decode_u64(&encode_u64(u64::MAX)), Some(u64::MAX));
  // 大写容错解码
  let upper = b32_64.as_str().to_ascii_uppercase();
  assert_eq!(decode_u64(&upper), Some(val64));
  // AsRef 与 Deref 转换
  assert_eq!(b32_64.as_ref() as &Path, Path::new(b32_64.as_str()));
  assert_eq!(b32_64.as_ref() as &OsStr, OsStr::new(b32_64.as_str()));
  assert_eq!(b32_64.as_ref() as &str, b32_64.as_str());
  assert_eq!(b32_64.as_ref() as &[u8], b32_64.as_bytes());
  assert_eq!(&*b32_64, b32_64.as_str());
  // PartialEq 跨类型对比
  assert_eq!(b32_64, b32_64.as_str());
  assert_eq!(b32_64.as_str(), b32_64);
  assert_eq!(format!("{b32_64}"), b32_64.as_str());
  assert_eq!(format!("{b32_64:?}"), b32_64.as_str());

  // 2. u128 编码与解码
  let val128 = 0x0123_4567_89ab_cdef_fedc_ba98_7654_3210_u128;
  let b32_128 = encode_u128(val128);
  assert_eq!(b32_128.len(), BASE32_LEN_U128);
  assert_eq!(decode_u128(&b32_128), Some(val128));
  assert_eq!(decode_u128(&encode_u128(0)), Some(0));
  assert_eq!(decode_u128(&encode_u128(u128::MAX)), Some(u128::MAX));
  let upper128 = b32_128.as_str().to_ascii_uppercase();
  assert_eq!(decode_u128(&upper128), Some(val128));
  // AsRef 与 Deref 转换
  assert_eq!(b32_128.as_ref() as &Path, Path::new(b32_128.as_str()));
  assert_eq!(b32_128.as_ref() as &OsStr, OsStr::new(b32_128.as_str()));
  assert_eq!(b32_128.as_ref() as &str, b32_128.as_str());
  assert_eq!(b32_128.as_ref() as &[u8], b32_128.as_bytes());
  assert_eq!(&*b32_128, b32_128.as_str());
  assert_eq!(b32_128, b32_128.as_str());
  assert_eq!(b32_128.as_str(), b32_128);
  assert_eq!(format!("{b32_128}"), b32_128.as_str());
  assert_eq!(format!("{b32_128:?}"), b32_128.as_str());

  // 3. 严格保序性测试（数值递增 == 字符串字典序递增）
  let s1 = encode_u64(100);
  let s2 = encode_u64(101);
  let s3 = encode_u64(0xFFFF_FFFF_0000_0000);
  let s4 = encode_u64(0xFFFF_FFFF_0000_0001);
  assert!(s1.as_str() < s2.as_str());
  assert!(s2.as_str() < s3.as_str());
  assert!(s3.as_str() < s4.as_str());
  assert!(s1 < s2 && s2 < s3 && s3 < s4);

  let u1 = encode_u128(100);
  let u2 = encode_u128(101);
  let u3 = encode_u128(0xFFFF_FFFF_0000_0000_FFFF_FFFF_0000_0000);
  let u4 = encode_u128(0xFFFF_FFFF_0000_0000_FFFF_FFFF_0000_0001);
  assert!(u1.as_str() < u2.as_str());
  assert!(u2.as_str() < u3.as_str());
  assert!(u3.as_str() < u4.as_str());
  assert!(u1 < u2 && u2 < u3 && u3 < u4);

  // 4. 校验器（仅测试内保留校验逻辑）
  let is_base32 = |s: &str| {
    s.as_bytes()
      .iter()
      .all(|&b| BASE32_LOWER_TABLE.contains(&b.to_ascii_lowercase()))
  };
  assert!(is_base32(""));
  assert!(is_base32(b32_64.as_str()));
  assert!(is_base32("0123456789abcdefghijklmnopqrstuv"));
  assert!(is_base32("0123456789ABCDEFGHIJKLMNOPQRSTUV"));
  assert!(!is_base32("w")); // w 不在 Base32hex 字符集内
  assert!(!is_base32("xyz")); // x, y, z 不是 Base32hex 字符 (只有 0..=v)
  assert!(!is_base32("WXYZ"));
  assert!(!is_base32("0123 4567"));
  assert!(!is_base32("0123-4567"));

  // 5. 防溢出与异常长度防御断言
  // decode_u64: 长度非 13
  assert_eq!(decode_u64(""), None);
  assert_eq!(decode_u64("000000000000"), None); // 12 字符
  assert_eq!(decode_u64("00000000000000"), None); // 14 字符
  // decode_u64: 首字符高位溢出 (0x0F 以上为非法，'g' 为 16，'v' 为 31)
  assert_eq!(decode_u64("g000000000000"), None);
  assert_eq!(decode_u64("v000000000000"), None);
  assert_eq!(decode_u64("fvvvvvvvvvvvv"), Some(u64::MAX)); // 恰好最大值
  // decode_u64: 非法字符
  assert_eq!(decode_u64("000000000000w"), None);
  assert_eq!(decode_u64("000000000000z"), None);
  assert_eq!(decode_u64("000000-000000"), None);

  // decode_u128: 长度非 26
  assert_eq!(decode_u128(""), None);
  assert_eq!(decode_u128(&"0".repeat(25)), None);
  assert_eq!(decode_u128(&"0".repeat(27)), None);
  // decode_u128: 首字符高位溢出 (0x07 以上为非法，'8' 为 8，'v' 为 31)
  assert_eq!(decode_u128(&format!("8{}", "0".repeat(25))), None);
  assert_eq!(decode_u128(&format!("a{}", "0".repeat(25))), None);
  assert_eq!(decode_u128(&format!("v{}", "0".repeat(25))), None);
  assert_eq!(
    decode_u128(&format!("7{}", "v".repeat(25))),
    Some(u128::MAX)
  ); // 恰好最大值
  // decode_u128: 非法字符
  assert_eq!(decode_u128(&format!("{}w", "0".repeat(25))), None);
  assert_eq!(decode_u128(&format!("{}z", "0".repeat(25))), None);
}
