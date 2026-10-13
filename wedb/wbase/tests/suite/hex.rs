//! hex 微工具行为测试（自 src/hex.rs 内联 mod tests 迁出）
//!
//! hex_encode / hex_encode_into 已随 crate 外零生产消费裁撤（定长出口
//! hex_encode_20 / hex_encode_32 覆盖全部在用形态），hex_val 降 pub(crate)
//! 后其折值语义经 hex_decode / hex_u128 的 roundtrip 断言间接锁定。

use wbase::hex::{
  HEX_CHARS_LOWER, HEX_CHARS_UPPER, generate_hex_id, hex_decode, hex_encode_20, hex_encode_32,
  hex_str_u128, hex_u128,
};

#[test]
fn hex_chars_len() {
  assert_eq!(HEX_CHARS_LOWER.len(), 16);
  assert_eq!(HEX_CHARS_UPPER.len(), 16);
}

#[test]
fn hex_decode_roundtrip() {
  let bytes = hex_decode::<4>(b"0aFf0102").unwrap();
  assert_eq!(bytes, [0x0a, 0xff, 0x01, 0x02]);

  // 长度不符
  assert!(hex_decode::<4>(b"0aFf01").is_none());
  // 非法字符
  assert!(hex_decode::<2>(b"0g").is_none());
  // 空输入定长 0
  assert_eq!(hex_decode::<0>(b"").unwrap(), [0u8; 0]);
}

#[test]
fn hex_encode_helpers() {
  let raw20 = [0xabu8; 20];
  let enc20 = hex_encode_20(&raw20);
  assert_eq!(&enc20[..4], b"abab");
  assert_eq!(hex_decode::<20>(&enc20).unwrap(), raw20);

  let raw32 = [0x0fu8; 32];
  let enc32 = hex_encode_32(&raw32);
  assert_eq!(&enc32[..4], b"0f0f");
  assert_eq!(hex_decode::<32>(&enc32).unwrap(), raw32);
}

#[test]
fn hex_u128_roundtrip() {
  let v = 0x0123_4567_89ab_cdef_0fed_cba9_8765_4321u128;
  // hex_encode_u128 为 pub(crate) 内核，经 pub 出口 hex_str_u128 锁定编码形态
  let enc = hex_str_u128(v);
  assert_eq!(&enc[..2], "01");
  assert_eq!(&enc[30..], "21");
  assert_eq!(hex_u128(enc.as_bytes()), Some(v));

  // 大写可解码
  assert_eq!(hex_u128(b"0123456789ABCDEF0FEDCBA987654321"), Some(v));
  // 零
  assert_eq!(hex_str_u128(0), "00000000000000000000000000000000");
  // 长度不符与非法字符
  assert!(hex_u128(b"01").is_none());
  assert!(hex_u128(b"0g23456789abcdef0fedcba987654321").is_none());
}

#[test]
fn test_generate_hex_id() {
  let id1 = generate_hex_id();
  let id2 = generate_hex_id();
  assert_eq!(id1.len(), 40);
  assert_eq!(id2.len(), 40);
  assert!(
    id1
      .chars()
      .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
  );
  assert!(
    id2
      .chars()
      .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
  );
  assert_ne!(id1, id2, "独立随机取样不得相同");
}

// 自 tests/main.rs 迁入（原 test_hex_primitives）

#[test]
fn test_hex_primitives() {
  use wbase::hex::{HEX_CHARS_LOWER, HEX_CHARS_UPPER, hex_decode};

  let bytes = hex_decode::<2>(b"AbCd").unwrap();
  assert_eq!(bytes, [0xab, 0xcd]);
  assert!(hex_decode::<2>(b"abc").is_none());

  assert_eq!(HEX_CHARS_LOWER[10], b'a');
  assert_eq!(HEX_CHARS_UPPER[10], b'A');
}
