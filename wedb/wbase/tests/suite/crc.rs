//! CRC-32 原语测试（标准校验向量，自 tests/main.rs 迁入）

#[test]
fn test_crc_primitives() {
  use wbase::crc::*;

  let data = b"123456789";
  let expected = 0xCBF4_3926; // 标准 CRC-32 校验向量
  assert_eq!(crc32(data), expected);

  let mut hasher = Crc32Hasher::new();
  hasher.update(b"12345");
  hasher.update(b"6789");
  assert_eq!(hasher.finalize(), expected);

  let mut h2 = Crc32Hasher::new();
  h2.update_u64(0x0102_0304_0506_0708);
  assert_ne!(h2.finalize(), 0);
}
