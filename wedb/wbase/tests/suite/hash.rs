//! murmur 哈希原语测试（自 tests/main.rs 迁入）

#[test]
fn test_hash_primitives() {
  use wbase::hash::*;

  let h1 = murmur_hash2_x64_a(b"test", 0);
  assert_eq!(h1, murmur_hash2_x64_a(b"test", 0));
  assert_eq!(murmur_hash2_x64_a(b"", 0), 0);
  assert_ne!(murmur_hash2_x64_a(b"abc", 0), murmur_hash2_x64_a(b"def", 0));
}
