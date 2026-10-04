//! 地址原语测试（48 位物理地址 / 读缓存位 / 绝对地址掩码，自 tests/main.rs 迁入）

#[test]
fn test_addr_primitives() {
  use wbase::addr::*;

  assert_eq!(ADDRESS_BITS, 48);
  assert_eq!(ADDRESS_MASK, 0x0000_FFFF_FFFF_FFFF);
  assert_eq!(READ_CACHE_BIT, 1u64 << 47);
  assert_eq!(ABSOLUTE_ADDRESS_MASK, 0x0000_7FFF_FFFF_FFFF);
  assert_eq!(INVALID_ADDRESS, 0);
  assert!(!is_valid(INVALID_ADDRESS));

  let raw = 0x1234_5678_9ABC;
  assert!(is_valid(raw));
  assert!(!is_read_cache(raw));
  assert_eq!(to_absolute(raw), raw);

  let rc_addr = with_read_cache(raw);
  assert!(is_read_cache(rc_addr));
  assert_eq!(to_absolute(rc_addr), raw);
}
