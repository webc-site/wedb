//! TTL 同步旁路记录编解码集成测试（I64Codec 契约）
//!
//! 覆盖 .NET Ticks 与 ETag 共享的 8 字节大端定长编解码契约：
//! 1. 经典时间戳、0、-1、i64::MIN、i64::MAX 等极限值往返恒等
//! 2. 大端字节序物理布局断言（与 C# Garnet 旁路记录格式对齐）
//! 3. 不足 8 字节切片解码安全返回 None，超长切片取前 8 字节前缀
//! 4. const fn 编译期求值能力验证

use wval::I64Codec;

/// 典型 .NET Ticks 时间戳编解码往返
#[test]
fn test_ttl_codec_roundtrip_normal_ticks() {
  let ticks = 638_600_000_000_000_000_i64;
  let bytes = I64Codec::encode(ticks);
  assert_eq!(I64Codec::decode(&bytes), Some(ticks));
}

/// 边界与极值编解码往返 (0, -1, i64::MIN, i64::MAX 等)
#[test]
fn test_ttl_codec_boundary_values() {
  let test_cases = [
    0_i64,
    1_i64,
    -1_i64,
    i64::MIN,
    i64::MAX,
    i64::MIN + 1,
    i64::MAX - 1,
    balance_case(),
    -123_456_789_012_345_i64,
  ];

  for val in test_cases {
    let bytes = I64Codec::encode(val);
    assert_eq!(
      I64Codec::decode(&bytes),
      Some(val),
      "值 {val} 编解码往返失败"
    );
  }
}

const fn balance_case() -> i64 {
  0x0123_4567_89AB_CDEF_u64 as i64
}

/// 大端字节序精确断言
#[test]
fn test_ttl_codec_endian_layout() {
  let val = 0x0102_0304_0506_0708_i64;
  let bytes = I64Codec::encode(val);
  assert_eq!(bytes, [1, 2, 3, 4, 5, 6, 7, 8]);
  assert_eq!(I64Codec::decode(&bytes), Some(val));
}

/// 非法切片及前缀切片容错解码
#[test]
fn test_ttl_codec_invalid_and_prefix_slices() {
  // 空切片
  assert_eq!(I64Codec::decode(&[]), None);
  // 1 ~ 7 字节短切片均判定为非法/无 TTL
  for len in 1..8 {
    let short_buf = vec![0u8; len];
    assert_eq!(
      I64Codec::decode(&short_buf),
      None,
      "长度 {len} 短切片未安全返回 None"
    );
  }
  // 超长切片取前 8 字节前缀进行解码
  let mut extended_buf = vec![0u8; 16];
  let val = 0x1122_3344_5566_7788_i64;
  extended_buf[..8].copy_from_slice(&I64Codec::encode(val));
  extended_buf[8..].fill(0xFF);
  assert_eq!(I64Codec::decode(&extended_buf), Some(val));
}

/// const fn 编译期编解码静态断言
#[test]
fn test_ttl_codec_const_eval() {
  const TICKS: i64 = 638_600_000_000_000_000_i64;
  const ENCODED: [u8; 8] = I64Codec::encode(TICKS);
  const DECODED: Option<i64> = I64Codec::decode(&ENCODED);

  assert_eq!(DECODED, Some(TICKS));
}
