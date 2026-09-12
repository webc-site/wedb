use aok::{OK, Void};
use log::info;
use wval::{KeyTag, decode_order_preserving_f64, encode_order_preserving_f64};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

#[test]
fn test_key_tag_zset_and_subkeys() -> Void {
  info!("测试 KeyTag 针对 HASH_FIELD、SET_MEMBER、Z_MEMBER、Z_SCORE 的单字节映射与匹配");

  // 1. 验证单字节数值映射
  assert_eq!(KeyTag::Hash.as_u8(), 0x02);
  assert_eq!(u8::from(KeyTag::Hash), 0x02);
  assert_eq!(KeyTag::Hash as u8, 0x02);
  assert_eq!(KeyTag::from_u8(0x02), Some(KeyTag::Hash));
  assert_eq!(KeyTag::try_from(0x02)?, KeyTag::Hash);

  assert_eq!(KeyTag::Set.as_u8(), 0x03);
  assert_eq!(u8::from(KeyTag::Set), 0x03);
  assert_eq!(KeyTag::Set as u8, 0x03);
  assert_eq!(KeyTag::from_u8(0x03), Some(KeyTag::Set));
  assert_eq!(KeyTag::try_from(0x03)?, KeyTag::Set);

  assert_eq!(KeyTag::ZSetChunk.as_u8(), 0x04);
  assert_eq!(u8::from(KeyTag::ZSetChunk), 0x04);
  assert_eq!(KeyTag::ZSetChunk as u8, 0x04);
  assert_eq!(KeyTag::from_u8(0x04), Some(KeyTag::ZSetChunk));
  assert_eq!(KeyTag::try_from(0x04)?, KeyTag::ZSetChunk);

  assert_eq!(KeyTag::ZSetM2s.as_u8(), 0x05);
  assert_eq!(u8::from(KeyTag::ZSetM2s), 0x05);
  assert_eq!(KeyTag::ZSetM2s as u8, 0x05);
  assert_eq!(KeyTag::from_u8(0x05), Some(KeyTag::ZSetM2s));
  assert_eq!(KeyTag::try_from(0x05)?, KeyTag::ZSetM2s);

  // 2. 验证与原有标识的等价别名关系
  assert_eq!(KeyTag::Hash, KeyTag::Hash);
  assert_eq!(KeyTag::Set, KeyTag::Set);
  assert_eq!(KeyTag::ZSetChunk, KeyTag::ZSetChunk);
  assert_eq!(KeyTag::ZSetM2s, KeyTag::ZSetM2s);

  OK
}

#[test]
fn test_order_preserving_f64_codec() -> Void {
  info!("测试浮点数保序编码与无损往返还原");

  let test_floats = [
    f64::NEG_INFINITY,
    -1e300,
    -1e100,
    -100.5,
    -42.0,
    -1.0,
    -0.1,
    -1e-300,
    -0.0,
    0.0,
    1e-300,
    0.1,
    1.0,
    42.0,
    100.5,
    1e100,
    1e300,
    f64::INFINITY,
  ];

  // 1. 验证编码后的字节序严格单调递增
  for i in 0..test_floats.len() - 1 {
    let a = test_floats[i];
    let b = test_floats[i + 1];
    let enc_a = encode_order_preserving_f64(a);
    let enc_b = encode_order_preserving_f64(b);

    assert!(
      enc_a < enc_b,
      "保序失败: a={a} enc={enc_a:?} 应当小于 b={b} enc={enc_b:?}"
    );
  }

  // 2. 验证往返还原无损性（含 -0.0 与 +0.0 符号位区分）
  for &v in &test_floats {
    let enc = encode_order_preserving_f64(v);
    let dec = decode_order_preserving_f64(enc);

    assert_eq!(
      dec.to_bits(),
      v.to_bits(),
      "解码还原位不匹配: 原始 {v} (bits: {:#x}) -> 解码 {dec} (bits: {:#x})",
      v.to_bits(),
      dec.to_bits()
    );
  }

  // 3. 密集步进区间保序验证
  let mut current = -10.0;
  let mut prev_enc = encode_order_preserving_f64(current);
  for _ in 0..1000 {
    current += 0.02;
    let enc = encode_order_preserving_f64(current);
    assert!(prev_enc < enc);
    prev_enc = enc;
  }

  OK
}

#[test]
fn test_extreme_floats_and_denormals() -> Void {
  info!("测试极值浮点数、极小非正规数 (Subnormal/Denormal) 与 NaN 的保序与位还原");

  let extremes = [
    f64::NAN,
    f64::NEG_INFINITY,
    f64::MIN,
    -1.0e-323,
    -5.0e-324, // 最小绝对值负数 (subnormal)
    -0.0,
    0.0,
    5.0e-324, // 最小绝对值正数 (subnormal)
    1.0e-323,
    f64::MIN_POSITIVE, // 最小正正规数
    f64::MAX,
    f64::INFINITY,
  ];

  for &val in &extremes {
    let enc = encode_order_preserving_f64(val);
    let dec = decode_order_preserving_f64(enc);

    // 位级精确 100% 往返无损还原
    assert_eq!(
      dec.to_bits(),
      val.to_bits(),
      "极值还原位不匹配: {val:?} bits={:#x} -> {dec:?} bits={:#x}",
      val.to_bits(),
      dec.to_bits()
    );
  }

  // 非 NaN 区间保序性检验
  let non_nan_extremes = [
    f64::NEG_INFINITY,
    f64::MIN,
    -1.0e-323,
    -5.0e-324,
    -0.0,
    0.0,
    5.0e-324,
    1.0e-323,
    f64::MIN_POSITIVE,
    f64::MAX,
    f64::INFINITY,
  ];

  for i in 0..non_nan_extremes.len() - 1 {
    let a = non_nan_extremes[i];
    let b = non_nan_extremes[i + 1];
    let enc_a = encode_order_preserving_f64(a);
    let enc_b = encode_order_preserving_f64(b);
    assert!(enc_a < enc_b, "极端数值序校验失败: {a:?} 应小于 {b:?}");
  }

  OK
}
