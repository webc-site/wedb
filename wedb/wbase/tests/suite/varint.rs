//! OPPV 保序变长整数测试（边界长度、字典序保序、防御性解码，自 tests/main.rs 迁入）

#[test]
fn test_varint_primitives() {
  use wbase::varint::*;

  // 1. 常量范围边界
  let boundary_samples = [
    0u64,
    1,
    127,
    128,
    129,
    16_511,
    16_512,
    16_513,
    2_113_663,
    2_113_664,
    2_113_665,
    270_549_119,
    270_549_120,
    270_549_121,
    u64::MAX - 1,
    u64::MAX,
  ];

  for &val in &boundary_samples {
    let (arr, len) = encode_u64_to_array(val);

    // 切片解码往返
    let (decoded, consumed) = decode_u64(&arr[..len]).unwrap();
    assert_eq!(decoded, val, "val={val} 解码还原不符");
    assert_eq!(consumed, len, "val={val} 消耗字节不符");

    // 写入目标切片
    let mut dst = [0u8; 16];
    let wlen = encode_u64(val, &mut dst).unwrap();
    assert_eq!(wlen, len);
    assert_eq!(&dst[..len], &arr[..len]);
  }

  // 2. 严格保序性（数值单调递增 == 二进制编码大端字典序单调递增）
  for i in 0..boundary_samples.len() - 1 {
    let a = boundary_samples[i];
    let b = boundary_samples[i + 1];
    let (arr_a, len_a) = encode_u64_to_array(a);
    let (arr_b, len_b) = encode_u64_to_array(b);
    assert!(arr_a[..len_a] < arr_b[..len_b], "保序失败: a={a} vs b={b}");
  }

  // 3. 错误与截断防御
  assert!(matches!(
    decode_u64(&[]),
    Err(VarintError::BufferTooShort {
      expected: 1,
      actual: 0
    })
  ));
  let (arr9, _) = encode_u64_to_array(1_000_000_000);
  assert!(matches!(
    decode_u64(&arr9[..5]),
    Err(VarintError::BufferTooShort { .. })
  ));

  // 4. 非规范编码防御（9字节形式但数值小于 270_549_120）
  let mut non_canonical = [0u8; 9];
  non_canonical[0] = VARINT_9B_MARKER;
  non_canonical[1..9].copy_from_slice(&100u64.to_be_bytes());
  assert_eq!(decode_u64(&non_canonical), Err(VarintError::NonCanonical));
}
