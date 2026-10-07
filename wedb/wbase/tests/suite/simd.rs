//! SIMD 键比对原语测试（fast_key_eq / first_masked_eq / masked_eq，自 tests/main.rs 迁入）

#[test]
fn test_simd_fast_key_eq() {
  use wbase::simd::fast_key_eq;

  assert!(fast_key_eq(b"", b""));
  assert!(fast_key_eq(b"hello", b"hello"));
  assert!(!fast_key_eq(b"hello", b"world"));
  assert!(!fast_key_eq(b"short", b"shorter"));

  // 16 字节对齐与长键测试
  let k1 = b"0123456789abcdef_long_key_vector";
  let k2 = b"0123456789abcdef_long_key_vector";
  let k3 = b"0123456789abcdef_long_key_vectoX";
  assert!(fast_key_eq(k1, k2));
  assert!(!fast_key_eq(k1, k3));
}

/// `first_masked_eq`：一次 16B 载入 + 逐组掩码与 + 组内整向量全等的命中核
///
/// 向量核（aarch64 NEON / x86_64 SSE2 起步）与非向量目标的逐字节回落须逐输入
/// 同判，故按伪随机语料与标量参考实现全等校验
#[test]
fn test_simd_first_masked_eq() {
  use wbase::simd::{MaskedGroup, first_masked_eq};

  const fn mask(len: usize) -> [u8; 16] {
    let mut m = [0u8; 16];
    let mut i = 0;
    while i < len {
      m[i] = 0xFF;
      i += 1;
    }
    m
  }

  // 13 字节档两项（尾 3 字节由掩码清零）、14 字节档一项、16 字节全等档一项
  static CAND_13: [[u8; 16]; 2] = [*b"*2\r\n$3\r\nGET\r\n\0\0\0", *b"*3\r\n$3\r\nSET\r\n\0\0\0"];
  static CAND_14: [[u8; 16]; 1] = [*b"*1\r\n$4\r\nPING\r\n\0\0"];
  static CAND_16: [[u8; 16]; 1] = [*b"*2\r\n$6\r\nEXISTS\r\n"];

  let groups = [
    MaskedGroup {
      mask: Some(mask(13)),
      candidates: &CAND_13,
      base: 0,
    },
    MaskedGroup {
      mask: Some(mask(14)),
      candidates: &CAND_14,
      base: 2,
    },
    MaskedGroup {
      mask: None,
      candidates: &CAND_16,
      base: 3,
    },
  ];

  // 命中回传全局序位（base + 组内下标），模式长度之后的输入字节不作约束
  assert_eq!(first_masked_eq(b"*2\r\n$3\r\nGET\r\njun", &groups), Some(0));
  assert_eq!(
    first_masked_eq(b"*3\r\n$3\r\nSET\r\n\x00\x00\x00", &groups),
    Some(1)
  );
  assert_eq!(first_masked_eq(b"*1\r\n$4\r\nPING\r\nXY", &groups), Some(2));
  assert_eq!(first_masked_eq(b"*2\r\n$6\r\nEXISTS\r\n", &groups), Some(3));
  // 16 字节档无掩码：末字节差异即失配
  assert_eq!(first_masked_eq(b"*2\r\n$6\r\nEXISTS\rX", &groups), None);
  // 掩码宽度内的差异即失配（13 字节档的第 13 字节属掩码内）
  assert_eq!(first_masked_eq(b"*2\r\n$3\r\nGET\rXjun", &groups), None);
  // 空候选组 / 空组集不参与比较
  assert_eq!(first_masked_eq(b"*2\r\n$3\r\nGET\r\njun", &[]), None);
  assert_eq!(
    first_masked_eq(
      b"*2\r\n$3\r\nGET\r\njun",
      &[MaskedGroup {
        mask: Some(mask(13)),
        candidates: &[],
        base: 9,
      }],
    ),
    None
  );

  // 标量参考实现：逐字节 (输入 & 掩码) == 候选，按组序与组内序首中
  fn reference(input: &[u8; 16], groups: &[MaskedGroup<'_>]) -> Option<usize> {
    for group in groups {
      for (idx, candidate) in group.candidates.iter().enumerate() {
        let hit = match group.mask {
          Some(mask) => (0..16).all(|i| (input[i] & mask[i]) == candidate[i]),
          None => input == candidate,
        };
        if hit {
          return Some(group.base + idx);
        }
      }
    }
    None
  }

  // 伪随机语料（LCG，进程内确定性）逐输入与参考实现同判，
  // 覆盖帧形态、掩码跨界字节差异与全随机噪声
  let mut state = 0x2545_F491_4F6C_DD1Du64;
  let mut next = move || {
    state = state
      .wrapping_mul(6364136223846793005)
      .wrapping_add(1442695040888963407);
    (state >> 33) as u8
  };
  for frame in [
    &b"*2\r\n$3\r\nGET\r\njun"[..],
    b"*3\r\n$3\r\nSET\r\n\xFF\xFF\xFF",
    b"*1\r\n$4\r\nPING\r\n\x00\x00",
    b"*2\r\n$6\r\nEXISTS\r\n",
    b"*9\r\n$9\r\nBOGUSBOG",
  ] {
    for _ in 0..200 {
      let mut input = [0u8; 16];
      input[..frame.len()].copy_from_slice(frame);
      for byte in &mut input {
        if *byte == 0 {
          *byte = next();
        }
      }
      // 随机翻转单字节 → 差异可落在掩码内或掩码外
      let flip = (next() as usize) % 16;
      input[flip] ^= 1 << (next() & 7);
      assert_eq!(
        first_masked_eq(&input, &groups),
        reference(&input, &groups),
        "输入 {input:?} 的掩码全等判定与标量参考不符"
      );
    }
  }
  for _ in 0..500 {
    let mut input = [0u8; 16];
    for byte in &mut input {
      *byte = next();
    }
    assert_eq!(first_masked_eq(&input, &groups), reference(&input, &groups));
  }
}

/// `masked_eq`：单候选一次 16B 载入 + 一次掩码与 + 整向量全等（会话 MRU 槽形态）
///
/// 向量核与非向量目标回落须逐输入同判，故按逐字节位翻转与伪随机语料和标量参考
/// 实现全等校验；掩码宽度之外的输入字节不参与判定
#[test]
fn test_simd_masked_eq() {
  use wbase::simd::masked_eq;

  const fn mask(len: usize) -> [u8; 16] {
    let mut m = [0u8; 16];
    let mut i = 0;
    while i < len {
      m[i] = 0xFF;
      i += 1;
    }
    m
  }
  // 标量参考：逐字节 (输入 & 掩码) == 模式
  fn reference(input: &[u8; 16], mask: &[u8; 16], pattern: &[u8; 16]) -> bool {
    (0..16).all(|i| (input[i] & mask[i]) == pattern[i])
  }

  // 14 字节帧入槽（ECHO 一类）：模式尾部零填，消费长度之外由掩码清零
  let mut pattern = [0u8; 16];
  pattern[..14].copy_from_slice(b"*2\r\n$4\r\nECHO\r\n");

  // 掩码宽度内的逐字节位翻转：命中态与标量参考逐输入同判（宽度外翻转为噪声）
  for width in [13usize, 14, 15, 16] {
    let slot_mask = mask(width);
    for position in 0..16 {
      for bit in 0..8 {
        let mut input = pattern;
        if position < width {
          // 掩码内翻转：模式字节被改写 → 两臂须同判失配
          input[position] ^= 1 << bit;
        } else {
          // 掩码外翻转：填成与之不同的杂字节 → 两臂须同判命中
          input[position] = !pattern[position];
        }
        assert_eq!(
          masked_eq(&input, &slot_mask, &pattern),
          reference(&input, &slot_mask, &pattern),
          "宽度 {width} 第 {position} 字节翻第 {bit} 位"
        );
      }
    }
  }

  // 锚点：帧 + 掩码宽度之外的杂字节仍命中（14 字节档），同窗口在 16 字节档失配
  let mut noisy = [0u8; 16];
  noisy[..14].copy_from_slice(b"*2\r\n$4\r\nECHO\r\n");
  noisy[15] = b'Z';
  assert!(
    masked_eq(&noisy, &mask(14), &pattern),
    "掩码外杂字节不作约束"
  );
  assert!(
    !masked_eq(&noisy, &mask(16), &pattern),
    "16 字节档须整窗全等"
  );

  // 伪随机语料（LCG，进程内确定性）：全随机与帧嫁接两类窗口均与参考同判
  let mut state = 0x853C_49E6_748F_A39Du64;
  let mut next = move || {
    state = state
      .wrapping_mul(6364136223846793005)
      .wrapping_add(1442695040888963407);
    (state >> 33) as u8
  };
  let masks = [mask(13), mask(14), mask(15), mask(16)];
  for _ in 0..4096 {
    let mut input = [0u8; 16];
    for byte in &mut input {
      *byte = next();
    }
    for mask in masks {
      assert_eq!(
        masked_eq(&input, &mask, &pattern),
        reference(&input, &mask, &pattern),
        "全随机输入 {input:?}"
      );
    }
  }
  for _ in 0..4096 {
    let mut input = [0u8; 16];
    for byte in &mut input {
      *byte = next();
    }
    let keep = (next() as usize) % 17;
    input[..keep.min(14)].copy_from_slice(&pattern[..keep.min(14)]);
    for mask in masks {
      assert_eq!(
        masked_eq(&input, &mask, &pattern),
        reference(&input, &mask, &pattern),
        "前缀嫁接输入 {input:?}"
      );
    }
  }
}
