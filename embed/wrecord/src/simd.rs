pub use wbase::simd::fast_key_eq;

#[cfg(test)]
mod tests {
  use super::fast_key_eq;

  #[test]
  fn test_fast_key_eq() {
    // 1. 空切片边界测试（同指针与不同指针）
    assert!(fast_key_eq(b"", b""));
    let empty_a: &[u8] = &[];
    let empty_b: &[u8] = &[];
    assert!(fast_key_eq(empty_a, empty_b));

    // 2. 基础切片测试
    assert!(fast_key_eq(b"hello", b"hello"));
    assert!(!fast_key_eq(b"hello", b"world"));
    assert!(!fast_key_eq(b"short", b"shorter"));

    // 3. 重叠内存切片比对（同一缓冲区不同偏移）
    let overlap_buf = b"0123456789abcdef0123456789abcdef";
    assert!(fast_key_eq(&overlap_buf[0..16], &overlap_buf[16..32]));
    assert!(!fast_key_eq(&overlap_buf[0..16], &overlap_buf[1..17]));
    assert!(fast_key_eq(&overlap_buf[0..8], &overlap_buf[16..24]));
    assert!(!fast_key_eq(&overlap_buf[0..8], &overlap_buf[1..9]));

    // 4. 遍历 1..=64 字节所有步长，且穷举变异每一个位置 (0..len)
    for len in 1..=64 {
      let v1 = vec![0x5Au8; len];
      let v2 = vec![0x5Au8; len];
      assert!(fast_key_eq(&v1, &v2));

      // 穷举变异每一个位置
      for pos in 0..len {
        let mut v3 = v1.clone();
        v3[pos] ^= 0xFF;
        assert!(
          !fast_key_eq(&v1, &v3),
          "fast_key_eq 应检测出 len={len} 在 pos={pos} 处的差异"
        );
      }
    }

    // 5. 超长切片（128B, 256B, 1024B）极端与跨步长差异比对
    for &long_len in &[128, 256, 1024] {
      let l1 = vec![0x33u8; long_len];
      let l2 = vec![0x33u8; long_len];
      assert!(fast_key_eq(&l1, &l2));

      // 头部变异
      let mut l_head = l1.clone();
      l_head[0] = 0x44;
      assert!(!fast_key_eq(&l1, &l_head));

      // 尾部变异
      let mut l_tail = l1.clone();
      l_tail[long_len - 1] = 0x44;
      assert!(!fast_key_eq(&l1, &l_tail));

      // 中间变异
      let mut l_mid = l1.clone();
      l_mid[long_len / 2] = 0x44;
      assert!(!fast_key_eq(&l1, &l_mid));

      // 16 字节边界对齐处变异
      for boundary in [15, 16, 31, 32, long_len - 17, long_len - 16] {
        let mut l_bound = l1.clone();
        l_bound[boundary] = 0x44;
        assert!(!fast_key_eq(&l1, &l_bound));
      }
    }
  }
}
