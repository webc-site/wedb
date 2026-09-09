//! 基于 SIMD 硬件向量化与多级宽字展开的高性能切片比对
//!
//! 专为存储引擎的键查找、版本链追溯与去重设计：
//! - aarch64：NEON `vld1q_u8` + `vceqq_u8` + `vminvq_u8` 向量比对；
//! - x86_64：SSE2 `_mm_loadu_si128` + `_mm_cmpeq_epi8` + `_mm_movemask_epi8` 向量比对；
//! - 尾部借末 16B 重叠比对彻底消除余数标量循环分支；
//! - 短键采用无对齐 64 位 / 32 位首尾宽字并集比对，零多余边界检查。

use core::slice::from_raw_parts;

/// 快速键与切片相等性比对
#[inline]
pub fn fast_key_eq(a: &[u8], b: &[u8]) -> bool {
  let len = a.len();
  if len != b.len() {
    return false;
  }
  if a.as_ptr() == b.as_ptr() || len == 0 {
    return true;
  }

  #[cfg(target_arch = "aarch64")]
  {
    if len >= 16 {
      return unsafe { neon_key_eq(a.as_ptr(), b.as_ptr(), len) };
    }
  }

  #[cfg(target_arch = "x86_64")]
  {
    if len >= 16 {
      return unsafe { sse2_key_eq(a.as_ptr(), b.as_ptr(), len) };
    }
  }

  unsafe { scalar_fallback_key_eq(a.as_ptr(), b.as_ptr(), len) }
}

#[cfg(target_arch = "aarch64")]
#[inline]
unsafe fn neon_key_eq(a: *const u8, b: *const u8, len: usize) -> bool {
  use core::arch::aarch64::{vceqq_u8, vld1q_u8, vminvq_u8};
  unsafe {
    let mut offset = 0;
    while offset + 16 <= len {
      let va = vld1q_u8(a.add(offset));
      let vb = vld1q_u8(b.add(offset));
      let vcmp = vceqq_u8(va, vb);
      if vminvq_u8(vcmp) != 0xFF {
        return false;
      }
      offset += 16;
    }
    if offset < len {
      let va = vld1q_u8(a.add(len - 16));
      let vb = vld1q_u8(b.add(len - 16));
      let vcmp = vceqq_u8(va, vb);
      return vminvq_u8(vcmp) == 0xFF;
    }
    true
  }
}

#[cfg(target_arch = "x86_64")]
#[inline]
unsafe fn sse2_key_eq(a: *const u8, b: *const u8, len: usize) -> bool {
  use core::arch::x86_64::{__m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8};
  unsafe {
    let mut offset = 0;
    while offset + 16 <= len {
      let va = _mm_loadu_si128(a.add(offset) as *const __m128i);
      let vb = _mm_loadu_si128(b.add(offset) as *const __m128i);
      let vcmp = _mm_cmpeq_epi8(va, vb);
      let mask = _mm_movemask_epi8(vcmp);
      if mask != 0xFFFF {
        return false;
      }
      offset += 16;
    }
    if offset < len {
      let va = _mm_loadu_si128(a.add(len - 16) as *const __m128i);
      let vb = _mm_loadu_si128(b.add(len - 16) as *const __m128i);
      let vcmp = _mm_cmpeq_epi8(va, vb);
      return _mm_movemask_epi8(vcmp) == 0xFFFF;
    }
    true
  }
}

#[inline]
unsafe fn scalar_fallback_key_eq(a: *const u8, b: *const u8, len: usize) -> bool {
  unsafe {
    if len >= 8 {
      let mut offset = 0;
      while offset + 8 <= len {
        if (a.add(offset) as *const u64).read_unaligned()
          != (b.add(offset) as *const u64).read_unaligned()
        {
          return false;
        }
        offset += 8;
      }
      let tail = len - 8;
      return (a.add(tail) as *const u64).read_unaligned()
        == (b.add(tail) as *const u64).read_unaligned();
    }
    if len >= 4 {
      let a_head = (a as *const u32).read_unaligned();
      let b_head = (b as *const u32).read_unaligned();
      let a_tail = (a.add(len - 4) as *const u32).read_unaligned();
      let b_tail = (b.add(len - 4) as *const u32).read_unaligned();
      return a_head == b_head && a_tail == b_tail;
    }
    from_raw_parts(a, len) == from_raw_parts(b, len)
  }
}
