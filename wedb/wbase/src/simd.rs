//! 基于 SIMD 硬件向量化与多级宽字展开的高性能切片比对
//!
//! 专为存储引擎的键查找、版本链追溯与去重设计，向量化路径经 fearless_simd 令牌驱动：
//! - aarch64：NEON 基线令牌驱动 `vceqq_u8` + `vminvq` 向量比对（编译期静态单臂，零运行时探测）；
//! - x86_64：`Sse2`（x86-64 基线）起步的令牌驱动 `_mm_cmpeq_epi8` + `_mm_movemask_epi8`
//!   向量比对（`Level::new()` 进程级缓存探测，按最优指令集多版本展开）；
//! - 长键按 16 字节步进比对，尾部借末 16B 重叠比对彻底消除余数标量循环分支：
//!   以 `load_array_ref` 直接加载 16 字节定长数组（u8 数组天然单字节对齐，
//!   底层对应 `vld1q_u8` / `_mm_loadu_si128` 非对齐载入，零切片边界检查与 panic 分支），重叠尾读即 `&*(ptr.add(len - 16) as *const [u8; 16])`；
//! - 短键采用无对齐 64 位 / 32 位首尾宽字并集比对，零多余边界检查。

#[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
use fearless_simd::{Level, dispatch, prelude::*, u8x16};

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

  #[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
  if len >= 16 {
    return dispatch!(Level::new(), simd => simd_key_eq(simd, a, b, len));
  }

  unsafe { scalar_fallback_key_eq(a.as_ptr(), b.as_ptr(), len) }
}

/// 16B 步进 + 末 16B 重叠尾读的向量化比对 (aarch64 NEON / x86_64 SSE2 起步)
///
/// 主循环逐块比对、首异即短路返回；存在余数 (len 非 16 对齐) 时借末 16B 重叠块
/// 一次性收尾，与逐指令手写版本语义一致 (穷举差异位测试锁定，见 wrecord::simd)
#[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
#[inline(always)]
fn simd_key_eq<S: Simd>(simd: S, a: &[u8], b: &[u8], len: usize) -> bool {
  let a_ptr = a.as_ptr();
  let b_ptr = b.as_ptr();
  let mut offset = 0;
  while offset + 16 <= len {
    let sa = unsafe { &*(a_ptr.add(offset) as *const [u8; 16]) };
    let sb = unsafe { &*(b_ptr.add(offset) as *const [u8; 16]) };
    if !eq16(simd, sa, sb) {
      return false;
    }
    offset += 16;
  }
  if offset < len {
    let tail_offset = len - 16;
    let sa = unsafe { &*(a_ptr.add(tail_offset) as *const [u8; 16]) };
    let sb = unsafe { &*(b_ptr.add(tail_offset) as *const [u8; 16]) };
    return eq16(simd, sa, sb);
  }
  true
}

/// 单个 16B 向量块相等性比对 (NEON: `vceqq_u8`+`vminvq`；SSE2: `pcmpeqb`+`pmovmskb`)
#[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
#[inline(always)]
fn eq16<S: Simd>(simd: S, a: &[u8; 16], b: &[u8; 16]) -> bool {
  u8x16::load_array_ref(simd, a)
    .simd_eq(u8x16::load_array_ref(simd, b))
    .all_true()
}

#[inline]
unsafe fn scalar_fallback_key_eq(a: *const u8, b: *const u8, len: usize) -> bool {
  #[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
  if len >= 8 {
    let a_head = unsafe { (a as *const u64).read_unaligned() };
    let b_head = unsafe { (b as *const u64).read_unaligned() };
    let a_tail = unsafe { (a.add(len - 8) as *const u64).read_unaligned() };
    let b_tail = unsafe { (b.add(len - 8) as *const u64).read_unaligned() };
    return ((a_head ^ b_head) | (a_tail ^ b_tail)) == 0;
  }
  #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
  if len >= 8 {
    let mut offset = 0;
    while offset + 8 <= len {
      if unsafe { (a.add(offset) as *const u64).read_unaligned() }
        != unsafe { (b.add(offset) as *const u64).read_unaligned() }
      {
        return false;
      }
      offset += 8;
    }
    let tail = len - 8;
    return unsafe { (a.add(tail) as *const u64).read_unaligned() }
      == unsafe { (b.add(tail) as *const u64).read_unaligned() };
  }
  if len >= 4 {
    let a_head = unsafe { (a as *const u32).read_unaligned() };
    let b_head = unsafe { (b as *const u32).read_unaligned() };
    let a_tail = unsafe { (a.add(len - 4) as *const u32).read_unaligned() };
    let b_tail = unsafe { (b.add(len - 4) as *const u32).read_unaligned() };
    return ((a_head ^ b_head) | (a_tail ^ b_tail)) == 0;
  }
  if len == 3 {
    let head_diff =
      unsafe { ((a as *const u16).read_unaligned() ^ (b as *const u16).read_unaligned()) as u32 };
    let tail_diff = unsafe { (*a.add(2) ^ *b.add(2)) as u32 };
    return (head_diff | tail_diff) == 0;
  }
  if len == 2 {
    return unsafe { (a as *const u16).read_unaligned() == (b as *const u16).read_unaligned() };
  }
  unsafe { *a == *b }
}
