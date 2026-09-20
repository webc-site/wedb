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
//! - 定长帧的多候选模式表（热命令解析一类）走 `first_masked_eq`：一次 16B 载入 +
//!   每长度组一次掩码与 + 组内整向量全等（`MaskedGroup` 描述组），零逐字节循环、
//!   零长度分支，与 C# `Vector128.BitwiseAnd` + `EqualsAll` 同型；
//! - 单候选的掩码全等（会话 MRU 槽一类）走 `masked_eq`，对位 C#
//!   `EqualsAll(BitwiseAnd(input, _cachedMaskN), _cachedPatternN)`。

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

  // SAFETY: 长度相等已在首段判定、len > 0 已在上方返回，两指针均由 `&[u8]` 保证指向 len 字节可读内存
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
    // SAFETY: 循环条件 offset + 16 <= len 保证 16 字节块严格落在切片界内；`[u8; 16]` 对齐为 1，
    // 任意字节偏移按该数组载入即等价非对齐读（vld1q_u8 / _mm_loadu_si128），不构造超界引用
    let sa = unsafe { &*(a_ptr.add(offset) as *const [u8; 16]) };
    let sb = unsafe { &*(b_ptr.add(offset) as *const [u8; 16]) };
    if !eq16(simd, sa, sb) {
      return false;
    }
    offset += 16;
  }
  if offset < len {
    let tail_offset = len - 16;
    // SAFETY: len >= 16（由 `fast_key_eq` 的分级门控保证）故 tail_offset = len - 16 非负，末块
    // [tail_offset, len) 严格界内；与主循环重叠的字节重复比对不影响相等性判定
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

/// 「一次 16B 载入 + 每组一次掩码与 + 组内整向量全等」比对核的候选组
///
/// 对标 C# `Vector128.BitwiseAnd(input, s_maskN)` 后逐模式的 `Vector128.EqualsAll`
/// （一组 = 一个长度档的掩码与其下全部模式）；`mask` 为 `None` 时整 16 字节参与
/// 全等，免按位与（C# 16 字节组形态）。
#[derive(Clone, Copy)]
pub struct MaskedGroup<'a> {
  /// 组掩码（比对宽度之外的字节清零）；`None` = 整 16B 全等
  pub mask: Option<[u8; 16]>,
  /// 组内候选模式（定长 16B，模式长度之外的字节须已清零）
  pub candidates: &'a [[u8; 16]],
  /// 本组首候选在调用方全局序中的下标（命中回传全局下标）
  pub base: usize,
}

/// 首个「(输入 & 掩码) 与候选整 16B 全等」的命中，返回其全局下标
///
/// 输入定长 16B 窗口只载入一次、每组至多一次按位与、组内整向量全等且首中即短路，
/// 判定序即 `groups` 的给出序（等价于调用方逐项标量比较的首中语义）。
#[inline]
pub fn first_masked_eq(input: &[u8; 16], groups: &[MaskedGroup<'_>]) -> Option<usize> {
  #[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
  return dispatch!(Level::new(), simd => simd_first_masked_eq(simd, input, groups));

  #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
  scalar_first_masked_eq(input, groups)
}

/// 向量核：一次载入 + 逐组掩码与 + 组内整向量全等 (aarch64 NEON / x86_64 SSE2 起步)
#[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
#[inline(always)]
fn simd_first_masked_eq<S: Simd>(
  simd: S,
  input: &[u8; 16],
  groups: &[MaskedGroup<'_>],
) -> Option<usize> {
  let vin = u8x16::load_array_ref(simd, input);
  for group in groups {
    let vm = match group.mask {
      Some(mask) => vin & u8x16::load_array_ref(simd, &mask),
      None => vin,
    };
    for (idx, candidate) in group.candidates.iter().enumerate() {
      if vm
        .simd_eq(u8x16::load_array_ref(simd, candidate))
        .all_true()
      {
        return Some(group.base + idx);
      }
    }
  }
  None
}

/// 非 aarch64/x86_64 目标的标量回落（与向量核逐字节同语义，掩码位为 `&` 后比较）
#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
#[inline]
fn scalar_first_masked_eq(input: &[u8; 16], groups: &[MaskedGroup<'_>]) -> Option<usize> {
  for group in groups {
    for (idx, candidate) in group.candidates.iter().enumerate() {
      if scalar_masked_eq(input, group.mask.as_ref(), candidate) {
        return Some(group.base + idx);
      }
    }
  }
  None
}

/// 单候选掩码全等：`(输入 & 掩码)` 与模式整 16B 逐 lane 相等
///
/// 一次载入 + 一次按位与 + 一次整向量全等，对位 C# 会话 MRU 槽的
/// `EqualsAll(BitwiseAnd(input, _cachedMaskN), _cachedPatternN)`（消费长度 16 时
/// 掩码全 1，与 C# 同样仍执行按位与）。
#[inline]
pub fn masked_eq(input: &[u8; 16], mask: &[u8; 16], pattern: &[u8; 16]) -> bool {
  #[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
  return dispatch!(Level::new(), simd => simd_masked_eq(simd, input, mask, pattern));

  #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
  scalar_masked_eq(input, Some(mask), pattern)
}

/// 向量核：单候选一次载入 + 一次掩码与 + 整向量全等 (aarch64 NEON / x86_64 SSE2 起步)
#[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
#[inline(always)]
fn simd_masked_eq<S: Simd>(simd: S, input: &[u8; 16], mask: &[u8; 16], pattern: &[u8; 16]) -> bool {
  let vm = u8x16::load_array_ref(simd, input) & u8x16::load_array_ref(simd, mask);
  vm.simd_eq(u8x16::load_array_ref(simd, pattern)).all_true()
}

/// 非向量目标的单候选掩码全等回落（`first_masked_eq` 的组内核共用此判定）
#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
#[inline]
fn scalar_masked_eq(input: &[u8; 16], mask: Option<&[u8; 16]>, pattern: &[u8; 16]) -> bool {
  match mask {
    Some(mask) => (0..16).all(|i| (input[i] & mask[i]) == pattern[i]),
    None => input == pattern,
  }
}

// SAFETY: 调用方须保证 a、b 各指向 len 字节可读内存且两侧长度同为 len——本函数无边界检查，
// 仅按 len 做首尾宽字与重叠载入
#[inline]
unsafe fn scalar_fallback_key_eq(a: *const u8, b: *const u8, len: usize) -> bool {
  #[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
  if len >= 8 {
    // SAFETY: len >= 8 保证首 8 字节 [0, 8) 落在两侧指针的 len 字节界内；read_unaligned 无对齐要求
    let a_head = unsafe { (a as *const u64).read_unaligned() };
    let b_head = unsafe { (b as *const u64).read_unaligned() };
    // SAFETY: len >= 8 保证尾 8 字节 [len - 8, len) 界内，与首字重叠不影响「首尾全等即整体全等」的判定
    let a_tail = unsafe { (a.add(len - 8) as *const u64).read_unaligned() };
    let b_tail = unsafe { (b.add(len - 8) as *const u64).read_unaligned() };
    return ((a_head ^ b_head) | (a_tail ^ b_tail)) == 0;
  }
  #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
  if len >= 8 {
    let mut offset = 0;
    while offset + 8 <= len {
      // SAFETY: 循环条件 offset + 8 <= len 保证 [offset, offset + 8) 在两侧指针的 len 字节界内
      if unsafe { (a.add(offset) as *const u64).read_unaligned() }
        != unsafe { (b.add(offset) as *const u64).read_unaligned() }
      {
        return false;
      }
      offset += 8;
    }
    let tail = len - 8;
    // SAFETY: len >= 8 故 tail = len - 8 非负，[tail, tail + 8) 恰以末字节收尾且界内；
    // 与前面已比对的块重叠不影响整体相等性判定
    return unsafe { (a.add(tail) as *const u64).read_unaligned() }
      == unsafe { (b.add(tail) as *const u64).read_unaligned() };
  }
  if len >= 4 {
    // SAFETY: len >= 4 保证首 4 字节 [0, 4) 落在两侧指针的 len 字节界内
    let a_head = unsafe { (a as *const u32).read_unaligned() };
    let b_head = unsafe { (b as *const u32).read_unaligned() };
    // SAFETY: len >= 4 保证尾 4 字节 [len - 4, len) 界内，与首字重叠不影响首尾全等判定
    let a_tail = unsafe { (a.add(len - 4) as *const u32).read_unaligned() };
    let b_tail = unsafe { (b.add(len - 4) as *const u32).read_unaligned() };
    return ((a_head ^ b_head) | (a_tail ^ b_tail)) == 0;
  }
  if len == 3 {
    // SAFETY: len == 3 保证 [0, 2) 的 u16 载入与 offset 2 处的单字节读取均在 3 字节界内
    let head_diff =
      unsafe { ((a as *const u16).read_unaligned() ^ (b as *const u16).read_unaligned()) as u32 };
    let tail_diff = unsafe { (*a.add(2) ^ *b.add(2)) as u32 };
    return (head_diff | tail_diff) == 0;
  }
  if len == 2 {
    // SAFETY: len == 2 保证 [0, 2) 的 u16 未对齐载入恰覆盖两侧指针的 2 字节，无越界读
    return unsafe { (a as *const u16).read_unaligned() == (b as *const u16).read_unaligned() };
  }
  // SAFETY: 走到此处 len 为 1（0 已由调用方短路），两处单字节读取即切片首元素
  unsafe { *a == *b }
}
