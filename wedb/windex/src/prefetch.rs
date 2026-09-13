#[cfg(target_arch = "aarch64")]
use core::arch::asm;
#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};

/// 预取目标指针到 CPU L1 数据缓存行（对标 Garnet Tsavorite Sse.Prefetch0）
#[inline(always)]
pub fn prefetch_read_l1<T>(p: *const T) {
  #[cfg(target_arch = "x86_64")]
  unsafe {
    _mm_prefetch(p.cast(), _MM_HINT_T0);
  }
  #[cfg(target_arch = "aarch64")]
  unsafe {
    asm!(
      "prfm pldl1keep, [{p}]",
      p = in(reg) p,
      options(nostack, readonly, preserves_flags)
    );
  }
}
