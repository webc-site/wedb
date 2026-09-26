use std::result;

/// 批量读预取窗口大小（全仓唯一定义，1:1 对标 C# 单批预取项数
/// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:ContextReadWithPrefetch 的
/// `const int PrefetchSize = 12`）
pub const PREFETCH_WINDOW: usize = 12;

#[cfg(target_arch = "aarch64")]
use core::arch::asm;
#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};

use crate::table::HashIndex;

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

/// 批量读单键预取探针（对标 C# ContextReadWithPrefetch 内 `hashes[]` 与 `HashEntryInfo` 的
/// 同源装载：键哈希在预取阶段一次算定，随首地址贯穿整条内存读链，内核与回溯零重算）
#[derive(Clone, Copy)]
pub struct PrefetchProbe {
  /// 键哈希（[`HashIndex::hash_key`] 单次算定，供分桶与全链探针复用）
  pub hash: u64,
  /// [`HashIndex::find_tag_by_hash`] 装载的链首地址
  pub first_addr: Option<u64>,
}

impl PrefetchProbe {
  /// 预取窗口内未填充的槽位（有效长度为调用方本批键数）
  const EMPTY: Self = Self {
    hash: 0,
    first_addr: None,
  };
}

impl HashIndex {
  /// 单批两级硬件预取内核：产出 [`PrefetchProbe`] 探针数组（严格对照
  /// C# Tsavorite.ContextReadWithPrefetch；本函数是其内部预取探针协作段，
  /// 公共 API 对位见 wkv `read_batch_with` 的锚点文档）
  ///
  /// 对标 C# 单批内的两趟预取（窗口 [`PREFETCH_WINDOW`] = C# `PrefetchSize`）：
  /// 1. 第一级：逐键单次算定哈希并预取主桶 cacheline（C# `Sse.Prefetch0(tableAligned +
  ///    (hash & size_mask))`）；每个哈希算定后经 `on_hash` 交回调用方推进在线扩容分块
  ///    （rust 协作式迁移，C# 无此步），失败显式上抛，杜绝半迁移状态下按未迁移桶取探针；
  /// 2. 第二级：逐哈希 [`Self::find_tag_by_hash`] 装载链首地址，命中者交 `prefetch_record`
  ///    预取记录物理内存（C# 同位 `FindTag` + `hlogBase.GetPhysicalAddress`；内存驻留区间
  ///    判定由持有日志的调用方在该回调内完成，索引层不感知日志）。
  ///
  /// `keys` 为本批键（长度不超过窗口，超窗由调用方分块逐批调用，内核按窗口长度截断），
  /// 返回定长 [`PREFETCH_WINDOW`] 探针数组，有效长度为 `keys.len()`。
  #[inline]
  pub fn prefetch_batch_probes<K, E>(
    &self,
    keys: &[K],
    mut on_hash: impl FnMut(u64) -> result::Result<(), E>,
    mut prefetch_record: impl FnMut(u64),
  ) -> result::Result<[PrefetchProbe; PREFETCH_WINDOW], E>
  where
    K: AsRef<[u8]>,
  {
    let count = keys.len().min(PREFETCH_WINDOW);
    let mut probes = [PrefetchProbe::EMPTY; PREFETCH_WINDOW];

    // 1. 第一级预取：哈希桶 cacheline（哈希随探针带出，后续读链零重算）
    for (key, probe) in keys.iter().zip(probes[..count].iter_mut()) {
      let hash = Self::hash_key(key.as_ref());
      on_hash(hash)?;
      prefetch_read_l1(self.get_bucket((hash as usize) & self.mask));
      probe.hash = hash;
    }

    // 2. 第二级预取：链首地址命中即预取记录物理内存
    for probe in probes[..count].iter_mut() {
      probe.first_addr = self.find_tag_by_hash(probe.hash);
      if let Some(addr) = probe.first_addr {
        prefetch_record(addr);
      }
    }

    Ok(probes)
  }
}
