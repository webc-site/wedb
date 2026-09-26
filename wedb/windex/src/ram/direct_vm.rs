//! 直接操作系统虚拟内存管理 (对标 C# Tsavorite `DirectVirtualMemory.cs`)
//!
//! 用于大容量、长生命周期、NUMA 敏感的单例底层映射（如哈希索引表、日志页面与恢复帧）。
//! 采用操作系统直接虚拟内存原语：
//! - Unix / Linux / macOS: `mmap(MAP_PRIVATE | MAP_ANON)`，按需置零（Demand-Zero）首次访问时由内核建立物理映射
//! - Windows: `VirtualAlloc(MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE)`
//! - Linux 下对于 >= 2MB 的大块映射自动提示透明大页 `madvise(MADV_HUGEPAGE)`，削减 dTLB 未命中开销
//! - 全局接入 [`NativeMemoryTracker`]，支持条带化无锁追踪原生已分配内存

use std::{
  ops::Range,
  ptr,
  slice::{from_raw_parts, from_raw_parts_mut},
};

use memmap2::MmapMut;
use wbase::{
  align::checked_align_up,
  error::{Error, Result},
};

use super::tracker::NativeMemoryTracker;

/// 直接操作系统虚拟内存预留块 (对标 C# `DirectVmBlock`)
///
/// 记录操作系统分配的基地址、按要求对齐的可用地址以及总预留字节长度。
/// 支持 RAII 自动安全释放与显式释放，且实现 `Send` 与 `Sync`。
#[derive(Debug)]
pub struct DirectVmBlock {
  pub base_ptr: *mut u8,
  pub aligned_ptr: *mut u8,
  pub reserved_length: usize,
  _mmap: Option<MmapMut>,
}

// SAFETY: 三字段完整刻画 `allocate` 返回的整块 OS 映射（base_ptr 为 mmap/VirtualAlloc 的原始返回地址、
// aligned_ptr 为其上的对齐地址、reserved_length 为预留字节数），结构独占该映射并由 `Drop`/`free` 归还，
// 移交即移交释放责任；字段本身均为 `Copy` 裸值，不含非线程安全状态。
unsafe impl Send for DirectVmBlock {}
// SAFETY: `&self` 方法只读字段并只在 `check_range`/`avail_len` 校验过的界内区间派生切片、绝不写入，
// 故共享引用下的内存安全成立；本类型只提供这一层可共享性，同段映射的并发读写竞态（`&self` 内部
// 可变写路径）由上层纪律排除：构造期独占写入、发布后只读或按桶加锁——哈希索引表绝不在役原位置零，
// 死条目交由 min_valid_addr 惰性清退（见 wkv/store/keyspace.rs 的 flush_all_databases）。
unsafe impl Sync for DirectVmBlock {}

impl DirectVmBlock {
  /// 创建空的虚拟内存块
  pub const fn empty() -> Self {
    Self {
      base_ptr: ptr::null_mut(),
      aligned_ptr: ptr::null_mut(),
      reserved_length: 0,
      _mmap: None,
    }
  }

  /// 检查块是否为空 (未映射)
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.base_ptr.is_null()
  }

  /// 自对齐地址起的可用字节长度 (预留长度扣除对齐偏移)
  #[inline]
  pub fn avail_len(&self) -> usize {
    let offset = self.aligned_ptr as usize - self.base_ptr as usize;
    self.reserved_length.saturating_sub(offset)
  }

  /// 校验子切片区间越界，合法时返回 (起始裸指针, 长度)
  ///
  /// 空块或 `start > end` 或末端越过可用长度均报错，绝不越界解引用
  fn check_range(&self, range: Range<usize>) -> Result<(*mut u8, usize)> {
    if self.is_empty() || range.start > range.end || range.end > self.avail_len() {
      return Err(Error::Overflow);
    }
    // SAFETY: range.start ≤ avail_len ≤ reserved_length - offset，越界不可能发生
    let start_ptr = unsafe { self.aligned_ptr.add(range.start) };
    Ok((start_ptr, range.end - range.start))
  }

  /// 获取指定偏移与长度的对齐子切片
  #[inline]
  pub fn slice(&self, range: Range<usize>) -> Result<&[u8]> {
    let (start_ptr, len) = self.check_range(range)?;
    if len == 0 {
      return Ok(&[]);
    }
    // SAFETY: `check_range` 已拒空块、start > end 与 end > avail_len，故 [start_ptr, start_ptr + len)
    // 完整落在 [aligned_ptr, base_ptr + reserved_length) 映射内；借用受 `&self` 生命周期约束
    Ok(unsafe { from_raw_parts(start_ptr, len) })
  }

  /// 获取可用全长对齐切片（供直接虚拟内存测试与调试断言）
  #[inline]
  pub fn as_aligned_slice(&self) -> &[u8] {
    if self.is_empty() || self.aligned_ptr.is_null() || self.avail_len() == 0 {
      &[]
    } else {
      // SAFETY: 非空、aligned_ptr 非零且 avail_len > 0 三判已过，avail_len = reserved_length - 偏移不越映射末端
      unsafe { from_raw_parts(self.aligned_ptr, self.avail_len()) }
    }
  }

  /// 获取可用全长对齐可变切片（供直接虚拟内存测试与调试断言）
  #[inline]
  pub fn as_aligned_mut_slice(&mut self) -> &mut [u8] {
    if self.is_empty() || self.aligned_ptr.is_null() || self.avail_len() == 0 {
      &mut []
    } else {
      // SAFETY: 同不可变分支的界内判据，可变切片的独占访问由 `&mut self` 保证
      unsafe { from_raw_parts_mut(self.aligned_ptr, self.avail_len()) }
    }
  }
}

impl Drop for DirectVmBlock {
  fn drop(&mut self) {
    DirectVirtualMemory::free(self);
  }
}

/// 操作系统直接虚拟内存分配器 (对标 C# `DirectVirtualMemory`)
pub struct DirectVirtualMemory;

/// Linux 透明大页粒度 (2MB，x86-64 / arm64 THP，对标 C# `HugePageSize`)
#[cfg(target_os = "linux")]
const HUGE_PAGE_SIZE: usize = 2 << 20;

impl DirectVirtualMemory {
  /// 预留并提交按指定对齐对齐的按需置零虚拟内存区域 (对标 C# `DirectVirtualMemory.Allocate`)
  ///
  /// `size`: 请求的有效字节大小，必须 > 0
  /// `alignment`: 对齐大小，必须为 2 的幂
  ///
  /// 平台映射原语（mmap/madvise 巨页提示/VirtualAlloc）统一内联于本函数：
  /// libs/storage/Tsavorite/cs/src/core/Native/DirectVirtualMemory.cs:Allocate
  /// libs/storage/Tsavorite/cs/src/core/Native/DirectVirtualMemory.cs:mmap
  /// libs/storage/Tsavorite/cs/src/core/Native/DirectVirtualMemory.cs:madvise
  /// libs/storage/Tsavorite/cs/src/core/Native/DirectVirtualMemory.cs:VirtualAlloc
  pub fn allocate(size: usize, alignment: usize) -> Result<DirectVmBlock> {
    if size == 0 {
      return Err(Error::InvalidSize(size));
    }
    if alignment == 0 || !alignment.is_power_of_two() {
      return Err(Error::InvalidAlignment(alignment, 1));
    }

    // Linux 下 >= 2MB 的大块映射将有效对齐提升至 2MB，使 THP 可从首字节就按大页回填
    // (对标 C# useHugePages / effectiveAlignment)；MADV_HUGEPAGE 为 Linux 专属建议
    #[cfg(target_os = "linux")]
    let use_huge_pages = size >= HUGE_PAGE_SIZE;
    #[cfg(target_os = "linux")]
    let effective_alignment = if use_huge_pages && alignment < HUGE_PAGE_SIZE {
      HUGE_PAGE_SIZE
    } else {
      alignment
    };

    #[cfg(not(target_os = "linux"))]
    let effective_alignment = alignment;

    let page_size = system_page_size();
    let total = size
      .checked_add(effective_alignment)
      .ok_or(Error::Overflow)?;
    let reserve = checked_align_up(total as u64, page_size as u64).ok_or(Error::Overflow)? as usize;

    let mut mmap = memmap2::MmapOptions::new()
      .len(reserve)
      .map_anon()
      .map_err(|source| Error::DirectVmAllocFailed {
        size: reserve,
        source,
      })?;

    #[cfg(target_os = "linux")]
    if use_huge_pages {
      let _ = mmap.advise(memmap2::Advice::HugePage);
    }

    let base_ptr = mmap.as_mut_ptr();
    let base_addr = base_ptr as usize;
    let aligned_addr = match checked_align_up(base_addr as u64, effective_alignment as u64) {
      Some(addr) => addr as usize,
      None => return Err(Error::Overflow),
    };
    let aligned_ptr = aligned_addr as *mut u8;

    NativeMemoryTracker::add(reserve);

    Ok(DirectVmBlock {
      base_ptr,
      aligned_ptr,
      reserved_length: reserve,
      _mmap: Some(mmap),
    })
  }

  /// 释放由 [`allocate`](Self::allocate) 分配的直接虚拟内存块
  ///
  /// libs/storage/Tsavorite/cs/src/core/Native/DirectVirtualMemory.cs:Free
  pub fn free(block: &mut DirectVmBlock) {
    if block.base_ptr.is_null() || block.reserved_length == 0 {
      return;
    }

    let len = block.reserved_length;
    block._mmap = None;
    NativeMemoryTracker::subtract(len);

    block.base_ptr = ptr::null_mut();
    block.aligned_ptr = ptr::null_mut();
    block.reserved_length = 0;
  }
}

/// 获取当前系统的物理页大小 (字节)
#[inline]
pub fn system_page_size() -> usize {
  page_size::get()
}
