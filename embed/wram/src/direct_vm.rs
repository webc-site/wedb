//! 直接操作系统虚拟内存管理 (对标 C# Tsavorite `DirectVirtualMemory.cs`)
//!
//! 用于大容量、长生命周期、NUMA 敏感的单例底层映射（如哈希索引表、日志页面与恢复帧）。
//! 采用操作系统直接虚拟内存原语：
//! - Unix / Linux / macOS: `mmap(MAP_PRIVATE | MAP_ANON)`，按需置零（Demand-Zero）首次访问时由内核建立物理映射
//! - Windows: `VirtualAlloc(MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE)`
//! - Linux 下对于 >= 2MB 的大块映射自动提示透明大页 `madvise(MADV_HUGEPAGE)`，削减 dTLB 未命中开销
//! - 全局接入 [`NativeMemoryTracker`]，支持条带化无锁追踪原生已分配内存

use std::{io::Error as IoError, ops::Range, ptr, slice::from_raw_parts, sync::OnceLock};

use crate::{Error, Result, tracker::NativeMemoryTracker};

#[cfg(windows)]
unsafe extern "system" {
  fn VirtualAlloc(
    lpAddress: *mut std::ffi::c_void,
    dwSize: usize,
    flAllocationType: u32,
    flProtect: u32,
  ) -> *mut std::ffi::c_void;

  fn VirtualFree(lpAddress: *mut std::ffi::c_void, dwSize: usize, dwFreeType: u32) -> i32;
}

#[cfg(windows)]
const MEM_COMMIT: u32 = 0x00001000;
#[cfg(windows)]
const MEM_RESERVE: u32 = 0x00002000;
#[cfg(windows)]
const MEM_RELEASE: u32 = 0x00008000;
#[cfg(windows)]
const PAGE_READWRITE: u32 = 0x04;

/// 直接操作系统虚拟内存预留块 (对标 C# `DirectVmBlock`)
///
/// 记录操作系统分配的基地址、按要求对齐的可用地址以及总预留字节长度。
/// 支持 RAII 自动安全释放与显式释放，且实现 `Send` 与 `Sync`。
#[derive(Debug)]
pub struct DirectVmBlock {
  pub base_ptr: *mut u8,
  pub aligned_ptr: *mut u8,
  pub reserved_length: usize,
}

unsafe impl Send for DirectVmBlock {}
unsafe impl Sync for DirectVmBlock {}

impl DirectVmBlock {
  /// 创建空的虚拟内存块
  pub const fn empty() -> Self {
    Self {
      base_ptr: ptr::null_mut(),
      aligned_ptr: ptr::null_mut(),
      reserved_length: 0,
    }
  }

  /// 检查块是否为空 (未映射)
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.base_ptr.is_null()
  }

  /// 自对齐地址起的可用字节长度 (预留长度扣除对齐偏移)
  #[inline]
  fn avail_len(&self) -> usize {
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
    Ok(unsafe { from_raw_parts(start_ptr, len) })
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

/// sysconf 失败或平台不支持时的兜底物理页大小
const FALLBACK_PAGE_SIZE: usize = 4096;

impl DirectVirtualMemory {
  /// 预留并提交按指定对齐对齐的按需置零虚拟内存区域 (对标 C# `DirectVirtualMemory.Allocate`)
  ///
  /// `size`: 请求的有效字节大小，必须 > 0
  /// `alignment`: 对齐大小，必须为 2 的幂
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
    let reserve = (total.checked_add(page_size - 1).ok_or(Error::Overflow)?) & !(page_size - 1);

    #[cfg(unix)]
    let base_ptr = unsafe {
      let ptr = libc::mmap(
        ptr::null_mut(),
        reserve,
        libc::PROT_READ | libc::PROT_WRITE,
        libc::MAP_PRIVATE | libc::MAP_ANON,
        -1,
        0,
      );
      if ptr == libc::MAP_FAILED || ptr.is_null() {
        return Err(Error::DirectVmAllocFailed {
          size: reserve,
          source: IoError::last_os_error(),
        });
      }
      #[cfg(target_os = "linux")]
      if use_huge_pages {
        let _ = libc::madvise(ptr, reserve, libc::MADV_HUGEPAGE);
      }
      ptr as *mut u8
    };

    #[cfg(windows)]
    let base_ptr = unsafe {
      let ptr = VirtualAlloc(
        ptr::null_mut(),
        reserve,
        MEM_COMMIT | MEM_RESERVE,
        PAGE_READWRITE,
      );
      if ptr.is_null() {
        return Err(Error::DirectVmAllocFailed {
          size: reserve,
          source: IoError::last_os_error(),
        });
      }
      ptr as *mut u8
    };

    let base_addr = base_ptr as usize;
    let aligned_addr = (base_addr
      .checked_add(effective_alignment - 1)
      .ok_or(Error::Overflow)?)
      & !(effective_alignment - 1);
    let aligned_ptr = aligned_addr as *mut u8;

    NativeMemoryTracker::add(reserve);

    Ok(DirectVmBlock {
      base_ptr,
      aligned_ptr,
      reserved_length: reserve,
    })
  }

  /// 释放由 [`allocate`](Self::allocate) 分配的直接虚拟内存块 (对标 C# `DirectVirtualMemory.Free`)
  pub fn free(block: &mut DirectVmBlock) {
    if block.base_ptr.is_null() || block.reserved_length == 0 {
      return;
    }

    let freed = {
      #[cfg(unix)]
      unsafe {
        libc::munmap(block.base_ptr as *mut libc::c_void, block.reserved_length) == 0
      }

      #[cfg(windows)]
      unsafe {
        VirtualFree(block.base_ptr as *mut std::ffi::c_void, 0, MEM_RELEASE) != 0
      }
    };

    if freed {
      NativeMemoryTracker::subtract(block.reserved_length);
    } else {
      log::error!(
        "直接虚拟内存释放失败: ptr={:?}, len={}",
        block.base_ptr,
        block.reserved_length
      );
    }

    block.base_ptr = ptr::null_mut();
    block.aligned_ptr = ptr::null_mut();
    block.reserved_length = 0;
  }

  /// 对指定裸指针区间全量置零 (对标 C# `DirectVirtualMemory.Clear`)
  ///
  /// # Safety
  ///
  /// 调用者须确保 `ptr` 在 `[ptr, ptr + len)` 范围内有效且可安全写入
  #[inline]
  pub unsafe fn clear(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
      unsafe { ptr::write_bytes(ptr, 0, len) };
    }
  }
}

/// 获取当前系统的物理页大小 (字节)
#[inline]
pub fn system_page_size() -> usize {
  static PAGE_SIZE: OnceLock<usize> = OnceLock::new();
  *PAGE_SIZE.get_or_init(|| {
    #[cfg(unix)]
    unsafe {
      let val = libc::sysconf(libc::_SC_PAGESIZE);
      if val > 0 {
        val as usize
      } else {
        FALLBACK_PAGE_SIZE
      }
    }
    #[cfg(not(unix))]
    {
      FALLBACK_PAGE_SIZE
    }
  })
}
