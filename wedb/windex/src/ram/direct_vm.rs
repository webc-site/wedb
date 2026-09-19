//! 直接操作系统虚拟内存管理 (对标 C# Tsavorite `DirectVirtualMemory.cs`)
//!
//! 用于大容量、长生命周期、NUMA 敏感的单例底层映射（如哈希索引表、日志页面与恢复帧）。
//! 采用操作系统直接虚拟内存原语：
//! - Unix / Linux / macOS: `mmap(MAP_PRIVATE | MAP_ANON)`，按需置零（Demand-Zero）首次访问时由内核建立物理映射
//! - Windows: `VirtualAlloc(MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE)`
//! - Linux 下对于 >= 2MB 的大块映射自动提示透明大页 `madvise(MADV_HUGEPAGE)`，削减 dTLB 未命中开销
//! - 全局接入 [`NativeMemoryTracker`]，支持条带化无锁追踪原生已分配内存

use std::{
  io::Error as IoError,
  ops::Range,
  ptr,
  slice::{from_raw_parts, from_raw_parts_mut},
  sync::OnceLock,
};

use wbase::error::{Error, Result};

use super::tracker::NativeMemoryTracker;

#[cfg(windows)]
// SAFETY: 声明须与 Windows API 的 system ABI、参数与返回类型逐一对应；本文件只在 allocate/free
// 路径以自证的 reserve 长度与自持句柄调用，不由他处传入裸地址
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

// SAFETY: 三字段完整刻画 `allocate` 返回的整块 OS 映射（base_ptr 为 mmap/VirtualAlloc 的原始返回地址、
// aligned_ptr 为其上的对齐地址、reserved_length 为预留字节数），结构独占该映射并由 `Drop`/`free` 归还，
// 移交即移交释放责任；字段本身均为 `Copy` 裸值，不含非线程安全状态。
unsafe impl Send for DirectVmBlock {}
// SAFETY: `&self` 方法只读字段并只在 `check_range`/`avail_len` 校验过的界内区间派生切片、绝不写入，
// 故共享引用下的内存安全成立；本类型只提供这一层可共享性，同段映射的并发读写竞态（如
// `HashBuckets::clear(&self)` 这类 `&self` 写路径）由上层纪律排除：构造期独占写入、发布后只读或按桶加锁。
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

    // SAFETY: reserve 为非零且页对齐的长度，PROT_READ|PROT_WRITE + MAP_PRIVATE|MAP_ANON 不映射任何文件；
    // 返回值先判 MAP_FAILED/null 再作块基址使用，madvise 仅是大页提示、失败可忽略
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

    // SAFETY: MEM_COMMIT | MEM_RESERVE + PAGE_READWRITE 由 OS 保证 reserve 字节可读可写，null 已判错返回
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
    let aligned_addr = match base_addr.checked_add(effective_alignment - 1) {
      Some(addr) => addr & !(effective_alignment - 1),
      None => {
        #[cfg(unix)]
        // SAFETY: base_ptr 与 reserve 恰为上方 mmap 成功的原始返回与长度，此时块尚未构造故无其他持有者
        unsafe {
          libc::munmap(base_ptr as *mut libc::c_void, reserve);
        }
        #[cfg(windows)]
        // SAFETY: base_ptr 与 reserve 恰为上方 VirtualAlloc 成功的原始返回与长度，dwSize=0 + MEM_RELEASE
        // 为其整块释放的合法参数组合
        unsafe {
          VirtualFree(base_ptr as *mut std::ffi::c_void, 0, MEM_RELEASE);
        }
        return Err(Error::Overflow);
      }
    };
    let aligned_ptr = aligned_addr as *mut u8;

    NativeMemoryTracker::add(reserve);

    Ok(DirectVmBlock {
      base_ptr,
      aligned_ptr,
      reserved_length: reserve,
    })
  }

  /// 释放由 [`allocate`](Self::allocate) 分配的直接虚拟内存块
  ///
  /// libs/storage/Tsavorite/cs/src/core/Native/DirectVirtualMemory.cs:Free
  pub fn free(block: &mut DirectVmBlock) {
    if block.base_ptr.is_null() || block.reserved_length == 0 {
      return;
    }

    let freed = {
      #[cfg(unix)]
      // SAFETY: base_ptr 与 reserved_length 恰为 `allocate` 时 mmap 的原始返回与长度，null/零长已提前返回；
      // 释放后随即把三字段置空，故 Drop 再入或同一块二次释放都被空块分支挡住
      unsafe {
        libc::munmap(block.base_ptr as *mut libc::c_void, block.reserved_length) == 0
      }

      #[cfg(windows)]
      // SAFETY: 同 unix 分支——参数取自本结构自持的原始返回与预留长度，且释放后立即置空字段防二次释放
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

  /// 对指定裸指针区间全量置零
  ///
  /// libs/storage/Tsavorite/cs/src/core/Native/DirectVirtualMemory.cs:Clear
  ///
  /// # Safety
  ///
  /// 调用者须保证 `[ptr, ptr + len)` 完整落在同一块已映射且可写的区域内（[`allocate`](Self::allocate)
  /// 产出的映射或等价的自持堆块），且该区间内不存在其他活动借用（等价 `&mut` 语义）——
  /// 本函数只自守空指针与零长度，不校验区间归属与借用独占性
  #[inline]
  pub unsafe fn clear(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
      // SAFETY: 上方已判 ptr 非零且 len > 0，写入范围即调用方承诺的 [ptr, ptr + len)；逐字节置零不产生中间态值
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
    // SAFETY: sysconf 为无可变全局状态的 libc 入口，`_SC_PAGESIZE` 为合法常量；失败/不支持时回落兜底页大小
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
