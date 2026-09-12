//! 系统硬件环境探测模块
//!
//! 提供跨平台、零外部依赖的硬件资源探测能力，包括总物理内存（RAM）与 CPU 可用核心数，
//! 用于存储引擎与服务守护进程自适应配置调优。
//! Rust 补齐模块：C# 设备层无对应实现（Garnet 以宿主进程配置承担），Rust 侧供
//! wkv 配置自 tuning 等场景按需探测。

#[cfg(any(target_vendor = "apple", windows))]
use std::mem::size_of;
use std::thread::available_parallelism;

/// 保底默认探测物理内存：4 GB（探测失败时的内部回退值，不对外承诺）
const FALLBACK_SYSTEM_MEMORY_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// 保底默认探测 CPU 核心数：4 核（探测失败时的内部回退值，不对外承诺）
const FALLBACK_CPU_CORES: usize = 4;

/// 最大允许段大小：4 EiB (1 << 62)，防止算术位移与遮罩越界溢出
pub const MAX_SEGMENT_SIZE: u64 = 1u64 << 62;

/// 探测当前宿主机的总物理内存大小（字节）
///
/// 优先通过操作系统底层原生调用精确探测，失败时回退至保守保底值（4GB）。
#[must_use]
pub fn detect_system_memory() -> u64 {
  #[cfg(target_vendor = "apple")]
  {
    use std::ptr::null_mut;
    let mut mem: u64 = 0;
    let mut len = size_of::<u64>();
    let mib = [libc::CTL_HW, libc::HW_MEMSIZE];
    if unsafe {
      libc::sysctl(
        mib.as_ptr() as *mut _,
        2,
        &mut mem as *mut _ as *mut _,
        &mut len,
        null_mut(),
        0,
      )
    } == 0
      && mem > 0
    {
      return mem;
    }
  }

  #[cfg(all(unix, not(target_vendor = "apple")))]
  {
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if pages > 0 && page_size > 0 {
      let mem = (pages as u64).saturating_mul(page_size as u64);
      if mem > 0 {
        return mem;
      }
    }
  }

  #[cfg(windows)]
  {
    #[repr(C)]
    struct MemoryStatusEx {
      dw_length: u32,
      dw_memory_load: u32,
      ull_total_phys: u64,
      ull_avail_phys: u64,
      ull_total_page_file: u64,
      ull_avail_page_file: u64,
      ull_total_virtual: u64,
      ull_avail_virtual: u64,
      ull_avail_extended_virtual: u64,
    }
    unsafe extern "system" {
      fn GlobalMemoryStatusEx(lpBuffer: *mut MemoryStatusEx) -> i32;
    }
    let mut status = MemoryStatusEx {
      dw_length: size_of::<MemoryStatusEx>() as u32,
      dw_memory_load: 0,
      ull_total_phys: 0,
      ull_avail_phys: 0,
      ull_total_page_file: 0,
      ull_avail_page_file: 0,
      ull_total_virtual: 0,
      ull_avail_virtual: 0,
      ull_avail_extended_virtual: 0,
    };
    if unsafe { GlobalMemoryStatusEx(&mut status) } != 0 && status.ull_total_phys > 0 {
      return status.ull_total_phys;
    }
  }

  FALLBACK_SYSTEM_MEMORY_BYTES
}

/// 探测当前进程可用的 CPU 并行核心数
///
/// 遵循 cgroups / 配额限制与物理核心数，失败时回退至保守保底值（4 核）。
#[must_use]
pub fn detect_cpu_cores() -> usize {
  available_parallelism().map_or(FALLBACK_CPU_CORES, |n| n.get())
}
