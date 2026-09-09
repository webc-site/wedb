//! 直接操作系统虚拟内存测试
//!
//! 对标 C#：libs/storage/Tsavorite/cs/test/NativeAllocatorTests.cs
//! （`DirectVmAllocateIsZeroedAlignedWritable`、`DirectVmTrackerReflectsAllocation`）。

use aok::{OK, Void};
use log::info;
use wram::{DirectVirtualMemory, DirectVmBlock, NativeMemoryTracker, system_page_size};

/// 直接虚拟内存分配：demand-zero 全零、对齐、可读写、可清零、可释放
#[test]
fn direct_vm_allocate_is_zeroed_aligned_writable() -> Void {
  info!("对标 DirectVmAllocateIsZeroedAlignedWritable：1MB/4KB 映射、全零、读写、Clear、Free");

  let size = 1 << 20; // 1 MB
  let align = 4096;
  let mut block = DirectVirtualMemory::allocate(size, align)?;

  assert!(!block.is_empty(), "分配块不得为空");
  assert!(block.reserved_length >= size, "预留长度必须覆盖请求大小");
  let aligned_addr = block.aligned_ptr as usize;
  assert_eq!(
    aligned_addr % align,
    0,
    "对齐地址 {aligned_addr} 必须按 {align} 对齐"
  );
  assert!(
    block.aligned_ptr >= block.base_ptr,
    "对齐指针必须 >= 基地址"
  );

  // OS 新映射必须 demand-zero 全零，且端到端可写
  let slice = block.as_aligned_mut_slice(size);
  assert_eq!(slice.len(), size);
  assert!(slice.iter().all(|&b| b == 0), "fresh 映射必须按需置零");
  slice[0] = 0xAA;
  slice[size - 1] = 0xBB;
  assert_eq!(slice[0], 0xAA);
  assert_eq!(slice[size - 1], 0xBB);

  // Clear 全量清零后必须恢复全零
  unsafe { DirectVirtualMemory::clear(block.aligned_ptr, size) };
  assert!(
    block.as_aligned_slice(size).iter().all(|&b| b == 0),
    "clear 后必须恢复全零"
  );

  // 对齐子切片
  assert_eq!(block.slice(100..200)?.len(), 100);

  // 显式释放且幂等无害
  DirectVirtualMemory::free(&mut block);
  assert!(block.is_empty(), "free 后块必须置空");
  assert_eq!(block.reserved_length, 0);
  DirectVirtualMemory::free(&mut block);

  info!("对标 DirectVmTrackerReflectsAllocation 的 8MB/512 对齐分配与 RAII Drop 释放");
  {
    let before = NativeMemoryTracker::bytes();
    let block2 = DirectVirtualMemory::allocate(8 << 20, 512)?;
    let after_alloc = NativeMemoryTracker::bytes();
    assert!(
      after_alloc >= before + (8 << 20),
      "分配后 tracker 必须增加至少 8MB: before={before}, after={after_alloc}"
    );
    drop(block2);
    let after_drop = NativeMemoryTracker::bytes();
    assert_eq!(
      after_drop, before,
      "RAII Drop 释放后 tracker 必须精准回滚: before={before}, after={after_drop}"
    );
  }

  // 空块方法安全
  let empty = DirectVmBlock::empty();
  assert!(empty.is_empty());
  assert_eq!(empty.as_aligned_slice(100).len(), 0);

  OK
}

/// 对标 C# `DirectVmTrackerReflectsAllocation`：显式 Free 时的内存追踪
#[test]
fn direct_vm_tracker_reflects_allocation() -> Void {
  info!("对标 DirectVmTrackerReflectsAllocation：显式 Free 前后 tracker 状态校验");
  let before = NativeMemoryTracker::bytes();
  let mut block = DirectVirtualMemory::allocate(8 << 20, 512)?;
  let after_alloc = NativeMemoryTracker::bytes();
  assert!(
    after_alloc >= before + (8 << 20),
    "分配后 tracker 应反映直接虚拟内存预留: before={before}, after={after_alloc}"
  );

  DirectVirtualMemory::free(&mut block);
  let after_free = NativeMemoryTracker::bytes();
  assert_eq!(
    after_free, before,
    "显式 Free 后 tracker 应准确扣减已释放字节: before={before}, after={after_free}"
  );

  OK
}

/// 非法参数与越界切片必须报错，绝不越界解引用
#[test]
fn direct_vm_rejects_invalid_arguments() -> Void {
  info!("验证 allocate 非法大小/对齐报错与越界子切片防护");

  // 大小为 0 报错
  assert!(DirectVirtualMemory::allocate(0, 4096).is_err());
  // 对齐为 0 或非 2 的幂报错
  assert!(DirectVirtualMemory::allocate(4096, 0).is_err());
  assert!(DirectVirtualMemory::allocate(4096, 3000).is_err());
  assert!(DirectVirtualMemory::allocate(4096, 5000).is_err());

  // 越界切片与非法区间必须报错
  let mut block = DirectVirtualMemory::allocate(4096, 4096)?;
  assert!(block.slice(0..usize::MAX).is_err());
  // start > end：以变量构造区间，避免 clippy reversed_empty_ranges 对字面量误报
  let (start, end) = (100usize, 50usize);
  assert!(block.slice(start..end).is_err());
  assert!(block.slice_mut(start..end).is_err());

  // 释放后子切片访问必须安全报错
  DirectVirtualMemory::free(&mut block);
  assert!(block.slice(0..1).is_err());
  assert!(block.as_aligned_slice(128).is_empty());

  OK
}

/// 零长度切片安全与系统页大小缓存一致性
#[test]
fn direct_vm_slice_edge_cases_and_page_size() -> Void {
  info!("验证零长度切片返回空切片、空块报错与 system_page_size 稳定性");

  let mut block = DirectVirtualMemory::allocate(8192, 4096)?;

  // 零长度切片在非空块内必须安全返回空切片
  assert_eq!(block.slice(100..100)?.len(), 0);
  assert_eq!(block.slice_mut(200..200)?.len(), 0);

  // 空块必须安全报错且绝不触发 UB
  let empty = DirectVmBlock::empty();
  assert!(empty.slice(0..0).is_err());

  // system_page_size 结果缓存且不低于 4096
  let ps1 = system_page_size();
  assert_eq!(ps1, system_page_size());
  assert!(ps1 >= 4096);

  OK
}
