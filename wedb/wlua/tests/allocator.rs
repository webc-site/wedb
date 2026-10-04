//! Lua 内存分配器集成测试
//! 对标 libs/server/Lua/LuaLimitedManagedAllocator.cs 与 LuaTrackedAllocator.cs

use std::{ptr::write_bytes, slice::from_raw_parts};

use wlua::{ILuaAllocator, LuaLimitedManagedAllocator, LuaTrackedAllocator};

#[test]
fn limited_allocate_free_coalesce_cycle() {
  let mut alloc = LuaLimitedManagedAllocator::new(1024);
  let a = alloc.allocate_new(64, 1).unwrap();
  let b = alloc.allocate_new(64, 1).unwrap();
  assert_ne!(a, b);
  assert!(alloc.check_correctness());
  // 分配后剩余空间保留为空闲块。
  assert_eq!(alloc.free_list_len(), 1);
}

#[test]
fn limited_quota_exhaustion_returns_none() {
  let mut alloc = LuaLimitedManagedAllocator::new(256);
  assert!(alloc.allocate_new(256, 1).is_some());
  assert!(alloc.allocate_new(32, 1).is_none());
  assert!(alloc.check_correctness());
}

#[test]
fn limited_split_marks_and_reuses() {
  let mut alloc = LuaLimitedManagedAllocator::new(1024);
  let a = alloc.allocate_new(64, 1).unwrap();
  // 分裂在用块：尾部转空闲。
  let tail = alloc.split_in_use_block_by_ptr(a, 32);
  assert!(tail.is_some());
  assert_eq!(alloc.free_list_len(), 2);
}

#[test]
fn limited_infallible_emergency_allocation_and_exit_detection() {
  let mut alloc = LuaLimitedManagedAllocator::new(128);
  let _a = alloc.allocate_new(128, 1).unwrap();
  // 普通分配超额被拒
  assert!(alloc.allocate_new(64, 1).is_none());

  // 进入 infallible 区域
  alloc.enter_infallible_allocation_region();
  let emergency = alloc.allocate_new(64, 16);
  assert!(emergency.is_some());
  let emergency_ptr = emergency.unwrap();
  // 应急登记表是单一事实源：指针已入表。
  assert!(alloc.contains_infallible_allocation(emergency_ptr));

  // 退出时返回 false，对标 C# NeedsDispose
  assert!(!alloc.try_exit_infallible_allocation_region());

  // 释放应急块
  unsafe {
    alloc.deallocate(emergency_ptr);
  }
}

#[test]
fn limited_resize_out_of_place_fallback_preserves_data() {
  let mut alloc = LuaLimitedManagedAllocator::new(256);
  let a = alloc.allocate_new(64, 1).unwrap();
  let b = alloc.allocate_new(64, 1).unwrap();
  unsafe {
    write_bytes(a, 0xAA, 64);
    write_bytes(b, 0xBB, 64);
  }

  // 此时 a 后面是 b (InUse)，a 无法原地扩容到 128
  let resized_a = unsafe { ILuaAllocator::resize_allocation(&mut alloc, a, 128) };
  assert!(resized_a.is_some());
  let new_a = resized_a.unwrap();
  assert_ne!(new_a, a);
  unsafe {
    let slice = from_raw_parts(new_a, 64);
    assert!(slice.iter().all(|&v| v == 0xAA));
  }
}

#[test]
fn tracked_quota_blocks_over_limit() {
  let mut alloc = LuaTrackedAllocator::new(64);
  assert!(alloc.allocate_new(32, 1).is_some());
  // 超出配额被拒。
  assert!(alloc.allocate_new(64, 1).is_none());
  // 未超配额允许。
  assert!(alloc.allocate_new(32, 1).is_some());
  assert_eq!(alloc.used_bytes(), 64);
}

#[test]
fn tracked_infallible_channel_ignores_quota() {
  let mut alloc = LuaTrackedAllocator::new(16);
  let ptr = alloc.infallible_allocate(8, 1);
  assert!(ptr.is_some());
  let ptr = ptr.unwrap();
  // 配额豁免：应急通道不计入 used_bytes，且登记在应急表内
  assert_eq!(alloc.used_bytes(), 0);
  assert!(alloc.contains_infallible_allocation(ptr));
}

#[test]
fn tracked_infallible_emergency_allocation_and_exit_detection() {
  let mut alloc = LuaTrackedAllocator::new(64);
  let _a = alloc.allocate_new(64, 1).unwrap();
  // 超额普通分配失败
  assert!(alloc.allocate_new(32, 1).is_none());

  // 进入 infallible 区域
  alloc.enter_infallible_allocation_region();
  let emergency = alloc.allocate_new(32, 1);
  assert!(emergency.is_some());
  let emergency_ptr = emergency.unwrap();
  // 应急登记表是单一事实源：指针已入表。
  assert!(alloc.contains_infallible_allocation(emergency_ptr));

  // 退出时返回 false，对标 C# NeedsDispose
  assert!(!alloc.try_exit_infallible_allocation_region());

  // 释放应急块
  unsafe {
    alloc.deallocate(emergency_ptr);
  }
}

#[test]
fn tracked_unlimited_when_no_limit() {
  let mut alloc = LuaTrackedAllocator::new(0);
  assert!(alloc.allocate_new(4096, 1).is_some());
}
