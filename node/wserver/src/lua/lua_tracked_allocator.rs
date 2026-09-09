//! 计数 Lua 分配器：在托管分配器之上叠加"当前分配量 vs 上限"的配额判定，
//! 支持 infallible 通道内的强制分配
//! （对标 libs/server/Lua/LuaTrackedAllocator.cs:LuaTrackedAllocator）。

use super::{i_lua_allocator::ILuaAllocator, lua_managed_allocator::LuaManagedAllocator};

/// 带配额的分配器。
pub struct LuaTrackedAllocator {
  /// 内层托管分配器。
  inner: LuaManagedAllocator,
  /// 配额上限（字节；0 = 无限制）。
  limit_bytes: usize,
  /// 当前用量（字节）。
  used_bytes: usize,
}

impl LuaTrackedAllocator {
  /// 构造：`limit_bytes` 为 0 表示无限制。
  pub fn new(limit_bytes: usize) -> Self {
    Self {
      inner: LuaManagedAllocator::default(),
      limit_bytes,
      used_bytes: 0,
    }
  }

  /// 当前用量。
  pub fn used_bytes(&self) -> usize {
    self.used_bytes
  }

  /// libs/server/Lua/LuaTrackedAllocator.cs:InfallibleAllocate
  ///
  /// infallible 通道分配：忽略配额（宿主簿记内存必须成功）。
  pub fn infallible_allocate(&mut self, size: usize, align: usize) -> Option<*mut u8> {
    self.inner.enter_infallible_allocation_region();
    let result = self.inner.allocate_new(size, align);
    let _ = self.inner.try_exit_infallible_allocation_region();
    if result.is_some() {
      self.used_bytes += size.max(1);
    }
    result
  }

  /// libs/server/Lua/LuaTrackedAllocator.cs:IsInfallibleAllocation
  ///
  /// 指针是否处于 infallible 通道（配额豁免）分配的判定入口：
  /// 无配额时全部分配均为 infallible。
  pub fn is_infallible_allocation(&self) -> bool {
    self.limit_bytes == 0
  }
}

impl ILuaAllocator for LuaTrackedAllocator {
  fn enter_infallible_allocation_region(&mut self) {
    self.inner.enter_infallible_allocation_region();
  }

  fn try_exit_infallible_allocation_region(&mut self) -> bool {
    self.inner.try_exit_infallible_allocation_region()
  }

  fn allocate_new(&mut self, size: usize, align: usize) -> Option<*mut u8> {
    if self.limit_bytes != 0 && self.used_bytes + size.max(1) > self.limit_bytes {
      return None;
    }
    let result = self.inner.allocate_new(size, align);
    if result.is_some() {
      self.used_bytes += size.max(1);
    }
    result
  }

  unsafe fn resize_allocation(&mut self, ptr: *mut u8, new_size: usize) -> Option<*mut u8> {
    if self.limit_bytes != 0 && new_size > self.limit_bytes {
      return None;
    }
    // SAFETY：转发约束由调用方保证。
    unsafe { self.inner.resize_allocation(ptr, new_size) }
  }
}

#[cfg(test)]
mod tests {
  use super::{ILuaAllocator, LuaTrackedAllocator};

  #[test]
  fn quota_blocks_over_limit() {
    let mut alloc = LuaTrackedAllocator::new(64);
    assert!(alloc.allocate_new(32, 1).is_some());
    // 超出配额被拒。
    assert!(alloc.allocate_new(64, 1).is_none());
    // 未超配额允许。
    assert!(alloc.allocate_new(32, 1).is_some());
    assert_eq!(alloc.used_bytes(), 64);
  }

  #[test]
  fn infallible_channel_ignores_quota() {
    let mut alloc = LuaTrackedAllocator::new(16);
    assert!(alloc.infallible_allocate(8, 1).is_some());
    // 有配额时常规通道判定为非 infallible；无配额时全量豁免。
    assert!(!alloc.is_infallible_allocation());
    assert!(LuaTrackedAllocator::new(0).is_infallible_allocation());
  }

  #[test]
  fn unlimited_when_no_limit() {
    let mut alloc = LuaTrackedAllocator::new(0);
    assert!(alloc.allocate_new(4096, 1).is_some());
  }
}
