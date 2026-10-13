//! 计数 Lua 分配器：在托管分配器之上叠加"当前分配量 vs 上限"的配额判定，
//! 支持 infallible 通道内的强制分配
//! （对标 libs/server/Lua/LuaTrackedAllocator.cs:LuaTrackedAllocator）。

use crate::{ILuaAllocator, LuaManagedAllocator};

/// 带配额的分配器。
pub struct LuaTrackedAllocator {
  /// 内层托管分配器。
  inner: LuaManagedAllocator,
  /// 配额上限（字节；0 = 无限制）。
  limit_bytes: usize,
  /// 当前用量（字节）。
  used_bytes: usize,
  /// infallible 嵌套深度。
  infallible_depth: u32,
  /// infallible 应急分配指针记录（对标 C# infallibleAllocations）。
  infallible_allocations: Vec<*mut u8>,
}

impl LuaTrackedAllocator {
  /// 构造：`limit_bytes` 为 0 表示无限制。
  pub fn new(limit_bytes: usize) -> Self {
    Self {
      inner: LuaManagedAllocator::default(),
      limit_bytes,
      used_bytes: 0,
      infallible_depth: 0,
      infallible_allocations: Vec::new(),
    }
  }

  /// 当前用量。
  #[inline]
  #[must_use]
  pub const fn used_bytes(&self) -> usize {
    self.used_bytes
  }

  /// libs/server/Lua/LuaTrackedAllocator.cs:InfallibleAllocate
  ///
  /// infallible 通道分配：忽略配额（宿主簿记内存必须成功），记录到应急分配列表。
  pub fn infallible_allocate(&mut self, size: usize, align: usize) -> Option<*mut u8> {
    self.inner.enter_infallible_allocation_region();
    let result = self.inner.allocate_new(size, align);
    let _ = self.inner.try_exit_infallible_allocation_region();
    if let Some(ptr) = result {
      self.infallible_allocations.push(ptr);
    }
    result
  }
  #[doc(hidden)]
  #[must_use]
  pub fn contains_infallible_allocation(&self, ptr: *mut u8) -> bool {
    self.infallible_allocations.contains(&ptr)
  }
}

impl ILuaAllocator for LuaTrackedAllocator {
  fn enter_infallible_allocation_region(&mut self) {
    self.infallible_depth += 1;
    self.inner.enter_infallible_allocation_region();
  }

  fn try_exit_infallible_allocation_region(&mut self) -> bool {
    self.infallible_depth = self.infallible_depth.saturating_sub(1);
    let inner_ok = self.inner.try_exit_infallible_allocation_region();
    self.infallible_allocations.is_empty() && inner_ok
  }

  fn allocate_new(&mut self, size: usize, align: usize) -> Option<*mut u8> {
    let req_size = size.max(1);
    // 配额按用户字节判定（与 used_bytes 记账单位一致）。
    if self.limit_bytes != 0 && self.used_bytes.saturating_add(req_size) > self.limit_bytes {
      if self.infallible_depth > 0 {
        return self.infallible_allocate(size, align);
      }
      return None;
    }
    let result = self.inner.allocate_new(size, align);
    if result.is_some() {
      self.used_bytes = self.used_bytes.saturating_add(req_size);
    }
    result
  }

  unsafe fn resize_allocation(&mut self, ptr: *mut u8, new_size: usize) -> Option<*mut u8> {
    if let Some(pos) = self.infallible_allocations.iter().position(|&p| p == ptr) {
      let new_ptr = unsafe { self.inner.resize_allocation(ptr, new_size) }?;
      self.infallible_allocations[pos] = new_ptr;
      return Some(new_ptr);
    }

    // 旧块用户容量（null = 新分配，旧用量为 0）。
    // SAFETY：本分配器此前产出的块。
    let old = if ptr.is_null() {
      0
    } else {
      unsafe { self.inner.capacity_of(ptr) }.unwrap_or_default()
    };
    let new = new_size.max(1);
    let delta = (new as i64) - (old as i64);

    if self.limit_bytes != 0 && (self.used_bytes as i64 + delta) > self.limit_bytes as i64 {
      if self.infallible_depth > 0 {
        let new_ptr = unsafe { self.inner.resize_allocation(ptr, new_size) }?;
        self.infallible_allocations.push(new_ptr);
        return Some(new_ptr);
      }
      return None;
    }

    // SAFETY：转发约束由调用方保证。
    let result = unsafe { self.inner.resize_allocation(ptr, new_size) }?;
    self.used_bytes = (self.used_bytes as i64 + delta).max(0) as usize;
    Some(result)
  }

  unsafe fn deallocate(&mut self, ptr: *mut u8) {
    if ptr.is_null() {
      return;
    }
    if let Some(pos) = self.infallible_allocations.iter().position(|&p| p == ptr) {
      self.infallible_allocations.swap_remove(pos);
      // SAFETY: 属于内层分配器产出
      unsafe { self.inner.deallocate(ptr) };
      return;
    }

    // SAFETY：内层托管分配器产出的块，约束由调用方保证。
    let freed = unsafe { self.inner.capacity_of(ptr) }.unwrap_or_default();
    unsafe { self.inner.deallocate(ptr) };
    self.used_bytes = self.used_bytes.saturating_sub(freed);
  }
}
