//! Lua 分配器接口（对标 libs/server/Lua/ILuaAllocator.cs:ILuaAllocator）。

/// lua 分配器抽象：宿主侧内存语义钩子。
pub trait ILuaAllocator {
  /// libs/server/Lua/ILuaAllocator.cs:EnterInfallibleAllocationRegion
  ///
  /// 进入"分配必成功"区（如 VM 内部簿记）；此区间内失败即中止。
  fn enter_infallible_allocation_region(&mut self);

  /// libs/server/Lua/ILuaAllocator.cs:TryExitInfallibleAllocationRegion
  ///
  /// 退出"分配必成功"区；返回 false 表示退出前已发生不可恢复失败。
  fn try_exit_infallible_allocation_region(&mut self) -> bool;

  /// libs/server/Lua/ILuaAllocator.cs:AllocateNew
  ///
  /// 分配 `size` 字节对齐 `align` 的新块；失败返回 None。
  fn allocate_new(&mut self, size: usize, align: usize) -> Option<*mut u8>;

  /// libs/server/Lua/ILuaAllocator.cs:ResizeAllocation
  ///
  /// 原地/搬移调整分配；失败返回 None（原块保持有效）。
  ///
  /// # Safety
  /// `ptr` 须为此分配器此前产出且仍有效的块。
  unsafe fn resize_allocation(&mut self, ptr: *mut u8, new_size: usize) -> Option<*mut u8>;
}
