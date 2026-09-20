//! lua 分配器挂接：`lua_Alloc` 适配层（对标 libs/server/Lua/ILuaAllocator.cs）。
//!
//! [`LuaState::with_allocator`](crate::LuaState::with_allocator) 把
//! [`ILuaAllocator`] 直挂 Luau 的 `lua_newstate` 分配器：配额超限返回 NULL，
//! VM 以 "not enough memory" 内存错误中止脚本（可被 pcall 捕获）。

use std::{cell::UnsafeCell, ffi::c_void, ptr};

use enum_dispatch::enum_dispatch;

use crate::{
  limited_allocator::LuaLimitedManagedAllocator, managed_allocator::LuaManagedAllocator,
  tracked_allocator::LuaTrackedAllocator,
};

/// lua 分配器抽象：宿主侧内存语义钩子。
///
/// 全部方法在 `lua_Alloc` 回调内执行——实现体**禁止 panic**（跨 FFI 展开
/// 即进程中止），失败一律以 `None` 表达。
#[enum_dispatch]
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

  /// lua_Alloc 的释放语义（`nsize == 0`）。
  ///
  /// # Safety
  /// `ptr` 须为此分配器此前产出且未释放的块。
  unsafe fn deallocate(&mut self, ptr: *mut u8);
}

/// 支持的 Lua 内存分配器枚举（静态分发）
#[enum_dispatch(ILuaAllocator)]
pub enum LuaAllocator {
  Tracked(LuaTrackedAllocator),
  Limited(LuaLimitedManagedAllocator),
  Managed(LuaManagedAllocator),
}

/// 分配器槽：`lua_Alloc` 的 `ud` 指向此堆址。
///
/// `UnsafeCell` 直取 `&mut`：回调重入不可能成立——LuaState 自身方法持有
/// `&mut self` 的全程内，槽的借用只发生在 VM 发起的分配点，两侧从不交叠。
pub(crate) type AllocatorSlot = UnsafeCell<LuaAllocator>;

/// `lua_Alloc` 适配：转发到 [`ILuaAllocator`]。
///
/// # Safety（回调契约）
/// - `ud` 由 [`crate::LuaState::with_allocator`] 注册，指向 LuaState 持有的
///   [`AllocatorSlot`] 堆址；生存期由 Drop 序（先 `lua_close` 后弃 Box）保证。
/// - 实现体无 panic 路径（trait 契约）；配额不足以 NULL 回报。
/// - `_osize`: Lua C API `lua_Alloc` 回调签名要求的原分配大小，本适配层委托 Rust 底层重置或释放语义故未直接消费。
pub(crate) unsafe extern "C" fn alloc_shim(
  ud: *mut c_void,
  ptr: *mut c_void,
  _osize: usize,
  nsize: usize,
) -> *mut c_void {
  // SAFETY：ud 指向随 LuaState 存活的槽（见上），借用仅限本回调帧。
  let allocator = unsafe { &mut *(*ud.cast::<AllocatorSlot>()).get() };
  let allocated = if nsize == 0 {
    if !ptr.is_null() {
      // SAFETY：VM 只回放本分配器产出且未释放的块。
      unsafe { allocator.deallocate(ptr.cast()) };
    }
    None
  } else if ptr.is_null() {
    // Luau 分配器契约：按 max_align_t 对齐（16 足覆盖所有内建类型）。
    const VM_ALIGN: usize = 16;
    allocator.allocate_new(nsize, VM_ALIGN)
  } else {
    // SAFETY：扩缩容的旧块由本分配器产出且有效。
    unsafe { allocator.resize_allocation(ptr.cast(), nsize) }
  };
  allocated.map_or(ptr::null_mut(), |p| p.cast())
}
