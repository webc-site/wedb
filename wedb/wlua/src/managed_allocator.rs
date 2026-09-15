//! 托管 Lua 分配器：以全局分配器承接 lua 请求并跟踪用量
//! （对标 libs/server/Lua/LuaManagedAllocator.cs:LuaManagedAllocator）。
//!
//! 每块前置 `BlockHeader`（容量 + 对齐）以支撑 resize；用量簿记供
//! INFO/监视面读取。

use std::alloc::{Layout, alloc as std_alloc, dealloc as std_dealloc, realloc as std_realloc};

use crate::ILuaAllocator;

/// 块头：记录实际布局，resize/释放时还原。
#[repr(C)]
struct BlockHeader {
  /// 含头的总容量。
  capacity: usize,
  /// 对齐。
  align: usize,
}

impl BlockHeader {
  const LAYOUT: Layout = Layout::new::<Self>();
}

/// 全局分配器承接的托管分配器（用量簿记 + infallible 区间）。
#[derive(Default)]
pub struct LuaManagedAllocator {
  /// 当前分配字节数（含块头）。
  allocated_bytes: usize,
  /// infallible 区间深度（>0 即处于其中）。
  infallible_depth: u32,
}

impl LuaManagedAllocator {
  /// 当前已分配字节数（含块头）。
  #[inline]
  #[must_use]
  pub const fn allocated_bytes(&self) -> usize {
    self.allocated_bytes
  }

  /// 块当前用户容量（不含块头；非本分配器产出的块或空指针返回 None）。
  ///
  /// # Safety
  /// `ptr` 须为本分配器产出且未释放的块，或为 null。
  pub unsafe fn capacity_of(&self, ptr: *mut u8) -> Option<usize> {
    if ptr.is_null() {
      return None;
    }
    // SAFETY：调用方约束 ptr 指向本分配器产出块，块头紧邻其前。
    let header = unsafe { Self::header_of(ptr) };
    // SAFETY：块头在块生存期内有效。
    let capacity = unsafe { (*header).capacity };
    capacity.checked_sub(BlockHeader::LAYOUT.size())
  }

  fn layout_for(size: usize, align: usize) -> Option<Layout> {
    Layout::from_size_align(
      size.max(1).checked_add(BlockHeader::LAYOUT.size())?,
      align.max(BlockHeader::LAYOUT.align()).max(16),
    )
    .ok()
  }

  unsafe fn header_of(ptr: *mut u8) -> *mut BlockHeader {
    // SAFETY：ptr 由 allocate_new 产出，块头紧邻其前。
    unsafe { ptr.cast::<BlockHeader>().sub(1) }
  }
}

impl ILuaAllocator for LuaManagedAllocator {
  fn enter_infallible_allocation_region(&mut self) {
    self.infallible_depth += 1;
  }

  fn try_exit_infallible_allocation_region(&mut self) -> bool {
    self.infallible_depth = self.infallible_depth.saturating_sub(1);
    true
  }

  fn allocate_new(&mut self, size: usize, align: usize) -> Option<*mut u8> {
    let layout = Self::layout_for(size, align)?;
    // SAFETY：layout 尺寸 >= 1 且对齐为 2 的幂（from_size_align 已校验）。
    let ptr = unsafe { std_alloc(layout) };
    if ptr.is_null() {
      return None;
    }
    let header = ptr.cast::<BlockHeader>();
    unsafe {
      (*header) = BlockHeader {
        capacity: layout.size(),
        align: layout.align(),
      };
    }
    self.allocated_bytes += layout.size();
    // SAFETY：块头之后的区域即用户区。
    Some(unsafe { ptr.add(BlockHeader::LAYOUT.size()) })
  }

  unsafe fn resize_allocation(&mut self, ptr: *mut u8, new_size: usize) -> Option<*mut u8> {
    if ptr.is_null() {
      return self.allocate_new(new_size, 16);
    }
    // SAFETY：ptr 为本分配器产出且有效的块。
    let header = unsafe { Self::header_of(ptr) };
    let (capacity, align) = unsafe { ((*header).capacity, (*header).align) };
    let new_layout = Self::layout_for(new_size, align)?;
    let old_layout = Layout::from_size_align(capacity, align).ok()?;
    // SAFETY：header 指向 old_layout 的块。
    let base = header.cast::<u8>();
    // SAFETY：base 为 alloc/realloc 产出且布局匹配。
    let resized = unsafe { std_realloc(base, old_layout, new_layout.size()) };
    if resized.is_null() {
      return None;
    }
    self.allocated_bytes = self.allocated_bytes.saturating_sub(capacity) + new_layout.size();
    // SAFETY：新块头随后。
    unsafe {
      (*resized.cast::<BlockHeader>()) = BlockHeader {
        capacity: new_layout.size(),
        align: new_layout.align(),
      };
    }
    // SAFETY：用户区在块头之后。
    Some(unsafe { resized.add(BlockHeader::LAYOUT.size()) })
  }

  unsafe fn deallocate(&mut self, ptr: *mut u8) {
    if ptr.is_null() {
      return;
    }
    // SAFETY：ptr 为本分配器产出且有效的块。
    let header = unsafe { Self::header_of(ptr) };
    let (capacity, align) = unsafe { ((*header).capacity, (*header).align) };
    let Ok(layout) = Layout::from_size_align(capacity, align) else {
      return;
    };
    self.allocated_bytes = self.allocated_bytes.saturating_sub(capacity);
    // SAFETY：base 为 alloc/realloc 产出且布局匹配。
    unsafe { std_dealloc(header.cast(), layout) };
  }
}

impl Drop for LuaManagedAllocator {
  fn drop(&mut self) {
    // luau VM 销毁时会回调释放全部块；此处的用量簿记随之归零。
    self.allocated_bytes = 0;
  }
}
