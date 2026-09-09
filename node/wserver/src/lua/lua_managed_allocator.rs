//! 托管 Lua 分配器：以全局分配器承接 lua 请求并跟踪用量
//! （对标 libs/server/Lua/LuaManagedAllocator.cs:LuaManagedAllocator）。
//!
//! 每块前置 `BlockHeader`（容量 + 对齐）以支撑 resize；用量簿记供
//! INFO/监视面读取。

use std::alloc::{alloc as std_alloc, dealloc as std_dealloc, realloc as std_realloc, Layout};

use super::i_lua_allocator::ILuaAllocator;

/// 块头：记录实际布局，resize/释放时还原。
#[repr(C)]
struct BlockHeader {
  /// 含头的总容量。
  capacity: usize,
  /// 对齐。
  align: usize,
}

impl BlockHeader {
  const LAYOUT: Layout = match Layout::from_size_align(size_of::<Self>(), align_of::<Self>()) {
    Ok(l) => l,
    Err(_) => panic!("块头布局非法"),
  };
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
  pub fn allocated_bytes(&self) -> usize {
    self.allocated_bytes
  }

  fn layout_for(size: usize, align: usize) -> Option<Layout> {
    Layout::from_size_align(size.max(1).checked_add(BlockHeader::LAYOUT.size())?, align.max(BlockHeader::LAYOUT.align())).ok()
  }

  unsafe fn header_of(ptr: *mut u8) -> *mut BlockHeader {
    ptr.cast::<BlockHeader>().sub(1)
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
    // SAFETY：ptr 指向 layout.size() 字节，可容纳块头。
    let header = unsafe { ptr.cast::<BlockHeader>() };
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
      return self.allocate_new(new_size, 1);
    }
    // SAFETY：ptr 为本分配器产出且有效的块。
    let header = unsafe { Self::header_of(ptr) };
    let (capacity, align) = unsafe { ((*header).capacity, (*header).align) };
    let Some(new_layout) = Self::layout_for(new_size, align) else {
      return None;
    };
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
}

impl Drop for LuaManagedAllocator {
  fn drop(&mut self) {
    // mlua 的 luau VM 会在销毁时回调释放全部块；此处的用量簿记随之归零。
    self.allocated_bytes = 0;
  }
}

// 引用全局释放函数以保持与 C# Dispose 形态对齐的编译期校验。
#[allow(dead_code)]
fn _dealloc_unused() {
  let _ = (std_dealloc, BlockHeader::LAYOUT);
}
