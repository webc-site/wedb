use std::{
  alloc::{GlobalAlloc, Layout},
  cmp::Ordering as CmpOrdering,
  sync::atomic::{AtomicUsize, Ordering},
};

use mimalloc::MiMalloc;

/// 全局内存分配监控器，追踪当前堆用量与峰值用量
pub struct TrackingAlloc<A: GlobalAlloc> {
  pub inner: A,
  pub allocated: AtomicUsize,
  pub peak_allocated: AtomicUsize,
}

impl<A: GlobalAlloc> TrackingAlloc<A> {
  pub const fn new(inner: A) -> Self {
    Self {
      inner,
      allocated: AtomicUsize::new(0),
      peak_allocated: AtomicUsize::new(0),
    }
  }

  /// 获取当前已分配堆内存字节数
  #[inline]
  pub fn current_allocated(&self) -> usize {
    self.allocated.load(Ordering::Relaxed)
  }

  /// 获取历史峰值堆内存字节数
  #[inline]
  pub fn peak_allocated(&self) -> usize {
    self.peak_allocated.load(Ordering::Relaxed)
  }
}

unsafe impl<A: GlobalAlloc> GlobalAlloc for TrackingAlloc<A> {
  #[inline]
  unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
    let ptr = unsafe { self.inner.alloc(layout) };
    if !ptr.is_null() {
      let curr = self.allocated.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
      self.peak_allocated.fetch_max(curr, Ordering::Relaxed);
    }
    ptr
  }

  #[inline]
  unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
    unsafe { self.inner.dealloc(ptr, layout) };
    self.allocated.fetch_sub(layout.size(), Ordering::Relaxed);
  }

  #[inline]
  unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
    let ptr = unsafe { self.inner.alloc_zeroed(layout) };
    if !ptr.is_null() {
      let curr = self.allocated.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
      self.peak_allocated.fetch_max(curr, Ordering::Relaxed);
    }
    ptr
  }

  #[inline]
  unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
    let new_ptr = unsafe { self.inner.realloc(ptr, layout, new_size) };
    if !new_ptr.is_null() {
      match new_size.cmp(&layout.size()) {
        CmpOrdering::Greater => {
          let diff = new_size - layout.size();
          let curr = self.allocated.fetch_add(diff, Ordering::Relaxed) + diff;
          self.peak_allocated.fetch_max(curr, Ordering::Relaxed);
        }
        CmpOrdering::Less => {
          let diff = layout.size() - new_size;
          self.allocated.fetch_sub(diff, Ordering::Relaxed);
        }
        CmpOrdering::Equal => {}
      }
    }
    new_ptr
  }
}

#[global_allocator]
pub static GLOBAL_ALLOC: TrackingAlloc<MiMalloc> = TrackingAlloc::new(MiMalloc);
