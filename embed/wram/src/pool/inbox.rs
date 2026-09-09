//! 跨线程 MPSC 无锁单向栈 (对标 C# `Bucket.crossThreadHead` 与 `TryPushCrossThread`/`ClaimCrossThread`)
//!
//! 在异线程（如 compio 异步 I/O 完成线程）归还缓冲区时，利用缓冲区首部侵入式存放的
//! [`FreeNode`]，通过 CAS 单向栈实现 0 锁推入；属主线程在需要时通过单次原子 `swap`
//! 批量收割整条链表，彻底消除 ABA 隐患。

use std::{
  mem::size_of,
  ptr::{self, NonNull, write_bytes},
  sync::atomic::{
    AtomicPtr,
    Ordering::{AcqRel, Acquire, Release},
  },
};

use super::{CachedBuf, NUM_CLASSES};

/// 跨线程单向无锁栈的侵入式链表节点 (嵌于空闲缓冲区的首部，零额外堆分配)
#[repr(C)]
pub(crate) struct FreeNode {
  pub(crate) next: *mut FreeNode,
  pub(crate) cap: usize,
  pub(crate) align: usize,
  pub(crate) cacheable: bool,
  pub(crate) dirty: bool,
}

/// 密封哨兵指针：指示属主线程已退出 (对标 C# SectorAlignedBufferPool.Sealed)
pub(crate) const SEALED: *mut FreeNode = usize::MAX as *mut FreeNode;

/// 单个 class 的跨线程栈顶指针
///
/// 强制 64 字节对齐且恰好 64 字节大小，使数组中相邻 class 的栈顶各自独占缓存行：
/// 异线程生产者跨 class 推入、属主逐 class 收割时互不弹跳 (对标 C# `Bucket` 以
/// Explicit FieldOffset 将 `crossThreadHead` 隔离到独立缓存行，并以
/// `BucketHeadsAreOnSeparateCacheLines` 回归测试守护；属主热路径的本地栈位于独立的
/// 堆分配 `TlsPoolEntry` 中，与收件箱天然不同行)
#[repr(align(64))]
pub(crate) struct Head(AtomicPtr<FreeNode>);

/// 跨线程 MPSC 无锁归还收件箱 (对标 C# Bucket.crossThreadHead)
pub(crate) struct CrossThreadInbox {
  heads: [Head; NUM_CLASSES],
}

impl CrossThreadInbox {
  pub(crate) fn new() -> Self {
    Self {
      heads: [const { Head(AtomicPtr::new(ptr::null_mut())) }; NUM_CLASSES],
    }
  }

  /// 跨线程归还：通过 CAS 推入单向栈，若已密封则返回 false (对标 C# TryPushCrossThread)
  pub(crate) fn try_push(&self, cls: usize, node: *mut FreeNode) -> bool {
    let head_ptr = &self.heads[cls].0;
    let mut head = head_ptr.load(Acquire);
    loop {
      if head == SEALED {
        return false;
      }
      unsafe { (*node).next = head };
      match head_ptr.compare_exchange_weak(head, node, Release, Acquire) {
        Ok(_) => return true,
        Err(actual) => head = actual,
      }
    }
  }

  /// 属主线程批量收割整条链表：1 次原子 CAS，无 ABA 隐患 (对标 C# ClaimCrossThread)
  pub(crate) fn claim(&self, cls: usize) -> *mut FreeNode {
    let head_ptr = &self.heads[cls].0;
    let mut head = head_ptr.load(Acquire);
    loop {
      if head.is_null() || head == SEALED {
        return ptr::null_mut();
      }
      match head_ptr.compare_exchange_weak(head, ptr::null_mut(), AcqRel, Acquire) {
        Ok(_) => return head,
        Err(actual) => head = actual,
      }
    }
  }

  /// 密封收件箱并拔出所有在途节点 (线程退出时调用)
  pub(crate) fn seal_and_drain(&self, cls: usize) -> *mut FreeNode {
    self.heads[cls].0.swap(SEALED, AcqRel)
  }
}

/// 侵入式单向链表的零分配就地迭代器
pub(crate) struct ChainIter {
  curr: *mut FreeNode,
}

impl ChainIter {
  #[inline]
  pub(crate) const fn new(curr: *mut FreeNode) -> Self {
    Self { curr }
  }
}

impl Iterator for ChainIter {
  type Item = CachedBuf;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    if self.curr.is_null() || self.curr == SEALED {
      return None;
    }
    unsafe {
      let node = self.curr;
      let next = (*node).next;
      let cap = (*node).cap;
      let align = (*node).align;
      let cacheable = (*node).cacheable;
      let dirty = (*node).dirty;
      let ptr = NonNull::new_unchecked(node as *mut u8);
      if !dirty {
        write_bytes(node as *mut u8, 0, size_of::<FreeNode>());
      }
      self.curr = next;
      Some(CachedBuf {
        ptr,
        cap,
        align,
        cacheable,
        dirty,
      })
    }
  }
}

#[cfg(test)]
mod tests {
  use std::mem::{offset_of, size_of};

  use super::{CrossThreadInbox, Head};

  /// 缓存行隔离回归 (对标 C# `BucketHeadsAreOnSeparateCacheLines`)：
  /// 每个 class 的跨线程栈顶独占 64B 缓存行，跨 class 并发推入/收割互不伪共享
  #[test]
  fn class_heads_are_on_separate_cache_lines() {
    assert_eq!(size_of::<Head>(), 64, "单个 class 栈顶必须独占 64B 缓存行");
    // 数组步长 = 64B：相邻 class 的栈顶必落在不同缓存行
    assert_eq!(size_of::<[Head; 4]>(), 256);
    // 栈顶数组必须位于收件箱首部，避免未来新增字段挤偏缓存行对齐
    assert_eq!(offset_of!(CrossThreadInbox, heads), 0);
  }
}
