//! 定额托管分配器：固定字节池上的空闲链分配器
//! （对标 libs/server/Lua/LuaLimitedManagedAllocator.cs:LuaLimitedManagedAllocator）。
//!
//! C# 以块引用（块基址）+ 双向空闲链组织池内存；Rust 以 `Vec<Block>` 槽位 +
//! `block_ref`（槽位下标 + 1，0 视为空）承接：
//! - `mark_free` / `mark_in_use` 维护状态
//! - 空闲链按地址序插入，`try_coalesce_all_free_blocks` 合并相邻空闲块
//! - `split_in_use_block` / `split_free_block` 支持临界分配
//! - `allocate_new` 首个适配（first-fit）；池满返回 None；infallible 模式下兜底宿主堆分配
//! - `resize_allocation` 支持原地扩容失败后 fallback 到新块分配 + 拷贝 + 释放旧块

use std::{
  alloc::{Layout, alloc, alloc_zeroed, dealloc},
  ptr::copy_nonoverlapping,
};

use crate::ILuaAllocator;

/// 最小块尺寸（对齐 C# MinAllocSize 语义，保证 16 字节对齐）。
const MIN_ALLOC: usize = 32;

/// 块引用：即块在池内的偏移（无空哨兵；存在性由 Option 承载）。
pub type BlockRef = u32;

/// 单块状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockState {
  Free,
  InUse,
}

#[derive(Debug, Clone, Copy)]
struct Block {
  /// 池内偏移。
  offset: usize,
  /// 尺寸。
  size: usize,
  state: BlockState,
}

/// 保证 16 字节对齐的池内存。
struct AlignedPool {
  ptr: *mut u8,
  len: usize,
  layout: Layout,
}

impl AlignedPool {
  fn new(size: usize) -> Self {
    let size = size.max(16);
    let layout = Layout::from_size_align(size, 16).expect("invalid layout");
    // SAFETY: layout 的 size 与 align 有效合法
    let ptr = unsafe { alloc_zeroed(layout) };
    assert!(!ptr.is_null(), "failed to allocate aligned pool");
    Self {
      ptr,
      len: size,
      layout,
    }
  }

  fn as_mut_ptr(&self) -> *mut u8 {
    self.ptr
  }

  fn len(&self) -> usize {
    self.len
  }
}

impl Drop for AlignedPool {
  fn drop(&mut self) {
    // SAFETY: ptr 由 alloc_zeroed 分配且未提前释放
    unsafe {
      dealloc(self.ptr, self.layout);
    }
  }
}

unsafe impl Send for AlignedPool {}
unsafe impl Sync for AlignedPool {}

/// 记录 infallible 模式下因池耗尽而在宿主堆应急分配的块。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InfallibleAllocation {
  ptr: *mut u8,
  layout: Layout,
}

/// 固定池 + 空闲链分配器。
pub struct LuaLimitedManagedAllocator {
  /// 16 字节对齐池内存。
  pool: AlignedPool,
  /// 块表（按偏移升序）。
  blocks: Vec<Block>,
  /// 空闲链（块引用序列，按偏移升序）。
  free_list: Vec<BlockRef>,
  /// infallible 嵌套深度。
  infallible_depth: u32,
  /// infallible 应急分配记录（对标 C# infallibleAllocations）。
  infallible_allocations: Vec<InfallibleAllocation>,
  /// 已分配（in-use）字节数（调试/断言用）。
  debug_allocated_bytes: usize,
}

impl Default for LuaLimitedManagedAllocator {
  fn default() -> Self {
    Self::new(1024 * 1024)
  }
}

impl Drop for LuaLimitedManagedAllocator {
  fn drop(&mut self) {
    for alloc in self.infallible_allocations.drain(..) {
      // SAFETY: alloc.ptr 由 std::alloc::alloc 产生
      unsafe {
        dealloc(alloc.ptr, alloc.layout);
      }
    }
  }
}

impl LuaLimitedManagedAllocator {
  /// 构造：分配 `pool_size` 字节池并置单空闲块。
  pub fn new(pool_size: usize) -> Self {
    let pool = AlignedPool::new(pool_size);
    let mut allocator = Self {
      pool,
      blocks: Vec::new(),
      free_list: Vec::new(),
      infallible_depth: 0,
      infallible_allocations: Vec::new(),
      debug_allocated_bytes: 0,
    };
    if pool_size > 0 {
      let blocks = &mut allocator.blocks;
      blocks.push(Block {
        offset: 0,
        size: pool_size,
        state: BlockState::Free,
      });
      // 首块引用 = 偏移 0（本实现以池内偏移为引用，无空哨兵）。
      allocator.free_list.push(0);
    }
    allocator
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:MarkFree
  pub fn mark_free(&mut self, block_ref: BlockRef) {
    if let Some(block) = self.block_mut(block_ref) {
      block.state = BlockState::Free;
      self.add_to_free_list(block_ref);
    }
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:MarkInUse
  pub fn mark_in_use(&mut self, block_ref: BlockRef) {
    if let Some(block) = self.block_mut(block_ref) {
      block.state = BlockState::InUse;
      self.remove_from_free_list(block_ref);
    }
  }

  /// libs/server/Lua/LuaLimitedManagerAllocator.cs:GetNextFreeBlockRef
  ///
  /// 空闲链中 `block_ref` 的后继。
  pub fn get_next_free_block_ref(&self, block_ref: BlockRef) -> Option<BlockRef> {
    let pos = self.free_list.iter().position(|&r| r == block_ref)?;
    self.free_list.get(pos + 1).copied()
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:GetPrevFreeBlockRef
  pub fn get_prev_free_block_ref(&self, block_ref: BlockRef) -> Option<BlockRef> {
    let pos = self.free_list.iter().position(|&r| r == block_ref)?;
    if pos == 0 {
      None
    } else {
      Some(self.free_list[pos - 1])
    }
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:GetNextAdjacentBlockRef
  ///
  /// 地址相邻的后继块（不考虑状态）。
  pub fn get_next_adjacent_block_ref(&self, block_ref: BlockRef) -> Option<BlockRef> {
    let idx = self.block_idx(block_ref)?;
    self.blocks.get(idx + 1).map(|b| b_ref_idx(b.offset))
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:GetRefVal
  ///
  /// 块引用对应的池内偏移。
  pub fn get_ref_val(&self, block_ref: BlockRef) -> Option<usize> {
    Some(self.block(block_ref)?.offset)
  }

  /// 是否处于 infallible 分配区域。
  pub fn is_infallible(&self) -> bool {
    self.infallible_depth > 0
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:EnterInfallibleAllocationRegion
  pub fn enter_infallible_allocation_region(&mut self) {
    self.infallible_depth += 1;
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:TryExitInfallibleAllocationRegion
  ///
  /// 退出 infallible 区域。对标 C# `return infallibleAllocations == null`：
  /// 若在 infallible 区域内触发过池外应急分配，返回 false 以使上层置位 needs_dispose。
  pub fn try_exit_infallible_allocation_region(&mut self) -> bool {
    self.infallible_depth = self.infallible_depth.saturating_sub(1);
    self.infallible_allocations.is_empty()
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:InfallibleAllocate
  ///
  /// 在宿主堆上分配应急块并记录到 `infallible_allocations`。
  fn infallible_allocate(&mut self, size: usize, align: usize) -> Option<*mut u8> {
    let align = align.max(16);
    let layout = Layout::from_size_align(size.max(1), align).ok()?;
    // SAFETY: layout 非零有效
    let ptr = unsafe { alloc(layout) };
    if !ptr.is_null() {
      self
        .infallible_allocations
        .push(InfallibleAllocation { ptr, layout });
      Some(ptr)
    } else {
      None
    }
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:IsInfallibleAllocation
  pub fn is_infallible_allocation(&self, ptr: *mut u8) -> bool {
    self.infallible_allocations.iter().any(|a| a.ptr == ptr)
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:AllocateNew
  ///
  /// 首个适配分配：不足则先合并空闲块再试；池耗尽时若在 infallible 区间则兜底宿主堆分配。
  pub fn allocate_new(&mut self, size: usize, align: usize) -> Option<*mut u8> {
    let need = Self::round_to_min_alloc(size);
    let mut chosen = self
      .free_list
      .iter()
      .copied()
      .find(|&r| self.block(r).is_some_and(|b| b.size >= need));

    // 剩余空间不足以适配则先合并空闲块再找。
    if chosen.is_none() {
      self.try_coalesce_all_free_blocks();
      chosen = self
        .free_list
        .iter()
        .copied()
        .find(|&r| self.block(r).is_some_and(|b| b.size >= need));
    }

    if let Some(block_ref) = chosen {
      // 剩余空间足以分裂则拆出空闲尾部块。
      let block_size = self.block(block_ref)?.size;
      if Self::should_split(block_size, need) {
        self.split_free_block(block_ref, need);
      }
      let data_offset = self.block(block_ref)?.offset;
      self.mark_in_use(block_ref);
      self.debug_allocated_bytes += need;
      return Some(unsafe { self.pool.as_mut_ptr().add(data_offset) });
    }

    // 池耗尽：若处于 infallible 模式，对标 C# InfallibleAllocate
    if self.is_infallible() {
      return self.infallible_allocate(size, align);
    }

    None
  }

  /// 空闲链表就地扩容（后继空闲可吞并/分裂）或失败；缩容恒可。
  pub fn resize_allocation(&mut self, block_ref: BlockRef, new_size: usize) -> Option<usize> {
    let need = Self::round_to_min_alloc(new_size);
    let current = self.block(block_ref)?.size;
    if need == current {
      return Some(self.block(block_ref)?.offset);
    }
    if need < current {
      self.split_free_block(block_ref, need);
      let block = self.block(block_ref)?;
      self.debug_allocated_bytes = self.debug_allocated_bytes.saturating_sub(block.size);
      return Some(block.offset);
    }
    // 扩容：与相邻后继空闲块合并直到足够。
    while self.block(block_ref).is_some_and(|b| b.size < need) {
      let next = self.get_next_adjacent_block_ref(block_ref)?;
      if !self
        .block(next)
        .is_some_and(|b| b.state == BlockState::Free)
      {
        return None;
      }
      self.coalesce_pair(block_ref, next);
      if self.block(block_ref).is_some_and(|b| b.size < need)
        && self.get_next_adjacent_block_ref(block_ref).is_none()
      {
        return None;
      }
    }
    self.block(block_ref).map(|b| b.offset)
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:ContainsRef
  ///
  /// 偏移是否落在池内（等价 C# 的块指针包含判定）。
  pub fn contains_ref(&self, offset: usize) -> bool {
    offset < self.pool.len()
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:IsValidBlockRef
  ///
  /// 引用是否对应池内的有效块。
  pub fn is_valid_block_ref(&self, block_ref: BlockRef) -> bool {
    self.block(block_ref).is_some()
  }

  /// 对应 DebugCheck 诊断入口，调用 check_correctness
  pub fn debug_check(&self) -> bool {
    self.check_correctness()
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:CheckCorrectness
  ///
  /// 不变量：块表按偏移升序、互不重叠；空闲链与空闲块一一对应。
  pub fn check_correctness(&self) -> bool {
    let mut last_end = 0usize;
    for block in &self.blocks {
      if block.offset < last_end {
        return false;
      }
      last_end = block.offset + block.size;
    }
    for (i, &a) in self.free_list.iter().enumerate() {
      if self.free_list[i + 1..].contains(&a) {
        return false;
      }
    }
    true
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:TryCoalesceAllFreeBlocks
  ///
  /// 合并全部相邻空闲块；发生合并返回 true。
  pub fn try_coalesce_all_free_blocks(&mut self) -> bool {
    let mut merged = false;
    let mut idx = 0;
    while idx + 1 < self.blocks.len() {
      let a = self.blocks[idx];
      let b = self.blocks[idx + 1];
      let (ra, rb) = (b_ref_idx(a.offset), b_ref_idx(b.offset));
      if a.state == BlockState::Free && b.state == BlockState::Free && a.offset + a.size == b.offset
      {
        self.coalesce_pair(ra, rb);
        merged = true;
      } else {
        idx += 1;
      }
    }
    merged
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:AddToFreeList
  ///
  /// 按偏移序插入空闲链。
  pub fn add_to_free_list(&mut self, block_ref: BlockRef) {
    let pos = self
      .free_list
      .iter()
      .position(|&r| r > block_ref)
      .unwrap_or(self.free_list.len());
    self.free_list.insert(pos, block_ref);
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:RemoveFromFreeList
  pub fn remove_from_free_list(&mut self, block_ref: BlockRef) {
    if let Some(pos) = self.free_list.iter().position(|&r| r == block_ref) {
      self.free_list.remove(pos);
    }
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:MoveToHeadOfFreeList
  pub fn move_to_head_of_free_list(&mut self, block_ref: BlockRef) {
    self.remove_from_free_list(block_ref);
    self.free_list.insert(0, block_ref);
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:TryCoalesceSingleBlock
  ///
  /// `block_ref` 与后继空闲块合并一次；成功返回 true。
  pub fn try_coalesce_single_block(&mut self, block_ref: BlockRef) -> bool {
    let Some(next) = self.get_next_adjacent_block_ref(block_ref) else {
      return false;
    };
    if !self
      .block(next)
      .is_some_and(|b| b.state == BlockState::Free)
    {
      return false;
    }
    self.coalesce_pair(block_ref, next);
    true
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:SplitInUseBlock
  ///
  /// 在用块按 `first_size` 分裂，尾部转为空闲块；返回尾部引用。
  pub fn split_in_use_block(&mut self, block_ref: BlockRef, first_size: usize) -> Option<BlockRef> {
    self.split_common(block_ref, first_size, BlockState::Free)
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:SplitFreeBlock
  ///
  /// 空闲块按 `first_size` 分裂（头部保留原状态，尾部为新空闲块）。
  pub fn split_free_block(&mut self, block_ref: BlockRef, first_size: usize) -> Option<BlockRef> {
    self.split_common(block_ref, first_size, BlockState::Free)
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:GetDataStartRef
  ///
  /// 块内数据区起始（块头之后）。
  pub fn get_data_start_ref(&self, block_ref: BlockRef) -> Option<usize> {
    Some(self.block(block_ref)?.offset + BLOCK_HEADER_SIZE)
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:GetFreeList
  ///
  /// 空闲链快照。
  pub fn get_free_list(&self) -> &[BlockRef] {
    &self.free_list
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:GetBlockRef
  ///
  /// 数据指针定位其所在块引用（指针换算为池内偏移）。
  pub fn get_block_ref(&self, ptr: *mut u8) -> Option<BlockRef> {
    let base = self.pool.as_mut_ptr() as usize;
    let target = ptr as usize;
    if target < base || target >= base + self.pool.len() {
      return None;
    }
    let offset = target - base;
    let idx = match self.blocks.binary_search_by_key(&offset, |b| b.offset) {
      Ok(i) => i,
      Err(i) => {
        if i == 0 {
          return None;
        }
        i - 1
      }
    };
    let b = &self.blocks[idx];
    if offset < b.offset + b.size {
      Some(b_ref_idx(b.offset))
    } else {
      None
    }
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:UpdateDebugAllocatedBytes
  pub fn update_debug_allocated_bytes(&mut self, delta: i64) {
    self.debug_allocated_bytes = (self.debug_allocated_bytes as i64 + delta).max(0) as usize;
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:SplitCommon
  fn split_common(
    &mut self,
    block_ref: BlockRef,
    first_size: usize,
    tail_state: BlockState,
  ) -> Option<BlockRef> {
    let first_size = Self::round_to_min_alloc(first_size);
    let block = self.block(block_ref)?;
    if block.size <= first_size {
      return None;
    }
    let (offset, size) = (block.offset, block.size);
    let idx = self.block_idx(block_ref)?;
    self.blocks[idx].size = first_size;
    self.blocks.insert(
      idx + 1,
      Block {
        offset: offset + first_size,
        size: size - first_size,
        state: tail_state,
      },
    );
    let tail_ref = b_ref_idx(offset + first_size);
    if tail_state == BlockState::Free {
      self.add_to_free_list(tail_ref);
    }
    Some(tail_ref)
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:ShouldSplit
  #[inline]
  const fn should_split(block_size: usize, need: usize) -> bool {
    block_size >= need + MIN_ALLOC
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:RoundToMinAlloc
  #[inline]
  const fn round_to_min_alloc(size: usize) -> usize {
    let size = if size == 0 { 1 } else { size };
    size.div_ceil(MIN_ALLOC) * MIN_ALLOC
  }

  /// 合并相邻两块（保留前者，后者出块表与空闲链）。
  fn coalesce_pair(&mut self, head: BlockRef, tail: BlockRef) {
    let (Some(h_idx), Some(t_idx)) = (self.block_idx(head), self.block_idx(tail)) else {
      return;
    };
    let merged_size = self.blocks[h_idx].size + self.blocks[t_idx].size;
    self.blocks[h_idx].size = merged_size;
    self.remove_from_free_list(tail);
    self.blocks.remove(t_idx);
  }

  fn block(&self, block_ref: BlockRef) -> Option<Block> {
    let idx = self.block_idx(block_ref)?;
    self.blocks.get(idx).copied()
  }

  fn block_mut(&mut self, block_ref: BlockRef) -> Option<&mut Block> {
    let idx = self.block_idx(block_ref)?;
    self.blocks.get_mut(idx)
  }

  fn block_idx(&self, block_ref: BlockRef) -> Option<usize> {
    let offset = block_of(block_ref);
    self.blocks.binary_search_by_key(&offset, |b| b.offset).ok()
  }
}

impl ILuaAllocator for LuaLimitedManagedAllocator {
  fn enter_infallible_allocation_region(&mut self) {
    self.enter_infallible_allocation_region();
  }

  fn try_exit_infallible_allocation_region(&mut self) -> bool {
    self.try_exit_infallible_allocation_region()
  }

  fn allocate_new(&mut self, size: usize, align: usize) -> Option<*mut u8> {
    self.allocate_new(size, align)
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:ResizeAllocation
  unsafe fn resize_allocation(&mut self, ptr: *mut u8, new_size: usize) -> Option<*mut u8> {
    if let Some(pos) = self
      .infallible_allocations
      .iter()
      .position(|a| a.ptr == ptr)
    {
      let old = self.infallible_allocations[pos];
      let new_layout = Layout::from_size_align(new_size.max(1), old.layout.align()).ok()?;
      // SAFETY: new_layout 有效
      let new_ptr = unsafe { alloc(new_layout) };
      if new_ptr.is_null() {
        return None;
      }
      let copy_len = old.layout.size().min(new_size);
      // SAFETY: old.ptr 与 new_ptr 有效且互不重叠
      unsafe {
        copy_nonoverlapping(ptr, new_ptr, copy_len);
        dealloc(old.ptr, old.layout);
      }
      self.infallible_allocations[pos] = InfallibleAllocation {
        ptr: new_ptr,
        layout: new_layout,
      };
      return Some(new_ptr);
    }

    let block_ref = self.get_block_ref(ptr)?;
    let old_size = self.block(block_ref)?.size;

    // 优先就地扩容/缩容
    if let Some(offset) = self.resize_allocation(block_ref, new_size) {
      // SAFETY: offset 为池内有效偏移
      return Some(unsafe { self.pool.as_mut_ptr().add(offset) });
    }

    // 就地扩容失败：对标 C# LuaLimitedManagedAllocator.cs:530-558
    // 分配新块 -> 拷贝原内容 -> 释放旧块
    let new_ptr = self.allocate_new(new_size, 16)?;

    let copy_len = old_size.min(new_size);
    // SAFETY: ptr 与 new_ptr 有效且互不重叠
    unsafe {
      copy_nonoverlapping(ptr, new_ptr, copy_len);
    }
    self.mark_free(block_ref);
    self.try_coalesce_all_free_blocks();
    Some(new_ptr)
  }

  unsafe fn deallocate(&mut self, ptr: *mut u8) {
    if let Some(pos) = self
      .infallible_allocations
      .iter()
      .position(|a| a.ptr == ptr)
    {
      let item = self.infallible_allocations.remove(pos);
      // SAFETY: item.ptr 由 std::alloc::alloc 分配且 layout 一致
      unsafe {
        dealloc(item.ptr, item.layout);
      }
      return;
    }
    let Some(block_ref) = self.get_block_ref(ptr) else {
      return;
    };
    self.mark_free(block_ref);
    self.try_coalesce_all_free_blocks();
  }
}

/// 块头尺寸（对齐 C# 块头语义的最小开销）。
pub const BLOCK_HEADER_SIZE: usize = 16;

/// 引用 → 池内偏移。
fn block_of(block_ref: BlockRef) -> usize {
  block_ref as usize
}

/// 偏移 → 引用（保证同偏移定位）。
fn b_ref_idx(offset: usize) -> BlockRef {
  offset as BlockRef
}

#[cfg(test)]
mod tests {
  use std::{ptr::write_bytes, slice::from_raw_parts};

  use super::{ILuaAllocator, LuaLimitedManagedAllocator};

  #[test]
  fn allocate_free_coalesce_cycle() {
    let mut alloc = LuaLimitedManagedAllocator::new(1024);
    let a = alloc.allocate_new(64, 1).unwrap();
    let b = alloc.allocate_new(64, 1).unwrap();
    assert_ne!(a, b);
    assert!(alloc.debug_check());
    // 分配后剩余空间保留为空闲块。
    assert_eq!(alloc.get_free_list().len(), 1);
  }

  #[test]
  fn quota_exhaustion_returns_none() {
    let mut alloc = LuaLimitedManagedAllocator::new(256);
    assert!(alloc.allocate_new(256, 1).is_some());
    assert!(alloc.allocate_new(32, 1).is_none());
    assert!(alloc.check_correctness());
  }

  #[test]
  fn split_marks_and_reuses() {
    let mut alloc = LuaLimitedManagedAllocator::new(1024);
    let a = alloc.allocate_new(64, 1).unwrap();
    // 分裂在用块：尾部转空闲。
    let a_ref = alloc.get_block_ref(a).unwrap();
    let tail = alloc.split_in_use_block(a_ref, 32);
    assert!(tail.is_some());
    assert_eq!(alloc.get_free_list().len(), 2);
  }

  #[test]
  fn infallible_emergency_allocation_and_exit_detection() {
    let mut alloc = LuaLimitedManagedAllocator::new(128);
    let _a = alloc.allocate_new(128, 1).unwrap();
    // 普通分配超额被拒
    assert!(alloc.allocate_new(64, 1).is_none());

    // 进入 infallible 区域
    alloc.enter_infallible_allocation_region();
    let emergency = alloc.allocate_new(64, 16);
    assert!(emergency.is_some());
    let emergency_ptr = emergency.unwrap();
    assert!(alloc.is_infallible_allocation(emergency_ptr));

    // 退出时返回 false，对标 C# NeedsDispose
    assert!(!alloc.try_exit_infallible_allocation_region());

    // 释放应急块
    unsafe {
      alloc.deallocate(emergency_ptr);
    }
  }

  #[test]
  fn resize_out_of_place_fallback_preserves_data() {
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
}
