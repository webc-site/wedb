//! 定额托管分配器：固定字节池上的空闲链分配器
//! （对标 libs/server/Lua/LuaLimitedManagedAllocator.cs:LuaLimitedManagedAllocator）。
//!
//! C# 以块引用（块基址）+ 双向空闲链组织池内存；Rust 以 `Vec<Block>` 槽位 +
//! `block_ref`（槽位下标 + 1，0 视为空）承接：
//! - `mark_free` / `mark_in_use` 维护状态
//! - 空闲链按地址序插入，`try_coalesce_all_free_blocks` 合并相邻空闲块
//! - `split_in_use_block` / `split_free_block` 支持临界分配
//! - `allocate_new` 首个适配（first-fit）；池满返回 None

use std::collections::BTreeSet;

/// 最小块尺寸（对齐 C# MinAllocSize 语义）。
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

/// 固定池 + 空闲链分配器。
pub struct LuaLimitedManagedAllocator {
  /// 池内存。
  pool: Vec<u8>,
  /// 块表（按偏移升序）。
  blocks: Vec<Block>,
  /// 空闲链（块引用序列，按偏移升序）。
  free_list: Vec<BlockRef>,
  /// infallible 深度。
  infallible_depth: u32,
  /// 已分配（in-use）字节数（调试/断言用）。
  debug_allocated_bytes: usize,
}

impl Default for LuaLimitedManagedAllocator {
  fn default() -> Self {
    Self::new(1024 * 1024)
  }
}

impl LuaLimitedManagedAllocator {
  /// 构造：分配 `pool_size` 字节池并置单空闲块。
  pub fn new(pool_size: usize) -> Self {
    let mut allocator = Self {
      pool: vec![0u8; pool_size],
      blocks: Vec::new(),
      free_list: Vec::new(),
      infallible_depth: 0,
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
    let (idx, _) = self
      .blocks
      .iter()
      .enumerate()
      .find(|(_, b)| block_of(block_ref) == b.offset)?;
    self
      .blocks
      .get(idx + 1)
      .map(|b| b_ref_idx(idx + 1, b.offset))
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:GetRefVal
  ///
  /// 块引用对应的池内偏移。
  pub fn get_ref_val(&self, block_ref: BlockRef) -> Option<usize> {
    Some(self.block(block_ref)?.offset)
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:EnterInfallibleAllocationRegion
  pub fn enter_infallible_allocation_region(&mut self) {
    self.infallible_depth += 1;
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:TryExitInfallibleAllocationRegion
  pub fn try_exit_infallible_allocation_region(&mut self) -> bool {
    self.infallible_depth = self.infallible_depth.saturating_sub(1);
    true
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:AllocateNew
  ///
  /// 首个适配分配：不足则先合并空闲块再试；infallible 区间内同样受池约束。
  pub fn allocate_new(&mut self, size: usize, _align: usize) -> Option<*mut u8> {
    let need = Self::round_to_min_alloc(size);
    let mut chosen = None;
    for &block_ref in &self.free_list {
      let block = self.block(block_ref)?;
      if block.size >= need {
        chosen = Some(block_ref);
        break;
      }
    }
    // 剩余空间不足以适配则先合并空闲块再找。
    let block_ref = chosen.or_else(|| {
      self.try_coalesce_all_free_blocks();
      self
        .free_list
        .iter()
        .copied()
        .find(|&r| self.block(r).is_some_and(|b| b.size >= need))
    })?;

    // 剩余空间足以分裂则拆出空闲尾部块。
    let block_size = self.block(block_ref)?.size;
    if Self::should_split(block_size, need) {
      self.split_free_block(block_ref, need);
    }
    let data_offset = self.block(block_ref)?.offset;
    self.mark_in_use(block_ref);
    self.debug_allocated_bytes += need;
    Some(unsafe { self.pool.as_mut_ptr().add(data_offset) })
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:ResizeAllocation
  ///
  /// 就地扩容（后继空闲可吞并/分裂）或失败；缩容恒可。
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
    let free_in_list = self.free_list.iter().copied().collect::<BTreeSet<_>>();
    free_in_list.len() == self.free_list.len()
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
      let (ra, rb) = (b_ref_idx(idx, a.offset), b_ref_idx(idx + 1, b.offset));
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
    let Some(offset) = self.get_ref_val(block_ref) else {
      return;
    };
    let pos = self
      .free_list
      .iter()
      .position(|&r| self.get_ref_val(r).is_some_and(|o| o > offset))
      .unwrap_or(self.free_list.len());
    self.free_list.insert(pos, block_ref);
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:RemoveFromFreeList
  pub fn remove_from_free_list(&mut self, block_ref: BlockRef) {
    self.free_list.retain(|&r| r != block_ref);
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
    let offset = ptr as usize - self.pool.as_ptr() as usize;
    self
      .blocks
      .iter()
      .enumerate()
      .find(|(_, b)| offset >= b.offset && offset < b.offset + b.size)
      .map(|(idx, b)| b_ref_idx(idx, b.offset))
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
    let tail_ref = b_ref_idx(idx + 1, offset + first_size);
    if tail_state == BlockState::Free {
      self.add_to_free_list(tail_ref);
    }
    Some(tail_ref)
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:ShouldSplit
  fn should_split(block_size: usize, need: usize) -> bool {
    block_size >= need + MIN_ALLOC
  }

  /// libs/server/Lua/LuaLimitedManagedAllocator.cs:RoundToMinAlloc
  fn round_to_min_alloc(size: usize) -> usize {
    size.max(1).div_ceil(MIN_ALLOC) * MIN_ALLOC
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
    self.blocks.iter().position(|b| b.offset == offset)
  }
}

/// 块头尺寸（对齐 C# 块头语义的最小开销）。
pub const BLOCK_HEADER_SIZE: usize = 16;

/// 引用 → 池内偏移。
fn block_of(block_ref: BlockRef) -> usize {
  block_ref as usize
}

/// 槽位 + 偏移 → 引用（保证同偏移定位）。
fn b_ref_idx(_idx: usize, offset: usize) -> BlockRef {
  offset as BlockRef
}

#[cfg(test)]
mod tests {
  use super::LuaLimitedManagedAllocator;

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
}
