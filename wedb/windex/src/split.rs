//! 单桶与分块分裂迁移原语 (1:1 对标 C# Garnet SplitIndex.cs)

use std::sync::atomic::Ordering;

use crate::{
  Error, Result,
  chain::{ChainStep, ChainWalker},
  entry::HashBucketEntry,
  table::HashIndex,
};

/// 分块位宽（1:1 对标 Garnet Constants.kSizeofChunkBits = 14）
pub const CHUNK_BITS: usize = 14;

/// 每个分块包含的主哈希桶数量（1:1 对标 Garnet Constants.kSizeofChunk = 16384）
pub const CHUNK_SIZE: usize = 1 << CHUNK_BITS;

/// 分块分裂状态：未开始 (0)
pub const SPLIT_UNSTARTED: i64 = 0;

/// 分块分裂状态：分裂中/已加排他锁 (1)
pub const SPLIT_IN_PROGRESS: i64 = 1;

/// 分块分裂状态：分裂完成 (2)
pub const SPLIT_COMPLETED: i64 = 2;

/// 计算给定旧表容量的分块总数（至少 1 块）
#[inline(always)]
pub const fn chunk_count(old_index_size: usize) -> usize {
  let count = old_index_size / CHUNK_SIZE;
  if count == 0 { 1 } else { count }
}

/// 计算哈希值在旧表中所属的分块偏移
#[inline(always)]
pub const fn chunk_offset_for_hash(hash: u64, old_mask: usize) -> usize {
  ((hash as usize) & old_mask) >> CHUNK_BITS
}

/// 单桶分裂迁移内核（处理单个桶的分裂与溢出链条目迁移）
///
/// 将旧表 `old_index` 中下标为 `bucket_idx` 的桶（及溢出链条目）无锁分裂并迁移至
/// 容量翻倍的新表 `new_index` 中的两个对应子桶：
/// - 左子桶：`bucket_idx`（对应新哈希掩码新增高位为 0）
/// - 右子桶：`bucket_idx + old_index.size`（对应新哈希掩码新增高位为 1）
///
/// `record_locator`：`FnMut(u64) -> Option<(u64, u64)>`，接收逻辑地址，返回 `Some((key_hash, prev_addr))`
/// 若记录在内存驻留区可读；若在冷磁盘区返回 `None`（此时该条目复制至左右两子桶）。
///
/// `trace_back`：`FnMut(u64, usize) -> Option<u64>`，沿日志链向上回溯首个高位为目标 bit (0 或 1) 的记录地址。
pub fn split_single_bucket<F, T>(
  old_index: &HashIndex,
  new_index: &HashIndex,
  bucket_idx: usize,
  mut record_locator: F,
  mut trace_back: T,
) -> Result<()>
where
  F: FnMut(u64) -> Option<(u64, u64)>,
  T: FnMut(u64, usize) -> Option<u64>,
{
  let old_size = old_index.size;
  let high_bit_pos = old_size.trailing_zeros();

  let left_bucket_idx = bucket_idx & old_index.mask;
  let right_bucket_idx = left_bucket_idx + old_size;

  let mut walker = ChainWalker::new(old_index.get_bucket(left_bucket_idx));

  loop {
    for slot in walker.curr.entries.iter().take(crate::DATA_ENTRIES) {
      let raw = slot.load(Ordering::Acquire);
      if raw == 0 {
        continue;
      }

      let entry = HashBucketEntry::from_raw(raw);
      if !entry.is_valid() {
        continue;
      }

      let addr = entry.address();
      if addr == 0 {
        continue;
      }

      let tag = entry.tag();

      if let Some((hash, prev_addr)) = record_locator(addr) {
        let bit = ((hash >> high_bit_pos) & 1) as usize;
        let (primary_idx, other_idx, target_bit) = if bit == 0 {
          (left_bucket_idx, right_bucket_idx, 1)
        } else {
          (right_bucket_idx, left_bucket_idx, 0)
        };

        new_index.insert_to_bucket(primary_idx, tag, addr)?;

        if prev_addr != 0
          && let Some(other_addr) = trace_back(prev_addr, target_bit)
        {
          new_index.insert_to_bucket(other_idx, tag, other_addr)?;
        }
      } else {
        // 冷磁盘区或无法在内存解析：双写至左右两个子桶
        new_index.insert_to_bucket(left_bucket_idx, tag, addr)?;
        new_index.insert_to_bucket(right_bucket_idx, tag, addr)?;
      }
    }

    match walker.advance(&old_index.overflow_pool) {
      ChainStep::Next => {}
      ChainStep::End => break,
      ChainStep::Cycle => return Err(Error::OverflowCycleDetected),
    }
  }

  Ok(())
}

/// 分块分裂迁移
///
/// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/SplitIndex.cs:SplitChunk
///
/// 迁移第 `chunk_idx` 个分块中的所有桶。
pub fn split_chunk<F, T>(
  old_index: &HashIndex,
  new_index: &HashIndex,
  chunk_idx: usize,
  num_chunks: usize,
  mut record_locator: F,
  mut trace_back: T,
) -> Result<()>
where
  F: FnMut(u64) -> Option<(u64, u64)>,
  T: FnMut(u64, usize) -> Option<u64>,
{
  let chunk_size = old_index.size / num_chunks;
  let start_bucket = chunk_size * (chunk_idx & (num_chunks - 1));
  let end_bucket = start_bucket + chunk_size;

  for b in start_bucket..end_bucket {
    split_single_bucket(
      old_index,
      new_index,
      b,
      &mut record_locator,
      &mut trace_back,
    )?;
  }

  Ok(())
}
