//! 扇区与分段切片计算工具
//!
//! 负责跨段或单段 I/O 的边界切片迭代、偏移校验与对齐检查，
//! 消除 `write_aligned` 与 `read_aligned` 中的重复循环与算术逻辑。

use wbase::AlignedBuf;

use crate::error::{Error, Result};

/// 校验扇区对齐的 I/O 参数合法性
///
/// 对标 libs/storage/Tsavorite/cs/src/core/Device/NativeStorageDevice.cs:ThrowIfMisaligned
/// （C# 校验 offset/length/缓冲地址三项对齐；Rust 另补 offset+len 溢出防御）
#[inline]
pub(crate) fn validate_aligned_io(
  offset: u64,
  len: usize,
  buf: &AlignedBuf,
  sector_size: usize,
) -> Result<()> {
  let mask = (sector_size - 1) as u64;
  if (offset & mask) != 0 {
    return Err(Error::UnalignedOffset {
      offset,
      align: sector_size,
    });
  }
  if (len & (sector_size - 1)) != 0 {
    return Err(Error::UnalignedLen {
      len,
      align: sector_size,
    });
  }
  if !buf.is_aligned_to(sector_size) {
    return Err(Error::UnalignedBuffer {
      ptr: buf.as_ptr() as usize,
      align: sector_size,
    });
  }
  if offset.checked_add(len as u64).is_none() {
    return Err(Error::OutOfBounds { offset, len });
  }
  Ok(())
}

/// 跨段或单段 I/O 的分片元数据（设备层内部机械，非公开契约）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentChunk {
  /// 目标段编号
  pub seg_id: u32,
  /// 该分片在段文件内部的起始字节偏移
  pub off_in_seg: u64,
  /// 该分片在用户缓冲区中的起始字节偏移
  pub buf_pos: usize,
  /// 该分片的数据长度（字节）
  pub len: usize,
}

/// 段尺寸位移（段地址换算共享内核：`段编号 = 偏移 >> shift`）
///
/// 对标 libs/storage/Tsavorite/cs/src/core/Device/StorageDeviceBase.cs:Initialize
/// 的 `segmentSizeBits = GetLogBase2(segmentSize)`。
/// 契约：`segment_size` 须为 2 的幂（`SegmentedDevice` 构造时已强校验）
#[inline]
pub(crate) const fn segment_shift(segment_size: u64) -> u32 {
  segment_size.trailing_zeros()
}

/// 段内偏移掩码（段地址换算共享内核：`段内偏移 = 偏移 & mask`）
///
/// 与 [`segment_shift`] 同源：`Initialize` 阶段由段尺寸计算的 `segmentSizeMask = segmentSize - 1`。
/// 契约：`segment_size` 须为 2 的幂（`SegmentedDevice` 构造时已强校验）
#[inline]
pub(crate) const fn segment_mask(segment_size: u64) -> u64 {
  segment_size - 1
}

/// 跨段切片迭代器，负责将任意范围精确切分为单段内的连续物理操作区间（设备层内部机械）
///
/// 对标 libs/storage/Tsavorite/cs/src/core/Device/StorageDeviceBase.cs:WriteAsync /
/// StorageDeviceBase.cs:ReadAsync 的 `address >> segmentSizeBits` 单点换段算术：
/// C# 每次调用仅换算单段，跨段范围由上层逐段下发；Rust 迭代器化后由设备层
/// 单次调用完成跨段全量切片。
/// 契约：`segment_size` 须为 2 的幂（`SegmentedDevice` 构造时已强校验），
/// 内部位移与掩码运算依赖该前提。
pub(crate) struct SegmentChunks {
  curr_offset: u64,
  buf_pos: usize,
  total_len: usize,
  segment_size: Option<u64>,
  shift: u32,
  mask: u64,
}

impl SegmentChunks {
  /// 创建新的跨段切片迭代器
  ///
  /// 契约：`segment_size` 须为 2 的幂（`SegmentedDevice` 构造时已强校验），
  /// 内部位移与掩码运算依赖该前提。防御性约定：`None` 或非法段尺寸
  /// （0 / 非 2 的幂，调用方违约）在构造期一律退化为单段整块切片，
  /// 杜绝后续 `seg_size - off_in_seg` 的减法下溢。
  #[inline]
  pub const fn new(offset: u64, total_len: usize, segment_size: Option<u64>) -> Self {
    let segment_size = match segment_size {
      Some(seg_size) if seg_size.is_power_of_two() => Some(seg_size),
      _ => None,
    };
    let (shift, mask) = match segment_size {
      Some(seg_size) => (segment_shift(seg_size), segment_mask(seg_size)),
      None => (0, 0),
    };
    Self {
      curr_offset: offset,
      buf_pos: 0,
      total_len,
      segment_size,
      shift,
      mask,
    }
  }
}

impl Iterator for SegmentChunks {
  type Item = Result<SegmentChunk>;

  fn next(&mut self) -> Option<Self::Item> {
    if self.buf_pos >= self.total_len {
      return None;
    }

    let Some(seg_size) = self.segment_size else {
      let len = self.total_len - self.buf_pos;
      let chunk = SegmentChunk {
        seg_id: 0,
        off_in_seg: self.curr_offset,
        buf_pos: self.buf_pos,
        len,
      };
      self.buf_pos = self.total_len;
      return Some(Ok(chunk));
    };

    let seg_id_u64 = self.curr_offset >> self.shift;
    let seg_id = match u32::try_from(seg_id_u64) {
      Ok(id) => id,
      Err(_) => {
        self.buf_pos = self.total_len;
        return Some(Err(Error::SegmentExceeded(seg_id_u64)));
      }
    };
    let off_in_seg = self.curr_offset & self.mask;
    let seg_remain = usize::try_from(seg_size - off_in_seg).unwrap_or(usize::MAX);
    let chunk_len = seg_remain.min(self.total_len - self.buf_pos);

    let chunk = SegmentChunk {
      seg_id,
      off_in_seg,
      buf_pos: self.buf_pos,
      len: chunk_len,
    };

    self.buf_pos += chunk_len;
    self.curr_offset = self.curr_offset.saturating_add(chunk_len as u64);

    Some(Ok(chunk))
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    if self.buf_pos >= self.total_len {
      return (0, Some(0));
    }
    let remaining_bytes = self.total_len - self.buf_pos;
    let Some(seg_size) = self.segment_size else {
      return (1, Some(1));
    };
    let off_in_seg = self.curr_offset & self.mask;
    let first_chunk = usize::try_from(seg_size - off_in_seg).unwrap_or(usize::MAX);
    if remaining_bytes <= first_chunk {
      (1, Some(1))
    } else {
      let rem_after_first = remaining_bytes - first_chunk;
      let additional = (rem_after_first as u64).div_ceil(seg_size) as usize;
      let count = 1 + additional;
      (count, Some(count))
    }
  }
}

impl ExactSizeIterator for SegmentChunks {}
