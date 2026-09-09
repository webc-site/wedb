//! 内存与扇区对齐常数及计算工具（缓存行 64B / 扇区 512B / 4KB / CachePadded 结构）
//!
//! 对照 C# Tsavorite `Utility.cs`：
//! - `RoundUp` / `RoundDown` / `IsAligned` 对应 [`align_up`] / [`align_down`] / [`is_aligned`]，
//!   C# 仅支持 2 的幂（Debug.Assert），此处对非 2 的幂按模运算回退，防御更强；
//! - `PreviousPowerOf2` 对应 [`prev_power_of2`]；`NextPowerOf2` / `GetLogBase2` / `IsPowerOfTwo`
//!   直接复用 std 内建 `next_power_of_two` / `ilog2` / `is_power_of_two`，不再重复实现。

use std::{
  error::Error,
  fmt::{self, Display, Formatter},
  ops::{Deref, DerefMut, Range},
};

/// CPU 缓存行大小（64 字节）
pub const CACHELINE_BYTES: usize = 64;

/// 默认扇区大小（4096 字节，4KB 高级格式化）
pub const DEFAULT_SECTOR_SIZE: usize = 4096;

/// 最小扇区大小（512 字节，传统机械盘/虚拟盘标准）
pub const MIN_SECTOR_SIZE: usize = 512;

/// 取不大于 v 的最大 2 的幂（v 为 0 时返回 0，对齐 C# `Utility.PreviousPowerOf2`）
#[inline]
#[must_use]
pub const fn prev_power_of2(v: u64) -> u64 {
  if v == 0 {
    0
  } else {
    1 << (63 - v.leading_zeros())
  }
}

/// 判断数值是否按指定字节对齐
#[inline(always)]
pub const fn is_aligned(val: u64, align: u64) -> bool {
  if align == 0 {
    return false;
  }
  if align.is_power_of_two() {
    (val & (align - 1)) == 0
  } else {
    val.is_multiple_of(align)
  }
}

/// 向下按指定对齐大小对齐（align 须为 2 的幂；若不是则按模运算回退）
#[inline(always)]
pub const fn align_down(val: u64, align: u64) -> u64 {
  if align <= 1 {
    return val;
  }
  if align.is_power_of_two() {
    val & !(align - 1)
  } else {
    (val / align) * align
  }
}

/// 向上按指定对齐大小对齐（带溢出检测的 Checked 版本）
///
/// 若向上对齐后的数值超出 `u64::MAX`，返回 `None`；
/// 若 align <= 1，直接返回 `Some(val)`
#[inline(always)]
#[must_use]
pub const fn checked_align_up(val: u64, align: u64) -> Option<u64> {
  if align <= 1 {
    return Some(val);
  }
  if align.is_power_of_two() {
    let mask = align - 1;
    if (val & mask) == 0 {
      Some(val)
    } else {
      match val.checked_add(mask) {
        Some(v) => Some(v & !mask),
        None => None,
      }
    }
  } else {
    let rem = val % align;
    if rem == 0 {
      Some(val)
    } else {
      val.checked_add(align - rem)
    }
  }
}

/// 向上按指定对齐大小对齐（align 须为 2 的幂；若不是则按模运算回退）
///
/// 加法溢出时饱和到 `u64::MAX` 内最大的 align 整数倍（即 `!(align - 1)` 或 `(u64::MAX / align) * align`）。
/// 需要严格检测溢出时请使用 [`checked_align_up`]。
#[inline(always)]
#[must_use]
pub const fn align_up(val: u64, align: u64) -> u64 {
  match checked_align_up(val, align) {
    Some(v) => v,
    None => {
      if align.is_power_of_two() {
        !(align - 1)
      } else {
        (u64::MAX / align) * align
      }
    }
  }
}

/// 判定是否按 64 字节缓存行对齐
#[inline(always)]
pub const fn is_cacheline_aligned(val: u64) -> bool {
  is_aligned(val, CACHELINE_BYTES as u64)
}

/// 向上按 64 字节缓存行对齐
#[inline(always)]
pub const fn align_to_cacheline(val: u64) -> u64 {
  align_up(val, CACHELINE_BYTES as u64)
}

/// 128 字节缓存行对齐包装器
///
/// 严格防御 CPU 伪共享 (False Sharing)，对齐 Apple Silicon M 系列与 Neoverse ARM64 及双缓存行预取场景
#[repr(align(128))]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CachePadded<T>(pub T);

impl<T> CachePadded<T> {
  #[inline(always)]
  pub const fn new(value: T) -> Self {
    Self(value)
  }

  #[inline(always)]
  pub fn into_inner(self) -> T {
    self.0
  }
}

impl<T> Deref for CachePadded<T> {
  type Target = T;

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl<T> DerefMut for CachePadded<T> {
  #[inline(always)]
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

impl<T> From<T> for CachePadded<T> {
  #[inline(always)]
  fn from(val: T) -> Self {
    Self::new(val)
  }
}

/// 64 字节标准缓存行对齐包装器
#[repr(align(64))]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CachePadded64<T>(pub T);

impl<T> CachePadded64<T> {
  #[inline(always)]
  pub const fn new(value: T) -> Self {
    Self(value)
  }

  #[inline(always)]
  pub fn into_inner(self) -> T {
    self.0
  }
}

impl<T> Deref for CachePadded64<T> {
  type Target = T;

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl<T> DerefMut for CachePadded64<T> {
  #[inline(always)]
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

impl<T> From<T> for CachePadded64<T> {
  #[inline(always)]
  fn from(val: T) -> Self {
    Self::new(val)
  }
}

/// 校验扇区大小是否为 2 的幂且不小于 [`MIN_SECTOR_SIZE`]（512 字节）
#[inline(always)]
pub const fn is_valid_sector_size(size: usize) -> bool {
  size >= MIN_SECTOR_SIZE && size.is_power_of_two()
}

/// 逻辑偏移和长度转换后的物理扇区范围计算错误
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SectorRangeError {
  /// 扇区大小非法（非 2 的幂或小于 512 字节）
  InvalidSectorSize(usize),
  /// 范围计算溢出
  Overflow,
}

impl Display for SectorRangeError {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    match self {
      Self::InvalidSectorSize(s) => {
        write!(f, "无效扇区大小: {s}，必须为 2 的幂且 >= {MIN_SECTOR_SIZE}")
      }
      Self::Overflow => write!(f, "扇区范围计算溢出"),
    }
  }
}

impl Error for SectorRangeError {}

/// 逻辑偏移和长度转换后的物理扇区范围
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SectorRange {
  /// 对齐后的物理起始扇区偏移
  pub aligned_offset: u64,
  /// 对齐后的物理总长度（字节数，必为 sector_size 的整数倍）
  pub aligned_len: usize,
  /// 逻辑起始位置在首个对齐扇区内的偏移
  pub internal_offset: usize,
}

impl SectorRange {
  /// 计算给定逻辑范围在指定扇区大小下的物理扇区范围
  pub fn calculate(offset: u64, len: usize, sector_size: usize) -> Result<Self, SectorRangeError> {
    if !is_valid_sector_size(sector_size) {
      return Err(SectorRangeError::InvalidSectorSize(sector_size));
    }

    let sector_u64 = sector_size as u64;
    let aligned_offset = align_down(offset, sector_u64);
    let internal_offset = (offset - aligned_offset) as usize;

    if len == 0 {
      return Ok(Self {
        aligned_offset,
        aligned_len: 0,
        internal_offset,
      });
    }

    let end = offset
      .checked_add(len as u64)
      .ok_or(SectorRangeError::Overflow)?;
    let aligned_end = checked_align_up(end, sector_u64).ok_or(SectorRangeError::Overflow)?;

    let aligned_len_u64 = aligned_end
      .checked_sub(aligned_offset)
      .ok_or(SectorRangeError::Overflow)?;
    let aligned_len = usize::try_from(aligned_len_u64).map_err(|_| SectorRangeError::Overflow)?;

    Ok(Self {
      aligned_offset,
      aligned_len,
      internal_offset,
    })
  }

  /// 获取该范围跨越的扇区数量
  #[inline(always)]
  pub const fn sector_count(&self, sector_size: usize) -> usize {
    if self.aligned_len == 0 || sector_size == 0 {
      0
    } else {
      self.aligned_len / sector_size
    }
  }

  /// 获取逻辑范围在对齐缓冲区切片中的 Range
  #[inline(always)]
  pub const fn sub_range(&self, len: usize) -> Range<usize> {
    self.internal_offset..self.internal_offset.saturating_add(len)
  }
}
