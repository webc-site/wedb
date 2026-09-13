use std::{
  fmt,
  mem::size_of,
  ops::{Deref, DerefMut},
  slice::{Iter, IterMut, from_raw_parts, from_raw_parts_mut},
};

use wram::{DirectVirtualMemory, DirectVmBlock};

use crate::{Result, bucket::HashBucket, error::Error};

/// 基于 DirectVirtualMemory 的 Demand-Zero 瞬时映射哈希桶数组 (严格对标 C# Tsavorite `DirectVirtualMemory.Allocate`)
///
/// 具备以下核心性能特性：
/// 1. Demand-Zero 首次访问由内核置零，创建哈希表时耗时亚微秒级，彻底清除用户态堆循环清零；
/// 2. 64 字节 Cacheline 严格对齐；在 Linux 上若 >= 2MB 自动 2MB 边界对齐并开启透明大页 (MADV_HUGEPAGE)，削减 80%~90% 的 dTLB Miss；
/// 3. RAII 自动通过 munmap / VirtualFree 安全退役释放，实现 Send + Sync。
pub struct HashBuckets {
  block: DirectVmBlock,
  len: usize,
}

impl HashBuckets {
  /// 分配指定数量的哈希桶
  pub fn new(len: usize) -> Result<Self> {
    if len == 0 {
      return Err(Error::InvalidBucketCount(0));
    }
    let size_bytes = len
      .checked_mul(size_of::<HashBucket>())
      .ok_or(Error::InvalidBucketCount(len))?;
    let block = DirectVirtualMemory::allocate(size_bytes, 64)?;
    Ok(Self { block, len })
  }

  /// 获取桶总数
  #[inline(always)]
  pub const fn len(&self) -> usize {
    self.len
  }

  /// 是否为空
  #[inline(always)]
  pub const fn is_empty(&self) -> bool {
    self.len == 0
  }

  /// 获取只读切片
  #[inline(always)]
  pub fn as_slice(&self) -> &[HashBucket] {
    if self.len == 0 {
      &[]
    } else {
      unsafe { from_raw_parts(self.block.aligned_ptr as *const HashBucket, self.len) }
    }
  }

  /// 获取可变切片
  #[inline(always)]
  pub fn as_mut_slice(&mut self) -> &mut [HashBucket] {
    if self.len == 0 {
      &mut []
    } else {
      unsafe { from_raw_parts_mut(self.block.aligned_ptr as *mut HashBucket, self.len) }
    }
  }

  /// 获取底层对齐指针
  #[inline(always)]
  pub fn as_ptr(&self) -> *const HashBucket {
    self.block.aligned_ptr as *const HashBucket
  }

  /// 获取底层对齐可变指针
  #[inline(always)]
  pub fn as_mut_ptr(&mut self) -> *mut HashBucket {
    self.block.aligned_ptr as *mut HashBucket
  }

  /// 无越界检查直接获取哈希桶只读引用（单指令寻址，零切片构造与分支开销）
  ///
  /// # Safety
  /// 调用方须保证 `index < self.len`。
  #[inline(always)]
  pub unsafe fn get_unchecked(&self, index: usize) -> &HashBucket {
    unsafe { &*(self.block.aligned_ptr as *const HashBucket).add(index) }
  }
}

impl Deref for HashBuckets {
  type Target = [HashBucket];

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    self.as_slice()
  }
}

impl DerefMut for HashBuckets {
  #[inline(always)]
  fn deref_mut(&mut self) -> &mut Self::Target {
    self.as_mut_slice()
  }
}

impl AsRef<[HashBucket]> for HashBuckets {
  #[inline(always)]
  fn as_ref(&self) -> &[HashBucket] {
    self.as_slice()
  }
}

impl AsMut<[HashBucket]> for HashBuckets {
  #[inline(always)]
  fn as_mut(&mut self) -> &mut [HashBucket] {
    self.as_mut_slice()
  }
}

impl<'a> IntoIterator for &'a HashBuckets {
  type Item = &'a HashBucket;
  type IntoIter = Iter<'a, HashBucket>;

  #[inline(always)]
  fn into_iter(self) -> Self::IntoIter {
    self.as_slice().iter()
  }
}

impl<'a> IntoIterator for &'a mut HashBuckets {
  type Item = &'a mut HashBucket;
  type IntoIter = IterMut<'a, HashBucket>;

  #[inline(always)]
  fn into_iter(self) -> Self::IntoIter {
    self.as_mut_slice().iter_mut()
  }
}

impl fmt::Debug for HashBuckets {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("HashBuckets")
      .field("len", &self.len)
      .field("aligned_ptr", &self.block.aligned_ptr)
      .finish()
  }
}
