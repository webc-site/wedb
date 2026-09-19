use crate::table::HashIndex;

/// 批处理多哈希桶 RAII 锁守卫（严格对标 C# Garnet OverflowBucketLockTable / TransactionalContext）
///
/// 封装一组已成功获取自旋锁的哈希桶下标及锁类型。
/// 离开作用域时自动逆序全部解锁，保证异常安全与完全释放。
pub struct MultiBucketGuard<'a> {
  pub(crate) index: &'a HashIndex,
  pub(crate) stack: [(usize, bool); HashIndex::INLINE_LOCK_ENTRIES],
  pub(crate) len: usize,
  pub(crate) extra: Vec<(usize, bool)>,
}

impl<'a> MultiBucketGuard<'a> {
  /// 栈上内联锁条目容量
  pub const INLINE_CAPACITY: usize = HashIndex::INLINE_LOCK_ENTRIES;

  /// 创建新的空锁守卫
  #[inline]
  pub fn new(index: &'a HashIndex) -> Self {
    Self {
      index,
      stack: [(0, false); Self::INLINE_CAPACITY],
      len: 0,
      extra: Vec::new(),
    }
  }

  /// 从已成功锁定的切片高效批量构造锁守卫（小集合单次切片拷贝，零循环开销）
  #[inline]
  pub(crate) fn from_slice(index: &'a HashIndex, entries: &[(usize, bool)]) -> Self {
    let count = entries.len();
    if count <= Self::INLINE_CAPACITY {
      let mut stack = [(0, false); Self::INLINE_CAPACITY];
      stack[..count].copy_from_slice(entries);
      Self {
        index,
        stack,
        len: count,
        extra: Vec::new(),
      }
    } else {
      let mut stack = [(0, false); Self::INLINE_CAPACITY];
      stack.copy_from_slice(&entries[..Self::INLINE_CAPACITY]);
      Self {
        index,
        stack,
        len: Self::INLINE_CAPACITY,
        extra: entries[Self::INLINE_CAPACITY..].to_vec(),
      }
    }
  }

  /// 已加锁的桶数量
  #[inline]
  pub fn len(&self) -> usize {
    self.len + self.extra.len()
  }

  /// 是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.len == 0
  }

  /// 迭代遍历所有已加锁的条目
  pub fn iter(&self) -> impl DoubleEndedIterator<Item = &(usize, bool)> {
    self.stack[..self.len].iter().chain(self.extra.iter())
  }
}

impl Drop for MultiBucketGuard<'_> {
  fn drop(&mut self) {
    // 逆序解锁，满足两阶段锁（2PL）释放规范
    for &(bucket_idx, is_exclusive) in self.iter().rev() {
      // SAFETY: push 为 crate 私有，登记仅发生在 acquire_unique_locked_entries
      // 加锁成功路径，bucket_idx 恒由 bucket_index_for_key/hash 产出（hash & mask
      // 截断，恒小于 buckets.len()），无越界风险
      let bucket = unsafe { self.index.buckets.get_unchecked(bucket_idx) };
      if is_exclusive {
        bucket.unlock_exclusive();
      } else {
        bucket.unlock_shared();
      }
    }
  }
}
