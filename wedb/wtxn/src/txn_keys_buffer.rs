//! 事务集群槽位校验键缓冲（对标 Garnet C# `SessionParseState txnKeysParseState`）
//!
//! 在事务生命周期内复用扁平连续字节缓冲与累积偏移量，消除 `Vec<Box<[u8]>>`
//! 带来的微型堆分配与内存碎片，单次分配容量在后续事务中复用（0 堆分配）。
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalContext.cs（键缓冲登记）

use smallvec::SmallVec;

use crate::txn_key_entry::LockType;

/// 键索引内联槽位数（64 字节空间可容纳 16 个 u32 累积终止偏移，覆盖绝大多数 Redis MULTI 事务）
pub(crate) const TXN_KEYS_INLINE_CAPACITY: usize = 16;

/// 事务键连续字节与切片范围缓冲
pub struct TxnKeysBuffer {
  /// 扁平连续键字节（事务间复用容量，0 额外堆分配）
  bytes: Vec<u8>,
  /// 键累积终止偏移 (end_offset)，内联 16 槽位覆盖绝大多数 Redis MULTI 事务
  offsets: SmallVec<[u32; TXN_KEYS_INLINE_CAPACITY]>,
  /// 键对应的锁类型（内联 16 槽位）
  lock_types: SmallVec<[LockType; TXN_KEYS_INLINE_CAPACITY]>,
}

impl TxnKeysBuffer {
  /// 构造空的键缓冲
  #[inline]
  pub fn new() -> Self {
    Self {
      bytes: Vec::new(),
      offsets: SmallVec::new(),
      lock_types: SmallVec::new(),
    }
  }

  /// 键缓冲是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.offsets.is_empty()
  }

  /// 键数量（测试观测面；生产消费走 [`Self::is_empty`] / 迭代器）
  #[doc(hidden)]
  #[inline]
  pub fn len(&self) -> usize {
    self.offsets.len()
  }

  /// 底层字节缓冲容量与键槽位容量（测试观测面：内联 16 槽不变量与
  /// clear 容量复用语义的直接观测口）
  #[doc(hidden)]
  #[inline]
  pub fn capacity(&self) -> (usize, usize) {
    (self.bytes.capacity(), self.offsets.capacity())
  }

  /// 迭代键切片引用
  #[inline]
  pub fn iter(&self) -> TxnKeysIter<'_> {
    TxnKeysIter {
      bytes: &self.bytes,
      offsets: &self.offsets,
      idx: 0,
      prev: 0,
    }
  }

  /// 迭代键切片与锁类型对
  #[inline]
  pub fn iter_with_lock(&self) -> impl Iterator<Item = (&[u8], LockType)> + '_ {
    self.iter().zip(self.lock_types.iter().copied())
  }

  /// 缓冲内全部键是否均为只读锁（无 Exclusive 排他锁）
  #[inline]
  pub fn is_read_only(&self) -> bool {
    !self.lock_types.contains(&LockType::Exclusive)
  }

  /// 获取指定下标的锁类型（测试观测面）
  #[doc(hidden)]
  #[inline]
  pub fn get_lock_type(&self, index: usize) -> Option<LockType> {
    self.lock_types.get(index).copied()
  }

  /// libs/server/Transaction/TxnClusterSlotCheck.cs:SaveKeyArgSlice
  ///
  /// 追加键与锁型（纯 O(1) 尾部追加，对标 C# `AddKey` / `SaveKeyArgSlice` 形态；
  /// 热循环内复用 bytes 容量，同桶最强锁型归并由 EXEC 期 lock_plan 单点承担）
  #[inline]
  pub fn push(&mut self, key: &[u8], lock_type: LockType) {
    self.bytes.extend_from_slice(key);
    self.offsets.push(self.bytes.len() as u32);
    self.lock_types.push(lock_type);
  }

  /// 清空键缓冲（保留底层 `bytes`、`offsets` 与 `lock_types` 容量，供下一事务零分配复用）
  #[inline]
  pub fn clear(&mut self) {
    self.bytes.clear();
    self.offsets.clear();
    self.lock_types.clear();
  }
}

impl Default for TxnKeysBuffer {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

/// 键切片引用迭代器
pub struct TxnKeysIter<'a> {
  bytes: &'a [u8],
  offsets: &'a [u32],
  idx: usize,
  prev: usize,
}

impl<'a> Iterator for TxnKeysIter<'a> {
  type Item = &'a [u8];

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    let &end = self.offsets.get(self.idx)?;
    self.idx += 1;
    let end = end as usize;
    let start = self.prev;
    self.prev = end;
    // SAFETY: start <= end <= bytes.len() 恒成立
    Some(unsafe { self.bytes.get_unchecked(start..end) })
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    let rem = self.offsets.len() - self.idx;
    (rem, Some(rem))
  }
}

impl ExactSizeIterator for TxnKeysIter<'_> {}

impl<'a> IntoIterator for &'a TxnKeysBuffer {
  type Item = &'a [u8];
  type IntoIter = TxnKeysIter<'a>;

  #[inline]
  fn into_iter(self) -> Self::IntoIter {
    self.iter()
  }
}
