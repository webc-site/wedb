//! 事务集群槽位校验键缓冲（对标 Garnet C# `SessionParseState txnKeysParseState`）
//!
//! 在事务生命周期内复用扁平连续字节缓冲与累积偏移量，消除 `Vec<Box<[u8]>>`
//! 带来的微型堆分配与内存碎片，单次分配容量在后续事务中复用（0 堆分配）。

use smallvec::SmallVec;

/// 键索引内联槽位数（64 字节空间可容纳 16 个 u32 累积终止偏移，覆盖绝大多数 Redis MULTI 事务）
pub const TXN_KEYS_INLINE_CAPACITY: usize = 16;

/// 事务键连续字节与切片范围缓冲
#[derive(Debug, Default, Clone)]
pub struct TxnKeysBuffer {
  /// 扁平连续键字节（事务间复用容量，0 额外堆分配）
  bytes: Vec<u8>,
  /// 键累积终止偏移 (end_offset)，内联 16 槽位覆盖绝大多数 Redis MULTI 事务
  offsets: SmallVec<[u32; TXN_KEYS_INLINE_CAPACITY]>,
}

impl TxnKeysBuffer {
  /// 构造空的键缓冲
  #[inline]
  pub fn new() -> Self {
    Self {
      bytes: Vec::new(),
      offsets: SmallVec::new(),
    }
  }

  /// 预分配容量构造
  #[inline]
  pub fn with_capacity(bytes_capacity: usize, keys_capacity: usize) -> Self {
    Self {
      bytes: Vec::with_capacity(bytes_capacity),
      offsets: SmallVec::with_capacity(keys_capacity),
    }
  }

  /// 键缓冲是否为空
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.offsets.is_empty()
  }

  /// 键数量
  #[inline]
  pub fn len(&self) -> usize {
    self.offsets.len()
  }

  /// 底层字节缓冲容量与键槽位容量
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

  /// 获取指定下标的键切片
  #[inline]
  pub fn get(&self, index: usize) -> Option<&[u8]> {
    let &end = self.offsets.get(index)?;
    let start = if index == 0 {
      0
    } else {
      // SAFETY: index < offsets.len()，index - 1 恒有效
      unsafe { *self.offsets.get_unchecked(index - 1) as usize }
    };
    // SAFETY: start 与 end 始终由 push 基于 bytes.len() 单调递增记录，范围恒有效
    Some(unsafe { self.bytes.get_unchecked(start..end as usize) })
  }

  /// 追加键（自动按内容去重；热循环内复用 bytes 容量）
  pub fn push(&mut self, key: &[u8]) {
    if self.iter().any(|k| k == key) {
      return;
    }
    self.bytes.extend_from_slice(key);
    self.offsets.push(self.bytes.len() as u32);
  }

  /// 收集键切片序列至 SmallVec（内联 16 键，0 堆分配）
  #[inline]
  pub fn to_smallvec(&self) -> SmallVec<[&[u8]; TXN_KEYS_INLINE_CAPACITY]> {
    self.iter().collect()
  }

  /// 清空键缓冲（保留底层 `bytes` 与 `offsets` 容量，供下一事务零分配复用）
  #[inline]
  pub fn clear(&mut self) {
    self.bytes.clear();
    self.offsets.clear();
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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_txn_keys_buffer_push_and_dedup() {
    let mut buf = TxnKeysBuffer::new();
    assert!(buf.is_empty());
    assert_eq!(buf.len(), 0);

    buf.push(b"key1");
    buf.push(b"key2");
    buf.push(b"key1"); // 重复键去重
    buf.push(b"key3");

    assert_eq!(buf.len(), 3);
    assert!(!buf.is_empty());
    assert_eq!(buf.get(0), Some(&b"key1"[..]));
    assert_eq!(buf.get(1), Some(&b"key2"[..]));
    assert_eq!(buf.get(2), Some(&b"key3"[..]));
    assert_eq!(buf.get(3), None);

    let collected: Vec<&[u8]> = buf.iter().collect();
    assert_eq!(collected, vec![&b"key1"[..], &b"key2"[..], &b"key3"[..]]);
  }

  #[test]
  fn test_txn_keys_buffer_clear_reuse_capacity() {
    let mut buf = TxnKeysBuffer::new();
    buf.push(b"first_key_long_enough_to_allocate");
    let (byte_cap, offset_cap) = buf.capacity();

    buf.clear();
    assert!(buf.is_empty());
    assert_eq!(buf.len(), 0);
    assert_eq!(buf.capacity(), (byte_cap, offset_cap));

    buf.push(b"second_key");
    assert_eq!(buf.len(), 1);
    assert_eq!(buf.get(0), Some(&b"second_key"[..]));
  }

  #[test]
  fn test_txn_keys_buffer_inline_capacity_16() {
    let mut buf = TxnKeysBuffer::new();
    for i in 0..16 {
      let key = format!("k{i}");
      buf.push(key.as_bytes());
    }
    assert_eq!(buf.len(), 16);
    assert!(!buf.offsets.spilled(), "16 键以内应保持内联，0 堆分配");

    buf.push(b"overflow_17");
    assert_eq!(buf.len(), 17);
    assert!(buf.offsets.spilled(), "超过 16 键优雅溢出至堆");
  }
}
