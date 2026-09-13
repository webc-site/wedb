//! 成员级过期队列（Hash / SortedSet 共用，对应 C# PriorityQueue<byte[], long>）

use std::{
  cmp::{Ordering, Reverse},
  collections::BinaryHeap,
};

/// 过期队列条目：按 (expiration, key) 升序的最小堆元素
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpirationQueueEntry {
  pub(crate) expiration: i64,
  pub(crate) key: Vec<u8>,
}

impl PartialOrd for ExpirationQueueEntry {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for ExpirationQueueEntry {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    self
      .expiration
      .cmp(&other.expiration)
      .then_with(|| self.key.cmp(&other.key))
  }
}

/// 反转堆序 → BinaryHeap 即最小堆
pub type ExpirationQueue = BinaryHeap<Reverse<ExpirationQueueEntry>>;
