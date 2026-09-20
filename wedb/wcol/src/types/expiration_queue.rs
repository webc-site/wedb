//! 成员级过期队列（Hash / SortedSet 共用，对应 C# PriorityQueue<byte[], long>
//! 在 libs/server/Objects/Hash/HashObject.cs 与
//! libs/server/Objects/SortedSet/SortedSetObject.cs 的内联定义）

use std::{
  cmp::{Ordering, Reverse},
  collections::BinaryHeap,
};

/// 过期队列条目：按 (expiration, key) 升序的最小堆元素
///
/// 对标 C# PriorityQueue<byte[], long> 的堆内条目
/// （libs/server/Objects/Hash/HashObject.cs:expirationQueue 与
/// libs/server/Objects/SortedSet/SortedSetObject.cs:expirationQueue）；
/// 同刻过期以 key 字节序决胜（C# 同优先级出队序为实现定义，删除结果
/// 以 expiration_times 源真值裁决，顺序无语义）
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
///
/// C# 无同名类型：PriorityQueue<byte[], long>（最小堆）内联于
/// libs/server/Objects/Hash/HashObject.cs 与
/// libs/server/Objects/SortedSet/SortedSetObject.cs 的 expirationQueue 字段，
/// 消费方为 DeleteExpiredItemsWorker / DeleteExpiredItems（堆顶先过期先出）
pub type ExpirationQueue = BinaryHeap<Reverse<ExpirationQueueEntry>>;
