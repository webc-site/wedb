//! 成员级过期队列（Hash / SortedSet 共用，对应 C# PriorityQueue<byte[], long>
//! 在 libs/server/Objects/Hash/HashObject.cs 与
//! libs/server/Objects/SortedSet/SortedSetObject.cs 的内联定义）

use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc};

/// 过期队列条目：按 (expiration, key) 升序的最小堆元素
///
/// 对标 C# PriorityQueue<byte[], long> 的堆内条目
/// （libs/server/Objects/Hash/HashObject.cs:expirationQueue 与
/// libs/server/Objects/SortedSet/SortedSetObject.cs:expirationQueue）；
/// 同刻过期以 key 字节序决胜（C# 同优先级出队序为实现定义，删除结果
/// 以 expiration_times 源真值裁决，顺序无语义）
///
/// 键为共享句柄（对位 C# 与主容器共享同一 byte[] 引用，堆内零复制）
///
/// 排序取字段序 derive（expiration 先比、同刻以 key 字节序决胜），与
/// (expiration, key) 元组序同构
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExpirationQueueEntry {
  pub(crate) expiration: i64,
  pub(crate) key: Arc<[u8]>,
}

/// 反转堆序 → BinaryHeap 即最小堆
///
/// C# 无同名类型：PriorityQueue<byte[], long>（最小堆）内联于
/// libs/server/Objects/Hash/HashObject.cs 与
/// libs/server/Objects/SortedSet/SortedSetObject.cs 的 expirationQueue 字段，
/// 消费方为 DeleteExpiredItemsWorker / DeleteExpiredItems（堆顶先过期先出）
pub type ExpirationQueue = BinaryHeap<Reverse<ExpirationQueueEntry>>;
