//! 并发字典（基于 papaya 无锁并发结构与 gxhash 硬件加速哈希）
//!
//! 统一提供 `ConcurrentMap` 别名及构造函数。

use gxhash::GxBuildHasher;
use papaya::HashMap;

pub type ConcurrentMap<K, V> = papaya::HashMap<K, V, gxhash::GxBuildHasher>;

pub fn new_concurrent_map<K, V>() -> ConcurrentMap<K, V> {
  HashMap::builder().hasher(GxBuildHasher::default()).build()
}
