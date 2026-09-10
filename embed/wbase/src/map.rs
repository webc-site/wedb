//! 并发字典与集合（基于 papaya 无锁并发结构与 gxhash 硬件加速哈希）
//!
//! 统一提供 `ConcurrentMap` / `ConcurrentSet` 别名及构造函数。

#[cfg(feature = "map")]
pub type ConcurrentMap<K, V> = papaya::HashMap<K, V, gxhash::GxBuildHasher>;

#[cfg(feature = "map")]
pub fn new_concurrent_map<K, V>() -> ConcurrentMap<K, V> {
  papaya::HashMap::builder()
    .hasher(gxhash::GxBuildHasher::default())
    .build()
}

#[cfg(feature = "set")]
pub type ConcurrentSet<T> = papaya::HashSet<T, gxhash::GxBuildHasher>;

#[cfg(feature = "set")]
pub fn new_concurrent_set<T>() -> ConcurrentSet<T> {
  papaya::HashSet::builder()
    .hasher(gxhash::GxBuildHasher::default())
    .build()
}
