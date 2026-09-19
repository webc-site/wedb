//! 并发字典与集合（基于 papaya 无锁并发结构与 gxhash 硬件加速哈希）
//!
//! 统一提供 `ConcurrentMap` / `ConcurrentSet` 别名及构造函数。
//!
//! 本构造口承载 transpile SKILL「gxhash 默认随机种子防碰撞 DoS」承诺：
//! workspace 开启 gxhash deterministic 后 `GxBuildHasher::default()` 恒种子 42，
//! papaya 域在此以进程级随机种子显式承接（[`SEED`]）；SCAN 族续扫容器的
//! 确定性例外不经此处（见 workspace Cargo.toml gxhash 注释，勿据 SKILL 默认
//! 再行去除该 feature），两消费面在种子层就此分离。

use std::sync::OnceLock;

use gxhash::GxBuildHasher;

/// 进程级 papaya 哈希种子：同进程所有并发表共享（同键同桶，不引入逐表差异），
/// 首次构造惰性随机化；种子不落盘、不外泄，防离线构造同桶键的碰撞 DoS
static SEED: OnceLock<i64> = OnceLock::new();

/// 取进程级随机种子（本仓对 C# ConcurrentDictionary 的单向安全增强，无 garnet 对位）
#[inline]
fn seed() -> i64 {
  *SEED.get_or_init(|| fastrand::i64(..))
}

/// 基于 papaya 与 gxhash 的无锁并发字典
#[cfg(feature = "map")]
pub type ConcurrentMap<K, V> = papaya::HashMap<K, V, GxBuildHasher>;

/// 创建基于 gxhash 的并发字典
#[cfg(feature = "map")]
#[inline]
pub fn new_concurrent_map<K, V>() -> ConcurrentMap<K, V> {
  papaya::HashMap::builder()
    .hasher(GxBuildHasher::with_seed(seed()))
    .build()
}

/// 基于 papaya 与 gxhash 的无锁并发集合
#[cfg(feature = "set")]
pub type ConcurrentSet<K> = papaya::HashSet<K, GxBuildHasher>;

/// 创建基于 gxhash 的并发集合
#[cfg(feature = "set")]
#[inline]
pub fn new_concurrent_set<K>() -> ConcurrentSet<K> {
  papaya::HashSet::builder()
    .hasher(GxBuildHasher::with_seed(seed()))
    .build()
}

#[cfg(test)]
mod tests {
  /// 种子非 deterministic 默认 42（fastrand 撞中概率 2^-64，可忽略）
  #[cfg(any(feature = "map", feature = "set"))]
  #[test]
  fn test_seed_randomized() {
    assert_ne!(super::seed(), 42);
  }

  /// map / set 共享同一进程种子，构造不重取（同进程同键同桶）
  #[cfg(any(feature = "map", feature = "set"))]
  #[test]
  fn test_seed_shared_across_tables() {
    let first = super::seed();
    let _map = super::new_concurrent_map::<u64, u64>();
    let _set = super::new_concurrent_set::<u64>();
    assert_eq!(super::SEED.get(), Some(&first));
  }

  #[cfg(feature = "map")]
  #[test]
  fn test_map() {
    let map = super::new_concurrent_map();
    let pin = map.pin();
    assert!(pin.is_empty());
    pin.insert("a", 1);
    assert_eq!(pin.get("a"), Some(&1));
    assert_eq!(pin.len(), 1);
    assert_eq!(pin.remove(&"a"), Some(&1));
    assert!(pin.is_empty());
  }

  #[cfg(feature = "set")]
  #[test]
  fn test_set() {
    let set = super::new_concurrent_set();
    let pin = set.pin();
    assert!(pin.is_empty());
    pin.insert("a");
    assert!(pin.contains("a"));
    assert_eq!(pin.len(), 1);
    assert!(!pin.is_empty());
    assert!(pin.remove(&"a"));
    assert!(pin.is_empty());
  }
}
