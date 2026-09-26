//! 并发字典与集合（基于 papaya 无锁并发结构与 gxhash 硬件加速哈希）
//!
//! 统一提供 `ConcurrentMap` / `ConcurrentSet` 别名及构造函数。
//!
//! 本模块承载 transpile SKILL「gxhash 默认随机种子防碰撞 DoS」承诺：
//! 全仓集合与并发映射统一收口至本模块 [`GxBuildHasher`]。该类型通过 newtype
//! 包装底层 `gxhash::GxBuildHasher`，其 [`Default`] 实现一律以进程级随机种子
//! （[`SEED`]）初始化，现无任何生产容器走恒 42 种子；进程内各集合共享同一种子，
//! SCAN 族续扫在进程生命周期内迭代序天然稳定。
//!
//! 自研依据: 技术选型契约 papaya + gxhash（transpile 契约，随机种子防碰撞 DoS）

use std::{collections, hash::BuildHasher, sync::OnceLock};

use gxhash::GxBuildHasher as GxhashGxBuildHasher;
pub use gxhash::{HashMapExt, HashSetExt};

/// 进程级 papaya 哈希种子：同进程所有并发表共享（同键同桶，不引入逐表差异），
/// 首次构造惰性随机化；种子不落盘、不外泄，防离线构造同桶键的碰撞 DoS
static SEED: OnceLock<i64> = OnceLock::new();

/// 取进程级随机种子（本仓对 C# ConcurrentDictionary 的单向安全增强，无 garnet 对位）
#[inline]
fn seed() -> i64 {
  *SEED.get_or_init(|| fastrand::i64(..))
}

/// 基于进程级随机种子的哈希构建器
#[derive(Clone, Debug)]
pub struct GxBuildHasher(gxhash::GxBuildHasher);

impl Default for GxBuildHasher {
  #[inline]
  fn default() -> Self {
    Self(GxhashGxBuildHasher::with_seed(seed()))
  }
}

impl BuildHasher for GxBuildHasher {
  type Hasher = gxhash::GxHasher;
  #[inline]
  fn build_hasher(&self) -> Self::Hasher {
    self.0.build_hasher()
  }
}

pub type HashMap<K, V> = collections::HashMap<K, V, GxBuildHasher>;
pub type HashSet<T> = collections::HashSet<T, GxBuildHasher>;

/// 基于 papaya 与 gxhash 的无锁并发字典
#[cfg(feature = "map")]
pub type ConcurrentMap<K, V> = papaya::HashMap<K, V, GxBuildHasher>;

/// 创建基于 gxhash 的并发字典
#[cfg(feature = "map")]
#[inline]
pub fn new_concurrent_map<K, V>() -> ConcurrentMap<K, V> {
  papaya::HashMap::builder()
    .hasher(GxBuildHasher::default())
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
    .hasher(GxBuildHasher::default())
    .build()
}

#[cfg(test)]
mod tests {
  /// 种子非固定 42（fastrand 撞中概率 2^-64，可忽略）
  #[cfg(any(feature = "map", feature = "set"))]
  #[test]
  fn test_seed_randomized() {
    assert_ne!(super::seed(), 42);
  }

  /// map / set 共享同一进程种子，构造不重取（同进程同键同桶）
  #[cfg(all(feature = "map", feature = "set"))]
  #[test]
  fn test_seed_shared_across_tables() {
    let first = super::seed();
    let _map = super::new_concurrent_map::<u64, u64>();
    let _set = super::new_concurrent_set::<u64>();
    assert_eq!(super::SEED.get(), Some(&first));
  }
}
