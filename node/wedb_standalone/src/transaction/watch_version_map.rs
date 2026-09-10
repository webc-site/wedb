//! WATCH 版本表（对标 libs/server/Transaction/WatchVersionMap.cs:WatchVersionMap）
//!
//! 每服务器一个实例，按键哈希分桶记录"被监视键"的写入版本；写方在修改
//! 键时推进对应桶计数（C# 由 Tsavorite 函数面调 IncrementVersion），WATCH
//! 校验对比监视时刻与当前的版本差即知键是否被改。
//!
//! 与本域的衔接：[`WatchVersionMap`] 是键级精确版本源；存储会话侧另有以
//! wkv 写日志尾地址为单调代理的保守校验（storage/session/storage_session.rs
//! 的 watch_key / validate_watch_version），两者方向一致——过估只会多中止，
//! 不会漏检。

use std::sync::atomic::{AtomicI64, Ordering::Acquire};

/// WATCH 版本表：2 的幂大小的原子计数数组
pub struct WatchVersionMap {
  /// 版本桶数组（C# long[] map）
  map: Box<[AtomicI64]>,
  /// 桶下标掩码（C# sizeMask = size - 1）
  size_mask: u64,
}

impl WatchVersionMap {
  /// 构造指定大小的版本表
  ///
  /// # Panics
  /// `size` 非 2 的幂或为 0 时 panic（C# Debug.Assert(IsPowerOfTwo) 的
  /// 常量错误属构造期契约破坏）。
  pub fn new(size: u64) -> Self {
    assert!(
      size.is_power_of_two(),
      "WatchVersionMap size must be a power of two"
    );
    let map = (0..size).map(|_| AtomicI64::new(0)).collect::<Box<[_]>>();
    Self {
      map,
      size_mask: size - 1,
    }
  }

  /// 读取键的当前版本（WATCH 之前调用）
  ///
  /// libs/server/Transaction/WatchVersionMap.cs:ReadVersion
  #[inline]
  pub fn read_version(&self, key_hash: u64) -> u64 {
    self.map[(key_hash & self.size_mask) as usize].load(Acquire) as u64
  }

  /// 推进键的版本（修改被监视键时调用）
  ///
  /// libs/server/Transaction/WatchVersionMap.cs:IncrementVersion
  #[inline]
  pub fn increment_version(&self, key_hash: u64) {
    self.map[(key_hash & self.size_mask) as usize].fetch_add(1, Acquire);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  #[should_panic(expected = "power of two")]
  fn rejects_non_power_of_two() {
    let _ = WatchVersionMap::new(3);
  }

  #[test]
  fn read_starts_at_zero_and_increment_bumps_only_own_bucket() {
    let map = WatchVersionMap::new(4);
    assert_eq!(map.read_version(0), 0);
    assert_eq!(map.read_version(u64::MAX), 0);

    map.increment_version(0);
    map.increment_version(0);
    map.increment_version(u64::MAX); // 掩码后落同桶（u64::MAX & 3 == 3）
    assert_eq!(map.read_version(0), 2);
    assert_eq!(map.read_version(3), 1);
    assert_eq!(map.read_version(7), 1); // 同桶别名
  }
}
