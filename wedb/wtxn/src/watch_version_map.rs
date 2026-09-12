//! WATCH 版本表（对标 libs/server/Transaction/WatchVersionMap.cs:WatchVersionMap）
//!
//! 每库一张实例，按键哈希分桶记录"被监视键"的写入版本；存储会话写入口在完成实际写入后调用
//! [`WatchVersionMap::increment_version`]。

use std::sync::atomic::{
  AtomicI64,
  Ordering::{Acquire, Release},
};

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
  /// `size` 非 2 的幂或为 0 时 panic
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
  /// 在 garnet 中的相对路径:libs/server/Transaction/WatchVersionMap.cs:ReadVersion
  #[inline]
  pub fn read_version(&self, key_hash: u64) -> u64 {
    self.map[(key_hash & self.size_mask) as usize].load(Acquire) as u64
  }

  /// 推进键的版本（修改被监视键时调用）
  ///
  /// 在 garnet 中的相对路径:libs/server/Transaction/WatchVersionMap.cs:IncrementVersion
  #[inline]
  pub fn increment_version(&self, key_hash: u64) {
    self.map[(key_hash & self.size_mask) as usize].fetch_add(1, Release);
  }

  /// 重置全部版本槽位为 0（FLUSHDB 等全库清空场景使用）
  ///
  /// 在 garnet 中的相对路径:libs/server/Transaction/WatchVersionMap.cs:Reset
  pub fn reset(&self) {
    for slot in self.map.iter() {
      slot.store(0, Release);
    }
  }

  /// 版本表槽位数（对齐 C# `Size` 属性）
  #[inline]
  pub fn size(&self) -> u64 {
    self.size_mask + 1
  }
}
