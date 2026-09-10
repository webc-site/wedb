//! 会话级被监视键容器（对标 libs/server/Transaction/TxnWatchedKeysContainer.cs:
//! WatchedKeysContainer）
//!
//! C# 以 WatchedKeySlice 数组 + 事务专用 scratch 缓冲持有被监视键副本，
//! 版本取自 [`super::watch_version_map::WatchVersionMap`]；Rust 侧键副本
//! 为自有 Vec（生来独立于接收缓冲），结构其余语义逐一保留。

use std::sync::Arc;

use super::{txn_key_entry_comparison::TxnKeyEntryComparison, watch_version_map::WatchVersionMap};

/// 单个被监视键的快照（C# WatchedKeySlice：slice / isWatched / hash / version）
#[derive(Debug, Clone)]
struct WatchedKeySlice {
  /// 键字节副本（C# slice，取自事务 scratch 缓冲）
  key: Box<[u8]>,
  /// 键哈希（C# hash = Utility.HashBytes）
  hash: u64,
  /// 监视时刻版本（C# version = versionMap.ReadVersion(hash)）
  version: u64,
  /// 是否仍被监视（C# isWatched；RemoveWatch 置否但不删位）
  is_watched: bool,
}

/// 每会话的被监视键容器
pub struct TxnWatchedKeysContainer {
  /// 被监视键数组（C# keySlices）
  key_slices: Vec<WatchedKeySlice>,
  /// 版本表（C# versionMap）
  version_map: Arc<WatchVersionMap>,
}

impl TxnWatchedKeysContainer {
  /// 构造容器（C# 构造入参 size 为切片缓冲初始容量；Vec 自扩容承接）
  pub fn new(version_map: Arc<WatchVersionMap>) -> Self {
    Self {
      key_slices: Vec::new(),
      version_map,
    }
  }

  /// 重置被监视键（EXEC / DISCARD / UNWATCH 收尾）
  ///
  /// libs/server/Transaction/TxnWatchedKeysContainer.cs:Reset
  pub fn reset(&mut self) {
    self.key_slices.clear();
  }

  /// 移除对指定键的监视（保留占位，isWatched 置否）
  ///
  /// libs/server/Transaction/TxnWatchedKeysContainer.cs:RemoveWatch
  pub fn remove_watch(&mut self, key: &[u8]) -> bool {
    for slice in &mut self.key_slices {
      if slice.key.as_ref() == key {
        slice.is_watched = false;
        return true;
      }
    }
    false
  }

  /// 追加被监视键并记录当前版本
  ///
  /// libs/server/Transaction/TxnWatchedKeysContainer.cs:AddWatch
  pub fn add_watch(&mut self, key: &[u8]) {
    let hash = TxnKeyEntryComparison::key_hash(key) as u64;
    let version = self.version_map.read_version(hash);
    self.key_slices.push(WatchedKeySlice {
      key: key.into(),
      hash,
      version,
      is_watched: true,
    });
  }

  /// 校验全部被监视键版本未变（记录未被修改）
  ///
  /// libs/server/Transaction/TxnWatchedKeysContainer.cs:ValidateWatchVersion
  pub fn validate_watch_version(&self) -> bool {
    for slice in &self.key_slices {
      if !slice.is_watched {
        continue;
      }
      if self.version_map.read_version(slice.hash) != slice.version {
        return false;
      }
    }
    true
  }

  /// 仍被监视键的快照（供管理器登记进锁集）
  ///
  /// libs/server/Transaction/TxnWatchedKeysContainer.cs:SaveKeysToLock
  ///
  /// C# 直接逐键调 txnManager.SaveKeyEntryToLock(slice, Shared)；Rust 侧
  /// 容器与管理器字段借用分域，返回快照由管理器登记。
  pub fn save_keys_to_lock(&self) -> Vec<Box<[u8]>> {
    self
      .key_slices
      .iter()
      .filter(|slice| slice.is_watched)
      .map(|slice| slice.key.clone())
      .collect()
  }

  /// 全部被监视键（含已移除监视位）的快照（供管理器登记进集群槽校验键列表）
  ///
  /// libs/server/Transaction/TxnWatchedKeysContainer.cs:SaveKeysToKeyList
  pub fn save_keys_to_key_list(&self) -> Vec<Box<[u8]>> {
    self
      .key_slices
      .iter()
      .map(|slice| slice.key.clone())
      .collect()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn container() -> (TxnWatchedKeysContainer, Arc<WatchVersionMap>) {
    let map = Arc::new(WatchVersionMap::new(64));
    (TxnWatchedKeysContainer::new(Arc::clone(&map)), map)
  }

  #[test]
  fn add_watch_then_untouched_key_validates() {
    let (mut c, _map) = container();
    c.add_watch(b"user:1");
    assert!(c.validate_watch_version());
  }

  #[test]
  fn modified_watched_key_fails_validation() {
    let (mut c, map) = container();
    c.add_watch(b"user:1");
    // 模拟写方推进该键版本（C# Tsavorite 函数面 IncrementVersion）
    map.increment_version(TxnKeyEntryComparison::key_hash(b"user:1") as u64);
    assert!(!c.validate_watch_version());
  }

  #[test]
  fn remove_watch_excludes_key_from_validation() {
    let (mut c, map) = container();
    c.add_watch(b"k");
    map.increment_version(TxnKeyEntryComparison::key_hash(b"k") as u64);
    assert!(c.remove_watch(b"k"));
    // C# RemoveWatch 按键匹配即置位（不复核 isWatched）：二次移除同样返回
    // true —— 1:1 保留该语义
    assert!(c.remove_watch(b"k"));
    assert!(c.validate_watch_version());
  }

  #[test]
  fn reset_clears_all_watches() {
    let (mut c, map) = container();
    c.add_watch(b"a");
    c.add_watch(b"b");
    map.increment_version(TxnKeyEntryComparison::key_hash(b"a") as u64);
    c.reset();
    assert!(c.validate_watch_version());
  }
}
