//! 会话级被监视键容器（对标 libs/server/Transaction/TxnWatchedKeysContainer.cs: WatchedKeysContainer）
//!
//! 持有被监视键副本，版本取自 [`super::watch_version_map::WatchVersionMap`]。

use std::sync::Arc;

use smallvec::SmallVec;

use super::{txn_key_entry_comparison::TxnKeyEntryComparison, watch_version_map::WatchVersionMap};

/// 单个被监视键的快照
#[derive(Debug, Clone)]
struct WatchedKeySlice {
  /// 键字节副本
  key: Box<[u8]>,
  /// 键哈希
  hash: u64,
  /// 监视时刻版本
  version: u64,
  /// 是否仍被监视
  is_watched: bool,
}

/// 每会话的被监视键容器
pub struct TxnWatchedKeysContainer {
  /// 被监视键数组（内联 4 槽位，覆盖绝大多数 WATCH 事务，消除堆分配）
  key_slices: SmallVec<[WatchedKeySlice; 4]>,
  /// 版本表
  version_map: Arc<WatchVersionMap>,
}

impl TxnWatchedKeysContainer {
  /// 构造容器
  pub fn new(version_map: Arc<WatchVersionMap>) -> Self {
    Self {
      key_slices: SmallVec::new(),
      version_map,
    }
  }

  /// 重置被监视键（EXEC / DISCARD / UNWATCH 收尾）
  ///
  /// 在 garnet 中的相对路径:libs/server/Transaction/TxnWatchedKeysContainer.cs:Reset
  pub fn reset(&mut self) {
    self.key_slices.clear();
  }

  /// 追加被监视键并记录当前版本
  ///
  /// 在 garnet 中的相对路径:libs/server/Transaction/TxnWatchedKeysContainer.cs:AddWatch
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
  /// 在 garnet 中的相对路径:libs/server/Transaction/TxnWatchedKeysContainer.cs:ValidateWatchVersion
  pub fn validate_watch_version(&self) -> bool {
    self
      .key_slices
      .iter()
      .filter(|slice| slice.is_watched)
      .all(|slice| self.version_map.read_version(slice.hash) == slice.version)
  }

  /// 仍被监视键的引用序列（供管理器登记进锁集）
  ///
  /// 在 garnet 中的相对路径:libs/server/Transaction/TxnWatchedKeysContainer.cs:SaveKeysToLock
  pub fn save_keys_to_lock(&self) -> impl Iterator<Item = &[u8]> {
    self
      .key_slices
      .iter()
      .filter(|slice| slice.is_watched)
      .map(|slice| slice.key.as_ref())
  }

  /// 全部被监视键（含已移除监视位）的引用序列（供管理器登记进集群槽校验键列表）
  ///
  /// 在 garnet 中的相对路径:libs/server/Transaction/TxnWatchedKeysContainer.cs:SaveKeysToKeyList
  pub fn save_keys_to_key_list(&self) -> impl Iterator<Item = &[u8]> {
    self.key_slices.iter().map(|slice| slice.key.as_ref())
  }
}
