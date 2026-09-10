//! 集群槽校验键登记（对标 libs/server/Transaction/TxnClusterSlotCheck.cs ——
//! C# partial TransactionManager，此处为跨文件 `impl` 块）
//!
//! C# 把命令实际触达的键登记进 `txnKeysParseState`（EXEC 时整体送集群槽
//! 校验）；键引用依赖接收缓冲指针，故有"缓冲变更即拷入 scratch"的维护面。
//! Rust 侧键列表为自有副本（`Vec<Box<[u8]>>`），生来独立于接收缓冲。

use super::transaction_manager::TransactionManager;

impl TransactionManager {
  /// 登记命令实际触达的键
  ///
  /// libs/server/Transaction/TxnClusterSlotCheck.cs:SaveKeyArgSlice
  ///
  /// 非集群模式不登记（C# 同款早退）；键副本进入槽校验键列表。
  pub fn save_key_arg_slice(&mut self, key: &[u8]) {
    if !self.cluster_enabled {
      return;
    }
    self.txn_keys.push(key.into());
  }

  /// 把既有键拷入事务私有缓冲，解除对旧接收缓冲的依赖
  ///
  /// libs/server/Transaction/TxnClusterSlotCheck.cs:CopyExistingKeysToScratchBuffer
  ///
  /// C# 在接收缓冲重分配后重拷键切片；Rust 键列表为自有副本（入列即拷贝），
  /// 无需重拷，保留方法对齐调用位（NetworkSKIP 的缓冲变更分支）。
  pub fn copy_existing_keys_to_scratch_buffer(&mut self) {
    debug_assert!(self.cluster_enabled);
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use super::*;
  use crate::transaction::watch_version_map::WatchVersionMap;

  #[test]
  fn save_key_arg_slice_registers_only_in_cluster_mode() {
    let mut txn = TransactionManager::new(Arc::new(WatchVersionMap::new(64)), None, true);
    txn.save_key_arg_slice(b"k1");
    txn.save_key_arg_slice(b"k2");
    assert_eq!(txn.txn_keys.len(), 2);
    assert_eq!(txn.txn_keys[0].as_ref(), b"k1");

    let mut standalone = TransactionManager::new(Arc::new(WatchVersionMap::new(64)), None, false);
    standalone.save_key_arg_slice(b"k1");
    assert!(standalone.txn_keys.is_empty());
  }

  #[test]
  fn keys_are_independent_of_caller_buffer() {
    let mut txn = TransactionManager::new(Arc::new(WatchVersionMap::new(64)), None, true);
    let scratch = b"transient".to_vec();
    txn.save_key_arg_slice(&scratch);
    drop(scratch);
    // 副本语义：原缓冲释放后登记键仍可用
    assert_eq!(txn.txn_keys[0].as_ref(), b"transient");
    txn.copy_existing_keys_to_scratch_buffer();
  }
}
