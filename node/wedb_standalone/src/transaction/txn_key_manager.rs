//! 事务键管理面（对标 libs/server/Transaction/TxnKeyManager.cs —— C# partial
//! TransactionManager，此处为跨文件 `impl` 块）
//!
//! C# 的 respSession.clusterSession / parseState 访问经会话引用；Rust 侧
//! 会话以 `&mut RespServerSession` 逐调用传入，集群面（槽校验缓存 / 迭代
//! 槽校验）在集群域接线前按 C# 的 `!clusterEnabled` 早退路径落地。

use super::{
  transaction_manager::{TransactionManager, TxnState},
  txn_key_entry::{LockType, TxnKeyEntries},
  txn_key_entry_comparison::TxnKeyEntryComparison,
};
use crate::{
  resp::resp_server_session::RespServerSession, storage::session::storage_session::StoreType,
};

/// 键规格检索参数（C# KeySpecification 检索产物的本域投影：
/// `firstIdx..=lastIdx step` 迭代窗口 + 读写属性）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxnKeySpec {
  /// 起始参数下标
  pub first_idx: usize,
  /// 终止参数下标（含；可为 -1 表"倒数第一个"，对齐键规格负索引）
  pub last_idx: i64,
  /// 步长
  pub step: usize,
  /// 是否只读键（决定共享 / 排他锁型）
  pub read_only: bool,
}

impl TxnKeySpec {
  /// 构造逐键检索窗口
  pub fn new(first_idx: usize, last_idx: i64, step: usize, read_only: bool) -> Self {
    Self {
      first_idx,
      last_idx,
      step,
      read_only,
    }
  }
}

/// 排队命令的键登记元数据（C# SimpleRespCommandInfo 中 LockKeys 所需子集
/// 的本域投影）
#[derive(Debug, Clone)]
pub struct TxnCommandKeys {
  /// 命令存储类别（并入事务存储面）
  pub store_type: StoreType,
  /// 键规格列表（C# cmdInfo.KeySpecs）
  pub key_specs: Vec<TxnKeySpec>,
}

impl TransactionManager {
  /// 锁登记内核（libs/server/Transaction/TxnKeyManager.cs:SaveKeyEntryToLock
  /// 主体；跨文件 impl 域的字段拆借路径共用，避免 WATCH 键并集路径与
  /// 容器借用冲突）
  pub(crate) fn register_key_lock(
    key_entries: &mut TxnKeyEntries,
    perform_writes: &mut bool,
    key: &[u8],
    lock_type: LockType,
  ) {
    // 排他锁型标记事务含写操作（决定 AOF 事务条目是否记录）
    *perform_writes |= lock_type == LockType::Exclusive;
    key_entries.add_key(TxnKeyEntryComparison::key_hash(key), lock_type);
  }

  /// 登记待锁键
  ///
  /// libs/server/Transaction/TxnKeyManager.cs:SaveKeyEntryToLock
  ///
  /// 排他锁型标记事务含写操作（决定 AOF 事务条目是否记录）。
  pub fn save_key_entry_to_lock(&mut self, key: &[u8], lock_type: LockType) {
    Self::register_key_lock(
      &mut self.key_entries,
      &mut self.perform_writes,
      key,
      lock_type,
    );
  }

  /// 重置集群槽校验结果缓存
  ///
  /// libs/server/Transaction/TxnKeyManager.cs:ResetCacheSlotVerificationResult
  pub fn reset_cache_slot_verification_result(&mut self) {
    if self.cluster_enabled {
      // 集群会话缓存面由 cluster 域承接（IClusterSession.ResetCachedSlotVerificationResult），
      // 接线前本域已保证状态自洽（无缓存可清）。
    }
  }

  /// 写出缓存的槽校验失败消息
  ///
  /// libs/server/Transaction/TxnKeyManager.cs:WriteCachedSlotVerificationMessage
  pub fn write_cached_slot_verification_message(&self, _output: &mut Vec<u8>) {
    if self.cluster_enabled {
      // 集群会话缓存消息面由 cluster 域承接；非集群路径不产出消息。
    }
  }

  /// 校验键归属（集群模式迭代槽校验）
  ///
  /// libs/server/Transaction/TxnKeyManager.cs:VerifyKeyOwnership
  ///
  /// C# 调 respSession.clusterSession.NetworkIterativeSlotVerify(key,
  /// readOnly, SessionAsking, waitForStableSlot: false)，失败置 Aborted；
  /// 集群校验面由 cluster 域接线（[`Self::abort_key_ownership`] 供其回填
  /// 失败置位），单机路径归属恒成立。
  pub fn verify_key_ownership(
    &mut self,
    _session: &RespServerSession,
    _key: &[u8],
    lock_type: LockType,
  ) {
    if !self.cluster_enabled || self.is_replaying {
      return;
    }
    // readOnly = lock_type == Shared（C# 同式）；接线前视为归属成立。
    let _ = lock_type == LockType::Shared;
  }

  /// 校验键归属失败置位的独立判定入口（集群域接线用）
  pub fn abort_key_ownership(&mut self) {
    self.state = TxnState::Aborted;
  }

  /// 按命令键规格锁定键
  ///
  /// libs/server/Transaction/TxnKeyManager.cs:LockKeys
  ///
  /// 并入存储面后逐规格展开键窗口：登记锁集（共享/排他按键规格只读位）
  /// 并同步集群槽校验键列表。
  pub fn lock_keys(&mut self, session: &RespServerSession, command_keys: &TxnCommandKeys) {
    if command_keys.key_specs.is_empty() {
      return;
    }

    self.add_transaction_store_type(command_keys.store_type);

    for key_spec in &command_keys.key_specs {
      let last_idx = if key_spec.last_idx < 0 {
        // 负索引：-1 表最后一个参数（C# searchArgs 的 endIdx 语义）
        (session.parse_state.count as i64 + key_spec.last_idx).max(0)
      } else {
        key_spec.last_idx.min(session.parse_state.count as i64)
      };
      let mut curr_idx = key_spec.first_idx;
      // curr_idx < count 越界防护：参数不足的边界形态不得 panic（C# 同位
      // 置 GetArgSliceByRef 越界抛异常，此处以静默截断承接）
      while curr_idx <= last_idx as usize && curr_idx < session.parse_state.count {
        let key = session.parse_state.get_arg_slice_by_ref(curr_idx);
        let key_bytes = key.as_slice();
        let lock_type = if key_spec.read_only {
          LockType::Shared
        } else {
          LockType::Exclusive
        };
        self.save_key_entry_to_lock(key_bytes, lock_type);
        self.save_key_arg_slice(key_bytes);
        curr_idx += key_spec.step.max(1);
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use super::{
    super::{
      transaction_manager::{TransactionStoreTypes, TxnState},
      watch_version_map::WatchVersionMap,
    },
    *,
  };
  use crate::{arg_slice::ArgSlice, resp::resp_server_session::RespServerSession};

  fn manager() -> TransactionManager {
    TransactionManager::new(Arc::new(WatchVersionMap::new(64)), None, false)
  }

  /// 带参数解析态的会话（参数缓冲先聚合后取指针，避免扩容失效）
  fn session_with_args(args: &[&[u8]]) -> (RespServerSession, Vec<u8>) {
    let mut buffer: Vec<u8> = Vec::new();
    for arg in args {
      buffer.extend_from_slice(arg);
    }
    let mut slices = Vec::with_capacity(args.len());
    let mut offset = 0usize;
    for arg in args {
      slices.push(ArgSlice::new(
        unsafe { buffer.as_ptr().add(offset) },
        arg.len(),
      ));
      offset += arg.len();
    }
    let mut session = RespServerSession::default();
    session.parse_state.initialize_with_args(&slices);
    (session, buffer)
  }

  #[test]
  fn save_key_entry_marks_perform_writes_on_exclusive() {
    let mut txn = manager();
    txn.save_key_entry_to_lock(b"a", LockType::Shared);
    assert!(!txn.perform_writes);
    txn.save_key_entry_to_lock(b"b", LockType::Exclusive);
    assert!(txn.perform_writes);
    assert_eq!(txn.key_entries.count(), 2);
  }

  #[test]
  fn lock_keys_expands_window_and_registers_keys() {
    let mut txn = manager();
    let (session, _buffer) = session_with_args(&[b"k1", b"k2", b"k3"]);
    let keys = TxnCommandKeys {
      store_type: StoreType::Main,
      key_specs: vec![TxnKeySpec::new(0, 2, 1, false)],
    };
    txn.lock_keys(&session, &keys);
    assert_eq!(txn.key_entries.count(), 3);
    assert!(txn.perform_writes);
    // 集群关闭时不登记槽校验键列表
    assert!(txn.txn_keys.is_empty());
    assert!(txn.store_types.contains(TransactionStoreTypes::Main));
  }

  #[test]
  fn lock_keys_negative_index_pins_to_last_arg() {
    let mut txn = manager();
    let (session, _buffer) = session_with_args(&[b"k1", b"k2", b"k3"]);
    let keys = TxnCommandKeys {
      store_type: StoreType::Main,
      key_specs: vec![TxnKeySpec::new(2, -1, 1, true)],
    };
    txn.lock_keys(&session, &keys);
    // -1 收敛到末参（k3），只读 → 共享锁
    assert_eq!(txn.key_entries.count(), 1);
    assert!(!txn.perform_writes);
  }

  #[test]
  fn verify_key_ownership_noop_without_cluster() {
    let mut txn = manager();
    let session = RespServerSession::default();
    txn.verify_key_ownership(&session, b"k", LockType::Shared);
    assert_eq!(txn.state, TxnState::None);
  }
}
