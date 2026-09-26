//! 事务键管理面（对标 libs/server/Transaction/TxnKeyManager.cs —— C# partial
//! TransactionManager，此处为跨文件 `impl` 块）
//!
//! C# 的 respSession.clusterSession / parseState 访问经会话引用；Rust 侧
//! 会话以 `&impl TxnSession` 逐调用传入，集群面（槽校验缓存 / 迭代
//! 槽校验）在集群域接线前按 C# 的 `!clusterEnabled` 早退路径落地。

use wbase::store_type::StoreType;

use crate::{
  transaction_manager::TransactionManager, txn_key_entry::LockType,
  txn_key_entry_comparison::TxnKeyEntryComparison, txn_key_spec::TxnKeySpec,
  txn_session::TxnSession,
};

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
  /// 登记待锁键
  ///
  /// libs/server/Transaction/TxnKeyManager.cs:SaveKeyEntryToLock
  ///
  /// 排他锁型标记事务含写操作（决定 AOF 事务条目是否记录）；`prefix` 为会话
  /// 物理归属前缀（`[NsVarint][DbVarint]`，锁轨=物理域，与数据面
  /// `StoreSession::session_prefix` 同一真值源；版本轨=逻辑域另见
  /// [`TransactionManager::watch`](super::transaction_manager::TransactionManager::watch)）：
  /// C# 每库独持锁表（MultiDatabaseManager 各库独立
  /// OverflowBucketLockTable，同名键天然隔离），rust 单物理引擎共享全服一张锁面
  /// （doc/zh/db.md 前缀刚性隔离），键哈希经 [`TxnKeyEntryComparison::scoped_key_hash`]
  /// 单点构造，跨租户/跨库同名键离散到不同主桶，消除假性互斥。仅在 cluster_enabled
  /// 时同步登记键入 txn_keys（对标 C# SaveKeyArgSlice 首行 `!clusterEnabled` 早退），
  /// 供 EXEC 集群槽位校验消费，单机形态零登记零堆分配。
  pub fn save_key_entry_to_lock(&mut self, prefix: &[u8], key: &[u8], lock_type: LockType) {
    // 排他锁型标记事务含写操作（决定 AOF 事务条目是否记录）
    self.perform_writes |= lock_type == LockType::Exclusive;
    self.key_entries.add_key(
      TxnKeyEntryComparison::scoped_key_hash(prefix, key),
      lock_type,
    );
    if self.cluster_enabled && !self.stored_proc_mode {
      self.txn_keys.push(key);
    }
  }

  /// 按命令键规格锁定键
  ///
  /// libs/server/Transaction/TxnKeyManager.cs:LockKeys
  ///
  /// 并入存储面后逐规格展开键窗口：登记锁集（共享/排他按键规格只读位）
  /// 并同步集群槽校验键列表。会话物理前缀（锁轨域）按循环前缀外提准则单次
  /// 外提，全部展开键共用同一归属域（与 WATCH 登记臂 `common_watch` 同为
  /// 单点外提形态，但两轨种子域分置：锁轨=物理、版本轨=逻辑，禁共口互染）。
  pub fn lock_keys(&mut self, session: &(impl TxnSession + ?Sized), command_keys: &TxnCommandKeys) {
    if command_keys.key_specs.is_empty() {
      return;
    }

    self.add_transaction_store_type(command_keys.store_type);

    // 循环前缀外提：会话物理归属前缀单次读取，与写面推进 / WATCH 登记同源
    let prefix = session.session_prefix();
    let prefix = prefix.as_slice();

    let arg_count = session.arg_count();
    for key_spec in &command_keys.key_specs {
      let last_idx = if key_spec.last_idx < 0 {
        // 负索引：-1 表最后一个参数（C# searchArgs 的 endIdx 语义）
        arg_count as i64 + key_spec.last_idx
      } else {
        key_spec.last_idx
      };
      if last_idx < 0 {
        continue;
      }
      let last_idx = (last_idx as usize).min(arg_count.saturating_sub(1));
      if key_spec.first_idx > last_idx || key_spec.first_idx >= arg_count {
        continue;
      }
      let lock_type = if key_spec.read_only {
        LockType::Shared
      } else {
        LockType::Exclusive
      };
      let step = key_spec.step.max(1);
      for idx in (key_spec.first_idx..=last_idx).step_by(step) {
        let key_bytes = session.get_arg(idx);
        self.save_key_entry_to_lock(prefix, key_bytes, lock_type);
      }
    }
  }
}
