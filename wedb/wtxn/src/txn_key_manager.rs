//! 事务键管理面（对标 libs/server/Transaction/TxnKeyManager.cs —— C# partial
//! TransactionManager，此处为跨文件 `impl` 块）
//!
//! C# 的 respSession.clusterSession / parseState 访问经会话引用；Rust 侧
//! 会话以 `&impl TxnSession` 逐调用传入，集群面（槽校验缓存 / 迭代
//! 槽校验）在集群域接线前按 C# 的 `!clusterEnabled` 早退路径落地。

use wbase::store_type::StoreType;

use crate::{
  transaction_manager::TransactionManager, txn_key_entry::LockType, txn_key_spec::TxnKeySpec,
  txn_session::TxnSession,
};

/// 排队命令的键登记元数据（C# SimpleRespCommandInfo 中 LockKeys 所需子集
/// 的本域投影）
pub struct TxnCommandKeys {
  /// 命令存储类别（C# cmdInfo.storeType 投影；C# LockKeys 随附的
  /// AddTransactionStoreType 登记在 wkv 纪元会话模型下为结构性空操作，
  /// rust 已删，字段仅供上层构造投影留存）
  pub store_type: StoreType,
  /// 键规格列表（C# cmdInfo.KeySpecs）
  pub key_specs: Vec<TxnKeySpec>,
}

impl TransactionManager {
  /// 登记待锁键
  ///
  /// libs/server/Transaction/TxnKeyManager.cs:SaveKeyEntryToLock
  ///
  /// 排队期仅将（裸键字节, lock_type）记入 `txn_keys` 缓冲，不冻结物理代际哈希
  /// （对标 C# 键哈希运行期现算契约）。单机与集群统一由 `txn_keys` 单一真源承接，
  /// 待 EXEC 时刻按当前会话物理前缀统一重展开入 `key_entries` 并仲裁写标记。
  pub fn save_key_entry_to_lock(&mut self, key: &[u8], lock_type: LockType) {
    self.txn_keys.push(key, lock_type);
  }

  /// 按命令键规格锁定键
  ///
  /// libs/server/Transaction/TxnKeyManager.cs:LockKeys
  ///
  /// 逐规格展开键窗口并登记锁集（共享/排他按键规格只读位）。
  /// 裸键与锁型进入 `txn_keys` 缓冲，物理前缀（锁轨域）延至 EXEC 展开期单点
  /// 计算，杜绝排队期冻结代际哈希在换号后穿透。
  pub fn lock_keys(&mut self, session: &(impl TxnSession + ?Sized), command_keys: &TxnCommandKeys) {
    if command_keys.key_specs.is_empty() {
      return;
    }

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
        self.save_key_entry_to_lock(key_bytes, lock_type);
      }
    }
  }
}
