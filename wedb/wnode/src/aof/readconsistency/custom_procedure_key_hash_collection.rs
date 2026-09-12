//! 存储过程 key 哈希集合（对标 libs/server/AOF/ReadConsistency/
//! CustomProcedureKeyHashCollection.cs:CustomProcedureKeyHashCollection）
//!
//! 追踪一次存储过程涉及的 key 哈希，执行完成后按同一序列号批量推进
//! 读取一致性时间戳。

use std::sync::Arc;

use super::read_consistency_manager::ReadConsistencyManager;

/// 存储过程 key 哈希集合。
#[derive(Default)]
pub struct CustomProcedureKeyHashCollection {
  manager: Option<Arc<ReadConsistencyManager>>,
  hashes: Vec<i64>,
}

impl CustomProcedureKeyHashCollection {
  /// 绑定一致性管理器构造（C# 构造入参 appendOnlyFile 的直连形态）。
  pub fn new(manager: Arc<ReadConsistencyManager>) -> Self {
    Self {
      manager: Some(manager),
      hashes: Vec::new(),
    }
  }

  /// libs/server/AOF/ReadConsistency/CustomProcedureKeyHashCollection.cs:AddHash
  ///
  /// 登记 key 哈希。
  pub fn add_hash(&mut self, hash: i64) {
    self.hashes.push(hash);
  }

  /// libs/server/AOF/ReadConsistency/CustomProcedureKeyHashCollection.cs:UpdateSequenceNumber
  ///
  /// 以同一序列号推进全部已登记 key 的时间戳。
  pub fn update_sequence_number(&self, sequence_number: i64) {
    if let Some(manager) = &self.manager {
      for &hash in &self.hashes {
        manager.update_key_sequence_number_by_hash(hash, sequence_number);
      }
    }
  }

  /// 已登记哈希数（测试面）。
  pub fn len(&self) -> usize {
    self.hashes.len()
  }

  /// 是否为空（测试面）。
  pub fn is_empty(&self) -> bool {
    self.hashes.is_empty()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn unbound_collection_is_noop() {
    let mut collection = CustomProcedureKeyHashCollection::default();
    assert!(collection.is_empty());
    collection.add_hash(1);
    collection.add_hash(2);
    assert_eq!(collection.len(), 2);
    // 未绑定管理器：推进为空操作（C# appendOnlyFile 为 null 的防御形态）
    collection.update_sequence_number(5);
  }

  #[test]
  fn bound_collection_advances_key_timestamps() {
    let manager = Arc::new(ReadConsistencyManager::new(1, 1, 1, -1, 0));
    let mut collection = CustomProcedureKeyHashCollection::new(Arc::clone(&manager));
    let hash = manager.key_hash(b"proc-key");
    collection.add_hash(hash);
    collection.update_sequence_number(9);
    assert_eq!(manager.get_key_sequence_number_by_hash(hash), 9);
  }
}
