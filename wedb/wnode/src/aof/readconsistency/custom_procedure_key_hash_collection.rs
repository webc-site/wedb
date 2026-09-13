//! 自定义过程键哈希收集器（对标 libs/server/AOF/ReadConsistency/CustomProcedureKeyHashCollection.cs）

use std::sync::Arc;

use crate::aof::readconsistency::read_consistency_manager::ReadConsistencyManager;

/// 用于追踪与特定 Custom Procedure 关联的键哈希，以便在 ReadConsistencyManager 中更新它们的时间戳。
pub struct CustomProcedureKeyHashCollection {
  manager: Arc<ReadConsistencyManager>,
  hashes: Vec<i64>,
}

impl CustomProcedureKeyHashCollection {
  /// 创建键哈希收集器
  pub fn new(manager: Arc<ReadConsistencyManager>) -> Self {
    Self {
      manager,
      hashes: Vec::new(),
    }
  }

  /// libs/server/AOF/ReadConsistency/CustomProcedureKeyHashCollection.cs:AddHash
  ///
  /// 添加需要追踪的键哈希
  pub fn add_hash(&mut self, hash: i64) {
    self.hashes.push(hash);
  }

  /// libs/server/AOF/ReadConsistency/CustomProcedureKeyHashCollection.cs:UpdateSequenceNumber
  ///
  /// 为集合中所有记录的键更新序列号
  pub fn update_sequence_number(&self, sequence_number: i64) {
    for &hash in &self.hashes {
      let idx = self.manager.virtual_sublog_idx_of_hash(hash);
      self
        .manager
        .update_virtual_sublog_key_sequence_number(idx, hash, sequence_number);
    }
  }
}
