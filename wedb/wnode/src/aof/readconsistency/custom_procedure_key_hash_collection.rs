//! 存储过程键哈希收集器（对标 libs/server/AOF/ReadConsistency/
//! CustomProcedureKeyHashCollection.cs:CustomProcedureKeyHashCollection）

use std::sync::Arc;

use super::read_consistency_manager::ReadConsistencyManager;
use crate::aof::garnet_append_only_file::GarnetAppendOnlyFile;

/// 用于跟踪给定存储过程所涉 key 的哈希集合，以便在回放结束后经
/// [`ReadConsistencyManager`] 推进它们的读一致性时间戳。
///
/// libs/server/Custom/CustomProcedureBase.cs:customProcKeyHashCollection
pub struct CustomProcedureKeyHashCollection {
  /// 追加日志文件（回放一致性管理器入口；C# appendOnlyFile 字段）。
  append_only_file: Arc<GarnetAppendOnlyFile>,
  /// 过程触达的 key 哈希序列（C# hashes）。
  hashes: Vec<i64>,
}

impl CustomProcedureKeyHashCollection {
  /// libs/server/AOF/ReadConsistency/CustomProcedureKeyHashCollection.cs:CustomProcedureKeyHashCollection
  ///
  /// 以追加日志文件构造收集器。
  pub fn new(append_only_file: &Arc<GarnetAppendOnlyFile>) -> Self {
    Self {
      append_only_file: Arc::clone(append_only_file),
      hashes: Vec::new(),
    }
  }

  /// libs/server/AOF/ReadConsistency/CustomProcedureKeyHashCollection.cs:AddHash
  ///
  /// 批量登记 key 哈希（回放驱动收集面；C# AddHash 逐键登记的等价批量形态）。
  #[inline]
  pub fn extend<I: IntoIterator<Item = i64>>(&mut self, hashes: I) {
    self.hashes.extend(hashes);
  }

  /// 已收集的 key 哈希切片。
  #[inline]
  pub fn hashes(&self) -> &[i64] {
    &self.hashes
  }

  /// 已收集哈希数。
  #[inline]
  pub fn len(&self) -> usize {
    self.hashes.len()
  }

  /// 是否未收集任何哈希。
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.hashes.is_empty()
  }

  /// libs/server/AOF/ReadConsistency/CustomProcedureKeyHashCollection.cs:UpdateSequenceNumber
  ///
  /// 以给定序列号推进集合内全部 key 的时间戳（经
  /// `ReadConsistencyManager.UpdateVirtualSublogKeySequenceNumber(hash, seq)`
  /// 双参重载：按哈希路由目标虚拟子日志）。
  ///
  /// 与 C# 版本的顺序差异：C# 在回放开始前对空集合调用（实际空转）、回放
  /// 期间收集的哈希未消费；Rust 侧按类声明的语义在回放结束后对已收集
  /// 哈希调用，使时间戳推进真实生效。
  pub fn update_sequence_number(&self, sequence_number: i64) {
    if self.hashes.is_empty() {
      return;
    }
    self
      .append_only_file
      .with_read_consistency_manager(|manager: &ReadConsistencyManager| {
        for &hash in &self.hashes {
          manager.update_key_sequence_number_by_hash(hash, sequence_number);
        }
      });
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use wconf::RuntimeServerOptions;

  use super::*;
  use crate::aof::{garnet_log::GarnetLog, test_support::test_backends};

  /// 2 物理 × 2 回放的多子日志拓扑装配（构造即建一致性管理器）
  fn consistency_aof() -> Arc<GarnetAppendOnlyFile> {
    let options = RuntimeServerOptions {
      aof_physical_sublog_count: 2,
      aof_replay_task_count: 2,
      ..RuntimeServerOptions::default()
    };
    let backends = test_backends("cphc", 2);
    Arc::new(GarnetAppendOnlyFile::new(
      Arc::new(GarnetLog::new(&options, backends, None).expect("构造 GarnetLog")),
      &options,
      None,
    ))
  }

  #[test]
  fn add_hash_then_update_routes_by_hash() {
    let aof = consistency_aof();
    let manager = aof.read_consistency_manager().expect("consistency manager");

    let hash_a: i64 = 0x0123_4567_89ab_cdef;
    let hash_b: i64 = -0x0fed_cba9_8765_4321;

    let mut tracker = CustomProcedureKeyHashCollection::new(&aof);
    assert!(tracker.is_empty());
    tracker.extend([hash_a, hash_b]);
    assert_eq!(tracker.len(), 2);
    assert_eq!(tracker.hashes(), &[hash_a, hash_b]);

    tracker.update_sequence_number(42);

    // 哈希路由后的虚拟子日志草图应推进到目标序列号
    for hash in [hash_a, hash_b] {
      let idx = manager.virtual_sublog_idx_of_hash(hash);
      assert_eq!(manager.vsr(idx).get_key_sequence_number(hash), 42);
    }
  }

  #[test]
  fn empty_update_is_noop() {
    let aof = consistency_aof();
    let tracker = CustomProcedureKeyHashCollection::new(&aof);
    tracker.update_sequence_number(7);
    assert!(tracker.hashes().is_empty());
  }
}
