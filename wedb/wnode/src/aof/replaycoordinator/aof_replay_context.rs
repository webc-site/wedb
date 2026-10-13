//! 子日志回放缓冲（对标 libs/server/AOF/ReplayCoordinator/
//! AofReplayContext.cs:AofReplayContext + ReplayOperation.cs）
//!
//! 每虚拟子日志一份：模糊区（检查点起始到结束之间的区间）操作缓冲、事务组
//! 缓冲、活动事务表、分块重组读取器。
//!
//! 模糊区语义：检查点起始与结束提交标记之间的区域可同时含有 (v) 与 (v+1)
//! 两代版本条目——(v) 即时处理，(v+1) 入缓冲，检查点结束后统一重放。

use std::collections::VecDeque;

use wbase::map::HashMap;

use crate::aof::aof_chunked_record_reader::{AofChunkedRecordReader, ChunkedAccumulator};

/// libs/server/AOF/ReplayCoordinator/ReplayOperation.cs:ReplayOperation
///
/// 缓冲的重放操作：原始非分块记录字节或已完成的分块累积器
///（C# ReplayOperation 的枚举形态，令事务组与模糊区缓冲共用一个序列）。
pub enum ReplayOperation {
  /// 原始非分块记录字节。
  Record(Vec<u8>),
  /// 已完成的分块累积器。
  Chunk(Box<ChunkedAccumulator>),
}

/// libs/server/AOF/ReplayCoordinator/TransactionGroup.cs:TransactionGroup
///
/// 事务组：同一会话 TxnStart..TxnCommit 间的操作序列。
#[derive(Default)]
pub struct TransactionGroup {
  /// 会话 id。
  pub session_id: i32,
  /// 参与回放任务数（多日志同步用）。
  pub participant_count: u8,
  /// TxnStart 条目的序列号/地址（重放同步用）。
  pub start_sequence_number: i64,
  /// 组内操作（TxnCommit 标记自身不入组）。
  pub operations: Vec<ReplayOperation>,
}

/// libs/server/AOF/ReplayCoordinator/AofReplayContext.cs:AofReplayContext
///
/// 子日志回放缓冲。
#[derive(Default)]
pub struct AofReplayContext {
  /// 模糊区操作缓冲（(v+1) 版本条目）。
  pub fuzzy_region_ops: Vec<ReplayOperation>,
  /// 模糊区内完成的事务组（FIFO，检查点结束后重放）。
  pub txn_group_buffer: VecDeque<TransactionGroup>,
  /// 活动事务组：会话 id → 组。
  pub active_txns: HashMap<i32, TransactionGroup>,
  /// 分块记录重组读取器。
  pub chunked_reader: AofChunkedRecordReader,
  /// 是否处于模糊区。
  pub in_fuzzy_region: bool,
}

impl AofReplayContext {
  /// libs/server/AOF/ReplayCoordinator/AofReplayContext.cs:inFuzzyRegion
  ///
  /// 模糊区标记读取。
  pub fn in_fuzzy_region(&self) -> bool {
    self.in_fuzzy_region
  }

  /// 模糊区标记写入。
  pub fn set_in_fuzzy_region(&mut self, value: bool) {
    self.in_fuzzy_region = value;
  }
}

impl AofReplayContext {
  /// libs/server/AOF/ReplayCoordinator/AofReplayContext.cs:AddTransactionGroup
  ///
  /// 为会话开新事务组（TxnStart）。
  pub fn add_transaction_group(
    &mut self,
    session_id: i32,
    participant_count: u8,
    start_sequence_number: i64,
  ) {
    self.active_txns.insert(
      session_id,
      TransactionGroup {
        session_id,
        participant_count,
        start_sequence_number,
        operations: Vec::new(),
      },
    );
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayContext.cs:AddToFuzzyRegionBuffer
  ///
  /// 模糊区内收到 TxnCommit：先登记提交标记，再把整组入队延后重放。
  pub fn add_to_fuzzy_region_buffer(&mut self, group: TransactionGroup, commit_marker: Vec<u8>) {
    self
      .fuzzy_region_ops
      .push(ReplayOperation::Record(commit_marker));
    self.txn_group_buffer.push_back(group);
  }

  /// C# AofReplayCoordinator FuzzyRegionBufferCount 查询面（精确锚点见 aof_replay_coordinator.rs）
  ///
  /// 模糊区缓冲条数。
  pub fn fuzzy_region_buffer_count(&self) -> usize {
    self.fuzzy_region_ops.len()
  }

  /// C# AofReplayCoordinator ClearFuzzyRegionBuffer 清理面（精确锚点见 aof_replay_coordinator.rs）
  ///
  /// 清空模糊区缓冲。
  pub fn clear_fuzzy_region_buffer(&mut self) {
    self.fuzzy_region_ops.clear();
    self.txn_group_buffer.clear();
  }
}
