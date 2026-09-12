//! AOF 回放协调器（对标 libs/server/AOF/ReplayCoordinator/
//! AofReplayCoordinator.cs:AofReplayCoordinator）
//!
//! 事务组建组/提交决策、模糊区缓冲、跨子日志同步栅栏（LeaderBarrier）。
//! C# 嵌套类形态直接持有 AofProcessor 回调；rust 侧协调器只做同步决策与
//! 缓冲，重放应用（异步存储面）由 [`AofProcessor`](super::super::aof_processor::AofProcessor)
//! 持有编排权，两侧以 [`TxnAction`] 与缓冲出入口衔接。
//!
//! 栅栏键（C# BarrierKey）：负值段为 LeaderBarrierType（检查点 / 流式检查点 /
//! FLUSH 族），正值段为会话 id；txnId 位放序列号。

use std::{mem::take, sync::Arc};

use gxhash::HashMap;
use parking_lot::{Mutex, RwLock};
use waof::AofEntryType;

use crate::aof::{
  aof_header::{
    AofHeader, AofHeaderType, AofShardedHeader, AofShardedLogTransactionHeader,
    AofSingleLogTransactionHeader,
  },
  readconsistency::read_consistency_manager::ReadConsistencyManager,
  replaycoordinator::aof_replay_context::{AofReplayContext, ReplayOperation, TransactionGroup},
};

/// 同步栅栏类别（C# LeaderBarrierType；仅负值，避免与会话 id 冲突）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum LeaderBarrierType {
  /// 检查点
  Checkpoint = -1,
  /// 流式检查点
  StreamingCheckpoint = -2,
  /// FLUSH DB
  FlushDb = -3,
  /// FLUSH ALL
  FlushDbAll = -4,
  /// 自定义存储过程
  CustomStoredProc = -5,
}

/// 栅栏键：会话段 + 序列号段。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BarrierKey {
  /// 会话 id（或 LeaderBarrierType 取值）。
  pub session_id: i32,
  /// 序列号 / 条目地址。
  pub txn_id: i64,
}

impl BarrierKey {
  pub const fn new(session_id: i32, txn_id: i64) -> Self {
    Self { session_id, txn_id }
  }

  /// C# BarrierKey.Equals。
  pub fn equals(&self, other: &BarrierKey) -> bool {
    self == other
  }
}

/// 跨子日志同步栅栏（C# LeaderBarrier）。
#[derive(Debug)]
pub struct LeaderBarrier {
  /// 参与者数。
  pub participant_count: i16,
  /// 已到场参与者数。
  pub arrived: i16,
  /// 是否已放行。
  pub released: bool,
}

impl LeaderBarrier {
  /// 新建栅栏。
  pub fn new(participant_count: i16) -> Self {
    Self {
      participant_count,
      arrived: 0,
      released: false,
    }
  }

  /// 到场：首个到达者为 leader（负责执行同步操作并清理栅栏）。
  /// 返回 (是否 leader, 是否全员到齐)。
  pub fn try_signal_or_wait(&mut self) -> (bool, bool) {
    self.arrived += 1;
    if self.arrived >= self.participant_count {
      self.released = true;
      return (self.arrived == 1, true);
    }
    (self.arrived == 1, false)
  }
}

/// 事务条目处理结论（协调器 → 处理器的决策面）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnAction {
  /// 条目已被消化（组内缓冲 / 事务标记），调用方无需再处理。
  Handled,
  /// TxnCommit：直接交还已取出的事务组由处理器立即重放。
  Commit { group: TransactionGroup },
  /// 非事务条目：交还调用方按常规分派。
  None,
}

/// AOF 回放协调器。
pub struct AofReplayCoordinator {
  /// 每虚拟子日志的回放缓冲（C# 按子日志线程私有；rust 以锁承接 &self 变更）。
  contexts: Vec<Mutex<AofReplayContext>>,
  /// 多日志拓扑（事务/栅栏同步仅在多日志模式启用）。
  multi_log_enabled: bool,
  /// 进行中的同步栅栏（C# leaderBarriers）。
  leader_barriers: Mutex<HashMap<BarrierKey, LeaderBarrier>>,
  /// 一致性管理器挂载点（处理器构造时注入；序列号推进用）。
  consistency_manager: RwLock<Option<Arc<ReadConsistencyManager>>>,
}

/// 上下文锁守卫别名（跨子日志缓冲的互斥访问面）。
pub type ContextGuard<'a> = parking_lot::MutexGuard<'a, AofReplayContext>;

impl AofReplayCoordinator {
  /// 构造：按虚拟子日志数初始化回放缓冲（C# InitializeReplayContext）。
  pub fn new(virtual_sublog_count: usize, multi_log_enabled: bool) -> Self {
    Self {
      contexts: (0..virtual_sublog_count.max(1))
        .map(|_| Mutex::new(AofReplayContext::default()))
        .collect(),
      multi_log_enabled,
      leader_barriers: Mutex::new(HashMap::default()),
      consistency_manager: RwLock::new(None),
    }
  }

  /// 注入一致性管理器（序列号推进出口）。
  pub fn set_consistency_manager(&self, manager: Arc<ReadConsistencyManager>) {
    *self.consistency_manager.write() = Some(manager);
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:GetReplayContext
  pub fn context(&self, sublog_idx: usize) -> ContextGuard<'_> {
    let idx = sublog_idx.min(self.contexts.len() - 1);
    self.contexts[idx].lock()
  }

  /// 模糊区缓冲条数（C# FuzzyRegionBufferCount）。
  pub fn fuzzy_region_buffer_count(&self, sublog_idx: usize) -> usize {
    self.context(sublog_idx).fuzzy_region_buffer_count()
  }

  /// 清空模糊区缓冲（C# ClearFuzzyRegionBuffer）。
  pub fn clear_fuzzy_region_buffer(&self, sublog_idx: usize) {
    self.context(sublog_idx).clear_fuzzy_region_buffer();
  }

  /// 入模糊区缓冲（C# AddFuzzyRegionOperation；原始记录与分块累积器两态）。
  pub fn add_fuzzy_region_operation(&self, sublog_idx: usize, operation: ReplayOperation) {
    self.context(sublog_idx).fuzzy_region_ops.push(operation);
  }

  /// 取走全部模糊区操作（重放后丢弃）。
  pub fn take_fuzzy_region_operations(&self, sublog_idx: usize) -> Vec<ReplayOperation> {
    let mut context = self.context(sublog_idx);
    take(&mut context.fuzzy_region_ops)
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:AddOrReplayTransactionOperation
  ///
  /// 事务条目消化：
  /// 1. 会话已有活动组 → TxnStart 不允许嵌套 / TxnAbort 清组 / TxnCommit
  ///    提交或入模糊区 / 其余入组；
  /// 2. 无活动组 → TxnStart 建组；孤儿 TxnAbort/TxnCommit 忽略（检查点
  ///    截断后的前代残留）；其余交还调用方（TxnAction::None）。
  pub fn add_or_replay_transaction_operation(
    &self,
    virtual_sublog_idx: usize,
    entry: &[u8],
    log_address_sequence_number: i64,
  ) -> TxnAction {
    let Some(header) = AofHeader::parse(entry) else {
      return TxnAction::None;
    };
    let Ok(op_type) = AofEntryType::try_from(header.op_type) else {
      return TxnAction::None;
    };
    let session_id = header.session_id;

    let mut ctx = self.context(virtual_sublog_idx);
    if ctx.active_txns.contains_key(&session_id) {
      match op_type {
        AofEntryType::TxnStart => TxnAction::Handled, // 不允许嵌套事务（C# GarnetException）
        AofEntryType::TxnAbort => {
          ctx.active_txns.remove(&session_id);
          drop(ctx);
          self.update_max_sequence_number_from_header(
            virtual_sublog_idx,
            entry,
            log_address_sequence_number,
          );
          TxnAction::Handled
        }
        AofEntryType::TxnCommit => {
          let in_fuzzy = ctx.in_fuzzy_region;
          let group = ctx.active_txns.remove(&session_id);
          if in_fuzzy {
            // 模糊区：登记提交标记 + 整组入队延后重放
            if let Some(group) = group {
              ctx.add_to_fuzzy_region_buffer(group, entry.to_vec());
            }
            TxnAction::Handled
          } else if let Some(group) = group {
            // 立即提交：组直接交处理器重放
            TxnAction::Commit { group }
          } else {
            TxnAction::Handled
          }
        }
        AofEntryType::StoredProcedure => {
          // 事务内不允许存储过程（C# GarnetException；按损坏条目消化）
          TxnAction::Handled
        }
        _ => {
          if let Some(group) = ctx.active_txns.get_mut(&session_id) {
            group
              .operations
              .push(ReplayOperation::Record(entry.to_vec()));
          }
          TxnAction::Handled
        }
      }
    } else {
      match op_type {
        AofEntryType::TxnStart => {
          // 参与者数：多日志事务头取头内值；其余形态取全量回放任务
          let participant_count = self
            .txn_header_participant_count(entry)
            .map(|c| c as u8)
            .unwrap_or(0);
          let start_sequence_number =
            self.txn_header_sequence_number(entry, log_address_sequence_number);
          ctx.add_transaction_group(
            session_id,
            virtual_sublog_idx,
            participant_count,
            start_sequence_number,
          );
          TxnAction::Handled
        }
        AofEntryType::TxnAbort | AofEntryType::TxnCommit => {
          // 检查点截断后的前代事务尾：忽略但推进序列号
          drop(ctx);
          self.update_max_sequence_number_from_header(
            virtual_sublog_idx,
            entry,
            log_address_sequence_number,
          );
          TxnAction::Handled
        }
        _ => TxnAction::None,
      }
    }
  }

  /// 事务头参与者数（SingleLog/Sharded 事务头形态）。
  fn txn_header_participant_count(&self, entry: &[u8]) -> Option<i16> {
    if let Some(h) = AofSingleLogTransactionHeader::parse(entry) {
      return Some(h.participant_count);
    }
    AofShardedLogTransactionHeader::parse_sharded(entry).map(|h| h.participant_count)
  }

  /// 事务头序列号（分片取内嵌；其余取条目地址）。
  fn txn_header_sequence_number(&self, entry: &[u8], entry_address: i64) -> i64 {
    if let Some(h) = AofShardedLogTransactionHeader::parse_sharded(entry) {
      return h.sharded.sequence_number;
    }
    if let Some(sh) = AofShardedHeader::parse(entry) {
      return sh.sequence_number;
    }
    entry_address
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:ClearSessionTxn
  pub fn clear_session_txn(&self, virtual_sublog_idx: usize, session_id: i32) {
    self
      .context(virtual_sublog_idx)
      .active_txns
      .remove(&session_id);
  }

  /// 取走会话活动事务组（提交路径）。
  pub fn take_transaction_group(
    &self,
    virtual_sublog_idx: usize,
    session_id: i32,
  ) -> Option<TransactionGroup> {
    self
      .context(virtual_sublog_idx)
      .active_txns
      .remove(&session_id)
  }

  /// 分块记录入组（分块路径的事务缓冲；C# 分块形态 AddOrReplay）。
  pub fn buffer_chunk_operation(
    &self,
    virtual_sublog_idx: usize,
    session_id: i32,
    chunk: ReplayOperation,
  ) -> bool {
    if let Some(group) = self
      .context(virtual_sublog_idx)
      .active_txns
      .get_mut(&session_id)
    {
      group.operations.push(chunk);
      return true;
    }
    false
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:UpdateMaxSequenceNumberFromHeader
  ///
  /// 按头型取序列号并推进虚拟子日志最大值（无 key 条目的时间推进）。
  pub fn update_max_sequence_number_from_header(
    &self,
    virtual_sublog_idx: usize,
    entry: &[u8],
    log_address_sequence_number: i64,
  ) {
    let Some(header) = AofHeader::parse(entry) else {
      return;
    };
    let sequence_number = match header.header_type() {
      Some(AofHeaderType::ShardedHeader) | Some(AofHeaderType::ShardedChunkHeader) => {
        AofShardedHeader::parse(entry).map_or(0, |sh| sh.sequence_number)
      }
      _ => log_address_sequence_number,
    };
    if let Some(manager) = self.consistency_manager.read().as_ref() {
      manager.update_virtual_sublog_max_sequence_number(virtual_sublog_idx, sequence_number);
    }
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:GetBarrier
  pub fn get_barrier(&self, barrier_id: BarrierKey, participant_count: i16) -> bool {
    let mut barriers = self.leader_barriers.lock();
    let barrier = barriers
      .entry(barrier_id)
      .or_insert_with(|| LeaderBarrier::new(participant_count));
    let (is_leader, all_arrived) = barrier.try_signal_or_wait();
    // 顺序回放（单参与者）恒为 leader 且立即到齐
    is_leader && (all_arrived || barrier.participant_count <= 1)
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:TryRemoveBarrier
  pub fn try_remove_barrier(&self, barrier_id: BarrierKey) -> bool {
    self.leader_barriers.lock().remove(&barrier_id).is_some()
  }

  /// 多日志拓扑标志。
  pub fn multi_log_enabled(&self) -> bool {
    self.multi_log_enabled
  }

  /// 取出模糊区事务组（FIFO 首组）。
  pub fn dequeue_txn_group(&self, sublog_idx: usize) -> Option<TransactionGroup> {
    self.context(sublog_idx).txn_group_buffer.pop_front()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_barrier_lifecycle() {
    let coord = AofReplayCoordinator::new(1, false);
    let key = BarrierKey::new(1, 100);
    assert!(coord.get_barrier(key, 1));
    assert!(coord.try_remove_barrier(key));
    assert!(!coord.try_remove_barrier(key));
  }
}
