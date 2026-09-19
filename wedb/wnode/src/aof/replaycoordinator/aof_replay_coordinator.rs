//! AOF 回放协调器（对标 libs/server/AOF/ReplayCoordinator/
//! AofReplayCoordinator.cs:AofReplayCoordinator）
//!
//! 事务组建组/提交决策、模糊区缓冲、跨子日志同步栅栏（LeaderBarrier）。
//! C# 嵌套类形态直接持有 AofProcessor 回调；rust 侧协调器只做同步决策与
//! 缓冲，重放应用（异步存储面）由 [`AofProcessor`](super::super::aof_processor::AofProcessor)
//! 持有编排权，两侧以 [`TxnAction`] 与缓冲出入口衔接。
//!
//! 栅栏键（C# BarrierKey）：负值段为 LeaderBarrierType（检查点 / FLUSH 族），
//! 正值段为会话 id；txnId 位放序列号。

use std::{future::Future, mem::take, sync::Arc, time::Duration};

use parking_lot::{Condvar, Mutex, RwLock};
use waof::{
  AofEntryType, AofHeader, AofShardedLogTransactionHeader, AofSingleLogTransactionHeader,
};
use wbase::map::{ConcurrentMap, new_concurrent_map};

use crate::aof::{
  aof_chunked_record_reader::ChunkedAccumulator,
  aof_processor::AofReplayError,
  readconsistency::read_consistency_manager::ReadConsistencyManager,
  replaycoordinator::aof_replay_context::{AofReplayContext, ReplayOperation, TransactionGroup},
};

/// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:LeaderBarrierType
///
/// 同步栅栏类别（C# LeaderBarrierType；仅负值，避免与会话 id 冲突）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum LeaderBarrierType {
  /// 检查点（C# AofProcessor.cs ProcessAofRecordInternal 检查点承接支消费）。
  Checkpoint = -1,
  /// FLUSH DB（rust FlushNs 回放支共用：同族局部清空，C# 无该形态不另造类别）。
  FlushDb = -3,
  /// FLUSH ALL
  FlushDbAll = -4,
}

/// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:BarrierKey
///
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

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:Equals
  #[inline]
  pub fn equals(&self, other: &Self) -> bool {
    self == other
  }
}

#[derive(Debug, Clone, Copy)]
struct BarrierState {
  arrived: i16,
  all_arrived: bool,
  released: bool,
}

/// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:LeaderBarrier
///
/// 跨子日志同步栅栏（C# LeaderBarrier）。
/// 协调多个并发回放任务/子日志线程，首个到场者担任 Leader，后续参与者等待 Leader 执行完同步操作后统一放行。
pub struct LeaderBarrier {
  /// 参与者数。
  pub participant_count: i16,
  state: Mutex<BarrierState>,
  release_first: Condvar,
  release_all: Condvar,
}

impl LeaderBarrier {
  /// 新建栅栏。
  pub fn new(participant_count: i16) -> Self {
    Self {
      participant_count,
      state: Mutex::new(BarrierState {
        arrived: 0,
        all_arrived: participant_count <= 1,
        released: false,
      }),
      release_first: Condvar::new(),
      release_all: Condvar::new(),
    }
  }

  /// 尝试签到并等待（对标 C# TrySignalOrWait）。
  /// 返回 Ok(true) 表示当前为 Leader（首个到场者，已等待全员到齐）；
  /// 返回 Ok(false) 表示当前为 Follower（已等待 Leader 释放）；
  /// 超时返回 Err。
  pub fn try_signal_or_wait(&self, timeout: Option<Duration>) -> Result<bool, AofReplayError> {
    if self.participant_count <= 1 {
      return Ok(true);
    }

    let mut state = self.state.lock();
    state.arrived += 1;
    let is_first = state.arrived == 1;

    if is_first {
      while !state.all_arrived && !state.released {
        if let Some(to) = timeout {
          let res = self.release_first.wait_for(&mut state, to);
          if res.timed_out() && !state.all_arrived && !state.released {
            return Err("LeaderBarrier 等待参与者到齐超时".into());
          }
        } else {
          self.release_first.wait(&mut state);
        }
      }
      Ok(true)
    } else {
      if state.arrived >= self.participant_count {
        state.all_arrived = true;
        self.release_first.notify_one();
      }
      while !state.released {
        if let Some(to) = timeout {
          let res = self.release_all.wait_for(&mut state, to);
          if res.timed_out() && !state.released {
            return Err("LeaderBarrier 等待 Leader 释放超时".into());
          }
        } else {
          self.release_all.wait(&mut state);
        }
      }
      Ok(false)
    }
  }

  /// 释放全部等待的参与者（对标 C# Release）。
  pub fn release(&self) {
    let mut state = self.state.lock();
    state.released = true;
    self.release_all.notify_all();
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

/// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:AofReplayCoordinator
///
/// AOF 回放协调器。
pub struct AofReplayCoordinator {
  /// 每虚拟子日志的回放缓冲（C# 按子日志线程私有；rust 以锁承接 &self 变更）。
  contexts: Vec<Mutex<AofReplayContext>>,
  /// 多日志拓扑（事务/栅栏同步仅在多日志模式启用）。
  multi_log_enabled: bool,
  /// 进行中的同步栅栏（C# leaderBarriers，基于 papaya 无锁并发字典）。
  leader_barriers: ConcurrentMap<BarrierKey, Arc<LeaderBarrier>>,
  /// 一致性管理器挂载点（处理器构造时注入；序列号推进用）。
  consistency_manager: RwLock<Option<Arc<ReadConsistencyManager>>>,
}

/// 上下文锁守卫别名（跨子日志缓冲的互斥访问面）。
pub type ContextGuard<'a> = parking_lot::MutexGuard<'a, AofReplayContext>;

/// 栅栏签到凭证（C# ProcessSynchronizedOperation 的 leaderBarrier + finally
/// 清理段）：Leader 到场即持有，drop 时移除栅栏并放行全部等待参与者；
/// Follower 的 drop 无操作（其放行已由 Leader release 完成）。
struct BarrierJoin<'a> {
  coord: &'a AofReplayCoordinator,
  key: BarrierKey,
  barrier: Arc<LeaderBarrier>,
  /// 本任务是否为 Leader（首个到场者）。
  is_leader: bool,
}

impl Drop for BarrierJoin<'_> {
  fn drop(&mut self) {
    if self.is_leader {
      self.coord.try_remove_barrier(self.key);
      self.barrier.release();
    }
  }
}

impl AofReplayCoordinator {
  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:AofReplayCoordinator
  ///
  /// 构造：按虚拟子日志数初始化回放缓冲（C# InitializeReplayContext）。
  pub fn new(virtual_sublog_count: usize, multi_log_enabled: bool) -> Self {
    Self {
      contexts: Self::initialize_replay_context(virtual_sublog_count),
      multi_log_enabled,
      leader_barriers: new_concurrent_map(),
      consistency_manager: RwLock::new(None),
    }
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:InitializeReplayContext
  pub fn initialize_replay_context(virtual_sublog_count: usize) -> Vec<Mutex<AofReplayContext>> {
    (0..virtual_sublog_count.max(1))
      .map(|_| Mutex::new(AofReplayContext::default()))
      .collect()
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:Dispose
  pub fn dispose(&self) {
    for ctx in &self.contexts {
      let mut guard = ctx.lock();
      guard.fuzzy_region_ops.clear();
      guard.active_txns.clear();
      guard.txn_group_buffer.clear();
    }
    self.leader_barriers.pin().clear();
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
  ///
  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:FuzzyRegionBufferCount
  pub fn fuzzy_region_buffer_count(&self, sublog_idx: usize) -> usize {
    self.context(sublog_idx).fuzzy_region_buffer_count()
  }

  /// 清空模糊区缓冲（C# ClearFuzzyRegionBuffer）。
  ///
  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:ClearFuzzyRegionBuffer
  pub fn clear_fuzzy_region_buffer(&self, sublog_idx: usize) {
    self.context(sublog_idx).clear_fuzzy_region_buffer();
  }

  /// 入模糊区缓冲（C# AddFuzzyRegionOperation；原始记录与分块累积器两态）。
  ///
  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:AddFuzzyRegionOperation
  pub fn add_fuzzy_region_operation(&self, sublog_idx: usize, operation: ReplayOperation) {
    self.context(sublog_idx).fuzzy_region_ops.push(operation);
  }

  /// 取走全部模糊区操作（重放后丢弃）。
  pub fn take_fuzzy_region_operations(&self, sublog_idx: usize) -> Vec<ReplayOperation> {
    let mut context = self.context(sublog_idx);
    take(&mut context.fuzzy_region_ops)
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:ClearSessionTxn
  ///
  /// 清理指定会话的活动事务组。
  pub fn clear_session_txn(&self, virtual_sublog_idx: usize, session_id: i32) {
    let mut ctx = self.context(virtual_sublog_idx);
    ctx.active_txns.remove(&session_id);
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
        AofEntryType::TxnStart => TxnAction::Handled,
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
            if let Some(group) = group {
              ctx.add_to_fuzzy_region_buffer(group, entry.to_vec());
            }
            TxnAction::Handled
          } else if let Some(group) = group {
            TxnAction::Commit { group }
          } else {
            TxnAction::Handled
          }
        }
        AofEntryType::StoredProcedure => TxnAction::Handled,
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
          let participant_count = self
            .txn_header_participant_count(entry)
            .map(|c| c as u8)
            .unwrap_or(0);
          let start_sequence_number =
            AofHeader::sequence_number_of(entry, log_address_sequence_number)
              .unwrap_or(log_address_sequence_number);
          ctx.add_transaction_group(
            session_id,
            virtual_sublog_idx,
            participant_count,
            start_sequence_number,
          );
          TxnAction::Handled
        }
        AofEntryType::TxnAbort | AofEntryType::TxnCommit => {
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
    AofShardedLogTransactionHeader::parse(entry).map(|h| h.participant_count)
  }

  /// 分块记录入组（若会话处于事务中则入组返回 None，否则返回原累积器 Some(acc)）。
  pub fn try_buffer_chunk_accumulator(
    &self,
    virtual_sublog_idx: usize,
    acc: ChunkedAccumulator,
  ) -> Option<ChunkedAccumulator> {
    let mut ctx = self.context(virtual_sublog_idx);
    if let Some(group) = ctx.active_txns.get_mut(&acc.session_id) {
      group.operations.push(ReplayOperation::Chunk(Box::new(acc)));
      None
    } else {
      Some(acc)
    }
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
    if AofHeader::parse(entry).is_none() {
      return;
    }
    let sequence_number =
      AofHeader::sequence_number_of(entry, log_address_sequence_number).unwrap_or(0);
    if let Some(manager) = self.consistency_manager.read().as_ref() {
      manager.update_virtual_sublog_max_sequence_number(virtual_sublog_idx, sequence_number);
    }
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:GetBarrier
  pub fn get_barrier(&self, barrier_id: BarrierKey, participant_count: i16) -> Arc<LeaderBarrier> {
    let pin = self.leader_barriers.pin();
    Arc::clone(pin.get_or_insert_with(barrier_id, || {
      Arc::new(LeaderBarrier::new(participant_count))
    }))
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:TryRemoveBarrier
  pub fn try_remove_barrier(&self, barrier_id: BarrierKey) -> bool {
    self.leader_barriers.pin().remove(&barrier_id).is_some()
  }

  /// libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:ProcessSynchronizedOperation
  ///
  /// 统一跨子日志同步操作处理（对标 C# ProcessSynchronizedOperation）：
  /// 1. 获取/注册 BarrierKey 对应的 LeaderBarrier；
  /// 2. 参与者到场等待，首个到达者为 Leader；
  /// 3. Leader 执行同步闭包（若提供）；
  /// 4. Leader 在 Drop/finally 中移除栅栏并唤醒全部等待参与者；
  /// 5. 推进当前子日志的虚拟最大序列号（UpdateVirtualSublogMaxSequenceNumber）。
  pub fn process_synchronized_operation<F, R>(
    &self,
    sublog_idx: usize,
    sequence_number: i64,
    participant_count: i16,
    barrier_id: i32,
    operation: Option<F>,
  ) -> Result<Option<R>, AofReplayError>
  where
    F: FnOnce() -> Result<R, AofReplayError>,
  {
    if !self.multi_log_enabled {
      let res = operation.map(|op| op()).transpose()?;
      self.advance_virtual_sublog_max_sequence_number(sublog_idx, sequence_number);
      return Ok(res);
    }

    let join = self.join_barrier(barrier_id, sequence_number, participant_count)?;
    let op_result = if join.is_leader {
      operation.map(|op| op()).transpose()?
    } else {
      None
    };
    // Leader 清理（移除栅栏 + 放行全员）随 BarrierJoin drop 完成；操作失败经
    // `?` 提前返回时同样触发清理、跳过推进（C# finally 后先抛不进尾步同构）
    drop(join);
    self.advance_virtual_sublog_max_sequence_number(sublog_idx, sequence_number);
    Ok(op_result)
  }

  /// 同步操作入口的异步操作形态（与 [`Self::process_synchronized_operation`]
  /// 共享 join_barrier / BarrierJoin / 推进单源编排，非第二套栅栏实现）：
  /// C# Leader 独占段为 BlockingWait 同步执行，rust 存储面异步（FLUSH 族清空），
  /// Leader 在栅栏独占期内 await 操作完成后再清栏放行；全员对齐、Leader 独占、
  /// finally 清栏、尾部序列号推进语义与同步入口全同。
  pub async fn process_synchronized_operation_async<F, Fut, R>(
    &self,
    sublog_idx: usize,
    sequence_number: i64,
    participant_count: i16,
    barrier_id: i32,
    operation: Option<F>,
  ) -> Result<Option<R>, AofReplayError>
  where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<R, AofReplayError>>,
  {
    if !self.multi_log_enabled {
      let res = match operation {
        Some(op) => Some(op().await?),
        None => None,
      };
      self.advance_virtual_sublog_max_sequence_number(sublog_idx, sequence_number);
      return Ok(res);
    }

    let join = self.join_barrier(barrier_id, sequence_number, participant_count)?;
    let op_result = if join.is_leader {
      match operation {
        Some(op) => Some(op().await?),
        None => None,
      }
    } else {
      None
    };
    drop(join);
    self.advance_virtual_sublog_max_sequence_number(sublog_idx, sequence_number);
    Ok(op_result)
  }

  /// 签到并等待全员对齐（C# ProcessSynchronizedOperation 的 GetBarrier +
  /// TrySignalOrWait 段）；返回的 BarrierJoin 承接 Leader 清理凭证。
  fn join_barrier(
    &self,
    barrier_id: i32,
    sequence_number: i64,
    participant_count: i16,
  ) -> Result<BarrierJoin<'_>, AofReplayError> {
    let key = BarrierKey::new(barrier_id, sequence_number);
    let barrier = self.get_barrier(key, participant_count);
    let is_leader = barrier.try_signal_or_wait(None)?;
    Ok(BarrierJoin {
      coord: self,
      key,
      barrier,
      is_leader,
    })
  }

  /// 虚拟子日志最大序列号推进（C# ProcessSynchronizedOperation 尾步
  /// UpdateVirtualSublogMaxSequenceNumber）。
  fn advance_virtual_sublog_max_sequence_number(&self, sublog_idx: usize, sequence_number: i64) {
    if let Some(manager) = self.consistency_manager.read().as_ref() {
      manager.update_virtual_sublog_max_sequence_number(sublog_idx, sequence_number);
    }
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
  use std::thread;

  use super::*;

  #[test]
  fn test_barrier_lifecycle() {
    let coord = AofReplayCoordinator::new(1, false);
    let key = BarrierKey::new(1, 100);
    let barrier = coord.get_barrier(key, 1);
    assert_eq!(barrier.participant_count, 1);
    assert!(coord.try_remove_barrier(key));
    assert!(!coord.try_remove_barrier(key));
  }

  #[test]
  fn test_multi_participant_barrier() {
    let coord = Arc::new(AofReplayCoordinator::new(2, true));
    let sequence_number = 100;
    let participant_count = 2;
    let barrier_id = 42;

    // 首个到场者为 Leader（对标 C# TrySignalOrWait），到场顺序由调度决定，
    // 断言恰有一侧独占执行同步操作，另一侧以 Follower 身份放行。
    let coord_clone = Arc::clone(&coord);
    let handle = thread::spawn(move || {
      coord_clone.process_synchronized_operation(
        1,
        sequence_number,
        participant_count,
        barrier_id,
        Some(|| Ok::<i32, AofReplayError>(999)),
      )
    });

    let res = coord
      .process_synchronized_operation(
        0,
        sequence_number,
        participant_count,
        barrier_id,
        Some(|| Ok::<i32, AofReplayError>(999)),
      )
      .unwrap();
    let other = handle.join().unwrap().unwrap();

    assert_eq!(res.is_some(), other.is_none());
    assert_eq!(res.or(other), Some(999));
  }
}
