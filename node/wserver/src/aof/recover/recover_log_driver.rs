//! 恢复回放驱动（对标 libs/server/AOF/Recover/RecoverLogDriver.cs:
//! RecoverLogDriver）
//!
//! 单物理子日志的扫描-消费循环：自起始地址扫描至目标地址，逐条目做前缀
//! 一致性过滤（SkipReplay）与任务归属判定（CanReplay），命中即交处理器
//! 重放。C# 的 BulkConsumeAllAsync + 页级双闸栏并行化在 rust 侧折叠为
//! 顺序消费（跨子日志仍可由调用方并行驱动多 driver；条目级归属判定
//! CanReplay 保留多回放任务语义）。

use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use wdev::Device;

use crate::aof::{
  aof_processor::{AofProcessor, AofReplayError, ReplayTarget},
  garnet_append_only_file::GarnetAppendOnlyFile,
};

/// 恢复回放驱动。
pub struct RecoverLogDriver {
  /// 物理子日志下标。
  physical_sublog_idx: usize,
  /// 起始地址。
  start_address: i64,
  /// 目标地址（含）。
  until_address: i64,
  /// 前缀一致序列号上界（-1 = 不限）。
  until_sequence_number: i64,
  /// 已重放条目数（C# ReplayedRecordCount）。
  replayed_record_count: AtomicU64,
}

impl RecoverLogDriver {
  /// 物理子日志下标。
  pub fn physical_sublog_idx(&self) -> usize {
    self.physical_sublog_idx
  }

  /// 构造（C# 主构造的参数面）。
  pub fn new(
    physical_sublog_idx: usize,
    start_address: i64,
    until_address: i64,
    until_sequence_number: i64,
  ) -> Self {
    Self {
      physical_sublog_idx,
      start_address,
      until_address,
      until_sequence_number,
      replayed_record_count: AtomicU64::new(0),
    }
  }

  /// 已重放条目数。
  pub fn replayed_record_count(&self) -> u64 {
    self.replayed_record_count.load(Ordering::Acquire)
  }

  /// libs/server/AOF/Recover/RecoverLogDriver.cs:Throttle
  ///
  /// 消费节流（C# 空实现：恢复期不限速）。
  pub fn throttle(&self) {}

  /// libs/server/AOF/Recover/RecoverLogDriver.cs:RunAsync（顺序驱动形态）
  ///
  /// 扫描 [start, until] 并逐条目消费；返回重放条目数。
  pub async fn run<D: Device>(
    &self,
    processor: &AofProcessor,
    aof: &Arc<GarnetAppendOnlyFile>,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<u64, AofReplayError> {
    if self.start_address == self.until_address {
      return Ok(0);
    }
    // C# SingleLogRecover 扫描路径（scan_single 承接设备面）
    let records = aof.log().scan_single(
      self.physical_sublog_idx,
      self.start_address,
      self.until_address,
    );
    for record in &records {
      let entry = record.payload.as_slice();
      // 前缀一致上界：None = 续块（无独立头），照常下发处理器累积
      if let Some((true, _sequence_number)) =
        processor.skip_replay(entry, self.until_sequence_number, record.address)
      {
        // 序列号单调：后续条目必超阈值（C# cts.Cancel 分支）
        break;
      }
      let virtual_sublog_idx = self.physical_sublog_idx * self.virtual_sublog_per_sublog(aof);
      processor
        .process_aof_record_internal(virtual_sublog_idx, entry, true, record.address, target)
        .await?;
      self.replayed_record_count.fetch_add(1, Ordering::AcqRel);
    }
    Ok(self.replayed_record_count.load(Ordering::Acquire))
  }

  /// 单子日志的虚拟子日志数（路由换算）。
  fn virtual_sublog_per_sublog(&self, aof: &Arc<GarnetAppendOnlyFile>) -> usize {
    aof.virtual_sublog_count() / aof.log().size().max(1)
  }

  /// libs/server/AOF/Recover/RecoverLogDriver.cs:Consume
  ///
  /// 单条目消费（C# IBulkLogEntryConsumer 形态）：多回放任务模式下按
  /// CanReplay 归属判定过滤后重放。
  pub async fn consume<D: Device>(
    &self,
    processor: &AofProcessor,
    aof: &Arc<GarnetAppendOnlyFile>,
    entry: &[u8],
    current_address: i64,
    replay_task_idx: usize,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let virtual_sublog_idx =
      self.physical_sublog_idx * self.virtual_sublog_per_sublog(aof) + replay_task_idx;
    processor
      .process_aof_record_internal(virtual_sublog_idx, entry, true, current_address, target)
      .await?;
    self.replayed_record_count.fetch_add(1, Ordering::AcqRel);
    Ok(())
  }

  /// libs/server/AOF/Recover/RecoverLogDriver.cs:CreateAndRunIntraPageParallelReplayTasks
  ///
  /// 页内并行回放任务（C# Task.Run × AofReplayTaskCount + 双闸栏）；rust 侧
  /// 消费为顺序驱动（调用方按条目归属 CanReplay 复用判定），此入口保留
  /// 归属分派语义：对页内全部条目按任务下标过滤重放。
  #[allow(clippy::too_many_arguments)]
  pub async fn create_and_run_intra_page_parallel_replay_tasks<D: Device>(
    &self,
    processor: &AofProcessor,
    aof: &Arc<GarnetAppendOnlyFile>,
    page_entries: &[&[u8]],
    page_start_address: i64,
    entry_stride: usize,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let replay_task_count = aof.virtual_sublog_count() / aof.log().size().max(1);
    for (entry_idx, entry) in page_entries.iter().enumerate() {
      let entry_address = page_start_address + (entry_idx * entry_stride) as i64;
      let virtual_sublog_idx = self.physical_sublog_idx * self.virtual_sublog_per_sublog(aof)
        + entry_idx % replay_task_count.max(1);
      if processor
        .can_replay(entry, virtual_sublog_idx, entry_address)
        .is_some_and(|(owned, _)| owned)
      {
        processor
          .process_aof_record_internal(virtual_sublog_idx, entry, true, entry_address, target)
          .await?;
        self.replayed_record_count.fetch_add(1, Ordering::AcqRel);
      }
    }
    Ok(())
  }
}
