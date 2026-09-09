//! 恢复回放任务（对标 libs/server/AOF/Recover/RecoverReplayTask.cs）
//!
//! C# 为 RecoverLogDriver 的 partial 分片：每个并行回放任务按
//! AofProcessor.CanReplay 认领页内归属条目，页边界经双闸栏与 leader 汇合。
//! rust 侧消费折叠进 [`RecoverLogDriver`]（顺序驱动），本文件保留页重放
//! 的条目归属语义入口。

use std::sync::Arc;

use wdev::Device;

use crate::aof::{
  aof_processor::{AofProcessor, AofReplayError, ReplayTarget},
  garnet_append_only_file::GarnetAppendOnlyFile,
  recover::recover_log_driver::RecoverLogDriver,
};

impl RecoverLogDriver {
  /// libs/server/AOF/Recover/RecoverReplayTask.cs:ReplayPage
  ///
  /// 单页条目按任务归属重放：仅应用 `replay_task_idx` 认领的条目；
  /// 返回本任务本页应用的条目数。
  pub async fn replay_page<D: Device>(
    &self,
    processor: &AofProcessor,
    aof: &Arc<GarnetAppendOnlyFile>,
    page_entries: &[&[u8]],
    page_start_address: i64,
    replay_task_idx: usize,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<u64, AofReplayError> {
    let virtual_per_sublog = aof.virtual_sublog_count() / aof.log().size().max(1);
    let mut applied = 0u64;
    for (entry_idx, entry) in page_entries.iter().enumerate() {
      let entry_address = page_start_address + entry_idx as i64;
      let virtual_sublog_idx = self.physical_sublog_idx() * virtual_per_sublog + replay_task_idx;
      if processor
        .can_replay(entry, replay_task_idx, entry_address)
        .is_some_and(|(owned, _)| owned)
      {
        processor
          .process_aof_record_internal(virtual_sublog_idx, entry, true, entry_address, target)
          .await?;
        applied += 1;
      }
    }
    Ok(applied)
  }

  /// libs/server/AOF/Recover/RecoverReplayTask.cs:RecoverReplayTaskAsync
  ///
  /// 任务循环体（C# 逐页闸栏汇合 + 取消检查；rust 顺序驱动下为单页重放
  /// 委托）。返回应用条目数。
  pub async fn recover_replay_task_async<D: Device>(
    &self,
    processor: &AofProcessor,
    aof: &Arc<GarnetAppendOnlyFile>,
    page_entries: &[&[u8]],
    page_start_address: i64,
    replay_task_idx: usize,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<u64, AofReplayError> {
    self
      .replay_page(
        processor,
        aof,
        page_entries,
        page_start_address,
        replay_task_idx,
        target,
      )
      .await
  }
}
