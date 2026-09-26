//! 恢复并行回放单页内核（对标 libs/server/AOF/Recover/RecoverReplayTask.cs:
//! ReplayPage）：利用 `can_replay` 哈希分片仅重放属于自身的 AOF 条目；检测到
//! 超出前缀一致性序列号时置位边界标记并平稳收尾。Worker 主循环（双闸栏会合、
//! 取消轮询、错误槽落账）由 recover_log_driver 驱动循环单点承接。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use wdev::Device;

use super::recover_log_driver::{ReplayBatchContext, log_replay_progress};
use crate::aof::{
  aof_processor::{AofProcessor, AofReplayError, ReplayTarget},
  garnet_append_only_file::GarnetAppendOnlyFile,
  record_gate,
};

/// 单页回放参数容器。
pub struct ReplayPageArgs<'a, D: Device> {
  pub replay_task_idx: usize,
  pub virtual_sublog_idx: usize,
  pub until_sequence_number: i64,
  /// 副本回放身份位，由驱动装配点透传（单机崩溃恢复恒 false）。
  pub as_replica: bool,
  pub processor: &'a AofProcessor,
  pub aof: &'a GarnetAppendOnlyFile,
  pub target: &'a ReplayTarget<'a, 'a, D>,
  pub batch_context: &'a ReplayBatchContext,
  pub prefix_consistency_boundary_reached: &'a AtomicBool,
  pub replayed_record_count: &'a AtomicU64,
}

/// libs/server/AOF/Recover/RecoverReplayTask.cs:ReplayPage
///
/// 单页/单批次条目扫描与哈希分片重放。
pub async fn replay_page<D: Device>(args: ReplayPageArgs<'_, D>) -> Result<(), AofReplayError> {
  // 克隆 Arc 句柄立即释放锁，避免持有 MutexGuard 跨 await 点
  let records = args.batch_context.records.read().clone();
  let mut max_sequence_number = 0i64;

  for record in records.iter() {
    let entry = record.payload.as_slice();
    let log_address = record.address as i64;

    if waof::is_commit_frame(entry) {
      continue;
    }

    // 归属判定失败（未知头型 / 头损坏）即中止恢复，不当作「不归本任务」跳过
    let (owned, seq_num) =
      record_gate::can_replay(args.aof, entry, args.replay_task_idx, log_address)?;
    if !owned {
      continue;
    }

    if args.until_sequence_number != -1 && seq_num > args.until_sequence_number {
      // 序列号单调递增：置位边界标志并终止本批次，允许其他任务完成其配额
      args
        .prefix_consistency_boundary_reached
        .store(true, Ordering::Release);
      break;
    }

    args
      .processor
      .process_aof_record_internal(
        args.virtual_sublog_idx,
        entry,
        args.as_replica,
        log_address,
        args.target,
      )
      .await?;
    max_sequence_number = max_sequence_number.max(seq_num);
    let count = args.replayed_record_count.fetch_add(1, Ordering::Relaxed) + 1;
    log_replay_progress(count, log_address);
  }

  // 推进当前虚拟子日志的最大序列号快照
  if let Some(rcm) = args.aof.read_consistency_manager() {
    rcm.update_virtual_sublog_max_sequence_number(args.virtual_sublog_idx, max_sequence_number);
  }

  Ok(())
}
