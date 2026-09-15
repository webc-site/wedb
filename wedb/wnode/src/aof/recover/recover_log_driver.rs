//! 恢复回放驱动（对标 libs/server/AOF/Recover/RecoverLogDriver.cs:
//! RecoverLogDriver）
//!
//! 单物理子日志的扫描-消费循环：自起始地址扫描至目标地址，逐条目做前缀
//! 一致性过滤（SkipReplay）后交处理器重放。C# 的 BulkConsumeAllAsync +
//! 页级双闸栏并行化在 rust 侧折叠为顺序消费（跨子日志仍可由调用方并行
//! 驱动多 driver）。

use wdev::Device;

use crate::aof::{
  aof_processor::{AofProcessor, AofReplayError, ReplayTarget},
  garnet_append_only_file::GarnetAppendOnlyFile,
};

/// libs/server/AOF/Recover/RecoverLogDriver.cs:RecoverLogDriver
///
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
}

impl RecoverLogDriver {
  /// libs/server/AOF/Recover/RecoverLogDriver.cs:RecoverLogDriver
  ///
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
    }
  }

  /// libs/server/AOF/Recover/RecoverLogDriver.cs:RunAsync（顺序驱动形态）
  ///
  /// 扫描 [start, until] 并逐条目消费；返回重放条目数。
  pub async fn run<D: Device>(
    &self,
    processor: &AofProcessor,
    aof: &GarnetAppendOnlyFile,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<u64, AofReplayError> {
    if self.start_address == self.until_address {
      return Ok(0);
    }
    // C# SingleLogRecover 扫描路径：scan_single_async 跨环形窗口与历史磁盘段
    //（设备面权威扫描，对标 TsavoriteLog.Scan）
    let records = aof
      .log()
      .scan_single_async(
        self.physical_sublog_idx,
        self.start_address,
        self.until_address,
      )
      .await;
    let mut replayed_record_count = 0u64;
    for record in &records {
      let entry = record.payload.as_slice();
      // 前缀一致上界：None = 续块（无独立头），照常下发处理器累积
      if let Some((true, _)) =
        processor.skip_replay(entry, self.until_sequence_number, record.address)
      {
        // 序列号单调：后续条目必超阈值（C# cts.Cancel 分支）
        break;
      }
      let virtual_sublog_idx = self.physical_sublog_idx * virtual_sublog_per_sublog(aof);
      processor
        .process_aof_record_internal(virtual_sublog_idx, entry, true, record.address, target)
        .await?;
      replayed_record_count += 1;
    }
    Ok(replayed_record_count)
  }
}

/// 单子日志的虚拟子日志数（路由换算）。
fn virtual_sublog_per_sublog(aof: &GarnetAppendOnlyFile) -> usize {
  aof.virtual_sublog_count() / aof.log().size().max(1)
}
