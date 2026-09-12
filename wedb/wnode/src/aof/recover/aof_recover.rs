//! AOF 恢复（对标 libs/server/AOF/Recover/AofRecover.cs）
//!
//! 库级恢复编排：按子日志数选择单日志 / 多日志恢复路径，统计重放条目并
//! 汇报吞吐面（C# 计数器 + 日志；rust 侧返回计数由调用方处置）。

use waof::AofAddress;
use wdev::Device;

use crate::aof::{
  aof_processor::{AofProcessor, AofReplayError, ReplayTarget},
  garnet_append_only_file::GarnetAppendOnlyFile,
  recover::recover_log_driver::RecoverLogDriver,
};

/// AOF 恢复编排器。
pub struct AofRecover;

impl AofRecover {
  /// libs/server/AOF/Recover/AofRecover.cs:RecoverReplayDriver
  ///
  /// 单库恢复驱动：装配 driver 并执行扫描-重放；`until_address` 为 -1 时
  /// 取日志尾地址。返回重放条目数。
  pub async fn recover_replay_driver<D: Device>(
    processor: &AofProcessor,
    aof: &GarnetAppendOnlyFile,
    physical_sublog_idx: usize,
    until_address: i64,
    until_sequence_number: i64,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<u64, AofReplayError> {
    let until = if until_address == -1 {
      aof.log().get_tail_address(physical_sublog_idx)
    } else {
      until_address
    };
    let begin = aof.log().get_sub_log(physical_sublog_idx).begin_address();
    let driver = RecoverLogDriver::new(physical_sublog_idx, begin, until, until_sequence_number);
    driver.run(processor, aof, target).await
  }

  /// libs/server/AOF/Recover/AofRecover.cs:SingleLogRecover
  ///
  /// 单日志恢复：顺序扫描单物理子日志（MultiLogEnabled 关闭形态）。
  pub async fn single_log_recover<D: Device>(
    processor: &AofProcessor,
    aof: &GarnetAppendOnlyFile,
    db_id: i64,
    physical_sublog_idx: usize,
    until_address: i64,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<u64, AofReplayError> {
    processor.switch_active_database_context(db_id);
    // C# SingleLogRecover 不做序列号过滤（恢复读全量）；i64::MAX 即无上界
    Self::recover_replay_driver(
      processor,
      aof,
      physical_sublog_idx,
      until_address,
      i64::MAX,
      target,
    )
    .await
  }

  /// libs/server/AOF/Recover/AofRecover.cs:MultiLogRecover
  ///
  /// 多日志恢复：逐物理子日志装配 driver（C# 并行 Task.WhenAll；rust 侧由
  /// 调用方跨 driver 并行，本入口顺序串联），累计重放条目数。
  pub async fn multi_log_recover<D: Device>(
    processor: &AofProcessor,
    aof: &GarnetAppendOnlyFile,
    db_id: i64,
    until_address: &AofAddress,
    until_sequence_number: i64,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<u64, AofReplayError> {
    processor.switch_active_database_context(db_id);
    let mut total = 0u64;
    for physical_sublog_idx in 0..aof.log().size() {
      let begin = aof.log().get_sub_log(physical_sublog_idx).begin_address();
      let driver = RecoverLogDriver::new(
        physical_sublog_idx,
        begin,
        until_address.get(physical_sublog_idx).unwrap_or(-1),
        until_sequence_number,
      );
      total += driver.run(processor, aof, target).await?;
    }
    Ok(total)
  }
}
