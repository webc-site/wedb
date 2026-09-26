//! AOF 恢复（对标 libs/server/AOF/Recover/AofRecover.cs）
//!
//! 库级恢复编排：单日志恢复（MultiLogEnabled 关闭形态）与多物理日志恢复
//! 双路径。多物理日志恢复上界自各子日志 commit 元数据帧收敛（WaofSublog
//! cookie 随 commit 帧跨重启持久，见 waof/src/wal/commit.rs），
//! [`AofRecover::multi_log_recover`] 由生产恢复重放入口按 multi_log_enabled
//! 分派点亮。

use waof::AofAddress;
use wdev::Device;

use crate::aof::{
  aof_processor::{AofProcessor, AofReplayError, ReplayTarget},
  garnet_append_only_file::GarnetAppendOnlyFile,
  recover::recover_log_driver::RecoverLogDriver,
};

/// libs/server/AOF/Recover/AofRecover.cs:Recover
///
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
    // 单机崩溃恢复契约：as_replica 恒 false（对标 C# AofRecover.cs:103
    // ProcessAofRecordInternal(asReplica: false)），未决快照增量经版本闸
    // 退化直写；副本回放由 wedb/src/server/replication/replica_replay_driver 承接
    let driver = RecoverLogDriver::new(
      physical_sublog_idx,
      begin,
      until,
      until_sequence_number,
      false,
    );
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
  /// 多物理日志恢复：恢复上界收敛自各物理子日志 commit 元数据帧恢复出的
  /// cookie 序列号（RecoverLatestSequenceNumber；任一子日志无 cookie 即按
  /// C# 同款保守语义零重放），随后逐物理子日志装配 RecoverLogDriver 以
  /// 序列号前缀一致上界并行重放（rust 单线程执行器下顺序驱动，跨子日志
  /// 消费面天然隔离）。
  pub async fn multi_log_recover<D: Device>(
    processor: &AofProcessor,
    aof: &GarnetAppendOnlyFile,
    db_id: i64,
    until_address: &AofAddress,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<u64, AofReplayError> {
    processor.switch_active_database_context(db_id);
    // C# RecoverLatestSequenceNumber：收敛前缀一致序列号上界，缺失即不重放
    let Some(until_sequence_number) = aof.log().recover_latest_sequence_number() else {
      log::warn!("MultiLogRecover 子日志提交 cookie 不齐备，恢复上界无从收敛，跳过重放");
      return Ok(0);
    };

    let begin = aof.log().begin_address();
    let mut replayed = 0u64;
    for physical_sublog_idx in 0..aof.log().size() {
      // 多物理日志恢复同为单机崩溃恢复臂：as_replica 恒 false（契约同上）
      let until = match until_address.get(physical_sublog_idx) {
        Some(addr) if addr >= 0 => addr,
        _ => aof.log().get_tail_address(physical_sublog_idx),
      };
      let driver = RecoverLogDriver::new(
        physical_sublog_idx,
        begin.get(physical_sublog_idx).unwrap_or(0),
        until,
        until_sequence_number,
        false,
      );
      replayed += driver.run(processor, aof, target).await?;
    }
    Ok(replayed)
  }
}
