//! Garnet 追加日志文件（对标 libs/server/AOF/GarnetAppendOnlyFile.cs:
//! GarnetAppendOnlyFile）
//!
//! C# 侧聚合 TsavoriteLog 拓扑（GarnetLog）、序列号生成器、读取一致性管理器
//! 与背压闸门；rust 侧背压与路由已由周期 1 的 [`GarnetLog`] 承载，本类型承接
//! 总尺寸 / 虚拟子日志换算 / 序列号生成与复位 / 一致性管理器代际更替 /
//! 副本同步重放地址决策与数据丢失检查。

use std::{io, sync::Arc};

use parking_lot::RwLock;
use waof::AofAddress;

use super::{
  AofProcessor, aof_backpressure::AofBackpressure, aof_processor::ReplayTarget,
  garnet_log::GarnetLog, readconsistency::read_consistency_manager::ReadConsistencyManager,
  recover::aof_recover::AofRecover, sequence_number_generator::SequenceNumberGenerator,
};
use crate::{
  config::runtime_server_options::RuntimeServerOptions,
  databases::{DatabaseAof, GarnetDatabase},
  storage::StorageSession,
};

/// 首个有效 AOF 地址（C# kFirstValidAofAddress = 64；对齐 TsavoriteLog 页头）。
pub const FIRST_VALID_AOF_ADDRESS: i64 = 64;

/// Garnet 追加日志文件。
pub struct GarnetAppendOnlyFile {
  /// 日志拓扑（单日志 / 分片路由层）。
  log: Arc<GarnetLog>,
  /// 服务器选项子集。
  physical_sublog_count: usize,
  replay_task_count: usize,
  /// 多物理日志或单物理多回放拓扑（C# MultiLogEnabled）。
  multi_log_enabled: bool,
  fast_aof_truncate: bool,
  /// 序列号生成器（仅多物理日志模式；C# seqNumGen，与 GarnetLog 共享）。
  seq_num_gen: Option<Arc<SequenceNumberGenerator>>,
  /// 读取一致性管理器（代际更替；C# readConsistencyManager）。
  read_consistency_manager: RwLock<Option<Arc<ReadConsistencyManager>>>,
}

/// 副本同步重放地址决策参数
#[derive(Debug, Clone, Copy)]
pub struct AofSyncReplayOptions<'a> {
  pub recover_from_remote: bool,
  pub same_main_store_checkpoint_history: bool,
  pub same_history2: bool,
  pub replication_offset2: &'a AofAddress,
  pub replica_aof_begin_address: &'a AofAddress,
  pub replica_aof_tail_address: &'a AofAddress,
  pub begin_address: &'a AofAddress,
}

impl GarnetAppendOnlyFile {
  /// 构造：绑定日志拓扑并按选项装配一致性管理器（C# 构造子的装配子集；
  /// 设备面日志设置由 GarnetLog 的后端注入承接）。
  pub fn new(
    log: Arc<GarnetLog>,
    server_options: &RuntimeServerOptions,
    seq_num_gen: Option<Arc<SequenceNumberGenerator>>,
  ) -> Self {
    let physical_sublog_count = server_options.aof_physical_sublog_count.max(1) as usize;
    let replay_task_count = server_options.aof_replay_task_count.max(1) as usize;
    // C# MultiLogEnabled = physical > 1 || replay > 1
    let multi_log_enabled = physical_sublog_count > 1 || replay_task_count > 1;
    let fast_aof_truncate = server_options.fast_aof_truncate;
    let aof = Self {
      log: Arc::clone(&log),
      physical_sublog_count,
      replay_task_count,
      multi_log_enabled,
      fast_aof_truncate,
      // 仅多物理日志模式持有（C# 构造同款条件），并与 GarnetLog 共享
      seq_num_gen: if physical_sublog_count > 1 {
        seq_num_gen
      } else {
        None
      },
      read_consistency_manager: RwLock::new(None),
    };
    if let Some(bp) = aof.log.backpressure() {
      bp.set_weak_log(Arc::downgrade(&aof.log));
    }
    // C# 构造即建一致性管理器（CreateOrUpdateKeySequenceManager）
    aof.create_or_update_key_sequence_manager();
    aof
  }

  /// 日志拓扑句柄。
  pub fn log(&self) -> &Arc<GarnetLog> {
    &self.log
  }

  /// 主侧复制背压闸门（C# backpressure）。
  pub fn backpressure(&self) -> Option<&Arc<AofBackpressure>> {
    self.log.backpressure()
  }

  /// 页头开销字节数（C# HeaderSize）。
  pub fn header_size(&self) -> i64 {
    self.log.header_size()
  }

  /// 读取一致性管理器快照。
  pub fn read_consistency_manager(&self) -> Option<Arc<ReadConsistencyManager>> {
    self.read_consistency_manager.read().clone()
  }

  /// 读锁借用读取一致性管理器，执行闭包并返回值（零 Arc 克隆）
  #[inline]
  pub fn with_read_consistency_manager<R>(
    &self,
    f: impl FnOnce(&ReadConsistencyManager) -> R,
  ) -> Option<R> {
    self.read_consistency_manager.read().as_deref().map(f)
  }

  /// 多物理日志模式是否启用（C# serverOptions.MultiLogEnabled）。
  pub fn multi_log_enabled(&self) -> bool {
    self.multi_log_enabled
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:TotalSize
  ///
  /// 全拓扑 begin..tail 字节总量。
  pub fn total_size(&self) -> i64 {
    self
      .log
      .tail_address()
      .aggregate_diff(&self.log.begin_address())
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:GetVirtualSublogIdx
  ///
  /// 物理子日志 × 回放任务 → 虚拟子日志下标。
  pub fn get_virtual_sublog_idx(&self, sublog_idx: usize, replay_idx: usize) -> usize {
    sublog_idx * self.replay_task_count + replay_idx
  }

  /// 虚拟子日志总数（C# serverOptions.AofVirtualSublogCount）。
  pub fn virtual_sublog_count(&self) -> usize {
    self.physical_sublog_count * self.replay_task_count
  }

  /// 无效地址向量（C# InvalidAofAddress：全槽 -1）。
  pub fn invalid_aof_address(&self) -> AofAddress {
    AofAddress::create(self.physical_sublog_count as i32, -1)
  }

  /// 最大地址向量（C# MaxAofAddress：全槽 i64::MAX）。
  pub fn max_aof_address(&self) -> AofAddress {
    AofAddress::create(self.physical_sublog_count as i32, i64::MAX)
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:GetLargerThanMaximumSequenceNumber
  ///
  /// 严格大于已观测尾部全部序列号的新序列号（调用方须先读尾地址）。
  ///（C# GetLargerThanMaximumSequenceNumber = GetSequenceNumber() + 1）
  pub fn get_larger_than_maximum_sequence_number(&self) -> i64 {
    self.get_sequence_number() + 1
  }

  /// 取序列号（C# SequenceNumberGenerator.GetSequenceNumber；仅多物理
  /// 日志模式构造，单物理日志模式恒 0——排序由日志地址承担）。
  pub fn get_sequence_number(&self) -> i64 {
    self
      .seq_num_gen
      .as_ref()
      .map_or(0, |g| g.get_sequence_number())
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:CreateOrUpdateKeySequenceManager
  ///
  /// 代际更替一致性管理器（版本 = 前代 + 1）；旧管理器栅栏禁用放行。
  pub fn create_or_update_key_sequence_manager(&self) {
    if !self.multi_log_enabled {
      // C# MultiLogEnabled 未启用时不建管理器
      return;
    }
    let previous = self.read_consistency_manager();
    let current_version = previous.as_ref().map_or(0, |m| m.current_version());
    let fresh = Arc::new(ReadConsistencyManager::new(
      current_version + 1,
      self.physical_sublog_count,
      self.replay_task_count,
      // rust 选项域未含漂移参数（缺口见汇报）：默认关闭主动扫描
      -1,
      0,
    ));
    if let Some(m) = previous {
      m.replay_barrier.disable();
    }
    *self.read_consistency_manager.write() = Some(fresh);
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:ResetSequenceNumberGenerator
  ///
  /// 恢复/故障转移后按已回放最大序列号抬升生成器起点（仅多物理日志模式；
  /// 时间前进保证后续取号 > 起点）。
  pub fn reset_sequence_number_generator(&self) {
    if self.physical_sublog_count <= 1 {
      return;
    }
    let Some(manager) = self.read_consistency_manager() else {
      return;
    };
    let Some(seq_num_gen) = &self.seq_num_gen else {
      return;
    };
    let start = manager
      .get_physical_sublog_max_replayed_sequence_number()
      .into_iter()
      .max()
      .unwrap_or(0);
    seq_num_gen.set_starting_offset(start);
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:ComputeAofSyncReplayAddress
  ///
  /// 副本同步重放地址决策：逐子日志判定是否需重放（位图置位）并收敛
  /// 重放上界。返回（重放子日志位图, 各子日志重放起点向量）。
  pub fn compute_aof_sync_replay_address(
    &self,
    opts: &AofSyncReplayOptions<'_>,
    checkpoint_aof_begin_address: &mut AofAddress,
  ) -> Result<(u64, AofAddress), String> {
    let mut replay_aof_map = 0u64;
    for sublog_idx in 0..self.physical_sublog_count {
      self.compute_aof_sublog_sync_replay_address(
        sublog_idx,
        &mut replay_aof_map,
        opts,
        checkpoint_aof_begin_address,
      )?;
    }
    Ok((replay_aof_map, *checkpoint_aof_begin_address))
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:ComputeAofSubloSyncReplayAddress（内层决策）
  fn compute_aof_sublog_sync_replay_address(
    &self,
    sublog_idx: usize,
    replay_aof_map: &mut u64,
    opts: &AofSyncReplayOptions<'_>,
    checkpoint_aof_begin_address: &mut AofAddress,
  ) -> Result<(), String> {
    if opts.recover_from_remote {
      return Ok(());
    }
    let replica_begin = opts.replica_aof_begin_address.get(sublog_idx).unwrap_or(0);
    let checkpoint_begin = checkpoint_aof_begin_address.get(sublog_idx).unwrap_or(0);
    if replica_begin > FIRST_VALID_AOF_ADDRESS && replica_begin > checkpoint_begin {
      // 副本起点越过主侧恢复偏移：远端 AOF 不可用（C# LogInformation 分支）
      return Ok(());
    }

    let replica_tail = opts.replica_aof_tail_address.get(sublog_idx).unwrap_or(0);
    let committed = self
      .log
      .committed_until_address()
      .get(sublog_idx)
      .unwrap_or(0);
    if replica_tail < checkpoint_begin && !self.fast_aof_truncate() {
      return Err(format!(
        "ReplicaSyncSession replicaAofTail {replica_tail} < canServeFromAofAddress {checkpoint_begin}"
      ));
    }

    // 重放上界 = min(副本尾, 主侧提交水位)
    let mut replay_until_address = replica_tail.min(committed);
    // 仅重放检查点未覆盖的记录
    if replay_until_address > checkpoint_begin {
      *replay_aof_map |= 1u64 << sublog_idx;
      // 连接副本源自旧主时按旧复制偏移收敛，避免重放分歧历史
      if opts.same_history2
        && replay_until_address > opts.replication_offset2.get(sublog_idx).unwrap_or(0)
      {
        replay_until_address = opts.replication_offset2.get(sublog_idx).unwrap_or(0);
      }
      checkpoint_aof_begin_address.set(sublog_idx, replay_until_address);
    }

    if !opts.same_main_store_checkpoint_history {
      // 检查点代际不同：自主侧起点起全量流式
      checkpoint_aof_begin_address.set(sublog_idx, opts.begin_address.get(sublog_idx).unwrap_or(0));
      *replay_aof_map &= !(1u64 << sublog_idx);
    }
    Ok(())
  }

  /// FastAofTruncate 选项镜像（对齐 C# serverOptions.FastAofTruncate）。
  fn fast_aof_truncate(&self) -> bool {
    self.fast_aof_truncate
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:DataLossCheck
  ///
  /// 恢复期数据丢失检查：请求的同步起点低于日志起点即截断后附加。
  /// `possible_aof_data_loss = false` 时视为致命（返回 Err）。
  pub fn data_loss_check(
    &self,
    possible_aof_data_loss: bool,
    sync_from_aof_address: &AofAddress,
  ) -> Result<(), String> {
    let begin = self.log.begin_address();
    if sync_from_aof_address.any_lesser(&begin) {
      if !possible_aof_data_loss {
        return Err(format!(
          "Failed syncing because replica requested truncated AOF address: {sync_from_aof_address:?} < begin {begin:?}"
        ));
      }
      log::warn!(
        "AOF truncated, unsafe attach: syncFromAofAddress: {sync_from_aof_address:?} < beginAofAddress: {begin:?}"
      );
    }
    Ok(())
  }

  /// 等待提交完成（已刷盘 >= 已提交）
  #[inline]
  pub fn wait_for_commit(&self) -> bool {
    let sublog = self.log().get_sub_log(0);
    sublog.flushed_until_address() >= sublog.committed_until_address()
  }

  /// 安全刷 AOF 尾地址
  #[inline]
  pub fn safe_flush_address(&self) -> Option<u64> {
    let flushed = self.log().get_sub_log(0).flushed_until_address();
    Some(flushed.max(0) as u64)
  }

  /// 是否满足截断条件
  #[inline]
  pub fn can_truncate(&self) -> bool {
    let sublog = self.log().get_sub_log(0);
    sublog.tail_address() <= sublog.begin_address()
  }

  /// 是否满足提交条件
  #[inline]
  pub fn can_commit(&self) -> bool {
    let flushed = self.log().get_sub_log(0).flushed_until_address();
    let tail = self.log().get_tail_address(0);
    flushed < tail
  }

  /// 安全写日志尾地址
  #[inline]
  pub fn tail_address(&self) -> i64 {
    self.log().get_tail_address(0)
  }

  /// 异步截断至指定 AOF 地址
  #[inline]
  pub async fn truncate_until_async(&self, until: &waof::AofAddress) {
    self.log().truncate_until_async(until).await;
  }

  /// 物理提交刷盘
  #[inline]
  pub async fn commit_flush_async(&self) {
    self.log().commit_flush_async().await;
  }

  /// 异步恢复
  #[inline]
  pub async fn recover_async(&self) {
    self.log().recover_async().await;
  }

  /// 重放 AOF 条目至指定地址
  pub async fn replay_database_aof<D: wdev::Device>(
    self: Arc<Self>,
    db: &GarnetDatabase<D>,
    until: u64,
  ) -> wkv::Result<u64> {
    let session = db.store.new_session()?;
    session.set_active_db(db.id.max(0) as u64);
    let batch = session.enter_batch();
    let storage = StorageSession::new(batch, Arc::clone(&db.version_map));
    let aof_clone = Arc::clone(&self);
    let processor = AofProcessor::new(self);
    let target = ReplayTarget {
      session: &storage,
      store: Arc::clone(&db.store),
      store_version: db.store.current_version(),
    };
    let until_address = if until == u64::MAX {
      -1
    } else {
      until.min(aof_clone.log().get_tail_address(0) as u64) as i64
    };
    let replayed =
      AofRecover::single_log_recover(&processor, &aof_clone, db.id, 0, until_address, &target)
        .await
        .map_err(|e| wkv::Error::Io(io::Error::other(e.to_string())))?;
    Ok(replayed)
  }
}

impl<D: wdev::Device> DatabaseAof<D> for GarnetAppendOnlyFile {
  fn total_size(&self) -> i64 {
    self.total_size()
  }

  fn tail_address(&self) -> i64 {
    self.log().get_tail_address(0)
  }

  fn truncate_until_async<'a>(
    &'a self,
    until: &'a waof::AofAddress,
  ) -> impl Future<Output = ()> + 'a {
    self.log().truncate_until_async(until)
  }

  fn commit_flush_async(&self) -> impl Future<Output = ()> + '_ {
    self.log().commit_flush_async()
  }

  fn recover_async(&self) -> impl Future<Output = ()> + '_ {
    self.log().recover_async()
  }

  fn can_truncate(&self) -> bool {
    let sublog = self.log().get_sub_log(0);
    sublog.tail_address() <= sublog.begin_address()
  }

  fn can_commit(&self) -> bool {
    let flushed = self.log().get_sub_log(0).flushed_until_address();
    let tail = self.log().get_tail_address(0);
    flushed < tail
  }

  fn wait_for_commit(&self) -> bool {
    let sublog = self.log().get_sub_log(0);
    sublog.flushed_until_address() >= sublog.committed_until_address()
  }

  fn safe_flush_address(&self) -> Option<u64> {
    let flushed = self.log().get_sub_log(0).flushed_until_address();
    Some(flushed.max(0) as u64)
  }

  async fn replay_database_aof(
    self: Arc<Self>,
    db: &GarnetDatabase<D>,
    until: u64,
  ) -> wkv::Result<u64> {
    let session = db.store.new_session()?;
    session.set_active_db(db.id.max(0) as u64);
    let batch = session.enter_batch();
    let storage = StorageSession::new(batch, Arc::clone(&db.version_map));
    let aof_clone = Arc::clone(&self);
    let processor = AofProcessor::new(self);
    let target = ReplayTarget {
      session: &storage,
      store: Arc::clone(&db.store),
      store_version: db.store.current_version(),
    };
    let until_address = if until == u64::MAX {
      -1
    } else {
      until.min(aof_clone.log().get_tail_address(0) as u64) as i64
    };
    let replayed =
      AofRecover::single_log_recover(&processor, &aof_clone, db.id, 0, until_address, &target)
        .await
        .map_err(|e| wkv::Error::Io(io::Error::other(e.to_string())))?;
    Ok(replayed)
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use waof::AofEntryType;

  use super::*;
  use crate::aof::{garnet_log::InMemorySublog, sublog::Sublog};

  fn aof_with(sublogs: usize, replay_tasks: i32) -> Arc<GarnetAppendOnlyFile> {
    let options = RuntimeServerOptions {
      aof_physical_sublog_count: sublogs as i32,
      aof_replay_task_count: replay_tasks,
      ..RuntimeServerOptions::default()
    };
    let backends: Vec<Arc<Sublog>> = (0..sublogs.max(1))
      .map(|_| Arc::new(Sublog::Mem(InMemorySublog::new())))
      .collect();
    let seq_num_gen = (sublogs > 1).then(|| Arc::new(SequenceNumberGenerator::new(0)));
    Arc::new(GarnetAppendOnlyFile::new(
      Arc::new(GarnetLog::new(&options, backends, seq_num_gen.clone())),
      &options,
      seq_num_gen,
    ))
  }

  #[test]
  fn total_size_and_virtual_index() {
    let aof = aof_with(1, 1);
    assert_eq!(aof.total_size(), 0);
    aof.log().enqueue(&crate::aof::garnet_log::RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 1,
      key: b"k",
      value: b"vv",
      input: &[],
      database_id: 0,
    });
    assert!(aof.total_size() > 0);
    assert_eq!(aof.get_virtual_sublog_idx(0, 0), 0);
    assert_eq!(aof.virtual_sublog_count(), 1);
  }

  #[test]
  fn sequence_numbers_strictly_increase_past_tail() {
    let aof = aof_with(2, 1);
    let first = aof.get_sequence_number();
    let larger = aof.get_larger_than_maximum_sequence_number();
    assert!(larger > first);
    assert!(aof.get_larger_than_maximum_sequence_number() >= larger);
  }

  #[test]
  fn consistency_manager_generation_bump() {
    let aof = aof_with(2, 1);
    let v1 = aof.read_consistency_manager().unwrap();
    assert_eq!(v1.current_version(), 1);
    aof.create_or_update_key_sequence_manager();
    let v2 = aof.read_consistency_manager().unwrap();
    assert_eq!(v2.current_version(), 2, "代际 = 前代 + 1");
  }

  #[test]
  fn reset_sequence_generator_after_replay() {
    let aof = aof_with(2, 1);
    let manager = aof.read_consistency_manager().unwrap();
    manager.update_physical_sublog_max_sequence_number(0, 77);
    manager.update_physical_sublog_max_sequence_number(1, 42);
    aof.reset_sequence_number_generator();
    // 抬升后取号不小于已回放最大序列号（时钟推进后严格增大）
    assert!(aof.get_sequence_number() >= 77, "恢复后时间前进");
  }

  #[test]
  fn sync_replay_address_decision() {
    let aof = aof_with(1, 1);
    // 主侧写入并提交：地址 1..tail
    let address = aof.log().enqueue(&crate::aof::garnet_log::RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 2,
      session_id: 1,
      key: b"k",
      value: b"v",
      input: &[],
      database_id: 0,
    });
    aof.log().commit();

    let mut checkpoint_begin = AofAddress::create(1, 1);
    let replication_offset2 = AofAddress::create(1, i64::MAX);
    let addr_1 = AofAddress::create(1, 1);
    let addr_tail = AofAddress::create(1, address + 1);
    let (replay_map, until) = aof
      .compute_aof_sync_replay_address(
        &AofSyncReplayOptions {
          recover_from_remote: false,
          same_main_store_checkpoint_history: true,
          same_history2: true,
          replication_offset2: &replication_offset2,
          replica_aof_begin_address: &addr_1,
          replica_aof_tail_address: &addr_tail,
          begin_address: &addr_1,
        },
        &mut checkpoint_begin,
      )
      .unwrap();
    assert_eq!(replay_map, 1, "检查点未覆盖的记录须重放");
    assert!(until.get(0).unwrap() > 1);
    assert_eq!(checkpoint_begin.get(0), until.get(0));

    // 代际不同：不重放，改全量流式
    let mut checkpoint_begin = AofAddress::create(1, 1);
    let addr_begin_9 = AofAddress::create(1, 9);
    let (replay_map, until) = aof
      .compute_aof_sync_replay_address(
        &AofSyncReplayOptions {
          recover_from_remote: false,
          same_main_store_checkpoint_history: false,
          same_history2: true,
          replication_offset2: &replication_offset2,
          replica_aof_begin_address: &addr_1,
          replica_aof_tail_address: &addr_tail,
          begin_address: &addr_begin_9,
        },
        &mut checkpoint_begin,
      )
      .unwrap();
    assert_eq!(replay_map, 0);
    assert_eq!(until.get(0).unwrap(), 9, "改自主侧起点流式");
  }

  #[test]
  fn sync_replay_rejects_divergent_tail_without_fast_truncate() {
    let aof = aof_with(1, 1);
    let mut checkpoint_begin = AofAddress::create(1, 1000);
    let offset2 = AofAddress::create(1, i64::MAX);
    let addr_1 = AofAddress::create(1, 1);
    let addr_10 = AofAddress::create(1, 10);
    let result = aof.compute_aof_sync_replay_address(
      &AofSyncReplayOptions {
        recover_from_remote: false,
        same_main_store_checkpoint_history: true,
        same_history2: true,
        replication_offset2: &offset2,
        replica_aof_begin_address: &addr_1,
        replica_aof_tail_address: &addr_10,
        begin_address: &addr_1,
      },
      &mut checkpoint_begin,
    );
    assert!(result.is_err(), "副本尾落后且未开 FastAofTruncate 即致命");
  }

  #[test]
  fn data_loss_check_fatal_and_tolerated() {
    let aof = aof_with(1, 1);
    aof.log().enqueue(&crate::aof::garnet_log::RecordShape {
      op_type: AofEntryType::StoreUpsert,
      version: 1,
      session_id: 1,
      key: b"k",
      value: b"v",
      input: &[],
      database_id: 0,
    });
    // 请求低于起点的地址：致命
    assert!(
      aof
        .data_loss_check(false, &AofAddress::create(1, 0))
        .is_err()
    );
    // 容忍截断：unsafe attach
    assert!(aof.data_loss_check(true, &AofAddress::create(1, 0)).is_ok());
    // 合法地址：通过
    assert!(
      aof
        .data_loss_check(false, &AofAddress::create(1, i64::MAX))
        .is_ok()
    );
  }

  #[test]
  fn invalid_and_max_address_shapes() {
    let aof = aof_with(2, 1);
    let invalid = aof.invalid_aof_address();
    assert_eq!(invalid.length(), 2);
    assert_eq!(invalid.get(0), Some(-1));
    let max = aof.max_aof_address();
    assert_eq!(max.get(1), Some(i64::MAX));
  }
}
