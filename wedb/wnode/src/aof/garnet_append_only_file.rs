//! Garnet 追加日志文件（对标 libs/server/AOF/GarnetAppendOnlyFile.cs:
//! GarnetAppendOnlyFile）
//!
//! C# 侧聚合 TsavoriteLog 拓扑（GarnetLog）、序列号生成器、读取一致性管理器
//! 与背压闸门；rust 侧背压与路由已由周期 1 的 [`GarnetLog`] 承载，本类型承接
//! 总尺寸 / 虚拟子日志换算 / 序列号生成与复位 / 一致性管理器代际更替。
//!（副本同步重放决策与数据丢失检查收敛于 ReplicationManager 单点）

use std::{io, sync::Arc, time::Duration};

use parking_lot::RwLock;
use waof::{AofAddress, AofEntryType, SequenceNumberGenerator};
use wconf::RuntimeServerOptions;
use wkv::Error;

use super::{
  AofProcessor,
  aof_backpressure::AofBackpressure,
  aof_processor::ReplayTarget,
  garnet_log::{AofWriteContext, GarnetLog, RecordShape, virtual_sublog_idx},
  readconsistency::read_consistency_manager::ReadConsistencyManager,
  recover::aof_recover::AofRecover,
  replay_input::{ReplayInput, ReplayInputSlice},
};
use crate::{
  database::GarnetDatabase, resp::vector::vector_manager::VectorManager, storage::StorageSession,
};

/// libs/server/AOF/GarnetAppendOnlyFile.cs:GarnetAppendOnlyFile
///
/// Garnet 追加日志文件。
pub struct GarnetAppendOnlyFile {
  /// 日志拓扑（单日志 / 分片路由层）。
  log: Arc<GarnetLog>,
  /// 服务器选项子集。
  physical_sublog_count: usize,
  replay_task_count: usize,
  replay_drift_threshold: i64,
  replay_drift_check_freq: i64,
  /// 副本一致读等待超时（C# serverOptions.ReplicaSyncTimeout 投影）。
  read_timeout: Duration,
  /// 多物理日志或单物理多回放拓扑（C# MultiLogEnabled）。
  multi_log_enabled: bool,
  /// 序列号生成器（仅多物理日志模式；C# seqNumGen，与 GarnetLog 共享）。
  seq_num_gen: Option<Arc<SequenceNumberGenerator>>,
  /// 读取一致性管理器（代际更替；C# readConsistencyManager）。
  read_consistency_manager: RwLock<Option<Arc<ReadConsistencyManager>>>,
  /// 向量集合管理器（重放面；AOF 点亮装配期注入，VADD/VREM/VSETATTR
  /// 条目重放经 AofProcessor 在此取承接面）。
  vector_manager: RwLock<Option<Arc<VectorManager>>>,
}

impl GarnetAppendOnlyFile {
  /// libs/server/AOF/GarnetAppendOnlyFile.cs:GarnetAppendOnlyFile
  ///
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
    let aof = Self {
      log: Arc::clone(&log),
      physical_sublog_count,
      replay_task_count,
      replay_drift_threshold: server_options.replay_drift_threshold,
      replay_drift_check_freq: server_options.replay_drift_check_freq,
      read_timeout: Duration::from_secs(server_options.replica_sync_timeout_secs),
      multi_log_enabled,
      // 仅多物理日志模式持有（C# 构造同款条件），并与 GarnetLog 共享
      seq_num_gen: if physical_sublog_count > 1 {
        seq_num_gen
      } else {
        None
      },
      read_consistency_manager: RwLock::new(None),
      vector_manager: RwLock::new(None),
    };
    if let Some(bp) = aof.log.backpressure() {
      bp.set_weak_log(Arc::downgrade(&aof.log));
    }
    // C# 构造即建一致性管理器（CreateOrUpdateKeySequenceManager）
    aof.create_or_update_key_sequence_manager();
    aof
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:Log
  ///
  /// 日志拓扑句柄。
  pub fn log(&self) -> &Arc<GarnetLog> {
    &self.log
  }

  /// 主库门控广播 FLUSH 条目：仅 `primary` 为真时向共享 AOF 追加安全 flush
  /// 广播条目（C# `clusterProvider.IsPrimary()` 门控 + `EnqueueSafeFlushAOF`）。
  /// 副本与嵌入式无 AOF 面为静默跳过——副本清库经回放条目承接，绝不二次
  /// 入队（防自激放大；回放面另有 pause_aof_listeners 双保险）。
  ///
  /// C# 对偶为 SafeFlushAOF 内联的「IsPrimary 门控 + Log.EnqueueSafeFlushAOF」
  /// 主库入队段，非函数级映射；SafeFlushAOF 的函数级映射唯一见
  /// SingleDatabaseManager::safe_flush_aof（其无条件 SafeTruncateAOF 截断段
  /// 由 flush_all_databases 的「先截断后入队」承接）。
  pub fn enqueue_safe_flush_aof_if_primary(
    &self,
    primary: bool,
    op_type: AofEntryType,
    unsafe_truncate_log: bool,
    ns: u64,
    db: u64,
  ) -> waof::Result<()> {
    if !primary {
      return Ok(());
    }
    self
      .log
      .enqueue_safe_flush_aof(op_type, unsafe_truncate_log, ns, db)?;
    Ok(())
  }

  /// 原始底层条目入队（统一装配 RecordShape 并写入日志，对标 C# 各 WriteLog* 共用单一 RecordShape 组装形态）
  #[inline]
  pub fn enqueue_raw(
    &self,
    op_type: AofEntryType,
    ctx: impl Into<AofWriteContext>,
    key: &[u8],
    value: &[u8],
    input: &[u8],
  ) -> waof::Result<i64> {
    let ctx = ctx.into();
    self
      .log
      .enqueue(&RecordShape::from_context(op_type, ctx, key, value, input))
  }

  /// 切片零分配/低分配条目入队统一出口
  #[inline]
  pub fn enqueue_slices<T: AsRef<[u8]>>(
    &self,
    op_type: AofEntryType,
    ctx: impl Into<AofWriteContext>,
    key: &[u8],
    value: &[u8],
    input: &ReplayInputSlice<'_, T>,
  ) -> waof::Result<i64> {
    let ctx = ctx.into();
    ReplayInput::with_encoded_slices(input, |serialized| {
      self.enqueue_raw(op_type, ctx, key, value, serialized)
    })
  }

  /// RMW 切片零分配条目入队统一出口（value 恒空）
  #[inline]
  pub fn enqueue_rmw_slices<T: AsRef<[u8]>>(
    &self,
    op_type: AofEntryType,
    ctx: impl Into<AofWriteContext>,
    key: &[u8],
    input: &ReplayInputSlice<'_, T>,
  ) -> waof::Result<i64> {
    self.enqueue_slices(op_type, ctx, key, &[], input)
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:backpressure
  ///
  /// 主侧复制背压闸门。
  pub fn backpressure(&self) -> Option<&Arc<AofBackpressure>> {
    self.log.backpressure()
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:Dispose
  ///
  /// AOF 关停对偶（唯一显式入口）：先置位背压闸门放行全部滞留追加方
  ///（C# `backpressure?.Dispose()`——Release all stalled appenders
  /// permanently），后直驱全拓扑刷盘收口未提交帧（C# `Log.Dispose()` 的
  /// 提交面对译，复用 [`GarnetLog::commit_async`] 单点刷盘口）。直驱路径
  /// 不经常驻提交协程通道——停机期 worker 运行时随 join 析构、协程已不在，
  /// 直驱刷盘不受影响。
  pub async fn dispose_async(&self) {
    if let Some(bp) = self.backpressure() {
      bp.dispose();
    }
    self.log().commit_async().await;
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:ReadConsistencyManager
  ///
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

  /// 注入向量集合管理器（AOF 点亮装配期；C# 由 storeWrapper.activeVectorManager
  /// 随数据库上下文激活承接）。
  pub fn set_vector_manager(&self, manager: Arc<VectorManager>) {
    *self.vector_manager.write() = Some(manager);
  }

  /// 向量集合管理器快照（未注入为 None，向量条目重放按无承接面失败）。
  pub fn vector_manager(&self) -> Option<Arc<VectorManager>> {
    self.vector_manager.read().clone()
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:MultiLogEnabled
  ///
  /// 多物理日志模式是否启用。
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
  /// 物理子日志 × 回放任务 → 虚拟子日志下标（公式单点转引域共享内核）。
  pub fn get_virtual_sublog_idx(&self, sublog_idx: usize, replay_idx: usize) -> usize {
    virtual_sublog_idx(sublog_idx, replay_idx, self.replay_task_count)
  }

  /// libs/server/Servers/GarnetServerOptions.cs:AofVirtualSublogCount
  ///
  /// 虚拟子日志总数。
  pub fn virtual_sublog_count(&self) -> usize {
    self.physical_sublog_count * self.replay_task_count
  }

  /// libs/server/Servers/GarnetServerOptions.cs:AofReplayTaskCount
  ///
  /// 单物理子日志所辖虚拟子日志数（= 回放任务数，与
  /// [`Self::get_virtual_sublog_idx`] 的槽算式同源，路由换算单点）。
  pub fn virtual_sublog_per_sublog(&self) -> usize {
    self.replay_task_count
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:InvalidAofAddress
  ///
  /// 无效地址向量（C# InvalidAofAddress：全槽 -1）。
  pub fn invalid_aof_address(&self) -> AofAddress {
    AofAddress::create(self.physical_sublog_count as i32, -1)
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:GetLargerThanMaximumSequenceNumber
  ///
  /// 严格大于已观测尾部全部序列号的新序列号（调用方须先读尾地址）。
  ///（C# GetLargerThanMaximumSequenceNumber = GetSequenceNumber() + 1）
  pub fn get_larger_than_maximum_sequence_number(&self) -> i64 {
    self.get_sequence_number() + 1
  }

  /// libs/server/AOF/GarnetAppendOnlyFile.cs:GetSequenceNumber
  ///
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
      self.replay_drift_threshold,
      self.replay_drift_check_freq,
      self.read_timeout,
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

  /// 等待提交完成（已刷盘 >= 已提交）
  #[inline]
  pub fn wait_for_commit(&self) -> bool {
    let sublogs = self.log().sublogs();
    !sublogs.is_empty()
      && sublogs
        .iter()
        .all(|s| s.flushed_until_address() >= s.committed_until_address())
  }

  /// 安全写日志尾地址（全子日志最大值；单日志拓扑即唯一子日志尾）
  #[inline]
  pub fn tail_address(&self) -> i64 {
    self
      .log()
      .sublogs()
      .iter()
      .map(|s| s.tail_address())
      .max()
      .unwrap_or(0)
  }

  /// C# DatabaseManagerBase ReplayDatabaseAOF 编排对应（精确锚点见 database_manager_base.rs）
  ///
  /// 重放 AOF 条目至指定地址
  pub async fn replay_database_aof<D: wdev::Device>(
    self: Arc<Self>,
    db: &GarnetDatabase<D>,
    until: u64,
  ) -> wkv::Result<u64> {
    let session = db.store.new_session()?;
    session.set_active_db(db.id.max(0) as u64);
    let batch = session.enter_batch();
    let storage = StorageSession::new(batch);
    let aof_clone = Arc::clone(&self);
    // 拓扑面先取（AofProcessor 构造消耗 Arc）
    let multi_log_enabled = self.multi_log_enabled;
    let physical_sublog_count = self.physical_sublog_count;
    let invalid = self.invalid_aof_address();
    let processor = AofProcessor::new(self);
    let target = ReplayTarget {
      session: &storage,
      store: Arc::clone(&db.store),
      store_version: db.store.current_version(),
    };
    // C# AofRecover.cs:Recover 分派（RecoverReplayDriver）：MultiLogEnabled
    // 时按 untilAddress 向量逐物理子日志收敛恢复，否则单子日志恢复
    let replayed = if multi_log_enabled {
      let mut until_vector = invalid;
      for i in 0..physical_sublog_count {
        let value = if until == u64::MAX {
          -1
        } else {
          until.min(aof_clone.log().get_tail_address(i) as u64) as i64
        };
        until_vector.set(i, value);
      }
      AofRecover::multi_log_recover(&processor, &aof_clone, db.id, &until_vector, &target)
        .await
        .map_err(|e| Error::Io(io::Error::other(e.to_string())))?
    } else {
      let until_address = if until == u64::MAX {
        -1
      } else {
        until.min(aof_clone.log().get_tail_address(0) as u64) as i64
      };
      AofRecover::single_log_recover(&processor, &aof_clone, db.id, 0, until_address, &target)
        .await
        .map_err(|e| Error::Io(io::Error::other(e.to_string())))?
    };
    Ok(replayed)
  }
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use compio::runtime::Runtime;
  use waof::AofEntryType;

  use super::*;
  use crate::aof::{test_support::test_sublog, waof_sublog::AofSublog};

  fn aof_with(
    sublogs: usize,
    replay_tasks: i32,
  ) -> (Vec<tempfile::TempDir>, Arc<GarnetAppendOnlyFile>) {
    let options = RuntimeServerOptions {
      aof_physical_sublog_count: sublogs as i32,
      aof_replay_task_count: replay_tasks,
      ..RuntimeServerOptions::default()
    };
    let dirs = (0..sublogs.max(1))
      .map(|i| {
        let (dir, backend) = test_sublog(&format!("aof_{i}"));
        (dir, backend)
      })
      .collect::<Vec<_>>();
    let backends: Vec<Arc<AofSublog>> = dirs.iter().map(|(_, b)| Arc::clone(b)).collect();
    let seq_num_gen = (sublogs > 1).then(|| Arc::new(SequenceNumberGenerator::new(0)));
    let aof = Arc::new(GarnetAppendOnlyFile::new(
      Arc::new(GarnetLog::new(&options, backends, seq_num_gen.clone()).expect("构造 GarnetLog")),
      &options,
      seq_num_gen,
    ));
    (dirs.into_iter().map(|(d, _)| d).collect(), aof)
  }

  #[test]
  fn total_size_and_virtual_index() {
    let (_dirs, aof) = aof_with(1, 1);
    assert_eq!(aof.total_size(), 0);
    let _ = aof.enqueue_raw(AofEntryType::StoreUpsert, (1, 1), b"k", b"vv", &[]);
    assert!(aof.total_size() > 0);
    assert_eq!(aof.get_virtual_sublog_idx(0, 0), 0);
    assert_eq!(aof.virtual_sublog_count(), 1);
  }

  /// 关停对偶：dispose 收口未提交帧并放行闸门（C# GarnetAppendOnlyFile.
  /// Dispose 的 backpressure?.Dispose + Log.Dispose 对位）
  #[test]
  fn dispose_async_flushes_frames_and_releases_gate() {
    let (_dirs, aof) = aof_with(1, 1);
    let _ = aof.enqueue_raw(AofEntryType::StoreUpsert, (1, 1), b"k", b"vv", &[]);
    let tail = aof.tail_address();
    let sublog = aof.log().get_sub_log(0);
    assert!(
      sublog.flushed_until_address() < tail,
      "预置：dispose 前未刷盘"
    );

    Runtime::new().unwrap().block_on(aof.dispose_async());

    assert!(
      sublog.flushed_until_address() >= tail,
      "dispose 后未提交帧已落设备"
    );
    // 闸门放行：dispose 置位后任何滞后快照均判释放
    assert!(aof.backpressure().unwrap().is_released(0, i64::MAX));
  }

  #[test]
  fn sequence_numbers_strictly_increase_past_tail() {
    let (_dirs, aof) = aof_with(2, 1);
    let first = aof.get_sequence_number();
    let larger = aof.get_larger_than_maximum_sequence_number();
    assert!(larger > first);
    assert!(aof.get_larger_than_maximum_sequence_number() >= larger);
  }

  #[test]
  fn consistency_manager_generation_bump() {
    let (_dirs, aof) = aof_with(2, 1);
    let v1 = aof.read_consistency_manager().unwrap();
    assert_eq!(v1.current_version(), 1);
    aof.create_or_update_key_sequence_manager();
    let v2 = aof.read_consistency_manager().unwrap();
    assert_eq!(v2.current_version(), 2, "代际 = 前代 + 1");
  }

  #[test]
  fn reset_sequence_generator_after_replay() {
    let (_dirs, aof) = aof_with(2, 1);
    let manager = aof.read_consistency_manager().unwrap();
    manager.update_physical_sublog_max_sequence_number(0, 77);
    manager.update_physical_sublog_max_sequence_number(1, 42);
    aof.reset_sequence_number_generator();
    // 抬升后取号不小于已回放最大序列号（时钟推进后严格增大）
    assert!(aof.get_sequence_number() >= 77, "恢复后时间前进");
  }

  #[test]
  fn invalid_address_shape() {
    let (_dirs, aof) = aof_with(2, 1);
    let invalid = aof.invalid_aof_address();
    assert_eq!(invalid.length(), 2);
    assert_eq!(invalid.get(0), Some(-1));
  }

  #[test]
  fn multi_sublog_probe_methods() {
    // TempDir 绑定持目录存活至测试结束，段文件随 Drop 清理
    let (_dirs, aof) = aof_with(2, 1);
    let sub0 = Arc::clone(aof.log().get_sub_log(0));
    let sub1 = Arc::clone(aof.log().get_sub_log(1));

    // 初始状态：双子日志空日志（真实段设备首地址 0：begin=tail=flushed=committed=0）
    assert_eq!(aof.tail_address(), 0);
    assert!(aof.wait_for_commit());

    // 仅向 sublog 1 写入数据，0 号保持空闲
    let addr = sub1.enqueue(b"sublog_1_record").unwrap();
    assert_eq!(addr, 0, "首条记录落设备首地址");
    assert!(sub1.tail_address() > 0);
    assert_eq!(sub0.tail_address(), 0);

    // tail_address: 取所有子日志的最大尾地址
    assert_eq!(aof.tail_address(), sub1.tail_address());

    // 提交 sub1
    let sub1_tail = sub1.tail_address();
    sub1.commit(sub1_tail, 0);
    assert!(aof.wait_for_commit());

    // 向 sub0 写入更大长度数据并提交
    let _ = sub0.enqueue(b"sublog_0_longer_payload_data");
    let sub0_tail = sub0.tail_address();
    sub0.commit(sub0_tail, 0);

    // tail_address 应取各子日志尾地址的最大值
    let expected_max_tail = sub0.tail_address().max(sub1.tail_address());
    assert_eq!(aof.tail_address(), expected_max_tail);
  }

  #[test]
  fn drift_options_forwarded_to_consistency_manager() {
    let options = RuntimeServerOptions {
      aof_physical_sublog_count: 2,
      aof_replay_task_count: 1,
      replay_drift_threshold: 10,
      replay_drift_check_freq: 2,
      ..RuntimeServerOptions::default()
    };
    let (dir0, b0) = test_sublog("aof_drift_0");
    let (dir1, b1) = test_sublog("aof_drift_1");
    let seq_num_gen = Some(Arc::new(SequenceNumberGenerator::new(0)));
    let aof = Arc::new(GarnetAppendOnlyFile::new(
      Arc::new(
        GarnetLog::new(
          &options,
          vec![Arc::clone(&b0), Arc::clone(&b1)],
          seq_num_gen.clone(),
        )
        .expect("构造 GarnetLog"),
      ),
      &options,
      seq_num_gen,
    ));
    let rcm = aof.read_consistency_manager().expect("存在一致性管理器");
    assert_eq!(rcm.current_version(), 1);
    assert_eq!(rcm.virtual_sublog_count(), 2);
    assert_eq!(rcm.vsr(0).next_drift_check_window_lower_bound(), 0);
    assert_eq!(rcm.vsr(1).next_drift_check_window_lower_bound(), 20);
    drop(dir0);
    drop(dir1);
  }
}
