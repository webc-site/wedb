//! 纪元推进与检查点/恢复面：`garnet_current_epoch` 三件套（同步/异步两版静止
//! 等待）+ 按需快照、在线引擎置换、检查点导入依赖束与整体析构
//! （epoch 属副本读写一致性栅栏，与检查点/置换同生命周期，故并件）

use std::{
  fs::{create_dir_all, read},
  ops::ControlFlow,
  sync::{Arc, atomic::Ordering},
  thread,
};

use coarsetime::Instant;
use waof::AofAddress;
use wbase::future::yield_now;
use wcpr::CheckpointMeta;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::cluster_session::ClusterSessionFace;

use crate::server::{
  cluster::CheckpointCallbackFace, cluster_provider::ClusterProvider,
  cluster_session::ERR_CLUSTER_NOT_INITIALIZED,
  replication::receive_checkpoint_handler::CheckpointImportCtx,
};

impl ClusterProvider {
  /// 获取 Garnet 当前纪元（对标 C# GarnetCurrentEpoch）
  #[inline]
  pub fn current_epoch(&self) -> i64 {
    self.garnet_current_epoch.load(Ordering::Acquire)
  }

  /// libs/cluster/Server/ClusterProvider.cs:BumpCurrentEpoch
  ///
  /// 推进 Garnet 集群纪元
  #[inline]
  pub fn bump_current_epoch(&self) -> i64 {
    self.garnet_current_epoch.fetch_add(1, Ordering::AcqRel) + 1
  }

  /// libs/cluster/Server/ClusterProvider.cs:BumpAndWaitForEpochTransitionAsync
  ///
  /// 推进集群纪元并自旋等待全部活跃集群会话批内纪元快照追平（C# 遍历
  /// storeWrapper.Servers → ActiveClusterSessions 逐会话重试至
  /// LocalCurrentEpoch 追平，快照 0 = 批外空闲放行；rust 每轮以
  /// yield_now 让步执行器，对标 C# await Task.Yield()）。以
  /// cluster_node_timeout() 为上限，超时返 false；None（0 = 无限）不设限，
  /// 与 C# 无限自旋一致（调用方同款忽略返值放行，false 仅表达静止未达成）
  pub async fn bump_and_wait_for_epoch_transition_async(&self) -> bool {
    let current_epoch = self.bump_current_epoch();
    let start = Instant::now();
    let limit = self.cluster_node_timeout();
    while !self.all_sessions_caught_up(current_epoch) {
      if limit.is_some_and(|d| start.elapsed() >= d.into()) {
        return false;
      }
      yield_now().await;
    }
    true
  }

  /// 纪元推进全会话静止的命令批内同步形态（C# 命令侧
  /// `AsyncUtils.BlockingWait(BumpAndWaitForEpochTransitionAsync())` 的
  /// 语义：网络线程阻塞等待，见 RespClusterSlotManagementCommands.cs:493）
  ///
  /// compio 单线程每核下，发起会话所在线程的其余会话必处批外（快照 0），
  /// 阻塞自旋仅等他核会话收尾，无死锁；上限与追平判定同异步形态
  pub fn bump_and_wait_for_epoch_transition(&self) -> bool {
    let current_epoch = self.bump_current_epoch();
    let start = Instant::now();
    let limit = self.cluster_node_timeout();
    while !self.all_sessions_caught_up(current_epoch) {
      if limit.is_some_and(|d| start.elapsed() >= d.into()) {
        return false;
      }
      thread::yield_now();
    }
    true
  }

  /// 全部活跃集群会话纪元是否追平（ClusterProvider.cs:377
  /// ActiveClusterSessions 枚举的等价面；papaya 无锁快照枚举对标 C#
  /// ConcurrentDictionary 轻量迭代，静止等待不排阻会话注册注销；枚举时
  /// 顺带清扫过期弱引用）
  fn all_sessions_caught_up(&self, current_epoch: i64) -> bool {
    let pin = self.cluster_sessions.pin();
    pin
      .iter()
      .try_for_each(|(key, weak)| match weak.upgrade() {
        Some(s) => {
          let entry_epoch = s.local_current_epoch();
          // C# 判定取反：entryEpoch != 0 && entryEpoch < currentEpoch 才重试
          if entry_epoch == 0 || entry_epoch >= current_epoch {
            ControlFlow::Continue(())
          } else {
            ControlFlow::Break(())
          }
        }
        // 会话已亡：当场自清扫死弱引用，免注销钩子
        None => {
          pin.remove(key);
          ControlFlow::Continue(())
        }
      })
      .is_continue()
  }

  /// 按需拍摄快照并注册检查点条目（对标 C# StoreWrapper.TakeOnDemandCheckpointAsync）
  pub async fn take_on_demand_checkpoint(&self) -> Result<bool, String> {
    let Some(dm) = self.try_database_manager() else {
      return Ok(false);
    };
    let taken = dm
      .take_checkpoint(false)
      .await
      .map_err(|e| format!("On-demand checkpoint failed: {e}"))?;
    if !taken {
      return Ok(false);
    }
    if let Some(checkpoint_dir) = self.try_checkpoint_dir()
      && let Ok(Some(token)) = wcpr::find_latest_checkpoint(&checkpoint_dir)
      && let Ok(meta_bytes) = read(checkpoint_dir.join(wcpr::meta_filename(token)))
      && let Ok(meta) = CheckpointMeta::decode(&meta_bytes)
    {
      let sublogs = self
        .replication_manager()
        .map(|rm| rm.sublog_count())
        .unwrap_or(1);
      let covered_u64 = meta.checkpoint_aof_address.unwrap_or(0);
      let covered_addr = AofAddress::create(sublogs as i32, covered_u64 as i64);
      self
        .add_new_checkpoint_entry(true, covered_addr, token, token)
        .await;
    }
    Ok(true)
  }

  /// 在线引擎置换（副本检查点导入闭环收口）：单次写本层引擎槽——宿主已采纳
  /// 同槽时新引擎即对后续新会话装配生效，无需第二处更新（对标 C# 全体调用方
  /// 经 StoreWrapper.cs:41 单计算属性自动转发恢复后的引擎；存量会话随批纪元
  /// 自然收敛）
  pub fn swap_online_store(&self, store: Arc<WedbStore<SegmentedDevice>>) {
    self.store_slot.read().swap(store);
  }

  /// 检查点导入落盘依赖束（三 arm 接收面现取现用；目录缺失时惰性创建）
  pub fn checkpoint_import_ctx(&self) -> Result<CheckpointImportCtx, String> {
    use crate::server::replication::receive_checkpoint_handler::CheckpointImportCtx;
    let dir = self
      .try_checkpoint_dir()
      .ok_or_else(|| ERR_CLUSTER_NOT_INITIALIZED.to_string())?;
    create_dir_all(&dir).map_err(|e| format!("IOERR create checkpoint dir: {e}"))?;
    let device = self
      .try_store()
      .ok_or_else(|| ERR_CLUSTER_NOT_INITIALIZED.to_string())?
      .device
      .clone();
    Ok(CheckpointImportCtx {
      store_device: device,
      checkpoint_dir: dir,
    })
  }

  /// 执行序列号生成器复位（故障转移触发时调用；对标 C#
  /// ReplicaFailoverSession.cs:154 经 storeWrapper.appendOnlyFile 直达
  /// GarnetAppendOnlyFile.ResetSequenceNumberGenerator，AOF 门面未装配
  /// 时空转——单物理日志模式 C# 侧同样短路）
  pub fn reset_sequence_number_generator(&self) {
    if let Some(aof) = self.try_aof() {
      aof.reset_sequence_number_generator();
    }
  }

  /// 注入副本重放最大滞后字节数（C# serverOptions.AofReplayMaxLagBytes 的
  /// 装配期注入；INFO 复制段直读）
  pub fn set_aof_replay_max_lag_bytes(&self, value: i32) {
    self
      .aof_replay_max_lag_bytes
      .store(value, Ordering::Relaxed);
  }

  /// 副本重放最大滞后字节数（C# runtimeConfig.GetInt(AOF_REPLAY_MAX_LAG_
  /// BYTES) 读取面：-1 = 异步重放不节流，0 = 同步重放（每帧锁步），>0 =
  /// 异步重放滞后超限阻塞推流；副本会话 ThrottlePrimary 门限源）
  #[inline]
  pub fn aof_replay_max_lag_bytes(&self) -> i32 {
    self.aof_replay_max_lag_bytes.load(Ordering::Acquire)
  }

  /// libs/cluster/Server/ClusterProvider.cs:Dispose
  pub fn dispose(&self) {
    if let Some(mgr) = self.cluster_manager() {
      mgr.dispose();
    }
    if let Some(rm) = self.replication_manager() {
      rm.dispose();
    }
    if let Some(fm) = self.failover_manager() {
      fm.dispose();
    }
    if let Some(mm) = self.migration_manager() {
      mm.dispose();
    }
  }
}
