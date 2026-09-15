use std::{
  sync::{
    Arc, OnceLock, Weak,
    atomic::{AtomicI32, AtomicI64, AtomicU64, Ordering},
  },
  thread,
};

use coarsetime::Instant;
use compio::runtime::spawn;
use itoa::Buffer;
use parking_lot::RwLock;
use waof::{AofAddress, WalLog};
use wbase::future::yield_now;
use wdatabase::checkpoint_version;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wmetric::MetricsItem;
use wnode::{
  ClusterProvider as WnodeClusterProvider, RoleInfo,
  aof::garnet_append_only_file::GarnetAppendOnlyFile, cluster_session::ClusterSessionFace,
  resp::vector::vector_manager::VectorManager, session_parse_state_extensions::ManagerType,
};
use wresp::RespCommand;

use crate::{
  args::DEFAULT_CLUSTER_NODE_TIMEOUT_MS,
  server::{
    cluster::{CheckpointCallbackFace, CheckpointMetadata, IClusterProvider},
    cluster_manager::ClusterManager,
    cluster_session::ClusterSession,
    connection_info::ConnectionInfo,
    failover::failover_manager::FailoverManager,
    gossip::gossip_manager::GossipManager,
    migration::migration_manager::MigrationManager,
    replication::{
      aof_replication_pump::AofReplicationPump, assembly::recover_replication,
      checkpoint_entry::CheckpointEntry, cluster_replication_session::ClusterReplicationSession,
      recovery_status::RecoveryStatus, replica_sync_session::ReplicaSyncSession,
      replication_manager::ReplicationManager, store_commit::StoreCommitChannel,
    },
    worker::NodeRole,
  },
};

/// gossip 周期默认毫秒数（garnet/libs/server/Servers/GarnetServerOptions.cs:246
/// GossipDelay = 5，秒）
pub const DEFAULT_GOSSIP_DELAY_MS: u64 = 5000;

/// gossip 抽样百分比默认值（garnet/libs/server/Servers/GarnetServerOptions.cs:241
/// GossipSamplePercent = 100）
pub const DEFAULT_GOSSIP_SAMPLE_PERCENT: i32 = 100;

/// 主端 AOF 推流装配面（CLUSTER INITIATE_REPLICA_SYNC 发起侧依赖束：
/// 物理日志 + 推流泵 + 主端同步会话；AOF 门控点亮时经
/// [`ClusterProvider::set_primary_replication`] 一次注入）
pub struct PrimaryReplicationAssets {
  /// 主端物理日志（同步策略协商的位点基准 + 推流数据源）
  pub wal: Arc<WalLog<SegmentedDevice>>,
  /// 主端推流泵（attach 副本 sink 后同栈分发新写入）
  pub pump: Arc<AofReplicationPump>,
  /// 主端副本同步会话（策略协商 + 建连 + 补扫）
  pub sync_session: Arc<ReplicaSyncSession>,
}

/// WeDB 分布式集群提供者核心门面（对标 C# Garnet.cluster.ClusterProvider）
pub struct ClusterProvider {
  pub cluster_manager: RwLock<Option<Arc<ClusterManager>>>,
  pub replication_manager: RwLock<Option<Arc<ReplicationManager>>>,
  pub failover_manager: RwLock<Option<Arc<FailoverManager>>>,
  pub migration_manager: RwLock<Option<Arc<MigrationManager>>>,
  pub gossip_manager: RwLock<Option<Arc<GossipManager>>>,
  pub auth_container: RwLock<(Option<String>, Option<String>)>,
  replication_reestablishment_timeout_secs: AtomicI32,
  /// 集群节点超时毫秒数（C# GarnetServerOptions.ClusterTimeout /
  /// RuntimeServerConfig ClusterNodeTimeout 的毫秒形态；槽位校验等待与
  /// 挂起重评的超时上限取此值，装配期自 ClusterArgs 注入）
  cluster_node_timeout_ms: AtomicU64,
  /// gossip 周期毫秒数（C# GarnetServerOptions.GossipDelay 的毫秒形态，
  /// 默认 5000；gossip 主循环 sleep 与 gossip 发送超时源，装配期自
  /// ClusterArgs 注入）
  gossip_delay_ms: AtomicU64,
  /// gossip 抽样百分比（C# GarnetServerOptions.GossipSamplePercent，
  /// 默认 100 = 全量广播；装配期自 ClusterArgs 注入）
  gossip_sample_percent: AtomicI32,
  /// Garnet 当前纪元（对标 C# ClusterProvider.GarnetCurrentEpoch，初始为 1）
  garnet_current_epoch: AtomicI64,
  /// 副本重放最大滞后字节数（C# GarnetServerOptions.AofReplayMaxLagBytes，
  /// 默认 -1；INFO 复制段 aof_replay_max_lag_bytes 直读源，装配期自
  /// ClusterArgs 注入）
  aof_replay_max_lag_bytes: AtomicI32,
  /// 活跃集群会话弱引用表（C# GarnetServerBase.activeHandlers 承接的集群
  /// 会话枚举面：会话体归连接任务独占，此处仅存弱引用，过期即会话已亡，
  /// 枚举时自清扫免注销钩子；BumpAndWaitForEpochTransition 的静止等待遍历源）
  cluster_sessions: RwLock<Vec<Weak<ClusterSession>>>,
  /// 弱引用自身（用于按需向上派生包含本对象的会话，无锁读取）
  self_weak: OnceLock<Weak<ClusterProvider>>,
  /// 共享存储引擎（对标 C# clusterProvider.storeWrapper 的存储可达面；
  /// CLUSTER RESET 的 HasKeysInSlots 扫描与 HARD 清库经此下发。装配期
  /// 一次注入，未注入时集群命令族仍可用，仅 RESET 慢路径降级报错）
  store: RwLock<Option<Arc<WedbStore<SegmentedDevice>>>>,
  /// 向量集合管理器（CLUSTER RESERVE 迁移预保留面，对标 C#
  /// RespServerSession 会话持有的 vectorManager；装配期一次注入）
  vector_manager: RwLock<Option<Arc<VectorManager>>>,
  /// AOF 门面（MLOG_KEY_TIME 序列号读取面，对标 C#
  /// storeWrapper.appendOnlyFile；AOF 门控点亮时注入，未启用为 None）
  aof: RwLock<Option<Arc<GarnetAppendOnlyFile>>>,
  /// 本地物理日志句柄（副本发起 INITIATE_REPLICA_SYNC 的 begin/tail 位点源
  /// 与副本接收会话落盘目标；AOF 门控点亮时装配期注入）
  wal: RwLock<Option<Arc<WalLog<SegmentedDevice>>>>,
  /// 副本接收面会话（CLUSTER APPENDLOG 落盘重放；AOF 门控点亮时注入）
  replica_replication: RwLock<Option<Arc<ClusterReplicationSession<SegmentedDevice>>>>,
  /// 主端推流装配面（CLUSTER INITIATE_REPLICA_SYNC 发起面）
  primary_replication: RwLock<Option<Arc<PrimaryReplicationAssets>>>,
}

impl Default for ClusterProvider {
  fn default() -> Self {
    Self {
      cluster_manager: RwLock::new(None),
      replication_manager: RwLock::new(None),
      failover_manager: RwLock::new(None),
      migration_manager: RwLock::new(None),
      gossip_manager: RwLock::new(None),
      auth_container: RwLock::new((None, None)),
      replication_reestablishment_timeout_secs: AtomicI32::new(0),
      cluster_node_timeout_ms: AtomicU64::new(DEFAULT_CLUSTER_NODE_TIMEOUT_MS),
      gossip_delay_ms: AtomicU64::new(DEFAULT_GOSSIP_DELAY_MS),
      gossip_sample_percent: AtomicI32::new(DEFAULT_GOSSIP_SAMPLE_PERCENT),
      garnet_current_epoch: AtomicI64::new(1),
      aof_replay_max_lag_bytes: AtomicI32::new(-1),
      cluster_sessions: RwLock::new(Vec::new()),
      self_weak: OnceLock::new(),
      store: RwLock::new(None),
      vector_manager: RwLock::new(None),
      aof: RwLock::new(None),
      wal: RwLock::new(None),
      replica_replication: RwLock::new(None),
      primary_replication: RwLock::new(None),
    }
  }
}

impl ClusterProvider {
  /// 当前节点是否为主节点
  #[inline]
  pub fn is_primary(&self) -> bool {
    IClusterProvider::is_primary(self)
  }

  /// 当前节点是否为从节点
  #[inline]
  pub fn is_replica(&self) -> bool {
    IClusterProvider::is_replica(self)
  }

  /// 创建并装配全部集群管理器组件（对标 C# ClusterProvider 构造函数）
  pub fn new() -> Arc<Self> {
    let cp = Arc::new(Self::default());
    if cp.self_weak.set(Arc::downgrade(&cp)).is_err() {
      log::warn!("self_weak 初始化重复调用");
    }
    *cp.cluster_manager.write() = Some(Arc::new(ClusterManager::new(Arc::clone(&cp))));
    *cp.replication_manager.write() = Some(Arc::new(ReplicationManager::new()));
    *cp.failover_manager.write() = Some(Arc::new(FailoverManager::new(Arc::clone(&cp))));
    *cp.migration_manager.write() = Some(Arc::new(MigrationManager::new(Arc::clone(&cp))));
    *cp.gossip_manager.write() = Some(Arc::new(GossipManager::new(Arc::clone(&cp))));
    cp
  }

  /// 初始化复制管理器（对标 C# ClusterProvider 构造函数初始化 ReplicationManager）
  pub fn initialize_replication_manager(&self) {
    if self.replication_manager.read().is_none() {
      *self.replication_manager.write() = Some(Arc::new(ReplicationManager::new()));
    }
  }

  /// 获取当前节点连接信息
  pub fn get_connection_info(&self, node_id: &str) -> ConnectionInfo {
    self
      .cluster_manager()
      .map(|cm| cm.get_connection_info(node_id))
      .unwrap_or_default()
  }

  /// 获取 ClusterManager 句柄
  #[inline]
  pub fn cluster_manager(&self) -> Option<Arc<ClusterManager>> {
    self.cluster_manager.read().clone()
  }

  /// 获取 ReplicationManager 句柄
  #[inline]
  pub fn replication_manager(&self) -> Option<Arc<ReplicationManager>> {
    self.replication_manager.read().clone()
  }

  /// 获取 FailoverManager 句柄
  #[inline]
  pub fn failover_manager(&self) -> Option<Arc<FailoverManager>> {
    self.failover_manager.read().clone()
  }

  /// 获取 MigrationManager 句柄
  #[inline]
  pub fn migration_manager(&self) -> Option<Arc<MigrationManager>> {
    self.migration_manager.read().clone()
  }

  /// 注入复制重连轮询频率（秒；0 = 禁用，服务器总装期自 RuntimeServerOptions 注入）
  pub fn set_replication_reestablishment_timeout(&self, secs: i32) {
    self
      .replication_reestablishment_timeout_secs
      .store(secs, Ordering::Release);
  }

  /// 注入集群节点超时毫秒数（装配期一次调用；槽位校验等待超时上限源）
  pub fn set_cluster_node_timeout_ms(&self, ms: u64) {
    self.cluster_node_timeout_ms.store(ms, Ordering::Release);
  }

  /// 集群节点超时毫秒数（未注入时取默认值）
  pub fn cluster_node_timeout_ms(&self) -> u64 {
    self.cluster_node_timeout_ms.load(Ordering::Acquire)
  }

  /// 注入 gossip 周期毫秒数（装配期一次调用；对标 GarnetServerOptions.GossipDelay
  /// 秒转毫秒，默认 5000）
  pub fn set_gossip_delay_ms(&self, ms: u64) {
    self.gossip_delay_ms.store(ms, Ordering::Release);
  }

  /// gossip 周期毫秒数（未注入时取默认值）
  pub fn gossip_delay_ms(&self) -> u64 {
    self.gossip_delay_ms.load(Ordering::Acquire)
  }

  /// 注入 gossip 抽样百分比（装配期一次调用；对标
  /// GarnetServerOptions.GossipSamplePercent，默认 100）
  pub fn set_gossip_sample_percent(&self, pct: i32) {
    self.gossip_sample_percent.store(pct, Ordering::Release);
  }

  /// gossip 抽样百分比（未注入时取默认值）
  pub fn gossip_sample_percent(&self) -> i32 {
    self.gossip_sample_percent.load(Ordering::Acquire)
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:EnsureReplication
  ///
  /// 入站 gossip 会话的复制健康检查（C# EnsureReplication 完整判定链；
  /// C# 在 rm 上实现并经 clusterProvider 直达各管理器，Rust 依赖方向反转后
  /// 判定链上收至本层，rm 保留节流判定与心跳时间戳原语供本链调用）：
  /// 1. 轮询频率 0 = 禁用；
  /// 2. 距上次尝试不足频率 → 返回（节流）；
  /// 3. 仅 REPLICA 且活跃会话来自其 primary 时动作；
  /// 4. 已有活跃复制流（IsReplicating 状态面）→ 无需动作；
  /// 5. failover 进行中抑制自动重连（防 ReadRole 锁阻塞 TakeOverAsPrimary）；
  /// 6. 心跳时间戳推进；
  /// 7. PreventRoleChange + 后台 RecoverReplication 重连发起（对标 C#
  ///    Task.Run(TryReplicateDiskbasedSyncAsync)，异步体内向 primary 发
  ///    INITIATE_REPLICA_SYNC；失败静默，按 ClusterReplicationReestablishment
  ///    Timeout 轮询节奏重试）
  pub fn ensure_replication(self: &Arc<Self>, active_remote_node_id: Option<&str>) {
    use std::sync::atomic::Ordering;

    let poll_frequency = self
      .replication_reestablishment_timeout_secs
      .load(Ordering::Acquire) as i64;
    // 1. 禁用
    if poll_frequency == 0 {
      return;
    }
    let Some(rm) = self.replication_manager() else {
      return;
    };
    // 2. 节流
    if !rm.ensure_replication_due(poll_frequency) {
      return;
    }
    // 6. 心跳时间戳推进（复制健康保活）
    rm.update_last_primary_sync_time();

    // 3. 角色判定：仅 REPLICA 且活跃会话来自其 primary
    let Some(cm) = self.cluster_manager() else {
      return;
    };
    let primary_id = {
      let config = cm.current_config();
      if !config.is_replica() {
        return;
      }
      config.local_node_primary_id().map(String::from)
    };
    if primary_id.as_deref() != active_remote_node_id {
      return;
    }

    // 4. IsReplicating 状态面：活跃复制流在册则无需动作
    if rm.has_active_replication_stream() {
      return;
    }

    // 5. failover 进行中抑制自动重连
    if let Some(fm) = self.failover_manager()
      && fm.is_failover_in_progress()
    {
      log::debug!("Suppressing auto-resync during active failover");
      return;
    }

    // 7. 重连动作面：PreventRoleChange + 后台 RecoverReplication
    //（对标 C# EnsureReplication 第 7 步：prevent → Task.Run → finally allow）
    let Some(primary) = primary_id else {
      return;
    };
    if !self.prevent_role_change() {
      return;
    }
    // TOCTOU 复检（对标 C# PreventRoleChange 后的二次判定：复制状态在持锁
    // 间隙已变更 → 释放角色锁并放弃本轮重连）
    let still_replica_of_primary = self.cluster_manager().is_some_and(|cm| {
      let config = cm.current_config();
      config.is_replica() && config.local_node_primary_id() == Some(primary.as_str())
    });
    if !still_replica_of_primary {
      self.allow_role_change();
      log::info!("Skip resync: replication state changed after PreventRoleChange");
      return;
    }
    let provider = Arc::clone(self);
    spawn(async move {
      log::info!("Beginning resync to {primary} after replication session failed");
      recover_replication(&provider, &primary).await;
      provider.allow_role_change();
      log::info!("Resync attempt to {primary} completed");
    })
    .detach();
  }

  /// 获取 GossipManager 句柄
  #[inline]
  pub fn gossip_manager(&self) -> Option<Arc<GossipManager>> {
    self.gossip_manager.read().clone()
  }

  /// 集群检查点装配：注册 storeWrapper 提交标记写入通道（一次注入）
  ///
  /// 对标 C# ReplicationManager 构造内经 clusterProvider.storeWrapper 反查
  /// AOF 写入面（Rust 依赖方向反转，由装配层正向注入 GarnetLog 适配）
  pub fn set_commit_channel(&self, channel: Option<StoreCommitChannel>) {
    if let Some(rm) = self.replication_manager() {
      rm.set_commit_channel(channel);
    }
  }

  /// 检查点版本切换开始通知（PRIMARY 侧写 CheckpointStartCommit 标记）
  ///
  /// 调用点组合（对标 C# ReplicationManager 构造内
  /// `ReplicationLogCheckpointManager.checkpointVersionShiftStart` 的委托装配）：
  /// 检查点发起方在快照执行前显式调用，取代闭包注入的运行时动态分发。
  /// REPLICA 角色直接返回（C# rm 方法首行判定，Rust rm 不持 clusterManager
  /// 引由本层承接）。
  pub fn notify_version_shift_start(&self, old_version: i64, new_version: i64) {
    if self.is_replica() {
      return;
    }
    if let Some(rm) = self.replication_manager() {
      rm.checkpoint_version_shift_start(true, old_version, new_version, false);
    }
  }

  /// 检查点版本切换结束通知（快照成功后写 CheckpointEndCommit 标记；
  /// 失败路径不调用——wedb 语义下失败 Token 已整体回收、版本未切换）
  pub fn notify_version_shift_end(&self, old_version: i64, new_version: i64) {
    if self.is_replica() {
      return;
    }
    if let Some(rm) = self.replication_manager() {
      rm.checkpoint_version_shift_end(true, old_version, new_version, false);
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:ClusterUsername
  pub fn cluster_username(&self) -> Option<String> {
    self.auth_container.read().0.clone()
  }

  /// libs/cluster/Server/ClusterProvider.cs:ClusterPassword
  pub fn cluster_password(&self) -> Option<String> {
    self.auth_container.read().1.clone()
  }

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
  /// cluster_node_timeout_ms 为上限，超时返 false（C# 无限自旋；调用方
  /// 同款忽略返值放行，false 仅表达静止未达成）
  pub async fn bump_and_wait_for_epoch_transition_async(&self) -> bool {
    let current_epoch = self.bump_current_epoch();
    let start = Instant::now();
    while !self.all_sessions_caught_up(current_epoch) {
      if start.elapsed().as_millis() >= self.cluster_node_timeout_ms() {
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
    while !self.all_sessions_caught_up(current_epoch) {
      if start.elapsed().as_millis() >= self.cluster_node_timeout_ms() {
        return false;
      }
      thread::yield_now();
    }
    true
  }

  /// 全部活跃集群会话纪元是否追平（ClusterProvider.cs:377
  /// ActiveClusterSessions 枚举的等价面；枚举时顺带清扫过期弱引用）
  fn all_sessions_caught_up(&self, current_epoch: i64) -> bool {
    let mut sessions = self.cluster_sessions.write();
    sessions.retain(|weak| weak.upgrade().is_some());
    sessions.iter().filter_map(Weak::upgrade).all(|s| {
      let entry_epoch = s.local_current_epoch();
      // C# 判定取反：entryEpoch != 0 && entryEpoch < currentEpoch 才重试
      entry_epoch == 0 || entry_epoch >= current_epoch
    })
  }

  /// 注入共享存储引擎（集群装配期一次调用；对标 C# 构造期经 storeWrapper
  /// 建立的存储可达面）
  pub fn set_store(&self, store: Arc<WedbStore<SegmentedDevice>>) {
    *self.store.write() = Some(store);
  }

  /// 共享存储引擎（未注入时 None）
  pub fn try_store(&self) -> Option<Arc<WedbStore<SegmentedDevice>>> {
    self.store.read().clone()
  }

  /// 注入向量集合管理器（集群装配期一次调用；对标 C# RespServerSession
  /// 会话持有的 vectorManager——CLUSTER RESERVE 迁移预保留面）
  pub fn set_vector_manager(&self, vector_manager: Arc<VectorManager>) {
    *self.vector_manager.write() = Some(vector_manager);
  }

  /// 向量集合管理器（未注入时 None）
  pub fn try_vector_manager(&self) -> Option<Arc<VectorManager>> {
    self.vector_manager.read().clone()
  }

  /// 注入 AOF 门面（AOF 门控点亮时装配期一次调用；对标 C#
  /// storeWrapper.appendOnlyFile 可达面——MLOG_KEY_TIME 序列号读取）。
  /// 物理日志句柄同步注入复制域驱动仓库（C# AofSyncDriverStore 构造期
  /// 反查 appendOnlyFile.Log；Rust 装配期注入，见
  /// AofSyncDriverStore::attach_log——SafeTruncateAof 物理截断面）
  pub fn set_aof(&self, aof: Option<Arc<GarnetAppendOnlyFile>>) {
    if let Some(rm) = self.replication_manager() {
      rm.aof_sync_driver_store
        .attach_log(aof.as_ref().map(|a| Arc::clone(a.log())));
    }
    *self.aof.write() = aof;
  }

  /// AOF 门面（AOF 门控未点亮时 None）
  pub fn try_aof(&self) -> Option<Arc<GarnetAppendOnlyFile>> {
    self.aof.read().clone()
  }

  /// 注入本地物理日志句柄（AOF 门控点亮时装配期一次调用；副本发起同步的
  /// begin/tail 位点源与副本接收会话落盘目标共用同一实例）
  pub fn set_wal(&self, wal: Arc<WalLog<SegmentedDevice>>) {
    *self.wal.write() = Some(wal);
  }

  /// 本地物理日志句柄（AOF 门控未点亮时 None）
  pub fn try_wal(&self) -> Option<Arc<WalLog<SegmentedDevice>>> {
    self.wal.read().clone()
  }

  /// 注入副本接收面会话（AOF 门控点亮时装配期一次调用；CLUSTER APPENDLOG
  /// 记录帧经此落盘重放，对标 C# 会话侧 replicaReplaySession 可达面）
  pub fn set_replica_replication_session(
    &self,
    session: Option<Arc<ClusterReplicationSession<SegmentedDevice>>>,
  ) {
    *self.replica_replication.write() = session;
  }

  /// 副本接收面会话（未注入时 None）
  pub fn try_replica_replication_session(
    &self,
  ) -> Option<Arc<ClusterReplicationSession<SegmentedDevice>>> {
    self.replica_replication.read().clone()
  }

  /// 注入主端推流装配面（AOF 门控点亮时装配期一次调用；CLUSTER
  /// INITIATE_REPLICA_SYNC 发起面）
  pub fn set_primary_replication(&self, assets: Option<Arc<PrimaryReplicationAssets>>) {
    *self.primary_replication.write() = assets;
  }

  /// 主端推流装配面（未注入时 None）
  pub fn try_primary_replication(&self) -> Option<Arc<PrimaryReplicationAssets>> {
    self.primary_replication.read().clone()
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
    self.aof_replay_max_lag_bytes.store(value, Ordering::Relaxed);
  }
}

impl IClusterProvider for ClusterProvider {
  /// libs/cluster/Server/ClusterProvider.cs:CreateClusterSession
  ///
  /// 构造即登记活跃会话弱引用表（C# 侧会话经 activeHandlers 承载，此处为
  /// 等价枚举源）。返回注册进表的同一 `Arc`——调用方（会话消费者装配 /
  /// 测试）持强引用，弱引用与会话生命周期闭合；C# 返回 IClusterSession
  /// 接口形态，rust 经 `Into<wnode ClusterSession>` 达成同款擦除
  fn create_cluster_session(&self) -> Arc<ClusterSession> {
    let Some(cp) = self.self_weak.get().and_then(|w| w.upgrade()) else {
      return Arc::new(ClusterSession::new(Arc::new(Self::default())));
    };
    let session = Arc::new(ClusterSession::new(cp));
    self.cluster_sessions.write().push(Arc::downgrade(&session));
    session
  }

  fn allow_data_loss(&self) -> bool {
    false
  }

  fn flush_config(&self) {
    if let Some(mgr) = self.cluster_manager() {
      mgr.flush_config();
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:GetGossipStats
  fn get_gossip_stats(&self, metrics_disabled: bool) -> Vec<MetricsItem> {
    if let Some(gm) = self.gossip_manager() {
      let open_conns = gm.connection_store.count();
      gm.stats.to_metrics_items(metrics_disabled, open_conns)
    } else {
      Vec::new()
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:GetReplicationInfo
  fn get_replication_info(&self) -> Vec<MetricsItem> {
    let Some(rm) = self.replication_manager() else {
      return Vec::new();
    };
    let is_pri = self.is_primary();
    let role = if is_pri { "master" } else { "slave" };
    let failover_status = self
      .failover_manager()
      .map(|fm| fm.get_failover_status())
      .unwrap_or_else(|| "no-failover".to_string());
    let last_failover_status = self
      .failover_manager()
      .map(|fm| fm.get_last_failover_status())
      .unwrap_or_else(|| "no-failover".to_string());
    let cur_offset = rm.get_current_replication_offset().to_aof_string();
    let offset2 = rm.get_replication_offset2().to_aof_string();
    let rec_status: &'static str = rm.recovery_status().into();
    let connected_slaves = rm.aof_sync_driver_store.count_connected_replicas();
    let sync_driver_count = rm.aof_sync_driver_store.count();

    let mut num_buf = Buffer::new();
    let mut items = vec![
      MetricsItem::new("role", role),
      MetricsItem::new("connected_slaves", num_buf.format(connected_slaves)),
      MetricsItem::new("master_failover_state", failover_status),
      MetricsItem::new("master_replid", rm.primary_repl_id()),
      MetricsItem::new("master_replid2", rm.primary_repl_id2()),
      MetricsItem::new("master_repl_offset", cur_offset.clone()),
      MetricsItem::new("second_repl_offset", offset2),
      MetricsItem::new(
        "store_current_safe_aof_address",
        rm.get_current_safe_aof_address().to_aof_string(),
      ),
      MetricsItem::new(
        "store_recovered_safe_aof_address",
        rm.get_recovered_safe_aof_address().to_aof_string(),
      ),
      MetricsItem::new("recover_status", rec_status),
      MetricsItem::new("last_failover_state", last_failover_status),
      MetricsItem::new("sync_driver_count", num_buf.format(sync_driver_count)),
    ];
    if !is_pri && let Some(cm) = self.cluster_manager() {
      let config = cm.current_config();
      let (addr, port) = config.get_local_node_primary_address();
      if let Some(a) = addr {
        items.push(MetricsItem::new("master_host", a));
      }
      items.push(MetricsItem::new("master_port", num_buf.format(port)));
      let link_status = cm.get_primary_link_status(&config);
      items.push(link_status[0].clone());
      items.push(link_status[1].clone());
      items.push(MetricsItem::new(
        "master_sync_in_progress",
        rm.is_recovering().to_string(),
      ));
      items.push(MetricsItem::new("slave_read_repl_offset", cur_offset));
      items.push(MetricsItem::new("slave_priority", "100"));
      items.push(MetricsItem::new("slave_read_only", "1"));
      items.push(MetricsItem::new("replica_announced", "1"));
      items.push(MetricsItem::new(
        "master_sync_last_io_seconds_ago",
        num_buf.format(rm.last_primary_sync_seconds()),
      ));
      // libs/cluster/Server/ClusterProvider.cs:255-259（副本侧滞后指标组）：
      // 日志尾与复制偏移的向量/聚合差、重放滞后上限与物理子日志重放进度向量
      let (vec_lag, acc_lag, sublog_vector, drift_vector) = match self
        .try_aof()
        .map(|aof| (aof.log().tail_address(), aof.read_consistency_manager()))
      {
        Some((tail, rcm)) => {
          let offset = rm.get_current_replication_offset();
          let sublog_vector = rcm.as_ref().map_or_else(
            || "-1".to_string(),
            |m| m.get_physical_sublog_max_sequence_vector(),
          );
          let drift_vector = rcm.as_ref().map_or_else(
            || "-1".to_string(),
            |m| m.get_physical_sublog_max_drift_sequence_vector(),
          );
          (
            tail.diff(&offset).to_aof_string(),
            tail.aggregate_diff(&offset).to_string(),
            sublog_vector,
            drift_vector,
          )
        }
        // AOF 门控未点亮：无复制滞后面（C# 禁用 AOF 时 appendOnlyFile 同样
        // 恒零输出）
        None => (
          "0".to_string(),
          "0".to_string(),
          "-1".to_string(),
          "-1".to_string(),
        ),
      };
      items.push(MetricsItem::new("replication_offset_vector_lag", vec_lag));
      items.push(MetricsItem::new("replication_offset_acc_lag", acc_lag));
      items.push(MetricsItem::new(
        "aof_replay_max_lag_bytes",
        self.aof_replay_max_lag_bytes.load(Ordering::Relaxed).to_string(),
      ));
      items.push(MetricsItem::new(
        "physical_sublog_max_sequence_vector",
        sublog_vector,
      ));
      items.push(MetricsItem::new(
        "physical_sublog_max_drift_sequence_vector",
        drift_vector,
      ));
    } else {
      // slave0: ip=...,port=...,state=online,offset=...,lag=...（对标 C# 逐副本条目）
      let primary_offset = rm.get_current_replication_offset();
      for (i, info) in rm
        .aof_sync_driver_store
        .get_replica_info(&primary_offset)
        .into_iter()
        .enumerate()
      {
        items.push(MetricsItem::new(
          format!("slave{i}"),
          format!(
            "node_id={},state={},offset={},lag={}",
            info.node_id,
            if info.is_connected {
              "online"
            } else {
              "offline"
            },
            info.replication_offset.to_aof_string(),
            info.replication_lag.to_aof_string()
          ),
        ));
      }
    }
    items
  }

  /// libs/cluster/Server/ClusterProvider.cs:GetBufferPoolStats
  fn get_buffer_pool_stats(&self) -> Vec<MetricsItem> {
    vec![
      MetricsItem::new(
        "migration_manager",
        self
          .migration_manager()
          .map(|mm| mm.get_buffer_pool_stats())
          .unwrap_or_default(),
      ),
      MetricsItem::new(
        "replication_manager",
        self
          .replication_manager()
          .map(|rm| rm.get_buffer_pool_stats())
          .unwrap_or_default(),
      ),
    ]
  }

  /// libs/cluster/Server/ClusterProvider.cs:GetPrimaryInfo
  ///
  /// PRIMARY 视角：返回当前复制位点与全副本元数据列表
  ///（address/port 经 ClusterConfig.GetWorkerAddressFromNodeId 反查，
  ///对标 C# AofSyncDriverStore.GetReplicaInfo 逐字段填充）
  fn get_primary_info(&self) -> (AofAddress, Vec<RoleInfo>) {
    let Some(rm) = self.replication_manager() else {
      return (AofAddress::default(), Vec::new());
    };
    let offset = rm.get_current_replication_offset();
    let cm = self.cluster_manager();
    let replicas = rm
      .aof_sync_driver_store
      .get_replica_info(&offset)
      .into_iter()
      .map(|info| {
        let (address, port) = cm
          .as_ref()
          .map(|cm| {
            cm.current_config()
              .get_worker_address_from_node_id(&info.node_id)
          })
          .unwrap_or((None, 0));
        RoleInfo {
          replication_offset: info.replication_offset.get(0).unwrap_or(0),
          replication_lag: info.replication_lag.get(0).unwrap_or(0),
          replication_state: if info.is_connected {
            "online"
          } else {
            "offline"
          }
          .into(),
          address: address.unwrap_or_default(),
          port,
          ..RoleInfo::default()
        }
      })
      .collect();
    (offset, replicas)
  }

  /// libs/cluster/Server/ClusterProvider.cs:GetReplicaInfo
  ///
  /// REPLICA 视角：address/port 取 LocalNodePrimaryAddress，状态由
  /// IsRecovering("sync") / gossip 连接("connected"/"connect") 判定
  fn get_replica_info(&self) -> RoleInfo {
    let Some(rm) = self.replication_manager() else {
      return RoleInfo::default();
    };
    let Some(cm) = self.cluster_manager() else {
      return RoleInfo::default();
    };
    let config = cm.current_config();
    let (address, port) = config.get_local_node_primary_address();
    let connected = config
      .local_node_primary_id()
      .is_some_and(|id| cm.get_connection_info(id).connected);
    RoleInfo {
      replication_offset: rm.get_replication_offset(0),
      replication_state: if rm.is_recovering() {
        "sync"
      } else if connected {
        "connected"
      } else {
        "connect"
      }
      .into(),
      address: address.unwrap_or_default(),
      port,
      ..RoleInfo::default()
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:PurgeBufferPool
  ///
  /// MigrationManager → migrationManager.Purge()；ReplicationManager →
  /// replicationManager.Purge()；其余（ServerListener）对标 C# GarnetException 拒绝
  fn purge_buffer_pool(&self, manager_type: ManagerType) {
    match manager_type {
      ManagerType::MigrationManager => {
        if let Some(mm) = self.migration_manager() {
          mm.purge();
        }
      }
      ManagerType::ReplicationManager => {
        if let Some(rm) = self.replication_manager() {
          rm.purge();
        }
      }
      ManagerType::ServerListener => {
        log::error!("PURGEBP: ServerListener buffer pool purge is not supported");
      }
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:ClusterPublishAsync
  async fn cluster_publish_async<'a>(
    &'a self,
    cmd: RespCommand,
    channel: &'a [u8],
    message: &'a [u8],
  ) {
    if let Some(mgr) = self.cluster_manager() {
      mgr.try_cluster_publish_async(cmd, channel, message).await;
    }
  }

  fn is_primary(&self) -> bool {
    self
      .cluster_manager()
      .map(|mgr| mgr.current_config().is_primary())
      .unwrap_or(true)
  }

  fn is_replica(&self) -> bool {
    let role_is_replica = self
      .cluster_manager()
      .map(|mgr| mgr.current_config().is_replica())
      .unwrap_or(false);
    let recovering = self
      .replication_manager()
      .map(|rm| rm.is_recovering())
      .unwrap_or(false);
    role_is_replica || recovering
  }

  fn is_replica_node(&self, node_id: &str) -> bool {
    self
      .cluster_manager()
      .map(|mgr| {
        mgr
          .current_config()
          .workers
          .iter()
          .any(|w| w.nodeid.as_deref() == Some(node_id) && w.role == NodeRole::Replica)
      })
      .unwrap_or(false)
  }

  async fn recover_async(&self) {
    if let Some(mgr) = self.cluster_manager() {
      mgr.start();
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:ResetGossipStats
  fn reset_gossip_stats(&self) {
    if let Some(gm) = self.gossip_manager() {
      gm.stats.reset();
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:SafeTruncateAOF
  ///
  /// PRIMARY：经 AofSyncDriverStore 按全副本最小已发位点安全截断（记账 +
  /// 注入日志的物理截断，见 AofSyncDriverStore::safe_truncate_aof）；
  /// REPLICA：物理截断本地 AOF 至指定位点（C# else 分支
  /// `storeWrapper.appendOnlyFile?.Log.TruncateUntil(truncateUntil)`——
  /// 副本无 Commit，刷盘由复制流驱动）
  fn safe_truncate_aof(&self, truncate_until: &AofAddress) {
    let Some(rm) = self.replication_manager() else {
      return;
    };
    if self.is_primary() {
      rm.aof_sync_driver_store.safe_truncate_aof(truncate_until);
    } else if let Some(aof) = self.try_aof() {
      aof.log().truncate_until(truncate_until);
    }
  }

  fn start(&self) {
    if let Some(mgr) = self.cluster_manager() {
      mgr.start();
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:UpdateClusterAuth
  fn update_cluster_auth(
    &self,
    cluster_username: Option<String>,
    cluster_password: Option<String>,
  ) {
    let mut auth = self.auth_container.write();
    let old_user = auth.0.clone();
    *auth = (cluster_username.or(old_user), cluster_password);
  }

  /// libs/cluster/Server/ClusterProvider.cs:GetCheckpointInfo
  ///
  /// 收集检查点监控指标（输出至 Redis `INFO CINFO` 与 `INFO ALL` 监控段）：
  /// - `memory_checkpoint_entry`: 当前内存复制链表中正在使用的最新检查点条目（活跃复制流基准）
  /// - `disk_checkpoint_entry`: 磁盘持久化检查点信息（对标 C# 盘上扫描；wedb 中因 CPR cookie 不落盘恒为 `(empty)`）
  fn get_checkpoint_info(&self) -> Vec<MetricsItem> {
    if let Some(rm) = self.replication_manager() {
      vec![
        MetricsItem::new(
          "memory_checkpoint_entry",
          rm.get_latest_checkpoint_from_memory_info(),
        ),
        MetricsItem::new(
          "disk_checkpoint_entry",
          rm.get_latest_checkpoint_from_disk_info(),
        ),
      ]
    } else {
      vec![
        MetricsItem::new("memory_checkpoint_entry", "(empty)"),
        MetricsItem::new("disk_checkpoint_entry", "(empty)"),
      ]
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:GetRunId
  fn get_run_id(&self) -> String {
    if let Some(rm) = self.replication_manager() {
      rm.primary_repl_id()
    } else {
      self
        .cluster_manager()
        .and_then(|mgr| mgr.current_config().local_node_id().map(String::from))
        .unwrap_or_default()
    }
  }

  fn prevent_role_change(&self) -> bool {
    if let Some(rm) = self.replication_manager() {
      rm.begin_recovery(RecoveryStatus::ReadRole, false)
    } else {
      true
    }
  }

  fn allow_role_change(&self) {
    if let Some(rm) = self.replication_manager() {
      rm.end_recovery(RecoveryStatus::NoRecovery, false);
    }
  }
}

/// 检查点回调切面（对标 C# ClusterProvider 对 IClusterProvider 检查点方法族的实现）
impl CheckpointCallbackFace for ClusterProvider {
  /// libs/server/Cluster/IClusterProvider.cs:OnCheckpointInitiated
  ///
  /// REPLICA 取检查点开始标记偏移（ReplicationCheckpointStartOffset），PRIMARY 取当前复制位点
  ///
  /// 与 C# `EnableAOF && clusterManager.CurrentConfig.LocalNodeRole == NodeRole.REPLICA`
  /// 保持一致：仅按配置角色（`local_node_role()`）判定，不看恢复态。
  /// 不可用 `is_replica()`——它叠加了 `replication_manager.is_recovering()`，
  /// 会让主节点恢复期误入副本分支，错取 ReplicationCheckpointStartOffset
  /// （那是副本检查点开始标记的截断位点，主节点语义完全不同）。
  fn on_checkpoint_initiated(&self, checkpoint_covered_aof_address: &mut AofAddress) {
    if let Some(rm) = self.replication_manager() {
      // 对标 C# CurrentConfig.LocalNodeRole == NodeRole.REPLICA 的配置角色直判
      let replica_by_config = self
        .cluster_manager()
        .is_some_and(|mgr| mgr.current_config().local_node_role() == NodeRole::Replica);
      if replica_by_config {
        *checkpoint_covered_aof_address = rm.get_replication_checkpoint_start_offset();
      } else {
        *checkpoint_covered_aof_address = rm.get_current_replication_offset();
      }
      rm.update_commit_safe_aof_address(checkpoint_covered_aof_address);
    }
  }

  /// libs/server/Cluster/IClusterProvider.cs:AddNewCheckpointEntry
  ///
  /// 登记新检查点条目到内存仓库并安全截断 AOF（对标 C# 逐字段构造）
  /// 保留 _object_store_checkpoint_token 形参以对标 IClusterProvider.AddNewCheckpointEntry 接口签名
  fn add_new_checkpoint_entry(
    &self,
    full: bool,
    checkpoint_covered_aof_address: AofAddress,
    store_checkpoint_token: u128,
    // 保留 _object_store_checkpoint_token 形参以对标 IClusterProvider.AddNewCheckpointEntry 接口签名
    _object_store_checkpoint_token: u128,
  ) {
    if let Some(rm) = self.replication_manager() {
      let mut metadata = CheckpointMetadata::new(rm.sublog_count());
      metadata.store_version = checkpoint_version(store_checkpoint_token);
      metadata.store_hlog_token = store_checkpoint_token;
      metadata.store_index_token = store_checkpoint_token;
      metadata.store_checkpoint_covered_aof_address = checkpoint_covered_aof_address;
      metadata.store_primary_repl_id = Some(rm.primary_repl_id());

      // 供副本跟踪检查点历史：attach 新主时据此清理旧检查点
      rm.add_checkpoint_entry(CheckpointEntry::new(metadata), full);
    }
    self.safe_truncate_aof(&checkpoint_covered_aof_address);
  }
}

impl WnodeClusterProvider for ClusterProvider {
  #[inline]
  fn is_cluster_enabled(&self) -> bool {
    true
  }

  #[inline]
  fn start(&self) {
    IClusterProvider::start(self);
  }

  #[inline]
  fn flush_config(&self) {
    IClusterProvider::flush_config(self);
  }

  #[inline]
  fn update_cluster_auth(&self, username: Option<String>, password: Option<String>) {
    IClusterProvider::update_cluster_auth(self, username, password);
  }

  #[inline]
  fn is_primary(&self) -> bool {
    IClusterProvider::is_primary(self)
  }

  #[inline]
  fn is_replica(&self) -> bool {
    IClusterProvider::is_replica(self)
  }

  #[inline]
  fn is_replica_node(&self, node_id: &str) -> bool {
    IClusterProvider::is_replica_node(self, node_id)
  }

  #[inline]
  fn get_run_id(&self) -> String {
    IClusterProvider::get_run_id(self)
  }

  #[inline]
  fn get_primary_info(&self) -> (AofAddress, Vec<RoleInfo>) {
    IClusterProvider::get_primary_info(self)
  }

  #[inline]
  fn get_replica_info(&self) -> RoleInfo {
    IClusterProvider::get_replica_info(self)
  }

  #[inline]
  fn get_replication_info(&self) -> Vec<MetricsItem> {
    IClusterProvider::get_replication_info(self)
  }

  #[inline]
  fn get_checkpoint_info(&self) -> Vec<MetricsItem> {
    IClusterProvider::get_checkpoint_info(self)
  }

  #[inline]
  fn get_gossip_stats(&self, metrics_disabled: bool) -> Vec<MetricsItem> {
    IClusterProvider::get_gossip_stats(self, metrics_disabled)
  }

  #[inline]
  fn get_buffer_pool_stats(&self) -> Vec<MetricsItem> {
    IClusterProvider::get_buffer_pool_stats(self)
  }

  #[inline]
  fn purge_buffer_pool(&self, manager_type: ManagerType) {
    IClusterProvider::purge_buffer_pool(self, manager_type);
  }

  #[inline]
  fn reset_gossip_stats(&self) {
    IClusterProvider::reset_gossip_stats(self);
  }
}
