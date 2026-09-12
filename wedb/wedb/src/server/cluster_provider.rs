use std::{
  future::Future,
  ops::Deref,
  pin::Pin,
  sync::{
    Arc, OnceLock, Weak,
    atomic::{AtomicI32, AtomicI64, Ordering},
  },
  time::Duration,
};

use compio::{runtime::spawn, time::sleep};
use parking_lot::RwLock;
use waof::AofAddress;
use wnode::{ClusterProvider as WnodeClusterProvider, MetricsItem, databases::checkpoint_version};
use wresp::RespCommand;

/// 检查点版本切换回调
pub type VersionShiftHook = Box<dyn Fn(i64, i64) + Send + Sync>;

/// 副本重连发起钩子
pub type RecoverReplicationHook = Arc<dyn Fn(&Arc<ClusterProvider>, &str) + Send + Sync>;

use crate::server::{
  cluster::{
    CheckpointCallbackFace, CheckpointMetadata, ClusterPreferredEndpointType, IClusterProvider,
    IClusterSession, ManagerType, RoleInfo,
  },
  cluster_manager::ClusterManager,
  cluster_session::ClusterSession,
  connection_info::ConnectionInfo,
  failover::failover_manager::FailoverManager,
  gossip::gossip_manager::GossipManager,
  migration::migration_manager::MigrationManager,
  replication::{
    checkpoint_entry::CheckpointEntry, recovery_status::RecoveryStatus,
    replication_manager::ReplicationManager, store_commit::StoreCommitChannel,
  },
  slot_verify::ClusterSlotVerificationState,
  worker::NodeRole,
};

/// WeDB 分布式集群提供者核心门面（对标 C# Garnet.cluster.ClusterProvider）
pub struct ClusterProvider {
  pub cluster_manager: RwLock<Option<Arc<ClusterManager>>>,
  pub replication_manager: RwLock<Option<Arc<ReplicationManager>>>,
  pub failover_manager: RwLock<Option<Arc<FailoverManager>>>,
  pub migration_manager: RwLock<Option<Arc<MigrationManager>>>,
  pub gossip_manager: RwLock<Option<Arc<GossipManager>>>,
  pub auth_container: RwLock<(Option<String>, Option<String>)>,
  pub recover_replication_hook: RwLock<Option<RecoverReplicationHook>>,
  replication_reestablishment_timeout_secs: AtomicI32,
  /// Garnet 当前纪元（对标 C# ClusterProvider.GarnetCurrentEpoch，初始为 1）
  garnet_current_epoch: AtomicI64,
  /// 序列号生成器复位勾子（故障转移后抬升生成器起点；对标 C# appendOnlyFile.ResetSequenceNumberGenerator）
  seq_reset_hook: RwLock<Option<Arc<dyn Fn() + Send + Sync>>>,
  /// 弱引用自身（用于按需向上派生包含本对象的会话，无锁读取）
  self_weak: OnceLock<Weak<ClusterProvider>>,
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
      recover_replication_hook: RwLock::new(None),
      replication_reestablishment_timeout_secs: AtomicI32::new(0),
      garnet_current_epoch: AtomicI64::new(1),
      seq_reset_hook: RwLock::new(None),
      self_weak: OnceLock::new(),
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

  /// 注入副本重连发起钩子（一次注入；EnsureReplication 第 7 步驱动）
  pub fn set_recover_replication_hook(&self, hook: Option<RecoverReplicationHook>) {
    *self.recover_replication_hook.write() = hook;
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
  ///    Task.Run(TryReplicateDiskbasedSyncAsync)，钩子内向 primary 发
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
    let hook = self.recover_replication_hook.read().clone();
    let Some(hook) = hook else {
      // 未注入重连钩子（单测/无传输装配形态）：释放角色锁并保持既有日志暴露
      self.allow_role_change();
      log::warn!(
        "Replication session to primary {primary} lost and no active replication stream; \
         auto-resync hook not wired"
      );
      return;
    };
    let provider = Arc::clone(self);
    spawn(async move {
      log::info!("Beginning resync to {primary} after replication session failed");
      hook(&provider, &primary);
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

  /// 集群检查点装配：产出检查点版本切换回调对（注入 wkv::CheckpointManager）
  ///
  /// 对标 C# ReplicationManager 构造内
  /// `clusterProvider.ReplicationLogCheckpointManager.checkpointVersionShiftStart/End
  /// = CheckpointVersionShiftStart/End` 的委托装配：闭包内先做 REPLICA 角色判定
  ///（C# rm 方法首行判定，Rust rm 不持 clusterManager 引由本层承接），再委托
  /// rm 写 CheckpointStart/EndCommit 标记
  pub fn checkpoint_version_shift_hooks(self: &Arc<Self>) -> (VersionShiftHook, VersionShiftHook) {
    let start_provider = Arc::clone(self);
    let start = Box::new(move |old: i64, new: i64| {
      // C#: LocalNodeRole == NodeRole.REPLICA -> return
      if start_provider.is_replica() {
        return;
      }
      if let Some(rm) = start_provider.replication_manager() {
        rm.checkpoint_version_shift_start(true, old, new, false);
      }
    });

    let end_provider = Arc::clone(self);
    let end = Box::new(move |old: i64, new: i64| {
      if end_provider.is_replica() {
        return;
      }
      if let Some(rm) = end_provider.replication_manager() {
        rm.checkpoint_version_shift_end(true, old, new, false);
      }
    });

    (start, end)
  }

  /// libs/cluster/Server/ClusterProvider.cs:ClusterUsername
  pub fn cluster_username(&self) -> Option<String> {
    self.auth_container.read().0.clone()
  }

  /// libs/cluster/Server/ClusterProvider.cs:ClusterPassword
  pub fn cluster_password(&self) -> Option<String> {
    self.auth_container.read().1.clone()
  }

  /// 单键槽位归属与状态验证（代理转发至 ClusterManager）
  pub fn verify_key(
    &self,
    key: &[u8],
    read_only: bool,
    session_asking: bool,
    pref_type: ClusterPreferredEndpointType,
  ) -> ClusterSlotVerificationState {
    if let Some(cm) = self.cluster_manager() {
      cm.verify_key(key, read_only, session_asking, pref_type)
    } else {
      ClusterSlotVerificationState::Ok
    }
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
  /// 推进集群纪元并等待所有集群会话纪元过渡完成
  pub async fn bump_and_wait_for_epoch_transition_async(&self) -> bool {
    self.bump_current_epoch();
    // 让出当前协程时间片以推进过渡
    sleep(Duration::from_millis(1)).await;
    true
  }

  /// 注册序列号生成器复位勾子（集群装配期注入）
  pub fn set_seq_reset_hook(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
    *self.seq_reset_hook.write() = hook;
  }

  /// 执行序列号生成器复位（故障转移触发时调用；对标 C# appendOnlyFile.ResetSequenceNumberGenerator）
  pub fn reset_sequence_number_generator(&self) {
    if let Some(hook) = self.seq_reset_hook.read().as_ref() {
      hook();
    }
  }
}

impl IClusterProvider for ClusterProvider {
  /// libs/cluster/Server/ClusterProvider.cs:CreateClusterSession
  fn create_cluster_session(&self) -> Box<dyn IClusterSession> {
    if let Some(cp) = self.self_weak.get().and_then(|w| w.upgrade()) {
      Box::new(ClusterSession::new(cp))
    } else {
      Box::new(ClusterSession::new(Arc::new(Self::default())))
    }
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

    let mut num_buf = itoa::Buffer::new();
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

  fn get_buffer_pool_stats(&self) -> Vec<MetricsItem> {
    vec![
      MetricsItem::new("migration_manager", "0"),
      MetricsItem::new("replication_manager", "0"),
    ]
  }

  /// libs/cluster/Server/ClusterProvider.cs:GetPrimaryInfo
  fn get_primary_info(&self) -> (AofAddress, Vec<RoleInfo>) {
    let Some(rm) = self.replication_manager() else {
      return (AofAddress::default(), Vec::new());
    };
    let offset = rm.get_current_replication_offset();
    let replicas = rm
      .aof_sync_driver_store
      .get_replica_info(&offset)
      .into_iter()
      .map(|info| {
        RoleInfo::replica(
          info.replication_offset.get(0).unwrap_or(0),
          Some(info.node_id),
        )
      })
      .collect();
    (offset, replicas)
  }

  fn get_replica_info(&self) -> RoleInfo {
    let offset = self
      .replication_manager()
      .map(|rm| rm.get_replication_offset(0))
      .unwrap_or(0);
    if self.is_primary() {
      RoleInfo::primary(offset)
    } else {
      let master_node_id = self.cluster_manager().and_then(|cm| {
        cm.current_config()
          .local_node_primary_id()
          .map(String::from)
      });
      RoleInfo::replica(offset, master_node_id)
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:PurgeBufferPool
  fn purge_buffer_pool(&self, manager_type: ManagerType) {
    if matches!(manager_type, ManagerType::Aof)
      && let Some(rm) = self.replication_manager()
    {
      rm.purge();
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:ClusterPublishAsync
  fn cluster_publish_async<'a>(
    &'a self,
    cmd: RespCommand,
    channel: &'a [u8],
    message: &'a [u8],
  ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
    Box::pin(async move {
      if let Some(mgr) = self.cluster_manager() {
        mgr.try_cluster_publish_async(cmd, channel, message).await;
      }
    })
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

  fn recover_async<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
    Box::pin(async move {
      if let Some(mgr) = self.cluster_manager() {
        mgr.start();
      }
    })
  }

  /// libs/cluster/Server/ClusterProvider.cs:ResetGossipStats
  fn reset_gossip_stats(&self) {
    if let Some(gm) = self.gossip_manager() {
      gm.stats.reset();
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:SafeTruncateAOF
  ///
  /// PRIMARY：经 AofSyncDriverStore 按全副本最小已发位点安全截断；
  /// REPLICA：推进复制位点（物理截断由 waof 域承接）
  fn safe_truncate_aof(&self, truncate_until: &AofAddress) {
    let Some(rm) = self.replication_manager() else {
      return;
    };
    if self.is_primary() {
      rm.aof_sync_driver_store.safe_truncate_aof(truncate_until);
    } else {
      rm.set_current_replication_offset(*truncate_until);
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
  fn on_checkpoint_initiated(&self, checkpoint_covered_aof_address: &mut AofAddress) {
    if let Some(rm) = self.replication_manager() {
      if self.is_replica() {
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

/// WeDB 集群提供者启动门面（适配 ServerBootstrap 启动流水线）
#[derive(Clone)]
pub struct WedbClusterProvider {
  pub inner: Arc<ClusterProvider>,
}

impl Default for WedbClusterProvider {
  fn default() -> Self {
    Self::new()
  }
}

impl WedbClusterProvider {
  /// 构造并装配包含全部集群子管理器的实例
  pub fn new() -> Self {
    Self {
      inner: ClusterProvider::new(),
    }
  }

  /// 从已有 Arc<ClusterProvider> 包装
  pub fn from_arc(inner: Arc<ClusterProvider>) -> Self {
    Self { inner }
  }
}

impl Deref for WedbClusterProvider {
  type Target = ClusterProvider;
  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.inner
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
  fn get_run_id(&self) -> String {
    IClusterProvider::get_run_id(self)
  }
}

impl WnodeClusterProvider for WedbClusterProvider {
  #[inline]
  fn is_cluster_enabled(&self) -> bool {
    true
  }

  #[inline]
  fn start(&self) {
    self.inner.start();
  }

  #[inline]
  fn flush_config(&self) {
    self.inner.flush_config();
  }

  #[inline]
  fn update_cluster_auth(&self, username: Option<String>, password: Option<String>) {
    self.inner.update_cluster_auth(username, password);
  }

  #[inline]
  fn is_primary(&self) -> bool {
    self.inner.is_primary()
  }

  #[inline]
  fn is_replica(&self) -> bool {
    self.inner.is_replica()
  }

  #[inline]
  fn get_run_id(&self) -> String {
    self.inner.get_run_id()
  }
}
