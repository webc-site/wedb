//! 集群提供者的契约实现面：`IClusterProvider`、`CheckpointCallbackFace`、
//! wnode 侧 `ClusterProvider` 三个 trait 的 impl 段
//! （对标 C# ClusterProvider 对 IClusterProvider 与提供方契约的实现部分）

use std::sync::Arc;

use itoa::Buffer;
use waof::AofAddress;
use wbase::hex::hex_str_u128;
use wnode::{
  ClusterProvider as WnodeClusterProvider, RoleInfo, database::checkpoint_version,
  resp::slow_path::SlowFuture, session_parse_state_extensions::ManagerType,
};
use wresp::{command::RespCommand, ext::RespVecExt, metrics::MetricsItem};

use crate::server::{
  cluster::{CheckpointCallbackFace, CheckpointMetadata, IClusterProvider},
  cluster_provider::ClusterProvider,
  cluster_session::ClusterSession,
  hash_slot::SlotState,
  replication::{checkpoint_entry::CheckpointEntry, recovery_status::RecoveryStatus},
  worker::NodeRole,
};

impl IClusterProvider for ClusterProvider {
  /// libs/cluster/Server/ClusterProvider.cs:CreateClusterSession
  /// libs/server/Cluster/IClusterProvider.cs:CreateClusterSession
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
    // 键取 Arc 指针地址（Weak::as_ptr 同址）：无锁登记，清扫按此键移除
    self
      .cluster_sessions
      .pin()
      .insert(Arc::as_ptr(&session) as usize, Arc::downgrade(&session));
    session
  }

  fn allow_data_loss(&self) -> bool {
    ClusterProvider::allow_data_loss(self)
  }

  /// libs/cluster/Server/ClusterProvider.cs:ClusterPublishAsync
  /// libs/server/Cluster/IClusterProvider.cs:ClusterPublishAsync
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

  /// libs/cluster/Server/ClusterProvider.cs:SafeTruncateAOF
  /// libs/server/Cluster/IClusterProvider.cs:SafeTruncateAOF
  ///
  /// PRIMARY：经 AofSyncDriverStore 按全副本最小已发位点安全截断（记账 + 走
  /// [`GarnetLog::truncate_until_async`] 唯一物理回收真身即时删段，见
  /// AofSyncDriverStore::safe_truncate_aof）；
  /// REPLICA：物理截断本地 AOF 至指定位点。C# 该分支按 FastAofTruncate 二择
  /// （真 `Log.UnsafeShiftBeginAddress(truncateUntil, truncateLog: true)` 即时删段 /
  /// 假 `Log.TruncateUntil(truncateUntil)` 逻辑截断）；rust 依方案 B 取单一口径恒走
  /// 物理真身——提交面不删段，逻辑截断永不落盘；副本无 Commit，刷盘由复制流驱动。
  async fn safe_truncate_aof(&self, truncate_until: &AofAddress) {
    let Some(rm) = self.replication_manager() else {
      return;
    };
    if self.is_primary() {
      rm.aof_sync_driver_store
        .safe_truncate_aof(truncate_until)
        .await;
    } else if let Some(aof) = self.try_aof() {
      aof.log().truncate_until_async(truncate_until).await;
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:PreventRoleChange
  /// libs/server/Cluster/IClusterProvider.cs:PreventRoleChange
  ///
  /// 角色变更前置：取 ReadRole 恢复态（C# BeginRecovery(ReadRole, upgradeLock:
  /// false) 同参），返 false 表示未获准、调用方不得改角色
  fn prevent_role_change(&self) -> bool {
    if let Some(rm) = self.replication_manager() {
      rm.begin_recovery(RecoveryStatus::ReadRole, false)
    } else {
      true
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:AllowRoleChange
  /// libs/server/Cluster/IClusterProvider.cs:AllowRoleChange
  ///
  /// 解除角色变更封锁（C# EndRecovery(NoRecovery, downgradeLock: false) 同参）
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
  async fn add_new_checkpoint_entry(
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
    self
      .safe_truncate_aof(&checkpoint_covered_aof_address)
      .await;
  }
}

impl WnodeClusterProvider for ClusterProvider {
  #[inline]
  fn is_cluster_enabled(&self) -> bool {
    true
  }

  /// 槽位本地掌管且处于 Stable 态的判定（SWAPDB 库级门禁消费面）：
  /// 属主半边走 C# ClusterConfig.IsLocal 的写面口径，read_write_session 恒 false
  ///（副本不因读放行而获得换库资格，换库为写操作）；状态半边走
  /// ClusterConfig.GetState 单点——Migrating 槽 eff 属主恒本地、Importing 槽
  /// 目标端亦判本地，迁移窗口内换号与在途搬迁必须互斥，非 Stable 一律拒绝
  fn is_slot_local_stable(&self, slot: u16) -> bool {
    self.cluster_manager().is_some_and(|cm| {
      let config = cm.current_config();
      config.is_local(slot, false) && config.get_state(slot) == SlotState::Stable
    })
  }

  /// libs/cluster/Server/ClusterProvider.cs:Start
  /// libs/server/Cluster/IClusterProvider.cs:Start
  ///
  /// 启动集群后台治理与复制：clusterManager.Start() 拉起 gossip 半段 +
  /// replicationManager.Start() 启动期主动 attach 半段（重启后 REPLICA 首帧前
  /// 接入，二者均已在 compio 运行时内，由 GarnetServer.Start → Provider.Start 调用）
  fn start(&self) {
    if let Some(mgr) = self.cluster_manager() {
      mgr.start();
    }
    self.start_replication_attach();
  }

  /// libs/cluster/Server/ClusterProvider.cs:FlushConfig
  /// libs/server/Cluster/IClusterProvider.cs:FlushConfig
  ///
  /// 清空集群配置面（C# clusterManager?.FlushConfig() 同名委托）
  fn flush_config(&self) {
    if let Some(mgr) = self.cluster_manager() {
      mgr.flush_config();
    }
  }

  #[inline]
  fn dispose(&self) {
    self.dispose();
  }

  /// libs/cluster/Server/ClusterProvider.cs:UpdateClusterAuth
  fn update_cluster_auth(
    &self,
    cluster_username: Option<String>,
    cluster_password: Option<String>,
  ) {
    self.update_cluster_auth(cluster_username, cluster_password);
  }

  /// CONFIG SET cluster-node-timeout 调停投影落点：直写本 provider 原子槽
  ///（gossip / failover / 集群管理全部消费面经 cluster_node_timeout() 单点
  /// 即时读取，对标 Gossip.cs:25 / FailoverManager.cs:24 每轮 GetTimeSpan
  /// (CLUSTER_NODE_TIMEOUT) 现取）；0 = 无限超时哨兵
  fn set_cluster_node_timeout_ms(&self, ms: u64) {
    self.set_cluster_node_timeout_ms(ms);
  }

  #[inline]
  fn is_device_contaminated(&self) -> bool {
    self.is_device_contaminated()
  }

  /// libs/cluster/Server/ClusterProvider.cs:IsPrimary
  /// libs/server/Cluster/IClusterProvider.cs:IsPrimary
  ///
  /// 配置角色是否 PRIMARY（C# CurrentConfig.LocalNodeRole == PRIMARY；无集群
  /// 管理器形态取 true，与 C# 单机 provider 恒主同语义）
  fn is_primary(&self) -> bool {
    self
      .cluster_manager()
      .map(|mgr| mgr.current_config().is_primary())
      .unwrap_or(true)
  }

  /// libs/cluster/Server/ClusterProvider.cs:IsReplica
  /// libs/server/Cluster/IClusterProvider.cs:IsReplica
  ///
  /// 角色 REPLICA 或处于恢复期即为真（C# `LocalNodeRole == REPLICA ||
  /// replicationManager.IsRecovering` 同式）；只看配置角色的判定请用
  /// local_node_role，勿改本件语义（检查点面依赖该区分）
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

  fn is_replica_node(&self, node_id: u128) -> bool {
    self
      .cluster_manager()
      .map(|mgr| {
        mgr
          .current_config()
          .workers
          .iter()
          .any(|w| w.nodeid == Some(node_id) && w.role == NodeRole::Replica)
      })
      .unwrap_or(false)
  }

  /// libs/cluster/Server/ClusterProvider.cs:GetRunId
  fn get_run_id(&self) -> String {
    if let Some(rm) = self.replication_manager() {
      rm.primary_repl_id()
    } else {
      self
        .cluster_manager()
        .and_then(|mgr| mgr.current_config().local_node_id())
        .map_or_else(String::new, hex_str_u128)
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:GetPrimaryInfo
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
              .get_worker_address_from_node_id(info.node_id)
          })
          .unwrap_or((None, 0));
        RoleInfo {
          replication_offset: info.replication_offset.get(0).unwrap_or(0),
          replication_offset_vector: info.replication_offset.to_aof_string(),
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
  /// libs/server/Cluster/IClusterProvider.cs:GetReplicaInfo
  ///
  /// 本节点作为副本的角色信息：位点 + 指向 primary 的连接态
  /// （connect / connected / sync 三态与 C# 同式，地址端口取自配置主指）
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
    let addr = rm.get_current_replication_offset();
    RoleInfo {
      replication_offset: addr.get(0).unwrap_or(0),
      replication_offset_vector: addr.to_aof_string(),
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

  /// libs/server/Cluster/IClusterProvider.cs:GetReplicationInfo
  ///
  /// INFO replication 条目装配（C# 同名件；ClusterProvider.cs 侧映射登记在
  /// 消费面 wnode/resp/info_provider.rs，本件为真身）。
  /// connected_slaves 计数的 C# 源头是
  /// libs/cluster/Server/Replication/PrimaryOps/ReplicationPrimaryAofSync.cs:ConnectedReplicasCount
  ///（ReplicationManager 单行属性委托 aofSyncDriverStore.CountConnectedReplicas()，
  /// rust 省属性层直呼 count_connected_replicas）
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
      items.push(MetricsItem::new("master_host", addr.unwrap_or_default()));
      items.push(MetricsItem::new("master_port", num_buf.format(port)));
      let link_status = cm.get_primary_link_status(&config);
      items.push(link_status[0].clone());
      items.push(link_status[1].clone());
      items.push(MetricsItem::new(
        "master_sync_in_progress",
        if rm.is_recovering() { "True" } else { "False" },
      ));
      items.push(MetricsItem::new("slave_read_repl_offset", cur_offset));
      items.push(MetricsItem::new("slave_priority", "100"));
      items.push(MetricsItem::new("slave_read_only", "1"));
      items.push(MetricsItem::new("replica_announced", "1"));
      items.push(MetricsItem::new(
        "master_sync_last_io_seconds_ago",
        num_buf.format(rm.last_primary_sync_seconds()),
      ));
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
        self.aof_replay_max_lag_bytes().to_string(),
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
      // 主端 slaveN 行 = C# replicaInfo[i].ToString() 逐项入列
      //（libs/cluster/Server/ClusterProvider.cs:263-267），复用 get_primary_info
      // 的 RoleInfo 投影（端点反查单点），ReplicaRoleInfo 保持内部身份不再对外渲染
      for (i, replica) in self.get_primary_info().1.into_iter().enumerate() {
        items.push(MetricsItem::new(format!("slave{i}"), replica.to_string()));
      }
    }
    items
  }

  /// libs/cluster/Server/ClusterProvider.cs:GetCheckpointInfo
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

  /// libs/cluster/Server/ClusterProvider.cs:GetGossipStats
  fn get_gossip_stats(&self, metrics_disabled: bool) -> Vec<MetricsItem> {
    if let Some(gm) = self.gossip_manager() {
      let open_conns = gm.connection_store.count();
      gm.stats.to_metrics_items(metrics_disabled, open_conns)
    } else {
      Vec::new()
    }
  }

  /// libs/server/Cluster/IClusterProvider.cs:GetBufferPoolStats
  ///
  /// 两管理器缓冲池统计条目（C# 数组字面量同形：migration_manager +
  /// replication_manager 两项；ClusterProvider.cs 侧映射登记在消费面
  /// wnode/resp/info_provider.rs）
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

  /// libs/cluster/Server/ClusterProvider.cs:PurgeBufferPool
  /// libs/server/Cluster/IClusterProvider.cs:PurgeBufferPool
  ///
  /// 按管理器类型净化对应缓冲池（C# 若两者皆非 throw GarnetException）。
  /// ServerListener 不入本层——调用点（PURGEBP 会话分派）已在会话侧直清
  /// 监听器池，此分支 100% 不可达，对标 C# GarnetException 防线
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
        unreachable!("PURGEBP ServerListener 在会话层直清监听池，不经集群提供者")
      }
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:ResetGossipStats
  fn reset_gossip_stats(&self) {
    if let Some(gm) = self.gossip_manager() {
      gm.stats.reset();
    }
  }

  #[inline]
  fn aof_sublog_count(&self) -> usize {
    self
      .replication_manager()
      .map(|rm| rm.sublog_count())
      .unwrap_or(1)
  }

  /// 全租户换号广播协调者注入（doc/zh/db.md 4.5；应答字节闭环：收齐全部
  /// Primary 的 +OK ack 才回 +OK，任一失败/超时回错误，入口据此应答）
  fn flushall_broadcast(&self, ns: u64) -> Option<SlowFuture> {
    let mgr = self.cluster_manager()?;
    Some(SlowFuture::new(async move {
      let mut out = Vec::new();
      match mgr.flushall_broadcast_async(ns).await {
        Ok(()) => out.write_resp_simple_string("OK"),
        Err(e) => out.write_resp_error(&format!("ERR FLUSHALL broadcast failed: {e}")),
      }
      out
    }))
  }

  /// 检查点版本切换开始（对标 C# ReplicationManager.CheckpointVersionShiftStart 委托）
  ///
  /// 检查点内核在版本推进（IN_PROGRESS）处经本句柄调用；REPLICA 角色直接返回
  /// （C# rm 首行判定，Rust rm 不持 clusterManager，判定上移本层），主库转发
  /// ReplicationManager 经提交通道写 CheckpointStartCommit 标记
  fn checkpoint_version_shift_start(&self, new_version: i64) {
    if self.is_replica() {
      return;
    }
    if let Some(rm) = self.replication_manager() {
      rm.checkpoint_version_shift_start(new_version);
    }
  }

  /// 检查点版本切换结束（对标 C# ReplicationManager.CheckpointVersionShiftEnd 委托）
  ///
  /// 快照成功、截断之前（WAIT_FLUSH）经本句柄调用；角色判定同
  /// [`Self::checkpoint_version_shift_start`]（失败路径不到此处）
  fn checkpoint_version_shift_end(&self, new_version: i64) {
    if self.is_replica() {
      return;
    }
    if let Some(rm) = self.replication_manager() {
      rm.checkpoint_version_shift_end(new_version);
    }
  }

  /// libs/cluster/Server/ClusterProvider.cs:OnCheckpointInitiated
  ///
  /// 检查点内核取 covered 源经本句柄下达：直转发同一 self 的
  /// [`CheckpointCallbackFace`] 实现（单机制，不建第二套取源/安全地址更新路径）
  fn on_checkpoint_initiated(&self, covered: &mut AofAddress) {
    <Self as CheckpointCallbackFace>::on_checkpoint_initiated(self, covered);
  }

  /// libs/cluster/Server/ClusterProvider.cs:AddNewCheckpointEntry
  ///
  /// 检查点完成段的登记 + 安全截断经 [`SlowFuture`] 擦除壳承载：self_arc 取
  /// owned Arc 脱离 &self 借用，内部 await 同一 self 的
  /// [`CheckpointCallbackFace::add_new_checkpoint_entry`]（复用其 CheckpointEntry
  /// 登记与 safe_truncate_aof 路径，不改其内部）；弱自引用缺位（provider 未
  /// 装配）返 None，调用方不截断
  fn add_new_checkpoint_entry(
    &self,
    full: bool,
    covered: AofAddress,
    store_checkpoint_token: u128,
    object_store_checkpoint_token: u128,
  ) -> Option<SlowFuture> {
    let cp = self.self_arc()?;
    Some(SlowFuture::new(async move {
      <ClusterProvider as CheckpointCallbackFace>::add_new_checkpoint_entry(
        cp.as_ref(),
        full,
        covered,
        store_checkpoint_token,
        object_store_checkpoint_token,
      )
      .await;
      Vec::new()
    }))
  }
}
