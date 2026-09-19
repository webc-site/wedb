//! 管理器初始化与复制编排判定链：复制管理器与集群拓扑持久化装配、连接信息
//! 取口，以及入站 gossip 会话的复制健康检查链（ensure_replication 七步）与
//! 重启后主动 attach 发起臂
//! （对标 C# ReplicationManager 的 EnsureReplication / Start 编排判定段）

use std::{fs::metadata, path::Path, sync::Arc, time::Duration};

use compio::runtime::spawn;
use wbase::hex::hex_str_u128;

use crate::{
  error,
  server::{
    cluster::IClusterProvider,
    cluster_config::ClusterConfig,
    cluster_manager::read_device,
    cluster_provider::ClusterProvider,
    connection_info::ConnectionInfo,
    replication::{
      assembly::try_replicate_sync_async, replicate_sync_options::ReplicateSyncOptions,
      replication_manager::ReplicationManager,
    },
    worker::NodeRole,
  },
};

impl ClusterProvider {
  /// 初始化复制管理器（对标 C# ClusterProvider 构造函数初始化 ReplicationManager：
  /// 构造期即持 CheckpointDir，`Recover && fileSize > 0` 门控恢复复制历史，
  /// 详见 ReplicationManager::with_options）
  ///
  /// rust 结构差异：ClusterProvider::new() 时数据目录未知，先建无持久化
  /// 默认实例保运行期路径可达；宿主装配期（端点 accept 之前、set_aof /
  /// wire_replication_data_plane 等挂 rm 资产的注入之前）以真实目录无条件
  /// 重建一次。
  pub fn initialize_replication_manager(
    &self,
    sublog_count: usize,
    config_dir: Option<&Path>,
    recover: bool,
  ) {
    *self.replication_manager.write() = Some(Arc::new(ReplicationManager::with_options(
      sublog_count,
      config_dir,
      recover,
    )));
  }

  /// 集群拓扑持久化装配（对标 C# ClusterManager 构造段 64-143 行的设备建立、
  /// 恢复与 InitLocal、周期刷盘拉起）
  ///
  /// libs/cluster/Server/ClusterManager.cs:ClusterManager
  ///
  /// rust 结构差异：ClusterProvider::new() 时数据目录未知，ClusterManager 仅
  /// 建空配置；宿主装配期（端点 accept 之前）以真实路径与刷盘频率调用本方法
  /// 完成恢复。recoverConfig 门控对标 C#:79：刷盘频率 != -1、盘上文件非空、
  /// 未指定 clean-cluster-config。`announce_hostname` 为集群宣告主机名配置
  /// （C# serverOptions.ClusterAnnounceHostname），透传至 InitLocal 定本地位
  /// 主机名。周期刷盘任务须在 compio 运行时内拉起
  /// （gossip/spawn 同款前置）
  pub fn initialize_cluster_config(
    &self,
    address: &str,
    port: i32,
    config_path: &Path,
    flush_frequency_ms: i32,
    clean_config: bool,
    announce_hostname: &str,
  ) -> error::Result<()> {
    let Some(cm) = self.cluster_manager() else {
      return Ok(());
    };
    cm.set_persist_options(config_path.to_path_buf(), flush_frequency_ms);

    let recover_config =
      flush_frequency_ms != -1 && !clean_config && metadata(config_path).is_ok_and(|m| m.len() > 0);
    if clean_config {
      log::info!("Skipping recovery of local config due to clean-cluster-config flag set");
    } else {
      log::info!("Attempt to recover cluster config from: {config_path:?}");
    }
    if recover_config {
      let bytes = read_device(config_path)?;
      let recovered = ClusterConfig::from_byte_array(&bytes)?;
      log::debug!("Recover cluster config from disk");
      // endpoint 变更（容器漂移）仅记日志，本地位由 init_local 按恢复字段重建
      if address != recovered.local_node_ip() || port != recovered.local_node_port() {
        log::info!(
          "Updating local Endpoint: From {}:{} to {address}:{port}",
          recovered.local_node_ip(),
          recovered.local_node_port()
        );
      }
      *cm.current_config.write() = recovered;
    } else {
      log::debug!("Initialize new node instance config");
    }

    cm.init_local(address, port, recover_config, announce_hostname);
    if flush_frequency_ms > 0 {
      cm.start_flush_task(Duration::from_millis(flush_frequency_ms as u64));
    }
    Ok(())
  }

  /// 获取当前节点连接信息
  pub fn get_connection_info(&self, node_id: u128) -> ConnectionInfo {
    self
      .cluster_manager()
      .map(|cm| cm.get_connection_info(node_id))
      .unwrap_or_default()
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:EnsureReplication
  ///
  /// 入站 gossip 会话的复制健康检查（C# EnsureReplication 完整判定链；
  /// C# 在 rm 上实现并经 clusterProvider 直达各管理器，Rust 依赖方向反转后
  /// 判定链上收至本层，rm 保留节流判定与节流消费两枚原语供本链调用）：
  /// 1. 轮询频率 0 = 禁用；
  /// 2. 距上次尝试不足频率 → 返回（节流；到期只判定不消费，见
  ///    [`ReplicationManager::ensure_replication_due`]，对标 C# :192 的
  ///    `Volatile.Read`）；
  /// 3. 仅 REPLICA 且活跃会话来自其 primary 时动作；
  /// 4. 已有活跃复制流（IsReplicating 状态面）→ 无需动作；
  /// 5. failover 进行中抑制自动重连（防 ReadRole 锁阻塞 TakeOverAsPrimary）；
  /// 6. PreventRoleChange + TOCTOU 复检通过后**才** CAS 消费节流窗口（对标 C#
  ///    :251-256；CAS 不等 = 判定与消费之间另有尝试在途，AllowRoleChange 后
  ///    返回，不重复发起）——第 2~5 步任一挡回的到期帧都不吃掉窗口，下一帧
  ///    仍立即可判到期；
  /// 7. 后台重连发起（对标 C# ReplicationManager.cs:271-272
  ///    Task.Run 体内的 `ReplicaDisklessSync ? TryReplicateDisklessSyncAsync :
  ///    TryReplicateDiskbasedSyncAsync` 选路：本挂点只构造参数束，选路交唯一
  ///    选路口 [`try_replicate_sync_async`]（diskless 支走副本主动 ATTACH_SYNC
  ///    发起端，diskbased 支走
  ///    [`recover_replication`](crate::server::replication::assembly::recover_replication)
  ///    向 primary 发 INITIATE_REPLICA_SYNC）；失败记告警，按
  ///    ClusterReplicationReestablishmentTimeout 轮询节奏重试，finally
  ///    AllowRoleChange）。
  ///
  /// 启动阻塞臂口径：C# ReplicationManager.cs:604-605 的 `Start` 内
  /// `BlockingWait(ReplicaDisklessSync ? ... : ...)`（NodeId 传 null、
  /// TryAddReplica:false，Force 取开关本身）在 rust 无对应挂点，且本函数也不
  /// 能承接——本函数受第 1 步（轮询频率 0 即禁用，默认 0）与第 3 步（要求
  /// `active_remote_node_id` 即已存在来自主端的活跃会话）双门控，只覆盖
  /// 「会话断链后的重连」，不覆盖「重启后首次接入」；故副本重启后的首帧前
  /// 角色为 REPLICA 不代表已 attach。启动期主动发起挂点为独立待办
  /// （task/ing/replicaof-diskbased-sync-initiate.md），本轮不在此另立第二
  /// 发起路径。
  ///
  /// 心跳口径：本函数不刷新 last_primary_sync_time（对标 C#——EnsureReplication
  /// 本体无 UpdateLastPrimarySyncTime 调用，C# 刷新点全在同步建立面
  /// TryReplicaDiskbasedRecovery / ReceiveCheckpointHandler）；rust 挂副本
  /// APPENDLOG 初始化帧握手成功处，见
  /// [`crate::server::replication::cluster_replication_session`]。
  pub fn ensure_replication(self: &Arc<Self>, active_remote_node_id: Option<u128>) {
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
    // 2. 节流到期判定（纯读；窗口消费推迟到第 6 步真正发起处）
    let Some(window_observed) = rm.ensure_replication_due(poll_frequency) else {
      return;
    };

    // 3. 角色判定：仅 REPLICA 且活跃会话来自其 primary
    let Some(cm) = self.cluster_manager() else {
      return;
    };
    let primary_id = {
      let config = cm.current_config();
      if !config.is_replica() {
        return;
      }
      config.local_node_primary_id()
    };
    if primary_id != active_remote_node_id {
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

    // 6. 重连动作面：PreventRoleChange + 复检 + 窗口消费
    //（对标 C# EnsureReplication 尾段：prevent → 复检 → CAS 消费 → Task.Run →
    // finally allow）
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
      config.is_replica() && config.local_node_primary_id() == Some(primary)
    });
    if !still_replica_of_primary {
      self.allow_role_change();
      log::info!("Skip resync: replication state changed after PreventRoleChange");
      return;
    }
    // 节流窗口消费（对标 C# ReplicationManager.cs:251-256：判定到期不消费，
    // 走到这里才算真正发起；CAS 不等 = 判定与消费之间另有尝试在途，
    // 释放角色锁后放弃本轮，不重复发起）
    if !rm.try_consume_ensure_replication_window(window_observed) {
      self.allow_role_change();
      log::info!("Skip resync: another ensure_replication attempt consumed the window");
      return;
    }
    let provider = Arc::clone(self);
    spawn(async move {
      log::info!(
        "Beginning resync to {} after replication session failed",
        hex_str_u128(primary)
      );
      // 断链重连发起（对标 C# ReplicationManager.cs:270-272：
      // Background:false Force:true TryAddReplica:true
      // AllowReplicaResetOnFailure:false UpgradeLock:true，按
      // ReplicaDisklessSync 开关在 diskless / diskbased 两支选路；
      // 失败仅告警，finally AllowRoleChange）
      let opts = ReplicateSyncOptions::new(primary, false, true, true, false, true);
      let resynced = try_replicate_sync_async(&provider, opts).await;
      provider.allow_role_change();
      match resynced {
        Ok(()) => log::info!("Resync to {} successfully started", hex_str_u128(primary)),
        Err(e) => log::warn!(
          "Failed to resync to {} after replication session failed: {e}",
          hex_str_u128(primary)
        ),
      }
    })
    .detach();
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:Start
  ///
  /// 重启后主动发起与 PRIMARY 的首次同步（C# ClusterProvider.cs:89-93
  /// `Start()` 内 `replicationManager.Start()` 段的对偶；同段的
  /// `clusterManager.Start()` 半段由 gossip 挂点承接，二者一并挂在装配尾段
  /// [`WnodeClusterProvider::start`]）。逐条对标 C# 三分支：
  /// - 本地角色 REPLICA 且 recover 且已记 primary → 当场经唯一选路口
  ///   [`try_replicate_sync_async`] 发起一次 attach（C# syncOpts:592-599
  ///   NodeId:null、Background:false、Force 取 ReplicaDisklessSync 开关本身、
  ///   TryAddReplica:false、AllowReplicaResetOnFailure:false、UpgradeLock:false；
  ///   rust node_id 取本端 primary 供日志可读，TryAddReplica:false 不消费），
  ///   失败仅记日志（C# LogError 同口径，绝不影响启动）；
  /// - PRIMARY 且无 primary → 空动作（重启为主，副本自行发起恢复）；
  /// - 其余 → 配置不一致告警（C# LogWarning 同口径）。
  ///
  /// 与 [`ClusterProvider::ensure_replication`] 断链重连臂的区别（C# 两处形态
  /// 不同：Start 阻塞一次性、EnsureReplication 后台轮询）：本臂无
  /// poll_frequency（默认 0 即禁用）与活跃会话双门控，专覆盖「重启首帧前
  /// 接入」；C# Start 以 BlockingWait 阻塞启动线程，rust 无网络线程阻塞约束，
  /// 以一次性 spawn 承接同一发起（错误口径同为「仅记日志」），不复用
  /// ensure_replication 的轮询体，杜绝第二条发起路径。
  pub fn start_replication_attach(&self) {
    let Some(cm) = self.cluster_manager() else {
      return;
    };
    let (role, primary_id) = {
      let config = cm.current_config();
      (config.local_node_role(), config.local_node_primary_id())
    };
    if role == NodeRole::Replica && self.recover() {
      let Some(primary) = primary_id else {
        log::warn!(
          "Replication manager starting configuration inconsistent role:{role:?} replicaOfId:None"
        );
        return;
      };
      // C# Start:Background:false Force:ReplicaDisklessSync TryAddReplica:false
      // AllowReplicaResetOnFailure:false UpgradeLock:false
      let opts = ReplicateSyncOptions::new(
        primary,
        false,
        self.replica_diskless_sync(),
        false,
        false,
        false,
      );
      let Some(provider) = self.self_arc() else {
        return;
      };
      spawn(async move {
        if let Err(e) = try_replicate_sync_async(&provider, opts).await {
          log::error!("An error occurred at ReplicationManager.Start: {e}");
        }
      })
      .detach();
    } else if role == NodeRole::Primary && primary_id.is_none() {
      // 重启为主：无动作，副本自行发起恢复（C# :612-616 同口径）
    } else {
      log::warn!(
        "Replication manager starting configuration inconsistent role:{role:?} replicaOfId:{primary_id:?}"
      );
    }
  }
}
