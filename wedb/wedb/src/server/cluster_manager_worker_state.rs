use wbase::hex::hex_str_u128;
use wnode::StorageSession;

use crate::{
  error::{Error, Result},
  server::{
    cluster_config::{ClusterConfig, LOCAL_WORKER_ID},
    cluster_manager::ClusterManager,
    replication::recovery_status::RecoveryStatus,
    worker::{LocalWorkerSpec, NodeRole},
  },
};

/// libs/cluster/Server/ClusterManagerWorkerState.cs:ClusterManager
impl ClusterManager {
  /// libs/cluster/Server/ClusterManagerWorkerState.cs:TryRemoveWorker
  pub async fn try_remove_worker(&self, node_id: u128, expiry_seconds: u64) -> Result<()> {
    // 挂起窗口为异步写锁守卫（C# :54 SuspendConfigMerge / :89 finally
    // ResumeConfigMerge），等待方让出任务而非冻线程
    let _guard = self.suspend_config_merge().await;
    {
      let mut current = self.current_config.write();
      if current.local_node_id() == Some(node_id) {
        return Err(Error::CannotForgetMyself);
      }
      if current.get_node_role_from_node_id(node_id) == NodeRole::Unassigned {
        return Err(Error::NodeNotFound(hex_str_u128(node_id)));
      }
      if current.local_node_role() == NodeRole::Replica
        && current.local_node_primary_id() == Some(node_id)
      {
        return Err(Error::CannotForgetPrimary);
      }
      let new_config = current.remove_worker(node_id);
      *current = new_config;
    }
    self.ban_node(node_id, expiry_seconds);
    self.flush_config();
    Ok(())
  }

  /// libs/cluster/Server/ClusterManagerWorkerState.cs:TryReset
  ///
  /// 键检查与槽位表取用在复位临界区内完成（C# :109-138 CAS 循环逐轮
  /// 随 currentConfig 重取——rust 以写锁取代 CAS，但「槽位表与检查时刻
  /// 配置同源」语义保持：槽位表读自当轮 current_config，await 键检查
  /// 让渡后、换配置前于写锁内复核槽位表未变，变化则回炉重检）。
  /// 保留 _expiry_seconds 形参以对标 ClusterManagerWorkerState.TryReset 签名规范
  pub async fn try_reset(
    &self,
    soft: bool,
    _expiry_seconds: u64,
    storage: &StorageSession<'_, wdev::SegmentedDevice>,
  ) -> Result<()> {
    // 挂起窗口守卫跨下方 has_keys_in_slots / close_all 的 await 持有
    // （C# :103 SuspendConfigMerge 贯穿 :112 HasKeysInSlots、:144 finally
    // ResumeConfigMerge 收口）
    let _guard = self.suspend_config_merge().await;
    // C# :106 ResetRecovery
    if let Some(repl_mgr) = self.cluster_provider.replication_manager() {
      repl_mgr.reset_recovery();
    }
    loop {
      // C# :111 GetSlotList(1)：当轮配置的本地槽位表
      let slots: Vec<u16> = self
        .current_config
        .read()
        .get_slot_list(LOCAL_WORKER_ID as u16)
        .into_iter()
        .map(|s| s as u16)
        .collect();
      // C# :112-116 HasKeysInSlots 拒否：有键整体放弃，不换配置
      if storage.has_keys_in_slots(&slots).await? {
        return Err(Error::ResetWithKeysAssigned);
      }
      // C# :118 CloseAll（检查通过后）
      if let Some(gm) = self.cluster_provider.gossip_manager() {
        gm.connection_store.close_all();
      }
      let mut current = self.current_config.write();
      // 取代 C# CAS 失败重试臂：await 让渡窗口内配置并发更新致槽位表
      // 变化时，本轮键检查结论不再同源，放锁重检
      if current
        .get_slot_list(LOCAL_WORKER_ID as u16)
        .into_iter()
        .map(|s| s as u16)
        .ne(slots.iter().copied())
      {
        continue;
      }
      let new_node_id = if soft {
        current.local_node_id().unwrap_or_default()
      } else {
        super::cluster_manager::create_node_id()
      };
      let address = current.local_node_ip().to_string();
      let port = current.local_node_port();
      let config_epoch = if soft {
        current.local_node_config_epoch()
      } else {
        0
      };

      let mut new_config = ClusterConfig::new();
      new_config.initialize_local_worker(LocalWorkerSpec {
        node_id: new_node_id,
        address: &address,
        port,
        config_epoch,
        role: NodeRole::Primary,
        replica_of_node_id: None,
        hostname: None,
      });
      *current = new_config;
      break;
    }
    // 不触 worker_ban_list：C# TryReset 全程不清封禁（全仓 workerBanList
    // 仅 FORGET 写入、gossip 过期清理 TryRemove、门禁 ContainsKey 三类
    // 访问），RESET HARD 后封禁保持自然过期，被忘节点在封禁窗内 gossip
    // 仍被 merge 门禁拒绝
    self.flush_config();
    Ok(())
  }

  /// libs/cluster/Server/ClusterManagerWorkerState.cs:TryAddReplicaAsync
  ///
  /// 恢复锁生命周期（对标 C# :204-230 成功路径）：begin_recovery
  /// (ClusterReplicate) 成功后握锁返回、本函数不释放——INITIATE 往返、检查点
  /// 传送、引擎置换全程持锁，收尾统一在 attach 体
  /// [`finish_replica_sync`](crate::server::replication::assembly::finish_replica_sync)
  /// （C# attach 体 finally 的对偶）。取锁之后的分支（翻转、挂起、清驱动、
  /// flush）无中途 return Err 路径；若日后出现提前返回，必须按 C# :218-224
  /// CAS 重试臂的形态在返回前 end_recovery，禁止裸退漏锁
  pub async fn try_add_replica_async(
    &self,
    node_id: u128,
    force: bool,
    upgrade_lock: bool,
  ) -> Result<()> {
    {
      let current = self.current_config.read();
      if current.local_node_id() == Some(node_id) {
        return Err(Error::MigrateToMyself);
      }
      if !force && current.local_node_role() != NodeRole::Primary {
        return Err(Error::TargetNotPrimary("local node is not primary".into()));
      }
      if !force && current.has_assigned_slots(LOCAL_WORKER_ID as u16) {
        return Err(Error::SlotAlreadyScheduled(0));
      }
      let worker_id = current.get_worker_id_from_node_id(node_id);
      if worker_id == 0 {
        return Err(Error::NodeNotFound(hex_str_u128(node_id)));
      }
      if current.get_node_role_from_node_id(node_id) != NodeRole::Primary {
        return Err(Error::TargetNotPrimary(hex_str_u128(node_id)));
      }
    }

    let repl_mgr = self.cluster_provider.replication_manager();
    if let Some(ref rm) = repl_mgr
      && !rm.begin_recovery(RecoveryStatus::ClusterReplicate, upgrade_lock)
    {
      return Err(Error::CannotAcquireRecoveryLock);
    }

    {
      let mut current = self.current_config.write();
      current
        .make_replica_of(Some(node_id))
        .bump_local_node_config_epoch();
    }
    // 挂起 Primary 类后台任务（C# TryAddReplicaAsync 尾段
    // SuspendPrimaryOnlyTasksAsync + StartReplicaTasks 对译；rust AOF 直推
    // 架构无 VectorReplicationReplay 副本任务，仅挂起侧生效）：周期提交、
    // 周期对象收集轮空，GC 扫描/紧缩停循环
    self.cluster_provider.suspend_primary_tasks();
    // 清残留主端推流驱动（对标 C# ReplicaSyncAttachTaskAsync 段
    // aofSyncDriverStore.Reset——"Remove aofSync tasks if this node was a
    // primary"）：本节点原为主端时旧驱动的 shipped watermark 停滞参与跨驱动
    // 最小值计算，钉死背压闸门与 safe_truncate 截断线；rust 配置翻转与
    // attach 拆为两段，此处承接「翻转后、attach 前」清驱动语义区间（逐驱动
    // dispose 断连 + 尾部闸门写 MAX 释放，见 AofSyncDriverStore::reset）
    if let Some(ref rm) = repl_mgr {
      rm.aof_sync_driver_store.reset();
    }
    self.flush_config();
    // C# :226-230 同形：成功路径握锁返回，不在此处 EndRecovery——锁交
    // attach 收尾 finish_replica_sync 释放（传送/置换窗口的
    // cannot_stream_aof 防线与恢复互斥都依赖该窗口持锁）
    Ok(())
  }

  /// libs/cluster/Server/ClusterManagerWorkerState.cs:ListReplicas
  pub fn list_replicas(&self, node_id: u128) -> Vec<String> {
    let current = self.current_config.read();
    current.get_replicas(node_id, Some(&self.cluster_provider))
  }
}

/// 恢复锁占用（C# RESP_ERR_GENERIC_CANNOT_ACQUIRE_RECOVERY_LOCK，即
/// ClusterManagerWorkerState 中 TryAddReplicaAsync 的 BeginRecovery 失败臂）
pub const ERR_RECOVERY_LOCK: &str = "ERR Recovery in progress, could not acquire recoverLock";

/// 副本接入错误 → C# TryAddReplicaAsync 各校验臂的错误文案（会话应答与副本
/// 同步发起端共用的唯一映射面：C# 在 TryAddReplicaAsync 内直接回错误 RESP
/// 文本，rust 以类型化 Error 上抛、在此统一落文案）
pub fn replicate_err_text(e: Error) -> String {
  use Error as E;
  match e {
    E::MigrateToMyself => "ERR Can't replicate myself".to_string(),
    E::NodeNotFound(id) => format!("ERR I don't know about node {id}"),
    E::TargetNotPrimary(id) => format!("ERR Target node {id} is not a master node."),
    E::SlotAlreadyScheduled(_) => {
      "ERR Primary has been assigned slots and cannot be a replica".to_string()
    }
    E::CannotAcquireRecoveryLock => ERR_RECOVERY_LOCK.to_string(),
    other => other.to_string(),
  }
}
