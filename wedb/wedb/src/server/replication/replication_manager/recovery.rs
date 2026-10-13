//! 恢复状态机与检查点接收处理器状态面（对标 C# BeginRecovery/EndRecovery/RecoverAsync）

use super::*;

impl ReplicationManager {
  /// libs/cluster/Server/Replication/ReplicationManager.cs:RecoveryStatus
  ///
  /// 获取当前恢复状态
  #[inline]
  pub fn recovery_status(&self) -> RecoveryStatus {
    *self.current_recovery_status.read()
  }

  /// 是否正在恢复中
  #[inline]
  pub fn is_recovering(&self) -> bool {
    let s = self.recovery_status();
    s != RecoveryStatus::NoRecovery && s != RecoveryStatus::ReadRole
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:CannotStreamAOF
  ///
  /// 是否无法流式传输 AOF
  #[inline]
  pub fn cannot_stream_aof(&self) -> bool {
    self.is_recovering() && self.recovery_status() != RecoveryStatus::CheckpointRecoveredAtReplica
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:BeginRecovery
  ///
  /// 开始恢复任务（设置恢复状态并加锁门控）
  pub fn begin_recovery(&self, next_recovery_status: RecoveryStatus, upgrade_lock: bool) -> bool {
    let mut status_guard = self.current_recovery_status.write();

    if upgrade_lock {
      if *status_guard != RecoveryStatus::ReadRole {
        return false;
      }
      *status_guard = next_recovery_status;
      trace!("Upgraded recover lock to [{next_recovery_status:?}]");
      return true;
    }

    if *status_guard != RecoveryStatus::NoRecovery {
      warn!(
        "Error background recovery task has not completed [{:?}]",
        *status_guard
      );
      return false;
    }

    *status_guard = next_recovery_status;
    trace!("Success recover lock [{next_recovery_status:?}]");
    true
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:EndRecovery
  ///
  /// 结束恢复任务并释放门控锁；严格对标 C# 状态转换矩阵，
  /// 非法转换记错误日志并拒绝变更（C# 抛 GarnetException 的非 panic 承接）
  pub fn end_recovery(&self, next_recovery_status: RecoveryStatus, downgrade_lock: bool) {
    let mut status_guard = self.current_recovery_status.write();
    let curr = *status_guard;
    trace!("EndRecovery [{curr:?} -> {next_recovery_status:?}]");

    if downgrade_lock {
      // C# Debug.Assert：只能降级到 ReadRole；且 ReadRole 起点不可再降级
      if curr == RecoveryStatus::ReadRole {
        error!("Cannot downgrade lock FROM a ReadRole [{curr:?}, {next_recovery_status:?}]");
        return;
      }
      *status_guard = RecoveryStatus::ReadRole;
      return;
    }

    let valid = match curr {
      RecoveryStatus::NoRecovery => false,
      RecoveryStatus::InitializeRecover
      | RecoveryStatus::ClusterReplicate
      | RecoveryStatus::ClusterFailover
      | RecoveryStatus::ReplicaOfNoOne => matches!(
        next_recovery_status,
        RecoveryStatus::CheckpointRecoveredAtReplica
          | RecoveryStatus::NoRecovery
          | RecoveryStatus::ReadRole
      ),
      // C#：CheckpointRecoveredAtReplica 只能转 NoRecovery / ReadRole
      RecoveryStatus::CheckpointRecoveredAtReplica => matches!(
        next_recovery_status,
        RecoveryStatus::NoRecovery | RecoveryStatus::ReadRole
      ),
      // C#：ReadRole 起点允许转任意 next（ReadUnlock + ResumeCheckpoints）
      RecoveryStatus::ReadRole => true,
    };

    if valid {
      *status_guard = next_recovery_status;
    } else {
      error!("Invalid state change [{curr:?} -> {next_recovery_status:?}]");
    }
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:ResetRecovery
  ///
  /// 重置恢复状态为 NoRecovery 并复位重连自愈
  pub fn reset_recovery(&self) {
    let mut status_guard = self.current_recovery_status.write();
    *status_guard = RecoveryStatus::NoRecovery;
  }

  /// 检查点接收状态置换（对标 C# TryReplicateDiskbasedSyncAsync:161 每次
  /// attach `recvCheckpointHandler = new(...)` / finally Dispose：全新处理
  /// 器零状态，弃残留活跃槽并开新一轮接收）
  ///
  /// 返回 true = 上一轮留有未成功恢复的 StoreHlog 半写，调用点须据此升级
  /// provider 管理面屏障（C# 无本屏障亦无残留：`new` 即抹掉上轮全部状态，
  /// 新一轮快照自起始地址覆盖重推，ReceiveCheckpointHandler.cs:64-65）
  pub fn reset_recv_checkpoint_handler(&self) -> bool {
    self.recv_checkpoint_handler.reset()
  }

  /// 检查设备是否处于污染状态
  #[inline]
  pub fn is_device_contaminated(&self) -> bool {
    self.recv_checkpoint_handler.is_device_contaminated()
  }

  /// 标记设备已被污染
  #[inline]
  pub fn mark_device_contaminated(&self) {
    self.recv_checkpoint_handler.mark_device_contaminated();
  }

  /// 解除本轮接收闸门（一次成功的全量恢复收口，见 [`Self::on_recovery_success`]）
  #[inline]
  pub fn clear_device_contaminated(&self) {
    self.recv_checkpoint_handler.clear_device_contaminated();
  }

  /// 是否有未提交的 StoreHlog 脏段写入
  #[inline]
  pub fn is_hlog_dirty(&self) -> bool {
    self.recv_checkpoint_handler.is_hlog_dirty()
  }

  /// 检查点成功导入收尾（安全复位接收槽与脏标记，并解除本轮接收闸门）
  #[inline]
  pub fn on_recovery_success(&self) {
    self.recv_checkpoint_handler.on_recovery_success();
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:RecoverAsync
  /// libs/cluster/Server/ClusterProvider.cs:RecoverAsync
  /// libs/server/Cluster/IClusterProvider.cs:RecoverAsync
  ///
  /// 复制域启动恢复。与 C# 的差异登记（依赖方向反转）：
  /// - provider 跳已折叠：C# `ClusterProvider.RecoverAsync` 是一行委托
  ///   （`=> replicationManager.RecoverAsync()`），rust 宿主装配期直调本件
  ///   （`boot.rs` `rm.recover_async(cluster.is_primary())`），中间件不再另设
  ///   承接面，故 provider 侧两锚统一登记于此。
  /// - 复制历史恢复在构造期完成（[`Self::with_options`] 门控，对标 C#
  ///   ReplicationManager 构造段 Recover && fileSize > 0），本方法不再重复。
  /// - C# PRIMARY / REPLICA+ClusterReplicaResumeWithData 分支经
  ///   `storeWrapper` 做 checkpoint+AOF 数据面恢复并 `ReplayAOF` 后
  ///   `replicationOffset.SetValue(replayedUntil)` 回填位点；rust rm 不持
  ///   storeWrapper 可达面，数据面恢复与位点回填由 wnode 宿主装配期承接
  ///   （`StorageSessionProvider::open_recovered_with_config_and_aof` + 装配尾段
  ///   set_current_replication_offset，时序同为端点 accept 之前——对齐
  ///   C# StoreWrapper.RecoverAsync 单机分支的结构），本方法仅承接 rm 自有
  ///   的检查点内存索引初始化。
  /// - `RecoverCheckpointAndAOFAsync` 的独立方法已删（原实现仅恢复
  ///   replication history，名实不符）；其 C# 职责按上述拆分归位。
  /// - REPLICA+ClusterReplicaResumeWithData 分支：该配置面未落地且无需落地
  ///   ——数据面恢复由 wnode `open_from_args` 角色无关前置承接（boot 装配尾段
  ///   双角色一律回填 recovered_aof_tail），副本 `--recover` 重启即本地续用
  ///   （等效 C# ResumeWithData=true 恒开），等待 attach 后走增量还是全量由
  ///   协商链（sameHistory/trunc_floor，§116）裁决；刻意改良裁决与装配序
  ///   护栏详见 deviations.md §162（严禁未前移角色可知就按 C# 门控回改）。
  pub async fn recover_async(&self, is_primary: bool) {
    if is_primary && !self.initialize_checkpoint_store() {
      warn!("Failed acquiring latest memory checkpoint metadata at RecoverAsync");
    }
  }
}
