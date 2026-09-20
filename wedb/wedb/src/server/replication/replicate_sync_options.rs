//! 副本同步发起参数束（对标 libs/cluster/Server/Replication/ReplicateSyncOptions.cs）
//!
//! C# 四处驱动点（ReplicationManager.cs:270 / :593-598、ReplicaOfCommand.cs:79-85、
//! RespClusterReplicationCommands.cs:96-102）各构造一份本参数束，交由
//! diskless / diskbased 两支发起函数消费；rust 同形：一处定义、两支共读。

/// 在 garnet 中的相对路径: libs/cluster/Server/Replication/ReplicateSyncOptions.cs
#[derive(Debug, Clone, Copy)]
pub struct ReplicateSyncOptions {
  /// 要复制的主节点 id（C# NodeId: string；rust 内部身份 u128，
  /// `try_add_replica` 为 false 时不消费，对应 C# 启动臂传 null 的形态）
  pub node_id: u128,
  /// 同步是否走后台任务（C# Background）
  pub background: bool,
  /// 强制登记为本节点副本（`try_add_replica` 为 false 时无意义；
  /// 为 true 时跳过本节点主角色与槽位洁净校验，C# Force）
  pub force: bool,
  /// 是否尝试把本节点登记为 `node_id` 的副本（C# TryAddReplica）
  pub try_add_replica: bool,
  /// 同步失败时是否把本节点复位为主（C# AllowReplicaResetOnFailure）
  pub allow_replica_reset_on_failure: bool,
  /// 是否允许把 ReadRole 读锁升级为写锁、同步结束后降回读锁（C# UpgradeLock）
  pub upgrade_lock: bool,
}

impl ReplicateSyncOptions {
  /// 位置参构造（对标 C# record struct 的位置参构造形态）
  pub const fn new(
    node_id: u128,
    background: bool,
    force: bool,
    try_add_replica: bool,
    allow_replica_reset_on_failure: bool,
    upgrade_lock: bool,
  ) -> Self {
    Self {
      node_id,
      background,
      force,
      try_add_replica,
      allow_replica_reset_on_failure,
      upgrade_lock,
    }
  }
}
