/// libs/cluster/Server/Replication/RecoveryStatus.cs:RecoveryStatus
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, strum::FromRepr, strum::IntoStaticStr)]
#[repr(u8)]
pub enum RecoveryStatus {
  /// 无恢复中任务
  #[default]
  NoRecovery = 0,
  /// 节点启动初始化恢复
  InitializeRecover = 1,
  /// 副本接入主节点同步恢复
  ClusterReplicate = 2,
  /// 故障转移接管恢复
  ClusterFailover = 3,
  /// 副本解除从属关系（REPLICAOF NO ONE）恢复
  ReplicaOfNoOne = 4,
  /// 检查点已在副本恢复完成
  CheckpointRecoveredAtReplica = 5,
  /// 读取当前角色锁（防止在提交或检查点期间角色变更）
  ReadRole = 6,
}
