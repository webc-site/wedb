//! 集群节点角色信息（对标 libs/server/Cluster/RoleInfo.cs）

/// 集群节点角色
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeRole {
  /// 主节点
  Primary,
  /// 从副本
  Replica,
}

/// 节点角色元数据条目
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleInfo {
  /// 角色类型
  pub role: NodeRole,
  /// 复制偏移量
  pub replication_offset: i64,
  /// 主节点 ID（仅当 role == Replica 时有值）
  pub master_node_id: Option<String>,
}

impl RoleInfo {
  /// 构造主节点角色信息
  pub fn primary(offset: i64) -> Self {
    Self {
      role: NodeRole::Primary,
      replication_offset: offset,
      master_node_id: None,
    }
  }

  /// 构造副本角色信息
  pub fn replica(offset: i64, master_node_id: Option<String>) -> Self {
    Self {
      role: NodeRole::Replica,
      replication_offset: offset,
      master_node_id,
    }
  }
}
