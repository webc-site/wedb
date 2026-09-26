//! 集群节点角色信息（libs/server/Cluster/RoleInfo.cs）
//!
//! 节点角色枚举统一由 wedb 域的 `crate::server::worker::NodeRole` 承载（对标
//! libs/cluster/Server/Worker.cs:NodeRole），一处定义全链路复用。

use std::fmt::{self, Display, Formatter};

/// 节点角色元数据条目
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoleInfo {
  /// 副本复制偏移量（C# replication_offset）
  pub replication_offset: i64,
  /// 副本复制偏移量全向量串（C# replication_offset.ToString()）
  pub replication_offset_vector: String,
  /// 最大发送时间戳（C# sequenceNumber）。C# GetReplicaInfo 未赋值该字段
  ///（libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:GetReplicaInfo），
  /// INFO 如实输出默认零值；AofSyncTask 的 max send 时间戳属 ADVANCETIME
  /// 脉冲面，与 INFO 无关，不在此新造取值源
  pub sequence_number: i64,
  /// 复制延迟（C# replication_lag）
  pub replication_lag: i64,
  /// 复制状态（ROLE 命令用 connect/connecting/sync/connected；指标用 online/offline）
  pub replication_state: String,
  /// 实例地址（C# address）
  pub address: String,
  /// 实例端口（C# port）
  pub port: i32,
}

/// INFO 指标行输出（libs/server/Cluster/RoleInfo.cs:ToString）
///
/// INFO replication 主端 slaveN 行的唯一格式化出口，逐字段对标 C#
/// `ip=..,port=..,state=..,offset=..,lag=..,sequenceNumber=..`，
/// 禁在 cluster_provider 内另写第二份字面量
impl Display for RoleInfo {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "ip={},port={},state={},offset={},lag={},sequenceNumber={}",
      self.address,
      self.port,
      self.replication_state,
      self.replication_offset,
      self.replication_lag,
      self.sequence_number
    )
  }
}

#[cfg(test)]
mod tests {
  use super::RoleInfo;

  /// 逐字段锁定 C# RoleInfo.cs:ToString 形态（样例对标
  /// ClusterProvider.cs:263 注释行）；sequenceNumber 按 C# GetReplicaInfo
  /// 未赋值口径输出默认零值，端点未知时回空 ip、port=0
  #[test]
  fn metrics_line_matches_csharp_tostring() {
    let info = RoleInfo {
      replication_offset: 56,
      replication_lag: 0,
      replication_state: "online".into(),
      address: "127.0.0.1".into(),
      port: 7001,
      ..Default::default()
    };
    assert_eq!(
      info.to_string(),
      "ip=127.0.0.1,port=7001,state=online,offset=56,lag=0,sequenceNumber=0"
    );
    assert_eq!(
      RoleInfo::default().to_string(),
      "ip=,port=0,state=,offset=0,lag=0,sequenceNumber=0"
    );
  }
}
