//! 集群配置选项与常量规范（对标 libs/cluster/Server/ClusterConfig.cs 中配置面）

use serde::{Deserialize, Serialize};
use wbase::hash_slot::MAX_HASH_SLOT_VALUE;

/// CLUSTER NODES 中 bus 端口默认偏移量（bus port = port + 10000）
pub const DEFAULT_BUS_PORT_OFFSET: i32 = 10000;

/// 默认集群节点超时秒数
pub const DEFAULT_CLUSTER_TIMEOUT_SECS: i32 = 60;

/// 默认从节点同步延迟毫秒数
pub const DEFAULT_REPLICA_SYNC_DELAY_MS: i32 = 5;

/// 集群配置错误
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClusterConfigError {
  #[error("Invalid cluster timeout: {0} (must be positive)")]
  InvalidTimeout(i32),
  #[error("Invalid bus port offset: {0} (must be >= 0)")]
  InvalidBusPortOffset(i32),
}

/// 集群配置选项
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterConfigOptions {
  /// 是否启用集群模式
  pub cluster_enabled: bool,
  /// 集群超时时长（秒）
  pub cluster_timeout: i32,
  /// 从节点同步延迟（毫秒）
  pub replica_sync_delay_ms: i32,
  /// 集群复制重建超时（秒）
  pub cluster_replication_reestablishment_timeout: i32,
  /// 总线端口偏移量
  pub bus_port_offset: i32,
}

impl Default for ClusterConfigOptions {
  fn default() -> Self {
    Self {
      cluster_enabled: false,
      cluster_timeout: DEFAULT_CLUSTER_TIMEOUT_SECS,
      replica_sync_delay_ms: DEFAULT_REPLICA_SYNC_DELAY_MS,
      cluster_replication_reestablishment_timeout: 0,
      bus_port_offset: DEFAULT_BUS_PORT_OFFSET,
    }
  }
}

impl ClusterConfigOptions {
  /// 校验集群配置合法性
  pub fn validate(&self) -> Result<(), ClusterConfigError> {
    if self.cluster_timeout <= 0 {
      return Err(ClusterConfigError::InvalidTimeout(self.cluster_timeout));
    }
    if self.bus_port_offset < 0 {
      return Err(ClusterConfigError::InvalidBusPortOffset(
        self.bus_port_offset,
      ));
    }
    Ok(())
  }

  /// 槽位索引越界判断
  #[inline]
  pub const fn is_slot_out_of_range(slot: usize) -> bool {
    slot >= MAX_HASH_SLOT_VALUE
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_cluster_config_defaults() {
    let opts = ClusterConfigOptions::default();
    assert!(!opts.cluster_enabled);
    assert_eq!(opts.cluster_timeout, 60);
    assert_eq!(opts.bus_port_offset, 10000);
    assert!(opts.validate().is_ok());
  }

  #[test]
  fn test_cluster_config_validation() {
    let opts = ClusterConfigOptions {
      cluster_timeout: -1,
      ..Default::default()
    };
    assert_eq!(
      opts.validate().unwrap_err(),
      ClusterConfigError::InvalidTimeout(-1)
    );

    let opts = ClusterConfigOptions {
      cluster_timeout: 60,
      bus_port_offset: -5,
      ..Default::default()
    };
    assert_eq!(
      opts.validate().unwrap_err(),
      ClusterConfigError::InvalidBusPortOffset(-5)
    );
  }

  #[test]
  fn test_slot_range_check() {
    assert!(!ClusterConfigOptions::is_slot_out_of_range(0));
    assert!(!ClusterConfigOptions::is_slot_out_of_range(16383));
    assert!(ClusterConfigOptions::is_slot_out_of_range(16384));
  }
}
