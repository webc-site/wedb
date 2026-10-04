//! 无盘同步会话状态机 (SyncStatus)
//!
//! 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/SyncStatus.cs
//!
//! 枚举序与判别值同 C#（SUCCESS=0, FAILED=1, INPROGRESS=2, INITIALIZING=3）。

use std::fmt;

/// 复制 attach 同步状态
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/SyncStatus.cs:SyncStatus
#[derive(Debug, Copy, PartialEq, Eq, Clone)]
#[repr(u8)]
pub enum SyncStatus {
  Success = 0,
  Failed = 1,
  InProgress = 2,
  Initializing = 3,
}

impl SyncStatus {
  /// 状态对应的协议字符串常量
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Success => "SUCCESS",
      Self::Failed => "FAILED",
      Self::InProgress => "INPROGRESS",
      Self::Initializing => "INITIALIZING",
    }
  }
}

impl fmt::Display for SyncStatus {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(self.as_str())
  }
}

/// 同步状态 + 首个错误文案
///
/// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/SyncStatus.cs:SyncStatusInfo
#[derive(Debug, Clone)]
pub struct SyncStatusInfo {
  pub sync_status: SyncStatus,
  pub error: Option<String>,
}

impl Default for SyncStatusInfo {
  fn default() -> Self {
    Self {
      sync_status: SyncStatus::Initializing,
      error: None,
    }
  }
}
