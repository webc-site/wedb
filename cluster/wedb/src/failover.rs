//! 主从切换决策：以共识引擎的角色裁决数据面行为
//!
//! 对标 libs/client/GarnetClientAPI/GarnetClientClusterCommands.cs:Failover/FailoverManager.cs`。差异：Garnet 的故障转移
//! 由 ClusterConfig 配置驱动（人工/管理面裁决），本实现把裁决权交给
//! [`ConsensusEngine`]——数据面（AOF 发货、写接受、请求重定向）统一
//! 从这里取行为依据，不再各自判角色。
//!
//! 门控语义：
//! - Leader：可以发货 AOF、可以接受写入
//! - Follower：只回放，写请求重定向到已知 Leader
//! - Candidate（选举中）：同 Follower，但无重定向目标

use std::result;

use wraft::{ConsensusEngine, Role};

#[derive(Debug, thiserror::Error)]
pub enum Error {
  /// 写请求落在非 Leader 节点（附带重定向目标，未知为 None）
  ///
  /// 刻意不复用 `wraft::ConsensusError::NotLeader`：那是引擎提案被拒的
  /// 算法层语义，本变体是数据面入口的准入拒绝，字段同形但演进独立
  #[error("not leader, redirect to {leader:?}")]
  NotLeader { leader: Option<u64> },
}

pub type Result<T> = result::Result<T, Error>;

/// 故障转移管理器：共识角色的数据面投影
///
/// 泛型 `E` 为共识引擎（`wraft` 提供，如 `NoopConsensus`/Raft 实现）；
/// 数据面各处（同步驱动、会话写路径）持有本管理器取行为依据
pub struct FailoverManager<E: ConsensusEngine> {
  engine: E,
}

impl<E: ConsensusEngine> FailoverManager<E> {
  /// 绑定共识引擎
  pub fn new(engine: E) -> Self {
    Self { engine }
  }

  /// 当前角色（透传共识引擎）
  #[inline]
  pub fn role(&self) -> Role {
    self.engine.role()
  }

  /// 已知 Leader（未知为 None）
  #[inline]
  pub fn leader(&self) -> Option<u64> {
    self.engine.leader()
  }

  /// 是否允许向副本发货 AOF（对标 FailoverManager 的 primary 职责判定）
  #[inline]
  pub fn can_ship_aof(&self) -> bool {
    self.engine.role() == Role::Leader
  }

  /// 是否接受客户端写入（对标 primary 语义；Leader 才接受）
  #[inline]
  pub fn can_accept_writes(&self) -> bool {
    self.engine.role() == Role::Leader
  }

  /// 写请求进入前校验：非 Leader 即拒绝并附重定向目标
  ///
  /// Candidate 无已知当主（选主已清空），重定向目标为 None
  pub fn check_write(&self) -> Result<()> {
    match self.engine.role() {
      Role::Leader => Ok(()),
      Role::Follower => Err(Error::NotLeader {
        leader: self.engine.leader(),
      }),
      Role::Candidate => Err(Error::NotLeader { leader: None }),
    }
  }
}

#[cfg(test)]
mod tests {
  use wraft::NoopConsensus;

  use super::*;

  #[test]
  fn leader_engine_allows_all() {
    let m = FailoverManager::new(NoopConsensus::default());
    assert_eq!(m.role(), Role::Leader);
    assert!(m.can_ship_aof());
    assert!(m.can_accept_writes());
    assert!(m.check_write().is_ok());
  }

  /// 恒 Follower 的假引擎：校验门控与重定向
  struct AlwaysFollower;

  impl ConsensusEngine for AlwaysFollower {
    fn role(&self) -> Role {
      Role::Follower
    }
    fn leader(&self) -> Option<u64> {
      Some(7)
    }
    async fn propose(&self, _entry: &[u8]) -> wraft::ConsensusResult<u64> {
      Err(wraft::ConsensusError::NotLeader { leader: Some(7) })
    }
    fn committed_index(&self) -> u64 {
      0
    }
  }

  /// Candidate 引擎：即使残留 stale leader，重定向目标也必须为 None
  struct StaleCandidate;

  impl ConsensusEngine for StaleCandidate {
    fn role(&self) -> Role {
      Role::Candidate
    }
    fn leader(&self) -> Option<u64> {
      Some(7)
    }
    async fn propose(&self, _entry: &[u8]) -> wraft::ConsensusResult<u64> {
      Err(wraft::ConsensusError::NotLeader { leader: None })
    }
    fn committed_index(&self) -> u64 {
      0
    }
  }

  #[test]
  fn candidate_engine_redirects_to_none() {
    let m = FailoverManager::new(StaleCandidate);
    assert!(!m.can_ship_aof());
    assert!(!m.can_accept_writes());
    let err = m.check_write().unwrap_err();
    assert!(matches!(err, Error::NotLeader { leader: None }));
  }

  #[test]
  fn follower_engine_rejects_writes_with_redirect() {
    let m = FailoverManager::new(AlwaysFollower);
    assert!(!m.can_ship_aof());
    assert!(!m.can_accept_writes());
    let err = m.check_write().unwrap_err();
    assert!(matches!(err, Error::NotLeader { leader: Some(7) }));
  }
}
