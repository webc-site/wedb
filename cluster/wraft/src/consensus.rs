//! 共识引擎可插拔接口
//!
//! Garnet OSS 是"主从复制 + 手动故障转移"，写权与故障切换由运维/集群
//! 管理器裁决。本模块把"谁有权写、写如何被确认"抽象为 [`ConsensusEngine`]
//! 单一入口：Raft、Multi-Paxos、租约仲裁等实现各自注入，上层（节点服务）
//! 只面对 role/propose/committed 三个语义。
//!
//! 与 AOF 的耦合契约：
//! - Leader 侧：`propose` 确认后的区间由数据面 `wedb::aof_sync::AofSyncDriver`
//!   按位点发货（WAL 是事实源，共识层只裁决可发货边界）
//! - Follower 侧：收到的帧经 `wnode::Replay` 回放，与本地恢复共用一条
//!   代码路径

use std::{result, sync::atomic};

use crate::election::Role;

#[derive(Debug, thiserror::Error)]
pub enum Error {
  /// 非 Leader 拒绝提案（附带已知 Leader，供重定向）
  #[error("not leader, redirect to {leader:?}")]
  NotLeader { leader: Option<u64> },
}

pub type Result<T> = result::Result<T, Error>;

/// 共识引擎接口
///
/// 泛型 `T` 为提案载荷的全部生命周期边界：调用方在 `propose` 返回 Ok 后
/// 才可将载荷对应的副作用（AOF 位点）对外发布
pub trait ConsensusEngine: Send + Sync {
  /// 当前角色
  fn role(&self) -> Role;

  /// 已知 Leader（未知为 None）
  fn leader(&self) -> Option<u64>;

  /// 追加一条日志并等待多数派确认，返回日志索引
  ///
  /// 非 Leader 角色调用返回 [`Error::NotLeader`]
  fn propose(&self, entry: &[u8]) -> impl Future<Output = Result<u64>> + Send;

  /// 当前已确认位点（主端同步驱动以此作为发货上界）
  fn committed_index(&self) -> u64;
}

/// 单节点退化实现：propose 即确认（role 恒 Leader）
///
/// 单机部署与集成测试的默认引擎，使上层无需判空集群配置
#[derive(Debug, Default)]
pub struct NoopConsensus {
  index: atomic::AtomicU64,
}

impl ConsensusEngine for NoopConsensus {
  fn role(&self) -> Role {
    Role::Leader
  }

  fn leader(&self) -> Option<u64> {
    Some(0)
  }

  async fn propose(&self, _entry: &[u8]) -> Result<u64> {
    Ok(self.index.fetch_add(1, atomic::Ordering::Relaxed))
  }

  fn committed_index(&self) -> u64 {
    self.index.load(atomic::Ordering::Relaxed)
  }
}

#[cfg(test)]
mod tests {
  use compio::runtime::Runtime;

  use super::*;

  #[test]
  fn noop_engine_confirms_immediately() -> aok::Void {
    let rt = Runtime::new()?;
    rt.block_on(async {
      let engine = NoopConsensus::default();
      assert_eq!(engine.role(), Role::Leader);
      assert_eq!(engine.leader(), Some(0));
      assert_eq!(engine.propose(b"a").await.unwrap(), 0);
      assert_eq!(engine.propose(b"b").await.unwrap(), 1);
      assert_eq!(engine.committed_index(), 2);
      aok::OK
    })
  }
}
