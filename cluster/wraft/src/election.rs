//! 选主：Raft 风格任期/投票状态机核心
//!
//! 纯逻辑状态机（无 IO、无时钟依赖，时间由调用方注入），传输与定时器
//! 由 [`ElectionTransport`] 与上层驱动实现。状态转移是 Raft 选主子集的
//! 1:1 实现：任期单调、一任期一票、多数派当选。
//!
//! 可选扩展方向（本层刻意不内置，保持核心可证伪）：
//! - 租约选主（lease-based）：以 `now_ms` 注入点做租约过期判定
//! - 预投票（pre-vote）防抖：在 [`ElectionTransport`] 增设 prevote RPC

use std::{collections::HashSet, io, result};

/// 集群节点 ID
pub type NodeId = u64;

/// 任期号（全集群单调递增）
pub type Term = u64;

/// 节点角色
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
  /// 跟随者
  Follower,
  /// 候选者（已发起选举）
  Candidate,
  /// 主节点（获多数派选票）
  Leader,
}

/// 投票请求（候选人 → 其他节点）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteRequest {
  /// 候选人任期
  pub term: Term,
  /// 候选人 ID
  pub candidate: NodeId,
}

/// 投票应答
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoteReply {
  /// 应答方当前任期（大于请求任期则候选人应立即退位）
  pub term: Term,
  /// 是否授票
  pub granted: bool,
}

/// 选主 RPC 传输抽象（Raft 的 RequestVote 通道）
///
/// 实现方负责寻址与重试策略；本层只消费应答
pub trait ElectionTransport {
  /// 向目标节点发起投票请求
  fn request_vote(
    &self,
    target: NodeId,
    req: VoteRequest,
  ) -> impl Future<Output = io::Result<VoteReply>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
  /// Leader 角色下发起选举被拒
  #[error("already leader of term {term}, resign first")]
  AlreadyLeader { term: Term },
  /// 选票未过半
  #[error("vote not granted by quorum: got {got}, need {need}")]
  NoQuorum { got: usize, need: usize },
  /// 传输层错误
  #[error(transparent)]
  Io(#[from] io::Error),
}

pub type Result<T> = result::Result<T, Error>;

/// 任期/投票状态机
///
/// 不变式：
/// 1. `term` 只增不减
/// 2. 同一任期内至多授出一票（`voted_for`）
/// 3. 观察到更高任期 → 无条件回 Follower 并更新任期
#[derive(Debug, Clone)]
pub struct ElectionState {
  /// 本节点 ID
  pub self_id: NodeId,
  /// 全体节点（含自身），多数派按此集合计算
  pub peers: HashSet<NodeId>,
  /// 当前任期
  pub term: Term,
  /// 本任期投给谁（Some(自己) = 作为候选人自投）
  pub voted_for: Option<NodeId>,
  /// 当前角色
  pub role: Role,
  /// 已知的当主（Follower 视角）
  pub leader: Option<NodeId>,
  /// 当前任期收到的选票（Candidate 视角，含自投）
  pub votes: HashSet<NodeId>,
}

impl ElectionState {
  /// 以 Follower 身份初始化
  pub fn new(self_id: NodeId, peers: impl IntoIterator<Item = NodeId>) -> Self {
    Self {
      self_id,
      peers: peers.into_iter().collect(),
      term: 0,
      voted_for: None,
      role: Role::Follower,
      leader: None,
      votes: HashSet::new(),
    }
  }

  /// 多数派门槛（含自身，向上取整）
  #[inline]
  pub fn quorum(&self) -> usize {
    self.peers.len() / 2 + 1
  }

  /// 发起选举：任期 +1、自投、转 Candidate
  ///
  /// Follower 直接发起；Candidate 选举超时后经此进入更高任期重选
  /// （Raft 语义：新任期即新的投票窗口，`voted_for` 重置合法）。
  /// Leader 须先 [`Self::resign`]，直接发起报错
  pub fn start_election(&mut self) -> Result<(Term, VoteRequest)> {
    if self.role == Role::Leader {
      return Err(Error::AlreadyLeader { term: self.term });
    }
    self.term += 1;
    self.voted_for = Some(self.self_id);
    self.role = Role::Candidate;
    self.leader = None;
    self.votes.clear();
    self.votes.insert(self.self_id);
    Ok((
      self.term,
      VoteRequest {
        term: self.term,
        candidate: self.self_id,
      },
    ))
  }

  /// Leader 主动退位（同任期转 Follower，如租约过期自降、运维降主）
  pub fn resign(&mut self) {
    if self.role == Role::Leader {
      self.role = Role::Follower;
      self.leader = None;
    }
  }

  /// 处理选票应答（Candidate 视角）；过半即当选 Leader
  pub fn tally_vote(&mut self, from: NodeId, reply: VoteReply) -> Result<Role> {
    if reply.term > self.term {
      self.observe_higher_term(reply.term);
      return Ok(self.role);
    }
    if self.role != Role::Candidate || reply.term < self.term {
      return Ok(self.role);
    }
    if reply.granted
      && self.peers.contains(&from)
      && self.votes.insert(from)
      && self.votes.len() >= self.quorum()
    {
      self.role = Role::Leader;
    }
    Ok(self.role)
  }

  /// 处理他方投票请求（Follower/Candidate/过期 Leader 视角）
  ///
  /// 规则：请求任期更新 → 接受并回 Follower；同任期且未投票（或已投给
  /// 该候选人）→ 授票；否则拒绝
  pub fn handle_vote_request(&mut self, req: VoteRequest) -> VoteReply {
    if req.term > self.term {
      self.observe_higher_term(req.term);
    }
    let granted = req.term == self.term && self.voted_for.is_none_or(|v| v == req.candidate);
    if granted {
      self.voted_for = Some(req.candidate);
      self.role = Role::Follower;
    }
    VoteReply {
      term: self.term,
      granted,
    }
  }

  /// 收到合法 Leader 心跳：回 Follower 并记当主（同任期下）
  pub fn accept_leader(&mut self, term: Term, leader: NodeId) {
    if term > self.term {
      self.observe_higher_term(term);
    }
    if term == self.term {
      self.role = Role::Follower;
      self.leader = Some(leader);
    }
  }

  /// 观察到更高任期：无条件退位、清除投票记忆
  fn observe_higher_term(&mut self, term: Term) {
    debug_assert!(term > self.term);
    self.term = term;
    self.voted_for = None;
    self.role = Role::Follower;
    self.leader = None;
    self.votes.clear();
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn three_nodes() -> ElectionState {
    ElectionState::new(1, [1, 2, 3])
  }

  #[test]
  fn candidate_wins_with_quorum() {
    let mut s = three_nodes();
    let (term, req) = s.start_election().unwrap();
    assert_eq!((term, s.role), (1, Role::Candidate));

    assert_eq!(
      s.tally_vote(
        2,
        VoteReply {
          term: 1,
          granted: true
        }
      )
      .unwrap(),
      Role::Leader
    );
    // 当选后再来迟到选票，角色不再变化
    assert_eq!(
      s.tally_vote(
        3,
        VoteReply {
          term: 1,
          granted: true
        }
      )
      .unwrap(),
      Role::Leader
    );
    assert_eq!(req.candidate, 1);
  }

  #[test]
  fn stale_vote_reply_keeps_candidate() {
    let mut s = three_nodes();
    s.start_election().unwrap();
    assert_eq!(
      s.tally_vote(
        2,
        VoteReply {
          term: 0,
          granted: true
        }
      )
      .unwrap(),
      Role::Candidate
    );
  }

  #[test]
  fn higher_term_forces_step_down() {
    let mut s = three_nodes();
    s.start_election().unwrap();
    let reply = s.handle_vote_request(VoteRequest {
      term: 5,
      candidate: 2,
    });
    assert!(reply.granted);
    assert_eq!((s.term, s.role, s.voted_for), (5, Role::Follower, Some(2)));
  }

  #[test]
  fn one_vote_per_term() {
    let mut s = three_nodes();
    let granted_a = s.handle_vote_request(VoteRequest {
      term: 1,
      candidate: 2,
    });
    let granted_b = s.handle_vote_request(VoteRequest {
      term: 1,
      candidate: 3,
    });
    assert!(granted_a.granted);
    assert!(!granted_b.granted);
  }

  #[test]
  fn duplicate_vote_same_candidate_ok() {
    let mut s = three_nodes();
    assert!(
      s.handle_vote_request(VoteRequest {
        term: 1,
        candidate: 2
      })
      .granted
    );
    assert!(
      s.handle_vote_request(VoteRequest {
        term: 1,
        candidate: 2
      })
      .granted
    );
  }

  #[test]
  fn follower_accepts_leader_heartbeat() {
    let mut s = three_nodes();
    s.start_election().unwrap();
    s.accept_leader(1, 2);
    assert_eq!((s.role, s.leader), (Role::Follower, Some(2)));
    // 旧任期心跳不影响 Candidate
    let mut c = three_nodes();
    c.start_election().unwrap();
    c.accept_leader(0, 3);
    assert_eq!(c.role, Role::Candidate);
  }

  #[test]
  fn candidate_timeout_restarts_with_higher_term() {
    let mut s = three_nodes();
    let (t1, _) = s.start_election().unwrap();
    assert_eq!(t1, 1);
    // 选举超时 → 更高任期重选，状态机不卡死
    let (t2, req) = s.start_election().unwrap();
    assert_eq!((t2, req.term, s.role), (2, 2, Role::Candidate));
  }

  #[test]
  fn leader_resign_then_reject_re_elect() {
    let mut s = three_nodes();
    s.start_election().unwrap();
    assert_eq!(
      s.tally_vote(
        2,
        VoteReply {
          term: 1,
          granted: true
        }
      )
      .unwrap(),
      Role::Leader
    );
    // Leader 直接重选被拒，须先退位
    assert!(matches!(
      s.start_election(),
      Err(Error::AlreadyLeader { term: 1 })
    ));
    s.resign();
    assert_eq!((s.role, s.leader), (Role::Follower, None));
    // 退位后正常重选，进入更高任期
    let (t, _) = s.start_election().unwrap();
    assert_eq!(t, 2);
  }

  #[test]
  fn vote_from_unknown_node_ignored() {
    let mut s = three_nodes();
    s.start_election().unwrap();
    // 非集群成员的选票不计数
    assert_eq!(
      s.tally_vote(
        99,
        VoteReply {
          term: 1,
          granted: true
        }
      )
      .unwrap(),
      Role::Candidate
    );
    assert_eq!(
      s.tally_vote(
        2,
        VoteReply {
          term: 1,
          granted: true
        }
      )
      .unwrap(),
      Role::Leader
    );
  }

  #[test]
  fn quorum_is_majority() {
    assert_eq!(three_nodes().quorum(), 2);
    assert_eq!(ElectionState::new(1, [1, 2, 3, 4, 5]).quorum(), 3);
    assert_eq!(ElectionState::new(1, [1]).quorum(), 1);
  }
}
