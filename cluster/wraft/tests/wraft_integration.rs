//! wraft 共识与选主集成测试
use aok::{OK, Void};
use compio::runtime::Runtime;
use wraft::{ConsensusEngine, ElectionState, NoopConsensus, Role, VoteReply};

#[test]
fn test_election_quorum_and_step_up() -> Void {
  let peers = [2u64, 3u64];
  let mut node = ElectionState::new(1, peers);
  assert_eq!(node.role, Role::Follower);

  // 节点发起选举
  let (term, req) = node.start_election().unwrap();
  assert_eq!(term, 1);
  assert_eq!(req.candidate, 1);
  assert_eq!(node.role, Role::Candidate);

  // 获得节点 2 的赞成票：自投 (1) + 节点 2 = 2/3 达到多数派，立即当选
  let new_role = node
    .tally_vote(
      2,
      VoteReply {
        term: 1,
        granted: true,
      },
    )
    .unwrap();
  assert_eq!(new_role, Role::Leader);
  assert_eq!(node.role, Role::Leader);
  assert_eq!(node.leader, Some(1));

  OK
}

#[test]
fn test_consensus_engine_noop_lifecycle() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let engine = NoopConsensus::default();
    assert_eq!(engine.role(), Role::Leader);
    assert_eq!(engine.leader(), Some(0));
    assert_eq!(engine.committed_index(), 0);

    let index = engine.propose(b"command_payload").await?;
    assert_eq!(index, 0);
    assert_eq!(engine.committed_index(), 1);

    OK
  })
}
