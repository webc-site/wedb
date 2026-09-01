use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::io::Error as IoError;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, error, info, warn};
use zenoh_raft::ChangeMembers;
use zenoh_raft::error::{ClientWriteError, RaftError};

use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::util::{now_millis, sleep};
use wedb_raft::FjallStateMachine;
use wedb_raft::types::{
  AppliedState, BatchWriteReq, Cmd, GetKVReq, GetMemberReply, GetMemberReq, JoinRequest,
  LeaveRequest, LogEntry, Node, NodeId, Raft, ScanPrefixReply, ScanPrefixReq, TxnReply, TxnReq,
};

#[inline]
fn log_err<E: Display>(ctx: &'static str, e: E) -> Error {
  error!("{ctx}: {e}");
  Error::internal(format!("{ctx}: {e}"))
}

pub struct LeaderHandler<'a> {
  node: &'a RaftNode,
  sm: &'a Arc<FjallStateMachine>,
  raft: &'a Arc<Raft>,
}

impl<'a> LeaderHandler<'a> {
  pub fn new(node: &'a RaftNode) -> Self {
    Self {
      node,
      sm: node.state_machine(),
      raft: node.raft(),
    }
  }

  pub fn node(&self) -> &RaftNode {
    self.node
  }

  pub fn raft(&self) -> &Arc<Raft> {
    self.raft
  }

  pub fn state_machine(&self) -> &Arc<FjallStateMachine> {
    self.sm
  }

  async fn change_membership_with_retry(
    &self,
    change: ChangeMembers<NodeId, Node>,
    node_id: NodeId,
  ) -> Result<()> {
    let mut retries = 0;
    loop {
      match self.raft.change_membership(change.clone(), false).await {
        Ok(_) => return Ok(()),
        Err(e) if retries < 10 => {
          warn!("Retrying change_membership for node {node_id} due to: {e}");
          sleep(Duration::from_millis(100)).await;
          retries += 1;
        }
        Err(e) => {
          error!("Failed to change membership: {e}");
          return Err(Error::internal(format!("Failed to change membership: {e}")));
        }
      }
    }
  }

  pub async fn add_node(&self, req: JoinRequest) -> Result<()> {
    let node_id = req.node_id;
    let ep = req.endpoint;
    let node = Node {
      node_id,
      endpoint: ep.clone(),
    };

    let contains_node = self
      .sm
      .contains_node(node_id)
      .map_err(|e| log_err("Failed to check if node exists", e))?;

    if !contains_node {
      let entry = LogEntry::new(Cmd::AddNode {
        node: node.clone(),
        overriding: true,
      });

      self
        .do_write(entry)
        .await
        .map_err(|e| log_err("Failed to add node to state machine", e))?;

      let mut voters = BTreeMap::new();
      voters.insert(node_id, node);

      info!("Adding voter {node_id} ({ep}) to cluster membership");
      self
        .change_membership_with_retry(ChangeMembers::AddVoters(voters), node_id)
        .await?;
      info!("Successfully added node {node_id} ({ep}) to cluster");
    } else {
      info!("Node {node_id} ({ep}) is already a member of the cluster");
    }

    Ok(())
  }

  pub async fn remove_node(&self, req: LeaveRequest) -> Result<()> {
    let node_id = req.node_id;

    let contains_node = self
      .sm
      .contains_node(node_id)
      .map_err(|e| log_err("Failed to check if node exists", e))?;

    if contains_node {
      let entry = LogEntry::new(Cmd::RemoveNode { node_id });

      self
        .do_write(entry)
        .await
        .map_err(|e| log_err("Failed to remove node from state machine", e))?;

      let mut voters = BTreeSet::new();
      voters.insert(node_id);

      info!("Removing voter {node_id} from cluster membership");
      self
        .change_membership_with_retry(ChangeMembers::RemoveVoters(voters), node_id)
        .await?;
      info!("Successfully removed node {node_id} from cluster");
    } else {
      info!("Node {node_id} is not a member of the cluster");
    }

    Ok(())
  }

  pub async fn write(&self, entry: LogEntry) -> Result<AppliedState> {
    self
      .do_write(entry)
      .await
      .map_err(|e| log_err("Failed to write log entry", e))
  }

  pub async fn batch_write(&self, req: BatchWriteReq) -> Result<()> {
    if req.entries.is_empty() {
      return Ok(());
    }
    self.node.batch_write_leader(req.entries).await
  }

  pub async fn read(&self, req: GetKVReq) -> Result<Option<Vec<u8>>> {
    self
      .sm
      .get_kv(&req.key)
      .map_err(|e| log_err("Failed to read from state machine", e))
  }

  pub async fn read_linearizable(&self, req: GetKVReq) -> Result<Option<Vec<u8>>> {
    self
      .raft
      .ensure_linearizable(wedb_raft::ReadPolicy::ReadIndex)
      .await
      .map_err(|e| log_err("ensure_linearizable", e))?;
    self.read(req).await
  }

  pub async fn scan_prefix(&self, req: ScanPrefixReq) -> Result<ScanPrefixReply> {
    self
      .sm
      .scan_prefix(&req.prefix)
      .map_err(|e| log_err("Failed to scan prefix", e))
  }

  pub async fn scan_prefix_linearizable(&self, req: ScanPrefixReq) -> Result<ScanPrefixReply> {
    self
      .raft
      .ensure_linearizable(wedb_raft::ReadPolicy::ReadIndex)
      .await
      .map_err(|e| log_err("ensure_linearizable", e))?;
    self.scan_prefix(req).await
  }

  pub async fn get_members(&self, _: GetMemberReq) -> Result<GetMemberReply> {
    let membership = self
      .sm
      .get_nodes()
      .map_err(|e| log_err("Failed to get members", e))?;
    let node_id = self.node.conf.node_id;
    let current_leader = self.raft.metrics().borrow_watched().current_leader;
    Ok(GetMemberReply {
      node_id,
      current_leader,
      membership,
    })
  }

  pub async fn txn(&self, req: TxnReq) -> Result<TxnReply> {
    let entry = LogEntry::new(Cmd::Txn { req });

    match self
      .do_write(entry)
      .await
      .map_err(|e| log_err("Failed to exec transaction", e))?
    {
      AppliedState::Txn(reply) => Ok(reply),
      _ => {
        error!("Unexpected AppliedState from transaction");
        Err(Error::internal("Unexpected response from transaction"))
      }
    }
  }

  async fn do_write(&self, mut entry: LogEntry) -> Result<AppliedState> {
    entry.time_ms = Some(now_millis());

    let node_id = self.raft.node_id();
    let response = match self.raft.client_write(entry).await {
      Ok(resp) => resp,
      Err(RaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
        log::warn!("Node {node_id} is not leader, forward to: {fwd:?}");
        return Err(Error::retryable(IoError::other(format!(
          "Forward to leader: {fwd:?}"
        ))));
      }
      Err(e) => {
        error!("client write: {e:?}");
        return Err(Error::internal(format!("client write: {e}")));
      }
    };
    let log_id = response.log_id;
    debug!("node_id: {node_id}, log_id: {log_id}, Successfully wrote log entry");
    Ok(response.data)
  }
}
