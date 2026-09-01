use std::fmt;

use crate::types::{Node, NodeId};

pub use webc_cmd::{
  BatchOp, Operation, RaftTxnOp, TxnCondition, TxnOp, TxnReply, TxnReq, UpsertKV,
};

#[derive(bitcode::Encode, bitcode::Decode, Debug, Clone, PartialEq, Eq)]
pub enum Cmd {
  AddNode { node: Node, overriding: bool },
  RemoveNode { node_id: NodeId },
  UpsertKV(UpsertKV),
  BatchUpsertKV { entries: Vec<UpsertKV> },
  Txn { req: TxnReq },
}

impl fmt::Display for Cmd {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Cmd::AddNode { node, overriding } => {
        if *overriding {
          write!(f, "add_node(override):{node}")
        } else {
          write!(f, "add_node(no-override):{node}")
        }
      }
      Cmd::RemoveNode { node_id } => {
        write!(f, "remove_node:{node_id}")
      }
      Cmd::UpsertKV(upsert_kv) => {
        write!(f, "upsert_kv:{upsert_kv}")
      }
      Cmd::BatchUpsertKV { entries } => {
        let len = entries.len();
        write!(f, "batch_upsert_kv: {len} entries")
      }
      Cmd::Txn { req, .. } => {
        let c_len = req.condition.len();
        let if_len = req.if_then.len();
        let el_len = req.else_then.len();
        write!(
          f,
          "txn: {c_len} conditions, {if_len} if_then, {el_len} else_then"
        )
      }
    }
  }
}
