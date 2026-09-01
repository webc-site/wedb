use crate::parse::util::*;
use crate::txn::{BatchOp, TxnReq};
use crate::types::Cmd;
use sonic_rs::{JsonContainerTrait, JsonValueTrait};
use wedb_embed::{Error, Result};

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: CLUSTER subcmd
    "cluster" => {
      if args.len() < 2 {
        return Ok(Some(Cmd::ClusterNodes));
      }
      let sub = args[1];
      if sub.eq_ignore_ascii_case(b"nodes") {
        Ok(Cmd::ClusterNodes)
      } else if sub.eq_ignore_ascii_case(b"info") {
        Ok(Cmd::ClusterInfo)
      } else if sub.eq_ignore_ascii_case(b"slots") {
        Ok(Cmd::ClusterSlots)
      } else if sub.eq_ignore_ascii_case(b"shards") {
        Ok(Cmd::ClusterShards)
      } else if sub.eq_ignore_ascii_case(b"members") {
        Ok(Cmd::ClusterMembers)
      } else if sub.eq_ignore_ascii_case(b"myid") {
        Ok(Cmd::ClusterMyId)
      } else if sub.eq_ignore_ascii_case(b"keyslot") && args.len() >= 3 {
        Ok(Cmd::ClusterKeySlot(arg_string(args[2])))
      } else if sub.eq_ignore_ascii_case(b"rebalance") {
        Ok(Cmd::ClusterRebalance)
      } else if sub.eq_ignore_ascii_case(b"failover") {
        Ok(Cmd::ClusterFailover)
      } else if sub.eq_ignore_ascii_case(b"saveconfig") {
        Ok(Cmd::ClusterSaveConfig)
      } else if sub.eq_ignore_ascii_case(b"reset") {
        let hard = args
          .get(2)
          .map(|a| a.eq_ignore_ascii_case(b"hard"))
          .unwrap_or(false);
        Ok(Cmd::ClusterReset(hard))
      } else if sub.eq_ignore_ascii_case(b"countkeysinslot") && args.len() >= 3 {
        let slot = arg_u32(args[2]).unwrap_or(0);
        Ok(Cmd::ClusterCountKeysInSlot(slot))
      } else if sub.eq_ignore_ascii_case(b"getkeysinslot") && args.len() >= 4 {
        let slot = arg_u32(args[2]).unwrap_or(0);
        let count = arg_usize(args[3]).unwrap_or(0);
        Ok(Cmd::ClusterGetKeysInSlot { slot, count })
      } else if (sub.eq_ignore_ascii_case(b"settags") || sub.eq_ignore_ascii_case(b"tags"))
        && args.len() >= 3
      {
        let node_id = arg_u64(args[2]).unwrap_or(0);
        let tags = args[3..].iter().map(|b| arg_string(b)).collect();
        Ok(Cmd::ClusterSetTags { node_id, tags })
      } else if sub.eq_ignore_ascii_case(b"gettags") && args.len() >= 3 {
        let node_id = arg_u64(args[2]).unwrap_or(0);
        Ok(Cmd::ClusterGetTags(node_id))
      } else if sub.eq_ignore_ascii_case(b"meet") && args.len() >= 4 {
        let ip = arg_string(args[2]);
        let port = arg_u16(args[3]).unwrap_or(0);
        let node_id = if args.len() >= 5 {
          arg_u64(args[4])
        } else {
          None
        };
        Ok(Cmd::ClusterMeet { ip, port, node_id })
      } else if sub.eq_ignore_ascii_case(b"forget") && args.len() >= 3 {
        let s = arg_str(args[2]);
        let id = if s.len() == 40 {
          u64::from_str_radix(s, 16).unwrap_or(0)
        } else {
          arg_u64(args[2]).unwrap_or(0)
        };
        Ok(Cmd::ClusterForget(id))
      } else {
        let list = args[1..].iter().map(|a| arg_string(a)).collect();
        Ok(Cmd::Cluster(list))
      }
    }
    // @cmd: CLUSTERX
    "clusterx" => {
      let list = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::ClusterX(list))
    }
    // @cmd: READONLY
    "readonly" => Ok(Cmd::ReadOnly),
    // @cmd: READWRITE
    "readwrite" => Ok(Cmd::ReadWrite),
    // @cmd: ASKING
    "asking" => Ok(Cmd::Asking),
    // @cmd: REPLCONF
    "replconf" => {
      let list = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::ReplConf(list))
    }
    // @cmd: PSYNC
    "psync" => {
      let list = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::PSync(list))
    }
    "_fetch_meta" => {
      let list = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::FetchMeta(list))
    }
    "_fetch_file" => {
      let list = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::FetchFile(list))
    }
    "_db_name" => Ok(Cmd::DbName),
    // @cmd: WAIT numreplicas timeout
    "wait" => {
      let numreplicas = if args.len() > 1 {
        arg_i64(args[1]).unwrap_or(0)
      } else {
        0
      };
      let timeout = if args.len() > 2 {
        arg_i64(args[2]).unwrap_or(0)
      } else {
        0
      };
      Ok(Cmd::Wait(numreplicas, timeout))
    }

    // @cmd: BATCH <op> <args...> [OP ...]
    "batch" | "raft.batch" => {
      check_min_args(cmd_name, args, 2)?;
      if args.len() == 2 {
        let json_str = arg_str(args[1]);
        let req: sonic_rs::Value = sonic_rs::from_str(json_str)
          .map_err(|e| Error::invalid_data(format!("Invalid JSON batch format: {e}")))?;
        let mut ops = Vec::new();
        if let Some(arr) = req.get("operations").and_then(|v| v.as_array()) {
          for item in arr {
            let op = item.get("op").and_then(|v| v.as_str()).unwrap_or("");
            let key = item
              .get("key")
              .and_then(|v| v.as_str())
              .unwrap_or("")
              .to_string();
            if op.eq_ignore_ascii_case("set") {
              let val = item
                .get("value")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .as_bytes()
                .to_vec();
              ops.push(BatchOp::Set(key, val));
            } else if op.eq_ignore_ascii_case("del") {
              ops.push(BatchOp::Del(key));
            }
          }
        }
        Ok(Cmd::Batch(ops))
      } else {
        let mut ops = Vec::new();
        let mut i = 1;
        while i < args.len() {
          let op_name = args[i];
          if op_name.eq_ignore_ascii_case(b"set") && i + 2 < args.len() {
            let k = arg_string(args[i + 1]);
            let v = args[i + 2].to_vec();
            ops.push(BatchOp::Set(k, v));
            i += 3;
          } else if op_name.eq_ignore_ascii_case(b"del") && i + 1 < args.len() {
            let k = arg_string(args[i + 1]);
            ops.push(BatchOp::Del(k));
            i += 2;
          } else {
            i += 1;
          }
        }
        Ok(Cmd::Batch(ops))
      }
    }
    // @cmd: TXN json_or_bitcode_payload / RAFT.TXN json_or_bitcode_payload
    "txn" | "raft.txn" => {
      check_min_args(cmd_name, args, 2)?;
      let raw_bytes = args[1];
      if let Ok(req) = bitcode::decode::<TxnReq>(raw_bytes) {
        Ok(Cmd::Txn(req))
      } else {
        let json_str = arg_str(raw_bytes);
        let val: sonic_rs::Value = sonic_rs::from_str(json_str)
          .map_err(|e| Error::invalid_data(format!("Invalid JSON txn format: {e}")))?;
        let req = parse_txn_json(&val)?;
        Ok(Cmd::Txn(req))
      }
    }
    // @cmd: RAFT.JOIN node_id endpoint
    "raft.join" => {
      check_min_args(cmd_name, args, 3)?;
      let node_id = parse_u64_strict(args[1])?;
      let ep = arg_string(args[2]);
      Ok(Cmd::RaftJoin {
        node_id,
        endpoint: ep,
      })
    }
    // @cmd: RAFT.LEAVE node_id
    "raft.leave" => {
      check_min_args(cmd_name, args, 2)?;
      let node_id = parse_u64_strict(args[1])?;
      Ok(Cmd::RaftLeave(node_id))
    }
    // @cmd: RAFT.MEMBERS
    "raft.members" => Ok(Cmd::RaftMembers),
    // @cmd: RAFT.SNAPSHOT
    "raft.snapshot" => Ok(Cmd::RaftSnapshot),
    // @cmd: RAFT.SNAPSHOT_STATUS
    "raft.snapshot_status" => Ok(Cmd::RaftSnapshotStatus),
    // @cmd: RAFT.PURGE upto_index
    "raft.purge" => {
      check_min_args(cmd_name, args, 2)?;
      let upto = parse_u64_strict(args[1])?;
      Ok(Cmd::RaftPurge(upto))
    }
    // @cmd: RAFT.HEALTH
    "raft.health" => Ok(Cmd::RaftHealth),
    // @cmd: RAFT.METRICS
    "raft.metrics" => Ok(Cmd::RaftMetrics),
    // @cmd: RAFT.STATUS
    "raft.status" => Ok(Cmd::RaftStatus),

    _ => return Ok(None),
  };
  res.map(Some)
}

fn parse_txn_json(val: &sonic_rs::Value) -> Result<TxnReq> {
  use crate::txn::{Operation, RaftTxnOp, TxnCondition, UpsertKV};

  let mut conditions = Vec::new();
  if let Some(cond_arr) = val.get("condition").and_then(|v| v.as_array()) {
    for c in cond_arr {
      let key = c
        .get("key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
      let op_str = c.get("op").and_then(|v| v.as_str()).unwrap_or("exists");
      let expected = match op_str.to_ascii_lowercase().as_str() {
        "exists" => RaftTxnOp::Exists,
        "notexists" | "not_exists" => RaftTxnOp::NotExists,
        "equal" | "eq" => {
          let v = c
            .get("value")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .as_bytes()
            .to_vec();
          RaftTxnOp::Equal(v)
        }
        "notequal" | "ne" => {
          let v = c
            .get("value")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .as_bytes()
            .to_vec();
          RaftTxnOp::NotEqual(v)
        }
        "greater" | "gt" => {
          let v = c
            .get("value")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .as_bytes()
            .to_vec();
          RaftTxnOp::Greater(v)
        }
        "less" | "lt" => {
          let v = c
            .get("value")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .as_bytes()
            .to_vec();
          RaftTxnOp::Less(v)
        }
        "greaterequal" | "ge" | "gte" => {
          let v = c
            .get("value")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .as_bytes()
            .to_vec();
          RaftTxnOp::GreaterEqual(v)
        }
        "lessequal" | "le" | "lte" => {
          let v = c
            .get("value")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .as_bytes()
            .to_vec();
          RaftTxnOp::LessEqual(v)
        }
        _ => RaftTxnOp::Exists,
      };
      conditions.push(TxnCondition { key, expected });
    }
  }

  let mut if_then = Vec::new();
  if let Some(if_arr) = val.get("if_then").and_then(|v| v.as_array()) {
    for op in if_arr {
      let key = op
        .get("key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
      let action = op
        .get("action")
        .or_else(|| op.get("op"))
        .and_then(|v| v.as_str())
        .unwrap_or("update");
      let ttl = op.get("ttl_ms").and_then(|v| v.as_u64());
      if action.eq_ignore_ascii_case("delete") || action.eq_ignore_ascii_case("del") {
        if_then.push(UpsertKV {
          key,
          value: Operation::Delete,
          expire_at_ms: None,
        });
      } else {
        let value = op
          .get("value")
          .and_then(|v| v.as_str())
          .unwrap_or("")
          .as_bytes()
          .to_vec();
        if_then.push(UpsertKV {
          key,
          value: Operation::Update(value),
          expire_at_ms: ttl,
        });
      }
    }
  }

  let mut else_then = Vec::new();
  if let Some(else_arr) = val.get("else_then").and_then(|v| v.as_array()) {
    for op in else_arr {
      let key = op
        .get("key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
      let action = op
        .get("action")
        .or_else(|| op.get("op"))
        .and_then(|v| v.as_str())
        .unwrap_or("update");
      let ttl = op.get("ttl_ms").and_then(|v| v.as_u64());
      if action.eq_ignore_ascii_case("delete") || action.eq_ignore_ascii_case("del") {
        else_then.push(UpsertKV {
          key,
          value: Operation::Delete,
          expire_at_ms: None,
        });
      } else {
        let value = op
          .get("value")
          .and_then(|v| v.as_str())
          .unwrap_or("")
          .as_bytes()
          .to_vec();
        else_then.push(UpsertKV {
          key,
          value: Operation::Update(value),
          expire_at_ms: ttl,
        });
      }
    }
  }

  let return_previous = val
    .get("return_previous")
    .and_then(|v| v.as_bool())
    .unwrap_or(false);

  Ok(TxnReq {
    condition: conditions,
    if_then,
    else_then,
    return_previous,
  })
}
