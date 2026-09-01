use std::sync::{Arc, RwLockWriteGuard};

use webc_cmd::ShardTopology;

use super::context::ConnectionContext;
use crate::conf::Endpoint;
use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::redis::cmd::{BatchOp, RedisCommand};
use crate::redis::protocol::RespValue;
use crate::redis::pubsub::GLOBAL_PUBSUB;
use wedb_raft::types::{
  BatchWriteReq, GetMemberReq, JoinRequest, LeaveRequest, TxnReply, UpsertKV,
};

/// 获取当前集群中各节点的 Redis 服务网络地址列表
pub(crate) fn get_cluster_sm_nodes(node: &Arc<RaftNode>) -> Vec<(u64, String)> {
  if let Ok(nodes) = node.state_machine().get_nodes() {
    nodes
      .into_iter()
      .map(|(id, n)| (id, node.resolve_node_redis_addr(&n)))
      .collect()
  } else {
    Vec::new()
  }
}

/// 同步并获取最新的 ShardTopology 写锁保护
#[inline]
pub(crate) fn sync_sharding_topology<'a>(
  node: &'a Arc<RaftNode>,
) -> Result<RwLockWriteGuard<'a, ShardTopology>> {
  let mut topo = node
    .sharding()
    .write()
    .map_err(|e| Error::internal(e.to_string()))?;
  let current_leader = node.current_leader_id();
  let sm_nodes = get_cluster_sm_nodes(node);
  topo.sync_raft_state(current_leader, &sm_nodes);
  Ok(topo)
}

/// 集群管理、Raft分布式扩展、事务与PubSub命令主调度处理器
pub async fn handle_admin(
  node: &Arc<RaftNode>,
  ctx: &mut ConnectionContext,
  cmd: RedisCommand,
) -> Result<RespValue> {
  let kc = ctx.key_composer();

  match cmd {
    // ================= 17. 发布订阅 (PubSub) =================
    RedisCommand::Publish(channel, message) => {
      let recipients = GLOBAL_PUBSUB.publish(&channel, &message);
      Ok(RespValue::Int(recipients as i64))
    }
    RedisCommand::Subscribe(channels) => {
      let mut results = Vec::with_capacity(channels.len());
      for (idx, ch) in channels.into_iter().enumerate() {
        results.push(RespValue::Arr(vec![
          RespValue::Blob(b"subscribe".to_vec()),
          RespValue::Blob(ch.into_bytes()),
          RespValue::Int((idx + 1) as i64),
        ]));
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::PSubscribe(patterns) => {
      let mut results = Vec::with_capacity(patterns.len());
      for (idx, pat) in patterns.into_iter().enumerate() {
        results.push(RespValue::Arr(vec![
          RespValue::Blob(b"psubscribe".to_vec()),
          RespValue::Blob(pat.into_bytes()),
          RespValue::Int((idx + 1) as i64),
        ]));
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::Unsubscribe(channels) => {
      let count = GLOBAL_PUBSUB.channel_count();
      let mut results = Vec::with_capacity(channels.len());
      for ch in channels {
        results.push(RespValue::Arr(vec![
          RespValue::Blob(b"unsubscribe".to_vec()),
          RespValue::Blob(ch.into_bytes()),
          RespValue::Int(count as i64),
        ]));
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::PUnsubscribe(patterns) => {
      let count = GLOBAL_PUBSUB.channel_count();
      let mut results = Vec::with_capacity(patterns.len());
      for pat in patterns {
        results.push(RespValue::Arr(vec![
          RespValue::Blob(b"punsubscribe".to_vec()),
          RespValue::Blob(pat.into_bytes()),
          RespValue::Int(count as i64),
        ]));
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::PubSub(args) => {
      let subcmd = args.first().cloned().unwrap_or_default();
      match subcmd.to_uppercase().as_str() {
        "CHANNELS" | "SHARDCHANNELS" => {
          let channels = GLOBAL_PUBSUB.list_channels();
          let pat = args.get(1).cloned().unwrap_or_else(|| "*".to_string());
          let list = channels
            .into_iter()
            .filter(|c| pat == "*" || c.contains(pat.as_str()))
            .map(|c| RespValue::Blob(c.as_bytes().to_vec()))
            .collect();
          Ok(RespValue::Arr(list))
        }
        "NUMSUB" | "SHARDNUMSUB" => {
          let mut pairs = Vec::with_capacity((args.len().saturating_sub(1)) * 2);
          for ch in &args[1..] {
            pairs.push(RespValue::Blob(ch.clone().into_bytes()));
            pairs.push(RespValue::Int(0));
          }
          Ok(RespValue::Arr(pairs))
        }
        "NUMPAT" => Ok(RespValue::Int(0)),
        "HELP" => Ok(RespValue::Arr(vec![
          RespValue::Blob(b"PUBSUB <subcmd> [<arg> [value] ...]. Subcmds are:".to_vec()),
          RespValue::Blob(
            b"CHANNELS [<pattern>] -- List active channels matching pattern.".to_vec(),
          ),
          RespValue::Blob(b"NUMSUB [<channel> ...] -- Return the number of subscribers.".to_vec()),
          RespValue::Blob(b"NUMPAT -- Return the number of subscriptions to patterns.".to_vec()),
          RespValue::Blob(b"SHARDCHANNELS [<pattern>] -- List active shard channels.".to_vec()),
          RespValue::Blob(
            b"SHARDNUMSUB [<channel> ...] -- Return subscribers count for shard channels.".to_vec(),
          ),
          RespValue::Blob(b"HELP -- Print this help.".to_vec()),
        ])),
        _ => Ok(RespValue::Arr(Vec::new())),
      }
    }

    // ================= 18. 分布式 Raft 与集群扩展命令 =================
    RedisCommand::RaftJoin { node_id, endpoint } => {
      let ep = Endpoint::parse(&endpoint).map_err(|e| Error::invalid_data(e.to_string()))?;
      let req = JoinRequest {
        node_id,
        endpoint: ep,
      };
      node.add_node(req).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::RaftLeave(node_id) => {
      let req = LeaveRequest { node_id };
      node.remove_node(req).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::RaftMembers | RedisCommand::ClusterMembers => {
      let reply = node.get_members(GetMemberReq {}).await?;
      let members = reply
        .membership
        .into_iter()
        .map(|(id, node_info)| {
          RespValue::Arr(vec![
            RespValue::Int(id as i64),
            RespValue::Blob(node_info.to_string().into_bytes()),
          ])
        })
        .collect();
      Ok(RespValue::Arr(members))
    }
    RedisCommand::RaftSnapshot => {
      let snapshot = node.trigger_snapshot().await?;
      match snapshot {
        Some(id) => {
          let idx = id.index;
          Ok(RespValue::Simple(format!("SNAPSHOT: index={idx}")))
        }
        None => Ok(RespValue::Simple("SNAPSHOT: ok".to_string())),
      }
    }
    RedisCommand::RaftSnapshotStatus => {
      let log_id = node.snapshot_log_id();
      match log_id {
        Some(id) => {
          let idx = id.index;
          Ok(RespValue::Simple(format!("SNAPSHOT: index={idx}")))
        }
        None => Ok(RespValue::Simple("SNAPSHOT: none".to_string())),
      }
    }
    RedisCommand::RaftHealth => {
      let metrics = node.raft().metrics().borrow_watched().clone();
      let map = vec![
        (RespValue::Blob(b"healthy".to_vec()), RespValue::Bool(true)),
        (
          RespValue::Blob(b"current_leader".to_vec()),
          match metrics.current_leader {
            Some(l) => RespValue::Int(l as i64),
            None => RespValue::Null,
          },
        ),
        (RespValue::Blob(b"state".to_vec()), {
          let state = metrics.state;
          RespValue::Simple(format!("{state:?}"))
        }),
      ];
      Ok(RespValue::Map(map))
    }
    RedisCommand::RaftMetrics => {
      let metrics = node.raft().metrics().borrow_watched().clone();
      let map = vec![
        (
          RespValue::Blob(b"current_term".to_vec()),
          RespValue::Int(metrics.current_term as i64),
        ),
        (
          RespValue::Blob(b"last_log_index".to_vec()),
          match metrics.last_log_index {
            Some(i) => RespValue::Int(i as i64),
            None => RespValue::Null,
          },
        ),
        (
          RespValue::Blob(b"last_applied".to_vec()),
          match metrics.last_applied {
            Some(log_id) => RespValue::Int(log_id.index as i64),
            None => RespValue::Null,
          },
        ),
      ];
      Ok(RespValue::Map(map))
    }
    RedisCommand::RaftStatus => {
      let metrics = node.raft().metrics().borrow_watched().clone();
      let state = metrics.state;
      Ok(RespValue::Simple(format!("{state:?}")))
    }
    RedisCommand::RaftPurge(upto_index) => {
      node.purge_log(upto_index).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::Txn(txn_req) => {
      let reply = node.txn(txn_req).await?;
      match reply {
        TxnReply::Success {
          branch,
          prev_values,
        } => {
          let mut prevs = Vec::new();
          for v_opt in prev_values {
            prevs.push(match v_opt {
              Some(v) => RespValue::Blob(v),
              None => RespValue::Null,
            });
          }
          let map = vec![
            (
              RespValue::Blob(b"branch".to_vec()),
              RespValue::Simple(
                if branch {
                  "SUCCESS_TRUE"
                } else {
                  "SUCCESS_FALSE"
                }
                .to_string(),
              ),
            ),
            (
              RespValue::Blob(b"prev_values".to_vec()),
              RespValue::Arr(prevs),
            ),
          ];
          Ok(RespValue::Map(map))
        }
      }
    }
    RedisCommand::Batch(ops) => {
      let mut entries = Vec::with_capacity(ops.len());
      for op in ops {
        match op {
          BatchOp::Set(k, v) => entries.push(UpsertKV::insert(kc.raw_key(&k), v)),
          BatchOp::Del(k) => entries.push(UpsertKV::delete(kc.raw_key(&k))),
        }
      }
      let count = entries.len();
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::Int(count as i64))
    }
    RedisCommand::ClusterNodes => {
      let topo = sync_sharding_topology(node)?;
      Ok(topo.to_cluster_nodes_resp(node.conf.node_id))
    }
    RedisCommand::ClusterSlots => {
      let topo = sync_sharding_topology(node)?;
      Ok(topo.to_cluster_slots_resp())
    }
    RedisCommand::ClusterShards => {
      let topo = sync_sharding_topology(node)?;
      Ok(topo.to_cluster_shards_resp())
    }
    RedisCommand::ClusterInfo => {
      let topo = node
        .sharding()
        .read()
        .map_err(|e| Error::internal(e.to_string()))?;
      Ok(topo.to_cluster_info_resp())
    }
    RedisCommand::ClusterMyId => {
      let node_id = node.conf.node_id;
      let my_id = format!("{node_id:040x}");
      Ok(RespValue::Blob(my_id.into_bytes()))
    }
    RedisCommand::ClusterKeySlot(key) => {
      let slot = webc_cmd::Crc::key_slot(key.as_bytes());
      Ok(RespValue::Int(slot as i64))
    }
    RedisCommand::ClusterMeet { ip, port, node_id } => {
      let mut topo = node
        .sharding()
        .write()
        .map_err(|e| Error::internal(e.to_string()))?;
      let nid = node_id.unwrap_or_else(|| (topo.nodes.len() as u64) + 1);
      let addr = format!("{ip}:{port}");
      topo.register_node(nid, addr);
      topo.auto_expand_under_replicated(webc_cmd::DEFAULT_REPLICAS_PER_SHARD);
      Ok(RespValue::ok())
    }
    RedisCommand::ClusterForget(nid) => {
      let mut topo = node
        .sharding()
        .write()
        .map_err(|e| Error::internal(e.to_string()))?;
      topo
        .drain_node(nid)
        .map_err(|e| Error::redis(e.to_string()))?;
      Ok(RespValue::ok())
    }
    RedisCommand::ClusterRebalance => {
      let mut topo = node
        .sharding()
        .write()
        .map_err(|e| Error::internal(e.to_string()))?;
      topo.rebalance_3replicas();
      Ok(RespValue::ok())
    }
    RedisCommand::ClusterSetTags { node_id, tags } => {
      let mut topo = node
        .sharding()
        .write()
        .map_err(|e| Error::internal(e.to_string()))?;
      let mut loc = topo.get_node_location(node_id).cloned().unwrap_or_default();
      for tag in tags {
        if let Some((k, v)) = tag.split_once('=') {
          match k.to_ascii_lowercase().as_str() {
            "region" => loc.region = v.to_string(),
            "zone" | "az" | "dc" => loc.zone = v.to_string(),
            "rack" => loc.rack = v.to_string(),
            "host" => loc.host = v.to_string(),
            "weight" => {
              if let Ok(w) = v.parse::<u32>() {
                topo.set_node_weight(node_id, w);
              }
            }
            _ => {}
          }
        }
      }
      topo.set_node_location(node_id, loc);
      Ok(RespValue::ok())
    }
    RedisCommand::ClusterGetTags(node_id) => {
      let topo = node
        .sharding()
        .read()
        .map_err(|e| Error::internal(e.to_string()))?;
      let loc = topo.get_node_location(node_id).cloned().unwrap_or_default();
      let weight = topo.get_node_weight(node_id);
      let addr = topo.nodes.get(&node_id).cloned().unwrap_or_default();
      let pairs = vec![
        (
          RespValue::Simple("node_id".to_string()),
          RespValue::Int(node_id as i64),
        ),
        (
          RespValue::Simple("addr".to_string()),
          RespValue::Simple(addr),
        ),
        (
          RespValue::Simple("weight".to_string()),
          RespValue::Int(weight as i64),
        ),
        (
          RespValue::Simple("region".to_string()),
          RespValue::Simple(loc.region),
        ),
        (
          RespValue::Simple("zone".to_string()),
          RespValue::Simple(loc.zone),
        ),
        (
          RespValue::Simple("rack".to_string()),
          RespValue::Simple(loc.rack),
        ),
        (
          RespValue::Simple("host".to_string()),
          RespValue::Simple(loc.host),
        ),
      ];
      Ok(RespValue::Map(pairs))
    }
    RedisCommand::ReadOnly => {
      ctx.is_readonly = true;
      Ok(RespValue::ok())
    }
    RedisCommand::ReadWrite => {
      ctx.is_readonly = false;
      Ok(RespValue::ok())
    }
    RedisCommand::Asking => {
      ctx.is_asking = true;
      Ok(RespValue::ok())
    }
    RedisCommand::ClusterFailover
    | RedisCommand::ClusterSaveConfig
    | RedisCommand::ClusterReset(_) => Ok(RespValue::ok()),
    RedisCommand::ClusterCountKeysInSlot(_) => Ok(RespValue::Int(0)),
    RedisCommand::ClusterGetKeysInSlot { .. } => Ok(RespValue::Arr(Vec::new())),
    RedisCommand::Cluster(_) => Ok(RespValue::ok()),
    _ => Err(Error::internal("unsupported admin/cluster command")),
  }
}
