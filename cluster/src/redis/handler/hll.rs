use std::sync::Arc;

use super::context::{ConnectionContext, HyperLogLogMeta};
use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::redis::cmd::RedisCommand;
use crate::redis::hll::HyperLogLog;
use crate::redis::protocol::RespValue;
use crate::util::now_millis;
use wedb_raft::types::{BatchWriteReq, GetKVReq, UpsertKV};

/// HyperLogLog 基数统计命令主调度处理器
pub async fn handle_hll(
  node: &Arc<RaftNode>,
  ctx: &mut ConnectionContext,
  cmd: RedisCommand,
) -> Result<RespValue> {
  let kc = ctx.key_composer();

  match cmd {
    RedisCommand::PfAdd(key, elements) => {
      let meta_k = kc.hll_meta(&key);
      let hll_k = kc.hll_key(&key);
      let now = now_millis();
      let meta = match node
        .read(GetKVReq {
          key: meta_k.clone(),
        })
        .await?
      {
        Some(b) => {
          let m =
            HyperLogLogMeta::decode(&b).ok_or_else(|| Error::internal("corrupted hll metadata"))?;
          if m.is_expired(now) {
            HyperLogLogMeta::new(0, 0)
          } else {
            m
          }
        }
        None => HyperLogLogMeta::new(0, 0),
      };

      let mut hll = match node.read(GetKVReq { key: hll_k.clone() }).await? {
        Some(b) => HyperLogLog::from_bytes(&b),
        None => HyperLogLog::new(),
      };

      let mut updated = false;
      for el in elements {
        updated |= hll.add(&el);
      }

      if updated || meta.base.version == 0 {
        let entries = vec![
          UpsertKV::insert(meta_k, meta.encode().to_vec()),
          UpsertKV::insert(hll_k, hll.to_bytes().to_vec()),
        ];
        node.batch_write(BatchWriteReq { entries }).await?;
      }
      Ok(RespValue::Int(if updated { 1 } else { 0 }))
    }
    RedisCommand::PfCount(keys) => {
      if keys.is_empty() {
        return Ok(RespValue::Int(0));
      }
      let now = now_millis();
      let mut merged_hll = HyperLogLog::new();
      for k in keys {
        let meta_k = kc.hll_meta(&k);
        if let Some(b) = node.read(GetKVReq { key: meta_k }).await?
          && let Some(m) = HyperLogLogMeta::decode(&b)
          && m.is_expired(now)
        {
          continue;
        }
        let hll_k = kc.hll_key(&k);
        if let Some(b) = node.read(GetKVReq { key: hll_k }).await? {
          let hll = HyperLogLog::from_bytes(&b);
          merged_hll.merge(&hll);
        }
      }
      Ok(RespValue::Int(merged_hll.count() as i64))
    }
    RedisCommand::PfMerge(dst, srcs) => {
      let now = now_millis();
      let mut merged_hll = HyperLogLog::new();
      let dst_meta_k = kc.hll_meta(&dst);
      let dst_k = kc.hll_key(&dst);

      if let Some(b) = node.read(GetKVReq { key: dst_k.clone() }).await? {
        let is_expired = if let Some(mb) = node
          .read(GetKVReq {
            key: dst_meta_k.clone(),
          })
          .await?
        {
          HyperLogLogMeta::decode(&mb)
            .map(|m| m.is_expired(now))
            .unwrap_or(false)
        } else {
          false
        };
        if !is_expired {
          merged_hll.merge(&HyperLogLog::from_bytes(&b));
        }
      }
      for s in srcs {
        let s_meta_k = kc.hll_meta(&s);
        if let Some(mb) = node.read(GetKVReq { key: s_meta_k }).await?
          && let Some(m) = HyperLogLogMeta::decode(&mb)
          && m.is_expired(now)
        {
          continue;
        }
        let s_k = kc.hll_key(&s);
        if let Some(b) = node.read(GetKVReq { key: s_k }).await? {
          merged_hll.merge(&HyperLogLog::from_bytes(&b));
        }
      }
      let meta = HyperLogLogMeta::new(0, 0);
      let entries = vec![
        UpsertKV::insert(dst_meta_k, meta.encode().to_vec()),
        UpsertKV::insert(dst_k, merged_hll.to_bytes().to_vec()),
      ];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::PfSelfTest => {
      if HyperLogLog::selftest() {
        Ok(RespValue::ok())
      } else {
        Err(Error::internal("ERR HyperLogLog selftest failed"))
      }
    }
    _ => Err(Error::internal("unsupported hyperloglog command")),
  }
}
