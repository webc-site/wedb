use std::sync::Arc;

use super::context::{ConnectionContext, KeyComposer};
use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::redis::cmd::RedisCommand;
use crate::redis::protocol::RespValue;
use crate::redis::resp_util::{float_or_nan, float_to_blob};
use crate::redis::tdigest::TDigest;
use wedb_raft::types::{BatchWriteReq, GetKVReq, UpsertKV};

#[inline]
async fn read_tdigest(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
) -> Result<Option<TDigest>> {
  let td_k = kc.tdigest_meta(key);
  match node.read(GetKVReq { key: td_k }).await? {
    Some(b) => Ok(Some(
      bitcode::decode::<TDigest>(&b).unwrap_or_else(|_| TDigest::new(100.0)),
    )),
    None => Ok(None),
  }
}

/// TDigest 分位数统计命令主调度处理器
pub async fn handle_tdigest(
  node: &Arc<RaftNode>,
  ctx: &mut ConnectionContext,
  cmd: RedisCommand,
) -> Result<RespValue> {
  let kc = ctx.key_composer();

  match cmd {
    RedisCommand::TDigestCreate { key, compression } => {
      let td_k = kc.tdigest_meta(&key);
      if node.read(GetKVReq { key: td_k.clone() }).await?.is_some() {
        return Err(Error::invalid_data("ERR TDigest: key already exists"));
      }
      let td = TDigest::new(compression.unwrap_or(100.0));
      let encoded = bitcode::encode(&td);
      let entries = vec![UpsertKV::insert(td_k, encoded)];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::TDigestAdd(key, values) => {
      let mut td = read_tdigest(node, &kc, &key)
        .await?
        .unwrap_or_else(|| TDigest::new(100.0));
      td.add_batch(&values);
      let encoded = bitcode::encode(&td);
      let entries = vec![UpsertKV::insert(kc.tdigest_meta(&key), encoded)];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::TDigestQuantile(key, quantiles) => {
      if let Some(mut td) = read_tdigest(node, &kc, &key).await? {
        let mut results = Vec::with_capacity(quantiles.len());
        for q in quantiles {
          results.push(float_or_nan(Some(td.quantile(q))));
        }
        Ok(RespValue::Arr(results))
      } else {
        let nulls = quantiles
          .into_iter()
          .map(|_| RespValue::Blob(b"nan".to_vec()))
          .collect();
        Ok(RespValue::Arr(nulls))
      }
    }
    RedisCommand::TDigestCdf(key, vals) => {
      if let Some(mut td) = read_tdigest(node, &kc, &key).await? {
        let mut results = Vec::with_capacity(vals.len());
        for v in vals {
          results.push(float_or_nan(Some(td.cdf(v))));
        }
        Ok(RespValue::Arr(results))
      } else {
        let nulls = vals
          .into_iter()
          .map(|_| RespValue::Blob(b"nan".to_vec()))
          .collect();
        Ok(RespValue::Arr(nulls))
      }
    }
    RedisCommand::TDigestMin(key) => {
      if let Some(td) = read_tdigest(node, &kc, &key).await? {
        if td.is_empty() {
          Ok(RespValue::Blob(b"nan".to_vec()))
        } else {
          Ok(float_to_blob(td.min))
        }
      } else {
        Ok(RespValue::Blob(b"nan".to_vec()))
      }
    }
    RedisCommand::TDigestMax(key) => {
      if let Some(td) = read_tdigest(node, &kc, &key).await? {
        if td.is_empty() {
          Ok(RespValue::Blob(b"nan".to_vec()))
        } else {
          Ok(float_to_blob(td.max))
        }
      } else {
        Ok(RespValue::Blob(b"nan".to_vec()))
      }
    }
    RedisCommand::TDigestRank(key, vals) => {
      if let Some(mut td) = read_tdigest(node, &kc, &key).await? {
        let ranks = vals
          .into_iter()
          .map(|v| RespValue::Int(td.rank(v)))
          .collect();
        Ok(RespValue::Arr(ranks))
      } else {
        let ranks = vals.into_iter().map(|_| RespValue::Int(-2)).collect();
        Ok(RespValue::Arr(ranks))
      }
    }
    RedisCommand::TDigestRevRank(key, vals) => {
      if let Some(mut td) = read_tdigest(node, &kc, &key).await? {
        let ranks = vals
          .into_iter()
          .map(|v| RespValue::Int(td.revrank(v)))
          .collect();
        Ok(RespValue::Arr(ranks))
      } else {
        let ranks = vals.into_iter().map(|_| RespValue::Int(-2)).collect();
        Ok(RespValue::Arr(ranks))
      }
    }
    RedisCommand::TDigestByRank(key, ranks) => {
      if let Some(mut td) = read_tdigest(node, &kc, &key).await? {
        let res = ranks
          .into_iter()
          .map(|r| float_or_nan(Some(td.byrank(r))))
          .collect();
        Ok(RespValue::Arr(res))
      } else {
        let res = ranks
          .into_iter()
          .map(|_| RespValue::Blob(b"nan".to_vec()))
          .collect();
        Ok(RespValue::Arr(res))
      }
    }
    RedisCommand::TDigestByRevRank(key, ranks) => {
      if let Some(mut td) = read_tdigest(node, &kc, &key).await? {
        let res = ranks
          .into_iter()
          .map(|r| float_or_nan(Some(td.byrevrank(r))))
          .collect();
        Ok(RespValue::Arr(res))
      } else {
        let res = ranks
          .into_iter()
          .map(|_| RespValue::Blob(b"nan".to_vec()))
          .collect();
        Ok(RespValue::Arr(res))
      }
    }
    RedisCommand::TDigestTrimmedMean(key, low_cut, high_cut) => {
      if let Some(mut td) = read_tdigest(node, &kc, &key).await? {
        let mean = td.trimmed_mean(low_cut, high_cut);
        Ok(float_or_nan(Some(mean)))
      } else {
        Ok(RespValue::Blob(b"nan".to_vec()))
      }
    }
    RedisCommand::TDigestMerge {
      dst,
      sources,
      compression,
      override_flag,
    } => {
      let dst_k = kc.tdigest_meta(&dst);
      let mut merged = if !override_flag {
        read_tdigest(node, &kc, &dst)
          .await?
          .unwrap_or_else(|| TDigest::new(compression.unwrap_or(100.0)))
      } else {
        TDigest::new(compression.unwrap_or(100.0))
      };

      for s in sources {
        if let Some(mut src_td) = read_tdigest(node, &kc, &s).await? {
          merged.merge_from(&mut src_td);
        }
      }

      let encoded = bitcode::encode(&merged);
      let entries = vec![UpsertKV::insert(dst_k, encoded)];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::TDigestReset(key) => {
      let mut td = read_tdigest(node, &kc, &key)
        .await?
        .unwrap_or_else(|| TDigest::new(100.0));
      td.reset();
      let encoded = bitcode::encode(&td);
      let entries = vec![UpsertKV::insert(kc.tdigest_meta(&key), encoded)];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::TDigestInfo(key) => {
      if let Some(mut td) = read_tdigest(node, &kc, &key).await? {
        td.ensure_merged();
        let unmerged_w = (td.total_weight - td.merged_weight).max(0.0);
        let min_val = if td.is_empty() {
          RespValue::Blob(b"nan".to_vec())
        } else {
          float_to_blob(td.min)
        };
        let max_val = if td.is_empty() {
          RespValue::Blob(b"nan".to_vec())
        } else {
          float_to_blob(td.max)
        };

        let info = vec![
          RespValue::Simple("Compression".to_string()),
          RespValue::Int(td.compression as i64),
          RespValue::Simple("Capacity".to_string()),
          RespValue::Int(td.capacity as i64),
          RespValue::Simple("Merged nodes".to_string()),
          RespValue::Int(td.centroids.len() as i64),
          RespValue::Simple("Unmerged nodes".to_string()),
          RespValue::Int(td.unmerged_buffer.len() as i64),
          RespValue::Simple("Merged weight".to_string()),
          float_to_blob(td.merged_weight),
          RespValue::Simple("Unmerged weight".to_string()),
          float_to_blob(unmerged_w),
          RespValue::Simple("Total weight".to_string()),
          float_to_blob(td.total_weight),
          RespValue::Simple("Observations".to_string()),
          RespValue::Int(td.total_observations as i64),
          RespValue::Simple("Minimum".to_string()),
          min_val,
          RespValue::Simple("Maximum".to_string()),
          max_val,
        ];
        Ok(RespValue::Arr(info))
      } else {
        Err(Error::invalid_data("ERR key does not exist"))
      }
    }
    _ => Err(Error::internal("unsupported tdigest command")),
  }
}
