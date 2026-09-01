use crate::handler::resp_util::{float_or_nan, float_to_blob};
use std::sync::Arc;
use webc_cmd::Cmd;
use wedb_embed::{Error, Result, TDigestMerge, WeDb};
use wedb_resp::RespValue;

/// 处理所有 TDigest (分位数估计) 命令
pub async fn handle_tdigest(db: &Arc<WeDb>, cmd: Cmd) -> Result<RespValue> {
  match cmd {
    Cmd::TDigestCreate { key, compression } => {
      let comp = compression.unwrap_or(100.0);
      db.tdigest_create(key.as_bytes(), comp)?;
      Ok(RespValue::ok())
    }
    Cmd::TDigestAdd(key, values) => {
      db.tdigest_add(key.as_bytes(), &values)?;
      Ok(RespValue::ok())
    }
    Cmd::TDigestQuantile(key, quantiles) => {
      let q_res = db.tdigest_quantile(key.as_bytes(), &quantiles)?;
      let results = q_res.into_iter().map(float_or_nan).collect();
      Ok(RespValue::Arr(results))
    }
    Cmd::TDigestCdf(key, vals) => {
      let cdf_res = db.tdigest_cdf(key.as_bytes(), &vals)?;
      let results = cdf_res.into_iter().map(float_or_nan).collect();
      Ok(RespValue::Arr(results))
    }
    Cmd::TDigestMin(key) => {
      let val = db.tdigest_min(key.as_bytes())?;
      if val.is_nan() {
        Ok(RespValue::Blob(b"nan".to_vec()))
      } else {
        Ok(float_to_blob(val))
      }
    }
    Cmd::TDigestMax(key) => {
      let val = db.tdigest_max(key.as_bytes())?;
      if val.is_nan() {
        Ok(RespValue::Blob(b"nan".to_vec()))
      } else {
        Ok(float_to_blob(val))
      }
    }
    Cmd::TDigestRank(key, vals) => {
      let ranks = db.tdigest_rank(key.as_bytes(), &vals)?;
      let results = ranks.into_iter().map(RespValue::Int).collect();
      Ok(RespValue::Arr(results))
    }
    Cmd::TDigestRevRank(key, vals) => {
      let ranks = db.tdigest_revrank(key.as_bytes(), &vals)?;
      let results = ranks.into_iter().map(RespValue::Int).collect();
      Ok(RespValue::Arr(results))
    }
    Cmd::TDigestByRank(key, ranks) => {
      let res = db.tdigest_byrank(key.as_bytes(), &ranks)?;
      let results = res.into_iter().map(float_or_nan).collect();
      Ok(RespValue::Arr(results))
    }
    Cmd::TDigestByRevRank(key, ranks) => {
      let res = db.tdigest_byrevrank(key.as_bytes(), &ranks)?;
      let results = res.into_iter().map(float_or_nan).collect();
      Ok(RespValue::Arr(results))
    }
    Cmd::TDigestTrimmedMean(key, low_cut, high_cut) => {
      let mean = db.tdigest_trimmed_mean(key.as_bytes(), low_cut, high_cut)?;
      Ok(float_or_nan(mean))
    }
    Cmd::TDigestReset(key) => {
      db.tdigest_reset(key.as_bytes())?;
      Ok(RespValue::ok())
    }
    Cmd::TDigestMerge {
      dst,
      sources,
      compression,
      override_flag,
    } => {
      let src_slices: Vec<&[u8]> = sources.iter().map(|s| s.as_bytes()).collect();
      let opts = TDigestMerge {
        compression: compression.map(|c| c as u32),
        override_dest: override_flag,
      };
      db.tdigest_merge(dst.as_bytes(), &src_slices, opts)?;
      Ok(RespValue::ok())
    }
    Cmd::TDigestInfo(key) => {
      let info = db.tdigest_info(key.as_bytes())?;
      let min_blob = float_or_nan(info.minimum);
      let max_blob = float_or_nan(info.maximum);
      let mw = info.merged_weight;
      let uw = info.unmerged_weight;
      let tw = info.total_weight;
      let entries = vec![
        RespValue::Simple("Compression".to_string()),
        RespValue::Int(info.compression as i64),
        RespValue::Simple("Capacity".to_string()),
        RespValue::Int(info.capacity as i64),
        RespValue::Simple("Merged nodes".to_string()),
        RespValue::Int(info.merged_nodes as i64),
        RespValue::Simple("Unmerged nodes".to_string()),
        RespValue::Int(info.unmerged_nodes as i64),
        RespValue::Simple("Merged weight".to_string()),
        RespValue::Blob(format!("{mw}").into_bytes()),
        RespValue::Simple("Unmerged weight".to_string()),
        RespValue::Blob(format!("{uw}").into_bytes()),
        RespValue::Simple("Total weight".to_string()),
        RespValue::Blob(format!("{tw}").into_bytes()),
        RespValue::Simple("Observations".to_string()),
        RespValue::Int(info.observations as i64),
        RespValue::Simple("Total compressions".to_string()),
        RespValue::Int(info.total_compressions as i64),
        RespValue::Simple("Minimum".to_string()),
        min_blob,
        RespValue::Simple("Maximum".to_string()),
        max_blob,
      ];
      Ok(RespValue::Arr(entries))
    }
    _ => Err(Error::internal("unsupported tdigest command")),
  }
}
