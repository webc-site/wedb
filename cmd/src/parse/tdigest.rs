use crate::parse::util::*;
use crate::types::Cmd;
use wedb_embed::Result;

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: TDIGEST.CREATE key [COMPRESSION compression]
    "tdigest.create" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let mut comp = None;
      let mut idx = 2;
      while idx < args.len() {
        let opt = args[idx];
        if opt.eq_ignore_ascii_case(b"compression") && idx + 1 < args.len() {
          comp = arg_f64(args[idx + 1]);
          idx += 2;
        } else if let Some(val) = arg_f64(args[idx]) {
          comp = Some(val);
          idx += 1;
        } else {
          idx += 1;
        }
      }
      Ok(Cmd::TDigestCreate {
        key,
        compression: comp,
      })
    }
    // @cmd: TDIGEST.INFO key
    "tdigest.info" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::TDigestInfo(arg_string(args[1])))
    }
    // @cmd: TDIGEST.ADD key value [value ...]
    "tdigest.add" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let mut values = Vec::with_capacity(args.len() - 2);
      for arg in &args[2..] {
        if let Some(v) = arg_f64(arg) {
          values.push(v);
        }
      }
      Ok(Cmd::TDigestAdd(key, values))
    }
    // @cmd: TDIGEST.MAX key
    "tdigest.max" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::TDigestMax(arg_string(args[1])))
    }
    // @cmd: TDIGEST.MIN key
    "tdigest.min" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::TDigestMin(arg_string(args[1])))
    }
    // @cmd: TDIGEST.REVRANK key value [value ...]
    "tdigest.revrank" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let mut vals = Vec::with_capacity(args.len() - 2);
      for arg in &args[2..] {
        if let Some(v) = arg_f64(arg) {
          vals.push(v);
        }
      }
      Ok(Cmd::TDigestRevRank(key, vals))
    }
    // @cmd: TDIGEST.RANK key value [value ...]
    "tdigest.rank" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let mut vals = Vec::with_capacity(args.len() - 2);
      for arg in &args[2..] {
        if let Some(v) = arg_f64(arg) {
          vals.push(v);
        }
      }
      Ok(Cmd::TDigestRank(key, vals))
    }
    // @cmd: TDIGEST.BYREVRANK key revrank [revrank ...]
    "tdigest.byrevrank" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let mut ranks = Vec::with_capacity(args.len() - 2);
      for arg in &args[2..] {
        if let Some(r) = arg_u64(arg) {
          ranks.push(r);
        }
      }
      Ok(Cmd::TDigestByRevRank(key, ranks))
    }
    // @cmd: TDIGEST.BYRANK key rank [rank ...]
    "tdigest.byrank" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let mut ranks = Vec::with_capacity(args.len() - 2);
      for arg in &args[2..] {
        if let Some(r) = arg_u64(arg) {
          ranks.push(r);
        }
      }
      Ok(Cmd::TDigestByRank(key, ranks))
    }
    // @cmd: TDIGEST.QUANTILE key quantile [quantile ...]
    "tdigest.quantile" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let mut quantiles = Vec::with_capacity(args.len() - 2);
      for arg in &args[2..] {
        if let Some(q) = arg_f64(arg) {
          quantiles.push(q);
        }
      }
      Ok(Cmd::TDigestQuantile(key, quantiles))
    }
    // @cmd: TDIGEST.TRIMMED_MEAN key low_cut_quantile high_cut_quantile
    "tdigest.trimmed_mean" => {
      check_min_args(cmd_name, args, 4)?;
      let low = arg_f64(args[2]).unwrap_or(0.1);
      let high = arg_f64(args[3]).unwrap_or(0.9);
      Ok(Cmd::TDigestTrimmedMean(arg_string(args[1]), low, high))
    }
    // @cmd: TDIGEST.RESET key
    "tdigest.reset" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::TDigestReset(arg_string(args[1])))
    }
    // @cmd: TDIGEST.MERGE dst numkeys src [src ...] [COMPRESSION c] [OVERRIDE]
    "tdigest.merge" => {
      check_min_args(cmd_name, args, 3)?;
      let dst = arg_string(args[1]);
      let mut sources = Vec::new();
      let mut comp = None;
      let mut override_flag = false;

      let mut idx = 2;
      if let Some(num_keys) = arg_usize(args[2]) {
        idx = 3;
        let end = (idx + num_keys).min(args.len());
        for arg in &args[idx..end] {
          sources.push(arg_string(arg));
        }
        idx = end;
      }

      while idx < args.len() {
        let opt = args[idx];
        if opt.eq_ignore_ascii_case(b"compression") && idx + 1 < args.len() {
          comp = arg_f64(args[idx + 1]);
          idx += 2;
        } else if opt.eq_ignore_ascii_case(b"override") {
          override_flag = true;
          idx += 1;
        } else {
          sources.push(arg_string(opt));
          idx += 1;
        }
      }

      Ok(Cmd::TDigestMerge {
        dst,
        sources,
        compression: comp,
        override_flag,
      })
    }
    // @cmd: TDIGEST.CDF key value [value ...]
    "tdigest.cdf" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let mut vals = Vec::with_capacity(args.len() - 2);
      for arg in &args[2..] {
        if let Some(v) = arg_f64(arg) {
          vals.push(v);
        }
      }
      Ok(Cmd::TDigestCdf(key, vals))
    }

    _ => return Ok(None),
  };
  res.map(Some)
}
