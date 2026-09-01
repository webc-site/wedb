use crate::parse::util::*;
use crate::types::Cmd;
use wedb_embed::Result;

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: TS.CREATE key [RETENTION ret] [ENCODING COMPRESSED|UNCOMPRESSED] [CHUNK_SIZE sz] [DUPLICATE_POLICY p] [LABELS l v ...]
    "ts.create" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let mut retention_ms = 0u64;
      let mut chunk_size = 0u64;
      let mut duplicate_policy = None;
      let mut labels = Vec::new();

      let mut idx = 2;
      while idx < args.len() {
        let opt = args[idx];
        if (opt.eq_ignore_ascii_case(b"retention") || opt.eq_ignore_ascii_case(b"retention_ms"))
          && idx + 1 < args.len()
        {
          retention_ms = arg_u64(args[idx + 1]).unwrap_or(0);
          idx += 2;
        } else if opt.eq_ignore_ascii_case(b"chunk_size") && idx + 1 < args.len() {
          chunk_size = arg_u64(args[idx + 1]).unwrap_or(0);
          idx += 2;
        } else if (opt.eq_ignore_ascii_case(b"duplicate_policy")
          || opt.eq_ignore_ascii_case(b"on_duplicate"))
          && idx + 1 < args.len()
        {
          duplicate_policy = Some(arg_string(args[idx + 1]));
          idx += 2;
        } else if opt.eq_ignore_ascii_case(b"labels") {
          idx += 1;
          while idx + 1 < args.len() {
            labels.push((arg_string(args[idx]), arg_string(args[idx + 1])));
            idx += 2;
          }
          break;
        } else {
          if let Some(v) = arg_u64(opt) {
            retention_ms = v;
          }
          idx += 1;
        }
      }

      Ok(Cmd::TsCreate {
        key,
        retention_ms,
        chunk_size,
        duplicate_policy,
        labels,
      })
    }
    // @cmd: TS.ALTER key [RETENTION ret] [CHUNK_SIZE sz] [DUPLICATE_POLICY p] [LABELS l v ...]
    "ts.alter" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let mut retention_ms = None;
      let mut chunk_size = None;
      let mut duplicate_policy = None;
      let mut labels = Vec::new();

      let mut idx = 2;
      while idx < args.len() {
        let opt = args[idx];
        if (opt.eq_ignore_ascii_case(b"retention") || opt.eq_ignore_ascii_case(b"retention_ms"))
          && idx + 1 < args.len()
        {
          retention_ms = arg_u64(args[idx + 1]);
          idx += 2;
        } else if opt.eq_ignore_ascii_case(b"chunk_size") && idx + 1 < args.len() {
          chunk_size = arg_u64(args[idx + 1]);
          idx += 2;
        } else if (opt.eq_ignore_ascii_case(b"duplicate_policy")
          || opt.eq_ignore_ascii_case(b"on_duplicate"))
          && idx + 1 < args.len()
        {
          duplicate_policy = Some(arg_string(args[idx + 1]));
          idx += 2;
        } else if opt.eq_ignore_ascii_case(b"labels") {
          idx += 1;
          while idx + 1 < args.len() {
            labels.push((arg_string(args[idx]), arg_string(args[idx + 1])));
            idx += 2;
          }
          break;
        } else {
          idx += 1;
        }
      }

      Ok(Cmd::TsAlter {
        key,
        retention_ms,
        chunk_size,
        duplicate_policy,
        labels,
      })
    }
    // @cmd: TS.ADD key <timestamp | *> val [RETENTION ret] [CHUNK_SIZE sz] [ON_DUPLICATE p] [LABELS l v ...]
    "ts.add" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let timestamp = if args[2] == b"*" {
        None
      } else {
        arg_u64(args[2])
      };
      let val = parse_float_strict(args[3])?;

      let mut retention_ms = None;
      let mut chunk_size = None;
      let mut on_duplicate = None;
      let mut labels = Vec::new();

      let mut idx = 4;
      while idx < args.len() {
        let opt = args[idx];
        if opt.eq_ignore_ascii_case(b"retention") && idx + 1 < args.len() {
          retention_ms = arg_u64(args[idx + 1]);
          idx += 2;
        } else if opt.eq_ignore_ascii_case(b"chunk_size") && idx + 1 < args.len() {
          chunk_size = arg_u64(args[idx + 1]);
          idx += 2;
        } else if (opt.eq_ignore_ascii_case(b"on_duplicate")
          || opt.eq_ignore_ascii_case(b"duplicate_policy"))
          && idx + 1 < args.len()
        {
          on_duplicate = Some(arg_string(args[idx + 1]));
          idx += 2;
        } else if opt.eq_ignore_ascii_case(b"labels") {
          idx += 1;
          while idx + 1 < args.len() {
            labels.push((arg_string(args[idx]), arg_string(args[idx + 1])));
            idx += 2;
          }
          break;
        } else {
          idx += 1;
        }
      }

      Ok(Cmd::TsAdd {
        key,
        timestamp,
        value: val,
        retention_ms,
        chunk_size,
        on_duplicate,
        labels,
      })
    }
    // @cmd: TS.MADD key timestamp value [key timestamp value ...]
    "ts.madd" => {
      check_min_args(cmd_name, args, 4)?;
      let mut list = Vec::new();
      for chunk in args[1..].as_chunks::<3>().0 {
        let key = arg_string(chunk[0]);
        let ts = if chunk[1] == b"*" {
          None
        } else {
          arg_u64(chunk[1])
        };
        let val = parse_float_strict(chunk[2])?;
        list.push((key, ts, val));
      }
      Ok(Cmd::TsMAdd(list))
    }
    // @cmd: TS.RANGE key fromTs toTs [LATEST] [FILTER_BY_TS ...] [FILTER_BY_VALUE min max] [COUNT c] [ALIGN a] [AGGREGATION agg dur]
    "ts.range" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let from_ts = if args[2] == b"-" {
        0
      } else {
        arg_u64(args[2]).unwrap_or(0)
      };
      let to_ts = if args[3] == b"+" {
        u64::MAX
      } else {
        arg_u64(args[3]).unwrap_or(u64::MAX)
      };

      let mut filter_by_ts = Vec::new();
      let mut filter_by_value = None;
      let mut count = None;
      let mut aggregation = None;
      let mut align = None;

      let mut idx = 4;
      while idx < args.len() {
        let opt = args[idx];
        if opt.eq_ignore_ascii_case(b"count") && idx + 1 < args.len() {
          count = arg_usize(args[idx + 1]);
          idx += 2;
        } else if (opt.eq_ignore_ascii_case(b"aggregation")
          || opt.eq_ignore_ascii_case(b"aggregate"))
          && idx + 2 < args.len()
        {
          let agg_name = arg_string(args[idx + 1]);
          let bucket = arg_u64(args[idx + 2]).unwrap_or(1);
          aggregation = Some((agg_name, bucket));
          idx += 3;
        } else if opt.eq_ignore_ascii_case(b"align") && idx + 1 < args.len() {
          align = arg_u64(args[idx + 1]);
          idx += 2;
        } else if opt.eq_ignore_ascii_case(b"filter_by_value") && idx + 2 < args.len() {
          let min_v = arg_f64(args[idx + 1]).unwrap_or(f64::NEG_INFINITY);
          let max_v = arg_f64(args[idx + 2]).unwrap_or(f64::INFINITY);
          filter_by_value = Some((min_v, max_v));
          idx += 3;
        } else if opt.eq_ignore_ascii_case(b"filter_by_ts") {
          idx += 1;
          while idx < args.len() {
            if let Some(ts) = arg_u64(args[idx]) {
              filter_by_ts.push(ts);
              idx += 1;
            } else {
              break;
            }
          }
        } else {
          idx += 1;
        }
      }

      Ok(Cmd::TsRange {
        key,
        from_ts,
        to_ts,
        filter_by_ts,
        filter_by_value,
        count,
        aggregation,
        align,
      })
    }
    // @cmd: TS.REVRANGE key fromTs toTs [LATEST] [COUNT c] [AGGREGATION agg dur]
    "ts.revrange" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let from_ts = if args[2] == b"-" {
        0
      } else {
        arg_u64(args[2]).unwrap_or(0)
      };
      let to_ts = if args[3] == b"+" {
        u64::MAX
      } else {
        arg_u64(args[3]).unwrap_or(u64::MAX)
      };

      let mut filter_by_ts = Vec::new();
      let mut filter_by_value = None;
      let mut count = None;
      let mut aggregation = None;
      let mut align = None;

      let mut idx = 4;
      while idx < args.len() {
        let opt = args[idx];
        if opt.eq_ignore_ascii_case(b"count") && idx + 1 < args.len() {
          count = arg_usize(args[idx + 1]);
          idx += 2;
        } else if (opt.eq_ignore_ascii_case(b"aggregation")
          || opt.eq_ignore_ascii_case(b"aggregate"))
          && idx + 2 < args.len()
        {
          let agg_name = arg_string(args[idx + 1]);
          let bucket = arg_u64(args[idx + 2]).unwrap_or(1);
          aggregation = Some((agg_name, bucket));
          idx += 3;
        } else if opt.eq_ignore_ascii_case(b"align") && idx + 1 < args.len() {
          align = arg_u64(args[idx + 1]);
          idx += 2;
        } else if opt.eq_ignore_ascii_case(b"filter_by_value") && idx + 2 < args.len() {
          let min_v = arg_f64(args[idx + 1]).unwrap_or(f64::NEG_INFINITY);
          let max_v = arg_f64(args[idx + 2]).unwrap_or(f64::INFINITY);
          filter_by_value = Some((min_v, max_v));
          idx += 3;
        } else if opt.eq_ignore_ascii_case(b"filter_by_ts") {
          idx += 1;
          while idx < args.len() {
            if let Some(ts) = arg_u64(args[idx]) {
              filter_by_ts.push(ts);
              idx += 1;
            } else {
              break;
            }
          }
        } else {
          idx += 1;
        }
      }

      Ok(Cmd::TsRevRange {
        key,
        from_ts,
        to_ts,
        filter_by_ts,
        filter_by_value,
        count,
        aggregation,
        align,
      })
    }
    // @cmd: TS.INFO key [DEBUG]
    "ts.info" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::TsInfo(arg_string(args[1])))
    }
    // @cmd: TS.GET key [LATEST]
    "ts.get" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let latest = args.len() > 2 && args[2].eq_ignore_ascii_case(b"latest");
      Ok(Cmd::TsGet { key, latest })
    }
    // @cmd: TS.CREATERULE sourceKey destKey AGGREGATION agg dur [alignTs]
    "ts.createrule" => {
      check_min_args(cmd_name, args, 5)?;
      let src = arg_string(args[1]);
      let dst = arg_string(args[2]);
      let raw_agg = arg_string(args[3]);

      let (agg, bucket) = if raw_agg.eq_ignore_ascii_case("aggregation") && args.len() > 5 {
        let a = arg_string(args[4]);
        let b = arg_u64(args[5]).unwrap_or(1000);
        (a, b)
      } else {
        let b = arg_u64(args[4]).unwrap_or(1000);
        (raw_agg, b)
      };
      Ok(Cmd::TsCreateRule(src, dst, agg, bucket))
    }
    // @cmd: TS.MGET [LATEST] [WITHLABELS | SELECTED_LABELS l ...] FILTER expr ...
    "ts.mget" => {
      let mut with_labels = false;
      let mut selected_labels = Vec::new();
      let mut filters = Vec::new();

      let mut idx = 1;
      while idx < args.len() {
        let opt = args[idx];
        if opt.eq_ignore_ascii_case(b"withlabels") {
          with_labels = true;
          idx += 1;
        } else if opt.eq_ignore_ascii_case(b"selected_labels") {
          idx += 1;
          while idx < args.len() {
            let s = args[idx];
            if s.eq_ignore_ascii_case(b"filter") {
              break;
            }
            selected_labels.push(arg_string(s));
            idx += 1;
          }
        } else if opt.eq_ignore_ascii_case(b"filter") {
          idx += 1;
          while idx < args.len() {
            filters.push(arg_string(args[idx]));
            idx += 1;
          }
        } else {
          filters.push(arg_string(opt));
          idx += 1;
        }
      }

      Ok(Cmd::TsMGet {
        with_labels,
        selected_labels,
        filters,
      })
    }
    // @cmd: TS.MRANGE fromTs toTs [LATEST] [WITHLABELS] [AGGREGATION agg dur] FILTER expr ...
    "ts.mrange" => {
      let from_ts = if args.len() > 1 && args[1] == b"-" {
        0
      } else if args.len() > 1 {
        arg_u64(args[1]).unwrap_or(0)
      } else {
        0
      };
      let to_ts = if args.len() > 2 && args[2] == b"+" {
        u64::MAX
      } else if args.len() > 2 {
        arg_u64(args[2]).unwrap_or(u64::MAX)
      } else {
        u64::MAX
      };

      let mut filter_by_ts = Vec::new();
      let mut filter_by_value = None;
      let mut count = None;
      let mut aggregation = None;
      let mut align = None;
      let mut with_labels = false;
      let mut selected_labels = Vec::new();
      let mut filters = Vec::new();

      let mut idx = 3;
      while idx < args.len() {
        let opt = args[idx];
        if opt.eq_ignore_ascii_case(b"count") && idx + 1 < args.len() {
          count = arg_usize(args[idx + 1]);
          idx += 2;
        } else if (opt.eq_ignore_ascii_case(b"aggregation")
          || opt.eq_ignore_ascii_case(b"aggregate"))
          && idx + 2 < args.len()
        {
          let agg_name = arg_string(args[idx + 1]);
          let bucket = arg_u64(args[idx + 2]).unwrap_or(1);
          aggregation = Some((agg_name, bucket));
          idx += 3;
        } else if opt.eq_ignore_ascii_case(b"align") && idx + 1 < args.len() {
          align = arg_u64(args[idx + 1]);
          idx += 2;
        } else if opt.eq_ignore_ascii_case(b"filter_by_value") && idx + 2 < args.len() {
          let min_v = arg_f64(args[idx + 1]).unwrap_or(f64::NEG_INFINITY);
          let max_v = arg_f64(args[idx + 2]).unwrap_or(f64::INFINITY);
          filter_by_value = Some((min_v, max_v));
          idx += 3;
        } else if opt.eq_ignore_ascii_case(b"filter_by_ts") {
          idx += 1;
          while idx < args.len() {
            if let Some(ts) = arg_u64(args[idx]) {
              filter_by_ts.push(ts);
              idx += 1;
            } else {
              break;
            }
          }
        } else if opt.eq_ignore_ascii_case(b"withlabels") {
          with_labels = true;
          idx += 1;
        } else if opt.eq_ignore_ascii_case(b"selected_labels") {
          idx += 1;
          while idx < args.len() {
            let s = args[idx];
            if s.eq_ignore_ascii_case(b"filter") {
              break;
            }
            selected_labels.push(arg_string(s));
            idx += 1;
          }
        } else if opt.eq_ignore_ascii_case(b"filter") {
          idx += 1;
          while idx < args.len() {
            filters.push(arg_string(args[idx]));
            idx += 1;
          }
        } else {
          filters.push(arg_string(opt));
          idx += 1;
        }
      }

      Ok(Cmd::TsMRange {
        from_ts,
        to_ts,
        filter_by_ts,
        filter_by_value,
        count,
        aggregation,
        align,
        with_labels,
        selected_labels,
        filters,
      })
    }
    // @cmd: TS.MREVRANGE fromTs toTs [LATEST] [WITHLABELS] [AGGREGATION agg dur] FILTER expr ...
    "ts.mrevrange" => {
      let from_ts = if args.len() > 1 && args[1] == b"-" {
        0
      } else if args.len() > 1 {
        arg_u64(args[1]).unwrap_or(0)
      } else {
        0
      };
      let to_ts = if args.len() > 2 && args[2] == b"+" {
        u64::MAX
      } else if args.len() > 2 {
        arg_u64(args[2]).unwrap_or(u64::MAX)
      } else {
        u64::MAX
      };

      let mut filter_by_ts = Vec::new();
      let mut filter_by_value = None;
      let mut count = None;
      let mut aggregation = None;
      let mut align = None;
      let mut with_labels = false;
      let mut selected_labels = Vec::new();
      let mut filters = Vec::new();

      let mut idx = 3;
      while idx < args.len() {
        let opt = args[idx];
        if opt.eq_ignore_ascii_case(b"count") && idx + 1 < args.len() {
          count = arg_usize(args[idx + 1]);
          idx += 2;
        } else if (opt.eq_ignore_ascii_case(b"aggregation")
          || opt.eq_ignore_ascii_case(b"aggregate"))
          && idx + 2 < args.len()
        {
          let agg_name = arg_string(args[idx + 1]);
          let bucket = arg_u64(args[idx + 2]).unwrap_or(1);
          aggregation = Some((agg_name, bucket));
          idx += 3;
        } else if opt.eq_ignore_ascii_case(b"align") && idx + 1 < args.len() {
          align = arg_u64(args[idx + 1]);
          idx += 2;
        } else if opt.eq_ignore_ascii_case(b"filter_by_value") && idx + 2 < args.len() {
          let min_v = arg_f64(args[idx + 1]).unwrap_or(f64::NEG_INFINITY);
          let max_v = arg_f64(args[idx + 2]).unwrap_or(f64::INFINITY);
          filter_by_value = Some((min_v, max_v));
          idx += 3;
        } else if opt.eq_ignore_ascii_case(b"filter_by_ts") {
          idx += 1;
          while idx < args.len() {
            if let Some(ts) = arg_u64(args[idx]) {
              filter_by_ts.push(ts);
              idx += 1;
            } else {
              break;
            }
          }
        } else if opt.eq_ignore_ascii_case(b"withlabels") {
          with_labels = true;
          idx += 1;
        } else if opt.eq_ignore_ascii_case(b"selected_labels") {
          idx += 1;
          while idx < args.len() {
            let s = args[idx];
            if s.eq_ignore_ascii_case(b"filter") {
              break;
            }
            selected_labels.push(arg_string(s));
            idx += 1;
          }
        } else if opt.eq_ignore_ascii_case(b"filter") {
          idx += 1;
          while idx < args.len() {
            filters.push(arg_string(args[idx]));
            idx += 1;
          }
        } else {
          filters.push(arg_string(opt));
          idx += 1;
        }
      }

      Ok(Cmd::TsMRevRange {
        from_ts,
        to_ts,
        filter_by_ts,
        filter_by_value,
        count,
        aggregation,
        align,
        with_labels,
        selected_labels,
        filters,
      })
    }
    // @cmd: TS.INCRBY key value [TIMESTAMP ts] [RETENTION ret] [CHUNK_SIZE sz] [LABELS l v ...]
    "ts.incrby" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let val = parse_float_strict(args[2])?;

      let mut timestamp = None;
      let mut retention_ms = None;
      let mut chunk_size = None;
      let mut labels = Vec::new();

      let mut idx = 3;
      while idx < args.len() {
        let opt = args[idx];
        if opt.eq_ignore_ascii_case(b"timestamp") && idx + 1 < args.len() {
          if args[idx + 1] != b"*" {
            timestamp = arg_u64(args[idx + 1]);
          }
          idx += 2;
        } else if opt.eq_ignore_ascii_case(b"retention") && idx + 1 < args.len() {
          retention_ms = arg_u64(args[idx + 1]);
          idx += 2;
        } else if opt.eq_ignore_ascii_case(b"chunk_size") && idx + 1 < args.len() {
          chunk_size = arg_u64(args[idx + 1]);
          idx += 2;
        } else if opt.eq_ignore_ascii_case(b"labels") {
          idx += 1;
          while idx + 1 < args.len() {
            labels.push((arg_string(args[idx]), arg_string(args[idx + 1])));
            idx += 2;
          }
          break;
        } else {
          if let Some(ts) = arg_u64(opt) {
            timestamp = Some(ts);
          }
          idx += 1;
        }
      }

      Ok(Cmd::TsIncrBy {
        key,
        value: val,
        timestamp,
        retention_ms,
        chunk_size,
        labels,
      })
    }
    // @cmd: TS.DECRBY key value [TIMESTAMP ts] [RETENTION ret] [CHUNK_SIZE sz] [LABELS l v ...]
    "ts.decrby" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let val = parse_float_strict(args[2])?;

      let mut timestamp = None;
      let mut retention_ms = None;
      let mut chunk_size = None;
      let mut labels = Vec::new();

      let mut idx = 3;
      while idx < args.len() {
        let opt = args[idx];
        if opt.eq_ignore_ascii_case(b"timestamp") && idx + 1 < args.len() {
          if args[idx + 1] != b"*" {
            timestamp = arg_u64(args[idx + 1]);
          }
          idx += 2;
        } else if opt.eq_ignore_ascii_case(b"retention") && idx + 1 < args.len() {
          retention_ms = arg_u64(args[idx + 1]);
          idx += 2;
        } else if opt.eq_ignore_ascii_case(b"chunk_size") && idx + 1 < args.len() {
          chunk_size = arg_u64(args[idx + 1]);
          idx += 2;
        } else if opt.eq_ignore_ascii_case(b"labels") {
          idx += 1;
          while idx + 1 < args.len() {
            labels.push((arg_string(args[idx]), arg_string(args[idx + 1])));
            idx += 2;
          }
          break;
        } else {
          if let Some(ts) = arg_u64(opt) {
            timestamp = Some(ts);
          }
          idx += 1;
        }
      }

      Ok(Cmd::TsDecrBy {
        key,
        value: val,
        timestamp,
        retention_ms,
        chunk_size,
        labels,
      })
    }
    // @cmd: TS.DEL key fromTimestamp toTimestamp
    "ts.del" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let from_ts = if args[2] == b"-" {
        0
      } else {
        arg_u64(args[2]).unwrap_or(0)
      };
      let to_ts = if args[3] == b"+" {
        u64::MAX
      } else {
        arg_u64(args[3]).unwrap_or(u64::MAX)
      };
      Ok(Cmd::TsDel(key, from_ts, to_ts))
    }
    // @cmd: TS.QUERYINDEX filter_expression ...
    "ts.queryindex" => {
      let list = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::TsQueryIndex(list))
    }

    _ => return Ok(None),
  };
  res.map(Some)
}
