use crate::parse::util::*;
use crate::types::{Cmd, ExpireCondition};
use wedb_embed::Result;

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: HSET key field value [field value ...]
    "hset" => {
      if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
        return Err(err_wrong_args(cmd_name));
      }
      let key = arg_string(args[1]);
      let mut pairs = Vec::with_capacity((args.len() - 2) / 2);
      for chunk in args[2..].as_chunks::<2>().0 {
        pairs.push((arg_string(chunk[0]), chunk[1].to_vec()));
      }
      Ok(Cmd::HSet(key, pairs))
    }
    // @cmd: HGET key field
    "hget" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::HGet(arg_string(args[1]), arg_string(args[2])))
    }
    // @cmd: HGETDEL key FIELDS numfields field [field ...]
    "hgetdel" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let fields = if args[2].eq_ignore_ascii_case(b"fields") {
        check_min_args(cmd_name, args, 4)?;
        let numfields = arg_usize(args[3]).unwrap_or(1);
        args[4..(4 + numfields).min(args.len())]
          .iter()
          .map(|f| arg_string(f))
          .collect()
      } else {
        args[2..].iter().map(|f| arg_string(f)).collect()
      };
      Ok(Cmd::HGetDel { key, fields })
    }
    // @cmd: HDEL key field [field ...]
    "hdel" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let fields = args[2..].iter().map(|f| arg_string(f)).collect();
      Ok(Cmd::HDel(key, fields))
    }
    // @cmd: HEXISTS key field
    "hexists" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::HExists(arg_string(args[1]), arg_string(args[2])))
    }
    // @cmd: HLEN key
    "hlen" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::HLen(arg_string(args[1])))
    }
    // @cmd: HGETALL key
    "hgetall" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::HGetAll(arg_string(args[1])))
    }
    // @cmd: HKEYS key
    "hkeys" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::HKeys(arg_string(args[1])))
    }
    // @cmd: HVALS key
    "hvals" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::HVals(arg_string(args[1])))
    }
    // @cmd: HMGET key field [field ...]
    "hmget" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let fields = args[2..].iter().map(|f| arg_string(f)).collect();
      Ok(Cmd::HMGet(key, fields))
    }
    // @cmd: HMSET key field value [field value ...]
    "hmset" => {
      if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
        return Err(err_wrong_args(cmd_name));
      }
      let key = arg_string(args[1]);
      let mut pairs = Vec::with_capacity((args.len() - 2) / 2);
      for chunk in args[2..].as_chunks::<2>().0 {
        pairs.push((arg_string(chunk[0]), chunk[1].to_vec()));
      }
      Ok(Cmd::HMSet(key, pairs))
    }
    // @cmd: HINCRBY key field increment
    "hincrby" => {
      check_min_args(cmd_name, args, 4)?;
      let delta = parse_i64_strict(args[3])?;
      Ok(Cmd::HIncrBy(
        arg_string(args[1]),
        arg_string(args[2]),
        delta,
      ))
    }
    // @cmd: HINCRBYFLOAT key field increment
    "hincrbyfloat" => {
      check_min_args(cmd_name, args, 4)?;
      let delta = parse_float_strict(args[3])?;
      Ok(Cmd::HIncrByFloat(
        arg_string(args[1]),
        arg_string(args[2]),
        delta,
      ))
    }
    // @cmd: HSETNX key field value
    "hsetnx" => {
      check_min_args(cmd_name, args, 4)?;
      Ok(Cmd::HSetNx(
        arg_string(args[1]),
        arg_string(args[2]),
        args[3].to_vec(),
      ))
    }
    // @cmd: HSTRLEN key field
    "hstrlen" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::HStrLen(arg_string(args[1]), arg_string(args[2])))
    }
    // @cmd: HSCAN key cursor [MATCH pattern] [COUNT count]
    "hscan" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let cursor = arg_u64(args[2]).unwrap_or(0);
      let scan_opts = ScanOptions::parse(args, 3);
      Ok(Cmd::HScan {
        key,
        cursor,
        pattern: scan_opts.pattern,
        count: scan_opts.count,
      })
    }
    // @cmd: HRANDFIELD key [count [WITHVALUES]]
    "hrandfield" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let mut count = None;
      let mut with_values = false;
      if args.len() >= 3 {
        if let Some(c) = arg_i64(args[2]) {
          count = Some(c);
          if args.len() >= 4 && args[3].eq_ignore_ascii_case(b"withvalues") {
            with_values = true;
          }
        } else if args[2].eq_ignore_ascii_case(b"withvalues") {
          with_values = true;
        }
      }
      Ok(Cmd::HRandField {
        key,
        count,
        with_values,
      })
    }
    // @cmd: HEXPIRE key seconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
    "hexpire" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let seconds = parse_i64_strict(args[2])?;
      let (condition, fields) = parse_hash_expire_fields(args, 3)?;
      Ok(Cmd::HExpire {
        key,
        seconds,
        condition,
        fields,
      })
    }
    // @cmd: HPEXPIRE key milliseconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
    "hpexpire" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let millis = parse_i64_strict(args[2])?;
      let (condition, fields) = parse_hash_expire_fields(args, 3)?;
      Ok(Cmd::HPExpire {
        key,
        millis,
        condition,
        fields,
      })
    }
    // @cmd: HEXPIREAT key unix-time-seconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
    "hexpireat" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let unix_time_sec = parse_i64_strict(args[2])?;
      let (condition, fields) = parse_hash_expire_fields(args, 3)?;
      Ok(Cmd::HExpireAt {
        key,
        unix_time_sec,
        condition,
        fields,
      })
    }
    // @cmd: HPEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
    "hpexpireat" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let unix_time_ms = parse_i64_strict(args[2])?;
      let (condition, fields) = parse_hash_expire_fields(args, 3)?;
      Ok(Cmd::HPExpireAt {
        key,
        unix_time_ms,
        condition,
        fields,
      })
    }
    // @cmd: HTTL key FIELDS numfields field [field ...]
    "httl" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let fields = parse_hash_query_fields(args, 2);
      Ok(Cmd::HTtl { key, fields })
    }
    // @cmd: HPTTL key FIELDS numfields field [field ...]
    "hpttl" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let fields = parse_hash_query_fields(args, 2);
      Ok(Cmd::HPTtl { key, fields })
    }
    // @cmd: HEXPIRETIME key FIELDS numfields field [field ...]
    "hexpiretime" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let fields = parse_hash_query_fields(args, 2);
      Ok(Cmd::HExpireTime { key, fields })
    }
    // @cmd: HPEXPIRETIME key FIELDS numfields field [field ...]
    "hpexpiretime" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let fields = parse_hash_query_fields(args, 2);
      Ok(Cmd::HPExpireTime { key, fields })
    }
    // @cmd: HPERSIST key FIELDS numfields field [field ...]
    "hpersist" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let fields = parse_hash_query_fields(args, 2);
      Ok(Cmd::HPersist { key, fields })
    }
    // @cmd: HSETEX key ttl field [field ...] / HSETEXPIRE key ttl field [field ...]
    "hsetex" | "hsetexpire" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let ttl_sec = parse_u64_strict(args[2])?;
      let fields = args[3..].iter().map(|f| arg_string(f)).collect();
      Ok(Cmd::HSetExpire {
        key,
        ttl_sec,
        fields,
      })
    }
    // @cmd: HGETEX key field
    "hgetex" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::HGetEx {
        key: arg_string(args[1]),
        field: arg_string(args[2]),
      })
    }
    // @cmd: HRANGEBYLEX key min max [LIMIT offset count]
    "hrangebylex" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let min = arg_string(args[2]);
      let max = arg_string(args[3]);
      let mut offset = 0;
      let mut count = None;
      if args.len() >= 7 && args[4].eq_ignore_ascii_case(b"limit") {
        offset = parse_usize_strict(args[5])?;
        count = Some(parse_usize_strict(args[6])?);
      }
      Ok(Cmd::HRangeByLex {
        key,
        min,
        max,
        offset,
        count,
      })
    }

    _ => return Ok(None),
  };
  res.map(Some)
}

fn parse_hash_expire_fields(
  args: &[&[u8]],
  start_idx: usize,
) -> Result<(ExpireCondition, Vec<String>)> {
  let mut condition = ExpireCondition::None;
  let mut idx = start_idx;

  while idx < args.len() {
    let opt = args[idx];
    if let Some(cond) = parse_expire_condition(opt) {
      condition = cond;
      idx += 1;
    } else if opt.eq_ignore_ascii_case(b"fields") {
      idx += 1;
      break;
    } else {
      break;
    }
  }

  let fields = if idx < args.len() {
    if let Some(numfields) = arg_usize(args[idx]) {
      let start = idx + 1;
      let end = (start + numfields).min(args.len());
      args[start..end].iter().map(|f| arg_string(f)).collect()
    } else {
      args[idx..].iter().map(|f| arg_string(f)).collect()
    }
  } else {
    Vec::new()
  };

  Ok((condition, fields))
}

fn parse_hash_query_fields(args: &[&[u8]], start_idx: usize) -> Vec<String> {
  let mut idx = start_idx;
  if idx < args.len() && args[idx].eq_ignore_ascii_case(b"fields") {
    idx += 1;
    if idx < args.len()
      && let Some(numfields) = arg_usize(args[idx])
    {
      let start = idx + 1;
      let end = (start + numfields).min(args.len());
      return args[start..end].iter().map(|f| arg_string(f)).collect();
    }
  }
  args[idx..].iter().map(|f| arg_string(f)).collect()
}
