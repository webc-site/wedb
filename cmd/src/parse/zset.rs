use crate::parse::util::*;
use crate::types::Cmd;
use wedb_embed::{Error, Result};

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: ZADD key [NX | XX] [GT | LT] [CH] [INCR] score member [score member ...]
    "zadd" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let mut nx = false;
      let mut xx = false;
      let mut gt = false;
      let mut lt = false;
      let mut ch = false;
      let mut incr = false;

      let mut idx = 2;
      while idx < args.len() {
        let opt = args[idx];
        if opt.eq_ignore_ascii_case(b"nx") {
          nx = true;
          idx += 1;
        } else if opt.eq_ignore_ascii_case(b"xx") {
          xx = true;
          idx += 1;
        } else if opt.eq_ignore_ascii_case(b"gt") {
          gt = true;
          idx += 1;
        } else if opt.eq_ignore_ascii_case(b"lt") {
          lt = true;
          idx += 1;
        } else if opt.eq_ignore_ascii_case(b"ch") {
          ch = true;
          idx += 1;
        } else if opt.eq_ignore_ascii_case(b"incr") {
          incr = true;
          idx += 1;
        } else {
          break;
        }
      }

      if nx && xx {
        return Err(Error::invalid_data(
          "ERR XX and NX options at the same time are not compatible",
        ));
      }
      if (gt || lt) && nx {
        return Err(Error::invalid_data(
          "ERR GT, LT, and NX options at the same time are not compatible",
        ));
      }
      if gt && lt {
        return Err(Error::invalid_data(
          "ERR GT and LT options at the same time are not compatible",
        ));
      }

      let remaining = &args[idx..];
      if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
        return Err(Error::invalid_data("ERR syntax error in zadd arguments"));
      }
      if incr && remaining.len() != 2 {
        return Err(Error::invalid_data(
          "ERR INCR option supports a single increment-element pair",
        ));
      }

      let mut members = Vec::with_capacity(remaining.len() / 2);
      for chunk in remaining.as_chunks::<2>().0 {
        let score = parse_float_strict(chunk[0])?;
        members.push((score, chunk[1].to_vec()));
      }

      Ok(Cmd::ZAdd {
        key,
        nx,
        xx,
        gt,
        lt,
        ch,
        incr,
        members,
      })
    }
    // @cmd: ZREM key member [member ...]
    "zrem" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let members = args[2..].iter().map(|m| m.to_vec()).collect();
      Ok(Cmd::ZRem(key, members))
    }
    // @cmd: ZSCORE key member
    "zscore" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::ZScore(arg_string(args[1]), args[2].to_vec()))
    }
    // @cmd: ZMSCORE key member [member ...]
    "zmscore" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let members = args[2..].iter().map(|m| m.to_vec()).collect();
      Ok(Cmd::ZMScore(key, members))
    }
    // @cmd: ZINCRBY key increment member
    "zincrby" => {
      check_min_args(cmd_name, args, 4)?;
      let delta = parse_float_strict(args[2])?;
      Ok(Cmd::ZIncrBy(arg_string(args[1]), delta, args[3].to_vec()))
    }
    // @cmd: ZCARD key
    "zcard" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::ZCard(arg_string(args[1])))
    }
    // @cmd: ZCOUNT key min max
    "zcount" => {
      check_min_args(cmd_name, args, 4)?;
      Ok(Cmd::ZCount(
        arg_string(args[1]),
        arg_string(args[2]),
        arg_string(args[3]),
      ))
    }
    // @cmd: ZRANGE key start stop [BYSCORE | BYLEX] [REV] [LIMIT off cnt] [WITHSCORES]
    "zrange" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let min = arg_string(args[2]);
      let max = arg_string(args[3]);
      let mut by_score = false;
      let mut by_lex = false;
      let mut rev = false;
      let mut with_scores = false;
      let mut offset = 0;
      let mut count = None;

      let mut i = 4;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"byscore") {
          by_score = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"bylex") {
          by_lex = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"rev") {
          rev = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"withscores") {
          with_scores = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"limit") && i + 2 < args.len() {
          offset = parse_usize_strict(args[i + 1])?;
          count = Some(parse_usize_strict(args[i + 2])?);
          i += 3;
        } else {
          i += 1;
        }
      }

      Ok(Cmd::ZRange {
        key,
        min,
        max,
        by_score,
        by_lex,
        rev,
        offset,
        count,
        with_scores,
      })
    }
    // @cmd: ZREVRANGE key start stop [WITHSCORES]
    "zrevrange" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let start = parse_i64_strict(args[2])?;
      let stop = parse_i64_strict(args[3])?;
      let with_scores = args.len() >= 5 && args[4].eq_ignore_ascii_case(b"withscores");
      Ok(Cmd::ZRevRange(key, start, stop, with_scores))
    }
    // @cmd: ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
    "zrangebyscore" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let min = arg_string(args[2]);
      let max = arg_string(args[3]);
      let mut with_scores = false;
      let mut offset = 0;
      let mut count = None;

      let mut i = 4;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"withscores") {
          with_scores = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"limit") && i + 2 < args.len() {
          offset = parse_usize_strict(args[i + 1])?;
          count = Some(parse_usize_strict(args[i + 2])?);
          i += 3;
        } else {
          i += 1;
        }
      }

      Ok(Cmd::ZRangeByScore {
        key,
        min,
        max,
        with_scores,
        offset,
        count,
      })
    }
    // @cmd: ZRANK key member [WITHSCORE]
    "zrank" => {
      check_min_args(cmd_name, args, 3)?;
      let with_score = args.len() >= 4 && args[3].eq_ignore_ascii_case(b"withscore");
      Ok(Cmd::ZRank {
        key: arg_string(args[1]),
        member: args[2].to_vec(),
        with_score,
      })
    }
    // @cmd: ZREVRANK key member [WITHSCORE]
    "zrevrank" => {
      check_min_args(cmd_name, args, 3)?;
      let with_score = args.len() >= 4 && args[3].eq_ignore_ascii_case(b"withscore");
      Ok(Cmd::ZRevRank {
        key: arg_string(args[1]),
        member: args[2].to_vec(),
        with_score,
      })
    }
    // @cmd: ZSCAN key cursor [MATCH pattern] [COUNT count]
    "zscan" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let cursor = arg_u64(args[2]).unwrap_or(0);
      let scan_opts = ScanOptions::parse(args, 3);
      Ok(Cmd::ZScan {
        key,
        cursor,
        pattern: scan_opts.pattern,
        count: scan_opts.count,
      })
    }
    // @cmd: ZPOPMIN key [count]
    "zpopmin" => {
      check_min_args(cmd_name, args, 2)?;
      let count = if args.len() > 2 {
        arg_usize(args[2])
      } else {
        None
      };
      Ok(Cmd::ZPopMin(arg_string(args[1]), count))
    }
    // @cmd: ZPOPMAX key [count]
    "zpopmax" => {
      check_min_args(cmd_name, args, 2)?;
      let count = if args.len() > 2 {
        arg_usize(args[2])
      } else {
        None
      };
      Ok(Cmd::ZPopMax(arg_string(args[1]), count))
    }
    // @cmd: BZPOPMIN key [key ...] timeout
    "bzpopmin" => {
      check_min_args(cmd_name, args, 3)?;
      let timeout = parse_float_strict(args[args.len() - 1])?;
      let keys = args[1..args.len() - 1]
        .iter()
        .map(|k| arg_string(k))
        .collect();
      Ok(Cmd::BZPopMin(keys, timeout))
    }
    // @cmd: BZPOPMAX key [key ...] timeout
    "bzpopmax" => {
      check_min_args(cmd_name, args, 3)?;
      let timeout = parse_float_strict(args[args.len() - 1])?;
      let keys = args[1..args.len() - 1]
        .iter()
        .map(|k| arg_string(k))
        .collect();
      Ok(Cmd::BZPopMax(keys, timeout))
    }
    // @cmd: ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
    "zmpop" => {
      check_min_args(cmd_name, args, 4)?;
      let numkeys = parse_usize_strict(args[1])?;
      if args.len() < 2 + numkeys + 1 {
        return Err(err_syntax());
      }
      let keys = args[2..2 + numkeys].iter().map(|k| arg_string(k)).collect();
      let min = parse_min_max(args[2 + numkeys])?;
      let mut count = 1;
      if args.len() >= 2 + numkeys + 3 && args[2 + numkeys + 1].eq_ignore_ascii_case(b"count") {
        count = parse_usize_strict(args[2 + numkeys + 2])?;
      }
      Ok(Cmd::ZMPop { keys, min, count })
    }
    // @cmd: BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
    "bzmpop" => {
      check_min_args(cmd_name, args, 5)?;
      let timeout = parse_float_strict(args[1])?;
      let numkeys = parse_usize_strict(args[2])?;
      if args.len() < 3 + numkeys + 1 {
        return Err(err_syntax());
      }
      let keys = args[3..3 + numkeys].iter().map(|k| arg_string(k)).collect();
      let min = parse_min_max(args[3 + numkeys])?;
      let mut count = 1;
      if args.len() >= 3 + numkeys + 3 && args[3 + numkeys + 1].eq_ignore_ascii_case(b"count") {
        count = parse_usize_strict(args[3 + numkeys + 2])?;
      }
      Ok(Cmd::BZMPop {
        timeout,
        keys,
        min,
        count,
      })
    }
    // @cmd: ZINTERSTORE dst numkeys key [key ...] [WEIGHTS w [w ...]] [AGGREGATE SUM|MIN|MAX]
    "zinterstore" => {
      check_min_args(cmd_name, args, 4)?;
      let dst = arg_string(args[1]);
      let (keys, weights, aggregate, _) = parse_zset_weights_aggregate(args, 2, false)?;
      Ok(Cmd::ZInterStore {
        dst,
        keys,
        weights,
        aggregate,
      })
    }
    // @cmd: ZINTER numkeys key [key ...] [WEIGHTS w [w ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
    "zinter" => {
      check_min_args(cmd_name, args, 3)?;
      let (keys, weights, aggregate, with_scores) = parse_zset_weights_aggregate(args, 1, true)?;
      Ok(Cmd::ZInter {
        keys,
        weights,
        aggregate,
        with_scores,
      })
    }
    // @cmd: ZINTERCARD numkeys key [key ...] [LIMIT limit]
    "zintercard" => {
      check_min_args(cmd_name, args, 3)?;
      let numkeys = parse_usize_strict(args[1])?;
      if args.len() < 2 + numkeys {
        return Err(err_syntax());
      }
      let keys = args[2..2 + numkeys].iter().map(|k| arg_string(k)).collect();
      let mut limit = 0;
      if args.len() >= 2 + numkeys + 2 && args[2 + numkeys].eq_ignore_ascii_case(b"limit") {
        limit = parse_usize_strict(args[2 + numkeys + 1])?;
      }
      Ok(Cmd::ZInterCard { keys, limit })
    }
    // @cmd: ZUNIONSTORE dst numkeys key [key ...] [WEIGHTS w [w ...]] [AGGREGATE SUM|MIN|MAX]
    "zunionstore" => {
      check_min_args(cmd_name, args, 4)?;
      let dst = arg_string(args[1]);
      let (keys, weights, aggregate, _) = parse_zset_weights_aggregate(args, 2, false)?;
      Ok(Cmd::ZUnionStore {
        dst,
        keys,
        weights,
        aggregate,
      })
    }
    // @cmd: ZUNION numkeys key [key ...] [WEIGHTS w [w ...]] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
    "zunion" => {
      check_min_args(cmd_name, args, 3)?;
      let (keys, weights, aggregate, with_scores) = parse_zset_weights_aggregate(args, 1, true)?;
      Ok(Cmd::ZUnion {
        keys,
        weights,
        aggregate,
        with_scores,
      })
    }
    // @cmd: ZDIFF numkeys key [key ...] [WITHSCORES]
    "zdiff" => {
      check_min_args(cmd_name, args, 3)?;
      let numkeys = parse_usize_strict(args[1])?;
      if args.len() < 2 + numkeys {
        return Err(err_syntax());
      }
      let keys = args[2..2 + numkeys].iter().map(|k| arg_string(k)).collect();
      let with_scores =
        args.len() > 2 + numkeys && args[2 + numkeys].eq_ignore_ascii_case(b"withscores");
      Ok(Cmd::ZDiff { keys, with_scores })
    }
    // @cmd: ZDIFFSTORE dst numkeys key [key ...]
    "zdiffstore" => {
      check_min_args(cmd_name, args, 4)?;
      let dst = arg_string(args[1]);
      let numkeys = parse_usize_strict(args[2])?;
      if args.len() < 3 + numkeys {
        return Err(err_syntax());
      }
      let keys = args[3..3 + numkeys].iter().map(|k| arg_string(k)).collect();
      Ok(Cmd::ZDiffStore { dst, keys })
    }
    // @cmd: ZLEXCOUNT key min max
    "zlexcount" => {
      check_min_args(cmd_name, args, 4)?;
      Ok(Cmd::ZLexCount(
        arg_string(args[1]),
        arg_string(args[2]),
        arg_string(args[3]),
      ))
    }
    // @cmd: ZRANGEBYLEX key min max [LIMIT offset count]
    "zrangebylex" => {
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
      Ok(Cmd::ZRangeByLex {
        key,
        min,
        max,
        offset,
        count,
      })
    }
    // @cmd: ZREVRANGEBYLEX key max min [LIMIT offset count]
    "zrevrangebylex" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let max = arg_string(args[2]);
      let min = arg_string(args[3]);
      let mut offset = 0;
      let mut count = None;
      if args.len() >= 7 && args[4].eq_ignore_ascii_case(b"limit") {
        offset = parse_usize_strict(args[5])?;
        count = Some(parse_usize_strict(args[6])?);
      }
      Ok(Cmd::ZRevRangeByLex {
        key,
        max,
        min,
        offset,
        count,
      })
    }
    // @cmd: ZREMRANGEBYRANK key start stop
    "zremrangebyrank" => {
      check_min_args(cmd_name, args, 4)?;
      let start = parse_i64_strict(args[2])?;
      let stop = parse_i64_strict(args[3])?;
      Ok(Cmd::ZRemRangeByRank(arg_string(args[1]), start, stop))
    }
    // @cmd: ZREMRANGEBYSCORE key min max
    "zremrangebyscore" => {
      check_min_args(cmd_name, args, 4)?;
      Ok(Cmd::ZRemRangeByScore(
        arg_string(args[1]),
        arg_string(args[2]),
        arg_string(args[3]),
      ))
    }
    // @cmd: ZREMRANGEBYLEX key min max
    "zremrangebylex" => {
      check_min_args(cmd_name, args, 4)?;
      Ok(Cmd::ZRemRangeByLex(
        arg_string(args[1]),
        arg_string(args[2]),
        arg_string(args[3]),
      ))
    }
    // @cmd: ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
    "zrevrangebyscore" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let max = arg_string(args[2]);
      let min = arg_string(args[3]);
      let mut with_scores = false;
      let mut offset = 0;
      let mut count = None;

      let mut i = 4;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"withscores") {
          with_scores = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"limit") && i + 2 < args.len() {
          offset = parse_usize_strict(args[i + 1])?;
          count = Some(parse_usize_strict(args[i + 2])?);
          i += 3;
        } else {
          i += 1;
        }
      }

      Ok(Cmd::ZRevRangeByScore {
        key,
        max,
        min,
        with_scores,
        offset,
        count,
      })
    }
    // @cmd: ZRANDMEMBER key [count [WITHSCORES]]
    "zrandmember" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let mut count = None;
      let mut with_scores = false;
      if args.len() >= 3 {
        if let Some(c) = arg_i64(args[2]) {
          count = Some(c);
          if args.len() >= 4 && args[3].eq_ignore_ascii_case(b"withscores") {
            with_scores = true;
          }
        } else if args[2].eq_ignore_ascii_case(b"withscores") {
          with_scores = true;
        }
      }
      Ok(Cmd::ZRandMember {
        key,
        count,
        with_scores,
      })
    }
    // @cmd: ZRANGESTORE dst src min max [BYSCORE | BYLEX] [REV] [LIMIT offset count]
    "zrangestore" => {
      check_min_args(cmd_name, args, 5)?;
      let dst = arg_string(args[1]);
      let src = arg_string(args[2]);
      let min = arg_string(args[3]);
      let max = arg_string(args[4]);
      let mut by_score = false;
      let mut by_lex = false;
      let mut rev = false;
      let mut offset = 0;
      let mut count = None;

      let mut i = 5;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"byscore") {
          by_score = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"bylex") {
          by_lex = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"rev") {
          rev = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"limit") && i + 2 < args.len() {
          offset = parse_usize_strict(args[i + 1])?;
          count = Some(parse_usize_strict(args[i + 2])?);
          i += 3;
        } else {
          i += 1;
        }
      }

      Ok(Cmd::ZRangeStore {
        dst,
        src,
        min,
        max,
        by_score,
        by_lex,
        rev,
        offset,
        count,
      })
    }

    _ => return Ok(None),
  };
  res.map(Some)
}

#[inline]
fn parse_min_max(b: &[u8]) -> Result<bool> {
  if b.eq_ignore_ascii_case(b"min") {
    Ok(true)
  } else if b.eq_ignore_ascii_case(b"max") {
    Ok(false)
  } else {
    Err(err_syntax())
  }
}

fn parse_zset_weights_aggregate(
  args: &[&[u8]],
  start_idx: usize,
  allow_withscores: bool,
) -> Result<(Vec<String>, Vec<f64>, String, bool)> {
  let numkeys = parse_usize_strict(args[start_idx])?;
  if args.len() < start_idx + 1 + numkeys {
    return Err(err_syntax());
  }
  let keys = args[start_idx + 1..start_idx + 1 + numkeys]
    .iter()
    .map(|k| arg_string(k))
    .collect();

  let mut weights = Vec::new();
  let mut aggregate = "SUM".to_string();
  let mut with_scores = false;

  let mut i = start_idx + 1 + numkeys;
  while i < args.len() {
    let opt = args[i];
    if opt.eq_ignore_ascii_case(b"weights") {
      i += 1;
      for _ in 0..numkeys {
        if i < args.len() {
          weights.push(parse_float_strict(args[i])?);
          i += 1;
        } else {
          return Err(err_syntax());
        }
      }
    } else if opt.eq_ignore_ascii_case(b"aggregate") && i + 1 < args.len() {
      let agg_str = arg_str(args[i + 1]).to_ascii_uppercase();
      if agg_str == "SUM" || agg_str == "MIN" || agg_str == "MAX" {
        aggregate = agg_str;
        i += 2;
      } else {
        return Err(err_syntax());
      }
    } else if allow_withscores && opt.eq_ignore_ascii_case(b"withscores") {
      with_scores = true;
      i += 1;
    } else {
      i += 1;
    }
  }

  Ok((keys, weights, aggregate, with_scores))
}
