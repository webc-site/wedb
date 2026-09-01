use crate::parse::util::*;
use crate::types::Cmd;
use wedb_embed::Result;

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: LPUSH key element [element ...]
    "lpush" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let elements = args[2..].iter().map(|e| e.to_vec()).collect();
      Ok(Cmd::LPush(key, elements))
    }
    // @cmd: RPUSH key element [element ...]
    "rpush" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let elements = args[2..].iter().map(|e| e.to_vec()).collect();
      Ok(Cmd::RPush(key, elements))
    }
    // @cmd: LPUSHX key element [element ...]
    "lpushx" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let elements = args[2..].iter().map(|e| e.to_vec()).collect();
      Ok(Cmd::LPushX(key, elements))
    }
    // @cmd: RPUSHX key element [element ...]
    "rpushx" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let elements = args[2..].iter().map(|e| e.to_vec()).collect();
      Ok(Cmd::RPushX(key, elements))
    }
    // @cmd: LPOP key [count]
    "lpop" => {
      check_min_args(cmd_name, args, 2)?;
      let count = if args.len() > 2 {
        arg_usize(args[2])
      } else {
        None
      };
      Ok(Cmd::LPop(arg_string(args[1]), count))
    }
    // @cmd: RPOP key [count]
    "rpop" => {
      check_min_args(cmd_name, args, 2)?;
      let count = if args.len() > 2 {
        arg_usize(args[2])
      } else {
        None
      };
      Ok(Cmd::RPop(arg_string(args[1]), count))
    }
    // @cmd: LLEN key
    "llen" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::LLen(arg_string(args[1])))
    }
    // @cmd: LINDEX key index
    "lindex" => {
      check_min_args(cmd_name, args, 3)?;
      let idx = parse_i64_strict(args[2])?;
      Ok(Cmd::LIndex(arg_string(args[1]), idx))
    }
    // @cmd: LSET key index element
    "lset" => {
      check_min_args(cmd_name, args, 4)?;
      let idx = parse_i64_strict(args[2])?;
      Ok(Cmd::LSet(arg_string(args[1]), idx, args[3].to_vec()))
    }
    // @cmd: LRANGE key start stop
    "lrange" => {
      check_min_args(cmd_name, args, 4)?;
      let start = parse_i64_strict(args[2])?;
      let stop = parse_i64_strict(args[3])?;
      Ok(Cmd::LRange(arg_string(args[1]), start, stop))
    }
    // @cmd: LTRIM key start stop
    "ltrim" => {
      check_min_args(cmd_name, args, 4)?;
      let start = parse_i64_strict(args[2])?;
      let stop = parse_i64_strict(args[3])?;
      Ok(Cmd::LTrim(arg_string(args[1]), start, stop))
    }
    // @cmd: LREM key count element
    "lrem" => {
      check_min_args(cmd_name, args, 4)?;
      let count = parse_i64_strict(args[2])?;
      Ok(Cmd::LRem(arg_string(args[1]), count, args[3].to_vec()))
    }
    // @cmd: LINSERT key BEFORE|AFTER pivot element
    "linsert" => {
      check_min_args(cmd_name, args, 5)?;
      let before = if args[2].eq_ignore_ascii_case(b"before") {
        true
      } else if args[2].eq_ignore_ascii_case(b"after") {
        false
      } else {
        return Err(err_syntax());
      };
      Ok(Cmd::LInsert {
        key: arg_string(args[1]),
        before,
        pivot: args[3].to_vec(),
        element: args[4].to_vec(),
      })
    }
    // @cmd: LMOVE source destination LEFT|RIGHT LEFT|RIGHT
    "lmove" => {
      check_min_args(cmd_name, args, 5)?;
      let src = arg_string(args[1]);
      let dst = arg_string(args[2]);
      let src_left = parse_left_right(args[3])?;
      let dst_left = parse_left_right(args[4])?;
      Ok(Cmd::LMove {
        src,
        dst,
        src_left,
        dst_left,
      })
    }
    // @cmd: LMOVEM source destination LEFT|RIGHT LEFT|RIGHT [count [EXACTLY exactly]]
    "lmovem" => {
      check_min_args(cmd_name, args, 5)?;
      let src = arg_string(args[1]);
      let dst = arg_string(args[2]);
      let src_left = parse_left_right(args[3])?;
      let dst_left = parse_left_right(args[4])?;
      let mut count = None;
      let mut exactly = None;
      let mut i = 5;
      while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"exactly") && i + 1 < args.len() {
          exactly = arg_usize(args[i + 1]);
          i += 2;
        } else if count.is_none() {
          count = arg_usize(args[i]);
          i += 1;
        } else {
          i += 1;
        }
      }
      Ok(Cmd::LMoveM {
        src,
        dst,
        src_left,
        dst_left,
        count,
        exactly,
      })
    }
    // @cmd: RPOPLPUSH source destination
    "rpoplpush" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::RPopLPush(arg_string(args[1]), arg_string(args[2])))
    }
    // @cmd: LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]
    "lpos" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let element = args[2].to_vec();
      let mut rank = None;
      let mut count = None;
      let mut max_len = None;

      let mut i = 3;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"rank") && i + 1 < args.len() {
          rank = arg_i64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"count") && i + 1 < args.len() {
          count = arg_usize(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"maxlen") && i + 1 < args.len() {
          max_len = arg_usize(args[i + 1]);
          i += 2;
        } else {
          i += 1;
        }
      }

      Ok(Cmd::LPos {
        key,
        element,
        rank,
        count,
        max_len,
      })
    }
    // @cmd: BLPOP key [key ...] timeout
    "blpop" => {
      check_min_args(cmd_name, args, 3)?;
      let timeout = parse_float_strict(args[args.len() - 1])?;
      let keys = args[1..args.len() - 1]
        .iter()
        .map(|k| arg_string(k))
        .collect();
      Ok(Cmd::BLPop(keys, timeout))
    }
    // @cmd: BRPOP key [key ...] timeout
    "brpop" => {
      check_min_args(cmd_name, args, 3)?;
      let timeout = parse_float_strict(args[args.len() - 1])?;
      let keys = args[1..args.len() - 1]
        .iter()
        .map(|k| arg_string(k))
        .collect();
      Ok(Cmd::BRPop(keys, timeout))
    }
    // @cmd: BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
    "blmove" => {
      check_min_args(cmd_name, args, 6)?;
      let src = arg_string(args[1]);
      let dst = arg_string(args[2]);
      let src_left = parse_left_right(args[3])?;
      let dst_left = parse_left_right(args[4])?;
      let timeout = parse_float_strict(args[5])?;
      Ok(Cmd::BLMove {
        src,
        dst,
        src_left,
        dst_left,
        timeout,
      })
    }
    // @cmd: BLMOVEM source destination LEFT|RIGHT LEFT|RIGHT [count [EXACTLY exactly]] timeout
    "blmovem" => {
      check_min_args(cmd_name, args, 6)?;
      let src = arg_string(args[1]);
      let dst = arg_string(args[2]);
      let src_left = parse_left_right(args[3])?;
      let dst_left = parse_left_right(args[4])?;
      let timeout = parse_float_strict(args[args.len() - 1])?;
      let mut count = None;
      let mut exactly = None;
      let mut i = 5;
      while i < args.len() - 1 {
        if args[i].eq_ignore_ascii_case(b"exactly") && i + 1 < args.len() - 1 {
          exactly = arg_usize(args[i + 1]);
          i += 2;
        } else if count.is_none() {
          count = arg_usize(args[i]);
          i += 1;
        } else {
          i += 1;
        }
      }
      Ok(Cmd::BLMoveM {
        src,
        dst,
        src_left,
        dst_left,
        count,
        exactly,
        timeout,
      })
    }
    // @cmd: LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
    "lmpop" => {
      check_min_args(cmd_name, args, 4)?;
      let numkeys = parse_usize_strict(args[1])?;
      if args.len() < 2 + numkeys + 1 {
        return Err(err_syntax());
      }
      let keys = args[2..2 + numkeys].iter().map(|k| arg_string(k)).collect();
      let left = parse_left_right(args[2 + numkeys])?;
      let mut count = 1;
      if args.len() >= 2 + numkeys + 3 && args[2 + numkeys + 1].eq_ignore_ascii_case(b"count") {
        count = parse_usize_strict(args[2 + numkeys + 2])?;
      }
      Ok(Cmd::LMPop { keys, left, count })
    }
    // @cmd: BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
    "blmpop" => {
      check_min_args(cmd_name, args, 5)?;
      let timeout = parse_float_strict(args[1])?;
      let numkeys = parse_usize_strict(args[2])?;
      if args.len() < 3 + numkeys + 1 {
        return Err(err_syntax());
      }
      let keys = args[3..3 + numkeys].iter().map(|k| arg_string(k)).collect();
      let left = parse_left_right(args[3 + numkeys])?;
      let mut count = 1;
      if args.len() >= 3 + numkeys + 3 && args[3 + numkeys + 1].eq_ignore_ascii_case(b"count") {
        count = parse_usize_strict(args[3 + numkeys + 2])?;
      }
      Ok(Cmd::BLMPop {
        timeout,
        keys,
        left,
        count,
      })
    }

    _ => return Ok(None),
  };
  res.map(Some)
}

#[inline]
fn parse_left_right(b: &[u8]) -> Result<bool> {
  if b.eq_ignore_ascii_case(b"left") {
    Ok(true)
  } else if b.eq_ignore_ascii_case(b"right") {
    Ok(false)
  } else {
    Err(err_syntax())
  }
}
