use crate::parse::util::*;
use crate::types::Cmd;
use wedb_embed::Result;

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: XADD key [NOMKSTREAM] [MAXLEN|MINID [= | ~] thr [LIMIT cnt]] <* | id> field val [field val ...]
    "xadd" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let mut i = 2;
      let mut nomkstream = false;
      let mut max_len = None;
      let mut min_id = None;
      let mut limit = None;
      let mut approximate = false;

      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"nomkstream") {
          nomkstream = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"maxlen") {
          i += 1;
          if i < args.len() {
            let arg_s = args[i];
            if arg_s == b"=" || arg_s == b"~" {
              if arg_s == b"~" {
                approximate = true;
              }
              i += 1;
            }
          }
          if i < args.len() {
            max_len = arg_usize(args[i]);
            i += 1;
          }
          if i < args.len() && args[i].eq_ignore_ascii_case(b"limit") {
            i += 1;
            if i < args.len() {
              limit = arg_usize(args[i]);
              i += 1;
            }
          }
        } else if opt.eq_ignore_ascii_case(b"minid") {
          i += 1;
          if i < args.len() {
            let arg_s = args[i];
            if arg_s == b"=" || arg_s == b"~" {
              if arg_s == b"~" {
                approximate = true;
              }
              i += 1;
            }
          }
          if i < args.len() {
            min_id = Some(arg_string(args[i]));
            i += 1;
          }
          if i < args.len() && args[i].eq_ignore_ascii_case(b"limit") {
            i += 1;
            if i < args.len() {
              limit = arg_usize(args[i]);
              i += 1;
            }
          }
        } else {
          break;
        }
      }

      if i >= args.len() {
        return Err(err_wrong_args(cmd_name));
      }

      let id = arg_string(args[i]);
      i += 1;

      let remaining = &args[i..];
      if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
        return Err(err_wrong_args(cmd_name));
      }

      let mut fields = Vec::with_capacity(remaining.len() / 2);
      for chunk in remaining.as_chunks::<2>().0 {
        fields.push((arg_string(chunk[0]), chunk[1].to_vec()));
      }

      Ok(Cmd::XAdd {
        key,
        id,
        fields,
        nomkstream,
        max_len,
        min_id,
        limit,
        approximate,
      })
    }
    // @cmd: XLEN key
    "xlen" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::XLen(arg_string(args[1])))
    }
    // @cmd: XRANGE key start end [COUNT count]
    "xrange" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let start = arg_string(args[2]);
      let end = arg_string(args[3]);
      let mut count = None;
      if args.len() >= 6 && args[4].eq_ignore_ascii_case(b"count") {
        count = arg_usize(args[5]);
      }
      Ok(Cmd::XRange {
        key,
        start,
        end,
        count,
      })
    }
    // @cmd: XREVRANGE key end start [COUNT count]
    "xrevrange" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let end = arg_string(args[2]);
      let start = arg_string(args[3]);
      let mut count = None;
      if args.len() >= 6 && args[4].eq_ignore_ascii_case(b"count") {
        count = arg_usize(args[5]);
      }
      Ok(Cmd::XRevRange {
        key,
        end,
        start,
        count,
      })
    }
    // @cmd: XDEL key id [id ...]
    "xdel" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let ids = args[2..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::XDel(key, ids))
    }
    // @cmd: XTRIM key <MAXLEN | MINID> [= | ~] threshold [LIMIT count]
    "xtrim" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let mut i = 2;
      let mut max_len = None;
      let mut min_id = None;
      let mut limit = None;
      let mut approximate = false;

      let opt = args[i];
      if opt.eq_ignore_ascii_case(b"maxlen") {
        i += 1;
        if i < args.len() {
          let arg_s = args[i];
          if arg_s == b"=" || arg_s == b"~" {
            if arg_s == b"~" {
              approximate = true;
            }
            i += 1;
          }
        }
        if i < args.len() {
          max_len = arg_usize(args[i]);
          i += 1;
        }
        if i < args.len() && args[i].eq_ignore_ascii_case(b"limit") {
          i += 1;
          if i < args.len() {
            limit = arg_usize(args[i]);
          }
        }
      } else if opt.eq_ignore_ascii_case(b"minid") {
        i += 1;
        if i < args.len() {
          let arg_s = args[i];
          if arg_s == b"=" || arg_s == b"~" {
            if arg_s == b"~" {
              approximate = true;
            }
            i += 1;
          }
        }
        if i < args.len() {
          min_id = Some(arg_string(args[i]));
          i += 1;
        }
        if i < args.len() && args[i].eq_ignore_ascii_case(b"limit") {
          i += 1;
          if i < args.len() {
            limit = arg_usize(args[i]);
          }
        }
      }

      Ok(Cmd::XTrim {
        key,
        max_len,
        min_id,
        limit,
        approximate,
      })
    }
    // @cmd: XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...]
    "xread" => {
      check_min_args(cmd_name, args, 4)?;
      let mut count = None;
      let mut block = None;
      let mut i = 1;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"count") && i + 1 < args.len() {
          count = arg_usize(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"block") && i + 1 < args.len() {
          block = arg_u64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"streams") {
          i += 1;
          break;
        } else {
          i += 1;
        }
      }

      let remaining = &args[i..];
      let num_streams = remaining.len() / 2;
      if num_streams == 0 {
        return Err(err_wrong_args(cmd_name));
      }
      let streams = remaining[..num_streams]
        .iter()
        .map(|a| arg_string(a))
        .collect();
      let ids = remaining[num_streams..]
        .iter()
        .map(|a| arg_string(a))
        .collect();

      Ok(Cmd::XRead {
        streams,
        ids,
        count,
        block,
      })
    }
    "xinfo" => {
      check_min_args(cmd_name, args, 3)?;
      let subcmd = arg_string(args[1]);
      let key = arg_string(args[2]);
      if subcmd.eq_ignore_ascii_case("stream") && args.len() >= 4 {
        let full = args.iter().any(|a| a.eq_ignore_ascii_case(b"full"));
        let mut count = None;
        for (idx, a) in args.iter().enumerate() {
          if a.eq_ignore_ascii_case(b"count") && idx + 1 < args.len() {
            count = arg_usize(args[idx + 1]);
          }
        }
        Ok(Cmd::XInfoStream { key, full, count })
      } else {
        Ok(Cmd::XInfo(subcmd, key))
      }
    }
    // @cmd: XACK key group id [id ...]
    "xack" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let group = arg_string(args[2]);
      let ids = args[3..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::XAck(key, group, ids))
    }
    // @cmd: XACKDEL key group id [id ...]
    "xackdel" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let group = arg_string(args[2]);
      let ids = args[3..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::XAckDel { key, group, ids })
    }
    // @cmd: XNACK key group id [id ...]
    "xnack" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let group = arg_string(args[2]);
      let ids = args[3..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::XNack { key, group, ids })
    }
    // @cmd: XDELEX key [MAXLEN maxlen] id [id ...]
    "xdelex" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let ids = args[2..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::XDelEx { key, ids })
    }
    // @cmd: XCLAIM key group consumer min-idle id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT c] [FORCE] [JUSTID]
    "xclaim" => {
      check_min_args(cmd_name, args, 6)?;
      let key = arg_string(args[1]);
      let group = arg_string(args[2]);
      let consumer = arg_string(args[3]);
      let min_idle = arg_u64(args[4]).unwrap_or(0);

      let mut ids = Vec::new();
      let mut idle = None;
      let mut time = None;
      let mut retrycount = None;
      let mut force = false;
      let mut justid = false;

      let mut i = 5;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"idle") && i + 1 < args.len() {
          idle = arg_u64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"time") && i + 1 < args.len() {
          time = arg_u64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"retrycount") && i + 1 < args.len() {
          retrycount = arg_u64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"force") {
          force = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"justid") {
          justid = true;
          i += 1;
        } else {
          ids.push(arg_string(opt));
          i += 1;
        }
      }

      Ok(Cmd::XClaim {
        key,
        group,
        consumer,
        min_idle,
        ids,
        idle,
        time,
        retrycount,
        force,
        justid,
      })
    }
    // @cmd: XAUTOCLAIM key group consumer min-idle start [COUNT count] [JUSTID]
    "xautoclaim" => {
      check_min_args(cmd_name, args, 6)?;
      let key = arg_string(args[1]);
      let group = arg_string(args[2]);
      let consumer = arg_string(args[3]);
      let min_idle = arg_u64(args[4]).unwrap_or(0);
      let start = arg_string(args[5]);
      let mut count = None;
      let mut justid = false;

      let mut i = 6;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"count") && i + 1 < args.len() {
          count = arg_usize(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"justid") {
          justid = true;
          i += 1;
        } else {
          i += 1;
        }
      }

      Ok(Cmd::XAutoClaim {
        key,
        group,
        consumer,
        min_idle,
        start,
        count,
        justid,
      })
    }
    // @cmd: XGROUP ...
    "xgroup" => {
      let list = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::XGroup(list))
    }
    // @cmd: XPENDING key group [[IDLE min-idle] start end count [consumer]]
    "xpending" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let group = arg_string(args[2]);
      let mut start = None;
      let mut end = None;
      let mut count = None;
      let mut consumer = None;
      let mut idle = None;

      let mut i = 3;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"idle") && i + 1 < args.len() {
          idle = arg_u64(args[i + 1]);
          i += 2;
        } else if start.is_none() {
          start = Some(arg_string(args[i]));
          i += 1;
        } else if end.is_none() {
          end = Some(arg_string(args[i]));
          i += 1;
        } else if count.is_none() {
          count = arg_usize(args[i]);
          i += 1;
        } else if consumer.is_none() {
          consumer = Some(arg_string(args[i]));
          i += 1;
        } else {
          i += 1;
        }
      }

      Ok(Cmd::XPending {
        key,
        group,
        start,
        end,
        count,
        consumer,
        idle,
      })
    }
    // @cmd: XREADGROUP GROUP grp csm [COUNT cnt] [BLOCK ms] [NOACK] STREAMS key [key ...] id [id ...]
    "xreadgroup" => {
      check_min_args(cmd_name, args, 6)?;
      let mut group = String::new();
      let mut consumer = String::new();
      let mut count = None;
      let mut block = None;
      let mut noack = false;

      let mut i = 1;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"group") && i + 2 < args.len() {
          group = arg_string(args[i + 1]);
          consumer = arg_string(args[i + 2]);
          i += 3;
        } else if opt.eq_ignore_ascii_case(b"count") && i + 1 < args.len() {
          count = arg_usize(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"block") && i + 1 < args.len() {
          block = arg_u64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"noack") {
          noack = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"streams") {
          i += 1;
          break;
        } else {
          i += 1;
        }
      }

      let remaining = &args[i..];
      let num_streams = remaining.len() / 2;
      if num_streams == 0 {
        return Err(err_wrong_args(cmd_name));
      }
      let streams = remaining[..num_streams]
        .iter()
        .map(|a| arg_string(a))
        .collect();
      let ids = remaining[num_streams..]
        .iter()
        .map(|a| arg_string(a))
        .collect();

      Ok(Cmd::XReadGroup {
        group,
        consumer,
        streams,
        ids,
        count,
        block,
        noack,
      })
    }
    // @cmd: XSETID key last-id [ENTRIESADDED read] [MAXDELETEDID del_id]
    "xsetid" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let last_id = arg_string(args[2]);
      let mut entries_added = None;
      let mut max_deleted_id = None;

      let mut i = 3;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"entriesadded") && i + 1 < args.len() {
          entries_added = arg_u64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"maxdeletedid") && i + 1 < args.len() {
          max_deleted_id = Some(arg_string(args[i + 1]));
          i += 2;
        } else {
          i += 1;
        }
      }

      Ok(Cmd::XSetId {
        key,
        last_id,
        entries_added,
        max_deleted_id,
      })
    }

    _ => return Ok(None),
  };
  res.map(Some)
}
