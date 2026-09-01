use crate::parse::util::*;
use crate::types::Cmd;
use wedb_embed::Result;

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: SADD key member [member ...]
    "sadd" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let members = args[2..].iter().map(|m| m.to_vec()).collect();
      Ok(Cmd::SAdd(key, members))
    }
    // @cmd: SREM key member [member ...]
    "srem" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let members = args[2..].iter().map(|m| m.to_vec()).collect();
      Ok(Cmd::SRem(key, members))
    }
    // @cmd: SMEMBERS key
    "smembers" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::SMembers(arg_string(args[1])))
    }
    // @cmd: SISMEMBER key member
    "sismember" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::SIsMember(arg_string(args[1]), args[2].to_vec()))
    }
    // @cmd: SMISMEMBER key member [member ...]
    "smismember" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let members = args[2..].iter().map(|m| m.to_vec()).collect();
      Ok(Cmd::SMIsMember(key, members))
    }
    // @cmd: SCARD key
    "scard" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::SCard(arg_string(args[1])))
    }
    // @cmd: SPOP key [count]
    "spop" => {
      check_min_args(cmd_name, args, 2)?;
      let count = if args.len() > 2 {
        arg_usize(args[2])
      } else {
        None
      };
      Ok(Cmd::SPop(arg_string(args[1]), count))
    }
    // @cmd: SRANDMEMBER key [count]
    "srandmember" => {
      check_min_args(cmd_name, args, 2)?;
      let count = if args.len() > 2 {
        arg_i64(args[2])
      } else {
        None
      };
      Ok(Cmd::SRandMember(arg_string(args[1]), count))
    }
    // @cmd: SMOVE source destination member
    "smove" => {
      check_min_args(cmd_name, args, 4)?;
      Ok(Cmd::SMove {
        src: arg_string(args[1]),
        dst: arg_string(args[2]),
        member: args[3].to_vec(),
      })
    }
    // @cmd: SUNION key [key ...]
    "sunion" => {
      check_min_args(cmd_name, args, 2)?;
      let keys = args[1..].iter().map(|k| arg_string(k)).collect();
      Ok(Cmd::SUnion(keys))
    }
    // @cmd: SINTER key [key ...]
    "sinter" => {
      check_min_args(cmd_name, args, 2)?;
      let keys = args[1..].iter().map(|k| arg_string(k)).collect();
      Ok(Cmd::SInter(keys))
    }
    // @cmd: SDIFF key [key ...]
    "sdiff" => {
      check_min_args(cmd_name, args, 2)?;
      let keys = args[1..].iter().map(|k| arg_string(k)).collect();
      Ok(Cmd::SDiff(keys))
    }
    // @cmd: SINTERCARD numkeys key [key ...] [LIMIT limit]
    "sintercard" => {
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
      Ok(Cmd::SInterCard { keys, limit })
    }
    // @cmd: SDIFFCARD numkeys key [key ...] [LIMIT limit]
    "sdiffcard" => {
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
      Ok(Cmd::SDiffCard { keys, limit })
    }
    // @cmd: SUNIONCARD numkeys key [key ...] [LIMIT limit]
    "sunioncard" => {
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
      Ok(Cmd::SUnionCard { keys, limit })
    }
    // @cmd: SDIFFSTORE destination key [key ...]
    "sdiffstore" => {
      check_min_args(cmd_name, args, 3)?;
      let dst = arg_string(args[1]);
      let keys = args[2..].iter().map(|k| arg_string(k)).collect();
      Ok(Cmd::SDiffStore(dst, keys))
    }
    // @cmd: SUNIONSTORE destination key [key ...]
    "sunionstore" => {
      check_min_args(cmd_name, args, 3)?;
      let dst = arg_string(args[1]);
      let keys = args[2..].iter().map(|k| arg_string(k)).collect();
      Ok(Cmd::SUnionStore(dst, keys))
    }
    // @cmd: SINTERSTORE destination key [key ...]
    "sinterstore" => {
      check_min_args(cmd_name, args, 3)?;
      let dst = arg_string(args[1]);
      let keys = args[2..].iter().map(|k| arg_string(k)).collect();
      Ok(Cmd::SInterStore(dst, keys))
    }
    // @cmd: SSCAN key cursor [MATCH pattern] [COUNT count]
    "sscan" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let cursor = arg_u64(args[2]).unwrap_or(0);
      let scan_opts = ScanOptions::parse(args, 3);
      Ok(Cmd::SScan {
        key,
        cursor,
        pattern: scan_opts.pattern,
        count: scan_opts.count,
      })
    }

    _ => return Ok(None),
  };
  res.map(Some)
}
