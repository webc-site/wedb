use crate::parse::util::*;
use crate::types::Cmd;
use wedb_embed::Result;
use wedb_embed::sortedint::parse_range_spec;

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: SIADD key id [id ...]
    "siadd" | "si.add" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let mut ids = Vec::with_capacity(args.len() - 2);
      for a in &args[2..] {
        ids.push(parse_u64_strict(a)?);
      }
      Ok(Cmd::SiAdd(key, ids))
    }
    // @cmd: SIREM key id [id ...]
    "sirem" | "si.rem" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let mut ids = Vec::with_capacity(args.len() - 2);
      for a in &args[2..] {
        ids.push(parse_u64_strict(a)?);
      }
      Ok(Cmd::SiRem(key, ids))
    }
    // @cmd: SICARD key
    "sicard" | "si.card" => {
      check_exact_args(cmd_name, args, 2)?;
      Ok(Cmd::SiCard(arg_string(args[1])))
    }
    // @cmd: SIEXISTS key id [id ...]
    "siexists" | "si.exists" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let mut ids = Vec::with_capacity(args.len() - 2);
      for a in &args[2..] {
        ids.push(parse_u64_strict(a)?);
      }
      Ok(Cmd::SiExists(key, ids))
    }
    // @cmd: SIRANGE key offset count [CURSOR cursor]
    "sirange" | "si.range" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let offset = parse_usize_strict(args[2])?;
      let limit = parse_usize_strict(args[3])?;
      let mut cursor = 0u64;
      if args.len() == 6 {
        if !args[4].eq_ignore_ascii_case(b"cursor") {
          return Err(err_syntax());
        }
        cursor = parse_u64_strict(args[5])?;
      } else if args.len() != 4 {
        return Err(err_wrong_args(cmd_name));
      }
      Ok(Cmd::SiRange {
        key,
        cursor,
        offset,
        limit,
      })
    }
    // @cmd: SIREVRANGE key offset count [CURSOR cursor]
    "sirevrange" | "si.revrange" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let offset = parse_usize_strict(args[2])?;
      let limit = parse_usize_strict(args[3])?;
      let mut cursor = 0u64;
      if args.len() == 6 {
        if !args[4].eq_ignore_ascii_case(b"cursor") {
          return Err(err_syntax());
        }
        cursor = parse_u64_strict(args[5])?;
      } else if args.len() != 4 {
        return Err(err_wrong_args(cmd_name));
      }
      Ok(Cmd::SiRevRange {
        key,
        cursor,
        offset,
        limit,
      })
    }
    // @cmd: SIRANGEBYVALUE key min max [LIMIT offset count]
    "sirangebyvalue" | "si.rangebyvalue" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let min_str = arg_str(args[2]);
      let max_str = arg_str(args[3]);
      let mut spec = parse_range_spec(min_str, max_str)?;
      let mut offset = 0;
      let mut count = None;
      if args.len() == 7 {
        if !args[4].eq_ignore_ascii_case(b"limit") {
          return Err(err_syntax());
        }
        offset = parse_usize_strict(args[5])?;
        count = Some(parse_usize_strict(args[6])?);
      } else if args.len() != 4 {
        return Err(err_wrong_args(cmd_name));
      }
      spec.offset = offset;
      spec.count = count;
      spec.reversed = false;
      Ok(Cmd::SiRangeByValue { key, spec })
    }
    // @cmd: SIREVRANGEBYVALUE key max min [LIMIT offset count]
    "sirevrangebyvalue" | "si.revrangebyvalue" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let max_str = arg_str(args[2]);
      let min_str = arg_str(args[3]);
      let mut spec = parse_range_spec(min_str, max_str)?;
      let mut offset = 0;
      let mut count = None;
      if args.len() == 7 {
        if !args[4].eq_ignore_ascii_case(b"limit") {
          return Err(err_syntax());
        }
        offset = parse_usize_strict(args[5])?;
        count = Some(parse_usize_strict(args[6])?);
      } else if args.len() != 4 {
        return Err(err_wrong_args(cmd_name));
      }
      spec.offset = offset;
      spec.count = count;
      spec.reversed = true;
      Ok(Cmd::SiRevRangeByValue { key, spec })
    }

    _ => return Ok(None),
  };
  res.map(Some)
}
