use crate::parse::util::*;
use crate::types::Cmd;
use std::str::FromStr;
use wedb_embed::{
  BitfieldEncoding, BitfieldOperation, BitfieldOverflow, Result, parse_bitfield_offset,
};

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: GET key
    "get" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::Get(arg_string(args[1])))
    }
    // @cmd: SET key value [EX sec | PX ms | EXAT ts | PXAT ts-ms] [NX | XX] [KEEPTTL] [GET]
    "set" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let value = args[2].to_vec();

      let mut ex = None;
      let mut px = None;
      let mut exat = None;
      let mut pxat = None;
      let mut nx = false;
      let mut xx = false;
      let mut get = false;
      let mut keepttl = false;

      let mut i = 3;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"ex") && i + 1 < args.len() {
          ex = arg_u64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"px") && i + 1 < args.len() {
          px = arg_u64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"exat") && i + 1 < args.len() {
          exat = arg_u64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"pxat") && i + 1 < args.len() {
          pxat = arg_u64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"nx") {
          nx = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"xx") {
          xx = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"get") {
          get = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"keepttl") {
          keepttl = true;
          i += 1;
        } else {
          i += 1;
        }
      }

      Ok(Cmd::Set {
        key,
        value,
        ex,
        px,
        exat,
        pxat,
        nx,
        xx,
        get,
        keepttl,
      })
    }
    // @cmd: SETNX key value
    "setnx" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::SetNx(arg_string(args[1]), args[2].to_vec()))
    }
    // @cmd: SETEX key seconds value
    "setex" => {
      check_min_args(cmd_name, args, 4)?;
      let ttl = arg_u64(args[2]).unwrap_or(0);
      Ok(Cmd::SetEx(arg_string(args[1]), ttl, args[3].to_vec()))
    }
    // @cmd: PSETEX key milliseconds value
    "psetex" => {
      check_min_args(cmd_name, args, 4)?;
      let ttl_ms = arg_u64(args[2]).unwrap_or(0);
      Ok(Cmd::PSetEx(arg_string(args[1]), ttl_ms, args[3].to_vec()))
    }
    // @cmd: GETSET key value
    "getset" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::GetSet(arg_string(args[1]), args[2].to_vec()))
    }
    // @cmd: GETDEL key
    "getdel" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::GetDel(arg_string(args[1])))
    }
    // @cmd: GETEX key [EX sec | PX ms | EXAT ts | PXAT ts-ms | PERSIST]
    "getex" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let mut ex = None;
      let mut px = None;
      let mut persist = false;

      let mut i = 2;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"ex") && i + 1 < args.len() {
          ex = arg_u64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"px") && i + 1 < args.len() {
          px = arg_u64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"persist") {
          persist = true;
          i += 1;
        } else {
          i += 1;
        }
      }
      Ok(Cmd::GetEx {
        key,
        ex,
        px,
        persist,
      })
    }
    // @cmd: MGET key [key ...]
    "mget" => {
      check_min_args(cmd_name, args, 2)?;
      let keys = args[1..].iter().map(|k| arg_string(k)).collect();
      Ok(Cmd::MGet(keys))
    }
    // @cmd: MSET key value [key value ...]
    "mset" => {
      if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return Err(err_wrong_args(cmd_name));
      }
      let mut pairs = Vec::with_capacity((args.len() - 1) / 2);
      for chunk in args[1..].as_chunks::<2>().0 {
        pairs.push((arg_string(chunk[0]), chunk[1].to_vec()));
      }
      Ok(Cmd::MSet(pairs))
    }
    // @cmd: MSETNX key value [key value ...]
    "msetnx" => {
      if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return Err(err_wrong_args(cmd_name));
      }
      let mut pairs = Vec::with_capacity((args.len() - 1) / 2);
      for chunk in args[1..].as_chunks::<2>().0 {
        pairs.push((arg_string(chunk[0]), chunk[1].to_vec()));
      }
      Ok(Cmd::MSetNx(pairs))
    }
    // @cmd: MSETEX ttl_sec key value [key value ...]
    "msetex" => {
      if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
        return Err(err_wrong_args(cmd_name));
      }
      let ttl = arg_u64(args[1]).unwrap_or(0);
      let mut pairs = Vec::with_capacity((args.len() - 2) / 2);
      for chunk in args[2..].as_chunks::<2>().0 {
        pairs.push((arg_string(chunk[0]), chunk[1].to_vec()));
      }
      Ok(Cmd::MSetEx {
        ttl_sec: ttl,
        pairs,
      })
    }
    // @cmd: INCR key
    "incr" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::Incr(arg_string(args[1])))
    }
    // @cmd: DECR key
    "decr" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::Decr(arg_string(args[1])))
    }
    // @cmd: INCRBY key increment
    "incrby" => {
      check_min_args(cmd_name, args, 3)?;
      let delta = parse_i64_strict(args[2])?;
      Ok(Cmd::IncrBy(arg_string(args[1]), delta))
    }
    // @cmd: DECRBY key decrement
    "decrby" => {
      check_min_args(cmd_name, args, 3)?;
      let delta = parse_i64_strict(args[2])?;
      Ok(Cmd::DecrBy(arg_string(args[1]), delta))
    }
    // @cmd: INCRBYFLOAT key increment
    "incrbyfloat" => {
      check_min_args(cmd_name, args, 3)?;
      let delta = parse_float_strict(args[2])?;
      Ok(Cmd::IncrByFloat(arg_string(args[1]), delta))
    }
    // @cmd: INCREX key [BYFLOAT f] [BYINT i] [SATURATE] [LBOUND lb] [UBOUND ub] [EX sec | PX ms | EXAT ts | PXAT ts-ms] [PERSIST] [ENX]
    "increx" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let mut by_float = None;
      let mut by_int = None;
      let mut saturate = false;
      let mut lbound = None;
      let mut ubound = None;
      let mut ex = None;
      let mut px = None;
      let mut exat = None;
      let mut pxat = None;
      let mut persist = false;
      let mut enx = false;

      let mut i = 2;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"byfloat") && i + 1 < args.len() {
          by_float = arg_f64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"byint") && i + 1 < args.len() {
          by_int = arg_i64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"saturate") {
          saturate = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"lbound") && i + 1 < args.len() {
          lbound = arg_f64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"ubound") && i + 1 < args.len() {
          ubound = arg_f64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"ex") && i + 1 < args.len() {
          ex = arg_u64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"px") && i + 1 < args.len() {
          px = arg_u64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"exat") && i + 1 < args.len() {
          exat = arg_u64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"pxat") && i + 1 < args.len() {
          pxat = arg_u64(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"persist") {
          persist = true;
          i += 1;
        } else if opt.eq_ignore_ascii_case(b"enx") {
          enx = true;
          i += 1;
        } else {
          i += 1;
        }
      }

      Ok(Cmd::IncrEx {
        key,
        by_float,
        by_int,
        saturate,
        lbound,
        ubound,
        ex,
        px,
        exat,
        pxat,
        persist,
        enx,
      })
    }
    // @cmd: STRLEN key
    "strlen" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::StrLen(arg_string(args[1])))
    }
    // @cmd: APPEND key value
    "append" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::Append(arg_string(args[1]), args[2].to_vec()))
    }
    // @cmd: GETRANGE key start end / SUBSTR key start end
    "getrange" | "substr" => {
      check_min_args(cmd_name, args, 4)?;
      let start = parse_i64_strict(args[2])?;
      let end = parse_i64_strict(args[3])?;
      Ok(Cmd::GetRange(arg_string(args[1]), start, end))
    }
    // @cmd: SETRANGE key offset value
    "setrange" => {
      check_min_args(cmd_name, args, 4)?;
      let offset = parse_usize_strict(args[2])?;
      Ok(Cmd::SetRange(arg_string(args[1]), offset, args[3].to_vec()))
    }
    // @cmd: DIGEST key
    "digest" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::Digest(arg_string(args[1])))
    }
    // @cmd: DELEX key [IF_EQ val]
    "delex" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let mut if_eq = None;
      let mut i = 2;
      while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"if_eq") && i + 1 < args.len() {
          if_eq = Some(args[i + 1].to_vec());
          i += 2;
        } else {
          i += 1;
        }
      }
      Ok(Cmd::DelEx { key, if_eq })
    }
    // @cmd: CAS key old_val new_val [EX sec]
    "cas" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let old_val = args[2].to_vec();
      let new_val = args[3].to_vec();
      let mut ex = None;
      if args.len() >= 6 && args[4].eq_ignore_ascii_case(b"ex") {
        ex = arg_u64(args[5]);
      }
      Ok(Cmd::Cas {
        key,
        old_val,
        new_val,
        ex,
      })
    }
    // @cmd: CAD key val
    "cad" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::Cad {
        key: arg_string(args[1]),
        val: args[2].to_vec(),
      })
    }
    // @cmd: LCS key1 key2 [LEN]
    "lcs" => {
      check_min_args(cmd_name, args, 3)?;
      let len_only = args.len() >= 4 && args[3].eq_ignore_ascii_case(b"len");
      Ok(Cmd::Lcs {
        key1: arg_string(args[1]),
        key2: arg_string(args[2]),
        len_only,
      })
    }
    // @cmd: GETBIT key offset
    "getbit" => {
      check_min_args(cmd_name, args, 3)?;
      let offset = parse_usize_strict(args[2])?;
      Ok(Cmd::GetBit(arg_string(args[1]), offset))
    }
    // @cmd: SETBIT key offset value
    "setbit" => {
      check_min_args(cmd_name, args, 4)?;
      let offset = parse_usize_strict(args[2])?;
      let val = arg_u8(args[3]).unwrap_or(0);
      Ok(Cmd::SetBit(arg_string(args[1]), offset, val))
    }
    // @cmd: BITCOUNT key [start end [BYTE | BIT]]
    "bitcount" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let mut start = None;
      let mut end = None;
      let mut is_bit = false;

      if args.len() >= 4 {
        start = arg_i64(args[2]);
        end = arg_i64(args[3]);
      }
      if args.len() >= 5 && args[4].eq_ignore_ascii_case(b"bit") {
        is_bit = true;
      }
      Ok(Cmd::BitCount {
        key,
        start,
        end,
        is_bit,
      })
    }
    // @cmd: BITPOS key bit [start [end]]
    "bitpos" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let bit = arg_u8(args[2]).unwrap_or(0);
      let start = if args.len() >= 4 {
        arg_i64(args[3])
      } else {
        None
      };
      let end = if args.len() >= 5 {
        arg_i64(args[4])
      } else {
        None
      };
      Ok(Cmd::BitPos {
        key,
        bit,
        start,
        end,
      })
    }
    // @cmd: BITOP operation destkey srckey [srckey ...]
    "bitop" => {
      check_min_args(cmd_name, args, 4)?;
      let op = arg_string(args[1]);
      let dest = arg_string(args[2]);
      let src_keys = args[3..].iter().map(|k| arg_string(k)).collect();
      Ok(Cmd::BitOp { op, dest, src_keys })
    }
    // @cmd: BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW WRAP|SAT|FAIL]
    "bitfield" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let ops = parse_bitfield_ops(&args[2..])?;
      Ok(Cmd::BitField { key, ops })
    }
    // @cmd: BITFIELD_RO key [GET type offset ...]
    "bitfield_ro" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let ops = parse_bitfield_ops(&args[2..])?;
      Ok(Cmd::BitFieldRo { key, ops })
    }

    _ => return Ok(None),
  };
  res.map(Some)
}

/// 解析 BITFIELD 子操作
fn parse_bitfield_ops(args: &[&[u8]]) -> Result<Vec<BitfieldOperation>> {
  let mut ops = Vec::new();
  let mut current_overflow = BitfieldOverflow::Wrap;
  let mut i = 0;

  while i < args.len() {
    let op_name = args[i];
    if op_name.eq_ignore_ascii_case(b"get") {
      if i + 2 >= args.len() {
        return Err(err_syntax());
      }
      let enc = BitfieldEncoding::from_str(arg_str(args[i + 1]))?;
      let offset = parse_bitfield_offset(arg_str(args[i + 2]), enc)?;
      ops.push(BitfieldOperation::get(enc, offset));
      i += 3;
    } else if op_name.eq_ignore_ascii_case(b"set") {
      if i + 3 >= args.len() {
        return Err(err_syntax());
      }
      let enc = BitfieldEncoding::from_str(arg_str(args[i + 1]))?;
      let offset = parse_bitfield_offset(arg_str(args[i + 2]), enc)?;
      let value = parse_i64_strict(args[i + 3])?;
      ops.push(BitfieldOperation::set(enc, offset, value, current_overflow));
      i += 4;
    } else if op_name.eq_ignore_ascii_case(b"incrby") {
      if i + 3 >= args.len() {
        return Err(err_syntax());
      }
      let enc = BitfieldEncoding::from_str(arg_str(args[i + 1]))?;
      let offset = parse_bitfield_offset(arg_str(args[i + 2]), enc)?;
      let increment = parse_i64_strict(args[i + 3])?;
      ops.push(BitfieldOperation::incrby(
        enc,
        offset,
        increment,
        current_overflow,
      ));
      i += 4;
    } else if op_name.eq_ignore_ascii_case(b"overflow") {
      if i + 1 >= args.len() {
        return Err(err_syntax());
      }
      current_overflow = BitfieldOverflow::from_str(arg_str(args[i + 1]))?;
      i += 2;
    } else {
      return Err(err_syntax());
    }
  }

  Ok(ops)
}
