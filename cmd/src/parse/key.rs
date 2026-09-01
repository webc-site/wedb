use crate::parse::util::*;
use crate::types::Cmd;
use wedb_embed::Result;

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: DEL key [key ...]
    "del" => {
      check_min_args(cmd_name, args, 2)?;
      let keys = args[1..].iter().map(|k| arg_string(k)).collect();
      Ok(Cmd::Del(keys))
    }
    // @cmd: UNLINK key [key ...]
    "unlink" => {
      check_min_args(cmd_name, args, 2)?;
      let keys = args[1..].iter().map(|k| arg_string(k)).collect();
      Ok(Cmd::Unlink(keys))
    }
    // @cmd: EXISTS key [key ...]
    "exists" => {
      check_min_args(cmd_name, args, 2)?;
      let keys = args[1..].iter().map(|k| arg_string(k)).collect();
      Ok(Cmd::Exists(keys))
    }
    // @cmd: KEYS pattern
    "keys" => {
      let pat = if args.len() > 1 {
        arg_string(args[1])
      } else {
        "*".to_string()
      };
      Ok(Cmd::Keys(pat))
    }
    // @cmd: SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
    "scan" => {
      let cursor = if args.len() > 1 {
        arg_u64(args[1]).unwrap_or(0)
      } else {
        0
      };
      let scan_opts = ScanOptions::parse(args, 2);
      Ok(Cmd::Scan {
        cursor,
        pattern: scan_opts.pattern,
        count: scan_opts.count,
      })
    }
    "scanprefix" | "prefix" => {
      let prefix = if args.len() > 1 {
        args[1].to_vec()
      } else {
        Vec::new()
      };
      let count = if args.len() > 2 {
        arg_usize(args[2])
      } else {
        None
      };
      Ok(Cmd::ScanPrefix { prefix, count })
    }
    // @cmd: DBSIZE
    "dbsize" => Ok(Cmd::DbSize),
    // @cmd: FLUSHDB [ASYNC | SYNC]
    "flushdb" => Ok(Cmd::FlushDb),
    // @cmd: FLUSHALL [ASYNC | SYNC]
    "flushall" => Ok(Cmd::FlushAll),
    // @cmd: TYPE key
    "type" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::Type(arg_string(args[1])))
    }
    // @cmd: TTL key
    "ttl" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::Ttl(arg_string(args[1])))
    }
    // @cmd: PTTL key
    "pttl" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::Pttl(arg_string(args[1])))
    }
    // @cmd: EXPIRETIME key
    "expiretime" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::ExpireTime(arg_string(args[1])))
    }
    // @cmd: PEXPIRETIME key
    "pexpiretime" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::PExpireTime(arg_string(args[1])))
    }
    // @cmd: EXPIRE key seconds [NX | XX | GT | LT]
    "expire" => {
      check_min_args(cmd_name, args, 3)?;
      let ttl = parse_u64_strict(args[2])?;
      Ok(Cmd::Expire(arg_string(args[1]), ttl))
    }
    // @cmd: PEXPIRE key milliseconds [NX | XX | GT | LT]
    "pexpire" => {
      check_min_args(cmd_name, args, 3)?;
      let ttl_ms = parse_u64_strict(args[2])?;
      Ok(Cmd::PExpire(arg_string(args[1]), ttl_ms))
    }
    // @cmd: EXPIREAT key timestamp [NX | XX | GT | LT]
    "expireat" => {
      check_min_args(cmd_name, args, 3)?;
      let ts = parse_u64_strict(args[2])?;
      Ok(Cmd::ExpireAt(arg_string(args[1]), ts))
    }
    // @cmd: PEXPIREAT key milliseconds-timestamp [NX | XX | GT | LT]
    "pexpireat" => {
      check_min_args(cmd_name, args, 3)?;
      let ts_ms = parse_u64_strict(args[2])?;
      Ok(Cmd::PExpireAt(arg_string(args[1]), ts_ms))
    }
    // @cmd: PERSIST key
    "persist" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::Persist(arg_string(args[1])))
    }
    // @cmd: RENAME key newkey
    "rename" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::Rename(arg_string(args[1]), arg_string(args[2])))
    }
    // @cmd: RENAMENX key newkey
    "renamenx" => {
      check_min_args(cmd_name, args, 3)?;
      Ok(Cmd::RenameNx(arg_string(args[1]), arg_string(args[2])))
    }
    // @cmd: COPY source destination [DB destination-db] [REPLACE]
    "copy" => {
      check_min_args(cmd_name, args, 3)?;
      let src = arg_string(args[1]);
      let dst = arg_string(args[2]);
      let mut db = None;
      let mut replace = false;

      let mut i = 3;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"db") && i + 1 < args.len() {
          db = arg_u32(args[i + 1]);
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"replace") {
          replace = true;
          i += 1;
        } else {
          i += 1;
        }
      }
      Ok(Cmd::Copy {
        src,
        dst,
        db,
        replace,
      })
    }
    // @cmd: RANDOMKEY
    "randomkey" => Ok(Cmd::RandomKey),
    // @cmd: TOUCH key [key ...]
    "touch" => {
      check_min_args(cmd_name, args, 2)?;
      let keys = args[1..].iter().map(|k| arg_string(k)).collect();
      Ok(Cmd::Touch(keys))
    }
    // @cmd: OBJECT subcommand [arguments ...]
    "object" => {
      check_min_args(cmd_name, args, 2)?;
      let subcmd = arg_string(args[1]);
      let key = if args.len() > 2 {
        arg_string(args[2])
      } else {
        String::new()
      };
      Ok(Cmd::Object { subcmd, key })
    }
    // @cmd: SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC | DESC] [ALPHA] [STORE destination]
    "sort" => {
      check_min_args(cmd_name, args, 2)?;
      let p = parse_sort_args(args)?;
      Ok(Cmd::Sort {
        key: p.key,
        by: p.by,
        offset: p.offset,
        count: p.count,
        patterns: p.patterns,
        desc: p.desc,
        alpha: p.alpha,
        store: p.store,
      })
    }
    // @cmd: SORT_RO key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC | DESC] [ALPHA]
    "sort_ro" => {
      check_min_args(cmd_name, args, 2)?;
      let p = parse_sort_args(args)?;
      Ok(Cmd::SortRo {
        key: p.key,
        by: p.by,
        offset: p.offset,
        count: p.count,
        patterns: p.patterns,
        desc: p.desc,
        alpha: p.alpha,
      })
    }
    // @cmd: KMETADATA key
    "kmetadata" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::KMetaData(arg_string(args[1])))
    }

    _ => return Ok(None),
  };
  res.map(Some)
}

struct ParsedSortArgs {
  key: String,
  by: Option<String>,
  offset: usize,
  count: Option<usize>,
  patterns: Vec<String>,
  desc: bool,
  alpha: bool,
  store: Option<String>,
}

fn parse_sort_args(args: &[&[u8]]) -> Result<ParsedSortArgs> {
  let key = arg_string(args[1]);
  let mut by = None;
  let mut offset = 0;
  let mut count = None;
  let mut patterns = Vec::new();
  let mut desc = false;
  let mut alpha = false;
  let mut store = None;

  let mut i = 2;
  while i < args.len() {
    let opt = args[i];
    if opt.eq_ignore_ascii_case(b"by") && i + 1 < args.len() {
      by = Some(arg_string(args[i + 1]));
      i += 2;
    } else if opt.eq_ignore_ascii_case(b"limit") && i + 2 < args.len() {
      offset = parse_usize_strict(args[i + 1])?;
      count = Some(parse_usize_strict(args[i + 2])?);
      i += 3;
    } else if opt.eq_ignore_ascii_case(b"get") && i + 1 < args.len() {
      patterns.push(arg_string(args[i + 1]));
      i += 2;
    } else if opt.eq_ignore_ascii_case(b"desc") {
      desc = true;
      i += 1;
    } else if opt.eq_ignore_ascii_case(b"asc") {
      desc = false;
      i += 1;
    } else if opt.eq_ignore_ascii_case(b"alpha") {
      alpha = true;
      i += 1;
    } else if opt.eq_ignore_ascii_case(b"store") && i + 1 < args.len() {
      store = Some(arg_string(args[i + 1]));
      i += 2;
    } else {
      i += 1;
    }
  }

  Ok(ParsedSortArgs {
    key,
    by,
    offset,
    count,
    patterns,
    desc,
    alpha,
    store,
  })
}
