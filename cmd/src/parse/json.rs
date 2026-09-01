use crate::parse::util::*;
use crate::types::Cmd;
use wedb_embed::Result;

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: JSON.SET key path json [NX | XX]
    "json.set" => {
      check_min_args(cmd_name, args, 4)?;
      let key = arg_string(args[1]);
      let path = arg_string(args[2]);
      let value = arg_string(args[3]);
      let mut nx = false;
      let mut xx = false;
      for &a in &args[4..] {
        if a.eq_ignore_ascii_case(b"nx") {
          nx = true;
        } else if a.eq_ignore_ascii_case(b"xx") {
          xx = true;
        }
      }
      Ok(Cmd::JsonSet {
        key,
        path,
        value,
        nx,
        xx,
      })
    }
    // @cmd: JSON.GET key [INDENT indent] [NEWLINE newline] [SPACE space] [path ...]
    "json.get" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let mut indent = None;
      let mut newline = None;
      let mut space = None;
      let mut paths = Vec::new();
      let mut i = 2;
      while i < args.len() {
        let opt = args[i];
        if opt.eq_ignore_ascii_case(b"indent") && i + 1 < args.len() {
          indent = Some(arg_string(args[i + 1]));
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"newline") && i + 1 < args.len() {
          newline = Some(arg_string(args[i + 1]));
          i += 2;
        } else if opt.eq_ignore_ascii_case(b"space") && i + 1 < args.len() {
          space = Some(arg_string(args[i + 1]));
          i += 2;
        } else {
          paths.push(arg_string(opt));
          i += 1;
        }
      }
      if paths.is_empty() {
        paths.push("$".to_string());
      }
      Ok(Cmd::JsonGet {
        key,
        indent,
        newline,
        space,
        paths,
      })
    }
    // @cmd: JSON.DEL / JSON.FORGET
    "json.del" | "json.forget" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let path = if args.len() > 2 {
        Some(arg_string(args[2]))
      } else {
        None
      };
      Ok(Cmd::JsonDel { key, path })
    }
    // @cmd: JSON.TYPE key [path]
    "json.type" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let path = if args.len() > 2 {
        Some(arg_string(args[2]))
      } else {
        None
      };
      Ok(Cmd::JsonType { key, path })
    }
    // @cmd: JSON.ARRAPPEND key path json [json ...]
    "json.arrappend" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let path = Some(arg_string(args[2]));
      let values = args[3..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::JsonArrAppend { key, path, values })
    }
    // @cmd: JSON.ARRINSERT key path index json [json ...]
    "json.arrinsert" => {
      check_min_args(cmd_name, args, 5)?;
      let key = arg_string(args[1]);
      let path = arg_string(args[2]);
      let index = parse_i64_strict(args[3])?;
      let values = args[4..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::JsonArrInsert {
        key,
        path,
        index,
        values,
      })
    }
    // @cmd: JSON.ARRTRIM key path start stop
    "json.arrtrim" => {
      check_min_args(cmd_name, args, 5)?;
      let key = arg_string(args[1]);
      let path = arg_string(args[2]);
      let start = parse_i64_strict(args[3])?;
      let stop = parse_i64_strict(args[4])?;
      Ok(Cmd::JsonArrTrim {
        key,
        path,
        start,
        stop,
      })
    }
    // @cmd: JSON.CLEAR key [path]
    "json.clear" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let path = if args.len() > 2 {
        Some(arg_string(args[2]))
      } else {
        None
      };
      Ok(Cmd::JsonClear { key, path })
    }
    // @cmd: JSON.TOGGLE key path
    "json.toggle" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let path = if args.len() > 2 {
        Some(arg_string(args[2]))
      } else {
        None
      };
      Ok(Cmd::JsonToggle { key, path })
    }
    // @cmd: JSON.ARRLEN key [path]
    "json.arrlen" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let path = if args.len() > 2 {
        Some(arg_string(args[2]))
      } else {
        None
      };
      Ok(Cmd::JsonArrLen { key, path })
    }
    // @cmd: JSON.MERGE key path json
    "json.merge" => {
      check_min_args(cmd_name, args, 4)?;
      Ok(Cmd::JsonMerge {
        key: arg_string(args[1]),
        path: arg_string(args[2]),
        value: arg_string(args[3]),
      })
    }
    // @cmd: JSON.OBJKEYS key [path]
    "json.objkeys" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let path = if args.len() > 2 {
        Some(arg_string(args[2]))
      } else {
        None
      };
      Ok(Cmd::JsonObjKeys { key, path })
    }
    // @cmd: JSON.ARRPOP key [path [index]]
    "json.arrpop" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let path = if args.len() > 2 {
        Some(arg_string(args[2]))
      } else {
        None
      };
      let index = if args.len() > 3 {
        arg_i64(args[3])
      } else {
        None
      };
      Ok(Cmd::JsonArrPop { key, path, index })
    }
    // @cmd: JSON.ARRINDEX key path json-scalar [start [stop]]
    "json.arrindex" => {
      check_min_args(cmd_name, args, 4)?;
      Ok(Cmd::JsonArrIndex {
        key: arg_string(args[1]),
        path: arg_string(args[2]),
        value: arg_string(args[3]),
      })
    }
    // @cmd: JSON.NUMINCRBY key path number
    "json.numincrby" => {
      check_min_args(cmd_name, args, 4)?;
      let num = parse_float_strict(args[3])?;
      Ok(Cmd::JsonNumIncrBy {
        key: arg_string(args[1]),
        path: arg_string(args[2]),
        number: num,
      })
    }
    // @cmd: JSON.NUMMULTBY key path number
    "json.nummultby" => {
      check_min_args(cmd_name, args, 4)?;
      let num = parse_float_strict(args[3])?;
      Ok(Cmd::JsonNumMultBy {
        key: arg_string(args[1]),
        path: arg_string(args[2]),
        number: num,
      })
    }
    // @cmd: JSON.OBJLEN key [path]
    "json.objlen" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let path = if args.len() > 2 {
        Some(arg_string(args[2]))
      } else {
        None
      };
      Ok(Cmd::JsonObjLen { key, path })
    }
    // @cmd: JSON.STRAPPEND key [path] json-string
    "json.strappend" => {
      check_min_args(cmd_name, args, 3)?;
      let key = arg_string(args[1]);
      let (path, value) = if args.len() >= 4 {
        (Some(arg_string(args[2])), arg_string(args[3]))
      } else {
        (None, arg_string(args[2]))
      };
      Ok(Cmd::JsonStrAppend { key, path, value })
    }
    // @cmd: JSON.STRLEN key [path]
    "json.strlen" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let path = if args.len() > 2 {
        Some(arg_string(args[2]))
      } else {
        None
      };
      Ok(Cmd::JsonStrLen { key, path })
    }
    // @cmd: JSON.MGET key [key ...] path
    "json.mget" => {
      check_min_args(cmd_name, args, 3)?;
      let path = arg_string(args[args.len() - 1]);
      let keys = args[1..args.len() - 1]
        .iter()
        .map(|a| arg_string(a))
        .collect();
      Ok(Cmd::JsonMGet { keys, path })
    }
    // @cmd: JSON.MSET key path json [key path json ...]
    "json.mset" => {
      if args.len() < 4 || !(args.len() - 1).is_multiple_of(3) {
        return Err(err_wrong_args(cmd_name));
      }
      let mut entries = Vec::with_capacity((args.len() - 1) / 3);
      for chunk in args[1..].as_chunks::<3>().0 {
        entries.push((
          arg_string(chunk[0]),
          arg_string(chunk[1]),
          arg_string(chunk[2]),
        ));
      }
      Ok(Cmd::JsonMSet(entries))
    }
    // @cmd: JSON.DEBUG [key]
    "json.debug" => {
      let key = if args.len() > 1 {
        arg_string(args[1])
      } else {
        String::new()
      };
      Ok(Cmd::JsonDebug(key, None))
    }
    // @cmd: JSON.RESP key [path]
    "json.resp" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let path = if args.len() > 2 {
        Some(arg_string(args[2]))
      } else {
        None
      };
      Ok(Cmd::JsonResp { key, path })
    }
    // @cmd: JSON.INFO key
    "json.info" => {
      check_min_args(cmd_name, args, 2)?;
      Ok(Cmd::JsonInfo(arg_string(args[1])))
    }

    _ => return Ok(None),
  };
  res.map(Some)
}
