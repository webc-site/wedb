use crate::parse::util::*;
use crate::types::Cmd;
use wedb_embed::{Error, Result};

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: EVAL script numkeys [key ...] [arg ...] / EVAL_RO script numkeys [key ...] [arg ...]
    "eval" | "eval_ro" => {
      check_min_args(cmd_name, args, 3)?;
      let script = arg_string(args[1]);
      let numkeys = parse_usize_strict(args[2])?;
      if args.len() < 3 + numkeys {
        return Err(Error::invalid_data(
          "ERR Number of keys can't be greater than number of args",
        ));
      }
      let keys = args[3..3 + numkeys].iter().map(|a| arg_string(a)).collect();
      let script_args = args[3 + numkeys..].iter().map(|a| a.to_vec()).collect();
      Ok(Cmd::Eval {
        script,
        keys,
        args: script_args,
      })
    }
    // @cmd: EVALSHA sha1 numkeys [key ...] [arg ...] / EVALSHA_RO sha1 numkeys [key ...] [arg ...]
    "evalsha" | "evalsha_ro" => {
      check_min_args(cmd_name, args, 3)?;
      let sha = arg_string(args[1]);
      let numkeys = parse_usize_strict(args[2])?;
      if args.len() < 3 + numkeys {
        return Err(Error::invalid_data(
          "ERR Number of keys can't be greater than number of args",
        ));
      }
      let keys = args[3..3 + numkeys].iter().map(|a| arg_string(a)).collect();
      let script_args = args[3 + numkeys..].iter().map(|a| a.to_vec()).collect();
      Ok(Cmd::EvalSha {
        sha,
        keys,
        args: script_args,
      })
    }
    // @cmd: SCRIPT LOAD script / SCRIPT EXISTS sha1 / SCRIPT FLUSH / SCRIPT KILL
    "script" => {
      let list = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::Script(list))
    }
    // @cmd: FUNCTION LOAD / LIST / DELETE
    "function" => {
      let list = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::Function(list))
    }

    _ => return Ok(None),
  };
  res.map(Some)
}
