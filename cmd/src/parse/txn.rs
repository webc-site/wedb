use crate::parse::util::*;
use crate::types::Cmd;
use wedb_embed::Result;

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: MULTI
    "multi" => Ok(Cmd::Multi),
    // @cmd: DISCARD
    "discard" => Ok(Cmd::Discard),
    // @cmd: EXEC
    "exec" => Ok(Cmd::Exec),
    // @cmd: WATCH key [key ...]
    "watch" => {
      check_min_args(cmd_name, args, 2)?;
      let keys = args[1..].iter().map(|a| arg_string(a)).collect();
      Ok(Cmd::Watch(keys))
    }
    // @cmd: UNWATCH
    "unwatch" => Ok(Cmd::Unwatch),

    _ => return Ok(None),
  };
  res.map(Some)
}
