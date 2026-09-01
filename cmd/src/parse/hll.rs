use crate::parse::util::*;
use crate::types::Cmd;
use wedb_embed::Result;

pub fn parse(cmd_name: &str, args: &[&[u8]]) -> Result<Option<Cmd>> {
  let res: Result<Cmd> = match cmd_name {
    // @cmd: PFADD key element [element ...]
    "pfadd" => {
      check_min_args(cmd_name, args, 2)?;
      let key = arg_string(args[1]);
      let items = args[2..].iter().map(|i| i.to_vec()).collect();
      Ok(Cmd::PfAdd(key, items))
    }
    // @cmd: PFCOUNT key [key ...]
    "pfcount" => {
      check_min_args(cmd_name, args, 2)?;
      let keys = args[1..].iter().map(|k| arg_string(k)).collect();
      Ok(Cmd::PfCount(keys))
    }
    // @cmd: PFMERGE destkey sourcekey [sourcekey ...]
    "pfmerge" => {
      check_min_args(cmd_name, args, 3)?;
      let dst = arg_string(args[1]);
      let sources = args[2..].iter().map(|k| arg_string(k)).collect();
      Ok(Cmd::PfMerge(dst, sources))
    }
    // @cmd: PFSELFTEST
    "pfselftest" => Ok(Cmd::PfSelfTest),

    _ => return Ok(None),
  };
  res.map(Some)
}
