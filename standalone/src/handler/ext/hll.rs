use crate::handler::resp_util::bool_to_int;
use std::sync::Arc;
use webc_cmd::Cmd;
use wedb_embed::{Error, Result, WeDb};
use wedb_resp::RespValue;

/// 处理所有 HyperLogLog 相关命令
pub async fn handle_hll(db: &Arc<WeDb>, cmd: Cmd) -> Result<RespValue> {
  match cmd {
    Cmd::PfAdd(key, elements) => {
      let e: Vec<&[u8]> = elements.iter().map(|e| e.as_slice()).collect();
      let changed = db.pfadd(key.as_bytes(), &e)?;
      Ok(bool_to_int(changed))
    }
    Cmd::PfCount(keys) => {
      let k: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
      let count = db.pfcount(&k)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::PfMerge(dest, keys) => {
      let k: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
      db.pfmerge(dest.as_bytes(), &k)?;
      Ok(RespValue::ok())
    }
    Cmd::PfSelfTest => Ok(RespValue::ok()),
    _ => Err(Error::internal("unsupported hyperloglog command")),
  }
}
