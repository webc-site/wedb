use crate::handler::resp_util::bools_to_arr;
use std::sync::Arc;
use webc_cmd::Cmd;
use wedb_embed::{Error, Result, WeDb};
use wedb_resp::RespValue;

/// 处理所有 SortedInt (有序整型集合) 命令
pub async fn handle_sortedint(db: &Arc<WeDb>, cmd: Cmd) -> Result<RespValue> {
  match cmd {
    Cmd::SiAdd(key, ids) => {
      let count = db.si_add(key.as_bytes(), &ids)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::SiRem(key, ids) => {
      let count = db.si_rem(key.as_bytes(), &ids)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::SiCard(key) => {
      let count = db.si_card(key.as_bytes())?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::SiExists(key, ids) => {
      let results = db.si_mexist(key.as_bytes(), &ids)?;
      Ok(bools_to_arr(results))
    }
    Cmd::SiRange {
      key,
      cursor,
      offset,
      limit,
    } => {
      let ids = db.si_range(key.as_bytes(), cursor, offset, limit, false)?;
      let arr = ids
        .into_iter()
        .map(|id| RespValue::Blob(format!("{id}").into_bytes()))
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::SiRevRange {
      key,
      cursor,
      offset,
      limit,
    } => {
      let ids = db.si_range(key.as_bytes(), cursor, offset, limit, true)?;
      let arr = ids
        .into_iter()
        .map(|id| RespValue::Blob(format!("{id}").into_bytes()))
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::SiRangeByValue { key, spec } => {
      let ids = db.si_range_by_value(key.as_bytes(), &spec)?;
      let arr = ids
        .into_iter()
        .map(|id| RespValue::Blob(format!("{id}").into_bytes()))
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::SiRevRangeByValue { key, spec } => {
      let ids = db.si_range_by_value(key.as_bytes(), &spec)?;
      let arr = ids
        .into_iter()
        .map(|id| RespValue::Blob(format!("{id}").into_bytes()))
        .collect();
      Ok(RespValue::Arr(arr))
    }
    _ => Err(Error::internal("unsupported sortedint command")),
  }
}
