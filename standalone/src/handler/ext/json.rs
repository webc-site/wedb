use crate::handler::resp_util::{blob_str_or_null, bool_to_int};
use std::sync::Arc;
use webc_cmd::Cmd;
use wedb_embed::{Error, JsonSet, Result, WeDb};
use wedb_resp::RespValue;

/// 处理所有 RedisJSON 相关命令
pub async fn handle_json(db: &Arc<WeDb>, cmd: Cmd) -> Result<RespValue> {
  match cmd {
    Cmd::JsonSet {
      key,
      path,
      value,
      nx,
      xx,
    } => {
      let opt = if nx {
        Some(JsonSet::Nx)
      } else if xx {
        Some(JsonSet::Xx)
      } else {
        None
      };
      let ok = db.json_set_opt(key.as_bytes(), &path, &value, opt)?;
      if ok {
        Ok(RespValue::ok())
      } else {
        Ok(RespValue::Null)
      }
    }
    Cmd::JsonGet {
      key,
      paths,
      indent,
      newline,
      space,
    } => {
      let path_refs: Vec<&str> = paths.iter().map(String::as_str).collect();
      let json_str = db.json_get_formatted(
        key.as_bytes(),
        &path_refs,
        indent.as_deref(),
        newline.as_deref(),
        space.as_deref(),
      )?;
      Ok(blob_str_or_null(json_str))
    }
    Cmd::JsonDel { key, path } => {
      let count = db.json_del(key.as_bytes(), path.as_deref())?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::JsonType { key, path } => {
      let types = db.json_type(key.as_bytes(), path.as_deref())?;
      let arr = types
        .into_iter()
        .map(|t| RespValue::Blob(t.into_bytes()))
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::JsonArrAppend { key, path, values } => {
      let val_refs: Vec<&str> = values.iter().map(String::as_str).collect();
      let lens = db.json_arrappend(key.as_bytes(), path.as_deref().unwrap_or("$"), &val_refs)?;
      let arr = lens
        .into_iter()
        .map(|opt| match opt {
          Some(len) => RespValue::Int(len as i64),
          None => RespValue::Null,
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::JsonArrInsert {
      key,
      path,
      index,
      values,
    } => {
      let val_refs: Vec<&str> = values.iter().map(String::as_str).collect();
      let lens = db.json_arrinsert(key.as_bytes(), &path, index as isize, &val_refs)?;
      let arr = lens
        .into_iter()
        .map(|opt| match opt {
          Some(len) => RespValue::Int(len as i64),
          None => RespValue::Null,
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::JsonArrTrim {
      key,
      path,
      start,
      stop,
    } => {
      let lens = db.json_arrtrim(key.as_bytes(), &path, start as isize, stop as isize)?;
      let arr = lens
        .into_iter()
        .map(|opt| match opt {
          Some(len) => RespValue::Int(len as i64),
          None => RespValue::Null,
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::JsonClear { key, path } => {
      let count = db.json_clear(key.as_bytes(), path.as_deref())?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::JsonToggle { key, path } => {
      let bools = db.json_toggle(key.as_bytes(), path.as_deref())?;
      let arr = bools
        .into_iter()
        .map(|opt| match opt {
          Some(b) => RespValue::Int(if b { 1 } else { 0 }),
          None => RespValue::Null,
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::JsonArrLen { key, path } => {
      let lens = db.json_arrlen(key.as_bytes(), path.as_deref())?;
      let arr = lens
        .into_iter()
        .map(|opt| match opt {
          Some(len) => RespValue::Int(len as i64),
          None => RespValue::Null,
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::JsonMerge { key, path, value } => {
      let ok = db.json_merge(key.as_bytes(), &path, &value)?;
      Ok(bool_to_int(ok))
    }
    Cmd::JsonObjKeys { key, path } => {
      let keys_list = db.json_objkeys(key.as_bytes(), path.as_deref())?;
      let arr = keys_list
        .into_iter()
        .map(|opt| match opt {
          Some(keys) => RespValue::Arr(
            keys
              .into_iter()
              .map(|k| RespValue::Blob(k.into_bytes()))
              .collect(),
          ),
          None => RespValue::Null,
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::JsonArrPop { key, path, index } => {
      let pops = db.json_arrpop(key.as_bytes(), path.as_deref(), index.map(|i| i as isize))?;
      let arr = pops
        .into_iter()
        .map(|opt| match opt {
          Some(s) => RespValue::Blob(s.into_bytes()),
          None => RespValue::Null,
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::JsonArrIndex { key, path, value } => {
      let indices = db.json_arrindex(key.as_bytes(), &path, &value, 0, None)?;
      let arr = indices
        .into_iter()
        .map(|opt| match opt {
          Some(idx) => RespValue::Int(idx as i64),
          None => RespValue::Null,
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::JsonNumIncrBy { key, path, number } => {
      let num_str = format!("{number}");
      let res = db.json_numincrby(key.as_bytes(), &path, &num_str)?;
      Ok(blob_str_or_null(res))
    }
    Cmd::JsonNumMultBy { key, path, number } => {
      let num_str = format!("{number}");
      let res = db.json_nummultby(key.as_bytes(), &path, &num_str)?;
      Ok(blob_str_or_null(res))
    }
    Cmd::JsonObjLen { key, path } => {
      let lens = db.json_objlen(key.as_bytes(), path.as_deref())?;
      let arr = lens
        .into_iter()
        .map(|opt| match opt {
          Some(len) => RespValue::Int(len as i64),
          None => RespValue::Null,
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::JsonStrAppend { key, path, value } => {
      let lens = db.json_strappend(key.as_bytes(), path.as_deref(), &value)?;
      let arr = lens
        .into_iter()
        .map(|opt| match opt {
          Some(len) => RespValue::Int(len as i64),
          None => RespValue::Null,
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::JsonStrLen { key, path } => {
      let lens = db.json_strlen(key.as_bytes(), path.as_deref())?;
      let arr = lens
        .into_iter()
        .map(|opt| match opt {
          Some(len) => RespValue::Int(len as i64),
          None => RespValue::Null,
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::JsonMGet { keys, path } => {
      let k_refs: Vec<&[u8]> = keys.iter().map(String::as_bytes).collect();
      let results = db.json_mget(&k_refs, &path)?;
      let arr = results
        .into_iter()
        .map(|opt| match opt {
          Some(s) => RespValue::Blob(s.into_bytes()),
          None => RespValue::Null,
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::JsonMSet(triplets) => {
      let t_refs: Vec<(&[u8], &str, &str)> = triplets
        .iter()
        .map(|(k, p, v)| (k.as_bytes(), p.as_str(), v.as_str()))
        .collect();
      db.json_mset(&t_refs)?;
      Ok(RespValue::ok())
    }
    Cmd::JsonDebug(key, subcmd) => {
      let mem = db.json_debug_memory(key.as_bytes(), subcmd.as_deref())?;
      let sum_mem: usize = mem.into_iter().sum();
      Ok(RespValue::Int(sum_mem as i64))
    }
    Cmd::JsonResp { key, path } => {
      let s = db.json_get(key.as_bytes(), path.as_deref())?;
      match s {
        Some(raw) => {
          let v: sonic_rs::Value = sonic_rs::from_str(&raw)
            .map_err(|e| Error::invalid_data(format!("ERR JSON parse: {e}")))?;
          Ok(wedb_resp::json_to_resp(&v))
        }
        None => Ok(RespValue::Null),
      }
    }
    Cmd::JsonInfo(key) => {
      let info = db.json_info(key.as_bytes())?;
      match info {
        Some((format, size)) => Ok(RespValue::Arr(vec![
          RespValue::Simple("key_name".to_string()),
          RespValue::Blob(key.into_bytes()),
          RespValue::Simple("format".to_string()),
          RespValue::Simple(format!("{format:?}")),
          RespValue::Simple("size".to_string()),
          RespValue::Int(size as i64),
        ])),
        None => Ok(RespValue::Null),
      }
    }
    _ => Err(Error::internal("unsupported json command")),
  }
}
