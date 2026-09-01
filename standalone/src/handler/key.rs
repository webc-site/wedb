use rapidhash::RapidHashSet as HashSet;
use std::str;
use std::sync::Arc;
use webc_cmd::Cmd;
use wedb_embed::{Error, Result, WeDb, current_now_ms, matches_glob_bytes};
use wedb_resp::RespValue;

/// 处理所有键 (Key) 生命周期与排序命令
pub async fn handle_key(db: &Arc<WeDb>, cmd: Cmd) -> Result<RespValue> {
  match cmd {
    Cmd::Del(keys) | Cmd::Unlink(keys) => {
      let k_refs: Vec<&[u8]> = keys.iter().map(String::as_bytes).collect();
      let count = db.del(&k_refs)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::Exists(keys) => {
      let k_refs: Vec<&[u8]> = keys.iter().map(String::as_bytes).collect();
      let count = db.exists(&k_refs)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::FlushAll | Cmd::FlushDb => {
      db.flushall()?;
      Ok(RespValue::ok())
    }
    Cmd::Type(key) => {
      let k_type = db.key_type(key.as_bytes())?;
      Ok(RespValue::Simple(k_type.to_string()))
    }
    Cmd::Ttl(key) => {
      let ttl = db.key_ttl(key.as_bytes())?;
      Ok(RespValue::Int(ttl))
    }
    Cmd::Pttl(key) => {
      let pttl = db.key_pttl(key.as_bytes())?;
      Ok(RespValue::Int(pttl))
    }
    Cmd::ExpireTime(key) => match db.get_key_expire_at(key.as_bytes())? {
      Some(0) => Ok(RespValue::Int(-1)),
      Some(exp) => Ok(RespValue::Int((exp / 1000) as i64)),
      None => Ok(RespValue::Int(-2)),
    },
    Cmd::PExpireTime(key) => match db.get_key_expire_at(key.as_bytes())? {
      Some(0) => Ok(RespValue::Int(-1)),
      Some(exp) => Ok(RespValue::Int(exp as i64)),
      None => Ok(RespValue::Int(-2)),
    },
    Cmd::Expire(key, sec) => {
      let expire_at_ms = current_now_ms().saturating_add(sec.saturating_mul(1000));
      let ok = db.set_key_expire_at(key.as_bytes(), expire_at_ms)?;
      Ok(RespValue::Int(if ok { 1 } else { 0 }))
    }
    Cmd::PExpire(key, ms) => {
      let expire_at_ms = current_now_ms().saturating_add(ms);
      let ok = db.set_key_expire_at(key.as_bytes(), expire_at_ms)?;
      Ok(RespValue::Int(if ok { 1 } else { 0 }))
    }
    Cmd::ExpireAt(key, ts_sec) => {
      let expire_at_ms = ts_sec.saturating_mul(1000);
      let ok = db.set_key_expire_at(key.as_bytes(), expire_at_ms)?;
      Ok(RespValue::Int(if ok { 1 } else { 0 }))
    }
    Cmd::PExpireAt(key, ts_ms) => {
      let ok = db.set_key_expire_at(key.as_bytes(), ts_ms)?;
      Ok(RespValue::Int(if ok { 1 } else { 0 }))
    }
    Cmd::Persist(key) => {
      let ok = db.key_persist(key.as_bytes())?;
      Ok(RespValue::Int(if ok { 1 } else { 0 }))
    }
    Cmd::Keys(pattern) => {
      let pat_bytes = pattern.as_bytes();
      let mut results = Vec::new();
      let mut seen = HashSet::default();
      for item in db.data.iter() {
        let k = item.key()?;
        if matches_glob_bytes(pat_bytes, &k) && seen.insert(k.to_vec()) {
          results.push(RespValue::Blob(k.to_vec()));
        }
      }
      Ok(RespValue::Arr(results))
    }
    Cmd::Scan {
      cursor,
      pattern,
      count,
    } => {
      let pat = pattern.unwrap_or_else(|| "*".to_string());
      let pat_bytes = pat.as_bytes();
      let limit = count.unwrap_or(10);

      let mut matched = Vec::new();
      let mut seen = HashSet::default();
      for item in db.data.iter() {
        let k = item.key()?;
        if matches_glob_bytes(pat_bytes, &k) && seen.insert(k.to_vec()) {
          matched.push(RespValue::Blob(k.to_vec()));
        }
      }

      let start = cursor as usize;
      let end = (start + limit).min(matched.len());
      let next_cursor = if end >= matched.len() { 0 } else { end as u64 };
      let slice = if start < matched.len() {
        matched[start..end].to_vec()
      } else {
        Vec::new()
      };

      Ok(RespValue::Arr(vec![
        RespValue::Blob(next_cursor.to_string().into_bytes()),
        RespValue::Arr(slice),
      ]))
    }
    Cmd::ScanPrefix { prefix, count } => {
      let limit = count.unwrap_or(10);
      let mut results = Vec::with_capacity(limit);
      for item in db.data.prefix(&prefix) {
        let k = item.key()?;
        results.push(RespValue::Blob(k.to_vec()));
        if results.len() >= limit {
          break;
        }
      }
      Ok(RespValue::Arr(results))
    }
    Cmd::DbSize => {
      let mut count = 0usize;
      for _ in db.data.iter() {
        count += 1;
      }
      Ok(RespValue::Int(count as i64))
    }
    Cmd::Rename(src, dst) => {
      if let Some(val) = db.get(src.as_bytes())? {
        db.set(dst.as_bytes(), &val, &[])?;
        db.del(&[src.as_bytes()])?;
        Ok(RespValue::ok())
      } else {
        Err(Error::not_found("ERR no such key"))
      }
    }
    Cmd::RenameNx(src, dst) => {
      if db.exists(&[dst.as_bytes()])? > 0 {
        return Ok(RespValue::Int(0));
      }
      if let Some(val) = db.get(src.as_bytes())? {
        db.set(dst.as_bytes(), &val, &[])?;
        db.del(&[src.as_bytes()])?;
        Ok(RespValue::Int(1))
      } else {
        Err(Error::not_found("ERR no such key"))
      }
    }
    Cmd::Copy {
      src, dst, replace, ..
    } => {
      if !replace && db.exists(&[dst.as_bytes()])? > 0 {
        return Ok(RespValue::Int(0));
      }
      if let Some(val) = db.get(src.as_bytes())? {
        db.set(dst.as_bytes(), &val, &[])?;
        Ok(RespValue::Int(1))
      } else {
        Ok(RespValue::Int(0))
      }
    }
    Cmd::Touch(keys) => {
      let k_refs: Vec<&[u8]> = keys.iter().map(String::as_bytes).collect();
      let count = db.exists(&k_refs)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::RandomKey => {
      if let Some(item) = db.data.iter().next() {
        let k = item.key()?;
        return Ok(RespValue::Blob(k.to_vec()));
      }
      Ok(RespValue::Null)
    }
    Cmd::Object { subcmd, .. } => match subcmd.to_ascii_uppercase().as_str() {
      "ENCODING" => Ok(RespValue::Simple("raw".to_string())),
      "IDLETIME" => Ok(RespValue::Int(0)),
      "REFCOUNT" => Ok(RespValue::Int(1)),
      "FREQ" => Ok(RespValue::Int(0)),
      _ => Ok(RespValue::Null),
    },
    Cmd::KMetaData(key) => {
      let exists = db.exists(&[key.as_bytes()])?;
      if exists > 0 {
        Ok(RespValue::Arr(vec![
          RespValue::Simple("type".to_string()),
          RespValue::Simple("string".to_string()),
          RespValue::Simple("size".to_string()),
          RespValue::Int(1),
        ]))
      } else {
        Ok(RespValue::Null)
      }
    }
    Cmd::Sort {
      key,
      offset,
      count,
      desc,
      alpha,
      ..
    }
    | Cmd::SortRo {
      key,
      offset,
      count,
      desc,
      alpha,
      ..
    } => {
      let mut elements: Vec<Vec<u8>> = db.smembers(key.as_bytes()).unwrap_or_default();
      if elements.is_empty() {
        elements = db.lrange(key.as_bytes(), 0, -1).unwrap_or_default();
      }
      if alpha {
        elements.sort();
      } else {
        elements.sort_by(|a, b| {
          let fa = str::from_utf8(a)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
          let fb = str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
          fa.total_cmp(&fb)
        });
      }
      if desc {
        elements.reverse();
      }
      let sliced: Vec<RespValue> = elements
        .into_iter()
        .skip(offset)
        .take(count.unwrap_or(usize::MAX))
        .map(RespValue::Blob)
        .collect();
      Ok(RespValue::Arr(sliced))
    }
    _ => Err(Error::invalid_data(
      "ERR unknown or unsupported generic key command",
    )),
  }
}
