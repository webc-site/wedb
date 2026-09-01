use crate::handler::resp_util::{blobs_opt_to_arr, float_to_blob};
use std::sync::Arc;
use webc_cmd::{Cmd, ExpireCondition};
use wedb_embed::{Error, HExpire, RangeLexSpec, Result, WeDb};
use wedb_resp::RespValue;

/// 处理所有哈希表 (Hash) 命令
pub async fn handle_hash(db: &Arc<WeDb>, cmd: Cmd) -> Result<RespValue> {
  match cmd {
    Cmd::HSet(key, pairs) | Cmd::HMSet(key, pairs) => {
      let p: Vec<(&[u8], &[u8])> = pairs
        .iter()
        .map(|(k, v)| (k.as_bytes(), v.as_slice()))
        .collect();
      let count = db.hset(key.as_bytes(), &p)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::HSetNx(key, field, value) => {
      let added = db.hsetnx(key.as_bytes(), field.as_bytes(), &value)?;
      Ok(RespValue::Int(if added { 1 } else { 0 }))
    }
    Cmd::HGet(key, field) => match db.hget(key.as_bytes(), field.as_bytes())? {
      Some(v) => Ok(RespValue::Blob(v)),
      None => Ok(RespValue::Null),
    },
    Cmd::HMGet(key, fields) => {
      let f: Vec<&[u8]> = fields.iter().map(|f| f.as_bytes()).collect();
      let vals = db.hmget(key.as_bytes(), &f)?;
      Ok(blobs_opt_to_arr(vals))
    }
    Cmd::HGetAll(key) => {
      let pairs = db.hgetall(key.as_bytes())?;
      let mut res = Vec::with_capacity(pairs.len() * 2);
      for (k, v) in pairs {
        res.push(RespValue::Blob(k));
        res.push(RespValue::Blob(v));
      }
      Ok(RespValue::Arr(res))
    }
    Cmd::HKeys(key) => {
      let keys = db.hkeys(key.as_bytes())?;
      Ok(RespValue::Arr(
        keys.into_iter().map(RespValue::Blob).collect(),
      ))
    }
    Cmd::HVals(key) => {
      let vals = db.hvals(key.as_bytes())?;
      Ok(RespValue::Arr(
        vals.into_iter().map(RespValue::Blob).collect(),
      ))
    }
    Cmd::HDel(key, fields) => {
      let f: Vec<&[u8]> = fields.iter().map(|f| f.as_bytes()).collect();
      let count = db.hdel(key.as_bytes(), &f)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::HGetDel { key, fields } => {
      let f: Vec<&[u8]> = fields.iter().map(|f| f.as_bytes()).collect();
      let vals = db.hgetdel(key.as_bytes(), &f)?;
      if fields.len() == 1 {
        match vals.into_iter().next().flatten() {
          Some(v) => Ok(RespValue::Blob(v)),
          None => Ok(RespValue::Null),
        }
      } else {
        Ok(blobs_opt_to_arr(vals))
      }
    }
    Cmd::HLen(key) => {
      let len = db.hlen(key.as_bytes())?;
      Ok(RespValue::Int(len as i64))
    }
    Cmd::HExists(key, field) => {
      let exists = db.hexists(key.as_bytes(), field.as_bytes())?;
      Ok(RespValue::Int(if exists { 1 } else { 0 }))
    }
    Cmd::HIncrBy(key, field, delta) => {
      let val = db.hincrby(key.as_bytes(), field.as_bytes(), delta)?;
      Ok(RespValue::Int(val))
    }
    Cmd::HIncrByFloat(key, field, delta) => {
      let val = db.hincrbyfloat(key.as_bytes(), field.as_bytes(), delta)?;
      Ok(float_to_blob(val))
    }
    Cmd::HStrLen(key, field) => {
      let len = db.hstrlen(key.as_bytes(), field.as_bytes())?;
      Ok(RespValue::Int(len as i64))
    }
    Cmd::HRandField {
      key,
      count,
      with_values,
    } => {
      let c = count.unwrap_or(1);
      let pairs = db.hrandfield(key.as_bytes(), c, with_values)?;
      if count.is_none() {
        match pairs.into_iter().next() {
          Some((f, _)) => Ok(RespValue::Blob(f)),
          None => Ok(RespValue::Null),
        }
      } else if with_values {
        let mut res = Vec::with_capacity(pairs.len() * 2);
        for (f, v) in pairs {
          res.push(RespValue::Blob(f));
          if let Some(val) = v {
            res.push(RespValue::Blob(val));
          } else {
            res.push(RespValue::Null);
          }
        }
        Ok(RespValue::Arr(res))
      } else {
        let res = pairs.into_iter().map(|(f, _)| RespValue::Blob(f)).collect();
        Ok(RespValue::Arr(res))
      }
    }
    Cmd::HExpire {
      key,
      seconds,
      condition,
      fields,
    } => {
      let f: Vec<&[u8]> = fields.iter().map(|s| s.as_bytes()).collect();
      let opt = to_hexpire(condition);
      let results = db.hexpire(key.as_bytes(), &f, seconds, opt)?;
      Ok(RespValue::Arr(
        results.into_iter().map(RespValue::Int).collect(),
      ))
    }
    Cmd::HPExpire {
      key,
      millis,
      condition,
      fields,
    } => {
      let f: Vec<&[u8]> = fields.iter().map(|s| s.as_bytes()).collect();
      let opt = to_hexpire(condition);
      let results = db.hpexpire(key.as_bytes(), &f, millis, opt)?;
      Ok(RespValue::Arr(
        results.into_iter().map(RespValue::Int).collect(),
      ))
    }
    Cmd::HExpireAt {
      key,
      unix_time_sec,
      condition,
      fields,
    } => {
      let f: Vec<&[u8]> = fields.iter().map(|s| s.as_bytes()).collect();
      let opt = to_hexpire(condition);
      let results = db.hexpireat(key.as_bytes(), &f, unix_time_sec as u64, opt)?;
      Ok(RespValue::Arr(
        results.into_iter().map(RespValue::Int).collect(),
      ))
    }
    Cmd::HPExpireAt {
      key,
      unix_time_ms,
      condition,
      fields,
    } => {
      let f: Vec<&[u8]> = fields.iter().map(|s| s.as_bytes()).collect();
      let opt = to_hexpire(condition);
      let results = db.hpexpireat(key.as_bytes(), &f, unix_time_ms as u64, opt)?;
      Ok(RespValue::Arr(
        results.into_iter().map(RespValue::Int).collect(),
      ))
    }
    Cmd::HTtl { key, fields } => {
      let f: Vec<&[u8]> = fields.iter().map(|s| s.as_bytes()).collect();
      let ttls = db.httl(key.as_bytes(), &f)?;
      Ok(RespValue::Arr(
        ttls.into_iter().map(RespValue::Int).collect(),
      ))
    }
    Cmd::HPTtl { key, fields } => {
      let f: Vec<&[u8]> = fields.iter().map(|s| s.as_bytes()).collect();
      let ttls = db.hpttl(key.as_bytes(), &f)?;
      Ok(RespValue::Arr(
        ttls.into_iter().map(RespValue::Int).collect(),
      ))
    }
    Cmd::HExpireTime { key, fields } => {
      let f: Vec<&[u8]> = fields.iter().map(|s| s.as_bytes()).collect();
      let times = db.hexpiretime(key.as_bytes(), &f)?;
      Ok(RespValue::Arr(
        times.into_iter().map(RespValue::Int).collect(),
      ))
    }
    Cmd::HPExpireTime { key, fields } => {
      let f: Vec<&[u8]> = fields.iter().map(|s| s.as_bytes()).collect();
      let times = db.hpexpiretime(key.as_bytes(), &f)?;
      Ok(RespValue::Arr(
        times.into_iter().map(RespValue::Int).collect(),
      ))
    }
    Cmd::HPersist { key, fields } => {
      let f: Vec<&[u8]> = fields.iter().map(|s| s.as_bytes()).collect();
      let results = db.hpersist(key.as_bytes(), &f)?;
      Ok(RespValue::Arr(
        results.into_iter().map(RespValue::Int).collect(),
      ))
    }
    Cmd::HSetExpire {
      key,
      ttl_sec,
      fields,
    } => {
      let f: Vec<&[u8]> = fields.iter().map(|s| s.as_bytes()).collect();
      let results = db.hexpire(key.as_bytes(), &f, ttl_sec as i64, HExpire::None)?;
      Ok(RespValue::Arr(
        results.into_iter().map(RespValue::Int).collect(),
      ))
    }
    Cmd::HGetEx { key, field } => match db.hget(key.as_bytes(), field.as_bytes())? {
      Some(v) => Ok(RespValue::Blob(v)),
      None => Ok(RespValue::Null),
    },
    Cmd::HRangeByLex {
      key,
      min,
      max,
      offset,
      count,
    } => {
      let spec = RangeLexSpec::from_bounds(min.as_bytes(), max.as_bytes(), offset, count)?;
      let pairs = db.hrangebylex(key.as_bytes(), spec)?;
      let mut res = Vec::with_capacity(pairs.len() * 2);
      for (f, v) in pairs {
        res.push(RespValue::Blob(f));
        res.push(RespValue::Blob(v));
      }
      Ok(RespValue::Arr(res))
    }
    Cmd::HScan {
      key,
      cursor,
      pattern,
      count,
    } => {
      let (next_cursor, pairs) = db.hscan(
        key.as_bytes(),
        cursor as usize,
        count.unwrap_or(10),
        pattern.as_deref().map(str::as_bytes),
      )?;
      let mut entries = Vec::with_capacity(pairs.len() * 2);
      for (f, v) in pairs {
        entries.push(RespValue::Blob(f));
        entries.push(RespValue::Blob(v));
      }
      Ok(RespValue::Arr(vec![
        RespValue::Blob(next_cursor.to_string().into_bytes()),
        RespValue::Arr(entries),
      ]))
    }
    _ => Err(Error::internal("unsupported hash command")),
  }
}

#[inline]
fn to_hexpire(cond: ExpireCondition) -> HExpire {
  match cond {
    ExpireCondition::None => HExpire::None,
    ExpireCondition::NX => HExpire::Nx,
    ExpireCondition::XX => HExpire::Xx,
    ExpireCondition::GT => HExpire::Gt,
    ExpireCondition::LT => HExpire::Lt,
  }
}
