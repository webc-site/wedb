use std::sync::Arc;
use webc_cmd::Cmd;
use wedb_embed::{Error, Result, WeDb};
use wedb_resp::RespValue;

pub async fn handle_set(db: &Arc<WeDb>, cmd: Cmd) -> Result<RespValue> {
  match cmd {
    Cmd::SAdd(key, members) => {
      let m: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
      let count = db.sadd(key.as_bytes(), &m)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::SRem(key, members) => {
      let m: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
      let count = db.srem(key.as_bytes(), &m)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::SCard(key) => {
      let count = db.scard(key.as_bytes())?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::SMembers(key) => {
      let members = db.smembers(key.as_bytes())?;
      Ok(RespValue::Arr(
        members.into_iter().map(RespValue::Blob).collect(),
      ))
    }
    Cmd::SIsMember(key, member) => {
      let exists = db.sismember(key.as_bytes(), &member)?;
      Ok(RespValue::Int(if exists { 1 } else { 0 }))
    }
    Cmd::SMIsMember(key, members) => {
      let m: Vec<&[u8]> = members.iter().map(|m| m.as_slice()).collect();
      let exists = db.smismember(key.as_bytes(), &m)?;
      Ok(RespValue::Arr(
        exists
          .into_iter()
          .map(|b| RespValue::Int(if b { 1 } else { 0 }))
          .collect(),
      ))
    }
    Cmd::SPop(key, count) => match count {
      Some(cnt) => {
        let popped = db.spop(key.as_bytes(), cnt)?;
        Ok(RespValue::Arr(
          popped.into_iter().map(RespValue::Blob).collect(),
        ))
      }
      None => {
        let popped = db.spop(key.as_bytes(), 1)?;
        match popped.into_iter().next() {
          Some(val) => Ok(RespValue::Blob(val)),
          None => Ok(RespValue::Null),
        }
      }
    },
    Cmd::SRandMember(key, count) => match count {
      Some(cnt) => {
        let res = db.srandmember(key.as_bytes(), cnt)?;
        Ok(RespValue::Arr(
          res.into_iter().map(RespValue::Blob).collect(),
        ))
      }
      None => {
        let res = db.srandmember(key.as_bytes(), 1)?;
        match res.into_iter().next() {
          Some(val) => Ok(RespValue::Blob(val)),
          None => Ok(RespValue::Null),
        }
      }
    },
    Cmd::SMove { src, dst, member } => {
      let moved = db.smove(src.as_bytes(), dst.as_bytes(), &member)?;
      Ok(RespValue::Int(if moved { 1 } else { 0 }))
    }
    Cmd::SDiff(keys) => {
      let k: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
      let res = db.sdiff(&k)?;
      Ok(RespValue::Arr(
        res.into_iter().map(RespValue::Blob).collect(),
      ))
    }
    Cmd::SUnion(keys) => {
      let k: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
      let res = db.sunion(&k)?;
      Ok(RespValue::Arr(
        res.into_iter().map(RespValue::Blob).collect(),
      ))
    }
    Cmd::SInter(keys) => {
      let k: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
      let res = db.sinter(&k)?;
      Ok(RespValue::Arr(
        res.into_iter().map(RespValue::Blob).collect(),
      ))
    }
    Cmd::SInterCard { keys, limit } => {
      let k: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
      let count = db.sintercard(&k, limit)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::SDiffCard { keys, limit } => {
      let k: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
      let count = db.sdiffcard(&k, limit)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::SUnionCard { keys, limit } => {
      let k: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
      let count = db.sunioncard(&k, limit)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::SDiffStore(dst, keys) => {
      let k: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
      let count = db.sdiffstore(dst.as_bytes(), &k)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::SUnionStore(dst, keys) => {
      let k: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
      let count = db.sunionstore(dst.as_bytes(), &k)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::SInterStore(dst, keys) => {
      let k: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
      let count = db.sinterstore(dst.as_bytes(), &k)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::SScan {
      key,
      cursor,
      pattern,
      count,
    } => {
      let pat_bytes = pattern.as_deref().map(|p| p.as_bytes());
      let (next_cursor, items) = db.sscan(key.as_bytes(), cursor, pat_bytes, count)?;
      Ok(RespValue::Arr(vec![
        RespValue::Blob(format!("{next_cursor}").into_bytes()),
        RespValue::Arr(items.into_iter().map(RespValue::Blob).collect()),
      ]))
    }
    _ => Err(Error::internal("unsupported set command")),
  }
}
