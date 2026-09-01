use std::sync::Arc;
use webc_cmd::Cmd;
use wedb_embed::{Error, PosSpec, Result, WeDb};
use wedb_resp::RespValue;

/// 处理所有列表 (List) 命令
pub async fn handle_list(db: &Arc<WeDb>, cmd: Cmd) -> Result<RespValue> {
  match cmd {
    Cmd::LPush(key, elements) => {
      let e: Vec<&[u8]> = elements.iter().map(Vec::as_slice).collect();
      let len = db.lpush(key.as_bytes(), &e)?;
      Ok(RespValue::Int(len as i64))
    }
    Cmd::RPush(key, elements) => {
      let e: Vec<&[u8]> = elements.iter().map(Vec::as_slice).collect();
      let len = db.rpush(key.as_bytes(), &e)?;
      Ok(RespValue::Int(len as i64))
    }
    Cmd::LPushX(key, elements) => {
      let e: Vec<&[u8]> = elements.iter().map(Vec::as_slice).collect();
      let len = db.lpushx(key.as_bytes(), &e)?;
      Ok(RespValue::Int(len as i64))
    }
    Cmd::RPushX(key, elements) => {
      let e: Vec<&[u8]> = elements.iter().map(Vec::as_slice).collect();
      let len = db.rpushx(key.as_bytes(), &e)?;
      Ok(RespValue::Int(len as i64))
    }
    Cmd::LPop(key, count) => {
      let c = count.unwrap_or(1);
      let popped = db.lpop(key.as_bytes(), c)?;
      if count.is_none() {
        match popped.into_iter().next() {
          Some(v) => Ok(RespValue::Blob(v)),
          None => Ok(RespValue::Null),
        }
      } else {
        Ok(RespValue::Arr(
          popped.into_iter().map(RespValue::Blob).collect(),
        ))
      }
    }
    Cmd::RPop(key, count) => {
      let c = count.unwrap_or(1);
      let popped = db.rpop(key.as_bytes(), c)?;
      if count.is_none() {
        match popped.into_iter().next() {
          Some(v) => Ok(RespValue::Blob(v)),
          None => Ok(RespValue::Null),
        }
      } else {
        Ok(RespValue::Arr(
          popped.into_iter().map(RespValue::Blob).collect(),
        ))
      }
    }
    Cmd::LLen(key) => {
      let len = db.llen(key.as_bytes())?;
      Ok(RespValue::Int(len as i64))
    }
    Cmd::LRange(key, start, stop) => {
      let items = db.lrange(key.as_bytes(), start, stop)?;
      Ok(RespValue::Arr(
        items.into_iter().map(RespValue::Blob).collect(),
      ))
    }
    Cmd::LIndex(key, idx) => match db.lindex(key.as_bytes(), idx)? {
      Some(v) => Ok(RespValue::Blob(v)),
      None => Ok(RespValue::Null),
    },
    Cmd::LSet(key, index, value) => {
      db.lset(key.as_bytes(), index, &value)?;
      Ok(RespValue::ok())
    }
    Cmd::LTrim(key, start, stop) => {
      db.ltrim(key.as_bytes(), start, stop)?;
      Ok(RespValue::ok())
    }
    Cmd::LRem(key, count, element) => {
      let removed = db.lrem(key.as_bytes(), count, &element)?;
      Ok(RespValue::Int(removed as i64))
    }
    Cmd::LInsert {
      key,
      before,
      pivot,
      element,
    } => {
      let len = db.linsert(key.as_bytes(), before, &pivot, &element)?;
      Ok(RespValue::Int(len))
    }
    Cmd::LMove {
      src,
      dst,
      src_left,
      dst_left,
    } => match db.lmove(src.as_bytes(), dst.as_bytes(), src_left, dst_left)? {
      Some(v) => Ok(RespValue::Blob(v)),
      None => Ok(RespValue::Null),
    },
    Cmd::LMoveM {
      src,
      dst,
      src_left,
      dst_left,
      count,
      exactly,
    } => {
      let c = exactly.or(count).unwrap_or(1);
      let items = if src_left {
        db.lpop(src.as_bytes(), c)?
      } else {
        db.rpop(src.as_bytes(), c)?
      };
      if items.is_empty() {
        return Ok(RespValue::Null);
      }
      let e: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
      if dst_left {
        db.lpush(dst.as_bytes(), &e)?;
      } else {
        db.rpush(dst.as_bytes(), &e)?;
      }
      Ok(RespValue::Arr(
        items.into_iter().map(RespValue::Blob).collect(),
      ))
    }
    Cmd::RPopLPush(src, dst) => match db.rpoplpush(src.as_bytes(), dst.as_bytes())? {
      Some(v) => Ok(RespValue::Blob(v)),
      None => Ok(RespValue::Null),
    },
    Cmd::LPos {
      key,
      element,
      rank,
      count,
      max_len,
    } => {
      let mut spec = PosSpec::default();
      if let Some(r) = rank {
        spec.rank = r;
      }
      spec.count = count;
      spec.max_len = max_len;
      let positions = db.lpos(key.as_bytes(), &element, spec)?;
      if count.is_none() {
        match positions.into_iter().next() {
          Some(pos) => Ok(RespValue::Int(pos)),
          None => Ok(RespValue::Null),
        }
      } else {
        Ok(RespValue::Arr(
          positions.into_iter().map(RespValue::Int).collect(),
        ))
      }
    }
    Cmd::BLPop(keys, _) => {
      for k in &keys {
        let popped = db.lpop(k.as_bytes(), 1)?;
        if let Some(v) = popped.into_iter().next() {
          return Ok(RespValue::Arr(vec![
            RespValue::Blob(k.clone().into_bytes()),
            RespValue::Blob(v),
          ]));
        }
      }
      Ok(RespValue::Null)
    }
    Cmd::BRPop(keys, _) => {
      for k in &keys {
        let popped = db.rpop(k.as_bytes(), 1)?;
        if let Some(v) = popped.into_iter().next() {
          return Ok(RespValue::Arr(vec![
            RespValue::Blob(k.clone().into_bytes()),
            RespValue::Blob(v),
          ]));
        }
      }
      Ok(RespValue::Null)
    }
    Cmd::BLMove {
      src,
      dst,
      src_left,
      dst_left,
      ..
    } => match db.lmove(src.as_bytes(), dst.as_bytes(), src_left, dst_left)? {
      Some(v) => Ok(RespValue::Blob(v)),
      None => Ok(RespValue::Null),
    },
    Cmd::BLMoveM {
      src,
      dst,
      src_left,
      dst_left,
      count,
      exactly,
      ..
    } => {
      let c = exactly.or(count).unwrap_or(1);
      let items = if src_left {
        db.lpop(src.as_bytes(), c)?
      } else {
        db.rpop(src.as_bytes(), c)?
      };
      if items.is_empty() {
        return Ok(RespValue::Null);
      }
      let e: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
      if dst_left {
        db.lpush(dst.as_bytes(), &e)?;
      } else {
        db.rpush(dst.as_bytes(), &e)?;
      }
      Ok(RespValue::Arr(
        items.into_iter().map(RespValue::Blob).collect(),
      ))
    }
    Cmd::LMPop { keys, left, count } => {
      for k in &keys {
        let popped = if left {
          db.lpop(k.as_bytes(), count)?
        } else {
          db.rpop(k.as_bytes(), count)?
        };
        if !popped.is_empty() {
          return Ok(RespValue::Arr(vec![
            RespValue::Blob(k.clone().into_bytes()),
            RespValue::Arr(popped.into_iter().map(RespValue::Blob).collect()),
          ]));
        }
      }
      Ok(RespValue::Null)
    }
    Cmd::BLMPop {
      keys, left, count, ..
    } => {
      for k in &keys {
        let popped = if left {
          db.lpop(k.as_bytes(), count)?
        } else {
          db.rpop(k.as_bytes(), count)?
        };
        if !popped.is_empty() {
          return Ok(RespValue::Arr(vec![
            RespValue::Blob(k.clone().into_bytes()),
            RespValue::Arr(popped.into_iter().map(RespValue::Blob).collect()),
          ]));
        }
      }
      Ok(RespValue::Null)
    }
    _ => Err(Error::internal("unsupported list command")),
  }
}
