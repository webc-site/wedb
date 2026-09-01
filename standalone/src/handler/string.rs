use std::sync::Arc;
use webc_cmd::Cmd;
use wedb_embed::{
  DelEx, Error, GetEx, Result, StringLCSArgs, StringLCSType, StringSetArgs, StringSetType, WeDb,
  current_now_ms,
};
use wedb_resp::RespValue;

pub async fn handle_string(db: &Arc<WeDb>, cmd: Cmd) -> Result<RespValue> {
  match cmd {
    Cmd::Get(key) => match db.get(key.as_bytes())? {
      Some(v) => Ok(RespValue::Blob(v)),
      None => Ok(RespValue::Null),
    },
    Cmd::Set {
      key,
      value,
      ex,
      px,
      exat,
      pxat,
      nx,
      xx,
      keepttl,
      get,
    } => {
      // 极速直写通道：常规 SET key value (无复杂条件/TTL/GET)
      if !nx
        && !xx
        && !get
        && !keepttl
        && ex.is_none()
        && px.is_none()
        && exat.is_none()
        && pxat.is_none()
      {
        db.set_args(key.as_bytes(), &value, &StringSetArgs::default())?;
        return Ok(RespValue::ok());
      }

      let now_ms = current_now_ms();
      let expire = if let Some(sec) = ex {
        now_ms.saturating_add(sec.saturating_mul(1000))
      } else if let Some(ms) = px {
        now_ms.saturating_add(ms)
      } else if let Some(ts) = exat {
        ts.saturating_mul(1000)
      } else {
        pxat.unwrap_or_default()
      };

      let set_type = if nx {
        StringSetType::Nx
      } else if xx {
        StringSetType::Xx
      } else {
        StringSetType::None
      };

      let args = StringSetArgs {
        expire,
        set_type,
        get,
        keep_ttl: keepttl,
        cmp_value: None,
      };

      let prev = db.set_args(key.as_bytes(), &value, &args)?;
      if get {
        match prev {
          Some(v) => Ok(RespValue::Blob(v)),
          None => Ok(RespValue::Null),
        }
      } else {
        match prev {
          Some(_) => Ok(RespValue::ok()),
          None => Ok(RespValue::Null),
        }
      }
    }
    Cmd::SetNx(key, value) => {
      let ok = db.setnx(key.as_bytes(), &value, 0)?;
      Ok(RespValue::Int(if ok { 1 } else { 0 }))
    }
    Cmd::SetEx(key, ttl, value) => {
      let expire_ms = current_now_ms().saturating_add(ttl.saturating_mul(1000));
      db.setex(key.as_bytes(), &value, expire_ms)?;
      Ok(RespValue::ok())
    }
    Cmd::PSetEx(key, ttl_ms, value) => {
      let expire_ms = current_now_ms().saturating_add(ttl_ms);
      db.setex(key.as_bytes(), &value, expire_ms)?;
      Ok(RespValue::ok())
    }
    Cmd::GetSet(key, value) => match db.getset(key.as_bytes(), &value)? {
      Some(v) => Ok(RespValue::Blob(v)),
      None => Ok(RespValue::Null),
    },
    Cmd::GetDel(key) => match db.getdel(key.as_bytes())? {
      Some(v) => Ok(RespValue::Blob(v)),
      None => Ok(RespValue::Null),
    },
    Cmd::GetEx {
      key,
      ex,
      px,
      persist,
    } => {
      let opt = if persist {
        Some(GetEx::Persist)
      } else if let Some(sec) = ex {
        Some(GetEx::Ex(sec))
      } else {
        px.map(GetEx::Px)
      };
      match db.getex(key.as_bytes(), opt)? {
        Some(v) => Ok(RespValue::Blob(v)),
        None => Ok(RespValue::Null),
      }
    }
    Cmd::MGet(keys) => {
      let key_slices: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
      let raw_res = db.mget(&key_slices)?;
      let res = raw_res
        .into_iter()
        .map(|v| match v {
          Some(b) => RespValue::Blob(b),
          None => RespValue::Null,
        })
        .collect();
      Ok(RespValue::Arr(res))
    }
    Cmd::MSet(pairs) => {
      let p: Vec<(&[u8], &[u8])> = pairs
        .iter()
        .map(|(k, v)| (k.as_bytes(), v.as_slice()))
        .collect();
      db.mset(&p)?;
      Ok(RespValue::ok())
    }
    Cmd::MSetNx(pairs) => {
      let p: Vec<(&[u8], &[u8])> = pairs
        .iter()
        .map(|(k, v)| (k.as_bytes(), v.as_slice()))
        .collect();
      let ok = db.msetnx(&p)?;
      Ok(RespValue::Int(if ok { 1 } else { 0 }))
    }
    Cmd::MSetEx { ttl_sec, pairs } => {
      let p: Vec<(&[u8], &[u8])> = pairs
        .iter()
        .map(|(k, v)| (k.as_bytes(), v.as_slice()))
        .collect();
      let expire_ms = current_now_ms().saturating_add(ttl_sec.saturating_mul(1000));
      db.msetex(&p, expire_ms)?;
      Ok(RespValue::ok())
    }
    Cmd::Incr(key) => {
      let n = db.incrby(key.as_bytes(), 1)?;
      Ok(RespValue::Int(n))
    }
    Cmd::Decr(key) => {
      let n = db.decrby(key.as_bytes(), 1)?;
      Ok(RespValue::Int(n))
    }
    Cmd::IncrBy(key, delta) => {
      let n = db.incrby(key.as_bytes(), delta)?;
      Ok(RespValue::Int(n))
    }
    Cmd::DecrBy(key, delta) => {
      let n = db.decrby(key.as_bytes(), delta)?;
      Ok(RespValue::Int(n))
    }
    Cmd::IncrByFloat(key, delta) => {
      let f = db.incrbyfloat(key.as_bytes(), delta)?;
      Ok(RespValue::Blob(format!("{f}").into_bytes()))
    }
    Cmd::IncrEx {
      key,
      by_float,
      by_int,
      saturate,
      lbound,
      ubound,
      ..
    } => {
      if let Some(delta_f) = by_float {
        let cur = db
          .get(key.as_bytes())?
          .and_then(|v| String::from_utf8(v).ok())
          .and_then(|s| s.parse::<f64>().ok())
          .unwrap_or(0.0);
        let mut target = cur + delta_f;
        if let Some(lb) = lbound
          && target < lb
          && saturate
        {
          target = lb;
        }
        if let Some(ub) = ubound
          && target > ub
          && saturate
        {
          target = ub;
        }
        let actual_delta = target - cur;
        let target_str = format!("{target}");
        db.set(key.as_bytes(), target_str.as_bytes(), &[])?;
        Ok(RespValue::Arr(vec![
          RespValue::Blob(target_str.into_bytes()),
          RespValue::Blob(format!("{actual_delta}").into_bytes()),
        ]))
      } else {
        let delta = by_int.unwrap_or(1);
        let cur = db
          .get(key.as_bytes())?
          .and_then(|v| String::from_utf8(v).ok())
          .and_then(|s| s.parse::<i64>().ok())
          .unwrap_or(0);
        let mut target = cur.saturating_add(delta);
        if let Some(lb) = lbound
          && target < lb as i64
          && saturate
        {
          target = lb as i64;
        }
        if let Some(ub) = ubound
          && target > ub as i64
          && saturate
        {
          target = ub as i64;
        }
        let actual_delta = target.saturating_sub(cur);
        let target_str = format!("{target}");
        db.set(key.as_bytes(), target_str.as_bytes(), &[])?;
        Ok(RespValue::Arr(vec![
          RespValue::Int(target),
          RespValue::Int(actual_delta),
        ]))
      }
    }
    Cmd::Digest(key) => match db.digest(key.as_bytes())? {
      Some(s) => Ok(RespValue::Blob(s.into_bytes())),
      None => Ok(RespValue::Null),
    },
    Cmd::DelEx { key, if_eq } => {
      let opt = match if_eq.as_deref() {
        Some(expected) => DelEx::IfEq(expected),
        None => DelEx::None,
      };
      let ok = db.delex(key.as_bytes(), opt)?;
      Ok(RespValue::Int(if ok { 1 } else { 0 }))
    }
    Cmd::Cas {
      key,
      old_val,
      new_val,
      ex,
    } => {
      let expire_ms = ex
        .map(|sec| current_now_ms().saturating_add(sec.saturating_mul(1000)))
        .unwrap_or(0);
      let ret = db.cas(key.as_bytes(), &old_val, &new_val, expire_ms)?;
      Ok(RespValue::Int(ret as i64))
    }
    Cmd::Cad { key, val } => {
      let ret = db.cad(key.as_bytes(), &val)?;
      Ok(RespValue::Int(ret as i64))
    }
    Cmd::Lcs {
      key1,
      key2,
      len_only,
    } => {
      let args = StringLCSArgs {
        lcs_type: if len_only {
          StringLCSType::Len
        } else {
          StringLCSType::None
        },
        min_match_len: 0,
      };
      match db.lcs(key1.as_bytes(), key2.as_bytes(), args)? {
        wedb_embed::StringLCSResult::Len(l) => Ok(RespValue::Int(l as i64)),
        wedb_embed::StringLCSResult::Str(s) => Ok(RespValue::Blob(s.into_bytes())),
        wedb_embed::StringLCSResult::Idx(_) => Ok(RespValue::Null),
      }
    }
    Cmd::BitField { key, ops } => {
      let results = db.bitfield(key.as_bytes(), &ops)?;
      let arr = results
        .into_iter()
        .map(|v| match v {
          Some(wedb_embed::BitfieldValue::Signed(n)) => RespValue::Int(n),
          Some(wedb_embed::BitfieldValue::Unsigned(n)) => RespValue::Int(n as i64),
          None => RespValue::Null,
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::BitFieldRo { key, ops } => {
      let results = db.bitfield_read_only(key.as_bytes(), &ops)?;
      let arr = results
        .into_iter()
        .map(|v| match v {
          Some(wedb_embed::BitfieldValue::Signed(n)) => RespValue::Int(n),
          Some(wedb_embed::BitfieldValue::Unsigned(n)) => RespValue::Int(n as i64),
          None => RespValue::Null,
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::StrLen(key) => {
      let len = db.strlen(key.as_bytes())?;
      Ok(RespValue::Int(len as i64))
    }
    Cmd::Append(key, val) => {
      let len = db.append(key.as_bytes(), &val)?;
      Ok(RespValue::Int(len as i64))
    }
    Cmd::GetRange(key, start, end) => {
      let bytes = db.getrange(key.as_bytes(), start, end)?;
      Ok(RespValue::Blob(bytes))
    }
    Cmd::SetRange(key, offset, val) => {
      let len = db.setrange(key.as_bytes(), offset, &val)?;
      Ok(RespValue::Int(len as i64))
    }
    Cmd::SetBit(key, offset, val) => {
      let old = db.setbit(key.as_bytes(), offset as u64, val)?;
      Ok(RespValue::Int(old as i64))
    }
    Cmd::GetBit(key, offset) => {
      let bit = db.getbit(key.as_bytes(), offset as u64)?;
      Ok(RespValue::Int(bit as i64))
    }
    Cmd::BitCount {
      key, start, end, ..
    } => {
      let count = db.bitcount(key.as_bytes(), start, end)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::BitPos {
      key,
      bit,
      start,
      end,
    } => {
      let pos = db.bitpos(key.as_bytes(), bit, start, end)?;
      Ok(RespValue::Int(pos))
    }
    Cmd::BitOp { op, dest, src_keys } => {
      let mut src_vals = Vec::with_capacity(src_keys.len());
      for k in &src_keys {
        let v = db.get(k.as_bytes())?.unwrap_or_default();
        src_vals.push(v);
      }
      let slices: Vec<&[u8]> = src_vals.iter().map(|v| v.as_slice()).collect();
      let out = wedb_embed::bit_op_exec(&op, &slices)?;
      let out_len = out.len() as i64;
      db.set(dest.as_bytes(), &out, &[])?;
      Ok(RespValue::Int(out_len))
    }
    _ => Err(Error::internal("unsupported string command")),
  }
}
