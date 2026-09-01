use std::mem::swap;
use std::str::from_utf8;
use std::sync::Arc;

use super::context::{
  ConnectionContext, KeyComposer, bit_op_exec, get_bit_from_bytes,
  normalize_bit_range_to_byte_mask, normalize_range, raw_bitpos, raw_popcount, set_bit_in_bytes,
};
use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::redis::cmd::RedisCommand;
use crate::redis::hll::rapid_hash;
use crate::redis::protocol::RespValue;
use crate::redis::resp_util::{float_to_blob, format_float_bytes};
use crate::util::now_millis;
use wedb_raft::types::{BatchWriteReq, GetKVReq, UpsertKV};

/// 字符串与位图命令主调度处理器（对标 Apache Kvrocks RedisString & RedisBitmap）
pub async fn handle_string(
  node: &Arc<RaftNode>,
  ctx: &mut ConnectionContext,
  cmd: RedisCommand,
) -> Result<RespValue> {
  let sm = node.state_machine();
  let kc = ctx.key_composer();

  match cmd {
    // ================= 1. 字符串基础读取与写入 =================
    RedisCommand::Get(key) => {
      let raw_k = kc.raw_key(&key);
      let val = node.read(GetKVReq { key: raw_k }).await?;
      match val {
        Some(v) => Ok(RespValue::Blob(v)),
        None => Ok(RespValue::Null),
      }
    }
    RedisCommand::Set {
      key,
      value,
      ex,
      px,
      exat,
      pxat,
      nx,
      xx,
      get,
      keepttl,
    } => {
      let raw_k = kc.raw_key(&key);
      let has_cond = nx || xx || get;
      let current = if has_cond {
        node.read(GetKVReq { key: raw_k.clone() }).await?
      } else {
        None
      };

      // 条件判断：NX (不存在才写入) 与 XX (存在才写入)
      if (nx && current.is_some()) || (xx && current.is_none()) {
        if get {
          return match current {
            Some(v) => Ok(RespValue::Blob(v)),
            None => Ok(RespValue::Null),
          };
        }
        return Ok(RespValue::Null);
      }

      let now = now_millis();
      let expire_at = if let Some(s) = ex {
        Some(now + s * 1000)
      } else if let Some(ms) = px {
        Some(now + ms)
      } else if let Some(eat) = exat {
        Some(eat * 1000)
      } else if let Some(pat) = pxat {
        Some(pat)
      } else if keepttl {
        sm.get_ttl_expire_at(&raw_k).ok().flatten()
      } else {
        None
      };

      let prev_resp = if get {
        match current {
          Some(v) => RespValue::Blob(v),
          None => RespValue::Null,
        }
      } else {
        RespValue::ok()
      };

      let ttl_opt = if let Some(eat) = expire_at {
        Some(eat)
      } else if !keepttl {
        Some(0)
      } else {
        None
      };

      let entries = vec![UpsertKV::insert_with_ttl(raw_k, value, ttl_opt)];
      node.batch_write(BatchWriteReq { entries }).await?;

      Ok(prev_resp)
    }
    RedisCommand::SetNx(key, value) => {
      let raw_k = kc.raw_key(&key);
      if node.read(GetKVReq { key: raw_k.clone() }).await?.is_some() {
        return Ok(RespValue::Int(0));
      }
      let entries = vec![UpsertKV::insert_with_ttl(raw_k, value, Some(0))];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::Int(1))
    }
    RedisCommand::SetEx(key, ttl_sec, value) => {
      let raw_k = kc.raw_key(&key);
      let expire_at = now_millis() + ttl_sec * 1000;
      let entries = vec![UpsertKV::insert_with_ttl(raw_k, value, Some(expire_at))];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::PSetEx(key, ttl_ms, value) => {
      let raw_k = kc.raw_key(&key);
      let expire_at = now_millis() + ttl_ms;
      let entries = vec![UpsertKV::insert_with_ttl(raw_k, value, Some(expire_at))];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::GetSet(key, value) => {
      let raw_k = kc.raw_key(&key);
      let old_val = node.read(GetKVReq { key: raw_k.clone() }).await?;
      let entries = vec![UpsertKV::insert_with_ttl(raw_k, value, Some(0))];
      node.batch_write(BatchWriteReq { entries }).await?;
      match old_val {
        Some(v) => Ok(RespValue::Blob(v)),
        None => Ok(RespValue::Null),
      }
    }
    RedisCommand::GetDel(key) => {
      let raw_k = kc.raw_key(&key);
      let val = node.read(GetKVReq { key: raw_k.clone() }).await?;
      if val.is_some() {
        let entries = vec![UpsertKV::delete(raw_k)];
        node.batch_write(BatchWriteReq { entries }).await?;
      }
      match val {
        Some(v) => Ok(RespValue::Blob(v)),
        None => Ok(RespValue::Null),
      }
    }
    RedisCommand::GetEx {
      key,
      ex,
      px,
      persist,
    } => {
      let raw_k = kc.raw_key(&key);
      let val = node.read(GetKVReq { key: raw_k.clone() }).await?;
      if let Some(ref data) = val {
        if persist {
          let entries = vec![UpsertKV::insert_with_ttl(raw_k, data.clone(), Some(0))];
          node.batch_write(BatchWriteReq { entries }).await?;
        } else if let Some(s) = ex {
          let expire_at = now_millis() + s * 1000;
          let entries = vec![UpsertKV::insert_with_ttl(
            raw_k,
            data.clone(),
            Some(expire_at),
          )];
          node.batch_write(BatchWriteReq { entries }).await?;
        } else if let Some(ms) = px {
          let expire_at = now_millis() + ms;
          let entries = vec![UpsertKV::insert_with_ttl(
            raw_k,
            data.clone(),
            Some(expire_at),
          )];
          node.batch_write(BatchWriteReq { entries }).await?;
        }
      }
      match val {
        Some(v) => Ok(RespValue::Blob(v)),
        None => Ok(RespValue::Null),
      }
    }
    RedisCommand::MGet(keys) => {
      let mut results = Vec::with_capacity(keys.len());
      for k in keys {
        let raw_k = kc.raw_key(&k);
        let v = node.read(GetKVReq { key: raw_k }).await?;
        match v {
          Some(data) => results.push(RespValue::Blob(data)),
          None => results.push(RespValue::Null),
        }
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::MSet(pairs) => {
      let mut entries = Vec::with_capacity(pairs.len());
      for (k, v) in pairs {
        let raw_k = kc.raw_key(&k);
        entries.push(UpsertKV::insert_with_ttl(raw_k, v, Some(0)));
      }
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::MSetNx(pairs) => {
      for (k, _) in &pairs {
        let raw_k = kc.raw_key(k);
        if node.read(GetKVReq { key: raw_k }).await?.is_some() {
          return Ok(RespValue::Int(0));
        }
      }
      let mut entries = Vec::with_capacity(pairs.len());
      for (k, v) in pairs {
        let raw_k = kc.raw_key(&k);
        entries.push(UpsertKV::insert_with_ttl(raw_k, v, Some(0)));
      }
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::Int(1))
    }
    RedisCommand::MSetEx { ttl_sec, pairs } => {
      let mut entries = Vec::with_capacity(pairs.len());
      let expire_at = now_millis() + ttl_sec * 1000;
      for (k, v) in pairs {
        let raw_k = kc.raw_key(&k);
        entries.push(UpsertKV::insert_with_ttl(raw_k, v, Some(expire_at)));
      }
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }

    // ================= 2. 数值原子自增/自减 =================
    RedisCommand::Incr(key) => incr_by_delta(node, kc.raw_key(&key), 1).await,
    RedisCommand::Decr(key) => incr_by_delta(node, kc.raw_key(&key), -1).await,
    RedisCommand::IncrBy(key, delta) => incr_by_delta(node, kc.raw_key(&key), delta).await,
    RedisCommand::DecrBy(key, delta) => incr_by_delta(node, kc.raw_key(&key), -delta).await,
    RedisCommand::IncrByFloat(key, delta) => {
      let raw_k = kc.raw_key(&key);
      let current_str = node.read(GetKVReq { key: raw_k.clone() }).await?;
      let mut num: f64 = 0.0;
      if let Some(b) = current_str {
        num = parse_redis_f64(&b)?;
      }
      num += delta;
      if num.is_nan() || num.is_infinite() {
        return Err(Error::invalid_data(
          "ERR increment would produce NaN or Infinity",
        ));
      }
      let mut buf = zmij::Buffer::new();
      let num_bytes = format_float_bytes(num, &mut buf).to_vec();
      let entries = vec![UpsertKV::insert(raw_k, num_bytes.clone())];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::Blob(num_bytes))
    }

    // ================= 3. 字符串长度、追加与区间操作 =================
    RedisCommand::StrLen(key) => {
      let raw_k = kc.raw_key(&key);
      let val = node.read(GetKVReq { key: raw_k }).await?;
      Ok(RespValue::Int(val.map(|v| v.len() as i64).unwrap_or(0)))
    }
    RedisCommand::Append(key, extra) => {
      let raw_k = kc.raw_key(&key);
      let mut cur = node
        .read(GetKVReq { key: raw_k.clone() })
        .await?
        .unwrap_or_default();
      cur.extend_from_slice(&extra);
      let new_len = cur.len() as i64;
      let entries = vec![UpsertKV::insert(raw_k, cur)];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::Int(new_len))
    }
    RedisCommand::GetRange(key, start, end) => {
      let raw_k = kc.raw_key(&key);
      let val = node
        .read(GetKVReq { key: raw_k })
        .await?
        .unwrap_or_default();
      let len = val.len() as i64;
      if len == 0 {
        return Ok(RespValue::Blob(Vec::new()));
      }
      let (s, e) = normalize_range(start, end, len);
      if s > e || s >= len {
        Ok(RespValue::Blob(Vec::new()))
      } else {
        let slice = &val[s as usize..=e as usize];
        Ok(RespValue::Blob(slice.to_vec()))
      }
    }
    RedisCommand::SetRange(key, offset, val) => {
      let raw_k = kc.raw_key(&key);
      let cur_val = node.read(GetKVReq { key: raw_k.clone() }).await?;
      let exists = cur_val.is_some();
      if val.is_empty() && !exists {
        return Ok(RespValue::Int(0));
      }
      let mut cur = cur_val.unwrap_or_default();
      if cur.len() < offset {
        cur.resize(offset, 0);
      }
      if cur.len() < offset + val.len() {
        cur.resize(offset + val.len(), 0);
      }
      cur[offset..offset + val.len()].copy_from_slice(&val);
      let len = cur.len() as i64;
      let entries = vec![UpsertKV::insert(raw_k, cur)];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::Int(len))
    }

    // ================= 4. 条件原子操作与摘要 =================
    RedisCommand::Digest(key) => {
      let raw_k = kc.raw_key(&key);
      let val = node
        .read(GetKVReq { key: raw_k })
        .await?
        .unwrap_or_default();
      let hash = rapid_hash(&val);
      Ok(RespValue::Blob(format!("{hash:016x}").into_bytes()))
    }
    RedisCommand::DelEx { key, if_eq } => {
      let raw_k = kc.raw_key(&key);
      let val = node.read(GetKVReq { key: raw_k.clone() }).await?;
      match val {
        Some(v) => {
          if let Some(target) = if_eq
            && v != target
          {
            return Ok(RespValue::Int(0));
          }
          let entries = vec![UpsertKV::delete(raw_k.clone())];
          node.batch_write(BatchWriteReq { entries }).await?;
          sm.remove_ttl(&raw_k).ok();
          Ok(RespValue::Int(1))
        }
        None => Ok(RespValue::Int(0)),
      }
    }
    RedisCommand::Cas {
      key,
      old_val,
      new_val,
      ex,
    } => {
      let raw_k = kc.raw_key(&key);
      let val = node.read(GetKVReq { key: raw_k.clone() }).await?;
      match val {
        Some(v) if v == old_val => {
          let entries = vec![UpsertKV::insert(raw_k.clone(), new_val)];
          node.batch_write(BatchWriteReq { entries }).await?;
          if let Some(s) = ex {
            sm.set_ttl(&raw_k, now_millis() + s * 1000).ok();
          } else {
            sm.remove_ttl(&raw_k).ok();
          }
          Ok(RespValue::Int(1))
        }
        _ => Ok(RespValue::Int(0)),
      }
    }
    RedisCommand::Cad { key, val } => {
      let raw_k = kc.raw_key(&key);
      let cur = node.read(GetKVReq { key: raw_k.clone() }).await?;
      match cur {
        Some(v) if v == val => {
          let entries = vec![UpsertKV::delete(raw_k.clone())];
          node.batch_write(BatchWriteReq { entries }).await?;
          sm.remove_ttl(&raw_k).ok();
          Ok(RespValue::Int(1))
        }
        _ => Ok(RespValue::Int(0)),
      }
    }
    RedisCommand::Lcs {
      key1,
      key2,
      len_only,
    } => {
      let k1 = kc.raw_key(&key1);
      let k2 = kc.raw_key(&key2);
      let v1 = node.read(GetKVReq { key: k1 }).await?.unwrap_or_default();
      let v2 = node.read(GetKVReq { key: k2 }).await?.unwrap_or_default();

      let m = v1.len();
      let n = v2.len();
      if m == 0 || n == 0 {
        return Ok(if len_only {
          RespValue::Int(0)
        } else {
          RespValue::Blob(Vec::new())
        });
      }

      if len_only {
        let (s1, s2) = if m <= n { (&v1, &v2) } else { (&v2, &v1) };
        let mut prev = vec![0u32; s1.len() + 1];
        let mut curr = vec![0u32; s1.len() + 1];
        for &b2 in s2.iter() {
          for (i, &b1) in s1.iter().enumerate() {
            if b1 == b2 {
              curr[i + 1] = prev[i] + 1;
            } else {
              curr[i + 1] = curr[i].max(prev[i + 1]);
            }
          }
          swap(&mut prev, &mut curr);
          curr.fill(0);
        }
        Ok(RespValue::Int(prev[s1.len()] as i64))
      } else {
        let cols = n + 1;
        let mut dp = vec![0u32; (m + 1) * cols];

        for (i, &b1) in v1.iter().enumerate() {
          for (j, &b2) in v2.iter().enumerate() {
            if b1 == b2 {
              dp[(i + 1) * cols + (j + 1)] = dp[i * cols + j] + 1;
            } else {
              dp[(i + 1) * cols + (j + 1)] = dp[(i + 1) * cols + j].max(dp[i * cols + (j + 1)]);
            }
          }
        }

        let mut lcs = Vec::new();
        let mut i = m;
        let mut j = n;
        while i > 0 && j > 0 {
          if v1[i - 1] == v2[j - 1] {
            lcs.push(v1[i - 1]);
            i -= 1;
            j -= 1;
          } else if dp[(i - 1) * cols + j] > dp[i * cols + (j - 1)] {
            i -= 1;
          } else {
            j -= 1;
          }
        }
        lcs.reverse();
        Ok(RespValue::Blob(lcs))
      }
    }

    // ================= 5. 位图 (Bitmap) 高性能位操作 =================
    RedisCommand::GetBit(key, offset) => {
      let raw_k = kc.raw_key(&key);
      let val = if sm.is_expired(&raw_k) {
        Vec::new()
      } else {
        node
          .read(GetKVReq { key: raw_k })
          .await?
          .unwrap_or_default()
      };
      let bit = get_bit_from_bytes(&val, offset);
      Ok(RespValue::Int(bit as i64))
    }
    RedisCommand::SetBit(key, offset, bit) => {
      let raw_k = kc.raw_key(&key);
      let mut val = if sm.is_expired(&raw_k) {
        Vec::new()
      } else {
        node
          .read(GetKVReq { key: raw_k.clone() })
          .await?
          .unwrap_or_default()
      };
      let old_bit = set_bit_in_bytes(&mut val, offset, bit);
      let entries = vec![UpsertKV::insert(raw_k, val)];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::Int(old_bit as i64))
    }
    RedisCommand::BitCount {
      key,
      start,
      end,
      is_bit,
    } => {
      let raw_k = kc.raw_key(&key);
      let val = node
        .read(GetKVReq { key: raw_k })
        .await?
        .unwrap_or_default();
      if val.is_empty() {
        return Ok(RespValue::Int(0));
      }

      if !is_bit {
        let len = val.len() as i64;
        let (s, e) = normalize_range(start.unwrap_or(0), end.unwrap_or(-1), len);
        if s > e {
          return Ok(RespValue::Int(0));
        }
        let count = raw_popcount(&val[s as usize..=e as usize]);
        Ok(RespValue::Int(count as i64))
      } else {
        let tot_bits = (val.len() * 8) as i64;
        let (s_bit, e_bit) = normalize_range(start.unwrap_or(0), end.unwrap_or(-1), tot_bits);
        if s_bit > e_bit {
          return Ok(RespValue::Int(0));
        }
        let (s_byte, e_byte, first_mask, last_mask) =
          normalize_bit_range_to_byte_mask(s_bit, e_bit);

        if s_byte == e_byte {
          let mask = first_mask | last_mask;
          let count = (val[s_byte] & !mask).count_ones() as i64;
          Ok(RespValue::Int(count))
        } else {
          let mut count = raw_popcount(&val[s_byte..=e_byte.min(val.len() - 1)]) as i64;
          if first_mask != 0 {
            count -= (val[s_byte] & first_mask).count_ones() as i64;
          }
          if last_mask != 0 && e_byte < val.len() {
            count -= (val[e_byte] & last_mask).count_ones() as i64;
          }
          Ok(RespValue::Int(count))
        }
      }
    }
    RedisCommand::BitPos {
      key,
      bit,
      start,
      end,
    } => {
      let raw_k = kc.raw_key(&key);
      let val = node
        .read(GetKVReq { key: raw_k })
        .await?
        .unwrap_or_default();
      if val.is_empty() {
        return Ok(RespValue::Int(if bit == 0 { 0 } else { -1 }));
      }
      let len = val.len() as i64;
      let stop_given = end.is_some();
      let (s, e) = normalize_range(start.unwrap_or(0), end.unwrap_or(-1), len);
      if s > e {
        return Ok(RespValue::Int(-1));
      }
      let start_byte = s as usize;
      let stop_byte = (e as usize).min(val.len() - 1);
      let slice = &val[start_byte..=stop_byte];

      if let Some(pos_in_slice) = raw_bitpos(slice, bit) {
        Ok(RespValue::Int((start_byte * 8 + pos_in_slice) as i64))
      } else if !stop_given && bit == 0 {
        Ok(RespValue::Int((val.len() * 8) as i64))
      } else {
        Ok(RespValue::Int(-1))
      }
    }
    RedisCommand::BitOp { op, dest, src_keys } => {
      let dest_k = kc.raw_key(&dest);
      let mut src_vals = Vec::with_capacity(src_keys.len());
      for k in &src_keys {
        let raw = kc.raw_key(k);
        let v = node.read(GetKVReq { key: raw }).await?.unwrap_or_default();
        src_vals.push(v);
      }

      let slices: Vec<&[u8]> = src_vals.iter().map(|v| v.as_slice()).collect();
      let out = bit_op_exec(&op, &slices)?;
      let out_len = out.len() as i64;
      let entries = vec![UpsertKV::insert(dest_k, out)];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::Int(out_len))
    }
    RedisCommand::BitField { key, ops } => handle_bitfield_ops(node, &kc, key, ops, false).await,
    RedisCommand::BitFieldRo { key, ops } => handle_bitfield_ops(node, &kc, key, ops, true).await,
    RedisCommand::IncrEx {
      key,
      by_float,
      by_int,
      saturate,
      lbound,
      ubound,
      ex,
      px,
      exat,
      pxat,
      persist,
      enx,
    } => {
      let raw_k = kc.raw_key(&key);
      let has_ttl = sm.get_ttl_expire_at(&raw_k).ok().flatten().is_some();
      let current_bytes = node.read(GetKVReq { key: raw_k.clone() }).await?;

      let now = now_millis();

      if let Some(delta_f) = by_float {
        let cur_f = match &current_bytes {
          Some(b) => parse_redis_f64(b)?,
          None => 0.0,
        };
        let mut target = cur_f + delta_f;
        let mut is_overflow = false;
        if let Some(lb) = lbound
          && target < lb
        {
          is_overflow = true;
          if saturate {
            target = lb;
          }
        }
        if let Some(ub) = ubound
          && target > ub
        {
          is_overflow = true;
          if saturate {
            target = ub;
          }
        }

        if is_overflow && !saturate {
          return Ok(RespValue::Arr(vec![
            float_to_blob(cur_f),
            RespValue::Blob(b"0".to_vec()),
          ]));
        }

        let actual_delta = target - cur_f;
        let mut buf = zmij::Buffer::new();
        let target_bytes = format_float_bytes(target, &mut buf).to_vec();
        let actual_delta_bytes = format_float_bytes(actual_delta, &mut buf).to_vec();

        let entries = vec![UpsertKV::insert(raw_k.clone(), target_bytes.clone())];
        node.batch_write(BatchWriteReq { entries }).await?;

        let should_set_ttl = !enx || !has_ttl;
        if should_set_ttl {
          if persist {
            sm.remove_ttl(&raw_k).ok();
          } else if let Some(s) = ex {
            sm.set_ttl(&raw_k, now + s * 1000).ok();
          } else if let Some(ms) = px {
            sm.set_ttl(&raw_k, now + ms).ok();
          } else if let Some(eat) = exat {
            sm.set_ttl(&raw_k, eat * 1000).ok();
          } else if let Some(pat) = pxat {
            sm.set_ttl(&raw_k, pat).ok();
          }
        }

        Ok(RespValue::Arr(vec![
          RespValue::Blob(target_bytes),
          RespValue::Blob(actual_delta_bytes),
        ]))
      } else {
        let delta = by_int.unwrap_or(1);
        let cur_i = match &current_bytes {
          Some(b) => parse_redis_i64(b)?,
          None => 0,
        };

        let lb_i = lbound.map(|v| v as i64);
        let ub_i = ubound.map(|v| v as i64);

        let (sum, has_math_overflow) = cur_i.overflowing_add(delta);
        let mut target = if has_math_overflow {
          if delta > 0 { i64::MAX } else { i64::MIN }
        } else {
          sum
        };

        let mut is_out_of_bounds = has_math_overflow;
        if let Some(lb) = lb_i
          && target < lb
        {
          is_out_of_bounds = true;
          if saturate {
            target = lb;
          }
        }
        if let Some(ub) = ub_i
          && target > ub
        {
          is_out_of_bounds = true;
          if saturate {
            target = ub;
          }
        }

        if is_out_of_bounds && !saturate {
          return Ok(RespValue::Arr(vec![
            RespValue::Int(cur_i),
            RespValue::Int(0),
          ]));
        }

        let actual_delta = target.saturating_sub(cur_i);
        let mut buf = itoa::Buffer::new();
        let num_bytes = buf.format(target).as_bytes().to_vec();

        let entries = vec![UpsertKV::insert(raw_k.clone(), num_bytes)];
        node.batch_write(BatchWriteReq { entries }).await?;

        let should_set_ttl = !enx || !has_ttl;
        if should_set_ttl {
          if persist {
            sm.remove_ttl(&raw_k).ok();
          } else if let Some(s) = ex {
            sm.set_ttl(&raw_k, now + s * 1000).ok();
          } else if let Some(ms) = px {
            sm.set_ttl(&raw_k, now + ms).ok();
          } else if let Some(eat) = exat {
            sm.set_ttl(&raw_k, eat * 1000).ok();
          } else if let Some(pat) = pxat {
            sm.set_ttl(&raw_k, pat).ok();
          }
        }

        Ok(RespValue::Arr(vec![
          RespValue::Int(target),
          RespValue::Int(actual_delta),
        ]))
      }
    }
    _ => Err(Error::redis("Command not matched in handle_string")),
  }
}

/// 解析 Redis 规范整型字符串
fn parse_redis_i64(bytes: &[u8]) -> Result<i64> {
  let s = from_utf8(bytes)
    .map_err(|_| Error::invalid_data("ERR value is not an integer or out of range"))?;
  if s.is_empty() || s.starts_with(' ') || s.ends_with(' ') {
    return Err(Error::invalid_data(
      "ERR value is not an integer or out of range",
    ));
  }
  s.parse::<i64>()
    .map_err(|_| Error::invalid_data("ERR value is not an integer or out of range"))
}

/// 解析 Redis 规范浮点数字符串
fn parse_redis_f64(bytes: &[u8]) -> Result<f64> {
  let s = from_utf8(bytes).map_err(|_| Error::invalid_data("ERR value is not a valid float"))?;
  if s.is_empty() || s.starts_with(' ') || s.ends_with(' ') {
    return Err(Error::invalid_data("ERR value is not a valid float"));
  }
  s.parse::<f64>()
    .map_err(|_| Error::invalid_data("ERR value is not a valid float"))
}

/// 高性能整数自增/自减运算实现
async fn incr_by_delta(node: &Arc<RaftNode>, raw_k: String, delta: i64) -> Result<RespValue> {
  let current_str = node.read(GetKVReq { key: raw_k.clone() }).await?;
  let mut num: i64 = 0;
  if let Some(b) = current_str {
    num = parse_redis_i64(&b)?;
  }
  if (delta > 0 && num > i64::MAX - delta) || (delta < 0 && num < i64::MIN - delta) {
    return Err(Error::invalid_data(
      "ERR increment or decrement would overflow",
    ));
  }
  num += delta;
  let mut buf = itoa::Buffer::new();
  let num_bytes = buf.format(num).as_bytes().to_vec();
  let entries = vec![UpsertKV::insert(raw_k, num_bytes)];
  node.batch_write(BatchWriteReq { entries }).await?;
  Ok(RespValue::Int(num))
}

/// 处理位域 BITFIELD / BITFIELD_RO 操作
async fn handle_bitfield_ops(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: String,
  ops: Vec<wedb_embed::BitfieldOperation>,
  is_ro: bool,
) -> Result<RespValue> {
  let raw_k = kc.raw_key(&key);
  let mut val = node
    .read(GetKVReq { key: raw_k.clone() })
    .await?
    .unwrap_or_default();

  let mut results = Vec::with_capacity(ops.len());
  let mut modified = false;

  for op in &ops {
    let bits = op.encoding.bits();
    let first_byte = (op.offset / 8) as usize;
    let last_byte = ((op.offset + bits as u64 - 1) / 8 + 1) as usize;

    if val.len() < last_byte {
      val.resize(last_byte, 0);
    }

    let mut bitfield_buf = wedb_embed::ArrayBitfieldBitmap::new(first_byte as u32);
    let slice_len = (last_byte - first_byte).min(9);
    bitfield_buf.set(first_byte as u32, &val[first_byte..first_byte + slice_len])?;

    let old_raw = if op.encoding.is_signed() {
      bitfield_buf.get_signed_bitfield(op.offset, bits)? as u64
    } else {
      bitfield_buf.get_unsigned_bitfield(op.offset, bits)?
    };

    let (ret_val, new_raw, is_overflow) = wedb_embed::bitfield_op_calc(op, old_raw);

    if !is_ro && op.op_type != wedb_embed::BitfieldOpType::Get && !is_overflow {
      bitfield_buf.set_bitfield(op.offset, bits, new_raw)?;
      let mut tmp = vec![0u8; slice_len];
      bitfield_buf.get(first_byte as u32, &mut tmp)?;
      val[first_byte..first_byte + slice_len].copy_from_slice(&tmp);
      modified = true;
    }

    match ret_val {
      Some(wedb_embed::BitfieldValue::Signed(v)) => results.push(RespValue::Int(v)),
      Some(wedb_embed::BitfieldValue::Unsigned(v)) => results.push(RespValue::Int(v as i64)),
      None => results.push(RespValue::Null),
    }
  }

  if modified {
    let entries = vec![UpsertKV::insert(raw_k, val)];
    node.batch_write(BatchWriteReq { entries }).await?;
  }

  Ok(RespValue::Arr(results))
}
