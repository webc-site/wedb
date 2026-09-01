use std::str::from_utf8;
use std::sync::Arc;

use super::context::{
  ConnectionContext, decode_hash_value, encode_hash_value, is_field_expired, matches_glob_bytes,
};
use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::redis::cmd::{ExpireCondition, RedisCommand};
use crate::redis::protocol::RespValue;
use crate::redis::resp_util::format_float_bytes;
use crate::util::now_millis;
use wedb_raft::types::{BatchWriteReq, GetKVReq, ScanPrefixReq, UpsertKV};

/// 校验字段过期条件是否满足（对标 Apache Kvrocks HExpireConditionPasses）
#[inline]
pub fn hexpire_condition_passes(
  condition: ExpireCondition,
  current_expire_at: u64,
  target_expire_at: u64,
) -> bool {
  match condition {
    ExpireCondition::None => true,
    ExpireCondition::NX => current_expire_at == 0,
    ExpireCondition::XX => current_expire_at > 0,
    ExpireCondition::GT => {
      if current_expire_at == 0 {
        // 持久字段拥有无限 TTL，任何有限 target_expire 都不大于无限
        false
      } else {
        target_expire_at > current_expire_at
      }
    }
    ExpireCondition::LT => {
      if current_expire_at == 0 {
        // 持久字段拥有无限 TTL，任何有限 target_expire 都小于无限
        true
      } else {
        target_expire_at < current_expire_at
      }
    }
  }
}

/// 哈希数据结构处理核心入口（涵盖 Redis 7.4+ 全套命令与字段级 TTL）
pub async fn handle_hash(
  node: &Arc<RaftNode>,
  ctx: &mut ConnectionContext,
  cmd: RedisCommand,
) -> Result<RespValue> {
  let kc = ctx.key_composer();
  let now_ms = now_millis();

  match cmd {
    // ---- 1. 字段写入与更新 (HSET / HMSET / HSETNX) ----
    RedisCommand::HSet(key, fields) => {
      let mut entries = Vec::with_capacity(fields.len());
      let mut added_count = 0;
      for (f, v) in fields {
        let h_k = kc.hash_item(&key, &f);
        let existing = node.read(GetKVReq { key: h_k.clone() }).await?;
        let is_new = match existing {
          Some(raw) => {
            let (exp, _) = decode_hash_value(&raw);
            is_field_expired(exp, now_ms)
          }
          None => true,
        };
        if is_new {
          added_count += 1;
        }
        // HSet 覆盖写入重置 TTL 为持久 (0)
        let val_enc = encode_hash_value(&v, 0);
        entries.push(UpsertKV::insert(h_k, val_enc));
      }
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::Int(added_count))
    }
    RedisCommand::HMSet(key, fields) => {
      let mut entries = Vec::with_capacity(fields.len());
      for (f, v) in fields {
        let h_k = kc.hash_item(&key, &f);
        let val_enc = encode_hash_value(&v, 0);
        entries.push(UpsertKV::insert(h_k, val_enc));
      }
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::HSetNx(key, field, value) => {
      let h_k = kc.hash_item(&key, &field);
      let existing = node.read(GetKVReq { key: h_k.clone() }).await?;
      if let Some(raw) = existing {
        let (exp, _) = decode_hash_value(&raw);
        if !is_field_expired(exp, now_ms) {
          return Ok(RespValue::Int(0));
        }
      }
      let val_enc = encode_hash_value(&value, 0);
      let entries = vec![UpsertKV::insert(h_k, val_enc)];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::Int(1))
    }

    // ---- 2. 字段读取与存在性判断 (HGET / HMGET / HEXISTS / HSTRLEN) ----
    RedisCommand::HGet(key, field) => {
      let h_k = kc.hash_item(&key, &field);
      eprintln!("HGET looking up h_k={:?}", h_k.as_bytes());
      let val = node.read(GetKVReq { key: h_k }).await?;
      match val {
        Some(raw) => {
          let (exp, payload) = decode_hash_value(&raw);
          if is_field_expired(exp, now_ms) {
            Ok(RespValue::Null)
          } else {
            Ok(RespValue::Blob(payload.to_vec()))
          }
        }
        None => Ok(RespValue::Null),
      }
    }
    RedisCommand::HMGet(key, fields) => {
      let mut results = Vec::with_capacity(fields.len());
      for f in fields {
        let h_k = kc.hash_item(&key, &f);
        let v = node.read(GetKVReq { key: h_k }).await?;
        match v {
          Some(raw) => {
            let (exp, payload) = decode_hash_value(&raw);
            if is_field_expired(exp, now_ms) {
              results.push(RespValue::Null);
            } else {
              results.push(RespValue::Blob(payload.to_vec()));
            }
          }
          None => results.push(RespValue::Null),
        }
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::HExists(key, field) => {
      let h_k = kc.hash_item(&key, &field);
      let val = node.read(GetKVReq { key: h_k }).await?;
      let exists = match val {
        Some(raw) => {
          let (exp, _) = decode_hash_value(&raw);
          !is_field_expired(exp, now_ms)
        }
        None => false,
      };
      Ok(RespValue::Int(if exists { 1 } else { 0 }))
    }
    RedisCommand::HStrLen(key, field) => {
      let h_k = kc.hash_item(&key, &field);
      let val = node.read(GetKVReq { key: h_k }).await?;
      let len = match val {
        Some(raw) => {
          let (exp, payload) = decode_hash_value(&raw);
          if is_field_expired(exp, now_ms) {
            0
          } else {
            payload.len() as i64
          }
        }
        None => 0,
      };
      Ok(RespValue::Int(len))
    }

    // ---- 3. 字段删除与统计 (HDEL / HLEN / HGETDEL) ----
    RedisCommand::HDel(key, fields) => {
      let mut entries = Vec::with_capacity(fields.len());
      let mut deleted_count = 0;
      for f in fields {
        let h_k = kc.hash_item(&key, &f);
        if let Some(raw) = node.read(GetKVReq { key: h_k.clone() }).await? {
          let (exp, _) = decode_hash_value(&raw);
          if !is_field_expired(exp, now_ms) {
            deleted_count += 1;
          }
          entries.push(UpsertKV::delete(h_k));
        }
      }
      if !entries.is_empty() {
        node.batch_write(BatchWriteReq { entries }).await?;
      }
      Ok(RespValue::Int(deleted_count))
    }
    RedisCommand::HGetDel { key, fields } => {
      let mut results = Vec::with_capacity(fields.len());
      let mut entries = Vec::new();
      for f in &fields {
        let h_k = kc.hash_item(&key, f);
        let v = node.read(GetKVReq { key: h_k.clone() }).await?;
        match v {
          Some(raw) => {
            let (exp, payload) = decode_hash_value(&raw);
            if is_field_expired(exp, now_ms) {
              results.push(RespValue::Null);
            } else {
              results.push(RespValue::Blob(payload.to_vec()));
              entries.push(UpsertKV::delete(h_k));
            }
          }
          None => results.push(RespValue::Null),
        }
      }
      if !entries.is_empty() {
        node.batch_write(BatchWriteReq { entries }).await?;
      }
      if fields.len() == 1 {
        Ok(results.into_iter().next().unwrap_or(RespValue::Null))
      } else {
        Ok(RespValue::Arr(results))
      }
    }
    RedisCommand::HLen(key) => {
      let prefix = kc.hash_prefix(&key);
      let items = node.scan_prefix(ScanPrefixReq { prefix }).await?;
      let mut active_count = 0i64;
      for (_, v) in items {
        let (exp, _) = decode_hash_value(&v);
        if !is_field_expired(exp, now_ms) {
          active_count += 1;
        }
      }
      Ok(RespValue::Int(active_count))
    }

    // ---- 4. 数值增减 (HINCRBY / HINCRBYFLOAT) ----
    RedisCommand::HIncrBy(key, field, delta) => {
      let h_k = kc.hash_item(&key, &field);
      let cur_b = node.read(GetKVReq { key: h_k.clone() }).await?;
      let mut num: i64 = 0;
      let mut keep_expire = 0u64;
      if let Some(raw) = cur_b {
        let (exp, payload) = decode_hash_value(&raw);
        if !is_field_expired(exp, now_ms) {
          keep_expire = exp;
          let s = from_utf8(payload)
            .map_err(|_| Error::invalid_data("ERR hash value is not an integer or out of range"))?;
          num = s
            .trim()
            .parse::<i64>()
            .map_err(|_| Error::invalid_data("ERR hash value is not an integer or out of range"))?;
        }
      }
      let new_num = num
        .checked_add(delta)
        .ok_or_else(|| Error::invalid_data("ERR increment or decrement would overflow"))?;
      let mut buf = itoa::Buffer::new();
      let num_bytes = buf.format(new_num).as_bytes();
      let val_enc = encode_hash_value(num_bytes, keep_expire);
      let entries = vec![UpsertKV::insert(h_k, val_enc)];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::Int(new_num))
    }
    RedisCommand::HIncrByFloat(key, field, delta) => {
      let h_k = kc.hash_item(&key, &field);
      let cur_b = node.read(GetKVReq { key: h_k.clone() }).await?;
      let mut num: f64 = 0.0;
      let mut keep_expire = 0u64;
      if let Some(raw) = cur_b {
        let (exp, payload) = decode_hash_value(&raw);
        if !is_field_expired(exp, now_ms) {
          keep_expire = exp;
          let s = from_utf8(payload)
            .map_err(|_| Error::invalid_data("ERR hash value is not a valid float"))?;
          num = s
            .trim()
            .parse::<f64>()
            .map_err(|_| Error::invalid_data("ERR hash value is not a valid float"))?;
        }
      }
      let new_num = num + delta;
      if new_num.is_nan() || new_num.is_infinite() {
        return Err(Error::invalid_data(
          "ERR increment would produce NaN or Infinity",
        ));
      }
      let mut buf = zmij::Buffer::new();
      let num_bytes = format_float_bytes(new_num, &mut buf).to_vec();
      let val_enc = encode_hash_value(&num_bytes, keep_expire);
      let entries = vec![UpsertKV::insert(h_k, val_enc)];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::Blob(num_bytes))
    }

    // ---- 5. 全表获取 (HGETALL / HKEYS / HVALS) ----
    RedisCommand::HGetAll(key) => {
      let prefix = kc.hash_prefix(&key);
      let items = node
        .scan_prefix(ScanPrefixReq {
          prefix: prefix.clone(),
        })
        .await?;
      let mut results = Vec::with_capacity(items.len() * 2);
      for (k, v) in items {
        let (exp, payload) = decode_hash_value(&v);
        if !is_field_expired(exp, now_ms) {
          let field = k[prefix.len()..].to_vec();
          results.push(RespValue::Blob(field));
          results.push(RespValue::Blob(payload.to_vec()));
        }
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::HKeys(key) => {
      let prefix = kc.hash_prefix(&key);
      let items = node
        .scan_prefix(ScanPrefixReq {
          prefix: prefix.clone(),
        })
        .await?;
      let mut results = Vec::with_capacity(items.len());
      for (k, v) in items {
        let (exp, _) = decode_hash_value(&v);
        if !is_field_expired(exp, now_ms) {
          let field = k[prefix.len()..].to_vec();
          results.push(RespValue::Blob(field));
        }
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::HVals(key) => {
      let prefix = kc.hash_prefix(&key);
      let items = node.scan_prefix(ScanPrefixReq { prefix }).await?;
      let mut results = Vec::with_capacity(items.len());
      for (_, v) in items {
        let (exp, payload) = decode_hash_value(&v);
        if !is_field_expired(exp, now_ms) {
          results.push(RespValue::Blob(payload.to_vec()));
        }
      }
      Ok(RespValue::Arr(results))
    }

    // ---- 6. 随机采样与迭代扫描 (HRANDFIELD / HSCAN) ----
    RedisCommand::HRandField {
      key,
      count,
      with_values,
    } => {
      let prefix = kc.hash_prefix(&key);
      let items = node
        .scan_prefix(ScanPrefixReq {
          prefix: prefix.clone(),
        })
        .await?;

      // 过滤未过期元素并提取 (field, payload)
      let mut live_items = Vec::with_capacity(items.len());
      for (k, v) in items {
        let (exp, payload) = decode_hash_value(&v);
        if !is_field_expired(exp, now_ms) {
          let field = k[prefix.len()..].to_vec();
          live_items.push((field, payload.to_vec()));
        }
      }

      if live_items.is_empty() {
        return Ok(if count.is_none() {
          RespValue::Null
        } else {
          RespValue::Arr(Vec::new())
        });
      }

      match count {
        None => {
          let idx = fastrand::usize(0..live_items.len());
          let (f, _) = &live_items[idx];
          Ok(RespValue::Blob(f.clone()))
        }
        Some(0) => Ok(RespValue::Arr(Vec::new())),
        Some(c) if c > 0 => {
          // 正数：无放回不重复采样
          let take_count = (c as usize).min(live_items.len());
          if take_count < live_items.len() {
            // Fisher-Yates 部分洗牌
            for i in 0..take_count {
              let j = fastrand::usize(i..live_items.len());
              live_items.swap(i, j);
            }
          }
          let mut results = Vec::with_capacity(take_count * if with_values { 2 } else { 1 });
          for (f, v) in live_items.into_iter().take(take_count) {
            results.push(RespValue::Blob(f));
            if with_values {
              results.push(RespValue::Blob(v));
            }
          }
          Ok(RespValue::Arr(results))
        }
        Some(c) => {
          // 负数：有放回允许重复采样
          let total = c.unsigned_abs() as usize;
          let mut results = Vec::with_capacity(total * if with_values { 2 } else { 1 });
          for _ in 0..total {
            let idx = fastrand::usize(0..live_items.len());
            let (f, v) = &live_items[idx];
            results.push(RespValue::Blob(f.clone()));
            if with_values {
              results.push(RespValue::Blob(v.clone()));
            }
          }
          Ok(RespValue::Arr(results))
        }
      }
    }
    RedisCommand::HScan {
      key,
      cursor,
      pattern,
      count,
    } => {
      let prefix = kc.hash_prefix(&key);
      let items = node
        .scan_prefix(ScanPrefixReq {
          prefix: prefix.clone(),
        })
        .await?;
      let limit = count.unwrap_or(10);
      let is_match_all = pattern.as_deref().unwrap_or("*") == "*";

      let mut matched = Vec::new();
      if is_match_all {
        // pattern == "*" 零拷贝快速路径，避免任何 glob 匹配
        for (k, v) in items {
          let (exp, payload) = decode_hash_value(&v);
          if !is_field_expired(exp, now_ms) {
            let field_bytes = k[prefix.len()..].to_vec();
            matched.push(RespValue::Blob(field_bytes));
            matched.push(RespValue::Blob(payload.to_vec()));
          }
        }
      } else {
        let pat = pattern.unwrap_or_else(|| "*".to_string());
        let pat_bytes = pat.as_bytes();
        for (k, v) in items {
          let (exp, payload) = decode_hash_value(&v);
          if !is_field_expired(exp, now_ms) {
            let field_bytes = &k[prefix.len()..];
            if matches_glob_bytes(pat_bytes, field_bytes) {
              matched.push(RespValue::Blob(field_bytes.to_vec()));
              matched.push(RespValue::Blob(payload.to_vec()));
            }
          }
        }
      }

      let start = (cursor as usize) * 2;
      let end = (start + limit * 2).min(matched.len());
      let next_cursor = if end >= matched.len() {
        0
      } else {
        (end / 2) as u64
      };
      let slice = if start < matched.len() {
        matched[start..end].to_vec()
      } else {
        Vec::new()
      };

      let mut buf = itoa::Buffer::new();
      let cursor_str = buf.format(next_cursor).as_bytes().to_vec();
      Ok(RespValue::Arr(vec![
        RespValue::Blob(cursor_str),
        RespValue::Arr(slice),
      ]))
    }

    // ---- 7. Redis 7.4+ Field 级 TTL 管理 (HEXPIRE / HPEXPIRE / HEXPIREAT / HPEXPIREAT) ----
    RedisCommand::HExpire {
      key,
      seconds,
      condition,
      fields,
    } => {
      let target_expire_ms = if seconds <= 0 {
        0
      } else {
        now_ms.saturating_add((seconds as u64).saturating_mul(1000))
      };
      let is_immediate = seconds <= 0 || target_expire_ms <= now_ms;

      let mut results = Vec::with_capacity(fields.len());
      let mut entries = Vec::new();

      for f in fields {
        let h_k = kc.hash_item(&key, &f);
        let existing = node.read(GetKVReq { key: h_k.clone() }).await?;
        match existing {
          None => results.push(RespValue::Int(-2)),
          Some(raw) => {
            let (cur_exp, payload) = decode_hash_value(&raw);
            if is_field_expired(cur_exp, now_ms) {
              entries.push(UpsertKV::delete(h_k));
              results.push(RespValue::Int(-2));
            } else if !hexpire_condition_passes(condition, cur_exp, target_expire_ms) {
              results.push(RespValue::Int(0));
            } else if is_immediate {
              entries.push(UpsertKV::delete(h_k));
              results.push(RespValue::Int(2));
            } else {
              let val_enc = encode_hash_value(payload, target_expire_ms);
              entries.push(UpsertKV::insert(h_k, val_enc));
              results.push(RespValue::Int(1));
            }
          }
        }
      }
      if !entries.is_empty() {
        node.batch_write(BatchWriteReq { entries }).await?;
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::HPExpire {
      key,
      millis,
      condition,
      fields,
    } => {
      let target_expire_ms = if millis <= 0 {
        0
      } else {
        now_ms.saturating_add(millis as u64)
      };
      let is_immediate = millis <= 0 || target_expire_ms <= now_ms;

      let mut results = Vec::with_capacity(fields.len());
      let mut entries = Vec::new();

      for f in fields {
        let h_k = kc.hash_item(&key, &f);
        let existing = node.read(GetKVReq { key: h_k.clone() }).await?;
        match existing {
          None => results.push(RespValue::Int(-2)),
          Some(raw) => {
            let (cur_exp, payload) = decode_hash_value(&raw);
            if is_field_expired(cur_exp, now_ms) {
              entries.push(UpsertKV::delete(h_k));
              results.push(RespValue::Int(-2));
            } else if !hexpire_condition_passes(condition, cur_exp, target_expire_ms) {
              results.push(RespValue::Int(0));
            } else if is_immediate {
              entries.push(UpsertKV::delete(h_k));
              results.push(RespValue::Int(2));
            } else {
              let val_enc = encode_hash_value(payload, target_expire_ms);
              entries.push(UpsertKV::insert(h_k, val_enc));
              results.push(RespValue::Int(1));
            }
          }
        }
      }
      if !entries.is_empty() {
        node.batch_write(BatchWriteReq { entries }).await?;
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::HExpireAt {
      key,
      unix_time_sec,
      condition,
      fields,
    } => {
      let target_expire_ms = (unix_time_sec as u64).saturating_mul(1000);
      let is_immediate = target_expire_ms <= now_ms;

      let mut results = Vec::with_capacity(fields.len());
      let mut entries = Vec::new();

      for f in fields {
        let h_k = kc.hash_item(&key, &f);
        let existing = node.read(GetKVReq { key: h_k.clone() }).await?;
        match existing {
          None => results.push(RespValue::Int(-2)),
          Some(raw) => {
            let (cur_exp, payload) = decode_hash_value(&raw);
            if is_field_expired(cur_exp, now_ms) {
              entries.push(UpsertKV::delete(h_k));
              results.push(RespValue::Int(-2));
            } else if !hexpire_condition_passes(condition, cur_exp, target_expire_ms) {
              results.push(RespValue::Int(0));
            } else if is_immediate {
              entries.push(UpsertKV::delete(h_k));
              results.push(RespValue::Int(2));
            } else {
              let val_enc = encode_hash_value(payload, target_expire_ms);
              entries.push(UpsertKV::insert(h_k, val_enc));
              results.push(RespValue::Int(1));
            }
          }
        }
      }
      if !entries.is_empty() {
        node.batch_write(BatchWriteReq { entries }).await?;
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::HPExpireAt {
      key,
      unix_time_ms,
      condition,
      fields,
    } => {
      let target_expire_ms = unix_time_ms as u64;
      let is_immediate = target_expire_ms <= now_ms;

      let mut results = Vec::with_capacity(fields.len());
      let mut entries = Vec::new();

      for f in fields {
        let h_k = kc.hash_item(&key, &f);
        let existing = node.read(GetKVReq { key: h_k.clone() }).await?;
        match existing {
          None => results.push(RespValue::Int(-2)),
          Some(raw) => {
            let (cur_exp, payload) = decode_hash_value(&raw);
            if is_field_expired(cur_exp, now_ms) {
              entries.push(UpsertKV::delete(h_k));
              results.push(RespValue::Int(-2));
            } else if !hexpire_condition_passes(condition, cur_exp, target_expire_ms) {
              results.push(RespValue::Int(0));
            } else if is_immediate {
              entries.push(UpsertKV::delete(h_k));
              results.push(RespValue::Int(2));
            } else {
              let val_enc = encode_hash_value(payload, target_expire_ms);
              entries.push(UpsertKV::insert(h_k, val_enc));
              results.push(RespValue::Int(1));
            }
          }
        }
      }
      if !entries.is_empty() {
        node.batch_write(BatchWriteReq { entries }).await?;
      }
      Ok(RespValue::Arr(results))
    }

    // ---- 8. 字段 TTL / 过期时间查看 (HTTL / HPTTL / HEXPIRETIME / HPEXPIRETIME) ----
    RedisCommand::HTtl { key, fields } => {
      let mut results = Vec::with_capacity(fields.len());
      for f in fields {
        let h_k = kc.hash_item(&key, &f);
        let val = node.read(GetKVReq { key: h_k }).await?;
        match val {
          None => results.push(RespValue::Int(-2)),
          Some(raw) => {
            let (exp, _) = decode_hash_value(&raw);
            if is_field_expired(exp, now_ms) {
              results.push(RespValue::Int(-2));
            } else if exp == 0 {
              results.push(RespValue::Int(-1));
            } else {
              let remain_sec = (exp - now_ms) / 1000;
              results.push(RespValue::Int(remain_sec as i64));
            }
          }
        }
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::HPTtl { key, fields } => {
      let mut results = Vec::with_capacity(fields.len());
      for f in fields {
        let h_k = kc.hash_item(&key, &f);
        let val = node.read(GetKVReq { key: h_k }).await?;
        match val {
          None => results.push(RespValue::Int(-2)),
          Some(raw) => {
            let (exp, _) = decode_hash_value(&raw);
            if is_field_expired(exp, now_ms) {
              results.push(RespValue::Int(-2));
            } else if exp == 0 {
              results.push(RespValue::Int(-1));
            } else {
              let remain_ms = exp - now_ms;
              results.push(RespValue::Int(remain_ms as i64));
            }
          }
        }
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::HExpireTime { key, fields } => {
      let mut results = Vec::with_capacity(fields.len());
      for f in fields {
        let h_k = kc.hash_item(&key, &f);
        let val = node.read(GetKVReq { key: h_k }).await?;
        match val {
          None => results.push(RespValue::Int(-2)),
          Some(raw) => {
            let (exp, _) = decode_hash_value(&raw);
            if is_field_expired(exp, now_ms) {
              results.push(RespValue::Int(-2));
            } else if exp == 0 {
              results.push(RespValue::Int(-1));
            } else {
              results.push(RespValue::Int((exp / 1000) as i64));
            }
          }
        }
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::HPExpireTime { key, fields } => {
      let mut results = Vec::with_capacity(fields.len());
      for f in fields {
        let h_k = kc.hash_item(&key, &f);
        let val = node.read(GetKVReq { key: h_k }).await?;
        match val {
          None => results.push(RespValue::Int(-2)),
          Some(raw) => {
            let (exp, _) = decode_hash_value(&raw);
            if is_field_expired(exp, now_ms) {
              results.push(RespValue::Int(-2));
            } else if exp == 0 {
              results.push(RespValue::Int(-1));
            } else {
              results.push(RespValue::Int(exp as i64));
            }
          }
        }
      }
      Ok(RespValue::Arr(results))
    }

    // ---- 9. 移除字段过期 (HPERSIST) 与扩展命令 (HSETEXPIRE / HGETEX / HRANGEBYLEX) ----
    RedisCommand::HPersist { key, fields } => {
      let mut results = Vec::with_capacity(fields.len());
      let mut entries = Vec::new();

      for f in fields {
        let h_k = kc.hash_item(&key, &f);
        let val = node.read(GetKVReq { key: h_k.clone() }).await?;
        match val {
          None => results.push(RespValue::Int(-2)),
          Some(raw) => {
            let (exp, payload) = decode_hash_value(&raw);
            if is_field_expired(exp, now_ms) {
              entries.push(UpsertKV::delete(h_k));
              results.push(RespValue::Int(-2));
            } else if exp == 0 {
              results.push(RespValue::Int(-1));
            } else {
              let val_enc = encode_hash_value(payload, 0);
              entries.push(UpsertKV::insert(h_k, val_enc));
              results.push(RespValue::Int(1));
            }
          }
        }
      }
      if !entries.is_empty() {
        node.batch_write(BatchWriteReq { entries }).await?;
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::HSetExpire {
      key,
      ttl_sec,
      fields,
    } => {
      let target_expire_ms = now_ms.saturating_add(ttl_sec.saturating_mul(1000));
      let mut results = Vec::with_capacity(fields.len());
      let mut entries = Vec::new();

      for f in fields {
        let h_k = kc.hash_item(&key, &f);
        let existing = node.read(GetKVReq { key: h_k.clone() }).await?;
        match existing {
          None => results.push(RespValue::Int(-2)),
          Some(raw) => {
            let (cur_exp, payload) = decode_hash_value(&raw);
            if is_field_expired(cur_exp, now_ms) {
              entries.push(UpsertKV::delete(h_k));
              results.push(RespValue::Int(-2));
            } else {
              let val_enc = encode_hash_value(payload, target_expire_ms);
              entries.push(UpsertKV::insert(h_k, val_enc));
              results.push(RespValue::Int(1));
            }
          }
        }
      }
      if !entries.is_empty() {
        node.batch_write(BatchWriteReq { entries }).await?;
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::HGetEx { key, field } => {
      let h_k = kc.hash_item(&key, &field);
      let val = node.read(GetKVReq { key: h_k }).await?;
      match val {
        Some(raw) => {
          let (exp, payload) = decode_hash_value(&raw);
          if is_field_expired(exp, now_ms) {
            Ok(RespValue::Null)
          } else {
            Ok(RespValue::Blob(payload.to_vec()))
          }
        }
        None => Ok(RespValue::Null),
      }
    }
    RedisCommand::HRangeByLex {
      key,
      min,
      max,
      offset,
      count,
    } => {
      let lex_spec =
        wedb_embed::RangeLexSpec::from_bounds(min.as_bytes(), max.as_bytes(), offset, count)
          .map_err(|e| Error::invalid_data(e.to_string()))?;

      let prefix = kc.hash_prefix(&key);
      let items = node
        .scan_prefix(ScanPrefixReq {
          prefix: prefix.clone(),
        })
        .await?;

      let mut matched = Vec::new();
      for (k, v) in items {
        let (exp, payload) = decode_hash_value(&v);
        if !is_field_expired(exp, now_ms) {
          let field_bytes = &k[prefix.len()..];
          if lex_spec.check(field_bytes) {
            matched.push((field_bytes.to_vec(), payload.to_vec()));
          }
        }
      }

      let start = offset.min(matched.len());
      let take_len = count.unwrap_or(matched.len());
      let end = (start + take_len).min(matched.len());

      let mut results = Vec::with_capacity((end - start) * 2);
      for (f, v) in &matched[start..end] {
        results.push(RespValue::Blob(f.clone()));
        results.push(RespValue::Blob(v.clone()));
      }
      Ok(RespValue::Arr(results))
    }

    _ => Err(Error::redis("Command not matched in handle_hash")),
  }
}
