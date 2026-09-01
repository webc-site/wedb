use rapidhash::RapidHashSet as HashSet;
use std::str;
use std::sync::Arc;

use super::conn::collect_key_storage_entries;
use super::context::{ConnectionContext, KeyComposer, SetMeta, matches_glob_bytes};
use super::search::sync_search_indices_on_doc_update;
use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::redis::cmd::RedisCommand;
use crate::redis::protocol::RespValue;
use crate::redis::resp_util::int_to_blob;
use crate::util::now_millis;
use wedb_raft::types::{BatchWriteReq, GetKVReq, ScanPrefixReq, UpsertKV};

pub async fn handle_key(
  node: &Arc<RaftNode>,
  ctx: &mut ConnectionContext,
  cmd: RedisCommand,
) -> Result<RespValue> {
  let sm = node.state_machine();
  let kc = ctx.key_composer();

  match cmd {
    RedisCommand::Del(keys) | RedisCommand::Unlink(keys) => {
      let mut entries = Vec::new();
      let mut deleted_count = 0;

      for k in keys {
        let storage_entries = collect_key_storage_entries(node, &kc, &k).await?;
        if !storage_entries.is_empty() {
          deleted_count += 1;
          for (storage_k, _) in storage_entries {
            entries.push(UpsertKV::delete(storage_k.clone()));
            sm.remove_ttl(&storage_k).ok();
          }
          let raw_k = kc.raw_key(&k);
          sm.remove_ttl(&raw_k).ok();
          sync_search_indices_on_doc_update(node, &kc, &k, None)
            .await
            .ok();
        }
      }

      if !entries.is_empty() {
        node.batch_write(BatchWriteReq { entries }).await?;
      }
      Ok(RespValue::Int(deleted_count))
    }
    RedisCommand::Exists(keys) => {
      let mut count = 0;
      for k in keys {
        let raw_k = kc.raw_key(&k);
        if !sm.is_expired(&raw_k) {
          let storage_entries = collect_key_storage_entries(node, &kc, &k).await?;
          if !storage_entries.is_empty() {
            count += 1;
          }
        }
      }
      Ok(RespValue::Int(count))
    }
    RedisCommand::Keys(pattern) => {
      let all = sm.scan_all().map_err(|e| Error::internal(e.to_string()))?;
      let pat_bytes = pattern.as_bytes();
      let mut results = Vec::new();
      let mut seen = HashSet::default();
      for (k, _) in all {
        if let Some(user_k) = kc.extract_user_key(&k)
          && matches_glob_bytes(pat_bytes, user_k)
          && seen.insert(user_k.to_vec())
        {
          results.push(RespValue::Blob(user_k.to_vec()));
        }
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::Scan {
      cursor,
      pattern,
      count,
    } => {
      let all = sm.scan_all().map_err(|e| Error::internal(e.to_string()))?;
      let limit = count.unwrap_or(10);
      let pat = pattern.unwrap_or_else(|| "*".to_string());
      let pat_bytes = pat.as_bytes();

      let mut matched = Vec::new();
      let mut seen = HashSet::default();
      for (k, _) in all {
        if let Some(user_k) = kc.extract_user_key(&k)
          && matches_glob_bytes(pat_bytes, user_k)
          && seen.insert(user_k.to_vec())
        {
          matched.push(RespValue::Blob(user_k.to_vec()));
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
        int_to_blob(next_cursor),
        RespValue::Arr(slice),
      ]))
    }
    RedisCommand::ScanPrefix { prefix, count } => {
      let full_prefix = kc.raw_key_bytes(&prefix);
      let raw_results = node
        .scan_prefix(ScanPrefixReq {
          prefix: full_prefix.into_owned(),
        })
        .await?;
      let limit = count.unwrap_or(raw_results.len());
      let mut list = Vec::with_capacity(limit.min(raw_results.len()) * 2);
      for (k, v) in raw_results.into_iter().take(limit) {
        if let Some(u_k) = kc.extract_user_key(&k) {
          list.push(RespValue::Blob(u_k.to_vec()));
          list.push(RespValue::Blob(v));
        }
      }
      Ok(RespValue::Arr(list))
    }
    RedisCommand::DbSize => {
      let all = sm.scan_all().map_err(|e| Error::internal(e.to_string()))?;
      let mut seen = HashSet::default();
      for (k, _) in all {
        if let Some(user_k) = kc.extract_user_key(&k) {
          seen.insert(user_k.to_vec());
        }
      }
      Ok(RespValue::Int(seen.len() as i64))
    }
    RedisCommand::FlushDb => {
      let all = sm.scan_all().map_err(|e| Error::internal(e.to_string()))?;
      let mut entries = Vec::new();
      for (k, _) in all {
        if kc.is_key_in_ns(&k) {
          let k_str = unsafe { String::from_utf8_unchecked(k) };
          entries.push(UpsertKV::delete(k_str.clone()));
          sm.remove_ttl(&k_str).ok();
        }
      }
      if !entries.is_empty() {
        node.batch_write(BatchWriteReq { entries }).await?;
      }
      Ok(RespValue::ok())
    }
    RedisCommand::FlushAll => {
      let all = sm.scan_all().map_err(|e| Error::internal(e.to_string()))?;
      let mut entries = Vec::with_capacity(all.len());
      for (k, _) in all {
        if !k.starts_with(b"_meta:") && !k.starts_with(b"_ttl:") && !k.starts_with(b"_raft:") {
          let k_str = unsafe { String::from_utf8_unchecked(k) };
          entries.push(UpsertKV::delete(k_str.clone()));
          sm.remove_ttl(&k_str).ok();
        }
      }
      if !entries.is_empty() {
        node.batch_write(BatchWriteReq { entries }).await?;
      }
      Ok(RespValue::ok())
    }
    RedisCommand::Type(key) => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Simple("none".to_string()));
      }
      if node
        .read(GetKVReq {
          key: kc.json_meta(&key),
        })
        .await?
        .is_some()
        || node
          .read(GetKVReq {
            key: kc.json_key(&key),
          })
          .await?
          .is_some()
      {
        return Ok(RespValue::Simple("ReJSON-RL".to_string()));
      }
      if !node
        .scan_prefix(ScanPrefixReq {
          prefix: kc.hash_prefix(&key),
        })
        .await?
        .is_empty()
      {
        return Ok(RespValue::Simple("hash".to_string()));
      }
      if node
        .read(GetKVReq {
          key: kc.set_meta(&key),
        })
        .await?
        .is_some()
        || !node
          .scan_prefix(ScanPrefixReq {
            prefix: kc.set_prefix(&key),
          })
          .await?
          .is_empty()
      {
        return Ok(RespValue::Simple("set".to_string()));
      }
      if !node
        .scan_prefix(ScanPrefixReq {
          prefix: kc.zset_prefix(&key),
        })
        .await?
        .is_empty()
      {
        return Ok(RespValue::Simple("zset".to_string()));
      }
      if node
        .read(GetKVReq {
          key: kc.list_meta(&key),
        })
        .await?
        .is_some()
      {
        return Ok(RespValue::Simple("list".to_string()));
      }
      if node
        .read(GetKVReq {
          key: kc.stream_meta(&key),
        })
        .await?
        .is_some()
        || !node
          .scan_prefix(ScanPrefixReq {
            prefix: kc.stream_prefix(&key),
          })
          .await?
          .is_empty()
      {
        return Ok(RespValue::Simple("stream".to_string()));
      }
      if node
        .read(GetKVReq {
          key: kc.hll_meta(&key),
        })
        .await?
        .is_some()
        || node
          .read(GetKVReq {
            key: kc.hll_key(&key),
          })
          .await?
          .is_some()
      {
        return Ok(RespValue::Simple("hyperloglog".to_string()));
      }
      if node
        .read(GetKVReq {
          key: kc.bf_meta(&key),
        })
        .await?
        .is_some()
      {
        return Ok(RespValue::Simple("MBbloom--".to_string()));
      }
      if node
        .read(GetKVReq {
          key: kc.cf_meta(&key),
        })
        .await?
        .is_some()
      {
        return Ok(RespValue::Simple("MBbloomCF".to_string()));
      }
      if node
        .read(GetKVReq {
          key: kc.tdigest_meta(&key),
        })
        .await?
        .is_some()
      {
        return Ok(RespValue::Simple("TDIS-TYPE".to_string()));
      }
      if node
        .read(GetKVReq {
          key: kc.ts_meta(&key),
        })
        .await?
        .is_some()
        || !node
          .scan_prefix(ScanPrefixReq {
            prefix: kc.ts_prefix(&key),
          })
          .await?
          .is_empty()
      {
        return Ok(RespValue::Simple("timeseries".to_string()));
      }
      if node
        .read(GetKVReq {
          key: kc.si_meta(&key),
        })
        .await?
        .is_some()
        || !node
          .scan_prefix(ScanPrefixReq {
            prefix: kc.si_prefix(&key),
          })
          .await?
          .is_empty()
      {
        return Ok(RespValue::Simple("sortedint".to_string()));
      }
      if node.read(GetKVReq { key: raw_k }).await?.is_some() {
        return Ok(RespValue::Simple("string".to_string()));
      }
      Ok(RespValue::Simple("none".to_string()))
    }
    RedisCommand::Ttl(key) => {
      let raw_k = kc.raw_key(&key);
      if let Ok(Some(exp)) = sm.get_ttl_expire_at(&raw_k) {
        let now = now_millis();
        if now >= exp {
          Ok(RespValue::Int(-2))
        } else {
          Ok(RespValue::Int((exp - now).div_ceil(1000) as i64))
        }
      } else if node.read(GetKVReq { key: raw_k }).await?.is_some() {
        Ok(RespValue::Int(-1))
      } else {
        Ok(RespValue::Int(-2))
      }
    }
    RedisCommand::Pttl(key) => {
      let raw_k = kc.raw_key(&key);
      if let Ok(Some(exp)) = sm.get_ttl_expire_at(&raw_k) {
        let now = now_millis();
        if now >= exp {
          Ok(RespValue::Int(-2))
        } else {
          Ok(RespValue::Int((exp - now) as i64))
        }
      } else if node.read(GetKVReq { key: raw_k }).await?.is_some() {
        Ok(RespValue::Int(-1))
      } else {
        Ok(RespValue::Int(-2))
      }
    }
    RedisCommand::ExpireTime(key) => {
      let raw_k = kc.raw_key(&key);
      if let Ok(Some(exp)) = sm.get_ttl_expire_at(&raw_k) {
        Ok(RespValue::Int((exp / 1000) as i64))
      } else if node.read(GetKVReq { key: raw_k }).await?.is_some() {
        Ok(RespValue::Int(-1))
      } else {
        Ok(RespValue::Int(-2))
      }
    }
    RedisCommand::PExpireTime(key) => {
      let raw_k = kc.raw_key(&key);
      if let Ok(Some(exp)) = sm.get_ttl_expire_at(&raw_k) {
        Ok(RespValue::Int(exp as i64))
      } else if node.read(GetKVReq { key: raw_k }).await?.is_some() {
        Ok(RespValue::Int(-1))
      } else {
        Ok(RespValue::Int(-2))
      }
    }
    RedisCommand::Expire(key, sec) => {
      let raw_k = kc.raw_key(&key);
      if let Some(val) = node.read(GetKVReq { key: raw_k.clone() }).await? {
        let expire_at = now_millis() + sec * 1000;
        let entries = vec![UpsertKV::insert_with_ttl(raw_k, val, Some(expire_at))];
        node.batch_write(BatchWriteReq { entries }).await?;
        Ok(RespValue::Int(1))
      } else {
        Ok(RespValue::Int(0))
      }
    }
    RedisCommand::PExpire(key, ms) => {
      let raw_k = kc.raw_key(&key);
      if let Some(val) = node.read(GetKVReq { key: raw_k.clone() }).await? {
        let expire_at = now_millis() + ms;
        let entries = vec![UpsertKV::insert_with_ttl(raw_k, val, Some(expire_at))];
        node.batch_write(BatchWriteReq { entries }).await?;
        Ok(RespValue::Int(1))
      } else {
        Ok(RespValue::Int(0))
      }
    }
    RedisCommand::ExpireAt(key, ts_sec) => {
      let raw_k = kc.raw_key(&key);
      if let Some(val) = node.read(GetKVReq { key: raw_k.clone() }).await? {
        let expire_at = ts_sec * 1000;
        let entries = vec![UpsertKV::insert_with_ttl(raw_k, val, Some(expire_at))];
        node.batch_write(BatchWriteReq { entries }).await?;
        Ok(RespValue::Int(1))
      } else {
        Ok(RespValue::Int(0))
      }
    }
    RedisCommand::PExpireAt(key, ts_ms) => {
      let raw_k = kc.raw_key(&key);
      if let Some(val) = node.read(GetKVReq { key: raw_k.clone() }).await? {
        let expire_at = ts_ms;
        let entries = vec![UpsertKV::insert_with_ttl(raw_k, val, Some(expire_at))];
        node.batch_write(BatchWriteReq { entries }).await?;
        Ok(RespValue::Int(1))
      } else {
        Ok(RespValue::Int(0))
      }
    }
    RedisCommand::Persist(key) => {
      let raw_k = kc.raw_key(&key);
      if sm.get_ttl_expire_at(&raw_k).ok().flatten().is_some()
        && let Some(val) = node.read(GetKVReq { key: raw_k.clone() }).await?
      {
        let entries = vec![UpsertKV::insert_with_ttl(raw_k, val, Some(0))];
        node.batch_write(BatchWriteReq { entries }).await?;
        Ok(RespValue::Int(1))
      } else {
        Ok(RespValue::Int(0))
      }
    }
    RedisCommand::Rename(src, dst) => {
      let src_k = kc.raw_key(&src);
      let dst_k = kc.raw_key(&dst);
      let val = node.read(GetKVReq { key: src_k.clone() }).await?;
      match val {
        Some(v) => {
          let exp = sm.get_ttl_expire_at(&src_k).ok().flatten();
          let entries = vec![
            UpsertKV::insert_with_ttl(dst_k, v, exp.or(Some(0))),
            UpsertKV::delete(src_k),
          ];
          node.batch_write(BatchWriteReq { entries }).await?;
          Ok(RespValue::ok())
        }
        None => Err(Error::invalid_data("ERR no such key")),
      }
    }
    RedisCommand::RenameNx(src, dst) => {
      let src_k = kc.raw_key(&src);
      let dst_k = kc.raw_key(&dst);
      if node.read(GetKVReq { key: dst_k.clone() }).await?.is_some() {
        return Ok(RespValue::Int(0));
      }
      let val = node.read(GetKVReq { key: src_k.clone() }).await?;
      match val {
        Some(v) => {
          let entries = vec![
            UpsertKV::insert(dst_k.clone(), v),
            UpsertKV::delete(src_k.clone()),
          ];
          if let Ok(Some(exp)) = sm.get_ttl_expire_at(&src_k) {
            sm.set_ttl(&dst_k, exp).ok();
            sm.remove_ttl(&src_k).ok();
          }
          node.batch_write(BatchWriteReq { entries }).await?;
          Ok(RespValue::Int(1))
        }
        None => Err(Error::invalid_data("ERR no such key")),
      }
    }
    RedisCommand::Copy {
      src,
      dst,
      db,
      replace,
    } => {
      let target_ns = if let Some(target_db) = db {
        if target_db == 0 {
          "default".to_string()
        } else {
          format!("db{target_db}")
        }
      } else {
        ctx.namespace.to_string()
      };
      let target_kc = KeyComposer::new(&target_ns);

      let src_entries = collect_key_storage_entries(node, &kc, &src).await?;
      if src_entries.is_empty() {
        return Ok(RespValue::Int(0));
      }

      let dst_entries = collect_key_storage_entries(node, &target_kc, &dst).await?;
      if !replace && !dst_entries.is_empty() {
        return Ok(RespValue::Int(0));
      }

      let mut entries = Vec::with_capacity(src_entries.len() + dst_entries.len());
      for (dst_k, _) in dst_entries {
        entries.push(UpsertKV::delete(dst_k));
      }
      for (src_k, v) in src_entries {
        if let Some(dst_k) = kc.transform_key_to_target(src_k.as_bytes(), &target_kc) {
          entries.push(UpsertKV::insert(dst_k, v));
        }
      }

      let src_raw_k = kc.raw_key(&src);
      let dst_raw_k = target_kc.raw_key(&dst);
      if let Ok(Some(exp)) = sm.get_ttl_expire_at(&src_raw_k) {
        sm.set_ttl(&dst_raw_k, exp).ok();
      }

      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::Int(1))
    }
    RedisCommand::RandomKey => {
      let all = sm.scan_all().map_err(|e| Error::internal(e.to_string()))?;
      for (k, _) in all {
        if let Some(u_k) = kc.extract_user_key(&k) {
          return Ok(RespValue::Blob(u_k.to_vec()));
        }
      }
      Ok(RespValue::Null)
    }
    RedisCommand::Touch(keys) => {
      let mut touched = 0;
      for k in keys {
        let raw_k = kc.raw_key(&k);
        if !sm.is_expired(&raw_k) {
          let entries = collect_key_storage_entries(node, &kc, &k).await?;
          if !entries.is_empty() {
            touched += 1;
          }
        }
      }
      Ok(RespValue::Int(touched))
    }
    RedisCommand::Sort {
      key,
      by: _,
      offset,
      count,
      patterns: _,
      desc,
      alpha,
      store,
    } => {
      sort_key(
        node,
        &kc,
        &key,
        SortOpts {
          offset,
          count,
          desc,
          alpha,
          store: store.as_deref(),
        },
      )
      .await
    }
    RedisCommand::SortRo {
      key,
      by: _,
      offset,
      count,
      patterns: _,
      desc,
      alpha,
    } => {
      sort_key(
        node,
        &kc,
        &key,
        SortOpts {
          offset,
          count,
          desc,
          alpha,
          store: None,
        },
      )
      .await
    }
    RedisCommand::Object { subcmd, key } => match subcmd.to_ascii_lowercase().as_str() {
      "help" => Ok(RespValue::Arr(vec![
        RespValue::Blob(b"OBJECT <subcmd> [<arg> [value] ...]. Subcmds are:".to_vec()),
        RespValue::Blob(b"ENCODING <key> -- Return internal encoding used for the key.".to_vec()),
        RespValue::Blob(b"FREQ <key> -- Return logarithmic access frequency.".to_vec()),
        RespValue::Blob(b"IDLETIME <key> -- Return idle time of the key.".to_vec()),
        RespValue::Blob(b"REFCOUNT <key> -- Return reference count of the key.".to_vec()),
        RespValue::Blob(b"HELP -- Print this help.".to_vec()),
      ])),
      "freq" => Ok(RespValue::Int(0)),
      "idletime" => Ok(RespValue::Int(0)),
      "refcount" => Ok(RespValue::Int(1)),
      "encoding" => {
        let raw_k = kc.raw_key(&key);
        if sm.is_expired(&raw_k) {
          return Ok(RespValue::Null);
        }
        if node
          .read(GetKVReq {
            key: kc.set_meta(&key),
          })
          .await?
          .is_some()
        {
          Ok(RespValue::Blob(b"hashtable".to_vec()))
        } else if node
          .read(GetKVReq {
            key: kc.zset_meta(&key),
          })
          .await?
          .is_some()
        {
          Ok(RespValue::Blob(b"skiplist".to_vec()))
        } else if node
          .read(GetKVReq {
            key: kc.list_meta(&key),
          })
          .await?
          .is_some()
        {
          Ok(RespValue::Blob(b"quicklist".to_vec()))
        } else if node.read(GetKVReq { key: raw_k }).await?.is_some() {
          Ok(RespValue::Blob(b"raw".to_vec()))
        } else {
          Ok(RespValue::Null)
        }
      }
      _ => Ok(RespValue::Simple("raw".to_string())),
    },
    RedisCommand::KMetaData(key) => {
      let raw_k = kc.raw_key(&key);
      let exp = sm.get_ttl_expire_at(&raw_k).ok().flatten().unwrap_or(0);
      if let Some(set_meta_bytes) = node
        .read(GetKVReq {
          key: kc.set_meta(&key),
        })
        .await?
        && let Some(meta) = SetMeta::decode(&set_meta_bytes)
      {
        return Ok(RespValue::Arr(vec![
          RespValue::Simple("type".to_string()),
          RespValue::Simple("set".to_string()),
          RespValue::Simple("expire".to_string()),
          RespValue::Int(meta.base.expire_at as i64),
          RespValue::Simple("size".to_string()),
          RespValue::Int(meta.base.size as i64),
          RespValue::Simple("version".to_string()),
          RespValue::Int(meta.base.version as i64),
        ]));
      }
      Ok(RespValue::Arr(vec![
        RespValue::Simple("type".to_string()),
        RespValue::Simple("string".to_string()),
        RespValue::Simple("expire".to_string()),
        RespValue::Int(exp as i64),
      ]))
    }
    _ => Err(Error::redis("Command not matched in handle_key")),
  }
}

/// SORT / SORT_RO 排序参数
#[derive(Debug, Clone, Copy)]
struct SortOpts<'a> {
  offset: usize,
  count: Option<usize>,
  desc: bool,
  alpha: bool,
  store: Option<&'a str>,
}

/// 键排序核心实现 (SORT / SORT_RO)
async fn sort_key(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  opts: SortOpts<'_>,
) -> Result<RespValue> {
  let SortOpts {
    offset,
    count,
    desc,
    alpha,
    store,
  } = opts;
  let mut elements: Vec<Vec<u8>> = Vec::new();

  // 尝试从 Set 获取
  let set_prefix = kc.set_prefix(key);
  let set_items = node
    .scan_prefix(ScanPrefixReq {
      prefix: set_prefix.clone(),
    })
    .await?;
  if !set_items.is_empty() {
    for (k, _) in set_items {
      if let Some(m) = k.strip_prefix(set_prefix.as_slice()) {
        elements.push(m.to_vec());
      }
    }
  } else {
    // 尝试从 List 获取
    let list_items = node
      .scan_prefix(ScanPrefixReq {
        prefix: kc.list_prefix(key),
      })
      .await?;
    if !list_items.is_empty() {
      for (_, v) in list_items {
        elements.push(v);
      }
    } else {
      // 尝试从 ZSet 获取
      let z_prefix = kc.zset_prefix(key);
      let zset_items = node
        .scan_prefix(ScanPrefixReq {
          prefix: z_prefix.clone(),
        })
        .await?;
      for (k, _) in zset_items {
        if let Some(m) = k.strip_prefix(z_prefix.as_slice()) {
          elements.push(m.to_vec());
        }
      }
    }
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

  let sliced: Vec<Vec<u8>> = elements
    .into_iter()
    .skip(offset)
    .take(count.unwrap_or(usize::MAX))
    .collect();

  if let Some(dst) = store {
    let stored_len = sliced.len() as i64;
    let mut entries = Vec::with_capacity(sliced.len() + 1);
    let dst_meta_k = kc.list_meta(dst);
    let mut meta = super::context::ListMeta::new(0, now_millis());
    let mut idx = meta.tail;
    for el in sliced {
      let item_k = kc.list_item(dst, idx);
      entries.push(UpsertKV::insert(item_k, el));
      idx = idx.wrapping_add(1);
    }
    meta.base.size = stored_len as u64;
    meta.tail = idx;
    entries.push(UpsertKV::insert(dst_meta_k, meta.encode().to_vec()));
    node.batch_write(BatchWriteReq { entries }).await?;
    Ok(RespValue::Int(stored_len))
  } else {
    Ok(RespValue::Arr(
      sliced.into_iter().map(RespValue::Blob).collect(),
    ))
  }
}
