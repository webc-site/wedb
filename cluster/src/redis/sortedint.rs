use rapidhash::RapidHashSet;
use std::collections::BTreeSet;
use std::sync::Arc;

use super::handler::context::{ConnectionContext, SortedintMeta};
use crate::error::Result;
use crate::node::RaftNode;
use crate::redis::cmd::RedisCommand;
use crate::redis::protocol::RespValue;
use crate::redis::resp_util::int_to_blob;
use crate::util::now_millis;
use wedb_raft::types::{BatchWriteReq, GetKVReq, ScanPrefixReq, UpsertKV};

pub use wedb_embed::sortedint::{
  SortedintRangeSpec as SortedIntRangeSpec, decode_hex_u64, encode_hex_u64, parse_range_spec,
};

/// 内存辅助有序 64 位整型集合（用于单元测试与快速计算）
#[derive(Debug, Clone, Default)]
pub struct SortedIntSet {
  pub members: BTreeSet<u64>,
}

impl SortedIntSet {
  pub fn new() -> Self {
    Self {
      members: BTreeSet::new(),
    }
  }

  pub fn from_slice(items: &[u64]) -> Self {
    let mut s = BTreeSet::new();
    for &item in items {
      s.insert(item);
    }
    Self { members: s }
  }

  pub fn add(&mut self, id: u64) -> bool {
    self.members.insert(id)
  }

  pub fn remove(&mut self, id: u64) -> bool {
    self.members.remove(&id)
  }

  pub fn card(&self) -> usize {
    self.members.len()
  }

  pub fn exists(&self, id: u64) -> bool {
    self.members.contains(&id)
  }

  pub fn range(&self, cursor: u64, limit: usize) -> Vec<u64> {
    self
      .members
      .range(cursor..)
      .filter(|&&id| cursor == 0 || id > cursor)
      .take(limit)
      .copied()
      .collect()
  }

  pub fn rev_range(&self, cursor: u64, limit: usize) -> Vec<u64> {
    let max_bound = if cursor == 0 { u64::MAX } else { cursor };
    self
      .members
      .range(..=max_bound)
      .rev()
      .filter(|&&id| cursor == 0 || id < cursor)
      .take(limit)
      .copied()
      .collect()
  }
}

/// 处理 SortedInt 全套 Redis 命令（基于 LSM-Tree 原生流式顺序扫描）
pub async fn handle_sortedint(
  node: &Arc<RaftNode>,
  ctx: &mut ConnectionContext,
  cmd: RedisCommand,
) -> Result<RespValue> {
  let kc = ctx.key_composer();
  let sm = node.state_machine();
  let now = now_millis();

  match cmd {
    RedisCommand::SiAdd(key, ids) => {
      let raw_k = kc.raw_key(&key);
      let mut metadata = if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.si_meta(&key),
        })
        .await?
        && let Some(m) = SortedintMeta::decode(&meta_bytes)
      {
        if m.is_expired(now) || sm.is_expired(&raw_k) {
          SortedintMeta::new(0, m.base.version + 1, 0)
        } else {
          m
        }
      } else {
        SortedintMeta::new(0, 0, 0)
      };

      let mut unique_ids = RapidHashSet::with_capacity_and_hasher(ids.len(), Default::default());
      for id in ids {
        unique_ids.insert(id);
      }

      let mut entries = Vec::with_capacity(unique_ids.len() + 1);
      let mut added_count = 0u64;

      for id in unique_ids {
        let si_k = kc.si_item(&key, id);
        if node.read(GetKVReq { key: si_k.clone() }).await?.is_none() {
          added_count += 1;
          entries.push(UpsertKV::insert(si_k, Vec::new()));
        }
      }

      if added_count > 0 {
        metadata.base.size += added_count;
        entries.push(UpsertKV::insert(
          kc.si_meta(&key),
          metadata.encode().to_vec(),
        ));
        node.batch_write(BatchWriteReq { entries }).await?;
      }

      Ok(RespValue::Int(added_count as i64))
    }

    RedisCommand::SiRem(key, ids) => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Int(0));
      }

      let meta_opt = if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.si_meta(&key),
        })
        .await?
      {
        SortedintMeta::decode(&meta_bytes)
      } else {
        None
      };

      let mut metadata = match meta_opt {
        Some(m) if !m.is_expired(now) && !m.is_empty() => m,
        _ => return Ok(RespValue::Int(0)),
      };

      let mut unique_ids = RapidHashSet::with_capacity_and_hasher(ids.len(), Default::default());
      for id in ids {
        unique_ids.insert(id);
      }

      let mut entries = Vec::with_capacity(unique_ids.len() + 1);
      let mut removed_count = 0u64;

      for id in unique_ids {
        let si_k = kc.si_item(&key, id);
        if node.read(GetKVReq { key: si_k.clone() }).await?.is_some() {
          removed_count += 1;
          entries.push(UpsertKV::delete(si_k));
        }
      }

      if removed_count > 0 {
        metadata.base.size = metadata.base.size.saturating_sub(removed_count);
        if metadata.base.size == 0 {
          entries.push(UpsertKV::delete(kc.si_meta(&key)));
        } else {
          entries.push(UpsertKV::insert(
            kc.si_meta(&key),
            metadata.encode().to_vec(),
          ));
        }
        node.batch_write(BatchWriteReq { entries }).await?;
      }

      Ok(RespValue::Int(removed_count as i64))
    }

    RedisCommand::SiCard(key) => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Int(0));
      }

      if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.si_meta(&key),
        })
        .await?
        && let Some(m) = SortedintMeta::decode(&meta_bytes)
        && !m.is_expired(now)
      {
        Ok(RespValue::Int(m.size() as i64))
      } else {
        Ok(RespValue::Int(0))
      }
    }

    RedisCommand::SiExists(key, ids) => {
      let raw_k = kc.raw_key(&key);
      let key_expired = sm.is_expired(&raw_k);

      let meta_valid = if !key_expired
        && let Some(meta_bytes) = node
          .read(GetKVReq {
            key: kc.si_meta(&key),
          })
          .await?
        && let Some(m) = SortedintMeta::decode(&meta_bytes)
      {
        !m.is_expired(now) && !m.is_empty()
      } else {
        false
      };

      let mut results = Vec::with_capacity(ids.len());
      if !meta_valid {
        results.resize(ids.len(), RespValue::Int(0));
      } else {
        for id in ids {
          let si_k = kc.si_item(&key, id);
          let exists = node.read(GetKVReq { key: si_k }).await?.is_some();
          results.push(RespValue::Int(if exists { 1 } else { 0 }));
        }
      }
      Ok(RespValue::Arr(results))
    }

    RedisCommand::SiRange {
      key,
      cursor,
      offset,
      limit,
    } => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Arr(Vec::new()));
      }

      if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.si_meta(&key),
        })
        .await?
        && let Some(m) = SortedintMeta::decode(&meta_bytes)
        && (m.is_expired(now) || m.is_empty())
      {
        return Ok(RespValue::Arr(Vec::new()));
      }

      let prefix = kc.si_prefix(&key);
      let items = node
        .scan_prefix(ScanPrefixReq {
          prefix: prefix.clone(),
        })
        .await?;

      let mut results = Vec::new();
      let mut pos = 0usize;

      for (k, _) in items {
        if k.len() < prefix.len() + 16 {
          continue;
        }
        if let Some(id) = decode_hex_u64(&k[prefix.len()..prefix.len() + 16]) {
          if cursor > 0 && id <= cursor {
            continue;
          }
          if pos < offset {
            pos += 1;
            continue;
          }
          results.push(int_to_blob(id));
          if limit > 0 && results.len() >= limit {
            break;
          }
        }
      }

      Ok(RespValue::Arr(results))
    }

    RedisCommand::SiRevRange {
      key,
      cursor,
      offset,
      limit,
    } => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Arr(Vec::new()));
      }

      if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.si_meta(&key),
        })
        .await?
        && let Some(m) = SortedintMeta::decode(&meta_bytes)
        && (m.is_expired(now) || m.is_empty())
      {
        return Ok(RespValue::Arr(Vec::new()));
      }

      let prefix = kc.si_prefix(&key);
      let items = node
        .scan_prefix(ScanPrefixReq {
          prefix: prefix.clone(),
        })
        .await?;

      let mut results = Vec::new();
      let mut pos = 0usize;

      for (k, _) in items.into_iter().rev() {
        if k.len() < prefix.len() + 16 {
          continue;
        }
        if let Some(id) = decode_hex_u64(&k[prefix.len()..prefix.len() + 16]) {
          if cursor > 0 && id >= cursor {
            continue;
          }
          if pos < offset {
            pos += 1;
            continue;
          }
          results.push(int_to_blob(id));
          if limit > 0 && results.len() >= limit {
            break;
          }
        }
      }

      Ok(RespValue::Arr(results))
    }

    RedisCommand::SiRangeByValue { key, spec } => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Arr(Vec::new()));
      }

      if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.si_meta(&key),
        })
        .await?
        && let Some(m) = SortedintMeta::decode(&meta_bytes)
        && (m.is_expired(now) || m.is_empty())
      {
        return Ok(RespValue::Arr(Vec::new()));
      }

      let prefix = kc.si_prefix(&key);
      let items = node
        .scan_prefix(ScanPrefixReq {
          prefix: prefix.clone(),
        })
        .await?;

      let mut results = Vec::new();
      let mut pos = 0usize;

      for (k, _) in items {
        if k.len() < prefix.len() + 16 {
          continue;
        }
        if let Some(id) = decode_hex_u64(&k[prefix.len()..prefix.len() + 16]) {
          if (spec.minex && id == spec.min) || id < spec.min {
            continue;
          }
          if (spec.maxex && id == spec.max) || id > spec.max {
            break;
          }
          if pos < spec.offset {
            pos += 1;
            continue;
          }
          results.push(int_to_blob(id));
          if let Some(count) = spec.count
            && count > 0
            && results.len() >= count
          {
            break;
          }
        }
      }

      Ok(RespValue::Arr(results))
    }

    RedisCommand::SiRevRangeByValue { key, spec } => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Arr(Vec::new()));
      }

      if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.si_meta(&key),
        })
        .await?
        && let Some(m) = SortedintMeta::decode(&meta_bytes)
        && (m.is_expired(now) || m.is_empty())
      {
        return Ok(RespValue::Arr(Vec::new()));
      }

      let prefix = kc.si_prefix(&key);
      let items = node
        .scan_prefix(ScanPrefixReq {
          prefix: prefix.clone(),
        })
        .await?;

      let mut results = Vec::new();
      let mut pos = 0usize;

      for (k, _) in items.into_iter().rev() {
        if k.len() < prefix.len() + 16 {
          continue;
        }
        if let Some(id) = decode_hex_u64(&k[prefix.len()..prefix.len() + 16]) {
          if (spec.maxex && id == spec.max) || id > spec.max {
            continue;
          }
          if (spec.minex && id == spec.min) || id < spec.min {
            break;
          }
          if pos < spec.offset {
            pos += 1;
            continue;
          }
          results.push(int_to_blob(id));
          if let Some(count) = spec.count
            && count > 0
            && results.len() >= count
          {
            break;
          }
        }
      }

      Ok(RespValue::Arr(results))
    }

    _ => Ok(RespValue::error("ERR unknown or unsupported command")),
  }
}
