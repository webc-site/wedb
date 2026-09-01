use rapidhash::RapidHashMap;
use std::str::from_utf8;
use std::sync::Arc;

use super::cmd::RedisCommand;
pub use super::handler::context::StreamId;
use super::handler::context::{
  ConnectionContext, KeyComposer, StreamConsumerGroupMeta, StreamConsumerMeta, StreamMeta,
  StreamPelEntry,
};
use super::protocol::RespValue;
use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::redis::resp_util::int_to_blob;
use crate::util::now_millis;
use wedb_raft::types::{BatchWriteReq, GetKVReq, ScanPrefixReq, UpsertKV};

/// 从底层消息子键提取 StreamId（零堆分配）
#[inline]
pub fn extract_stream_id_from_item_key(item_key: &[u8]) -> Option<StreamId> {
  if item_key.len() < 34 {
    return None;
  }
  let n = item_key.len();
  if item_key[n - 17] != b':' || item_key[n - 34] != b':' {
    return None;
  }
  let seq_str = from_utf8(&item_key[n - 16..]).ok()?;
  let ms_str = from_utf8(&item_key[n - 33..n - 17]).ok()?;
  let ms = u64::from_str_radix(ms_str, 16).ok()?;
  let seq = u64::from_str_radix(seq_str, 16).ok()?;
  Some(StreamId { ms, seq })
}

/// Stream 消息记录
#[derive(Debug, Clone)]
pub struct StreamEntry {
  pub id: StreamId,
  pub fields: Vec<(String, Vec<u8>)>,
}

/// Stream 修剪策略
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamTrimStrategy {
  MaxLen(usize),
  MinId(StreamId),
}

/// 编码 Stream 条目 Fields (高性能二进制紧凑格式)
#[inline]
pub fn encode_stream_entry_fields(fields: &[(String, Vec<u8>)]) -> Vec<u8> {
  let mut total_size = 0;
  for (k, v) in fields {
    total_size += 4 + k.len() + 4 + v.len();
  }
  let mut buf = Vec::with_capacity(total_size);
  for (k, v) in fields {
    let k_bytes = k.as_bytes();
    buf.extend_from_slice(&(k_bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(k_bytes);
    buf.extend_from_slice(&(v.len() as u32).to_be_bytes());
    buf.extend_from_slice(v);
  }
  buf
}

/// 解码 Stream 条目 Fields
#[inline]
pub fn decode_stream_entry_fields(mut bytes: &[u8]) -> Option<Vec<(String, Vec<u8>)>> {
  let mut fields = Vec::new();
  while !bytes.is_empty() {
    if bytes.len() < 4 {
      return sonic_rs::from_slice(bytes).ok();
    }
    let mut len_buf = [0u8; 4];
    len_buf.copy_from_slice(&bytes[..4]);
    let k_len = u32::from_be_bytes(len_buf) as usize;
    bytes = &bytes[4..];
    if bytes.len() < k_len {
      return sonic_rs::from_slice(bytes).ok();
    }
    let k = from_utf8(&bytes[..k_len]).ok()?.to_string();
    bytes = &bytes[k_len..];

    if bytes.len() < 4 {
      return None;
    }
    len_buf.copy_from_slice(&bytes[..4]);
    let v_len = u32::from_be_bytes(len_buf) as usize;
    bytes = &bytes[4..];
    if bytes.len() < v_len {
      return None;
    }
    let v = bytes[..v_len].to_vec();
    bytes = &bytes[v_len..];
    fields.push((k, v));
  }
  Some(fields)
}

/// 执行 Stream 头部条目修剪
pub async fn trim_stream_entries(
  node: &Arc<RaftNode>,
  kc: &KeyComposer<'_>,
  key: &str,
  metadata: &mut StreamMeta,
  strategy: &StreamTrimStrategy,
  limit: Option<usize>,
) -> Result<(usize, Vec<UpsertKV>)> {
  if metadata.base.size == 0 {
    return Ok((0, Vec::new()));
  }

  let mut deleted_count = 0usize;
  let mut ops = Vec::new();
  let prefix = kc.stream_prefix(key);
  let items = node.scan_prefix(ScanPrefixReq { prefix }).await?;

  let max_to_delete = limit.unwrap_or(usize::MAX);
  let mut last_deleted_id = StreamId::min();
  let mut remaining_first_id = None;

  for (k_bytes, _) in items {
    if let Some(id) = extract_stream_id_from_item_key(&k_bytes) {
      let should_delete = match strategy {
        StreamTrimStrategy::MaxLen(max_len) => {
          (metadata.base.size as usize).saturating_sub(deleted_count) > *max_len
        }
        StreamTrimStrategy::MinId(min_id) => id < *min_id,
      };

      if should_delete && deleted_count < max_to_delete {
        deleted_count += 1;
        last_deleted_id = id;
        ops.push(UpsertKV::delete(
          String::from_utf8_lossy(&k_bytes).into_owned(),
        ));
      } else {
        if remaining_first_id.is_none() {
          remaining_first_id = Some(id);
        }
        if let StreamTrimStrategy::MaxLen(max_len) = strategy
          && (metadata.base.size as usize).saturating_sub(deleted_count) <= *max_len
        {
          break;
        }
      }
    }
  }

  if deleted_count > 0 {
    metadata.base.size = metadata.base.size.saturating_sub(deleted_count as u64);
    metadata.max_deleted_entry_id = last_deleted_id;
    if metadata.base.size == 0 {
      metadata.first_entry_id = StreamId::min();
      metadata.last_entry_id = StreamId::min();
      metadata.recorded_first_entry_id = StreamId::min();
    } else if let Some(first_id) = remaining_first_id {
      metadata.first_entry_id = first_id;
      metadata.recorded_first_entry_id = first_id;
    }
  }

  Ok((deleted_count, ops))
}

/// 处理全套 Redis Stream 命令（对标 Apache Kvrocks 与 Redis 7.2+）
pub async fn handle_stream(
  node: &Arc<RaftNode>,
  ctx: &mut ConnectionContext,
  cmd: RedisCommand,
) -> Result<RespValue> {
  let sm = node.state_machine();
  let kc = ctx.key_composer();
  let now = now_millis();

  match cmd {
    // ================= 1. XADD =================
    RedisCommand::XAdd {
      key,
      id,
      fields,
      nomkstream,
      max_len,
      min_id,
      limit,
      approximate: _,
    } => {
      let meta_k = kc.stream_meta(&key);

      let mut metadata = if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: meta_k.clone(),
        })
        .await?
      {
        if let Some(m) = StreamMeta::decode(&meta_bytes) {
          if m.is_expired(now) {
            StreamMeta::new(0, now)
          } else {
            m
          }
        } else {
          StreamMeta::new(0, now)
        }
      } else {
        if nomkstream {
          return Ok(RespValue::Null);
        }
        StreamMeta::new(0, now)
      };

      // 1. 生成或校验 Stream ID
      let next_id = if id == "*" {
        if metadata.last_generated_id.is_min() {
          StreamId::new(if now == 0 { 1 } else { now }, 0)
        } else if now > metadata.last_generated_id.ms {
          StreamId::new(now, 0)
        } else {
          StreamId::new(
            metadata.last_generated_id.ms,
            metadata.last_generated_id.seq + 1,
          )
        }
      } else if let Some((ms_str, seq_str)) = id.split_once('-') {
        let ms = ms_str
          .parse::<u64>()
          .map_err(|_| Error::invalid_data("ERR Invalid stream ID specified"))?;
        if seq_str == "*" {
          if ms < metadata.last_generated_id.ms {
            return Err(Error::invalid_data(
              "ERR The ID specified in XADD is equal or smaller than the target stream top item",
            ));
          } else if ms == metadata.last_generated_id.ms {
            StreamId::new(ms, metadata.last_generated_id.seq + 1)
          } else {
            StreamId::new(ms, 0)
          }
        } else {
          let seq = seq_str
            .parse::<u64>()
            .map_err(|_| Error::invalid_data("ERR Invalid stream ID specified"))?;
          let explicit = StreamId::new(ms, seq);
          if explicit.is_min() {
            return Err(Error::invalid_data(
              "ERR The ID specified in XADD must be greater than 0-0",
            ));
          }
          if explicit <= metadata.last_generated_id {
            return Err(Error::invalid_data(
              "ERR The ID specified in XADD is equal or smaller than the target stream top item",
            ));
          }
          explicit
        }
      } else {
        let ms = id
          .parse::<u64>()
          .map_err(|_| Error::invalid_data("ERR Invalid stream ID specified"))?;
        let explicit = StreamId::new(ms, 0);
        if explicit.is_min() {
          return Err(Error::invalid_data(
            "ERR The ID specified in XADD must be greater than 0-0",
          ));
        }
        if explicit <= metadata.last_generated_id {
          return Err(Error::invalid_data(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item",
          ));
        }
        explicit
      };

      let mut batch_entries = Vec::new();

      // 2. 执行可选的前置/后置修剪策略
      if let Some(m_len) = max_len {
        let (deleted, trim_ops) = trim_stream_entries(
          node,
          &kc,
          &key,
          &mut metadata,
          &StreamTrimStrategy::MaxLen(m_len),
          limit,
        )
        .await?;
        if deleted > 0 {
          batch_entries.extend(trim_ops);
        }
      } else if let Some(m_id_str) = min_id
        && let Ok(m_id) = StreamId::parse(&m_id_str)
      {
        let (deleted, trim_ops) = trim_stream_entries(
          node,
          &kc,
          &key,
          &mut metadata,
          &StreamTrimStrategy::MinId(m_id),
          limit,
        )
        .await?;
        if deleted > 0 {
          batch_entries.extend(trim_ops);
        }
      }

      // 3. 写入消息项
      let item_k = kc.stream_item(&key, next_id.ms, next_id.seq);
      let encoded_fields = encode_stream_entry_fields(&fields);
      batch_entries.push(UpsertKV::insert(item_k, encoded_fields));

      // 4. 更新元数据
      metadata.last_generated_id = next_id;
      metadata.last_entry_id = next_id;
      metadata.base.size += 1;
      metadata.entries_added += 1;
      if metadata.base.size == 1 || metadata.first_entry_id.is_min() {
        metadata.first_entry_id = next_id;
        metadata.recorded_first_entry_id = next_id;
      }

      batch_entries.push(UpsertKV::insert(meta_k, metadata.encode().to_vec()));
      node
        .batch_write(BatchWriteReq {
          entries: batch_entries,
        })
        .await?;

      Ok(RespValue::Blob(next_id.to_string_id().into_bytes()))
    }

    // ================= 2. XLEN =================
    RedisCommand::XLen(key) => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Int(0));
      }
      if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.stream_meta(&key),
        })
        .await?
        && let Some(m) = StreamMeta::decode(&meta_bytes)
        && !m.is_expired(now)
      {
        Ok(RespValue::Int(m.size() as i64))
      } else {
        Ok(RespValue::Int(0))
      }
    }

    // ================= 3. XRANGE =================
    RedisCommand::XRange {
      key,
      start,
      end,
      count,
    } => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Arr(Vec::new()));
      }

      let meta_opt = if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.stream_meta(&key),
        })
        .await?
      {
        StreamMeta::decode(&meta_bytes)
      } else {
        None
      };

      match meta_opt {
        Some(m) if !m.is_expired(now) && !m.is_empty() => {}
        _ => return Ok(RespValue::Arr(Vec::new())),
      }

      let (start_id, exclude_start) =
        StreamId::parse_range_start(&start).unwrap_or((StreamId::min(), false));
      let (end_id, exclude_end) =
        StreamId::parse_range_end(&end).unwrap_or((StreamId::max(), false));

      let prefix = kc.stream_prefix(&key);
      let items = node.scan_prefix(ScanPrefixReq { prefix }).await?;
      let limit = count.unwrap_or(usize::MAX);

      let mut results = Vec::new();
      for (k_bytes, v_bytes) in items {
        if let Some(id) = extract_stream_id_from_item_key(&k_bytes) {
          if (exclude_start && id <= start_id) || (!exclude_start && id < start_id) {
            continue;
          }
          if (exclude_end && id >= end_id) || (!exclude_end && id > end_id) {
            break;
          }
          if let Some(fields) = decode_stream_entry_fields(&v_bytes) {
            let mut field_values = Vec::with_capacity(fields.len() * 2);
            for (f, v) in fields {
              field_values.push(RespValue::Blob(f.into_bytes()));
              field_values.push(RespValue::Blob(v));
            }
            results.push(RespValue::Arr(vec![
              RespValue::Blob(id.to_string_id().into_bytes()),
              RespValue::Arr(field_values),
            ]));
            if results.len() >= limit {
              break;
            }
          }
        }
      }

      Ok(RespValue::Arr(results))
    }

    // ================= 4. XREVRANGE =================
    RedisCommand::XRevRange {
      key,
      end,
      start,
      count,
    } => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Arr(Vec::new()));
      }

      let meta_opt = if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.stream_meta(&key),
        })
        .await?
      {
        StreamMeta::decode(&meta_bytes)
      } else {
        None
      };

      match meta_opt {
        Some(m) if !m.is_expired(now) && !m.is_empty() => {}
        _ => return Ok(RespValue::Arr(Vec::new())),
      }

      // end 是高边界，start 是低边界
      let (high_id, exclude_high) =
        StreamId::parse_range_end(&end).unwrap_or((StreamId::max(), false));
      let (low_id, exclude_low) =
        StreamId::parse_range_start(&start).unwrap_or((StreamId::min(), false));

      let prefix = kc.stream_prefix(&key);
      let items = node.scan_prefix(ScanPrefixReq { prefix }).await?;
      let limit = count.unwrap_or(usize::MAX);

      let mut matched = Vec::new();
      for (k_bytes, v_bytes) in items {
        if let Some(id) = extract_stream_id_from_item_key(&k_bytes) {
          if (exclude_low && id <= low_id) || (!exclude_low && id < low_id) {
            continue;
          }
          if (exclude_high && id >= high_id) || (!exclude_high && id > high_id) {
            break;
          }
          if let Some(fields) = decode_stream_entry_fields(&v_bytes) {
            matched.push((id, fields));
          }
        }
      }

      let mut results = Vec::with_capacity(matched.len().min(limit));
      for (id, fields) in matched.into_iter().rev().take(limit) {
        let mut field_values = Vec::with_capacity(fields.len() * 2);
        for (f, v) in fields {
          field_values.push(RespValue::Blob(f.into_bytes()));
          field_values.push(RespValue::Blob(v));
        }
        results.push(RespValue::Arr(vec![
          RespValue::Blob(id.to_string_id().into_bytes()),
          RespValue::Arr(field_values),
        ]));
      }

      Ok(RespValue::Arr(results))
    }

    // ================= 5. XDEL =================
    RedisCommand::XDel(key, ids) => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Int(0));
      }

      let meta_opt = if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.stream_meta(&key),
        })
        .await?
      {
        StreamMeta::decode(&meta_bytes)
      } else {
        None
      };

      let mut metadata = match meta_opt {
        Some(m) if !m.is_expired(now) && !m.is_empty() => m,
        _ => return Ok(RespValue::Int(0)),
      };

      let mut deleted_count = 0u64;
      let mut entries = Vec::with_capacity(ids.len() + 1);

      for id_str in ids {
        if let Ok(id) = StreamId::parse(&id_str) {
          let item_k = kc.stream_item(&key, id.ms, id.seq);
          if node
            .read(GetKVReq {
              key: item_k.clone(),
            })
            .await?
            .is_some()
          {
            deleted_count += 1;
            entries.push(UpsertKV::delete(item_k));
            if id > metadata.max_deleted_entry_id {
              metadata.max_deleted_entry_id = id;
            }
          }
        }
      }

      if deleted_count > 0 {
        metadata.base.size = metadata.base.size.saturating_sub(deleted_count);
        if metadata.base.size == 0 {
          entries.push(UpsertKV::delete(kc.stream_meta(&key)));
        } else {
          // 重新检索首尾项边界
          let prefix = kc.stream_prefix(&key);
          let rem_items = node.scan_prefix(ScanPrefixReq { prefix }).await?;
          let mut first = None;
          let mut last = None;
          for (k_bytes, _) in rem_items {
            if let Some(id) = extract_stream_id_from_item_key(&k_bytes) {
              if first.is_none() {
                first = Some(id);
              }
              last = Some(id);
            }
          }
          if let Some(f) = first {
            metadata.first_entry_id = f;
            metadata.recorded_first_entry_id = f;
          }
          if let Some(l) = last {
            metadata.last_entry_id = l;
          }
          entries.push(UpsertKV::insert(
            kc.stream_meta(&key),
            metadata.encode().to_vec(),
          ));
        }
        node.batch_write(BatchWriteReq { entries }).await?;
      }

      Ok(RespValue::Int(deleted_count as i64))
    }

    // ================= 6. XTRIM =================
    RedisCommand::XTrim {
      key,
      max_len,
      min_id,
      limit,
      approximate: _,
    } => {
      let raw_k = kc.raw_key(&key);
      if sm.is_expired(&raw_k) {
        return Ok(RespValue::Int(0));
      }

      let meta_opt = if let Some(meta_bytes) = node
        .read(GetKVReq {
          key: kc.stream_meta(&key),
        })
        .await?
      {
        StreamMeta::decode(&meta_bytes)
      } else {
        None
      };

      let mut metadata = match meta_opt {
        Some(m) if !m.is_expired(now) && !m.is_empty() => m,
        _ => return Ok(RespValue::Int(0)),
      };

      let (deleted, mut entries) = if let Some(m_len) = max_len {
        trim_stream_entries(
          node,
          &kc,
          &key,
          &mut metadata,
          &StreamTrimStrategy::MaxLen(m_len),
          limit,
        )
        .await?
      } else if let Some(m_id_str) = min_id
        && let Ok(m_id) = StreamId::parse(&m_id_str)
      {
        trim_stream_entries(
          node,
          &kc,
          &key,
          &mut metadata,
          &StreamTrimStrategy::MinId(m_id),
          limit,
        )
        .await?
      } else {
        (0, Vec::new())
      };

      if deleted > 0 {
        if metadata.base.size == 0 {
          entries.push(UpsertKV::delete(kc.stream_meta(&key)));
        } else {
          entries.push(UpsertKV::insert(
            kc.stream_meta(&key),
            metadata.encode().to_vec(),
          ));
        }
        node.batch_write(BatchWriteReq { entries }).await?;
      }

      Ok(RespValue::Int(deleted as i64))
    }

    // ================= 7. XREAD =================
    RedisCommand::XRead {
      streams,
      ids,
      count,
      block: _,
    } => {
      let limit = count.unwrap_or(usize::MAX);
      let mut stream_results = Vec::new();

      for (k, start_id_str) in streams.into_iter().zip(ids) {
        let meta_opt = if let Some(meta_bytes) = node
          .read(GetKVReq {
            key: kc.stream_meta(&k),
          })
          .await?
        {
          StreamMeta::decode(&meta_bytes)
        } else {
          None
        };

        let metadata = match meta_opt {
          Some(m) if !m.is_expired(now) && !m.is_empty() => m,
          _ => continue,
        };

        let (start_id, exclude) = if start_id_str == "$" {
          (metadata.last_entry_id, true)
        } else {
          (
            StreamId::parse(&start_id_str).unwrap_or(StreamId::min()),
            true,
          )
        };

        let prefix = kc.stream_prefix(&k);
        let items = node.scan_prefix(ScanPrefixReq { prefix }).await?;
        let mut entries = Vec::new();

        for (k_bytes, v_bytes) in items {
          if let Some(id) = extract_stream_id_from_item_key(&k_bytes) {
            if (exclude && id <= start_id) || (!exclude && id < start_id) {
              continue;
            }
            if let Some(fields) = decode_stream_entry_fields(&v_bytes) {
              let mut field_values = Vec::with_capacity(fields.len() * 2);
              for (f, v) in fields {
                field_values.push(RespValue::Blob(f.into_bytes()));
                field_values.push(RespValue::Blob(v));
              }
              entries.push(RespValue::Arr(vec![
                RespValue::Blob(id.to_string_id().into_bytes()),
                RespValue::Arr(field_values),
              ]));
              if entries.len() >= limit {
                break;
              }
            }
          }
        }

        if !entries.is_empty() {
          stream_results.push(RespValue::Arr(vec![
            RespValue::Blob(k.into_bytes()),
            RespValue::Arr(entries),
          ]));
        }
      }

      if stream_results.is_empty() {
        Ok(RespValue::Null)
      } else {
        Ok(RespValue::Arr(stream_results))
      }
    }

    // ================= 8. XINFO =================
    RedisCommand::XInfo(subcmd, key) => {
      let upper = subcmd.to_ascii_uppercase();
      if upper == "STREAM" {
        let meta_bytes = match node
          .read(GetKVReq {
            key: kc.stream_meta(&key),
          })
          .await?
        {
          Some(b) => b,
          None => return Err(Error::invalid_data("ERR no such key")),
        };
        let meta =
          StreamMeta::decode(&meta_bytes).ok_or_else(|| Error::invalid_data("ERR no such key"))?;

        let first_entry_resp = if meta.base.size > 0 {
          let first_k = kc.stream_item(&key, meta.first_entry_id.ms, meta.first_entry_id.seq);
          if let Some(first_v) = node.read(GetKVReq { key: first_k }).await?
            && let Some(fields) = decode_stream_entry_fields(&first_v)
          {
            let mut fv = Vec::with_capacity(fields.len() * 2);
            for (f, v) in fields {
              fv.push(RespValue::Blob(f.into_bytes()));
              fv.push(RespValue::Blob(v));
            }
            RespValue::Arr(vec![
              RespValue::Blob(meta.first_entry_id.to_string_id().into_bytes()),
              RespValue::Arr(fv),
            ])
          } else {
            RespValue::Null
          }
        } else {
          RespValue::Null
        };

        let last_entry_resp = if meta.base.size > 0 {
          let last_k = kc.stream_item(&key, meta.last_entry_id.ms, meta.last_entry_id.seq);
          if let Some(last_v) = node.read(GetKVReq { key: last_k }).await?
            && let Some(fields) = decode_stream_entry_fields(&last_v)
          {
            let mut fv = Vec::with_capacity(fields.len() * 2);
            for (f, v) in fields {
              fv.push(RespValue::Blob(f.into_bytes()));
              fv.push(RespValue::Blob(v));
            }
            RespValue::Arr(vec![
              RespValue::Blob(meta.last_entry_id.to_string_id().into_bytes()),
              RespValue::Arr(fv),
            ])
          } else {
            RespValue::Null
          }
        } else {
          RespValue::Null
        };

        let info = vec![
          RespValue::Simple("length".to_string()),
          RespValue::Int(meta.base.size as i64),
          RespValue::Simple("radix-tree-keys".to_string()),
          RespValue::Int(1),
          RespValue::Simple("radix-tree-nodes".to_string()),
          RespValue::Int(2),
          RespValue::Simple("groups".to_string()),
          RespValue::Int(meta.group_number as i64),
          RespValue::Simple("last-generated-id".to_string()),
          RespValue::Blob(meta.last_generated_id.to_string_id().into_bytes()),
          RespValue::Simple("max-deleted-entry-id".to_string()),
          RespValue::Blob(meta.max_deleted_entry_id.to_string_id().into_bytes()),
          RespValue::Simple("entries-added".to_string()),
          RespValue::Int(meta.entries_added as i64),
          RespValue::Simple("recorded-first-entry-id".to_string()),
          RespValue::Blob(meta.recorded_first_entry_id.to_string_id().into_bytes()),
          RespValue::Simple("first-entry".to_string()),
          first_entry_resp,
          RespValue::Simple("last-entry".to_string()),
          last_entry_resp,
        ];
        Ok(RespValue::Arr(info))
      } else if upper == "GROUPS" {
        let prefix = kc.stream_group_prefix(&key);
        let group_items = node
          .scan_prefix(ScanPrefixReq {
            prefix: prefix.clone(),
          })
          .await?;
        let mut groups_resp = Vec::new();
        for (k_bytes, v_bytes) in group_items {
          if let Some(g_meta) = StreamConsumerGroupMeta::decode(&v_bytes) {
            let group_name = String::from_utf8_lossy(&k_bytes[prefix.len()..]).to_string();
            groups_resp.push(RespValue::Arr(vec![
              RespValue::Simple("name".to_string()),
              RespValue::Blob(group_name.into_bytes()),
              RespValue::Simple("consumers".to_string()),
              RespValue::Int(g_meta.consumer_number as i64),
              RespValue::Simple("pending".to_string()),
              RespValue::Int(g_meta.pending_number as i64),
              RespValue::Simple("last-delivered-id".to_string()),
              RespValue::Blob(g_meta.last_delivered_id.to_string_id().into_bytes()),
              RespValue::Simple("entries-read".to_string()),
              RespValue::Int(g_meta.entries_read),
              RespValue::Simple("lag".to_string()),
              RespValue::Int(g_meta.lag as i64),
            ]));
          }
        }
        Ok(RespValue::Arr(groups_resp))
      } else {
        Ok(RespValue::Arr(Vec::new()))
      }
    }

    RedisCommand::XInfoStream {
      key,
      full: _,
      count: _,
    } => {
      Box::pin(handle_stream(
        node,
        ctx,
        RedisCommand::XInfo("STREAM".to_string(), key),
      ))
      .await
    }

    // ================= 9. 消费组命令 (XGROUP) =================
    RedisCommand::XGroup(args) => {
      if args.is_empty() {
        return Err(Error::invalid_data(
          "ERR wrong number of arguments for 'xgroup' cmd",
        ));
      }
      let sub = args[0].to_ascii_uppercase();
      match sub.as_str() {
        "CREATE" => {
          if args.len() < 4 {
            return Err(Error::invalid_data(
              "ERR wrong number of arguments for 'xgroup create' cmd",
            ));
          }
          let key = &args[1];
          let group = &args[2];
          let id_str = &args[3];
          let mkstream = args.iter().any(|a| a.eq_ignore_ascii_case("MKSTREAM"));
          let mut entries_read = None;
          for (i, a) in args.iter().enumerate() {
            if a.eq_ignore_ascii_case("ENTRIESREAD") && i + 1 < args.len() {
              entries_read = args[i + 1].parse::<i64>().ok();
            }
          }

          let meta_k = kc.stream_meta(key);
          let mut metadata = match node
            .read(GetKVReq {
              key: meta_k.clone(),
            })
            .await?
          {
            Some(b) => StreamMeta::decode(&b).unwrap_or_else(|| StreamMeta::new(0, now)),
            None => {
              if mkstream {
                StreamMeta::new(0, now)
              } else {
                return Err(Error::invalid_data(
                  "ERR The XGROUP subcmd requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
                ));
              }
            }
          };

          let group_k = kc.stream_group_meta(key, group);
          if node
            .read(GetKVReq {
              key: group_k.clone(),
            })
            .await?
            .is_some()
          {
            return Err(Error::invalid_data(
              "BUSYGROUP Consumer Group name already exists",
            ));
          }

          let last_delivered_id = if id_str == "$" {
            metadata.last_entry_id
          } else {
            StreamId::parse(id_str).unwrap_or(StreamId::min())
          };

          let group_meta = StreamConsumerGroupMeta {
            consumer_number: 0,
            pending_number: 0,
            last_delivered_id,
            entries_read: entries_read.unwrap_or(0),
            lag: 0,
          };

          metadata.group_number += 1;
          let entries = vec![
            UpsertKV::insert(group_k, group_meta.encode().to_vec()),
            UpsertKV::insert(meta_k, metadata.encode().to_vec()),
          ];
          node.batch_write(BatchWriteReq { entries }).await?;
          Ok(RespValue::ok())
        }
        "DESTROY" => {
          if args.len() < 3 {
            return Err(Error::invalid_data(
              "ERR wrong number of arguments for 'xgroup destroy' cmd",
            ));
          }
          let key = &args[1];
          let group = &args[2];
          let meta_k = kc.stream_meta(key);
          let mut metadata = match node
            .read(GetKVReq {
              key: meta_k.clone(),
            })
            .await?
          {
            Some(b) => StreamMeta::decode(&b).unwrap_or_else(|| StreamMeta::new(0, now)),
            None => {
              return Err(Error::invalid_data(
                "ERR The XGROUP subcmd requires the key to exist.",
              ));
            }
          };

          let group_k = kc.stream_group_meta(key, group);
          if node
            .read(GetKVReq {
              key: group_k.clone(),
            })
            .await?
            .is_none()
          {
            return Ok(RespValue::Int(0));
          }

          let mut entries = vec![UpsertKV::delete(group_k)];
          // 清理所有关联 consumers 与 PEL
          let c_prefix = kc.stream_consumer_prefix(key, group);
          for (k, _) in node.scan_prefix(ScanPrefixReq { prefix: c_prefix }).await? {
            entries.push(UpsertKV::delete(String::from_utf8_lossy(&k).into_owned()));
          }
          let p_prefix = kc.stream_pel_prefix(key, group);
          for (k, _) in node.scan_prefix(ScanPrefixReq { prefix: p_prefix }).await? {
            entries.push(UpsertKV::delete(String::from_utf8_lossy(&k).into_owned()));
          }

          metadata.group_number = metadata.group_number.saturating_sub(1);
          entries.push(UpsertKV::insert(meta_k, metadata.encode().to_vec()));
          node.batch_write(BatchWriteReq { entries }).await?;
          Ok(RespValue::Int(1))
        }
        "SETID" => {
          if args.len() < 4 {
            return Err(Error::invalid_data(
              "ERR wrong number of arguments for 'xgroup setid' cmd",
            ));
          }
          let key = &args[1];
          let group = &args[2];
          let id_str = &args[3];
          let meta_k = kc.stream_meta(key);
          let metadata = match node.read(GetKVReq { key: meta_k }).await? {
            Some(b) => StreamMeta::decode(&b).unwrap_or_else(|| StreamMeta::new(0, now)),
            None => {
              return Err(Error::invalid_data(
                "ERR The XGROUP subcmd requires the key to exist.",
              ));
            }
          };

          let group_k = kc.stream_group_meta(key, group);
          let mut group_meta = match node
            .read(GetKVReq {
              key: group_k.clone(),
            })
            .await?
          {
            Some(b) => StreamConsumerGroupMeta::decode(&b).unwrap_or_default(),
            None => {
              return Err(Error::invalid_data(
                "NOGROUP No such consumer group for key name",
              ));
            }
          };

          let last_delivered_id = if id_str == "$" {
            metadata.last_entry_id
          } else {
            StreamId::parse(id_str).unwrap_or(StreamId::min())
          };
          group_meta.last_delivered_id = last_delivered_id;
          for (i, a) in args.iter().enumerate() {
            if a.eq_ignore_ascii_case("ENTRIESREAD")
              && i + 1 < args.len()
              && let Ok(er) = args[i + 1].parse::<i64>()
            {
              group_meta.entries_read = er;
            }
          }

          let entries = vec![UpsertKV::insert(group_k, group_meta.encode().to_vec())];
          node.batch_write(BatchWriteReq { entries }).await?;
          Ok(RespValue::ok())
        }
        "CREATECONSUMER" => {
          if args.len() < 4 {
            return Err(Error::invalid_data(
              "ERR wrong number of arguments for 'xgroup createconsumer' cmd",
            ));
          }
          let key = &args[1];
          let group = &args[2];
          let consumer = &args[3];

          let group_k = kc.stream_group_meta(key, group);
          let mut group_meta = match node
            .read(GetKVReq {
              key: group_k.clone(),
            })
            .await?
          {
            Some(b) => StreamConsumerGroupMeta::decode(&b).unwrap_or_default(),
            None => {
              return Err(Error::invalid_data(
                "NOGROUP No such consumer group for key name",
              ));
            }
          };

          let c_k = kc.stream_consumer_meta(key, group, consumer);
          if node.read(GetKVReq { key: c_k.clone() }).await?.is_some() {
            return Ok(RespValue::Int(0));
          }

          let c_meta = StreamConsumerMeta {
            pending_number: 0,
            last_attempted_interaction_ms: now,
            last_successful_interaction_ms: now,
          };
          group_meta.consumer_number += 1;

          let entries = vec![
            UpsertKV::insert(c_k, c_meta.encode().to_vec()),
            UpsertKV::insert(group_k, group_meta.encode().to_vec()),
          ];
          node.batch_write(BatchWriteReq { entries }).await?;
          Ok(RespValue::Int(1))
        }
        "DELCONSUMER" => {
          if args.len() < 4 {
            return Err(Error::invalid_data(
              "ERR wrong number of arguments for 'xgroup delconsumer' cmd",
            ));
          }
          let key = &args[1];
          let group = &args[2];
          let consumer = &args[3];

          let group_k = kc.stream_group_meta(key, group);
          let mut group_meta = match node
            .read(GetKVReq {
              key: group_k.clone(),
            })
            .await?
          {
            Some(b) => StreamConsumerGroupMeta::decode(&b).unwrap_or_default(),
            None => {
              return Err(Error::invalid_data(
                "NOGROUP No such consumer group for key name",
              ));
            }
          };

          let c_k = kc.stream_consumer_meta(key, group, consumer);
          if node.read(GetKVReq { key: c_k.clone() }).await?.is_none() {
            return Ok(RespValue::Int(0));
          }

          let mut entries = vec![UpsertKV::delete(c_k)];
          let mut deleted_pel = 0u64;
          let p_prefix = kc.stream_pel_prefix(key, group);
          for (k, v) in node.scan_prefix(ScanPrefixReq { prefix: p_prefix }).await? {
            if let Some(pel) = StreamPelEntry::decode(&v)
              && pel.consumer_name == *consumer
            {
              deleted_pel += 1;
              entries.push(UpsertKV::delete(String::from_utf8_lossy(&k).into_owned()));
            }
          }

          group_meta.pending_number = group_meta.pending_number.saturating_sub(deleted_pel);
          group_meta.consumer_number = group_meta.consumer_number.saturating_sub(1);
          entries.push(UpsertKV::insert(group_k, group_meta.encode().to_vec()));
          node.batch_write(BatchWriteReq { entries }).await?;
          Ok(RespValue::Int(deleted_pel as i64))
        }
        _ => Ok(RespValue::ok()),
      }
    }

    // ================= 10. XACK =================
    RedisCommand::XAck(key, group, ids) => {
      let group_k = kc.stream_group_meta(&key, &group);
      let mut group_meta = match node
        .read(GetKVReq {
          key: group_k.clone(),
        })
        .await?
      {
        Some(b) => StreamConsumerGroupMeta::decode(&b).unwrap_or_default(),
        None => return Ok(RespValue::Int(0)),
      };

      let mut acknowledged = 0u64;
      let mut entries = Vec::new();
      let mut consumer_decrements: RapidHashMap<String, u64> = RapidHashMap::default();

      for id_str in ids {
        if let Ok(id) = StreamId::parse(&id_str) {
          let pel_k = kc.stream_pel_item(&key, &group, id.ms, id.seq);
          if let Some(pel_bytes) = node.read(GetKVReq { key: pel_k.clone() }).await? {
            if let Some(pel) = StreamPelEntry::decode(&pel_bytes) {
              *consumer_decrements.entry(pel.consumer_name).or_insert(0) += 1;
            }
            acknowledged += 1;
            entries.push(UpsertKV::delete(pel_k));
          }
        }
      }

      if acknowledged > 0 {
        group_meta.pending_number = group_meta.pending_number.saturating_sub(acknowledged);
        entries.push(UpsertKV::insert(group_k, group_meta.encode().to_vec()));

        for (c_name, dec) in consumer_decrements {
          let c_k = kc.stream_consumer_meta(&key, &group, &c_name);
          if let Some(c_bytes) = node.read(GetKVReq { key: c_k.clone() }).await?
            && let Some(mut c_meta) = StreamConsumerMeta::decode(&c_bytes)
          {
            c_meta.pending_number = c_meta.pending_number.saturating_sub(dec);
            entries.push(UpsertKV::insert(c_k, c_meta.encode().to_vec()));
          }
        }
        node.batch_write(BatchWriteReq { entries }).await?;
      }

      Ok(RespValue::Int(acknowledged as i64))
    }

    // ================= 10.1 XACKDEL =================
    RedisCommand::XAckDel { key, group, ids } => {
      let ack_res = Box::pin(handle_stream(
        node,
        ctx,
        RedisCommand::XAck(key.clone(), group, ids.clone()),
      ))
      .await?;
      let del_res = Box::pin(handle_stream(node, ctx, RedisCommand::XDel(key, ids))).await?;
      let ack_cnt = match ack_res {
        RespValue::Int(n) => n,
        _ => 0,
      };
      let del_cnt = match del_res {
        RespValue::Int(n) => n,
        _ => 0,
      };
      Ok(RespValue::Arr(vec![
        RespValue::Int(ack_cnt),
        RespValue::Int(del_cnt),
      ]))
    }

    // ================= 10.2 XNACK =================
    RedisCommand::XNack { key, group, ids } => {
      let mut count = 0i64;
      for id_str in ids {
        if let Ok(id) = StreamId::parse(&id_str) {
          let pel_k = kc.stream_pel_item(&key, &group, id.ms, id.seq);
          if node.read(GetKVReq { key: pel_k }).await?.is_some() {
            count += 1;
          }
        }
      }
      Ok(RespValue::Int(count))
    }

    // ================= 10.3 XDELEX =================
    RedisCommand::XDelEx { key, ids } => {
      Box::pin(handle_stream(node, ctx, RedisCommand::XDel(key, ids))).await
    }

    // ================= 11. XPENDING =================
    RedisCommand::XPending {
      key,
      group,
      start,
      end,
      count,
      consumer,
      idle,
    } => {
      let group_k = kc.stream_group_meta(&key, &group);
      let group_meta = match node.read(GetKVReq { key: group_k }).await? {
        Some(b) => StreamConsumerGroupMeta::decode(&b).unwrap_or_default(),
        None => {
          return Ok(RespValue::Arr(vec![
            RespValue::Int(0),
            RespValue::Null,
            RespValue::Null,
            RespValue::Arr(Vec::new()),
          ]));
        }
      };

      let prefix = kc.stream_pel_prefix(&key, &group);
      let pel_items = node.scan_prefix(ScanPrefixReq { prefix }).await?;

      if start.is_none() {
        // 返回概要形式: [pending_count, min_id, max_id, [[consumer_name, count], ...]]
        let mut min_id: Option<StreamId> = None;
        let mut max_id: Option<StreamId> = None;
        let mut consumer_counts: RapidHashMap<String, u64> = RapidHashMap::default();

        for (k_bytes, v_bytes) in &pel_items {
          if let Some(id) = extract_stream_id_from_item_key(k_bytes) {
            if min_id.is_none() || Some(id) < min_id {
              min_id = Some(id);
            }
            if max_id.is_none() || Some(id) > max_id {
              max_id = Some(id);
            }
            if let Some(pel) = StreamPelEntry::decode(v_bytes) {
              *consumer_counts.entry(pel.consumer_name).or_insert(0) += 1;
            }
          }
        }

        let mut consumers_arr = Vec::with_capacity(consumer_counts.len());
        for (c, cnt) in consumer_counts {
          consumers_arr.push(RespValue::Arr(vec![
            RespValue::Blob(c.into_bytes()),
            int_to_blob(cnt),
          ]));
        }

        return Ok(RespValue::Arr(vec![
          RespValue::Int(group_meta.pending_number as i64),
          min_id
            .map(|id| RespValue::Blob(id.to_string_id().into_bytes()))
            .unwrap_or(RespValue::Null),
          max_id
            .map(|id| RespValue::Blob(id.to_string_id().into_bytes()))
            .unwrap_or(RespValue::Null),
          RespValue::Arr(consumers_arr),
        ]));
      }

      // 返回详情形式: [[id, consumer, idle_time_ms, delivery_count], ...]
      let start_id = start
        .as_deref()
        .and_then(|s| StreamId::parse(s).ok())
        .unwrap_or(StreamId::min());
      let end_id = end
        .as_deref()
        .and_then(|s| StreamId::parse(s).ok())
        .unwrap_or(StreamId::max());
      let limit = count.unwrap_or(usize::MAX);
      let min_idle = idle.unwrap_or(0);

      let mut detail = Vec::new();
      for (k_bytes, v_bytes) in pel_items {
        if let Some(id) = extract_stream_id_from_item_key(&k_bytes) {
          if id < start_id || id > end_id {
            continue;
          }
          if let Some(pel) = StreamPelEntry::decode(&v_bytes) {
            if let Some(ref target_c) = consumer
              && &pel.consumer_name != target_c
            {
              continue;
            }
            let idle_time = now.saturating_sub(pel.last_delivery_time_ms);
            if idle_time < min_idle {
              continue;
            }
            detail.push(RespValue::Arr(vec![
              RespValue::Blob(id.to_string_id().into_bytes()),
              RespValue::Blob(pel.consumer_name.into_bytes()),
              RespValue::Int(idle_time as i64),
              RespValue::Int(pel.last_delivery_count as i64),
            ]));
            if detail.len() >= limit {
              break;
            }
          }
        }
      }

      Ok(RespValue::Arr(detail))
    }

    // ================= 12. XREADGROUP =================
    RedisCommand::XReadGroup {
      group,
      consumer,
      streams,
      ids,
      count,
      block: _,
      noack,
    } => {
      let limit = count.unwrap_or(usize::MAX);
      let mut stream_results = Vec::new();

      for (k, start_id_str) in streams.into_iter().zip(ids) {
        let meta_k = kc.stream_meta(&k);
        if node.read(GetKVReq { key: meta_k }).await?.is_none() {
          continue;
        }

        let group_k = kc.stream_group_meta(&k, &group);
        let mut group_meta = match node
          .read(GetKVReq {
            key: group_k.clone(),
          })
          .await?
        {
          Some(b) => StreamConsumerGroupMeta::decode(&b).unwrap_or_default(),
          None => {
            return Err(Error::invalid_data(
              "NOGROUP No such consumer group for key name",
            ));
          }
        };

        let c_k = kc.stream_consumer_meta(&k, &group, &consumer);
        let mut c_meta = match node.read(GetKVReq { key: c_k.clone() }).await? {
          Some(b) => StreamConsumerMeta::decode(&b).unwrap_or_default(),
          None => {
            group_meta.consumer_number += 1;
            StreamConsumerMeta {
              pending_number: 0,
              last_attempted_interaction_ms: now,
              last_successful_interaction_ms: now,
            }
          }
        };

        let mut entries = Vec::new();
        let mut batch_entries = Vec::new();

        if start_id_str == ">" {
          // 读取未分发的新消息
          let prefix = kc.stream_prefix(&k);
          let items = node.scan_prefix(ScanPrefixReq { prefix }).await?;
          let mut max_id = group_meta.last_delivered_id;

          for (k_bytes, v_bytes) in items {
            if let Some(id) = extract_stream_id_from_item_key(&k_bytes) {
              if id <= group_meta.last_delivered_id {
                continue;
              }
              if let Some(fields) = decode_stream_entry_fields(&v_bytes) {
                let mut fv = Vec::with_capacity(fields.len() * 2);
                for (f, v) in fields {
                  fv.push(RespValue::Blob(f.into_bytes()));
                  fv.push(RespValue::Blob(v));
                }
                entries.push(RespValue::Arr(vec![
                  RespValue::Blob(id.to_string_id().into_bytes()),
                  RespValue::Arr(fv),
                ]));

                if !noack {
                  let pel_k = kc.stream_pel_item(&k, &group, id.ms, id.seq);
                  let pel = StreamPelEntry {
                    last_delivery_time_ms: now,
                    last_delivery_count: 1,
                    consumer_name: consumer.clone(),
                  };
                  batch_entries.push(UpsertKV::insert(pel_k, pel.encode()));
                  group_meta.pending_number += 1;
                  c_meta.pending_number += 1;
                }

                if id > max_id {
                  max_id = id;
                }
                if entries.len() >= limit {
                  break;
                }
              }
            }
          }
          group_meta.last_delivered_id = max_id;
          group_meta.entries_read += entries.len() as i64;
        } else {
          // 读取本 consumer 的 pending 历史消息
          let start_id = StreamId::parse(&start_id_str).unwrap_or(StreamId::min());
          let p_prefix = kc.stream_pel_prefix(&k, &group);
          for (k_bytes, v_bytes) in node.scan_prefix(ScanPrefixReq { prefix: p_prefix }).await? {
            if let Some(id) = extract_stream_id_from_item_key(&k_bytes) {
              if id < start_id {
                continue;
              }
              if let Some(mut pel) = StreamPelEntry::decode(&v_bytes)
                && pel.consumer_name == consumer
              {
                let item_k = kc.stream_item(&k, id.ms, id.seq);
                if let Some(item_bytes) = node.read(GetKVReq { key: item_k }).await?
                  && let Some(fields) = decode_stream_entry_fields(&item_bytes)
                {
                  let mut fv = Vec::with_capacity(fields.len() * 2);
                  for (f, v) in fields {
                    fv.push(RespValue::Blob(f.into_bytes()));
                    fv.push(RespValue::Blob(v));
                  }
                  entries.push(RespValue::Arr(vec![
                    RespValue::Blob(id.to_string_id().into_bytes()),
                    RespValue::Arr(fv),
                  ]));

                  pel.last_delivery_time_ms = now;
                  pel.last_delivery_count += 1;
                  let pel_k = kc.stream_pel_item(&k, &group, id.ms, id.seq);
                  batch_entries.push(UpsertKV::insert(pel_k, pel.encode()));
                  if entries.len() >= limit {
                    break;
                  }
                }
              }
            }
          }
        }

        c_meta.last_attempted_interaction_ms = now;
        c_meta.last_successful_interaction_ms = now;
        batch_entries.push(UpsertKV::insert(c_k, c_meta.encode().to_vec()));
        batch_entries.push(UpsertKV::insert(group_k, group_meta.encode().to_vec()));
        node
          .batch_write(BatchWriteReq {
            entries: batch_entries,
          })
          .await?;

        if !entries.is_empty() {
          stream_results.push(RespValue::Arr(vec![
            RespValue::Blob(k.into_bytes()),
            RespValue::Arr(entries),
          ]));
        }
      }

      if stream_results.is_empty() {
        Ok(RespValue::Null)
      } else {
        Ok(RespValue::Arr(stream_results))
      }
    }

    // ================= 13. XCLAIM =================
    RedisCommand::XClaim {
      key,
      group,
      consumer,
      min_idle,
      ids,
      idle,
      time,
      retrycount,
      force,
      justid,
    } => {
      let group_k = kc.stream_group_meta(&key, &group);
      let mut group_meta = match node
        .read(GetKVReq {
          key: group_k.clone(),
        })
        .await?
      {
        Some(b) => StreamConsumerGroupMeta::decode(&b).unwrap_or_default(),
        None => {
          return Err(Error::invalid_data(
            "NOGROUP No such consumer group for key name",
          ));
        }
      };

      let c_k = kc.stream_consumer_meta(&key, &group, &consumer);
      let mut c_meta = match node.read(GetKVReq { key: c_k.clone() }).await? {
        Some(b) => StreamConsumerMeta::decode(&b).unwrap_or_default(),
        None => {
          group_meta.consumer_number += 1;
          StreamConsumerMeta {
            pending_number: 0,
            last_attempted_interaction_ms: now,
            last_successful_interaction_ms: now,
          }
        }
      };

      let mut batch_entries = Vec::new();
      let mut claimed_entries = Vec::new();
      let mut claimed_ids = Vec::new();

      for id_str in ids {
        if let Ok(id) = StreamId::parse(&id_str) {
          let pel_k = kc.stream_pel_item(&key, &group, id.ms, id.seq);
          let pel_opt = if let Some(b) = node.read(GetKVReq { key: pel_k.clone() }).await? {
            StreamPelEntry::decode(&b)
          } else if force {
            Some(StreamPelEntry {
              last_delivery_time_ms: 0,
              last_delivery_count: 0,
              consumer_name: String::new(),
            })
          } else {
            None
          };

          if let Some(mut pel) = pel_opt {
            let cur_idle = now.saturating_sub(pel.last_delivery_time_ms);
            if cur_idle >= min_idle || force {
              if pel.consumer_name != consumer && !pel.consumer_name.is_empty() {
                let old_c_k = kc.stream_consumer_meta(&key, &group, &pel.consumer_name);
                if let Some(old_bytes) = node
                  .read(GetKVReq {
                    key: old_c_k.clone(),
                  })
                  .await?
                  && let Some(mut old_meta) = StreamConsumerMeta::decode(&old_bytes)
                {
                  old_meta.pending_number = old_meta.pending_number.saturating_sub(1);
                  batch_entries.push(UpsertKV::insert(old_c_k, old_meta.encode().to_vec()));
                }
              }

              pel.consumer_name = consumer.clone();
              pel.last_delivery_time_ms = time.unwrap_or(now.saturating_sub(idle.unwrap_or(0)));
              pel.last_delivery_count = retrycount.unwrap_or(pel.last_delivery_count + 1);
              c_meta.pending_number += 1;
              batch_entries.push(UpsertKV::insert(pel_k, pel.encode()));

              if justid {
                claimed_ids.push(RespValue::Blob(id.to_string_id().into_bytes()));
              } else {
                let item_k = kc.stream_item(&key, id.ms, id.seq);
                if let Some(item_bytes) = node.read(GetKVReq { key: item_k }).await?
                  && let Some(fields) = decode_stream_entry_fields(&item_bytes)
                {
                  let mut fv = Vec::with_capacity(fields.len() * 2);
                  for (f, v) in fields {
                    fv.push(RespValue::Blob(f.into_bytes()));
                    fv.push(RespValue::Blob(v));
                  }
                  claimed_entries.push(RespValue::Arr(vec![
                    RespValue::Blob(id.to_string_id().into_bytes()),
                    RespValue::Arr(fv),
                  ]));
                }
              }
            }
          }
        }
      }

      if !batch_entries.is_empty() {
        batch_entries.push(UpsertKV::insert(c_k, c_meta.encode().to_vec()));
        batch_entries.push(UpsertKV::insert(group_k, group_meta.encode().to_vec()));
        node
          .batch_write(BatchWriteReq {
            entries: batch_entries,
          })
          .await?;
      }

      if justid {
        Ok(RespValue::Arr(claimed_ids))
      } else {
        Ok(RespValue::Arr(claimed_entries))
      }
    }

    // ================= 14. XAUTOCLAIM =================
    RedisCommand::XAutoClaim {
      key,
      group,
      consumer,
      min_idle,
      start,
      count,
      justid,
    } => {
      let start_id = StreamId::parse(&start).unwrap_or(StreamId::min());
      let limit = count.unwrap_or(100);

      let group_k = kc.stream_group_meta(&key, &group);
      let mut group_meta = match node
        .read(GetKVReq {
          key: group_k.clone(),
        })
        .await?
      {
        Some(b) => StreamConsumerGroupMeta::decode(&b).unwrap_or_default(),
        None => {
          return Err(Error::invalid_data(
            "NOGROUP No such consumer group for key name",
          ));
        }
      };

      let c_k = kc.stream_consumer_meta(&key, &group, &consumer);
      let mut c_meta = match node.read(GetKVReq { key: c_k.clone() }).await? {
        Some(b) => StreamConsumerMeta::decode(&b).unwrap_or_default(),
        None => {
          group_meta.consumer_number += 1;
          StreamConsumerMeta {
            pending_number: 0,
            last_attempted_interaction_ms: now,
            last_successful_interaction_ms: now,
          }
        }
      };

      let p_prefix = kc.stream_pel_prefix(&key, &group);
      let pel_items = node.scan_prefix(ScanPrefixReq { prefix: p_prefix }).await?;

      let mut batch_entries = Vec::new();
      let mut claimed_entries = Vec::new();
      let mut claimed_ids = Vec::new();
      let mut next_id = StreamId::min();

      for (k_bytes, v_bytes) in pel_items {
        if let Some(id) = extract_stream_id_from_item_key(&k_bytes) {
          if id < start_id {
            continue;
          }
          if let Some(mut pel) = StreamPelEntry::decode(&v_bytes) {
            let cur_idle = now.saturating_sub(pel.last_delivery_time_ms);
            if cur_idle >= min_idle {
              pel.consumer_name = consumer.clone();
              pel.last_delivery_time_ms = now;
              pel.last_delivery_count += 1;
              c_meta.pending_number += 1;
              let pel_k = kc.stream_pel_item(&key, &group, id.ms, id.seq);
              batch_entries.push(UpsertKV::insert(pel_k, pel.encode()));

              if justid {
                claimed_ids.push(RespValue::Blob(id.to_string_id().into_bytes()));
              } else {
                let item_k = kc.stream_item(&key, id.ms, id.seq);
                if let Some(item_bytes) = node.read(GetKVReq { key: item_k }).await?
                  && let Some(fields) = decode_stream_entry_fields(&item_bytes)
                {
                  let mut fv = Vec::with_capacity(fields.len() * 2);
                  for (f, v) in fields {
                    fv.push(RespValue::Blob(f.into_bytes()));
                    fv.push(RespValue::Blob(v));
                  }
                  claimed_entries.push(RespValue::Arr(vec![
                    RespValue::Blob(id.to_string_id().into_bytes()),
                    RespValue::Arr(fv),
                  ]));
                }
              }
              if claimed_entries.len() + claimed_ids.len() >= limit {
                next_id = id;
                break;
              }
            }
          }
        }
      }

      if !batch_entries.is_empty() {
        batch_entries.push(UpsertKV::insert(c_k, c_meta.encode().to_vec()));
        batch_entries.push(UpsertKV::insert(group_k, group_meta.encode().to_vec()));
        node
          .batch_write(BatchWriteReq {
            entries: batch_entries,
          })
          .await?;
      }

      let entries_resp = if justid {
        RespValue::Arr(claimed_ids)
      } else {
        RespValue::Arr(claimed_entries)
      };

      Ok(RespValue::Arr(vec![
        RespValue::Blob(next_id.to_string_id().into_bytes()),
        entries_resp,
        RespValue::Arr(Vec::new()),
      ]))
    }

    // ================= 15. XSETID =================
    RedisCommand::XSetId {
      key,
      last_id,
      entries_added,
      max_deleted_id,
    } => {
      let meta_k = kc.stream_meta(&key);
      let mut metadata = match node
        .read(GetKVReq {
          key: meta_k.clone(),
        })
        .await?
      {
        Some(b) => StreamMeta::decode(&b).unwrap_or_else(|| StreamMeta::new(0, now)),
        None => StreamMeta::new(0, now),
      };

      let parsed_last_id = StreamId::parse(&last_id).unwrap_or(StreamId::min());
      metadata.last_generated_id = parsed_last_id;
      if let Some(ea) = entries_added {
        metadata.entries_added = ea;
      }
      if let Some(mdi_str) = max_deleted_id
        && let Ok(mdi) = StreamId::parse(&mdi_str)
      {
        metadata.max_deleted_entry_id = mdi;
      }

      let entries = vec![UpsertKV::insert(meta_k, metadata.encode().to_vec())];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }

    _ => Err(Error::redis("Command not matched in handle_stream")),
  }
}
