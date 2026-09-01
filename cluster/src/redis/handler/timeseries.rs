use std::str::from_utf8;
use std::sync::Arc;

use super::context::ConnectionContext;
use crate::error::{Error, Result};
use crate::node::RaftNode;
use crate::redis::cmd::RedisCommand;
use crate::redis::protocol::RespValue;
use crate::redis::resp_util::float_to_blob;
use crate::redis::timeseries::{
  AggregationType, Aggregator, DuplicatePolicy, TimeSeriesLabelFilter, TimeSeriesMeta,
};
use crate::util::now_millis;
use wedb_raft::types::{BatchWriteReq, GetKVReq, ScanPrefixReq, UpsertKV};

#[inline]
fn decode_ts_timestamp(key_bytes: &[u8]) -> Option<u64> {
  let colon_pos = memchr::memrchr(b':', key_bytes)?;
  let hex_str = from_utf8(&key_bytes[colon_pos + 1..]).ok()?;
  u64::from_str_radix(hex_str, 16).ok()
}

#[inline]
fn decode_ts_sample(key_bytes: &[u8], val_bytes: &[u8]) -> Option<(u64, f64)> {
  let ts = decode_ts_timestamp(key_bytes)?;
  if val_bytes.len() < 8 {
    return None;
  }
  let val = f64::from_be_bytes(val_bytes[..8].try_into().ok()?);
  Some((ts, val))
}

/// TimeSeries 时间序列命令主调度处理器
pub async fn handle_timeseries(
  node: &Arc<RaftNode>,
  ctx: &mut ConnectionContext,
  cmd: RedisCommand,
) -> Result<RespValue> {
  let kc = ctx.key_composer();

  match cmd {
    RedisCommand::TsCreate {
      key,
      retention_ms,
      chunk_size,
      duplicate_policy,
      labels,
    } => {
      let meta_k = kc.ts_meta(&key);
      if node
        .read(GetKVReq {
          key: meta_k.clone(),
        })
        .await?
        .is_some()
      {
        return Err(Error::invalid_data("ERR TSDB: key already exists"));
      }
      let dup_policy = duplicate_policy
        .as_deref()
        .and_then(DuplicatePolicy::parse)
        .unwrap_or_default();
      let meta = TimeSeriesMeta::new(retention_ms, chunk_size, dup_policy, labels);
      let entries = vec![UpsertKV::insert(meta_k, meta.encode())];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::TsAlter {
      key,
      retention_ms,
      chunk_size,
      duplicate_policy,
      labels,
    } => {
      let meta_k = kc.ts_meta(&key);
      let mut meta = match node
        .read(GetKVReq {
          key: meta_k.clone(),
        })
        .await?
      {
        Some(b) => TimeSeriesMeta::decode(&b)
          .ok_or_else(|| Error::invalid_data("ERR TSDB: corrupted metadata"))?,
        None => return Err(Error::invalid_data("ERR TSDB: the key does not exist")),
      };
      if let Some(r) = retention_ms {
        meta.retention_time = r;
      }
      if let Some(c) = chunk_size {
        meta.chunk_size = if c == 0 {
          TimeSeriesMeta::DEFAULT_CHUNK_SIZE
        } else {
          c
        };
      }
      if let Some(ref dp) = duplicate_policy
        && let Some(p) = DuplicatePolicy::parse(dp)
      {
        meta.duplicate_policy = p;
      }
      if !labels.is_empty() {
        meta.labels = labels;
      }
      let entries = vec![UpsertKV::insert(meta_k, meta.encode())];
      node.batch_write(BatchWriteReq { entries }).await?;
      Ok(RespValue::ok())
    }
    RedisCommand::TsAdd {
      key,
      timestamp,
      value,
      retention_ms,
      chunk_size,
      on_duplicate,
      labels,
    } => {
      let meta_k = kc.ts_meta(&key);
      let mut meta = match node
        .read(GetKVReq {
          key: meta_k.clone(),
        })
        .await?
      {
        Some(b) => TimeSeriesMeta::decode(&b).unwrap_or_else(|| {
          let dup_p = on_duplicate
            .as_deref()
            .and_then(DuplicatePolicy::parse)
            .unwrap_or_default();
          TimeSeriesMeta::new(
            retention_ms.unwrap_or(0),
            chunk_size.unwrap_or(0),
            dup_p,
            labels.clone(),
          )
        }),
        None => {
          let dup_p = on_duplicate
            .as_deref()
            .and_then(DuplicatePolicy::parse)
            .unwrap_or_default();
          TimeSeriesMeta::new(
            retention_ms.unwrap_or(0),
            chunk_size.unwrap_or(0),
            dup_p,
            labels,
          )
        }
      };

      let ts = timestamp.unwrap_or_else(now_millis);
      let item_k = kc.ts_item(&key, ts);

      let policy = on_duplicate
        .as_deref()
        .and_then(DuplicatePolicy::parse)
        .unwrap_or(meta.duplicate_policy);

      let mut batch_entries = Vec::new();
      if let Some(old_bytes) = node
        .read(GetKVReq {
          key: item_k.clone(),
        })
        .await?
      {
        let old_arr: [u8; 8] = old_bytes[..8.min(old_bytes.len())]
          .try_into()
          .unwrap_or([0; 8]);
        let old_val = f64::from_be_bytes(old_arr);
        match policy.merge_value(old_val, value) {
          Some(merged_val) => {
            batch_entries.push(UpsertKV::insert(item_k, merged_val.to_be_bytes().to_vec()));
          }
          None => {
            return Err(Error::invalid_data(format!(
              "ERR TSDB: for key {key} timestamp {ts} already exists"
            )));
          }
        }
      } else {
        batch_entries.push(UpsertKV::insert(item_k, value.to_be_bytes().to_vec()));
        meta.total_samples += 1;
        if meta.first_time == 0 || ts < meta.first_time {
          meta.first_time = ts;
        }
        if ts > meta.last_time {
          meta.last_time = ts;
        }
      }

      // 物理修剪过期窗口采样点
      if meta.retention_time > 0 && meta.last_time > meta.retention_time {
        let cutoff = meta.last_time - meta.retention_time;
        if meta.first_time < cutoff {
          let prefix = kc.ts_prefix(&key);
          let old_items = node.scan_prefix(ScanPrefixReq { prefix }).await?;
          let mut pruned_count = 0u64;
          let mut new_first_ts = 0u64;

          for (sub_k, _) in old_items {
            if let Some(sub_ts) = decode_ts_timestamp(&sub_k) {
              if sub_ts < cutoff {
                batch_entries.push(UpsertKV::delete(
                  String::from_utf8_lossy(&sub_k).into_owned(),
                ));
                pruned_count += 1;
              } else if new_first_ts == 0 || sub_ts < new_first_ts {
                new_first_ts = sub_ts;
              }
            }
          }
          meta.total_samples = meta.total_samples.saturating_sub(pruned_count);
          meta.first_time = new_first_ts;
        }
      }

      batch_entries.push(UpsertKV::insert(meta_k, meta.encode()));
      node
        .batch_write(BatchWriteReq {
          entries: batch_entries,
        })
        .await?;
      Ok(RespValue::Int(ts as i64))
    }
    RedisCommand::TsMAdd(list) => {
      let mut results = Vec::with_capacity(list.len());
      for (k, t, v) in list {
        let res = match Box::pin(handle_timeseries(
          node,
          ctx,
          RedisCommand::TsAdd {
            key: k,
            timestamp: t,
            value: v,
            retention_ms: None,
            chunk_size: None,
            on_duplicate: None,
            labels: Vec::new(),
          },
        ))
        .await
        {
          Ok(val) => val,
          Err(e) => RespValue::Error(e.to_string()),
        };
        results.push(res);
      }
      Ok(RespValue::Arr(results))
    }
    RedisCommand::TsRange {
      key,
      from_ts,
      to_ts,
      filter_by_ts,
      filter_by_value,
      count,
      aggregation,
      align,
    } => {
      let prefix = kc.ts_prefix(&key);
      let entries = node.scan_prefix(ScanPrefixReq { prefix }).await?;
      let mut samples = Vec::new();

      for (sub_k, sub_v) in entries {
        if let Some((ts, val)) = decode_ts_sample(&sub_k, &sub_v)
          && ts >= from_ts
          && ts <= to_ts
        {
          if !filter_by_ts.is_empty() && !filter_by_ts.contains(&ts) {
            continue;
          }
          if let Some((min_v, max_v)) = filter_by_value
            && (val < min_v || val > max_v)
          {
            continue;
          }
          samples.push((ts, val));
        }
      }

      if let Some((agg_name, bucket_duration)) = aggregation {
        let agg_type = AggregationType::parse(&agg_name).unwrap_or(AggregationType::Avg);
        let alignment = align.unwrap_or(from_ts);
        let aggregator = Aggregator::new(agg_type, bucket_duration, alignment);
        let agg_samples = aggregator.split_and_aggregate(&samples, count);
        let resp_list = agg_samples
          .into_iter()
          .map(|(t, v)| RespValue::Arr(vec![RespValue::Int(t as i64), float_to_blob(v)]))
          .collect();
        Ok(RespValue::Arr(resp_list))
      } else {
        let iter = samples.into_iter();
        let list: Vec<RespValue> = if let Some(c) = count {
          iter
            .take(c)
            .map(|(t, v)| RespValue::Arr(vec![RespValue::Int(t as i64), float_to_blob(v)]))
            .collect()
        } else {
          iter
            .map(|(t, v)| RespValue::Arr(vec![RespValue::Int(t as i64), float_to_blob(v)]))
            .collect()
        };
        Ok(RespValue::Arr(list))
      }
    }
    RedisCommand::TsRevRange {
      key,
      from_ts,
      to_ts,
      filter_by_ts,
      filter_by_value,
      count,
      aggregation,
      align,
    } => {
      let prefix = kc.ts_prefix(&key);
      let entries = node.scan_prefix(ScanPrefixReq { prefix }).await?;
      let mut samples = Vec::new();

      for (sub_k, sub_v) in entries {
        if let Some((ts, val)) = decode_ts_sample(&sub_k, &sub_v)
          && ts >= from_ts
          && ts <= to_ts
        {
          if !filter_by_ts.is_empty() && !filter_by_ts.contains(&ts) {
            continue;
          }
          if let Some((min_v, max_v)) = filter_by_value
            && (val < min_v || val > max_v)
          {
            continue;
          }
          samples.push((ts, val));
        }
      }

      if let Some((agg_name, bucket_duration)) = aggregation {
        let agg_type = AggregationType::parse(&agg_name).unwrap_or(AggregationType::Avg);
        let alignment = align.unwrap_or(from_ts);
        let aggregator = Aggregator::new(agg_type, bucket_duration, alignment);
        let mut agg_samples = aggregator.split_and_aggregate(&samples, None);
        agg_samples.reverse();
        let iter = agg_samples.into_iter();
        let resp_list: Vec<RespValue> = if let Some(c) = count {
          iter
            .take(c)
            .map(|(t, v)| RespValue::Arr(vec![RespValue::Int(t as i64), float_to_blob(v)]))
            .collect()
        } else {
          iter
            .map(|(t, v)| RespValue::Arr(vec![RespValue::Int(t as i64), float_to_blob(v)]))
            .collect()
        };
        Ok(RespValue::Arr(resp_list))
      } else {
        samples.reverse();
        let iter = samples.into_iter();
        let list: Vec<RespValue> = if let Some(c) = count {
          iter
            .take(c)
            .map(|(t, v)| RespValue::Arr(vec![RespValue::Int(t as i64), float_to_blob(v)]))
            .collect()
        } else {
          iter
            .map(|(t, v)| RespValue::Arr(vec![RespValue::Int(t as i64), float_to_blob(v)]))
            .collect()
        };
        Ok(RespValue::Arr(list))
      }
    }
    RedisCommand::TsGet { key, latest: _ } => {
      let meta_k = kc.ts_meta(&key);
      if let Some(b) = node.read(GetKVReq { key: meta_k }).await?
        && let Some(meta) = TimeSeriesMeta::decode(&b)
        && meta.last_time > 0
      {
        let item_k = kc.ts_item(&key, meta.last_time);
        if let Some(val_b) = node.read(GetKVReq { key: item_k }).await?
          && val_b.len() >= 8
          && let Ok(bytes) = val_b[..8].try_into()
        {
          let val = f64::from_be_bytes(bytes);
          return Ok(RespValue::Arr(vec![
            RespValue::Int(meta.last_time as i64),
            float_to_blob(val),
          ]));
        }
      }
      // 回退扫描
      let prefix = kc.ts_prefix(&key);
      let entries = node.scan_prefix(ScanPrefixReq { prefix }).await?;
      if let Some((last_k, last_v)) = entries.last()
        && let Some((ts, val)) = decode_ts_sample(last_k, last_v)
      {
        return Ok(RespValue::Arr(vec![
          RespValue::Int(ts as i64),
          float_to_blob(val),
        ]));
      }
      Ok(RespValue::Arr(Vec::new()))
    }
    RedisCommand::TsCreateRule(_, _, _, _) => Ok(RespValue::ok()),
    RedisCommand::TsMGet {
      with_labels,
      selected_labels,
      filters,
    } => {
      let filter = TimeSeriesLabelFilter::parse(&filters);
      let meta_prefix = kc.ts_meta_prefix();
      let all_series = node
        .scan_prefix(ScanPrefixReq {
          prefix: meta_prefix,
        })
        .await?;

      let mut results = Vec::new();
      for (meta_k, meta_v) in all_series {
        if let Some(user_k_bytes) = kc.extract_user_key(&meta_k)
          && let Some(meta) = TimeSeriesMeta::decode(&meta_v)
        {
          if !filters.is_empty() && !filter.matches(&meta.labels) {
            continue;
          }

          let user_key = String::from_utf8_lossy(user_k_bytes).into_owned();
          let mut labels_arr = Vec::new();
          if !selected_labels.is_empty() {
            for (lk, lv) in &meta.labels {
              if selected_labels.contains(lk) {
                labels_arr.push(RespValue::Arr(vec![
                  RespValue::Blob(lk.clone().into_bytes()),
                  RespValue::Blob(lv.clone().into_bytes()),
                ]));
              }
            }
          } else if with_labels {
            for (lk, lv) in &meta.labels {
              labels_arr.push(RespValue::Arr(vec![
                RespValue::Blob(lk.clone().into_bytes()),
                RespValue::Blob(lv.clone().into_bytes()),
              ]));
            }
          }

          let last_sample = if meta.last_time > 0 {
            if let Some(b) = node
              .read(GetKVReq {
                key: kc.ts_item(&user_key, meta.last_time),
              })
              .await?
              && b.len() >= 8
              && let Ok(bytes) = b[..8].try_into()
            {
              let val = f64::from_be_bytes(bytes);
              RespValue::Arr(vec![
                RespValue::Int(meta.last_time as i64),
                float_to_blob(val),
              ])
            } else {
              RespValue::Arr(Vec::new())
            }
          } else {
            RespValue::Arr(Vec::new())
          };

          results.push(RespValue::Arr(vec![
            RespValue::Blob(user_key.into_bytes()),
            RespValue::Arr(labels_arr),
            last_sample,
          ]));
        }
      }

      Ok(RespValue::Arr(results))
    }
    RedisCommand::TsMRange {
      from_ts,
      to_ts,
      filter_by_ts,
      filter_by_value,
      count,
      aggregation,
      align,
      with_labels,
      selected_labels,
      filters,
    } => {
      let filter = TimeSeriesLabelFilter::parse(&filters);
      let meta_prefix = kc.ts_meta_prefix();
      let all_series = node
        .scan_prefix(ScanPrefixReq {
          prefix: meta_prefix,
        })
        .await?;

      let mut results = Vec::new();
      for (meta_k, meta_v) in all_series {
        if let Some(user_k_bytes) = kc.extract_user_key(&meta_k)
          && let Some(meta) = TimeSeriesMeta::decode(&meta_v)
        {
          if !filters.is_empty() && !filter.matches(&meta.labels) {
            continue;
          }

          let user_key = String::from_utf8_lossy(user_k_bytes).into_owned();
          let mut labels_arr = Vec::new();
          if !selected_labels.is_empty() {
            for (lk, lv) in &meta.labels {
              if selected_labels.contains(lk) {
                labels_arr.push(RespValue::Arr(vec![
                  RespValue::Blob(lk.clone().into_bytes()),
                  RespValue::Blob(lv.clone().into_bytes()),
                ]));
              }
            }
          } else if with_labels {
            for (lk, lv) in &meta.labels {
              labels_arr.push(RespValue::Arr(vec![
                RespValue::Blob(lk.clone().into_bytes()),
                RespValue::Blob(lv.clone().into_bytes()),
              ]));
            }
          }

          let prefix = kc.ts_prefix(&user_key);
          let sample_entries = node.scan_prefix(ScanPrefixReq { prefix }).await?;
          let mut samples = Vec::new();
          for (sub_k, sub_v) in sample_entries {
            if let Some((ts, val)) = decode_ts_sample(&sub_k, &sub_v)
              && ts >= from_ts
              && ts <= to_ts
            {
              if !filter_by_ts.is_empty() && !filter_by_ts.contains(&ts) {
                continue;
              }
              if let Some((min_v, max_v)) = filter_by_value
                && (val < min_v || val > max_v)
              {
                continue;
              }
              samples.push((ts, val));
            }
          }

          let samples_resp: Vec<RespValue> =
            if let Some((ref agg_name, bucket_duration)) = aggregation {
              let agg_type = AggregationType::parse(agg_name).unwrap_or(AggregationType::Avg);
              let alignment = align.unwrap_or(from_ts);
              let aggregator = Aggregator::new(agg_type, bucket_duration, alignment);
              aggregator
                .split_and_aggregate(&samples, count)
                .into_iter()
                .map(|(t, v)| RespValue::Arr(vec![RespValue::Int(t as i64), float_to_blob(v)]))
                .collect()
            } else {
              let iter = samples.into_iter();
              if let Some(c) = count {
                iter
                  .take(c)
                  .map(|(t, v)| RespValue::Arr(vec![RespValue::Int(t as i64), float_to_blob(v)]))
                  .collect()
              } else {
                iter
                  .map(|(t, v)| RespValue::Arr(vec![RespValue::Int(t as i64), float_to_blob(v)]))
                  .collect()
              }
            };

          results.push(RespValue::Arr(vec![
            RespValue::Blob(user_key.into_bytes()),
            RespValue::Arr(labels_arr),
            RespValue::Arr(samples_resp),
          ]));
        }
      }

      Ok(RespValue::Arr(results))
    }
    RedisCommand::TsMRevRange {
      from_ts,
      to_ts,
      filter_by_ts,
      filter_by_value,
      count,
      aggregation,
      align,
      with_labels,
      selected_labels,
      filters,
    } => {
      let filter = TimeSeriesLabelFilter::parse(&filters);
      let meta_prefix = kc.ts_meta_prefix();
      let all_series = node
        .scan_prefix(ScanPrefixReq {
          prefix: meta_prefix,
        })
        .await?;

      let mut results = Vec::new();
      for (meta_k, meta_v) in all_series {
        if let Some(user_k_bytes) = kc.extract_user_key(&meta_k)
          && let Some(meta) = TimeSeriesMeta::decode(&meta_v)
        {
          if !filters.is_empty() && !filter.matches(&meta.labels) {
            continue;
          }

          let user_key = String::from_utf8_lossy(user_k_bytes).into_owned();
          let mut labels_arr = Vec::new();
          if !selected_labels.is_empty() {
            for (lk, lv) in &meta.labels {
              if selected_labels.contains(lk) {
                labels_arr.push(RespValue::Arr(vec![
                  RespValue::Blob(lk.clone().into_bytes()),
                  RespValue::Blob(lv.clone().into_bytes()),
                ]));
              }
            }
          } else if with_labels {
            for (lk, lv) in &meta.labels {
              labels_arr.push(RespValue::Arr(vec![
                RespValue::Blob(lk.clone().into_bytes()),
                RespValue::Blob(lv.clone().into_bytes()),
              ]));
            }
          }

          let prefix = kc.ts_prefix(&user_key);
          let sample_entries = node.scan_prefix(ScanPrefixReq { prefix }).await?;
          let mut samples = Vec::new();
          for (sub_k, sub_v) in sample_entries {
            if let Some((ts, val)) = decode_ts_sample(&sub_k, &sub_v)
              && ts >= from_ts
              && ts <= to_ts
            {
              if !filter_by_ts.is_empty() && !filter_by_ts.contains(&ts) {
                continue;
              }
              if let Some((min_v, max_v)) = filter_by_value
                && (val < min_v || val > max_v)
              {
                continue;
              }
              samples.push((ts, val));
            }
          }

          let samples_resp: Vec<RespValue> =
            if let Some((ref agg_name, bucket_duration)) = aggregation {
              let agg_type = AggregationType::parse(agg_name).unwrap_or(AggregationType::Avg);
              let alignment = align.unwrap_or(from_ts);
              let aggregator = Aggregator::new(agg_type, bucket_duration, alignment);
              let mut agg_samples = aggregator.split_and_aggregate(&samples, None);
              agg_samples.reverse();
              let iter = agg_samples.into_iter();
              if let Some(c) = count {
                iter
                  .take(c)
                  .map(|(t, v)| RespValue::Arr(vec![RespValue::Int(t as i64), float_to_blob(v)]))
                  .collect()
              } else {
                iter
                  .map(|(t, v)| RespValue::Arr(vec![RespValue::Int(t as i64), float_to_blob(v)]))
                  .collect()
              }
            } else {
              samples.reverse();
              let iter = samples.into_iter();
              if let Some(c) = count {
                iter
                  .take(c)
                  .map(|(t, v)| RespValue::Arr(vec![RespValue::Int(t as i64), float_to_blob(v)]))
                  .collect()
              } else {
                iter
                  .map(|(t, v)| RespValue::Arr(vec![RespValue::Int(t as i64), float_to_blob(v)]))
                  .collect()
              }
            };

          results.push(RespValue::Arr(vec![
            RespValue::Blob(user_key.into_bytes()),
            RespValue::Arr(labels_arr),
            RespValue::Arr(samples_resp),
          ]));
        }
      }

      Ok(RespValue::Arr(results))
    }
    RedisCommand::TsIncrBy {
      key,
      value,
      timestamp,
      retention_ms,
      chunk_size,
      labels,
    } => {
      let last_v = match Box::pin(handle_timeseries(
        node,
        ctx,
        RedisCommand::TsGet {
          key: key.clone(),
          latest: true,
        },
      ))
      .await?
      {
        RespValue::Arr(arr) if arr.len() >= 2 => {
          if let RespValue::Blob(ref b) = arr[1] {
            from_utf8(b)
              .ok()
              .and_then(|s| s.parse::<f64>().ok())
              .unwrap_or(0.0)
          } else {
            0.0
          }
        }
        _ => 0.0,
      };
      let new_v = last_v + value;
      Box::pin(handle_timeseries(
        node,
        ctx,
        RedisCommand::TsAdd {
          key,
          timestamp,
          value: new_v,
          retention_ms,
          chunk_size,
          on_duplicate: None,
          labels,
        },
      ))
      .await
    }
    RedisCommand::TsDecrBy {
      key,
      value,
      timestamp,
      retention_ms,
      chunk_size,
      labels,
    } => {
      let last_v = match Box::pin(handle_timeseries(
        node,
        ctx,
        RedisCommand::TsGet {
          key: key.clone(),
          latest: true,
        },
      ))
      .await?
      {
        RespValue::Arr(arr) if arr.len() >= 2 => {
          if let RespValue::Blob(ref b) = arr[1] {
            from_utf8(b)
              .ok()
              .and_then(|s| s.parse::<f64>().ok())
              .unwrap_or(0.0)
          } else {
            0.0
          }
        }
        _ => 0.0,
      };
      let new_v = last_v - value;
      Box::pin(handle_timeseries(
        node,
        ctx,
        RedisCommand::TsAdd {
          key,
          timestamp,
          value: new_v,
          retention_ms,
          chunk_size,
          on_duplicate: None,
          labels,
        },
      ))
      .await
    }
    RedisCommand::TsDel(key, from_ts, to_ts) => {
      let prefix = kc.ts_prefix(&key);
      let entries = node.scan_prefix(ScanPrefixReq { prefix }).await?;
      let mut to_delete = Vec::new();
      let mut remaining_first_ts = 0u64;
      let mut remaining_last_ts = 0u64;
      let mut remaining_count = 0u64;

      for (sub_k, _) in entries {
        if let Some(ts) = decode_ts_timestamp(&sub_k) {
          if ts >= from_ts && ts <= to_ts {
            to_delete.push(UpsertKV::delete(
              String::from_utf8_lossy(&sub_k).into_owned(),
            ));
          } else {
            remaining_count += 1;
            if remaining_first_ts == 0 || ts < remaining_first_ts {
              remaining_first_ts = ts;
            }
            if ts > remaining_last_ts {
              remaining_last_ts = ts;
            }
          }
        }
      }

      let deleted_len = to_delete.len();
      if deleted_len > 0 {
        let meta_k = kc.ts_meta(&key);
        if let Some(b) = node
          .read(GetKVReq {
            key: meta_k.clone(),
          })
          .await?
          && let Some(mut meta) = TimeSeriesMeta::decode(&b)
        {
          meta.total_samples = remaining_count;
          meta.first_time = remaining_first_ts;
          meta.last_time = remaining_last_ts;
          to_delete.push(UpsertKV::insert(meta_k, meta.encode()));
        }
        node
          .batch_write(BatchWriteReq { entries: to_delete })
          .await?;
      }
      Ok(RespValue::Int(deleted_len as i64))
    }
    RedisCommand::TsInfo(key) => {
      let meta_k = kc.ts_meta(&key);
      let meta: TimeSeriesMeta = match node.read(GetKVReq { key: meta_k }).await? {
        Some(b) => TimeSeriesMeta::decode(&b)
          .ok_or_else(|| Error::invalid_data("ERR TSDB: corrupted metadata"))?,
        None => return Err(Error::invalid_data("ERR TSDB: the key does not exist")),
      };

      let mut labels_arr = Vec::with_capacity(meta.labels.len());
      for (lk, lv) in &meta.labels {
        labels_arr.push(RespValue::Arr(vec![
          RespValue::Blob(lk.clone().into_bytes()),
          RespValue::Blob(lv.clone().into_bytes()),
        ]));
      }

      let chunk_count = meta.total_samples.div_ceil(meta.chunk_size.max(1));

      let info = vec![
        RespValue::Simple("totalSamples".to_string()),
        RespValue::Int(meta.total_samples as i64),
        RespValue::Simple("memoryUsage".to_string()),
        RespValue::Int((meta.total_samples * 24 + 128) as i64),
        RespValue::Simple("firstTimestamp".to_string()),
        RespValue::Int(meta.first_time as i64),
        RespValue::Simple("lastTimestamp".to_string()),
        RespValue::Int(meta.last_time as i64),
        RespValue::Simple("retentionTime".to_string()),
        RespValue::Int(meta.retention_time as i64),
        RespValue::Simple("chunkCount".to_string()),
        RespValue::Int(chunk_count as i64),
        RespValue::Simple("chunkSize".to_string()),
        RespValue::Int(meta.chunk_size as i64),
        RespValue::Simple("duplicatePolicy".to_string()),
        RespValue::Simple(meta.duplicate_policy.as_str().to_string()),
        RespValue::Simple("labels".to_string()),
        RespValue::Arr(labels_arr),
      ];
      Ok(RespValue::Arr(info))
    }
    RedisCommand::TsQueryIndex(filters) => {
      let filter = TimeSeriesLabelFilter::parse(&filters);
      let meta_prefix = kc.ts_meta_prefix();
      let all_series = node
        .scan_prefix(ScanPrefixReq {
          prefix: meta_prefix,
        })
        .await?;

      let mut matched_keys = Vec::new();
      for (meta_k, meta_v) in all_series {
        if let Some(user_k_bytes) = kc.extract_user_key(&meta_k)
          && let Some(meta) = TimeSeriesMeta::decode(&meta_v)
          && filter.matches(&meta.labels)
        {
          matched_keys.push(RespValue::Blob(user_k_bytes.to_vec()));
        }
      }
      Ok(RespValue::Arr(matched_keys))
    }
    _ => Err(Error::internal("unsupported timeseries command")),
  }
}
