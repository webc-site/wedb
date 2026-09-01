use crate::handler::resp_util::float_to_blob;
use std::sync::Arc;
use webc_cmd::Cmd;
use wedb_embed::{
  AggregationType, Aggregator, DuplicatePolicy, Error, GroupReducerType, Result, TSCreateOption,
  TSMGetOption, TSMRangeOption, TSRangeOption, WeDb, current_now_ms,
};
use wedb_resp::RespValue;

/// 处理所有 TimeSeries (时间序列) 命令
pub async fn handle_timeseries(db: &Arc<WeDb>, cmd: Cmd) -> Result<RespValue> {
  match cmd {
    Cmd::TsCreate {
      key,
      retention_ms,
      chunk_size,
      duplicate_policy,
      labels,
    } => {
      let dup_policy = duplicate_policy
        .as_deref()
        .and_then(DuplicatePolicy::parse)
        .unwrap_or(DuplicatePolicy::Block);
      let opt = TSCreateOption {
        retention_time: retention_ms,
        chunk_size,
        duplicate_policy: dup_policy,
        labels,
        ..Default::default()
      };
      db.ts_create_opt(key.as_bytes(), &opt)?;
      Ok(RespValue::ok())
    }
    Cmd::TsAlter {
      key,
      retention_ms,
      chunk_size,
      duplicate_policy,
      labels,
    } => {
      let dup = duplicate_policy.as_deref().and_then(DuplicatePolicy::parse);
      db.ts_alter(key.as_bytes(), retention_ms, chunk_size, dup, Some(labels))?;
      Ok(RespValue::ok())
    }
    Cmd::TsAdd {
      key,
      timestamp,
      value,
      retention_ms,
      chunk_size,
      on_duplicate,
      labels,
    } => {
      let ts = timestamp.unwrap_or_else(current_now_ms);
      let dup = on_duplicate.as_deref().and_then(DuplicatePolicy::parse);
      let opt = TSCreateOption {
        retention_time: retention_ms.unwrap_or(0),
        chunk_size: chunk_size.unwrap_or(0),
        duplicate_policy: dup.unwrap_or(DuplicatePolicy::Block),
        labels,
        ..Default::default()
      };
      let added_ts = db.ts_add_opt(key.as_bytes(), ts, value, dup, Some(&opt))?;
      Ok(RespValue::Int(added_ts as i64))
    }
    Cmd::TsMAdd(triplets) => {
      let mut list = Vec::with_capacity(triplets.len());
      for (k, ts, v) in &triplets {
        let t = ts.unwrap_or_else(current_now_ms);
        list.push((k.as_bytes(), t, *v));
      }
      let results = db.ts_madd(&list)?;
      let arr = results
        .into_iter()
        .map(|r| match r {
          Ok(ts) => RespValue::Int(ts as i64),
          Err(e) => RespValue::error(format!("ERR {e}")),
        })
        .collect();
      Ok(RespValue::Arr(arr))
    }
    Cmd::TsRange {
      key,
      from_ts,
      to_ts,
      count,
      aggregation,
      align,
      ..
    } => {
      let aggregator = aggregation.as_ref().map(|(agg_s, d)| {
        let agg_type = AggregationType::parse(agg_s).unwrap_or(AggregationType::Avg);
        Aggregator::new(agg_type, *d, align.unwrap_or(0))
      });
      let opt = TSRangeOption {
        start_ts: from_ts,
        end_ts: to_ts,
        count_limit: count,
        aggregator,
        ..Default::default()
      };
      let samples = db.ts_range_opt(key.as_bytes(), &opt)?;
      let mut arr = Vec::with_capacity(samples.len());
      for (ts, val) in samples {
        arr.push(RespValue::Arr(vec![
          RespValue::Int(ts as i64),
          float_to_blob(val),
        ]));
      }
      Ok(RespValue::Arr(arr))
    }
    Cmd::TsRevRange {
      key,
      from_ts,
      to_ts,
      count,
      aggregation,
      align,
      ..
    } => {
      let aggregator = aggregation.as_ref().map(|(agg_s, d)| {
        let agg_type = AggregationType::parse(agg_s).unwrap_or(AggregationType::Avg);
        Aggregator::new(agg_type, *d, align.unwrap_or(0))
      });
      let opt = TSRangeOption {
        start_ts: from_ts,
        end_ts: to_ts,
        count_limit: count,
        aggregator,
        ..Default::default()
      };
      let mut samples = db.ts_range_opt(key.as_bytes(), &opt)?;
      samples.reverse();
      let mut arr = Vec::with_capacity(samples.len());
      for (ts, val) in samples {
        arr.push(RespValue::Arr(vec![
          RespValue::Int(ts as i64),
          float_to_blob(val),
        ]));
      }
      Ok(RespValue::Arr(arr))
    }
    Cmd::TsGet { key, .. } => match db.ts_get(key.as_bytes())? {
      Some((ts, val)) => Ok(RespValue::Arr(vec![
        RespValue::Int(ts as i64),
        float_to_blob(val),
      ])),
      None => Ok(RespValue::Null),
    },
    Cmd::TsInfo(key) => {
      let info = db.ts_info(key.as_bytes())?;
      let mut label_arr = Vec::with_capacity(info.labels.len());
      for (lk, lv) in info.labels {
        label_arr.push(RespValue::Arr(vec![
          RespValue::Blob(lk.into_bytes()),
          RespValue::Blob(lv.into_bytes()),
        ]));
      }
      let entries = vec![
        RespValue::Simple("totalSamples".to_string()),
        RespValue::Int(info.total_samples as i64),
        RespValue::Simple("memoryUsage".to_string()),
        RespValue::Int(info.memory_usage as i64),
        RespValue::Simple("firstTimestamp".to_string()),
        RespValue::Int(info.first_timestamp as i64),
        RespValue::Simple("lastTimestamp".to_string()),
        RespValue::Int(info.last_timestamp as i64),
        RespValue::Simple("retentionTime".to_string()),
        RespValue::Int(info.retention_time as i64),
        RespValue::Simple("chunkSize".to_string()),
        RespValue::Int(info.chunk_size as i64),
        RespValue::Simple("duplicatePolicy".to_string()),
        RespValue::Simple(format!("{:?}", info.duplicate_policy)),
        RespValue::Simple("labels".to_string()),
        RespValue::Arr(label_arr),
      ];
      Ok(RespValue::Arr(entries))
    }
    Cmd::TsCreateRule(src, dst, agg_str, bucket_duration) => {
      let agg = AggregationType::parse(&agg_str)
        .ok_or_else(|| Error::invalid_data("ERR unknown aggregation type"))?;
      db.ts_createrule(src.as_bytes(), dst.as_bytes(), agg, bucket_duration, None)?;
      Ok(RespValue::ok())
    }
    Cmd::TsMGet {
      with_labels,
      selected_labels,
      filters,
    } => {
      let opt = TSMGetOption {
        filters,
        with_labels,
        selected_labels: selected_labels.into_iter().collect(),
      };
      let results = db.ts_mget(&opt)?;
      let mut arr = Vec::with_capacity(results.len());
      for res in results {
        let mut row = vec![RespValue::Blob(res.name.into_bytes())];
        let mut label_arr = Vec::new();
        for (lk, lv) in res.labels {
          label_arr.push(RespValue::Arr(vec![
            RespValue::Blob(lk.into_bytes()),
            RespValue::Blob(lv.into_bytes()),
          ]));
        }
        row.push(RespValue::Arr(label_arr));
        if let Some((ts, val)) = res.sample {
          row.push(RespValue::Arr(vec![
            RespValue::Int(ts as i64),
            float_to_blob(val),
          ]));
        } else {
          row.push(RespValue::Arr(Vec::new()));
        }
        arr.push(RespValue::Arr(row));
      }
      Ok(RespValue::Arr(arr))
    }
    Cmd::TsMRange {
      from_ts,
      to_ts,
      count,
      aggregation,
      align,
      with_labels,
      selected_labels,
      filters,
      ..
    } => {
      let aggregator = aggregation.as_ref().map(|(agg_s, d)| {
        let agg_type = AggregationType::parse(agg_s).unwrap_or(AggregationType::Avg);
        Aggregator::new(agg_type, *d, align.unwrap_or(0))
      });
      let opt = TSMRangeOption {
        mget: TSMGetOption {
          filters,
          with_labels,
          selected_labels: selected_labels.into_iter().collect(),
        },
        range: TSRangeOption {
          start_ts: from_ts,
          end_ts: to_ts,
          count_limit: count,
          aggregator,
          ..Default::default()
        },
        reducer: GroupReducerType::None,
        group_by_label: None,
      };
      let results = db.ts_mrange(&opt)?;
      let mut arr = Vec::with_capacity(results.len());
      for res in results {
        let mut row = vec![RespValue::Blob(res.name.into_bytes())];
        let mut label_arr = Vec::new();
        for (lk, lv) in res.labels {
          label_arr.push(RespValue::Arr(vec![
            RespValue::Blob(lk.into_bytes()),
            RespValue::Blob(lv.into_bytes()),
          ]));
        }
        row.push(RespValue::Arr(label_arr));
        let mut sample_arr = Vec::with_capacity(res.samples.len());
        for (ts, val) in res.samples {
          sample_arr.push(RespValue::Arr(vec![
            RespValue::Int(ts as i64),
            float_to_blob(val),
          ]));
        }
        row.push(RespValue::Arr(sample_arr));
        arr.push(RespValue::Arr(row));
      }
      Ok(RespValue::Arr(arr))
    }
    Cmd::TsMRevRange {
      from_ts,
      to_ts,
      count,
      aggregation,
      align,
      with_labels,
      selected_labels,
      filters,
      ..
    } => {
      let aggregator = aggregation.as_ref().map(|(agg_s, d)| {
        let agg_type = AggregationType::parse(agg_s).unwrap_or(AggregationType::Avg);
        Aggregator::new(agg_type, *d, align.unwrap_or(0))
      });
      let opt = TSMRangeOption {
        mget: TSMGetOption {
          filters,
          with_labels,
          selected_labels: selected_labels.into_iter().collect(),
        },
        range: TSRangeOption {
          start_ts: from_ts,
          end_ts: to_ts,
          count_limit: count,
          aggregator,
          ..Default::default()
        },
        reducer: GroupReducerType::None,
        group_by_label: None,
      };
      let results = db.ts_mrevrange(&opt)?;
      let mut arr = Vec::with_capacity(results.len());
      for res in results {
        let mut row = vec![RespValue::Blob(res.name.into_bytes())];
        let mut label_arr = Vec::new();
        for (lk, lv) in res.labels {
          label_arr.push(RespValue::Arr(vec![
            RespValue::Blob(lk.into_bytes()),
            RespValue::Blob(lv.into_bytes()),
          ]));
        }
        row.push(RespValue::Arr(label_arr));
        let mut sample_arr = Vec::with_capacity(res.samples.len());
        for (ts, val) in res.samples {
          sample_arr.push(RespValue::Arr(vec![
            RespValue::Int(ts as i64),
            float_to_blob(val),
          ]));
        }
        row.push(RespValue::Arr(sample_arr));
        arr.push(RespValue::Arr(row));
      }
      Ok(RespValue::Arr(arr))
    }
    Cmd::TsIncrBy {
      key,
      value,
      timestamp,
      retention_ms,
      chunk_size,
      labels,
    } => {
      let opt = TSCreateOption {
        retention_time: retention_ms.unwrap_or(0),
        chunk_size: chunk_size.unwrap_or(0),
        labels,
        ..Default::default()
      };
      let res_ts = db.ts_incrby_opt(key.as_bytes(), value, timestamp, Some(&opt))?;
      Ok(RespValue::Int(res_ts as i64))
    }
    Cmd::TsDecrBy {
      key,
      value,
      timestamp,
      retention_ms,
      chunk_size,
      labels,
    } => {
      let opt = TSCreateOption {
        retention_time: retention_ms.unwrap_or(0),
        chunk_size: chunk_size.unwrap_or(0),
        labels,
        ..Default::default()
      };
      let res_ts = db.ts_decrby_opt(key.as_bytes(), value, timestamp, Some(&opt))?;
      Ok(RespValue::Int(res_ts as i64))
    }
    Cmd::TsDel(key, from_ts, to_ts) => {
      let count = db.ts_del(key.as_bytes(), from_ts, to_ts)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::TsQueryIndex(filters) => {
      let keys = db.ts_queryindex(&filters)?;
      let arr = keys
        .into_iter()
        .map(|k| RespValue::Blob(k.into_bytes()))
        .collect();
      Ok(RespValue::Arr(arr))
    }
    _ => Err(Error::internal("unsupported timeseries command")),
  }
}
