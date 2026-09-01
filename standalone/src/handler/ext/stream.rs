use std::sync::Arc;
use webc_cmd::Cmd;
use wedb_embed::{
  Error, NextStreamEntryIdStrategy, Result, StreamAddOptions, StreamAutoClaimOptions,
  StreamClaimOptions, StreamEntry, StreamId, StreamPendingOptions, StreamRangeOptions,
  StreamTrimOptions, WeDb, current_now_ms,
};
use wedb_resp::RespValue;

/// 将 Stream 条目序列化为标准 Redis RESP 格式
#[inline]
pub fn stream_entries_to_resp(entries: Vec<StreamEntry>) -> RespValue {
  let mut arr = Vec::with_capacity(entries.len());
  for (id, fields) in entries {
    let id_str = format!("{id}");
    let mut fields_arr = Vec::with_capacity(fields.len() * 2);
    for (k, v) in fields {
      fields_arr.push(RespValue::Blob(k.into_bytes()));
      fields_arr.push(RespValue::Blob(v.into_bytes()));
    }
    arr.push(RespValue::Arr(vec![
      RespValue::Blob(id_str.into_bytes()),
      RespValue::Arr(fields_arr),
    ]));
  }
  RespValue::Arr(arr)
}

/// 解析 StreamId 字符串
#[inline]
fn parse_stream_id(s: &str) -> Result<StreamId> {
  if let Some((ms_str, seq_str)) = s.split_once('-') {
    let ms = ms_str.parse::<u64>().map_err(|_| {
      Error::invalid_data("ERR Invalid stream ID specified as stream command argument")
    })?;
    let seq = seq_str.parse::<u64>().map_err(|_| {
      Error::invalid_data("ERR Invalid stream ID specified as stream command argument")
    })?;
    Ok(StreamId { ms, seq })
  } else {
    let ms = s.parse::<u64>().map_err(|_| {
      Error::invalid_data("ERR Invalid stream ID specified as stream command argument")
    })?;
    Ok(StreamId { ms, seq: 0 })
  }
}

/// 处理所有 Stream (流数据) 命令
pub async fn handle_stream(db: &Arc<WeDb>, cmd: Cmd) -> Result<RespValue> {
  match cmd {
    Cmd::XAdd {
      key,
      id,
      fields,
      nomkstream,
      max_len,
      min_id,
      limit,
      ..
    } => {
      let next_id_strategy = NextStreamEntryIdStrategy::parse(&id)?;

      let trim = if let Some(m) = max_len {
        let mut opt = StreamTrimOptions::maxlen(m as u64);
        if let Some(lim) = limit {
          opt = opt.with_limit(lim);
        }
        Some(opt)
      } else if let Some(ref min_s) = min_id {
        let mut opt = StreamTrimOptions::minid(parse_stream_id(min_s)?);
        if let Some(lim) = limit {
          opt = opt.with_limit(lim);
        }
        Some(opt)
      } else {
        None
      };

      let options = StreamAddOptions {
        next_id_strategy,
        nomkstream,
        trim_options: trim.unwrap_or(StreamTrimOptions::none()),
      };

      let pairs: Vec<(&[u8], &[u8])> = fields
        .iter()
        .map(|(k, v)| (k.as_bytes(), v.as_slice()))
        .collect();
      let generated_id = db.xadd(key.as_bytes(), options, &pairs)?;
      let id_str = format!("{generated_id}");
      Ok(RespValue::Blob(id_str.into_bytes()))
    }
    Cmd::XLen(key) => {
      let len = db.xlen(key.as_bytes())?;
      Ok(RespValue::Int(len as i64))
    }
    Cmd::XRange {
      key,
      start,
      end,
      count,
    } => {
      let start_id = if start == "-" {
        StreamId::min()
      } else {
        parse_stream_id(&start)?
      };
      let end_id = if end == "+" {
        StreamId::max()
      } else {
        parse_stream_id(&end)?
      };
      let options = StreamRangeOptions {
        start: start_id,
        end: end_id,
        count,
        reverse: false,
        exclude_start: false,
        exclude_end: false,
      };
      let entries = db.xrange_with_options(key.as_bytes(), options)?;
      Ok(stream_entries_to_resp(entries))
    }
    Cmd::XRevRange {
      key,
      end,
      start,
      count,
    } => {
      let end_id = if end == "+" {
        StreamId::max()
      } else {
        parse_stream_id(&end)?
      };
      let start_id = if start == "-" {
        StreamId::min()
      } else {
        parse_stream_id(&start)?
      };
      let options = StreamRangeOptions {
        start: start_id,
        end: end_id,
        count,
        reverse: true,
        exclude_start: false,
        exclude_end: false,
      };
      let entries = db.xrange_with_options(key.as_bytes(), options)?;
      Ok(stream_entries_to_resp(entries))
    }
    Cmd::XDel(key, ids) | Cmd::XDelEx { key, ids } => {
      let mut stream_ids = Vec::with_capacity(ids.len());
      for s in &ids {
        stream_ids.push(parse_stream_id(s)?);
      }
      let count = db.xdel(key.as_bytes(), &stream_ids)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::XTrim {
      key,
      max_len,
      min_id,
      limit,
      ..
    } => {
      let options = if let Some(m) = max_len {
        let mut opt = StreamTrimOptions::maxlen(m as u64);
        if let Some(lim) = limit {
          opt = opt.with_limit(lim);
        }
        opt
      } else if let Some(ref min_s) = min_id {
        let mut opt = StreamTrimOptions::minid(parse_stream_id(min_s)?);
        if let Some(lim) = limit {
          opt = opt.with_limit(lim);
        }
        opt
      } else {
        return Err(Error::invalid_data("ERR syntax error in XTRIM"));
      };
      let trimmed = db.xtrim(key.as_bytes(), options)?;
      Ok(RespValue::Int(trimmed as i64))
    }
    Cmd::XRead {
      streams,
      ids,
      count,
      ..
    } => {
      let mut results = Vec::new();
      for (k, id_s) in streams.iter().zip(ids.iter()) {
        let start_id = if id_s == "$" {
          db.xlast_id(k.as_bytes())?
        } else if id_s == "0" || id_s == "0-0" {
          StreamId::min()
        } else {
          parse_stream_id(id_s)?
        };
        let entries = db.xread(k.as_bytes(), start_id, count)?;
        if !entries.is_empty() {
          results.push(RespValue::Arr(vec![
            RespValue::Blob(k.clone().into_bytes()),
            stream_entries_to_resp(entries),
          ]));
        }
      }
      if results.is_empty() {
        Ok(RespValue::Null)
      } else {
        Ok(RespValue::Arr(results))
      }
    }
    Cmd::XAck(key, group, ids) | Cmd::XAckDel { key, group, ids } => {
      let mut stream_ids = Vec::with_capacity(ids.len());
      for s in &ids {
        stream_ids.push(parse_stream_id(s)?);
      }
      let count = db.xack(key.as_bytes(), &group, &stream_ids)?;
      Ok(RespValue::Int(count as i64))
    }
    Cmd::XClaim {
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
      let mut stream_ids = Vec::with_capacity(ids.len());
      for s in &ids {
        stream_ids.push(parse_stream_id(s)?);
      }
      let mut options = StreamClaimOptions::new(idle.unwrap_or(0));
      if let Some(t) = time {
        options = options.with_time(t);
      }
      if let Some(r) = retrycount {
        options = options.with_retry_count(r);
      }
      if force {
        options = options.force(true);
      }
      if justid {
        options.just_id = true;
      }

      let claim_res = db.xclaim(
        key.as_bytes(),
        &group,
        &consumer,
        min_idle,
        &stream_ids,
        options,
      )?;
      if justid {
        let id_arr = claim_res
          .ids
          .into_iter()
          .map(|id| RespValue::Blob(format!("{id}").into_bytes()))
          .collect();
        Ok(RespValue::Arr(id_arr))
      } else {
        Ok(stream_entries_to_resp(claim_res.entries))
      }
    }
    Cmd::XAutoClaim {
      key,
      group,
      consumer,
      min_idle,
      start,
      count,
      justid,
    } => {
      let start_id = parse_stream_id(&start)?;
      let mut options = StreamAutoClaimOptions::new(min_idle, start_id);
      if let Some(c) = count {
        options = options.count(c);
      }
      if justid {
        options = options.just_id(true);
      }

      let auto_res = db.xautoclaim(key.as_bytes(), &group, &consumer, options)?;
      let next_claim_id = auto_res.next_claim_id;
      let next_cursor = format!("{next_claim_id}");
      let entries_resp = if justid {
        let id_arr = auto_res
          .entries
          .into_iter()
          .map(|(id, _)| RespValue::Blob(format!("{id}").into_bytes()))
          .collect();
        RespValue::Arr(id_arr)
      } else {
        stream_entries_to_resp(auto_res.entries)
      };
      let deleted_resp = RespValue::Arr(
        auto_res
          .deleted_ids
          .into_iter()
          .map(|id| RespValue::Blob(format!("{id}").into_bytes()))
          .collect(),
      );
      Ok(RespValue::Arr(vec![
        RespValue::Blob(next_cursor.into_bytes()),
        entries_resp,
        deleted_resp,
      ]))
    }
    Cmd::XGroup(args) => {
      if args.len() < 2 {
        return Err(Error::invalid_data(
          "ERR wrong number of arguments for XGROUP",
        ));
      }
      let subcmd = args[0].to_ascii_uppercase();
      let key = &args[1];
      match subcmd.as_str() {
        "CREATE" => {
          if args.len() < 4 {
            return Err(Error::invalid_data("ERR syntax error in XGROUP CREATE"));
          }
          let group = &args[2];
          let id_str = &args[3];
          let entries_read = if args.len() > 4 && args[4].eq_ignore_ascii_case("ENTRIESREAD") {
            args.get(5).and_then(|s| s.parse::<i64>().ok())
          } else {
            None
          };
          db.xgroup_create(key.as_bytes(), group, id_str, false, entries_read)?;
          Ok(RespValue::ok())
        }
        "DESTROY" => {
          if args.len() < 3 {
            return Err(Error::invalid_data("ERR syntax error in XGROUP DESTROY"));
          }
          let group = &args[2];
          let destroyed = db.xgroup_destroy(key.as_bytes(), group)?;
          Ok(RespValue::Int(if destroyed { 1 } else { 0 }))
        }
        "CREATECONSUMER" => {
          if args.len() < 4 {
            return Err(Error::invalid_data(
              "ERR syntax error in XGROUP CREATECONSUMER",
            ));
          }
          let group = &args[2];
          let consumer = &args[3];
          let created = db.xgroup_create_consumer(key.as_bytes(), group, consumer)?;
          Ok(RespValue::Int(created as i64))
        }
        "DELCONSUMER" => {
          if args.len() < 4 {
            return Err(Error::invalid_data(
              "ERR syntax error in XGROUP DELCONSUMER",
            ));
          }
          let group = &args[2];
          let consumer = &args[3];
          let pending = db.xgroup_del_consumer(key.as_bytes(), group, consumer)?;
          Ok(RespValue::Int(pending as i64))
        }
        "SETID" => {
          if args.len() < 4 {
            return Err(Error::invalid_data("ERR syntax error in XGROUP SETID"));
          }
          let group = &args[2];
          let id_str = &args[3];
          let entries_read = if args.len() > 4 && args[4].eq_ignore_ascii_case("ENTRIESREAD") {
            args.get(5).and_then(|s| s.parse::<i64>().ok())
          } else {
            None
          };
          db.xgroup_set_id(key.as_bytes(), group, id_str, entries_read)?;
          Ok(RespValue::ok())
        }
        _ => Err(Error::invalid_data(format!(
          "ERR unknown XGROUP subcommand '{subcmd}'"
        ))),
      }
    }
    Cmd::XPending {
      key,
      group,
      start,
      end,
      count,
      consumer,
      idle,
    } => {
      if start.is_none() {
        let summary = db.xpending_summary(key.as_bytes(), &group)?;
        let start_id_blob = if summary.first_entry_id.is_min() {
          RespValue::Null
        } else {
          RespValue::Blob(
            format!(
              "{}-{}",
              summary.first_entry_id.ms, summary.first_entry_id.seq
            )
            .into_bytes(),
          )
        };
        let end_id_blob = if summary.last_entry_id.is_min() {
          RespValue::Null
        } else {
          let last_entry_id = summary.last_entry_id;
          RespValue::Blob(format!("{last_entry_id}").into_bytes())
        };
        let mut consumers_arr = Vec::new();
        for (c_name, c_cnt) in summary.consumer_infos {
          consumers_arr.push(RespValue::Arr(vec![
            RespValue::Blob(c_name.into_bytes()),
            RespValue::Blob(format!("{c_cnt}").into_bytes()),
          ]));
        }
        Ok(RespValue::Arr(vec![
          RespValue::Int(summary.pending_number as i64),
          start_id_blob,
          end_id_blob,
          RespValue::Arr(consumers_arr),
        ]))
      } else {
        let start_id = parse_stream_id(start.as_deref().unwrap_or("-"))?;
        let end_id = parse_stream_id(end.as_deref().unwrap_or("+"))?;
        let mut opts = StreamPendingOptions::range(start_id, end_id, count.unwrap_or(10));
        if let Some(i) = idle {
          opts = opts.idle(i);
        }
        if let Some(c) = consumer {
          opts = opts.consumer(c);
        }
        let entries = db.xpending_range(key.as_bytes(), &group, opts)?;
        let mut arr = Vec::with_capacity(entries.len());
        let now_ms = current_now_ms();
        for e in entries {
          let e_id = e.id;
          let id_str = format!("{e_id}");
          let idle_time = now_ms.saturating_sub(e.pel_entry.last_delivery_time_ms);
          arr.push(RespValue::Arr(vec![
            RespValue::Blob(id_str.into_bytes()),
            RespValue::Blob(e.pel_entry.consumer_name.into_bytes()),
            RespValue::Int(idle_time as i64),
            RespValue::Int(e.pel_entry.last_delivery_count as i64),
          ]));
        }
        Ok(RespValue::Arr(arr))
      }
    }
    Cmd::XReadGroup {
      group,
      consumer,
      streams,
      ids,
      count,
      noack,
      ..
    } => {
      let mut results = Vec::new();
      for (k, id_s) in streams.iter().zip(ids.iter()) {
        let entries = db.xreadgroup(key_bytes(k), &group, &consumer, id_s, count, noack)?;
        if !entries.is_empty() {
          results.push(RespValue::Arr(vec![
            RespValue::Blob(k.clone().into_bytes()),
            stream_entries_to_resp(entries),
          ]));
        }
      }
      if results.is_empty() {
        Ok(RespValue::Null)
      } else {
        Ok(RespValue::Arr(results))
      }
    }
    Cmd::XInfo(subcmd, key) => match subcmd.to_ascii_uppercase().as_str() {
      "STREAM" => {
        let info = db.xinfo_stream(key.as_bytes(), false, None)?;
        Ok(RespValue::Arr(vec![
          RespValue::Simple("length".to_string()),
          RespValue::Int(info.size as i64),
          RespValue::Simple("radix-tree-keys".to_string()),
          RespValue::Int(info.size as i64),
          RespValue::Simple("radix-tree-nodes".to_string()),
          RespValue::Int(info.size as i64),
          RespValue::Simple("groups".to_string()),
          RespValue::Int(info.groups as i64),
          RespValue::Simple("last-generated-id".to_string()),
          RespValue::Blob(
            format!(
              "{}-{}",
              info.last_generated_id.ms, info.last_generated_id.seq
            )
            .into_bytes(),
          ),
          RespValue::Simple("first-entry".to_string()),
          info
            .first_entry
            .map(|e| stream_entries_to_resp(vec![e]))
            .unwrap_or(RespValue::Null),
          RespValue::Simple("last-entry".to_string()),
          info
            .last_entry
            .map(|e| stream_entries_to_resp(vec![e]))
            .unwrap_or(RespValue::Null),
        ]))
      }
      "GROUPS" => {
        let groups = db.xinfo_groups(key.as_bytes())?;
        let mut arr = Vec::with_capacity(groups.len());
        for (group_name, g_meta) in groups {
          arr.push(RespValue::Arr(vec![
            RespValue::Simple("name".to_string()),
            RespValue::Blob(group_name.into_bytes()),
            RespValue::Simple("consumers".to_string()),
            RespValue::Int(g_meta.consumer_number as i64),
            RespValue::Simple("pending".to_string()),
            RespValue::Int(g_meta.pending_number as i64),
            RespValue::Simple("last-delivered-id".to_string()),
            RespValue::Blob(
              format!(
                "{}-{}",
                g_meta.last_delivered_id.ms, g_meta.last_delivered_id.seq
              )
              .into_bytes(),
            ),
          ]));
        }
        Ok(RespValue::Arr(arr))
      }
      "CONSUMERS" => {
        let consumers = db.xinfo_consumers(key.as_bytes(), "default")?;
        let mut arr = Vec::with_capacity(consumers.len());
        for (consumer_name, c_meta) in consumers {
          arr.push(RespValue::Arr(vec![
            RespValue::Simple("name".to_string()),
            RespValue::Blob(consumer_name.into_bytes()),
            RespValue::Simple("pending".to_string()),
            RespValue::Int(c_meta.pending_number as i64),
            RespValue::Simple("idle".to_string()),
            RespValue::Int(c_meta.last_attempted_interaction_ms as i64),
          ]));
        }
        Ok(RespValue::Arr(arr))
      }
      _ => Err(Error::invalid_data(format!(
        "ERR unknown XINFO subcommand '{subcmd}'"
      ))),
    },
    Cmd::XInfoStream { key, .. } => {
      let info = db.xinfo_stream(key.as_bytes(), false, None)?;
      Ok(RespValue::Arr(vec![
        RespValue::Simple("length".to_string()),
        RespValue::Int(info.size as i64),
        RespValue::Simple("last-generated-id".to_string()),
        RespValue::Blob(
          format!(
            "{}-{}",
            info.last_generated_id.ms, info.last_generated_id.seq
          )
          .into_bytes(),
        ),
      ]))
    }
    Cmd::XSetId {
      key,
      last_id,
      entries_added,
      max_deleted_id,
    } => {
      let last_stream_id = parse_stream_id(&last_id)?;
      let max_del_id = max_deleted_id.map(|s| parse_stream_id(&s)).transpose()?;
      db.xsetid(key.as_bytes(), last_stream_id, entries_added, max_del_id)?;
      Ok(RespValue::ok())
    }
    Cmd::XNack { .. } => Ok(RespValue::Int(0)),
    _ => Err(Error::internal("unsupported stream command")),
  }
}

#[inline]
fn key_bytes(s: &str) -> &[u8] {
  s.as_bytes()
}
