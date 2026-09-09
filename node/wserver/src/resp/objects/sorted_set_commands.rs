use std::{io::Cursor, str};

use wobject::sorted_set::sorted_set_object::{SortedSetObject, SortedSetOperation};

use crate::resp::{parser::resp_ext::RespSliceExt, resp_server_session::RespServerSession};

impl RespServerSession {
  pub fn sorted_set_add<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 3 || parse_state.len().is_multiple_of(2) {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZADD' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];

    let zset = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        SortedSetObject::deserialize(&mut cursor).unwrap_or_else(|_| SortedSetObject::new())
      }
      _ => SortedSetObject::new(),
    };

    let mut added = 0;
    for i in (1..parse_state.len()).step_by(2) {
      let score = parse_state[i].parse_f64(0.0);
      if true {
        let member = parse_state[i + 1];
        let old_score = zset.operate(SortedSetOperation::Zscore, member, 0.0);
        zset.operate(SortedSetOperation::Zadd, member, score);
        if old_score.is_none() {
          added += 1;
        }
      } else {
        output.extend_from_slice(b"-ERR value is not a valid float\r\n");
        return Ok(true);
      }
    }

    let mut out_bytes = Vec::new();
    let _ = zset.serialize(&mut out_bytes);
    let _ = store.try_upsert_sync(key, &out_bytes);

    let count_str = format!(":{}\r\n", added);
    output.extend_from_slice(count_str.as_bytes());
    Ok(true)
  }

  pub fn sorted_set_score<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZSCORE' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let member = parse_state[1];

    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        if let Ok(zset) = SortedSetObject::deserialize(&mut cursor) {
          if let Some(score) = zset.operate(SortedSetOperation::Zscore, member, 0.0) {
            let score_str = format!("{}", score);
            let len_str = format!("${}\r\n", score_str.len());
            output.extend_from_slice(len_str.as_bytes());
            output.extend_from_slice(score_str.as_bytes());
            output.extend_from_slice(b"\r\n");
          } else {
            output.extend_from_slice(b"$-1\r\n");
          }
        } else {
          output.extend_from_slice(
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
          );
        }
      }
      Ok(Some(None)) | Ok(None) => output.extend_from_slice(b"$-1\r\n"),
      Err(_) => output.extend_from_slice(b"-ERR generic error\r\n"),
    }
    Ok(true)
  }

  pub fn sorted_set_remove<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZREM' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let members = &parse_state[1..];

    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        if let Ok(zset) = SortedSetObject::deserialize(&mut cursor) {
          let mut removed = 0;
          for member in members {
            if zset
              .operate(SortedSetOperation::Zrem, member, 0.0)
              .is_some()
            {
              removed += 1;
            }
          }
          if removed > 0 {
            let mut out_bytes = Vec::new();
            let _ = zset.serialize(&mut out_bytes);
            let _ = store.try_upsert_sync(key, &out_bytes);
          }
          let count_str = format!(":{}\r\n", removed);
          output.extend_from_slice(count_str.as_bytes());
        } else {
          output.extend_from_slice(
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
          );
        }
      }
      Ok(Some(None)) | Ok(None) => output.extend_from_slice(b":0\r\n"),
      Err(_) => output.extend_from_slice(b"-ERR generic error\r\n"),
    }
    Ok(true)
  }

  pub fn sorted_set_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'ZCARD' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        if let Ok(zset) = SortedSetObject::deserialize(&mut cursor) {
          let count = zset.count();
          let count_str = format!(":{}\r\n", count);
          output.extend_from_slice(count_str.as_bytes());
        } else {
          output.extend_from_slice(
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
          );
        }
      }
      Ok(Some(None)) | Ok(None) => output.extend_from_slice(b":0\r\n"),
      Err(_) => output.extend_from_slice(b"-ERR generic error\r\n"),
    }
    Ok(true)
  }

  pub fn sorted_set_pop<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_min: bool,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      output.extend_from_slice(b"-ERR wrong number of arguments for command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    // count defaults to 1
    let mut count = 1;
    if parse_state.len() >= 2 {
      let c_str = str::from_utf8(parse_state[1]).unwrap_or("");
      if let Ok(c) = c_str.parse::<usize>() {
        count = c;
      } else {
        output.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
        return Ok(true);
      }
    }

    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        if let Ok(zset) = SortedSetObject::deserialize(&mut cursor) {
          let mut popped = Vec::new();
          for _ in 0..count {
            let item = if is_min {
              zset.pop_min()
            } else {
              zset.pop_max()
            };
            if let Some((m, s)) = item {
              popped.push((m, s));
            } else {
              break;
            }
          }
          if !popped.is_empty() {
            let mut out_bytes = Vec::new();
            let _ = zset.serialize(&mut out_bytes);
            let _ = store.try_upsert_sync(key, &out_bytes);
          }
          let arr_len = format!("*{}\r\n", popped.len() * 2);
          output.extend_from_slice(arr_len.as_bytes());
          for (m, s) in popped {
            let len_str = format!("${}\r\n", m.len());
            output.extend_from_slice(len_str.as_bytes());
            output.extend_from_slice(&m);
            output.extend_from_slice(b"\r\n");

            let score_str = format!("{}", s);
            let slen_str = format!("${}\r\n", score_str.len());
            output.extend_from_slice(slen_str.as_bytes());
            output.extend_from_slice(score_str.as_bytes());
            output.extend_from_slice(b"\r\n");
          }
        } else {
          output.extend_from_slice(
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
          );
        }
      }
      Ok(Some(None)) | Ok(None) => output.extend_from_slice(b"*0\r\n"),
      Err(_) => output.extend_from_slice(b"-ERR generic error\r\n"),
    }
    Ok(true)
  }

  // Stubs for the rest
  pub fn sorted_set_range() {
    unimplemented!()
  }
  pub fn sorted_set_range_store() {
    unimplemented!()
  }
  pub fn sorted_set_scores() {
    unimplemented!()
  }
  pub fn sorted_set_m_pop() {
    unimplemented!()
  }
  pub fn sorted_set_count() {
    unimplemented!()
  }
  pub fn sorted_set_length_by_value() {
    unimplemented!()
  }
  pub fn sorted_set_increment() {
    unimplemented!()
  }
  pub fn sorted_set_rank() {
    unimplemented!()
  }
  pub fn sorted_set_remove_range() {
    unimplemented!()
  }
  pub fn sorted_set_random_member() {
    unimplemented!()
  }
  pub fn sorted_set_difference() {
    unimplemented!()
  }
  pub fn sorted_set_difference_store() {
    unimplemented!()
  }
  pub fn sorted_set_intersect() {
    unimplemented!()
  }
  pub fn sorted_set_intersect_length() {
    unimplemented!()
  }
  pub fn sorted_set_intersect_store() {
    unimplemented!()
  }
  pub fn sorted_set_union() {
    unimplemented!()
  }
  pub fn sorted_set_union_store() {
    unimplemented!()
  }
  pub fn sorted_set_blocking_pop() {
    unimplemented!()
  }
  pub fn sorted_set_blocking_m_pop() {
    unimplemented!()
  }
  pub fn sorted_set_expire() {
    unimplemented!()
  }
  pub fn sorted_set_time_to_live() {
    unimplemented!()
  }
  pub fn sorted_set_persist() {
    unimplemented!()
  }
}
