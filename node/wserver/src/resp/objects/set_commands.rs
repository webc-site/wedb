use std::io::Cursor;

use wobject::set::set_object::{SetObject, SetOperation};

use crate::resp::resp_server_session::RespServerSession;

impl RespServerSession {
  pub fn set_add<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'SADD' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let members = &parse_state[1..];

    let set_obj = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        SetObject::deserialize(&mut cursor).unwrap_or_else(|_| SetObject::new())
      }
      _ => SetObject::new(),
    };

    let mut added = 0;
    for member in members {
      if set_obj.operate(SetOperation::Sadd, member) {
        added += 1;
      }
    }

    if added > 0 {
      let mut out_bytes = Vec::new();
      let _ = set_obj.serialize(&mut out_bytes);
      let _ = store.try_upsert_sync(key, &out_bytes);
    }

    let count_str = format!(":{}\r\n", added);
    output.extend_from_slice(count_str.as_bytes());
    Ok(true)
  }

  pub fn set_remove<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'SREM' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let members = &parse_state[1..];

    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        if let Ok(set_obj) = SetObject::deserialize(&mut cursor) {
          let mut removed = 0;
          for member in members {
            if set_obj.operate(SetOperation::Srem, member) {
              removed += 1;
            }
          }
          if removed > 0 {
            let mut out_bytes = Vec::new();
            let _ = set_obj.serialize(&mut out_bytes);
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
      Ok(Some(None)) => output.extend_from_slice(b":0\r\n"),
      Ok(None) => return Ok(false),
      Err(_) => output.extend_from_slice(b"-ERR generic error\r\n"),
    }
    Ok(true)
  }

  pub fn set_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'SCARD' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        if let Ok(set_obj) = SetObject::deserialize(&mut cursor) {
          let count = set_obj.count();
          let count_str = format!(":{}\r\n", count);
          output.extend_from_slice(count_str.as_bytes());
        } else {
          output.extend_from_slice(
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
          );
        }
      }
      Ok(Some(None)) => output.extend_from_slice(b":0\r\n"),
      Ok(None) => return Ok(false),
      Err(_) => output.extend_from_slice(b"-ERR generic error\r\n"),
    }
    Ok(true)
  }

  pub fn set_members<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'SMEMBERS' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        if let Ok(set_obj) = SetObject::deserialize(&mut cursor) {
          let members = set_obj.get_keys();
          let arr_len = format!("*{}\r\n", members.len());
          output.extend_from_slice(arr_len.as_bytes());
          for m in members {
            let len_str = format!("${}\r\n", m.len());
            output.extend_from_slice(len_str.as_bytes());
            output.extend_from_slice(&m);
            output.extend_from_slice(b"\r\n");
          }
        } else {
          output.extend_from_slice(
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
          );
        }
      }
      Ok(Some(None)) => output.extend_from_slice(b"*0\r\n"),
      Ok(None) => return Ok(false),
      Err(_) => output.extend_from_slice(b"-ERR generic error\r\n"),
    }
    Ok(true)
  }

  pub fn set_is_member<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'SISMEMBER' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let member = parse_state[1];
    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        if let Ok(set_obj) = SetObject::deserialize(&mut cursor) {
          if set_obj.operate(SetOperation::Sismember, member) {
            output.extend_from_slice(b":1\r\n");
          } else {
            output.extend_from_slice(b":0\r\n");
          }
        } else {
          output.extend_from_slice(
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
          );
        }
      }
      Ok(Some(None)) => output.extend_from_slice(b":0\r\n"),
      Ok(None) => return Ok(false),
      Err(_) => output.extend_from_slice(b"-ERR generic error\r\n"),
    }
    Ok(true)
  }

  pub fn set_pop<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      // Redis SPOP supports count but Garnet's basic implementation sometimes handles count differently
      // Let's do simple pop for 1 element
      output.extend_from_slice(b"-ERR wrong number of arguments for 'SPOP' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        if let Ok(set_obj) = SetObject::deserialize(&mut cursor) {
          if let Some(m) = set_obj.pop() {
            let mut out_bytes = Vec::new();
            let _ = set_obj.serialize(&mut out_bytes);
            let _ = store.try_upsert_sync(key, &out_bytes);
            let len_str = format!("${}\r\n", m.len());
            output.extend_from_slice(len_str.as_bytes());
            output.extend_from_slice(&m);
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
      Ok(Some(None)) => output.extend_from_slice(b"$-1\r\n"),
      Ok(None) => return Ok(false),
      Err(_) => output.extend_from_slice(b"-ERR generic error\r\n"),
    }
    Ok(true)
  }

  pub fn set_random_member<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'SRANDMEMBER' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        if let Ok(set_obj) = SetObject::deserialize(&mut cursor) {
          if let Some(m) = set_obj.random_member() {
            let len_str = format!("${}\r\n", m.len());
            output.extend_from_slice(len_str.as_bytes());
            output.extend_from_slice(&m);
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
      Ok(Some(None)) => output.extend_from_slice(b"$-1\r\n"),
      Ok(None) => return Ok(false),
      Err(_) => output.extend_from_slice(b"-ERR generic error\r\n"),
    }
    Ok(true)
  }

  pub fn set_intersect() {
    unimplemented!()
  }
  pub fn set_intersect_store() {
    unimplemented!()
  }
  pub fn set_intersect_length() {
    unimplemented!()
  }
  pub fn set_union() {
    unimplemented!()
  }
  pub fn set_union_store() {
    unimplemented!()
  }
  pub fn set_move() {
    unimplemented!()
  }
  pub fn set_diff() {
    unimplemented!()
  }
  pub fn set_diff_store() {
    unimplemented!()
  }
}
