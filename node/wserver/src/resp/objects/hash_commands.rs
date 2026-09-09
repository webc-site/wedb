use std::io::Cursor;

use wobject::hash::hash_object::HashObject;

use crate::resp::resp_server_session::RespServerSession;

impl RespServerSession {
  /// libs/server/Resp/Objects/HashCommands.cs:HashGet
  pub fn hash_get<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'HGET' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let field = parse_state[1];

    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        if let Ok(hash_obj) = HashObject::deserialize(&mut cursor) {
          if let Some(res) = hash_obj.operate(2 /* HGET */, field, &[]) {
            let len_str = format!("${}\r\n", res.len());
            output.extend_from_slice(len_str.as_bytes());
            output.extend_from_slice(&res);
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
      Ok(Some(None)) => {
        output.extend_from_slice(b"$-1\r\n");
      }
      Ok(None) => return Ok(false),
      Err(_) => {
        output.extend_from_slice(b"-ERR generic error\r\n");
      }
    }
    Ok(true)
  }

  /// libs/server/Resp/Objects/HashCommands.cs:HashSet
  pub fn hash_set<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 3 || parse_state.len() % 2 != 1 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'HSET' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];

    let hash_obj = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        HashObject::deserialize(&mut cursor).unwrap_or_else(|_| HashObject::new())
      }
      _ => HashObject::new(),
    };

    let mut added = 0;
    for i in (1..parse_state.len()).step_by(2) {
      let field = parse_state[i];
      let value = parse_state[i + 1];
      if hash_obj.operate(0 /* HSET */, field, value).is_none() {
        // HashObject operate returns None for HSET.
        added += 1;
      }
    }

    let mut out_bytes = Vec::new();
    let _ = hash_obj.serialize(&mut out_bytes);
    let _ = store.try_upsert_sync(key, &out_bytes);

    let count_str = format!(":{}\r\n", added);
    output.extend_from_slice(count_str.as_bytes());
    Ok(true)
  }

  /// libs/server/Resp/Objects/HashCommands.cs:HashGetAll
  pub fn hash_get_all<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'HGETALL' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        if let Ok(hash_obj) = HashObject::deserialize(&mut cursor) {
          let all = hash_obj.hash_get_all();
          let arr_len = format!("*{}\r\n", all.len() * 2);
          output.extend_from_slice(arr_len.as_bytes());
          for (k, v) in all {
            let k_len = format!("${}\r\n", k.len());
            output.extend_from_slice(k_len.as_bytes());
            output.extend_from_slice(&k);
            output.extend_from_slice(b"\r\n");
            let v_len = format!("${}\r\n", v.len());
            output.extend_from_slice(v_len.as_bytes());
            output.extend_from_slice(&v);
            output.extend_from_slice(b"\r\n");
          }
        } else {
          output.extend_from_slice(
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
          );
        }
      }
      Ok(Some(None)) => {
        output.extend_from_slice(b"*0\r\n");
      }
      Ok(None) => return Ok(false),
      Err(_) => output.extend_from_slice(b"-ERR generic error\r\n"),
    }
    Ok(true)
  }

  /// libs/server/Resp/Objects/HashCommands.cs:HashGetMultiple
  pub fn hash_get_multiple<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'HMGET' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let fields = &parse_state[1..];
    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        if let Ok(hash_obj) = HashObject::deserialize(&mut cursor) {
          let arr_len = format!("*{}\r\n", fields.len());
          output.extend_from_slice(arr_len.as_bytes());
          for field in fields {
            if let Some(res) = hash_obj.operate(2 /* HGET */, field, &[]) {
              let len_str = format!("${}\r\n", res.len());
              output.extend_from_slice(len_str.as_bytes());
              output.extend_from_slice(&res);
              output.extend_from_slice(b"\r\n");
            } else {
              output.extend_from_slice(b"$-1\r\n");
            }
          }
        } else {
          output.extend_from_slice(
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
          );
        }
      }
      Ok(Some(None)) => {
        let arr_len = format!("*{}\r\n", fields.len());
        output.extend_from_slice(arr_len.as_bytes());
        for _ in fields {
          output.extend_from_slice(b"$-1\r\n");
        }
      }
      Ok(None) => return Ok(false),
      Err(_) => output.extend_from_slice(b"-ERR generic error\r\n"),
    }
    Ok(true)
  }

  /// libs/server/Resp/Objects/HashCommands.cs:HashLength
  pub fn hash_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'HLEN' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        if let Ok(hash_obj) = HashObject::deserialize(&mut cursor) {
          if let Some(res) = hash_obj.operate(6 /* HLEN */, &[], &[]) {
            output.extend_from_slice(b":");
            output.extend_from_slice(&res);
            output.extend_from_slice(b"\r\n");
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

  /// libs/server/Resp/Objects/HashCommands.cs:HashDelete
  pub fn hash_delete<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'HDEL' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let fields = &parse_state[1..];
    let mut deleted = 0;

    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        if let Ok(hash_obj) = HashObject::deserialize(&mut cursor) {
          for field in fields {
            if hash_obj.operate(5 /* HDEL */, field, &[]).is_some() {
              deleted += 1;
            }
          }
          if deleted > 0 {
            let mut out_bytes = Vec::new();
            let _ = hash_obj.serialize(&mut out_bytes);
            let _ = store.try_upsert_sync(key, &out_bytes);
          }
          let count_str = format!(":{}\r\n", deleted);
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

  /// libs/server/Resp/Objects/HashCommands.cs:HashExists
  pub fn hash_exists<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'HEXISTS' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let field = parse_state[1];
    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        if let Ok(hash_obj) = HashObject::deserialize(&mut cursor) {
          if let Some(res) = hash_obj.operate(7 /* HEXISTS */, field, &[]) {
            output.extend_from_slice(b":");
            output.extend_from_slice(&res);
            output.extend_from_slice(b"\r\n");
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

  /// libs/server/Resp/Objects/HashCommands.cs:HashKeys
  pub fn hash_keys<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'HKEYS' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        if let Ok(hash_obj) = HashObject::deserialize(&mut cursor) {
          let keys = hash_obj.get_keys();
          let arr_len = format!("*{}\r\n", keys.len());
          output.extend_from_slice(arr_len.as_bytes());
          for k in keys {
            let len_str = format!("${}\r\n", k.len());
            output.extend_from_slice(len_str.as_bytes());
            output.extend_from_slice(&k);
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

  pub fn hash_vals<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'HVALS' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = Cursor::new(val);
        if let Ok(hash_obj) = HashObject::deserialize(&mut cursor) {
          let vals = hash_obj.get_values();
          let arr_len = format!("*{}\r\n", vals.len());
          output.extend_from_slice(arr_len.as_bytes());
          for v in vals {
            let len_str = format!("${}\r\n", v.len());
            output.extend_from_slice(len_str.as_bytes());
            output.extend_from_slice(&v);
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

  /// libs/server/Resp/Objects/HashCommands.cs:HashRandomField
  pub fn hash_random_field() {
    unimplemented!()
  }
  /// libs/server/Resp/Objects/HashCommands.cs:HashStrLength
  pub fn hash_str_length() {
    unimplemented!()
  }
  /// libs/server/Resp/Objects/HashCommands.cs:HashIncrement
  pub fn hash_increment() {
    unimplemented!()
  }
  /// libs/server/Resp/Objects/HashCommands.cs:HashTimeToLive
  pub fn hash_time_to_live() {
    unimplemented!()
  }
}
