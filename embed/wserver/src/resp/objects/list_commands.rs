use wobject::list::list_object::{ListObject, ListOperation};

impl crate::resp::resp_server_session::RespServerSession {
  pub fn list_push<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_left: bool,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let items = &parse_state[1..];

    let list_obj = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(val))) => {
        let mut cursor = std::io::Cursor::new(val);
        ListObject::deserialize(&mut cursor).unwrap_or_else(|_| ListObject::new())
      }
      _ => ListObject::new(),
    };

    let op = if is_left {
      ListOperation::Lpush
    } else {
      ListOperation::Rpush
    };

    for item in items {
      list_obj.operate(op, item);
    }

    let mut out_bytes = Vec::new();
    let _ = list_obj.serialize(&mut out_bytes);
    let _ = store.try_upsert_sync(key, &out_bytes);

    let count_str = format!(":{}\r\n", list_obj.count());
    output.extend_from_slice(count_str.as_bytes());
    Ok(true)
  }

  pub fn list_pop<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    is_left: bool,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      output.extend_from_slice(b"-ERR wrong number of arguments for command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];

    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = std::io::Cursor::new(val);
        if let Ok(list_obj) = ListObject::deserialize(&mut cursor) {
          let op = if is_left {
            ListOperation::Lpop
          } else {
            ListOperation::Rpop
          };
          if let Some(res) = list_obj.operate(op, &[]) {
            let mut out_bytes = Vec::new();
            let _ = list_obj.serialize(&mut out_bytes);
            let _ = store.try_upsert_sync(key, &out_bytes);

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
      Ok(Some(None)) | Ok(None) => output.extend_from_slice(b"$-1\r\n"),
      Err(_) => output.extend_from_slice(b"-ERR generic error\r\n"),
    }
    Ok(true)
  }

  pub fn list_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'LLEN' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = std::io::Cursor::new(val);
        if let Ok(list_obj) = ListObject::deserialize(&mut cursor) {
          let count = list_obj.count();
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

  pub fn list_range<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 3 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'LRANGE' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let start_str = std::str::from_utf8(parse_state[1]).unwrap_or("");
    let end_str = std::str::from_utf8(parse_state[2]).unwrap_or("");
    let start = start_str.parse::<isize>().unwrap_or(0);
    let stop = end_str.parse::<isize>().unwrap_or(-1);

    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = std::io::Cursor::new(val);
        if let Ok(list_obj) = ListObject::deserialize(&mut cursor) {
          let items = list_obj.range(start, stop);
          let arr_len = format!("*{}\r\n", items.len());
          output.extend_from_slice(arr_len.as_bytes());
          for item in items {
            let len_str = format!("${}\r\n", item.len());
            output.extend_from_slice(len_str.as_bytes());
            output.extend_from_slice(&item);
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

  pub fn list_index<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'LINDEX' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let idx_str = std::str::from_utf8(parse_state[1]).unwrap_or("");
    let idx = idx_str.parse::<isize>().unwrap_or(0);

    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = std::io::Cursor::new(val);
        if let Ok(list_obj) = ListObject::deserialize(&mut cursor) {
          if let Some(res) = list_obj.index(idx) {
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
      Ok(Some(None)) | Ok(None) => output.extend_from_slice(b"$-1\r\n"),
      Err(_) => output.extend_from_slice(b"-ERR generic error\r\n"),
    }
    Ok(true)
  }

  pub fn list_trim<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 3 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'LTRIM' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];
    let start_str = std::str::from_utf8(parse_state[1]).unwrap_or("");
    let end_str = std::str::from_utf8(parse_state[2]).unwrap_or("");
    let start = start_str.parse::<isize>().unwrap_or(0);
    let stop = end_str.parse::<isize>().unwrap_or(-1);

    let status = store.try_read_sync(key, |v| v.to_vec());
    match status {
      Ok(Some(Some(val))) => {
        let mut cursor = std::io::Cursor::new(val);
        if let Ok(list_obj) = ListObject::deserialize(&mut cursor) {
          list_obj.trim(start, stop);
          let mut out_bytes = Vec::new();
          let _ = list_obj.serialize(&mut out_bytes);
          let _ = store.try_upsert_sync(key, &out_bytes);
          output.extend_from_slice(b"+OK\r\n");
        } else {
          output.extend_from_slice(
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
          );
        }
      }
      Ok(Some(None)) | Ok(None) => output.extend_from_slice(b"+OK\r\n"),
      Err(_) => output.extend_from_slice(b"-ERR generic error\r\n"),
    }
    Ok(true)
  }

  pub fn list_position() {
    unimplemented!()
  }
  pub fn list_pop_multiple() {
    unimplemented!()
  }
  pub fn list_blocking_pop() {
    unimplemented!()
  }
  pub fn list_blocking_move() {
    unimplemented!()
  }
  pub fn list_blocking_pop_push() {
    unimplemented!()
  }
  pub fn list_insert() {
    unimplemented!()
  }
  pub fn list_remove() {
    unimplemented!()
  }
  pub fn list_move() {
    unimplemented!()
  }
  pub fn list_right_pop_left_push() {
    unimplemented!()
  }
  pub fn list_set() {
    unimplemented!()
  }
  pub fn list_blocking_pop_multiple() {
    unimplemented!()
  }
}
