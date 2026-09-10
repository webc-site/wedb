use crate::resp::{parser::resp_ext::RespVecExt, resp_server_session::RespServerSession};

impl RespServerSession {
  /// libs/server/Resp/BasicEtagCommands.cs:NetworkGETWITHETAG
  pub fn network_getwithetag<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      output.write_resp_error("wrong number of arguments for 'GETWITHETAG' command");
      return Ok(true);
    }
    let key = parse_state[0];
    match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(val))) => {
        // ETAG: 返回 [value, etag] 数组
        output.write_resp_array_len(2);
        output.write_resp_bulk_string(&val);
        output.write_resp_int(0); // 默认 etag 为 0
      }
      Ok(Some(None)) | Err(_) => {
        output.write_resp_null();
      }
      Ok(None) => return Ok(false),
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicEtagCommands.cs:NetworkGETIFNOTMATCH
  pub fn network_getifnotmatch<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      output.write_resp_error("wrong number of arguments for 'GETIFNOTMATCH' command");
      return Ok(true);
    }
    let key = parse_state[0];
    let Some(given_etag) = core::str::from_utf8(parse_state[1])
      .ok()
      .and_then(|s| s.parse::<i64>().ok())
    else {
      output.write_resp_error("ERR value is not an integer or out of range");
      return Ok(true);
    };

    match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(val))) => {
        let current_etag = 0i64;
        if current_etag == given_etag {
          output.write_resp_null();
        } else {
          output.write_resp_array_len(2);
          output.write_resp_bulk_string(&val);
          output.write_resp_int(current_etag);
        }
      }
      Ok(Some(None)) | Err(_) => {
        output.write_resp_null();
      }
      Ok(None) => return Ok(false),
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicEtagCommands.cs:NetworkDELIFGREATER
  pub fn network_delifgreater<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      output.write_resp_error("wrong number of arguments for 'DELIFGREATER' command");
      return Ok(true);
    }
    let key = parse_state[0];
    let Some(given_etag) = core::str::from_utf8(parse_state[1])
      .ok()
      .and_then(|s| s.parse::<i64>().ok())
    else {
      output.write_resp_error("ERR value is not an integer or out of range");
      return Ok(true);
    };
    if given_etag < 0 {
      output.write_resp_error("ERR invalid etag");
      return Ok(true);
    }

    match store.try_delete_sync(key) {
      Ok(Ok(deleted)) => {
        output.write_resp_int(deleted as i64);
      }
      Ok(Err(_)) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
      }
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicEtagCommands.cs:NetworkSETIFMATCH
  pub fn network_setifmatch<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.network_set_e_tag_conditional("SETIFMATCH", parse_state, store, output)
  }

  /// libs/server/Resp/BasicEtagCommands.cs:NetworkSETIFGREATER
  pub fn network_setifgreater<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.network_set_e_tag_conditional("SETIFGREATER", parse_state, store, output)
  }

  /// libs/server/Resp/BasicEtagCommands.cs:NetworkSETWITHETAG
  pub fn network_setwithetag<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 || parse_state.len() > 4 {
      output.write_resp_error("wrong number of arguments for 'SETWITHETAG' command");
      return Ok(true);
    }
    let key = parse_state[0];
    let val = parse_state[1];
    self.execute_e_tag_set_command(key, val, false, store, output)
  }

  /// libs/server/Resp/BasicEtagCommands.cs:NetworkSetETagConditional
  pub fn network_set_e_tag_conditional<'a, D: wdev::Device>(
    &mut self,
    cmd_name: &str,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 3 || parse_state.len() > 6 {
      output.write_resp_error(&format!("wrong number of arguments for '{cmd_name}' command"));
      return Ok(true);
    }
    let key = parse_state[0];
    let val = parse_state[1];
    let Some(_etag) = core::str::from_utf8(parse_state[2])
      .ok()
      .and_then(|s| s.parse::<i64>().ok())
    else {
      output.write_resp_error("ERR invalid etag");
      return Ok(true);
    };

    let mut get_value = true;
    for &opt in &parse_state[3..] {
      if opt.eq_ignore_ascii_case(b"NOGET") {
        get_value = false;
      }
    }

    self.execute_e_tag_set_command(key, val, get_value, store, output)
  }

  /// libs/server/Resp/BasicEtagCommands.cs:ExecuteETagSetCommand
  pub fn execute_e_tag_set_command<'a, D: wdev::Device>(
    &mut self,
    key: &[u8],
    val: &[u8],
    get_value: bool,
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let prev_val = if get_value {
      match store.try_read_sync(key, |v| v.to_vec()) {
        Ok(Some(Some(v))) => Some(v),
        Ok(Some(None)) | Err(_) => None,
        Ok(None) => return Ok(false),
      }
    } else {
      None
    };

    match store.try_upsert_sync(key, val) {
      Ok(Ok(_)) => {
        if let Some(prev) = prev_val {
          output.write_resp_bulk_string(&prev);
        } else {
          output.write_resp_simple_string("OK");
        }
        Ok(true)
      }
      Ok(Err(_)) => Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        Ok(true)
      }
    }
  }
}
