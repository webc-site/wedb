use crate::resp::resp_server_session::RespServerSession;

impl RespServerSession {
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkRESTORE
  pub fn network_restore<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR not implemented\r\n");
    Ok(true)
  }
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkDUMP
  pub fn network_dump<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR not implemented\r\n");
    Ok(true)
  }
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkRENAME
  pub fn network_rename<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR not implemented\r\n");
    Ok(true)
  }
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkRENAMENX
  pub fn network_renamenx<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR not implemented\r\n");
    Ok(true)
  }
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkGETDEL
  pub fn network_getdel<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'GETDEL' command\r\n");
      return Ok(true);
    }
    let key = parse_state[0];

    match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(val))) => {
        let len_str = format!("${}\r\n", val.len());
        output.extend_from_slice(len_str.as_bytes());
        output.extend_from_slice(&val);
        output.extend_from_slice(b"\r\n");
        let _ = store.try_delete_sync(key);
      }
      Ok(Some(None)) => {
        output.extend_from_slice(b"$-1\r\n");
      }
      Ok(None) => return Ok(false),
      Err(_) => output.extend_from_slice(b"-ERR generic error\r\n"),
    }
    Ok(true)
  }
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkEXISTS
  pub fn network_exists<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'EXISTS' command\r\n");
      return Ok(true);
    }

    let mut exists_count = 0;
    for key in parse_state {
      let status = store.try_read_sync(key, |_| ());
      if let Ok(Some(Some(_))) = status {
        exists_count += 1;
      }
    }

    let count_str = format!(":{}\r\n", exists_count);
    output.extend_from_slice(count_str.as_bytes());
    Ok(true)
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkEXPIRE
  pub fn network_expire<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'EXPIRE' command\r\n");
      return Ok(true);
    }
    // Stub: pretend successful
    output.extend_from_slice(b":1\r\n");
    Ok(true)
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkPERSIST
  pub fn network_persist<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'PERSIST' command\r\n");
      return Ok(true);
    }
    // Stub
    output.extend_from_slice(b":1\r\n");
    Ok(true)
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkTTL
  pub fn network_ttl<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'TTL' command\r\n");
      return Ok(true);
    }
    // Stub: return -1 (no expiration)
    output.extend_from_slice(b":-1\r\n");
    Ok(true)
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkEXPIRETIME
  pub fn network_expiretime<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR not implemented\r\n");
    Ok(true)
  }
}
