pub struct ClientCommands;

impl ClientCommands {
  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTLIST
  pub fn network_clientlist<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR CLIENT LIST is not implemented\r\n");
    Ok(true)
  }
  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTINFO
  pub fn network_clientinfo<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR CLIENT INFO is not implemented\r\n");
    Ok(true)
  }
  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTKILL
  pub fn network_clientkill<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR CLIENT KILL is not implemented\r\n");
    Ok(true)
  }
  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTGETNAME
  pub fn network_clientgetname<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if !parse_state.is_empty() {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'client|getname' command\r\n");
      return Ok(true);
    }
    // We don't have state for client name yet, return null
    output.extend_from_slice(b"$-1\r\n");
    Ok(true)
  }
  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTSETNAME
  pub fn network_clientsetname<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'client|setname' command\r\n");
      return Ok(true);
    }
    // We parse the name to make sure it's valid, but don't store it since there is no state yet
    let name = parse_state[0];
    for &b in name {
      if !(0x21..=0x7E).contains(&b) {
        output.extend_from_slice(
          b"-ERR Client names cannot contain spaces, newlines or special characters.\r\n",
        );
        return Ok(true);
      }
    }
    output.extend_from_slice(b"+OK\r\n");
    Ok(true)
  }
  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTSETINFO
  pub fn network_clientsetinfo<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'client|setinfo' command\r\n");
      return Ok(true);
    }
    let option = parse_state[0];
    if !option.eq_ignore_ascii_case(b"lib-name") && !option.eq_ignore_ascii_case(b"lib-ver") {
      output.extend_from_slice(b"-ERR syntax error\r\n");
      return Ok(true);
    }
    output.extend_from_slice(b"+OK\r\n");
    Ok(true)
  }
  /// libs/server/Resp/ClientCommands.cs:NetworkCLIENTUNBLOCK
  pub fn network_clientunblock<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() || parse_state.len() > 2 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'client|unblock' command\r\n");
      return Ok(true);
    }
    // Parse client id
    let id_str = std::str::from_utf8(parse_state[0]).unwrap_or("");
    if id_str.parse::<i64>().is_err() {
      output.extend_from_slice(b"-ERR value is not an integer or out of range\r\n");
      return Ok(true);
    }
    if parse_state.len() == 2 {
      let option = parse_state[1];
      if !option.eq_ignore_ascii_case(b"TIMEOUT") && !option.eq_ignore_ascii_case(b"ERROR") {
        output.extend_from_slice(b"-ERR syntax error\r\n");
        return Ok(true);
      }
    }
    output.extend_from_slice(b":0\r\n"); // Always unblocks 0 clients currently
    Ok(true)
  }
}
