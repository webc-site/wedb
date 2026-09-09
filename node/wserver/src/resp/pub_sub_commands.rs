pub struct PubSubCommands;

impl PubSubCommands {
  /// libs/server/Resp/PubSubCommands.cs:Publish
  pub fn publish<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR PUBLISH is disabled, enable it with --pubsub option.\r\n");
    Ok(true)
  }
  /// libs/server/Resp/PubSubCommands.cs:PatternPublish
  pub fn pattern_publish<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR PUBLISH is disabled, enable it with --pubsub option.\r\n");
    Ok(true)
  }
  /// libs/server/Resp/PubSubCommands.cs:NetworkPUBLISH
  pub fn network_publish<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
        output.extend_from_slice(b"-ERR wrong number of arguments for 'publish' command\r\n");
        return Ok(true);
    }
    output.extend_from_slice(b"-ERR PUBLISH is disabled, enable it with --pubsub option.\r\n");
    Ok(true)
  }
  /// libs/server/Resp/PubSubCommands.cs:NetworkSUBSCRIBE
  pub fn network_subscribe<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
        output.extend_from_slice(b"-ERR wrong number of arguments for 'subscribe' command\r\n");
        return Ok(true);
    }
    output.extend_from_slice(b"-ERR SUBSCRIBE is disabled, enable it with --pubsub option.\r\n");
    Ok(true)
  }
  /// libs/server/Resp/PubSubCommands.cs:NetworkPSUBSCRIBE
  pub fn network_psubscribe<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
        output.extend_from_slice(b"-ERR wrong number of arguments for 'psubscribe' command\r\n");
        return Ok(true);
    }
    output.extend_from_slice(b"-ERR PSUBSCRIBE is disabled, enable it with --pubsub option.\r\n");
    Ok(true)
  }
  /// libs/server/Resp/PubSubCommands.cs:NetworkUNSUBSCRIBE
  pub fn network_unsubscribe<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR UNSUBSCRIBE is disabled, enable it with --pubsub option.\r\n");
    Ok(true)
  }
  /// libs/server/Resp/PubSubCommands.cs:NetworkPUNSUBSCRIBE
  pub fn network_punsubscribe<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR PUNSUBSCRIBE is disabled, enable it with --pubsub option.\r\n");
    Ok(true)
  }
  /// libs/server/Resp/PubSubCommands.cs:NetworkPUBSUB_CHANNELS
  pub fn network_pubsub_channels<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() > 1 {
        output.extend_from_slice(b"-ERR wrong number of arguments for 'pubsub channels' command\r\n");
        return Ok(true);
    }
    output.extend_from_slice(b"-ERR PUBSUB CHANNELS is disabled, enable it with --pubsub option.\r\n");
    Ok(true)
  }
  /// libs/server/Resp/PubSubCommands.cs:NetworkPUBSUB_NUMPAT
  pub fn network_pubsub_numpat<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if !parse_state.is_empty() {
        output.extend_from_slice(b"-ERR wrong number of arguments for 'pubsub numpat' command\r\n");
        return Ok(true);
    }
    output.extend_from_slice(b"-ERR PUBSUB NUMPAT is disabled, enable it with --pubsub option.\r\n");
    Ok(true)
  }
  /// libs/server/Resp/PubSubCommands.cs:NetworkPUBSUB_NUMSUB
  pub fn network_pubsub_numsub<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR PUBSUB NUMSUB is disabled, enable it with --pubsub option.\r\n");
    Ok(true)
  }
}
