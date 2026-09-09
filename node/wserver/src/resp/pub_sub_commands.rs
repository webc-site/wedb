pub struct PubSubCommands;

impl PubSubCommands {
  /// libs/server/Resp/PubSubCommands.cs:Publish
  pub fn publish<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR not implemented\r\n");
    Ok(true)
  }
  /// libs/server/Resp/PubSubCommands.cs:PatternPublish
  pub fn pattern_publish<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR not implemented\r\n");
    Ok(true)
  }
  /// libs/server/Resp/PubSubCommands.cs:NetworkPUBLISH
  pub fn network_publish<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR not implemented\r\n");
    Ok(true)
  }
  /// libs/server/Resp/PubSubCommands.cs:NetworkSUBSCRIBE
  pub fn network_subscribe<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR not implemented\r\n");
    Ok(true)
  }
  /// libs/server/Resp/PubSubCommands.cs:NetworkPSUBSCRIBE
  pub fn network_psubscribe<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR not implemented\r\n");
    Ok(true)
  }
  /// libs/server/Resp/PubSubCommands.cs:NetworkUNSUBSCRIBE
  pub fn network_unsubscribe<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR not implemented\r\n");
    Ok(true)
  }
  /// libs/server/Resp/PubSubCommands.cs:NetworkPUNSUBSCRIBE
  pub fn network_punsubscribe<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR not implemented\r\n");
    Ok(true)
  }
  /// libs/server/Resp/PubSubCommands.cs:NetworkPUBSUB_CHANNELS
  pub fn network_pubsub_channels<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR not implemented\r\n");
    Ok(true)
  }
  /// libs/server/Resp/PubSubCommands.cs:NetworkPUBSUB_NUMPAT
  pub fn network_pubsub_numpat<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR not implemented\r\n");
    Ok(true)
  }
  /// libs/server/Resp/PubSubCommands.cs:NetworkPUBSUB_NUMSUB
  pub fn network_pubsub_numsub<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.extend_from_slice(b"-ERR not implemented\r\n");
    Ok(true)
  }
}
