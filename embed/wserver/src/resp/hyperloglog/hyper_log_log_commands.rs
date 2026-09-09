impl crate::resp::resp_server_session::RespServerSession {
  pub fn hyper_log_log_add<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    _output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    unimplemented!()
  }

  pub fn hyper_log_log_length<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    _output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    unimplemented!()
  }

  pub fn hyper_log_log_merge<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    _output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    unimplemented!()
  }
}
